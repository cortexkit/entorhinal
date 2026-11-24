//! Durable, machine-local cleanup after the move has crossed its final boundary.

use std::sync::atomic::Ordering;
use std::time::Duration;

use entorhinal_core::RegistryStore;
use serde_json::json;

use crate::door_client::{CutRequest, DoorError};
use crate::move_driver::{backoff, Worker};
use crate::HandlerError;

const SLOW_RETRY: Duration = Duration::from_secs(6 * 60 * 60);

/// How often the releaser looks for due releases. A release is never urgent,
/// since the move it follows has already finished, so a coarse cadence avoids a
/// store read every second on a machine that is almost always idle. Retry
/// deadlines finer than this are honoured at the next scan.
const SCAN_INTERVAL: Duration = Duration::from_secs(15);

pub(crate) async fn run(worker: Worker) {
    let mut interval = tokio::time::interval(SCAN_INTERVAL);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if worker.store.upgrade().is_none() {
            break;
        }
        let _ = tick(&worker).await;
    }
}

pub(crate) async fn tick(worker: &Worker) -> Result<(), HandlerError> {
    // Scan committed checkpoints without taking the writer; only due releases write.
    let rows = worker
        .sql(|store| {
            if store.is_foreign() {
                Ok(Vec::new())
            } else {
                store.list_agent_moves()
            }
        })
        .await?;
    worker.health.cut_releases_pending.store(
        rows.iter()
            .filter(|row| row.plan["_cut_release"]["state"] == "pending")
            .count() as u64,
        Ordering::Relaxed,
    );
    for row in rows {
        let mut release = row.plan["_cut_release"].clone();
        if release["state"] != "pending"
            || release["retry_at_ms"].as_i64().unwrap_or(0) > (worker.clock)()
        {
            continue;
        }
        // Persist the next deadline before sending: a crash or lost reply must
        // not turn an unsupported plane into a rapid retry loop after restart.
        // Use the slow deadline until a reply tells us a shorter one is safe.
        let attempts = release["attempts"].as_i64().unwrap_or(0).saturating_add(1);
        release["attempts"] = json!(attempts);
        release["retry_at_ms"] =
            json!((worker.clock)().saturating_add(SLOW_RETRY.as_millis() as i64));
        save(worker, &row.cut_id, release.clone()).await?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            worker.door.release(&CutRequest {
                cut_id: row.cut_id.clone(),
            }),
        )
        .await;
        let mut stopped = None;
        match result {
            Ok(Ok(reply)) => {
                release = json!({"state":"released","outcome":reply.outcome,
                    "reservationDropped":reply.reservation_dropped});
            }
            Ok(Err(DoorError::Refused { code, .. }))
                if matches!(
                    code.as_str(),
                    "invalid_request" | "caller_forbidden" | "cut_release_unknown"
                ) =>
            {
                release = json!({"state":"stopped","code":code});
                stopped = Some(code);
            }
            Ok(Err(DoorError::Refused { code, .. })) if code == "plane_version_unsupported" => {}
            _ => {
                release["retry_at_ms"] =
                    json!((worker.clock)().saturating_add(backoff(attempts).as_millis() as i64));
            }
        }
        save(worker, &row.cut_id, release.clone()).await?;
        if release["state"] == "released" {
            worker
                .health
                .cut_releases_released
                .fetch_add(1, Ordering::Relaxed);
        } else if let Some(code) = stopped {
            worker
                .health
                .cut_releases_stopped
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(cut_id = row.cut_id, code, "cut release stopped");
        }
    }
    worker.gauges().await
}

async fn save(worker: &Worker, cut: &str, release: serde_json::Value) -> Result<(), HandlerError> {
    let cut = cut.to_owned();
    worker
        .sql(move |store: &RegistryStore| store.set_move_cut_release(&cut, &release))
        .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        door_client::{Request as DoorRequest, SealRequest},
        fake_door::{Effect as DoorEffect, FakeDoor},
        log_client::{LogConnector, LogTransport, TransportError},
        move_driver::{MoveWake, SqlOperations},
        tests::scratch_descriptor,
        ProjectsHandler,
    };
    use entorhinal_core::{
        move_store::{AgentMove, MoveSide, MoveStep},
        RegistryStore,
    };
    use serde_json::Value;
    use std::{
        cell::Cell,
        path::PathBuf,
        sync::{
            atomic::{AtomicUsize, Ordering},
            Arc,
        },
    };

    const CUT: &str = "aabb0123456789abcdef0123456789ab";
    thread_local! { static NOW: Cell<i64> = const { Cell::new(1000) }; }
    fn clock() -> i64 {
        NOW.with(|now| now.get())
    }
    fn elapse(ms: i64) {
        NOW.with(|now| now.set(now.get() + ms));
    }
    async fn drive<F: std::future::Future>(future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => break result,
                () = tokio::task::yield_now() => {},
            }
        }
    }
    async fn advance(ms: i64) {
        elapse(ms);
        tokio::time::advance(Duration::from_millis(ms as u64)).await;
    }

    struct Fixture {
        handler: ProjectsHandler,
        worker: Worker,
        door: FakeDoor,
        descriptor: cortexkit_store_types::StorageDescriptor,
        dir: PathBuf,
    }

    impl Fixture {
        async fn new(side: MoveSide, step: MoveStep) -> Self {
            NOW.with(|now| now.set(1000));
            let (dir, descriptor) = scratch_descriptor("cut-release");
            let mut handler = ProjectsHandler::with_runtime("cut-release".into(), clock);
            let door = FakeDoor::new([1; 16]);
            handler.door_client = door.client();
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            let worker = Worker {
                store: Arc::downgrade(&handler.store),
                sql_operations: Arc::new(SqlOperations::default()),
                door: handler.door_client.clone(),
                discovery: handler.participant_discovery.clone(),
                catalog: None,
                health: handler.health.clone(),
                clock,
                wake: Arc::new(MoveWake::default()),
            };
            let item = crate::door_client::CutItem {
                session_ref: "11".repeat(16),
                stream: "reserved:data".into(),
                end_offset: 1,
                digest: "a".repeat(64),
            };
            door.set_stream(item.clone());
            door.script(
                "cut.seal",
                DoorEffect::Seal(crate::door_client::SealState::Published, None),
            );
            drive(handler.door_client.seal(&SealRequest {
                cut_id: CUT.into(),
                items: vec![item],
                receiver: None,
            }))
            .await
            .unwrap();
            handler
                .store
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .insert_agent_move(&AgentMove {
                    cut_id: CUT.into(),
                    agent_id: "agent_0123456789abcdef".into(),
                    side,
                    peer: "02".repeat(16),
                    step,
                    plan: json!({"agent_id":"agent_0123456789abcdef","from":"01".repeat(16),
                        "to":"02".repeat(16),"claim_version":1,"hub_gen":1,"participants":[],
                        "_cut_release":{"state":"pending","attempts":0,"retry_at_ms":0}}),
                    request_body: None,
                    record_version: 1,
                    claim_version: Some(1),
                    claim_take_body: None,
                    claim_take_version: None,
                    claim_landed_version: None,
                    seal_sent: true,
                    abort_requested: None,
                    hold: None,
                    cut_accept: None,
                    reason: None,
                    detail: None,
                    request_key: None,
                    attempts: 0,
                    activated_at_ms: None,
                    created_at_ms: clock(),
                    updated_at_ms: clock(),
                })
                .unwrap();
            Self {
                handler,
                worker,
                door,
                descriptor,
                dir,
            }
        }

        fn store(&self) -> RegistryStore {
            self.handler.with_store(|store| Ok(store.clone())).unwrap()
        }
        fn row(&self) -> AgentMove {
            self.store().agent_move(CUT).unwrap().unwrap()
        }
        fn state(&self) -> Value {
            self.row().plan["_cut_release"].clone()
        }
        async fn tick(&self) {
            drive(tick(&self.worker)).await.unwrap();
        }
        fn calls(&self) -> usize {
            self.door
                .calls()
                .iter()
                .filter(|call| call.request.op() == "cut.release")
                .count()
        }
        fn reopen(&self) {
            self.handler.store.lock().unwrap().take();
            *self.handler.store.lock().unwrap() =
                Some(RegistryStore::open(&self.descriptor).unwrap());
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn published_receiver_releases_once_and_status_is_optional() {
        let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
        let generation = f.store().generation().unwrap();
        f.door.script("cut.release", DoorEffect::Release(false));
        f.tick().await;
        f.tick().await;
        assert_eq!(f.calls(), 1);
        assert_eq!(
            f.state(),
            json!({"state":"released","outcome":"published","reservationDropped":false})
        );
        let public = f.store().move_status_view(CUT).unwrap().unwrap();
        assert_eq!(public["cut_release"], f.state());
        assert_eq!(f.store().moves_view().unwrap()[0]["cut_release"], f.state());
        assert_eq!(f.store().generation().unwrap(), generation);
        assert_eq!(
            f.handler
                .health
                .cut_releases_pending
                .load(Ordering::Relaxed),
            0
        );
        assert_eq!(
            f.handler
                .health
                .cut_releases_released
                .load(Ordering::Relaxed),
            1
        );
    }

    #[tokio::test(start_paused = true)]
    async fn aborted_cut_releases_only_on_aborting_machine() {
        let f = Fixture::new(MoveSide::Source, MoveStep::Aborted).await;
        drive(f.worker.door.abort(&CutRequest { cut_id: CUT.into() }))
            .await
            .unwrap();
        f.tick().await;
        f.tick().await;
        assert_eq!(f.calls(), 1);
        assert_eq!(
            f.state(),
            json!({"state":"released","outcome":"aborted","reservationDropped":true})
        );
        let ops: Vec<_> = f
            .door
            .calls()
            .iter()
            .map(|call| call.request.op())
            .collect();
        assert!(
            ops.iter().position(|op| *op == "cut.abort").unwrap()
                < ops.iter().position(|op| *op == "cut.release").unwrap()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn activation_release_survives_crash_and_stale_checkpoints() {
        let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
        let stale = f.row();
        f.reopen();
        f.tick().await;
        f.store().checkpoint_agent_move(&stale, false).unwrap();
        assert_eq!(f.state()["state"], "released");
        f.tick().await;
        assert_eq!(f.calls(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn release_stop_codes_are_terminal() {
        for code in ["invalid_request", "caller_forbidden", "cut_release_unknown"] {
            let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
            f.door
                .script("cut.release", DoorEffect::Refuse(code.into()));
            f.tick().await;
            assert_eq!(f.state(), json!({"state":"stopped","code":code}));
            assert_eq!(
                f.handler
                    .health
                    .cut_releases_stopped
                    .load(Ordering::Relaxed),
                1
            );
            f.reopen();
            advance(24 * 60 * 60 * 1000).await;
            f.tick().await;
            assert_eq!(f.calls(), 1, "{code}");
        }
    }

    #[tokio::test(start_paused = true)]
    async fn release_retry_codes_back_off_durably_to_five_minutes() {
        for code in ["unavailable", "lease_not_active", "future_unlisted_code"] {
            let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
            for attempt in 1..=11 {
                f.door
                    .script("cut.release", DoorEffect::Refuse(code.into()));
                f.tick().await;
                let delay = (1000i64 << (attempt - 1)).min(300_000);
                assert_eq!(f.state()["state"], "pending");
                assert_eq!(f.state()["retry_at_ms"], clock() + delay);
                if attempt == 1 {
                    f.reopen();
                }
                advance(delay - 1).await;
                f.tick().await;
                assert_eq!(f.calls(), attempt as usize, "early retry {code}");
                advance(1).await;
            }
            f.tick().await;
            assert_eq!(f.state()["state"], "released");
            assert_eq!(f.calls(), 12);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn release_unsupported_plane_waits_six_hours_across_restart() {
        let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
        f.door.script(
            "cut.release",
            DoorEffect::Refuse("plane_version_unsupported".into()),
        );
        f.tick().await;
        assert_eq!(f.state()["retry_at_ms"], clock() + 21_600_000);
        f.reopen();
        advance(21_599_999).await;
        f.tick().await;
        assert_eq!(f.calls(), 1);
        advance(1).await;
        f.tick().await;
        assert_eq!(f.calls(), 2);
        assert_eq!(f.state()["state"], "released");
    }

    #[tokio::test(start_paused = true)]
    async fn release_lost_reply_resends_identical_request_for_one_release() {
        let f = Fixture::new(MoveSide::Receiver, MoveStep::Activated).await;
        f.door.script(
            "cut.release",
            DoorEffect::LoseReply(TransportError::NoReply),
        );
        f.tick().await;
        assert_eq!(f.state()["state"], "pending");
        assert_eq!(f.door.release_count(), 1);
        advance(1000).await;
        f.tick().await;
        assert_eq!(f.state()["state"], "released");
        assert_eq!(f.door.release_count(), 1);
        let requests: Vec<_> = f
            .door
            .calls()
            .into_iter()
            .filter(|call| call.request.op() == "cut.release")
            .map(|call| call.request)
            .collect();
        assert_eq!(
            requests,
            vec![DoorRequest::Release(CutRequest { cut_id: CUT.into() }); 2]
        );
    }

    struct Gate {
        entered: tokio::sync::Semaphore,
        resume: tokio::sync::Semaphore,
        active: Arc<AtomicUsize>,
    }
    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }
    struct GatedDoor(FakeDoor, Arc<Gate>);
    #[async_trait::async_trait]
    impl LogTransport for GatedDoor {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            if method == "cut.release" {
                self.1.active.fetch_add(1, Ordering::Relaxed);
                let _active = Active(self.1.active.clone());
                self.1.entered.add_permits(1);
                self.1.resume.acquire().await.unwrap().forget();
            }
            self.0.call(method, params).await
        }
    }
    struct GatedConnector(Arc<GatedDoor>);
    #[async_trait::async_trait]
    impl LogConnector for GatedConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    fn gate(f: &mut Fixture) -> Arc<Gate> {
        let gate = Arc::new(Gate {
            entered: tokio::sync::Semaphore::new(0),
            resume: tokio::sync::Semaphore::new(0),
            active: Arc::new(AtomicUsize::new(0)),
        });
        f.handler.door_client = crate::door_client::DoorClient::new(Arc::new(GatedConnector(
            Arc::new(GatedDoor(f.door.clone(), gate.clone())),
        )));
        f.worker.door = f.handler.door_client.clone();
        gate
    }

    #[tokio::test(start_paused = true)]
    async fn held_release_does_not_hold_move_or_next_move() {
        let mut f = Fixture::new(MoveSide::Receiver, MoveStep::Done).await;
        let gate = gate(&mut f);
        let worker = f.worker.clone();
        let task = tokio::spawn(run(worker.clone()));
        drive(gate.entered.acquire()).await.unwrap().forget();
        worker.gauges().await.unwrap();
        assert_eq!(f.handler.health.moves_open.load(Ordering::Relaxed), 0);
        let mut next = f.row();
        next.cut_id = "bbbb0123456789abcdef0123456789ab".into();
        next.plan.as_object_mut().unwrap().remove("_cut_release");
        worker
            .sql(move |store| store.insert_agent_move(&next))
            .await
            .unwrap();
        assert_eq!(
            worker
                .sql(RegistryStore::list_agent_moves)
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(gate.active.load(Ordering::Relaxed), 1);
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        worker.sql_operations.wait().await;
    }

    #[tokio::test(start_paused = true)]
    async fn stop_store_tasks_cancels_releaser_before_reopen() {
        let mut f = Fixture::new(MoveSide::Receiver, MoveStep::Done).await;
        let gate = gate(&mut f);
        f.handler.start_move_driver();
        drive(gate.entered.acquire()).await.unwrap().forget();
        assert_eq!(gate.active.load(Ordering::Relaxed), 1);
        drive(crate::tests::stop_store_tasks(&f.handler)).await;
        assert_eq!(gate.active.load(Ordering::Relaxed), 0);
        f.reopen();
        assert_eq!(f.state()["state"], "pending");
    }
}

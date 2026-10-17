//! Machine-local move tasks. Remote calls never own a store handle or the writer.

use crate::{
    door_client::DoorClient,
    move_receiver::{poll_inbox, CutAccept, Receiver},
    move_source::Source,
    participant_discovery::ParticipantDiscovery,
    record_client::RecordClient,
    HandlerError, HealthGauges, ProjectsHandler,
};
use cortexkit_role_agent_data::{
    errors::{driver_action_for, DriverAction, ErrorBody, RefusalCode},
    Op,
};
use entorhinal_core::{move_store::AgentMove, RegistryError, RegistryStore};
use std::{
    collections::{HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

pub(crate) const POLL: Duration = Duration::from_secs(30);

type Owner = (String, bool);
#[derive(Default)]
struct TaskOwners {
    active: HashSet<Owner>,
    tasks: HashMap<tokio::task::Id, Owner>,
    retry_at: HashMap<Owner, tokio::time::Instant>,
}

impl TaskOwners {
    fn acquire(&mut self, owner: Owner) -> bool {
        if self
            .retry_at
            .get(&owner)
            .is_some_and(|at| tokio::time::Instant::now() < *at)
        {
            return false;
        }
        self.active.insert(owner)
    }

    fn track(&mut self, task: tokio::task::AbortHandle, owner: Owner) {
        self.tasks.insert(task.id(), owner);
    }

    async fn joined(
        &mut self,
        worker: &Worker,
        result: Result<(tokio::task::Id, Owner), tokio::task::JoinError>,
    ) {
        let (id, failure) = match result {
            Ok((id, _)) => (id, None),
            Err(error) => (error.id(), Some(error.to_string())),
        };
        let Some(owner) = self.tasks.remove(&id) else {
            return;
        };
        self.active.remove(&owner);
        if let Some(error) = failure {
            worker
                .health
                .move_task_panics
                .fetch_add(1, Ordering::Relaxed);
            eprintln!("move task {} failed: {error}", owner.0);
            let mut delay = backoff(1);
            if let Ok(mut row) = worker.row(&owner.0).await {
                row.attempts = row.attempts.saturating_add(1);
                delay = backoff(row.attempts);
                row.hold = Some(
                    serde_json::json!({"code":"move_task_failed","error":error,"move_error":true,"cut_accept":owner.1}),
                );
                let _ = worker.save(&mut row, false).await;
            }
            // A panic must release ownership, but an immediate respawn could spin
            // on the same corrupted row. The deadline also applies to wakeups.
            self.retry_at
                .insert(owner, tokio::time::Instant::now() + delay);
        } else {
            self.retry_at.remove(&owner);
        }
    }
}

pub(crate) fn participant_action(op: &str, code: &str) -> Option<DriverAction> {
    Op::ALL
        .into_iter()
        .find(|candidate| candidate.method() == op)
        .map(|op| driver_action_for(op, &RefusalCode::from_code(code)))
}

pub(crate) fn digest_reason(code: &str, detail: Option<&serde_json::Value>) -> Option<String> {
    let mut body = ErrorBody::new(RefusalCode::from_code(code), "");
    body.detail = detail.cloned();
    body.digest_mismatch_detail().ok().flatten()?.reason
}

/// What a move driver keeps from a refusal the door or a participant sent,
/// beyond its code and detail. Drivers hold it boxed inside their failure
/// value so that `Result<_, Failure>` stays small.
pub(crate) struct PeerRefusal {
    /// The refusal's human-readable reason from the wire error body.
    pub(crate) message: Option<String>,
}

/// Writes one fleet-log line the first time a move's driver sees a given
/// refusal, so an operator can read why engram or a participant said no.
/// Retries of the same refusal repeat every backoff interval and would
/// otherwise flood the log, so a (peer, op, code) triple is logged once per
/// driver instance; a module restart logs it once more. The line carries only
/// identifiers and the peer's own message, never a request body or session
/// content.
#[derive(Default)]
pub(crate) struct RefusalLog(HashSet<(String, String, String)>);

impl RefusalLog {
    pub(crate) fn note(
        &mut self,
        cut: &str,
        peer: &str,
        op: &str,
        code: &str,
        message: Option<&str>,
    ) {
        if self.0.insert((peer.into(), op.into(), code.into())) {
            tracing::warn!(
                target: "move",
                cut,
                peer,
                op,
                code,
                message = message.unwrap_or(""),
                "move step refused"
            );
        }
    }
}

#[cfg(test)]
pub(crate) fn digest_refusal(op: &str) -> crate::participant_client::Refusal {
    let vectors: serde_json::Value = serde_json::from_str(
        cortexkit_role_agent_data::vectors::file("digest-mismatch.json").unwrap(),
    )
    .unwrap();
    let vector = vectors["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["op"] == op)
        .unwrap();
    let body: ErrorBody = serde_json::from_value(vector["body"].clone()).unwrap();
    crate::participant_client::Refusal {
        code: body.code.as_str().into(),
        detail: body.detail,
        message: Some(body.message),
    }
}

#[derive(Default)]
pub(crate) struct SqlOperations {
    count: AtomicUsize,
    idle: tokio::sync::Notify,
    #[cfg(test)]
    waiting: tokio::sync::Notify,
}

impl SqlOperations {
    pub(crate) fn begin(self: &Arc<Self>) -> SqlGuard {
        self.count.fetch_add(1, Ordering::AcqRel);
        SqlGuard(self.clone())
    }

    pub(crate) async fn wait(&self) {
        #[cfg(test)]
        self.waiting.notify_one();
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

pub(crate) struct SqlGuard(Arc<SqlOperations>);

impl Drop for SqlGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

pub(crate) struct MoveDriver {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    sql: Arc<SqlOperations>,
}

pub(crate) struct ReconnectConnector {
    pub(crate) inner: Arc<dyn crate::log_client::LogConnector>,
    pub(crate) wake: Arc<MoveWake>,
}

/// Wakes move workers. Local wake-ups, sent after an operator action or after a
/// new move is admitted, use Notify's wake-one or wake-current-waiters behavior.
/// A transport replacement instead reaches every move worker independently,
/// including a worker in the middle of a poll: it must poll again after using
/// an old connection.
pub(crate) struct MoveWake {
    local: tokio::sync::Notify,
    reconnect: tokio::sync::watch::Sender<u64>,
    transport: std::sync::Mutex<Option<Arc<dyn crate::log_client::LogTransport>>>,
    #[cfg(test)]
    passes: tokio::sync::watch::Sender<usize>,
}

impl Default for MoveWake {
    fn default() -> Self {
        Self {
            local: tokio::sync::Notify::new(),
            reconnect: tokio::sync::watch::channel(0).0,
            transport: std::sync::Mutex::new(None),
            #[cfg(test)]
            passes: tokio::sync::watch::channel(0).0,
        }
    }
}

impl MoveWake {
    pub(crate) fn notify_one(&self) {
        self.local.notify_one();
    }

    pub(crate) fn notify_waiters(&self) {
        self.local.notify_waiters();
    }

    fn connected(&self, transport: Arc<dyn crate::log_client::LogTransport>) {
        let mut last = self.transport.lock().unwrap();
        // RecordClient calls connect() for every request, even when the connector
        // returns its cached transport. Only a replacement is a reconnect;
        // the first acquisition belongs to startup.
        // Keep a strong clone so an allocation cannot be recycled at the same
        // address and mistaken for the previous transport. Failed connects never
        // reach connected() and therefore leave the previous transport intact.
        let replaced = last
            .as_ref()
            .is_some_and(|previous| !Arc::ptr_eq(previous, &transport));
        *last = Some(transport);
        if replaced {
            self.reconnect.send_modify(|generation| {
                *generation = generation.wrapping_add(1);
            });
        }
    }

    pub(crate) fn subscribe(self: &Arc<Self>) -> MoveSignals {
        MoveSignals {
            wake: self.clone(),
            reconnect: self.reconnect.subscribe(),
        }
    }
}

pub(crate) struct MoveSignals {
    wake: Arc<MoveWake>,
    reconnect: tokio::sync::watch::Receiver<u64>,
}

impl MoveSignals {
    pub(crate) async fn notified(&mut self) {
        tokio::select! {
            _ = self.wake.local.notified() => {},
            _ = self.reconnect.changed() => {},
        }
    }

    pub(crate) fn handled(&mut self) {
        // Mark the latest reconnect generation before polling, even if a timer
        // or local notification selected the poll. A replacement during the poll
        // then leaves a newer, unseen generation for the next notified() call.
        self.reconnect.borrow_and_update();
    }

    #[cfg(test)]
    pub(crate) fn finished(&self) {
        self.wake.passes.send_modify(|passes| *passes += 1);
    }
}

#[async_trait::async_trait]
impl crate::log_client::LogConnector for ReconnectConnector {
    async fn connect(
        &self,
    ) -> Result<Arc<dyn crate::log_client::LogTransport>, crate::log_client::TransportError> {
        let transport = self.inner.connect().await?;
        self.wake.connected(transport.clone());
        Ok(transport)
    }
}

#[derive(Clone)]
pub(crate) struct Worker {
    pub(crate) store: Weak<Mutex<Option<RegistryStore>>>,
    pub(crate) sql_operations: Arc<SqlOperations>,
    pub(crate) records: RecordClient,
    pub(crate) door: DoorClient,
    pub(crate) discovery: Arc<dyn ParticipantDiscovery>,
    pub(crate) catalog: Option<Arc<crate::catalog_move::CatalogMove>>,
    pub(crate) health: Arc<HealthGauges>,
    #[cfg(test)]
    pub(crate) writer: Arc<tokio::sync::Mutex<()>>,
    pub(crate) clock: fn() -> i64,
    pub(crate) wake: Arc<MoveWake>,
}

impl Worker {
    pub(crate) async fn sql<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&RegistryStore) -> Result<T, RegistryError> + Send + 'static,
    ) -> Result<T, HandlerError> {
        let slot = self.store.clone();
        // Count queued work too: aborting its async caller cannot stop a blocking closure.
        let guard = self.sql_operations.begin();
        tokio::task::spawn_blocking(move || {
            // The store is dropped before this guard, including when the operation panics.
            let _guard = guard;
            let store = slot
                .upgrade()
                .and_then(|slot| slot.lock().unwrap().clone())
                .ok_or_else(|| HandlerError::new("storage_unavailable", "move store is gone"))?;
            operation(&store).map_err(HandlerError::from)
        })
        .await
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?
    }

    pub(crate) async fn row(&self, cut: &str) -> Result<AgentMove, HandlerError> {
        let cut = cut.to_owned();
        self.sql(move |s| s.agent_move(&cut))
            .await?
            .ok_or_else(|| HandlerError::new("invalid_state", "move row is gone"))
    }

    pub(crate) async fn consume_abort_retry(&self, cut: &str) -> Result<bool, HandlerError> {
        let cut = cut.to_owned();
        self.sql(move |store| store.consume_move_abort_retry(&cut))
            .await
    }

    /// Serialize checkpoints with local operator edits in the fenced writer,
    /// without waiting for journal catch-up's shared writer lock.
    pub(crate) async fn save(
        &self,
        row: &mut AgentMove,
        clear_abort: bool,
    ) -> Result<(), HandlerError> {
        let mut next = row.clone();
        next.updated_at_ms = (self.clock)();
        if clear_abort {
            next.abort_requested = None;
        }
        let saved = self
            .sql(move |s| s.checkpoint_agent_move(&next, clear_abort))
            .await?;
        *row = saved;
        self.gauges().await
    }

    pub(crate) async fn gauges(&self) -> Result<(), HandlerError> {
        let rows = self.sql(RegistryStore::list_agent_moves).await?;
        self.health.moves_open.store(
            rows.iter().filter(|r| !r.step.is_terminal()).count() as u64,
            Ordering::Relaxed,
        );
        let errors = rows
            .iter()
            .map(|r| {
                let diagnostics = ["_source", "_receiver"]
                    .iter()
                    .map(|key| {
                        r.plan
                            .get(key)
                            .and_then(|s| s.get("errors"))
                            .and_then(serde_json::Value::as_array)
                            .map_or(0, Vec::len)
                    })
                    .sum::<usize>();
                diagnostics
                    + usize::from(
                        r.cut_accept
                            .as_ref()
                            .is_some_and(|v| v["state"] != "accepted"),
                    )
                    + usize::from(r.hold.as_ref().is_some_and(|v| v["move_error"] == true))
            })
            .sum::<usize>();
        self.health
            .move_errors
            .store(errors as u64, Ordering::Relaxed);
        Ok(())
    }
}

impl ProjectsHandler {
    pub(crate) fn start_move_driver(&self) {
        let mut task = self.move_driver_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let sql = Arc::new(SqlOperations::default());
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            sql_operations: sql.clone(),
            records: self.record_client.clone(),
            door: self.door_client.clone(),
            discovery: self.participant_discovery.clone(),
            catalog: Some(Arc::new(self.catalog_move_context())),
            health: self.health.clone(),
            #[cfg(test)]
            writer: self.writer.clone(),
            clock: self.clock,
            wake: self.move_wake.clone(),
        };
        let liveness = self.liveness.clone();
        let mut signals = self.move_wake.subscribe();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut owners = TaskOwners::default();
            let mut tasks = tokio::task::JoinSet::new();
            let poll = async {
                loop {
                    tokio::select! {
                        _ = interval.tick() => {},
                        _ = signals.notified() => {},
                        Some(result) = tasks.join_next_with_id(), if !tasks.is_empty() => {
                            owners.joined(&worker, result).await;
                        }
                    }
                    signals.handled();
                    if worker.store.upgrade().is_none() {
                        break;
                    }
                    let _ = poll_inbox(&worker).await;
                    let Ok(rows) = worker.sql(RegistryStore::list_open_agent_moves).await else {
                        continue;
                    };
                    let _ = worker.gauges().await;
                    for row in rows {
                        let owner = (row.cut_id.clone(), false);
                        if owners.acquire(owner.clone()) {
                            let worker = worker.clone();
                            let liveness = liveness.clone();
                            let task = tasks.spawn(async move {
                                let cut = row.cut_id;
                                match row.side {
                                    entorhinal_core::move_store::MoveSide::Source => {
                                        Source::new(worker, cut.clone()).run().await
                                    }
                                    entorhinal_core::move_store::MoveSide::Receiver => {
                                        Receiver::new(worker, cut.clone(), liveness).run().await
                                    }
                                }
                                (cut, false)
                            });
                            owners.track(task, owner);
                        }
                    }
                    if let Ok(rows) = worker.sql(RegistryStore::list_pending_cut_accepts).await {
                        for row in rows {
                            let owner = (row.cut_id.clone(), true);
                            if owners.acquire(owner.clone()) {
                                let worker = worker.clone();
                                let task = tasks.spawn(async move {
                                    let cut = row.cut_id;
                                    CutAccept::new(worker, cut.clone()).run().await;
                                    (cut, true)
                                });
                                owners.track(task, owner);
                            }
                        }
                    }
                    #[cfg(test)]
                    signals.finished();
                }
            };
            tokio::select! {
                _ = stopped => {},
                _ = poll => {},
            }
            tasks.shutdown().await;
        });
        *task = Some(MoveDriver {
            stop,
            task: driver,
            sql,
        });
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn stop_move_driver(&self) {
        let driver = self.move_driver_task.lock().unwrap().take();
        if let Some(driver) = driver {
            let _ = driver.stop.send(());
            let _ = driver.task.await;
            // Draining async tasks does not release stores held by spawn_blocking work.
            driver.sql.wait().await;
        }
    }
}

pub(crate) fn backoff(attempts: i64) -> Duration {
    Duration::from_secs(
        1u64.checked_shl(attempts.saturating_sub(1).clamp(0, 9) as u32)
            .unwrap_or(300)
            .min(300),
    )
}

#[cfg(test)]
pub(crate) mod reconnect_test {
    use super::*;
    use crate::log_client::{LogConnector, LogTransport, TransportError};
    use std::sync::{atomic::AtomicBool, Mutex};

    struct Probe {
        inner: Arc<dyn LogTransport>,
        subject: String,
        reads: tokio::sync::watch::Sender<usize>,
        blocked: AtomicBool,
        release: tokio::sync::Semaphore,
    }

    struct Transport(Arc<Probe>);

    #[async_trait::async_trait]
    impl LogTransport for Transport {
        async fn call(
            &self,
            method: &str,
            params: serde_json::Value,
        ) -> Result<Vec<u8>, TransportError> {
            if method == "record.read" && params["subject"] == self.0.subject {
                self.0.reads.send_modify(|reads| *reads += 1);
                if self.0.blocked.swap(false, Ordering::Relaxed) {
                    self.0.release.acquire().await.unwrap().forget();
                }
            }
            self.0.inner.call(method, params).await
        }
    }

    pub(crate) struct Connector {
        probe: Arc<Probe>,
        current: Mutex<Arc<dyn LogTransport>>,
        pub(crate) fail: AtomicBool,
    }

    impl Connector {
        pub(crate) fn new(
            inner: Arc<dyn LogTransport>,
            subject: String,
            blocked: bool,
        ) -> Arc<Self> {
            let probe = Arc::new(Probe {
                inner,
                subject,
                reads: tokio::sync::watch::channel(0).0,
                blocked: AtomicBool::new(blocked),
                release: tokio::sync::Semaphore::new(0),
            });
            Arc::new(Self {
                current: Mutex::new(Arc::new(Transport(probe.clone()))),
                probe,
                fail: AtomicBool::new(false),
            })
        }

        pub(crate) fn replace(&self) {
            *self.current.lock().unwrap() = Arc::new(Transport(self.probe.clone()));
        }

        pub(crate) fn release(&self) {
            self.probe.release.add_permits(1);
        }

        pub(crate) async fn wait_for_reads(&self, expected: usize) {
            let mut reads = self.probe.reads.subscribe();
            tokio::time::timeout(Duration::from_secs(5), reads.wait_for(|n| *n >= expected))
                .await
                .unwrap_or_else(|_| panic!("timed out waiting for read {expected}"))
                .unwrap();
            assert_eq!(*reads.borrow(), expected, "unexpected extra record read");
        }

        pub(crate) async fn quiet_for(&self, expected: usize, duration: Duration) {
            let mut reads = self.probe.reads.subscribe();
            assert!(
                tokio::time::timeout(duration, reads.wait_for(|n| *n > expected))
                    .await
                    .is_err(),
                "unexpected record read before the poll interval"
            );
            assert_eq!(*reads.borrow(), expected);
        }

        pub(crate) async fn check_polling(&self, connector: &ReconnectConnector, mid_poll: bool) {
            self.wait_for_reads(1).await;
            let mut passes = connector.wake.passes.subscribe();
            if !mid_poll {
                tokio::time::timeout(Duration::from_secs(5), passes.wait_for(|n| *n >= 1))
                    .await
                    .expect("startup pass did not finish")
                    .unwrap();
            }
            let start = tokio::time::Instant::now();
            // Reusing the same transport is not a reconnect, even after a failed
            // connection attempt; only replacing that transport warrants a poll.
            connector.connect().await.unwrap();
            self.fail.store(true, Ordering::Relaxed);
            assert!(connector.connect().await.is_err());
            self.fail.store(false, Ordering::Relaxed);
            connector.connect().await.unwrap();
            self.replace();
            connector.connect().await.unwrap();
            connector.connect().await.unwrap();
            if mid_poll {
                self.release();
            }
            self.wait_for_reads(2).await;
            tokio::time::timeout(Duration::from_secs(5), passes.wait_for(|n| *n >= 2))
                .await
                .expect("reconnect pass did not finish")
                .unwrap();
            self.quiet_for(2, Duration::from_secs(29)).await;
            assert_eq!(start.elapsed(), Duration::from_secs(29));
            self.wait_for_reads(3).await;
            assert_eq!(start.elapsed(), POLL);
        }
    }

    #[async_trait::async_trait]
    impl LogConnector for Connector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            if self.fail.load(Ordering::Relaxed) {
                Err(TransportError::Unavailable)
            } else {
                Ok(self.current.lock().unwrap().clone())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    #[tokio::test(start_paused = true)]
    async fn reconnect_generations_broadcast_and_coalesce_independently() {
        let wake = Arc::new(MoveWake::default());
        let transport: Arc<dyn crate::log_client::LogTransport> =
            crate::fake_record::FakeRecord::new([1; 16]);
        let retained = Arc::downgrade(&transport);
        let mut waiting = wake.subscribe();
        let mut busy = wake.subscribe();
        wake.connected(transport.clone());
        drop(transport);
        assert!(
            retained.upgrade().is_some(),
            "last transport must stay alive"
        );
        wake.connected(retained.upgrade().unwrap());
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting.notified())
                .await
                .is_err()
        );
        wake.connected(crate::fake_record::FakeRecord::new([2; 16]));
        assert!(retained.upgrade().is_none());
        tokio::time::timeout(Duration::from_secs(1), waiting.notified())
            .await
            .unwrap();
        waiting.handled();
        // The busy subscriber has not handled either replacement. Both updates
        // collapse into one follow-up without consuming the waiting subscriber's wake.
        wake.connected(crate::fake_record::FakeRecord::new([3; 16]));
        tokio::time::timeout(Duration::from_secs(1), busy.notified())
            .await
            .unwrap();
        busy.handled();
        tokio::time::timeout(Duration::from_secs(1), waiting.notified())
            .await
            .unwrap();
        waiting.handled();
        assert!(
            tokio::time::timeout(Duration::from_secs(1), busy.notified())
                .await
                .is_err()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(1), waiting.notified())
                .await
                .is_err()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn panicked_cut_releases_owner_records_hold_and_redrives_after_backoff() {
        let (dir, descriptor) = crate::tests::scratch_descriptor("move-driver-panic");
        let store = RegistryStore::open(&descriptor).unwrap();
        let row = |cut: &str| AgentMove {
            cut_id: cut.into(),
            agent_id: format!("agent-{cut}"),
            side: entorhinal_core::move_store::MoveSide::Receiver,
            peer: "peer".into(),
            step: entorhinal_core::move_store::MoveStep::PickedUp,
            plan: serde_json::json!({}),
            request_body: None,
            record_version: 1,
            claim_version: None,
            claim_take_body: None,
            claim_take_version: None,
            claim_landed_version: None,
            seal_sent: false,
            abort_requested: None,
            hold: None,
            cut_accept: None,
            reason: None,
            detail: None,
            request_key: None,
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 0,
            updated_at_ms: 0,
        };
        store.insert_agent_move(&row("bad")).unwrap();
        store.insert_agent_move(&row("other")).unwrap();
        let slot = Arc::new(Mutex::new(Some(store)));
        let handler = ProjectsHandler::with_runtime("panic-test".into(), || 0);
        let worker = Worker {
            store: Arc::downgrade(&slot),
            sql_operations: Arc::default(),
            records: handler.record_client.clone(),
            door: handler.door_client.clone(),
            discovery: handler.participant_discovery.clone(),
            catalog: None,
            health: handler.health.clone(),
            writer: handler.writer.clone(),
            clock: handler.clock,
            wake: handler.move_wake.clone(),
        };
        let bad = ("bad".to_owned(), false);
        let other = ("other".to_owned(), false);
        let mut owners = TaskOwners::default();
        let mut tasks = tokio::task::JoinSet::new();
        assert!(owners.acquire(bad.clone()));
        owners.track(
            tasks.spawn(async {
                panic!("scripted move task panic");
                #[allow(unreachable_code)]
                ("bad".into(), false)
            }),
            bad.clone(),
        );
        let (release, gate) = tokio::sync::oneshot::channel();
        assert!(owners.acquire(other.clone()));
        owners.track(
            tasks.spawn(async {
                gate.await.unwrap();
                ("other".into(), false)
            }),
            other.clone(),
        );
        owners
            .joined(&worker, tasks.join_next_with_id().await.unwrap())
            .await;
        let held = worker.row("bad").await.unwrap();
        assert_eq!(held.hold.unwrap()["code"], "move_task_failed");
        assert_eq!(worker.health.move_task_panics.load(Ordering::Relaxed), 1);
        assert_eq!(worker.health.move_errors.load(Ordering::Relaxed), 1);
        assert!(
            !owners.acquire(bad.clone()),
            "panic retries must honor the deadline"
        );
        assert!(
            !owners.acquire(other.clone()),
            "the other cut still owns its task"
        );
        release.send(()).unwrap();
        owners
            .joined(&worker, tasks.join_next_with_id().await.unwrap())
            .await;
        assert!(worker.row("other").await.unwrap().hold.is_none());
        assert!(
            owners.acquire(other),
            "a healthy completion releases its slot"
        );
        tokio::time::advance(backoff(held.attempts)).await;
        assert!(
            owners.acquire(bad.clone()),
            "panicked cut must be eligible for re-drive"
        );
        let redrive = worker.clone();
        owners.track(
            tasks.spawn(async move {
                let mut row = redrive.row("bad").await.unwrap();
                row.step = entorhinal_core::move_store::MoveStep::Done;
                row.hold = None;
                redrive.save(&mut row, false).await.unwrap();
                ("bad".into(), false)
            }),
            bad,
        );
        owners
            .joined(&worker, tasks.join_next_with_id().await.unwrap())
            .await;
        assert_eq!(
            worker.row("bad").await.unwrap().step,
            entorhinal_core::move_store::MoveStep::Done
        );
        assert_eq!(worker.health.move_errors.load(Ordering::Relaxed), 0);
        drop(slot);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn stop_move_driver_waits_for_blocking_sql_before_store_reopen() {
        let (dir, descriptor) = crate::tests::scratch_descriptor("move-driver-stop");
        let mut handler = ProjectsHandler::with_runtime("sql-drain".into(), || 0);
        handler.record_client = RecordClient::new(Arc::new(
            crate::fake_record::FakeRecordConnector(crate::fake_record::FakeRecord::new([1; 16])),
        ));
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        handler.start_move_driver();
        let sql = handler
            .move_driver_task
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .sql
            .clone();
        let worker = Worker {
            store: Arc::downgrade(&handler.store),
            sql_operations: sql.clone(),
            records: handler.record_client.clone(),
            door: handler.door_client.clone(),
            discovery: handler.participant_discovery.clone(),
            catalog: None,
            health: handler.health.clone(),
            writer: handler.writer.clone(),
            clock: handler.clock,
            wake: handler.move_wake.clone(),
        };
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let blocked_worker = worker.clone();
        let operation = tokio::spawn(async move {
            blocked_worker
                .sql(move |store| {
                    entered.send(()).unwrap();
                    gate.recv().unwrap();
                    store.list_open_agent_moves()
                })
                .await
        });
        running.await.unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());

        let mut stop = Box::pin(handler.stop_move_driver());
        let stopped_early = tokio::select! {
            _ = &mut stop => true,
            _ = sql.waiting.notified() => {
                // Poll after the driver and its move tasks have drained, not merely after stop begins.
                std::future::poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx).is_ready())).await
            }
        };
        // Release the blocking thread even if shutdown returned early, so a red test cannot hang.
        release.send(()).unwrap();
        if stopped_early {
            sql.wait().await;
        } else {
            stop.await;
        }
        assert!(
            !stopped_early,
            "stop_move_driver returned while blocking SQL still held the writer lease"
        );

        assert_eq!(
            worker
                .sql::<()>(|_| panic!("SQL panic exercises the drop guard"))
                .await
                .unwrap_err()
                .code,
            "storage_error"
        );
        assert_eq!(sql.count.load(Ordering::Acquire), 0);

        *handler.store.lock().unwrap() = None;
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        handler.start_move_driver();
        handler.stop_move_driver().await;
        handler.stop_move_driver().await;
        *handler.store.lock().unwrap() = None;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

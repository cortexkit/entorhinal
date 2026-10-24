use super::*;
use crate::cut_release;
use crate::door_client::{CutRequest, DoorClient};

fn calls(node: &MoveMachine) -> Vec<String> {
    node.door
        .calls()
        .into_iter()
        .filter_map(|call| matches!(call.request, DoorRequest::Release(_)).then_some(call.author))
        .collect()
}

fn state(node: &MoveMachine, cut: &str) -> Value {
    node.row(cut).plan["_cut_release"].clone()
}

async fn tick(node: &MoveMachine) {
    drive(cut_release::tick(&node.worker())).await.unwrap();
}

async fn activated(f: &Pair, key: &str) -> String {
    let cut = f.published(key).await;
    f.b.pickup().await;
    f.b.receiver_until(&cut, MoveStep::Activated).await;
    cut
}

async fn cleanup(f: Pair) {
    for node in [f.a, f.b] {
        crate::tests::stop_store_tasks(&node.machine.handler).await;
        node.machine.handler.store.lock().unwrap().take();
    }
}

async fn advance(ms: i64) {
    elapse(ms);
    tokio::time::advance(Duration::from_millis(ms as u64)).await;
}

#[tokio::test(start_paused = true)]
async fn published_receiver_releases_once_and_status_is_optional() {
    let f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = f.published("release-published").await;
    let before = f.a.store().move_status_view(&cut).unwrap().unwrap();
    assert!(before.get("cut_release").is_none());
    f.b.pickup().await;
    f.b.receiver_until(&cut, MoveStep::Activated).await;
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    let generations = [
        f.a.store().generation().unwrap(),
        f.b.store().generation().unwrap(),
    ];
    f.b.door.script("cut.release", DoorEffect::Release(false));
    tick(&f.a).await;
    tick(&f.b).await;
    tick(&f.b).await;
    assert_eq!(calls(&f.b), [B]);
    assert_eq!(
        state(&f.b, &cut),
        json!({"state":"released","outcome":"published","reservationDropped":false})
    );
    let public = f.b.store().move_status_view(&cut).unwrap().unwrap();
    assert_eq!(public["cut_release"], state(&f.b, &cut));
    assert_eq!(
        f.b.store().moves_view().unwrap()[0]["cut_release"],
        public["cut_release"]
    );
    assert_eq!(f.a.store().move_status_view(&cut).unwrap().unwrap(), before);
    assert_eq!(
        [
            f.a.store().generation().unwrap(),
            f.b.store().generation().unwrap()
        ],
        generations
    );
    assert_eq!(
        f.b.machine
            .handler
            .health
            .cut_releases_pending
            .load(Ordering::Relaxed),
        0
    );
    assert_eq!(
        f.b.machine
            .handler
            .health
            .cut_releases_released
            .load(Ordering::Relaxed),
        1
    );
    cleanup(f).await;
}

#[tokio::test(start_paused = true)]
async fn aborted_cut_releases_only_on_aborting_machine() {
    let f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = f.published("release-aborted").await;
    f.a.abort(&cut);
    f.a.source_until(&cut, MoveStep::Aborted).await;
    assert!(f.a.door.cut(&cut).unwrap().aborted);
    assert_eq!(state(&f.a, &cut)["state"], "pending");
    tick(&f.b).await;
    tick(&f.a).await;
    tick(&f.a).await;
    assert_eq!(calls(&f.a), [A]);
    assert_eq!(
        state(&f.a, &cut),
        json!({"state":"released","outcome":"aborted","reservationDropped":true})
    );
    let ops: Vec<_> =
        f.a.door
            .calls()
            .iter()
            .map(|call| call.request.op())
            .collect();
    assert!(
        ops.iter().position(|op| *op == "cut.abort").unwrap()
            < ops.iter().position(|op| *op == "cut.release").unwrap()
    );
    cleanup(f).await;
}

#[tokio::test(start_paused = true)]
async fn activation_release_survives_crash_and_stale_checkpoints() {
    let mut f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = activated(&f, "release-crash").await;
    assert!(calls(&f.b).is_empty());
    crate::tests::stop_store_tasks(&f.b.machine.handler).await;
    f.b.restart().await;
    let stale = f.b.row(&cut);
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    tick(&f.b).await;
    f.b.store().checkpoint_agent_move(&stale, false).unwrap();
    assert_eq!(state(&f.b, &cut)["state"], "released");
    f.a.source_until(&cut, MoveStep::Released).await;
    f.b.receiver_until(&cut, MoveStep::Done).await;
    tick(&f.b).await;
    assert_eq!(calls(&f.b), [B]);
    assert_eq!(state(&f.b, &cut)["state"], "released");
    cleanup(f).await;
}

#[tokio::test(start_paused = true)]
async fn release_stop_codes_are_terminal() {
    for code in ["invalid_request", "caller_forbidden", "cut_release_unknown"] {
        let mut f = Pair::new(&[("pi", "pi-session")]).await;
        let cut = activated(&f, "release-stop").await;
        f.b.door
            .script("cut.release", DoorEffect::Refuse(code.into()));
        tick(&f.b).await;
        assert_eq!(state(&f.b, &cut), json!({"state":"stopped","code":code}));
        assert_eq!(
            f.b.machine
                .handler
                .health
                .cut_releases_stopped
                .load(Ordering::Relaxed),
            1
        );
        crate::tests::stop_store_tasks(&f.b.machine.handler).await;
        f.b.restart().await;
        advance(24 * 60 * 60 * 1000).await;
        tick(&f.b).await;
        assert_eq!(calls(&f.b), [B], "{code}");
        cleanup(f).await;
    }
}

#[tokio::test(start_paused = true)]
async fn release_retry_codes_back_off_durably_to_five_minutes() {
    for code in ["unavailable", "lease_not_active", "future_unlisted_code"] {
        let mut f = Pair::new(&[("pi", "pi-session")]).await;
        let cut = activated(&f, "release-retry").await;
        for attempt in 1..=11 {
            f.b.door
                .script("cut.release", DoorEffect::Refuse(code.into()));
            tick(&f.b).await;
            let delay = (1000i64 << (attempt - 1)).min(300_000);
            assert_eq!(state(&f.b, &cut)["state"], "pending");
            assert_eq!(state(&f.b, &cut)["retry_at_ms"], clock() + delay);
            if attempt == 1 {
                crate::tests::stop_store_tasks(&f.b.machine.handler).await;
                f.b.restart().await;
            }
            advance(delay - 1).await;
            tick(&f.b).await;
            assert_eq!(calls(&f.b).len(), attempt as usize, "early retry {code}");
            advance(1).await;
        }
        tick(&f.b).await;
        assert_eq!(state(&f.b, &cut)["state"], "released");
        assert_eq!(calls(&f.b).len(), 12);
        cleanup(f).await;
    }
}

#[tokio::test(start_paused = true)]
async fn release_unsupported_plane_waits_six_hours_across_restart() {
    let mut f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = activated(&f, "release-plane").await;
    f.b.door.script(
        "cut.release",
        DoorEffect::Refuse("plane_version_unsupported".into()),
    );
    tick(&f.b).await;
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    assert_eq!(state(&f.b, &cut)["retry_at_ms"], clock() + 21_600_000);
    crate::tests::stop_store_tasks(&f.b.machine.handler).await;
    f.b.restart().await;
    advance(21_599_999).await;
    tick(&f.b).await;
    assert_eq!(calls(&f.b), [B]);
    advance(1).await;
    tick(&f.b).await;
    assert_eq!(calls(&f.b), [B, B]);
    assert_eq!(state(&f.b, &cut)["state"], "released");
    cleanup(f).await;
}

#[tokio::test(start_paused = true)]
async fn release_lost_reply_resends_identical_request_for_one_release() {
    let f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = activated(&f, "release-lost").await;
    f.b.door.script(
        "cut.release",
        DoorEffect::LoseReply(TransportError::NoReply),
    );
    tick(&f.b).await;
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    assert_eq!(f.b.door.release_count(), 1);
    advance(1000).await;
    tick(&f.b).await;
    assert_eq!(state(&f.b, &cut)["state"], "released");
    assert_eq!(f.b.door.release_count(), 1);
    let requests: Vec<_> =
        f.b.door
            .calls()
            .into_iter()
            .filter(|call| call.request.op() == "cut.release")
            .map(|call| call.request)
            .collect();
    assert_eq!(
        requests,
        vec![
            DoorRequest::Release(CutRequest {
                cut_id: cut.clone()
            });
            2
        ]
    );
    cleanup(f).await;
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

#[async_trait]
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

#[async_trait]
impl LogConnector for GatedConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(self.0.clone())
    }
}

fn gate(node: &mut MoveMachine) -> Arc<Gate> {
    let gate = Arc::new(Gate {
        entered: tokio::sync::Semaphore::new(0),
        resume: tokio::sync::Semaphore::new(0),
        active: Arc::new(AtomicUsize::new(0)),
    });
    node.machine.handler.door_client = DoorClient::new(Arc::new(GatedConnector(Arc::new(
        GatedDoor(node.door.clone(), gate.clone()),
    ))));
    gate
}

#[tokio::test(start_paused = true)]
async fn held_release_does_not_hold_move_or_next_move() {
    let mut f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = activated(&f, "release-held").await;
    let gate = gate(&mut f.b);
    let worker = f.b.worker();
    let task = tokio::spawn(cut_release::run(worker.clone()));
    drive(gate.entered.acquire()).await.unwrap().forget();
    drive(CutAccept::new(f.b.worker(), cut.clone()).tick())
        .await
        .unwrap();
    f.a.source_until(&cut, MoveStep::Released).await;
    f.b.receiver_until(&cut, MoveStep::Done).await;
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    assert_eq!(
        f.b.machine
            .handler
            .health
            .moves_open
            .load(Ordering::Relaxed),
        0
    );
    f.b.door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    let next = f.b.admit_to(A, "release-held-next").await.unwrap();
    f.b.source_until(&next, MoveStep::Published).await;
    assert_ne!(cut, next);
    assert_eq!(gate.active.load(Ordering::Relaxed), 1);
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    worker.sql_operations.wait().await;
    cleanup(f).await;
}

#[tokio::test(start_paused = true)]
async fn stop_store_tasks_cancels_releaser_before_reopen() {
    let mut f = Pair::new(&[("pi", "pi-session")]).await;
    let cut = f.published("release-lifecycle").await;
    f.finish(&cut).await;
    let gate = gate(&mut f.b);
    f.b.machine.handler.start_move_driver();
    drive(gate.entered.acquire()).await.unwrap().forget();
    assert_eq!(gate.active.load(Ordering::Relaxed), 1);
    drive(crate::tests::stop_store_tasks(&f.b.machine.handler)).await;
    assert_eq!(gate.active.load(Ordering::Relaxed), 0);
    f.b.restart().await;
    assert_eq!(state(&f.b, &cut)["state"], "pending");
    cleanup(f).await;
}

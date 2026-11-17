use super::*;
use serde_json::json;
use tokio::sync::mpsc;

pub(crate) fn put(key: &str, harness: &str, session: &str, state: &str) -> Update {
    Update::Put(
        key.into(),
        serde_json::to_vec(&json!({
            "harness":harness,"sessionId":session,"state":state,
            "lastActivityMs":18,"projectId":"project_sample","futureField":true
        }))
        .unwrap(),
    )
}

pub(crate) fn marker() -> Update {
    Update::Put(
        MARKER.into(),
        br#"{"bootId":"boot_sample","sessions":2}"#.to_vec(),
    )
}

fn pushed(state: &mut LivenessState, key: &SessionKey, value: &str) {
    state.stale = false;
    state.sessions.insert(
        key.clone(),
        super::super::MirrorEntry {
            state: value.into(),
            canonical_root: "/p".into(),
            last_activity_ms: 99,
            cause: None,
        },
    );
}

#[test]
fn liveness_bus_marker_selects_complete_mirror_and_preserves_fallback_latch() {
    let mut state = LivenessState::default();
    let key = SessionKey::new("broca", "session_sample");
    pushed(&mut state, &key, "active");
    state.bus.watch_live = true;
    state.bus.finish_replay();
    state
        .bus
        .apply(put("opaque.with.dots", "broca", "session_sample", "idle"))
        .unwrap();
    assert!(matches!(state.read(&key), Read::Active));
    assert!(matches!(
        state.read(&SessionKey::new("other", "missing")),
        Read::Unknown(None)
    ));
    state.bus.apply(marker()).unwrap();
    assert!(matches!(state.read(&key), Read::Idle));
    assert!(matches!(
        state.read(&SessionKey::new("other", "missing")),
        Read::Gone
    ));
    assert_eq!(
        state.answer(&key).state,
        "active",
        "served replies still collapse idle"
    );
    state.bus.apply(Update::Delete(MARKER.into())).unwrap();
    assert!(matches!(state.read(&key), Read::Active));
    state.stale = true;
    assert!(matches!(state.read(&key), Read::Unknown(None)));
    state.bus.apply(marker()).unwrap();
    state.bus.watch_live = false;
    assert!(
        matches!(state.read(&key), Read::Unknown(None)),
        "a marker alone cannot vouch for a dead watch"
    );
}

#[test]
fn liveness_bus_decodes_prefrontal_values_and_orders_keys_before_marker() {
    // Prefrontal's bus-rows-v1 active/idle values use nullable project ids.
    // The synthetic second key also exercises unknown fields.
    let mut state = LivenessState::default();
    state.bus.watch_live = true;
    for value in [
        r#"{"harness":"broca","lastActivityMs":17,"projectId":"project_sample","sessionId":"session_sample","state":"active"}"#,
        r#"{"harness":"broca","lastActivityMs":18,"projectId":null,"sessionId":"session_sample","state":"idle"}"#,
    ] {
        state
            .bus
            .apply(Update::Put(
                "broca.session_sample".into(),
                value.as_bytes().to_vec(),
            ))
            .unwrap();
        assert!(!state.bus.authoritative());
    }
    state
        .bus
        .apply(put(
            "not.the.identity",
            "harness.dotted",
            "s.dotted",
            "active",
        ))
        .unwrap();
    assert!(!state.bus.authoritative());
    state.bus.apply(marker()).unwrap();
    state.bus.finish_replay();
    assert_eq!(state.bus.sessions.len(), 2);
    assert!(matches!(
        state.read(&SessionKey::new("broca", "session_sample")),
        Read::Idle
    ));
    assert!(matches!(
        state.read(&SessionKey::new("harness.dotted", "s.dotted")),
        Read::Active
    ));
    assert_eq!(state.project_activity("project_sample", &[]), Some(18));
    assert_eq!(state.project_activity("unrelated", &[]), None);
    state
        .bus
        .apply(Update::Delete("broca.session_sample".into()))
        .unwrap();
    assert!(matches!(
        state.read(&SessionKey::new("broca", "session_sample")),
        Read::Gone
    ));
}

/// Prefrontal's recorded bucket rows (hex-encoded session keys, the literal
/// marker key) drive the mirror through a full pass: marker delete, entry,
/// marker put, then the entry's delete.
#[test]
fn liveness_bus_reads_prefrontal_recorded_rows() {
    fn row(name: &str) -> Update {
        let path = format!(
            "{}/tests/fixtures/prefrontal-session-liveness-v1/session_liveness.{name}.json",
            env!("CARGO_MANIFEST_DIR")
        );
        let row: serde_json::Value =
            serde_json::from_str(&std::fs::read_to_string(path).unwrap()).unwrap();
        let key = row["key"].as_str().unwrap().to_owned();
        match row["operation"].as_str().unwrap() {
            "PUT" => Update::Put(key, row["value"].as_str().unwrap().as_bytes().to_vec()),
            "DEL" => Update::Delete(key),
            other => panic!("unexpected operation {other}"),
        }
    }
    let session = SessionKey::new("broca", "session_sample");
    let mut state = LivenessState::default();
    state.bus.watch_live = true;
    state.bus.finish_replay();
    state.bus.apply(row("reconciled.deleted")).unwrap();
    state.bus.apply(row("active")).unwrap();
    assert!(!state.bus.authoritative());
    state.bus.apply(row("reconciled")).unwrap();
    assert!(state.bus.authoritative());
    assert!(matches!(state.read(&session), Read::Active));
    state.bus.apply(row("idle")).unwrap();
    assert!(matches!(state.read(&session), Read::Idle));
    assert_eq!(state.bus.apply(row("gone")).unwrap(), Some(session.clone()));
    assert!(matches!(state.read(&session), Read::Gone));
    state.bus.apply(row("reconciled.deleted")).unwrap();
    assert!(!state.bus.authoritative());
}

/// The writer's switch to hex keys deletes each old raw-id key. If the new
/// key's rewrite arrives first, the old key's delete must not end the session.
#[test]
fn liveness_bus_old_key_delete_keeps_session_rewritten_under_new_key() {
    let session = SessionKey::new("broca", "session_sample");
    let mut state = LivenessState::default();
    state.bus.watch_live = true;
    state.bus.finish_replay();
    state
        .bus
        .apply(put(
            "broca.session_sample",
            "broca",
            "session_sample",
            "idle",
        ))
        .unwrap();
    state
        .bus
        .apply(put(
            "broca.73657373696f6e5f73616d706c65",
            "broca",
            "session_sample",
            "idle",
        ))
        .unwrap();
    state.bus.apply(marker()).unwrap();
    assert_eq!(
        state
            .bus
            .apply(Update::Delete("broca.session_sample".into()))
            .unwrap(),
        None
    );
    assert!(matches!(state.read(&session), Read::Idle));
}

#[derive(Debug, PartialEq)]
enum Event {
    Open,
    Next,
}

struct Fake {
    events: mpsc::UnboundedSender<Event>,
    opens: mpsc::UnboundedReceiver<()>,
    updates: mpsc::UnboundedReceiver<Result<Delivery, Failure>>,
}

struct Control {
    events: mpsc::UnboundedReceiver<Event>,
    opens: mpsc::UnboundedSender<()>,
    updates: mpsc::UnboundedSender<Result<Delivery, Failure>>,
    failed: Arc<tokio::sync::Notify>,
}

#[async_trait]
impl Source for Fake {
    async fn watch_all(&mut self) -> Result<(), Failure> {
        self.events.send(Event::Open).unwrap();
        self.opens.recv().await.expect("permit fresh watch");
        Ok(())
    }
    async fn next(&mut self) -> Result<Delivery, Failure> {
        self.events.send(Event::Next).unwrap();
        self.updates.recv().await.expect("scripted next update")
    }
}

impl Control {
    async fn event(&mut self, expected: Event) {
        assert_eq!(self.events.recv().await, Some(expected));
    }
    async fn ready(&mut self) {
        self.opens.send(()).unwrap();
        self.event(Event::Next).await;
    }
    async fn apply(&mut self, update: Update) {
        self.deliver(update, false).await;
    }
    async fn complete(&mut self, update: Update) {
        self.deliver(update, true).await;
    }
    async fn deliver(&mut self, update: Update, seen_current: bool) {
        self.updates
            .send(Ok(Delivery {
                update,
                seen_current,
            }))
            .unwrap();
        self.event(Event::Next).await;
    }
}

fn start(handler: &ProjectsHandler) -> Control {
    let (events_tx, events) = mpsc::unbounded_channel();
    let (opens, opens_rx) = mpsc::unbounded_channel();
    let (updates, updates_rx) = mpsc::unbounded_channel();
    let worker = handler.liveness_bus_worker();
    let failed = worker.failed.clone();
    *handler.liveness_bus_task.lock().unwrap() = Some(tokio::spawn(worker.run(Fake {
        events: events_tx,
        opens: opens_rx,
        updates: updates_rx,
    })));
    Control {
        events,
        opens,
        updates,
        failed,
    }
}

#[tokio::test]
async fn liveness_bus_key_updated_after_marker_waits_for_initial_replay() {
    let handler = ProjectsHandler::with_runtime("bus-late-key".into(), || 7);
    let late = SessionKey::new("broca", "late");
    pushed(&mut handler.liveness.lock().unwrap(), &late, "active");
    let mut control = start(&handler);
    control.event(Event::Open).await;
    control.ready().await;
    control.apply(put("early", "broca", "early", "idle")).await;
    control.apply(marker()).await;
    {
        let state = handler.liveness.lock().unwrap();
        assert!(!state.bus.authoritative());
        assert!(
            matches!(state.read(&late), Read::Active),
            "a late replayed live key must not look gone"
        );
    }
    control.complete(put("late", "broca", "late", "idle")).await;
    {
        let state = handler.liveness.lock().unwrap();
        assert!(state.bus.authoritative());
        assert!(matches!(state.read(&late), Read::Idle));
        assert!(matches!(
            state.read(&SessionKey::new("broca", "early")),
            Read::Idle
        ));
    }
    handler.stop_liveness_bus().await;
}

#[tokio::test]
async fn liveness_bus_empty_bucket_waits_for_marker_on_first_delivery() {
    let handler = ProjectsHandler::with_runtime("bus-empty".into(), || 7);
    let key = SessionKey::new("broca", "pushed");
    pushed(&mut handler.liveness.lock().unwrap(), &key, "active");
    let mut control = start(&handler);
    control.event(Event::Open).await;
    control.ready().await;
    assert!(!handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    assert!(matches!(
        handler.liveness.lock().unwrap().read(&key),
        Read::Active
    ));
    // An empty watch emits no row. Its first later delivery carries
    // seen_current=true, which supplies the replay boundary with that row.
    control
        .complete(Update::Put(
            MARKER.into(),
            br#"{"bootId":"empty","sessions":0}"#.to_vec(),
        ))
        .await;
    assert!(handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    assert_eq!(
        handler.health.liveness_bus_sessions.load(Ordering::Relaxed),
        0
    );
    assert!(matches!(
        handler.liveness.lock().unwrap().read(&key),
        Read::Gone
    ));
    handler.stop_liveness_bus().await;
}

#[tokio::test(start_paused = true)]
async fn liveness_bus_watch_error_clears_rewatches_and_replays_marker() {
    let handler = ProjectsHandler::with_runtime("bus-test".into(), || 7);
    let mut control = start(&handler);
    control.event(Event::Open).await;
    control.ready().await;
    control.apply(put("opaque", "broca", "s", "idle")).await;
    assert!(!handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    control.complete(marker()).await;
    assert!(handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    control
        .updates
        .send(Err(Failure::new("MissingHeartbeat", "leaf outage")))
        .unwrap();
    control.failed.notified().await;
    {
        let state = handler.liveness.lock().unwrap();
        assert!(!state.bus.authoritative());
        assert!(state.bus.sessions.is_empty());
        assert!(matches!(
            state.read(&SessionKey::new("broca", "s")),
            Read::Unknown(None)
        ));
    }
    assert!(!handler
        .health
        .liveness_bus_connected
        .load(Ordering::Relaxed));
    assert_eq!(
        handler.health.liveness_bus_failures.load(Ordering::Relaxed),
        1
    );
    control.event(Event::Open).await;
    control.ready().await;
    control.apply(put("opaque", "broca", "s", "active")).await;
    control.complete(marker()).await;
    assert!(handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    assert_eq!(
        handler.health.liveness_bus_sessions.load(Ordering::Relaxed),
        1
    );
    assert!(matches!(
        handler
            .liveness
            .lock()
            .unwrap()
            .read(&SessionKey::new("broca", "s")),
        Read::Active
    ));
    handler.stop_liveness_bus().await;
    assert!(!handler
        .health
        .liveness_bus_connected
        .load(Ordering::Relaxed));
}

#[tokio::test]
async fn liveness_bus_upkeep_stamps_only_observed_authoritative_deletes() {
    use crate::sessions::tests::{provider, registration, result, Fixture, EXECUTIVE, NOW};
    let f = Fixture::new("bus-upkeep", true);
    f.providers(
        provider(crate::WRITER_MODULE),
        provider(crate::WRITER_MODULE),
    );
    let agent = f.create("Bus upkeep");
    for (reference, session) in [("r1", "s1"), ("r2", "s2")] {
        result(
            f.register(EXECUTIVE, registration(&agent, reference, "broca", session))
                .await,
        );
    }
    let ended = |session: &str| {
        f.handler
            .with_store(|store| Ok(store.agent_sessions_filtered(&agent, None, 10, false)?))
            .unwrap()
            .sessions
            .into_iter()
            .find(|row| row.session.as_deref() == Some(session))
            .unwrap()
            .ended_at_ms
    };
    f.handler.start_session_upkeep();
    let mut control = start(&f.handler);
    control.event(Event::Open).await;
    control.ready().await;
    control.apply(put("opaque1", "broca", "s1", "active")).await;
    control.apply(Update::Delete("opaque1".into())).await;
    control.apply(put("opaque1", "broca", "s1", "active")).await;
    control.apply(put("opaque2", "broca", "s2", "idle")).await;
    control.complete(marker()).await;
    control.apply(Update::Delete("never.observed".into())).await;
    // An authoritative live bus entry defeats an older pushed terminal report,
    // without changing the pushed batch's accepted/snapshotRequested reply.
    let reply = f.feed(json!({"seq":1,"snapshot":true,"sessions":[{
        "harness":"broca","sessionId":"s1","state":"gone","canonicalRoot":"/p","lastActivityMs":18
    }]})).await;
    assert_eq!(reply["accepted"], true);
    assert_eq!(reply["snapshotRequested"], false);
    assert_eq!(
        f.handler.read_session_liveness(&[("pi", "r1")]).unwrap()[0].state,
        "active"
    );
    control.apply(Update::Delete("opaque2".into())).await;
    loop {
        let done = f.handler.session_upkeep.stamps_done.notified();
        tokio::pin!(done);
        done.as_mut().enable();
        if ended("s2").is_some() {
            break;
        }
        done.await;
    }
    assert_eq!(ended("s2"), Some(NOW));
    assert_eq!(
        ended("s1"),
        None,
        "neither an incomplete bus delete nor a superseded pushed gone may end this binding"
    );
    control.apply(Update::Delete("opaque1".into())).await;
    loop {
        let done = f.handler.session_upkeep.stamps_done.notified();
        tokio::pin!(done);
        done.as_mut().enable();
        if ended("s1").is_some() {
            break;
        }
        done.await;
    }
    assert_eq!(ended("s1"), Some(NOW));
    f.handler.stop_liveness_bus().await;
    f.handler.stop_session_upkeep().await;
}

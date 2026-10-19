use super::*;
use crate::watches::{WatchTransport, IDENTITY, WHERE};
use crate::{HandlerError, ModuleHandler, RequestCtx};
use async_trait::async_trait;
use std::{future::Future, pin::Pin, task::Poll, time::Duration};
use tokio::sync::{mpsc, watch};

struct Sink {
    sender: mpsc::UnboundedSender<Vec<u8>>,
    cancel: watch::Sender<bool>,
}

impl Sink {
    fn new() -> (Self, mpsc::UnboundedReceiver<Vec<u8>>) {
        let (sender, receiver) = mpsc::unbounded_channel();
        (
            Self {
                sender,
                cancel: watch::channel(false).0,
            },
            receiver,
        )
    }
}

#[async_trait]
impl WatchTransport for Sink {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        self.sender.send(body).unwrap();
        Ok(())
    }
    async fn ended(&self) {
        let _ = self.cancel.subscribe().wait_for(|value| *value).await;
    }
}

async fn pending(future: Pin<&mut impl Future>) {
    let mut future = future;
    std::future::poll_fn(|cx| {
        assert!(
            future.as_mut().poll(cx).is_pending(),
            "watch ended unexpectedly"
        );
        Poll::Ready(())
    })
    .await;
}

fn request(method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
}

fn notice(receiver: &mut mpsc::UnboundedReceiver<Vec<u8>>) -> Value {
    let body = receiver.try_recv().expect("watch notice");
    assert!(body.len() <= 192 * 1024);
    serde_json::from_slice(&body).unwrap()
}

fn quiet(receiver: &mut mpsc::UnboundedReceiver<Vec<u8>>) {
    assert!(matches!(
        receiver.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

async fn debounce(future: Pin<&mut impl Future>) {
    let mut future = future;
    pending(future.as_mut()).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    pending(future).await;
}

async fn seeded(label: &str) -> Fixture {
    let f = Fixture::new(label).await;
    let source = f.snapshot();
    seed_agent(
        &source,
        HEAD,
        "Ada",
        Some(&json!({"genome":"a".repeat(2048),"type":"creature.classic","version":2})),
        false,
        true,
    );
    seed_agent(&source, ASSISTANT, "Grace", None, false, false);
    drop(source);
    assert!(matches!(f.import().await, HandlerOutcome::Response(_)));
    f
}

fn fleet(f: &Fixture) -> Value {
    let bytes = f
        .handler
        .with_agent_store(|s| s.agent_fleet_identity(None, INCARNATION))
        .unwrap();
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
}

fn store(f: &Fixture) -> RegistryStore {
    f.handler.store.lock().unwrap().as_ref().unwrap().clone()
}

fn observe(f: &Fixture, id: &str, observation: ClaimObservation, now: i64) {
    f.handler
        .watches
        .cache_claims(&store(f), &[(id.into(), observation)], now)
        .unwrap();
}

fn absent(stale: bool) -> ClaimObservation {
    ClaimObservation::Absent {
        stale,
        self_author: "01010101010101010101010101010101".into(),
    }
}

#[tokio::test(start_paused = true)]
async fn identity_resume_safety_resyncs_non_agent_ranges_and_bad_tokens() {
    let f = seeded("watch-resume-safety").await;
    let old = fleet(&f)["token"].clone();
    f.success(DIRECT, "assign_workspace", json!({"projectId":"served-project","workspaceId":"new-workspace","requestKey":"placement"})).await;
    let current = fleet(&f)["token"].clone();
    for token in [
        Value::Null,
        json!(7),
        json!("foreign:4"),
        json!(format!("{INCARNATION}:999999")),
        json!(format!("{INCARNATION}:-1")),
        json!("bad"),
        old,
    ] {
        let (sink, mut receiver) = Sink::new();
        let body = request(IDENTITY, json!({"token":token}));
        let watch = f.handler.serve_watch(&sink, &body, READER);
        tokio::pin!(watch);
        pending(watch.as_mut()).await;
        assert_eq!(
            notice(&mut receiver),
            json!({"kind":"resync","token":current})
        );
        quiet(&mut receiver);
        sink.cancel.send_replace(true);
        assert!(matches!(watch.await, HandlerOutcome::Streamed));
    }
}

#[tokio::test(start_paused = true)]
async fn identity_resume_changes_rows_and_removals_use_journal_versions() {
    let f = seeded("watch-resume-changes").await;
    let old = fleet(&f)["token"].clone();
    let renamed = f
        .success(
            CORE,
            "agent.rename",
            json!({"agent_id":HEAD,"name":"Ada Lovelace","request_key":"rename"}),
        )
        .await;
    let removed = f
        .success(
            CORE,
            "agent.dispose",
            json!({"agent_id":ASSISTANT,"request_key":"dispose"}),
        )
        .await;
    let (sink, mut receiver) = Sink::new();
    let body = request(IDENTITY, json!({"token":old}));
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let changed = notice(&mut receiver);
    assert_eq!(changed["kind"], "changes");
    assert_eq!(changed["agents"].as_array().unwrap().len(), 1);
    assert_eq!(changed["agents"][0]["displayName"], "Ada Lovelace");
    assert_eq!(changed["agents"][0]["version"], renamed["generation"]);
    assert_eq!(
        changed["agents"][0]["avatar"]["fingerprint"],
        "b4c04a5d931e65a48e022b8943678d55"
    );
    assert!(changed["agents"][0]["avatar"].get("genome").is_none());
    assert_eq!(
        changed["removed"],
        json!([{"agentId":ASSISTANT,"version":removed["generation"]}])
    );
    assert_eq!(changed["token"], fleet(&f)["token"]);
    quiet(&mut receiver);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn identity_current_resume_waits_and_coalesces_full_row_placement_changes() {
    let f = seeded("watch-placement").await;
    let body = request(IDENTITY, json!({"token":fleet(&f)["token"]}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    quiet(&mut receiver);
    f.success(
        CORE,
        "agent.rename",
        json!({"agent_id":HEAD,"name":"Changed","request_key":"rename"}),
    )
    .await;
    pending(watch.as_mut()).await;
    tokio::time::advance(Duration::from_millis(100)).await;
    f.success(DIRECT, "assign_workspace", json!({"projectId":"served-project","workspaceId":"new-workspace","requestKey":"placement"})).await;
    pending(watch.as_mut()).await;
    tokio::time::advance(Duration::from_millis(199)).await;
    pending(watch.as_mut()).await;
    quiet(&mut receiver);
    tokio::time::advance(Duration::from_millis(1)).await;
    pending(watch.as_mut()).await;
    let changed = notice(&mut receiver);
    assert_eq!(changed["agents"].as_array().unwrap().len(), 1);
    assert_eq!(changed["agents"][0]["displayName"], "Changed");
    assert_eq!(
        changed["agents"][0]["workspace"],
        json!({"id":"new-workspace","name":"new-workspace"})
    );
    assert_eq!(changed["agents"][0]["version"], fleet(&f)["generation"]);
    assert_eq!(changed["removed"], json!([]));
    quiet(&mut receiver);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn watch_debounce_has_one_second_ceiling() {
    let f = seeded("watch-ceiling").await;
    let body = request(IDENTITY, json!({"token":fleet(&f)["token"]}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    for i in 0..11 {
        f.success(
            CORE,
            "agent.update_tag",
            json!({"agent_id":HEAD,"tag":format!("tag-{i}"),"request_key":format!("tag-{i}")}),
        )
        .await;
        pending(watch.as_mut()).await;
        if i < 10 {
            quiet(&mut receiver);
            tokio::time::advance(Duration::from_millis(100)).await;
        }
    }
    assert_eq!(notice(&mut receiver)["kind"], "changes");
    quiet(&mut receiver);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn watch_size_guard_resyncs_instead_of_oversize_or_split() {
    let f = Fixture::new("watch-oversize").await;
    let body = request(IDENTITY, json!({"token":fleet(&f)["token"]}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let source = f.snapshot();
    for i in 1..=1200 {
        let id = format!("agent_{i:016x}");
        seed_agent(&source, &id, &format!("Agent {i}"), None, false, false);
        source
            .execute(
                "UPDATE agent SET tag=?1 WHERE agent_id=?2",
                params!["t".repeat(256), id],
            )
            .unwrap();
    }
    drop(source);
    assert!(matches!(f.import().await, HandlerOutcome::Response(_)));
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"resync","token":fleet(&f)["token"]})
    );
    quiet(&mut receiver);
    assert_eq!(
        f.handler
            .health
            .watch_resyncs
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));

    let body = request(WHERE, json!({"cursor":{"incarnation":INCARNATION,"seq":0}}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let observations = (1..=1200)
        .map(|i| (format!("agent_{i:016x}"), absent(false)))
        .collect::<Vec<_>>();
    f.handler
        .watches
        .cache_claims(&store(&f), &observations, 701)
        .unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"resync","cursor":{"incarnation":INCARNATION,"seq":1}})
    );
    quiet(&mut receiver);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn where_resume_filters_versions_stale_known_and_ring_overflow() {
    let f = seeded("watch-where-ring").await;
    let body = request(
        WHERE,
        json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}}),
    );
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    quiet(&mut receiver);
    observe(&f, ASSISTANT, absent(false), 701);
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);
    observe(&f, HEAD, absent(false), 702);
    debounce(watch.as_mut()).await;
    let changed = notice(&mut receiver);
    assert_eq!(
        changed["cursor"],
        json!({"incarnation":INCARNATION,"seq":2})
    );
    let mut expected = f
        .success(READER, "agent.where_many", json!({"agentIds":[HEAD]}))
        .await["agents"][0]
        .clone();
    expected["version"] = json!(2);
    assert_eq!(changed["where"], json!([expected]));
    observe(&f, HEAD, ClaimObservation::Unavailable, 703);
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["where"][0]["stale"], true);
    observe(&f, HEAD, absent(false), 704);
    observe(&f, HEAD, absent(false), 704);
    debounce(watch.as_mut()).await;
    let restored = notice(&mut receiver);
    assert_eq!(restored["where"][0]["stale"], false);
    assert_eq!(
        restored["cursor"]["seq"], 4,
        "identical transaction must not bump seq"
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));

    for now in 705..=962 {
        observe(&f, HEAD, absent(false), now);
    }
    for cursor in [
        json!({"incarnation":INCARNATION,"seq":0}),
        json!({"incarnation":INCARNATION,"seq":999999}),
        json!({"incarnation":"foreign","seq":262}),
    ] {
        let (sink, mut receiver) = Sink::new();
        let body = request(WHERE, json!({"cursor":cursor}));
        let watch = f.handler.serve_watch(&sink, &body, READER);
        tokio::pin!(watch);
        pending(watch.as_mut()).await;
        assert_eq!(
            notice(&mut receiver),
            json!({"kind":"resync","cursor":{"incarnation":INCARNATION,"seq":262}})
        );
        sink.cancel.send_replace(true);
        assert!(matches!(watch.await, HandlerOutcome::Streamed));
    }
    let (sink, mut receiver) = Sink::new();
    let body = request(
        WHERE,
        json!({"cursor":{"incarnation":INCARNATION,"seq":261}}),
    );
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["where"][0]["version"], 262);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn watch_capacity_is_shared_releases_on_cancel_and_sends_nothing_after_cancel() {
    let f = seeded("watch-capacity").await;
    let mut sinks = Vec::new();
    let mut receivers = Vec::new();
    for _ in 0..16 {
        let (sink, receiver) = Sink::new();
        sinks.push(sink);
        receivers.push(receiver);
    }
    let bodies = (0..16)
        .map(|i| request(if i % 2 == 0 { IDENTITY } else { WHERE }, json!({})))
        .collect::<Vec<_>>();
    let mut watches = sinks
        .iter()
        .zip(&bodies)
        .map(|(sink, body)| Box::pin(f.handler.serve_watch(sink, body, READER)))
        .collect::<Vec<_>>();
    for watch in &mut watches {
        pending(watch.as_mut()).await;
    }
    for receiver in &mut receivers {
        notice(receiver);
    }
    assert_eq!(
        f.handler
            .health
            .watches_open
            .load(std::sync::atomic::Ordering::Relaxed),
        16
    );
    let body = request(WHERE, json!({}));
    let (extra, mut receiver) = Sink::new();
    {
        let refused = f.handler.serve_watch(&extra, &body, READER);
        tokio::pin!(refused);
        let outcome = std::future::poll_fn(|cx| Poll::Ready(refused.as_mut().poll(cx))).await;
        assert!(
            matches!(outcome, Poll::Ready(HandlerOutcome::Error { code, .. }) if code == "watch_capacity"),
            "the seventeenth watch must refuse before starting"
        );
    }
    sinks[0].cancel.send_replace(true);
    assert!(matches!(watches.remove(0).await, HandlerOutcome::Streamed));
    let accepted = f.handler.serve_watch(&extra, &body, READER);
    tokio::pin!(accepted);
    pending(accepted.as_mut()).await;
    notice(&mut receiver);
    f.success(
        CORE,
        "agent.rename",
        json!({"agent_id":HEAD,"name":"After cancel","request_key":"after"}),
    )
    .await;
    tokio::time::advance(Duration::from_secs(1)).await;
    assert!(
        receivers[0].try_recv().is_err(),
        "cancelled watch emitted another notice"
    );
    for sink in &sinks[1..] {
        sink.cancel.send_replace(true);
    }
    for watch in watches {
        assert!(matches!(watch.await, HandlerOutcome::Streamed));
    }
    extra.cancel.send_replace(true);
    assert!(matches!(accepted.await, HandlerOutcome::Streamed));
    assert_eq!(
        f.handler
            .health
            .watches_open
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test(start_paused = true)]
async fn watch_validation_and_drain_refusal_match_existing_reads() {
    let f = seeded("watch-validation").await;
    let (sink, _) = Sink::new();
    for (op, params) in [
        (IDENTITY, json!({"agentId":HEAD})),
        (WHERE, json!({"agentIds":[]})),
        (WHERE, json!({"agentIds":[HEAD,HEAD]})),
        (WHERE, json!({"agentIds":["bad"]})),
        (WHERE, json!({"agentIds":vec![HEAD;65]})),
        (WHERE, json!({"workspace_id":"ambient"})),
    ] {
        assert!(
            matches!(f.handler.serve_watch(&sink, &request(op, params), READER).await, HandlerOutcome::Error { code, .. } if code == "invalid_request")
        );
    }
    f.handler
        .on_draining(
            subc_client_rs::RouteCloseReason::Restart,
            std::time::SystemTime::now(),
        )
        .await;
    assert!(
        matches!(f.handler.serve_watch(&sink, &request(IDENTITY, json!({})), READER).await, HandlerOutcome::Error { code, .. } if code == "module_draining")
    );
    assert_eq!(
        f.handler
            .health
            .watches_open
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
}

#[tokio::test(start_paused = true)]
async fn where_known_changes_and_exact_ring_boundary_resume() {
    let f = seeded("watch-where-known").await;
    store(&f)
        .apply_entry("fixture", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                [],
            )
        })
        .unwrap();
    let unknown = f
        .success(READER, "agent.where_many", json!({"agentIds":[HEAD]}))
        .await;
    assert_eq!(unknown["agents"][0]["known"], false);
    observe(&f, HEAD, absent(false), 701);
    let body = request(WHERE, json!({"cursor":{"incarnation":INCARNATION,"seq":0}}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["where"][0]["known"], true);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
    for now in 702..=956 {
        observe(&f, HEAD, absent(false), now);
    }
    let body = request(WHERE, json!({"cursor":{"incarnation":INCARNATION,"seq":0}}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver)["where"][0]["version"],
        256,
        "oldest retained predecessor is resumable"
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn identity_live_diffs_cover_add_labels_avatar_merge_and_unchanged_rows() {
    let f = seeded("watch-live-identity").await;
    let body = request(IDENTITY, json!({"token":fleet(&f)["token"]}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let added = f
        .success(
            CORE,
            "agent.create",
            json!({"name":"New","role":"assistant","tag":"new","request_key":"create"}),
        )
        .await["agent"]["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["agents"][0]["agentId"], added);
    for (op, params) in [
        (
            "agent.set_labels",
            json!({"agent_id":HEAD,"labels":["new-label"],"request_key":"labels"}),
        ),
        (
            "agent.set_avatar",
            json!({"agentId":HEAD,"genome":"b".repeat(2048),"type":"creature.classic","version":2,"request_key":"avatar"}),
        ),
    ] {
        f.success(CORE, op, params).await;
        debounce(watch.as_mut()).await;
        let changed = notice(&mut receiver);
        let mut expected = fleet(&f)["agents"][0].clone();
        expected["version"] = fleet(&f)["generation"].clone();
        assert_eq!(changed["agents"], json!([expected]));
        assert_eq!(changed["removed"], json!([]));
    }
    f.success(CORE, "agent.set_github_identity", json!({"agent_id":HEAD,"github_identity":{"kind":"user_token","login":"octocat","credential_ref":"cred"},"request_key":"github"})).await;
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);
    f.success(
        CORE,
        "agent.merge",
        json!({"agent_id":added,"into_agent_id":ASSISTANT,"request_key":"merge"}),
    )
    .await;
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver)["removed"],
        json!([{"agentId":added,"version":fleet(&f)["generation"]}])
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn identity_project_name_workspace_name_and_root_commits_wake_full_row_diff() {
    let f = seeded("watch-project-fields").await;
    let body = request(IDENTITY, json!({"token":fleet(&f)["token"]}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let root = f.dir.join("z-root");
    std::fs::create_dir_all(&root).unwrap();
    let root = RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap();
    let first_root = f.dir.join("a-root");
    std::fs::create_dir_all(&first_root).unwrap();
    let first_root = RegistryStore::canonical_mutation_root(first_root.to_str().unwrap()).unwrap();
    for (op, params) in [
        (
            "register",
            json!({"projectId":"served-project","name":"Renamed project","roots":[]}),
        ),
        (
            "upgrade_implicit",
            json!({"implicitId":"former-implicit","projectId":"served-project","name":"Upgraded project"}),
        ),
        (
            "seed_import",
            json!({"source":"mc","payload":{"workspaces":[{"workspaceId":"served-workspace","name":"Renamed workspace"}]}}),
        ),
        (
            "add_root",
            json!({"projectId":"served-project","root":root}),
        ),
        (
            "add_root",
            json!({"projectId":"served-project","root":first_root}),
        ),
        (
            "remove_root",
            json!({"projectId":"served-project","root":first_root}),
        ),
    ] {
        f.success(DIRECT, op, params).await;
        debounce(watch.as_mut()).await;
        let changed = notice(&mut receiver);
        let mut expected = fleet(&f)["agents"][0].clone();
        expected["version"] = fleet(&f)["generation"].clone();
        assert_eq!(changed["agents"], json!([expected]), "{op}");
        assert_eq!(changed["removed"], json!([]));
        quiet(&mut receiver);
    }
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

struct ServedHandler(Arc<ProjectsHandler>);

#[async_trait]
impl ModuleHandler for ServedHandler {
    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        self.0.handle(ctx, body).await
    }
    async fn on_bind(&self, request: &crate::RouteBindRequest) -> crate::BindDecision {
        self.0.on_bind(request).await
    }
    async fn on_draining(
        &self,
        reason: subc_client_rs::RouteCloseReason,
        deadline: std::time::SystemTime,
    ) {
        self.0.on_draining(reason, deadline).await;
    }
    async fn on_route_gone(&self, handle: &subc_client_rs::RouteHandle) {
        self.0.on_route_gone(handle).await;
    }
}

use subc_client_rs::test_support::{BindIdentity, Frame, FrameType, ModuleHarness, RouteTarget};
use subc_client_rs::RouteHandle;

struct Served {
    fixture: Fixture,
    handler: Arc<ProjectsHandler>,
    module: ModuleHarness,
    route: RouteHandle,
}

impl Served {
    async fn new(label: &str) -> Self {
        let mut fixture = seeded(label).await;
        let handler = Arc::new(std::mem::replace(
            &mut fixture.handler,
            ProjectsHandler::with_runtime(INCARNATION.into(), || 700),
        ));
        let served = handler.clone();
        let mut module = ModuleHarness::start(move |_| ServedHandler(served), []).await;
        let route = Self::bind(&mut module, 1801).await;
        Self {
            fixture,
            handler,
            module,
            route,
        }
    }

    async fn bind(module: &mut ModuleHarness, channel: u16) -> RouteHandle {
        module
            .bind_route(
                crate::RouteBindRequest::new(
                    RouteHandle::detached(channel, 1),
                    RouteTarget::ManagementSurface {
                        module_id: crate::MODULE_ID.into(),
                    },
                    BindIdentity::new("/tmp/watch", "test", "phone"),
                )
                .with_principal(reserved("callosum")),
            )
            .await
    }

    async fn start(&mut self, method: &str, params: Value) -> u64 {
        let waiting = self.handler.health.watch_waiting.notified();
        tokio::pin!(waiting);
        waiting.as_mut().enable();
        let corr = self
            .module
            .send_request(self.route, request(method, params))
            .await;
        waiting.await;
        corr
    }

    async fn frame(&mut self, corr: u64) -> Frame {
        let frame = tokio::time::timeout(Duration::from_secs(10), self.module.next_frame())
            .await
            .expect("watch transport stalled")
            .expect("watch transport closed");
        assert_eq!(frame.header.corr, corr);
        assert!(frame.body.len() <= 192 * 1024);
        frame
    }

    async fn success(&self, method: &str, params: Value) -> Value {
        let result = self
            .handler
            .handle_served_request(&request(method, params), CORE)
            .await;
        let HandlerOutcome::Response(body) = result else {
            panic!("setup refused: {}", refusal(result));
        };
        serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
    }

    async fn cancel(&mut self, corr: u64) {
        self.module.cancel_request(self.route, corr).await;
        assert_eq!(self.frame(corr).await.header.ty, FrameType::StreamEnd);
    }

    fn fleet(&self) -> Value {
        let bytes = self
            .handler
            .with_agent_store(|s| s.agent_fleet_identity(None, INCARNATION))
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
    }

    async fn stop(mut self) {
        self.module.shutdown().await;
        crate::tests::stop_store_tasks(&self.handler).await;
        // Every task is stopped before releasing SQLite, then reopen the same
        // descriptor to prove no watch retained a store handle or writer lease.
        self.handler.store.lock().unwrap().take();
        let reopened = RegistryStore::open(&self.fixture.descriptor).unwrap();
        drop(reopened);
    }
}

fn record(method: &str, params: Value, frames: &[Frame]) -> String {
    let frames = frames
        .iter()
        .map(|frame| {
            let kind = match frame.header.ty {
                FrameType::StreamData => "StreamData",
                FrameType::StreamEnd => "StreamEnd",
                FrameType::Error => "Error",
                other => panic!("unexpected frame {other:?}"),
            };
            let body = if frame.body.is_empty() {
                "null".into()
            } else {
                String::from_utf8(frame.body.clone()).unwrap()
            };
            format!("{{\"type\":{},\"body\":{body}}}", json!(kind))
        })
        .collect::<Vec<_>>()
        .join(",");
    format!("  {{\"op\":{},\"request\":{},\"principal\":{{\"kind\":\"reserved\",\"moduleId\":\"callosum\"}},\"frames\":[{frames}]}}", json!(method), params)
}

#[tokio::test(start_paused = true)]
async fn served_fleet_watch_v1_records_match_published_files() {
    let mut s = Served::new("served-fleet-watch").await;
    let corr = s.start(IDENTITY, json!({})).await;
    let first = s.frame(corr).await;
    assert_eq!(first.header.ty, FrameType::StreamData);
    assert_eq!(
        serde_json::from_slice::<Value>(&first.body).unwrap()["kind"],
        "resync"
    );
    published_file(
        "fleet-watch-v1",
        "identity-resync.json",
        vec![record(IDENTITY, json!({}), &[first])],
    );
    s.cancel(corr).await;

    let token = s.fleet()["token"].clone();
    s.success(
        "agent.rename",
        json!({"agent_id":HEAD,"name":"Ada Lovelace","request_key":"rename"}),
    )
    .await;
    s.success(
        "agent.dispose",
        json!({"agent_id":ASSISTANT,"request_key":"dispose"}),
    )
    .await;
    let params = json!({"token":token});
    let corr = s.start(IDENTITY, params.clone()).await;
    let changed = s.frame(corr).await;
    assert_eq!(
        serde_json::from_slice::<Value>(&changed.body).unwrap()["removed"][0]["agentId"],
        ASSISTANT
    );
    published_file(
        "fleet-watch-v1",
        "identity-changes.json",
        vec![record(IDENTITY, params, &[changed])],
    );
    s.cancel(corr).await;

    let params = json!({"token":s.fleet()["token"]});
    let corr = s.start(IDENTITY, params.clone()).await;
    {
        let next = s.module.next_frame();
        tokio::pin!(next);
        pending(next.as_mut()).await;
    }
    s.success(
        "agent.update_tag",
        json!({"agent_id":HEAD,"tag":"updated","request_key":"tag"}),
    )
    .await;
    let changed = s.frame(corr).await;
    published_file(
        "fleet-watch-v1",
        "identity-resumed.json",
        vec![
            record(IDENTITY, params.clone(), &[]),
            record(IDENTITY, params, &[changed]),
        ],
    );
    s.cancel(corr).await;

    let corr = s.start(WHERE, json!({})).await;
    let first = s.frame(corr).await;
    published_file(
        "fleet-watch-v1",
        "where-resync.json",
        vec![record(WHERE, json!({}), &[first])],
    );
    s.cancel(corr).await;
    let store = s.handler.store.lock().unwrap().as_ref().unwrap().clone();
    s.handler
        .watches
        .cache_claims(&store, &[(HEAD.into(), absent(false))], 701)
        .unwrap();
    drop(store);
    let params = json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}});
    let corr = s.start(WHERE, params.clone()).await;
    let changed = s.frame(corr).await;
    published_file(
        "fleet-watch-v1",
        "where-changes.json",
        vec![record(WHERE, params, &[changed])],
    );
    s.cancel(corr).await;

    let mut held = Vec::new();
    for i in 0..16 {
        let corr = s
            .start(if i % 2 == 0 { IDENTITY } else { WHERE }, json!({}))
            .await;
        s.frame(corr).await;
        held.push(corr);
    }
    let corr = s
        .module
        .send_request(s.route, request(WHERE, json!({})))
        .await;
    let refused = s.frame(corr).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.body).unwrap()["code"],
        "watch_capacity"
    );
    published_file(
        "fleet-watch-v1",
        "watch-capacity.json",
        vec![record(WHERE, json!({}), &[refused])],
    );
    for corr in held {
        s.cancel(corr).await;
    }
    s.stop().await;
}

#[tokio::test(start_paused = true)]
async fn served_watch_route_cancel_is_isolated_and_module_drain_sends_stream_end() {
    let mut s = Served::new("served-watch-drain").await;
    let first = s.start(IDENTITY, json!({})).await;
    s.frame(first).await;
    let other_route = Served::bind(&mut s.module, 1802).await;
    let route = std::mem::replace(&mut s.route, other_route);
    let second = s.start(WHERE, json!({})).await;
    s.frame(second).await;
    s.module.cancel_request(route, first).await;
    assert_eq!(s.frame(first).await.header.ty, FrameType::StreamEnd);
    assert_eq!(
        s.handler
            .health
            .watches_open
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    s.handler
        .on_draining(
            subc_client_rs::RouteCloseReason::Restart,
            std::time::SystemTime::now() + Duration::from_secs(30),
        )
        .await;
    assert_eq!(s.frame(second).await.header.ty, FrameType::StreamEnd);
    assert_eq!(
        s.handler
            .health
            .watches_open
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    let corr = s
        .module
        .send_request(s.route, request(WHERE, json!({})))
        .await;
    let refused = s.frame(corr).await;
    assert_eq!(refused.header.ty, FrameType::Error);
    assert_eq!(
        serde_json::from_slice::<Value>(&refused.body).unwrap()["code"],
        "module_draining"
    );
    s.stop().await;
}

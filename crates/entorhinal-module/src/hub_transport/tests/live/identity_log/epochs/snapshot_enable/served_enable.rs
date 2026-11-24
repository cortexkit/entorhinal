use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    hub_switch::{MigrationImage, Probe, Switch},
    log_client::{LogClient, TransportError},
    sharing_baselines::{artifacts, StoreFile},
};
use tokio::sync::Notify;

mod convergence;

#[derive(Default)]
struct RecordingConnector(Arc<StdMutex<Vec<String>>>);

impl RecordingConnector {
    fn record(&self, target: &str) {
        self.0.lock().unwrap().push(target.into());
    }

    fn count(&self, target: &str) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|call| *call == target)
            .count()
    }

    fn assert_allowed(&self) {
        let requests = self.0.lock().unwrap();
        assert!(!requests.is_empty());
        assert!(
            requests
                .iter()
                .all(|target| matches!(target.split('.').next().unwrap(), "consent" | "hub")),
            "unexpected outbound requests: {requests:?}"
        );
    }
}

#[async_trait::async_trait]
impl crate::log_client::LogConnector for RecordingConnector {
    async fn connect(&self) -> Result<Arc<dyn crate::log_client::LogTransport>, TransportError> {
        Ok(Arc::new(Self(self.0.clone())))
    }
}

#[async_trait::async_trait]
impl crate::log_client::LogTransport for RecordingConnector {
    async fn call(&self, method: &str, _: Value) -> Result<Vec<u8>, TransportError> {
        self.record(method.split('.').next().unwrap());
        Err(TransportError::NoReply)
    }
}

struct Prompt {
    requests: Arc<RecordingConnector>,
    allow: bool,
    entered: Notify,
    release: Option<Notify>,
}

#[async_trait::async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        route: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        assert_eq!(route, ROUTE);
        assert_eq!(summary, "Start a new shared registry for this account?");
        self.requests.record("consent");
        self.entered.notify_one();
        if let Some(release) = &self.release {
            release.notified().await;
        }
        if self.allow {
            Ok(())
        } else {
            Err(OperatorConfirmError::Declined)
        }
    }
}

struct LiveProbe {
    log: Arc<IdentityLog<Leaf, TestKeys>>,
    leaf: Arc<Leaf>,
    requests: Arc<RecordingConnector>,
}

#[async_trait::async_trait]
impl Probe for LiveProbe {
    fn activate(&self) {}

    async fn head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.requests.record("hub");
        self.leaf.log_head(deadline).await
    }

    async fn local_box(&self, _: Instant) -> Result<String, HandlerError> {
        self.requests.record("hub.prepare");
        Ok(self.leaf.box_id.to_string())
    }

    async fn hub_cuts(&self, _: Instant) -> Result<bool, HandlerError> {
        unreachable!("native enable does not probe session cuts")
    }

    async fn transfer_snapshot(
        &self,
        image: Option<&MigrationImage>,
        deadline: Instant,
    ) -> Option<Result<bool, HandlerError>> {
        self.requests.record("hub");
        Some(transfer(&self.log, image, deadline).await)
    }
}

struct Machine {
    handler: Arc<ProjectsHandler>,
    log: Arc<IdentityLog<Leaf, TestKeys>>,
    prompt: Arc<Prompt>,
    requests: Arc<RecordingConnector>,
}

impl Machine {
    fn new(
        rig: &IdentityRig,
        store: RegistryStore,
        fleet: &Arc<Fleet>,
        leader: bool,
        allow: bool,
        hold: bool,
    ) -> Self {
        let leaf = if leader { &rig.a.leaf } else { &rig.b.leaf };
        let (mut handler, log, _) = bootstrap(rig, store, leaf.clone(), fleet.clone(), None, 0);
        let requests = Arc::new(RecordingConnector::default());
        let prompt = Arc::new(Prompt {
            requests: requests.clone(),
            allow,
            entered: Notify::new(),
            release: hold.then(Notify::new),
        });
        handler.operator_confirmer = prompt.clone();
        handler.log_client = LogClient::new(requests.clone());
        handler.record_client = crate::record_client::RecordClient::new(requests.clone());
        handler.hub_switch = Some(Switch::with_probe(Arc::new(LiveProbe {
            log: log.clone(),
            leaf: leaf.clone(),
            requests: requests.clone(),
        })));
        *handler.hub_shared_log.lock().unwrap() = Some(log.clone());
        Self {
            handler: Arc::new(handler),
            log,
            prompt,
            requests,
        }
    }

    fn store(&self) -> RegistryStore {
        self.log.sql.test_store()
    }

    async fn enable(&self, params: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(&body("identity_log.enable", params), ROUTE)
            .await
    }

    async fn status(&self) -> Value {
        result(
            self.handler
                .handle_served_request(&body("identity_log.status", json!({})), ROUTE)
                .await,
        )
    }

    fn unchanged_disabled(&self, generation: i64) {
        assert_eq!(self.store().generation().unwrap(), generation);
        assert_eq!(
            self.store().identity_log_status().unwrap().state,
            "disabled"
        );
        assert!(self.store().hub_enable_plan().unwrap().is_none());
    }
}

fn refused(outcome: HandlerOutcome, expected: &str) {
    assert!(
        matches!(outcome, HandlerOutcome::Error { ref code, .. } | HandlerOutcome::ErrorWithDetail { ref code, .. } if code == expected),
        "expected {expected}: {outcome:?}"
    );
}

fn pair_fleet(rig: &IdentityRig) -> Arc<Fleet> {
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, rig, &[A, B]);
    fleet
}

async fn empty_rig() -> Option<IdentityRig> {
    let rig = IdentityRig::new().await?;
    // These tests provision their own members and the keys needed to open the
    // bootstrap snapshot. Stop the rig's default readers before publishing it.
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.hub_tail_task.lock().unwrap().take().unwrap();
        tail.abort();
        assert!(tail.await.unwrap_err().is_cancelled());
    }
    Some(rig)
}

#[tokio::test]
async fn hub_served_enable_guards() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let fleet = pair_fleet(&rig);
    let leader = Machine::new(
        &rig,
        store(&rig._processes, "enable-guards-a"),
        &fleet,
        true,
        true,
        false,
    );
    let denied = Machine::new(
        &rig,
        store(&rig._processes, "enable-deny"),
        &fleet,
        false,
        false,
        false,
    );
    let before = denied.store().generation().unwrap();
    refused(denied.enable(json!({})).await, "agents_not_imported");
    assert_eq!(denied.requests.count("consent"), 0);
    denied.unchanged_disabled(before);
    assert_eq!(leader.log.log_head(deadline()).await.unwrap(), 0);
    refused(
        denied.enable(json!({"without_agents":true})).await,
        "operator_declined",
    );
    assert_eq!(denied.requests.count("consent"), 1);
    denied.unchanged_disabled(before);
    assert_eq!(leader.log.log_head(deadline()).await.unwrap(), 0);
    denied.requests.assert_allowed();

    add_agents(&leader.store(), 4);
    let before_import = leader.store().generation().unwrap();
    refused(leader.enable(json!({})).await, "authority_not_cut_over");
    leader.unchanged_disabled(before_import);
    leader
        .store()
        .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let after_import = leader.store().generation().unwrap();
    refused(
        leader.enable(json!({"without_agents":true})).await,
        "invalid_request",
    );
    leader.unchanged_disabled(after_import);
    assert_eq!(leader.requests.count("consent"), 0);
    let pending = Machine::new(
        &rig,
        store(&rig._processes, "enable-race"),
        &fleet,
        false,
        true,
        true,
    );
    let race = tokio::spawn({
        let pending = pending.handler.clone();
        async move {
            pending
                .handle_served_request(
                    &body("identity_log.enable", json!({"without_agents":true})),
                    ROUTE,
                )
                .await
        }
    });
    within(pending.prompt.entered.notified()).await;
    assert!(pending.store().hub_enable_plan().unwrap().is_none());
    assert_eq!(result(leader.enable(json!({})).await)["state"], "enabled");
    assert_eq!(leader.requests.count("consent"), 1);
    assert_eq!(leader.store().hub_log_base().unwrap(), Some(0));
    let head = leader.log.log_head(deadline()).await.unwrap();
    assert!(head > 0);
    let before = leader.store().generation().unwrap();
    assert_eq!(result(leader.enable(json!({})).await)["state"], "enabled");
    assert_eq!(leader.store().generation().unwrap(), before);
    assert_eq!(leader.requests.count("consent"), 1);
    leader.requests.assert_allowed();

    pending.prompt.release.as_ref().unwrap().notify_one();
    assert_eq!(result(within(race).await.unwrap())["state"], "enabled");
    assert_eq!(
        pending.store().installed_hub_snapshot().unwrap(),
        leader.store().installed_hub_snapshot().unwrap()
    );
    assert!(pending.store().hub_enable_plan().unwrap().is_none());
    assert_eq!(leader.log.log_head(deadline()).await.unwrap(), head);
    assert_eq!(pending.requests.count("hub.prepare"), 0);
    pending.requests.assert_allowed();

    let nonempty = Machine::new(
        &rig,
        store(&rig._processes, "enable-nonempty"),
        &fleet,
        false,
        true,
        false,
    );
    add_agents(&nonempty.store(), 4);
    nonempty
        .store()
        .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let before = nonempty.store().generation().unwrap();
    refused(
        nonempty.enable(json!({})).await,
        "join_requires_empty_registry",
    );
    nonempty.unchanged_disabled(before);
    assert_eq!(nonempty.requests.count("consent"), 0);
    assert_eq!(leader.log.log_head(deadline()).await.unwrap(), head);
    nonempty.requests.assert_allowed();
}

#[tokio::test]
async fn hub_served_enable_finishes_the_saved_native_plan() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let file = StoreFile::copy(&artifacts().join("native-enabling.db"));
    let a = Machine::new(&rig, file.open(), &pair_fleet(&rig), true, false, false);
    let saved = a.store().hub_enable_plan().unwrap().unwrap();
    let (landed, wait_landed) = oneshot::channel();
    let (release, wait_release) = oneshot::channel();
    *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(landed, wait_release));
    let call = tokio::spawn({
        let handler = a.handler.clone();
        async move {
            handler
                .handle_served_request(&body("identity_log.enable", json!({})), ROUTE)
                .await
        }
    });
    within(wait_landed).await.unwrap();
    assert!(a.store().hub_enable_plan().unwrap().as_ref() == Some(&saved));
    assert_eq!(a.store().hub_log_base().unwrap(), None);
    assert_eq!(a.requests.count("consent"), 0);
    release.send(()).unwrap();
    assert_eq!(result(within(call).await.unwrap())["state"], "enabled");
    assert!(a.store().hub_enable_plan().unwrap().is_none());
    assert_eq!(
        a.store().installed_hub_snapshot().unwrap().migration_id,
        saved.enable_id
    );
    a.requests.assert_allowed();
}

#[tokio::test]
async fn hub_catch_up_resumes_saved_enable_and_joins_disabled_store_then_applies_tail() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let file = StoreFile::copy(&artifacts().join("native-enabling.db"));
    let fleet = pair_fleet(&rig);
    let a = Machine::new(&rig, file.open(), &fleet, true, false, false);
    let b = Machine::new(
        &rig,
        store(&rig._processes, "catch-up-join"),
        &fleet,
        false,
        false,
        false,
    );
    let saved = a.store().hub_enable_plan().unwrap().unwrap();
    assert_eq!(a.store().hub_log_base().unwrap(), None);
    assert_eq!(b.status().await["state"], "disabled");
    assert!(b.store().hub_enable_plan().unwrap().is_none());
    a.handler.start_catch_up();
    within(async {
        while a.handler.hub_tail_task.lock().unwrap().is_none() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        a.store().installed_hub_snapshot().unwrap().migration_id,
        saved.enable_id
    );
    assert!(a.store().hub_enable_plan().unwrap().is_none());
    assert_eq!(a.requests.count("consent"), 0);
    a.requests.assert_allowed();

    b.handler.start_catch_up();
    within(async {
        while b.handler.hub_tail_task.lock().unwrap().is_none() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        b.store().installed_hub_snapshot().unwrap(),
        a.store().installed_hub_snapshot().unwrap()
    );
    assert_eq!(b.requests.count("consent"), 0);
    assert_eq!(b.requests.count("hub.prepare"), 0);
    b.requests.assert_allowed();

    // The background worker installed the log tail. Activate the residence
    // watch as well so the served write follows the normal readiness guard.
    let feed = Feed::kv(a.handler.health.clone(), {
        let leaf = rig.a.leaf.clone();
        move |_| {
            Binding {
                session: leaf.session(),
                changes: None,
            }
            .residence_watch()
        }
    })
    .with_durable_cursor();
    let image = a.store().installed_hub_snapshot().unwrap().image;
    a.handler
        .activate_hub_residence(a.log.clone(), feed, Arc::new(Seeds::new(image)))
        .unwrap();
    within(async {
        while !a.handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result(a.handler.handle_served_request(&body("register", json!({"projectId":"after-bootstrap","name":"Tail entry","roots":[],"requestKey":"tail-status"})), ROUTE).await);
    let position = a.log.log_head(deadline()).await.unwrap();
    within(async {
        loop {
            let status = b.status().await;
            if status["lastAppliedPosition"] == position {
                assert_eq!(status["lastSeenHead"], position);
                assert_eq!(status["state"], "enabled");
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(
        b.store()
            .enumerate(None)
            .unwrap()
            .projects
            .iter()
            .filter(|p| p.project_id == "after-bootstrap")
            .count(),
        1
    );
}

#[tokio::test]
async fn log_enable_cli_request_enables_through_operator_route() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let a = Machine::new(
        &rig,
        store(&rig._processes, "cli-enable"),
        &pair_fleet(&rig),
        true,
        true,
        false,
    );
    let wire = crate::cli::project_log_test_wire(&["log", "enable", "--without-agents"]);
    assert_eq!(
        result(a.handler.handle_served_request(&wire, ROUTE).await)["state"],
        "enabled"
    );
    assert_eq!(a.requests.count("consent"), 1);
    assert_eq!(a.log.log_head(deadline()).await.unwrap(), 1);
    activate(&a.handler, a.log.clone(), rig.a.leaf.clone());
    within(async {
        while !a.handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let wire = crate::cli::project_log_test_wire(&["log", "name", "Studio"]);
    assert!(result(a.handler.handle_served_request(&wire, ROUTE).await).is_object());
    assert_eq!(
        a.store().node_self().unwrap().display_name.as_deref(),
        Some("Studio")
    );
    a.requests.assert_allowed();
}

use super::*;
use async_trait::async_trait;
use cortexkit_store_types::{StorageBackend, StorageDescriptor};
use rusqlite::{Connection, OptionalExtension};
const OLD: &str = "11111111111111111111111111111111";
const NEW: &str = "22222222222222222222222222222222";
const DIRECT: RouteKey = (70, 1);
const CORE: RouteKey = (71, 1);

struct Fixture {
    handler: ProjectsHandler,
    descriptor: StorageDescriptor,
    dir: std::path::PathBuf,
    log: crate::fake_log::FakeLog,
    id: String,
}
impl Fixture {
    async fn new(recorded: Option<&str>, daemon: Option<&str>) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor("store-origin");
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let body = store.agent_mutation("agent.create",json!({"role":"assistant","workspace_id":null,"name":"Ada","tag":"assistant","request_key":"ada"}),1).unwrap();
        let id = serde_json::from_slice::<Value>(&body).unwrap()["result"]["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        if let Some(id) = recorded {
            store.check_store_origin(Some(id), 1).unwrap();
        }
        drop(store);
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            unreachable!()
        };
        let conn = Connection::open(path).unwrap();
        conn.execute("INSERT INTO self_identity VALUES(1,?1,1)", [OLD])
            .unwrap();
        conn.execute(
            "INSERT INTO claim_cache VALUES(?1,?2,1,NULL,1,1,0,0)",
            rusqlite::params![id, OLD],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO role_cache VALUES('leader',?1,1,'cloud',0,1,1,0)",
            [OLD],
        )
        .unwrap();
        drop(conn);
        let log = crate::fake_log::FakeLog::default();
        let handler =
            ProjectsHandler::with_log_connector("origin".into(), || 3, Arc::new(log.clone()));
        for (route, principal) in [
            (DIRECT, Principal::Direct),
            (
                CORE,
                Principal::Reserved {
                    module_id: WRITER_MODULE.into(),
                },
            ),
        ] {
            handler
                .route_admissions()
                .insert(route, RouteAdmission::from_bind(Some(principal), None));
        }
        let mut ack = crate::tests::hello_ack(&descriptor);
        ack.machine_id = daemon.map(|id| serde_json::from_value(json!(id)).unwrap());
        handler.on_hello_ack(&ack).await;
        Self {
            handler,
            descriptor,
            dir,
            log,
            id,
        }
    }
    fn conn(&self) -> Connection {
        let StorageBackend::Sqlite { path } = &self.descriptor.backend else {
            unreachable!()
        };
        Connection::open(path).unwrap()
    }
    async fn stop(self) {
        crate::tests::stop_store_tasks(&self.handler).await;
        drop(self.handler);
        std::fs::remove_dir_all(self.dir).unwrap();
    }
    async fn call(&self, method: &str, params: Value, key: RouteKey) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                key,
            )
            .await
    }
}
fn refused(outcome: HandlerOutcome, code: &str) {
    match outcome {
        HandlerOutcome::Error { code: actual, .. }
        | HandlerOutcome::ErrorWithDetail { code: actual, .. } => assert_eq!(actual, code),
        other => panic!("expected {code}, got {other:?}"),
    }
}
fn result(outcome: HandlerOutcome) -> Value {
    let HandlerOutcome::Response(bytes) = outcome else {
        panic!("{outcome:?}")
    };
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
}
struct Confirm {
    declined: bool,
    summaries: Mutex<Vec<String>>,
}
#[derive(Default)]
struct Watch {
    frames: Mutex<Vec<Value>>,
    ended: tokio::sync::Notify,
}
#[async_trait]
impl watches::WatchTransport for Watch {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        self.frames
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&body).unwrap());
        self.ended.notify_one();
        Ok(())
    }
    async fn ended(&self) {
        self.ended.notified().await;
    }
}
#[async_trait]
impl agent_ops::OperatorConfirmer for Confirm {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), agent_ops::OperatorConfirmError> {
        assert!(summary.chars().count() <= 200);
        assert!(summary.contains(OLD) && summary.contains(NEW));
        self.summaries.lock().unwrap().push(summary.into());
        if self.declined {
            Err(agent_ops::OperatorConfirmError::Declined)
        } else {
            Ok(())
        }
    }
}

#[tokio::test]
async fn store_origin_first_start_adopts_equal_is_normal_and_no_id_is_unchecked() {
    for (recorded, daemon, label) in [
        (None, Some(NEW), "this_machine"),
        (Some(NEW), Some(NEW), "this_machine"),
        (None, None, "unchecked"),
        (Some(OLD), None, "unchecked"),
    ] {
        let f = Fixture::new(recorded, daemon).await;
        let health = f.handler.health().await;
        assert_eq!(health.metrics.unwrap()["storeOrigin"], label);
        assert!(!f.handler.health.origin.is_foreign());
        assert!(f.handler.admit("register", DIRECT).is_ok());
        assert_eq!(
            f.conn()
                .query_row("SELECT machine_id FROM store_origin", [], |r| r
                    .get::<_, String>(0))
                .optional()
                .unwrap()
                .as_deref(),
            recorded.or(daemon)
        );
        f.stop().await;
    }
}

#[tokio::test]
async fn foreign_store_refuses_every_write_class_before_decoding_or_confirmation() {
    let f = Fixture::new(Some(OLD), Some(NEW)).await;
    for method in MUTATING_METHODS
        .iter()
        .copied()
        .filter(|m| !matches!(*m, "projects.session_liveness" | ADOPT))
    {
        for key in [DIRECT, CORE] {
            refused(
                f.call(method, json!({}), key).await,
                "store_from_another_machine",
            );
            assert_eq!(
                f.handler.admit(method, key).unwrap_err().code,
                "store_from_another_machine",
                "{method}"
            );
        }
    }
    for tool in [
        "projects_create_workspace",
        "projects_register",
        "agents_create_head",
        "agents_hire",
        "agents_retire_hire",
        "catalog_prepare",
        "catalog_install",
    ] {
        refused(
            f.handler
                .handle_tool_request(json!({"name":tool,"arguments":{}}), DIRECT)
                .await,
            "store_from_another_machine",
        );
    }
    for method in cortexkit_role_agent_data::REQUIRED_OPS
        .iter()
        .copied()
        .filter(|m| {
            !matches!(
                *m,
                "agent_data.describe" | "agent_data.busy" | "agent_data.status"
            )
        })
    {
        refused(
            f.call(method, json!({}), DIRECT).await,
            "store_from_another_machine",
        );
    }
    let error = f.handler.admit_store_write().unwrap_err();
    assert_eq!(
        error.detail.unwrap(),
        json!({"storeMachineId":OLD,"daemonMachineId":NEW})
    );
    assert_eq!(f.log.calls(), 0);
    f.stop().await;
}

#[tokio::test]
async fn foreign_store_starts_no_machine_background_work_and_health_is_lock_free() {
    let f = Fixture::new(Some(OLD), Some(NEW)).await;
    assert!(
        f.handler.catch_up_task.lock().unwrap().is_none(),
        "foreign store must not start catch-up"
    );
    assert!(f.handler.claim_refresh_task.lock().unwrap().is_none());
    assert!(f.handler.role_refresh_task.lock().unwrap().is_none());
    assert!(f.handler.move_driver_task.lock().unwrap().is_none());
    assert!(f.handler.catalog_poll_task.lock().unwrap().is_none());
    assert_eq!(f.log.calls(), 0);
    let held = f.handler.store.lock().unwrap();
    let health = {
        use std::{
            future::Future,
            task::{Context, Poll, Waker},
        };
        match std::pin::pin!(f.handler.health())
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(report) => report,
            Poll::Pending => panic!("health must not wait for the store mutex"),
        }
    };
    assert_eq!(health.status, HealthStatus::Degraded);
    let metrics = health.metrics.unwrap();
    assert_eq!(metrics["storeOrigin"], "foreign");
    assert_eq!(metrics["storeMachineId"], OLD);
    assert_eq!(metrics["daemonMachineId"], NEW);
    drop(held);
    f.stop().await;
}

#[tokio::test]
async fn foreign_store_where_many_watch_and_role_never_hold_here() {
    let f = Fixture::new(Some(OLD), Some(NEW)).await;
    let where_ = result(
        f.call("agent.where", json!({"agent_id":f.id}), DIRECT)
            .await,
    );
    assert_eq!(where_["known"], false);
    assert_ne!(where_["status"], "held_here");
    let many = result(
        f.call("agent.where_many", json!({"agentIds":[f.id]}), DIRECT)
            .await,
    );
    assert_eq!(many["agents"][0]["known"], false);
    assert_ne!(many["agents"][0]["status"], "held_here");
    let role = result(f.call("role.read", json!({"role":"leader"}), DIRECT).await);
    assert_eq!(role["known"], false);
    assert_eq!(role["held_here"], false);
    result(
        f.call("role.read", json!({"role":"new_role"}), DIRECT)
            .await,
    );
    assert_eq!(
        f.conn()
            .query_row("SELECT count(*) FROM role_cache", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        1,
        "foreign reads must not enroll roles"
    );
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    let mut claim = store.cached_agent_claim(&f.id).unwrap().unwrap();
    claim.version += 1;
    f.handler
        .watches
        .cache_claims(
            &store,
            &[(
                f.id.clone(),
                entorhinal_core::claim_cache::ClaimObservation::Present {
                    claim,
                    self_author: OLD.into(),
                },
            )],
            4,
        )
        .unwrap();
    let watch = Watch::default();
    let body = serde_json::to_vec(&json!({"method":watches::WHERE,"params":{"agentIds":[f.id],"cursor":{"incarnation":"origin","seq":0}}})).unwrap();
    assert!(matches!(
        f.handler.serve_watch(&watch, &body, DIRECT).await,
        HandlerOutcome::Streamed
    ));
    let encoded = {
        let frames = watch.frames.lock().unwrap();
        assert!(!frames.is_empty());
        frames[0].to_string()
    };
    assert!(!encoded.contains("held_here"));
    assert!(encoded.contains("\"known\":false"), "{encoded}");
    drop(store);
    f.stop().await;
}

#[tokio::test]
async fn adopt_store_confirmed_clears_self_identity_and_readmits_writes() {
    let mut f = Fixture::new(Some(OLD), Some(NEW)).await;
    let confirm = Arc::new(Confirm {
        declined: false,
        summaries: Mutex::default(),
    });
    f.handler.operator_confirmer = confirm.clone();
    result(f.call(ADOPT, json!({}), DIRECT).await);
    assert_eq!(
        f.conn()
            .query_row("SELECT count(*) FROM self_identity", [], |r| r
                .get::<_, i64>(0))
            .unwrap(),
        0,
        "adoption clears the old author"
    );
    assert!(f.handler.admit("register", DIRECT).is_ok());
    assert_eq!(
        f.handler.health().await.metrics.unwrap()["storeOrigin"],
        "this_machine"
    );
    assert!(confirm.summaries.lock().unwrap()[0].contains("keep projects"));
    assert!(f.handler.with_store(|s| s.verify()).unwrap().ok);
    refused(f.call(ADOPT, json!({}), DIRECT).await, "store_not_foreign");
    refused(
        f.call(ADOPT, json!({"same_machine":true}), DIRECT).await,
        "store_not_foreign",
    );
    f.stop().await;
}

#[tokio::test]
async fn adopt_store_declined_and_non_direct_change_nothing_same_machine_keeps_author() {
    let mut f = Fixture::new(Some(OLD), Some(NEW)).await;
    let before = f.handler.with_store(RegistryStore::generation).unwrap();
    f.handler.operator_confirmer = Arc::new(Confirm {
        declined: true,
        summaries: Mutex::default(),
    });
    refused(f.call(ADOPT, json!({}), CORE).await, "caller_forbidden");
    refused(f.call(ADOPT, json!({}), DIRECT).await, "operator_declined");
    assert_eq!(
        f.handler.with_store(RegistryStore::generation).unwrap(),
        before
    );
    assert!(f.handler.health.origin.is_foreign());
    assert_eq!(
        f.handler
            .with_store(RegistryStore::persisted_self_author)
            .unwrap()
            .as_deref(),
        Some(OLD)
    );
    let confirm = Arc::new(Confirm {
        declined: false,
        summaries: Mutex::default(),
    });
    f.handler.operator_confirmer = confirm.clone();
    result(f.call(ADOPT, json!({"same_machine":true}), DIRECT).await);
    assert!(confirm.summaries.lock().unwrap()[0].contains("nothing is cleared"));
    assert_eq!(
        f.handler
            .with_store(RegistryStore::persisted_self_author)
            .unwrap()
            .as_deref(),
        Some(OLD)
    );
    assert_eq!(
        f.handler.with_store(RegistryStore::generation).unwrap(),
        before
    );
    f.stop().await;
}

#[tokio::test]
async fn foreign_store_liveness_feed_updates_only_its_memory_mirror() {
    let f = Fixture::new(Some(OLD), Some(NEW)).await;
    let key = (72, 1);
    f.handler.route_admissions().insert(
        key,
        RouteAdmission::from_bind(
            Some(Principal::Reserved {
                module_id: "host".into(),
            }),
            None,
        ),
    );
    let providers = sessions::ResolvedProviders {
        host: sessions::ProviderResolution::Provider("host".into()),
        orchestrator: sessions::ProviderResolution::Unprovided,
    };
    let generation = f.handler.with_store(RegistryStore::generation).unwrap();
    let bytes = f.handler.execute_with(WireRequest{method:"projects.session_liveness".into(),params:json!({"seq":1,"snapshot":true,"sessions":[{"sessionId":"session","canonicalRoot":"/tmp/project","state":"active"}]})},key,Some(&providers)).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["tracked"],
        1
    );
    assert_eq!(
        f.handler.health.liveness_sessions.load(Ordering::Relaxed),
        1
    );
    assert_eq!(
        f.handler.with_store(RegistryStore::generation).unwrap(),
        generation
    );
    f.stop().await;
}

struct ServedScratch {
    handler: Arc<ProjectsHandler>,
    descriptor: StorageDescriptor,
}
#[async_trait]
impl ModuleHandler for ServedScratch {
    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        self.handler.handle(ctx, body).await
    }
    async fn on_hello_ack(&self, ack: &ModuleHelloAckBody) {
        let ack = ModuleHelloAckBody {
            storage: Some(serde_json::to_value(&self.descriptor).unwrap()),
            machine_id: Some(serde_json::from_value(json!(NEW)).unwrap()),
            ..ack.clone()
        };
        self.handler.on_hello_ack(&ack).await;
    }
    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        self.handler.on_bind(request).await
    }
    async fn on_route_gone(&self, handle: &RouteHandle) {
        self.handler.on_route_gone(handle).await;
    }
    async fn health(&self) -> HealthReport {
        self.handler.health().await
    }
}

#[tokio::test]
async fn adopt_store_uses_real_sdk_confirmation_on_fake_daemon() {
    use subc_client_rs::test_support::{
        BindIdentity, FrameType, ModuleHarness, OperatorAnswer, OperatorRefusal, RouteTarget,
    };
    for confirmed in [false, true] {
        let Fixture {
            mut handler,
            descriptor,
            dir,
            ..
        } = Fixture::new(Some(OLD), Some(NEW)).await;
        handler.store.lock().unwrap().take();
        let confirmer = Arc::new(agent_ops::DaemonConfirmer::new(
            handler.route_admissions.clone(),
        ));
        handler.operator_confirmer = confirmer.clone();
        let handler = Arc::new(handler);
        let observed = handler.clone();
        let answer = if confirmed {
            OperatorAnswer::Confirmed
        } else {
            OperatorAnswer::Refused {
                refusal: OperatorRefusal::Declined,
                reason: "declined".into(),
            }
        };
        let mut module = ModuleHarness::start(
            move |handle| {
                confirmer.set_module_handle(handle);
                ServedScratch {
                    handler,
                    descriptor,
                }
            },
            [answer],
        )
        .await;
        let route = module
            .bind_route(
                RouteBindRequest::new(
                    RouteHandle::detached(DIRECT.0, DIRECT.1),
                    RouteTarget::ManagementSurface {
                        module_id: MODULE_ID.into(),
                    },
                    BindIdentity::new("/scratch", "test", "ck-projects"),
                )
                .with_principal(Principal::Direct),
            )
            .await;
        let corr = module
            .send_request(
                route,
                serde_json::to_vec(&json!({"method":ADOPT,"params":{}})).unwrap(),
            )
            .await;
        let frame = module.read_reply(route, corr).await;
        assert_eq!(
            frame.header.ty,
            if confirmed {
                FrameType::Response
            } else {
                FrameType::Error
            }
        );
        let requests: Vec<_> = module.confirm_requests().collect();
        assert_eq!(requests.len(), 1);
        assert!(requests[0].1.summary.contains(OLD) && requests[0].1.summary.contains(NEW));
        assert!(requests[0].1.summary.chars().count() <= 200);
        assert_eq!(
            (requests[0].1.route_channel, requests[0].1.route_epoch),
            DIRECT
        );
        assert_eq!(observed.health.origin.is_foreign(), !confirmed);
        assert_eq!(
            observed
                .with_store(RegistryStore::persisted_self_author)
                .unwrap()
                .is_some(),
            !confirmed
        );
        drop(module);
        crate::tests::stop_store_tasks(&observed).await;
        // The harness's serve task owns another reference to this handler and
        // may outlive `drop(module)`, keeping the store's slot (and so its
        // SQLite files) open. Release the slot explicitly: Windows refuses to
        // delete a folder whose files are still open.
        observed.store.lock().unwrap().take();
        drop(observed);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

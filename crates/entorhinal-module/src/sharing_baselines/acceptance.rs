use super::{reply_exceptions::*, *};
use crate::agent_ops::{OperatorConfirmError, OperatorConfirmer};
use crate::watches::WatchTransport;
use tokio::sync::mpsc;

#[derive(Default)]
struct PromptCounter(AtomicU64);

#[async_trait::async_trait]
impl OperatorConfirmer for PromptCounter {
    async fn confirm_operator(&self, _: &str, _: RouteKey) -> Result<(), OperatorConfirmError> {
        self.0.fetch_add(1, Ordering::Relaxed);
        Err(OperatorConfirmError::Declined)
    }
}

async fn assert_take_refuses(handler: &ProjectsHandler, file: &StoreFile, refusal: &str) {
    let input = json!({"role":"connector_home","expected_epoch":0,
        "request_id":"11".repeat(16),"reason":"run coordinator"});
    let reader = file.read();
    let version = || rows(&reader, "PRAGMA data_version");
    let before = version();
    let take = |route, params| async move {
        let wire = serde_json::to_vec(&json!({"method":"role.take","params":params})).unwrap();
        match handler.handle_served_request(&wire, route).await {
            HandlerOutcome::Error { code, .. } => code,
            other => panic!("expected role.take refusal, got {other:?}"),
        }
    };
    assert_eq!(take(SESSIONS, input.clone()).await, "caller_forbidden");
    for (field, bad) in [
        ("role", json!("Connector")),
        ("expected_epoch", json!(-1)),
        ("request_id", json!("not-a-request-id")),
    ] {
        let mut malformed = input.clone();
        malformed[field] = bad;
        assert_eq!(take(DIRECT, malformed).await, "invalid_request", "{field}");
    }
    assert_eq!(take(DIRECT, input).await, refusal);
    assert_eq!(version(), before, "a refused take committed to the store");
}

#[tokio::test]
async fn sharing_baseline_migrated_stores_serve_sharing_off_and_before_selection() {
    for (name, state, base, refusal) in [
        ("disabled", "disabled", None, "identity_log_disabled"),
        ("native-installed", "enabled", Some(0), "hub_not_ready"),
    ] {
        let file = StoreFile::copy(&artifacts().join(format!("{name}.db")));
        let store = migrate_history(&file, name);
        assert_eq!(store.identity_log_status().unwrap().state, state);
        assert_eq!(store.hub_log_base().unwrap(), base);

        let prompts = Arc::new(PromptCounter::default());
        let handler = ProjectsHandler::with_runtime(BOX_A.into(), || NOW)
            .with_operator_confirmer(prompts.clone());
        install_handler(&handler, store);
        assert_eq!(handler.hub_backend_selected(), base.is_some());
        force_membership(&handler, false);
        handler.catalog_sql.wait().await;
        let disabled = state == "disabled";
        let result = |wire: String| {
            serde_json::from_str::<Value>(&wire)
                .unwrap()
                .get("result")
                .unwrap_or_else(|| panic!("expected success, got {wire}"))
                .clone()
        };
        assert_eq!(
            result(call(&handler, "agent.where", json!({"agent_id":BASELINE_AGENT})).await),
            json!({"cut_id":null,"epoch":0,"holder":null,"holder_name":null,
                "known":disabled,"read_at_ms":null,"stale":!disabled,
                "status":"unclaimed"})
        );
        for role in if disabled {
            &["connector_home", "enrolled", "local_role"][..]
        } else {
            &["connector_home", "enrolled"][..]
        } {
            let params = json!({"role":role});
            assert_eq!(
                result(call(&handler, "role.read", params.clone()).await),
                unobserved_role(role)
            );
            assert_eq!(
                result(call(&handler, "role.fence", params).await),
                unobserved_fence(role)
            );
        }
        assert_take_refuses(&handler, &file, refusal).await;
        assert_eq!(prompts.0.load(Ordering::Relaxed), 0, "{name}");

        if disabled {
            assert_local_lease(handler).await;
        } else {
            tests::stop_store_tasks(&handler).await;
        }
    }
}

struct LeaseSink(mpsc::UnboundedSender<Vec<u8>>);

#[async_trait::async_trait]
impl WatchTransport for LeaseSink {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        self.0.send(body).unwrap();
        Ok(())
    }

    async fn ended(&self) {
        std::future::pending().await
    }
}

async fn assert_local_lease(handler: ProjectsHandler) {
    let route = (73, 4);
    handler.route_admissions().insert(
        route,
        RouteAdmission::from_bind(Some(tests::reserved("plexus")), None),
    );
    let role = handler
        .admit_role_lease_watch(json!({"role":"connector_home"}), route)
        .unwrap();
    let (send, mut recv) = mpsc::unbounded_channel();
    let sink = LeaseSink(send);
    let bytes = tokio::select! {
        result = handler.serve_role_lease(&sink, role, route) => panic!("lease ended early: {result:?}"),
        notice = tokio::time::timeout(Duration::from_secs(5), recv.recv()) => notice.unwrap().unwrap(),
    };
    let lease: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(lease["holding"], true, "{lease}");
    assert_eq!(
        lease["gen"], 14,
        "grant must advance the preserved high-water mark"
    );
    tests::stop_store_tasks(&handler).await;
}

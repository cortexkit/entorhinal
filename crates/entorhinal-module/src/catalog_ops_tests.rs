use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    fake_daemon::{DaemonSnapshot, FakeDaemon},
    tests::{flow_stamp, reserved, scratch_descriptor},
    RouteAdmission,
};
use async_trait::async_trait;
use std::sync::{Arc, Mutex};
use subc_client_rs::HandlerOutcome;
const ROUTE: RouteKey = (180, 1);
const KEY: &str = "abababababababababababababababababababababababababababababababab";
struct Prompt {
    answer: Result<(), OperatorConfirmError>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.calls.lock().unwrap().push(summary.into());
        self.answer
    }
}
struct Fixture {
    root: std::path::PathBuf,
    handler: ProjectsHandler,
    prompt: Arc<Prompt>,
}
impl Fixture {
    fn new(answer: Result<(), OperatorConfirmError>) -> Self {
        let (root, descriptor) = scratch_descriptor("catalog-ops");
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        let prompt = Arc::new(Prompt {
            answer,
            calls: Mutex::new(vec![]),
        });
        let mut handler =
            ProjectsHandler::with_runtime("catalog-test".into(), || 1_767_225_600_999);
        handler.operator_confirmer = prompt.clone();
        *handler.store.lock().unwrap() = Some(store);
        handler.route_admissions().insert(
            ROUTE,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        Self {
            root,
            handler,
            prompt,
        }
    }
    fn store(&self) -> entorhinal_core::RegistryStore {
        self.handler.with_store(|s| Ok(s.clone())).unwrap()
    }
    async fn call(&self, method: &str, params: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                ROUTE,
            )
            .await
    }
    fn bind(&self, principal: Option<Principal>, flow: bool) {
        let stamp = flow.then(|| flow_stamp(Some("flow_1")));
        self.handler
            .route_admissions()
            .insert(ROUTE, RouteAdmission::from_bind(principal, stamp.as_ref()));
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn code(outcome: HandlerOutcome) -> String {
    match outcome {
        HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => code,
        other => panic!("expected refusal, got {other:?}"),
    }
}
fn result(outcome: HandlerOutcome) -> Value {
    match outcome {
        HandlerOutcome::Response(bytes) => {
            serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
        }
        other => panic!("expected response, got {other:?}"),
    }
}
fn add() -> Value {
    json!({"url":"https://a.test","scopes":["@a"],"key_id":"k1","public_key":KEY})
}
#[tokio::test(start_paused = true)]
async fn registry_direct_flow_decline_and_sync_guards() {
    let f = Fixture::new(Err(OperatorConfirmError::Declined));
    for principal in [
        None,
        Some(reserved("prefrontal-core")),
        Some(Principal::Unverified),
    ] {
        f.bind(principal, false);
        for method in WRITES {
            assert_eq!(code(f.call(method, json!({})).await), "write_not_permitted");
        }
    }
    f.bind(Some(Principal::Direct), true);
    for method in WRITES {
        assert_eq!(
            code(f.call(method, json!({})).await),
            "flow_scope_not_admitted"
        );
    }
    f.bind(Some(Principal::Direct), false);
    assert_eq!(
        code(f.handler.handle_request(
            &serde_json::to_vec(&json!({"method":ADD,"params":add()})).unwrap(),
            ROUTE
        )),
        "served_path_required"
    );
    assert_eq!(code(f.call(ADD, add()).await), "operator_declined");
    assert!(f.store().catalog_registries().unwrap().is_empty());
    f.store()
        .catalog_registry_add("https://a.test", &["@a".into()], false, "k1", KEY, 0)
        .unwrap();
    assert_eq!(
        code(
            f.call(
                REPIN,
                json!({"url":"https://a.test","key_id":"k2","public_key":"cd".repeat(32)})
            )
            .await
        ),
        "operator_declined"
    );
    assert_eq!(
        code(f.call(REMOVE, json!({"url":"https://a.test"})).await),
        "operator_declined"
    );
    assert_eq!(f.store().catalog_registries().unwrap()[0].keys.len(), 1);
    assert_eq!(f.store().generation().unwrap(), 0);
    assert_eq!(f.prompt.calls.lock().unwrap().len(), 3);
}
#[tokio::test(start_paused = true)]
async fn registry_confirmed_lifecycle_and_prompts() {
    let f = Fixture::new(Ok(()));
    result(f.call(ADD, add()).await);
    let summary = f.prompt.calls.lock().unwrap()[0].clone();
    assert!(
        summary.contains("https://a.test")
            && summary.contains("k1")
            && summary.contains(&key_fingerprint(KEY).unwrap())
    );
    assert!(summary.chars().count() <= 200);
    result(
        f.call(
            REPIN,
            json!({"url":"https://a.test","key_id":"k2","public_key":"cd".repeat(32)}),
        )
        .await,
    );
    let registry = &f.store().catalog_registries().unwrap()[0];
    assert_eq!(registry.keys.len(), 2);
    assert!(registry.keys[0].valid_from.is_none());
    assert_eq!(
        registry.keys[1].valid_from.as_deref(),
        Some("2026-01-01T00:00:00Z")
    );
    result(f.call(REPIN,json!({"url":"https://a.test","key_id":"k3","public_key":"ef".repeat(32),"valid_from":"2026-01-02T00:00:00Z"})).await);
    assert_eq!(
        f.store().catalog_registries().unwrap()[0].keys[2]
            .valid_from
            .as_deref(),
        Some("2026-01-02T00:00:00Z")
    );
    assert_eq!(
        result(f.call(LIST, json!({})).await)["registries"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    result(f.call(REMOVE, json!({"url":"https://a.test"})).await);
    assert!(f
        .prompt
        .calls
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .contains("mappings and held state"));
    assert!(f.store().catalog_registries().unwrap().is_empty());
    assert_eq!(f.store().generation().unwrap(), 0);
}
#[test]
fn repin_whole_second_boundaries() {
    assert_eq!(
        clock_second(1_767_225_599_999).unwrap(),
        "2025-12-31T23:59:59Z"
    );
    assert_eq!(
        clock_second(1_767_225_600_000).unwrap(),
        "2026-01-01T00:00:00Z"
    );
    assert_eq!(
        clock_second(1_767_225_600_999).unwrap(),
        "2026-01-01T00:00:00Z"
    );
    for good in ["2026-01-01T00:00:00Z", "2024-02-29T23:59:59Z"] {
        whole_second(good).unwrap();
    }
    for bad in [
        "2026-01-01T00:00:00.000Z",
        "2026-01-01T00:00:00+00:00",
        "2026-02-29T00:00:00Z",
        "2026-01-01T00:00:00z",
    ] {
        assert!(whole_second(bad).is_err(), "{bad}");
    }
}
#[tokio::test(start_paused = true)]
async fn registry_invalid_inputs_write_nothing() {
    let f = Fixture::new(Ok(()));
    for p in [
        json!({"url":"http://a.test","scopes":["@a"],"key_id":"k","public_key":KEY}),
        json!({"url":"https://a.test","scopes":["@A"],"key_id":"k","public_key":KEY}),
        json!({"url":"https://a.test","default":true,"scopes":["@a"],"key_id":"k","public_key":KEY}),
        json!({"url":"https://a.test","default":true,"key_id":"x".repeat(200),"public_key":KEY}),
        json!({"url":"https://a.test","default":true,"key_id":"k","public_key":"xx".repeat(32)}),
    ] {
        assert!(matches!(
            f.call(ADD, p).await,
            HandlerOutcome::Error { .. } | HandlerOutcome::ErrorWithDetail { .. }
        ));
    }
    assert!(f.prompt.calls.lock().unwrap().is_empty());
    assert!(f.store().catalog_registries().unwrap().is_empty());
}
#[tokio::test(start_paused = true)]
async fn read_blob_requires_daemon_consumer_capability() {
    let mut f = Fixture::new(Ok(()));
    let bytes = b"catalog package bytes";
    let digest = BlobStore::new(&f.root).unwrap().publish(bytes).unwrap();
    let request = json!({"sha256":digest,"offset":0,"length":1048576});
    for principal in [None, Some(Principal::Direct), Some(Principal::Unverified)] {
        f.bind(principal, false);
        assert_eq!(
            code(f.call(READ_BLOB, request.clone()).await),
            "catalog_consumer_required"
        );
    }
    f.bind(Some(reserved("consumer")), false);
    for capabilities in [
        json!({"provides":[],"requires":[]}),
        json!({"provides":["ck-catalog-consumer/v1"],"requires":[]}),
        json!({"provides":["ck-catalog-consumer-skill/v10"],"requires":[]}),
    ] {
        f.handler.participant_discovery = Arc::new(FakeDaemon::new(DaemonSnapshot::new(
            1,
            vec![],
            vec![
                json!({"module_id":"consumer","ready":true,"roles":[],"control_ops":[],"capabilities":capabilities}),
            ],
        )));
        assert_eq!(
            code(f.call(READ_BLOB, request.clone()).await),
            "catalog_consumer_required"
        );
    }
    for capability in crate::catalog_consumer::CAPABILITIES {
        f.handler.participant_discovery = Arc::new(FakeDaemon::new(DaemonSnapshot::new(
            1,
            vec![],
            vec![
                json!({"module_id":"consumer","ready":true,"roles":[],"control_ops":[],"capabilities":{"provides":[capability],"requires":[]}}),
            ],
        )));
        let reply = result(f.call(READ_BLOB, request.clone()).await);
        use base64::Engine;
        assert_eq!(
            base64::engine::general_purpose::STANDARD
                .decode(reply["bytes"].as_str().unwrap())
                .unwrap(),
            bytes
        );
        assert_eq!(reply["total"], bytes.len());
    }
    assert_eq!(f.store().generation().unwrap(), 0);
    for name in ["store.db", "store.db-wal"] {
        if let Ok(database) = std::fs::read(f.root.join(name)) {
            assert!(
                !database.windows(bytes.len()).any(|window| window == bytes),
                "package bytes in {name}"
            );
        }
    }
}

#[tokio::test(start_paused = true)]
async fn confirmation_exclusion_survives_dropped_catalog_write() {
    let mut f = Fixture::new(Ok(()));
    let entered = Arc::new(tokio::sync::Notify::new());
    let (release, wait) = std::sync::mpsc::channel();
    let wait = Mutex::new(wait);
    let signal = entered.clone();
    f.handler.shared_commit_hook = Some(Arc::new(move || {
        signal.notify_one();
        wait.lock().unwrap().recv().unwrap();
    }));
    let mut request = Box::pin(f.call(ADD, add()));
    tokio::select! {
        _ = entered.notified() => {},
        outcome = &mut request => panic!("write completed before release: {outcome:?}"),
    }
    drop(request);
    let second = ConfirmationGuard::take(&f.handler.confirmation_slot, ROUTE);
    release.send(()).unwrap();
    let _writer = f.handler.writer.lock().await;
    assert!(matches!(second,Err(error) if error.code == "operator_confirmation_busy"));
    assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
    assert!(f.handler.confirmation_slot.lock().unwrap().is_none());
    assert_eq!(f.store().catalog_registries().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn repin_retirement_boundary_uses_persisted_whole_second() {
    use entorhinal_catalog::status::{apply_status, Pin, StatusKey};
    let vectors: Value = serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/status.json"
    ))
    .unwrap();
    let sample = &vectors["records"][0];
    let pin: Pin = serde_json::from_value(sample["record"].clone()).unwrap();
    for (cutoff, accept) in [
        ("2026-10-14T08:59:59Z", false),
        ("2026-10-14T09:00:00Z", false),
        ("2026-10-14T09:00:01Z", true),
    ] {
        let f = Fixture::new(Ok(()));
        f.store()
            .catalog_registry_add(
                "https://a.test",
                &["@example".into()],
                false,
                "test-registry-1",
                vectors["registry_keys"]["test-registry-1"]["public_key_hex"]
                    .as_str()
                    .unwrap(),
                0,
            )
            .unwrap();
        result(f.call(REPIN,json!({"url":"https://a.test","key_id":"replacement","public_key":KEY,"valid_from":cutoff})).await);
        let chain = f.store().catalog_registries().unwrap()[0]
            .keys
            .iter()
            .map(|key| StatusKey {
                key_id: key.key_id.clone(),
                public_key: key.public_key.clone(),
                valid_from: key.valid_from.clone(),
            })
            .collect::<Vec<_>>();
        let applied = apply_status(
            None,
            sample["canonical"].as_str().unwrap(),
            sample["signature"].as_str().unwrap(),
            &chain,
            &pin,
        );
        if accept {
            assert!(applied.is_ok());
        } else {
            assert_eq!(applied.unwrap_err().code.as_str(), "key_not_valid");
        }
    }
}

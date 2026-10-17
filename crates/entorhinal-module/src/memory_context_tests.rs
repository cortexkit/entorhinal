use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    tests::{flow_stamp, reserved},
    RouteAdmission, RouteKey, WRITER_MODULE,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;
const DIRECT: RouteKey = (501, 1);
const CORE: RouteKey = (502, 1);
#[derive(Default)]
struct Prompt {
    summaries: Mutex<Vec<String>>,
    decline: bool,
}
#[async_trait::async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.summaries.lock().unwrap().push(summary.into());
        if self.decline {
            Err(OperatorConfirmError::Declined)
        } else {
            Ok(())
        }
    }
}
struct Fixture {
    handler: ProjectsHandler,
    dir: std::path::PathBuf,
    prompt: Arc<Prompt>,
}
impl Fixture {
    fn new(decline: bool) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor("memory-context");
        let prompt = Arc::new(Prompt {
            decline,
            ..Default::default()
        });
        let handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700)
            .with_operator_confirmer(prompt.clone());
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        store
            .register(entorhinal_core::RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        *handler.store.lock().unwrap() = Some(store);
        for (key, principal) in [(DIRECT, Principal::Direct), (CORE, reserved(WRITER_MODULE))] {
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
        }
        Self {
            handler,
            dir,
            prompt,
        }
    }
    async fn call(&self, key: RouteKey, op: &str, params: Value) -> Result<Value, String> {
        match self
            .handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":op,"params":params})).unwrap(),
                key,
            )
            .await
        {
            HandlerOutcome::Response(bytes) => {
                Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
            }
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                Err(code)
            }
            _ => panic!("unexpected handler outcome"),
        }
    }
    fn count(&self) -> usize {
        self.prompt.summaries.lock().unwrap().len()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}
fn document(revision: i64) -> Value {
    json!({"workspace_id":"W","expected_revision":revision,"charter":"Purpose","document":"Policy","request_key":format!("doc-{revision}")})
}
#[tokio::test]
async fn memory_direct_domain_and_document_writes_require_touch_id() {
    for decline in [true, false] {
        let f = Fixture::new(decline);
        let req = json!({"project_id":"P","domain":"@example/software","request_key":"domain"});
        let result = f.call(DIRECT, "project.set_domain", req.clone()).await;
        assert_eq!(f.count(), 1);
        assert_eq!(
            f.prompt.summaries.lock().unwrap()[0],
            "set domain of project Project to @example/software"
        );
        if decline {
            assert_eq!(result.unwrap_err(), "operator_declined");
        } else {
            assert_eq!(result.unwrap()["domain"], "@example/software");
            f.call(DIRECT, "project.set_domain", req).await.unwrap();
            assert_eq!(f.count(), 1);
        }
        let result = f.call(DIRECT, "workspace.set_document", document(0)).await;
        assert_eq!(f.count(), 2);
        let summary = f.prompt.summaries.lock().unwrap()[1].clone();
        assert!(
            summary.contains("workspace W revision 1 (charter 7 bytes, document 6 bytes, sha256 ")
        );
        assert!(summary.chars().count() <= 200);
        if decline {
            assert_eq!(result.unwrap_err(), "operator_declined");
        } else {
            let accepted = result.unwrap();
            assert_eq!(accepted["revision"], 1);
            assert_eq!(accepted["accepted_via"], "direct");
            f.call(DIRECT, "workspace.set_document", document(0))
                .await
                .unwrap();
            assert_eq!(f.count(), 2);
            let mut stale = document(0);
            stale["request_key"] = json!("stale");
            assert_eq!(
                f.call(DIRECT, "workspace.set_document", stale)
                    .await
                    .unwrap_err(),
                "revision_conflict"
            );
            assert_eq!(f.count(), 2);
            assert!(f.handler.with_store(|s| Ok(s.verify()?.ok)).unwrap());
        }
    }
}
#[tokio::test]
async fn memory_documents_refuse_every_non_direct_principal_and_flow_before_decode() {
    let f = Fixture::new(false);
    for (n, principal) in [
        None,
        Some(reserved(WRITER_MODULE)),
        Some(reserved("other")),
        Some(Principal::Direct),
    ]
    .into_iter()
    .enumerate()
    {
        let key = (510 + n as u16, 1);
        for flow in [false, true] {
            let stamp = flow_stamp(Some("flow"));
            f.handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal.clone(), flow.then_some(&stamp)),
            );
            let error = f
                .call(key, "workspace.set_document", Value::Null)
                .await
                .unwrap_err();
            assert_eq!(
                error,
                if flow {
                    "flow_scope_not_admitted"
                } else if matches!(principal, Some(Principal::Direct)) {
                    "invalid_params"
                } else {
                    "direct_caller_required"
                }
            );
            if flow {
                assert_eq!(
                    f.call(key, "project.set_domain", Value::Null)
                        .await
                        .unwrap_err(),
                    "flow_scope_not_admitted"
                );
            }
        }
    }
    assert_eq!(f.count(), 0);
    f.call(
        CORE,
        "project.set_domain",
        json!({"project_id":"P","domain":null,"request_key":"clear"}),
    )
    .await
    .unwrap();
    assert_eq!(f.count(), 0);
}
#[tokio::test]
async fn memory_validation_and_long_summaries_refuse_before_prompt() {
    let f = Fixture::new(false);
    for (field, value, code) in [
        ("document", json!("x".repeat(65537)), "document_too_large"),
        ("charter", json!("\0"), "document_invalid_text"),
        ("workspace_id", json!("unknown"), "workspace_unknown"),
    ] {
        let mut req = document(0);
        req[field] = value;
        assert_eq!(
            f.call(DIRECT, "workspace.set_document", req)
                .await
                .unwrap_err(),
            code
        );
    }
    f.handler
        .with_store(|s| {
            s.register(entorhinal_core::RegisterRequest {
                project_id: Some("P".into()),
                name: "x".repeat(200),
                ..Default::default()
            })
            .map(|_| ())
        })
        .unwrap();
    assert_eq!(
        f.call(
            DIRECT,
            "project.set_domain",
            json!({"project_id":"P","domain":"@example/software","request_key":"long"})
        )
        .await
        .unwrap_err(),
        "operator_summary_too_long"
    );
    assert_eq!(f.count(), 0);
}

#[tokio::test]
async fn memory_context_reads_are_open_to_every_principal_including_flows() {
    let f = Fixture::new(false);
    for (n, principal) in [
        None,
        Some(Principal::Direct),
        Some(reserved(WRITER_MODULE)),
        Some(reserved("other")),
    ]
    .into_iter()
    .enumerate()
    {
        let key = (600 + n as u16, 1);
        for flow in [false, true] {
            let stamp = flow_stamp(Some("read-flow"));
            f.handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal.clone(), flow.then_some(&stamp)),
            );
            let folder = crate::tests::canonical_test_path(&f.dir)
                .to_string_lossy()
                .into_owned();
            assert_eq!(
                f.call(key, "context.project", json!({"folder":folder}))
                    .await
                    .unwrap()["project_id"],
                Value::Null
            );
            assert_eq!(
                f.call(key, "workspace.document", json!({"workspace_id":"W"}))
                    .await
                    .unwrap_err(),
                "revision_unknown"
            );
            assert_eq!(
                f.call(key, "context.session", json!({"agent_id":"unknown"}))
                    .await
                    .unwrap_err(),
                "unknown_agent"
            );
        }
    }
    assert_eq!(f.count(), 0);
}

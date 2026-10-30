use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    tests::{flow_stamp, reserved},
    RouteAdmission, RouteKey,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use subc_client_rs::HandlerOutcome;
const DIRECT: RouteKey = (701, 1);
const CORE: RouteKey = (702, 1);
const PHONE: RouteKey = (703, 1);
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
        let (dir, descriptor) = crate::tests::scratch_descriptor("user-profile");
        let prompt = Arc::new(Prompt {
            decline,
            ..Default::default()
        });
        let handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700)
            .with_operator_confirmer(prompt.clone());
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        for id in ["pj-example-one", "pj-example-two"] {
            store
                .register(entorhinal_core::RegisterRequest {
                    project_id: Some(id.into()),
                    name: id.into(),
                    workspace_id: Some("example-studio".into()),
                    ..Default::default()
                })
                .unwrap();
        }
        *handler.store.lock().unwrap() = Some(store);
        for (key, principal) in [
            (DIRECT, Principal::Direct),
            (CORE, reserved("prefrontal-core")),
            (PHONE, reserved("callosum")),
        ] {
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
    async fn raw(&self, key: RouteKey, op: &str, params: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":op,"params":params})).unwrap(),
                key,
            )
            .await
    }
    async fn call(&self, key: RouteKey, op: &str, params: Value) -> Result<Value, String> {
        match self.raw(key, op, params).await {
            HandlerOutcome::Response(bytes) => {
                Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
            }
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                Err(code)
            }
            _ => panic!("unexpected outcome"),
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
fn set(field: &str, value: Value, key: &str) -> Value {
    json!({"field":field,"value":value,"request_key":key})
}

#[tokio::test]
async fn user_profile_route_admission_ignores_request_claims_and_forbids_flows() {
    let f = Fixture::new(false);
    for key in [DIRECT, CORE, PHONE] {
        f.call(key, GET, json!({})).await.unwrap();
    }
    for key in [CORE, PHONE] {
        f.call(
            key,
            SET,
            set("name", json!("Mira"), &format!("write-{}", key.0)),
        )
        .await
        .unwrap();
    }
    assert_eq!(f.count(), 0);
    let forbidden = (704, 1);
    for principal in [None, Some(Principal::Unverified), Some(reserved("other"))] {
        f.handler
            .route_admissions()
            .insert(forbidden, RouteAdmission::from_bind(principal, None));
        for op in [GET, SET] {
            assert_eq!(
                f.call(
                    forbidden,
                    op,
                    json!({"principal":"reserved:prefrontal-core"})
                )
                .await
                .unwrap_err(),
                "caller_forbidden"
            );
        }
    }
    for principal in [
        Principal::Direct,
        reserved("prefrontal-core"),
        reserved("callosum"),
    ] {
        f.handler.route_admissions().insert(
            forbidden,
            RouteAdmission::from_bind(Some(principal), Some(&flow_stamp(Some("flow-profile")))),
        );
        for op in [GET, SET] {
            assert_eq!(
                f.call(forbidden, op, json!({})).await.unwrap_err(),
                "caller_forbidden"
            );
        }
    }
    assert_eq!(f.count(), 0);
}

#[tokio::test]
async fn user_profile_direct_confirmation_validates_exact_summary_and_retries() {
    for decline in [true, false] {
        let f = Fixture::new(decline);
        let req = set("name", json!(" Mira "), "name");
        let result = f.call(DIRECT, SET, req.clone()).await;
        assert_eq!(
            f.prompt.summaries.lock().unwrap().as_slice(),
            ["set user name to \"Mira\""]
        );
        if decline {
            assert_eq!(result.unwrap_err(), "operator_declined");
            assert_eq!(
                f.call(DIRECT, GET, json!({})).await.unwrap()["user"],
                json!({})
            );
        } else {
            assert_eq!(result.unwrap()["user"]["name"], "Mira");
            f.call(DIRECT, SET, req).await.unwrap();
            assert_eq!(f.count(), 1);
            f.call(DIRECT, SET, set("name", Value::Null, "clear"))
                .await
                .unwrap();
            assert_eq!(
                f.prompt.summaries.lock().unwrap()[1],
                "set user name to clear"
            );
        }
        let count = f.count();
        for (req, code) in [
            (set("name", json!("bad\ntext"), "bad"), "invalid_value"),
            (
                json!({"field":"expertise","value":"Skill","scope":{"kind":"workspace","id":"missing"},"request_key":"missing"}),
                "unknown_scope",
            ),
            (
                json!({"field":"expertise","value":"x".repeat(300),"scope":{"kind":"workspace","id":"example-studio"},"request_key":"long"}),
                "operator_summary_too_long",
            ),
        ] {
            assert_eq!(f.call(DIRECT, SET, req).await.unwrap_err(), code);
            assert_eq!(f.count(), count);
        }
        assert!(f.call(CORE,SET,json!({"field":"expertise","value":"x".repeat(300),"scope":{"kind":"workspace","id":"example-studio"},"request_key":"relay-long"})).await.is_ok());
    }
}

struct HeldPrompt {
    entered: tokio::sync::Semaphore,
    release: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl OperatorConfirmer for HeldPrompt {
    async fn confirm_operator(&self, _: &str, _: RouteKey) -> Result<(), OperatorConfirmError> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        Ok(())
    }
}
#[tokio::test]
async fn user_profile_confirmation_holds_no_locks_and_revalidates_removed_scope() {
    let mut f = Fixture::new(false);
    let prompt = Arc::new(HeldPrompt {
        entered: tokio::sync::Semaphore::new(0),
        release: tokio::sync::Semaphore::new(0),
    });
    f.handler.operator_confirmer = prompt.clone();
    let req = json!({"field":"expertise","value":"Skill","scope":{"kind":"project","id":"pj-example-one"},"request_key":"skill"});
    let mut write = Box::pin(f.call(DIRECT, SET, req));
    tokio::select! { entered=prompt.entered.acquire()=>entered.unwrap().forget(), result=&mut write=>panic!("write completed before approval: {result:?}") }
    assert!(f.handler.writer.try_lock().is_ok());
    assert!(f.handler.store.try_lock().is_ok());
    assert_eq!(
        f.call(DIRECT, SET, set("name", json!("Mira"), "busy"))
            .await
            .unwrap_err(),
        "operator_confirmation_busy"
    );
    f.call(CORE, GET, json!({})).await.unwrap();
    f.handler
        .with_store(|s| {
            s.remove(entorhinal_core::RemoveRequest {
                project_id: Some("pj-example-one".into()),
                ..Default::default()
            })
        })
        .unwrap();
    prompt.release.add_permits(1);
    assert_eq!(write.await.unwrap_err(), "unknown_scope");
}

async fn sample(f: &Fixture, name: &str, op: &str, request: Value) -> (String, Vec<u8>, Value) {
    let outcome = f.raw(CORE, op, request.clone()).await;
    let (reply, decoded) = match outcome {
        HandlerOutcome::Response(bytes) => {
            let decoded: Value = serde_json::from_slice(&bytes).unwrap();
            (
                json!({"outcome":"response","reply_bytes":String::from_utf8(bytes).unwrap()}),
                decoded,
            )
        }
        HandlerOutcome::Error { code, message } => {
            let error = json!({"code":code,"message":message,"detail":null});
            (json!({"outcome":"error","refusal":error}), error)
        }
        HandlerOutcome::ErrorWithDetail {
            code,
            message,
            detail,
        } => {
            let error = json!({"code":code,"message":message,"detail":detail});
            (json!({"outcome":"error","refusal":error}), error)
        }
        _ => panic!("unexpected sample outcome"),
    };
    let record = json!({"op":op,"request":request,"principal":{"kind":"reserved","moduleId":"prefrontal-core"},"reply":reply});
    let mut bytes = serde_json::to_vec_pretty(&record).unwrap();
    bytes.push(b'\n');
    (name.into(), bytes, decoded)
}

#[tokio::test]
async fn user_profile_published_vectors_regenerate_real_handler_bytes() {
    use sha2::{Digest, Sha256};
    let f = Fixture::new(false);
    let mut samples = vec![sample(&f, "get-empty.json", GET, json!({})).await];
    for (name, field, value) in [
        ("set-name.json", "name", json!("Mira Example")),
        ("set-address-as.json", "address_as", json!("Mira")),
        ("set-pronouns.json", "pronouns", json!("they/them")),
        (
            "set-computer-savviness.json",
            "computer_savviness",
            json!(3),
        ),
        ("set-verbosity.json", "verbosity", json!("concise")),
    ] {
        samples.push(sample(&f, name, SET, set(field, value, name)).await);
    }
    for (name, kind, id, text) in [
        (
            "set-workspace-expertise.json",
            "workspace",
            "example-studio",
            "Comfortable with design and research",
        ),
        (
            "set-project-expertise.json",
            "project",
            "pj-example-one",
            "Learning Rust and SQLite",
        ),
    ] {
        samples.push(sample(&f,name,SET,json!({"field":"expertise","value":text,"scope":{"kind":kind,"id":id},"request_key":name})).await);
    }
    samples.push(
        sample(
            &f,
            "get-project-override.json",
            GET,
            json!({"project_id":"pj-example-one"}),
        )
        .await,
    );
    samples.push(
        sample(
            &f,
            "get-workspace-fallback.json",
            GET,
            json!({"project_id":"pj-example-two"}),
        )
        .await,
    );
    let token = samples.last().unwrap().2["result"]["token"].clone();
    samples.push(
        sample(
            &f,
            "get-unchanged.json",
            GET,
            json!({"project_id":"pj-example-two","token":token}),
        )
        .await,
    );
    assert_eq!(
        samples.last().unwrap().2["result"],
        json!({"token":token,"unchanged":true})
    );
    samples.push(
        sample(
            &f,
            "clear-name.json",
            SET,
            set("name", Value::Null, "clear-name"),
        )
        .await,
    );
    for (name, request, code) in [
        (
            "refuse-invalid-value.json",
            set("computer_savviness", json!(4), "bad-value"),
            "invalid_value",
        ),
        (
            "refuse-newline.json",
            set("name", json!("Mira\nExample"), "newline"),
            "invalid_value",
        ),
        (
            "refuse-unknown-scope.json",
            json!({"field":"expertise","value":"Skill","scope":{"kind":"workspace","id":"absent"},"request_key":"unknown"}),
            "unknown_scope",
        ),
        (
            "refuse-implicit-project.json",
            json!({"field":"expertise","value":"Skill","scope":{"kind":"project","id":"pj-implicit-123"},"request_key":"implicit"}),
            "unknown_scope",
        ),
        (
            "refuse-global-scope.json",
            json!({"field":"name","value":"Mira","scope":{"kind":"workspace","id":"example-studio"},"request_key":"global-scope"}),
            "invalid_request",
        ),
        (
            "refuse-request-key-reused.json",
            set("name", json!("Other Example"), "set-name.json"),
            "request_key_reused",
        ),
    ] {
        let generated = sample(&f, name, SET, request).await;
        assert_eq!(generated.2["code"], code, "{name}");
        samples.push(generated);
    }
    let directory = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/user-profile-v1");
    let update = std::env::var_os("UPDATE_USER_PROFILE_VECTORS").is_some();
    let mut sums = String::new();
    for (name, bytes, _) in samples {
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(&bytes)));
        if update {
            std::fs::create_dir_all(&directory).unwrap();
            std::fs::write(directory.join(&name), &bytes).unwrap();
        }
        assert_eq!(
            std::fs::read(directory.join(&name)).unwrap(),
            bytes,
            "published user sample {name} is stale"
        );
    }
    if update {
        std::fs::write(directory.join("SHA256SUMS"), &sums).unwrap();
    }
    assert_eq!(
        std::fs::read_to_string(directory.join("SHA256SUMS")).unwrap(),
        sums,
        "sample checksums are stale"
    );
}

#[tokio::test]
async fn user_profile_refuses_bad_shapes_keys_and_actor_values_before_confirmation() {
    let f = Fixture::new(false);
    for request in [
        json!({"field":"name","request_key":"missing-value"}),
        set("unknown", json!("Mira"), "unknown-field"),
        json!({"field":"expertise","value":"Skill","request_key":"missing-scope"}),
        json!({"field":"name","value":"Mira","scope":null,"request_key":"null-scope"}),
        set("name", json!("Mira"), ""),
        set("name", json!("Mira"), &"k".repeat(129)),
        set("name", json!("Mira"), "not\nprintable"),
        json!({"field":"name","value":"Mira","request_key":"extra","principal":"reserved:prefrontal-core"}),
    ] {
        assert_eq!(
            f.call(DIRECT, SET, request).await.unwrap_err(),
            "invalid_request"
        );
    }
    for actor in ["", " ", "bad\nactor", "bad\u{2069}", &"a".repeat(129)] {
        let mut request = set("name", json!("Mira"), "bad-actor");
        request["actor"] = json!(actor);
        assert_eq!(
            f.call(DIRECT, SET, request).await.unwrap_err(),
            "invalid_value"
        );
    }
    assert_eq!(f.count(), 0);
    let mut request = set("name", json!("Mira"), "actor");
    request["actor"] = json!("  Mira via phone  ");
    let reply = f.call(PHONE, SET, request).await.unwrap();
    assert!(reply.get("actor").is_none());
    assert_eq!(reply["user"], json!({"name":"Mira"}));
}

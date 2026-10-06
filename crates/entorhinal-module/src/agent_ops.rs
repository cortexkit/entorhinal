//! Serves agent identity mutations. Who may call them is decided in
//! `ProjectsHandler::admit`; this module only routes an admitted request to
//! `entorhinal-core`, which owns request decoding, request-key caching, the
//! one-time import that hands agent identity over from core, and every
//! identity rule.

use entorhinal_core::agent::AgentMutationError;
use serde_json::Value;

use super::{HandlerError, ProjectsHandler};

pub(super) const MUTATING_METHODS: &[&str] = &[
    "agent.create",
    "agent.rename",
    "agent.update_tag",
    "agent.set_labels",
    "agent.set_avatar",
    "agent.set_github_identity",
    "agent.dispose",
    "agent.merge",
    "agent.import",
];

impl From<AgentMutationError> for HandlerError {
    fn from(error: AgentMutationError) -> Self {
        Self {
            code: error.code,
            message: error.message,
            detail: error.detail,
        }
    }
}

impl ProjectsHandler {
    /// Called only after `admit`, including on retries. Delegate the body intact
    /// so core's deny_unknown_fields runs before its request-key cache: a retry
    /// cannot bypass decoding or restore a removed residence/persona field.
    pub(super) fn agent_mutation(
        &self,
        method: &str,
        params: Value,
        principal: &str,
    ) -> Result<Vec<u8>, HandlerError> {
        let result = (|| {
            let guard = self
                .store
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            let store = guard.as_ref().ok_or_else(|| {
                HandlerError::new("storage_unavailable", "agent identity storage is not ready")
            })?;
            let writer = store.with_principal(principal);
            let body = if method == "agent.import" {
                writer.agent_import(params, (self.clock)())
            } else {
                writer.agent_mutation(method, params, (self.clock)())
            }
            .map_err(HandlerError::from)?;
            // Cached generation belongs to the commit, but incarnation belongs
            // to the serving process. Never write the latter into the cache.
            let mut body: Value = serde_json::from_slice(&body)
                .map_err(|error| HandlerError::new("encode_failed", error.to_string()))?;
            body["result"]["incarnation"] = Value::String(self.incarnation.clone());
            serde_json::to_vec(&body)
                .map_err(|error| HandlerError::new("encode_failed", error.to_string()))
        })();
        self.record_mutation(result)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
    use entorhinal_core::{RegisterRequest, RegistryStore};
    use serde::Deserialize;
    use serde_json::json;
    use subc_client_rs::HandlerOutcome;
    use subc_protocol::{ErrorBody, Principal};

    use super::*;
    use crate::{
        tests::{flow_stamp, reserved},
        RouteAdmission, RouteKey, WRITER_MODULE,
    };

    const OPERATOR: RouteKey = (30, 1);
    const CALLER: RouteKey = (31, 1);
    const NOW: i64 = 700;
    const INCARNATION: &str = "0123456789abcdef";
    const A: &str = "agent_0000000000000001";
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    // Import sources are built with core's own migrations, copied byte for byte,
    // so the handler reads the schema a real core store has. The bad-row cases
    // here keep every foreign-key reference valid (see the fixture builder below).
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/
    // 076_agent_registry.sql, 080_agent_github_identity.sql, 082_wake_delivery.sql,
    // 109_agent_generation.sql, 112_agent_avatar.sql, 126_agent_labels.sql.
    const MIGRATIONS: &[&str] = &[
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/076_agent_registry.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/082_wake_delivery.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/109_agent_generation.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
    ];

    struct Fixture {
        root: PathBuf,
        source: Option<RegistryStore>,
        descriptor: StorageDescriptor,
        handler: ProjectsHandler,
    }

    impl Fixture {
        fn new() -> Self {
            let fixture_id = format!(
                "{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/module-agent-fixtures")
                .join(&fixture_id);
            std::fs::create_dir_all(&root).unwrap();
            let descriptor = |name: &str| StorageDescriptor {
                module_id: "entorhinal".into(),
                storage_namespace: format!("{fixture_id}-{name}"),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join(name).to_string_lossy().into_owned(),
                },
            };
            // Build the source through `RegistryStore`'s public API rather than
            // adding a SQLite dependency to this crate. That API keeps foreign keys
            // on, which is fine here: these tests prove the request path, and the
            // broken-reference cases live in entorhinal-core's agent_import tests,
            // which build sources with foreign keys off.
            let source = RegistryStore::open(&descriptor("snapshot.db")).unwrap();
            source.apply_entry("fixture", "{}", "fixture", None, |tx| {
                tx.execute_batch("PRAGMA defer_foreign_keys=ON; DROP TABLE agent_name_claim; DROP TABLE agent;")?;
                for migration in MIGRATIONS { tx.execute_batch(migration)?; }
                Ok(())
            }).unwrap();
            let descriptor = descriptor("store.db");
            let handler = ProjectsHandler::with_runtime(INCARNATION.into(), || NOW);
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            handler.route_admissions().insert(
                OPERATOR,
                RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), None),
            );
            Self {
                root,
                source: Some(source),
                descriptor,
                handler,
            }
        }

        fn source_sql(&self, sql: &str) {
            self.source
                .as_ref()
                .unwrap()
                .apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch(sql))
                .unwrap();
        }

        fn bind(&self, principal: Option<Principal>, flow: bool) {
            self.handler.route_admissions().remove(&CALLER);
            if principal.is_some() || flow {
                self.handler.route_admissions().insert(
                    CALLER,
                    RouteAdmission::from_bind(
                        principal,
                        flow.then(|| flow_stamp(Some("fl_test"))).as_ref(),
                    ),
                );
            }
        }

        fn head(&self) -> i64 {
            self.handler.with_store(|store| store.generation()).unwrap()
        }

        fn outcome(&self, route: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler.handle_request(
                &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                route,
            )
        }

        fn success(&self, method: &str, params: Value) -> Value {
            let body = match self.outcome(OPERATOR, method, params) {
                HandlerOutcome::Response(body) => body,
                HandlerOutcome::Error { code, message } => panic!("{method}: {code}: {message}"),
                HandlerOutcome::ErrorWithDetail {
                    code,
                    message,
                    detail,
                } => panic!("{method}: {code}: {message}: {detail}"),
                HandlerOutcome::Streamed => panic!("mutations must not stream"),
            };
            let value: Value = serde_json::from_slice(&body).unwrap();
            let result = value["result"].clone();
            assert_eq!(result["incarnation"], self.handler.incarnation);
            assert!(result["generation"].is_i64());
            assert!(result.get("noop").is_none());
            result
        }

        fn refusal(&self, route: RouteKey, method: &str, params: Value, code: &str) -> ErrorBody {
            let head = self.head();
            let error = wire_error(self.outcome(route, method, params));
            assert_eq!(error.code, code, "{method}: {}", error.message);
            assert_eq!(self.head(), head, "{method} refusal must append nothing");
            error
        }

        fn params(&self, method: &str, key: Option<&str>) -> Value {
            // Field names and casing are core's request bodies for the same op.
            // Entorhinal's differences: a required `request_key`, an optional
            // `actor`, and no caller or residence fields (core strips those).
            // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:548-732.
            let mut value = match method {
                "agent.create" => json!({"role":"assistant","name":"Alice","tag":"test"}),
                "agent.rename" => json!({"agent_id":A,"name":"Renamed"}),
                "agent.update_tag" => json!({"agent_id":A,"tag":"changed"}),
                "agent.set_labels" => json!({"agent_id":A,"labels":["One","Two"]}),
                "agent.set_avatar" => {
                    json!({"agentId":A,"genome":"a".repeat(2048),"type":"creature.classic","seedOnly":false})
                }
                "agent.set_github_identity" => json!({"agent_id":A,"github_identity":null}),
                "agent.dispose" => json!({"agent_id":A}),
                "agent.merge" => json!({"agent_id":A,"into_agent_id":"agent_0000000000000002"}),
                "agent.import" => json!({"snapshot_path":self.root.join("snapshot.db")}),
                _ => panic!("not a mutation: {method}"),
            };
            if let Some(key) = key {
                value["request_key"] = json!(key);
            }
            value
        }

        fn cut_over(&self) -> Value {
            self.success(
                "agent.import",
                self.params("agent.import", Some("committed-agent.import")),
            )
        }

        fn create(&self, name: &str, key: &str) -> String {
            let mut params = self.params("agent.create", Some(key));
            params["name"] = json!(name);
            self.success("agent.create", params)["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }

        fn restart(&mut self) {
            let previous = self.handler.store.lock().unwrap().take();
            drop(previous);
            self.handler = ProjectsHandler::with_runtime("fedcba9876543210".into(), || NOW + 1);
            *self.handler.store.lock().unwrap() =
                Some(RegistryStore::open(&self.descriptor).unwrap());
            self.handler.route_admissions().insert(
                OPERATOR,
                RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), None),
            );
        }

        fn register(&self) {
            self.handler
                .with_store(|store| {
                    store.register(RegisterRequest {
                        project_id: Some("project".into()),
                        workspace_id: Some("workspace".into()),
                        name: "Project".into(),
                        ..Default::default()
                    })
                })
                .unwrap();
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            self.source.take();
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    // Turn a handler outcome into the error body the SDK sends on the wire, the
    // same conversion its serve loop does, then decode that body independently.
    // A refusal's structured fields must arrive in `detail`; a test that only
    // inspected the in-process error could pass while the wire carried prose.
    // Source: subc-client-rs 0.26.1 src/lib.rs, ErrorWithDetail serve arm;
    // docs/designs/agent-identity-pinned-facts.md, "The pinned SDK".
    fn wire_error(outcome: HandlerOutcome) -> ErrorBody {
        let body = match outcome {
            HandlerOutcome::Error { code, message } => ErrorBody::new(code, message),
            HandlerOutcome::ErrorWithDetail {
                code,
                message,
                detail,
            } => ErrorBody::new(code, message).with_detail(detail),
            _ => panic!("expected a refusal, never a cached success"),
        };
        let bytes = serde_json::to_vec(&body).unwrap();
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Decoder {
            code: String,
            message: String,
            detail: Option<Value>,
        }
        let decoded: Decoder = serde_json::from_slice(&bytes).unwrap();
        ErrorBody {
            code: decoded.code,
            message: decoded.message,
            detail: decoded.detail,
        }
    }

    fn principals() -> Vec<Option<Principal>> {
        vec![
            Some(reserved(WRITER_MODULE)),
            Some(Principal::Direct),
            Some(reserved("callosum")),
            Some(reserved("aft")),
            Some(Principal::Unverified),
            None,
        ]
    }

    /// Commit a real key for every op; later admission tests must not read those
    /// cache entries, even when their bodies would otherwise decode or succeed.
    fn commit_every_mutation(f: &Fixture) -> Vec<(&'static str, Value, Value)> {
        let import = f.cut_over();
        assert_eq!(import["agents_imported"], 0);
        assert_eq!(import["claims_imported"], 0);
        let mut committed = vec![(
            "agent.import",
            f.params("agent.import", Some("committed-agent.import")),
            import,
        )];
        let mut create = f.params("agent.create", Some("committed-agent.create"));
        create["actor"] = json!("operator:test");
        let created = f.success("agent.create", create.clone());
        let id = created["agent"]["agent_id"].as_str().unwrap();
        committed.push(("agent.create", create, created.clone()));
        for method in [
            "agent.rename",
            "agent.update_tag",
            "agent.set_labels",
            "agent.set_avatar",
            "agent.set_github_identity",
            "agent.dispose",
        ] {
            let mut params = f.params(method, Some(&format!("committed-{method}")));
            params[if method == "agent.set_avatar" {
                "agentId"
            } else {
                "agent_id"
            }] = json!(id);
            let result = f.success(method, params.clone());
            committed.push((method, params, result));
        }
        let source = f.create("Merge source", "merge-source");
        let target = f.create("Merge target", "merge-target");
        let params =
            json!({"agent_id":source,"into_agent_id":target,"request_key":"committed-agent.merge"});
        let result = f.success("agent.merge", params.clone());
        committed.push(("agent.merge", params, result));
        assert_eq!(committed.len(), 9);
        committed
    }

    #[test]
    fn every_agent_mutation_is_flow_refused_for_every_principal_before_and_after_cutover() {
        let f = Fixture::new();
        let mut committed = Vec::new();
        for after in [false, true] {
            if after {
                committed = commit_every_mutation(&f);
            }
            for principal in principals() {
                f.bind(principal.clone(), true);
                for method in MUTATING_METHODS {
                    assert!(
                        crate::MUTATING_METHODS.contains(method),
                        "{method} must be flow checked"
                    );
                    let cached = committed
                        .iter()
                        .find(|(op, _, _)| op == method)
                        .map(|(_, params, _)| params.clone())
                        .unwrap_or_else(|| f.params(method, Some(&format!("committed-{method}"))));
                    for params in [
                        Value::Null,
                        f.params(method, None),
                        f.params(method, Some("fresh-flow")),
                        cached,
                    ] {
                        let error = f.refusal(CALLER, method, params, "flow_scope_not_admitted");
                        assert!(
                            error.detail.is_none(),
                            "{method} from {principal:?}, after={after}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn agent_principal_matrix_precedes_decode_keys_and_cached_replies_before_and_after_cutover() {
        let f = Fixture::new();
        for method in MUTATING_METHODS {
            if *method != "agent.import" {
                f.refusal(
                    OPERATOR,
                    method,
                    f.params(method, Some("before")),
                    "authority_not_cut_over",
                );
            }
        }
        let mut committed = Vec::new();
        for after in [false, true] {
            if after {
                committed = commit_every_mutation(&f);
            }
            for principal in principals() {
                f.bind(principal.clone(), false);
                for method in MUTATING_METHODS {
                    if principal == Some(reserved(WRITER_MODULE)) {
                        assert!(f.handler.admit(method, CALLER).is_ok());
                        continue;
                    }
                    let code = if principal == Some(Principal::Direct) {
                        "direct_identity_write_not_admitted"
                    } else {
                        "write_not_permitted"
                    };
                    if *method == "agent.import" {
                        let mut missing = f.params(method, Some("missing-snapshot"));
                        missing["snapshot_path"] = json!(f.root.join("missing.db"));
                        f.refusal(CALLER, method, missing, code);
                    }
                    let cached = committed
                        .iter()
                        .find(|(op, _, _)| op == method)
                        .map(|(_, params, _)| params.clone())
                        .unwrap_or_else(|| f.params(method, Some(&format!("committed-{method}"))));
                    for params in [
                        Value::Null,
                        f.params(method, None),
                        f.params(method, Some("fresh-principal")),
                        cached,
                    ] {
                        let error = f.refusal(CALLER, method, params, code);
                        assert!(error.detail.is_none());
                        assert!(error.message.contains(method));
                        if principal == Some(Principal::Direct) {
                            assert!(
                                error.message.contains("prefrontal-core")
                                    && error.message.contains("relay")
                            );
                        }
                    }
                }
            }
        }
        for (method, params, result) in committed {
            let head = f.head();
            assert_eq!(f.success(method, params), result);
            assert_eq!(f.head(), head, "operator retry of {method} is cached");
        }
    }

    #[test]
    fn strict_agent_bodies_and_request_keys_precede_cutover_and_cache() {
        let f = Fixture::new();
        for after in [false, true] {
            if after {
                f.cut_over();
            }
            for method in MUTATING_METHODS {
                for body in [Value::Null, json!([]), json!("bad"), json!({})] {
                    f.refusal(OPERATOR, method, body, "invalid_request");
                }
                for key in [None, Some("")] {
                    f.refusal(
                        OPERATOR,
                        method,
                        f.params(method, key),
                        "request_key_required",
                    );
                }
                let mut body = f.params(method, None);
                body["request_key"] = Value::Null;
                f.refusal(OPERATOR, method, body, "request_key_required");
                for field in ["request_key", "actor"] {
                    let mut body = f.params(method, Some("key"));
                    body[field] = json!(42);
                    f.refusal(OPERATOR, method, body, "invalid_request");
                }
                for field in [
                    "callerHarness",
                    "callerSession",
                    "harness",
                    "session_id",
                    "session",
                    "caller_directory",
                    "caller_session",
                    "persona_ref",
                    "residence",
                    "wake_policy",
                    "wake_policy_version",
                    "sleep",
                    "bounced_deliveries",
                    "residence_machine_id",
                    "origin",
                    "unknown",
                ] {
                    let mut body = f.params(method, Some("key"));
                    body[field] = Value::Null;
                    f.refusal(OPERATOR, method, body, "invalid_request");
                }
            }
        }
        let params = f.params("agent.create", Some("create-retry"));
        let created = f.success("agent.create", params.clone());
        let mut removed = params;
        removed["persona_ref"] = json!("not restored by a retry");
        f.refusal(OPERATOR, "agent.create", removed, "invalid_request");
        assert_eq!(created["agent"]["created_at"], NOW);

        for body in [b"not json".as_slice(), b"{}", b"{\"method\":42}"] {
            let head = f.head();
            let error = wire_error(f.handler.handle_request(body, OPERATOR));
            assert_eq!(error.code, "invalid_request");
            assert_eq!(f.head(), head);
        }
    }

    #[test]
    fn mutation_fields_pin_core_casing_labels_targets_and_all_four_create_roles() {
        let f = Fixture::new();
        f.register();
        f.cut_over();
        for (role, project, workspace) in [
            ("assistant", None, None),
            ("workspace_head", None, Some("workspace")),
            ("head", Some("project"), None),
            ("hiree", Some("project"), None),
        ] {
            let body = json!({"role":role,"name":role,"tag":"test","project_id":project,"workspace_id":workspace,"actor":null,"supervisor_agent_id":null,"request_key":role});
            let reply = f.success("agent.create", body);
            assert_eq!(reply["agent"]["role"], role);
            assert_eq!(reply["agent"]["name_version"], 1);
        }
        let id = f.create("Labels", "labels-target");
        // Core's set_labels names its target as either `agent` or `agent_id`, and
        // exactly one of them must be present and non-empty.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:609-628.
        for target in ["agent", "agent_id"] {
            let body = json!({target:id,"labels":["One"],"request_key":target});
            assert_eq!(f.success("agent.set_labels", body)["agent"]["agent_id"], id);
        }
        for body in [
            json!({"agent":id,"agent_id":id}),
            json!({}),
            json!({"agent":""}),
            json!({"agent_id":""}),
        ] {
            let mut body = body;
            body["labels"] = json!([]);
            body["request_key"] = json!("bad-labels");
            f.refusal(OPERATOR, "agent.set_labels", body, "invalid_request");
        }
        // Core's set_avatar body is camelCase (`agentId`, `seedOnly`, `type`); the
        // snake_case spellings are refused as unknown fields, as core refuses them.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:631-646.
        for (field, alias) in [
            ("agentId", "agent_id"),
            ("seedOnly", "seed_only"),
            ("type", "avatar_type"),
        ] {
            let mut body = f.params("agent.set_avatar", Some("bad-avatar"));
            let value = body.as_object_mut().unwrap().remove(field).unwrap();
            body[alias] = value;
            f.refusal(OPERATOR, "agent.set_avatar", body, "invalid_request");
        }
        for (method, field, value) in [
            ("agent.create", "role", json!("hire")),
            ("agent.create", "projectId", json!("project")),
            (
                "agent.set_github_identity",
                "github_identity",
                json!({"kind":"unknown"}),
            ),
        ] {
            let mut body = f.params(method, Some("bad-shape"));
            body[field] = value;
            f.refusal(OPERATOR, method, body, "invalid_request");
        }
    }

    #[test]
    fn import_and_mutation_retries_keep_commit_generation_but_use_restart_incarnation() {
        let mut f = Fixture::new();
        let committed = commit_every_mutation(&f);
        let head = f.head();
        f.restart();
        for (method, mut params, mut expected) in committed {
            // Changed valid bodies still return the original commit, including
            // an import whose snapshot path no longer points to an existing file.
            if method == "agent.import" {
                params["snapshot_path"] = json!(f.root.join("missing.db"));
            }
            if method == "agent.create" {
                params["name"] = json!("Changed retry");
            }
            expected["incarnation"] = json!("fedcba9876543210");
            assert_eq!(f.success(method, params), expected);
            assert_eq!(f.head(), head);
        }
        let mut params = f.params("agent.import", Some("fresh-import"));
        params["snapshot_path"] = json!(f.root.join("missing.db"));
        f.refusal(OPERATOR, "agent.import", params, "import_already_done");
        f.refusal(
            OPERATOR,
            "agent.import",
            json!({"request_key":"committed-agent.import"}),
            "invalid_request",
        );
        let mut params = f.params("agent.rename", Some("committed-agent.create"));
        params["agent_id"] = json!("missing");
        f.refusal(
            OPERATOR,
            "agent.rename",
            params,
            "request_key_reused_across_ops",
        );
    }

    #[test]
    fn structured_refusals_use_error_with_detail_and_wire_decoder() {
        let f = Fixture::new();
        f.register();
        f.source_sql("INSERT INTO agent(agent_id,name,tag,role,project_id,created_at_ms,updated_at_ms) VALUES('agent_0000000000000001','Imported','test','head','project',1,2);
            INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_0000000000000001','workspace','workspace','imported','Imported',1);");
        f.cut_over();
        let mut params = f.params("agent.create", Some("taken"));
        params["role"] = json!("head");
        params["project_id"] = json!("project");
        let error = f.refusal(OPERATOR, "agent.create", params, "agent_project_taken");
        assert_eq!(error.detail, Some(json!({"agent_id":A,"request_key":null})));
        f.success("agent.dispose", f.params("agent.dispose", Some("dispose")));
        let error = f.refusal(
            OPERATOR,
            "agent.rename",
            f.params("agent.rename", Some("gone")),
            "gone",
        );
        // Replies about a retired agent use core's `gone` object, reason `deleted`
        // with the time in milliseconds, so existing clients decode it unchanged.
        // The stored row and the change feed call the same state `retired`.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:992-1001.
        assert_eq!(error.detail, Some(json!({"reason":"deleted","at":NOW})));
        let mut head = f.params("agent.create", Some("head-create"));
        head["role"] = json!("head");
        head["project_id"] = json!("project");
        let created = f.success("agent.create", head.clone());
        head["name"] = json!("Another head");
        head["request_key"] = json!("head-duplicate");
        let error = f.refusal(OPERATOR, "agent.create", head, "agent_project_taken");
        assert_eq!(
            error.detail,
            Some(json!({"agent_id":created["agent"]["agent_id"],"request_key":"head-create"}))
        );
        let target = f.create("Target", "target");
        let source = f.create("Source", "source");
        f.success(
            "agent.merge",
            json!({"agent_id":source,"into_agent_id":target,"request_key":"merge"}),
        );
        let error = f.refusal(
            OPERATOR,
            "agent.update_tag",
            json!({"agent_id":source,"tag":"new","request_key":"merged-gone"}),
            "gone",
        );
        assert_eq!(
            error.detail,
            Some(json!({"reason":"merged","at":NOW,"into_agent_id":target}))
        );

        let bad = Fixture::new();
        bad.source_sql("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms) VALUES('INVALID','Bad','test','assistant',1,2,'deleted',3);");
        let error = bad.refusal(
            OPERATOR,
            "agent.import",
            bad.params("agent.import", Some("bad-import")),
            "import_invariant_failed",
        );
        assert_eq!(
            error.detail,
            Some(json!({"check":"agent_id","agent_id":"INVALID"}))
        );
        bad.refusal(
            OPERATOR,
            "agent.rename",
            bad.params("agent.rename", Some("no-marker")),
            "authority_not_cut_over",
        );
    }

    #[test]
    fn project_mutations_keep_their_gate_before_and_after_cutover_including_rebuild() {
        let f = Fixture::new();
        for after in [false, true] {
            if after {
                f.cut_over();
            }
            for principal in principals() {
                f.bind(principal.clone(), false);
                for method in crate::MUTATING_METHODS
                    .iter()
                    // identity_log.enable is Direct-only, covered by attach_surface_enable_is_direct_only_and_fails_closed.
                    .filter(|method| {
                        !MUTATING_METHODS.contains(method) && **method != "identity_log.enable"
                    })
                {
                    if principal == Some(Principal::Direct)
                        || principal == Some(reserved(WRITER_MODULE))
                    {
                        assert!(f.handler.admit(method, CALLER).is_ok());
                    } else {
                        f.refusal(CALLER, method, Value::Null, "write_not_permitted");
                    }
                }
                if principal == Some(Principal::Direct)
                    || principal == Some(reserved(WRITER_MODULE))
                {
                    assert!(matches!(
                        f.outcome(CALLER, "rebuild", json!({})),
                        HandlerOutcome::Response(_)
                    ));
                }
            }
        }
    }

    #[test]
    fn deferred_and_removed_methods_are_unknown_for_every_route_and_write_nothing() {
        let f = Fixture::new();
        for after in [false, true] {
            if after {
                f.cut_over();
            }
            for principal in principals() {
                for flow in [false, true] {
                    f.bind(principal.clone(), flow);
                    for method in [
                        "create_project",
                        "create_head",
                        "create_hire",
                        "dispose_agent",
                        "agent.set_hire_cap",
                        "grants.list",
                        "grants.revoke",
                        "grants.would_ask",
                        "grants.offer",
                        "grants.anything",
                        "agent.fleet_overview",
                        "agent.set_sleep",
                        "agent.update_wake_policy",
                    ] {
                        f.refusal(
                            CALLER,
                            method,
                            json!({"request_key":"unused"}),
                            "unknown_method",
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn incarnation_is_a_fresh_lowercase_sixteen_hex_nonce() {
        let a = crate::incarnation::new_incarnation().unwrap();
        let b = crate::incarnation::new_incarnation().unwrap();
        for value in [&a, &b] {
            assert_eq!(value.len(), 16);
            assert!(value
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c)));
        }
        assert_ne!(a, b);
    }

    #[test]
    fn agent_writes_record_attested_principal_and_optional_actor() {
        let f = Fixture::new();
        f.cut_over();
        for (key, actor) in [
            ("absent", None),
            ("null", Some(Value::Null)),
            ("present", Some(json!("operator:test"))),
        ] {
            let mut params = f.params("agent.create", Some(key));
            params["name"] = json!(key);
            if let Some(actor) = actor {
                params["actor"] = actor;
            }
            f.success("agent.create", params);
        }
        // A fixture-only journal probe exposes the stored columns; it is not a
        // second production write path or a synthetic handler reply.
        let (_, rows) = f.handler.with_store(|store| store.apply_entry("fixture-probe", "{}", "fixture", None, |tx| {
            let mut statement = tx.prepare("SELECT op,principal,actor,created_at FROM registry_journal WHERE op LIKE 'agent.%' ORDER BY seq")?;
            let rows = statement.query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?, row.get::<_, i64>(3)?)))?;
            rows.collect::<Result<Vec<_>, _>>()
        })).unwrap();
        assert_eq!(
            rows,
            vec![
                (
                    "agent.cutover".into(),
                    "reserved:prefrontal-core".into(),
                    "module".into(),
                    NOW
                ),
                (
                    "agent.create".into(),
                    "reserved:prefrontal-core".into(),
                    "module".into(),
                    NOW
                ),
                (
                    "agent.create".into(),
                    "reserved:prefrontal-core".into(),
                    "module".into(),
                    NOW
                ),
                (
                    "agent.create".into(),
                    "reserved:prefrontal-core".into(),
                    "operator:test".into(),
                    NOW
                ),
            ]
        );
    }
}

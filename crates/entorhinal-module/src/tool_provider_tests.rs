use super::*;
use crate::{
    route_key,
    tests::{reserved, scratch_descriptor},
    unix_millis, MODULE_ID,
};
use entorhinal_core::{RegisterRequest, RegistryStore};
use std::path::PathBuf;
use std::sync::Arc;
use subc_client_rs::{ModuleHandler, RouteBindRequest, RouteHandle};
use subc_protocol::{scope::ScopeStamp, BindIdentity, RouteTarget};

const ROUTE: RouteKey = (80, 2);
const TOOL_NAMES: &[&str] = &[
    "projects_create_workspace",
    "projects_register",
    "agents_create_head",
    "agents_hire",
    "agents_retire_hire",
    "agents_list_hires",
];

pub(crate) struct Fixture {
    pub(crate) root: PathBuf,
    pub(crate) descriptor: crate::StorageDescriptor,
    pub(crate) handler: ProjectsHandler,
    pub(crate) agents: Vec<String>,
}

struct RejectConsent;
#[async_trait::async_trait]
impl crate::consent_client::ConsentConnector for RejectConsent {
    async fn open(
        &self,
        _: &ScopeStamp,
        _: tokio::time::Instant,
    ) -> Result<Box<dyn crate::consent_client::ConsentRoute>, crate::consent_client::ConsentError>
    {
        Err(crate::consent_client::ConsentError::Refused(
            "unavailable".into(),
        ))
    }
}

impl Fixture {
    async fn admitted(&self, tool: &str, args: Value) {
        let expected = match tool {
            "projects_register" => "root_not_found",
            "agents_create_head" => "agent_project_taken",
            "agents_retire_hire" => "not_your_hire",
            "agents_list_hires" => {
                assert_eq!(result(self.call(tool, args).await)["agents"], json!([]));
                return;
            }
            _ => "consent_unavailable",
        };
        self.refusal(tool, args, expected).await;
    }
    pub(crate) fn new() -> Self {
        let (root, descriptor) = scratch_descriptor("agent-tool-provider");
        // Tests register directories under this root, and registration refuses
        // a non-canonical path; macOS temp folders resolve through /private.
        let root = crate::tests::canonical_test_path(root);
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        let agents = ["assistant", "workspace_head", "head", "hiree"]
            .into_iter()
            .map(|role| {
                let (project, workspace) = match role {
                    "workspace_head" => (None, Some("W")),
                    "head" | "hiree" => (Some("P"), None),
                    _ => (None, None),
                };
                let bytes = store
                    .agent_mutation(
                        "agent.create",
                        json!({
                            "name":role, "role":role, "tag":"test", "project_id":project,
                            "workspace_id":workspace, "request_key":format!("create-{role}"),
                        }),
                        100,
                    )
                    .unwrap();
                serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["agent"]["agent_id"]
                    .as_str()
                    .unwrap()
                    .to_owned()
            })
            .collect();
        let mut handler = ProjectsHandler::with_runtime("tool-incarnation".into(), unix_millis);
        handler.consent_connector = Arc::new(RejectConsent);
        *handler.store.lock().unwrap() = Some(store);
        Self {
            root,
            descriptor,
            handler,
            agents,
        }
    }

    pub(crate) fn stamp(&self, role: usize) -> ScopeStamp {
        serde_json::from_value(json!({
            "owner":{"kind":"reserved", "module_id":"prefrontal"},
            "ref":format!("head:{}", self.agents[role]), "scope_epoch":7,
            "kind":"head", "attributes":{"agent_id":self.agents[role]}, "owner_authorized":true,
        }))
        .unwrap()
    }

    pub(crate) fn admit(&self, stamp: Option<&ScopeStamp>) {
        self.handler.route_admissions().insert(
            ROUTE,
            RouteAdmission::from_bind(Some(reserved("prefrontal")), stamp),
        );
    }

    pub(crate) fn counts(&self) -> Vec<i64> {
        let connection = rusqlite::Connection::open_with_flags(
            self.root.join("store.db"),
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        [
            "registry_journal",
            "agent",
            "consent_request",
            "agent_audit",
            "consent_grant",
        ]
        .iter()
        .map(|table| {
            connection
                .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                    row.get(0)
                })
                .unwrap()
        })
        .collect()
    }

    pub(crate) async fn call(&self, name: &str, arguments: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"name":name, "arguments":arguments})).unwrap(),
                ROUTE,
            )
            .await
    }

    async fn refusal(&self, name: &str, arguments: Value, expected: &str) {
        let before = self.counts();
        let outcome = self.call(name, arguments).await;
        assert_eq!(error_code(outcome), expected, "{name}");
        assert_eq!(self.counts(), before, "a refused tool must write nothing");
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn arguments(name: &str) -> Value {
    match name {
        "projects_create_workspace" => {
            json!({"workspace_id":"New", "name":"New workspace", "request_key":"key"})
        }
        "projects_register" => {
            json!({"name":"New project", "dir":"/project", "workspace_id":"W", "request_key":"key"})
        }
        "agents_create_head" => {
            json!({"project_id":"P", "name":"New head", "tag":"test", "request_key":"key"})
        }
        "agents_hire" => json!({"name":"New hire", "tag":"test", "request_key":"key"}),
        "agents_retire_hire" => json!({"agent_id":"agent_0000000000000001", "request_key":"key"}),
        "agents_list_hires" => json!({}),
        _ => panic!("unexpected tool {name}"),
    }
}

pub(crate) fn error_code(outcome: HandlerOutcome) -> String {
    match outcome {
        HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => code,
        HandlerOutcome::Response(body) => {
            panic!("expected refusal, got {}", String::from_utf8_lossy(&body))
        }
        HandlerOutcome::Streamed => panic!("tools must not stream"),
    }
}

pub(crate) fn result(outcome: HandlerOutcome) -> Value {
    match outcome {
        HandlerOutcome::Response(body) => {
            serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
        }
        _ => panic!("expected provider response"),
    }
}

#[tokio::test]
async fn every_tool_admits_only_its_table_role_before_key_or_arguments() {
    let f = Fixture::new();
    for (role_index, role) in ["assistant", "workspace_head", "head", "hiree"]
        .iter()
        .enumerate()
    {
        f.admit(Some(&f.stamp(role_index)));
        for (index, tool) in TOOL_NAMES.iter().enumerate() {
            let allowed = if index < 3 { "assistant" } else { "head" };
            if *role == allowed {
                f.admitted(tool, arguments(tool)).await;
            } else {
                f.refusal(tool, json!(null), "role_forbidden").await;
                f.refusal(tool, arguments(tool), "role_forbidden").await;
            }
        }
    }
}

#[tokio::test]
async fn stamped_caller_is_required_and_body_identity_cannot_supply_it() {
    let f = Fixture::new();
    let mut cases = vec![None];
    let mut missing = f.stamp(0);
    missing.attributes.agent_id = None;
    cases.push(Some(missing));
    let mut empty = f.stamp(0);
    empty.attributes.agent_id = Some(String::new());
    cases.push(Some(empty));
    let mut flow = f.stamp(0);
    flow.attributes.flow_id = Some("flow_test".into());
    cases.push(Some(flow));
    let mut worker = f.stamp(0);
    worker.kind = ScopeKind::Worker;
    worker.attributes.agent_id = None;
    cases.push(Some(worker));
    let mut worker_with_agent = f.stamp(0);
    worker_with_agent.kind = ScopeKind::Worker;
    cases.push(Some(worker_with_agent));
    let mut unauthorized = f.stamp(0);
    unauthorized.owner_authorized = false;
    cases.push(Some(unauthorized));
    let mut nondelegating_run = f.stamp(0);
    nondelegating_run.attributes.run_id = Some("run_test".into());
    cases.push(Some(nondelegating_run));
    for stamp in cases {
        f.admit(stamp.as_ref());
        for tool in TOOL_NAMES {
            let mut args = arguments(tool);
            args["caller_agent_id"] = json!(f.agents[0]);
            args["scope"] = serde_json::to_value(f.stamp(0)).unwrap();
            f.refusal(tool, args, "caller_forbidden").await;
        }
        let body = json!({"name":"projects_create_workspace", "arguments":arguments("projects_create_workspace"),
            "agent_id":f.agents[0], "scope":f.stamp(0), "principal":{"kind":"reserved", "module_id":"prefrontal"}});
        assert_eq!(
            error_code(
                f.handler
                    .handle_served_request(&serde_json::to_vec(&body).unwrap(), ROUTE)
                    .await
            ),
            "caller_forbidden"
        );
    }
    f.admit(Some(&f.stamp(3)));
    let body = json!({"name":"agents_hire", "arguments":arguments("agents_hire"), "agent_id":f.agents[2], "scope":f.stamp(2)});
    assert_eq!(
        error_code(
            f.handler
                .handle_served_request(&serde_json::to_vec(&body).unwrap(), ROUTE)
                .await
        ),
        "role_forbidden"
    );
}

#[tokio::test]
async fn retired_and_unknown_callers_are_refused_on_previously_bound_routes() {
    let f = Fixture::new();
    f.admit(Some(&f.stamp(0)));
    f.refusal(
        "projects_create_workspace",
        arguments("projects_create_workspace"),
        "consent_unavailable",
    )
    .await;
    f.handler
        .with_agent_store(|store| {
            store.agent_mutation(
                "agent.dispose",
                json!({"agent_id":f.agents[0], "request_key":"retire-caller"}),
                200,
            )
        })
        .unwrap();
    for tool in TOOL_NAMES {
        f.refusal(tool, json!(null), "gone").await;
    }
    let mut unknown = f.stamp(0);
    unknown.attributes.agent_id = Some("agent_ffffffffffffffff".into());
    f.admit(Some(&unknown));
    for tool in TOOL_NAMES {
        f.refusal(tool, json!(null), "unknown_agent").await;
    }
}

#[tokio::test(start_paused = true)]
async fn agent_tools_refuse_cached_claims_held_elsewhere_even_with_standing_grants() {
    use entorhinal_core::{
        agent::consent::{GrantClass, StandingGrant},
        claim_cache::{CachedClaim, ClaimObservation},
    };
    let f = Fixture::new();
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    let own = "01010101010101010101010101010101";
    let elsewhere = "02020202020202020202020202020202";
    let id = f.agents[2].clone();
    f.admit(Some(&f.stamp(2)));
    store
        .consent_transaction(|tx| {
            tx.put_grant(&StandingGrant {
                id: "standing".into(),
                holder_agent_id: id.clone(),
                class: GrantClass::Hire,
                scope_id: "P".into(),
                granted_at_ms: 100,
                card_ref: "card".into(),
                revoked_at_ms: None,
            })
        })
        .unwrap();
    for holder in [None, Some(own), Some(elsewhere)] {
        store
            .cache_agent_claims(
                &[(
                    id.clone(),
                    ClaimObservation::Present {
                        claim: CachedClaim {
                            holder: holder.map(str::to_owned),
                            epoch: if holder.is_none() { 0 } else { 1 },
                            cut_id: None,
                            version: 1,
                            read_at_ms: 100,
                            stale: false,
                            integrity_failed: false,
                        },
                        self_author: own.into(),
                    },
                )],
                100,
            )
            .unwrap();
        let admitted = f.handler.admit_agent_tool(
            TOOLS
                .iter()
                .find(|t| t.name == "agents_hire")
                .copied()
                .unwrap(),
            ROUTE,
        );
        if holder == Some(elsewhere) {
            assert_eq!(admitted.unwrap_err().code, "held_elsewhere");
            let before = f.counts();
            assert_eq!(
                error_code(
                    f.call(
                        "agents_hire",
                        json!({"request_key":"old-Mac","name":"Hire","tag":"worker","labels":[]})
                    )
                    .await
                ),
                "held_elsewhere"
            );
            assert_eq!(f.counts(), before);
        } else {
            assert!(admitted.is_ok());
        }
    }
}

#[tokio::test]
async fn every_write_validates_printable_ascii_request_keys_before_other_arguments() {
    let f = Fixture::new();
    let mut invalid = vec![
        Value::Null,
        json!(42),
        json!(true),
        json!([]),
        json!(""),
        json!("a".repeat(129)),
        json!("é"),
        json!("a\nb"),
    ];
    for byte in 0u8..=127 {
        if !(0x20..=0x7e).contains(&byte) {
            invalid.push(json!(String::from_utf8(vec![byte]).unwrap()));
        }
    }
    let printable: String = (0x20u8..=0x7e).map(char::from).collect();
    for (index, tool) in TOOL_NAMES[..5].iter().enumerate() {
        f.admit(Some(&f.stamp(if index < 3 { 0 } else { 2 })));
        f.refusal(tool, json!({}), "invalid_request_key").await;
        for key in &invalid {
            f.refusal(
                tool,
                json!({"request_key":key, "unexpected":true}),
                "invalid_request_key",
            )
            .await;
        }
        for key in [
            " ".to_owned(),
            "~".to_owned(),
            printable.clone(),
            "a".repeat(128),
        ] {
            let mut args = arguments(tool);
            args["request_key"] = json!(key);
            f.admitted(tool, args).await;
        }
    }
    f.admit(Some(&f.stamp(2)));
    assert_eq!(
        result(f.call("agents_list_hires", json!({})).await)["agents"],
        json!([])
    );
    f.refusal(
        "agents_list_hires",
        json!({"request_key":"unexpected"}),
        "invalid_request",
    )
    .await;
}

#[tokio::test]
async fn bind_captures_scope_epoch_and_mode_and_route_gone_drops_authority() {
    let f = Fixture::new();
    let handle = RouteHandle::detached(ROUTE.0, ROUTE.1);
    let stamp = f.stamp(2);
    let request = RouteBindRequest::new(
        handle,
        RouteTarget::ToolProvider {
            module_id: MODULE_ID.into(),
        },
        BindIdentity::new(f.root.clone(), "project", "session"),
    )
    .with_principal(reserved("prefrontal"))
    .with_scope(stamp.clone())
    .with_call_mode(CallMode::Codemode);
    f.handler.on_bind(&request).await;
    let admission = f.handler.admission_for(route_key(&handle));
    assert_eq!(admission.scope, Some(stamp));
    assert_eq!(admission.call_mode, Some(CallMode::Codemode));
    assert_eq!(admission.handle, Some(handle));
    f.refusal(
        "agents_hire",
        arguments("agents_hire"),
        "consent_unavailable",
    )
    .await;
    f.handler.on_route_gone(&handle).await;
    f.refusal("agents_hire", arguments("agents_hire"), "caller_forbidden")
        .await;
}

#[tokio::test]
async fn tool_arguments_match_the_flat_schemas_without_caller_or_role_overrides() {
    let f = Fixture::new();
    for (index, tool) in TOOL_NAMES.iter().enumerate() {
        f.admit(Some(&f.stamp(if index < 3 { 0 } else { 2 })));
        let valid = arguments(tool);
        let fields: Vec<_> = valid.as_object().unwrap().keys().cloned().collect();
        for field in fields {
            if field == "request_key" {
                continue;
            }
            let mut missing = valid.clone();
            missing.as_object_mut().unwrap().remove(&field);
            f.refusal(tool, missing, "invalid_request").await;
            let mut wrong_type = valid.clone();
            wrong_type[&field] = json!(42);
            f.refusal(tool, wrong_type, "invalid_request").await;
        }
        for field in [
            "caller_agent_id",
            "role",
            "supervisor_agent_id",
            "scope_ref",
            "epoch",
            "unexpected",
        ] {
            let mut extra = valid.clone();
            extra[field] = json!("forged");
            f.refusal(tool, extra, "invalid_request").await;
        }
    }
    let mut hire = arguments("agents_hire");
    hire["labels"] = json!(["build", "test"]);
    f.refusal("agents_hire", hire.clone(), "consent_unavailable")
        .await;
    for labels in [json!(null), json!("build"), json!(["build", 42])] {
        hire["labels"] = labels;
        f.refusal("agents_hire", hire.clone(), "invalid_request")
            .await;
    }
}

#[tokio::test]
async fn schema_pins_and_call_modes_are_checked_after_agent_admission() {
    let f = Fixture::new();
    f.admit(Some(&f.stamp(2)));
    let tool = TOOLS
        .iter()
        .find(|tool| tool.name == "agents_hire")
        .unwrap()
        .catalog();
    let pin = cortexkit_role_tool_provider::call::SchemaPin::new(
        &tool.name,
        &tool.schema_digest,
        tool.semantics,
    );
    let call = |schema_pin: Value, call_key: Value| json!({"name":"agents_hire", "arguments":arguments("agents_hire"), "schema_pin":schema_pin, "call_key":call_key});
    let mut changed = pin.clone();
    changed.schema_digest = "0".repeat(64);
    let mut changed_semantics = pin.clone();
    changed_semantics.semantics += 1;
    let mut wrong_tool = pin.clone();
    wrong_tool.tool = "agents_retire_hire".into();
    for (body, expected) in [
        (
            call(json!(pin.encode().unwrap()), json!("opaque-call")),
            "consent_unavailable",
        ),
        (
            call(json!(changed.encode().unwrap()), json!("opaque-call")),
            "tool_schema_changed",
        ),
        (
            call(
                json!(changed_semantics.encode().unwrap()),
                json!("opaque-call"),
            ),
            "tool_semantics_changed",
        ),
        (
            call(json!(wrong_tool.encode().unwrap()), json!("opaque-call")),
            "invalid_request",
        ),
        (
            call(json!("not-a-pin"), json!("opaque-call")),
            "invalid_request",
        ),
        (
            call(json!(pin.encode().unwrap()), json!("bad call key")),
            "invalid_request",
        ),
    ] {
        let before = f.counts();
        assert_eq!(
            error_code(
                f.handler
                    .handle_served_request(&serde_json::to_vec(&body).unwrap(), ROUTE)
                    .await
            ),
            expected
        );
        assert_eq!(f.counts(), before);
    }
    for mode in [
        CallMode::Classic,
        CallMode::Codemode,
        CallMode::Flow,
        CallMode::Unknown("future".into()),
    ] {
        let expected = if matches!(mode, CallMode::Classic | CallMode::Codemode) {
            "consent_unavailable"
        } else {
            "call_mode_unsupported"
        };
        f.handler
            .route_admissions()
            .get_mut(&ROUTE)
            .unwrap()
            .call_mode = Some(mode);
        f.refusal("agents_hire", arguments("agents_hire"), expected)
            .await;
    }
    let mut flow = f.stamp(2);
    flow.attributes.flow_id = Some("flow_test".into());
    f.admit(Some(&flow));
    f.handler
        .route_admissions()
        .get_mut(&ROUTE)
        .unwrap()
        .call_mode = Some(CallMode::Flow);
    f.refusal("agents_hire", arguments("agents_hire"), "caller_forbidden")
        .await;
}

#[tokio::test]
async fn provider_discovery_catalog_and_hello_agree_on_exact_tool_table() {
    let f = Fixture::new();
    let role: RoleDescribe =
        serde_json::from_value(result(f.call("role.describe", json!({})).await)).unwrap();
    assert_eq!(role.majors.len(), 1);
    assert_eq!(role.majors[0].version, "tool-provider/v1");
    assert_eq!(role.majors[0].ops, vec!["role.describe", "tool.catalog"]);
    assert!(role.capabilities.is_empty());
    let catalog: CatalogAnswer =
        serde_json::from_value(result(f.call("tool.catalog", json!({})).await)).unwrap();
    for tool in &catalog.tools {
        cortexkit_role_tool_provider::catalog::check_flat_schema(&tool.input_schema).unwrap();
        cortexkit_role_tool_provider::catalog::check_modes(tool).unwrap();
        cortexkit_role_tool_provider::catalog::check_reply(tool).unwrap();
    }
    assert_eq!(
        catalog
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        TOOL_NAMES
            .iter()
            .copied()
            .chain([
                "catalog_prepare",
                "catalog_plan",
                "catalog_install",
                "catalog_generation"
            ])
            .collect::<Vec<_>>()
    );
    assert!(catalog.capabilities.is_empty());
    let expected_fields = [
        vec!["workspace_id", "name"],
        vec!["name", "dir", "workspace_id"],
        vec!["project_id", "name", "tag"],
        vec!["name", "tag"],
        vec!["agent_id"],
        vec![],
        vec!["spec"],
        vec!["plan_id"],
        vec!["plan_id"],
        vec![],
    ];
    let hello = crate::manifest();
    let hello_tools = hello
        .provides
        .iter()
        .find_map(|role| {
            if let ProviderRole::ToolProvider { tools, .. } = role {
                Some(tools)
            } else {
                None
            }
        })
        .unwrap();
    assert_eq!(hello_tools.len(), 10);
    for (index, (tool, fields)) in catalog.tools.iter().zip(expected_fields).enumerate() {
        let mut properties: Map<String, Value> = fields
            .iter()
            .map(|field| ((*field).into(), json!({"type":"string"})))
            .collect();
        let mut required = fields;
        if index == 6 {
            properties.insert("spec".into(),json!({"type":"object","properties":{"operation":{"type":"string","enum":["install","upgrade"]},"target":{"type":"object","properties":{"agent_id":{"type":"string"}},"required":["agent_id"],"additionalProperties":false},"package":{"type":"string"},"range":{"type":"string"}},"required":["operation","target","package","range"],"additionalProperties":false}));
        }
        if index == 3 {
            properties.insert(
                "labels".into(),
                json!({"type":"array", "items":{"type":"string"}}),
            );
        }
        if index < 5 || index == 6 {
            required.push("request_key");
            properties.insert("request_key".into(), json!({"type":"string", "minLength":1, "maxLength":128, "pattern":"^[ -~]+$", "not":{"pattern":"[^ -~]"},
                "description":"A caller-chosen key for this write. Reuse it only with the same arguments."}));
        }
        assert_eq!(
            tool.input_schema,
            json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
        );
        assert_eq!(tool.reply.as_ref().unwrap().max_ms, 120_000);
        assert_eq!(
            tool.modes,
            Some(vec![CallMode::Classic, CallMode::Codemode])
        );
        assert_eq!(
            tool.result_format, None,
            "opaque results do not promise content/v1"
        );
        assert_eq!(tool.semantics, 1);
        assert_eq!(tool.schema_digest.len(), 64);
        assert_eq!(hello_tools[index].name, tool.name);
        assert_eq!(hello_tools[index].description, tool.description);
        assert_eq!(hello_tools[index].schema, tool.input_schema);
        assert_eq!(
            hello_tools[index].execution_mode,
            if index < 5 || index == 6 || index == 8 {
                ExecutionMode::Mutating
            } else {
                ExecutionMode::Pure
            }
        );
    }
    let module_cargo = include_str!("../Cargo.toml");
    assert!(module_cargo.contains("cortexkit-role-tool-provider = \"=0.7.3\""));
    let lock = include_str!("../../../Cargo.lock");
    assert_eq!(
        lock.matches("name = \"cortexkit-role-tool-provider\"\n")
            .count(),
        1
    );
    assert!(lock.contains("name = \"cortexkit-role-tool-provider\"\nversion = \"0.7.3\""));
}

#[tokio::test]
async fn catalog_digest_only_and_composition_are_stable_and_unsupported_inputs_refuse() {
    let f = Fixture::new();
    for input in [
        json!({}),
        json!({"composition":{"provider":"entorhinal", "params":{}}}),
    ] {
        let full = result(f.call("tool.catalog", input.clone()).await);
        let mut digest_only = input.clone();
        digest_only["digest_only"] = json!(true);
        let digest = result(f.call("tool.catalog", digest_only).await);
        assert_eq!(
            digest,
            json!({"generation":full["generation"], "catalog_digest":full["catalog_digest"]})
        );
        assert_eq!(full, result(f.call("tool.catalog", input).await));
    }
    let full = result(f.call("tool.catalog", json!({})).await);
    let composed = result(f.call("tool.catalog", json!({"composition":{"x":1}})).await);
    assert_ne!(full["catalog_digest"], composed["catalog_digest"]);
    assert_eq!(full["generation"], full["catalog_digest"]);
    assert!(full.get("composition_digest").is_none());
    assert!(composed["composition_digest"].is_string());
    for input in [
        json!({"params":{"unknown":true}}),
        json!({"params":null}),
        json!({"digest_only":1}),
        json!({"preset":"default"}),
        json!({"system_text":{}}),
        json!({"composition":null}),
        json!({"unknown":true}),
    ] {
        f.refusal("tool.catalog", input, "invalid_request").await;
    }
    f.refusal("role.describe", json!({"unknown":true}), "invalid_request")
        .await;
    f.refusal("projects_unknown", json!({}), "unknown_tool")
        .await;
    f.refusal("tool.withdraw", json!({}), "unknown_tool").await;
    f.refusal("late_results", json!({}), "unknown_tool").await;
}

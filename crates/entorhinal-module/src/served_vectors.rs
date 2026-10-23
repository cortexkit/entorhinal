use std::{collections::BTreeMap, path::PathBuf, sync::Arc};

use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    move_store::{AgentMove, MoveSide, MoveStep},
    RegistryStore,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;

use crate::{
    fake_record::{FakeRecord, FakeRecordConnector},
    log_client::{LogConnector, LogTransport, TransportError},
    record_client::{RecordClient, CAS},
};
use crate::{
    install_store, tests::reserved, ProjectsHandler, RouteAdmission, RouteKey, WRITER_MODULE,
};

#[path = "watch_tests.rs"]
mod watch_tests;

const DIRECT: RouteKey = (901, 1);
const CORE: RouteKey = (902, 1);
const READER: RouteKey = (903, 1);
const READER_MODULE: &str = "phone";
const INCARNATION: &str = "0123456789abcdef";
const HEAD: &str = "agent_0000000000000001";
const ASSISTANT: &str = "agent_0000000000000002";
const NO_AVATAR: &str = "agent_0000000000000003";
const RETIRED: &str = "agent_0000000000000004";
const UNKNOWN: &str = "agent_ffffffffffffffff";
const ROLE_DIRECT: RouteKey = (904, 1);
const ROLE_PLEXUS: RouteKey = (905, 1);
const ROLE: &str = "x";
const ROLE_SELF_AUTHOR: &str = "01010101010101010101010101010101";
const ROLE_OTHER_AUTHOR: &str = "02020202020202020202020202020202";

// Use the preserved migrations so the import sees the same columns and defaults
// as a core snapshot, including the name claims required for live identities.
pub(super) const MIGRATIONS: &[&str] = &[
    include_str!(
        "../../entorhinal-core/tests/fixtures/core-agent-migrations/076_agent_registry.sql"
    ),
    include_str!(
        "../../entorhinal-core/tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"
    ),
    include_str!(
        "../../entorhinal-core/tests/fixtures/core-agent-migrations/082_wake_delivery.sql"
    ),
    include_str!(
        "../../entorhinal-core/tests/fixtures/core-agent-migrations/109_agent_generation.sql"
    ),
    include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
    include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
];

struct Fixture {
    dir: PathBuf,
    handler: ProjectsHandler,
    descriptor: crate::StorageDescriptor,
}

impl Fixture {
    async fn new(label: &str) -> Self {
        Self::with_confirmer(
            label,
            std::sync::Arc::new(Approve {
                summaries: std::sync::Mutex::new(vec![]),
            }),
        )
        .await
    }

    async fn with_confirmer(label: &str, confirmer: std::sync::Arc<Approve>) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let handler = ProjectsHandler::with_runtime(INCARNATION.into(), || 700)
            .with_operator_confirmer(confirmer);
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        install_store(
            &handler.store,
            &handler.health,
            &handler.catalog_sql,
            Ok(store),
        );
        for (route, principal) in [
            (DIRECT, Principal::Direct),
            (CORE, reserved(WRITER_MODULE)),
            (READER, reserved(READER_MODULE)),
        ] {
            handler
                .route_admissions()
                .insert(route, RouteAdmission::from_bind(Some(principal), None));
        }
        let fixture = Self {
            dir,
            handler,
            descriptor,
        };
        // A rootless project is valid. Its fleet placement has no canonicalRoot,
        // so scratch paths and Windows drive letters never enter the records.
        fixture
            .success(
                DIRECT,
                "register",
                json!({"projectId":"served-project","name":"Served project","roots":[],"workspaceId":"served-workspace"}),
            )
            .await;
        fixture
    }

    async fn outcome(&self, route: RouteKey, op: &str, request: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":op,"params":request})).unwrap(),
                route,
            )
            .await
    }

    async fn success(&self, route: RouteKey, op: &str, request: Value) -> Value {
        let outcome = self.outcome(route, op, request).await;
        let HandlerOutcome::Response(bytes) = outcome else {
            panic!("{op} refused fixture setup: {}", refusal(outcome));
        };
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
    }

    fn snapshot(&self) -> Connection {
        let source = Connection::open(self.dir.join("core-snapshot.db")).unwrap();
        for migration in MIGRATIONS {
            source.execute_batch(migration).unwrap();
        }
        source
    }

    async fn import(&self) -> HandlerOutcome {
        self.outcome(
            CORE,
            "agent.import",
            json!({"snapshot_path":self.dir.join("core-snapshot.db"),"request_key":"served-import"}),
        )
        .await
    }

    async fn record(&self, op: &str, request: Value) -> (String, Value) {
        let reply = match self.outcome(READER, op, request.clone()).await {
            HandlerOutcome::Response(bytes) => String::from_utf8(bytes).unwrap(),
            outcome => serde_json::to_string(&refusal(outcome)).unwrap(),
        };
        let decoded = serde_json::from_str(&reply).unwrap();
        // Embed successful replies verbatim: parsing and re-serializing them
        // would conceal a handler change in whitespace or object-key order.
        let record = format!(
            "  {{\"op\":{},\"request\":{},\"principal\":{},\"reply\":{reply}}}",
            json!(op),
            request,
            json!({"kind":"reserved","moduleId":READER_MODULE}),
        );
        (record, decoded)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        // Release SQLite and the lease before removing files on Windows.
        self.handler.store.lock().unwrap().take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

struct RoleFixture {
    dir: PathBuf,
    descriptor: cortexkit_store_types::StorageDescriptor,
    handler: ProjectsHandler,
    record: Arc<FakeRecord>,
}

impl RoleFixture {
    async fn new(label: &str, move_on_cas: bool) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let record = FakeRecord::new([1; 16]);
        let confirmer = Arc::new(Approve {
            summaries: std::sync::Mutex::new(vec![]),
        });
        let mut handler = ProjectsHandler::with_runtime(INCARNATION.into(), || 700)
            .with_operator_confirmer(confirmer);
        let connector: Arc<dyn LogConnector> = if move_on_cas {
            Arc::new(MoveRoleCasConnector(record.clone()))
        } else {
            Arc::new(FakeRecordConnector(record.clone()))
        };
        handler.record_client = RecordClient::new(connector);
        install_store(
            &handler.store,
            &handler.health,
            &handler.catalog_sql,
            Ok(RegistryStore::open(&descriptor).unwrap()),
        );
        handler.route_admissions().insert(
            ROLE_DIRECT,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        handler.route_admissions().insert(
            ROLE_PLEXUS,
            RouteAdmission::from_bind(Some(reserved("plexus")), None),
        );
        let fixture = Self {
            dir,
            descriptor,
            handler,
            record,
        };
        fixture.seed_member_names();
        fixture
    }

    fn store(&self) -> RegistryStore {
        self.handler.with_store(|store| Ok(store.clone())).unwrap()
    }

    fn seed_member_names(&self) {
        self.store()
            .apply_entry("fixture.role_member_names", "{}", "fixture", None, |tx| {
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,?2)",
                    params![ROLE_SELF_AUTHOR, "This Mac"],
                )?;
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,?2)",
                    params![ROLE_OTHER_AUTHOR, "Other Mac"],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn enroll(&self) {
        self.handler
            .role_read(json!({"role":ROLE}))
            .expect("enroll role for refresh");
    }

    async fn refresh(&self) {
        self.handler.start_role_refresh();
        self.handler.role_refresh.notify_one();
        self.handler.health.role_refresh_done.notified().await;
    }

    async fn restart_store(&self) {
        crate::tests::stop_store_tasks(&self.handler).await;
        self.handler.store.lock().unwrap().take();
        install_store(
            &self.handler.store,
            &self.handler.health,
            &self.handler.catalog_sql,
            Ok(RegistryStore::open(&self.descriptor).unwrap()),
        );
    }

    async fn record(
        &self,
        records: &mut Vec<String>,
        case: &str,
        method: &str,
        params: Value,
        route: RouteKey,
        principal: Value,
    ) -> Value {
        let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        let outcome = self.handler.handle_served_request(&request, route).await;
        let (kind, reply) = match outcome {
            HandlerOutcome::Response(bytes) => ("response", String::from_utf8(bytes).unwrap()),
            other => ("error", refusal(other).to_string()),
        };
        let decoded = serde_json::from_str(&reply).unwrap();
        records.push(format!(
            "  {{\"case\":{},\"request_bytes\":{},\"principal\":{},\"outcome\":{},\"reply_bytes\":{}}}",
            json!(case),
            json!(String::from_utf8(request).unwrap()),
            principal,
            json!(kind),
            json!(reply)
        ));
        decoded
    }
}

impl Drop for RoleFixture {
    fn drop(&mut self) {
        self.handler
            .role_refresh_task
            .lock()
            .unwrap()
            .take()
            .inspect(|task| task.abort());
        self.handler
            .catch_up_task
            .lock()
            .unwrap()
            .take()
            .inspect(|task| task.abort());
        self.handler.store.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

#[derive(Clone)]
struct MoveRoleCasConnector(Arc<FakeRecord>);

struct MoveRoleCasTransport(Arc<FakeRecord>);

#[async_trait::async_trait]
impl LogConnector for MoveRoleCasConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(MoveRoleCasTransport(self.0.clone())))
    }
}

#[async_trait::async_trait]
impl LogTransport for MoveRoleCasTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        if method == CAS {
            let subject = params["subject"].as_str().unwrap();
            let expected_version = params["expected_version"].as_u64().unwrap();
            self.0.put(
                subject,
                expected_version + 1,
                role_claim(1, ROLE_OTHER_AUTHOR),
                ROLE_OTHER_AUTHOR,
            );
        }
        self.0.call(method, params).await
    }
}

fn role_claim(epoch: i64, holder: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({
        "v":1,
        "role":ROLE,
        "holder":holder,
        "epoch":epoch,
        "reason":"cloud"
    }))
    .unwrap()
}

fn refusal(outcome: HandlerOutcome) -> Value {
    match outcome {
        HandlerOutcome::Error { code, message } => {
            json!({"code":code,"message":message,"detail":null})
        }
        HandlerOutcome::ErrorWithDetail {
            code,
            message,
            detail,
        } => json!({"code":code,"message":message,"detail":detail}),
        _ => panic!("expected a handler refusal"),
    }
}

fn seed_agent(
    source: &Connection,
    id: &str,
    name: &str,
    avatar: Option<&Value>,
    retired: bool,
    head: bool,
) {
    source.execute(
        "INSERT INTO agent(agent_id,name,tag,role,project_id,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms,avatar_genome,avatar_type,avatar_version)
         VALUES(?1,?2,'',?3,?4,10,10,?5,?6,?7,?8,?9)",
        params![id,name,if head { "head" } else { "assistant" },head.then_some("served-project"),retired.then_some("deleted"),retired.then_some(10),avatar.and_then(|v| v["genome"].as_str()),avatar.and_then(|v| v["type"].as_str()),avatar.and_then(|v| v["version"].as_i64())],
    ).unwrap();
    source.execute(
        "INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms,released_at_ms)
         VALUES(?1,?2,?3,?4,?5,10,?6)",
        params![id,if head { "workspace" } else { "assistant" },if head { "served-workspace" } else { "global" },name.to_lowercase(),name,retired.then_some(10)],
    ).unwrap();
}

fn assert_avatar_reply(actual: &Value, expected: &Value) {
    let mut expected_keys: Vec<_> = expected
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    expected_keys.extend(["generation", "incarnation"]);
    expected_keys.sort_unstable();
    let actual_keys: Vec<_> = actual
        .as_object()
        .unwrap()
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        actual_keys, expected_keys,
        "avatar reply has unexpected top-level keys"
    );
    assert!(actual["generation"].as_i64().unwrap() > 0);
    assert_eq!(actual["incarnation"], INCARNATION);
    assert_eq!(
        actual["avatars"], expected["avatars"],
        "avatar triples differ"
    );
}

fn published_file(directory: &str, name: &str, records: Vec<String>) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors")
        .join(directory)
        .join(name);
    let served = format!("[\n{}\n]\n", records.join(",\n"));
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, served).unwrap();
        println!("wrote {directory}/{name}");
    } else {
        let published = String::from_utf8(std::fs::read(&path).unwrap()).unwrap();
        assert_eq!(
            published, served,
            "{name} differs from the real handler reply"
        );
    }
}

#[tokio::test]
async fn served_records_match_published_files() {
    let fixture = Fixture::new("served-records").await;
    let source = fixture.snapshot();
    seed_agent(
        &source,
        HEAD,
        "Ada",
        Some(&json!({"genome":"a".repeat(2048),"type":"creature.classic","version":2})),
        false,
        true,
    );
    seed_agent(
        &source,
        ASSISTANT,
        "Lin",
        Some(&json!({"genome":"b".repeat(2048),"type":"creature.classic"})),
        false,
        false,
    );
    seed_agent(&source, NO_AVATAR, "Sam", None, false, false);
    seed_agent(
        &source,
        RETIRED,
        "Grace",
        Some(&json!({"genome":"c".repeat(2048),"type":"creature.classic","version":1})),
        true,
        false,
    );
    drop(source);
    let imported = fixture.import().await;
    let HandlerOutcome::Response(bytes) = imported else {
        panic!("served-record import refused: {}", refusal(imported));
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["agents_imported"],
        4
    );

    let (full_record, full) = fixture.record("agent.fleet_identity", json!({})).await;
    let agents = full["result"]["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 3);
    assert_eq!(
        agents
            .iter()
            .map(|a| a["agentId"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [HEAD, ASSISTANT, NO_AVATAR]
    );
    assert_eq!(
        agents[0]["project"],
        json!({"id":"served-project","name":"Served project"})
    );
    assert_eq!(
        agents[0]["workspace"],
        json!({"id":"served-workspace","name":"served-workspace"})
    );
    assert_eq!(agents[0]["avatar"]["version"], 2);
    assert!(agents[1]["avatar"].get("version").is_none());
    assert!(agents[2].get("avatar").is_none());
    let (unchanged_record, unchanged) = fixture
        .record(
            "agent.fleet_identity",
            json!({"token":full["result"]["token"]}),
        )
        .await;
    assert_eq!(unchanged["result"]["unchanged"], true);
    assert!(unchanged["result"].get("agents").is_none());
    let (unknown_record, unknown) = fixture
        .record("agent.fleet_identity", json!({"unknown":true}))
        .await;
    assert_eq!(unknown["code"], "invalid_request");
    published_file(
        "agent-identity-reads-v1",
        "fleet-identity.json",
        vec![full_record, unchanged_record, unknown_record],
    );

    let (all_record, all) = fixture
        .record("agent.avatar_read", json!({"agentIds":[HEAD,ASSISTANT]}))
        .await;
    assert_avatar_reply(
        &all["result"],
        &json!({"avatars":[
            {"agentId":HEAD,"genome":"a".repeat(2048),"type":"creature.classic","version":2},
            {"agentId":ASSISTANT,"genome":"b".repeat(2048),"type":"creature.classic"}
        ]}),
    );
    let (mixed_record, mixed) = fixture
        .record(
            "agent.avatar_read",
            json!({"agentIds":[ASSISTANT,UNKNOWN,NO_AVATAR,RETIRED,HEAD,ASSISTANT]}),
        )
        .await;
    // Avatar reads preserve duplicates and also serve stored terminal avatars;
    // fleet identity, in contrast, lists only live agents.
    assert_avatar_reply(
        &mixed["result"],
        &json!({"avatars":[
            {"agentId":ASSISTANT,"genome":"b".repeat(2048),"type":"creature.classic"},
            {"agentId":RETIRED,"genome":"c".repeat(2048),"type":"creature.classic","version":1},
            {"agentId":HEAD,"genome":"a".repeat(2048),"type":"creature.classic","version":2},
            {"agentId":ASSISTANT,"genome":"b".repeat(2048),"type":"creature.classic"}
        ]}),
    );
    let mut records = vec![all_record, mixed_record];
    for ids in [json!([]), json!(vec![HEAD; 65]), json!([""])] {
        let (record, error) = fixture
            .record("agent.avatar_read", json!({"agentIds":ids}))
            .await;
        assert_eq!(error["code"], "invalid_request");
        records.push(record);
    }
    published_file("agent-identity-reads-v1", "avatar-read.json", records);
}

const SESSION_AGENT: &str = "agent_0000000000000101";
const REF_OWNER: &str = "agent_0000000000000102";
const CLAIMED_AGENT: &str = "agent_0000000000000103";
const LEGACY_AGENT: &str = "agent_0000000000000104";
const WHERE_UNCLAIMED: &str = "agent_0000000000000105";
const WHERE_HERE: &str = "agent_0000000000000106";
const WHERE_ELSEWHERE: &str = "agent_0000000000000107";
const WHERE_MOVING: &str = "agent_0000000000000108";
const SESSION_REF: &str = "0123456789abcdef0123456789abcdef";
const OPENCODE_REF: &str = "abcdef0123456789abcdef0123456789";
const SELF_AUTHOR: &str = "1111111111111111";
const OTHER_AUTHOR: &str = "2222222222222222";
const PLEXUS_MODULE: &str = "plexus";
const SESSION_CORE: RouteKey = (904, 1);
const SESSION_REGISTRAR: RouteKey = (905, 1);
const SESSION_READER: RouteKey = (906, 1);
const SESSION_FORBIDDEN: RouteKey = (907, 1);

struct SessionFixture {
    dir: PathBuf,
    handler: ProjectsHandler,
}

impl SessionFixture {
    async fn new(label: &str) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let resolver = crate::sessions::FakeResolver::new();
        resolver.set(
            crate::sessions::ORCHESTRATOR_CAPABILITY,
            crate::sessions::ProviderResolution::Provider(WRITER_MODULE.into()),
        );
        resolver.set(
            crate::sessions::HOST_CAPABILITY,
            crate::sessions::ProviderResolution::Provider(WRITER_MODULE.into()),
        );
        let mut handler = ProjectsHandler::with_runtime(INCARNATION.into(), || 700);
        handler.provider_resolver = resolver;
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        install_store(
            &handler.store,
            &handler.health,
            &handler.catalog_sql,
            Ok(store),
        );
        for (route, module_id) in [
            (SESSION_CORE, WRITER_MODULE),
            (SESSION_REGISTRAR, WRITER_MODULE),
            (SESSION_READER, READER_MODULE),
            (SESSION_FORBIDDEN, PLEXUS_MODULE),
        ] {
            handler.route_admissions().insert(
                route,
                RouteAdmission::from_bind(Some(reserved(module_id)), None),
            );
        }
        Self { dir, handler }
    }

    fn snapshot(&self) -> Connection {
        let source = Connection::open(self.dir.join("session-snapshot.db")).unwrap();
        for migration in MIGRATIONS {
            source.execute_batch(migration).unwrap();
        }
        source
    }

    async fn import(&self) -> HandlerOutcome {
        let body = serde_json::to_vec(&json!({
            "method":"agent.import",
            "params":{
                "snapshot_path":self.dir.join("session-snapshot.db"),
                "request_key":"session-vector-import"
            }
        }))
        .unwrap();
        self.handler
            .handle_served_request(&body, SESSION_CORE)
            .await
    }

    fn store(&self) -> RegistryStore {
        self.handler.store.lock().unwrap().as_ref().unwrap().clone()
    }

    fn cache_claims(&self) {
        let store = self.store();
        let observations = [
            (CLAIMED_AGENT, OTHER_AUTHOR),
            (WHERE_HERE, SELF_AUTHOR),
            (WHERE_ELSEWHERE, OTHER_AUTHOR),
        ]
        .into_iter()
        .map(|(agent_id, holder)| {
            (
                agent_id.to_owned(),
                ClaimObservation::Present {
                    claim: CachedClaim {
                        holder: Some(holder.to_owned()),
                        epoch: 1,
                        cut_id: None,
                        version: 1,
                        read_at_ms: 700,
                        stale: false,
                        integrity_failed: false,
                    },
                    self_author: SELF_AUTHOR.to_owned(),
                },
            )
        })
        .collect::<Vec<_>>();
        store.cache_agent_claims(&observations, 700).unwrap();
        store
            .apply_entry("fixture.member_names", "{}", "fixture", None, |tx| {
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,?2)",
                    params![SELF_AUTHOR, "This Mac"],
                )?;
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,?2)",
                    params![OTHER_AUTHOR, "Other Mac"],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn seed_legacy_binding(&self) {
        self.store()
            .apply_entry("fixture.legacy_session", "{}", "fixture", None, |tx| {
                // Schema 8 rows predate project_root; migration 9 backfills an empty root for them.
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES(?1,?2,?3,?4,?5,'','host',700)",
                    params!["pi-session", "fedcba98765432100123456789abcdef", LEGACY_AGENT, "pi", "old-session"],
                )?;
                Ok(())
            })
            .unwrap();
    }

    async fn record(
        &self,
        route: RouteKey,
        principal: &str,
        method: &str,
        params: Value,
    ) -> (String, Value) {
        let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        let outcome = self.handler.handle_served_request(&request, route).await;
        let reply = match outcome {
            HandlerOutcome::Response(bytes) => String::from_utf8(bytes).unwrap(),
            refused => serde_json::to_string(&refusal(refused)).unwrap(),
        };
        let decoded = serde_json::from_str(&reply).unwrap();
        let record = format!(
            "  {{\"request\":{},\"principal\":{},\"reply\":{reply}}}",
            String::from_utf8(request).unwrap(),
            json!({"kind":"reserved","moduleId":principal}),
        );
        (record, decoded)
    }
}

impl Drop for SessionFixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn json_string_contents(value: &str) -> String {
    let encoded = serde_json::to_string(value).unwrap();
    encoded[1..encoded.len() - 1].to_owned()
}

fn normalize_project_record(
    mut record: String,
    scratch_root: &str,
    paths: &[(String, String)],
    implicit_ids: &[(String, String)],
    reply: &Value,
) -> String {
    for (actual, stable) in implicit_ids {
        record = record.replace(actual, stable);
    }
    if let Some(root_records) = reply
        .pointer("/result/rootRecords")
        .and_then(Value::as_array)
    {
        for root_record in root_records {
            if let Some(epoch) = root_record.get("registrationEpoch").and_then(Value::as_str) {
                record = record.replace(epoch, "00000000000000000000000000000001");
            }
        }
    }
    let mut paths = paths.to_vec();
    paths.sort_by_key(|(actual, _)| std::cmp::Reverse(actual.len()));
    for (actual, stable) in paths {
        record = record.replace(
            &json_string_contents(&actual),
            &json_string_contents(&stable),
        );
    }
    record.replace(
        &json_string_contents(scratch_root),
        &json_string_contents("/fixture"),
    )
}

#[tokio::test]
async fn project_resolve_v1_records_match_published_files() {
    let fixture = SessionFixture::new("served-project-resolve-v1").await;
    fixture
        .handler
        .store
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .set_root_records(false);

    let root = fixture.dir.join("root-checkout");
    std::fs::create_dir_all(root.join(".git")).unwrap();
    std::fs::write(root.join(".git/config"), "[core]\n\tbare = false\n").unwrap();
    let root = crate::tests::canonical_test_path(&root)
        .to_string_lossy()
        .into_owned();
    let subfolder = PathBuf::from(&root).join("subfolder");
    std::fs::create_dir_all(&subfolder).unwrap();
    let subfolder = crate::tests::canonical_test_path(subfolder)
        .to_string_lossy()
        .into_owned();

    let gone_root = fixture.dir.join("gone-checkout");
    std::fs::create_dir_all(gone_root.join(".git")).unwrap();
    std::fs::write(gone_root.join(".git/config"), "[core]\n\tbare = false\n").unwrap();
    let gone_root = crate::tests::canonical_test_path(&gone_root)
        .to_string_lossy()
        .into_owned();

    for (project_id, name, workspace_id, project_root) in [
        (
            "pj-resolve-root",
            "Resolve Root",
            "ws-resolve",
            root.as_str(),
        ),
        ("pj-gone-root", "Gone Root", "ws-gone", gone_root.as_str()),
    ] {
        let (_, reply) = fixture
            .record(
                SESSION_CORE,
                WRITER_MODULE,
                "register",
                json!({
                    "projectId":project_id,
                    "name":name,
                    "roots":[project_root],
                    "workspaceId":workspace_id
                }),
            )
            .await;
        assert!(
            reply.get("result").is_some(),
            "fixture project registration failed: {reply}"
        );
    }

    let worktree = fixture.dir.join("external-worktree");
    let admin = PathBuf::from(&root)
        .join(".git")
        .join("worktrees")
        .join("project-resolve-v1");
    std::fs::create_dir_all(&admin).unwrap();
    std::fs::write(admin.join("commondir"), "../..\n").unwrap();
    std::fs::create_dir_all(&worktree).unwrap();
    std::fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", admin.display()),
    )
    .unwrap();
    let worktree = crate::tests::canonical_test_path(worktree)
        .to_string_lossy()
        .into_owned();

    std::fs::remove_dir_all(PathBuf::from(&gone_root)).unwrap();

    let implicit = fixture.dir.join("implicit-folder");
    std::fs::create_dir_all(&implicit).unwrap();
    let implicit = crate::tests::canonical_test_path(implicit)
        .to_string_lossy()
        .into_owned();
    let alias_source = fixture.dir.join("alias-source");
    std::fs::create_dir_all(&alias_source).unwrap();
    let alias_source = crate::tests::canonical_test_path(alias_source)
        .to_string_lossy()
        .into_owned();
    let alias_id = entorhinal_core::implicit_project_id(&alias_source);
    let alias_stable_id = entorhinal_core::implicit_project_id("/fixture/alias-source");
    let (_, upgraded) = fixture
        .record(
            SESSION_CORE,
            WRITER_MODULE,
            "upgrade_implicit",
            json!({
                "implicitId":alias_id,
                "projectId":"pj-alias-target",
                "name":"Alias Target"
            }),
        )
        .await;
    assert!(
        upgraded.get("result").is_some(),
        "fixture alias creation failed: {upgraded}"
    );

    let fixed_token = "0123456789abcdef".repeat(4);
    std::fs::write(
        PathBuf::from(&root)
            .join(".git")
            .join("cortexkit-root-incarnation"),
        format!("{fixed_token}\n"),
    )
    .unwrap();
    {
        let mut store = fixture.handler.store.lock().unwrap();
        let store = store.as_mut().unwrap();
        store.set_root_records(true);
        store.bind_root(&root, "project-resolve-v1").unwrap();
    }

    let scratch_root = crate::tests::canonical_test_path(&fixture.dir)
        .to_string_lossy()
        .into_owned();
    let paths = [
        (root.clone(), "/fixture/root-checkout".to_string()),
        (
            subfolder.clone(),
            "/fixture/root-checkout/subfolder".to_string(),
        ),
        (worktree.clone(), "/fixture/external-worktree".to_string()),
        (implicit.clone(), "/fixture/implicit-folder".to_string()),
        (gone_root.clone(), "/fixture/gone-checkout".to_string()),
        (alias_source.clone(), "/fixture/alias-source".to_string()),
    ];
    let implicit_id = entorhinal_core::implicit_project_id(&implicit);
    let implicit_stable_id = entorhinal_core::implicit_project_id("/fixture/implicit-folder");
    let implicit_ids = [
        (implicit_id, implicit_stable_id),
        (alias_id.clone(), alias_stable_id),
    ];

    let mut resolve_records = Vec::new();
    for path in [&root, &subfolder, &worktree, &implicit, &gone_root] {
        let (record, reply) = fixture
            .record(
                SESSION_READER,
                READER_MODULE,
                "resolve",
                json!({"canonicalRoot":path}),
            )
            .await;
        resolve_records.push(normalize_project_record(
            record,
            &scratch_root,
            &paths,
            &implicit_ids,
            &reply,
        ));
    }
    let (malformed, malformed_reply) = fixture
        .record(SESSION_READER, READER_MODULE, "resolve", json!({}))
        .await;
    assert_eq!(malformed_reply["code"], "invalid_params");
    resolve_records.push(normalize_project_record(
        malformed,
        &scratch_root,
        &paths,
        &implicit_ids,
        &malformed_reply,
    ));
    published_file("project-resolve-v1", "resolve.json", resolve_records);

    let mut project_id_records = Vec::new();
    for project_id in ["pj-resolve-root", alias_id.as_str(), "pj-no-such-project"] {
        let (record, reply) = fixture
            .record(
                SESSION_READER,
                READER_MODULE,
                "resolve_project_id",
                json!({"projectId":project_id}),
            )
            .await;
        project_id_records.push(normalize_project_record(
            record,
            &scratch_root,
            &paths,
            &implicit_ids,
            &reply,
        ));
    }
    published_file(
        "project-resolve-v1",
        "resolve-project-id.json",
        project_id_records,
    );

    let enumeration = SessionFixture::new("served-project-enumerate-v1").await;
    for (project_id, name) in [
        ("pj-dashboard-editor", "Editor"),
        ("pj-dashboard-terminal", "Terminal"),
    ] {
        let (_, reply) = enumeration
            .record(
                SESSION_CORE,
                WRITER_MODULE,
                "register",
                json!({
                    "projectId":project_id,
                    "name":name,
                    "roots":[],
                    "workspaceId":"ws-dashboard"
                }),
            )
            .await;
        assert!(
            reply.get("result").is_some(),
            "fixture project registration failed: {reply}"
        );
    }
    enumeration
        .handler
        .store
        .lock()
        .unwrap()
        .as_mut()
        .unwrap()
        .set_root_records(true);
    let (record, _) = enumeration
        .record(SESSION_READER, READER_MODULE, "enumerate", json!({}))
        .await;
    published_file("project-resolve-v1", "enumerate.json", vec![record]);
}

#[tokio::test]
async fn session_records_match_published_files() {
    let fixture = SessionFixture::new("served-sessions").await;
    let source = fixture.snapshot();
    for (id, name) in [
        (SESSION_AGENT, "Session Agent"),
        (REF_OWNER, "Reference Owner"),
        (CLAIMED_AGENT, "Claimed Agent"),
        (LEGACY_AGENT, "Legacy Agent"),
        (WHERE_UNCLAIMED, "Unclaimed Agent"),
        (WHERE_HERE, "Here Agent"),
        (WHERE_ELSEWHERE, "Elsewhere Agent"),
    ] {
        seed_agent(&source, id, name, None, false, false);
    }
    drop(source);
    let imported = fixture.import().await;
    let HandlerOutcome::Response(imported) = imported else {
        panic!("session-vector import refused: {}", refusal(imported));
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&imported).unwrap()["result"]["agents_imported"],
        7
    );
    fixture.cache_claims();
    fixture.seed_legacy_binding();

    let registration = json!({
        "agent_id":SESSION_AGENT,
        "ref_kind":"pi-session",
        "ref":SESSION_REF,
        "harness":"pi",
        "session_id":"session-001",
        "project_root":"/workspace/session-vectors"
    });
    let (register_record, registered) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            registration.clone(),
        )
        .await;
    assert_eq!(registered["result"]["registered_by"], "orchestrator");
    assert_eq!(registered["result"]["registered_at_ms"], 700);
    let re_registration = json!({
        "agent_id":SESSION_AGENT,
        "ref_kind":"pi-session",
        "ref":SESSION_REF,
        "harness":"pi",
        "session_id":"session-002",
        "project_root":"/workspace/session-vectors/reopened"
    });
    let (reregister_record, reregistered) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            re_registration,
        )
        .await;
    assert_eq!(reregistered["result"]["session_id"], "session-002");

    let opencode_registration = json!({
        "agent_id":SESSION_AGENT,
        "ref_kind":"opencode-session",
        "ref":OPENCODE_REF,
        "harness":"opencode",
        "session_id":"opencode-session-001",
        "project_root":"/workspace/session-vectors/opencode"
    });
    let (opencode_record, opencode_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            opencode_registration,
        )
        .await;
    assert_eq!(opencode_reply["result"]["registered_by"], "orchestrator");

    let mut ref_collision = registration.clone();
    ref_collision["agent_id"] = json!(REF_OWNER);
    ref_collision["session_id"] = json!("owner-session");
    let (ref_collision_record, ref_collision_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            ref_collision,
        )
        .await;
    assert_eq!(ref_collision_reply["code"], "ref_bound_elsewhere");

    let mut held_collision = registration.clone();
    held_collision["agent_id"] = json!(CLAIMED_AGENT);
    held_collision["ref"] = json!("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    held_collision["session_id"] = json!("held-session");
    let (held_collision_record, held_collision_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            held_collision,
        )
        .await;
    assert_eq!(held_collision_reply["code"], "held_elsewhere");

    let (forbidden_record, forbidden_reply) = fixture
        .record(
            SESSION_FORBIDDEN,
            PLEXUS_MODULE,
            "agent.register_session",
            registration.clone(),
        )
        .await;
    assert_eq!(forbidden_reply["code"], "caller_forbidden");

    let mut unknown_field = registration;
    unknown_field["unexpected"] = json!(true);
    let (unknown_field_record, unknown_field_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            unknown_field,
        )
        .await;
    assert_eq!(unknown_field_reply["code"], "invalid_request");
    published_file(
        "agent-sessions-v1",
        "register-session.json",
        vec![
            register_record,
            reregister_record,
            opencode_record,
            ref_collision_record,
            held_collision_record,
            forbidden_record,
            unknown_field_record,
        ],
    );

    let (for_session_found, found_session) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.for_session",
            json!({"harness":"pi","session_id":"session-002"}),
        )
        .await;
    assert_eq!(found_session["result"]["agent_id"], SESSION_AGENT);
    let (opencode_for_session, opencode_session) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.for_session",
            json!({"harness":"opencode","session_id":"opencode-session-001"}),
        )
        .await;
    assert_eq!(opencode_session["result"]["agent_id"], SESSION_AGENT);
    let (for_session_missing, missing_session) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.for_session",
            json!({"harness":"pi","session_id":"not-registered"}),
        )
        .await;
    assert_eq!(missing_session["code"], "not_found");
    published_file(
        "agent-sessions-v1",
        "for-session.json",
        vec![for_session_found, opencode_for_session, for_session_missing],
    );

    let (for_ref_found, found_ref) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.session_for_ref",
            json!({"ref_kind":"pi-session","ref":SESSION_REF}),
        )
        .await;
    let (opencode_for_ref, opencode_ref_reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.session_for_ref",
            json!({"ref_kind":"opencode-session","ref":OPENCODE_REF}),
        )
        .await;
    let (legacy_ref, legacy_reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.session_for_ref",
            json!({"ref_kind":"pi-session","ref":"fedcba98765432100123456789abcdef"}),
        )
        .await;
    assert!(legacy_reply["result"]["project_root"].is_null());
    let (for_ref_missing, missing_ref) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.session_for_ref",
            json!({"ref_kind":"pi-session","ref":"no-such-reference"}),
        )
        .await;
    assert_eq!(missing_ref["code"], "not_found");
    published_file(
        "agent-sessions-v1",
        "session-for-ref.json",
        vec![for_ref_found, opencode_for_ref, legacy_ref, for_ref_missing],
    );
    assert_eq!(
        found_ref["result"]["project_root"],
        "/workspace/session-vectors/reopened"
    );
    assert_eq!(
        opencode_ref_reply["result"]["project_root"],
        "/workspace/session-vectors/opencode"
    );

    // On a Mac that has never turned sharing on, no other Mac can hold an
    // agent, so an agent with no cached claim is a verified "unclaimed".
    // Core's head registration accepts exactly this reply.
    let (sharing_off, sharing_off_reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.where",
            json!({"agent_id":WHERE_UNCLAIMED}),
        )
        .await;
    assert_eq!(
        sharing_off_reply["result"],
        json!({"known":true,"status":"unclaimed","holder":null,"holder_name":null,"epoch":0,"cut_id":null,"read_at_ms":null,"stale":false})
    );
    published_file(
        "agent-sessions-v1",
        "where-sharing-off.json",
        vec![sharing_off],
    );
    // The remaining location replies describe a Mac with sharing on, where only
    // verified engram reads count and an unread claim is unknown.
    fixture
        .store()
        .apply_entry("fixture.sharing_on", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let mut where_records = Vec::new();
    for (agent_id, status, holder_name) in [
        (WHERE_UNCLAIMED, "unclaimed", Value::Null),
        (WHERE_HERE, "held_here", json!("This Mac")),
        (WHERE_ELSEWHERE, "held_elsewhere", json!("Other Mac")),
    ] {
        let (record, answer) = fixture
            .record(
                SESSION_READER,
                READER_MODULE,
                "agent.where",
                json!({"agent_id":agent_id}),
            )
            .await;
        assert_eq!(answer["result"]["status"], status);
        assert_eq!(answer["result"]["holder_name"], holder_name);
        where_records.push(record);
    }
    let (unknown_agent, unknown_agent_reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.where",
            json!({"agent_id":"agent_ffffffffffffffff"}),
        )
        .await;
    assert_eq!(unknown_agent_reply["code"], "unknown_agent");
    where_records.push(unknown_agent);
    published_file("agent-sessions-v1", "where.json", where_records);

    let (where_many, where_many_reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.where_many",
            json!({"agentIds":[WHERE_ELSEWHERE,"agent_ffffffffffffffff",WHERE_HERE]}),
        )
        .await;
    assert_eq!(
        where_many_reply["result"]["agents"]
            .as_array()
            .unwrap()
            .iter()
            .map(|agent| agent["agent_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        [WHERE_ELSEWHERE, WHERE_HERE]
    );
    assert_eq!(
        where_many_reply["result"]["agents"][0]["status"],
        "held_elsewhere"
    );
    published_file("agent-sessions-v1", "where-many.json", vec![where_many]);
}

#[tokio::test]
async fn session_rekey_records_match_published_files() {
    let fixture = SessionFixture::new("served-session-rekey").await;
    let source = fixture.snapshot();
    seed_agent(&source, SESSION_AGENT, "Session Agent", None, false, false);
    seed_agent(&source, REF_OWNER, "Reference Owner", None, false, false);
    drop(source);
    assert!(matches!(
        fixture.import().await,
        HandlerOutcome::Response(_)
    ));
    let mut records = Vec::new();
    let initial = json!({
        "agent_id":SESSION_AGENT, "ref_kind":"pi-session", "ref":SESSION_REF,
        "harness":"pi", "session_id":"session-rekey",
        "project_root":"/workspace/session-vectors", "runner":"pi-runner", "network":"yes"
    });
    let (record, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            initial.clone(),
        )
        .await;
    assert_eq!(reply["result"]["ref"], SESSION_REF);
    records.push(record);
    let mut rekey = initial;
    rekey["ref"] = json!("runner-session-ref");
    for field in ["project_root", "runner", "network"] {
        rekey.as_object_mut().unwrap().remove(field);
    }
    let (record, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            rekey.clone(),
        )
        .await;
    assert_eq!(reply["result"]["ref"], "runner-session-ref");
    assert_eq!(
        reply["result"]["project_root"],
        "/workspace/session-vectors"
    );
    records.push(record);
    rekey["agent_id"] = json!(REF_OWNER);
    rekey["ref"] = json!("another-agent-ref");
    let (record, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            rekey,
        )
        .await;
    assert_eq!(reply["code"], "session_bound_elsewhere");
    records.push(record);
    let rootless = json!({
        "agent_id":SESSION_AGENT, "ref_kind":"pi-session", "ref":"rootless-ref",
        "harness":"pi", "session_id":"rootless-session", "runner":"pi-runner"
    });
    let (record, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            rootless.clone(),
        )
        .await;
    assert_eq!(reply["result"].get("project_root"), Some(&Value::Null));
    records.push(record);
    published_file("agent-sessions-v1", "register-session-rekey.json", records);

    for (method, params) in [
        (
            "agent.session_for_ref",
            json!({"ref_kind":"pi-session", "ref":"rootless-ref"}),
        ),
        (
            "agent.session_for_runner",
            json!({"runner":"pi-runner", "session":"rootless-session"}),
        ),
    ] {
        let (_, reply) = fixture
            .record(SESSION_READER, READER_MODULE, method, params)
            .await;
        assert!(reply.get("result").is_some(), "{reply}");
        assert_eq!(reply["result"].get("project_root"), Some(&Value::Null));
    }
    let (_, page) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.sessions",
            json!({"agent_id":SESSION_AGENT}),
        )
        .await;
    let sessions = page["result"]["sessions"].as_array().unwrap();
    assert_eq!(
        sessions
            .iter()
            .find(|row| row["ref"] == "rootless-ref")
            .unwrap()
            .get("project_root"),
        Some(&Value::Null)
    );
    let known = sessions
        .iter()
        .find(|row| row["ref"] == "runner-session-ref")
        .unwrap();
    assert_eq!(known["project_root"], "/workspace/session-vectors");
    assert_eq!(known["runner"], "pi-runner");
    assert_eq!(known["network"], "yes");

    for invalid in [Value::Null, json!(""), json!(1), json!("x".repeat(4097))] {
        let mut params = rootless.clone();
        params["project_root"] = invalid;
        let (_, reply) = fixture
            .record(
                SESSION_REGISTRAR,
                WRITER_MODULE,
                "agent.register_session",
                params,
            )
            .await;
        assert_eq!(reply["code"], "invalid_request");
    }
}

#[tokio::test]
async fn moving_agent_where_records_match_published_files() {
    const CUT_ID: &str = "cut-moving-agent";

    for (side, name) in [
        (MoveSide::Source, "where-moving-source.json"),
        (MoveSide::Receiver, "where-moving-receiver.json"),
    ] {
        let label = match side {
            MoveSide::Source => "served-where-moving-source",
            MoveSide::Receiver => "served-where-moving-receiver",
        };
        let fixture = SessionFixture::new(label).await;
        let source = fixture.snapshot();
        seed_agent(&source, WHERE_MOVING, "Moving Agent", None, false, false);
        drop(source);

        let imported = fixture.import().await;
        let HandlerOutcome::Response(imported) = imported else {
            panic!(
                "moving session-vector import refused: {}",
                refusal(imported)
            );
        };
        assert_eq!(
            serde_json::from_slice::<Value>(&imported).unwrap()["result"]["agents_imported"],
            1
        );
        fixture.cache_claims();
        fixture
            .store()
            .apply_entry("fixture.sharing_on", "{}", "fixture", None, |tx| {
                tx.execute(
                    "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();

        let epoch = if side == MoveSide::Source { 1 } else { 2 };
        let claim = CachedClaim {
            holder: Some(SELF_AUTHOR.to_owned()),
            epoch,
            cut_id: (side == MoveSide::Receiver).then(|| CUT_ID.to_owned()),
            version: epoch,
            read_at_ms: 700,
            stale: false,
            integrity_failed: false,
        };
        fixture
            .store()
            .cache_agent_claims(
                &[(
                    WHERE_MOVING.to_owned(),
                    ClaimObservation::Present {
                        claim,
                        self_author: SELF_AUTHOR.to_owned(),
                    },
                )],
                700,
            )
            .unwrap();

        let mut row = AgentMove {
            cut_id: CUT_ID.into(),
            agent_id: WHERE_MOVING.into(),
            side,
            peer: OTHER_AUTHOR.into(),
            step: if side == MoveSide::Source {
                MoveStep::Published
            } else {
                MoveStep::PickedUp
            },
            plan: if side == MoveSide::Source {
                json!({"_source":{"published_at_ms":650}})
            } else {
                json!({"_receiver":{}})
            },
            request_body: Some(json!({"agent_id":WHERE_MOVING,"to":"Other Mac"})),
            record_version: 0,
            claim_version: Some(epoch),
            claim_take_body: None,
            claim_take_version: None,
            claim_landed_version: None,
            seal_sent: false,
            abort_requested: None,
            hold: None,
            cut_accept: None,
            reason: None,
            detail: None,
            request_key: None,
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 600,
            updated_at_ms: 650,
        };
        let store = fixture.store();
        if side == MoveSide::Source {
            store.insert_agent_move(&row).unwrap();
        } else {
            assert_eq!(store.admit_received_move(&row).unwrap(), row);
            row.step = MoveStep::Claimed;
            assert!(store.update_agent_move(&row).unwrap());
        }

        let (record, answer) = fixture
            .record(
                SESSION_READER,
                READER_MODULE,
                "agent.where",
                json!({"agent_id":WHERE_MOVING}),
            )
            .await;
        assert_eq!(answer["result"]["status"], "held_here");
        assert_eq!(
            answer["result"]["moving"]["side"],
            if side == MoveSide::Source {
                "source"
            } else {
                "receiver"
            }
        );
        assert_eq!(
            answer["result"]["moving"]["step"],
            if side == MoveSide::Source {
                "published"
            } else {
                "claimed"
            }
        );
        if side == MoveSide::Source {
            assert_eq!(answer["result"]["moving"]["waiting_ms"], 50);
        } else {
            assert!(answer["result"]["moving"]["waiting_ms"].is_null());
        }
        published_file("agent-sessions-v1", name, vec![record]);
    }
}

/// The reference of a live Broca worker session in the lifecycle samples.
const LIVE_WORKER_REF: &str = "11111111111111111111111111111111";
/// The reference of an ended Broca worker session in the lifecycle samples.
const ENDED_WORKER_REF: &str = "22222222222222222222222222222222";
/// The reference of a Pi session registered without a runner.
const NO_RUNNER_REF: &str = "33333333333333333333333333333333";

/// Served samples for the session lifecycle: a registration naming its
/// runner, `agent.end_session` replies, `agent.set_session_network` replies,
/// and an agent's two-page session list with a live worker whose network
/// went from `yes` to `no`, an ended worker and a row with no runner, both
/// with no network history.
#[tokio::test]
async fn session_lifecycle_records_match_published_files() {
    let mut fixture = SessionFixture::new("served-session-lifecycle").await;
    let source = fixture.snapshot();
    seed_agent(&source, SESSION_AGENT, "Session Agent", None, false, false);
    drop(source);
    let imported = fixture.import().await;
    let HandlerOutcome::Response(_) = imported else {
        panic!("lifecycle import refused: {}", refusal(imported));
    };

    let worker = |session_ref: &str, session_id: &str| {
        json!({
            "agent_id":SESSION_AGENT,
            "ref_kind":"broca-session",
            "ref":session_ref,
            "harness":"broca",
            "session_id":session_id,
            "project_root":"/workspace/session-vectors",
            "runner":"broca"
        })
    };
    let (register_record, registered) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            worker(LIVE_WORKER_REF, "broca-live"),
        )
        .await;
    assert_eq!(registered["result"]["registered_by"], "orchestrator");
    assert!(registered["result"].get("runner").is_none());
    published_file(
        "agent-sessions-v1",
        "register-session-runner.json",
        vec![register_record],
    );
    for params in [
        worker(ENDED_WORKER_REF, "broca-ended"),
        json!({
            "agent_id":SESSION_AGENT,
            "ref_kind":"pi-session",
            "ref":NO_RUNNER_REF,
            "harness":"pi",
            "session_id":"pi-session-001",
            "project_root":"/workspace/session-vectors"
        }),
    ] {
        let (_, reply) = fixture
            .record(
                SESSION_REGISTRAR,
                WRITER_MODULE,
                "agent.register_session",
                params,
            )
            .await;
        assert!(reply.get("result").is_some(), "{reply}");
    }

    let mut end_records = Vec::new();
    for (params, expected) in [
        (
            json!({"harness":"broca","session":"broca-ended","runner":"broca"}),
            json!({"result":{"found":true,"ended_at_ms":700}}),
        ),
        (
            json!({"harness":"broca","session":"broca-ended","runner":"broca"}),
            json!({"result":{"found":true,"ended_at_ms":700}}),
        ),
        (
            json!({"harness":"broca","session":"never-registered"}),
            json!({"result":{"found":false}}),
        ),
    ] {
        let (record, reply) = fixture
            .record(
                SESSION_REGISTRAR,
                WRITER_MODULE,
                "agent.end_session",
                params,
            )
            .await;
        assert_eq!(reply, expected);
        end_records.push(record);
    }
    let (mismatch, mismatch_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.end_session",
            json!({"harness":"broca","session":"broca-live","runner":"thalamus-harness"}),
        )
        .await;
    assert_eq!(mismatch_reply["code"], "runner_mismatch");
    end_records.push(mismatch);
    published_file("agent-sessions-v1", "end-session.json", end_records);

    // The live worker's network: `yes`, the same value again (nothing
    // written, the first entry's time), then `no` once the clock moved on.
    let network = |session: &str, network: &str| json!({"harness":"broca","session":session,"network":network});
    let mut network_records = Vec::new();
    for (params, expected) in [
        (
            network("broca-live", "yes"),
            json!({"result":{"network":"yes","since_ms":700}}),
        ),
        (
            network("broca-live", "yes"),
            json!({"result":{"network":"yes","since_ms":700}}),
        ),
    ] {
        let (record, reply) = fixture
            .record(
                SESSION_REGISTRAR,
                WRITER_MODULE,
                "agent.set_session_network",
                params,
            )
            .await;
        assert_eq!(reply, expected);
        network_records.push(record);
    }
    fixture.handler.clock = || 710;
    let (record, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.set_session_network",
            network("broca-live", "no"),
        )
        .await;
    assert_eq!(reply, json!({"result":{"network":"no","since_ms":710}}));
    network_records.push(record);

    // The host reports the live worker active; the others are not mirrored.
    let (_, fed) = fixture
        .record(
            SESSION_CORE,
            WRITER_MODULE,
            "projects.session_liveness",
            json!({"seq":1,"snapshot":true,"sessions":[
                {"harness":"broca","sessionId":"broca-live","canonicalRoot":"/workspace/session-vectors","state":"active","runner":"broca"}]}),
        )
        .await;
    assert_eq!(fed["result"]["accepted"], true);
    let (first_page, first) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.sessions",
            json!({"agent_id":SESSION_AGENT,"limit":2}),
        )
        .await;
    let liveness = |page: &Value| {
        page["result"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .map(|row| (row["ref"].clone(), row["liveness"].clone()))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        liveness(&first),
        [
            (json!(LIVE_WORKER_REF), json!("active")),
            (json!(ENDED_WORKER_REF), json!("gone"))
        ]
    );
    let cursor = first["result"]["next_cursor"].clone();
    assert!(cursor.is_string());
    let (second_page, second) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.sessions",
            json!({"agent_id":SESSION_AGENT,"cursor":cursor,"limit":2}),
        )
        .await;
    assert_eq!(
        liveness(&second),
        [(json!(NO_RUNNER_REF), json!("unknown"))]
    );
    assert!(second["result"]["sessions"][0]["runner"].is_null());
    assert!(second["result"]["next_cursor"].is_null());
    published_file(
        "agent-sessions-v1",
        "sessions.json",
        vec![first_page, second_page],
    );

    // A runner's session id resolves to its binding; an unknown pair, and a
    // session registered without a runner, are `not_found` like
    // `agent.for_session`'s miss.
    let mut runner_records = Vec::new();
    for (params, found) in [
        (json!({"runner":"broca","session":"broca-live"}), true),
        (json!({"runner":"broca","session":"broca-ended"}), true),
        (
            json!({"runner":"broca","session":"never-registered"}),
            false,
        ),
        (json!({"runner":"broca","session":"pi-session-001"}), false),
    ] {
        let (record, reply) = fixture
            .record(
                SESSION_READER,
                READER_MODULE,
                "agent.session_for_runner",
                params,
            )
            .await;
        if found {
            assert_eq!(reply["result"]["harness"], "broca");
        } else {
            assert_eq!(reply["code"], "not_found");
            assert!(reply["detail"].is_null());
        }
        runner_records.push(record);
    }
    assert_eq!(
        runner_records.len(),
        4,
        "live, ended, unknown and runner-less lookups"
    );
    published_file(
        "agent-sessions-v1",
        "session-for-runner.json",
        runner_records,
    );

    let mut open_records = Vec::new();
    let mut cursor = Value::Null;
    loop {
        let mut request = json!({"agent_id":SESSION_AGENT,"open":true,"limit":1});
        if !cursor.is_null() {
            request["cursor"] = cursor;
        }
        let (record, reply) = fixture
            .record(SESSION_READER, READER_MODULE, "agent.sessions", request)
            .await;
        assert!(reply["result"]["sessions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|row| row["ended_at_ms"].is_null()));
        cursor = reply["result"]["next_cursor"].clone();
        open_records.push(record);
        if cursor.is_null() {
            break;
        }
    }
    published_file("agent-sessions-v1", "sessions-open.json", open_records);

    fixture
        .handler
        .with_store(|store| {
            store.apply_entry("fixture.remote_finished", "{}", "fixture", None, |tx| {
                tx.execute(
                    "UPDATE session_binding SET held_on='source-mac' WHERE ref=?1",
                    [ENDED_WORKER_REF],
                )?;
                Ok(())
            })
        })
        .unwrap();
    let (listed, reply) = fixture
        .record(
            SESSION_READER,
            READER_MODULE,
            "agent.sessions",
            json!({"agent_id":SESSION_AGENT}),
        )
        .await;
    assert_eq!(reply["result"]["sessions"][1]["heldOn"], "source-mac");
    let (refused, reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.register_session",
            worker(ENDED_WORKER_REF, "broca-ended"),
        )
        .await;
    assert_eq!(reply["code"], "session_on_other_machine");
    published_file(
        "agent-sessions-v1",
        "sessions-held-on.json",
        vec![listed, refused],
    );

    // Fill the ended worker's history to the cap through the handler, after
    // the reads above so they show it with no history. The next different
    // value is refused with nothing written.
    for entry in 0..entorhinal_core::NETWORK_HISTORY_MAX {
        let (_, reply) = fixture
            .record(
                SESSION_REGISTRAR,
                WRITER_MODULE,
                "agent.set_session_network",
                network("broca-ended", if entry % 2 == 0 { "yes" } else { "no" }),
            )
            .await;
        assert!(reply.get("result").is_some(), "{reply}");
    }
    let (full, full_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.set_session_network",
            network("broca-ended", "yes"),
        )
        .await;
    assert_eq!(full_reply["code"], "network_history_full");
    network_records.push(full);
    let (unknown, unknown_reply) = fixture
        .record(
            SESSION_REGISTRAR,
            WRITER_MODULE,
            "agent.set_session_network",
            network("never-registered", "no"),
        )
        .await;
    assert_eq!(unknown_reply["code"], "not_found");
    network_records.push(unknown);
    published_file(
        "agent-sessions-v1",
        "set-session-network.json",
        network_records,
    );
}

/// The SHA-256 of each agent-sessions sample whose bytes must not change.
/// Core decodes these replies strictly, so later additions (the session
/// lifecycle, then network access) must not change a byte of them. The last
/// two were added with the lifecycle and pinned when network access was.
const PINNED_SESSION_SAMPLES: [(&str, &str); 10] = [
    (
        "for-session.json",
        "dd41914934a9b85b35670a815bef17de84752d8da7d05a1bf8833f3edbd1996b",
    ),
    (
        "register-session.json",
        "20015ad11322866712bf4e43666ba3356c87700b3905884b8deed67fb3d1edbb",
    ),
    (
        "session-for-ref.json",
        "f8314b13df331e185f2c4960a314a3e8fac97af5d8e9904150ba024973fbb69a",
    ),
    (
        "where.json",
        "da88d465b4a190dbf14f7224ed8fd4a6cae10a8639fbbc7953b7d8b1f11dce8d",
    ),
    (
        "where-many.json",
        "93cc230f690999df6e7876f0bcabeaf391e5d8c9c7166e16a61d4c9112c153e6",
    ),
    (
        "where-moving-receiver.json",
        "ed04f74915d7f552f2472575a5786beecb2354e42077b29db5200b23e17e2ac0",
    ),
    (
        "where-moving-source.json",
        "3f2dc20b7216d6da8efe89cd9315bab65fbda232faf89191bea27fe84e2da00f",
    ),
    (
        "where-sharing-off.json",
        "0b8fe82664439e2eb38e0816f900c08f7822bdfe29373a77579cfbbd080c6b63",
    ),
    (
        "end-session.json",
        "efd3a0b6c6ac2463ec20e3be9ee26593ad3fd0a4a62c3457ad65f5e876ff6d18",
    ),
    (
        "register-session-runner.json",
        "8ec0505594c6336a924595edf8af73304f1e2bd4d80f197b26b8e18628a4dca3",
    ),
];

/// The earlier samples keep their pinned bytes, and `SHA256SUMS` names every
/// JSON sample in the directory with its current hash.
#[test]
fn agent_session_samples_keep_their_pinned_bytes_and_checksums() {
    use sha2::{Digest, Sha256};
    let directory =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/agent-sessions-v1");
    let hash = |name: &str| {
        format!(
            "{:x}",
            Sha256::digest(std::fs::read(directory.join(name)).unwrap())
        )
    };
    for (name, pinned) in PINNED_SESSION_SAMPLES {
        assert_eq!(hash(name), pinned, "{name} changed");
    }
    let sums = std::fs::read_to_string(directory.join("SHA256SUMS")).unwrap();
    let mut listed = Vec::new();
    for line in sums.lines() {
        let (sum, name) = line.split_once("  ").expect("a SHA256SUMS line");
        assert_eq!(hash(name), sum, "SHA256SUMS is stale for {name}");
        listed.push(name.to_owned());
    }
    let mut samples: Vec<String> = std::fs::read_dir(&directory)
        .unwrap()
        .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
        .filter(|name| name.ends_with(".json"))
        .collect();
    samples.sort();
    listed.sort();
    assert_eq!(listed, samples, "SHA256SUMS lists every sample");
}

#[tokio::test]
async fn avatar_read_matches_core_contract_v1_0() {
    let core: Value = serde_json::from_str(include_str!(
        "../tests/fixtures/core-avatar-read-vectors-v1.0.json"
    ))
    .unwrap();
    assert_eq!(core["version"], "avatar-read-vectors-v1.0");
    let vectors = core["vectors"].as_array().unwrap();
    assert!(!vectors.is_empty());
    let mut ids = BTreeMap::new();
    let mut original_ids = BTreeMap::new();
    for vector in vectors {
        for id in vector["request"]["agentIds"].as_array().unwrap() {
            let id = id.as_str().unwrap();
            let mapped = map_core_agent_id(id);
            if let Some(previous) = original_ids.insert(mapped.clone(), id) {
                assert_eq!(
                    previous, id,
                    "core id mapping is not one-to-one: {previous} and {id} map to {mapped}"
                );
            }
            ids.insert(id, mapped);
        }
    }
    let mut avatars = BTreeMap::new();
    for vector in vectors {
        for avatar in vector["reply"]["avatars"].as_array().unwrap() {
            let id = avatar["agentId"].as_str().unwrap();
            assert!(
                ids.contains_key(id),
                "core vector {} has an unmapped reply agentId {id}",
                vector["name"]
            );
            if let Some(previous) = avatars.insert(id, avatar) {
                assert_eq!(previous, avatar, "conflicting core triples for {id}");
            }
        }
    }
    assert!(!avatars.is_empty());
    let fixture = Fixture::new("core-avatar-contract").await;
    let source = fixture.snapshot();
    for (index, (id, avatar)) in avatars.iter().enumerate() {
        seed_agent(
            &source,
            &ids[id],
            &format!("Avatar {index}"),
            Some(avatar),
            false,
            false,
        );
    }
    // The absent id represents a known row without an avatar, while the unknown
    // id remains unseeded. Both omissions must survive the mapped request.
    let absent = ids.get("agent-avatar-absent");
    if let Some(id) = absent {
        seed_agent(&source, id, "No avatar", None, false, false);
    }
    drop(source);
    let imported = fixture.import().await;
    let HandlerOutcome::Response(bytes) = imported else {
        panic!("mapped core-avatar import refused: {}", refusal(imported));
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["agents_imported"],
        avatars.len() + usize::from(absent.is_some())
    );
    for vector in vectors {
        let mut request = vector["request"].clone();
        for id in request["agentIds"].as_array_mut().unwrap() {
            let mapped = ids.get(id.as_str().unwrap()).unwrap_or_else(|| {
                panic!(
                    "core vector {} uses an unmapped request id {id}",
                    vector["name"]
                )
            });
            *id = json!(mapped);
        }
        let mut actual = fixture.success(READER, "agent.avatar_read", request).await;
        // Avatar reads echo the request id. Reverse only that echo, leaving
        // genome, type, version, order, omissions, and reply keys untouched.
        for avatar in actual["avatars"].as_array_mut().unwrap() {
            let mapped = avatar["agentId"].as_str().unwrap();
            let id = original_ids.get(mapped).unwrap_or_else(|| {
                panic!(
                    "core vector {} returned an unmapped agentId {mapped}",
                    vector["name"]
                )
            });
            avatar["agentId"] = json!(id);
        }
        assert_avatar_reply(&actual, &vector["reply"]);
    }
}

fn map_core_agent_id(id: &str) -> String {
    match id {
        "agent-avatar-absent" => "agent_fffffffffffffffe".into(),
        "agent-avatar-unknown" => "agent_ffffffffffffffff".into(),
        _ => {
            let number = id.strip_prefix("agent-avatar-").filter(|number| {
                !number.is_empty() && number.bytes().all(|byte| byte.is_ascii_digit())
            }).unwrap_or_else(|| panic!("cannot map core agent id {id}: expected agent-avatar-NN, agent-avatar-absent, or agent-avatar-unknown"));
            let number: u64 = number.parse().unwrap_or_else(|error| {
                panic!("cannot map core agent id {id}: numeric suffix must fit u64 ({error})")
            });
            format!("agent_{number:016x}")
        }
    }
}

pub(super) struct Approve {
    pub(super) summaries: std::sync::Mutex<Vec<String>>,
}

#[async_trait::async_trait]
impl crate::agent_ops::OperatorConfirmer for Approve {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), crate::agent_ops::OperatorConfirmError> {
        self.summaries.lock().unwrap().push(summary.into());
        Ok(())
    }
}

#[tokio::test]
async fn served_changes_match_published_file() {
    let confirmer = std::sync::Arc::new(Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    let fixture = Fixture::with_confirmer("served-changes", confirmer.clone()).await;
    fixture.success(DIRECT, "register", json!({"projectId":"hiree-project","name":"Hiree project","roots":[],"workspaceId":"served-workspace"})).await;
    fixture.success(DIRECT, "register", json!({"projectId":"clear-project","name":"Clear project","roots":[],"workspaceId":"served-workspace"})).await;
    let source = fixture.snapshot();
    seed_agent(&source, HEAD, "Off placement", None, false, true);
    seed_agent(&source, NO_AVATAR, "Stored workspace", None, false, false);
    seed_agent(&source, ASSISTANT, "Live hiree", None, false, false);
    source
        .execute(
            "UPDATE agent SET workspace_id='served-project' WHERE agent_id=?1",
            [HEAD],
        )
        .unwrap();
    source
        .execute(
            "UPDATE agent_name_claim SET namespace_key='served-project' WHERE agent_id=?1",
            [HEAD],
        )
        .unwrap();
    source
        .execute(
            "UPDATE agent SET role='head',project_id='clear-project',workspace_id='served-workspace' WHERE agent_id=?1",
            [NO_AVATAR],
        )
        .unwrap();
    source
        .execute(
            "UPDATE agent SET role='hiree',project_id='hiree-project' WHERE agent_id=?1",
            [ASSISTANT],
        )
        .unwrap();
    source.execute("UPDATE agent_name_claim SET namespace_kind='workspace',namespace_key='served-workspace' WHERE agent_id IN (?1,?2)", params![NO_AVATAR,ASSISTANT]).unwrap();
    // Every agent create and tag change refuses an empty tag, and core's
    // readers reject one too, so these published entries carry real tags.
    for (id, tag) in [
        (HEAD, "Head whose name sits off its project's placement"),
        (NO_AVATAR, "Head that still stores its workspace"),
        (ASSISTANT, "Hiree carried with its project"),
    ] {
        source
            .execute(
                "UPDATE agent SET tag=?2 WHERE agent_id=?1",
                params![id, tag],
            )
            .unwrap();
    }
    drop(source);
    assert!(matches!(
        fixture.import().await,
        HandlerOutcome::Response(_)
    ));
    let cursor = fixture
        .success(
            READER,
            "agent.changes",
            json!({"incarnation":INCARNATION,"cursor":0,"wait":false}),
        )
        .await["cursor"]
        .as_i64()
        .unwrap();
    let preview = fixture
        .success(READER, "agent.preview_repair_workspaces", json!({}))
        .await;
    assert!(preview["refusal"].is_null());
    assert_eq!(preview["changes"].as_array().unwrap().len(), 2);
    let repaired_bytes = match fixture
        .outcome(
            DIRECT,
            "agent.repair_workspaces",
            json!({"request_key":"served-repair"}),
        )
        .await
    {
        HandlerOutcome::Response(bytes) => bytes,
        outcome => panic!("repair refused: {}", refusal(outcome)),
    };
    let repaired = serde_json::from_slice::<Value>(&repaired_bytes).unwrap()["result"].clone();
    assert_eq!(repaired["cleared"], 2);
    assert_eq!(repaired["moved"], 1);
    let generation = fixture
        .handler
        .with_store(RegistryStore::generation)
        .unwrap();
    match fixture
        .outcome(
            DIRECT,
            "agent.repair_workspaces",
            json!({"request_key":"served-repair"}),
        )
        .await
    {
        HandlerOutcome::Response(bytes) => assert_eq!(bytes, repaired_bytes),
        _ => panic!("cached correction must succeed"),
    }
    assert_eq!(
        fixture
            .handler
            .with_store(RegistryStore::generation)
            .unwrap(),
        generation
    );
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 1);
    let executive = refusal(
        fixture
            .outcome(
                CORE,
                "agent.repair_workspaces",
                json!({"request_key":"served-repair"}),
            )
            .await,
    );
    assert_eq!(executive["code"], "direct_caller_required");
    fixture.success(DIRECT, "assign_workspace", json!({"projectId":"hiree-project","workspaceId":"destination","requestKey":"served-carry"})).await;
    let (record, reply) = fixture
        .record(
            "agent.changes",
            json!({"incarnation":INCARNATION,"cursor":cursor,"wait":false}),
        )
        .await;
    let entries = reply["result"]["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 3);
    assert!(entries
        .iter()
        .all(|e| e["op"] == "agent.set_namespace" && e["row"]["workspace_id"].is_null()));
    assert_eq!(entries[0]["claims"].as_array().unwrap().len(), 2);
    assert!(entries[1]["claims"].as_array().unwrap().is_empty());
    assert_eq!(entries[2]["claims"].as_array().unwrap().len(), 2);
    assert_eq!(
        *confirmer.summaries.lock().unwrap(),
        vec![
            "repair agent workspaces: clear 2 stored workspaces, move 1 name claims",
            "move \"hiree-project\" to \"destination\": \"Live hiree\""
        ]
    );
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/agent-identity-changes-v1/agent-changes.json");
    let served = format!("[\n{record}\n]\n");
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, &served).unwrap();
    } else {
        assert_eq!(
            std::fs::read_to_string(path).unwrap(),
            served,
            "agent.changes differs from the real handler reply"
        );
    }
    let store = fixture.handler.with_store(|s| Ok(s.clone())).unwrap();
    assert!(store.verify().unwrap().ok);
    assert!(store.rebuild().unwrap().replay.ok);
}

#[tokio::test]
async fn repair_preview_is_open_and_cutover_refusal_precedes_confirmation() {
    let confirmer = std::sync::Arc::new(Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    let fixture = Fixture::with_confirmer("repair-before-cutover", confirmer.clone()).await;
    for route in [DIRECT, CORE, READER, (999, 1)] {
        let preview = fixture
            .success(route, "agent.preview_repair_workspaces", json!({}))
            .await;
        assert_eq!(preview["refusal"]["code"], "authority_not_cut_over");
    }
    let before = fixture
        .handler
        .with_store(RegistryStore::generation)
        .unwrap();
    let outcome = refusal(
        fixture
            .outcome(
                DIRECT,
                "agent.repair_workspaces",
                json!({"request_key":"before"}),
            )
            .await,
    );
    assert_eq!(outcome["code"], "authority_not_cut_over");
    let outcome = refusal(
        fixture
            .outcome(CORE, "agent.repair_workspaces", Value::Null)
            .await,
    );
    assert_eq!(outcome["code"], "direct_caller_required");
    assert!(confirmer.summaries.lock().unwrap().is_empty());
    assert_eq!(
        fixture
            .handler
            .with_store(RegistryStore::generation)
            .unwrap(),
        before
    );
}

#[tokio::test]
async fn catalog_generation_v1_records_match_published_files() {
    use entorhinal_core::catalog_agent::{
        CatalogPin, CatalogSpec, CatalogTarget, PreparedPackage, PreparedPlan,
    };
    use entorhinal_core::catalog_grants::{coverage, Coverage, Operation, PublisherGrant};
    use entorhinal_core::catalog_trust::{CatalogStatus, TrustWrite};
    let mut f = Fixture::new("catalog-generation-vectors").await;
    let source = f.snapshot();
    seed_agent(&source, ASSISTANT, "Catalog assistant", None, false, false);
    drop(source);
    assert!(matches!(f.import().await, HandlerOutcome::Response(_)));
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    let pin = CatalogPin {
        name: "@example/hello-skill".into(),
        version: "1.0.0".into(),
        kind: entorhinal_catalog::Kind::Skill,
        manifest_sha256: "11".repeat(32),
        archive_sha256: "22".repeat(32),
        publisher_fingerprint: "33".repeat(32),
        root: true,
        range: Some("^1.0.0".into()),
    };
    let trust_vectors = crate::catalog_trust::tests::vectors("statements");
    let publisher_key = &trust_vectors["keys"]["cortexkit_publish"][0];
    let test_root = trust_vectors["keys"]["root"]["public_key_hex"]
        .as_str()
        .unwrap();
    let certificate = &trust_vectors["ck-key-cert/v1"]["cases"][0]["served"];
    let certificate_text = entorhinal_catalog::jcs::to_string(&certificate["statement"]).unwrap();
    let certified = entorhinal_catalog::statements::verify_key_cert(
        &certificate_text,
        certificate["signature"].as_str().unwrap(),
        test_root,
    )
    .unwrap();
    assert_eq!(certified.public_key, publisher_key["public_key_hex"]);
    let first_party_pin = CatalogPin {
        name: "@cortexkit/hello-skill".into(),
        version: "1.0.0".into(),
        kind: entorhinal_catalog::Kind::Skill,
        manifest_sha256: "55".repeat(32),
        archive_sha256: "66".repeat(32),
        publisher_fingerprint: publisher_key["fingerprint"].as_str().unwrap().into(),
        root: true,
        range: Some("^1.0.0".into()),
    };
    let prepared = PreparedPlan {
        packages: vec![
            PreparedPackage {
                pin: first_party_pin.clone(),
                trust: "trusted".into(),
                capabilities: vec![],
                has_scripts: false,
                root_certified: true,
            },
            PreparedPackage {
                pin: pin.clone(),
                trust: "trusted".into(),
                capabilities: vec![],
                has_scripts: false,
                root_certified: false,
            },
        ],
        moves: json!({}),
        checks: vec![],
    };
    store
        .insert_publisher_grant(
            &PublisherGrant {
                id: "publisher:catalog-generation-vector".into(),
                publisher_fingerprint: pin.publisher_fingerprint.clone(),
                operation: Operation::Install,
                placement_agent_id: ASSISTANT.into(),
                ceiling: vec![],
                card_ref: None,
                granted_at_ms: 50,
                revoked_at_ms: None,
            },
            &json!("catalog-generation-v1-test"),
        )
        .unwrap();
    assert_eq!(
        coverage(
            &prepared
                .packages
                .iter()
                .map(PreparedPackage::coverage)
                .collect::<Vec<_>>(),
            &store.publisher_grants().unwrap(),
            Operation::Install,
            ASSISTANT,
            true,
        ),
        Coverage::Covered
    );
    store
        .catalog_registry_add(
            "https://registry.example.invalid",
            &["@example".into()],
            false,
            "test-status-key",
            &"44".repeat(32),
            1,
        )
        .unwrap();
    let generation = |id: &str| {
        let plan = format!("plan:{id}");
        store
            .catalog_create_plan(
                &plan,
                ASSISTANT,
                CatalogTarget::Agent(ASSISTANT.into()),
                "install",
                CatalogSpec {
                    package: pin.name.clone(),
                    range: "^1.0.0".into(),
                },
                100,
            )
            .unwrap();
        store
            .catalog_save_prepared(&plan, ASSISTANT, &prepared)
            .unwrap();
        store
            .catalog_plan_ready(
                &plan,
                ASSISTANT,
                vec![first_party_pin.clone(), pin.clone()],
                200,
            )
            .unwrap();
        store
            .catalog_approve_plan(&plan, ASSISTANT, id, 300)
            .unwrap();
        store.catalog_generation_ready(id, ASSISTANT).unwrap();
    };
    let status = |state: &str, seq: u64| {
        let canonical = json!({"name":pin.name,"version":pin.version,"state":state,"reason":"publisher security notice","archive_sha256":pin.archive_sha256,"published_at":"2026-01-01T00:00:00Z","compromise_from":if state == "revoked" {json!("2026-01-01T00:00:00Z")} else {Value::Null}}).to_string();
        store
            .catalog_trust_transaction::<_, entorhinal_core::catalog::CatalogWriteError>(
                "@example",
                |r, _, _| {
                    Ok((
                        (),
                        TrustWrite {
                            statuses: vec![CatalogStatus {
                                name: pin.name.clone(),
                                version: pin.version.clone(),
                                registry_url: r.url.clone(),
                                sequence: seq,
                                changed_at: "2026-01-01T00:00:00Z".into(),
                                published_at: "2026-01-01T00:00:00Z".into(),
                                signer_index: 0,
                                canonical,
                                signature: "fixture".into(),
                                starter_proof: None,
                            }],
                            ..Default::default()
                        },
                    ))
                },
            )
            .unwrap();
    };
    let mut records = Vec::new();
    async fn record_at(
        f: &Fixture,
        records: &mut Vec<String>,
        case: &str,
        method: &str,
        params: Value,
        route: RouteKey,
    ) -> Value {
        let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        let outcome = f.handler.handle_served_request(&request, route).await;
        let (kind, reply) = match outcome {
            HandlerOutcome::Response(bytes) => ("response", String::from_utf8(bytes).unwrap()),
            other => ("error", refusal(other).to_string()),
        };
        let decoded: Value = serde_json::from_str(&reply).unwrap();
        records.push(format!(
            "  {{\"case\":{},\"request_bytes\":{},\"outcome\":{},\"reply_bytes\":{}}}",
            json!(case),
            json!(String::from_utf8(request).unwrap()),
            json!(kind),
            json!(reply)
        ));
        decoded
    }
    async fn record(
        f: &Fixture,
        records: &mut Vec<String>,
        case: &str,
        method: &str,
        params: Value,
    ) -> Value {
        record_at(f, records, case, method, params, CORE).await
    }
    let blob_folder = f
        .handler
        .with_store(|store| Ok(store.store_folder().to_path_buf()))
        .unwrap();
    let blob_bytes = b"catalog package bytes";
    let blob_digest = crate::catalog_blob::BlobStore::new(&blob_folder)
        .unwrap()
        .publish(blob_bytes)
        .unwrap();
    f.handler.participant_discovery = std::sync::Arc::new(crate::fake_daemon::FakeDaemon::new(
        crate::fake_daemon::DaemonSnapshot::new(
            1,
            vec![],
            vec![
                json!({"module_id":READER_MODULE,"ready":true,"roles":[],"control_ops":[],"capabilities":{"provides":[crate::catalog_consumer::SKILL],"requires":[]}}),
            ],
        ),
    ));
    use base64::Engine;
    let first_chunk = record_at(
        &f,
        &mut records,
        "blob_first_chunk",
        "catalog.read_blob",
        json!({"sha256":blob_digest,"offset":0,"length":7}),
        READER,
    )
    .await;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(first_chunk["result"]["bytes"].as_str().unwrap())
            .unwrap(),
        b"catalog"
    );
    // The middle chunk joins the first and final chunks without a gap, so a
    // consumer can assemble the whole blob from captured replies and check
    // its digest.
    let final_offset = blob_bytes.len() - 6;
    let middle_chunk = record_at(
        &f,
        &mut records,
        "blob_middle_chunk",
        "catalog.read_blob",
        json!({"sha256":blob_digest,"offset":7,"length":final_offset - 7}),
        READER,
    )
    .await;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(middle_chunk["result"]["bytes"].as_str().unwrap())
            .unwrap(),
        &blob_bytes[7..final_offset]
    );
    let final_chunk = record_at(
        &f,
        &mut records,
        "blob_final_chunk",
        "catalog.read_blob",
        json!({"sha256":blob_digest,"offset":final_offset,"length":10}),
        READER,
    )
    .await;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(final_chunk["result"]["bytes"].as_str().unwrap())
            .unwrap(),
        &blob_bytes[final_offset..]
    );
    let eof = record_at(
        &f,
        &mut records,
        "blob_eof_empty",
        "catalog.read_blob",
        json!({"sha256":blob_digest,"offset":blob_bytes.len(),"length":1}),
        READER,
    )
    .await;
    assert_eq!(eof["result"]["bytes"], "");
    let unknown = record_at(
        &f,
        &mut records,
        "blob_unknown",
        "catalog.read_blob",
        json!({"sha256":"ff".repeat(32),"offset":0,"length":1}),
        READER,
    )
    .await;
    assert_eq!(unknown["code"], "blob_unknown");
    generation("generation:available");
    let ready = record(
        &f,
        &mut records,
        "ready",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
    )
    .await;
    assert_eq!(ready["result"]["ready"][0]["state"], "ready");
    let closure = ready["result"]["ready"][0]["closure"].as_array().unwrap();
    assert_eq!(
        closure[0]["publisher"]["fingerprint"],
        first_party_pin.publisher_fingerprint
    );
    assert_eq!(closure[0]["publisher"]["scope"], "@cortexkit");
    assert_eq!(closure[0]["publisher"]["first_party"], true);
    assert_eq!(
        closure[1]["publisher"]["fingerprint"],
        pin.publisher_fingerprint
    );
    assert_eq!(closure[1]["publisher"]["scope"], "@example");
    assert_eq!(closure[1]["publisher"]["first_party"], false);
    record(
        &f,
        &mut records,
        "ack_success",
        "catalog.ack_activation",
        json!({"agent_id":ASSISTANT,"generation_id":"generation:available"}),
    )
    .await;
    let active = record(
        &f,
        &mut records,
        "active_available",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
    )
    .await;
    assert!(active["result"]["active"].get("pinned_status").is_none());
    status("withdrawn", 1);
    let withdrawn = record(
        &f,
        &mut records,
        "active_withdrawn",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
    )
    .await;
    assert_eq!(withdrawn["result"]["active"]["state"], "active");
    assert_eq!(
        withdrawn["result"]["active"]["pinned_status"][0]["state"],
        "withdrawn"
    );
    generation("generation:blocked");
    status("revoked", 2);
    let degraded = record(
        &f,
        &mut records,
        "degraded_and_ready_blocked",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
    )
    .await;
    assert_eq!(degraded["result"]["active"]["state"], "degraded");
    assert_eq!(
        degraded["result"]["active"]["error"]["code"],
        "pinned_revoked"
    );
    let refused = record(
        &f,
        &mut records,
        "ack_blocked",
        "catalog.ack_activation",
        json!({"agent_id":ASSISTANT,"generation_id":"generation:blocked"}),
    )
    .await;
    assert_eq!(refused["code"], "activation_blocked");
    assert_eq!(refused["detail"]["name"], pin.name);
    published_file("catalog-generation-v1", "records.json", records);
}

#[tokio::test]
async fn role_claims_v1_records_match_published_files() {
    let plexus = json!({"kind":"reserved","moduleId":"plexus"});
    let direct = serde_json::to_value(Principal::Direct).unwrap();
    let mut records = Vec::new();

    let f = RoleFixture::new("role-claims-never-read", false).await;
    let unknown = f
        .record(
            &mut records,
            "never_read",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert_eq!(unknown["result"]["known"], false);

    let f = RoleFixture::new("role-claims-unheld", false).await;
    f.enroll();
    f.refresh().await;
    let unheld = f
        .record(
            &mut records,
            "read_unheld",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert_eq!(unheld["result"]["epoch"], 0);
    assert_eq!(unheld["result"]["known"], true);
    assert_eq!(unheld["result"]["stale"], false);

    let f = RoleFixture::new("role-claims-held-here", false).await;
    f.enroll();
    f.record.put(
        "role:x",
        4,
        role_claim(4, ROLE_SELF_AUTHOR),
        ROLE_SELF_AUTHOR,
    );
    f.refresh().await;
    let held_here = f
        .record(
            &mut records,
            "held_here",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert!(held_here["result"]["epoch"].as_i64().unwrap() >= 1);
    assert_eq!(held_here["result"]["holder"], ROLE_SELF_AUTHOR);
    assert_eq!(held_here["result"]["holder_name"], "This Mac");
    assert_eq!(held_here["result"]["held_here"], true);

    let f = RoleFixture::new("role-claims-held-elsewhere", false).await;
    f.enroll();
    f.record.put(
        "role:x",
        5,
        role_claim(5, ROLE_OTHER_AUTHOR),
        ROLE_OTHER_AUTHOR,
    );
    f.refresh().await;
    let held_elsewhere = f
        .record(
            &mut records,
            "held_elsewhere",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert_eq!(held_elsewhere["result"]["holder"], ROLE_OTHER_AUTHOR);
    assert_eq!(held_elsewhere["result"]["holder_name"], "Other Mac");
    assert_eq!(held_elsewhere["result"]["held_here"], false);

    let f = RoleFixture::new("role-claims-stale-after-restart", false).await;
    f.enroll();
    f.record.put(
        "role:x",
        6,
        role_claim(6, ROLE_SELF_AUTHOR),
        ROLE_SELF_AUTHOR,
    );
    f.refresh().await;
    f.restart_store().await;
    let stale = f
        .record(
            &mut records,
            "stale_after_restart",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert_eq!(stale["result"]["known"], true);
    assert_eq!(stale["result"]["holder"], ROLE_SELF_AUTHOR);
    assert_eq!(stale["result"]["held_here"], true);
    assert_eq!(stale["result"]["stale"], true);

    let f = RoleFixture::new("role-claims-integrity-failure", false).await;
    f.enroll();
    f.record.error("role:x", Some("decrypt_failed"));
    f.refresh().await;
    let integrity = f
        .record(
            &mut records,
            "integrity_failure",
            "role.read",
            json!({"role":ROLE}),
            ROLE_PLEXUS,
            plexus.clone(),
        )
        .await;
    assert_eq!(integrity["code"], "claim_integrity_failed");

    let f = RoleFixture::new("role-claims-take-epoch-moved", true).await;
    let moved = f
        .record(
            &mut records,
            "take_epoch_moved",
            "role.take",
            json!({
                "role":ROLE,
                "expected_epoch":0,
                "request_id":"11".repeat(16),
                "reason":"run coordinator"
            }),
            ROLE_DIRECT,
            direct.clone(),
        )
        .await;
    assert_eq!(moved["code"], "epoch_moved");
    assert_eq!(moved["detail"]["current_epoch"], 1);
    assert_eq!(moved["detail"]["holder"], ROLE_OTHER_AUTHOR);

    let f = RoleFixture::new("role-claims-take-reserved-plexus", false).await;
    let denied = f
        .record(
            &mut records,
            "take_reserved_plexus",
            "role.take",
            json!({
                "role":ROLE,
                "expected_epoch":0,
                "request_id":"22".repeat(16),
                "reason":"run coordinator"
            }),
            ROLE_PLEXUS,
            plexus,
        )
        .await;
    assert_eq!(denied["code"], "caller_forbidden");

    published_file("role-claims-v1", "records.json", records);
}

#[tokio::test]
async fn memory_context_v1_records_match_published_files() {
    let fixture = Fixture::new("served-memory-context").await;
    let root = crate::tests::canonical_test_path(&fixture.dir).join("project");
    std::fs::create_dir_all(root.join(".git/worktrees/context")).unwrap();
    let worktree = crate::tests::canonical_test_path(&fixture.dir).join("worktree");
    std::fs::create_dir_all(&worktree).unwrap();
    std::fs::write(root.join(".git/worktrees/context/commondir"), "../..\n").unwrap();
    std::fs::write(
        worktree.join(".git"),
        format!(
            "gitdir: {}\n",
            root.join(".git/worktrees/context").display()
        ),
    )
    .unwrap();
    fixture.success(CORE,"register",json!({"projectId":"served-project","name":"Served project","roots":[root],"workspaceId":"served-workspace","domain":"@example/software"})).await;
    let bare = crate::tests::canonical_test_path(&fixture.dir).join("bare");
    std::fs::create_dir_all(&bare).unwrap();
    fixture
        .success(
            CORE,
            "register",
            json!({"projectId":"agentless","name":"Agentless","roots":[bare]}),
        )
        .await;
    let missing = crate::tests::canonical_test_path(&fixture.dir).join("unregistered");
    std::fs::create_dir_all(&missing).unwrap();
    let mut records = Vec::new();
    let (record, no_doc) = fixture
        .record("context.project", json!({"folder":root}))
        .await;
    assert_eq!(no_doc["result"]["document"], Value::Null);
    assert_eq!(no_doc["result"]["domain"], "@example/software");
    records.push(record);
    for (revision, document) in [(0, "Initial policy\n"), (1, "Accepted policy\n")] {
        fixture.success(DIRECT,"workspace.set_document",json!({"workspace_id":"served-workspace","expected_revision":revision,"charter":"Build carefully.\n","document":document,"request_key":format!("context-doc-{revision}")})).await;
    }
    for folder in [&root, &bare, &missing, &worktree] {
        let (record, reply) = fixture
            .record("context.project", json!({"folder":folder}))
            .await;
        if folder == &root || folder == &worktree {
            assert_eq!(reply["result"]["document"]["revision"], 2);
            assert_eq!(reply["result"]["project_id"], "served-project");
        }
        if folder == &missing {
            assert_eq!(reply["result"]["project_id"], Value::Null);
            assert_eq!(reply["result"]["via"], "implicit");
        }
        records.push(record);
    }
    // Map each folder to a fixed path, as the project-resolve samples do, so the
    // published bytes don't depend on this OS's path separator.
    let stable = [
        (root.clone(), "/fixture/project"),
        (bare.clone(), "/fixture/bare"),
        (missing.clone(), "/fixture/unregistered"),
        (worktree.clone(), "/fixture/worktree"),
    ];
    let records = records
        .into_iter()
        .map(|mut record| {
            for (actual, fixed) in &stable {
                record = record.replace(&json_string_contents(&actual.to_string_lossy()), fixed);
            }
            record
        })
        .collect();
    published_file("memory-context-v1", "context-project.json", records);
    let source = fixture.snapshot();
    seed_agent(&source, ASSISTANT, "Lin", None, false, false);
    seed_agent(&source, NO_AVATAR, "Sam", None, false, false);
    drop(source);
    assert!(matches!(
        fixture.import().await,
        HandlerOutcome::Response(_)
    ));
    let store = fixture.handler.with_store(|s| Ok(s.clone())).unwrap();
    store
        .with_principal("reserved:prefrontal-core")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: ASSISTANT.into(),
                pins: vec![entorhinal_core::catalog_agent::CatalogPin {
                    name: "@example/memory".into(),
                    version: "1.0.0".into(),
                    kind: entorhinal_catalog::Kind::MemoryProfile,
                    manifest_sha256: "a".repeat(64),
                    archive_sha256: "b".repeat(64),
                    publisher_fingerprint: "c".repeat(64),
                    root: true,
                    range: Some("^1.0.0".into()),
                }],
                request_key: Some("memory-pin".into()),
            },
            700,
        )
        .unwrap();
    let mut records = Vec::new();
    for id in [ASSISTANT, NO_AVATAR] {
        let (record, reply) = fixture
            .record("context.session", json!({"agent_id":id}))
            .await;
        assert_eq!(
            reply["result"]["pins"].as_array().unwrap().len(),
            usize::from(id == ASSISTANT)
        );
        records.push(record);
    }
    // Then the registry revokes that version. The trust sync writes the status
    // row; here the test writes the same row directly so the sample shows the
    // populated `status` shape a reader polling for revocations sees.
    rusqlite::Connection::open(fixture.dir.join("store.db"))
        .unwrap()
        .execute(
            "INSERT INTO catalog_status(name,version,sequence,changed_at,published_at,signer_index,canonical,signature) VALUES('@example/memory','1.0.0',2,'2026-10-02T00:00:00Z','2026-10-01T00:00:00Z',0,?1,'')",
            [json!({"name":"@example/memory","version":"1.0.0","state":"revoked","published_at":"2026-10-01T00:00:00Z"}).to_string()],
        )
        .unwrap();
    let (record, reply) = fixture
        .record("context.session", json!({"agent_id":ASSISTANT}))
        .await;
    assert_eq!(
        reply["result"]["pins"][0]["status"],
        json!({"state":"revoked","blocked":true})
    );
    records.push(record);
    published_file("memory-context-v1", "context-session.json", records);
    let mut records = Vec::new();
    for revision in [None, Some(1), Some(99)] {
        let (record, reply) = fixture
            .record(
                "workspace.document",
                json!({"workspace_id":"served-workspace","revision":revision}),
            )
            .await;
        if revision == Some(99) {
            assert_eq!(reply["code"], "revision_unknown");
        } else {
            assert_eq!(reply["result"]["revision"], revision.unwrap_or(2));
        }
        records.push(record);
    }
    published_file("memory-context-v1", "workspace-document.json", records);
}

struct CatalogConsumerVectorRoute {
    capability: &'static str,
    check_reply: Value,
    calls: std::sync::Mutex<Vec<(String, Value)>>,
}

#[async_trait::async_trait]
impl LogTransport for CatalogConsumerVectorRoute {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.calls.lock().unwrap().push((method.into(), params));
        let result = match method {
            "catalog_consumer.check" => self.check_reply.clone(),
            "catalog_consumer.prefetch" => json!({"ready":true}),
            _ => return Err(TransportError::Unavailable),
        };
        Ok(serde_json::to_vec(&json!({"result":result})).unwrap())
    }
}

struct CatalogConsumerVectorDiscovery {
    snapshot: crate::fake_daemon::DaemonSnapshot,
    routes: BTreeMap<String, Arc<CatalogConsumerVectorRoute>>,
}

#[async_trait::async_trait]
impl crate::catalog_consumer::ConsumerDiscovery for CatalogConsumerVectorDiscovery {
    async fn supervisor_list(
        &self,
    ) -> Result<subc_client_rs::SupervisorListResponse, TransportError> {
        Ok(self.snapshot.supervisor.clone())
    }

    async fn catalog_list(&self) -> Result<subc_client_rs::CatalogList, TransportError> {
        Ok(self.snapshot.catalog.clone())
    }

    async fn open(&self, module: &str) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.routes
            .get(module)
            .cloned()
            .map(|route| route as Arc<dyn LogTransport>)
            .ok_or(TransportError::Unavailable)
    }
}

fn catalog_consumer_vector_discovery(check_reply: Value) -> CatalogConsumerVectorDiscovery {
    let consumers = [
        ("fake-flow-consumer", crate::catalog_consumer::FLOW),
        ("fake-skill-consumer", crate::catalog_consumer::SKILL),
    ];
    let routes = consumers
        .iter()
        .map(|(module, capability)| {
            (
                (*module).to_owned(),
                Arc::new(CatalogConsumerVectorRoute {
                    capability,
                    check_reply: check_reply.clone(),
                    calls: std::sync::Mutex::new(Vec::new()),
                }),
            )
        })
        .collect();
    let snapshot = crate::fake_daemon::DaemonSnapshot::new(
        1,
        consumers
            .iter()
            .map(|(module, _)| {
                json!({"module_id":module,"enabled":true,"state":"running","live":true,"health":"ok"})
            })
            .collect(),
        consumers
            .iter()
            .map(|(module, capability)| {
                json!({"module_id":module,"ready":true,"roles":[],"control_ops":[],"capabilities":{"provides":[capability],"requires":[]}})
            })
            .collect(),
    );
    CatalogConsumerVectorDiscovery { snapshot, routes }
}

fn catalog_consumer_vector_requests(
    discovery: &CatalogConsumerVectorDiscovery,
    method: &str,
) -> Vec<Value> {
    let mut requests = Vec::new();
    for (module, route) in &discovery.routes {
        for (called_method, params) in route.calls.lock().unwrap().iter() {
            if called_method == method {
                requests.push(json!({
                    "consumer_module":module,
                    "declared_capability":route.capability,
                    "method":called_method,
                    "params":params
                }));
            }
        }
    }
    requests
}

fn publish_catalog_consumer_vector(name: &str, record: Value) {
    let record = serde_json::to_string_pretty(&record).unwrap();
    published_file("catalog-consumer-v1", name, vec![record]);
}

#[tokio::test]
async fn catalog_consumer_v1_requests_match_published_files() {
    use entorhinal_core::catalog_agent::{CatalogPin, PreparedPackage};

    let packages = vec![
        PreparedPackage {
            pin: CatalogPin {
                name: "@example/vector-skill".into(),
                version: "1.0.0".into(),
                kind: entorhinal_catalog::Kind::Skill,
                manifest_sha256: "11".repeat(32),
                archive_sha256: "22".repeat(32),
                publisher_fingerprint: "33".repeat(32),
                root: true,
                range: Some("^1.0.0".into()),
            },
            trust: "trusted".into(),
            capabilities: vec![],
            has_scripts: false,
            root_certified: false,
        },
        PreparedPackage {
            pin: CatalogPin {
                name: "@example/vector-flow".into(),
                version: "2.0.0".into(),
                kind: entorhinal_catalog::Kind::Flow,
                manifest_sha256: "44".repeat(32),
                archive_sha256: "55".repeat(32),
                publisher_fingerprint: "66".repeat(32),
                root: true,
                range: Some("^2.0.0".into()),
            },
            trust: "trusted".into(),
            capabilities: vec![],
            has_scripts: false,
            root_certified: false,
        },
    ];
    let accepted_reply = json!({
        "ok":true,
        "card_section":"Sample catalog consumer review."
    });
    let refusal_reply = json!({
        "ok":false,
        "refusals":[{"package":"@example/vector-skill","reason":"sample refusal"}]
    });
    let discovery = catalog_consumer_vector_discovery(accepted_reply.clone());
    let consumers = crate::catalog_consumer::discover(&discovery, &packages)
        .await
        .unwrap();
    let checks =
        crate::catalog_consumer::check(&consumers, "plan:catalog-consumer-v1-fake", &packages)
            .await
            .unwrap();
    let check_requests = catalog_consumer_vector_requests(&discovery, "catalog_consumer.check");

    // Each route must receive one package, and its kind must match its capability.
    assert_eq!(check_requests.len(), 2);
    for request in &check_requests {
        let expected_kind = match request["declared_capability"].as_str().unwrap() {
            crate::catalog_consumer::SKILL => "skill",
            crate::catalog_consumer::FLOW => "flow",
            other => panic!("unexpected consumer capability {other}"),
        };
        let received = request["params"]["packages"].as_array().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0]["kind"], expected_kind);
    }

    let refusal_discovery = catalog_consumer_vector_discovery(refusal_reply.clone());
    let refusal_consumers = crate::catalog_consumer::discover(&refusal_discovery, &packages)
        .await
        .unwrap();
    let refusal = crate::catalog_consumer::check(
        &refusal_consumers,
        "plan:catalog-consumer-v1-fake",
        &packages,
    )
    .await
    .unwrap_err();
    assert_eq!(refusal.code, "consumer_refused");
    assert_eq!(
        refusal.detail.unwrap()["refusals"],
        refusal_reply["refusals"]
    );

    let prefetch_reply = json!({"ready":true});
    crate::catalog_consumer::prefetch(
        &discovery,
        "generation:catalog-consumer-v1-fake",
        &packages,
        &checks,
        tokio::time::Instant::now() + std::time::Duration::from_secs(20),
    )
    .await
    .unwrap();
    let prefetch_requests =
        catalog_consumer_vector_requests(&discovery, "catalog_consumer.prefetch");
    assert_eq!(prefetch_requests.len(), 2);
    for request in &prefetch_requests {
        let expected_kind = match request["declared_capability"].as_str().unwrap() {
            crate::catalog_consumer::SKILL => "skill",
            crate::catalog_consumer::FLOW => "flow",
            other => panic!("unexpected consumer capability {other}"),
        };
        let received = request["params"]["packages"].as_array().unwrap();
        assert_eq!(received.len(), 1);
        assert_eq!(received[0]["kind"], expected_kind);
    }

    publish_catalog_consumer_vector(
        "check.json",
        json!({
            "accepted_reply":accepted_reply,
            "refusal_reply":refusal_reply,
            "requests":check_requests
        }),
    );
    publish_catalog_consumer_vector(
        "prefetch.json",
        json!({
            "accepted_reply":prefetch_reply,
            "requests":prefetch_requests
        }),
    );
}

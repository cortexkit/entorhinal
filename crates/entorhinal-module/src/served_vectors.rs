use std::{collections::BTreeMap, path::PathBuf};

use entorhinal_core::RegistryStore;
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;

use crate::{
    install_store, tests::reserved, ProjectsHandler, RouteAdmission, RouteKey, WRITER_MODULE,
};

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

// Use the preserved migrations so the import sees the same columns and defaults
// as a core snapshot, including the name claims required for live identities.
const MIGRATIONS: &[&str] = &[
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
}

impl Fixture {
    async fn new(label: &str) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let handler = ProjectsHandler::with_runtime(INCARNATION.into(), || 700);
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        install_store(&handler.store, &handler.health, Ok(store));
        for (route, principal) in [
            (DIRECT, Principal::Direct),
            (CORE, reserved(WRITER_MODULE)),
            (READER, reserved(READER_MODULE)),
        ] {
            handler
                .route_admissions()
                .insert(route, RouteAdmission::from_bind(Some(principal), None));
        }
        let fixture = Self { dir, handler };
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

fn published_file(name: &str, records: Vec<String>) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors/agent-identity-reads-v1")
        .join(name);
    let served = format!("[\n{}\n]\n", records.join(",\n"));
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, served).unwrap();
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
    published_file("avatar-read.json", records);
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

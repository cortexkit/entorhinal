use std::{collections::BTreeMap, path::PathBuf};

use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    RegistryStore,
};
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
        install_store(&handler.store, &handler.health, Ok(store));
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
    use entorhinal_core::catalog_agent::{CatalogPin, CatalogSpec, CatalogTarget};
    use entorhinal_core::catalog_trust::{CatalogStatus, TrustWrite};
    let f = Fixture::new("catalog-generation-vectors").await;
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
            .catalog_plan_ready(&plan, ASSISTANT, vec![pin.clone()], 200)
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
                            }],
                            ..Default::default()
                        },
                    ))
                },
            )
            .unwrap();
    };
    let mut records = Vec::new();
    async fn record(
        f: &Fixture,
        records: &mut Vec<String>,
        case: &str,
        method: &str,
        params: Value,
    ) -> Value {
        let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        let outcome = f.handler.handle_served_request(&request, CORE).await;
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

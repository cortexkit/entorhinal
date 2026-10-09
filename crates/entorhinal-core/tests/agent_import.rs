use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    agent::{import::SNAPSHOT_AGENT_COLUMNS, AgentChangeEntry, AgentMutationError},
    RegisterRequest, RegistryStore, UpgradeImplicitRequest,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};

#[path = "support/scratch_cleanup.rs"]
mod scratch_cleanup;

#[cfg(windows)]
#[path = "support/windows_acl.rs"]
mod windows_acl;

// Core's agent-table migrations, copied byte for byte, so the import is tested
// against the schema a real core store has rather than a hand-written imitation
// of it. Each citation below names the original file so a copy can be checked
// against its source.
const MIGRATIONS: &[&str] = &[
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/076_agent_registry.sql.
    include_str!("fixtures/core-agent-migrations/076_agent_registry.sql"),
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/080_agent_github_identity.sql.
    include_str!("fixtures/core-agent-migrations/080_agent_github_identity.sql"),
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/082_wake_delivery.sql.
    include_str!("fixtures/core-agent-migrations/082_wake_delivery.sql"),
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/109_agent_generation.sql.
    include_str!("fixtures/core-agent-migrations/109_agent_generation.sql"),
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/112_agent_avatar.sql.
    include_str!("fixtures/core-agent-migrations/112_agent_avatar.sql"),
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/126_agent_labels.sql.
    include_str!("fixtures/core-agent-migrations/126_agent_labels.sql"),
];

const A: &str = "agent_0000000000000001";
const B: &str = "agent_0000000000000002";
const C: &str = "agent_0000000000000003";
const D: &str = "agent_0000000000000004";
const E: &str = "agent_0000000000000005";
const F: &str = "agent_0000000000000006";
const LEGACY: &str = "agent_16013c86";

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    root: PathBuf,
    source_path: PathBuf,
    source: Connection,
    destination: Connection,
    store: Option<RegistryStore>,
    descriptor: StorageDescriptor,
    _cleanup: scratch_cleanup::ScratchCleanup,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/agent-import-fixtures")
            .join(format!(
                "{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(&root).unwrap();
        let source_path = root.join("snapshot.db");
        let source = Connection::open(&source_path).unwrap();
        // Broken source references are intentional refusal fixtures, not writes
        // through core's validator. The destination still enforces foreign keys.
        source.execute_batch("PRAGMA foreign_keys=OFF;").unwrap();
        for migration in MIGRATIONS {
            source.execute_batch(migration).unwrap();
        }
        let descriptor = StorageDescriptor {
            module_id: "entorhinal-agent-import-test".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        let store = RegistryStore::open(&descriptor).unwrap();
        let destination = Connection::open(root.join("store.db")).unwrap();
        Self {
            _cleanup: scratch_cleanup::ScratchCleanup(root.clone()),
            root,
            source_path,
            source,
            destination,
            store: Some(store),
            descriptor,
        }
    }

    fn store(&self) -> &RegistryStore {
        self.store.as_ref().unwrap()
    }

    fn request(&self, key: &str) -> Value {
        json!({"snapshot_path":self.source_path, "request_key":key})
    }

    fn import(&self, key: &str) -> Result<Value, AgentMutationError> {
        self.call(self.request(key))
    }

    fn call(&self, body: Value) -> Result<Value, AgentMutationError> {
        self.store()
            .with_principal("reserved:prefrontal-core")
            .agent_import(body, 900)
            .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
    }

    fn restart(&mut self) {
        drop(self.store.take());
        self.store = Some(RegistryStore::open(&self.descriptor).unwrap());
    }

    #[allow(clippy::too_many_arguments)]
    fn row(
        &self,
        id: &str,
        name: &str,
        role: &str,
        project: Option<&str>,
        workspace: Option<&str>,
        terminal: Option<&str>,
        into: Option<&str>,
    ) {
        self.source.execute("INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms,merged_into_agent_id) VALUES(?1,?2,'fixture tag',?3,?4,?5,101,202,?6,?7,?8)",
            params![id,name,role,project,workspace,terminal,terminal.map(|_| 303),into]).unwrap();
        if terminal.is_none() {
            self.claim(id, name, None);
        }
    }

    fn assistant(&self, id: &str, name: &str) {
        self.row(id, name, "assistant", None, None, None, None);
    }

    fn claim(&self, id: &str, name: &str, released: Option<i64>) {
        // Fixture names here are ASCII, and these expected folds are independent
        // of the normalizer used by the importer.
        self.source.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms,released_at_ms) VALUES(?1,'assistant','global',?2,?3,404,?4)", params![id,name.to_ascii_lowercase(),name,released]).unwrap();
    }

    fn register(&self, project: &str, workspace: Option<&str>, aliases: Vec<String>) {
        self.store()
            .register(RegisterRequest {
                project_id: Some(project.into()),
                name: project.into(),
                workspace_id: workspace.map(str::to_owned),
                ..Default::default()
            })
            .unwrap();
        for alias in aliases {
            self.store()
                .upgrade_implicit(UpgradeImplicitRequest {
                    implicit_id: alias,
                    project_id: Some(project.into()),
                    name: project.into(),
                    workspace_id: workspace.map(str::to_owned),
                    ..Default::default()
                })
                .unwrap();
        }
    }

    fn count(&self, table: &str) -> i64 {
        self.destination
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    fn empty(&self, head: i64) {
        assert_eq!(self.count("agent"), 0);
        assert_eq!(self.count("agent_name_claim"), 0);
        assert_eq!(self.store().generation().unwrap(), head);
        assert_eq!(self.destination.query_row("SELECT COUNT(*) FROM registry_journal WHERE op IN ('agent.import','agent.cutover')", [], |r| r.get::<_,i64>(0)).unwrap(), 0);
    }

    fn refusal(&self, expected_detail: Value) {
        let head = self.store().generation().unwrap();
        let error = self.import("retryable").unwrap_err();
        assert_eq!(error.code, "import_invariant_failed", "{error}");
        assert_eq!(error.detail, Some(expected_detail));
        self.empty(head);
    }

    fn entries(&self) -> Vec<AgentChangeEntry> {
        self.destination.prepare("SELECT seq,payload_json FROM registry_journal WHERE op='agent.import' ORDER BY seq").unwrap()
            .query_map([], |r| {
                let seq: i64 = r.get(0)?;
                let payload: Value = serde_json::from_str(&r.get::<_,String>(1)?).unwrap();
                let entry: AgentChangeEntry = serde_json::from_value(payload["entry"].clone()).unwrap();
                assert_eq!(entry.seq, seq, "the feed cursor is each entry's own seq");
                Ok(entry)
            }).unwrap().collect::<rusqlite::Result<_>>().unwrap()
    }
}

#[test]
fn source_migration_columns_match_the_actual_import_select() {
    let f = Fixture::new();
    let columns: Vec<String> = f
        .source
        .prepare("PRAGMA table_info(agent)")
        .unwrap()
        .query_map([], |r| r.get(1))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    let select = f
        .source
        .prepare(&format!("SELECT {SNAPSHOT_AGENT_COLUMNS} FROM agent"))
        .unwrap();
    assert_eq!(columns.len(), 28);
    assert_eq!(
        select.column_names(),
        columns,
        "core migration columns and import SELECT drifted"
    );
}

#[test]
fn invariant_agent_id_reports_smallest_bad_id_without_writes() {
    let f = Fixture::new();
    for id in [
        "bad_z",
        "AGENT_0000000000000001",
        "agent_ABCDEF12",
        "agent_123456789",
    ] {
        f.assistant(id, id);
    }
    f.refusal(json!({"check":"agent_id", "agent_id":"AGENT_0000000000000001"}));
}

#[test]
fn invariant_claim_owner_checks_released_claims_and_smallest_claim_id() {
    for valid_agent in [false, true] {
        let f = Fixture::new();
        if valid_agent {
            f.assistant(A, "Alice");
        }
        f.claim("agent_ffffffffffffffff", "Released", Some(505));
        let first = f.source.last_insert_rowid();
        f.claim("agent_00000000", "Other", None);
        f.refusal(
            json!({"check":"claim_owner", "claim_id":first, "agent_id":"agent_ffffffffffffffff"}),
        );
    }
}

#[test]
fn invariant_claim_mismatch_checks_every_live_claim_property() {
    for sql in [
        "DELETE FROM agent_name_claim",
        "UPDATE agent_name_claim SET normalized_name='wrong' WHERE agent_id='agent_0000000000000001'",
        "UPDATE agent_name_claim SET display_name='Other'",
        "PRAGMA ignore_check_constraints=ON; UPDATE agent_name_claim SET name_normalization_version=2",
        "DROP INDEX uq_active_agent_claim; INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_0000000000000001','workspace','other','alice','Alice',404)",
        "DROP TABLE agent_name_claim",
        "UPDATE agent SET name='  ' WHERE agent_id='agent_0000000000000001'",
    ] {
        let f = Fixture::new();
        f.assistant(B, "Bob");
        f.assistant(A, "Alice");
        f.source.execute_batch(sql).unwrap();
        f.refusal(json!({"check":"claim_mismatch", "agent_id":A}));
    }
}

#[test]
fn invariant_live_head_reports_project_with_smallest_head_and_sorted_ids() {
    let f = Fixture::new();
    f.source.execute_batch("DROP INDEX uq_live_head").unwrap();
    for (id, name, project) in [
        (F, "Fred", "aaa"),
        (E, "Eve", "aaa"),
        (C, "Carol", "zzz"),
        (A, "Alice", "zzz"),
    ] {
        f.row(id, name, "head", Some(project), None, None, None);
    }
    f.refusal(json!({"check":"live_head", "project_id":"zzz", "agent_ids":[A,C]}));
}

#[test]
fn invariant_merged_into_reports_smallest_source_and_missing_target() {
    let f = Fixture::new();
    f.row(
        B,
        "Bob",
        "assistant",
        None,
        None,
        Some("merged"),
        Some("agent_ffffffffffffffff"),
    );
    f.row(
        A,
        "Alice",
        "assistant",
        None,
        None,
        Some("merged"),
        Some("agent_ffffffff"),
    );
    f.refusal(json!({"check":"merged_into", "agent_id":A, "merged_into":"agent_ffffffff"}));
}

#[test]
fn invariant_binding_requires_destination_rows_not_aliases_or_stored_placement() {
    let f = Fixture::new();
    f.register("actual", Some("W"), vec!["alias".into()]);
    f.row(
        B,
        "Bob",
        "workspace_head",
        None,
        Some("missing-workspace"),
        None,
        None,
    );
    f.row(A, "Alice", "hiree", Some("alias"), Some("W"), None, None);
    f.refusal(json!({"check":"binding", "agent_id":A, "project_id":"alias", "workspace_id":null}));
    f.source
        .execute(
            "UPDATE agent SET project_id='actual' WHERE agent_id=?1",
            [A],
        )
        .unwrap();
    f.refusal(json!({"check":"binding", "agent_id":B, "project_id":null, "workspace_id":"missing-workspace"}));
    f.source
        .execute("UPDATE agent SET workspace_id='W' WHERE agent_id=?1", [B])
        .unwrap();
    assert_eq!(f.import("retryable").unwrap()["agents_imported"], 2);
}

#[test]
fn invariant_check_order_and_same_key_repairs_are_pinned() {
    let f = Fixture::new();
    f.source.execute_batch("DROP INDEX uq_live_head").unwrap();
    f.assistant("bad", "Bad");
    f.claim("absent", "Orphan", Some(505));
    let orphan = f.source.last_insert_rowid();
    f.assistant(A, "Alice");
    f.source
        .execute("DELETE FROM agent_name_claim WHERE agent_id=?1", [A])
        .unwrap();
    f.row(B, "Bob", "head", Some("missing"), None, None, None);
    f.row(C, "Carol", "head", Some("missing"), None, None, None);
    f.row(D, "Donna", "assistant", None, None, Some("merged"), Some(E));
    f.refusal(json!({"check":"agent_id", "agent_id":"bad"}));
    f.source
        .execute("DELETE FROM agent_name_claim WHERE agent_id='bad'", [])
        .unwrap();
    f.source
        .execute("DELETE FROM agent WHERE agent_id='bad'", [])
        .unwrap();
    f.refusal(json!({"check":"claim_owner", "claim_id":orphan, "agent_id":"absent"}));
    f.source
        .execute("DELETE FROM agent_name_claim WHERE claim_id=?1", [orphan])
        .unwrap();
    f.refusal(json!({"check":"claim_mismatch", "agent_id":A}));
    f.claim(A, "Alice", None);
    f.refusal(json!({"check":"live_head", "project_id":"missing", "agent_ids":[B,C]}));
    f.source
        .execute("UPDATE agent SET role='hiree' WHERE agent_id=?1", [C])
        .unwrap();
    f.refusal(json!({"check":"merged_into", "agent_id":D, "merged_into":E}));
    f.source
        .execute(
            "UPDATE agent SET merged_into_agent_id=?1 WHERE agent_id=?2",
            params![A, D],
        )
        .unwrap();
    f.refusal(
        json!({"check":"binding", "agent_id":B, "project_id":"missing", "workspace_id":null}),
    );
    f.register("missing", None, vec![]);
    let reply = f.import("retryable").unwrap();
    assert_eq!(reply["agents_imported"], 4);
    assert_eq!(reply["claims_imported"], 3);
}

fn expected_row(
    id: &str,
    name: &str,
    role: &str,
    project: Option<&str>,
    workspace: Option<&str>,
    status: &str,
    into: Option<&str>,
) -> Value {
    json!({"agent_id":id,"name":name,"name_version":1,"name_normalization_version":1,
        "tag":"fixture tag","labels":[],"role":role,"project_id":project,"workspace_id":workspace,
        "avatar":null,"github_identity":null,"status":status,"merged_into":into,
        "supervisor_agent_id":null,"request_key":null,"created_at_ms":101,"updated_at_ms":202,
        "terminal_at_ms":if status=="live" {None} else {Some(303)},"agent_generation":1})
}

fn expected_claim(
    claim_id: i64,
    id: &str,
    name: &str,
    kind: &str,
    key: &str,
    released: Option<i64>,
) -> Value {
    json!({"claim_id":claim_id,"agent_id":id,"namespace_kind":kind,"namespace_key":key,
        "normalized_name":name.to_ascii_lowercase(),"name_normalization_version":1,
        "display_name":name,"claimed_at_ms":404,"released_at_ms":released})
}

fn project_state(conn: &Connection) -> Vec<(String, Vec<Vec<Value>>)> {
    [
        "project",
        "workspace",
        "project_alias",
        "project_workspace",
        "project_root",
        "workspace_member",
    ]
    .into_iter()
    .map(|table| {
        let mut stmt = conn
            .prepare(&format!("SELECT * FROM {table} ORDER BY 1,2"))
            .unwrap();
        let columns = stmt.column_count();
        let rows = stmt
            .query_map([], |r| {
                (0..columns)
                    .map(|i| {
                        Ok(match r.get_ref(i)? {
                            rusqlite::types::ValueRef::Null => Value::Null,
                            rusqlite::types::ValueRef::Integer(n) => json!(n),
                            rusqlite::types::ValueRef::Text(s) => {
                                json!(std::str::from_utf8(s).unwrap())
                            }
                            other => panic!("unexpected fixture value {other:?}"),
                        })
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<_>>()
            .unwrap();
        (table.into(), rows)
    })
    .collect()
}

#[test]
fn carried_fields_and_allowed_legacy_shapes_survive_import_and_rebuild() {
    let f = Fixture::new();
    f.register("unplaced", None, vec![]);
    f.register("placed", Some("W2"), vec!["old-project".into()]);
    let projects_before = project_state(&f.destination);
    f.assistant(A, "Alice");
    f.row(B, "Bob", "hiree", Some("placed"), Some("W1"), None, None);
    f.row(C, "Carol", "head", Some("placed"), Some("W1"), None, None);
    f.row(D, "Donna", "workspace_head", None, Some("W2"), None, None);
    f.row(
        E,
        "Eve",
        "head",
        Some("removed-project"),
        Some("removed-workspace"),
        Some("merged"),
        Some(LEGACY),
    );
    f.row(
        F,
        "Fred",
        "workspace_head",
        None,
        Some("removed-workspace"),
        Some("deleted"),
        None,
    );
    f.row(LEGACY, "Legacy", "head", Some("unplaced"), None, None, None);
    f.claim(A, "Reusable", Some(505));
    f.claim(E, "Eve", Some(606));
    f.claim(F, "Fred", Some(707));
    f.source.execute_batch(r#"UPDATE agent_name_claim SET namespace_kind='workspace',namespace_key='W1' WHERE agent_id IN ('agent_0000000000000002','agent_0000000000000003','agent_16013c86'); UPDATE agent_name_claim SET namespace_kind='workspace',namespace_key='W2' WHERE agent_id='agent_0000000000000004';
        UPDATE agent SET avatar_version=2 WHERE agent_id='agent_0000000000000001';
        UPDATE agent SET persona_ref='persona',wake_policy_json='{"schedule":true}',wake_policy_version=9,residence_machine_id='machine',residence_harness='broca',residence_address_json='{}',residence_epoch=8,residence_state='live',sleep=1 WHERE agent_id='agent_16013c86';
        UPDATE agent SET name_version=7,tag='carried tag',labels_json='["Red","Blue"]',generation=1000000 WHERE agent_id='agent_0000000000000004';
        CREATE VIEW hire_identity_mapping AS SELECT * FROM deliberately_absent_table;"#).unwrap();
    // The import must read only `agent` and `agent_name_claim`. Core's
    // `hire_identity_mapping` records no supervising head, so it has nothing to
    // import; replacing it with a broken view makes any accidental read fail.
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/migrations/079_hire_identity_mapping.sql:4-9.
    let github = json!({"kind":"app","app_id":42,"app_slug":"fixture-app","installation_id":84,"credential_ref":"cred:fixture","client_id":"client","coauthor_line":"Fixture <fixture@example.test>"});
    f.source.execute("UPDATE agent SET avatar_genome=?1,avatar_type='creature.classic',avatar_version=NULL,github_identity_json=?2 WHERE agent_id=?3", params!["a".repeat(2048),github.to_string(),D]).unwrap();
    let mut expected = vec![
        expected_row(A, "Alice", "assistant", None, None, "live", None),
        expected_row(B, "Bob", "hiree", Some("placed"), Some("W1"), "live", None),
        expected_row(C, "Carol", "head", Some("placed"), Some("W1"), "live", None),
        expected_row(D, "Donna", "workspace_head", None, Some("W2"), "live", None),
        expected_row(
            E,
            "Eve",
            "head",
            Some("removed-project"),
            Some("removed-workspace"),
            "merged",
            Some(LEGACY),
        ),
        expected_row(
            F,
            "Fred",
            "workspace_head",
            None,
            Some("removed-workspace"),
            "retired",
            None,
        ),
        expected_row(
            LEGACY,
            "Legacy",
            "head",
            Some("unplaced"),
            None,
            "live",
            None,
        ),
    ];
    expected[3]["name_version"] = json!(7);
    expected[3]["tag"] = json!("carried tag");
    expected[3]["labels"] = json!(["Red", "Blue"]);
    expected[3]["agent_generation"] = json!(1000001);
    expected[3]["avatar"] =
        json!({"genome":"a".repeat(2048),"type":"creature.classic","version":null});
    expected[3]["github_identity"] = github;
    // Core's generation trigger (as redefined by migrations 112 and 126) adds one
    // to `generation` for the avatar and GitHub identity edit made above.
    let expected_claims = vec![
        expected_claim(1, A, "Alice", "assistant", "global", None),
        expected_claim(2, B, "Bob", "workspace", "W1", None),
        expected_claim(3, C, "Carol", "workspace", "W1", None),
        expected_claim(4, D, "Donna", "workspace", "W2", None),
        expected_claim(5, LEGACY, "Legacy", "workspace", "W1", None),
        expected_claim(6, A, "Reusable", "assistant", "global", Some(505)),
        expected_claim(7, E, "Eve", "assistant", "global", Some(606)),
        expected_claim(8, F, "Fred", "assistant", "global", Some(707)),
    ];
    // Core's trigger also counts the runtime-only edits (persona, wake, residence) and the
    // avatar_version edit, so the imported generations below include them: the
    // import carries core's number as is, it doesn't recount identity changes.
    expected[0]["agent_generation"] = json!(2);
    expected[6]["agent_generation"] = json!(2);
    let source_bytes = fs::read(&f.source_path).unwrap();
    let head = f.store().generation().unwrap();
    let reply = f
        .call(json!({"snapshot_path":f.source_path,"request_key":"import-key","actor":"operator"}))
        .unwrap();
    assert_eq!(
        reply,
        json!({"agents_imported":7,"claims_imported":8,"generation":head+8})
    );
    assert_eq!(
        fs::read(&f.source_path).unwrap(),
        source_bytes,
        "source must remain byte-exact"
    );
    let marker_before: (i64,String,String,String,String) = f.destination.query_row("SELECT seq,request_key,response_json,actor,principal FROM registry_journal WHERE op='agent.cutover'", [], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?,r.get(4)?))).unwrap();
    assert_eq!(marker_before.0, head + 8);
    assert_eq!(marker_before.1, "import-key");
    assert_eq!(
        serde_json::from_str::<Value>(&marker_before.2).unwrap()["result"],
        reply
    );
    assert_eq!(marker_before.3, "operator");
    assert_eq!(marker_before.4, "reserved:prefrontal-core");
    for rebuilt in [false, true] {
        if rebuilt {
            assert_eq!(f.store().rebuild().unwrap().generation, head + 8);
        }
        for row in &expected {
            let id = row["agent_id"].as_str().unwrap();
            assert_eq!(
                serde_json::to_value(f.store().agent_row(id).unwrap().unwrap()).unwrap(),
                *row
            );
            let claims = serde_json::to_value(f.store().agent_claims(id).unwrap()).unwrap();
            let owner_claims: Vec<_> = expected_claims
                .iter()
                .filter(|c| c["agent_id"] == id)
                .cloned()
                .collect();
            assert_eq!(claims, json!(owner_claims));
        }
        assert_eq!(project_state(&f.destination), projects_before);
        assert_eq!(
            f.destination
                .query_row(
                    "SELECT avatar_version FROM agent WHERE agent_id=?1",
                    [A],
                    |r| r.get::<_, Option<i64>>(0)
                )
                .unwrap(),
            None
        );
        assert_eq!(
            f.destination
                .query_row(
                    "SELECT request_key FROM registry_journal WHERE op='agent.cutover'",
                    [],
                    |r| r.get::<_, String>(0)
                )
                .unwrap(),
            "import-key"
        );
        let entries = f.entries();
        assert_eq!(entries.len(), 7);
        for (index, entry) in entries.iter().enumerate() {
            assert_eq!(entry.op, "agent.import");
            assert_eq!(entry.seq, head + index as i64 + 1);
            assert_eq!(serde_json::to_value(&entry.row).unwrap(), expected[index]);
            assert_eq!(
                entry.agent_generation,
                expected[index]["agent_generation"].as_i64().unwrap()
            );
        }
        let mut feed_claims: Vec<_> = entries
            .iter()
            .flat_map(|entry| entry.claims.iter())
            .map(|claim| serde_json::to_value(claim).unwrap())
            .collect();
        feed_claims.sort_by_key(|claim| claim["claim_id"].as_i64().unwrap());
        assert_eq!(feed_claims, expected_claims);
        // Page through the served feed one agent entry at a time to check no entry is
        // skipped or repeated at a page boundary. The cutover marker that ends the
        // import is not an agent entry; the change feed's serving code skips it.
        let mut cursor = head;
        let mut seen = std::collections::BTreeSet::new();
        for expected_entry in entries {
            let page = f.store().agent_changes(cursor, Some(1)).unwrap();
            assert_eq!(page.entries.len(), 1);
            let entry = &page.entries[0];
            assert_eq!(*entry, expected_entry);
            assert!(entry.seq > cursor);
            assert_eq!(page.cursor, entry.seq);
            assert!(
                seen.insert(entry.agent_id.clone()),
                "feed repeated an imported agent"
            );
            cursor = page.cursor;
        }
        assert_eq!(
            seen,
            [A, B, C, D, E, F, LEGACY]
                .into_iter()
                .map(str::to_owned)
                .collect()
        );
        let trailing = f.store().agent_changes(cursor, Some(1)).unwrap();
        assert!(
            trailing.entries.is_empty(),
            "cutover marker must not appear in the identity feed"
        );
        assert_eq!(trailing.cursor, head + 8);
        assert_eq!(trailing.generation, head + 8);
        assert!(f
            .store()
            .agent_changes(trailing.cursor, Some(1))
            .unwrap()
            .entries
            .is_empty());
    }
    let create = f.store().agent_mutation("agent.create", json!({"role":"assistant","name":"Reusable","tag":"new","request_key":"reuse-released"}), 1000).unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&create).unwrap()["result"]["agent"]["name"],
        "Reusable"
    );
}

#[test]
fn every_imported_agent_journal_row_records_the_admitted_principal() {
    let f = Fixture::new();
    for (id, name) in [(A, "Alice"), (B, "Bob"), (C, "Carol")] {
        f.assistant(id, name);
    }
    f.import("principal").unwrap();
    let principals: Vec<String> = f
        .destination
        .prepare("SELECT principal FROM registry_journal WHERE op='agent.import' ORDER BY seq")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    assert_eq!(principals, vec!["reserved:prefrontal-core"; 3]);
}

#[test]
fn zero_agent_sources_commit_marker_even_with_orphan_claims() {
    for leftover_claims in [false, true] {
        let f = Fixture::new();
        f.source.execute_batch("DROP TABLE agent;").unwrap();
        if leftover_claims {
            f.claim("absent", "Orphan", Some(505));
        } else {
            f.source
                .execute_batch("DROP TABLE agent_name_claim")
                .unwrap();
        }
        assert_eq!(
            f.import("empty").unwrap(),
            json!({"agents_imported":0,"claims_imported":0,"generation":1})
        );
        assert_eq!(f.count("agent"), 0);
        assert_eq!(f.count("registry_journal"), 1);
        f.store().rebuild().unwrap();
        f.store()
            .agent_mutation(
                "agent.create",
                json!({"role":"assistant","name":"First","tag":"new","request_key":"first"}),
                1000,
            )
            .unwrap();
    }
    let f = Fixture::new();
    assert_eq!(
        f.import("present-empty").unwrap(),
        json!({"agents_imported":0,"claims_imported":0,"generation":1})
    );
}

#[test]
fn missing_unreadable_and_non_sqlite_paths_refuse_without_writes() {
    let f = Fixture::new();
    let missing = f.root.join("missing.db");
    let non_sqlite = f.root.join("text.db");
    fs::write(&non_sqlite, b"not a sqlite database").unwrap();
    for path in [&missing, &non_sqlite, &f.root] {
        let error = f
            .call(json!({"snapshot_path":path,"request_key":"bad-path"}))
            .unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains(path.to_str().unwrap()), "{error}");
        f.empty(0);
    }
    assert!(
        !missing.exists(),
        "read-only open must not create a missing source"
    );
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&f.source_path, fs::Permissions::from_mode(0o000)).unwrap();
        let result = f.import("unreadable");
        fs::set_permissions(&f.source_path, fs::Permissions::from_mode(0o600)).unwrap();
        let error = result.unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains(f.source_path.to_str().unwrap()));
        f.empty(0);
    }
    #[cfg(windows)]
    {
        let acl = windows_acl::deny_read_for_current_user(&f.source_path).unwrap();
        let result = f.import("unreadable");
        windows_acl::restore(acl).unwrap();
        let error = result.unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert!(error.message.contains(f.source_path.to_str().unwrap()));
        f.empty(0);
    }
}

#[test]
fn decode_key_marker_and_restart_cache_checks_precede_source_access() {
    let mut f = Fixture::new();
    let before = f
        .store()
        .agent_mutation(
            "agent.rename",
            json!({"agent_id":A,"name":"Other","request_key":"rename"}),
            1,
        )
        .unwrap_err();
    assert_eq!(before.code, "authority_not_cut_over");
    for body in [
        json!({"request_key":"k"}),
        json!({"snapshot_path":5,"request_key":"k"}),
        json!({"snapshot_path":"missing","request_key":"k","persona_ref":"x"}),
    ] {
        assert_eq!(f.call(body).unwrap_err().code, "invalid_request");
        f.empty(0);
    }
    for key in [Value::Null, json!("")] {
        assert_eq!(
            f.call(json!({"snapshot_path":"missing","request_key":key}))
                .unwrap_err()
                .code,
            "request_key_required"
        );
        f.empty(0);
    }
    f.assistant(A, "Alice");
    let reply = f.import("once").unwrap();
    f.restart();
    assert_eq!(
        f.call(json!({"snapshot_path":"now-missing","request_key":"once"}))
            .unwrap(),
        reply
    );
    assert_eq!(
        f.call(json!({"snapshot_path":"now-missing","request_key":"new"}))
            .unwrap_err()
            .code,
        "import_already_done"
    );
    assert_eq!(
        f.call(json!({"request_key":"once"})).unwrap_err().code,
        "invalid_request"
    );
    assert_eq!(
        f.store()
            .agent_mutation(
                "agent.update_tag",
                json!({"agent_id":A,"tag":"new","request_key":"once"}),
                1000
            )
            .unwrap_err()
            .code,
        "request_key_reused_across_ops"
    );
    assert_eq!(f.count("registry_journal"), 2);
    f.store()
        .agent_mutation(
            "agent.rename",
            json!({"agent_id":A,"name":"Other","request_key":"rename"}),
            1001,
        )
        .unwrap();
    assert_eq!(f.store().agent_row(A).unwrap().unwrap().name, "Other");
    assert_eq!(
        f.call(json!({"snapshot_path":"missing","request_key":"rename"}))
            .unwrap_err()
            .code,
        "request_key_reused_across_ops"
    );
}

#[test]
fn mid_import_and_marker_faults_roll_back_rows_claims_and_journal_then_retry() {
    for fail_at in ["second-entry", "marker"] {
        let f = Fixture::new();
        f.assistant(A, "Alice");
        f.assistant(B, "Bob");
        let condition = if fail_at == "marker" {
            "NEW.op='agent.cutover'"
        } else {
            "NEW.op='agent.import' AND (SELECT COUNT(*) FROM registry_journal WHERE op='agent.import')=1"
        };
        f.destination.execute_batch(&format!("CREATE TRIGGER fail_import BEFORE INSERT ON registry_journal WHEN {condition} BEGIN SELECT RAISE(ABORT,'injected import fault'); END;")).unwrap();
        let error = f.import("retry").unwrap_err();
        assert_eq!(error.code, "storage_error");
        assert!(error.message.contains("injected import fault"));
        f.empty(0);
        f.destination
            .execute_batch("DROP TRIGGER fail_import")
            .unwrap();
        assert_eq!(
            f.import("retry").unwrap(),
            json!({"agents_imported":2,"claims_imported":2,"generation":3})
        );
        assert_eq!(f.count("agent"), 2);
        assert_eq!(f.count("agent_name_claim"), 2);
        assert_eq!(f.count("registry_journal"), 3);
    }
}

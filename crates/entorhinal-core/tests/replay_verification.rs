use std::{
    collections::BTreeMap,
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{RegisterRequest, RegistryStore, ReplayReport, SetWorkspaceRootRequest};
use rusqlite::{types::ValueRef, Connection};
use serde_json::{json, Value};

static NEXT: AtomicU64 = AtomicU64::new(0);

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Fixture {
    // Drop both connections before removing the scratch database.
    store: RegistryStore,
    conn: Connection,
    _scratch: Scratch,
    project_root: String,
    agent_id: String,
    claim_id: i64,
}

impl Fixture {
    fn new() -> Self {
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/replay-verification-fixtures")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        fs::create_dir_all(root.join("project")).unwrap();
        let root = fs::canonicalize(root).unwrap();
        let project_root =
            RegistryStore::canonical_mutation_root(root.join("project").to_str().unwrap()).unwrap();
        let path = root.join("store.db");
        let store = RegistryStore::open(&StorageDescriptor {
            module_id: "entorhinal-replay-verification-test".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: path.to_string_lossy().into_owned(),
            },
        })
        .unwrap();
        store
            .register(RegisterRequest {
                project_id: Some("p-main".into()),
                name: "Journal name".into(),
                roots: vec![project_root.clone()],
                workspace_id: Some("w-main".into()),
                ..Default::default()
            })
            .unwrap();
        store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "w-main".into(),
                root: Some(RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap()),
                ..Default::default()
            })
            .unwrap();
        store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let created: Value = serde_json::from_slice(&store.agent_mutation(
            "agent.create",
            json!({"role":"assistant", "name":"First", "tag":"test", "request_key":"create"}),
            100,
        ).unwrap()).unwrap();
        let agent_id = created["result"]["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        store
            .agent_mutation(
                "agent.rename",
                json!({"agent_id":agent_id, "name":"Second", "request_key":"rename"}),
                200,
            )
            .unwrap();
        let conn = Connection::open(path).unwrap();
        conn.execute_batch("PRAGMA foreign_keys=ON;").unwrap();
        let claim_id = conn
            .query_row(
                "SELECT claim_id FROM agent_name_claim WHERE released_at_ms IS NOT NULL",
                [],
                |r| r.get(0),
            )
            .unwrap();
        Self {
            store,
            conn,
            _scratch: Scratch(root),
            project_root,
            agent_id,
            claim_id,
        }
    }
}

// Read the real database through a separate connection, discovering every table
// (including the journal, migration ledger and sqlite_sequence). The image
// preserves storage classes and cell bytes, not just selected projection fields.
fn database_image(conn: &Connection) -> Vec<u8> {
    let names = conn
        .prepare("SELECT name FROM sqlite_schema WHERE type='table' ORDER BY name")
        .unwrap()
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .collect::<rusqlite::Result<Vec<_>>>()
        .unwrap();
    let mut image = BTreeMap::new();
    for name in names {
        let quoted = name.replace('"', "\"\"");
        let mut stmt = conn
            .prepare(&format!(
                "SELECT * FROM \"{quoted}\" ORDER BY {}",
                (1..=conn
                    .prepare(&format!("SELECT * FROM \"{quoted}\""))
                    .unwrap()
                    .column_count())
                    .map(|index| index.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ))
            .unwrap();
        let columns = stmt.column_count();
        let rows = stmt
            .query_map([], |row| {
                (0..columns)
                    .map(|index| {
                        Ok(match row.get_ref(index)? {
                            ValueRef::Null => vec![0],
                            ValueRef::Integer(value) => {
                                [vec![1], value.to_be_bytes().to_vec()].concat()
                            }
                            ValueRef::Real(value) => {
                                [vec![2], value.to_be_bytes().to_vec()].concat()
                            }
                            ValueRef::Text(value) => [vec![3], value.to_vec()].concat(),
                            ValueRef::Blob(value) => [vec![4], value.to_vec()].concat(),
                        })
                    })
                    .collect::<rusqlite::Result<Vec<_>>>()
            })
            .unwrap()
            .collect::<rusqlite::Result<Vec<_>>>()
            .unwrap();
        image.insert(name, rows);
    }
    serde_json::to_vec(&image).unwrap()
}

#[test]
fn clean_verify_replays_and_preserves_every_table_byte_for_byte() {
    let f = Fixture::new();
    // Replaying an already-correct projection can write identical row values.
    // A transactional trigger makes even that write observable if verification
    // accidentally commits; the probe itself must be rolled back too.
    f.conn
        .execute_batch(
            "CREATE TABLE verify_write_probe (deleted_project TEXT);
        CREATE TRIGGER observe_verify_delete AFTER DELETE ON project BEGIN
            INSERT INTO verify_write_probe VALUES (old.project_id);
        END;",
        )
        .unwrap();
    let before = database_image(&f.conn);
    let generation = f.store.generation().unwrap();
    let reply = f.store.verify().unwrap();
    assert!(reply.ok);
    assert_eq!(
        reply.replay,
        ReplayReport {
            ok: true,
            tables: Vec::new()
        }
    );
    assert_eq!(reply.generation, generation);
    assert_eq!(reply.local_members, 1);
    assert_eq!(reply.project_workspaces, 1);
    assert!(reply.mismatches.is_empty());
    assert!(
        database_image(&f.conn) == before,
        "verify committed replay side effects"
    );
    // The connection must remain usable after the rollback.
    assert!(f.store.verify().unwrap().ok);
    assert_eq!(database_image(&f.conn), before);
}

#[test]
fn corrupt_verify_reports_all_tables_and_rebuild_repairs_the_same_difference() {
    let f = Fixture::new();
    let healthy = database_image(&f.conn);
    assert!(f.store.verify().unwrap().replay.ok);
    // Corrupt every projection, including non-key columns and a composite key.
    // None of these writes has a journal entry for replay to reproduce.
    for index in 0..7 {
        f.conn
            .execute(
                "INSERT INTO project(project_id,name) VALUES (?1,'Unjournaled')",
                [format!("extra-{index}")],
            )
            .unwrap();
    }
    f.conn
        .execute(
            "DELETE FROM agent_name_claim WHERE claim_id=?1",
            [f.claim_id],
        )
        .unwrap();
    let membership_consistent = database_image(&f.conn);
    let drift_only = f.store.verify().unwrap();
    assert!(drift_only.mismatches.is_empty());
    assert_eq!(drift_only.local_members, 1);
    assert_eq!(drift_only.project_workspaces, 1);
    assert!(!drift_only.ok, "replay drift alone must make verify fail");
    assert!(!drift_only.replay.ok);
    assert_eq!(
        drift_only
            .replay
            .tables
            .iter()
            .map(|t| (t.table.as_str(), t.missing, t.unexpected))
            .collect::<Vec<_>>(),
        [("agent_name_claim", 1, 0), ("project", 0, 7)]
    );
    assert_eq!(database_image(&f.conn), membership_consistent);
    // The unjournaled w-extra row alone produces workspace (0 missing, 1
    // unexpected). Clearing w-main's local path changes workspace_root instead.
    f.conn.execute_batch("UPDATE project SET name='Wrong name' WHERE project_id='p-main';
        UPDATE project_root SET added_at=added_at+1;
        INSERT INTO project_alias(old_id,project_id,created_at) VALUES ('extra-alias','p-main',10);
        INSERT INTO derived_root_parent(canonical_parent,project_id,source_root,registration_epoch) VALUES ('/untracked-parent','p-main',NULL,NULL);
        INSERT INTO workspace(workspace_id,name,created_at,updated_at) VALUES ('w-extra','Unjournaled',10,10);
        UPDATE workspace_root SET root=NULL WHERE workspace_id='w-main';
        UPDATE project_workspace SET workspace_id='w-extra';
        DELETE FROM workspace_member;
        INSERT INTO root_binding(canonical_root,project_id,incarnation,registration_epoch,bound_seq) VALUES ('/untracked-root','p-main','token','extra-binding',1);
        INSERT INTO retired_binding(registration_epoch,canonical_root,project_id,incarnation,reason,retired_seq) VALUES ('extra-retired','/retired-root','p-main','token','removed',1);
        INSERT INTO root_approval(registration_epoch,approved_seq) VALUES ('extra-binding',1);
        UPDATE agent SET agent_generation=agent_generation+1;").unwrap();
    let corrupted = database_image(&f.conn);
    let reply = f.store.verify().unwrap();
    assert!(!reply.ok);
    assert!(!reply.replay.ok);
    assert_eq!(reply.local_members, 0);
    assert_eq!(reply.project_workspaces, 1);
    assert_eq!(reply.mismatches, ["w-extra:p-main"]);
    let tables = &reply.replay.tables;
    // Expectations come from the raw changes above, not the replay comparator.
    assert_eq!(
        tables
            .iter()
            .map(|t| (t.table.as_str(), t.missing, t.unexpected))
            .collect::<Vec<_>>(),
        vec![
            ("agent_name_claim", 1, 0),
            ("agent", 1, 1),
            ("workspace_member", 1, 0),
            ("project_workspace", 1, 1),
            ("project_alias", 0, 1),
            ("derived_root_parent", 0, 1),
            ("project_root", 1, 1),
            ("project", 1, 8),
            ("workspace_root", 1, 1),
            ("workspace", 0, 1),
            ("root_binding", 0, 1),
            ("retired_binding", 0, 1),
            ("root_approval", 0, 1),
        ]
    );
    let expected_keys = [
        (json!([{"claim_id":f.claim_id}]), json!([])),
        (
            json!([{"agent_id":f.agent_id}]),
            json!([{"agent_id":f.agent_id}]),
        ),
        (
            json!([{"workspace_id":"w-main","ref_kind":"local","device_fingerprint":"","project_id":"p-main"}]),
            json!([]),
        ),
        (
            json!([{"project_id":"p-main"}]),
            json!([{"project_id":"p-main"}]),
        ),
        (json!([]), json!([{"old_id":"extra-alias"}])),
        (json!([]), json!([{"canonical_parent":"/untracked-parent"}])),
        (
            json!([{"canonical_root":f.project_root}]),
            json!([{"canonical_root":f.project_root}]),
        ),
        (
            json!([{"project_id":"p-main"}]),
            json!([
                {"project_id":"extra-0"}, {"project_id":"extra-1"}, {"project_id":"extra-2"},
                {"project_id":"extra-3"},
            ]),
        ),
        (
            json!([{"workspace_id":"w-main"}]),
            json!([{"workspace_id":"w-main"}]),
        ),
        (json!([]), json!([{"workspace_id":"w-extra"}])),
        (json!([]), json!([{"canonical_root":"/untracked-root"}])),
        (json!([]), json!([{"registration_epoch":"extra-retired"}])),
        (json!([]), json!([{"registration_epoch":"extra-binding"}])),
    ];
    for (table, (missing, unexpected)) in tables.iter().zip(expected_keys) {
        assert_eq!(
            serde_json::to_value(&table.missing_keys).unwrap(),
            missing,
            "{} missing keys",
            table.table
        );
        assert_eq!(
            serde_json::to_value(&table.unexpected_keys).unwrap(),
            unexpected,
            "{} unexpected keys",
            table.table
        );
        assert!(table.missing_keys.len() + table.unexpected_keys.len() <= 5);
    }
    assert_eq!(
        database_image(&f.conn),
        corrupted,
        "verify repaired live tables"
    );
    let rebuilt = f.store.rebuild().unwrap();
    assert_eq!(rebuilt.generation, reply.generation);
    assert_eq!(rebuilt.replay, reply.replay);
    assert_eq!(
        database_image(&f.conn),
        healthy,
        "rebuild must restore the journal projection"
    );
    let clean = f.store.verify().unwrap();
    assert!(clean.ok && clean.replay.ok);
    assert!(clean.replay.tables.is_empty());
    assert!(clean.mismatches.is_empty());
    assert_eq!(database_image(&f.conn), healthy);
    let no_change = f.store.rebuild().unwrap();
    assert!(no_change.replay.ok && no_change.replay.tables.is_empty());
}

#[test]
fn replay_error_returns_an_error_and_rolls_back_every_table() {
    let f = Fixture::new();
    // Force both deletion of live rows and a successful earlier journal replay
    // before the bad payload is decoded. A failed verify must undo all of it.
    f.conn
        .execute(
            "INSERT INTO project(project_id,name) VALUES ('extra','Unjournaled')",
            [],
        )
        .unwrap();
    f.conn.execute("UPDATE registry_journal SET payload_json='not JSON' WHERE seq=(SELECT MAX(seq) FROM registry_journal)", []).unwrap();
    let before = database_image(&f.conn);
    let error = f
        .store
        .verify()
        .expect_err("an undecodable journal must not report ok");
    assert!(error.to_string().contains("expected"), "{error}");
    assert_eq!(
        database_image(&f.conn),
        before,
        "failed verify persisted partial replay"
    );
    // Neither the malformed journal nor the connection is changed by failure.
    assert!(f.store.verify().is_err());
    assert_eq!(database_image(&f.conn), before);
    // A normal write still works after the failed transaction.
    f.store
        .set_workspace_root(SetWorkspaceRootRequest {
            workspace_id: "w-main".into(),
            root: Some(f.project_root.clone()),
            ..Default::default()
        })
        .unwrap();
}

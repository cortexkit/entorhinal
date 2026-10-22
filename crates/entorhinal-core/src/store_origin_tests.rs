use super::*;
use serde_json::{json, Value};
const OLD: &str = "11111111111111111111111111111111";
const NEW: &str = "22222222222222222222222222222222";

fn populated() -> (TestStore, String, String) {
    let mut f = TestStore::new("store-origin");
    let root = f.root.join("repo");
    fs::create_dir_all(&root).unwrap();
    assert!(std::process::Command::new("git")
        .args(["init", "-q"])
        .arg(&root)
        .status()
        .unwrap()
        .success());
    let root = RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap();
    f.store.set_root_records(true);
    f.store
        .register(RegisterRequest {
            project_id: Some("P".into()),
            name: "Project".into(),
            workspace_id: Some("W".into()),
            roots: vec![root.clone()],
            ..Default::default()
        })
        .unwrap();
    f.store.bind_root(&root, "test").unwrap();
    f.store.approve_root(&root, "test").unwrap();
    f.store
        .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let body = f
        .store
        .agent_mutation(
            "agent.create",
            json!({"role":"head","name":"Ada","tag":"head","project_id":"P","request_key":"head"}),
            1,
        )
        .unwrap();
    let id = serde_json::from_slice::<Value>(&body).unwrap()["result"]["agent"]["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    f.store.check_store_origin(Some(OLD), 1).unwrap();
    f.store.check_store_origin(Some(NEW), 2).unwrap();
    (f, root, id)
}
fn count(store: &RegistryStore, table: &str) -> i64 {
    store
        .read(|c| c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0)))
        .unwrap()
}
fn snapshot(store: &RegistryStore, table: &str) -> Vec<Vec<rusqlite::types::Value>> {
    store
        .read(|c| {
            let mut q = c.prepare(&format!("SELECT * FROM {table} ORDER BY 1"))?;
            let n = q.column_count();
            let rows = q
                .query_map([], |r| (0..n).map(|i| r.get(i)).collect())?
                .collect();
            rows
        })
        .unwrap()
}
fn seed_local(store: &RegistryStore) {
    store.db.with_conn_fenced(|tx| tx.execute_batch(r#"
        INSERT INTO self_identity VALUES(1,'old-author',1);
        INSERT INTO retired_binding SELECT 'retired',canonical_root,project_id,incarnation,'replaced',1 FROM root_binding LIMIT 1;
        INSERT INTO workspace_root SELECT workspace_id,'/old/workspace',1 FROM workspace;
        INSERT INTO root_owned_remotes SELECT canonical_root,'["origin"]' FROM project_root;
        INSERT INTO claim_cache VALUES('agent', 'old-author',1,NULL,1,1,0,0);
        INSERT INTO role_cache VALUES('leader','old-author',1,'cloud',0,1,1,0);
        INSERT INTO role_take_request VALUES('req','leader',0,NULL,0,X'00',1,1);
        INSERT INTO agent_move(cut_id,agent_id,side,peer,step,plan,record_version,seal_sent,attempts,created_at_ms,updated_at_ms) VALUES('cut','agent','source','peer','preparing','{}',1,0,1,1,1);
        UPDATE move_setting SET lid_close_moves=1;
        INSERT INTO session_binding VALUES('session','ref','agent','harness','session','/repo','host',1,'broca',NULL);
        INSERT INTO session_network VALUES('session','ref','no',1);
        INSERT INTO pending_entry VALUES('pending',1);
        INSERT INTO consent_grant VALUES('grant','agent','hire','P',1,'card',NULL);
        INSERT INTO consent_request VALUES('agent','tool','key','digest','scope',1,1,'card','{}','awaiting',NULL);
        INSERT INTO consent_write_plan VALUES('agent','tool','key','{}');
        INSERT INTO consent_feed_cursor VALUES('scope',1,4);
        INSERT INTO agent_audit(time_ms,agent_id,action,target_json,authorization_json,outcome_json) VALUES(1,'agent','hire','{}','{}','{}');
        INSERT INTO project_hire_cap VALUES('P',5);
        INSERT INTO catalog_registry VALUES('registry',1,1,'incarnation');
        INSERT INTO catalog_scope_map VALUES('@scope','registry');
        INSERT INTO catalog_registry_key VALUES('registry',0,'key','pub',NULL,NULL);
        INSERT INTO catalog_feed VALUES('@scope','registry',1,NULL);
        INSERT INTO catalog_status VALUES('pkg','1','registry',1,'then','then',0,'{}','sig');
        INSERT INTO catalog_starter_proof VALUES('pkg','1','proof');
        INSERT INTO catalog_publisher_grant VALUES('grant','pub','install','agent','{}',1,NULL,NULL);
        INSERT INTO catalog_audit VALUES(1,1,'grant','grant','{}');
        INSERT INTO catalog_plan VALUES('plan','agent','install','spec','pending',NULL,NULL,'digest',NULL,1,NULL);
        INSERT INTO catalog_prepared VALUES('plan','{}');
        INSERT INTO catalog_generation VALUES('gen','agent','plan','active','{}','digest',1,1,NULL,'[]');
        INSERT INTO catalog_transition VALUES('entity','id','transition','digest','{}');
        CREATE TABLE catalog_poll_clock(id INTEGER PRIMARY KEY,offset_ms INTEGER NOT NULL);
        INSERT INTO catalog_poll_clock VALUES(1,42);
    "#)).unwrap();
}

#[test]
fn adopt_store_clears_local_state_keeps_shared() {
    let (f, _, _) = populated();
    seed_local(&f.store);
    let shared = [
        "project",
        "workspace",
        "project_workspace",
        "project_alias",
        "project_root_key",
        "project_root",
        "derived_root_parent",
        "workspace_member",
        "agent",
        "agent_name_claim",
        "member_name",
        "workspace_document",
        "catalog_pin",
        "catalog_pin_set",
    ];
    let before: Vec<_> = shared.iter().map(|t| snapshot(&f.store, t)).collect();
    let journal = snapshot(&f.store, "registry_journal");
    let generation = f.store.generation().unwrap();
    f.store.adopt_store(NEW, false, 3).unwrap();
    // This independent list makes omitting any cleared table observable.
    for table in [
        "self_identity",
        "claim_cache",
        "role_cache",
        "role_take_request",
        "agent_move",
        "session_network",
        "session_binding",
        "pending_entry",
        "consent_write_plan",
        "consent_request",
        "consent_feed_cursor",
        "consent_grant",
        "agent_audit",
        "project_hire_cap",
        "catalog_generation",
        "catalog_prepared",
        "catalog_plan",
        "catalog_transition",
        "catalog_starter_proof",
        "catalog_status",
        "catalog_feed",
        "catalog_scope_map",
        "catalog_registry_key",
        "catalog_registry",
        "catalog_publisher_grant",
        "catalog_audit",
        "catalog_poll_clock",
        "root_binding",
        "retired_binding",
        "root_approval",
        "workspace_root",
        "root_owned_remotes",
    ] {
        assert_eq!(count(&f.store, table), 0, "adoption must clear {table}");
    }
    assert!(!f.store.move_setting().unwrap().lid_close_moves);
    for (table, rows) in shared.iter().zip(before) {
        assert_eq!(snapshot(&f.store, table), rows, "kept {table}");
    }
    let after = snapshot(&f.store, "registry_journal");
    assert_eq!(&after[..journal.len()], &journal);
    assert_eq!(after.len(), journal.len() + 1);
    assert!(f
        .store
        .agent_changes(generation, None)
        .unwrap()
        .entries
        .is_empty());
    assert!(f
        .store
        .journal_tail(0, 1000)
        .unwrap()
        .entries
        .iter()
        .all(|e| e.op != "store.adopt"));
    assert_eq!(f.store.identity_log_status().unwrap().state, "disabled");
    assert!(!f.store.is_foreign());
    assert!(f.store.verify().unwrap().ok);
}

#[test]
fn adopt_store_rebuild_does_not_resurrect_authority() {
    let (f, root, _) = populated();
    f.store.adopt_store(NEW, false, 3).unwrap();
    f.store.rebuild().unwrap();
    assert_eq!(
        count(&f.store, "root_approval"),
        0,
        "replay must clear old approvals"
    );
    assert_eq!(
        count(&f.store, "root_binding"),
        0,
        "replay must clear old bindings"
    );
    f.store.bind_root(&root, "test").unwrap();
    f.store.approve_root(&root, "test").unwrap();
    let bound = snapshot(&f.store, "root_binding");
    let approvals = snapshot(&f.store, "root_approval");
    assert!(!approvals.is_empty());
    f.store.rebuild().unwrap();
    assert_eq!(snapshot(&f.store, "root_binding"), bound);
    assert_eq!(snapshot(&f.store, "root_approval"), approvals);
    assert!(f.store.verify().unwrap().ok);
}

#[test]
fn adopt_store_shared_registry_resets_and_replays_empty() {
    let (f, _, _) = populated();
    f.store
        .db
        .with_conn_fenced(|tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled'", [])
                .map(|_| ())
        })
        .unwrap();
    assert!(f.store.check_enable_preconditions(1).is_err());
    f.store.adopt_store(NEW, false, 3).unwrap();
    assert!(f.store.enable_registry_is_empty().unwrap());
    f.store.check_enable_preconditions(1).unwrap();
    f.store.rebuild().unwrap();
    for table in crate::mutations::DERIVED_TABLES {
        assert_eq!(count(&f.store, table), 0, "reset {table}");
    }
    assert!(f.store.verify().unwrap().ok);
    assert!(f
        .store
        .journal_tail(0, 1000)
        .unwrap()
        .entries
        .iter()
        .all(|e| e.op != "store.adopt"));
}

#[test]
fn adopt_store_same_machine_changes_only_origin() {
    let (f, _, _) = populated();
    seed_local(&f.store);
    let tables = f.store.read(|c| c.prepare("SELECT name FROM sqlite_schema WHERE type='table' AND name NOT IN ('store_origin','cortexkit_fence') ORDER BY name")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()).unwrap();
    let before: Vec<_> = tables.iter().map(|t| snapshot(&f.store, t)).collect();
    f.store.adopt_store(NEW, true, 3).unwrap();
    for (table, rows) in tables.iter().zip(before) {
        assert_eq!(snapshot(&f.store, table), rows, "same-machine kept {table}");
    }
    assert_eq!(
        f.store.check_store_origin(Some(NEW), 4).unwrap().as_deref(),
        Some(NEW)
    );
}

#[test]
fn adopt_store_failure_rolls_back_the_entire_reset() {
    let (f, _, _) = populated();
    seed_local(&f.store);
    let journal = snapshot(&f.store, "registry_journal");
    let author = snapshot(&f.store, "self_identity");
    let approvals = snapshot(&f.store, "root_approval");
    f.store.db.with_conn_fenced(|tx| tx.execute_batch("CREATE TRIGGER refuse_origin BEFORE INSERT ON store_origin BEGIN SELECT RAISE(ABORT,'origin refused'); END;")).unwrap();
    assert!(f.store.adopt_store(NEW, false, 3).is_err());
    assert_eq!(snapshot(&f.store, "registry_journal"), journal);
    assert_eq!(snapshot(&f.store, "self_identity"), author);
    assert_eq!(snapshot(&f.store, "root_approval"), approvals);
    assert!(f.store.is_foreign());
}

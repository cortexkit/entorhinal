//! Schema for the optional identity log and machine-local root paths.

/// Migration 7 leaves legacy journal bytes intact and opts every store out of
/// sharing. Root keys are chosen only when the operator later enables the log.
pub(crate) const V7_IDENTITY_LOG: &str = r#"
ALTER TABLE registry_journal ADD COLUMN stream TEXT NOT NULL DEFAULT 'local'
    CHECK(stream IN ('shared','local'));
ALTER TABLE registry_journal ADD COLUMN entry_id TEXT NULL;
ALTER TABLE registry_journal ADD COLUMN log_position INTEGER NULL;
ALTER TABLE registry_journal ADD COLUMN origin TEXT NOT NULL DEFAULT 'here'
    CHECK(origin IN ('here','log'));
ALTER TABLE registry_journal ADD COLUMN entry BLOB NULL;

CREATE TABLE identity_log_state (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    state TEXT NOT NULL DEFAULT 'disabled'
        CHECK(state IN ('disabled','enabling','joining','enabled')),
    last_applied_position INTEGER NOT NULL DEFAULT 0 CHECK(last_applied_position >= 0),
    last_seen_head INTEGER NOT NULL DEFAULT 0 CHECK(last_seen_head >= 0),
    enable_parts TEXT NULL,
    enable_backfill TEXT NULL
);
INSERT INTO identity_log_state(id) VALUES(1);

CREATE TABLE pending_entry (
    entry_id TEXT PRIMARY KEY NOT NULL,
    expected_head INTEGER NOT NULL CHECK(expected_head >= 0)
);

CREATE TABLE project_root_key (
    project_id TEXT NOT NULL REFERENCES project(project_id),
    kind TEXT NOT NULL CHECK(kind IN ('remote','label')),
    root_key TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY(project_id, kind, root_key)
);
CREATE UNIQUE INDEX project_root_key_remote ON project_root_key(root_key)
    WHERE kind = 'remote';
ALTER TABLE project_root ADD COLUMN root_key TEXT NULL;

CREATE TABLE workspace_root (
    workspace_id TEXT PRIMARY KEY NOT NULL REFERENCES workspace(workspace_id) ON DELETE CASCADE,
    root TEXT NULL,
    updated_at INTEGER NOT NULL
);
-- A later rename updates workspace.updated_at, not the root's timestamp. Use
-- the latest root-setting journal row, accepting both historical request shapes.
-- Every non-null path with no setting row is carried, using workspace.updated_at
-- as its timestamp. A null path is carried only after an explicit journal clear.
-- A deleted and recreated workspace must not inherit a pre-removal local path.
WITH requests AS (
    SELECT seq, op, created_at,
           COALESCE(json_extract(payload_json, '$.request'), payload_json) AS request
    FROM registry_journal WHERE op IN ('set_workspace_root','remove')
)
INSERT INTO workspace_root(workspace_id, root, updated_at)
SELECT w.workspace_id, w.root, COALESCE(j.created_at, w.updated_at)
FROM workspace w LEFT JOIN requests j ON j.seq = (
    SELECT MAX(s.seq) FROM requests s
    WHERE s.op = 'set_workspace_root'
      AND json_extract(s.request, '$.workspaceId') = w.workspace_id
      AND s.seq > COALESCE((
          SELECT MAX(d.seq) FROM requests d
          WHERE d.op = 'remove'
            AND json_extract(d.request, '$.workspaceId') = w.workspace_id
      ), 0)
)
WHERE (j.seq IS NOT NULL AND (w.root IS NOT NULL OR json_type(j.request, '$.root') = 'null'))
   OR (w.root IS NOT NULL AND NOT EXISTS (
       SELECT 1 FROM requests s WHERE s.op = 'set_workspace_root'
         AND json_extract(s.request, '$.workspaceId') = w.workspace_id
   ));

-- Roots from older write paths or hand edits have no replay source. Record only
-- those fallback after-images locally, without rewriting historical rows or
-- changing the journal head of an ordinary store (including an empty store).
INSERT INTO registry_journal(op, payload_json, actor, created_at, principal, stream, origin)
SELECT 'workspace_root.backfill',
       json_object('roots', json_group_array(json_object(
           'workspaceId', workspace_id, 'root', root, 'updatedAt', updated_at
       ))),
       'migration', MAX(updated_at), 'entorhinal', 'local', 'here'
FROM (
    SELECT r.workspace_id, r.root, r.updated_at FROM workspace_root r
    WHERE r.root IS NOT NULL AND NOT EXISTS (
        SELECT 1 FROM registry_journal j WHERE j.op = 'set_workspace_root'
          AND json_extract(COALESCE(json_extract(j.payload_json, '$.request'), j.payload_json), '$.workspaceId') = r.workspace_id
    )
    ORDER BY r.workspace_id
)
HAVING COUNT(*) > 0;
ALTER TABLE workspace DROP COLUMN root;
"#;

#[derive(serde::Deserialize)]
struct WorkspaceRootBackfill {
    roots: Vec<WorkspaceRootAfterImage>,
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct WorkspaceRootAfterImage {
    workspace_id: String,
    root: String,
    updated_at: i64,
}

pub(crate) fn replay_workspace_root_backfill(
    tx: &rusqlite::Transaction<'_>,
    value: serde_json::Value,
) -> rusqlite::Result<()> {
    let backfill: WorkspaceRootBackfill = serde_json::from_value(value)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    // A migration after-image describes the local path only. Its timestamp must
    // not overwrite the workspace's independently journaled rename timestamp.
    for root in backfill.roots {
        tx.execute(
            "INSERT INTO workspace_root(workspace_id,root,updated_at) VALUES(?1,?2,?3)
             ON CONFLICT(workspace_id) DO UPDATE SET root=excluded.root,updated_at=excluded.updated_at",
            rusqlite::params![root.workspace_id, root.root, root.updated_at],
        )?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{mutations::tests::Fixture, *};
    use cortexkit_store::{Isolation, StorageBackend};
    use rusqlite::types::Value as SqlValue;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn descriptor(f: &Fixture) -> StorageDescriptor {
        StorageDescriptor {
            module_id: "log-schema-legacy".into(),
            storage_namespace: "tests".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: f.root.join("legacy.db").to_string_lossy().into_owned(),
            },
        }
    }

    // Read actual SQLite cells from every application table, including the
    // journal and durable log state, rather than comparing only wire replies.
    // The store's writer-fence epoch is lease metadata, not a replayed cell.
    fn cells(store: &RegistryStore) -> BTreeMap<String, Vec<Vec<SqlValue>>> {
        store.read(|conn| {
            let tables = conn.prepare(
                "SELECT name FROM sqlite_schema WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name <> 'cortexkit_fence' ORDER BY name",
            )?.query_map([], |r| r.get::<_, String>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let mut result = BTreeMap::new();
            for table in tables {
                let mut stmt = conn.prepare(&format!("SELECT * FROM \"{table}\""))?;
                let columns = stmt.column_count();
                drop(stmt);
                let order = (1..=columns).map(|n| n.to_string()).collect::<Vec<_>>().join(",");
                stmt = conn.prepare(&format!("SELECT * FROM \"{table}\" ORDER BY {order}"))?;
                let rows = stmt.query_map([], |r| {
                    (0..columns).map(|n| r.get(n)).collect::<rusqlite::Result<Vec<SqlValue>>>()
                })?.collect::<rusqlite::Result<Vec<_>>>()?;
                result.insert(table, rows);
            }
            Ok(result)
        }).unwrap()
    }

    #[test]
    fn migration_7_initializes_disabled_state_and_enforces_log_schema() {
        let f = Fixture::new("log-schema-defaults");
        f.store.db.with_conn_fenced(|tx| {
            assert_eq!(tx.query_row(
                "SELECT id,state,last_applied_position,last_seen_head,enable_parts,enable_backfill FROM identity_log_state",
                [], |r| Ok((r.get::<_, i64>(0)?,r.get::<_, String>(1)?,r.get::<_, i64>(2)?,r.get::<_, i64>(3)?,r.get::<_, Option<String>>(4)?,r.get::<_, Option<String>>(5)?)),
            )?, (1,"disabled".into(),0,0,None,None));
            for sql in [
                "INSERT INTO identity_log_state(id) VALUES(2)",
                "UPDATE identity_log_state SET state='unknown'",
                "INSERT INTO pending_entry VALUES('negative',-1)",
            ] {
                assert!(tx.execute(sql, []).is_err(), "accepted {sql}");
            }
            tx.execute("INSERT INTO pending_entry VALUES('attempt',0)", [])?;
            assert!(tx.execute("INSERT INTO pending_entry VALUES('attempt',1)", []).is_err());
            tx.execute("DELETE FROM pending_entry", [])?;
            tx.execute("INSERT INTO registry_journal(op,payload_json) VALUES('test','{}')", [])?;
            assert_eq!(tx.query_row(
                "SELECT stream,origin,entry_id,log_position,entry FROM registry_journal", [],
                |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?,r.get::<_, Option<String>>(2)?,r.get::<_, Option<i64>>(3)?,r.get::<_, Option<Vec<u8>>>(4)?)),
            )?, ("local".into(),"here".into(),None,None,None));
            for sql in [
                "UPDATE registry_journal SET stream='other'",
                "UPDATE registry_journal SET origin='elsewhere'",
            ] {
                assert!(tx.execute(sql, []).is_err(), "accepted {sql}");
            }
            tx.execute_batch("INSERT INTO project(project_id) VALUES('p'),('q');
                INSERT INTO project_root(canonical_root,project_id) VALUES('/p','p');
                INSERT INTO project_root_key VALUES('p','remote','owner/repo',1);
                INSERT INTO project_root_key VALUES('p','label','owner/repo',1),('q','label','owner/repo',1);")?;
            assert!(tx.execute("INSERT INTO project_root_key VALUES('q','remote','owner/repo',2)", []).is_err());
            assert!(tx.execute("INSERT INTO project_root_key VALUES('q','other','key',2)", []).is_err());
            assert!(tx.execute("INSERT INTO project_root_key VALUES('missing','label','key',2)", []).is_err());
            assert_eq!(tx.query_row("SELECT root_key FROM project_root", [], |r| r.get::<_, Option<String>>(0))?, None);
            assert_eq!(tx.query_row("SELECT COUNT(*) FROM workspace_root", [], |r| r.get::<_, i64>(0))?, 0);
            Ok(())
        }).unwrap();
    }

    #[test]
    fn v6_upgrade_uses_root_journal_timestamp_and_replays_every_cell() {
        let f = Fixture::new("log-schema-upgrade");
        let descriptor = descriptor(&f);
        let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..6]).unwrap();
        old.db.with_conn_fenced(|tx| {
            let entries = [
                ("register", json!({"projectId":"p","name":"P","roots":["/legacy/p"],"workspaceId":"W"}), 10),
                ("set_workspace_root", json!({"workspaceId":"W","root":"/old"}), 20),
                ("set_workspace_root", json!({"request":{"workspaceId":"W","root":"/current"}}), 30),
                ("seed_import", json!({"source":"mc","payload":{"workspaces":[{"workspaceId":"W","name":"Renamed"},{"workspaceId":"C","name":"Cleared"},{"workspaceId":"N","name":"Never set"},{"workspaceId":"R","name":"Recreated"}]}}), 40),
                ("set_workspace_root", json!({"workspaceId":"C","root":"/clear-me"}), 50),
                ("set_workspace_root", json!({"workspaceId":"C","root":null}), 60),
                ("set_workspace_root", json!({"workspaceId":"R","root":"/removed"}), 70),
                ("remove", json!({"workspaceId":"R"}), 80),
                ("seed_import", json!({"source":"mc","payload":{"workspaces":[{"workspaceId":"R","name":"Recreated"}]}}), 90),
            ];
            for (op,payload,now) in entries {
                tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at,principal) VALUES(?1,?2,'operator',?3,'direct')", params![op,payload.to_string(),now])?;
            }
            tx.execute_batch("INSERT INTO project VALUES('p','P',0,NULL,10,10);
                INSERT INTO workspace VALUES('W','Renamed',10,40,'/current'),('C','Cleared',40,60,NULL),('N','Never set',40,40,NULL),('R','Recreated',90,90,NULL);
                INSERT INTO project_root VALUES('/legacy/p','p',10);
                INSERT INTO project_workspace VALUES('p','W');
                INSERT INTO workspace_member VALUES('W','local','','p');")?;
            tx.execute("INSERT INTO project_alias VALUES(?1,'p',10)", [implicit_project_id("/legacy/p")])?;
            Ok(())
        }).unwrap();
        let original_journal = old.journal_tail(0, 100).unwrap();
        drop(old);
        let store = RegistryStore::open(&descriptor).unwrap();
        let migrated = cells(&store);
        assert_eq!(
            migrated["workspace_root"],
            vec![
                vec![
                    SqlValue::Text("C".into()),
                    SqlValue::Null,
                    SqlValue::Integer(60)
                ],
                vec![
                    SqlValue::Text("W".into()),
                    SqlValue::Text("/current".into()),
                    SqlValue::Integer(30)
                ],
            ]
        );
        assert_eq!(
            migrated["workspace"][3],
            vec![
                SqlValue::Text("W".into()),
                SqlValue::Text("Renamed".into()),
                SqlValue::Integer(10),
                SqlValue::Integer(40)
            ]
        );
        assert_eq!(
            serde_json::to_vec(&store.journal_tail(0, 100).unwrap()).unwrap(),
            serde_json::to_vec(&original_journal).unwrap()
        );
        for row in &migrated["registry_journal"] {
            assert_eq!(
                &row[8..],
                &[
                    SqlValue::Text("local".into()),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Text("here".into()),
                    SqlValue::Null
                ]
            );
        }
        assert_eq!(migrated["project_root"][0][3], SqlValue::Null);
        assert!(migrated["project_root_key"].is_empty());
        assert!(migrated["pending_entry"].is_empty());
        let verified = store.verify().unwrap();
        assert!(verified.ok, "{verified:?}");
        assert_eq!(cells(&store), migrated, "verify wrote live cells");
        assert!(store.rebuild().unwrap().replay.ok);
        assert_eq!(cells(&store), migrated, "rebuild changed migrated cells");
        store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE workspace_root SET updated_at=999 WHERE workspace_id='W'",
                    [],
                )
            })
            .unwrap();
        let corrupt = cells(&store);
        let verified = store.verify().unwrap();
        assert!(!verified.ok);
        assert_eq!(verified.replay.tables.len(), 1);
        assert_eq!(verified.replay.tables[0].table, "workspace_root");
        assert_eq!(cells(&store), corrupt, "verify repaired live cells");
        assert!(!store.rebuild().unwrap().replay.ok);
        assert_eq!(
            cells(&store),
            migrated,
            "rebuild did not repair local root cells"
        );
    }

    #[test]
    fn v6_unjournaled_workspace_roots_are_carried_and_replay_cleanly() {
        let f = Fixture::new("log-schema-unjournaled");
        let descriptor = descriptor(&f);
        let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..6]).unwrap();
        old.db.with_conn_fenced(|tx| {
            let entries = [
                ("register", json!({"projectId":"p","name":"P","workspaceId":"F"}), 10),
                ("seed_import", json!({"source":"mc","payload":{"workspaces":[{"workspaceId":"F","name":"Renamed"},{"workspaceId":"H","name":"Second fallback"},{"workspaceId":"N","name":"Unset"},{"workspaceId":"L","name":"Logged"},{"workspaceId":"R","name":"Recreated"}]}}), 60),
                ("set_workspace_root", json!({"workspaceId":"L","root":"/logged"}), 70),
                ("set_workspace_root", json!({"workspaceId":"R","root":"/deleted"}), 80),
                ("remove", json!({"workspaceId":"R"}), 90),
                ("seed_import", json!({"source":"mc","payload":{"workspaces":[{"workspaceId":"R","name":"Recreated"}]}}), 100),
            ];
            for (op,payload,now) in entries {
                tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at,principal) VALUES(?1,?2,'operator',?3,'direct')", params![op,payload.to_string(),now])?;
            }
            tx.execute_batch("INSERT INTO project(project_id,name,implicit,created_at,updated_at) VALUES('p','P',0,10,10);
                INSERT INTO workspace(workspace_id,name,created_at,updated_at,root) VALUES
                    ('F','Renamed',10,60,'/unjournaled'),('H','Second fallback',60,60,'/second'),
                    ('N','Unset',60,60,NULL),('L','Logged',60,70,'/logged'),('R','Recreated',100,100,'/deleted');
                INSERT INTO project_workspace(project_id,workspace_id) VALUES('p','F');
                INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES('F','local','','p');")?;
            Ok(())
        }).unwrap();
        assert_eq!(old.generation().unwrap(), 6);
        drop(old);

        let store = RegistryStore::open(&descriptor).unwrap();
        let migrated = cells(&store);
        assert_eq!(
            migrated["workspace_root"],
            vec![
                vec![
                    SqlValue::Text("F".into()),
                    SqlValue::Text("/unjournaled".into()),
                    SqlValue::Integer(60)
                ],
                vec![
                    SqlValue::Text("H".into()),
                    SqlValue::Text("/second".into()),
                    SqlValue::Integer(60)
                ],
                vec![
                    SqlValue::Text("L".into()),
                    SqlValue::Text("/logged".into()),
                    SqlValue::Integer(70)
                ],
            ],
            "fallback paths must survive, unset and pre-removal paths must not"
        );
        assert_eq!(store.generation().unwrap(), 7);
        let backfill = store.journal_tail(6, 10).unwrap().entries;
        assert_eq!(backfill.len(), 1);
        assert_eq!(backfill[0].op, "workspace_root.backfill");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&backfill[0].payload_json).unwrap(),
            json!({"roots":[
                {"workspaceId":"F","root":"/unjournaled","updatedAt":60},
                {"workspaceId":"H","root":"/second","updatedAt":60},
            ]})
        );
        let row = &migrated["registry_journal"][6];
        assert_eq!(row[4], SqlValue::Null);
        assert_eq!(
            &row[8..],
            &[
                SqlValue::Text("local".into()),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Text("here".into()),
                SqlValue::Null
            ]
        );
        // A local root after-image must not touch the shared workspace's own
        // timestamp, even if the two timestamps no longer happen to be equal.
        store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE workspace SET updated_at=777 WHERE workspace_id='F'",
                    [],
                )?;
                super::replay_workspace_root_backfill(
                    tx,
                    json!({"roots":[{"workspaceId":"F","root":"/unjournaled","updatedAt":60}]}),
                )?;
                assert_eq!(
                    tx.query_row(
                        "SELECT updated_at FROM workspace WHERE workspace_id='F'",
                        [],
                        |r| r.get::<_, i64>(0)
                    )?,
                    777
                );
                tx.execute(
                    "UPDATE workspace SET updated_at=60 WHERE workspace_id='F'",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        let verified = store.verify().unwrap();
        assert!(verified.ok, "{verified:?}");
        assert_eq!(cells(&store), migrated, "verify wrote live cells");
        assert!(store.rebuild().unwrap().replay.ok);
        assert_eq!(cells(&store), migrated, "rebuild lost fallback cells");

        // Replay must use the recorded after-image, not the possibly corrupted
        // live path. The fallback is not an exemption from root verification.
        store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE workspace_root SET root='/corrupt' WHERE workspace_id='F'",
                    [],
                )
            })
            .unwrap();
        assert!(!store.verify().unwrap().ok);
        assert!(!store.rebuild().unwrap().replay.ok);
        assert_eq!(cells(&store), migrated);
        drop(store);
        let reopened = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(reopened.generation().unwrap(), 7);
        assert_eq!(cells(&reopened), migrated, "reopen duplicated the backfill");
        assert!(reopened.verify().unwrap().ok);
    }

    #[test]
    fn v6_without_fallback_roots_keeps_generation_and_writes_no_backfill() {
        for mode in ["empty", "unset", "set"] {
            let f = Fixture::new("log-schema-no-backfill");
            let descriptor = descriptor(&f);
            let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..6]).unwrap();
            old.db.with_conn_fenced(|tx| {
                if mode != "empty" {
                    tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at) VALUES('register',?1,'operator',10)", [json!({"projectId":"p","name":"P","workspaceId":"W"}).to_string()])?;
                    tx.execute_batch("INSERT INTO project(project_id,name,implicit,created_at,updated_at) VALUES('p','P',0,10,10);
                        INSERT INTO workspace(workspace_id,name,created_at,updated_at) VALUES('W','W',10,10);
                        INSERT INTO project_workspace(project_id,workspace_id) VALUES('p','W');
                        INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES('W','local','','p');")?;
                }
                if mode == "set" {
                    tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at) VALUES('set_workspace_root',?1,'operator',20)", [json!({"workspaceId":"W","root":"/ordinary"}).to_string()])?;
                    tx.execute("UPDATE workspace SET root='/ordinary',updated_at=20 WHERE workspace_id='W'", [])?;
                }
                Ok(())
            }).unwrap();
            let before = old.journal_tail(0, 100).unwrap();
            drop(old);
            let store = RegistryStore::open(&descriptor).unwrap();
            assert_eq!(
                store.generation().unwrap(),
                before.generation,
                "{mode} moved generation"
            );
            assert_eq!(
                serde_json::to_vec(&store.journal_tail(0, 100).unwrap()).unwrap(),
                serde_json::to_vec(&before).unwrap(),
                "{mode} wrote a journal row"
            );
            assert!(!store
                .journal_tail(0, 100)
                .unwrap()
                .entries
                .iter()
                .any(|row| row.op == "workspace_root.backfill"));
            assert!(store.verify().unwrap().ok, "{mode}");
            assert!(store.rebuild().unwrap().replay.ok, "{mode}");
            assert_eq!(
                store.generation().unwrap(),
                before.generation,
                "{mode} replay moved generation"
            );
        }
    }

    #[test]
    fn disabled_workspace_root_writes_preserve_journal_cache_noops_and_timestamps() {
        let f = Fixture::new("log-schema-disabled");
        let root = f.dir("root");
        f.store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "P".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        // A pinned old timestamp ensures a missing workspace timestamp update
        // cannot accidentally pass merely because two calls share a millisecond.
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("UPDATE workspace SET updated_at=1", []))
            .unwrap();
        let request = SetWorkspaceRootRequest {
            workspace_id: "W".into(),
            root: Some(root.clone()),
            request_key: Some("root-request".into()),
            actor: Some("operator".into()),
        };
        let reply = f
            .store
            .with_principal("direct")
            .set_workspace_root(request.clone())
            .unwrap();
        let set_cells = cells(&f.store);
        let journal = &set_cells["registry_journal"][1];
        assert_eq!(journal[1], SqlValue::Text("set_workspace_root".into()));
        assert_eq!(
            journal[2],
            SqlValue::Text(serde_json::to_value(&request).unwrap().to_string())
        );
        assert_eq!(journal[3], SqlValue::Text("operator".into()));
        assert_eq!(journal[4], SqlValue::Text("root-request".into()));
        assert_eq!(
            journal[6],
            SqlValue::Text(String::from_utf8(reply.clone()).unwrap())
        );
        assert_eq!(journal[7], SqlValue::Text("direct".into()));
        assert_eq!(
            &journal[8..],
            &[
                SqlValue::Text("local".into()),
                SqlValue::Null,
                SqlValue::Null,
                SqlValue::Text("here".into()),
                SqlValue::Null
            ]
        );
        assert_eq!(set_cells["workspace"][0][3], journal[5]);
        assert_eq!(set_cells["workspace_root"][0][2], journal[5]);
        assert_ne!(journal[5], SqlValue::Integer(1));
        assert_eq!(f.store.set_workspace_root(request).unwrap(), reply);
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(root),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            cells(&f.store),
            set_cells,
            "cache or same-value noop wrote cells"
        );
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: None,
                ..Default::default()
            })
            .unwrap();
        let cleared = cells(&f.store);
        assert_eq!(cleared["workspace_root"][0][1], SqlValue::Null);
        assert_eq!(
            cleared["workspace_root"][0][2],
            cleared["registry_journal"][2][5]
        );
        assert_eq!(
            cleared["workspace"][0][3],
            cleared["registry_journal"][2][5]
        );
        assert!(f.store.verify().unwrap().ok);
        assert!(f.store.rebuild().unwrap().replay.ok);
        assert_eq!(cells(&f.store), cleared);
        f.store
            .remove(RemoveRequest {
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        assert!(cells(&f.store)["workspace_root"].is_empty());
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "W".into(),
                ..Default::default()
            })
            .unwrap();
        assert!(cells(&f.store)["workspace_root"].is_empty());
        assert!(f.store.verify().unwrap().ok);
        assert!(f.store.rebuild().unwrap().replay.ok);
        assert!(cells(&f.store)["project_root_key"].is_empty());
        assert!(cells(&f.store)["pending_entry"].is_empty());
        assert_eq!(
            cells(&f.store)["identity_log_state"][0][1],
            SqlValue::Text("disabled".into())
        );
    }

    #[test]
    fn local_workspace_roots_feed_unchanged_resolve_and_enumerate_shapes() {
        let mut f = Fixture::new("log-schema-reads");
        let member = f.dir("member");
        let root = f.dir("workspace");
        f.store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "P".into(),
                roots: vec![member.clone()],
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(f.store.resolve(&member).unwrap().workspace_root, None);
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(root.clone()),
                ..Default::default()
            })
            .unwrap();
        let generation = f.store.generation().unwrap();
        let expected = format!(
            r#"{{"projectId":"p","workspaceId":"W","workspaceRoot":{},"projectName":"P","via":"root","gone":false,"generation":{generation},"canonicalRoot":{}}}"#,
            serde_json::to_string(&root).unwrap(),
            serde_json::to_string(&member).unwrap(),
        ).into_bytes();
        assert_eq!(
            serde_json::to_vec(&f.store.resolve(&member).unwrap()).unwrap(),
            expected
        );
        for root_records in [false, true] {
            f.store.set_root_records(root_records);
            assert_eq!(
                f.store.resolve(&member).unwrap().workspace_root.as_deref(),
                Some(root.as_str())
            );
            for filter in [None, Some("W")] {
                let listed = f.store.enumerate(filter).unwrap();
                assert_eq!(
                    serde_json::to_vec(&listed.workspaces).unwrap(),
                    format!(
                        r#"[{{"workspaceId":"W","name":"W","root":{}}}]"#,
                        serde_json::to_string(&root).unwrap()
                    )
                    .into_bytes()
                );
            }
        }
        f.store
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: None,
                ..Default::default()
            })
            .unwrap();
        assert_eq!(f.store.resolve(&member).unwrap().workspace_root, None);
        assert_eq!(f.store.enumerate(None).unwrap().workspaces[0].root, None);
    }

    #[test]
    fn binary_without_log_migration_refuses_migrated_store_as_ahead() {
        let f = Fixture::new("log-schema-ahead");
        let descriptor = descriptor(&f);
        drop(RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..6]).unwrap());
        let current = RegistryStore::open(&descriptor).unwrap();
        let before = cells(&current);
        drop(current);
        let error = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..6]).unwrap_err();
        assert!(matches!(error, RegistryError::Database(_)));
        assert_eq!(error.to_string(), "database: registry store is at schema version 7, ahead of this binary's highest migration 6; run a binary at or above the store's version");
        assert_eq!(cells(&RegistryStore::open(&descriptor).unwrap()), before);
    }
}

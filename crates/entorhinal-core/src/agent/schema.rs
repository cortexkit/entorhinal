/// Migration 4 stores agent identity and claim history, enforcing role shape,
/// terminal/avatar consistency and one live head per project without generation
/// triggers. Bindings have no foreign keys so terminal rows and historical claims
/// survive removal of their projects or workspaces.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/migrations/076_agent_registry.sql:1-98,
/// crates/prefrontal-core-store/migrations/082_wake_delivery.sql:12-105,
/// crates/prefrontal-core-store/migrations/112_agent_avatar.sql:16-19,
/// crates/prefrontal-core-store/migrations/126_agent_labels.sql:1-3.
pub const V4_AGENT_IDENTITY: &str = r#"
CREATE TABLE agent (
    agent_id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    name_version INTEGER NOT NULL DEFAULT 1 CHECK(name_version > 0),
    name_normalization_version INTEGER NOT NULL DEFAULT 1 CHECK(name_normalization_version = 1),
    tag TEXT NOT NULL,
    labels_json TEXT NOT NULL DEFAULT '[]',
    role TEXT NOT NULL CHECK(role IN ('assistant', 'workspace_head', 'head', 'hiree')),
    project_id TEXT NULL,
    workspace_id TEXT NULL,
    avatar_genome TEXT NULL,
    avatar_type TEXT NULL,
    avatar_version INTEGER NULL,
    github_identity_json TEXT NULL,
    terminal_reason TEXT NULL CHECK(terminal_reason IN ('retired', 'merged')),
    terminal_at_ms INTEGER NULL CHECK(terminal_at_ms >= 0),
    merged_into TEXT NULL REFERENCES agent(agent_id),
    supervisor_agent_id TEXT NULL REFERENCES agent(agent_id),
    request_key TEXT NULL,
    created_at_ms INTEGER NOT NULL CHECK(created_at_ms >= 0),
    updated_at_ms INTEGER NOT NULL CHECK(updated_at_ms >= 0),
    agent_generation INTEGER NOT NULL DEFAULT 1 CHECK(agent_generation >= 0),
    CHECK (
        (role = 'assistant' AND project_id IS NULL AND workspace_id IS NULL)
        OR (role = 'workspace_head' AND project_id IS NULL AND workspace_id IS NOT NULL)
        OR (role IN ('head', 'hiree') AND project_id IS NOT NULL)
    ),
    CHECK ((avatar_genome IS NULL) = (avatar_type IS NULL)),
    CHECK (avatar_genome IS NOT NULL OR avatar_version IS NULL),
    CHECK ((terminal_reason IS NULL) = (terminal_at_ms IS NULL)),
    CHECK (
        (terminal_reason IS NULL AND merged_into IS NULL)
        OR (terminal_reason IS NOT NULL AND terminal_reason = 'retired' AND merged_into IS NULL)
        OR (terminal_reason IS NOT NULL AND terminal_reason = 'merged' AND merged_into IS NOT NULL)
    )
);

CREATE UNIQUE INDEX uq_live_head
ON agent(project_id)
WHERE role = 'head' AND terminal_reason IS NULL;

CREATE INDEX idx_agent_project ON agent(project_id) WHERE project_id IS NOT NULL;
CREATE INDEX idx_agent_workspace ON agent(workspace_id) WHERE workspace_id IS NOT NULL;
CREATE INDEX idx_agent_role ON agent(role);

CREATE TABLE agent_name_claim (
    claim_id INTEGER PRIMARY KEY AUTOINCREMENT,
    agent_id TEXT NOT NULL REFERENCES agent(agent_id),
    namespace_kind TEXT NOT NULL CHECK(namespace_kind IN ('assistant', 'workspace')),
    namespace_key TEXT NOT NULL,
    name_normalization_version INTEGER NOT NULL DEFAULT 1 CHECK(name_normalization_version = 1),
    normalized_name TEXT NOT NULL,
    display_name TEXT NOT NULL,
    claimed_at_ms INTEGER NOT NULL CHECK(claimed_at_ms >= 0),
    released_at_ms INTEGER NULL CHECK(released_at_ms >= 0)
);

CREATE UNIQUE INDEX uq_active_claim
ON agent_name_claim(namespace_kind, namespace_key, name_normalization_version, normalized_name)
WHERE released_at_ms IS NULL;

CREATE UNIQUE INDEX uq_active_agent_claim
ON agent_name_claim(agent_id)
WHERE released_at_ms IS NULL;

CREATE INDEX idx_claim_agent ON agent_name_claim(agent_id);
"#;

#[cfg(test)]
mod tests {
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
    use rusqlite::{params, Connection, Error, Result};

    use super::*;
    use crate::{RegistryError, RegistryStore, MIGRATIONS};

    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct Scratch {
        root: PathBuf,
        descriptor: StorageDescriptor,
    }

    impl Scratch {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "entorhinal-agent-schema-{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = StorageDescriptor {
                module_id: "entorhinal-agent-schema-test".into(),
                storage_namespace: "test".into(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            Self { root, descriptor }
        }
    }

    impl Drop for Scratch {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }

    fn connection() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute_batch("PRAGMA foreign_keys = ON;").unwrap();
        conn.execute_batch(V4_AGENT_IDENTITY).unwrap();
        conn
    }

    fn insert_agent(
        conn: &Connection,
        id: &str,
        role: &str,
        project: Option<&str>,
        workspace: Option<&str>,
    ) -> Result<usize> {
        conn.execute(
            "INSERT INTO agent (agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms)
             VALUES (?1,'Alice','test',?2,?3,?4,0,0)",
            params![id, role, project, workspace],
        )
    }

    fn assert_check(result: Result<usize>) {
        assert!(matches!(result, Err(Error::SqliteFailure(error, _))
            if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_CHECK));
    }

    fn assert_unique(result: Result<usize>) {
        assert!(matches!(result, Err(Error::SqliteFailure(error, _))
            if error.extended_code == rusqlite::ffi::SQLITE_CONSTRAINT_UNIQUE));
    }

    #[test]
    fn production_migration_has_exact_identity_columns_and_no_runtime_tables() {
        let scratch = Scratch::new();
        let store = RegistryStore::open(&scratch.descriptor).unwrap();
        store
            .read(|conn| {
                let columns = |table| -> Result<Vec<String>> {
                    conn.prepare(&format!("PRAGMA table_info({table})"))?
                        .query_map([], |row| row.get(1))?
                        .collect()
                };
                assert_eq!(
                    columns("agent")?,
                    [
                        "agent_id",
                        "name",
                        "name_version",
                        "name_normalization_version",
                        "tag",
                        "labels_json",
                        "role",
                        "project_id",
                        "workspace_id",
                        "avatar_genome",
                        "avatar_type",
                        "avatar_version",
                        "github_identity_json",
                        "terminal_reason",
                        "terminal_at_ms",
                        "merged_into",
                        "supervisor_agent_id",
                        "request_key",
                        "created_at_ms",
                        "updated_at_ms",
                        "agent_generation",
                    ]
                );
                assert_eq!(
                    columns("agent_name_claim")?,
                    [
                        "claim_id",
                        "agent_id",
                        "namespace_kind",
                        "namespace_key",
                        "name_normalization_version",
                        "normalized_name",
                        "display_name",
                        "claimed_at_ms",
                        "released_at_ms",
                    ]
                );
                Ok(())
            })
            .unwrap();
        // Assert the migration's exact table set so any extra table fails the test.
        let conn = connection();
        let tables: Vec<String> = conn.prepare(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ).unwrap().query_map([], |row| row.get(0)).unwrap().collect::<Result<_>>().unwrap();
        assert_eq!(tables, ["agent", "agent_name_claim"]);
    }

    #[test]
    fn one_live_head_per_project_keeps_terminal_history() {
        let conn = connection();
        insert_agent(&conn, "head1", "head", Some("p"), Some("w")).unwrap();
        assert_unique(insert_agent(&conn, "head2", "head", Some("p"), None));
        insert_agent(&conn, "hire", "hiree", Some("p"), Some("w")).unwrap();
        conn.execute(
            "UPDATE agent SET terminal_reason='retired', terminal_at_ms=1 WHERE agent_id='head1'",
            [],
        )
        .unwrap();
        insert_agent(&conn, "head2", "head", Some("p"), None).unwrap();
    }

    #[test]
    fn stored_roles_allow_null_or_non_null_workspace_on_project_agents() {
        let conn = connection();
        for role in ["head", "hiree"] {
            insert_agent(
                &conn,
                &format!("{role}-null"),
                role,
                Some(&format!("p-{role}-1")),
                None,
            )
            .unwrap();
            insert_agent(
                &conn,
                &format!("{role}-placed"),
                role,
                Some(&format!("p-{role}-2")),
                Some("w"),
            )
            .unwrap();
            assert_check(insert_agent(
                &conn,
                &format!("{role}-invalid"),
                role,
                None,
                Some("w"),
            ));
        }
        insert_agent(&conn, "assistant", "assistant", None, None).unwrap();
        insert_agent(&conn, "workspace-head", "workspace_head", None, Some("w")).unwrap();
        assert_check(insert_agent(
            &conn,
            "bad-assistant",
            "assistant",
            Some("p"),
            None,
        ));
        assert_check(insert_agent(
            &conn,
            "bad-workspace-head",
            "workspace_head",
            None,
            None,
        ));
        assert_check(insert_agent(&conn, "unknown-role", "unknown", None, None));
    }

    #[test]
    fn avatar_pair_and_version_need_genome_are_checked() {
        let conn = connection();
        insert_agent(&conn, "a", "assistant", None, None).unwrap();
        assert_check(conn.execute("UPDATE agent SET avatar_genome='a'", []));
        assert_check(conn.execute("UPDATE agent SET avatar_type='creature.classic'", []));
        assert_check(conn.execute("UPDATE agent SET avatar_version=2", []));
        conn.execute(
            "UPDATE agent SET avatar_genome='a',avatar_type='creature.classic'",
            [],
        )
        .unwrap();
        conn.execute("UPDATE agent SET avatar_version=2", [])
            .unwrap();
        assert_check(conn.execute("UPDATE agent SET avatar_genome=NULL,avatar_type=NULL", []));
        conn.execute(
            "UPDATE agent SET avatar_genome=NULL,avatar_type=NULL,avatar_version=NULL",
            [],
        )
        .unwrap();
    }

    #[test]
    fn terminal_reason_timestamp_and_merge_target_are_checked() {
        let conn = connection();
        insert_agent(&conn, "a", "assistant", None, None).unwrap();
        insert_agent(&conn, "b", "assistant", None, None).unwrap();
        for sql in [
            "UPDATE agent SET terminal_reason='deleted', terminal_at_ms=1 WHERE agent_id='a'",
            "UPDATE agent SET terminal_reason='other', terminal_at_ms=1 WHERE agent_id='a'",
            "UPDATE agent SET terminal_reason='retired' WHERE agent_id='a'",
            "UPDATE agent SET terminal_at_ms=1 WHERE agent_id='a'",
            "UPDATE agent SET terminal_reason='merged', terminal_at_ms=1 WHERE agent_id='a'",
            "UPDATE agent SET merged_into='b' WHERE agent_id='a'",
            "UPDATE agent SET terminal_reason='retired', terminal_at_ms=1, merged_into='b' WHERE agent_id='a'",
        ] {
            assert_check(conn.execute(sql, []));
        }
        conn.execute("UPDATE agent SET terminal_reason='merged',terminal_at_ms=1,merged_into='b' WHERE agent_id='a'", []).unwrap();
        conn.execute(
            "UPDATE agent SET terminal_reason='retired',terminal_at_ms=1 WHERE agent_id='b'",
            [],
        )
        .unwrap();
    }

    #[test]
    fn claims_enforce_active_name_and_owner_uniqueness_but_keep_history() {
        let conn = connection();
        insert_agent(&conn, "a", "assistant", None, None).unwrap();
        insert_agent(&conn, "b", "assistant", None, None).unwrap();
        let claim = |owner, namespace, name| {
            conn.execute(
            "INSERT INTO agent_name_claim (agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms)
             VALUES (?1,'workspace',?2,?3,'Alice',0)", params![owner, namespace, name]
        )
        };
        claim("a", "w1", "alice").unwrap();
        assert_unique(claim("b", "w1", "alice"));
        assert_unique(claim("a", "w2", "other"));
        claim("b", "w2", "alice").unwrap();
        conn.execute(
            "UPDATE agent_name_claim SET released_at_ms=1 WHERE agent_id='a'",
            [],
        )
        .unwrap();
        claim("a", "w1", "alice").unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM agent_name_claim WHERE agent_id='a'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            2
        );
        assert_check(conn.execute(
            "UPDATE agent_name_claim SET name_normalization_version=2",
            [],
        ));
        assert_check(conn.execute("UPDATE agent_name_claim SET namespace_kind='project'", []));
        assert_check(conn.execute("UPDATE agent_name_claim SET claimed_at_ms=-1", []));
        assert_check(conn.execute("UPDATE agent_name_claim SET released_at_ms=-1", []));
    }

    #[test]
    fn agent_generation_is_explicit_without_a_mutation_trigger() {
        let conn = connection();
        insert_agent(&conn, "a", "assistant", None, None).unwrap();
        conn.execute("UPDATE agent SET agent_generation=1000000", [])
            .unwrap();
        conn.execute(
            "UPDATE agent SET name='Bob',tag='new',labels_json='[\"label\"]',updated_at_ms=2",
            [],
        )
        .unwrap();
        assert_eq!(
            conn.query_row("SELECT agent_generation FROM agent", [], |row| row
                .get::<_, i64>(0))
                .unwrap(),
            1000000
        );
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='trigger' AND tbl_name='agent'",
                [],
                |row| row.get::<_, i64>(0)
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn binary_without_agent_migration_refuses_migrated_store_as_ahead() {
        let scratch = Scratch::new();
        let old_chain = &MIGRATIONS[..3];
        // Check that the store opens with the first three migrations before migration 4.
        drop(RegistryStore::open_with_migrations(&scratch.descriptor, old_chain).unwrap());
        drop(RegistryStore::open(&scratch.descriptor).unwrap());
        let error =
            RegistryStore::open_with_migrations(&scratch.descriptor, old_chain).unwrap_err();
        assert!(matches!(error, RegistryError::Database(_)));
        let current_version = MIGRATIONS
            .iter()
            .map(|migration| migration.version)
            .max()
            .unwrap();
        assert_eq!(error.to_string(), format!("database: registry store is at schema version {current_version}, ahead of this binary's highest migration 3; run a binary at or above the store's version"));
        // The refused open by the three-migration binary must not have touched the
        // store: a binary with every migration still reads the `agent` table as written.
        let current = RegistryStore::open(&scratch.descriptor).unwrap();
        current
            .read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM agent", [], |row| row.get::<_, i64>(0))
            })
            .unwrap();
    }
}

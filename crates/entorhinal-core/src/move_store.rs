//! Machine-local move progress, independent of the identity journal.

/// Migration 9 keeps move progress outside the journal-replayed tables. A
/// received session reference can bind to an agent before a local host opens it,
/// so its session_binding row permits NULL harness and session_id.
pub const V9_AGENT_MOVES: &str = r#"
CREATE TABLE agent_move (
    cut_id TEXT PRIMARY KEY NOT NULL,
    agent_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('source','receiver')),
    peer TEXT NOT NULL,
    step TEXT NOT NULL CHECK(step IN (
        'preparing','exported','sealed','published','releasing','released','aborted',
        'picked_up','imported','staged','claimed','activating','activated','done','taken_over'
    )),
    plan TEXT NOT NULL,
    request_body TEXT NULL,
    record_version INTEGER NOT NULL,
    claim_version INTEGER NULL,
    claim_take_body TEXT NULL,
    claim_take_version INTEGER NULL,
    claim_landed_version INTEGER NULL,
    seal_sent INTEGER NOT NULL CHECK(seal_sent IN (0,1)),
    abort_requested TEXT NULL,
    hold TEXT NULL,
    cut_accept TEXT NULL,
    reason TEXT NULL,
    detail TEXT NULL,
    request_key TEXT UNIQUE NULL,
    attempts INTEGER NOT NULL,
    activated_at_ms INTEGER NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX agent_move_open_agent ON agent_move(agent_id)
    WHERE step NOT IN ('released','aborted','done','taken_over');

CREATE TABLE move_setting (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    lid_close_moves INTEGER NOT NULL DEFAULT 0 CHECK(lid_close_moves IN (0,1))
);
INSERT INTO move_setting(id, lid_close_moves) VALUES (1, 0);

CREATE TABLE session_binding_v9 (
    ref_kind TEXT NOT NULL,
    ref TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    harness TEXT NULL,
    session_id TEXT NULL,
    registered_by TEXT NOT NULL CHECK(registered_by IN ('orchestrator','host','move')),
    registered_at_ms INTEGER NOT NULL,
    PRIMARY KEY (ref_kind, ref)
);
INSERT INTO session_binding_v9
    (ref_kind, ref, agent_id, harness, session_id, registered_by, registered_at_ms)
    SELECT ref_kind, ref, agent_id, harness, session_id, registered_by, registered_at_ms
    FROM session_binding;
DROP TABLE session_binding;
ALTER TABLE session_binding_v9 RENAME TO session_binding;
CREATE UNIQUE INDEX session_binding_session ON session_binding(harness, session_id);
"#;

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{RegistryError, RegistryStore};

macro_rules! text_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $text)] $variant),+
        }

        impl rusqlite::types::ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                let text = match self { $(Self::$variant => $text),+ };
                Ok(text.into())
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
                match value.as_str()? {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
        }
    };
}

text_enum!(MoveSide { Source => "source", Receiver => "receiver" });
text_enum!(MoveStep {
    Preparing => "preparing",
    Exported => "exported",
    Sealed => "sealed",
    Published => "published",
    Releasing => "releasing",
    Released => "released",
    Aborted => "aborted",
    PickedUp => "picked_up",
    Imported => "imported",
    Staged => "staged",
    Claimed => "claimed",
    Activating => "activating",
    Activated => "activated",
    Done => "done",
    TakenOver => "taken_over",
});

impl MoveStep {
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Released | Self::Aborted | Self::Done | Self::TakenOver
        )
    }
}

/// A durable move checkpoint. Storage preserves the JSON values without
/// interpreting them: the participant/ref plan and the claim body prepared for
/// a compare-and-swap must be available unchanged when the driver restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMove {
    pub cut_id: String,
    pub agent_id: String,
    pub side: MoveSide,
    pub peer: String,
    pub step: MoveStep,
    pub plan: Value,
    pub request_body: Option<Value>,
    pub record_version: i64,
    pub claim_version: Option<i64>,
    pub claim_take_body: Option<Value>,
    pub claim_take_version: Option<i64>,
    pub claim_landed_version: Option<i64>,
    pub seal_sent: bool,
    pub abort_requested: Option<Value>,
    pub hold: Option<Value>,
    pub cut_accept: Option<Value>,
    pub reason: Option<String>,
    pub detail: Option<Value>,
    pub request_key: Option<String>,
    pub attempts: i64,
    pub activated_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveSetting {
    pub lid_close_moves: bool,
}

const MOVE_COLUMNS: &str = "cut_id, agent_id, side, peer, step, plan, request_body,
    record_version, claim_version, claim_take_body, claim_take_version, claim_landed_version,
    seal_sent, abort_requested, hold, cut_accept, reason, detail, request_key, attempts,
    activated_at_ms, created_at_ms, updated_at_ms";

fn json_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<Value>> {
    let text: Option<String> = row.get(index)?;
    text.map(|text| {
        serde_json::from_str(&text).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })
    .transpose()
}

impl AgentMove {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            cut_id: row.get(0)?,
            agent_id: row.get(1)?,
            side: row.get(2)?,
            peer: row.get(3)?,
            step: row.get(4)?,
            plan: json_column(row, 5)?.ok_or(rusqlite::Error::InvalidColumnType(
                5,
                "plan".into(),
                rusqlite::types::Type::Null,
            ))?,
            request_body: json_column(row, 6)?,
            record_version: row.get(7)?,
            claim_version: row.get(8)?,
            claim_take_body: json_column(row, 9)?,
            claim_take_version: row.get(10)?,
            claim_landed_version: row.get(11)?,
            seal_sent: row.get(12)?,
            abort_requested: json_column(row, 13)?,
            hold: json_column(row, 14)?,
            cut_accept: json_column(row, 15)?,
            reason: row.get(16)?,
            detail: json_column(row, 17)?,
            request_key: row.get(18)?,
            attempts: row.get(19)?,
            activated_at_ms: row.get(20)?,
            created_at_ms: row.get(21)?,
            updated_at_ms: row.get(22)?,
        })
    }

    fn write(&self, conn: &rusqlite::Connection, insert: bool) -> rusqlite::Result<usize> {
        let sql = if insert {
            format!(
                "INSERT INTO agent_move ({MOVE_COLUMNS}) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)"
            )
        } else {
            "UPDATE agent_move SET agent_id=?2, side=?3, peer=?4, step=?5, plan=?6,
                request_body=?7, record_version=?8, claim_version=?9, claim_take_body=?10,
                claim_take_version=?11, claim_landed_version=?12, seal_sent=?13,
                abort_requested=?14, hold=?15, cut_accept=?16, reason=?17, detail=?18,
                request_key=?19, attempts=?20, activated_at_ms=?21, created_at_ms=?22,
                updated_at_ms=?23 WHERE cut_id=?1"
                .into()
        };
        conn.execute(
            &sql,
            params![
                self.cut_id,
                self.agent_id,
                self.side,
                self.peer,
                self.step,
                self.plan.to_string(),
                self.request_body.as_ref().map(Value::to_string),
                self.record_version,
                self.claim_version,
                self.claim_take_body.as_ref().map(Value::to_string),
                self.claim_take_version,
                self.claim_landed_version,
                self.seal_sent,
                self.abort_requested.as_ref().map(Value::to_string),
                self.hold.as_ref().map(Value::to_string),
                self.cut_accept.as_ref().map(Value::to_string),
                self.reason,
                self.detail.as_ref().map(Value::to_string),
                self.request_key,
                self.attempts,
                self.activated_at_ms,
                self.created_at_ms,
                self.updated_at_ms,
            ],
        )
    }
}

impl RegistryStore {
    /// Insert a checkpoint, refusing duplicate cuts or request keys and a second
    /// non-terminal move for the same agent, on either side.
    pub fn insert_agent_move(&self, row: &AgentMove) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                row.write(conn, true)?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }

    /// Replace an existing checkpoint; a missing cut returns false, never inserts.
    pub fn update_agent_move(&self, row: &AgentMove) -> Result<bool, RegistryError> {
        self.db
            .with_conn_fenced(|conn| Ok(row.write(conn, false)? != 0))
            .map_err(RegistryError::Store)
    }

    pub fn agent_move(&self, cut_id: &str) -> Result<Option<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE cut_id=?1"),
                    [cut_id],
                    AgentMove::from_row,
                )
                .optional()
            })
            .map_err(RegistryError::Store)
    }

    /// Look up agent_move.request_key independently of registry_journal.request_key;
    /// the same key may identify one move and one unrelated journal operation.
    pub fn agent_move_by_request_key(
        &self,
        request_key: &str,
    ) -> Result<Option<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE request_key=?1"),
                    [request_key],
                    AgentMove::from_row,
                )
                .optional()
            })
            .map_err(RegistryError::Store)
    }

    pub fn list_agent_moves(&self) -> Result<Vec<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move ORDER BY created_at_ms, cut_id"
                ))?;
                let rows = stmt
                    .query_map([], AgentMove::from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .map_err(RegistryError::Store)
    }

    pub fn list_open_agent_moves(&self) -> Result<Vec<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move
                 WHERE step NOT IN ('released','aborted','done','taken_over')
                 ORDER BY created_at_ms, cut_id"
                ))?;
                let rows = stmt
                    .query_map([], AgentMove::from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .map_err(RegistryError::Store)
    }

    pub fn move_setting(&self) -> Result<MoveSetting, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    "SELECT lid_close_moves FROM move_setting WHERE id=1",
                    [],
                    |row| {
                        Ok(MoveSetting {
                            lid_close_moves: row.get(0)?,
                        })
                    },
                )
            })
            .map_err(RegistryError::Store)
    }

    pub fn set_move_setting(&self, setting: MoveSetting) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.execute(
                    "UPDATE move_setting SET lid_close_moves=?1 WHERE id=1",
                    [setting.lid_close_moves],
                )?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        store: RegistryStore,
        descriptor: StorageDescriptor,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_migrations(&crate::MIGRATIONS)
        }

        fn with_migrations(migrations: &[cortexkit_store::Migration]) -> Self {
            let root = std::env::temp_dir().join(format!(
                "agent-move-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = StorageDescriptor {
                module_id: "entorhinal-test".into(),
                storage_namespace: "moves".into(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let store = RegistryStore::open_with_migrations(&descriptor, migrations).unwrap();
            Self {
                store,
                descriptor,
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
            }
        }
    }

    fn checkpoint(cut_id: &str, side: MoveSide) -> AgentMove {
        AgentMove {
            cut_id: cut_id.into(),
            agent_id: "agent-1".into(),
            side,
            peer: "peer-author".into(),
            step: match side {
                MoveSide::Source => MoveStep::Preparing,
                MoveSide::Receiver => MoveStep::PickedUp,
            },
            plan: json!({"participants": [{"module_id": "a", "refs": [{"ref_kind": "session", "ref": "r"}]}]}),
            request_body: Some(json!({"agent_id": "agent-1", "to": "Display name as submitted"})),
            record_version: 3,
            claim_version: Some(4),
            claim_take_body: Some(
                json!({"holder": "self", "epoch": 2, "cut_id": cut_id, "reason": null}),
            ),
            claim_take_version: Some(4),
            claim_landed_version: Some(5),
            seal_sent: true,
            abort_requested: Some(json!({"reason": "operator", "detail": {"note": "full detail"}})),
            hold: Some(json!({"code": "external_not_present", "ref": "r", "note": "外部"})),
            cut_accept: Some(json!({"state": "refused", "code": "unavailable"})),
            reason: Some("operator".into()),
            detail: Some(json!({"nested": [null, true, 17, "verbatim"]})),
            request_key: Some(format!("key-{cut_id}")),
            attempts: 7,
            activated_at_ms: Some(99),
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[test]
    fn migration_9_preserves_part1_bindings_and_session_index() {
        let Fixture {
            store,
            descriptor,
            _cleanup,
        } = Fixture::with_migrations(&crate::MIGRATIONS[..8]);
        store
            .db
            .with_conn_fenced(|conn| {
                conn.execute_batch(
                    "INSERT INTO session_binding VALUES
                ('session','r1','a1','host-a','s1','host',11),
                ('session','r2','a2','host-b','s2','orchestrator',22);",
                )?;
                Ok(())
            })
            .unwrap();
        drop(store);
        let store = RegistryStore::open(&descriptor).unwrap();
        store.db.with_conn_fenced(|conn| {
            let rows = conn.prepare("SELECT ref_kind,ref,agent_id,harness,session_id,registered_by,registered_at_ms FROM session_binding ORDER BY ref")?
                .query_map([], |row| Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?, row.get::<_, String>(4)?, row.get::<_, String>(5)?, row.get::<_, i64>(6)?,
                )))?.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(rows, vec![
                ("session".into(), "r1".into(), "a1".into(), "host-a".into(), "s1".into(), "host".into(), 11),
                ("session".into(), "r2".into(), "a2".into(), "host-b".into(), "s2".into(), "orchestrator".into(), 22),
            ]);
            let index_sql: String = conn.query_row("SELECT sql FROM sqlite_master WHERE name='session_binding_session'", [], |row| row.get(0))?;
            assert!(index_sql.contains("UNIQUE INDEX"));
            assert!(index_sql.contains("session_binding(harness, session_id)"));
            assert!(conn.execute("INSERT INTO session_binding VALUES ('session','collision','a3','host-a','s1','host',33)", []).is_err());
            conn.execute_batch("INSERT INTO session_binding VALUES
                ('session','m1','a1',NULL,NULL,'move',44),
                ('session','m2','a1',NULL,NULL,'move',55);")?;
            let moved: i64 = conn.query_row("SELECT COUNT(*) FROM session_binding WHERE registered_by='move' AND harness IS NULL AND session_id IS NULL", [], |row| row.get(0))?;
            assert_eq!(moved, 2);
            assert!(conn.execute("INSERT INTO session_binding VALUES ('session','bad','a1',NULL,NULL,'other',66)", []).is_err());
            Ok(())
        }).unwrap();
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: false
            }
        );
    }

    #[test]
    fn move_checkpoint_insert_update_and_restart_round_trip() {
        let Fixture {
            store,
            descriptor,
            _cleanup,
        } = Fixture::new();
        let mut row = checkpoint("cut-1", MoveSide::Receiver);
        store.insert_agent_move(&row).unwrap();
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert_eq!(
            store
                .agent_move_by_request_key(row.request_key.as_ref().unwrap())
                .unwrap(),
            Some(row.clone())
        );
        assert_eq!(store.list_agent_moves().unwrap(), vec![row.clone()]);
        assert_eq!(store.list_open_agent_moves().unwrap(), vec![row.clone()]);
        row.step = MoveStep::Activated;
        row.plan = json!({"pinned": [1, 2, 3]});
        row.request_body = Some(Value::Null);
        row.record_version = 10;
        row.claim_version = None;
        row.claim_take_body = Some(json!({"epoch": 100, "reason": null}));
        row.claim_take_version = Some(80);
        row.claim_landed_version = Some(81);
        row.abort_requested = None;
        row.hold = Some(json!({"code": "busy", "module_id": "b"}));
        row.cut_accept = Some(json!({"state": "accepted"}));
        row.seal_sent = false;
        row.reason = None;
        row.detail = None;
        row.attempts = 0;
        row.activated_at_ms = Some(101);
        row.updated_at_ms = 102;
        assert!(store.update_agent_move(&row).unwrap());
        store
            .set_move_setting(MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        drop(store);
        let store = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: true
            }
        );
        row.step = MoveStep::Done;
        row.request_body = None;
        row.claim_take_body = None;
        row.claim_take_version = None;
        row.claim_landed_version = None;
        row.hold = None;
        row.cut_accept = None;
        row.request_key = None;
        row.activated_at_ms = None;
        assert!(store.update_agent_move(&row).unwrap());
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert!(store.list_open_agent_moves().unwrap().is_empty());
        row.cut_id = "missing".into();
        assert!(!store.update_agent_move(&row).unwrap());
        assert_eq!(store.agent_move("missing").unwrap(), None);
        assert_eq!(store.agent_move_by_request_key("missing").unwrap(), None);
    }

    #[test]
    fn one_open_move_per_agent_on_either_side() {
        for first_side in [MoveSide::Source, MoveSide::Receiver] {
            for second_side in [MoveSide::Source, MoveSide::Receiver] {
                let fixture = Fixture::new();
                let store = &fixture.store;
                let mut first = checkpoint("first", first_side);
                let mut second = checkpoint("second", second_side);
                store.insert_agent_move(&first).unwrap();
                for step in [
                    MoveStep::Preparing,
                    MoveStep::Exported,
                    MoveStep::Sealed,
                    MoveStep::Published,
                    MoveStep::Releasing,
                    MoveStep::PickedUp,
                    MoveStep::Imported,
                    MoveStep::Staged,
                    MoveStep::Claimed,
                    MoveStep::Activating,
                    MoveStep::Activated,
                ] {
                    assert!(!step.is_terminal());
                    first.step = step;
                    store.update_agent_move(&first).unwrap();
                    let error = store.insert_agent_move(&second).unwrap_err();
                    assert!(
                        error.to_string().contains("agent_move.agent_id"),
                        "{first_side:?}/{second_side:?}/{step:?}: {error}"
                    );
                }
                assert_eq!(store.list_agent_moves().unwrap(), vec![first.clone()]);
                for terminal in [
                    MoveStep::Released,
                    MoveStep::Aborted,
                    MoveStep::Done,
                    MoveStep::TakenOver,
                ] {
                    assert!(terminal.is_terminal());
                    first.step = terminal;
                    store.update_agent_move(&first).unwrap();
                    second.cut_id = format!("second-{terminal:?}");
                    second.request_key = Some(format!("key-{terminal:?}"));
                    store.insert_agent_move(&second).unwrap();
                    first.step = match first_side {
                        MoveSide::Source => MoveStep::Preparing,
                        MoveSide::Receiver => MoveStep::PickedUp,
                    };
                    assert!(store.update_agent_move(&first).is_err());
                    second.step = MoveStep::Aborted;
                    store.update_agent_move(&second).unwrap();
                    first.step = terminal;
                    second.step = match second_side {
                        MoveSide::Source => MoveStep::Preparing,
                        MoveSide::Receiver => MoveStep::PickedUp,
                    };
                }
                second.agent_id = "other-agent".into();
                second.cut_id = "other-cut".into();
                second.request_key = None;
                store.insert_agent_move(&second).unwrap();
            }
        }
    }

    #[test]
    fn move_request_keys_are_unique_and_separate_from_journal_keys() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let mut first = checkpoint("first", MoveSide::Source);
        first.request_key = Some("same-key".into());
        store
            .apply_entry("test", "{}", "test", Some("same-key"), |_| Ok(()))
            .unwrap();
        let generation = store.generation().unwrap();
        store.insert_agent_move(&first).unwrap();
        let mut second = checkpoint("second", MoveSide::Receiver);
        second.agent_id = "other-agent".into();
        second.request_key = first.request_key.clone();
        let error = store.insert_agent_move(&second).unwrap_err();
        assert!(
            error.to_string().contains("agent_move.request_key"),
            "{error}"
        );
        assert_eq!(
            store.agent_move_by_request_key("same-key").unwrap(),
            Some(first.clone())
        );
        second.request_key = None;
        store.insert_agent_move(&second).unwrap();
        second.request_key = first.request_key.clone();
        assert!(store.update_agent_move(&second).is_err());
        assert_eq!(store.generation().unwrap(), generation);
    }

    #[test]
    fn move_apis_do_not_append_journal_rows() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let generation = store.generation().unwrap();
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: false
            }
        );
        store
            .set_move_setting(MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        let mut row = checkpoint("first", MoveSide::Source);
        store.insert_agent_move(&row).unwrap();
        store.agent_move("first").unwrap();
        store
            .agent_move_by_request_key(row.request_key.as_ref().unwrap())
            .unwrap();
        store.list_agent_moves().unwrap();
        store.list_open_agent_moves().unwrap();
        row.step = MoveStep::Published;
        store.update_agent_move(&row).unwrap();
        assert_eq!(store.generation().unwrap(), generation);
    }

    #[test]
    fn verify_and_rebuild_leave_move_tables_intact() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let row = checkpoint("first", MoveSide::Receiver);
        let setting = MoveSetting {
            lid_close_moves: true,
        };
        store.insert_agent_move(&row).unwrap();
        store.set_move_setting(setting).unwrap();
        let verify = store.verify().unwrap();
        assert!(verify.ok, "{verify:?}");
        assert_eq!(store.list_agent_moves().unwrap(), vec![row.clone()]);
        assert_eq!(store.move_setting().unwrap(), setting);
        let rebuilt = store.rebuild().unwrap();
        assert!(rebuilt.replay.ok, "{rebuilt:?}");
        assert_eq!(store.list_agent_moves().unwrap(), vec![row]);
        assert_eq!(store.move_setting().unwrap(), setting);
        assert!(store.verify().unwrap().ok);
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

use rusqlite::params;

use super::*;
use crate::{agent, SessionRegistration, MIGRATIONS};

const AGENT: &str = "agent_0123456789abcdef";
const OTHER: &str = "agent_fedcba9876543210";
const RETIRED: &str = "agent_00000000000000aa";
static NEXT: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    store: RegistryStore,
    descriptor: cortexkit_store::StorageDescriptor,
    _cleanup: crate::scratch_cleanup::ScratchCleanup,
}

impl Fixture {
    fn new() -> Self {
        Self::with_migrations(&MIGRATIONS)
    }

    fn with_migrations(migrations: &[cortexkit_store::Migration]) -> Self {
        let root = std::env::temp_dir().join(format!(
            "session-lifecycle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        let descriptor = cortexkit_store::StorageDescriptor {
            module_id: "entorhinal-test".into(),
            storage_namespace: "sessions".into(),
            isolation: cortexkit_store::Isolation::Module,
            backend: cortexkit_store::StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        let store = RegistryStore::open_with_migrations(&descriptor, migrations).unwrap();
        store
            .db
            .with_conn_fenced(|tx| {
                for id in [AGENT, OTHER] {
                    tx.execute(
                        "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,?1,'test','assistant',1,1)",
                        [id],
                    )?;
                }
                tx.execute(
                    "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms,terminal_reason,terminal_at_ms) VALUES(?1,'Retired','test','assistant',1,1,'retired',1)",
                    [RETIRED],
                )?;
                tx.execute(
                    "INSERT INTO registry_journal(op,payload_json) VALUES(?1,'{}')",
                    [agent::IDENTITY_MARKER_OP],
                )?;
                Ok(())
            })
            .unwrap();
        Self {
            store,
            descriptor,
            _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
        }
    }

    /// Insert a binding directly, as rows from before migration 15 or from a
    /// move would exist.
    fn bind(
        &self,
        agent_id: &str,
        session_ref: &str,
        registered_at_ms: i64,
        runner: Option<&str>,
        ended_at_ms: Option<i64>,
    ) {
        self.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,runner,ended_at_ms)
                     VALUES('broca-session',?1,?2,'broca',?1,'/project','orchestrator',?3,?4,?5)",
                    params![session_ref, agent_id, registered_at_ms, runner, ended_at_ms],
                )?;
                Ok(())
            })
            .unwrap();
    }

    /// `(runner, ended_at_ms)` of the binding with this session id, or `None`
    /// once it is deleted.
    fn lifecycle(&self, session_id: &str) -> Option<(Option<String>, Option<i64>)> {
        self.store
            .read(|conn| {
                conn.query_row(
                    "SELECT runner, ended_at_ms FROM session_binding WHERE session_id=?1",
                    [session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()
            })
            .unwrap()
    }

    fn count(&self) -> i64 {
        self.store
            .read(|conn| conn.query_row("SELECT COUNT(*) FROM session_binding", [], |r| r.get(0)))
            .unwrap()
    }

    fn legacy_binding(&self, request: &SessionRegistration<'_>) {
        self.store.db.with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
                params![request.ref_kind, request.session_ref, request.agent_id,
                    request.harness, request.session_id, request.project_root,
                    request.registered_by, request.now_ms],
            )?;
            Ok(())
        }).unwrap();
    }

    fn open_move(&self, agent_id: &str, cut_id: &str, step: &str) {
        self.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO agent_move(cut_id,agent_id,side,peer,step,plan,record_version,seal_sent,attempts,created_at_ms,updated_at_ms)
                     VALUES(?1,?2,'source','peer',?3,'{}',1,0,1,1,1)",
                    params![cut_id, agent_id, step],
                )?;
                Ok(())
            })
            .unwrap();
    }
}

fn registration<'a>(
    session_id: &'a str,
    runner: Option<&'a str>,
    now_ms: i64,
) -> SessionRegistration<'a> {
    SessionRegistration {
        role: None,
        agent_id: AGENT,
        ref_kind: "broca-session",
        session_ref: "ref-1",
        harness: "broca",
        session_id,
        project_root: Some("/project"),
        registered_by: "orchestrator",
        runner,
        network: None,
        now_ms,
    }
}

#[test]
fn move_session_regression_delayed_delete_keeps_new_registration_open() {
    let f = Fixture::new();
    let register = |now| {
        f.store
            .register_session(&registration("s", Some("broca"), now))
            .unwrap();
    };
    register(700);
    let queued_delete = GoneSession {
        harness: "broca".into(),
        session_id: "s".into(),
        runner: Some("broca".into()),
        ended_at_ms: 800,
    };
    register(900);
    f.store.stamp_gone_sessions(&[queued_delete]).unwrap();
    assert_eq!(
        f.lifecycle("s").unwrap().1,
        None,
        "old DEL cannot end the new active registration"
    );
}

#[test]
fn move_session_regression_registration_uses_cutover_marker_index() {
    let f = Fixture::with_migrations(&MIGRATIONS[..MIGRATIONS.len() - 1]);
    drop(f.store);
    let store = RegistryStore::open(&f.descriptor).unwrap();
    let plan = store
        .read(|conn| {
            let mut stmt = conn.prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                RegistryStore::AGENT_CUTOVER_SQL
            ))?;
            let rows = stmt.query_map([], |row| row.get::<_, String>(3))?;
            Ok(rows.collect::<rusqlite::Result<Vec<_>>>()?.join("\n"))
        })
        .unwrap();
    assert!(
        plan.contains("registry_journal_cutover"),
        "session registration looks for a single journal marker: {plan}"
    );
}

#[test]
fn session_role_absent_and_migrated_rows_store_head() {
    let f = Fixture::with_migrations(&MIGRATIONS[..23]);
    f.bind(AGENT, "legacy", 1, Some("broca"), None);
    drop(f.store);
    let store = RegistryStore::open_with_migrations(&f.descriptor, &MIGRATIONS).unwrap();
    let legacy = store
        .session_binding_for("broca", "legacy")
        .unwrap()
        .unwrap();
    assert_eq!(legacy.role, "head");
    assert!(serde_json::to_value(legacy).unwrap().get("role").is_none());
    let binding = store
        .register_session(&registration("new", Some("broca"), 2))
        .unwrap();
    assert_eq!(binding.role, "head");
    let stored: String = store
        .read(|conn| {
            conn.query_row(
                "SELECT role FROM session_binding WHERE session_id='new'",
                [],
                |row| row.get(0),
            )
        })
        .unwrap();
    assert_eq!(stored, "head");
}

#[test]
fn session_role_unknown_refuses_without_writes() {
    let f = Fixture::new();
    let mut request = registration("s1", Some("broca"), 10);
    request.role = Some("worker");
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 10,
    });
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "invalid_request"
    );
    assert_eq!(f.count(), 0);
    let history: i64 = f
        .store
        .read(|conn| conn.query_row("SELECT COUNT(*) FROM session_network", [], |row| row.get(0)))
        .unwrap();
    assert_eq!(history, 0);
    request.role = None;
    f.store.register_session(&request).unwrap();
    request.role = Some("worker");
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "invalid_request"
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .role,
        "head"
    );
}

#[test]
fn session_role_reregistration_and_rekeys_update_in_place() {
    let f = Fixture::new();
    let mut request = registration("s1", Some("broca"), 10);
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 10,
    });
    f.store.register_session(&request).unwrap();
    request.role = Some("delegated");
    let binding = f.store.register_session(&request).unwrap();
    assert_eq!(binding.role, "delegated");
    request.session_ref = "rekeyed";
    request.role = Some("head");
    assert_eq!(f.store.register_session(&request).unwrap().role, "head");
    request.ref_kind = "new-host-kind";
    request.session_ref = "new-host-ref";
    request.harness = "new-host";
    request.role = Some("delegated");
    assert_eq!(
        f.store.register_session(&request).unwrap().role,
        "delegated"
    );
    assert_eq!(f.count(), 1);
    let read = f
        .store
        .session_binding_for_runner("broca", "s1")
        .unwrap()
        .unwrap();
    assert_eq!(read.role, "delegated");
    assert_eq!(read.network_history.len(), 1);
    assert_eq!(
        f.store.agent_sessions(AGENT, None, 100).unwrap().sessions[0].role,
        "delegated"
    );
}

/// A store just before migration 15 keeps its bindings across it, with no
/// runner and no end time, and gains the per-agent index the session list
/// reads through.
#[test]
fn migration_15_backfills_null_runner_and_end_and_indexes_agent() {
    // Found by its statements rather than by being last, so later
    // migrations can follow it in the chain.
    let lifecycle = MIGRATIONS
        .iter()
        .position(|migration| migration.statements == V15_SESSION_LIFECYCLE)
        .expect("the session lifecycle migration is in the chain");
    let Fixture {
        store,
        descriptor,
        _cleanup,
    } = Fixture::with_migrations(&MIGRATIONS[..lifecycle]);
    store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
                 VALUES('broca-session','old',?1,'broca','old-session','','orchestrator',5)",
                [AGENT],
            )?;
            Ok(())
        })
        .unwrap();
    drop(store);
    let store = RegistryStore::open(&descriptor).unwrap();
    let (runner, ended): (Option<String>, Option<i64>) = store
        .read(|conn| {
            conn.query_row(
                "SELECT runner, ended_at_ms FROM session_binding WHERE ref='old'",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
        })
        .unwrap();
    assert_eq!((runner, ended), (None, None));
    let indexed: Vec<String> = store
        .read(|conn| {
            conn.prepare("PRAGMA index_info(session_binding_agent)")?
                .query_map([], |row| row.get::<_, String>(2))?
                .collect()
        })
        .unwrap();
    assert_eq!(indexed[0], "agent_id");
    let page = store.agent_sessions(AGENT, None, 10).unwrap();
    assert_eq!(page.sessions.len(), 1);
    assert_eq!(page.sessions[0].runner, None);
    assert_eq!(page.sessions[0].project_root, None);
}

#[test]
fn registration_requires_runner_before_writing() {
    let f = Fixture::new();
    for runner in [None, Some("")] {
        let result = f.store.register_session(&registration("s1", runner, 10));
        assert!(result.is_err(), "registration accepted runner {runner:?}");
        let refusal = result.unwrap_err();
        assert_eq!(refusal.code, "invalid_request");
        assert!(refusal.message.contains("runner"));
        assert_eq!(f.count(), 0, "refusal must not create a binding");
    }
}

/// Registering records the required runner, replaces it when another is
/// named, and always clears the end time because the session is live again.
#[test]
fn registration_records_runner_and_clears_end() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), None)));
    f.store.end_session("broca", "s1", None, 20).unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), Some(20))));
    f.store
        .register_session(&registration("s1", Some("broca"), 30))
        .unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), None)));
    f.store
        .register_session(&registration("s2", Some("thalamus-harness"), 40))
        .unwrap();
    assert_eq!(
        f.lifecycle("s2"),
        Some((Some("thalamus-harness".into()), None))
    );
}

#[test]
fn registration_rekey_adds_runner_to_legacy_row_and_reopens_in_place() {
    let f = Fixture::new();
    f.bind(AGENT, "s1", 10, None, Some(20));
    let rowid = || {
        f.store
            .read(|conn| {
                conn.query_row(
                    "SELECT rowid FROM session_binding WHERE harness='broca' AND session_id='s1'",
                    [],
                    |row| row.get::<_, i64>(0),
                )
            })
            .unwrap()
    };
    let original_rowid = rowid();
    let mut request = registration("s1", Some("broca"), 30);
    request.session_ref = "ref-new";
    request.registered_by = "host";
    request.project_root = None;
    let binding = f.store.register_session(&request).unwrap();
    assert_eq!(binding.session_ref, "ref-new");
    assert_eq!(binding.agent_id, AGENT);
    assert_eq!(binding.ref_kind, "broca-session");
    assert_eq!(binding.harness.as_deref(), Some("broca"));
    assert_eq!(binding.project_root.as_deref(), Some("/project"));
    assert_eq!(binding.registered_by, "orchestrator");
    assert_eq!(binding.registered_at_ms, 30);
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), None)));
    assert_eq!(rowid(), original_rowid);
    assert_eq!(f.count(), 1);
    assert_eq!(
        f.store
            .session_bindings_for_refs(&[("broca-session", "s1")])
            .unwrap(),
        vec![None]
    );
}

#[test]
fn registration_rekey_different_agent_is_refused() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    let mut request = registration("s1", Some("broca"), 30);
    request.session_ref = "ref-new";
    request.agent_id = OTHER;
    let refusal = f.store.register_session(&request).unwrap_err();
    assert_eq!(refusal.code, "session_bound_elsewhere");
    assert_eq!(
        refusal.detail,
        Some(serde_json::json!({
            "ref_kind":"broca-session", "ref":"ref-1", "agent_id":AGENT
        }))
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-1"
    );
    assert_eq!(f.count(), 1);
}

#[test]
fn registration_rekey_keeps_runner() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("runner"), 10))
        .unwrap();
    let mut request = registration("s1", Some("runner"), 30);
    request.session_ref = "ref-new";
    f.store.register_session(&request).unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("runner".into()), None)));
    assert_eq!(
        f.store
            .session_binding_for_runner("runner", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-new"
    );
    request.session_ref = "ref-newer";
    request.runner = Some("replacement");
    f.store.register_session(&request).unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("replacement".into()), None)));
}

#[test]
fn registration_rekey_follows_network_history() {
    let f = Fixture::new();
    let mut request = registration("s1", Some("broca"), 10);
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 10,
    });
    f.store.register_session(&request).unwrap();
    f.store
        .set_session_network("broca", "s1", "no", || 20)
        .unwrap();
    let history = f
        .store
        .session_network_history("broca-session", "ref-1")
        .unwrap();
    request.session_ref = "ref-new";
    request.network = Some(crate::NetworkStamp {
        network: "no",
        clock: || 30,
    });
    f.store.register_session(&request).unwrap();
    assert_eq!(
        f.store
            .session_network_history("broca-session", "ref-new")
            .unwrap(),
        history
    );
    assert!(f
        .store
        .session_network_history("broca-session", "ref-1")
        .unwrap()
        .is_empty());
    request.session_ref = "ref-newer";
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 15,
    });
    f.store.register_session(&request).unwrap();
    let moved = f
        .store
        .session_network_history("broca-session", "ref-newer")
        .unwrap();
    assert_eq!(&moved[..2], history);
    assert_eq!(
        moved[2],
        crate::NetworkEntry {
            network: "yes".into(),
            since_ms: 21
        }
    );
}

#[test]
fn registration_rekey_during_open_move_is_refused() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    f.open_move(AGENT, "cut-rekey", "published");
    let mut request = registration("s1", Some("broca"), 30);
    request.session_ref = "ref-new";
    let refusal = f.store.register_session(&request).unwrap_err();
    assert_eq!(refusal.code, "agent_moving");
    assert_eq!(
        refusal.detail,
        Some(serde_json::json!({"cut_id":"cut-rekey", "side":"source", "step":"published"}))
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-1"
    );
}

#[test]
fn registration_rekey_absent_project_root_keeps_stored_value() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    let mut request = registration("s1", Some("broca"), 30);
    request.project_root = None;
    request.session_ref = "ref-new";
    assert_eq!(
        f.store
            .register_session(&request)
            .unwrap()
            .project_root
            .as_deref(),
        Some("/project")
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .project_root
            .as_deref(),
        Some("/project")
    );
    assert_eq!(
        f.store
            .register_session(&request)
            .unwrap()
            .project_root
            .as_deref(),
        Some("/project")
    );
    request.project_root = Some("/updated");
    request.session_ref = "ref-newer";
    assert_eq!(
        f.store
            .register_session(&request)
            .unwrap()
            .project_root
            .as_deref(),
        Some("/updated")
    );
}

#[test]
fn registration_rekey_refuses_occupied_destination_and_other_ref_kind() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    let mut occupied = registration("s2", Some("broca"), 20);
    occupied.session_ref = "ref-taken";
    f.store.register_session(&occupied).unwrap();
    let mut request = registration("s1", Some("broca"), 30);
    request.session_ref = "ref-taken";
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "ref_bound_elsewhere"
    );
    request.session_ref = "ref-new";
    request.ref_kind = "other-kind";
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "session_bound_elsewhere"
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-1"
    );
    assert_eq!(f.count(), 2);
}

#[test]
fn registration_rekey_network_refusal_and_write_failure_leave_old_binding() {
    let f = Fixture::new();
    let mut request = registration("s1", Some("broca"), 10);
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 10,
    });
    f.store.register_session(&request).unwrap();
    request.session_ref = "ref-new";
    request.network = Some(crate::NetworkStamp {
        network: "invalid",
        clock: || 20,
    });
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "invalid_request"
    );
    f.store.db.with_conn_fenced(|tx| {
        tx.execute_batch("CREATE TRIGGER fail_history_rekey BEFORE UPDATE ON session_network BEGIN SELECT RAISE(ABORT, 'history write failed'); END")
    }).unwrap();
    request.network = None;
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "storage_error"
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-1"
    );
    assert_eq!(
        f.store
            .session_network_history("broca-session", "ref-1")
            .unwrap()
            .len(),
        1
    );
    assert_eq!(f.count(), 1);
}

#[test]
fn registration_rekey_checks_original_network_history_capacity() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    f.store.db.with_conn_fenced(|tx| {
        for since in 0..crate::NETWORK_HISTORY_MAX {
            tx.execute("INSERT INTO session_network(ref_kind,ref,network,since_ms) VALUES('broca-session','ref-1','yes',?1)", [since])?;
        }
        Ok(())
    }).unwrap();
    let mut request = registration("s1", Some("broca"), 30);
    request.session_ref = "ref-new";
    request.network = Some(crate::NetworkStamp {
        network: "no",
        clock: || 30,
    });
    assert_eq!(
        f.store.register_session(&request).unwrap_err().code,
        "network_history_full"
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .unwrap()
            .session_ref,
        "ref-1"
    );
    request.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 30,
    });
    f.store.register_session(&request).unwrap();
    assert_eq!(
        f.store
            .session_network_history("broca-session", "ref-new")
            .unwrap()
            .len(),
        crate::NETWORK_HISTORY_MAX as usize
    );
}

#[test]
fn end_session_fills_runner_refuses_mismatch_and_repeats_without_writing() {
    let f = Fixture::new();
    assert_eq!(
        f.store
            .end_session("broca", "nobody", Some("broca"), 5)
            .unwrap(),
        SessionEnd::NotFound
    );
    assert_eq!(f.count(), 0, "an unknown session writes nothing");

    f.bind(AGENT, "open", 1, None, None);
    assert_eq!(
        f.store
            .end_session("broca", "open", Some("broca"), 50)
            .unwrap(),
        SessionEnd::Ended { ended_at_ms: 50 }
    );
    assert_eq!(f.lifecycle("open"), Some((Some("broca".into()), Some(50))));

    // The repeat keeps the first end time even with a later clock.
    assert_eq!(
        f.store
            .end_session("broca", "open", Some("broca"), 90)
            .unwrap(),
        SessionEnd::AlreadyEnded { ended_at_ms: 50 }
    );
    assert_eq!(f.lifecycle("open"), Some((Some("broca".into()), Some(50))));

    // An ended binding with no runner stays without one: a repeat writes
    // nothing at all.
    f.bind(AGENT, "ended", 1, None, Some(7));
    assert_eq!(
        f.store
            .end_session("broca", "ended", Some("broca"), 90)
            .unwrap(),
        SessionEnd::AlreadyEnded { ended_at_ms: 7 }
    );
    assert_eq!(f.lifecycle("ended"), Some((None, Some(7))));

    f.bind(AGENT, "hosted", 1, Some("broca"), None);
    let refusal = f
        .store
        .end_session("broca", "hosted", Some("thalamus-harness"), 60)
        .unwrap_err();
    assert_eq!(refusal.code, "runner_mismatch");
    assert_eq!(refusal.detail, Some(serde_json::json!({"runner":"broca"})));
    assert_eq!(f.lifecycle("hosted"), Some((Some("broca".into()), None)));
    // The mismatch is refused even once the session has ended.
    f.store.end_session("broca", "hosted", None, 61).unwrap();
    assert_eq!(
        f.store
            .end_session("broca", "hosted", Some("thalamus-harness"), 62)
            .unwrap_err()
            .code,
        "runner_mismatch"
    );
    assert_eq!(
        f.lifecycle("hosted"),
        Some((Some("broca".into()), Some(61)))
    );
}

#[test]
fn gone_stamps_only_open_bindings_of_a_matching_or_unknown_runner() {
    let f = Fixture::new();
    f.bind(AGENT, "plain", 1, None, None);
    f.bind(AGENT, "same", 1, Some("broca"), None);
    f.bind(AGENT, "other", 1, Some("thalamus-harness"), None);
    f.bind(AGENT, "ended", 1, Some("broca"), Some(3));
    let stamp = |session_id: &str, runner: Option<&str>| GoneSession {
        harness: "broca".into(),
        session_id: session_id.into(),
        runner: runner.map(Into::into),
        ended_at_ms: 100,
    };
    let stamped = f
        .store
        .stamp_gone_sessions(&[
            stamp("plain", Some("broca")),
            stamp("same", Some("broca")),
            stamp("other", Some("broca")),
            stamp("ended", None),
            stamp("missing", None),
        ])
        .unwrap();
    assert_eq!(stamped, 2);
    assert_eq!(f.lifecycle("plain"), Some((None, Some(100))));
    assert_eq!(f.lifecycle("same"), Some((Some("broca".into()), Some(100))));
    assert_eq!(
        f.lifecycle("other"),
        Some((Some("thalamus-harness".into()), None))
    );
    assert_eq!(f.lifecycle("ended"), Some((Some("broca".into()), Some(3))));
    // Without a runner on the feed entry, any open binding of the key stamps.
    f.store
        .stamp_gone_sessions(&[stamp("other", None)])
        .unwrap();
    assert_eq!(
        f.lifecycle("other"),
        Some((Some("thalamus-harness".into()), Some(100)))
    );
}

/// A binding ended exactly thirty days ago is kept, one a millisecond older
/// is pruned, and a binding that never ended is kept however old it is.
#[test]
fn retention_prunes_past_thirty_days_and_keeps_the_boundary() {
    let f = Fixture::new();
    let now = SESSION_RETENTION_MS + 1_000;
    f.bind(AGENT, "boundary", 1, None, Some(now - SESSION_RETENTION_MS));
    f.bind(AGENT, "past", 1, None, Some(now - SESSION_RETENTION_MS - 1));
    f.bind(AGENT, "recent", 1, None, Some(now - 1));
    f.bind(AGENT, "open", 0, None, None);
    assert_eq!(f.store.prune_ended_sessions(now).unwrap(), 1);
    assert!(f.lifecycle("past").is_none());
    for kept in ["boundary", "recent", "open"] {
        assert!(f.lifecycle(kept).is_some(), "{kept}");
    }
}

/// While an agent has a move that has not finished, none of its bindings is
/// pruned, ended or not; a finished move no longer protects them.
#[test]
fn retention_keeps_every_binding_of_an_agent_with_an_open_move() {
    let f = Fixture::new();
    let now = SESSION_RETENTION_MS * 2;
    f.bind(AGENT, "moving-old", 1, None, Some(1));
    f.bind(OTHER, "settled-old", 1, None, Some(1));
    f.open_move(AGENT, "cut-open", "published");
    f.open_move(OTHER, "cut-done", "released");
    assert_eq!(f.store.prune_ended_sessions(now).unwrap(), 1);
    assert!(f.lifecycle("moving-old").is_some());
    assert!(f.lifecycle("settled-old").is_none());
}

/// More expired rows than one batch deletes are all pruned in one call.
#[test]
fn retention_prunes_a_backlog_larger_than_one_batch() {
    let f = Fixture::new();
    let rows = PRUNE_BATCH as usize * 2 + 3;
    f.store
        .db
        .with_conn_fenced(|tx| {
            for n in 0..rows {
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,ended_at_ms)
                     VALUES('broca-session',?1,?2,'broca',?1,'','orchestrator',1,1)",
                    params![format!("backlog-{n}"), AGENT],
                )?;
            }
            Ok(())
        })
        .unwrap();
    assert_eq!(
        f.store
            .prune_ended_sessions(SESSION_RETENTION_MS * 2)
            .unwrap(),
        rows
    );
    assert_eq!(f.count(), 0);
}

/// Pages run newest registration first with ties in `(ref_kind, ref)`
/// order, and a cursor keeps its place when newer bindings arrive.
#[test]
fn agent_sessions_pages_newest_first_and_cursor_survives_inserts() {
    let f = Fixture::new();
    f.bind(AGENT, "b", 20, None, None);
    f.bind(AGENT, "a", 20, Some("broca"), Some(25));
    f.bind(AGENT, "c", 10, None, None);
    f.bind(AGENT, "d", 30, None, None);
    f.bind(OTHER, "elsewhere", 40, None, None);
    let first = f.store.agent_sessions(AGENT, None, 2).unwrap();
    let refs = |page: &AgentSessionPage| {
        page.sessions
            .iter()
            .map(|s| s.session_ref.clone())
            .collect::<Vec<_>>()
    };
    assert_eq!(refs(&first), ["d", "a"]);
    let cursor = first.next.clone().unwrap();
    assert_eq!(
        cursor,
        SessionCursor {
            registered_at_ms: 20,
            ref_kind: "broca-session".into(),
            session_ref: "a".into()
        }
    );
    // A newer binding lands on the first page, not inside the next one.
    f.bind(AGENT, "e", 50, None, None);
    let second = f.store.agent_sessions(AGENT, Some(&cursor), 2).unwrap();
    assert_eq!(refs(&second), ["b", "c"]);
    assert_eq!(second.next, None, "the last page has no cursor");
    assert_eq!(
        second.sessions[0],
        AgentSession {
            role: "head".into(),
            runner: None,
            session: Some("b".into()),
            harness: Some("broca".into()),
            ref_kind: "broca-session".into(),
            session_ref: "b".into(),
            project_root: Some("/project".into()),
            registered_at_ms: 20,
            ended_at_ms: None,
            network: "unknown".into(),
            held_on: None,
        }
    );
    assert_eq!(first.sessions[1].ended_at_ms, Some(25));
    assert_eq!(first.sessions[1].runner.as_deref(), Some("broca"));
}

#[test]
fn agent_sessions_refuses_unknown_agent_and_lists_retired() {
    let f = Fixture::new();
    assert_eq!(
        f.store
            .agent_sessions("agent_ffffffffffffffff", None, 10)
            .unwrap_err()
            .code,
        "unknown_agent"
    );
    f.bind(RETIRED, "kept", 1, None, Some(2));
    let page = f.store.agent_sessions(RETIRED, None, 10).unwrap();
    assert_eq!(page.sessions.len(), 1);
    let empty = f.store.agent_sessions(AGENT, None, 10).unwrap();
    assert!(empty.sessions.is_empty());
    assert_eq!(empty.next, None);
}

/// Nothing in the session lifecycle is journaled: it is machine-local and
/// must never reach replay or the identity log.
#[test]
fn lifecycle_writes_append_no_journal_entry() {
    let f = Fixture::new();
    let before = f.store.generation().unwrap();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    f.store.end_session("broca", "s1", None, 20).unwrap();
    f.store
        .register_session(&registration("s1", Some("broca"), 30))
        .unwrap();
    f.store
        .stamp_gone_sessions(&[GoneSession {
            harness: "broca".into(),
            session_id: "s1".into(),
            runner: None,
            ended_at_ms: 40,
        }])
        .unwrap();
    f.store
        .prune_ended_sessions(SESSION_RETENTION_MS * 2)
        .unwrap();
    assert_eq!(f.store.generation().unwrap(), before);
}

fn hosted<'a>(
    session_ref: &'a str,
    harness: &'a str,
    session_id: &'a str,
    runner: Option<&'a str>,
) -> SessionRegistration<'a> {
    SessionRegistration {
        role: None,
        agent_id: AGENT,
        ref_kind: "broca-session",
        session_ref,
        harness,
        session_id,
        project_root: Some("/project"),
        registered_by: "orchestrator",
        runner,
        network: None,
        now_ms: 10,
    }
}

/// A runner and session id name one binding. Registering them under another
/// reference refuses `session_bound_elsewhere` with the same detail as a
/// taken harness session key, and writes nothing, including when the runner
/// comes from the reference's earlier registration rather than the request.
#[test]
fn registration_refuses_a_runner_session_bound_elsewhere() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
    let refusal = f
        .store
        .register_session(&hosted("ref-2", "other-harness", "s1", Some("broca")))
        .unwrap_err();
    assert_eq!(refusal.code, "session_bound_elsewhere");
    assert_eq!(
        refusal.detail,
        Some(serde_json::json!({"ref_kind":"broca-session","ref":"ref-1","agent_id":AGENT}))
    );
    assert_eq!(f.count(), 1, "the refused registration wrote nothing");

    // Moving another reference onto a session id the named runner already
    // uses is refused too.
    f.store
        .register_session(&hosted("ref-3", "broca", "s3", Some("broca")))
        .unwrap();
    assert_eq!(
        f.store
            .register_session(&hosted("ref-3", "other-harness", "s1", Some("broca")))
            .unwrap_err()
            .code,
        "session_bound_elsewhere"
    );
    assert_eq!(f.lifecycle("s3"), Some((Some("broca".into()), None)));

    // Another runner's namespace is separate, and so is a row with no runner.
    f.store
        .register_session(&hosted("ref-4", "thalamus", "s1", Some("thalamus-harness")))
        .unwrap();
    f.legacy_binding(&hosted("ref-5", "pi", "s1", None));
    // Re-registering the owning reference is not a conflict with itself.
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
}

/// A host session that moves to a new host kind on the same runner keeps its
/// id (an OpenCode 1 session reopened under OpenCode 2). The same agent's
/// registration under the new reference kind and harness takes the binding
/// over in place, keeping its network history and caller role, and leaves no
/// row under the old kind. Another agent is still refused.
#[test]
fn registration_moves_a_runner_session_to_a_new_host_kind() {
    let f = Fixture::new();
    let mut old = hosted("ses_1", "opencode", "ses_1", Some("thalamus-harness"));
    old.ref_kind = "opencode-session";
    old.network = Some(crate::NetworkStamp {
        network: "yes",
        clock: || 10,
    });
    f.store.register_session(&old).unwrap();
    f.store
        .set_session_network("opencode", "ses_1", "no", || 20)
        .unwrap();
    let history = f
        .store
        .session_network_history("opencode-session", "ses_1")
        .unwrap();
    assert_eq!(history.len(), 2);

    let mut stranger = hosted("ses_1", "opencode2", "ses_1", Some("thalamus-harness"));
    stranger.ref_kind = "opencode2-session";
    stranger.agent_id = OTHER;
    assert_eq!(
        f.store.register_session(&stranger).unwrap_err().code,
        "session_bound_elsewhere"
    );

    let mut new = hosted("ses_1", "opencode2", "ses_1", Some("thalamus-harness"));
    new.ref_kind = "opencode2-session";
    new.registered_by = "host";
    new.now_ms = 30;
    let bound = f.store.register_session(&new).unwrap();
    assert_eq!(bound.ref_kind, "opencode2-session");
    assert_eq!(bound.harness.as_deref(), Some("opencode2"));
    assert_eq!(bound.registered_by, "orchestrator", "caller role is kept");
    assert_eq!(f.count(), 1, "the binding moved; no second row");
    assert_eq!(
        f.store
            .session_network_history("opencode2-session", "ses_1")
            .unwrap(),
        history
    );
    assert!(f
        .store
        .session_binding_for("opencode", "ses_1")
        .unwrap()
        .is_none());
    assert_eq!(
        f.store
            .session_binding_for("opencode2", "ses_1")
            .unwrap()
            .map(|b| b.agent_id),
        Some(AGENT.to_owned())
    );
}

#[test]
fn session_binding_for_runner_finds_only_runner_rows() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
    f.legacy_binding(&hosted("ref-2", "pi", "s2", None));
    f.store.end_session("broca", "s1", None, 20).unwrap();
    assert_eq!(
        f.store.session_binding_for_runner("broca", "s1").unwrap(),
        Some(crate::RunnerSessionBinding {
            role: "head".into(),
            agent_id: AGENT.into(),
            runner: Some("broca".into()),
            session: "s1".into(),
            harness: Some("broca".into()),
            ref_kind: "broca-session".into(),
            session_ref: "ref-1".into(),
            project_root: Some("/project".into()),
            ended_at_ms: Some(20),
            network: "unknown".into(),
            network_history: Vec::new(),
            held_on: None,
        })
    );
    assert_eq!(
        f.store.session_binding_for_runner("pi", "s2").unwrap(),
        None
    );
    assert_eq!(
        f.store
            .session_binding_for_runner("thalamus-harness", "s1")
            .unwrap(),
        None
    );
}

#[test]
fn session_binding_for_harness_includes_runnerless_rows() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "pi", "s1", Some("broca")))
        .unwrap();
    f.legacy_binding(&hosted("ref-2", "opencode", "s2", None));
    f.store.end_session("pi", "s1", None, 20).unwrap();
    assert_eq!(
        f.store.session_binding_for_harness("pi", "s1").unwrap(),
        f.store.session_binding_for_runner("broca", "s1").unwrap()
    );
    let runnerless = f
        .store
        .session_binding_for_harness("opencode", "s2")
        .unwrap()
        .unwrap();
    assert_eq!(runnerless.runner, None);
    assert_eq!(runnerless.session_ref, "ref-2");
    assert_eq!(runnerless.project_root.as_deref(), Some("/project"));
    assert_eq!(runnerless.network, "unknown");
    assert!(runnerless.network_history.is_empty());
    for (harness, session) in [("pi", "s2"), ("broca", "s1"), ("opencode", "missing")] {
        assert_eq!(
            f.store
                .session_binding_for_harness(harness, session)
                .unwrap(),
            None
        );
    }
}

/// Ending a runner-less binding with a runner whose session id another
/// binding already holds still records the end but leaves the runner empty.
#[test]
fn end_session_leaves_runner_empty_when_another_binding_holds_it() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
    f.legacy_binding(&hosted("ref-2", "pi", "s1", None));
    assert_eq!(
        f.store.end_session("pi", "s1", Some("broca"), 30).unwrap(),
        SessionEnd::Ended { ended_at_ms: 30 }
    );
    let rows: Vec<(Option<String>, Option<i64>)> = f
        .store
        .read(|conn| {
            conn.prepare("SELECT runner, ended_at_ms FROM session_binding ORDER BY ref")?
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect()
        })
        .unwrap();
    assert_eq!(rows, [(Some("broca".into()), None), (None, Some(30))]);
}

#[test]
fn migration_17_preserves_local_rows_and_indexes_open_only_pages() {
    let f = Fixture::with_migrations(&MIGRATIONS[..16]);
    f.bind(AGENT, "live", 20, Some("broca"), None);
    f.bind(AGENT, "ended", 30, Some("broca"), Some(40));
    drop(f.store);
    let store = RegistryStore::open(&f.descriptor).unwrap();
    let page = store.agent_sessions(AGENT, None, 100).unwrap();
    assert_eq!(page.sessions.len(), 2);
    assert!(page.sessions.iter().all(|row| row.held_on.is_none()));
    let open = store
        .agent_sessions_filtered(AGENT, None, 100, true)
        .unwrap();
    assert_eq!(open.sessions.len(), 1);
    assert_eq!(open.sessions[0].session_ref, "live");
    let plan = store.read(|conn| {
        let mut stmt = conn.prepare("EXPLAIN QUERY PLAN SELECT ref FROM session_binding WHERE agent_id=?1 AND ended_at_ms IS NULL ORDER BY registered_at_ms DESC,ref_kind,ref LIMIT 100")?;
        let rows = stmt.query_map([AGENT], |r| r.get::<_,String>(3))?.collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(rows.join("\n"))
    }).unwrap();
    assert!(plan.contains("session_binding_open_agent"), "{plan}");
    assert!(!plan.contains("TEMP B-TREE"), "{plan}");
}

/// The last migration adds the ended-binding index on a store that already
/// holds bindings, and the retention prune reads it to find candidates
/// instead of scanning every binding that never ended.
#[test]
fn retention_prune_reads_the_ended_binding_index_after_migration() {
    let f = Fixture::with_migrations(&MIGRATIONS[..MIGRATIONS.len() - 1]);
    f.bind(AGENT, "live", 20, Some("broca"), None);
    f.bind(AGENT, "ended", 30, Some("broca"), Some(40));
    let plan = |store: &RegistryStore| {
        store
            .read(|conn| {
                let mut statement =
                    conn.prepare(&format!("EXPLAIN QUERY PLAN {PRUNE_ENDED_SQL}"))?;
                let rows = statement
                    .query_map(params![0, PRUNE_BATCH], |r| r.get::<_, String>(3))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(rows.join("\n"))
            })
            .unwrap()
    };
    assert!(!plan(&f.store).contains("session_binding_ended"));
    drop(f.store);
    let store = RegistryStore::open(&f.descriptor).unwrap();
    let plan = plan(&store);
    assert!(
        plan.contains("USING INDEX session_binding_ended")
            || plan.contains("USING COVERING INDEX session_binding_ended"),
        "{plan}"
    );
    assert_eq!(
        store
            .prune_ended_sessions(40 + SESSION_RETENTION_MS + 1)
            .unwrap(),
        1
    );
    assert_eq!(
        store
            .agent_sessions(AGENT, None, 100)
            .unwrap()
            .sessions
            .len(),
        1
    );
}

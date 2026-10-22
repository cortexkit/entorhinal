use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

use rusqlite::params;

use super::*;
use crate::{agent, RegistryStore, SessionRegistration, MIGRATIONS, SESSION_RETENTION_MS};

const AGENT: &str = "agent_0123456789abcdef";
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
            "session-network-{}-{}",
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
                tx.execute(
                    "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,?1,'test','assistant',1,1)",
                    [AGENT],
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

    /// Insert a Broca binding directly, whose reference and session id are
    /// both `session`.
    fn bind(&self, session: &str, ended_at_ms: Option<i64>) {
        self.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,runner,ended_at_ms)
                     VALUES('broca-session',?1,?2,'broca',?1,'/project','orchestrator',1,'broca',?3)",
                    params![session, AGENT, ended_at_ms],
                )?;
                Ok(())
            })
            .unwrap();
    }

    fn history(&self, session: &str) -> Vec<(String, i64)> {
        self.store
            .session_network_history("broca-session", session)
            .unwrap()
            .into_iter()
            .map(|entry| (entry.network, entry.since_ms))
            .collect()
    }

    fn rows(&self) -> i64 {
        self.store
            .read(|conn| conn.query_row("SELECT COUNT(*) FROM session_network", [], |r| r.get(0)))
            .unwrap()
    }

    fn set(&self, session: &str, network: &str, clock: fn() -> i64) -> NetworkChange {
        self.store
            .set_session_network("broca", session, network, clock)
            .unwrap()
    }
}

fn entry(network: &str, since_ms: i64) -> NetworkEntry {
    NetworkEntry {
        network: network.into(),
        since_ms,
    }
}

fn registration<'a>(
    session_id: &'a str,
    network: Option<&'a str>,
    clock: fn() -> i64,
) -> SessionRegistration<'a> {
    SessionRegistration {
        agent_id: AGENT,
        ref_kind: "broca-session",
        session_ref: "ref-1",
        harness: "broca",
        session_id,
        project_root: Some("/project"),
        registered_by: "orchestrator",
        runner: Some("broca"),
        network: network.map(|network| NetworkStamp { network, clock }),
        now_ms: 5,
    }
}

/// A store just before migration 16 keeps its bindings across it. They gain
/// an empty history, and the new table takes only the three values, one
/// entry per binding and time, and only for a binding that exists.
#[test]
fn migration_16_adds_an_empty_history_keyed_to_the_binding() {
    let network = MIGRATIONS
        .iter()
        .position(|migration| migration.statements == V16_SESSION_NETWORK)
        .expect("the session network migration is in the chain");
    let Fixture {
        store,
        descriptor,
        _cleanup,
    } = Fixture::with_migrations(&MIGRATIONS[..network]);
    store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,runner)
                 VALUES('broca-session','old',?1,'broca','old','/project','orchestrator',5,'broca')",
                [AGENT],
            )?;
            Ok(())
        })
        .unwrap();
    drop(store);
    let store = RegistryStore::open(&descriptor).unwrap();
    let binding = store
        .session_binding_for_runner("broca", "old")
        .unwrap()
        .unwrap();
    assert_eq!(binding.network, "unknown");
    assert!(binding.network_history.is_empty());
    let columns: Vec<String> = store
        .read(|conn| {
            conn.prepare("PRAGMA table_info(session_network)")?
                .query_map([], |row| row.get::<_, String>(1))?
                .collect()
        })
        .unwrap();
    assert_eq!(columns, ["ref_kind", "ref", "network", "since_ms"]);
    let foreign: (String, String) = store
        .read(|conn| {
            conn.query_row(
                "SELECT \"table\", on_delete FROM pragma_foreign_key_list('session_network')",
                [],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
        })
        .unwrap();
    assert_eq!(foreign, ("session_binding".into(), "CASCADE".into()));
    store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO session_network VALUES('broca-session','old','yes',10)",
                [],
            )?;
            for refused in [
                "INSERT INTO session_network VALUES('broca-session','old','maybe',11)",
                "INSERT INTO session_network VALUES('broca-session','old','no',10)",
                "INSERT INTO session_network VALUES('broca-session','missing','no',12)",
            ] {
                assert!(tx.execute(refused, []).is_err(), "{refused}");
            }
            Ok(())
        })
        .unwrap();
}

/// A new value is appended at the clock, the current value again writes
/// nothing and returns the current entry, and both reads show the current
/// value and the history oldest first.
#[test]
fn set_appends_new_values_and_a_repeat_writes_nothing() {
    let f = Fixture::new();
    f.bind("s1", None);
    assert_eq!(
        f.set("s1", "yes", || 10),
        NetworkChange::Recorded(entry("yes", 10))
    );
    assert_eq!(
        f.set("s1", "yes", || 15),
        NetworkChange::Unchanged(entry("yes", 10)),
        "a repeat returns the current entry"
    );
    assert_eq!(f.rows(), 1, "a repeat writes nothing");
    assert_eq!(
        f.set("s1", "no", || 20),
        NetworkChange::Recorded(entry("no", 20))
    );
    assert_eq!(f.history("s1"), [("yes".into(), 10), ("no".into(), 20)]);
    let binding = f
        .store
        .session_binding_for_runner("broca", "s1")
        .unwrap()
        .unwrap();
    assert_eq!(binding.network, "no");
    assert_eq!(binding.network_history, [entry("yes", 10), entry("no", 20)]);
    let page = f.store.agent_sessions(AGENT, None, 10).unwrap();
    assert_eq!(page.sessions[0].network, "no");
}

/// The stamp follows the clock, but never lands at or before the previous
/// entry: a clock that steps back gives the previous time plus one.
#[test]
fn since_ms_stays_strictly_increasing_when_the_clock_steps_back() {
    static CLOCK: AtomicI64 = AtomicI64::new(100);
    fn clock() -> i64 {
        CLOCK.load(Ordering::Relaxed)
    }
    let f = Fixture::new();
    f.bind("s1", None);
    f.set("s1", "yes", clock);
    CLOCK.store(50, Ordering::Relaxed);
    f.set("s1", "no", clock);
    f.set("s1", "unknown", clock);
    CLOCK.store(300, Ordering::Relaxed);
    f.set("s1", "yes", clock);
    assert_eq!(
        f.history("s1"),
        [
            ("yes".into(), 100),
            ("no".into(), 101),
            ("unknown".into(), 102),
            ("yes".into(), 300),
        ]
    );
}

/// A binding holding the most entries refuses another value with nothing
/// written, while the current value still answers as a repeat.
#[test]
fn a_full_history_refuses_network_history_full_and_writes_nothing() {
    let f = Fixture::new();
    f.bind("s1", None);
    f.store
        .db
        .with_conn_fenced(|tx| {
            let mut insert =
                tx.prepare("INSERT INTO session_network VALUES('broca-session','s1',?1,?2)")?;
            for since in 0..NETWORK_HISTORY_MAX - 1 {
                insert.execute(params![if since % 2 == 0 { "no" } else { "yes" }, since])?;
            }
            Ok(())
        })
        .unwrap();
    let last = NETWORK_HISTORY_MAX - 1;
    assert_eq!(
        f.set("s1", "yes", stalled_clock),
        NetworkChange::Recorded(entry("yes", last)),
        "the last entry the cap allows is written"
    );
    let refused = f
        .store
        .set_session_network("broca", "s1", "no", stalled_clock)
        .unwrap_err();
    assert_eq!(refused.code, "network_history_full");
    assert_eq!(f.rows(), NETWORK_HISTORY_MAX, "a refusal writes nothing");
    assert_eq!(
        f.set("s1", "yes", stalled_clock),
        NetworkChange::Unchanged(entry("yes", last))
    );
    // A registration naming another value is refused the same way and
    // leaves the binding as it was.
    let mut register = registration("s2", Some("no"), stalled_clock);
    register.session_ref = "s1";
    assert_eq!(
        f.store.register_session(&register).unwrap_err().code,
        "network_history_full"
    );
    assert_eq!(
        f.store
            .session_binding_for("broca", "s1")
            .unwrap()
            .map(|b| b.registered_at_ms),
        Some(1)
    );
}

/// A clock that never moves past the last seeded entry, so each stamp is
/// raised to the previous entry plus one.
fn stalled_clock() -> i64 {
    0
}

/// A binding with no history reads `unknown` on both reads, never `no`.
#[test]
fn an_absent_value_reads_unknown_not_no() {
    let f = Fixture::new();
    f.bind("s1", None);
    let binding = f
        .store
        .session_binding_for_runner("broca", "s1")
        .unwrap()
        .unwrap();
    assert_eq!(binding.network, "unknown");
    assert!(binding.network_history.is_empty());
    let page = f.store.agent_sessions(AGENT, None, 10).unwrap();
    assert_eq!(page.sessions[0].network, "unknown");
}

/// Retention deletes a pruned binding's history with it and keeps the
/// history of a binding it keeps.
#[test]
fn history_rows_are_deleted_with_their_binding_on_prune() {
    let now = SESSION_RETENTION_MS * 2;
    let f = Fixture::new();
    f.bind("old", Some(1));
    f.bind("open", None);
    f.set("old", "yes", || 1);
    f.set("old", "no", || 2);
    f.set("open", "no", || 3);
    assert_eq!(f.store.prune_ended_sessions(now).unwrap(), 1);
    assert!(f.history("old").is_empty());
    assert_eq!(f.history("open"), [("no".into(), 3)]);
    assert_eq!(f.rows(), 1);
}

/// An unknown session refuses `not_found`; a value other than the three
/// refuses `invalid_request`, before the session is looked up.
#[test]
fn unknown_session_and_invalid_value_refuse() {
    let f = Fixture::new();
    f.bind("s1", None);
    let missing = f
        .store
        .set_session_network("broca", "nobody", "yes", || 1)
        .unwrap_err();
    assert_eq!(missing.code, "not_found");
    for value in ["", "No", "maybe"] {
        for session in ["s1", "nobody"] {
            assert_eq!(
                f.store
                    .set_session_network("broca", session, value, || 1)
                    .unwrap_err()
                    .code,
                "invalid_request",
                "{value} on {session}"
            );
        }
    }
    assert_eq!(f.rows(), 0);
}

/// A registration's network value is appended when it differs from the
/// current one; an absent value records nothing, and an invalid one refuses
/// with no binding written.
#[test]
fn registration_records_a_changed_network_value() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", None, || 1))
        .unwrap();
    assert!(f.history("ref-1").is_empty(), "absent records nothing");
    f.store
        .register_session(&registration("s1", Some("yes"), || 10))
        .unwrap();
    f.store
        .register_session(&registration("s1", Some("yes"), || 20))
        .unwrap();
    f.store
        .register_session(&registration("s1", None, || 25))
        .unwrap();
    f.store
        .register_session(&registration("s1", Some("no"), || 30))
        .unwrap();
    assert_eq!(f.history("ref-1"), [("yes".into(), 10), ("no".into(), 30)]);

    let mut invalid = registration("s9", Some("maybe"), || 40);
    invalid.session_ref = "ref-9";
    assert_eq!(
        f.store.register_session(&invalid).unwrap_err().code,
        "invalid_request"
    );
    assert_eq!(f.store.session_binding_for("broca", "s9").unwrap(), None);
}

/// Network writes are machine-local: no journal entry, so `verify` and
/// replay never see them.
#[test]
fn network_writes_append_no_journal_entry() {
    let f = Fixture::new();
    f.bind("s1", None);
    let before = f.store.generation().unwrap();
    f.set("s1", "yes", || 1);
    f.set("s1", "no", || 2);
    f.store
        .register_session(&registration("s2", Some("yes"), || 3))
        .unwrap();
    assert_eq!(f.store.generation().unwrap(), before);
}

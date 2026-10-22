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
        agent_id: AGENT,
        ref_kind: "broca-session",
        session_ref: "ref-1",
        harness: "broca",
        session_id,
        project_root: "/project",
        registered_by: "orchestrator",
        runner,
        network: None,
        now_ms,
    }
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

/// Registering records a named runner, keeps it when a later registration
/// names none, replaces it when one names another, and always clears the end
/// time because the session is live again.
#[test]
fn registration_records_runner_keeps_it_when_absent_and_clears_end() {
    let f = Fixture::new();
    f.store
        .register_session(&registration("s1", Some("broca"), 10))
        .unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), None)));
    f.store.end_session("broca", "s1", None, 20).unwrap();
    assert_eq!(f.lifecycle("s1"), Some((Some("broca".into()), Some(20))));
    f.store
        .register_session(&registration("s1", None, 30))
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
            runner: None,
            session: Some("b".into()),
            harness: Some("broca".into()),
            ref_kind: "broca-session".into(),
            session_ref: "b".into(),
            project_root: Some("/project".into()),
            registered_at_ms: 20,
            ended_at_ms: None,
            network: "unknown".into(),
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
        .register_session(&registration("s1", None, 30))
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
        agent_id: AGENT,
        ref_kind: "broca-session",
        session_ref,
        harness,
        session_id,
        project_root: "/project",
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

    // The reference keeps its runner when re-registered without one, so
    // moving it onto a session id that runner already uses is refused too.
    f.store
        .register_session(&hosted("ref-3", "broca", "s3", Some("broca")))
        .unwrap();
    assert_eq!(
        f.store
            .register_session(&hosted("ref-3", "other-harness", "s1", None))
            .unwrap_err()
            .code,
        "session_bound_elsewhere"
    );
    assert_eq!(f.lifecycle("s3"), Some((Some("broca".into()), None)));

    // Another runner's namespace is separate, and so is a row with no runner.
    f.store
        .register_session(&hosted("ref-4", "thalamus", "s1", Some("thalamus-harness")))
        .unwrap();
    f.store
        .register_session(&hosted("ref-5", "pi", "s1", None))
        .unwrap();
    // Re-registering the owning reference is not a conflict with itself.
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", None))
        .unwrap();
}

#[test]
fn session_binding_for_runner_finds_only_runner_rows() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
    f.store
        .register_session(&hosted("ref-2", "pi", "s2", None))
        .unwrap();
    f.store.end_session("broca", "s1", None, 20).unwrap();
    assert_eq!(
        f.store.session_binding_for_runner("broca", "s1").unwrap(),
        Some(crate::RunnerSessionBinding {
            agent_id: AGENT.into(),
            runner: "broca".into(),
            session: "s1".into(),
            harness: Some("broca".into()),
            ref_kind: "broca-session".into(),
            session_ref: "ref-1".into(),
            project_root: Some("/project".into()),
            ended_at_ms: Some(20),
            network: "unknown".into(),
            network_history: Vec::new(),
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

/// Ending a runner-less binding with a runner whose session id another
/// binding already holds still records the end but leaves the runner empty.
#[test]
fn end_session_leaves_runner_empty_when_another_binding_holds_it() {
    let f = Fixture::new();
    f.store
        .register_session(&hosted("ref-1", "broca", "s1", Some("broca")))
        .unwrap();
    f.store
        .register_session(&hosted("ref-2", "pi", "s1", None))
        .unwrap();
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

//! Machine-local claim replicas. These transactions never append to the
//! identity journal; a refresh is a local observation, not a shared write.

use crate::{agent, RegistryError, RegistryStore};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::Serialize;
use serde_json::Value;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CachedClaim {
    pub holder: Option<String>,
    pub epoch: i64,
    pub cut_id: Option<String>,
    pub version: i64,
    pub read_at_ms: i64,
    pub stale: bool,
    pub integrity_failed: bool,
}

/// Only verified replies supply a self author. Integrity failures supply no
/// body; transport failures do not turn a never-read agent into a known one.
#[derive(Clone, Debug)]
pub enum ClaimObservation {
    Present {
        claim: CachedClaim,
        self_author: String,
    },
    Absent {
        stale: bool,
        self_author: String,
    },
    IntegrityFailed,
    CodecFailed {
        self_author: String,
    },
    Unavailable,
}

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct AgentWhere {
    pub known: bool,
    pub status: &'static str,
    pub holder: Option<String>,
    pub holder_name: Option<String>,
    pub epoch: i64,
    pub cut_id: Option<String>,
    pub read_at_ms: Option<i64>,
    pub stale: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub moving: Option<Value>,
    #[serde(rename = "moveErrors", skip_serializing_if = "Vec::is_empty")]
    pub move_errors: Vec<Value>,
    #[serde(rename = "moveWarnings", skip_serializing_if = "Vec::is_empty")]
    pub move_warnings: Vec<Value>,
    #[serde(rename = "moveDiagnostics", skip_serializing_if = "Vec::is_empty")]
    pub move_diagnostics: Vec<Value>,
}

pub(crate) fn load_claim(conn: &Connection, id: &str) -> rusqlite::Result<Option<CachedClaim>> {
    conn.query_row(
        "SELECT holder,epoch,cut_id,version,read_at_ms,stale,integrity_failed FROM claim_cache WHERE agent_id=?1",
        [id],
        |row| Ok(CachedClaim { holder: row.get(0)?, epoch: row.get(1)?, cut_id: row.get(2)?, version: row.get(3)?, read_at_ms: row.get(4)?, stale: row.get(5)?, integrity_failed: row.get(6)? }),
    ).optional()
}

pub(crate) fn self_author(conn: &Connection) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT self_author FROM self_identity WHERE id=1",
        [],
        |row| row.get(0),
    )
    .optional()
}

/// Returns false on conflict without replacing the persisted identity. The
/// caller records that conflict in health, independently of identity-log state.
pub(crate) fn learn_self_author(
    tx: &Transaction<'_>,
    author: &str,
    now: i64,
) -> rusqlite::Result<bool> {
    if let Some(known) = self_author(tx)? {
        return Ok(known == author);
    }
    tx.execute(
        "INSERT INTO self_identity(id,self_author,learned_at_ms) VALUES(1,?1,?2)",
        params![author, now],
    )?;
    Ok(true)
}

pub(crate) fn holder_name(
    conn: &Connection,
    holder: Option<&str>,
) -> rusqlite::Result<Option<String>> {
    conn.query_row(
        "SELECT display_name FROM member_name WHERE author=?1",
        [holder],
        |row| row.get(0),
    )
    .optional()
}

fn refusal(code: &str, detail: Option<serde_json::Value>) -> agent::AgentMutationError {
    agent::AgentMutationError {
        code: code.into(),
        message: code.into(),
        detail,
    }
}

pub(crate) fn check_registration_claim(
    tx: &Transaction<'_>,
    id: &str,
) -> Result<(), agent::AgentMutationError> {
    if let Some(claim) = load_claim(tx, id)? {
        if claim.integrity_failed {
            return Err(refusal("claim_integrity_failed", None));
        }
        if claim.epoch >= 1 && (claim.holder.is_none() || claim.holder != self_author(tx)?) {
            return Err(refusal(
                "held_elsewhere",
                Some(serde_json::json!({
                    "holder": claim.holder,
                    "holder_name": holder_name(tx, claim.holder.as_deref())?,
                    "epoch": claim.epoch,
                })),
            ));
        }
    }
    #[cfg(test)]
    REGISTRATION_HOOK.with(|hook| {
        if let Some(hook) = hook.borrow_mut().take() {
            hook();
        }
    });
    Ok(())
}

#[cfg(test)]
thread_local! { static REGISTRATION_HOOK: std::cell::RefCell<Option<Box<dyn FnOnce()>>> = const { std::cell::RefCell::new(None) }; }

fn unclaimed(now: i64, stale: bool, integrity_failed: bool) -> CachedClaim {
    CachedClaim {
        holder: None,
        epoch: 0,
        cut_id: None,
        version: 0,
        read_at_ms: now,
        stale,
        integrity_failed,
    }
}

/// Implements the cache observation table: integrity errors retain the body and
/// set the mark; older versions and stale absence retain newer bodies and marks;
/// verified replacements and fresh absence clear the mark. Unavailable reads
/// only stale an existing row. A never-read role supplies `None` as its old row.
pub(crate) fn decide(
    old: Option<CachedClaim>,
    observation: &ClaimObservation,
    now: i64,
) -> Option<CachedClaim> {
    let mut next = match observation {
        ClaimObservation::IntegrityFailed | ClaimObservation::CodecFailed { .. } => {
            let mut row = old.unwrap_or_else(|| unclaimed(now, true, true));
            row.integrity_failed = true;
            row
        }
        ClaimObservation::Unavailable => {
            let mut row = old?;
            row.stale = true;
            row
        }
        ClaimObservation::Present { claim, .. } => match old {
            Some(mut cached) if claim.version < cached.version => {
                cached.stale = true;
                cached
            }
            _ if claim.epoch == 0 => unclaimed(now, claim.stale, false),
            _ => {
                let mut row = claim.clone();
                row.integrity_failed = false;
                row
            }
        },
        ClaimObservation::Absent { stale, .. } => match old {
            Some(mut cached) if *stale && cached.version >= 1 => {
                cached.stale = true;
                cached
            }
            cached => unclaimed(
                now,
                *stale,
                *stale && cached.is_some_and(|c| c.integrity_failed),
            ),
        },
    };
    next.read_at_ms = now;
    Some(next)
}

fn apply(
    tx: &Transaction<'_>,
    id: &str,
    observation: &ClaimObservation,
    now: i64,
) -> rusqlite::Result<bool> {
    let old = load_claim(tx, id)?;
    let conflict = match observation {
        ClaimObservation::Present { self_author, .. }
        | ClaimObservation::Absent { self_author, .. }
        | ClaimObservation::CodecFailed { self_author } => {
            !learn_self_author(tx, self_author, now)?
        }
        _ => false,
    };
    let Some(next) = decide(old, observation, now) else {
        return Ok(conflict);
    };
    tx.execute(
        "INSERT INTO claim_cache(agent_id,holder,epoch,cut_id,version,read_at_ms,stale,integrity_failed) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
         ON CONFLICT(agent_id) DO UPDATE SET holder=excluded.holder,epoch=excluded.epoch,cut_id=excluded.cut_id,version=excluded.version,read_at_ms=excluded.read_at_ms,stale=excluded.stale,integrity_failed=excluded.integrity_failed",
        params![id, next.holder, next.epoch, next.cut_id, next.version, next.read_at_ms, next.stale, next.integrity_failed],
    )?;
    Ok(conflict)
}

impl RegistryStore {
    /// The agent pass snapshots its subject set before any remote call.
    pub fn live_claim_agents(&self) -> Result<Vec<String>, RegistryError> {
        self.read(|conn| {
            let mut stmt = conn.prepare(
                "SELECT agent_id FROM agent WHERE terminal_reason IS NULL ORDER BY agent_id",
            )?;
            let rows = stmt.query_map([], |row| row.get(0))?.collect();
            rows
        })
    }

    /// All replies in a page commit together. Returns the number of conflicting
    /// self-author replies; no SQLite transaction spans a remote call.
    pub fn cache_agent_claims(
        &self,
        observations: &[(String, ClaimObservation)],
        now: i64,
    ) -> Result<usize, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let mut conflicts = 0;
                for (id, observation) in observations {
                    conflicts += usize::from(apply(tx, id, observation, now)?);
                }
                Ok(conflicts)
            })
            .map_err(RegistryError::Store)
    }

    pub fn cached_agent_claim(&self, id: &str) -> Result<Option<CachedClaim>, RegistryError> {
        self.read(|conn| load_claim(conn, id))
    }

    pub fn persisted_self_author(&self) -> Result<Option<String>, RegistryError> {
        self.read(self_author)
    }

    /// A read snapshot joins the cache, identity and display name. Nulls are
    /// retained on the wire so unknown values never masquerade as empty text.
    pub fn agent_where(&self, id: &str) -> Result<AgentWhere, agent::AgentMutationError> {
        self.read(|conn| Ok(agent_where_in(conn, id)))
            .map_err(|e| {
                refusal(
                    "storage_error",
                    Some(serde_json::json!({"message":e.to_string()})),
                )
            })?
    }
}

pub(crate) fn agent_where_in(
    conn: &Connection,
    id: &str,
) -> Result<AgentWhere, agent::AgentMutationError> {
    let row = agent::load_row(conn, id)?.ok_or_else(|| refusal("unknown_agent", None))?;
    if let Some(gone) = row.gone() {
        return Err(refusal("gone", Some(gone)));
    }
    let claim = load_claim(conn, id)?;
    // Claims live only in the shared log. While sharing has never
    // been turned on, no other Mac can hold this agent, so having no
    // cached claim is a verified "unclaimed", not an unknown. Once
    // sharing starts, only verified engram reads count.
    let sharing_off = conn
        .query_row(
            "SELECT state='disabled' FROM identity_log_state WHERE id=1",
            [],
            |r| r.get::<_, bool>(0),
        )
        .optional()?
        .unwrap_or(true);
    let known = claim.is_some() || sharing_off;
    let read_at_ms = claim.as_ref().map(|c| c.read_at_ms);
    let claim = claim.unwrap_or_else(|| unclaimed(0, !sharing_off, false));
    if claim.integrity_failed {
        return Err(refusal("claim_integrity_failed", None));
    }
    let own = self_author(conn)?;
    let status = if claim.epoch == 0 {
        "unclaimed"
    } else if claim.holder.is_some() && claim.holder == own {
        "held_here"
    } else {
        "held_elsewhere"
    };
    let (moving, move_errors, move_warnings, move_diagnostics) =
        crate::move_store::where_moves(conn, id)?;
    Ok(AgentWhere {
        known,
        status,
        holder_name: if own.is_some() {
            holder_name(conn, claim.holder.as_deref())?
        } else {
            None
        },
        holder: claim.holder,
        epoch: claim.epoch,
        cut_id: claim.cut_id,
        read_at_ms,
        stale: claim.stale,
        moving,
        move_errors,
        move_warnings,
        move_diagnostics,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SessionRegistration;
    use std::sync::atomic::{AtomicU64, Ordering};
    const ID: &str = "agent_0123456789abcdef";
    const SELF: &str = "01010101010101010101010101010101";
    const OTHER: &str = "02020202020202020202020202020202";
    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        store: RegistryStore,
        descriptor: cortexkit_store::StorageDescriptor,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "claim-cache-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = cortexkit_store::StorageDescriptor {
                module_id: "entorhinal-test".into(),
                storage_namespace: "claims".into(),
                isolation: cortexkit_store::Isolation::Module,
                backend: cortexkit_store::StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let store = RegistryStore::open(&descriptor).unwrap();
            store.db.with_conn_fenced(|tx| {
                tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Ada','test','assistant',1,1)", [ID])?;
                tx.execute("INSERT INTO registry_journal(op,payload_json) VALUES(?1,'{}')", [agent::IDENTITY_MARKER_OP])?;
                Ok(())
            }).unwrap();
            Self {
                store,
                descriptor,
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
            }
        }
        fn observe(&self, observation: ClaimObservation) -> usize {
            self.store
                .cache_agent_claims(&[(ID.into(), observation)], 9)
                .unwrap()
        }
        fn row(&self) -> CachedClaim {
            self.store.cached_agent_claim(ID).unwrap().unwrap()
        }
        fn seed(&self, mark: bool) {
            self.observe(present(5, SELF, false));
            if mark {
                self.observe(ClaimObservation::IntegrityFailed);
            }
        }
        fn register(&self) -> Result<crate::SessionBinding, agent::AgentMutationError> {
            self.store.register_session(&registration())
        }
    }
    fn present(version: i64, holder: &str, stale: bool) -> ClaimObservation {
        ClaimObservation::Present {
            self_author: SELF.into(),
            claim: CachedClaim {
                holder: Some(holder.into()),
                epoch: version,
                cut_id: Some("cut".into()),
                version,
                read_at_ms: 0,
                stale,
                integrity_failed: false,
            },
        }
    }
    fn registration() -> SessionRegistration<'static> {
        SessionRegistration {
            agent_id: ID,
            ref_kind: "test",
            session_ref: "ref",
            harness: "pi",
            session_id: "session",
            project_root: "/project",
            registered_by: "host",
            now_ms: 10,
        }
    }
    fn absent(stale: bool) -> ClaimObservation {
        ClaimObservation::Absent {
            stale,
            self_author: SELF.into(),
        }
    }

    #[test]
    fn integrity_present_keeps_body_and_sets_mark() {
        let f = Fixture::new();
        f.seed(false);
        f.observe(ClaimObservation::IntegrityFailed);
        let row = f.row();
        assert_eq!(
            (
                row.version,
                row.epoch,
                row.holder.as_deref(),
                row.read_at_ms,
                row.integrity_failed
            ),
            (5, 5, Some(SELF), 9, true)
        );
    }
    #[test]
    fn integrity_none_creates_marked_unclaimed_version_zero() {
        let f = Fixture::new();
        f.observe(ClaimObservation::IntegrityFailed);
        assert_eq!(f.row(), unclaimed(9, true, true));
    }
    #[test]
    fn codec_refusal_sets_mark_and_learns_verified_reply_identity() {
        let f = Fixture::new();
        f.observe(ClaimObservation::CodecFailed {
            self_author: SELF.into(),
        });
        assert_eq!(f.row(), unclaimed(9, true, true));
        assert_eq!(
            f.store.persisted_self_author().unwrap().as_deref(),
            Some(SELF)
        );
        f.seed(false);
        f.observe(ClaimObservation::CodecFailed {
            self_author: SELF.into(),
        });
        assert_eq!(f.row().version, 5);
        assert!(f.row().integrity_failed);
    }
    #[test]
    fn verified_newer_equal_or_first_replaces_and_clears_mark() {
        let f = Fixture::new();
        f.observe(present(5, OTHER, true));
        assert_eq!(f.row().holder.as_deref(), Some(OTHER));
        f.observe(ClaimObservation::IntegrityFailed);
        f.observe(present(5, SELF, false));
        assert!(!f.row().integrity_failed);
        assert!(!f.row().stale);
        assert_eq!(f.row().holder.as_deref(), Some(SELF));
        f.observe(present(6, OTHER, true));
        assert_eq!(f.row().version, 6);
        assert!(f.row().stale);
    }
    #[test]
    fn verified_older_keeps_body_and_mark_and_becomes_stale() {
        let f = Fixture::new();
        f.seed(true);
        f.observe(present(4, OTHER, false));
        let row = f.row();
        assert_eq!(row.version, 5);
        assert_eq!(row.holder.as_deref(), Some(SELF));
        assert!(row.integrity_failed && row.stale);
        assert_eq!(row.read_at_ms, 9);
    }
    #[test]
    fn stale_absent_preserves_positive_version_and_mark() {
        let f = Fixture::new();
        f.seed(true);
        f.observe(absent(true));
        let row = f.row();
        assert_eq!(row.version, 5);
        assert_eq!(row.holder.as_deref(), Some(SELF));
        assert!(row.integrity_failed && row.stale);
    }
    #[test]
    fn stale_absent_none_or_zero_stores_zero_and_keeps_mark() {
        let f = Fixture::new();
        f.observe(absent(true));
        assert_eq!(f.row(), unclaimed(9, true, false));
        f.observe(ClaimObservation::IntegrityFailed);
        f.observe(absent(true));
        assert_eq!(f.row(), unclaimed(9, true, true));
    }
    #[test]
    fn confirmed_absent_resets_version_and_clears_mark() {
        let f = Fixture::new();
        f.seed(true);
        f.observe(absent(false));
        assert_eq!(f.row(), unclaimed(9, false, false));
    }
    #[test]
    fn verified_epoch_zero_is_unclaimed_with_version_zero() {
        let f = Fixture::new();
        let mut observation = present(7, OTHER, false);
        if let ClaimObservation::Present { claim, .. } = &mut observation {
            claim.epoch = 0;
        }
        f.observe(observation);
        assert_eq!(f.row(), unclaimed(9, false, false));
        f.register().unwrap();
    }
    #[test]
    fn transport_failure_keeps_unknown_or_marks_cached_body_stale() {
        let f = Fixture::new();
        f.observe(ClaimObservation::Unavailable);
        assert!(f.store.cached_agent_claim(ID).unwrap().is_none());
        f.seed(true);
        f.observe(ClaimObservation::Unavailable);
        assert_eq!(f.row().version, 5);
        assert!(f.row().stale && f.row().integrity_failed);
    }
    #[test]
    fn self_author_conflict_never_overwrites_identity_and_cache_never_journals() {
        let f = Fixture::new();
        let generation = f.store.generation().unwrap();
        assert_eq!(f.observe(absent(false)), 0);
        assert_eq!(
            f.observe(ClaimObservation::Absent {
                stale: false,
                self_author: OTHER.into()
            }),
            1
        );
        assert_eq!(
            f.store.persisted_self_author().unwrap().as_deref(),
            Some(SELF)
        );
        assert_eq!(f.store.generation().unwrap(), generation);
        assert_eq!(f.store.live_claim_agents().unwrap(), vec![ID]);
    }
    #[test]
    fn where_wire_nulls_known_status_integrity_and_restart() {
        let f = Fixture::new();
        // With sharing never turned on, no other Mac can hold the agent, so an
        // absent claim is a verified "unclaimed". Core's head registration
        // accepts exactly this shape.
        assert_eq!(serde_json::to_string(&f.store.agent_where(ID).unwrap()).unwrap(),"{\"known\":true,\"status\":\"unclaimed\",\"holder\":null,\"holder_name\":null,\"epoch\":0,\"cut_id\":null,\"read_at_ms\":null,\"stale\":false}");
        // In every other sharing state only a verified engram read counts.
        for state in ["enabling", "joining", "enabled"] {
            f.store
                .db
                .with_conn_fenced(|tx| {
                    tx.execute("UPDATE identity_log_state SET state=?1 WHERE id=1", [state])?;
                    Ok(())
                })
                .unwrap();
            assert_eq!(serde_json::to_string(&f.store.agent_where(ID).unwrap()).unwrap(),"{\"known\":false,\"status\":\"unclaimed\",\"holder\":null,\"holder_name\":null,\"epoch\":0,\"cut_id\":null,\"read_at_ms\":null,\"stale\":true}", "{state}");
        }
        f.observe(present(1, OTHER, false));
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("INSERT INTO member_name VALUES(?1,'Studio')", [OTHER])?;
                Ok(())
            })
            .unwrap();
        let row = f.store.agent_where(ID).unwrap();
        assert_eq!(row.status, "held_elsewhere");
        assert_eq!(row.holder_name.as_deref(), Some("Studio"));
        f.observe(present(2, SELF, false));
        let descriptor = f.descriptor.clone();
        drop(f.store);
        let store = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(store.agent_where(ID).unwrap().status, "held_here");
        store
            .cache_agent_claims(&[(ID.into(), ClaimObservation::IntegrityFailed)], 11)
            .unwrap();
        assert_eq!(
            store.agent_where(ID).unwrap_err().code,
            "claim_integrity_failed"
        );
        drop(store);
    }
    #[test]
    fn register_claim_admission_checks_integrity_before_holder_and_binding() {
        let f = Fixture::new();
        f.register().unwrap();
        f.observe(present(1, SELF, false));
        f.register().unwrap();
        f.observe(present(2, OTHER, false));
        let error = f.register().unwrap_err();
        assert_eq!(error.code, "held_elsewhere");
        assert_eq!(
            error.detail,
            Some(serde_json::json!({"holder":OTHER,"holder_name":null,"epoch":2}))
        );
        f.observe(ClaimObservation::IntegrityFailed);
        assert_eq!(f.register().unwrap_err().code, "claim_integrity_failed");
        f.observe(absent(false));
        f.register().unwrap();
        f.observe(present(0, OTHER, false));
        f.register().unwrap();
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM self_identity", [])?;
                Ok(())
            })
            .unwrap();
        f.observe(ClaimObservation::IntegrityFailed);
        assert_eq!(f.register().unwrap_err().code, "claim_integrity_failed");
    }
    #[test]
    fn holder_without_self_identity_is_elsewhere_and_never_assumed_here() {
        let f = Fixture::new();
        f.observe(present(1, SELF, false));
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM self_identity", [])?;
                Ok(())
            })
            .unwrap();
        assert_eq!(f.store.agent_where(ID).unwrap().status, "held_elsewhere");
        assert_eq!(f.register().unwrap_err().code, "held_elsewhere");
        assert_eq!(
            f.store
                .agent_where("agent_ffffffffffffffff")
                .unwrap_err()
                .code,
            "unknown_agent"
        );
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE agent SET terminal_reason='retired',terminal_at_ms=2 WHERE agent_id=?1",
                    [ID],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(f.store.agent_where(ID).unwrap_err().code, "gone");
        assert_eq!(f.register().unwrap_err().code, "gone");
        assert!(f.store.live_claim_agents().unwrap().is_empty());
    }
    #[test]
    fn registration_snapshot_excludes_a_cache_write_started_after_claim_read() {
        let f = Fixture::new();
        f.observe(absent(false));
        let writer = f.store.clone();
        let (start, started) = std::sync::mpsc::channel();
        let (done, completed) = std::sync::mpsc::channel();

        // Hold the same write connection from before starting the competing
        // refresh until registration commits; its claim snapshot stays absent.
        let thread = f
            .store
            .db
            .with_conn_fenced(|tx| {
                let thread = std::thread::spawn(move || {
                    start.send(()).unwrap();
                    writer
                        .cache_agent_claims(&[(ID.into(), present(1, OTHER, false))], 20)
                        .unwrap();
                    let _ = done.send(());
                });
                apply(tx, ID, &absent(false), 9)?;
                REGISTRATION_HOOK.with(|hook| {
                    *hook.borrow_mut() = Some(Box::new(move || {
                        started.recv().unwrap();
                        assert!(completed.try_recv().is_err());
                    }));
                });
                assert!(RegistryStore::register_session_in(tx, &registration()).is_ok());
                Ok(thread)
            })
            .unwrap();
        thread.join().unwrap();
        assert_eq!(f.register().unwrap_err().code, "held_elsewhere");
        assert_eq!(
            f.store
                .session_binding_for("pi", "session")
                .unwrap()
                .unwrap()
                .agent_id,
            ID
        );
    }
}

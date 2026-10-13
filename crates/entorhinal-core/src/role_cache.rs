//! Machine-local role observations and operator-approved CAS attempts. These
//! fenced transactions never append to the registry journal or identity log.

use crate::{
    agent::AgentMutationError,
    claim_cache::{self, CachedClaim, ClaimObservation},
    RegistryError, RegistryStore,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RoleRead {
    pub role: String,
    pub known: bool,
    pub held_here: bool,
    pub epoch: i64,
    pub holder: Option<String>,
    pub holder_name: Option<String>,
    pub source: Option<String>,
    pub stale: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct RoleTakeInput {
    pub role: String,
    pub expected_epoch: i64,
    pub request_id: String,
    pub reason: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RoleTakeAttempt {
    pub input: RoleTakeInput,
    pub expected_version: i64,
    pub body: Vec<u8>,
    pub epoch: Option<i64>,
}

pub fn valid_role(role: &str) -> bool {
    (1..=64).contains(&role.len())
        && role
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}

fn refusal(code: &str) -> AgentMutationError {
    AgentMutationError {
        code: code.into(),
        message: code.into(),
        detail: None,
    }
}

fn ensure_role(tx: &Transaction<'_>, role: &str) -> Result<(), AgentMutationError> {
    if !valid_role(role) {
        return Err(refusal("invalid_request"));
    }
    let present = tx
        .query_row("SELECT 1 FROM role_cache WHERE role=?1", [role], |_| Ok(()))
        .optional()?
        .is_some();
    if !present {
        let count: i64 = tx.query_row("SELECT COUNT(*) FROM role_cache", [], |r| r.get(0))?;
        if count >= 256 {
            return Err(refusal("role_set_full"));
        }
        tx.execute(
            "INSERT INTO role_cache(role,epoch,stale,version,integrity_failed) VALUES(?1,0,1,0,0)",
            [role],
        )?;
    }
    Ok(())
}

fn cached(conn: &Connection, role: &str) -> rusqlite::Result<Option<CachedClaim>> {
    conn.query_row("SELECT holder,epoch,version,read_at_ms,stale,integrity_failed FROM role_cache WHERE role=?1 AND read_at_ms IS NOT NULL", [role], |r| {
        Ok(CachedClaim { holder: r.get(0)?, epoch: r.get(1)?, cut_id: None, version: r.get(2)?, read_at_ms: r.get(3)?, stale: r.get(4)?, integrity_failed: r.get(5)? })
    }).optional()
}

fn write_cache(
    tx: &Transaction<'_>,
    role: &str,
    row: &CachedClaim,
    source: Option<&str>,
) -> rusqlite::Result<()> {
    tx.execute("INSERT INTO role_cache(role,holder,epoch,source,stale,version,read_at_ms,integrity_failed) VALUES(?1,?2,?3,?4,?5,?6,?7,?8)
        ON CONFLICT(role) DO UPDATE SET holder=excluded.holder,epoch=excluded.epoch,source=excluded.source,stale=excluded.stale,version=excluded.version,read_at_ms=excluded.read_at_ms,integrity_failed=excluded.integrity_failed",
        params![role,row.holder,row.epoch,source,row.stale,row.version,row.read_at_ms,row.integrity_failed])?;
    Ok(())
}

fn load_attempt(
    conn: &Connection,
    input: &RoleTakeInput,
) -> Result<Option<RoleTakeAttempt>, AgentMutationError> {
    let attempt = conn.query_row("SELECT role,expected_epoch,reason,expected_version,body,epoch FROM role_take_request WHERE request_id=?1", [&input.request_id], |r| {
        Ok(RoleTakeAttempt { input: RoleTakeInput { request_id: input.request_id.clone(), role: r.get(0)?, expected_epoch: r.get(1)?, reason: r.get(2)? }, expected_version: r.get(3)?, body: r.get(4)?, epoch: r.get(5)? })
    }).optional()?;
    if attempt.as_ref().is_some_and(|a| a.input != *input) {
        return Err(refusal("id_reused"));
    }
    Ok(attempt)
}

fn read_role(conn: &Connection, role: &str) -> Result<RoleRead, AgentMutationError> {
    let row = cached(conn, role)?;
    if row.as_ref().is_some_and(|r| r.integrity_failed) {
        return Err(refusal("claim_integrity_failed"));
    }
    let own = claim_cache::self_author(conn)?;
    let source = conn.query_row("SELECT source FROM role_cache WHERE role=?1", [role], |r| {
        r.get(0)
    })?;
    Ok(RoleRead {
        role: role.into(),
        known: row.is_some(),
        held_here: row
            .as_ref()
            .is_some_and(|r| r.epoch >= 1 && r.holder.is_some() && r.holder == own),
        epoch: row.as_ref().map_or(0, |r| r.epoch),
        holder_name: claim_cache::holder_name(
            conn,
            row.as_ref().and_then(|r| r.holder.as_deref()),
        )?,
        holder: row.as_ref().and_then(|r| r.holder.clone()),
        source,
        stale: row.as_ref().is_none_or(|r| r.stale),
    })
}

impl RegistryStore {
    /// Installed stores retain their observations, but no prior read proves
    /// freshness in this process. A verified refresh or landed take can replace
    /// the stale observation.
    pub fn stale_roles(&self) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                tx.execute("UPDATE role_cache SET stale=1", [])?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }
    /// A first read enrolls a role in the bounded, persisted refresh set. Reads
    /// of an enrolled role use the read connection, not the fenced writer.
    pub fn role_read(&self, role: &str) -> Result<RoleRead, AgentMutationError> {
        if !valid_role(role) {
            return Err(refusal("invalid_request"));
        }
        let enrolled = self.read(|conn| {
            Ok(conn
                .query_row("SELECT 1 FROM role_cache WHERE role=?1", [role], |_| Ok(()))
                .optional()?
                .is_some())
        })?;
        if enrolled {
            return self.read(|conn| Ok(read_role(conn, role)))?;
        }
        self.db
            .with_conn_fenced(|tx| {
                Ok((|| {
                    ensure_role(tx, role)?;
                    read_role(tx, role)
                })())
            })
            .map_err(RegistryError::Store)?
    }

    /// Snapshot the role set before starting remote reads.
    pub fn role_subjects(&self) -> Result<Vec<String>, RegistryError> {
        self.read(|conn| {
            let mut stmt = conn.prepare("SELECT role FROM role_cache ORDER BY role")?;
            let rows = stmt.query_map([], |r| r.get(0))?.collect();
            rows
        })
    }

    /// Apply `claim_cache::decide`: retain newer bodies and integrity marks on
    /// stale reads, and clear marks only on verified replacements or fresh
    /// absence. An enrolled role with NULL read_at_ms has no prior observation.
    /// A retained body also retains its cloud/local source.
    pub fn cache_roles(
        &self,
        observations: &[(String, ClaimObservation)],
        now: i64,
    ) -> Result<usize, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let mut conflicts = 0;
                for (role, observation) in observations {
                    let old = cached(tx, role)?;
                    let source: Option<String> = tx
                        .query_row("SELECT source FROM role_cache WHERE role=?1", [role], |r| {
                            r.get(0)
                        })
                        .optional()?
                        .flatten();
                    let replaces = match observation {
                        ClaimObservation::Present { claim, .. } => {
                            old.as_ref().is_none_or(|r| claim.version >= r.version)
                        }
                        ClaimObservation::Absent { stale, .. } => {
                            !stale || old.as_ref().is_none_or(|r| r.version == 0)
                        }
                        _ => false,
                    };
                    match observation {
                        ClaimObservation::Present { self_author, .. }
                        | ClaimObservation::Absent { self_author, .. }
                        | ClaimObservation::CodecFailed { self_author } => {
                            conflicts +=
                                usize::from(!claim_cache::learn_self_author(tx, self_author, now)?);
                        }
                        _ => {}
                    }
                    if let Some(row) = claim_cache::decide(old, observation, now) {
                        write_cache(
                            tx,
                            role,
                            &row,
                            if replaces {
                                Some("cloud")
                            } else {
                                source.as_deref()
                            },
                        )?;
                    }
                }
                Ok(conflicts)
            })
            .map_err(RegistryError::Store)
    }

    /// Read role_take_request before asking for operator confirmation or calling
    /// engram. A reused id with different role, epoch or reason is refused.
    pub fn role_take_attempt(
        &self,
        input: &RoleTakeInput,
    ) -> Result<Option<RoleTakeAttempt>, AgentMutationError> {
        self.read(|conn| Ok(load_attempt(conn, input)))?
    }

    /// Persist the body and expected version after the operator approved a take.
    /// Reusing a request id requires the same role, expected epoch and reason.
    /// A different device author refuses before saving or sending the record.
    pub fn prepare_role_take(
        &self,
        attempt: &RoleTakeAttempt,
        self_author: &str,
        now: i64,
    ) -> Result<RoleTakeAttempt, AgentMutationError> {
        self.db.with_conn_fenced(|tx| {
            match load_attempt(tx, &attempt.input) {
                Ok(Some(existing)) => return Ok(Ok(existing)),
                Err(error) => return Ok(Err(error)),
                Ok(None) => {}
            }
            if claim_cache::self_author(tx)?.is_some_and(|known| known != self_author) {
                return Ok(Err(refusal("self_author_conflict")));
            }
            if let Err(error) = ensure_role(tx, &attempt.input.role) {
                return Ok(Err(error));
            }
            claim_cache::learn_self_author(tx, self_author, now)?;
            tx.execute("INSERT INTO role_take_request(request_id,role,expected_epoch,reason,expected_version,body,created_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
                params![attempt.input.request_id,attempt.input.role,attempt.input.expected_epoch,attempt.input.reason,attempt.expected_version,attempt.body,now])?;
            Ok(Ok(attempt.clone()))
        }).map_err(RegistryError::Store)?
    }

    /// Once engram confirms the approved record was written, commit the returned
    /// epoch in role_take_request and its local role_cache observation together.
    /// Do not call this for an unresolved or superseded write: its ledger epoch
    /// must stay NULL so a retry can check the same approved attempt again.
    pub fn finish_role_take(
        &self,
        attempt: &RoleTakeAttempt,
        holder: &str,
        epoch: i64,
        version: i64,
        now: i64,
    ) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE role_take_request SET epoch=?2 WHERE request_id=?1",
                    params![attempt.input.request_id, epoch],
                )?;
                write_cache(
                    tx,
                    &attempt.input.role,
                    &CachedClaim {
                        holder: Some(holder.into()),
                        epoch,
                        cut_id: None,
                        version,
                        read_at_ms: now,
                        stale: false,
                        integrity_failed: false,
                    },
                    Some("local"),
                )
            })
            .map_err(RegistryError::Store)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};
    static NEXT: AtomicU64 = AtomicU64::new(0);
    const SELF: &str = "01010101010101010101010101010101";
    const OTHER: &str = "02020202020202020202020202020202";
    struct Fixture {
        store: RegistryStore,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }
    impl Fixture {
        fn new() -> Self {
            let root = std::env::temp_dir().join(format!(
                "role-cache-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = cortexkit_store::StorageDescriptor {
                module_id: "entorhinal-test".into(),
                storage_namespace: "roles".into(),
                isolation: cortexkit_store::Isolation::Module,
                backend: cortexkit_store::StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            Self {
                store: RegistryStore::open(&descriptor).unwrap(),
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
            }
        }
        fn observe(&self, observation: ClaimObservation) {
            self.store
                .cache_roles(&[("x".into(), observation)], 99)
                .unwrap();
        }
        fn row(&self) -> CachedClaim {
            self.store.read(|conn| cached(conn, "x")).unwrap().unwrap()
        }
        fn seed(&self) {
            self.store.role_read("x").unwrap();
            self.observe(present(5, OTHER));
            self.observe(ClaimObservation::IntegrityFailed);
        }
    }
    fn present(version: i64, holder: &str) -> ClaimObservation {
        ClaimObservation::Present {
            claim: CachedClaim {
                holder: Some(holder.into()),
                epoch: version,
                cut_id: None,
                version,
                read_at_ms: 0,
                stale: false,
                integrity_failed: false,
            },
            self_author: SELF.into(),
        }
    }
    fn absent(stale: bool) -> ClaimObservation {
        ClaimObservation::Absent {
            stale,
            self_author: SELF.into(),
        }
    }

    #[test]
    fn role_read_derives_identity_names_and_keeps_nulls_and_role_set_bound() {
        let f = Fixture::new();
        assert_eq!(serde_json::to_string(&f.store.role_read("x").unwrap()).unwrap(), "{\"role\":\"x\",\"known\":false,\"held_here\":false,\"epoch\":0,\"holder\":null,\"holder_name\":null,\"source\":null,\"stale\":true}");
        f.store
            .db
            .with_conn_fenced(|tx| {
                write_cache(
                    tx,
                    "x",
                    &CachedClaim {
                        holder: Some(SELF.into()),
                        epoch: 1,
                        version: 1,
                        cut_id: None,
                        read_at_ms: 1,
                        stale: false,
                        integrity_failed: false,
                    },
                    Some("cloud"),
                )?;
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,'Studio')",
                    [SELF],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(!f.store.role_read("x").unwrap().held_here);
        f.store
            .cache_agent_claims(&[("unused".into(), absent(false))], 1)
            .unwrap();
        let read = f.store.role_read("x").unwrap();
        assert!(read.known && read.held_here);
        assert_eq!(read.holder_name.as_deref(), Some("Studio"));
        f.observe(present(2, OTHER));
        let read = f.store.role_read("x").unwrap();
        assert!(read.known && !read.held_here && read.epoch >= 1 && read.holder.is_some());
        f.store.stale_roles().unwrap();
        assert!(f.store.role_read("x").unwrap().stale);
        assert_eq!(
            f.store.role_read("x").unwrap().source.as_deref(),
            Some("cloud")
        );
        for i in 0..255 {
            f.store.role_read(&format!("role_{i}")).unwrap();
        }
        assert_eq!(f.store.role_subjects().unwrap().len(), 256);
        assert_eq!(
            f.store.role_read("beyond").unwrap_err().code,
            "role_set_full"
        );
        f.store.role_read("x").unwrap();
        for invalid in ["", "A", "x-y", "x:y", &"x".repeat(65)] {
            assert_eq!(
                f.store.role_read(invalid).unwrap_err().code,
                "invalid_request"
            );
        }
        assert_eq!(f.store.generation().unwrap(), 0);
    }

    #[test]
    fn role_integrity_present_keeps_body_and_sets_mark() {
        let f = Fixture::new();
        f.seed();
        f.observe(ClaimObservation::IntegrityFailed);
        assert_eq!(f.row().holder.as_deref(), Some(OTHER));
        assert!(f.row().integrity_failed);
        assert_eq!(
            f.store.role_read("x").unwrap_err().code,
            "claim_integrity_failed"
        );
    }
    #[test]
    fn role_integrity_none_creates_mark_with_null_read_at_treated_as_none() {
        let f = Fixture::new();
        f.store.role_read("x").unwrap();
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE role_cache SET epoch=8,version=8,holder=?1 WHERE role='x'",
                    [OTHER],
                )?;
                Ok(())
            })
            .unwrap();
        f.observe(ClaimObservation::IntegrityFailed);
        let row = f.row();
        assert_eq!((row.epoch, row.version, row.holder), (0, 0, None));
        assert!(row.stale && row.integrity_failed);
    }
    #[test]
    fn role_codec_failure_keeps_body_and_sets_mark() {
        let f = Fixture::new();
        f.seed();
        f.observe(ClaimObservation::CodecFailed {
            self_author: SELF.into(),
        });
        assert_eq!(f.row().version, 5);
        assert!(f.row().integrity_failed);
    }
    #[test]
    fn role_verified_replacement_clears_mark_and_sets_cloud() {
        let f = Fixture::new();
        f.seed();
        f.observe(present(5, SELF));
        assert!(!f.row().integrity_failed);
        assert!(!f.row().stale);
        assert_eq!(f.row().holder.as_deref(), Some(SELF));
        assert_eq!(
            f.store.role_read("x").unwrap().source.as_deref(),
            Some("cloud")
        );
    }
    #[test]
    fn role_older_version_retains_body_mark_and_source() {
        let f = Fixture::new();
        f.seed();
        f.observe(present(4, SELF));
        assert_eq!(f.row().version, 5);
        assert!(f.row().stale && f.row().integrity_failed);
    }
    #[test]
    fn role_stale_absent_retains_newer_body_and_mark() {
        let f = Fixture::new();
        f.seed();
        f.observe(absent(true));
        assert_eq!(f.row().version, 5);
        assert!(f.row().stale && f.row().integrity_failed);
    }
    #[test]
    fn role_stale_absent_none_or_zero_is_unclaimed_and_keeps_mark() {
        let f = Fixture::new();
        f.store.role_read("x").unwrap();
        f.observe(absent(true));
        assert!(f.row().stale);
        assert_eq!(f.row().epoch, 0);
        f.observe(ClaimObservation::IntegrityFailed);
        f.observe(absent(true));
        assert!(f.row().integrity_failed);
    }
    #[test]
    fn role_fresh_absent_clears_mark() {
        let f = Fixture::new();
        f.seed();
        f.observe(absent(false));
        assert_eq!(f.row().epoch, 0);
        assert!(!f.row().integrity_failed && !f.row().stale);
    }
    #[test]
    fn role_unavailable_never_makes_unread_role_known() {
        let f = Fixture::new();
        f.store.role_read("x").unwrap();
        f.observe(ClaimObservation::Unavailable);
        assert!(!f.store.role_read("x").unwrap().known);
        f.observe(present(1, SELF));
        f.observe(ClaimObservation::Unavailable);
        assert!(f.row().stale);
    }
    #[test]
    fn role_ledger_checks_input_conflicts_and_commits_result_without_journal() {
        let f = Fixture::new();
        let input = RoleTakeInput {
            role: "x".into(),
            expected_epoch: 0,
            request_id: "01".repeat(16),
            reason: "test".into(),
        };
        assert_eq!(f.store.role_take_attempt(&input).unwrap(), None);
        let attempt = RoleTakeAttempt {
            input: input.clone(),
            expected_version: 0,
            body: b"approved".to_vec(),
            epoch: None,
        };
        f.store.db.with_conn_fenced(|tx| tx.execute_batch("CREATE TRIGGER fail_role_ledger BEFORE INSERT ON role_take_request BEGIN SELECT RAISE(ABORT,'injected ledger failure'); END")).unwrap();
        assert!(f.store.prepare_role_take(&attempt, SELF, 1).is_err());
        assert!(f.store.persisted_self_author().unwrap().is_none());
        assert!(f.store.role_subjects().unwrap().is_empty());
        assert!(f.store.role_take_attempt(&input).unwrap().is_none());
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute_batch("DROP TRIGGER fail_role_ledger"))
            .unwrap();
        assert_eq!(
            f.store.prepare_role_take(&attempt, SELF, 1).unwrap(),
            attempt
        );
        let mut other = input.clone();
        other.reason = "different".into();
        assert_eq!(
            f.store.role_take_attempt(&other).unwrap_err().code,
            "id_reused"
        );
        let mut conflicting = attempt.clone();
        conflicting.input.request_id = "02".repeat(16);
        assert_eq!(
            f.store
                .prepare_role_take(&conflicting, OTHER, 2)
                .unwrap_err()
                .code,
            "self_author_conflict"
        );
        assert!(f
            .store
            .role_take_attempt(&conflicting.input)
            .unwrap()
            .is_none());
        assert_eq!(
            f.store.prepare_role_take(&attempt, SELF, 2).unwrap(),
            attempt
        );
        f.store.db.with_conn_fenced(|tx| {
            tx.execute_batch("CREATE TRIGGER fail_role_cache BEFORE UPDATE ON role_cache BEGIN SELECT RAISE(ABORT,'injected cache failure'); END")
        }).unwrap();
        assert!(f.store.finish_role_take(&attempt, SELF, 1, 1, 3).is_err());
        assert_eq!(
            f.store.role_take_attempt(&input).unwrap().unwrap().epoch,
            None
        );
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute_batch("DROP TRIGGER fail_role_cache"))
            .unwrap();
        f.store.finish_role_take(&attempt, SELF, 1, 1, 3).unwrap();
        assert_eq!(
            f.store.role_take_attempt(&input).unwrap().unwrap().epoch,
            Some(1)
        );
        let read = f.store.role_read("x").unwrap();
        assert!(read.held_here);
        assert_eq!(read.source.as_deref(), Some("local"));
        f.observe(present(1, SELF));
        assert_eq!(
            f.store.role_read("x").unwrap().source.as_deref(),
            Some("cloud")
        );
        assert_eq!(f.store.generation().unwrap(), 0);
        assert!(f.store.verify().unwrap().ok);
        f.store.rebuild().unwrap();
        assert!(f.store.verify().unwrap().ok);
        assert_eq!(
            f.store.role_take_attempt(&input).unwrap().unwrap().epoch,
            Some(1)
        );
    }
}

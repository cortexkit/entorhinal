//! Machine-local role reads and lease generation high-water marks.

use crate::{
    agent::AgentMutationError,
    claim_cache::{self, CachedClaim},
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

/// The subject and version to stamp on work that requires a role fence.
/// Callers use the subject verbatim.
#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct RoleFence {
    pub role: String,
    pub subject: String,
    pub version: i64,
    pub epoch: i64,
    pub held_here: bool,
    pub holder: Option<String>,
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
    pub fn role_gen_high_water(&self, role: &str) -> Result<i64, RegistryError> {
        self.read(|conn| gen_high_water(conn, role))
    }

    pub fn observe_role_gen(&self, role: &str, gen: i64) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| raise_gen(tx, role, gen))
            .map_err(RegistryError::Store)
    }

    pub fn grant_local_role_gen(
        &self,
        role: &str,
        watch_is_open: impl FnOnce() -> bool,
    ) -> Result<Option<i64>, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let high_water = gen_high_water(tx, role)?;
                // A blocking refresh can outlive its watch. Check after the
                // read so it cannot mint from a hub generation recorded after
                // the watch closed.
                if !watch_is_open() {
                    return Ok(None);
                }
                let gen = high_water
                    .checked_add(1)
                    .ok_or(rusqlite::Error::InvalidQuery)?;
                raise_gen(tx, role, gen)?;
                Ok(Some(gen))
            })
            .map_err(RegistryError::Store)
    }

    /// A first read enrolls a role in the bounded local cache. Reads of an
    /// enrolled role use the read connection, not the fenced writer.
    pub fn role_read(&self, role: &str) -> Result<RoleRead, AgentMutationError> {
        if !valid_role(role) {
            return Err(refusal("invalid_request"));
        }
        if self.is_foreign() {
            return Ok(RoleRead {
                role: role.into(),
                known: false,
                held_here: false,
                epoch: 0,
                holder: None,
                holder_name: None,
                source: None,
                stale: true,
            });
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

    /// The fence for the same cache row that supplies `role_read`.
    pub fn role_fence(&self, role: &str) -> Result<RoleFence, AgentMutationError> {
        let read = self.role_read(role)?;
        let version = self.read(|conn| {
            conn.query_row(
                "SELECT version FROM role_cache WHERE role=?1 AND read_at_ms IS NOT NULL",
                [role],
                |r| r.get::<_, i64>(0),
            )
            .optional()
        })?;
        Ok(RoleFence {
            subject: format!("role:{role}"),
            version: version.unwrap_or(0),
            epoch: read.epoch,
            held_here: read.held_here,
            holder: read.holder,
            stale: read.stale,
            role: read.role,
        })
    }

    /// Roles to include in the legacy Switch quiet record. Remove this reader
    /// with the Switch; served role operations do not enumerate the cache.
    pub fn legacy_switch_quiet_roles(
        &self,
    ) -> Result<std::collections::BTreeSet<String>, RegistryError> {
        self.read(|conn| {
            let mut stmt = conn.prepare("SELECT role FROM role_cache ORDER BY role")?;
            let rows = stmt.query_map([], |r| r.get(0))?.collect();
            rows
        })
    }
}

fn gen_high_water(conn: &Connection, role: &str) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT gen FROM role_gen_high_water WHERE role=?1",
            [role],
            |r| r.get(0),
        )
        .optional()?
        .unwrap_or(0))
}

pub(crate) fn raise_gen(tx: &Transaction<'_>, role: &str, gen: i64) -> rusqlite::Result<()> {
    if gen > 0 {
        tx.execute(
            "INSERT INTO role_gen_high_water(role,gen) VALUES(?1,?2)
            ON CONFLICT(role) DO UPDATE SET gen=excluded.gen WHERE excluded.gen>gen",
            params![role, gen],
        )?;
    }
    Ok(())
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
    }

    #[test]
    fn role_read_derives_identity_names_and_keeps_nulls_and_role_set_bound() {
        let f = Fixture::new();
        assert_eq!(serde_json::to_string(&f.store.role_read("x").unwrap()).unwrap(), "{\"role\":\"x\",\"known\":false,\"held_here\":false,\"epoch\":0,\"holder\":null,\"holder_name\":null,\"source\":null,\"stale\":true}");
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE role_cache SET holder=?1, epoch=1, version=1, read_at_ms=1,
                         stale=0, source='cloud' WHERE role='x'",
                    [SELF],
                )?;
                tx.execute(
                    "INSERT INTO member_name(author,display_name) VALUES(?1,'Studio')",
                    [SELF],
                )?;
                Ok(())
            })
            .unwrap();
        assert!(!f.store.role_read("x").unwrap().held_here);
        assert!(f.store.learn_verified_self_author(SELF, 1).unwrap());
        let read = f.store.role_read("x").unwrap();
        assert!(read.known && read.held_here);
        assert_eq!(read.holder_name.as_deref(), Some("Studio"));
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "UPDATE role_cache SET holder=?1, epoch=2, version=2 WHERE role='x'",
                    [OTHER],
                )
            })
            .unwrap();
        let read = f.store.role_read("x").unwrap();
        assert!(read.known && !read.held_here && read.epoch >= 1 && read.holder.is_some());
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("UPDATE role_cache SET stale=1 WHERE role='x'", []))
            .unwrap();
        assert!(f.store.role_read("x").unwrap().stale);
        assert_eq!(
            f.store.role_read("x").unwrap().source.as_deref(),
            Some("cloud")
        );
        for i in 0..255 {
            f.store.role_read(&format!("role_{i}")).unwrap();
        }
        assert_eq!(f.store.legacy_switch_quiet_roles().unwrap().len(), 256);
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
}

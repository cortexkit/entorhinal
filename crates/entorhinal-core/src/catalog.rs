//! Catalog trust configuration is machine-local and never journaled or replayed.
use crate::{RegistryError, RegistryStore};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

pub(crate) const V11_CATALOG: &str = r#"
CREATE TABLE catalog_registry(url TEXT PRIMARY KEY, added_at_ms INTEGER NOT NULL,
    is_default INTEGER NOT NULL DEFAULT 0 CHECK(is_default IN (0,1)), incarnation TEXT NOT NULL);
CREATE UNIQUE INDEX catalog_one_default ON catalog_registry(is_default) WHERE is_default=1;
CREATE TABLE catalog_scope_map(scope TEXT PRIMARY KEY,
    registry_url TEXT NOT NULL REFERENCES catalog_registry(url) ON DELETE CASCADE);
CREATE TABLE catalog_registry_key(registry_url TEXT NOT NULL REFERENCES catalog_registry(url) ON DELETE CASCADE,
    chain_index INTEGER NOT NULL, key_id TEXT NOT NULL, public_key TEXT NOT NULL,
    statement TEXT, valid_from TEXT, PRIMARY KEY(registry_url,chain_index),
    UNIQUE(registry_url,key_id), UNIQUE(registry_url,public_key));
CREATE TABLE catalog_feed(scope TEXT PRIMARY KEY,
    registry_url TEXT NOT NULL REFERENCES catalog_registry(url) ON DELETE CASCADE,
    feed_seq INTEGER NOT NULL, etag TEXT);
CREATE TABLE catalog_status(name TEXT NOT NULL, version TEXT NOT NULL,
    registry_url TEXT REFERENCES catalog_registry(url) ON DELETE CASCADE,
    sequence INTEGER NOT NULL, changed_at TEXT NOT NULL, published_at TEXT NOT NULL,
    signer_index INTEGER NOT NULL, canonical TEXT NOT NULL, signature TEXT NOT NULL,
    PRIMARY KEY(name,version));
CREATE TABLE catalog_starter_proof(name TEXT NOT NULL, version TEXT NOT NULL, proof TEXT NOT NULL,
    PRIMARY KEY(name,version), FOREIGN KEY(name,version) REFERENCES catalog_status(name,version) ON DELETE CASCADE);
CREATE TABLE catalog_publisher_grant(id TEXT PRIMARY KEY, publisher_fingerprint TEXT NOT NULL,
    operation TEXT NOT NULL, placement_agent_id TEXT NOT NULL, ceiling_json TEXT NOT NULL,
    granted_at_ms INTEGER NOT NULL, card_ref TEXT, revoked_at_ms INTEGER,
    UNIQUE(publisher_fingerprint,operation,placement_agent_id));
CREATE TABLE catalog_audit(id INTEGER PRIMARY KEY, at_ms INTEGER NOT NULL, kind TEXT NOT NULL,
    grant_id TEXT, detail_json TEXT NOT NULL);
"#;

#[derive(Debug)]
pub enum CatalogWriteError {
    Refused(&'static str),
    Store(RegistryError),
}
impl std::fmt::Display for CatalogWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Refused(code) => write!(f, "{code}"),
            Self::Store(error) => error.fmt(f),
        }
    }
}
impl std::error::Error for CatalogWriteError {}

#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RegistryKey {
    pub chain_index: i64,
    pub key_id: String,
    pub public_key: String,
    pub statement: Option<String>,
    pub valid_from: Option<String>,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct CatalogRegistry {
    pub url: String,
    pub added_at_ms: i64,
    pub is_default: bool,
    #[serde(skip_serializing)]
    pub incarnation: String,
    pub scopes: Vec<String>,
    pub keys: Vec<RegistryKey>,
}
impl RegistryStore {
    pub(crate) fn catalog_transaction<T>(
        &self,
        action: impl FnOnce(&rusqlite::Transaction<'_>) -> Result<T, CatalogWriteError>,
    ) -> Result<T, CatalogWriteError> {
        let mut failure = None;
        self.db
            .with_conn_fenced(|tx| {
                action(tx).map_err(|e| {
                    failure = Some(e);
                    rusqlite::Error::QueryReturnedNoRows
                })
            })
            .map_err(|e| failure.unwrap_or(CatalogWriteError::Store(RegistryError::Store(e))))
    }
    pub fn catalog_registry_add(
        &self,
        url: &str,
        scopes: &[String],
        is_default: bool,
        key_id: &str,
        public_key: &str,
        now: i64,
    ) -> Result<(), CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let exists = |sql: &str, value: &str| -> Result<bool, CatalogWriteError> {
                tx.query_row(sql, [value], |r| r.get(0)).map_err(store_error)
            };
            if exists("SELECT EXISTS(SELECT 1 FROM catalog_registry WHERE url=?1)", url)? {
                return Err(CatalogWriteError::Refused("registry_already_added"));
            }
            if is_default && exists("SELECT EXISTS(SELECT 1 FROM catalog_registry WHERE is_default=1 AND ?1 IS NOT NULL)", url)? {
                return Err(CatalogWriteError::Refused("default_already_set"));
            }
            for scope in scopes {
                if exists("SELECT EXISTS(SELECT 1 FROM catalog_scope_map WHERE scope=?1)", scope)? {
                    return Err(CatalogWriteError::Refused("scope_already_mapped"));
                }
            }
            let mut random = [0u8; 16];
            getrandom::getrandom(&mut random).map_err(|_| CatalogWriteError::Refused("store_error"))?;
            let incarnation = random.iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            tx.execute("INSERT INTO catalog_registry VALUES(?1,?2,?3,?4)", params![url,now,is_default,incarnation]).map_err(store_error)?;
            for scope in scopes {
                tx.execute("INSERT INTO catalog_scope_map VALUES(?1,?2)", params![scope,url]).map_err(store_error)?;
            }
            tx.execute("INSERT INTO catalog_registry_key VALUES(?1,0,?2,?3,NULL,NULL)", params![url,key_id,public_key]).map_err(store_error)?;
            Ok(())
        })
    }
    pub fn catalog_registry_repin(
        &self,
        url: &str,
        key_id: &str,
        public_key: &str,
        valid_from: &str,
    ) -> Result<(), CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let index: Option<i64> = tx.query_row("SELECT MAX(chain_index) FROM catalog_registry_key WHERE registry_url=?1", [url], |r| r.get(0)).map_err(store_error)?;
            let Some(index) = index else { return Err(CatalogWriteError::Refused("registry_unknown")); };
            let conflict: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM catalog_registry_key WHERE registry_url=?1 AND (key_id=?2 OR public_key=?3))", params![url,key_id,public_key], |r| r.get(0)).map_err(store_error)?;
            if conflict { return Err(CatalogWriteError::Refused("key_conflict")); }
            tx.execute("INSERT INTO catalog_registry_key VALUES(?1,?2,?3,?4,NULL,?5)", params![url,index+1,key_id,public_key,valid_from]).map_err(store_error)?;
            Ok(())
        })
    }
    pub fn catalog_registry_remove(&self, url: &str) -> Result<(), CatalogWriteError> {
        self.catalog_transaction(|tx| {
            // Delete the child rows explicitly, so removal is complete even on a
            // connection where foreign-key cascades are off.
            for table in [
                "catalog_scope_map",
                "catalog_registry_key",
                "catalog_feed",
                "catalog_status",
            ] {
                tx.execute(&format!("DELETE FROM {table} WHERE registry_url=?1"), [url])
                    .map_err(store_error)?;
            }
            if tx
                .execute("DELETE FROM catalog_registry WHERE url=?1", [url])
                .map_err(store_error)?
                == 0
            {
                return Err(CatalogWriteError::Refused("registry_unknown"));
            }
            Ok(())
        })
    }
    pub fn catalog_registries(&self) -> Result<Vec<CatalogRegistry>, RegistryError> {
        self.read(read_registries)
    }
    pub fn catalog_registry_for_scope(&self, scope: &str) -> Result<Option<String>, RegistryError> {
        self.read(|conn| registry_for_scope(conn, scope))
    }
}
pub(crate) fn read_registries(
    conn: &rusqlite::Connection,
) -> rusqlite::Result<Vec<CatalogRegistry>> {
    let mut registries = conn
        .prepare(
            "SELECT url,added_at_ms,is_default,incarnation FROM catalog_registry ORDER BY url",
        )?
        .query_map([], |r| {
            Ok(CatalogRegistry {
                url: r.get(0)?,
                added_at_ms: r.get(1)?,
                is_default: r.get(2)?,
                incarnation: r.get(3)?,
                scopes: vec![],
                keys: vec![],
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for registry in &mut registries {
        registry.scopes = conn
            .prepare("SELECT scope FROM catalog_scope_map WHERE registry_url=?1 ORDER BY scope")?
            .query_map([&registry.url], |r| r.get(0))?
            .collect::<rusqlite::Result<_>>()?;
        registry.keys = conn.prepare("SELECT chain_index,key_id,public_key,statement,valid_from FROM catalog_registry_key WHERE registry_url=?1 ORDER BY chain_index")?.query_map([&registry.url], |r| Ok(RegistryKey { chain_index:r.get(0)?, key_id:r.get(1)?, public_key:r.get(2)?, statement:r.get(3)?, valid_from:r.get(4)? }))?.collect::<rusqlite::Result<_>>()?;
    }
    Ok(registries)
}
pub(crate) fn registry_for_scope(
    conn: &rusqlite::Connection,
    scope: &str,
) -> rusqlite::Result<Option<String>> {
    let explicit = conn
        .query_row(
            "SELECT registry_url FROM catalog_scope_map WHERE scope=?1",
            [scope],
            |r| r.get(0),
        )
        .optional()?;
    if explicit.is_some() {
        return Ok(explicit);
    }
    conn.query_row(
        "SELECT url FROM catalog_registry WHERE is_default=1",
        [],
        |r| r.get(0),
    )
    .optional()
}
pub(crate) fn store_error(e: rusqlite::Error) -> CatalogWriteError {
    CatalogWriteError::Store(e.into())
}

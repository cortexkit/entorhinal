//! Local catalog storage: verification reads and writes share one SQLite
//! transaction, so concurrent status or registry changes cannot be overwritten.
use crate::catalog::{read_registries, registry_for_scope, CatalogRegistry, CatalogWriteError};
use crate::RegistryStore;
use rusqlite::{params, OptionalExtension};
use serde::Serialize;

#[derive(Clone, Debug, Serialize)]
pub struct CatalogStatus {
    pub name: String,
    pub version: String,
    pub registry_url: String,
    pub sequence: u64,
    pub changed_at: String,
    pub published_at: String,
    pub signer_index: usize,
    pub canonical: String,
    pub signature: String,
}
#[derive(Default)]
pub struct TrustWrite {
    pub keys: Option<Vec<crate::catalog::RegistryKey>>,
    pub statuses: Vec<CatalogStatus>,
    pub feed_seq: Option<u64>,
    pub feed_etag: Option<String>,
}
impl RegistryStore {
    /// The verifier runs against current mapping, chain and holds inside the same
    /// transaction as its writes. No network I/O belongs in this closure.
    pub fn catalog_trust_transaction<T, E: From<CatalogWriteError>>(
        &self,
        scope: &str,
        action: impl FnOnce(
            &CatalogRegistry,
            &[CatalogStatus],
            Option<u64>,
        ) -> Result<(T, TrustWrite), E>,
    ) -> Result<T, E> {
        let mut failure = None;
        self.db.with_conn_fenced(|tx| {
            let result = (|| {
                let sql = |e: rusqlite::Error| E::from(CatalogWriteError::Store(e.into()));
                let url = registry_for_scope(tx, scope).map_err(sql)?
                    .ok_or_else(|| E::from(CatalogWriteError::Refused("scope_unmapped")))?;
                let registry = read_registries(tx).map_err(sql)?.into_iter().find(|r| r.url == url)
                    .ok_or_else(|| E::from(CatalogWriteError::Refused("registry_unknown")))?;
                let statuses = read_statuses(tx, &url).map_err(sql)?;
                let feed: Option<u64> = tx.query_row("SELECT feed_seq FROM catalog_feed WHERE scope=?1 AND registry_url=?2", params![scope,url], |r| r.get(0)).optional().map_err(sql)?;
                let (value, write) = action(&registry, &statuses, feed)?;
                if let Some(keys) = write.keys {
                    tx.execute("DELETE FROM catalog_registry_key WHERE registry_url=?1", [&url]).map_err(sql)?;
                    for key in keys {
                        tx.execute("INSERT INTO catalog_registry_key VALUES(?1,?2,?3,?4,?5,?6)", params![url,key.chain_index,key.key_id,key.public_key,key.statement,key.valid_from]).map_err(sql)?;
                    }
                }
                let status_changed = !write.statuses.is_empty();
                for row in write.statuses {
                    if row.registry_url != url {
                        return Err(E::from(CatalogWriteError::Refused("registry_changed")));
                    }
                    tx.execute("INSERT INTO catalog_status VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(name,version) DO UPDATE SET registry_url=excluded.registry_url,sequence=excluded.sequence,changed_at=excluded.changed_at,published_at=excluded.published_at,signer_index=excluded.signer_index,canonical=excluded.canonical,signature=excluded.signature", params![row.name,row.version,url,row.sequence,row.changed_at,row.published_at,row.signer_index,row.canonical,row.signature]).map_err(sql)?;
                }
                if status_changed {
                    crate::catalog_agent::apply_status_effects(tx).map_err(sql)?;
                }
                if let Some(seq) = write.feed_seq {
                    tx.execute("INSERT INTO catalog_feed(scope,registry_url,feed_seq,etag) VALUES(?1,?2,?3,?4) ON CONFLICT(scope) DO UPDATE SET registry_url=excluded.registry_url,feed_seq=excluded.feed_seq,etag=excluded.etag", params![scope,url,seq,write.feed_etag]).map_err(sql)?;
                }
                Ok(value)
            })();
            result.map_err(|e| { failure = Some(e); rusqlite::Error::QueryReturnedNoRows })
        }).map_err(|e| failure.unwrap_or_else(|| E::from(CatalogWriteError::Store(crate::RegistryError::Store(e)))))
    }
    pub fn catalog_statuses(
        &self,
        registry_url: &str,
    ) -> Result<Vec<CatalogStatus>, crate::RegistryError> {
        self.read(|conn| read_statuses(conn, registry_url))
    }
    pub fn catalog_feed_seq(&self, scope: &str) -> Result<Option<u64>, crate::RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT feed_seq FROM catalog_feed WHERE scope=?1",
                [scope],
                |r| r.get(0),
            )
            .optional()
        })
    }
    pub fn catalog_feed_etag(&self, scope: &str) -> Result<Option<String>, crate::RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT etag FROM catalog_feed WHERE scope=?1",
                [scope],
                |r| r.get(0),
            )
            .optional()
            .map(Option::flatten)
        })
    }
}
fn read_statuses(conn: &rusqlite::Connection, url: &str) -> rusqlite::Result<Vec<CatalogStatus>> {
    conn.prepare("SELECT name,version,registry_url,sequence,changed_at,published_at,signer_index,canonical,signature FROM catalog_status WHERE registry_url=?1 ORDER BY name,version")?
        .query_map([url], |r| Ok(CatalogStatus { name:r.get(0)?, version:r.get(1)?, registry_url:r.get(2)?, sequence:r.get(3)?, changed_at:r.get(4)?, published_at:r.get(5)?, signer_index:r.get(6)?, canonical:r.get(7)?, signature:r.get(8)? }))?
        .collect()
}

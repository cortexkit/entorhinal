//! Publisher grants authorize only this Mac. Neither grants nor their audit rows
//! enter the identity journal that other Macs replay.
use crate::catalog::{store_error, CatalogWriteError};
use crate::{RegistryError, RegistryStore};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Operation {
    Install,
    Upgrade,
}
/// Adding pins is an install; changing or removing any held pin needs upgrade
/// authority, even when the request calls itself an install.
pub fn authorization_operation(
    requested: &str,
    previous: &[crate::catalog_agent::CatalogPin],
    next: &[crate::catalog_agent::CatalogPin],
) -> Operation {
    if requested != "install"
        || previous
            .iter()
            .any(|held| !next.iter().any(|pin| pin == held))
    {
        Operation::Upgrade
    } else {
        Operation::Install
    }
}
impl Operation {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Install => "install",
            Self::Upgrade => "upgrade",
        }
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublisherGrant {
    pub id: String,
    pub publisher_fingerprint: String,
    pub operation: Operation,
    pub placement_agent_id: String,
    pub ceiling: Vec<String>,
    pub card_ref: Option<String>,
    pub granted_at_ms: i64,
    pub revoked_at_ms: Option<i64>,
}
#[derive(Clone, Debug)]
pub struct CoveragePackage {
    pub name: String,
    pub publisher_fingerprint: String,
    pub capabilities: Vec<String>,
    pub has_scripts: bool,
    pub root_certified: bool,
}
#[derive(Debug, PartialEq, Eq)]
pub enum Coverage {
    Covered,
    Uncovered(Vec<String>),
}
/// The caller supplies the placement agent's liveness from current local state;
/// a grant cannot authorize installation for a retired or merged agent.
pub fn coverage(
    packages: &[CoveragePackage],
    grants: &[PublisherGrant],
    operation: Operation,
    agent_id: &str,
    agent_live: bool,
) -> Coverage {
    let mut reasons = Vec::new();
    for package in packages {
        if package.root_certified {
            if package.has_scripts {
                reasons.push(format!(
                    "{}: scripts require package approval",
                    package.name
                ));
            } else if !agent_live {
                reasons.push(format!("{}: placement agent is not live", package.name));
            }
            continue;
        }
        let covered = agent_live
            && !package.has_scripts
            && grants.iter().any(|grant| {
                grant.revoked_at_ms.is_none()
                    && grant.publisher_fingerprint == package.publisher_fingerprint
                    && grant.operation == operation
                    && grant.placement_agent_id == agent_id
                    && package
                        .capabilities
                        .iter()
                        .all(|token| grant.ceiling.contains(token))
            });
        if !covered {
            let reason = if !agent_live {
                "placement agent is not live"
            } else if package.has_scripts {
                "scripts require package approval"
            } else if grants.iter().any(|g| {
                g.revoked_at_ms.is_none()
                    && g.publisher_fingerprint == package.publisher_fingerprint
                    && g.operation == operation
                    && g.placement_agent_id == agent_id
            }) {
                "capabilities exceed the grant ceiling"
            } else {
                "no live publisher grant for this operation and placement"
            };
            reasons.push(format!("{}: {reason}", package.name));
        }
    }
    if reasons.is_empty() {
        Coverage::Covered
    } else {
        Coverage::Uncovered(reasons)
    }
}
impl RegistryStore {
    /// Callers authenticate the pinned manifests whose capability tokens form
    /// grant.ceiling and authorize this write before entering storage. A manifest
    /// requiring no capability tokens legitimately contributes an empty ceiling.
    pub fn insert_publisher_grant(
        &self,
        grant: &PublisherGrant,
        principal: &serde_json::Value,
    ) -> Result<String, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let status = crate::agent::load_row(tx, &grant.placement_agent_id)
                .map_err(store_error)?
                .map(|row| row.status);
            match status.as_deref() {
                None => return Err(CatalogWriteError::Refused("unknown_agent")),
                Some("live") => (),
                Some(_) => return Err(CatalogWriteError::Refused("gone")),
            }
            put_grant(tx, grant, principal).map_err(store_error)
        })
    }
    pub fn publisher_grants(&self) -> Result<Vec<PublisherGrant>, RegistryError> {
        self.read(read_grants)
    }
    pub fn revoke_publisher_grant(
        &self,
        id: &str,
        now: i64,
        principal: &serde_json::Value,
    ) -> Result<bool, CatalogWriteError> {
        self.catalog_transaction(|tx| {
            let grant = read_grants(tx)
                .map_err(store_error)?
                .into_iter()
                .find(|g| g.id == id && g.revoked_at_ms.is_none());
            let Some(grant) = grant else {
                return Ok(false);
            };
            let live = crate::agent::load_row(tx, &grant.placement_agent_id)
                .map_err(store_error)?
                .is_some_and(|row| row.status == "live");
            if !live {
                return Ok(false);
            }
            tx.execute(
                "UPDATE catalog_publisher_grant SET revoked_at_ms=?2 WHERE id=?1",
                params![id, now],
            )
            .map_err(store_error)?;
            audit(tx, "grant_revoke", &grant, principal, now).map_err(store_error)?;
            Ok(true)
        })
    }
    pub fn catalog_audit(&self) -> Result<Vec<serde_json::Value>, RegistryError> {
        self.read(|conn| conn.prepare("SELECT at_ms,kind,grant_id,detail_json FROM catalog_audit ORDER BY id")?.query_map([], |r| Ok(serde_json::json!({"at_ms":r.get::<_,i64>(0)?,"kind":r.get::<_,String>(1)?,"grant_id":r.get::<_,String>(2)?,"detail":decode(r.get(3)?)?})))?.collect())
    }
}
pub(crate) fn put_grant(
    tx: &rusqlite::Transaction<'_>,
    grant: &PublisherGrant,
    principal: &serde_json::Value,
) -> rusqlite::Result<String> {
    let prior = read_grants(tx)?.into_iter().find(|g| {
        g.publisher_fingerprint == grant.publisher_fingerprint
            && g.operation == grant.operation
            && g.placement_agent_id == grant.placement_agent_id
    });
    let mut row = grant.clone();
    let kind = if let Some(prior) = prior {
        row.id = prior.id;
        row.ceiling.extend(prior.ceiling);
        "grant_refresh"
    } else {
        "grant_insert"
    };
    row.ceiling = row
        .ceiling
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    tx.execute("INSERT INTO catalog_publisher_grant VALUES(?1,?2,?3,?4,?5,?6,?7,NULL) ON CONFLICT(publisher_fingerprint,operation,placement_agent_id) DO UPDATE SET ceiling_json=excluded.ceiling_json,granted_at_ms=excluded.granted_at_ms,card_ref=excluded.card_ref,revoked_at_ms=NULL", params![row.id,row.publisher_fingerprint,row.operation.as_str(),row.placement_agent_id,serde_json::to_string(&row.ceiling).unwrap(),row.granted_at_ms,row.card_ref])?;
    audit(tx, kind, &row, principal, row.granted_at_ms)?;
    Ok(row.id)
}
fn audit(
    tx: &rusqlite::Transaction<'_>,
    kind: &str,
    grant: &PublisherGrant,
    principal: &serde_json::Value,
    now: i64,
) -> rusqlite::Result<()> {
    let detail = serde_json::json!({"publisher_fingerprint":grant.publisher_fingerprint,"operation":grant.operation,"placement_agent_id":grant.placement_agent_id,"ceiling":grant.ceiling,"principal":principal});
    tx.execute(
        "INSERT INTO catalog_audit(at_ms,kind,grant_id,detail_json) VALUES(?1,?2,?3,?4)",
        params![now, kind, grant.id, detail.to_string()],
    )?;
    Ok(())
}
fn decode(text: String) -> rusqlite::Result<serde_json::Value> {
    serde_json::from_str(&text).map_err(|e| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(e))
    })
}
pub(crate) fn read_grants(conn: &rusqlite::Connection) -> rusqlite::Result<Vec<PublisherGrant>> {
    conn.prepare("SELECT id,publisher_fingerprint,operation,placement_agent_id,ceiling_json,card_ref,granted_at_ms,revoked_at_ms FROM catalog_publisher_grant ORDER BY id")?.query_map([], |r| {
        let operation = serde_json::from_value(serde_json::json!(r.get::<_,String>(2)?)).map_err(|e| rusqlite::Error::FromSqlConversionFailure(2,rusqlite::types::Type::Text,Box::new(e)))?;
        let ceiling = serde_json::from_value(decode(r.get(4)?)?).map_err(|e| rusqlite::Error::FromSqlConversionFailure(4,rusqlite::types::Type::Text,Box::new(e)))?;
        Ok(PublisherGrant { id:r.get(0)?,publisher_fingerprint:r.get(1)?,operation,placement_agent_id:r.get(3)?,ceiling,card_ref:r.get(5)?,granted_at_ms:r.get(6)?,revoked_at_ms:r.get(7)? })
    })?.collect()
}

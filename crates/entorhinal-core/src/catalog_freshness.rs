//! Machine-local registry confirmation clocks. Signed status timestamps describe
//! registry events, not when this machine last checked for a revocation.
use crate::{catalog::store_error, catalog_agent::CatalogPin, RegistryError, RegistryStore};
use rusqlite::{params, Connection, OptionalExtension, Transaction};

pub(crate) const V20_CATALOG_FRESHNESS: &str = r#"
CREATE TABLE catalog_status_confirmation(name TEXT NOT NULL, version TEXT NOT NULL,
 last_confirmed_ms INTEGER NOT NULL, PRIMARY KEY(name,version));
CREATE TABLE catalog_fast_lane(name TEXT NOT NULL, version TEXT NOT NULL,
 entered_at_ms INTEGER NOT NULL, stale_since_ms INTEGER, PRIMARY KEY(name,version));
"#;
pub const STALE_AFTER_MS: i64 = 900_000;

pub(crate) fn confirm(
    tx: &Transaction<'_>,
    name: &str,
    version: &str,
    now: i64,
) -> rusqlite::Result<()> {
    tx.execute("INSERT INTO catalog_status_confirmation VALUES(?1,?2,?3) ON CONFLICT(name,version) DO UPDATE SET last_confirmed_ms=max(last_confirmed_ms,excluded.last_confirmed_ms)", params![name,version,now])?;
    tx.execute(
        "UPDATE catalog_fast_lane SET stale_since_ms=NULL WHERE name=?1 AND version=?2",
        params![name, version],
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        catalog_agent::{
            tests::{agent, pin},
            PinWriteRequest,
        },
        mutations::tests::Fixture,
    };

    #[test]
    fn catalog_freshness_entry_confirmation_boundary_and_exit() {
        let f = Fixture::new("catalog-freshness");
        let id = agent(&f.store);
        let mut flow = pin("@example/flow", "1.0.0", true);
        flow.kind = entorhinal_catalog::Kind::Flow;
        let skill = pin("@example/skill", "1.0.0", true);
        f.store
            .with_principal("test")
            .catalog_set_pins(
                PinWriteRequest {
                    agent_id: id,
                    pins: vec![flow.clone(), skill.clone()],
                    request_key: None,
                },
                100,
            )
            .unwrap();
        f.store
            .catalog_sync_fast_lane(&[flow.clone(), skill.clone()], 100)
            .unwrap();
        f.store.catalog_advance_freshness(900_099).unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            None
        );
        f.store.catalog_advance_freshness(900_100).unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            Some(900_100)
        );
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &skill.name, &skill.version))
                .unwrap(),
            None
        );
        f.store
            .catalog_transaction(|tx| {
                confirm(tx, &flow.name, &flow.version, 1_000_000).map_err(store_error)
            })
            .unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            None
        );
        f.store.catalog_advance_freshness(1_900_000).unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            Some(1_900_000)
        );
        f.store.catalog_sync_fast_lane(&[], 2_000_000).unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            None
        );
        f.store
            .catalog_sync_fast_lane(&[flow.clone()], 3_000_000)
            .unwrap();
        f.store.catalog_advance_freshness(3_900_000).unwrap();
        assert_eq!(
            f.store
                .read(|c| stale_since(c, &flow.name, &flow.version))
                .unwrap(),
            Some(3_900_000)
        );
        let journal = f.store.verify().unwrap();
        assert!(journal.ok, "{journal:?}");
    }
}

pub(crate) fn stale_since(
    conn: &Connection,
    name: &str,
    version: &str,
) -> rusqlite::Result<Option<i64>> {
    conn.query_row(
        "SELECT stale_since_ms FROM catalog_fast_lane f WHERE name=?1 AND version=?2 AND EXISTS(SELECT 1 FROM catalog_pin p JOIN agent a USING(agent_id) WHERE p.name=f.name AND p.version=f.version AND p.kind='flow' AND a.terminal_reason IS NULL)",
        params![name, version],
        |r| r.get(0),
    )
    .optional()
    .map(Option::flatten)
}

fn advance(tx: &Transaction<'_>, now: i64) -> rusqlite::Result<()> {
    tx.execute("UPDATE catalog_fast_lane SET stale_since_ms=max(entered_at_ms,coalesce((SELECT last_confirmed_ms FROM catalog_status_confirmation c WHERE c.name=catalog_fast_lane.name AND c.version=catalog_fast_lane.version),entered_at_ms))+?1 WHERE ?2 >= max(entered_at_ms,coalesce((SELECT last_confirmed_ms FROM catalog_status_confirmation c WHERE c.name=catalog_fast_lane.name AND c.version=catalog_fast_lane.version),entered_at_ms))+?1", params![STALE_AFTER_MS,now])?;
    Ok(())
}

impl RegistryStore {
    /// Only fields that change a consumer's hold decision belong in this watch
    /// snapshot. Signed reason text and confirmation-clock advances are not news.
    pub fn catalog_served_statuses(
        &self,
    ) -> Result<std::collections::BTreeMap<(String, String), serde_json::Value>, RegistryError>
    {
        self.read(|conn| {
            let pins = conn.prepare("SELECT DISTINCT p.name,p.version,p.kind,s.canonical FROM catalog_pin p JOIN agent a USING(agent_id) LEFT JOIN catalog_status s ON s.name=p.name AND s.version=p.version WHERE a.terminal_reason IS NULL ORDER BY p.name,p.version")?.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,String>(2)?,r.get::<_,Option<String>>(3)?)))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut statuses = std::collections::BTreeMap::new();
            for (name,version,kind,text) in pins {
                let mut served = if let Some(text) = text {
                    let value: serde_json::Value = serde_json::from_str(&text).map_err(|e|rusqlite::Error::FromSqlConversionFailure(3,rusqlite::types::Type::Text,Box::new(e)))?;
                    serde_json::json!({"state":value["state"],"blocked":crate::catalog_agent::status_blocks(&value)})
                } else { serde_json::Value::Null };
                if kind == "flow" && !served.is_null() {
                    if let Some(since) = stale_since(conn,&name,&version)? { served["stale_since_ms"] = serde_json::json!(since); }
                }
                statuses.insert((name,version),served);
            }
            Ok(statuses)
        })
    }
    pub fn catalog_fast_scopes(&self) -> Result<Vec<String>, RegistryError> {
        self.read(|conn| {
            let names = conn.prepare("SELECT DISTINCT f.name FROM catalog_fast_lane f JOIN catalog_pin p ON p.name=f.name AND p.version=f.version JOIN agent a USING(agent_id) WHERE a.terminal_reason IS NULL AND p.kind='flow'")?.query_map([],|r|r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            Ok(names.iter().filter_map(|n| entorhinal_catalog::names::map_name(n).ok().map(|n|format!("@{}",n.scope))).collect::<std::collections::BTreeSet<_>>().into_iter().collect())
        })
    }
    pub fn catalog_advance_freshness(&self, now: i64) -> Result<(), RegistryError> {
        self.catalog_transaction(|tx| advance(tx, now).map_err(store_error))
            .map_err(|e| RegistryError::Domain {
                code: "storage_error".into(),
                message: e.to_string(),
                detail: None,
            })
    }
    /// The poller reads verified manifests outside a store snapshot, then this
    /// transaction rechecks live pins before recording lane membership. Leaving
    /// and re-entering the lane starts a new grace period, even after a restart.
    pub fn catalog_sync_fast_lane(
        &self,
        pins: &[CatalogPin],
        now: i64,
    ) -> Result<(), RegistryError> {
        self.catalog_transaction(|tx| {
            let mut keep = std::collections::BTreeSet::new();
            for pin in pins {
                if pin.kind != entorhinal_catalog::Kind::Flow { continue; }
                let live: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM catalog_pin p JOIN agent a USING(agent_id) WHERE a.terminal_reason IS NULL AND p.kind='flow' AND p.name=?1 AND p.version=?2 AND p.manifest_sha256=?3)", params![pin.name,pin.version,pin.manifest_sha256], |r| r.get(0)).map_err(store_error)?;
                if live {
                    keep.insert((pin.name.clone(),pin.version.clone()));
                    tx.execute("INSERT OR IGNORE INTO catalog_fast_lane(name,version,entered_at_ms) VALUES(?1,?2,?3)", params![pin.name,pin.version,now]).map_err(store_error)?;
                }
            }
            let held = tx.prepare("SELECT name,version FROM catalog_fast_lane").map_err(store_error)?.query_map([], |r| Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?))).map_err(store_error)?.collect::<rusqlite::Result<Vec<_>>>().map_err(store_error)?;
            for (name,version) in held {
                if !keep.contains(&(name.clone(),version.clone())) {
                    tx.execute("DELETE FROM catalog_fast_lane WHERE name=?1 AND version=?2", params![name,version]).map_err(store_error)?;
                }
            }
            // A confirmation before entry does not establish freshness in the
            // new lane. Never-confirmed versions use their entry's grace period.
            advance(tx,now).map_err(store_error)?;
            Ok(())
        }).map_err(|e| RegistryError::Domain { code:"storage_error".into(), message:e.to_string(), detail:None })
    }

    pub fn catalog_live_flow_pins(&self) -> Result<Vec<CatalogPin>, RegistryError> {
        self.read(|conn| {
            let agents = conn.prepare("SELECT DISTINCT p.agent_id FROM catalog_pin p JOIN agent a USING(agent_id) WHERE a.terminal_reason IS NULL AND p.kind='flow'")?.query_map([], |r| r.get::<_,String>(0))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let mut pins = std::collections::BTreeMap::new();
            for agent in agents {
                for pin in crate::catalog_agent::read_pins(conn,&agent)? {
                    if pin.kind == entorhinal_catalog::Kind::Flow { pins.insert((pin.name.clone(),pin.version.clone()),pin); }
                }
            }
            Ok(pins.into_values().collect())
        })
    }
}

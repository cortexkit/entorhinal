use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};

use super::{AgentMutationError, NormalizedAgentName};

/// Claim history survives workspace removal: the namespace is an identity
/// key, not a foreign key to the current workspace table.
/// Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_claims.rs:248-266.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentNameClaim {
    pub claim_id: i64,
    pub agent_id: String,
    pub namespace_kind: String,
    pub namespace_key: String,
    pub normalized_name: String,
    pub name_normalization_version: i64,
    pub display_name: String,
    pub claimed_at_ms: i64,
    pub released_at_ms: Option<i64>,
}

fn decode(row: &rusqlite::Row<'_>) -> rusqlite::Result<AgentNameClaim> {
    Ok(AgentNameClaim {
        claim_id: row.get(0)?,
        agent_id: row.get(1)?,
        namespace_kind: row.get(2)?,
        namespace_key: row.get(3)?,
        normalized_name: row.get(4)?,
        name_normalization_version: row.get(5)?,
        display_name: row.get(6)?,
        claimed_at_ms: row.get(7)?,
        released_at_ms: row.get(8)?,
    })
}

const COLUMNS: &str = "claim_id,agent_id,namespace_kind,namespace_key,normalized_name,name_normalization_version,display_name,claimed_at_ms,released_at_ms";

pub(crate) fn load_claims(
    conn: &Connection,
    agent_id: &str,
) -> rusqlite::Result<Vec<AgentNameClaim>> {
    conn.prepare(&format!(
        "SELECT {COLUMNS} FROM agent_name_claim WHERE agent_id=?1 ORDER BY claim_id"
    ))?
    .query_map([agent_id], decode)?
    .collect()
}

pub(crate) fn active_claim(conn: &Connection, agent_id: &str) -> rusqlite::Result<AgentNameClaim> {
    conn.query_row(
        &format!(
            "SELECT {COLUMNS} FROM agent_name_claim WHERE agent_id=?1 AND released_at_ms IS NULL"
        ),
        [agent_id],
        decode,
    )
}

pub(crate) fn check_name(
    conn: &Connection,
    kind: &str,
    key: &str,
    name: &NormalizedAgentName,
    except_agent: Option<&str>,
) -> Result<(), AgentMutationError> {
    let owner = conn.query_row(
        "SELECT agent_id FROM agent_name_claim WHERE namespace_kind=?1 AND namespace_key=?2 AND name_normalization_version=?3 AND normalized_name=?4 AND released_at_ms IS NULL",
        params![kind,key,name.normalization_version,name.normalized_name], |row| row.get::<_, String>(0),
    ).optional()?;
    if owner
        .as_deref()
        .is_some_and(|owner| Some(owner) != except_agent)
    {
        return Err(AgentMutationError::new(
            "name_conflict",
            format!("name '{}' is claimed in {kind}/{key}", name.stored_name),
        ));
    }
    Ok(())
}

pub(crate) fn claim(
    tx: &Transaction<'_>,
    agent_id: &str,
    kind: &str,
    key: &str,
    name: &NormalizedAgentName,
    now: i64,
) -> Result<AgentNameClaim, AgentMutationError> {
    tx.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,name_normalization_version,normalized_name,display_name,claimed_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7)",
        params![agent_id,kind,key,name.normalization_version,name.normalized_name,name.stored_name,now])
        .map_err(|error| {
            // Keep the index refusal typed even if this helper is used without
            // the preflight collision check.
            // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_claims.rs:146-172.
            let message = error.to_string();
            if message.contains("agent_name_claim.namespace_kind") && message.contains("agent_name_claim.normalized_name") {
                AgentMutationError::new("name_conflict", message)
            } else { error.into() }
        })?;
    active_claim(tx, agent_id).map_err(Into::into)
}

pub(crate) fn release(
    tx: &Transaction<'_>,
    agent_id: &str,
    now: i64,
) -> rusqlite::Result<AgentNameClaim> {
    let mut claim = active_claim(tx, agent_id)?;
    if tx.execute("UPDATE agent_name_claim SET released_at_ms=?2 WHERE claim_id=?1 AND released_at_ms IS NULL", params![claim.claim_id, now])? != 1 {
        return Err(rusqlite::Error::QueryReturnedNoRows);
    }
    claim.released_at_ms = Some(now);
    Ok(claim)
}

pub(crate) fn write_claim(tx: &Transaction<'_>, claim: &AgentNameClaim) -> rusqlite::Result<()> {
    tx.execute("INSERT INTO agent_name_claim(claim_id,agent_id,namespace_kind,namespace_key,normalized_name,name_normalization_version,display_name,claimed_at_ms,released_at_ms) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9) ON CONFLICT(claim_id) DO UPDATE SET released_at_ms=excluded.released_at_ms",
        params![claim.claim_id,claim.agent_id,claim.namespace_kind,claim.namespace_key,claim.normalized_name,claim.name_normalization_version,claim.display_name,claim.claimed_at_ms,claim.released_at_ms])?;
    Ok(())
}

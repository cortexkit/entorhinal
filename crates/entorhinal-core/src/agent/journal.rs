use rusqlite::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{claims::write_claim, store::write_row, AgentMutationError, AgentNameClaim, AgentRow};
use crate::mutations::{append, Action};

/// Each journal entry carries the full agent row after the operation and the
/// claims it inserted or released. Replay restores rows, ids, generations and
/// claim history exactly from these recorded values, without re-running
/// validation or drawing new values from the id source.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentChangeEntry {
    pub seq: i64,
    pub op: String,
    pub agent_id: String,
    pub agent_generation: i64,
    pub row: AgentRow,
    pub claims: Vec<AgentNameClaim>,
    pub old_display_name: Option<String>,
    pub new_display_name: Option<String>,
    pub status: Option<String>,
    pub merged_into: Option<String>,
}

impl AgentChangeEntry {
    pub fn new(op: &str, row: AgentRow, claims: Vec<AgentNameClaim>) -> Self {
        let terminal = matches!(op, "agent.dispose" | "agent.merge");
        Self {
            seq: 0,
            op: op.into(),
            agent_id: row.agent_id.clone(),
            agent_generation: row.agent_generation,
            status: terminal.then(|| row.status.clone()),
            merged_into: if terminal {
                row.merged_into.clone()
            } else {
                None
            },
            row,
            claims,
            old_display_name: None,
            new_display_name: None,
        }
    }
}

pub(crate) fn record(
    tx: &Transaction<'_>,
    mut entry: AgentChangeEntry,
    value: Value,
    actor: &str,
    key: &str,
    now: i64,
    principal: &str,
) -> Result<Action, AgentMutationError> {
    entry.seq = append(tx, &entry.op, &json!({}), actor, Some(key), now, principal)?;
    Ok(Action {
        // Even an unchanged after-image needs a keyed, cached journal entry.
        changed: true,
        seq: Some(entry.seq),
        value,
        payload: json!({"entry": entry}),
    })
}

/// Restore an identity after-image. This also accepts import entries, so an
/// import and subsequent mutations use the same projection representation.
pub fn replay_agent_entry(
    tx: &Transaction<'_>,
    op: &str,
    payload: &Value,
) -> rusqlite::Result<bool> {
    if op == "agent.cutover" {
        return Ok(true);
    }
    if !matches!(
        op,
        "agent.create"
            | "agent.rename"
            | "agent.update_tag"
            | "agent.set_labels"
            | "agent.set_avatar"
            | "agent.set_github_identity"
            | "agent.dispose"
            | "agent.merge"
            | "agent.import"
    ) {
        return Ok(false);
    }
    let entry: AgentChangeEntry = serde_json::from_value(
        payload
            .get("entry")
            .cloned()
            .unwrap_or_else(|| payload.clone()),
    )
    .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
    write_row(tx, &entry.row)?;
    // Rename records the released claim first, avoiding the active-owner index
    // collision when inserting its replacement.
    for claim in &entry.claims {
        write_claim(tx, claim)?;
    }
    Ok(true)
}

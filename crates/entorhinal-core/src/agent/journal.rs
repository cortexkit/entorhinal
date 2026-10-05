use rusqlite::Transaction;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{claims::write_claim, store::write_row, AgentMutationError, AgentNameClaim, AgentRow};
use crate::mutations::{append, Action};

/// The journal op of the one-time row that records entorhinal taking over agent
/// identity from core (written by the import, also when it imports no agents).
/// It carries no agent row, so replay keeps it but restores nothing from it, and
/// the change feed doesn't deliver it.
pub(crate) const IDENTITY_MARKER_OP: &str = "agent.cutover";

/// Every journal op that records an agent row after a change. Three readers
/// depend on this one list: the change feed delivers exactly these ops, rebuild
/// replays exactly these into the agent tables, and `journal_tail` hides these
/// plus the marker from project consumers. If they disagreed, an op could reach
/// project consumers, vanish from the feed, or be dropped on rebuild.
pub(crate) const IDENTITY_CHANGE_OPS: &[&str] = &[
    "agent.create",
    "agent.rename",
    "agent.update_tag",
    "agent.set_labels",
    "agent.set_avatar",
    "agent.set_github_identity",
    "agent.dispose",
    "agent.merge",
    "agent.import",
];

/// These SQL literals come only from the fixed operation names above, never
/// from requests or persisted payloads.
pub(crate) fn change_ops_sql() -> String {
    IDENTITY_CHANGE_OPS
        .iter()
        .map(|op| format!("'{op}'"))
        .collect::<Vec<_>>()
        .join(",")
}

/// Each journal entry carries the full agent row after the operation and the
/// claims it inserted or released. Replay restores rows, ids, generations and
/// claim history exactly from these recorded values, without re-running
/// validation or drawing new values from the id source.
/// This unversioned journal payload denies unknown fields. Future fields must
/// be `Option` with `#[serde(default)]`, or older rows will fail rebuild and feed decoding.
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
    if op == IDENTITY_MARKER_OP {
        return Ok(true);
    }
    if !IDENTITY_CHANGE_OPS.contains(&op) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn every_agent_store_journal_op_is_an_identity_change_or_marker() {
        // Inspect production operation literals as well as exercised operations:
        // a new mutation branch must not disappear just because no fixture calls it yet.
        for source in [include_str!("store.rs"), include_str!("import.rs")] {
            let production = source.split("#[cfg(test)]").next().unwrap();
            for op in production.split('"').skip(1).step_by(2) {
                if op.starts_with("agent.") && !op.contains(char::is_whitespace) {
                    assert!(
                        IDENTITY_CHANGE_OPS.contains(&op) || op == IDENTITY_MARKER_OP,
                        "store operation {op} is absent from the identity journal operations"
                    );
                }
            }
        }
    }
}

//! Carrying an agent's session bindings, with their network history, from
//! the source Mac of a move to the receiving Mac.
//!
//! The shared move record names live bindings only by `(ref_kind, ref)`, so
//! without more data the receiver could only insert placeholder rows: no host
//! session, no runner, no history. Entorhinal therefore sends the full rows in
//! its own agent-data stream (ref kind [`SESSION_BINDINGS_KIND`], one ref per
//! agent), and the receiver installs them when the move activates. This
//! module holds the store side of that: reading the rows on the source and
//! installing them on the receiver. The stream framing lives in the module
//! crate.

use std::collections::{BTreeMap, BTreeSet};

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    agent::AgentMutationError,
    move_record::MoveRef,
    move_store::{MoveSide, MoveStep},
    session_network::{network_history, NETWORK_HISTORY_MAX, NETWORK_VALUES},
    session_refusal, NetworkEntry, RegistryError, RegistryStore,
};

/// The agent-data ref kind of the stream that carries an agent's bindings.
/// Its ref is the agent id.
pub const SESSION_BINDINGS_KIND: &str = "session-bindings";

/// The ref kind of entorhinal's catalog package closure.
pub const PACKAGE_CLOSURE_KIND: &str = "package-closure";

/// Whether a move ref names a session binding. Entorhinal adds two refs of
/// its own to a move (the package closure and the carried bindings), each
/// naming the agent rather than a session. Neither may ever become a
/// `session_binding` row or be looked up as one.
pub fn is_binding_ref_kind(ref_kind: &str) -> bool {
    ref_kind != PACKAGE_CLOSURE_KIND && ref_kind != SESSION_BINDINGS_KIND
}

/// One binding as carried between Macs: every column of its
/// `session_binding` row except the agent id (the move names the agent), and
/// its whole network history, oldest first, with the source's `since_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CarriedBinding {
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub session_ref: String,
    pub runner: Option<String>,
    pub harness: Option<String>,
    pub session_id: Option<String>,
    pub project_root: String,
    pub registered_by: String,
    pub registered_at_ms: i64,
    pub ended_at_ms: Option<i64>,
    pub network_history: Vec<NetworkEntry>,
    /// Only metadata travels for a finished session outside the frozen ref set.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub held_on: Option<String>,
}

impl CarriedBinding {
    pub fn key(&self) -> (&str, &str) {
        (&self.ref_kind, &self.session_ref)
    }

    /// Check the row's fields and history against the receiving store's
    /// constraints, so a malformed row is refused when the stream is
    /// imported, before the agent's claim moves, rather than failing the
    /// activation transaction afterwards.
    pub fn check(&self) -> Result<(), String> {
        if self.ref_kind.is_empty() || self.session_ref.is_empty() {
            return Err("binding has an empty ref".into());
        }
        if !is_binding_ref_kind(&self.ref_kind) {
            return Err(format!("{} is not a session binding kind", self.ref_kind));
        }
        if !matches!(
            self.registered_by.as_str(),
            "orchestrator" | "host" | "move"
        ) {
            return Err(format!("unknown registered_by {}", self.registered_by));
        }
        if self.harness.is_some() != self.session_id.is_some() {
            return Err("harness and session id must both be present or both absent".into());
        }
        if self.held_on.as_ref().is_some_and(|id| id.is_empty())
            || (self.held_on.is_some() && self.ended_at_ms.is_none())
        {
            return Err("only finished bindings may name a nonempty data holder".into());
        }
        if self.network_history.len() > NETWORK_HISTORY_MAX as usize {
            return Err("network history exceeds its cap".into());
        }
        let mut previous = None;
        for entry in &self.network_history {
            if !NETWORK_VALUES.contains(&entry.network.as_str()) {
                return Err(format!("unknown network value {}", entry.network));
            }
            // Readers find the value in effect at a time by the last entry
            // at or before it, so entries must keep strictly rising times.
            if previous.is_some_and(|p| p >= entry.since_ms) {
                return Err("network history is not in strictly rising time order".into());
            }
            previous = Some(entry.since_ms);
        }
        Ok(())
    }
}

/// What installing carried bindings at activation did, for health counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct CarriedInstall {
    /// Carried rows written as they were on the source.
    pub installed: u64,
    /// Carried rows written as placeholders because their host session or
    /// runner session already names another binding on this Mac.
    pub placeholders: u64,
    /// Carried rows skipped because this Mac already had the binding.
    pub kept: u64,
    /// Finished metadata skipped because a local binding owns its host or
    /// runner session key: (incoming ref, local binding ref).
    pub skipped_finished: Vec<(MoveRef, MoveRef)>,
}

/// The session-binding refs of a stored move plan, sorted and without
/// duplicates. A ref shared by several participants appears once.
pub fn plan_binding_refs(plan: &Value) -> BTreeSet<MoveRef> {
    plan.get("participants")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|p| p.get("refs").and_then(Value::as_array))
        .flatten()
        .filter_map(|r| serde_json::from_value::<MoveRef>(r.clone()).ok())
        .filter(|r| is_binding_ref_kind(&r.ref_kind))
        .collect()
}

fn carried_rows(conn: &Connection, agent_id: &str) -> rusqlite::Result<Vec<CarriedBinding>> {
    let mut statement = conn.prepare(
        "SELECT ref_kind, ref, runner, harness, session_id, project_root, registered_by,
                registered_at_ms, ended_at_ms, held_on
         FROM session_binding WHERE agent_id=?1 ORDER BY ref_kind, ref",
    )?;
    let mut rows = statement
        .query_map([agent_id], |row| {
            Ok(CarriedBinding {
                ref_kind: row.get(0)?,
                session_ref: row.get(1)?,
                runner: row.get(2)?,
                harness: row.get(3)?,
                session_id: row.get(4)?,
                project_root: row.get(5)?,
                registered_by: row.get(6)?,
                registered_at_ms: row.get(7)?,
                ended_at_ms: row.get(8)?,
                network_history: Vec::new(),
                held_on: row.get(9)?,
            })
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    for row in &mut rows {
        row.network_history = network_history(conn, &row.ref_kind, &row.session_ref)?;
    }
    Ok(rows)
}

impl RegistryStore {
    /// Every binding of the agent moving out under `cut_id`, with its network
    /// history, read in one snapshot. The snapshot also shows the open source
    /// move, which since admission has refused every new registration for
    /// the agent, and its plan, whose binding refs must still be present:
    /// a mismatch is refused `invalid_state` rather than carrying a set the
    /// receiver was never told about.
    pub fn export_session_bindings(
        &self,
        cut_id: &str,
        agent_id: &str,
    ) -> Result<Vec<CarriedBinding>, AgentMutationError> {
        self.read(|conn| {
            let row = crate::move_store::load_move(conn, cut_id).optional()?;
            // Prepare runs while the source has not yet published the cut;
            // a restarted source repeats it from the exported and sealed
            // steps, and every step here still refuses registration.
            let Some(row) = row.filter(|row| {
                row.agent_id == agent_id
                    && row.side == MoveSide::Source
                    && matches!(
                        row.step,
                        MoveStep::Preparing | MoveStep::Exported | MoveStep::Sealed
                    )
            }) else {
                return Ok(Err(session_refusal(
                    "invalid_state",
                    "no open source move freezes this agent",
                    None,
                )));
            };
            let mut rows = carried_rows(conn, agent_id)?;
            let planned = plan_binding_refs(&row.plan);
            let read: BTreeSet<MoveRef> = rows
                .iter()
                .map(|b| MoveRef {
                    ref_kind: b.ref_kind.clone(),
                    reference: b.session_ref.clone(),
                })
                .collect();
            if !planned.is_subset(&read)
                || rows.iter().any(|b| {
                    b.ended_at_ms.is_none()
                        && !planned.contains(&MoveRef {
                            ref_kind: b.ref_kind.clone(),
                            reference: b.session_ref.clone(),
                        })
                })
            {
                return Ok(Err(session_refusal(
                    "invalid_state",
                    "the agent's bindings differ from the admitted move",
                    None,
                )));
            }
            let origin: Option<String> = conn
                .query_row("SELECT machine_id FROM store_origin", [], |r| r.get(0))
                .optional()?;
            for binding in &mut rows {
                if !planned.contains(&MoveRef {
                    ref_kind: binding.ref_kind.clone(),
                    reference: binding.session_ref.clone(),
                }) && binding.held_on.is_none()
                {
                    let Some(origin) = &origin else {
                        return Ok(Err(session_refusal(
                            "invalid_state",
                            "store origin is unknown",
                            None,
                        )));
                    };
                    binding.held_on = Some(origin.clone());
                }
            }
            Ok(Ok(rows))
        })
        .map_err(|e: RegistryError| session_refusal("storage_error", e.to_string(), None))?
    }
}

/// Write the moved-in bindings inside the activation transaction. `refs` are
/// the move's binding refs; `carried` are the rows the source sent, already
/// checked when the stream was imported. Finished metadata outside `refs`
/// is installed too, but never creates a placeholder. A live ref with no
/// carried row gets a placeholder, as a move that carries nothing always did.
pub(crate) fn install_moved_bindings(
    tx: &Transaction<'_>,
    agent_id: &str,
    activated_at_ms: Option<i64>,
    refs: &[MoveRef],
    carried: &[CarriedBinding],
) -> rusqlite::Result<CarriedInstall> {
    let carried: BTreeMap<(&str, &str), &CarriedBinding> =
        carried.iter().map(|b| (b.key(), b)).collect();
    let mut outcome = CarriedInstall::default();
    let mut seen = BTreeSet::new();
    let metadata_refs: Vec<MoveRef> = carried
        .values()
        .filter(|b| b.held_on.is_some())
        .map(|b| MoveRef {
            ref_kind: b.ref_kind.clone(),
            reference: b.session_ref.clone(),
        })
        .collect();
    let origin: Option<String> = tx
        .query_row("SELECT machine_id FROM store_origin", [], |r| r.get(0))
        .optional()?;
    for r in refs.iter().chain(metadata_refs.iter()) {
        if !seen.insert((r.ref_kind.as_str(), r.reference.as_str())) {
            continue;
        }
        let row = carried
            .get(&(r.ref_kind.as_str(), r.reference.as_str()))
            .copied();
        // A binding this Mac already has keeps its own row and history
        // untouched; the carried copy is skipped without comparing the two.
        // That copy was opened or moved in here before, and this Mac's host
        // may have registered or ended it since, so replacing it could undo
        // local changes. Placeholders always followed the same rule through
        // `ON CONFLICT DO NOTHING`.
        let local = tx
            .query_row(
                "SELECT 1 FROM session_binding WHERE ref_kind=?1 AND ref=?2",
                params![r.ref_kind, r.reference],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if local {
            if row.is_some() {
                outcome.kept += 1;
            }
            continue;
        }
        let Some(row) = row else {
            insert_placeholder(tx, agent_id, activated_at_ms, r)?;
            continue;
        };
        if collides(tx, row)? {
            // A local host key wins too. Remote finished metadata must never
            // turn into a live placeholder that a later move would export.
            if row.held_on.is_some() {
                outcome.kept += 1;
                let local = tx.query_row(
                    "SELECT ref_kind,ref FROM session_binding
                     WHERE (harness=?1 AND session_id=?2) OR (runner=?3 AND session_id=?2)
                     ORDER BY ref_kind,ref LIMIT 1",
                    params![row.harness, row.session_id, row.runner],
                    |r| {
                        Ok(MoveRef {
                            ref_kind: r.get(0)?,
                            reference: r.get(1)?,
                        })
                    },
                )?;
                outcome.skipped_finished.push((r.clone(), local));
                continue;
            }
            // The unique indexes allow one binding per host session and one
            // per runner session. Another binding here already holds this
            // one's, so it lands as a placeholder, as it would have before
            // rows were carried, and the move goes on. Its history still
            // travels: it is keyed by the ref, which does not collide.
            insert_placeholder(tx, agent_id, activated_at_ms, r)?;
            outcome.placeholders += 1;
        } else {
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,
                     registered_by,registered_at_ms,runner,ended_at_ms,held_on)
                 VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11)",
                params![
                    row.ref_kind,
                    row.session_ref,
                    agent_id,
                    row.harness,
                    row.session_id,
                    row.project_root,
                    row.registered_by,
                    row.registered_at_ms,
                    row.runner,
                    row.ended_at_ms,
                    row.held_on
                        .as_deref()
                        .filter(|holder| Some(*holder) != origin.as_deref()),
                ],
            )?;
            outcome.installed += 1;
        }
        // Verbatim, including `since_ms` from the source Mac's clock: the
        // display rows these values are compared with were stamped by that
        // same clock.
        for entry in &row.network_history {
            tx.execute(
                "INSERT INTO session_network(ref_kind, ref, network, since_ms) VALUES(?1,?2,?3,?4)",
                params![row.ref_kind, row.session_ref, entry.network, entry.since_ms],
            )?;
        }
    }
    Ok(outcome)
}

fn insert_placeholder(
    tx: &Transaction<'_>,
    agent_id: &str,
    activated_at_ms: Option<i64>,
    r: &MoveRef,
) -> rusqlite::Result<()> {
    // A placeholder has no host session, runner or end time. All three stay
    // NULL until this Mac's host registers the session, and that
    // registration records the runner. Retention never prunes a row whose
    // end time is NULL, so a moved-in reference survives for a later move
    // back.
    tx.execute(
        "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
         VALUES(?1,?2,?3,NULL,NULL,'','move',?4) ON CONFLICT(ref_kind,ref) DO NOTHING",
        params![r.ref_kind, r.reference, agent_id, activated_at_ms],
    )?;
    Ok(())
}

/// Whether another binding on this Mac already holds the carried row's host
/// session or runner session.
fn collides(tx: &Transaction<'_>, row: &CarriedBinding) -> rusqlite::Result<bool> {
    if let (Some(harness), Some(session_id)) = (&row.harness, &row.session_id) {
        let taken = tx
            .query_row(
                "SELECT 1 FROM session_binding WHERE harness=?1 AND session_id=?2",
                params![harness, session_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if taken {
            return Ok(true);
        }
    }
    if let (Some(runner), Some(session_id)) = (&row.runner, &row.session_id) {
        let taken = tx
            .query_row(
                "SELECT 1 FROM session_binding WHERE runner=?1 AND session_id=?2",
                params![runner, session_id],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        if taken {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
#[path = "session_carry_tests.rs"]
mod tests;

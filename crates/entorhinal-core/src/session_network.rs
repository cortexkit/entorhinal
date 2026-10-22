//! Whether the agent behind a session binding could reach the network, as a
//! history of values per binding.
//!
//! The orchestrator knows from each session's plan whether its agent can
//! reach the network and writes that here as `yes`, `no` or `unknown`.
//! Readers stamp what they display with the value in effect at the time, so
//! the store keeps every change with the time it was recorded, oldest first.
//! The store keeps these values without interpreting them.
//!
//! Like the binding itself the history is machine-local: nothing here is
//! written to `registry_journal` or sent to the identity log, so `verify` and
//! journal replay never see it. Rows are keyed to their binding by
//! `(ref_kind, ref)` with a cascading foreign key, so every path that deletes
//! a binding (retention, clearing a store restored from another Mac) deletes
//! its history in the same statement.

use rusqlite::{params, OptionalExtension, Transaction};
use serde::Serialize;

use crate::{agent::AgentMutationError, session_refusal, RegistryStore};

/// Migration 16. The primary key starts with the binding's key, so it also
/// serves the per-binding history reads in time order and the cascade's
/// lookup of a deleted binding's rows.
pub const V16_SESSION_NETWORK: &str = r#"
CREATE TABLE session_network (
    ref_kind TEXT NOT NULL,
    ref TEXT NOT NULL,
    network TEXT NOT NULL CHECK(network IN ('yes','no','unknown')),
    since_ms INTEGER NOT NULL,
    PRIMARY KEY (ref_kind, ref, since_ms),
    FOREIGN KEY (ref_kind, ref) REFERENCES session_binding(ref_kind, ref) ON DELETE CASCADE
);
"#;

/// The values a session's network access may take.
pub const NETWORK_VALUES: [&str; 3] = ["yes", "no", "unknown"];

/// What a binding with no history reads as. Never `no`: the store must not
/// claim an agent was cut off from the network when nobody said so.
pub const NETWORK_ABSENT: &str = "unknown";

/// The most entries one binding's history may hold. A change beyond it is
/// refused `network_history_full` with nothing written, so one session can
/// never grow its history without bound.
pub const NETWORK_HISTORY_MAX: i64 = 1024;

/// One entry of a binding's network history: `network` took effect at
/// `since_ms` and held until the next entry's `since_ms`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct NetworkEntry {
    pub network: String,
    pub since_ms: i64,
}

/// What setting a session's network access did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkChange {
    /// A new entry was appended.
    Recorded(NetworkEntry),
    /// The value was already current; nothing was written and this is the
    /// current entry.
    Unchanged(NetworkEntry),
}

impl NetworkChange {
    pub fn entry(&self) -> &NetworkEntry {
        match self {
            Self::Recorded(entry) | Self::Unchanged(entry) => entry,
        }
    }
}

/// A network value to record with a registration, and the clock that stamps
/// it. The clock is read inside the registration's transaction, after every
/// check, so the stamp is the time the entry commits.
#[derive(Debug, Clone, Copy)]
pub struct NetworkStamp<'a> {
    pub network: &'a str,
    pub clock: fn() -> i64,
}

/// Refuse anything but `yes`, `no` or `unknown`, naming the field.
pub fn check_network(network: &str) -> Result<(), AgentMutationError> {
    if NETWORK_VALUES.contains(&network) {
        return Ok(());
    }
    Err(session_refusal(
        "invalid_request",
        "network must be yes, no or unknown",
        None,
    ))
}

/// The newest entry of a binding's history, if it has one.
fn current_entry(
    conn: &rusqlite::Connection,
    ref_kind: &str,
    session_ref: &str,
) -> rusqlite::Result<Option<NetworkEntry>> {
    conn.query_row(
        "SELECT network, since_ms FROM session_network WHERE ref_kind=?1 AND ref=?2
         ORDER BY since_ms DESC LIMIT 1",
        params![ref_kind, session_ref],
        |row| {
            Ok(NetworkEntry {
                network: row.get(0)?,
                since_ms: row.get(1)?,
            })
        },
    )
    .optional()
}

/// A binding's whole history, oldest first.
pub(crate) fn network_history(
    conn: &rusqlite::Connection,
    ref_kind: &str,
    session_ref: &str,
) -> rusqlite::Result<Vec<NetworkEntry>> {
    conn.prepare_cached(
        "SELECT network, since_ms FROM session_network WHERE ref_kind=?1 AND ref=?2
         ORDER BY since_ms",
    )?
    .query_map(params![ref_kind, session_ref], |row| {
        Ok(NetworkEntry {
            network: row.get(0)?,
            since_ms: row.get(1)?,
        })
    })?
    .collect()
}

/// What recording a value will do, decided before anything is written.
pub(crate) enum NetworkPlan {
    /// The value is already current: write nothing and report this entry.
    Unchanged(NetworkEntry),
    /// Append a new entry after the previous one, if there is one.
    Append { previous_since_ms: Option<i64> },
}

/// Every check for recording `network` on the binding of
/// `(ref_kind, ref)`: the value itself, then the history cap. It writes
/// nothing, so a caller may run it before its other writes and refuse
/// without leaving any of them behind.
pub(crate) fn plan_network(
    tx: &Transaction<'_>,
    ref_kind: &str,
    session_ref: &str,
    network: &str,
) -> Result<NetworkPlan, AgentMutationError> {
    check_network(network)?;
    let current = current_entry(tx, ref_kind, session_ref)?;
    if let Some(current) = current.as_ref().filter(|entry| entry.network == network) {
        return Ok(NetworkPlan::Unchanged(current.clone()));
    }
    let entries: i64 = tx.query_row(
        "SELECT COUNT(*) FROM session_network WHERE ref_kind=?1 AND ref=?2",
        params![ref_kind, session_ref],
        |row| row.get(0),
    )?;
    if entries >= NETWORK_HISTORY_MAX {
        return Err(session_refusal(
            "network_history_full",
            format!("this session's network history already holds {NETWORK_HISTORY_MAX} entries"),
            None,
        ));
    }
    Ok(NetworkPlan::Append {
        previous_since_ms: current.map(|entry| entry.since_ms),
    })
}

/// Carry out a plan from [`plan_network`] in the same transaction. The
/// binding must exist by now, because the new row references it.
pub(crate) fn apply_network_plan(
    tx: &Transaction<'_>,
    ref_kind: &str,
    session_ref: &str,
    network: &str,
    plan: NetworkPlan,
    clock: fn() -> i64,
) -> rusqlite::Result<NetworkChange> {
    let previous_since_ms = match plan {
        NetworkPlan::Unchanged(current) => return Ok(NetworkChange::Unchanged(current)),
        NetworkPlan::Append { previous_since_ms } => previous_since_ms,
    };
    // Readers find the value in effect at a time by the last entry at or
    // before it, which needs every entry's time to be later than the one
    // before. The wall clock can step back (a manual change or a time sync),
    // so a stamp at or below the previous entry is raised just past it.
    let now = clock();
    let since_ms = match previous_since_ms {
        Some(previous) => now.max(previous.saturating_add(1)),
        None => now,
    };
    tx.execute(
        "INSERT INTO session_network(ref_kind, ref, network, since_ms) VALUES(?1,?2,?3,?4)",
        params![ref_kind, session_ref, network, since_ms],
    )?;
    Ok(NetworkChange::Recorded(NetworkEntry {
        network: network.to_owned(),
        since_ms,
    }))
}

impl RegistryStore {
    /// Record whether the agent behind the binding of `{harness, session_id}`
    /// can reach the network, stamped with `clock` read when the entry
    /// commits. The current value again writes nothing and returns the
    /// current entry. Refusals: `invalid_request` for a value other than
    /// `yes`, `no` or `unknown`, `not_found` when no binding has this
    /// harness and session id, and `network_history_full` once the binding
    /// holds [`NETWORK_HISTORY_MAX`] entries.
    pub fn set_session_network(
        &self,
        harness: &str,
        session_id: &str,
        network: &str,
        clock: fn() -> i64,
    ) -> Result<NetworkChange, AgentMutationError> {
        // A malformed value is refused before the binding is looked up, so
        // it reads the same for a known and an unknown session.
        check_network(network)?;
        let outcome = self.db.with_conn_fenced(|tx| {
            let binding: Option<(String, String)> = tx
                .query_row(
                    "SELECT ref_kind, ref FROM session_binding WHERE harness=?1 AND session_id=?2",
                    params![harness, session_id],
                    |row| Ok((row.get(0)?, row.get(1)?)),
                )
                .optional()?;
            let Some((ref_kind, session_ref)) = binding else {
                return Ok(Err(session_refusal(
                    "not_found",
                    "no agent session is registered under this harness and session",
                    None,
                )));
            };
            // A refusal is returned inside `Ok`, which commits the
            // transaction; that is safe because the plan writes nothing.
            let plan = match plan_network(tx, &ref_kind, &session_ref, network) {
                Ok(plan) => plan,
                Err(refusal) => return Ok(Err(refusal)),
            };
            apply_network_plan(tx, &ref_kind, &session_ref, network, plan, clock).map(Ok)
        });
        match outcome {
            Ok(result) => result,
            Err(error) => Err(session_refusal("storage_error", error.to_string(), None)),
        }
    }

    /// The network history of the binding of `(ref_kind, ref)`, oldest
    /// first; empty when nothing was recorded or the binding is gone.
    pub fn session_network_history(
        &self,
        ref_kind: &str,
        session_ref: &str,
    ) -> Result<Vec<NetworkEntry>, crate::RegistryError> {
        self.read(|conn| network_history(conn, ref_kind, session_ref))
    }
}

#[cfg(test)]
#[path = "session_network_tests.rs"]
mod tests;

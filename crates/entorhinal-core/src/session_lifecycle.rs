//! When a bound session ended, which llm-runner hosts it, the per-agent list
//! of sessions, and the retention of ended bindings.
//!
//! All of it lives on `session_binding`, which is machine-local: nothing here
//! is written to `registry_journal` or sent to the identity log, so `verify`
//! and journal replay never see it.

use rusqlite::{params, OptionalExtension, Transaction};
use serde::Serialize;

use crate::{
    agent::AgentMutationError, session_network::network_history, session_refusal, RegistryError,
    RegistryStore,
};

/// Migration 15. `runner` names the module id of the llm-runner hosting the
/// session (for example `broca`); `ended_at_ms` is when the session was seen
/// to end. Rows written before this migration have neither. The first index
/// serves the per-agent list in its page order, newest registration first.
/// The second makes a runner's session id name one binding: a runner owns its
/// session namespace, so callers that know only `{runner, session}` can find
/// the harness and project root behind it. Rows with no runner are outside it.
pub const V15_SESSION_LIFECYCLE: &str = r#"
ALTER TABLE session_binding ADD COLUMN runner TEXT NULL;
ALTER TABLE session_binding ADD COLUMN ended_at_ms INTEGER NULL;
CREATE INDEX session_binding_agent
    ON session_binding(agent_id, registered_at_ms DESC, ref_kind, ref);
CREATE UNIQUE INDEX session_binding_runner
    ON session_binding(runner, session_id) WHERE runner IS NOT NULL;
"#;

/// Remote finished sessions retain metadata without pretending their data
/// moved. The partial index lets open-only paging skip the ended backlog.
pub const V17_SESSION_LOCATION: &str = r#"
ALTER TABLE session_binding ADD COLUMN held_on TEXT NULL;
CREATE INDEX session_binding_open_agent
    ON session_binding(agent_id, registered_at_ms DESC, ref_kind, ref)
    WHERE ended_at_ms IS NULL;
"#;

/// How long a binding is kept after its session ended. A binding that never
/// ended is never pruned, however old it is.
pub const SESSION_RETENTION_MS: i64 = 30 * 24 * 60 * 60 * 1000;

/// Rows deleted per retention transaction, so a large backlog never holds the
/// writer connection for long and other writes interleave between batches.
const PRUNE_BATCH: i64 = 500;

/// What ending a session found.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionEnd {
    /// No binding has this harness and session id.
    NotFound,
    /// The binding was open and is now ended at this time.
    Ended { ended_at_ms: i64 },
    /// The binding had already ended at this time; nothing was written.
    AlreadyEnded { ended_at_ms: i64 },
}

/// A clean end the liveness feed reported for one host session. `runner`,
/// when the feed named one, limits the stamp to a binding of that runner or
/// one with no runner recorded.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GoneSession {
    pub harness: String,
    pub session_id: String,
    pub runner: Option<String>,
    pub ended_at_ms: i64,
}

/// One row of the per-agent session list. `session` is the stored host
/// session id; it and `harness` are null for a reference that moved in from
/// another machine and has not been opened here yet.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct AgentSession {
    pub runner: Option<String>,
    pub session: Option<String>,
    pub harness: Option<String>,
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub session_ref: String,
    pub project_root: Option<String>,
    pub registered_at_ms: i64,
    pub ended_at_ms: Option<i64>,
    /// The network value currently in effect, `unknown` with no history.
    pub network: String,
    #[serde(rename = "heldOn", skip_serializing_if = "Option::is_none")]
    pub held_on: Option<String>,
}

/// Where the next page starts: just after this row in list order. Keyset
/// paging keeps a cursor valid across inserts, which land on earlier pages.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionCursor {
    pub registered_at_ms: i64,
    pub ref_kind: String,
    pub session_ref: String,
}

/// The binding a runner's session id names.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct RunnerSessionBinding {
    pub agent_id: String,
    pub runner: String,
    pub session: String,
    pub harness: Option<String>,
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub session_ref: String,
    pub project_root: Option<String>,
    pub ended_at_ms: Option<i64>,
    /// The network value currently in effect, `unknown` with no history.
    pub network: String,
    /// Every recorded network value, oldest first; empty with no history.
    pub network_history: Vec<crate::NetworkEntry>,
    #[serde(rename = "heldOn", skip_serializing_if = "Option::is_none")]
    pub held_on: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentSessionPage {
    pub sessions: Vec<AgentSession>,
    /// `None` on the last page.
    pub next: Option<SessionCursor>,
}

impl RegistryStore {
    /// Mark the binding of a host session ended. A runner named here fills a
    /// binding that has none, unless another binding already holds that
    /// runner and session id, in which case the end is still recorded and the
    /// runner left empty; one that differs from the recorded runner is
    /// refused `runner_mismatch` with nothing written. Ending an ended session
    /// returns its first end time and writes nothing, not even a missing
    /// runner.
    pub fn end_session(
        &self,
        harness: &str,
        session_id: &str,
        runner: Option<&str>,
        now_ms: i64,
    ) -> Result<SessionEnd, AgentMutationError> {
        let outcome = self.db.with_conn_fenced(|tx| {
            Ok(Self::end_session_in(
                tx, harness, session_id, runner, now_ms,
            ))
        });
        match outcome {
            Ok(result) => result,
            Err(error) => Err(session_refusal("storage_error", error.to_string(), None)),
        }
    }

    // A refusal is returned inside `Ok`, which commits the transaction. That
    // is safe because every check runs before the single write at the end.
    fn end_session_in(
        tx: &Transaction<'_>,
        harness: &str,
        session_id: &str,
        runner: Option<&str>,
        now_ms: i64,
    ) -> Result<SessionEnd, AgentMutationError> {
        let row: Option<(Option<String>, Option<i64>)> = tx
            .query_row(
                "SELECT runner, ended_at_ms FROM session_binding WHERE harness=?1 AND session_id=?2",
                params![harness, session_id],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;
        let Some((recorded, ended_at_ms)) = row else {
            return Ok(SessionEnd::NotFound);
        };
        if let (Some(recorded), Some(named)) = (recorded.as_deref(), runner) {
            if recorded != named {
                return Err(session_refusal(
                    "runner_mismatch",
                    format!("this session is hosted by runner {recorded}, not {named}"),
                    Some(serde_json::json!({"runner": recorded})),
                ));
            }
        }
        if let Some(ended_at_ms) = ended_at_ms {
            return Ok(SessionEnd::AlreadyEnded { ended_at_ms });
        }
        let runner = match runner {
            Some(named)
                if recorded.is_none() && runner_session_owner(tx, named, session_id)?.is_some() =>
            {
                None
            }
            named => named,
        };
        tx.execute(
            "UPDATE session_binding SET ended_at_ms=?3, runner=COALESCE(runner, ?4)
             WHERE harness=?1 AND session_id=?2",
            params![harness, session_id, now_ms, runner],
        )?;
        Ok(SessionEnd::Ended {
            ended_at_ms: now_ms,
        })
    }

    /// Stamp the end time on the open bindings of sessions the liveness feed
    /// reported `gone`, in one transaction. A binding that already ended keeps
    /// its first end time. Returns how many bindings were stamped.
    pub fn stamp_gone_sessions(&self, stamps: &[GoneSession]) -> Result<usize, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let mut statement = tx.prepare_cached(
                    "UPDATE session_binding SET ended_at_ms=?3
                     WHERE harness=?1 AND session_id=?2 AND ended_at_ms IS NULL
                       AND (?4 IS NULL OR runner IS NULL OR runner=?4)",
                )?;
                let mut stamped = 0;
                for stamp in stamps {
                    stamped += statement.execute(params![
                        stamp.harness,
                        stamp.session_id,
                        stamp.ended_at_ms,
                        stamp.runner
                    ])?;
                }
                Ok(stamped)
            })
            .map_err(RegistryError::Store)
    }

    /// Delete bindings whose session ended more than [`SESSION_RETENTION_MS`]
    /// before `now_ms`. A binding ended exactly at the boundary is kept. Every
    /// binding of an agent with an open move is kept, because the move still
    /// reads the agent's references; so is every binding that never ended,
    /// including references moved in from another machine and not yet reopened,
    /// which a later move back still needs. Returns how many were deleted.
    pub fn prune_ended_sessions(&self, now_ms: i64) -> Result<usize, RegistryError> {
        let cutoff = now_ms.saturating_sub(SESSION_RETENTION_MS);
        let mut pruned = 0;
        loop {
            let deleted = self
                .db
                .with_conn_fenced(|tx| {
                    tx.execute(
                        "DELETE FROM session_binding WHERE rowid IN (
                             SELECT rowid FROM session_binding
                             WHERE ended_at_ms IS NOT NULL AND ended_at_ms < ?1
                               AND agent_id NOT IN (
                                   SELECT agent_id FROM agent_move
                                   WHERE step NOT IN ('released','aborted','done','taken_over'))
                             LIMIT ?2)",
                        params![cutoff, PRUNE_BATCH],
                    )
                })
                .map_err(RegistryError::Store)?;
            pruned += deleted;
            if (deleted as i64) < PRUNE_BATCH {
                return Ok(pruned);
            }
        }
    }

    /// The binding a runner's session id names. A binding registered without
    /// a runner is not found here; callers fall back to looking it up by
    /// harness and session id.
    pub fn session_binding_for_runner(
        &self,
        runner: &str,
        session: &str,
    ) -> Result<Option<RunnerSessionBinding>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT agent_id, runner, session_id, harness, ref_kind, ref,
                        NULLIF(project_root,''), ended_at_ms, held_on
                 FROM session_binding WHERE runner=?1 AND session_id=?2",
                params![runner, session],
                |row| {
                    Ok(RunnerSessionBinding {
                        agent_id: row.get(0)?,
                        runner: row.get(1)?,
                        session: row.get(2)?,
                        harness: row.get(3)?,
                        ref_kind: row.get(4)?,
                        session_ref: row.get(5)?,
                        project_root: row.get(6)?,
                        ended_at_ms: row.get(7)?,
                        network: crate::NETWORK_ABSENT.to_owned(),
                        network_history: Vec::new(),
                        held_on: row.get(8)?,
                    })
                },
            )
            .optional()?
            .map(|mut binding| {
                // Read in the same snapshot as the binding.
                binding.network_history =
                    network_history(conn, &binding.ref_kind, &binding.session_ref)?;
                if let Some(current) = binding.network_history.last() {
                    binding.network = current.network.clone();
                }
                Ok(binding)
            })
            .transpose()
        })
    }

    /// One page of an agent's session bindings, newest registration first,
    /// ties in `(ref_kind, ref)` order. A retired or merged agent still lists
    /// its bindings; an id no agent ever had is refused `unknown_agent`. The
    /// agent check and the page are read in one snapshot from the read-only
    /// connection.
    pub fn agent_sessions(
        &self,
        agent_id: &str,
        after: Option<&SessionCursor>,
        limit: usize,
    ) -> Result<AgentSessionPage, AgentMutationError> {
        self.agent_sessions_filtered(agent_id, after, limit, false)
    }

    /// The same keyset page, optionally limited to bindings not yet ended.
    pub fn agent_sessions_filtered(
        &self,
        agent_id: &str,
        after: Option<&SessionCursor>,
        limit: usize,
        open: bool,
    ) -> Result<AgentSessionPage, AgentMutationError> {
        self.read(|conn| {
            if crate::agent::load_row(conn, agent_id)?.is_none() {
                return Ok(Err(session_refusal(
                    "unknown_agent",
                    format!("unknown agent {agent_id}"),
                    None,
                )));
            }
            let open_clause = if open { "AND ended_at_ms IS NULL" } else { "" };
            let mut statement = conn.prepare(&format!(
                "SELECT runner, session_id, harness, ref_kind, ref, NULLIF(project_root,''),
                        registered_at_ms, ended_at_ms,
                        COALESCE((SELECT network FROM session_network AS history
                                  WHERE history.ref_kind=session_binding.ref_kind
                                    AND history.ref=session_binding.ref
                                  ORDER BY since_ms DESC LIMIT 1), ?6), held_on
                 FROM session_binding
                  WHERE agent_id=?1
                    {open_clause}
                   AND (?2 IS NULL OR registered_at_ms < ?2
                        OR (registered_at_ms = ?2
                            AND (ref_kind > ?3 OR (ref_kind = ?3 AND ref > ?4))))
                 ORDER BY registered_at_ms DESC, ref_kind, ref
                 LIMIT ?5",
            ))?;
            let mut sessions = statement
                .query_map(
                    params![
                        agent_id,
                        after.map(|cursor| cursor.registered_at_ms),
                        after.map(|cursor| cursor.ref_kind.as_str()),
                        after.map(|cursor| cursor.session_ref.as_str()),
                        // One extra row says whether another page follows.
                        i64::try_from(limit).unwrap_or(i64::MAX).saturating_add(1),
                        crate::NETWORK_ABSENT,
                    ],
                    |row| {
                        Ok(AgentSession {
                            runner: row.get(0)?,
                            session: row.get(1)?,
                            harness: row.get(2)?,
                            ref_kind: row.get(3)?,
                            session_ref: row.get(4)?,
                            project_root: row.get(5)?,
                            registered_at_ms: row.get(6)?,
                            ended_at_ms: row.get(7)?,
                            network: row.get(8)?,
                            held_on: row.get(9)?,
                        })
                    },
                )?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            let next = if sessions.len() > limit {
                sessions.truncate(limit);
                sessions.last().map(|last| SessionCursor {
                    registered_at_ms: last.registered_at_ms,
                    ref_kind: last.ref_kind.clone(),
                    session_ref: last.session_ref.clone(),
                })
            } else {
                None
            };
            Ok(Ok(AgentSessionPage { sessions, next }))
        })?
    }
}

/// The `(ref_kind, ref, agent_id)` of the binding holding this runner and
/// session id, if any.
pub(crate) fn runner_session_owner(
    tx: &Transaction<'_>,
    runner: &str,
    session_id: &str,
) -> rusqlite::Result<Option<(String, String, String)>> {
    tx.query_row(
        "SELECT ref_kind, ref, agent_id FROM session_binding WHERE runner=?1 AND session_id=?2",
        params![runner, session_id],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
}

#[cfg(test)]
#[path = "session_lifecycle_tests.rs"]
mod tests;

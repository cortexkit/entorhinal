//! A conservative, machine-local residence decision for restoring sessions.

use crate::{claim_cache, move_store, session_lifecycle, RegistryError, RegistryStore};
use serde::Serialize;

#[derive(Debug, Serialize, PartialEq, Eq)]
pub struct SessionResidence {
    pub state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub agent_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub held_on: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub holder_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub epoch: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cut_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub read_at_ms: Option<i64>,
}

impl SessionResidence {
    fn state(state: &'static str) -> Self {
        Self {
            state,
            agent_id: None,
            held_on: None,
            holder: None,
            holder_name: None,
            epoch: None,
            cut_id: None,
            read_at_ms: None,
        }
    }
}

impl RegistryStore {
    /// The binding, move, claim and machine identity share one read transaction.
    /// No hosting evidence substitutes for a usable cached claim.
    pub fn session_residence(
        &self,
        runner: &str,
        session: &str,
    ) -> Result<SessionResidence, RegistryError> {
        self.read(|conn| {
            let Some(binding) =
                session_lifecycle::session_binding_for_key_in(conn, "runner", runner, session)?
            else {
                return Ok(SessionResidence::state("not_registered"));
            };
            if let Some(held_on) = binding.held_on {
                return Ok(SessionResidence {
                    agent_id: Some(binding.agent_id),
                    held_on: Some(held_on),
                    ..SessionResidence::state("elsewhere")
                });
            }
            let mut result = SessionResidence {
                agent_id: Some(binding.agent_id.clone()),
                ..SessionResidence::state("moving")
            };
            if move_store::where_moves(conn, &binding.agent_id)?
                .0
                .is_some()
            {
                return Ok(result);
            }
            let view = claim_cache::load_claim_view(conn, &binding.agent_id, self.is_foreign())?;
            result.state = if !view.known
                || view.claim.stale
                || view.claim.integrity_failed
                || (view.claim.epoch >= 1 && view.own.is_none())
            {
                "unknown"
            } else {
                match view.status() {
                    "held_here" => "here",
                    "held_elsewhere" => "elsewhere",
                    _ => "unclaimed",
                }
            };
            result.holder_name = view.holder_name(conn)?;
            result.epoch = view.known.then_some(view.claim.epoch);
            result.holder = view.claim.holder;
            result.cut_id = view.claim.cut_id;
            result.read_at_ms = view.read_at_ms;
            Ok(result)
        })
    }
}

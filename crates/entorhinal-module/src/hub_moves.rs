//! The fleet KV move and participant records. Local checkpoints still own the
//! participant work; a move record grants permission to advance that work.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use entorhinal_core::move_record::{
    self as wire, MoveParticipant, MoveReason, MoveRecord, MoveRecordState,
};

use crate::{door_client::SealedCut, HandlerError};

pub(super) const HOUR: i64 = 60 * 60 * 1000;
pub(super) const WEEK: i64 = 7 * 24 * HOUR;

pub(super) fn move_key(cut: &str) -> String {
    format!("move.{cut}")
}
pub(super) fn node_key(box_id: &str) -> String {
    format!("node.{box_id}")
}

pub(super) fn conflict() -> HandlerError {
    HandlerError::new("move_state_conflict", "the move and residence do not agree")
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum State {
    Published,
    Staged,
    Handing,
    Handed,
    Activated,
    Released,
    Aborted,
}

impl State {
    pub(super) fn terminal(self) -> bool {
        matches!(self, Self::Released | Self::Aborted)
    }

    pub(super) fn name(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Staged => "staged",
            Self::Handing => "handing",
            Self::Handed => "handed",
            Self::Activated => "activated",
            Self::Released => "released",
            Self::Aborted => "aborted",
        }
    }

    fn successors(self) -> &'static [Self] {
        match self {
            Self::Published => &[Self::Staged, Self::Aborted],
            Self::Staged => &[Self::Handing, Self::Aborted],
            Self::Handing => &[Self::Handed, Self::Activated, Self::Aborted],
            Self::Handed => &[Self::Activated, Self::Aborted],
            Self::Activated => &[Self::Released],
            Self::Released | Self::Aborted => &[],
        }
    }

    fn writer(self, source: bool, source_can_abort: bool) -> bool {
        match self {
            Self::Published | Self::Handing | Self::Handed | Self::Released => source,
            Self::Staged | Self::Activated => !source,
            Self::Aborted => !source || source_can_abort,
        }
    }

    fn reachable(self, to: Self, source: bool, source_can_abort: bool) -> bool {
        self.successors().iter().any(|next| {
            if *next == to {
                to.writer(
                    source,
                    source_can_abort && matches!(self, Self::Published | Self::Staged),
                )
            } else {
                next.reachable(to, source, source_can_abort)
            }
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Move {
    pub v: u64,
    pub cut_id: String,
    pub agent_id: String,
    pub source: String,
    pub receiver: String,
    pub gen_n: i64,
    pub state: State,
    pub participants: Vec<MoveParticipant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pin_digest: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub skipped: Vec<String>,
    pub reason: Option<MoveReason>,
    pub detail: Option<Value>,
    pub cut: SealedCut,
}

impl Move {
    pub(super) fn published(plan: &MoveRecord, gen_n: i64, cut: SealedCut) -> Self {
        Self {
            v: 1,
            cut_id: plan.cut_id.clone(),
            agent_id: plan.agent_id.clone(),
            source: plan.from.clone(),
            receiver: plan.to.clone(),
            gen_n,
            state: State::Published,
            participants: plan.participants.clone(),
            pin_digest: plan.pin_digest.clone(),
            skipped: Vec::new(),
            reason: None,
            detail: None,
            cut,
        }
    }

    // The participant drivers share the legacy plan representation; only this
    // module translates it. Decisions always read the actual fleet KV state.
    pub(super) fn participant_plan(&self) -> MoveRecord {
        MoveRecord {
            v: self.v,
            cut_id: self.cut_id.clone(),
            agent_id: self.agent_id.clone(),
            from: self.source.clone(),
            to: self.receiver.clone(),
            claim_version: self.gen_n as u64,
            participants: self.participants.clone(),
            pin_digest: self.pin_digest.clone(),
            state: match self.state {
                State::Published => MoveRecordState::Published,
                State::Staged => MoveRecordState::Staged,
                State::Handing | State::Handed => MoveRecordState::Claimed,
                State::Activated => MoveRecordState::Activated,
                State::Released => MoveRecordState::Released,
                State::Aborted => MoveRecordState::Aborted,
            },
            skipped: self.skipped.clone(),
            reason: self.reason.clone(),
            detail: self.detail.clone(),
        }
    }

    pub(super) fn pin(&self, other: &Self) -> Result<(), HandlerError> {
        let mut expected = self.clone();
        expected.state = other.state;
        expected.skipped.clone_from(&other.skipped);
        expected.reason.clone_from(&other.reason);
        expected.detail.clone_from(&other.detail);
        if expected != *other {
            return Err(conflict());
        }
        Ok(())
    }

    pub(super) fn judge(
        &self,
        cut: &str,
        writer: &str,
        old: Option<&Self>,
        source_can_abort: bool,
    ) -> Result<(), HandlerError> {
        if self.v != 1
            || self.cut_id != cut
            || !crate::door_client::valid_cut_id(cut)
            || !entorhinal_core::agent::valid_agent_id(&self.agent_id)
            || !crate::hub_membership::box_id(&self.source)
            || !crate::hub_membership::box_id(&self.receiver)
            || self.source == self.receiver
            || self.gen_n < 1
            || self.gen_n.checked_add(2).is_none()
            || self.cut.manifest_sha256.is_empty()
            || self.cut.sealed_key.is_empty()
            || (writer != self.source && writer != self.receiver)
        {
            return Err(conflict());
        }
        let source = writer == self.source;
        if let Some(old) = old {
            old.pin(self)?;
            if !matches!(old.state, State::Published) && self.skipped != old.skipped {
                return Err(conflict());
            }
        }
        if !(old.is_none() && self.state == State::Published && source)
            && !old.map_or(State::Published, |old| old.state).reachable(
                self.state,
                source,
                source_can_abort,
            )
        {
            return Err(conflict());
        }
        wire::encode_move_record(&format!("move:{cut}"), &self.participant_plan())
            .map_err(|err| HandlerError::new(err.code(), err.to_string()))?;
        Ok(())
    }

    pub(super) fn check_send(&self, old: Option<State>, source: bool) -> Result<(), HandlerError> {
        let valid = match old {
            None => self.state == State::Published && source,
            Some(old) => {
                old.successors().contains(&self.state)
                    && self
                        .state
                        .writer(source, matches!(old, State::Published | State::Staged))
            }
        };
        if valid {
            Ok(())
        } else {
            Err(conflict())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub(super) struct SeenMove {
    pub body: Move,
    pub revision: u64,
    pub writer: String,
    pub writer_ep: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(super) struct Participant {
    pub module: String,
    pub ref_kinds: Vec<String>,
    pub optional: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Node {
    pub v: u64,
    #[serde(rename = "box")]
    pub box_id: String,
    pub participants: Vec<Participant>,
    #[serde(default)]
    pub engram_hub_cuts: bool,
    pub written_at: i64,
}

impl Node {
    pub(super) fn supports(&self, participant: &MoveParticipant) -> bool {
        self.participants.iter().any(|p| {
            p.module == participant.module_id
                && participant
                    .refs
                    .iter()
                    .all(|r| p.ref_kinds.contains(&r.ref_kind))
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(super) enum Decision {
    Abort,
    Activate,
}

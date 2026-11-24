//! Restartable source half of the move protocol. Recovery uses the participant
//! and ref set recorded at admission, never a fresh daemon discovery result.

use crate::{
    door_client::{
        CutItem, CutRequest as DoorCut, DoorError, HandoffState, HubStatusReply, SealRequest,
        SealState, StatusReply,
    },
    hub_moves::{self, SeenMove, State as HubState},
    hub_residence::{MovePosition, MoveTransfer, Residence},
    move_driver::{
        backoff, digest_reason, participant_action, skipped_streams, PeerRefusal, RefusalLog,
        SkippedStreams, Worker, POLL,
    },
    participant_client::{
        AgentRef, ClientError, CutRequest, ParticipantClient, Refs, ReleaseRequest,
    },
    participant_discovery::Participant,
    HandlerError,
};
use cortexkit_role_agent_data::{errors::DriverAction, refs::CutId};
use entorhinal_core::{
    move_record::{
        self as wire, ExternalRef, MoveParticipant, MoveReason, MoveRecord,
        MoveRecordState as State,
    },
    move_store::{AgentMove, MoveStep as Step},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Progress {
    #[serde(default)]
    aborted: Vec<String>,
    #[serde(default)]
    prepared: bool,
    #[serde(default)]
    items: Vec<CutItem>,
    /// Once the receiver has staged an optional plan, it cannot change which
    /// participants the source must leave untouched, even across a restart.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skipped: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    skipped_streams: SkippedStreams,
    #[serde(default)]
    errors: Vec<Value>,
    #[serde(default)]
    pending: Option<PendingCall>,
    #[serde(default)]
    retry_at_ms: i64,
    #[serde(default)]
    seal_state: Option<SealState>,
    #[serde(default)]
    handoff_state: Option<HandoffState>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    release_holder_box: Option<String>,
    #[serde(default)]
    enforced: BTreeMap<String, bool>,
    #[serde(default)]
    contract_violation: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    published_at_ms: Option<i64>,
    /// The refusal code the door gave for this cut's `cut.seal`, recorded when
    /// the refusal happened. A refused seal created no cut at the door, so
    /// cleanup must neither re-send the seal (the door would refuse it again,
    /// forever) nor abort a cut the door never held. A lost seal reply leaves
    /// this unset: the door may hold the cut, so cleanup re-seals and aborts.
    /// Optional and absent by default, so rows written before this field
    /// existed still decode.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seal_refused: Option<String>,
    /// A failed hub upload cannot be restarted by replaying the seal.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    terminal_hub_seal: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    sealed_cut: Option<crate::door_client::SealedCut>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    handing: Option<(SeenMove, MoveTransfer)>,
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    publish_started: bool,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    exports_with_items: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
struct PendingCall {
    module_id: String,
    op: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct Plan {
    agent_id: String,
    from: String,
    to: String,
    claim_version: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub_gen: Option<i64>,
    participants: Vec<MoveParticipant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pin_digest: Option<String>,
    #[serde(default, rename = "_source")]
    source: Progress,
}

impl Plan {
    fn hub_record(&self, cut: &str) -> Result<hub_moves::Move, Failure> {
        Ok(hub_moves::Move::published(
            &self.record(cut),
            self.hub_gen.ok_or_else(state_conflict)?,
            self.source.sealed_cut.clone().ok_or_else(state_conflict)?,
        ))
    }
    fn record(&self, cut: &str) -> MoveRecord {
        MoveRecord {
            v: 1,
            cut_id: cut.into(),
            agent_id: self.agent_id.clone(),
            from: self.from.clone(),
            to: self.to.clone(),
            claim_version: self.claim_version,
            participants: self.participants.clone(),
            pin_digest: self.pin_digest.clone(),
            state: State::Published,
            skipped: Vec::new(),
            reason: None,
            detail: None,
        }
    }
}

fn state_conflict() -> Failure {
    hub_failure(hub_moves::conflict())
}

fn record_unavailable() -> Failure {
    let mut failure = Failure::new("hub.move", "unavailable");
    failure.diagnostic = true;
    failure
}

fn hub_failure(error: HandlerError) -> Failure {
    let mut failure = Failure::new("hub.move", &error.code);
    failure.detail = error
        .detail
        .unwrap_or_else(|| json!({"message":error.message}));
    failure.diagnostic = error.code != "record_too_large";
    failure
}

struct Failure {
    code: String,
    detail: Value,
    op: String,
    module: Option<String>,
    diagnostic: bool,
    unknown: bool,
    contract_violation: bool,
    terminal_hub_seal: bool,
    /// Set when the door or a participant answered with a refusal (or a
    /// terminal seal state), as opposed to a lost reply or a local store or
    /// record error. Boxed to keep `Result<_, Failure>` small.
    refusal: Option<Box<PeerRefusal>>,
}

impl Failure {
    fn new(op: &str, code: &str) -> Self {
        Self {
            code: code.into(),
            detail: Value::Null,
            op: op.into(),
            module: None,
            diagnostic: false,
            unknown: false,
            contract_violation: false,
            terminal_hub_seal: false,
            refusal: None,
        }
    }
    fn participant(op: &str, module: &str, error: ClientError) -> Self {
        let (code, detail, unknown, contract_violation, message) = match error {
            ClientError::Refused(r) => (
                r.code,
                r.detail.unwrap_or(Value::Null),
                false,
                false,
                r.message,
            ),
            ClientError::ContractViolation {
                code,
                detail,
                message,
                ..
            } => (code, detail.unwrap_or(Value::Null), false, true, message),
            ClientError::OutcomeUnknown => ("unavailable".into(), Value::Null, true, false, None),
            ClientError::InvalidReply(detail) => {
                ("unavailable".into(), json!(detail), true, false, None)
            }
        };
        Self {
            code,
            detail,
            op: op.into(),
            module: Some(module.into()),
            diagnostic: false,
            refusal: (!unknown).then(|| Box::new(PeerRefusal { message })),
            unknown,
            contract_violation,
            terminal_hub_seal: false,
        }
    }
    fn door(op: &str, error: DoorError) -> Self {
        let mut failure = Self::new(op, error.code());
        match error {
            DoorError::Refused {
                detail, message, ..
            } => {
                failure.detail = detail.unwrap_or(Value::Null);
                failure.refuse(message);
            }
            DoorError::OutcomeUnknown | DoorError::InvalidReply(_) => {
                failure.code = "unavailable".into();
                failure.unknown = true;
            }
        }
        failure
    }
    fn hub_seal(op: &str, error: DoorError) -> Self {
        let mut failure = Self::door(op, error);
        failure.terminal_hub_seal = matches!(
            failure.code.as_str(),
            "cut_store_full" | "hub_unavailable" | "key_unavailable"
        );
        failure
    }
    fn refuse(&mut self, message: Option<String>) {
        self.refusal = Some(Box::new(PeerRefusal { message }));
    }
    fn refused(&self) -> bool {
        self.refusal.is_some()
    }
    fn message(&self) -> Option<&str> {
        self.refusal.as_ref().and_then(|r| r.message.as_deref())
    }
    fn hold_with_message(&self, mut hold: Value) -> Value {
        if let Some(message) = self
            .refusal
            .as_deref()
            .filter(|_| self.module.is_some())
            .and_then(PeerRefusal::hold_message)
        {
            hold["message"] = json!(message);
        }
        hold
    }
    fn detail(&self) -> Value {
        let mut detail = json!({"op":self.op,"code":self.code,"detail":self.detail,
            "contract_violation":self.contract_violation});
        if let Some(module) = &self.module {
            detail["module_id"] = json!(module);
        }
        if let Some(message) = self.message() {
            detail["message"] = json!(message);
        }
        if let Some(reason) = digest_reason(&self.code, Some(&self.detail)) {
            detail["reason"] = json!(reason);
        }
        detail
    }
    fn action(&self) -> Option<DriverAction> {
        if self.op == "route.open" {
            // No participant answered, so there is no participant refusal to map to a
            // driver action: retry.
            return Some(DriverAction::Retry);
        }
        self.module
            .as_ref()
            .and_then(|_| participant_action(&self.op, &self.code))
    }
    fn retryable(&self) -> bool {
        !self.contract_violation
            && !self.terminal_hub_seal
            && if self.module.is_some() {
                matches!(
                    self.action(),
                    Some(DriverAction::RetryWithinStep | DriverAction::Retry)
                )
            } else {
                matches!(
                    self.code.as_str(),
                    "cut_not_ready"
                        | "unavailable"
                        | "handoff_not_covered"
                        | "cut_store_full"
                        | "hub_unavailable"
                )
            }
    }
}
impl From<HandlerError> for Failure {
    fn from(e: HandlerError) -> Self {
        let mut failure = Self::new("store", &e.code);
        failure.detail = json!(e.message);
        failure.diagnostic = true;
        failure
    }
}
pub(crate) struct Source {
    worker: Worker,
    cut: String,
    clients: BTreeMap<String, ParticipantClient>,
    resumed: bool,
    last_poll: Option<tokio::time::Instant>,
    abort_held: bool,
    refusals: RefusalLog,
}

impl Source {
    pub(crate) fn new(worker: Worker, cut: String) -> Self {
        Self {
            worker,
            cut,
            clients: BTreeMap::new(),
            resumed: false,
            last_poll: None,
            abort_held: false,
            refusals: RefusalLog::default(),
        }
    }

    /// Resume participant aborts after an operator repairs corrupted undo bytes.
    /// A digest refusal pauses cleanup for this driver instance; restarting
    /// retries once, then pauses again if the participant still refuses.
    pub(crate) fn retry_held_abort(&mut self) {
        self.abort_held = false;
    }

    pub(crate) async fn run(mut self) {
        let mut signals = self.worker.wake.subscribe();
        let mut delay = Duration::ZERO;
        loop {
            tokio::select! {
                _ = tokio::time::sleep(delay) => {},
                _ = signals.notified() => {},
            }
            signals.handled();
            if self.worker.store.upgrade().is_none() {
                return;
            }
            if self
                .worker
                .consume_abort_retry(&self.cut)
                .await
                .unwrap_or(false)
            {
                self.retry_held_abort();
            }
            match self.tick().await {
                Ok(Some(next)) => delay = next,
                Ok(None) => return,
                Err(_) => delay = POLL,
            }
            #[cfg(test)]
            signals.finished();
        }
    }

    async fn save(&self, row: &mut AgentMove, plan: &Plan, clear: bool) -> Result<(), Failure> {
        row.plan =
            serde_json::to_value(plan).map_err(|_| Failure::new("plan.save", "plan_malformed"))?;
        if plan.hub_gen.is_some() && row.step == Step::Released {
            // The receiver releases the hub objects; the source still has a
            // local reservation. Queue its release with the terminal checkpoint
            // so a restart cannot miss it. The store preserves any prior outcome.
            row.plan["_cut_release"] = json!({"state":"pending","attempts":0,"retry_at_ms":0});
        }
        self.worker.save(row, clear).await?;
        Ok(())
    }

    fn change_step(row: &mut AgentMove, plan: &mut Plan, step: Step) {
        row.step = step;
        row.attempts = 0;
        row.hold = None;
        plan.source.retry_at_ms = 0;
    }

    pub(crate) async fn tick(&mut self) -> Result<Option<Duration>, HandlerError> {
        let mut row = self.worker.row(&self.cut).await?;
        if row.step.is_terminal() {
            return Ok(None);
        }
        let mut plan: Plan = serde_json::from_value(row.plan.clone())
            .map_err(|e| HandlerError::new("plan_malformed", e.to_string()))?;
        if plan.hub_gen.is_none() {
            return Err(HandlerError::new(
                "plan_malformed",
                "hub move generation is missing",
            ));
        }
        // Poll the shared move record even during a long backoff. Reads that
        // fail signature, author, or persisted-plan checks cannot advance the row.
        let watching = matches!(row.step, Step::Sealed | Step::Published | Step::Releasing);
        let poll = watching || self.last_poll.is_none_or(|last| last.elapsed() >= POLL);
        let mut hub_seen = None;
        let observed = if poll {
            self.last_poll = Some(tokio::time::Instant::now());
            let read = self.read_hub_move(&plan).await.map(|seen| {
                hub_seen = seen;
                hub_seen
                    .as_ref()
                    .map(|seen| (seen.body.participant_plan(), seen.revision))
            });
            match read {
                Ok(record) => record,
                Err(error) => return self.failed(&mut row, &mut plan, error).await,
            }
        } else {
            None
        };
        if let Some((record, version)) = observed.as_ref().filter(|_| watching) {
            row.record_version = *version as i64;
            if matches!(
                record.state,
                State::Staged | State::Claimed | State::Activated | State::Released
            ) && plan.source.skipped.is_none()
                && plan.participants.iter().any(|p| p.optional)
            {
                plan.source.skipped_streams = skipped_streams(&plan.source.items, &record.skipped);
                plan.source.skipped = Some(record.skipped.clone());
                self.save(&mut row, &plan, false)
                    .await
                    .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            }
            if record.state == State::Aborted && row.step != Step::Releasing {
                let reason = record.reason.clone().map(String::from);
                if row.reason != reason || row.abort_requested.is_none() {
                    row.detail = record.detail.clone();
                }
                row.reason = reason;
                row.abort_requested = Some(json!({"reason":row.reason,"detail":row.detail}));
                self.save(&mut row, &plan, false)
                    .await
                    .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            } else if record.state != State::Published
                && row.abort_requested.is_some()
                && !hub_seen
                    .as_ref()
                    .is_some_and(|seen| seen.body.state == HubState::Staged)
            {
                self.refuse_abort(&mut row, &plan, record.state).await?;
            }
            if record.state == State::Activated && row.step != Step::Releasing {
                Self::change_step(&mut row, &mut plan, Step::Releasing);
                self.save(&mut row, &plan, true)
                    .await
                    .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            }
        }
        if plan.source.contract_violation.is_some() {
            if observed
                .as_ref()
                .is_some_and(|(record, _)| record.state == State::Released)
            {
                Self::change_step(&mut row, &mut plan, Step::Released);
                self.save(&mut row, &plan, true)
                    .await
                    .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
                return Ok(None);
            }
            return Ok(Some(POLL));
        }
        if self.abort_held {
            return Ok(Some(POLL));
        }
        if (self.worker.clock)() < plan.source.retry_at_ms {
            let remaining =
                Duration::from_millis((plan.source.retry_at_ms - (self.worker.clock)()) as u64);
            return Ok(Some(remaining.min(POLL)));
        }
        let result = if row.abort_requested.is_some() && row.step != Step::Releasing {
            self.cleanup(&mut row, &mut plan, observed, hub_seen.as_ref())
                .await
        } else {
            self.advance(&mut row, &mut plan, observed, hub_seen.as_ref())
                .await
        };
        match result {
            Ok(delay) => Ok(if row.step.is_terminal() {
                None
            } else {
                Some(delay)
            }),
            Err(error) => self.failed(&mut row, &mut plan, error).await,
        }
    }

    async fn failed(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
        error: Failure,
    ) -> Result<Option<Duration>, HandlerError> {
        if error.code == "move_state_conflict" && !error.terminal_hub_seal {
            // An inconsistent move/residence pair cannot authorize an abort or a handover.
            let detail = error.detail();
            plan.source.contract_violation = Some(detail.clone());
            row.hold = Some(error.hold_with_message(detail.clone()));
            row.detail = Some(detail);
            self.save(row, plan, false)
                .await
                .map_err(|failure| HandlerError::new(&failure.code, failure.detail.to_string()))?;
            return Ok(Some(POLL));
        }
        if error.refused() {
            self.refusals.note(
                &self.cut,
                error.module.as_deref().unwrap_or("engram"),
                &error.op,
                &error.code,
                error.message(),
            );
        }
        let busy_retry = !error.contract_violation
            && error.action() == Some(DriverAction::RetryLater)
            && error.op == "agent_data.prepare"
            && row.attempts < 8;
        let cleanup = row.abort_requested.is_some();
        if !error.contract_violation
            && error.op == "agent_data.abort"
            && error.action() == Some(DriverAction::RetryAfterOperator)
        {
            self.abort_held = true;
            let mut detail = error.detail();
            detail["state"] = json!("stuck");
            detail["step"] = json!(row.step);
            detail["move_error"] = json!(true);
            row.detail = Some(detail.clone());
            row.hold = Some(error.hold_with_message(detail));
            plan.source.retry_at_ms = 0;
            self.save(row, plan, false)
                .await
                .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            return Ok(Some(POLL));
        }
        let unknown_action = error.module.is_some() && error.action().is_none();
        if error.contract_violation || unknown_action {
            let detail = error.detail();
            if !plan.source.errors.contains(&detail) {
                plan.source.errors.push(detail.clone());
            }
            if unknown_action || cleanup || row.step == Step::Releasing {
                // Unknown actions cannot be executed safely. Release cannot be
                // rolled back, and cleanup cannot trust a broken contract.
                // Stop calls durably rather than retrying in either case.
                plan.source.contract_violation = Some(detail.clone());
                row.detail = Some(detail.clone());
                row.hold = Some(error.hold_with_message(detail));
                plan.source.retry_at_ms = 0;
                self.save(row, plan, false)
                    .await
                    .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
                return Ok(Some(POLL));
            }
        }
        let retry = !error.contract_violation
            && (error.diagnostic
                || error.unknown
                || error.retryable()
                || busy_retry
                || cleanup
                || row.step == Step::Releasing);
        if error.diagnostic {
            let diagnostic = json!({"code":error.code,"op":error.op});
            if !plan.source.errors.contains(&diagnostic) {
                plan.source.errors.push(diagnostic);
            }
        }
        if retry {
            row.attempts += 1;
            let delay = backoff(row.attempts);
            plan.source.retry_at_ms =
                (self.worker.clock)().saturating_add(delay.as_millis() as i64);
            row.hold = Some(error.hold_with_message(
                json!({"code":error.code,"module_id":error.module,"op":error.op,
                "move_error":row.step == Step::Releasing && !error.retryable()}),
            ));
            self.save(row, plan, false)
                .await
                .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            Ok(Some(delay.min(POLL)))
        } else {
            let reason = if error.terminal_hub_seal {
                plan.source.terminal_hub_seal = true;
                MoveReason::from(error.code.clone())
            } else if error.op.starts_with("cut.") {
                MoveReason::DoorRefused
            } else if matches!(error.code.as_str(), "move_too_large" | "record_too_large") {
                MoveReason::MoveTooLarge
            } else if error.code == "participant_unavailable_on_target" {
                MoveReason::Other(error.code.clone())
            } else if error.code == "inbox_full" {
                MoveReason::ReceiverInboxFull
            } else {
                MoveReason::ParticipantRefused
            };
            let reason = String::from(reason);
            // Only a seal refused before the door ever reported the cut proves
            // the door holds nothing; once the step is sealed the door
            // published the cut, whatever a later replay answers.
            if error.op == "cut.seal" && error.refused() && row.step == Step::Exported {
                plan.source.seal_refused = Some(error.code.clone());
            }
            let detail = error.detail();
            let detail = if error.terminal_hub_seal {
                wire::bounded_detail(&detail)
            } else {
                detail
            };
            row.abort_requested = Some(json!({"reason":reason,"detail":detail}));
            row.reason = Some(reason);
            row.detail = Some(detail);
            self.save(row, plan, false)
                .await
                .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            Ok(Some(Duration::ZERO))
        }
    }

    async fn client(&mut self, module: &str) -> Result<ParticipantClient, Failure> {
        if let Some(client) = self.clients.get(module) {
            return Ok(client.clone());
        }
        let transport = self
            .worker
            .discovery
            .open_participant(module)
            .await
            .map_err(|e| {
                let mut failure = Failure::new("route.open", "unavailable");
                failure.module = Some(module.into());
                failure.detail = json!({"error":format!("{e:?}")});
                failure
            })?;
        let client = ParticipantClient::new(&Participant {
            module_id: module.into(),
            transport,
        });
        self.clients.insert(module.into(), client.clone());
        Ok(client)
    }

    fn request(&self, participant: &MoveParticipant) -> Result<CutRequest, Failure> {
        Ok(CutRequest::new(
            Refs::new(
                participant
                    .refs
                    .iter()
                    .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
                    .collect(),
            )
            .map_err(|_| Failure::new("plan.read", "invalid_refs"))?,
            CutId::new(self.cut.clone())
                .map_err(|_| Failure::new("plan.read", "invalid_cut_id"))?,
        ))
    }

    fn hub(&self) -> Result<std::sync::Arc<Residence>, Failure> {
        self.worker.hub().ok_or_else(|| {
            hub_failure(HandlerError::new(
                "hub_not_ready",
                "hub move watch is not installed",
            ))
        })
    }

    async fn read_hub_move(&self, plan: &Plan) -> Result<Option<SeenMove>, Failure> {
        let seen = self
            .hub()?
            .watched_move(&self.cut)
            .await
            .map_err(hub_failure)?;
        if let Some(seen) = &seen {
            plan.hub_record(&self.cut)?
                .pin(&seen.body)
                .map_err(hub_failure)?;
            if plan
                .source
                .skipped
                .as_ref()
                .is_some_and(|skipped| skipped != &seen.body.skipped)
            {
                return Err(state_conflict());
            }
        }
        Ok(seen)
    }

    async fn check_target(&self, plan: &Plan) -> Result<(), Failure> {
        let Some(target) = self
            .hub()?
            .watched_node(&plan.to)
            .await
            .map_err(hub_failure)?
        else {
            return Ok(());
        };
        for p in &plan.participants {
            if !p.optional
                && !target.supports(p)
                && plan.source.exports_with_items.contains(&p.module_id)
            {
                let mut failure = Failure::new("move.export", "participant_unavailable_on_target");
                failure.detail = json!({"module":p.module_id});
                return Err(failure);
            }
        }
        Ok(())
    }

    async fn freeze_export(&mut self, row: &mut AgentMove, plan: &mut Plan) -> Result<(), Failure> {
        let mut participants = plan.participants.clone();
        let mut items = Vec::new();
        for participant in &mut participants {
            let client = self.client(&participant.module_id).await?;
            let request = self.request(participant)?;
            plan.source.pending = Some(PendingCall {
                module_id: participant.module_id.clone(),
                op: "agent_data.prepare".into(),
            });
            self.save(row, plan, false).await?;
            let reply = match client.prepare(&request).await {
                Ok(reply) => reply,
                Err(e) => {
                    let failure =
                        Failure::participant("agent_data.prepare", &participant.module_id, e);
                    if !failure.unknown {
                        plan.source.pending = None;
                        self.save(row, plan, false).await?;
                    }
                    return Err(failure);
                }
            };
            participant.external = reply
                .external
                .into_iter()
                .map(|external| {
                    serde_json::to_value(external)
                        .and_then(serde_json::from_value::<ExternalRef>)
                        .map_err(|_| Failure::new("agent_data.prepare", "invalid_external_ref"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            items.extend(reply.items.into_iter().map(|item| CutItem {
                session_ref: item.session_ref,
                stream: item.stream,
                end_offset: item.end_offset,
                digest: item.digest,
            }));
            plan.source.pending = None;
            self.save(row, plan, false).await?;
            if row.abort_requested.is_some() {
                return Ok(());
            }
        }
        if plan.source.prepared && (participants != plan.participants || items != plan.source.items)
        {
            let mut error = Failure::new("agent_data.prepare", "plan_mismatch");
            error.diagnostic = true;
            return Err(error);
        }
        plan.participants = participants;
        plan.source.items = items;
        plan.source.prepared = true;
        self.save(row, plan, false).await?;
        wire::encode_move_record(&format!("move:{}", self.cut), &plan.record(&self.cut)).map_err(
            |e| {
                let mut failure = Failure::new("move.encode", e.code());
                failure.detail = json!({"limit":wire::MOVE_BODY_CAP});
                failure
            },
        )?;
        for participant in plan.participants.clone() {
            let client = self.client(&participant.module_id).await?;
            plan.source.pending = Some(PendingCall {
                module_id: participant.module_id.clone(),
                op: "agent_data.export".into(),
            });
            self.save(row, plan, false).await?;
            let result = client.export(&self.request(&participant)?).await;
            match result {
                Ok(export) => {
                    if !export.items.is_empty()
                        && !plan
                            .source
                            .exports_with_items
                            .contains(&participant.module_id)
                    {
                        plan.source.exports_with_items.push(participant.module_id);
                    }
                }
                Err(e) => {
                    let failure =
                        Failure::participant("agent_data.export", &participant.module_id, e);
                    if !failure.unknown {
                        plan.source.pending = None;
                        self.save(row, plan, false).await?;
                    }
                    return Err(failure);
                }
            }
            plan.source.pending = None;
            self.save(row, plan, false).await?;
            if row.abort_requested.is_some() {
                return Ok(());
            }
        }
        Ok(())
    }

    async fn advance(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
        observed: Option<(MoveRecord, u64)>,
        hub_seen: Option<&SeenMove>,
    ) -> Result<Duration, Failure> {
        if !self.resumed && row.step == Step::Exported && !row.seal_sent {
            self.freeze_export(row, plan).await?;
            if row.abort_requested.is_some() {
                return Ok(Duration::ZERO);
            }
            self.resumed = true;
        }
        match row.step {
            Step::Preparing => {
                self.freeze_export(row, plan).await?;
                if row.abort_requested.is_some() {
                    return Ok(Duration::ZERO);
                }
                self.resumed = true;
                Self::change_step(row, plan, Step::Exported);
                self.save(row, plan, false).await?;
                Ok(Duration::ZERO)
            }
            Step::Exported => {
                let status = self.seal_hub(row, plan).await?;
                if plan.source.seal_state != Some(status.state) {
                    row.attempts = 0;
                    plan.source.retry_at_ms = 0;
                }
                plan.source.seal_state = Some(status.state);
                match status.state {
                    SealState::Sealed => {
                        let (Some(manifest_sha256), Some(sealed_key)) =
                            (status.manifest_sha256, status.sealed_key)
                        else {
                            return Err(state_conflict());
                        };
                        if manifest_sha256.is_empty() || sealed_key.is_empty() {
                            return Err(state_conflict());
                        }
                        plan.source.sealed_cut = Some(crate::door_client::SealedCut {
                            manifest_sha256,
                            sealed_key,
                        });
                        Self::change_step(row, plan, Step::Sealed);
                        self.save(row, plan, false).await?;
                        Ok(Duration::ZERO)
                    }
                    SealState::Pending => {
                        row.attempts += 1;
                        let delay = backoff(row.attempts);
                        plan.source.retry_at_ms = (self.worker.clock)() + delay.as_millis() as i64;
                        self.save(row, plan, false).await?;
                        Ok(delay.min(POLL))
                    }
                    state => {
                        let mut failure = Failure::new(
                            "cut.status",
                            if state == SealState::Failed {
                                "failed"
                            } else {
                                "aborted"
                            },
                        );
                        failure.detail = status.failure.unwrap_or(Value::Null);
                        failure.refuse(None);
                        Err(failure)
                    }
                }
            }
            Step::Sealed => {
                if hub_seen.is_none() {
                    self.publish_hub(row, plan).await?;
                }
                plan.source
                    .published_at_ms
                    .get_or_insert_with(|| (self.worker.clock)());
                Self::change_step(row, plan, Step::Published);
                self.save(row, plan, false).await?;
                Ok(Duration::ZERO)
            }
            Step::Published => {
                if let Some(seen) = hub_seen {
                    if matches!(seen.body.state, HubState::Staged | HubState::Handing) {
                        return self.hand_over(row, plan, seen).await;
                    }
                    if seen.body.state == HubState::Handed {
                        let hub = self.hub()?;
                        let position = hub.move_position(&seen.body).await.map_err(hub_failure)?;
                        if position == MovePosition::Source {
                            let position = hub
                                .move_live_position(&seen.body)
                                .await
                                .map_err(hub_failure)?;
                            if !matches!(position, MovePosition::Receiver | MovePosition::Returned)
                            {
                                return Err(state_conflict());
                            }
                        } else if !matches!(
                            position,
                            MovePosition::Receiver | MovePosition::Returned
                        ) {
                            return Err(state_conflict());
                        }
                    }
                }
                if let Some((record, _)) = observed {
                    if record.state == State::Released {
                        Self::change_step(row, plan, Step::Released);
                        self.save(row, plan, true).await?;
                    }
                }
                Ok(POLL)
            }
            Step::Releasing => self.release(row, plan, observed, hub_seen).await,
            _ => Ok(POLL),
        }
    }

    fn door_cut(&self) -> DoorCut {
        DoorCut {
            cut_id: self.cut.clone(),
        }
    }

    fn seal_request(&self, plan: &Plan) -> SealRequest {
        SealRequest {
            cut_id: self.cut.clone(),
            items: plan.source.items.clone(),
            receiver: Some(plan.to.clone()),
        }
    }

    async fn seal_hub(&self, row: &mut AgentMove, plan: &mut Plan) -> Result<StatusReply, Failure> {
        if row.seal_sent {
            match self.worker.door.status_hub(&self.door_cut()).await {
                Ok(status) => return Self::check_hub_upload(status),
                Err(DoorError::Refused { code, .. }) if code == "not_found" => {}
                Err(error) => return Err(Failure::hub_seal("cut.status", error)),
            }
        } else {
            self.check_target(plan).await?;
            row.seal_sent = true;
            self.save(row, plan, false).await?;
            if row.abort_requested.is_some() {
                // No seal was sent, so cleanup has no door work to settle.
                row.seal_sent = false;
                self.save(row, plan, false).await?;
                return Err(hub_failure(HandlerError::new(
                    "abort_requested",
                    "move was cancelled before seal",
                )));
            }
        }
        let sealed = self
            .worker
            .door
            .seal_hub(&self.seal_request(plan))
            .await
            .map_err(|e| Failure::hub_seal("cut.seal", e))?;
        Self::check_hub_upload(sealed)?;
        let status = self
            .worker
            .door
            .status_hub(&self.door_cut())
            .await
            .map_err(|e| Failure::hub_seal("cut.status", e))?;
        Self::check_hub_upload(status)
    }

    fn check_hub_upload(status: HubStatusReply) -> Result<StatusReply, Failure> {
        if let Some((code, detail)) = status.upload_failure() {
            // The upload job is terminal for this cut, even if the same code
            // would be retryable from a different door operation.
            let mut failure = Failure::new("cut.status", code);
            failure.detail = detail.cloned().unwrap_or(Value::Null);
            failure.refuse(None);
            failure.terminal_hub_seal = true;
            return Err(failure);
        }
        Ok(status.status)
    }

    async fn publish_hub(&self, row: &mut AgentMove, plan: &mut Plan) -> Result<(), Failure> {
        let body = plan.hub_record(&self.cut)?;
        if !plan.source.publish_started {
            plan.source.publish_started = true;
            self.save(row, plan, false).await?;
        }
        match self.hub()?.write_move(&body, None).await {
            Ok(revision) => {
                row.record_version = revision as i64;
                Ok(())
            }
            Err(error) => {
                if error.code == "record_too_large" {
                    plan.source.publish_started = false;
                    self.save(row, plan, false).await?;
                }
                Err(hub_failure(error))
            }
        }
    }

    async fn hand_over(
        &self,
        row: &mut AgentMove,
        plan: &mut Plan,
        seen: &SeenMove,
    ) -> Result<Duration, Failure> {
        let hub = self.hub()?;
        if plan.source.handing.is_none() {
            if seen.body.state != HubState::Staged {
                return Err(state_conflict());
            }
            let transfer = hub
                .plan_transfer(&seen.body, None)
                .await
                .map_err(hub_failure)?;
            plan.source.handing = Some((seen.clone(), transfer));
            self.save(row, plan, false).await?;
            if row.abort_requested.is_some() {
                return Ok(Duration::ZERO);
            }
        }
        let (staged, transfer) = plan.source.handing.as_ref().ok_or_else(state_conflict)?;
        let mut handing = staged.body.clone();
        handing.state = HubState::Handing;
        let handing_revision = match hub.write_move(&handing, Some(staged)).await {
            Ok(revision) => revision,
            // A competing receiver abort may have bumped the staged revision.
            Err(error) if error.code == "hub_write_superseded" => return Ok(POLL),
            Err(error) => return Err(hub_failure(error)),
        };
        if let Err(error) = hub.transfer_move(&handing, transfer, false).await {
            if error.code == "hub_write_superseded" {
                if hub
                    .move_live_position(&handing)
                    .await
                    .map_err(hub_failure)?
                    == MovePosition::Forced
                {
                    return Ok(POLL);
                }
                return Err(state_conflict());
            }
            return Err(hub_failure(error));
        }
        let mut handed = handing.clone();
        handed.state = HubState::Handed;
        let previous = SeenMove {
            body: handing,
            revision: handing_revision,
            writer: plan.from.clone(),
            writer_ep: staged.writer_ep,
        };
        match hub.write_move(&handed, Some(&previous)).await {
            Ok(_) => Ok(POLL),
            // The receiver can finish directly from handing after residence lands.
            Err(error) if error.code == "hub_write_superseded" => Ok(POLL),
            Err(error) => Err(hub_failure(error)),
        }
    }

    async fn refuse_abort(
        &self,
        row: &mut AgentMove,
        plan: &Plan,
        state: State,
    ) -> Result<(), HandlerError> {
        row.abort_requested = None;
        row.reason = None;
        row.detail =
            Some(json!({"code":"invalid_state","state":state,"message":"the receiver must abort"}));
        self.save(row, plan, true)
            .await
            .map_err(|e| HandlerError::new(&e.code, e.detail.to_string()))
    }

    async fn settle_pending(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
    ) -> Result<(), Failure> {
        let Some(pending) = plan.source.pending.clone() else {
            return Ok(());
        };
        let participant = plan
            .participants
            .iter()
            .find(|p| p.module_id == pending.module_id)
            .ok_or_else(|| Failure::new("plan.read", "pending_participant_missing"))?
            .clone();
        let client = self.client(&pending.module_id).await?;
        let request = self.request(&participant)?;
        client
            .status(&request)
            .await
            .map_err(|e| Failure::participant("agent_data.status", &pending.module_id, e))?;
        let result = if pending.op == "agent_data.prepare" {
            client.prepare(&request).await.map(|_| ())
        } else {
            client.export(&request).await.map(|_| ())
        };
        if let Err(error) = result {
            let failure = Failure::participant(&pending.op, &pending.module_id, error);
            if failure.unknown || failure.contract_violation {
                return Err(failure);
            }
        }
        plan.source.pending = None;
        self.save(row, plan, false).await
    }

    async fn cleanup(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
        observed: Option<(MoveRecord, u64)>,
        hub_seen: Option<&SeenMove>,
    ) -> Result<Duration, Failure> {
        self.settle_pending(row, plan).await?;
        let request = row
            .abort_requested
            .clone()
            .unwrap_or_else(|| json!({"reason":"operator","detail":null}));
        let reason: MoveReason =
            serde_json::from_value(request.get("reason").cloned().unwrap_or(json!("operator")))
                .unwrap_or(MoveReason::Operator);
        row.reason = Some(String::from(reason.clone()));
        row.detail = request.get("detail").cloned().filter(|v| !v.is_null());
        self.save(row, plan, false).await?;
        let shared = matches!(row.step, Step::Sealed | Step::Published);
        if shared {
            if plan.hub_gen.is_some() && hub_seen.is_none() && plan.source.publish_started {
                // Settle a pending publication before undoing the participants' freeze.
                self.publish_hub(row, plan).await?;
                return Ok(POLL);
            }
            match observed {
                Some((record, _))
                    if record.state == State::Published
                        || hub_seen.is_some_and(|seen| seen.body.state == HubState::Staged) =>
                {
                    let aborted = wire::aborted_record(&record, reason, row.detail.as_ref());
                    let seen = hub_seen.ok_or_else(state_conflict)?;
                    let mut next = seen.body.clone();
                    next.state = HubState::Aborted;
                    next.reason = aborted.reason;
                    next.detail = aborted.detail;
                    match self.hub()?.write_move(&next, Some(seen)).await {
                        Ok(_) => {}
                        Err(error) if error.code == "hub_write_superseded" => {}
                        Err(error) => return Err(hub_failure(error)),
                    }
                    return Ok(POLL);
                }
                Some((record, _)) if record.state == State::Aborted => {
                    let seen = hub_seen.ok_or_else(state_conflict)?;
                    let hub = self.hub()?;
                    let mut reconciled = None;
                    if let Some((staged, _)) = &plan.source.handing {
                        let mut handing = staged.body.clone();
                        handing.state = HubState::Handing;
                        match hub.write_move(&handing, Some(staged)).await {
                            Ok(_) => {}
                            Err(error) if error.code == "hub_write_superseded" => {}
                            Err(error) if error.code == "hub_write_unsettled" => {
                                // The current value does not say whether
                                // Handing ever landed. Continue only after
                                // re-reading a verified abort.
                                let revision =
                                    error.detail.as_ref().and_then(|d| d["revision"].as_u64());
                                reconciled = self.read_hub_move(plan).await?.filter(|current| {
                                    revision.is_some_and(|revision| current.revision >= revision)
                                        && current.body.state == HubState::Aborted
                                });
                                if reconciled.is_none() {
                                    return Err(hub_failure(error));
                                }
                            }
                            Err(error) => return Err(hub_failure(error)),
                        }
                    }
                    let seen = reconciled.as_ref().unwrap_or(seen);
                    if !matches!(
                        hub.move_position(&seen.body).await.map_err(hub_failure)?,
                        MovePosition::Source | MovePosition::Returned
                    ) || hub
                        .move_member(&plan.from)
                        .await
                        .map_err(hub_failure)?
                        .is_none()
                    {
                        return Err(state_conflict());
                    }
                    if row.seal_sent {
                        self.abort_door_cut().await?;
                    }
                    self.abort_participants(row, plan).await?;
                    Self::change_step(row, plan, Step::Aborted);
                    self.save(row, plan, true).await?;
                    return Ok(Duration::ZERO);
                }
                Some((record, _)) => {
                    self.refuse_abort(row, plan, record.state).await?;
                    return Ok(Duration::ZERO);
                }
                None if row.step == Step::Published => {
                    return Err(record_unavailable());
                }
                None => {}
            }
        }
        self.abort_participants(row, plan).await?;
        if row.seal_sent && plan.source.terminal_hub_seal {
            // A failed hub upload may still have reserved space. Abort the
            // original cut without replaying its terminal seal.
            self.abort_door_cut().await?;
        } else if row.seal_sent && plan.source.seal_refused.is_none() {
            // seal_sent is durable before the send, so it does not prove the door
            // received the seal. Repeat the identical seal so the door reaches a
            // known state, then abort that cut.
            let resealed = self
                .worker
                .door
                .seal(&self.seal_request(plan))
                .await
                .map_err(|e| Failure::door("cut.seal", e));
            match resealed {
                Ok(_) => self.abort_door_cut().await?,
                // The door replays an identical seal it already accepted, so a
                // permanent refusal before the cut was ever reported sealed
                // means it never created this cut and there is nothing to
                // abort. Record that durably so a restart does not send the
                // refused seal again.
                Err(failure)
                    if failure.refused() && !failure.retryable() && row.step == Step::Exported =>
                {
                    self.refusals.note(
                        &self.cut,
                        "engram",
                        &failure.op,
                        &failure.code,
                        failure.message(),
                    );
                    plan.source.seal_refused = Some(failure.code);
                    self.save(row, plan, false).await?;
                }
                Err(failure) => return Err(failure),
            }
        }
        Self::change_step(row, plan, Step::Aborted);
        self.save(row, plan, true).await?;
        Ok(Duration::ZERO)
    }

    async fn abort_door_cut(&self) -> Result<(), Failure> {
        match self.worker.door.abort(&self.door_cut()).await {
            Ok(_) => Ok(()),
            // The door answers not_found for a cut it does not hold, including
            // one it never accepted. Nothing is left to abort.
            Err(DoorError::Refused { code, .. }) if code == "not_found" => Ok(()),
            Err(error) => Err(Failure::door("cut.abort", error)),
        }
    }

    async fn abort_participants(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
    ) -> Result<(), Failure> {
        for participant in plan.participants.clone() {
            if plan.source.aborted.contains(&participant.module_id) {
                continue;
            }
            self.client(&participant.module_id)
                .await?
                .abort(&self.request(&participant)?)
                .await
                .map_err(|e| Failure::participant("agent_data.abort", &participant.module_id, e))?;
            plan.source.aborted.push(participant.module_id.clone());
            self.save(row, plan, false).await?;
        }
        Ok(())
    }

    async fn release(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
        observed: Option<(MoveRecord, u64)>,
        hub_seen: Option<&SeenMove>,
    ) -> Result<Duration, Failure> {
        let Some((record, _)) = observed else {
            return Err(record_unavailable());
        };
        if record.state == State::Released {
            Self::change_step(row, plan, Step::Released);
            self.save(row, plan, true).await?;
            return Ok(Duration::ZERO);
        }
        if record.state != State::Activated {
            return Err(Failure::new("hub.move", "invalid_state"));
        }
        let handoff = self
            .worker
            .door
            .handoff(&self.door_cut())
            .await
            .map_err(|e| Failure::door("cut.handoff", e))?;
        if plan.source.handoff_state != Some(handoff.state) {
            // The first handoff checkpoint must precede any participant release.
            // An existing checkpoint without a box means omit it, even on an
            // older row: the participant may already have accepted that request.
            if plan.source.handoff_state.is_none() && plan.source.enforced.is_empty() {
                plan.source.release_holder_box = Some(plan.to.clone());
            }
            row.attempts = 0;
            plan.source.retry_at_ms = 0;
            plan.source.handoff_state = Some(handoff.state);
            self.save(row, plan, false).await?;
        }
        if !matches!(
            handoff.state,
            HandoffState::Released | HandoffState::TakenOver
        ) {
            let status = self
                .worker
                .door
                .status(&self.door_cut())
                .await
                .map_err(|e| Failure::door("cut.status", e))?;
            if !status.handoff.is_some_and(|h| {
                matches!(h.state, HandoffState::Released | HandoffState::TakenOver)
            }) {
                return Err(Failure::new(
                    "cut.handoff",
                    handoff.code.as_deref().unwrap_or("handoff_not_covered"),
                ));
            }
        }
        for participant in plan.participants.clone() {
            if plan.source.enforced.contains_key(&participant.module_id) {
                continue;
            }
            if participant.optional && record.skipped.binary_search(&participant.module_id).is_ok()
            {
                continue;
            }
            let request = self.request(&participant)?;
            let release = ReleaseRequest::new(request.refs, request.cut_id, plan.to.clone());
            let release = match &plan.source.release_holder_box {
                Some(box_id) => release
                    .with_holder_box(box_id.clone())
                    .map_err(|_| Failure::new("agent_data.release", "plan_malformed"))?,
                None => release,
            };
            let reply = self
                .client(&participant.module_id)
                .await?
                .release(&release)
                .await
                .map_err(|e| {
                    Failure::participant("agent_data.release", &participant.module_id, e)
                })?;
            plan.source
                .enforced
                .entry(participant.module_id)
                .or_insert(reply.enforced);
            self.save(row, plan, false).await?;
        }
        let seen = hub_seen.ok_or_else(state_conflict)?;
        let mut body = seen.body.clone();
        body.state = HubState::Released;
        row.record_version = self
            .hub()?
            .write_move(&body, Some(seen))
            .await
            .map_err(hub_failure)? as i64;
        Self::change_step(row, plan, Step::Released);
        self.save(row, plan, true).await?;
        Ok(Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fake_daemon::{DaemonSnapshot, FakeDaemon},
        fake_door::{Effect as DoorEffect, FakeDoor},
        fake_participant::{Effect as ParticipantEffect, FakeParticipant},
        log_client::TransportError,
        move_driver::{MoveWake, SqlOperations},
        participant_client::Refusal,
        participant_discovery::{provider_role, ParticipantDiscovery},
        tests::scratch_descriptor,
        HealthGauges,
    };
    use entorhinal_core::{move_store::MoveSide, RegistryStore};
    use std::sync::{Arc, Mutex};
    use subc_client_rs::{CatalogList, SupervisorListResponse};

    struct Fixture {
        worker: Worker,
        door: FakeDoor,
        slot: Arc<Mutex<Option<RegistryStore>>>,
        _dir: std::path::PathBuf,
    }

    impl Fixture {
        fn new() -> Self {
            let (_dir, descriptor) = scratch_descriptor("source-failure");
            let store = RegistryStore::open(&descriptor).unwrap();
            let slot = Arc::new(Mutex::new(Some(store)));
            let door = FakeDoor::new([1; 16]);
            let worker = Worker {
                store: Arc::downgrade(&slot),
                sql_operations: Arc::new(SqlOperations::default()),
                door: door.client(),
                discovery: Arc::new(FakeDaemon::new(DaemonSnapshot::new(1, vec![], vec![]))),
                catalog: None,
                health: Arc::new(HealthGauges::default()),
                clock: || 1000,
                wake: Arc::new(MoveWake::default()),
            };
            let row = AgentMove {
                cut_id: "aabb0123456789abcdef0123456789ab".into(),
                agent_id: "agent_0123456789abcdef".into(),
                side: MoveSide::Source,
                peer: "02".repeat(16),
                step: Step::Preparing,
                plan: json!({"agent_id":"agent_0123456789abcdef","from":"01".repeat(16),
                    "to":"02".repeat(16),"claim_version":1,"hub_gen":1,"participants":[]}),
                request_body: None,
                record_version: 0,
                claim_version: Some(1),
                claim_take_body: None,
                claim_take_version: None,
                claim_landed_version: None,
                seal_sent: false,
                abort_requested: None,
                hold: None,
                cut_accept: None,
                reason: None,
                detail: None,
                request_key: None,
                attempts: 0,
                activated_at_ms: None,
                created_at_ms: 1000,
                updated_at_ms: 1000,
            };
            slot.lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .insert_agent_move(&row)
                .unwrap();
            Self {
                worker,
                door,
                slot,
                _dir,
            }
        }

        fn source(&self) -> Source {
            Source::new(
                self.worker.clone(),
                "aabb0123456789abcdef0123456789ab".into(),
            )
        }
        fn row(&self) -> AgentMove {
            self.slot
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .agent_move("aabb0123456789abcdef0123456789ab")
                .unwrap()
                .unwrap()
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            self.slot.lock().unwrap().take();
            let _ = std::fs::remove_dir_all(&self._dir);
        }
    }

    struct Discovery {
        peers: BTreeMap<String, Arc<FakeParticipant>>,
    }

    #[async_trait::async_trait]
    impl ParticipantDiscovery for Discovery {
        async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
            Ok(DaemonSnapshot::new(
                1,
                self.peers
                    .keys()
                    .map(|id| json!({"module_id":id,"state":"running","enabled":true,"live":true,"health":"ok"}))
                    .collect(),
                vec![],
            )
            .supervisor)
        }

        async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
            Ok(DaemonSnapshot::new(
                1,
                vec![],
                self.peers
                    .keys()
                    .map(|id| json!({"module_id":id,"ready":true,"roles":[provider_role()],"control_ops":[],
                        "capabilities":{"provides":["agent-data/v1"],"requires":[]}}))
                    .collect(),
            )
            .catalog)
        }

        async fn open_participant(
            &self,
            module_id: &str,
        ) -> Result<Arc<dyn crate::log_client::LogTransport>, TransportError> {
            Ok(self.peers[module_id].clone())
        }
    }

    async fn release_fixture(
        second_optional: bool,
    ) -> (
        Fixture,
        Arc<FakeParticipant>,
        Arc<FakeParticipant>,
        SeenMove,
    ) {
        let mut f = Fixture::new();
        let a = Arc::new(FakeParticipant::new(
            &["agent"],
            vec![AgentRef::new("agent", "agent_0123456789abcdef")],
        ));
        let b = Arc::new(FakeParticipant::new(
            &["agent"],
            vec![AgentRef::new("agent", "agent_0123456789abcdef")],
        ));
        f.worker.discovery = Arc::new(Discovery {
            peers: BTreeMap::from([("a".into(), a.clone()), ("b".into(), b.clone())]),
        });
        let item = crate::door_client::CutItem {
            session_ref: "11".repeat(16),
            stream: "reserved:a".into(),
            end_offset: 42,
            digest: "a".repeat(64),
        };
        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        plan.participants = serde_json::from_value(json!([
            {"module_id":"a","refs":[{"ref_kind":"agent","ref":"agent_0123456789abcdef"}],"external":[]},
            {"module_id":"b","optional":second_optional,"refs":[{"ref_kind":"agent","ref":"agent_0123456789abcdef"}],"external":[]}
        ]))
        .unwrap();
        plan.source.items = vec![item.clone()];
        plan.source.sealed_cut = Some(crate::door_client::SealedCut {
            manifest_sha256: "manifest".into(),
            sealed_key: "sealed-key".into(),
        });
        row.step = Step::Releasing;
        row.seal_sent = true;
        row.record_version = 2;
        row.plan = serde_json::to_value(&plan).unwrap();
        f.slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .update_agent_move(&row)
            .unwrap();
        let participant_request = CutRequest::new(
            Refs::new(vec![AgentRef::new("agent", "agent_0123456789abcdef")]).unwrap(),
            CutId::new(row.cut_id.clone()).unwrap(),
        );
        for (module, peer) in [("a", &a), ("b", &b)] {
            let client = ParticipantClient::new(&Participant {
                module_id: module.into(),
                transport: peer.clone(),
            });
            client.prepare(&participant_request).await.unwrap();
            client.export(&participant_request).await.unwrap();
        }
        f.door.set_stream(item.clone());
        f.door
            .client()
            .seal_hub(&SealRequest {
                cut_id: row.cut_id.clone(),
                items: vec![item],
                receiver: Some("02".repeat(16)),
            })
            .await
            .unwrap();
        f.door.script(
            "cut.handoff",
            DoorEffect::Handoff(crate::door_client::HandoffReply {
                state: HandoffState::Released,
                session_ref: None,
                code: None,
            }),
        );
        let mut body = plan
            .hub_record(&row.cut_id)
            .unwrap_or_else(|error| panic!("hub record: {}", error.code));
        body.state = HubState::Activated;
        body.skipped = if second_optional {
            vec!["b".into()]
        } else {
            vec![]
        };
        let seen = SeenMove {
            body,
            revision: 2,
            writer: "02".repeat(16),
            writer_ep: 1,
        };
        (f, a, b, seen)
    }

    #[tokio::test]
    async fn source_missing_driver_action_holds_without_abort_or_retry_after_restart() {
        let f = Fixture::new();
        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        // An unknown op has the same missing-action signal as a future driver action.
        let error = Failure::participant(
            "agent_data.future",
            "data",
            ClientError::Refused(Refusal {
                code: "participant_failed".into(),
                detail: None,
                message: None,
            }),
        );
        assert!(error.action().is_none());
        f.source()
            .failed(&mut row, &mut plan, error)
            .await
            .unwrap_or_else(|error| panic!("source failure: {}", error.code));
        let held = f.row();
        assert_eq!(held.step, Step::Preparing);
        assert!(held.abort_requested.is_none());
        assert_eq!(held.attempts, 0);
        assert!(held.plan["_source"]["contract_violation"].is_object());
        assert_eq!(held.hold.unwrap()["module_id"], "data");
    }

    #[test]
    fn participant_contract_violations_are_never_retryable_by_code() {
        let violation = Failure::participant(
            "agent_data.prepare",
            "data",
            ClientError::ContractViolation {
                op: "agent_data.prepare",
                code: "busy".into(),
                detail: None,
                message: None,
            },
        );
        assert!(violation.contract_violation);
        assert!(!violation.retryable());
        let refused = Failure::participant(
            "agent_data.prepare",
            "data",
            ClientError::Refused(Refusal {
                code: "busy".into(),
                detail: None,
                message: None,
            }),
        );
        assert!(!refused.contract_violation);
        assert_eq!(refused.action(), Some(DriverAction::RetryLater));
    }

    #[test]
    fn participant_refusal_message_is_kept_in_the_stored_detail() {
        for (error, expected) in [
            (
                ClientError::Refused(Refusal {
                    code: "busy".into(),
                    detail: None,
                    message: Some("still running".into()),
                }),
                Some("still running"),
            ),
            (
                ClientError::ContractViolation {
                    op: "agent_data.prepare",
                    code: "busy".into(),
                    detail: None,
                    message: Some("bad reply".into()),
                },
                Some("bad reply"),
            ),
            (ClientError::OutcomeUnknown, None),
        ] {
            let failure = Failure::participant("agent_data.prepare", "data", error);
            assert_eq!(failure.detail()["message"].as_str(), expected);
        }
    }
    #[tokio::test]
    async fn source_release_enforcement_is_persisted_per_module_across_a_mid_release_restart() {
        let (f, a, b, seen) = release_fixture(false).await;
        b.script(
            "agent_data.release",
            ParticipantEffect::Refuse(Refusal {
                code: "unavailable".into(),
                detail: None,
                message: None,
            }),
        );
        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        let observed = Some((seen.body.participant_plan(), seen.revision));
        let error = f
            .source()
            .release(&mut row, &mut plan, observed.clone(), Some(&seen))
            .await;
        assert_eq!(error.err().unwrap().code, "unavailable");
        let saved: Plan = serde_json::from_value(f.row().plan).unwrap();
        assert!(saved.source.enforced.contains_key("a"));
        assert!(!saved.source.enforced.contains_key("b"));
        assert_eq!(
            saved.source.release_holder_box.as_deref(),
            Some(row.plan["to"].as_str().unwrap())
        );

        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        let error = f
            .source()
            .release(&mut row, &mut plan, observed, Some(&seen))
            .await;
        assert_eq!(error.err().unwrap().code, "hub_not_ready");
        let a_calls: Vec<_> = a
            .wire_calls()
            .into_iter()
            .filter(|(method, _)| method == "agent_data.release")
            .collect();
        let b_calls: Vec<_> = b
            .wire_calls()
            .into_iter()
            .filter(|(method, _)| method == "agent_data.release")
            .collect();
        assert_eq!(a_calls.len(), 1);
        assert_eq!(b_calls.len(), 2);
        for (_, params) in a_calls.iter().chain(&b_calls) {
            assert_eq!(params["holder"], row.plan["to"]);
            assert_eq!(params["holderBox"], row.plan["to"]);
        }
    }

    #[tokio::test]
    async fn optional_move_source_keeps_skipped_data_but_releases_required_no_items() {
        let (f, a, b, seen) = release_fixture(true).await;
        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        let observed = Some((seen.body.participant_plan(), seen.revision));
        let error = f
            .source()
            .release(&mut row, &mut plan, observed, Some(&seen))
            .await;
        assert_eq!(error.err().unwrap().code, "hub_not_ready");
        assert_eq!(
            a.wire_calls()
                .iter()
                .filter(|(method, _)| method == "agent_data.release")
                .count(),
            1
        );
        assert!(!b
            .wire_calls()
            .iter()
            .any(|(method, _)| method == "agent_data.release"));
        assert!(b.tombstones().is_empty());
    }
    #[tokio::test]
    async fn source_abort_waits_for_unknown_prepare_to_settle_even_after_restart() {
        let (f, a, b, _) = release_fixture(false).await;
        let mut row = f.row();
        let mut plan: Plan = serde_json::from_value(row.plan.clone()).unwrap();
        row.step = Step::Preparing;
        row.seal_sent = false;
        row.abort_requested = Some(json!({"reason":"operator","detail":null}));
        plan.source.pending = Some(PendingCall {
            module_id: "a".into(),
            op: "agent_data.prepare".into(),
        });
        f.source()
            .cleanup(&mut row, &mut plan, None, None)
            .await
            .unwrap_or_else(|error| panic!("source cleanup: {}", error.code));
        assert_eq!(row.step, Step::Aborted);
        let ops: Vec<_> = a
            .calls()
            .iter()
            .map(|request| request.op().method())
            .collect();
        let status = ops
            .iter()
            .rposition(|op| *op == "agent_data.status")
            .unwrap();
        let prepare = ops
            .iter()
            .rposition(|op| *op == "agent_data.prepare")
            .unwrap();
        let abort = ops
            .iter()
            .rposition(|op| *op == "agent_data.abort")
            .unwrap();
        assert!(status < prepare && prepare < abort, "{ops:?}");
        assert!(b
            .calls()
            .iter()
            .any(|request| request.op().method() == "agent_data.abort"));
    }
    #[test]
    fn source_refusals_log_once_per_code_and_keep_the_message_in_detail() {
        let mut log = RefusalLog::default();
        for _ in 0..2 {
            log.note(
                "cut",
                "data",
                "agent_data.prepare",
                "busy",
                Some("still running"),
            );
        }
        log.note(
            "cut",
            "data",
            "agent_data.prepare",
            "participant_failed",
            Some("failed safely"),
        );
        assert_eq!(log.lines.len(), 2);
        assert!(log.lines[0].contains("still running"));
        assert!(log.lines[1].contains("failed safely"));
        assert!(!log.lines.join("\n").contains("session-secret"));
    }
}

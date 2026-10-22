//! Restartable source half of the move protocol. Recovery uses the participant
//! and ref set recorded at admission, never a fresh daemon discovery result.

use crate::{
    claim_codec::AgentClaim,
    door_client::{
        CutItem, CutRequest as DoorCut, DoorError, HandoffState, SealRequest, SealState,
    },
    log_client::TransportError,
    move_driver::{
        backoff, digest_reason, participant_action, PeerRefusal, RefusalLog, Worker, POLL,
    },
    participant_client::{
        AgentRef, ClientError, CutRequest, ParticipantClient, Refs, ReleaseRequest,
    },
    participant_discovery::Participant,
    record_client::{CasRequest, CasSettlement, RecordError, RecordReply},
    HandlerError,
};
use cortexkit_role_agent_data::{errors::DriverAction, refs::CutId};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    move_record::{
        self as wire, ExternalRef, InboxRecord, MoveParticipant, MoveReason, MoveRecord,
        MoveRecordState as State, PendingMove,
    },
    move_store::{AgentMove, MoveSide, MoveStep as Step},
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::atomic::Ordering, time::Duration};

#[derive(Clone, Default, Serialize, Deserialize)]
struct Progress {
    #[serde(default)]
    aborted: Vec<String>,
    #[serde(default)]
    prepared: bool,
    #[serde(default)]
    items: Vec<CutItem>,
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
    participants: Vec<MoveParticipant>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pin_digest: Option<String>,
    #[serde(default, rename = "_source")]
    source: Progress,
}

impl Plan {
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
            reason: None,
            detail: None,
        }
    }
}

struct Failure {
    code: String,
    detail: Value,
    op: String,
    module: Option<String>,
    diagnostic: bool,
    unknown: bool,
    contract_violation: bool,
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
    fn refuse(&mut self, message: Option<String>) {
        self.refusal = Some(Box::new(PeerRefusal { message }));
    }
    fn refused(&self) -> bool {
        self.refusal.is_some()
    }
    fn message(&self) -> Option<&str> {
        self.refusal.as_ref().and_then(|r| r.message.as_deref())
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
        self.module
            .as_ref()
            .and_then(|_| participant_action(&self.op, &self.code))
    }
    fn retryable(&self) -> bool {
        !self.contract_violation
            && if self.module.is_some() {
                matches!(
                    self.action(),
                    Some(DriverAction::RetryWithinStep | DriverAction::Retry)
                )
            } else {
                ["cut_not_ready", "unavailable", "handoff_not_covered"]
                    .contains(&self.code.as_str())
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
impl From<RecordError> for Failure {
    fn from(e: RecordError) -> Self {
        let mut failure = Self::new("record.read", e.code());
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
        // Poll the shared move record even during a long backoff. Reads that
        // fail signature, author, or persisted-plan checks cannot advance the row.
        let watching = matches!(row.step, Step::Sealed | Step::Published | Step::Releasing);
        let poll = watching || self.last_poll.is_none_or(|last| last.elapsed() >= POLL);
        let observed = if poll {
            self.last_poll = Some(tokio::time::Instant::now());
            match self.read_move(&plan).await {
                Ok(record) => record,
                Err(error) => return self.failed(&mut row, &mut plan, error).await,
            }
        } else {
            None
        };
        if let Some((record, version)) = observed.as_ref().filter(|_| watching) {
            row.record_version = *version as i64;
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
            } else if record.state != State::Published && row.abort_requested.is_some() {
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
            self.cleanup(&mut row, &mut plan, observed).await
        } else {
            self.advance(&mut row, &mut plan, observed).await
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
            row.hold = Some(detail);
            plan.source.retry_at_ms = 0;
            self.save(row, plan, false)
                .await
                .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            return Ok(Some(POLL));
        }
        if error.contract_violation {
            let detail = error.detail();
            if !plan.source.errors.contains(&detail) {
                plan.source.errors.push(detail.clone());
            }
            if cleanup || row.step == Step::Releasing {
                // Release cannot be rolled back, and cleanup cannot finish by
                // trusting a participant that broke its contract. Stop calls
                // durably rather than turning the violation into another retry.
                plan.source.contract_violation = Some(detail.clone());
                row.detail = Some(detail.clone());
                row.hold = Some(detail);
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
            row.hold = Some(
                json!({"code":error.code,"module_id":error.module,"op":error.op,
                "move_error":row.step == Step::Releasing && !error.retryable()}),
            );
            self.save(row, plan, false)
                .await
                .map_err(|f| HandlerError::new(&f.code, f.detail.to_string()))?;
            Ok(Some(delay.min(POLL)))
        } else {
            let reason = if error.op.starts_with("cut.") {
                MoveReason::DoorRefused
            } else if error.code == "move_too_large" {
                MoveReason::MoveTooLarge
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
            row.abort_requested = Some(json!({"reason":reason,"detail":error.detail()}));
            row.reason = Some(reason);
            row.detail = Some(error.detail());
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
                if let TransportError::Refusal {
                    code,
                    detail,
                    message,
                } = e
                {
                    failure.code = code;
                    failure.detail = detail.unwrap_or(Value::Null);
                    failure.refuse(message);
                }
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

    async fn read_move(&self, plan: &Plan) -> Result<Option<(MoveRecord, u64)>, Failure> {
        let subject = format!("move:{}", self.cut);
        let reply = self.worker.records.read(&subject).await?;
        if reply.stale {
            return Err(RecordError::refusal("unavailable", None).into());
        }
        if i64::try_from(reply.version).is_err() {
            let mut failure = Failure::new("record.read", "record_version_unknown");
            failure.diagnostic = true;
            return Err(failure);
        }
        if reply.absent {
            return Ok(None);
        }
        let verified = wire::decode_move_record(
            &subject,
            reply.body.as_deref().unwrap_or_default(),
        )
        .and_then(|record| {
            wire::check_record_author(&record, reply.author.as_deref().unwrap_or_default())?;
            wire::check_pinned_plan(&plan.record(&self.cut), &record)?;
            Ok(record)
        });
        match verified {
            Ok(record) => Ok(Some((record, reply.version))),
            Err(e) => {
                let mut error = Failure::new("record.read", e.code());
                error.diagnostic = true;
                Err(error)
            }
        }
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
            if let Err(e) = result {
                let failure = Failure::participant("agent_data.export", &participant.module_id, e);
                if !failure.unknown {
                    plan.source.pending = None;
                    self.save(row, plan, false).await?;
                }
                return Err(failure);
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
    ) -> Result<Duration, Failure> {
        if !self.resumed && matches!(row.step, Step::Exported | Step::Sealed) {
            self.freeze_export(row, plan).await?;
            if row.abort_requested.is_some() {
                return Ok(Duration::ZERO);
            }
            if row.seal_sent {
                self.worker
                    .door
                    .seal(&SealRequest {
                        cut_id: self.cut.clone(),
                        items: plan.source.items.clone(),
                    })
                    .await
                    .map_err(|e| Failure::door("cut.seal", e))?;
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
                let status = if !row.seal_sent {
                    row.seal_sent = true;
                    self.save(row, plan, false).await?;
                    self.worker
                        .door
                        .seal(&SealRequest {
                            cut_id: self.cut.clone(),
                            items: plan.source.items.clone(),
                        })
                        .await
                        .map_err(|e| Failure::door("cut.seal", e))?
                } else {
                    // seal_sent is durable before the send, so it does not prove the
                    // door received the request. Repeat the identical seal to recover.
                    self.worker
                        .door
                        .seal(&SealRequest {
                            cut_id: self.cut.clone(),
                            items: plan.source.items.clone(),
                        })
                        .await
                        .map_err(|e| Failure::door("cut.seal", e))?;
                    self.worker
                        .door
                        .status(&self.door_cut())
                        .await
                        .map_err(|e| Failure::door("cut.status", e))?
                };
                if plan.source.seal_state != Some(status.state) {
                    row.attempts = 0;
                    plan.source.retry_at_ms = 0;
                }
                plan.source.seal_state = Some(status.state);
                match status.state {
                    SealState::Published => {
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
                if observed.is_none() {
                    let (_, inbox) = self.inbox(&plan.to).await?;
                    if inbox.pending.len() >= wire::INBOX_CAP
                        && !inbox.pending.iter().any(|p| p.cut_id == self.cut)
                    {
                        row.abort_requested =
                            Some(json!({"reason":"receiver_inbox_full","detail":null}));
                        self.save(row, plan, false).await?;
                        return Ok(Duration::ZERO);
                    }
                    if let CasSettlement::Landed(version) =
                        self.cas(plan, None, 0, &plan.record(&self.cut)).await?
                    {
                        row.record_version = version as i64;
                        self.save(row, plan, false).await?;
                    }
                }
                self.append_inbox(plan).await?;
                plan.source
                    .published_at_ms
                    .get_or_insert_with(|| (self.worker.clock)());
                Self::change_step(row, plan, Step::Published);
                self.save(row, plan, false).await?;
                Ok(Duration::ZERO)
            }
            Step::Published => {
                if let Some((record, _)) = observed {
                    if record.state == State::Released {
                        Self::change_step(row, plan, Step::Released);
                        self.save(row, plan, true).await?;
                    }
                }
                Ok(POLL)
            }
            Step::Releasing => self.release(row, plan, observed).await,
            _ => Ok(POLL),
        }
    }

    fn door_cut(&self) -> DoorCut {
        DoorCut {
            cut_id: self.cut.clone(),
        }
    }

    async fn cas(
        &self,
        plan: &Plan,
        from: Option<State>,
        version: u64,
        body: &MoveRecord,
    ) -> Result<CasSettlement, Failure> {
        wire::check_transition(from, body.state, MoveSide::Source)
            .map_err(|e| Failure::new("record.cas", e.code()))?;
        let subject = format!("move:{}", self.cut);
        let request = CasRequest {
            subject: subject.clone(),
            expected_version: version,
            body: wire::encode_move_record(&subject, body)
                .map_err(|e| Failure::new("record.cas", e.code()))?,
            write_id: wire::move_write_id(&self.cut, MoveSide::Source, body.state),
        };
        self.worker
            .records
            .cas_settled(&request, &plan.from, |bytes| {
                wire::decode_move_record(&subject, bytes).map_err(|e| e.code())
            })
            .await
            .map_err(Failure::from)
    }

    async fn inbox(&self, peer: &str) -> Result<(RecordReply, InboxRecord), Failure> {
        let reply = self.worker.records.read(&format!("inbox:{peer}")).await?;
        if reply.stale {
            return Err(RecordError::refusal("unavailable", None).into());
        }
        let inbox = if reply.absent {
            InboxRecord::default()
        } else {
            wire::decode_inbox_record(reply.body.as_deref().unwrap_or_default()).map_err(|e| {
                let mut failure = Failure::new("inbox.read", e.code());
                failure.diagnostic = true;
                failure
            })?
        };
        Ok((reply, inbox))
    }

    async fn append_inbox(&self, plan: &Plan) -> Result<(), Failure> {
        // Re-read the verified move before each inbox append retry. Once the
        // receiver advances it beyond published, no inbox notification is needed,
        // including when the previous append landed but its reply was lost.
        for _ in 0..8 {
            let Some((record, _)) = self.read_move(plan).await? else {
                return Err(Failure::new("record.read", "unavailable"));
            };
            if record.state != State::Published {
                return Ok(());
            }
            let (reply, inbox) = self.inbox(&plan.to).await?;
            if inbox.pending.iter().any(|p| p.cut_id == self.cut) {
                return Ok(());
            }
            let appended = wire::append_inbox(
                &inbox,
                PendingMove {
                    cut_id: self.cut.clone(),
                    from: plan.from.clone(),
                    agent_id: plan.agent_id.clone(),
                },
            )
            .map_err(|e| Failure::new("inbox.append", e.code()))?;
            let mut write_id = [0; 16];
            getrandom::getrandom(&mut write_id)
                .map_err(|_| Failure::new("inbox.append", "unavailable"))?;
            let request = CasRequest {
                subject: format!("inbox:{}", plan.to),
                expected_version: reply.version,
                body: wire::encode_inbox_record(&appended)
                    .map_err(|e| Failure::new("inbox.append", e.code()))?,
                write_id,
            };
            match self.worker.records.cas(&request).await {
                Ok(_) => return Ok(()),
                Err(e) if e.code() == "version_moved" || e.code() == "unavailable" => continue,
                Err(e) => return Err(e.into()),
            }
        }
        Err(RecordError::refusal("unavailable", None).into())
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
            match observed {
                Some((record, version)) if record.state == State::Published => {
                    let aborted = wire::aborted_record(&record, reason, row.detail.as_ref());
                    let settlement = self
                        .cas(plan, Some(State::Published), version, &aborted)
                        .await?;
                    if let CasSettlement::Landed(version) = settlement {
                        row.record_version = version as i64;
                        self.save(row, plan, false).await?;
                    }
                    // The receiver may stage the move while we attempt to abort it.
                    // Re-read and verify the writer and persisted plan before cleanup;
                    // the body we tried to write is not proof that the abort landed.
                    return Ok(Duration::ZERO);
                }
                Some((record, _)) if record.state == State::Aborted => {
                    self.apply_claim(row, false).await?;
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
                    return Err(RecordError::refusal("unavailable", None).into())
                }
                None => {}
            }
        }
        self.abort_participants(row, plan).await?;
        // A seal the door refused created no cut there, so a recorded refusal
        // skips both door calls.
        if row.seal_sent && plan.source.seal_refused.is_none() {
            // seal_sent is durable before the send, so it does not prove the door
            // received the seal. Repeat the identical seal so the door reaches a
            // known state, then abort that cut.
            let resealed = self
                .worker
                .door
                .seal(&SealRequest {
                    cut_id: self.cut.clone(),
                    items: plan.source.items.clone(),
                })
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

    async fn apply_claim(&self, row: &AgentMove, require_present: bool) -> Result<(), Failure> {
        let reply = self
            .worker
            .records
            .read(&format!("claim:{}", row.agent_id))
            .await?;
        if reply.stale {
            return Err(RecordError::refusal("unavailable", None).into());
        }
        let observation = if reply.absent && !require_present {
            ClaimObservation::Absent {
                stale: false,
                self_author: reply.self_author,
            }
        } else {
            let claim =
                AgentClaim::decode(&row.agent_id, reply.body.as_deref().unwrap_or_default())
                    .map_err(|code| {
                        let mut failure = Failure::new("claim.read", code);
                        failure.diagnostic = true;
                        failure
                    })?;
            // A shared abort record says the move was cancelled. For a claimed
            // agent, only a claim held here permits this Mac to resume writes;
            // keep participant data read-only while another Mac holds it.
            if !require_present
                && claim.epoch >= 1
                && claim.holder.as_deref() != Some(&reply.self_author)
            {
                return Err(Failure::new("claim.read", "claim_moved"));
            }
            let version = i64::try_from(reply.version)
                .map_err(|_| Failure::new("claim.read", "claim_version_unknown"))?;
            ClaimObservation::Present {
                claim: CachedClaim {
                    holder: claim.holder,
                    epoch: claim.epoch,
                    cut_id: claim.cut_id,
                    version,
                    read_at_ms: 0,
                    stale: false,
                    integrity_failed: false,
                },
                self_author: reply.self_author,
            }
        };
        let observations = vec![(row.agent_id.clone(), observation)];
        let now = (self.worker.clock)();
        let conflicts = self.worker.cache_claims(observations, now).await?;
        self.worker
            .health
            .self_author_conflicts
            .fetch_add(conflicts as u64, Ordering::Relaxed);
        Ok(())
    }

    async fn release(
        &mut self,
        row: &mut AgentMove,
        plan: &mut Plan,
        observed: Option<(MoveRecord, u64)>,
    ) -> Result<Duration, Failure> {
        let Some((record, version)) = observed else {
            return Err(RecordError::refusal("unavailable", None).into());
        };
        if record.state == State::Released {
            Self::change_step(row, plan, Step::Released);
            self.save(row, plan, true).await?;
            return Ok(Duration::ZERO);
        }
        if record.state != State::Activated {
            return Err(Failure::new("record.read", "invalid_state"));
        }
        self.apply_claim(row, true).await?;
        let handoff = self
            .worker
            .door
            .handoff(&self.door_cut())
            .await
            .map_err(|e| Failure::door("cut.handoff", e))?;
        if plan.source.handoff_state != Some(handoff.state) {
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
            let request = self.request(&participant)?;
            let release = ReleaseRequest::new(request.refs, request.cut_id, plan.to.clone());
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
        let mut released = record;
        released.state = State::Released;
        match self
            .cas(plan, Some(State::Activated), version, &released)
            .await?
        {
            CasSettlement::Landed(version) => {
                row.record_version = version as i64;
                Self::change_step(row, plan, Step::Released);
                self.save(row, plan, true).await?;
            }
            CasSettlement::Moved(_) => {}
        }
        Ok(Duration::ZERO)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fake_door::{Effect as DoorEffect, FakeDoor},
        fake_participant::{Effect as ParticipantEffect, FakeParticipant},
        fake_record::{FakeRecord, FakeRecordConnector},
        log_client::{LogConnector, LogTransport},
        participant_client::{PrepareReply, Refusal, Request},
        participant_discovery::ParticipantDiscovery,
        record_client::RecordClient,
        tests::scratch_descriptor,
        HealthGauges,
    };
    use async_trait::async_trait;
    use entorhinal_core::{move_record::MoveRef, RegistryStore};
    use std::sync::{Arc, Mutex, OnceLock};
    use subc_client_rs::{CatalogList, SupervisorListResponse};

    const CUT: &str = "00112233445566778899aabbccddeeff";
    const FROM: &str = "01010101010101010101010101010101";
    const TO: &str = "02020202020202020202020202020202";
    const AGENT: &str = "agent_0123456789abcdef";
    type Slot = Arc<Mutex<Option<RegistryStore>>>;

    fn clock() -> i64 {
        static EPOCH: OnceLock<std::time::Instant> = OnceLock::new();
        tokio::time::Instant::now()
            .into_std()
            .saturating_duration_since(*EPOCH.get_or_init(std::time::Instant::now))
            .as_millis() as i64
    }

    #[derive(Debug, Clone)]
    struct Event {
        op: String,
        step: Step,
        seal_sent: bool,
        record_version: i64,
    }
    struct Trace {
        slot: std::sync::Weak<Mutex<Option<RegistryStore>>>,
        events: Mutex<Vec<Event>>,
        participant_called: tokio::sync::Notify,
    }
    impl Trace {
        fn call(&self, op: &str) {
            let row = self
                .slot
                .upgrade()
                .unwrap()
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .agent_move(CUT)
                .unwrap()
                .unwrap();
            self.events.lock().unwrap().push(Event {
                op: op.into(),
                step: row.step,
                seal_sent: row.seal_sent,
                record_version: row.record_version,
            });
        }
    }
    struct ReleaseGate {
        enabled: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Semaphore,
        resume: tokio::sync::Semaphore,
    }

    impl Default for ReleaseGate {
        fn default() -> Self {
            Self {
                enabled: std::sync::atomic::AtomicBool::new(false),
                entered: tokio::sync::Semaphore::new(0),
                resume: tokio::sync::Semaphore::new(0),
            }
        }
    }

    struct Peer {
        participant: Arc<FakeParticipant>,
        trace: Arc<Trace>,
        release_gate: Option<Arc<ReleaseGate>>,
    }
    #[async_trait]
    impl LogTransport for Peer {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.trace.call(method);
            if method == "agent_data.release" {
                if let Some(gate) = &self.release_gate {
                    if gate.enabled.load(Ordering::Relaxed) {
                        gate.entered.add_permits(1);
                        gate.resume.acquire().await.unwrap().forget();
                    }
                }
            }
            let result = self.participant.call(method, params).await;
            self.trace.participant_called.notify_one();
            result
        }
    }
    struct Discovery {
        peers: BTreeMap<String, Arc<Peer>>,
    }
    #[async_trait]
    impl ParticipantDiscovery for Discovery {
        async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
            panic!("resume must not rediscover")
        }
        async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
            panic!("resume must not rediscover")
        }
        async fn open_participant(
            &self,
            module: &str,
        ) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self
                .peers
                .get(module)
                .expect("only pinned participants may be called")
                .clone())
        }
    }
    type RecordRace = Arc<Mutex<Option<(Vec<u8>, String)>>>;

    struct Records {
        fake: Arc<FakeRecord>,
        trace: Arc<Trace>,
        lose: Arc<Mutex<Option<String>>>,
        race: RecordRace,
    }
    #[async_trait]
    impl LogConnector for Records {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(RecordPeer {
                fake: self.fake.clone(),
                trace: self.trace.clone(),
                lose: self.lose.clone(),
                race: self.race.clone(),
            }))
        }
    }
    struct RecordPeer {
        fake: Arc<FakeRecord>,
        trace: Arc<Trace>,
        lose: Arc<Mutex<Option<String>>>,
        race: RecordRace,
    }
    #[async_trait]
    impl LogTransport for RecordPeer {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.trace.call(method);
            if method == "record.cas" {
                if params["subject"] == format!("move:{CUT}") {
                    if let Some((body, author)) = self.race.lock().unwrap().take() {
                        self.fake.put(
                            &format!("move:{CUT}"),
                            params["expected_version"].as_u64().unwrap() + 1,
                            body,
                            &author,
                        );
                    }
                }
                let mut lose = self.lose.lock().unwrap();
                if lose
                    .as_ref()
                    .is_some_and(|subject| params["subject"] == subject.as_str())
                {
                    lose.take();
                    self.fake.drop_reply.store(true, Ordering::Relaxed);
                }
            }
            self.fake.call(method, params).await
        }
    }
    struct Door {
        fake: FakeDoor,
        trace: Arc<Trace>,
    }
    #[async_trait]
    impl LogConnector for Door {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(DoorPeer {
                fake: self.fake.clone(),
                trace: self.trace.clone(),
            }))
        }
    }
    struct DoorPeer {
        fake: FakeDoor,
        trace: Arc<Trace>,
    }
    #[async_trait]
    impl LogTransport for DoorPeer {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.trace.call(method);
            self.fake.handle("reserved:entorhinal", method, params)
        }
    }

    struct Fixture {
        slot: Slot,
        worker: Worker,
        participants: Vec<Arc<FakeParticipant>>,
        release_gate: Arc<ReleaseGate>,
        door: FakeDoor,
        records: Arc<FakeRecord>,
        record_pipe: Arc<Records>,
        trace: Arc<Trace>,
        dir: std::path::PathBuf,
        descriptor: cortexkit_store_types::StorageDescriptor,
    }
    impl Fixture {
        fn new() -> Self {
            let _ = clock();
            let (dir, descriptor) = scratch_descriptor("move-source");
            let store = RegistryStore::open(&descriptor).unwrap();
            let plan = json!({"agent_id":AGENT,"from":FROM,"to":TO,"claim_version":0,
            "participants":[
                {"module_id":"a","refs":[{"ref_kind":"session","ref":"ra"}],"external":[]},
                {"module_id":"b","refs":[{"ref_kind":"session","ref":"rb"}],"external":[]},
                {"module_id":"c","refs":[{"ref_kind":"session","ref":"rc"}],"external":[]}
            ]});
            store
                .insert_agent_move(&AgentMove {
                    cut_id: CUT.into(),
                    agent_id: AGENT.into(),
                    side: MoveSide::Source,
                    peer: TO.into(),
                    step: Step::Preparing,
                    plan,
                    request_body: Some(json!({"agent_id":AGENT,"to":TO})),
                    record_version: 0,
                    claim_version: Some(0),
                    claim_take_body: None,
                    claim_take_version: None,
                    claim_landed_version: None,
                    seal_sent: false,
                    abort_requested: None,
                    hold: None,
                    cut_accept: None,
                    reason: None,
                    detail: None,
                    request_key: Some("move".into()),
                    attempts: 0,
                    activated_at_ms: None,
                    created_at_ms: clock(),
                    updated_at_ms: clock(),
                })
                .unwrap();
            let slot = Arc::new(Mutex::new(Some(store)));
            let trace = Arc::new(Trace {
                slot: Arc::downgrade(&slot),
                events: Mutex::new(vec![]),
                participant_called: tokio::sync::Notify::new(),
            });
            let door = FakeDoor::new([1; 16]);
            let records = FakeRecord::new([1; 16]);
            let mut peers = BTreeMap::new();
            let mut participants = vec![];
            let release_gate = Arc::new(ReleaseGate::default());
            for (index, module) in ["a", "b", "c"].into_iter().enumerate() {
                let reference = format!("r{module}");
                let participant = Arc::new(FakeParticipant::new(
                    &["session"],
                    vec![AgentRef::new("session", &reference)],
                ));
                let session_ref = format!("{:032x}", index + 16);
                let item = CutItem {
                    session_ref: session_ref.clone(),
                    stream: format!("reserved:{module}"),
                    end_offset: 42,
                    digest: "a".repeat(64),
                };
                door.set_stream(item.clone());
                let prepare: PrepareReply = serde_json::from_value(json!({"items":[{
                    "ref_kind":"session","ref":reference,"sessionRef":session_ref,"stream":item.stream,
                    "endOffset":42,"digest":item.digest,"format":"v1","frozen":true}],"external":[]})).unwrap();
                participant.set_prepare(prepare);
                peers.insert(
                    module.into(),
                    Arc::new(Peer {
                        participant: participant.clone(),
                        trace: trace.clone(),
                        release_gate: (module == "b").then(|| release_gate.clone()),
                    }),
                );
                participants.push(participant);
            }
            let record_pipe = Arc::new(Records {
                fake: records.clone(),
                trace: trace.clone(),
                lose: Arc::new(Mutex::new(None)),
                race: Arc::new(Mutex::new(None)),
            });
            let worker = Worker {
                sql_operations: Arc::default(),
                store: Arc::downgrade(&slot),
                records: RecordClient::new(record_pipe.clone()),
                door: crate::door_client::DoorClient::new(Arc::new(Door {
                    fake: door.clone(),
                    trace: trace.clone(),
                })),
                discovery: Arc::new(Discovery { peers }),
                catalog: None,
                health: Arc::new(HealthGauges::default()),
                watches: Arc::default(),
                writer: Arc::new(tokio::sync::Mutex::new(())),
                clock,
                wake: Arc::new(crate::move_driver::MoveWake::default()),
            };
            Self {
                slot,
                worker,
                participants,
                release_gate,
                door,
                records,
                record_pipe,
                trace,
                dir,
                descriptor,
            }
        }
        fn source(&self) -> Source {
            Source::new(self.worker.clone(), CUT.into())
        }
        fn row(&self) -> AgentMove {
            self.slot
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .agent_move(CUT)
                .unwrap()
                .unwrap()
        }
        fn edit(&self, edit: impl FnOnce(&mut AgentMove)) {
            let mut row = self.row();
            edit(&mut row);
            self.slot
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .update_agent_move(&row)
                .unwrap();
        }
        fn plan(&self) -> Plan {
            serde_json::from_value(self.row().plan).unwrap()
        }
        fn state(&self, state: State, author: &str) {
            let mut record = self.plan().record(CUT);
            record.state = state;
            if state == State::Aborted {
                record.reason = Some(MoveReason::Operator);
            }
            self.records.put(
                &format!("move:{CUT}"),
                self.row().record_version.max(1) as u64 + 1,
                wire::encode_move_record(&format!("move:{CUT}"), &record).unwrap(),
                author,
            );
        }
        fn claim(&self) {
            self.records.put(
                &format!("claim:{AGENT}"),
                1,
                serde_json::to_vec(&AgentClaim {
                    v: 1,
                    agent_id: AGENT.into(),
                    holder: Some(TO.into()),
                    epoch: 1,
                    cut_id: Some(CUT.into()),
                    reason: None,
                })
                .unwrap(),
                TO,
            );
        }
        async fn step(&self, source: &mut Source) -> Option<Duration> {
            let delay = source.tick().await.unwrap();
            if let Some(delay) = delay {
                tokio::time::advance(delay).await;
            }
            delay
        }
        async fn until(&self, source: &mut Source, step: Step) {
            for _ in 0..20 {
                if self.row().step == step {
                    return;
                }
                self.step(source).await;
            }
            panic!("did not reach {step:?}: {:?}", self.row());
        }
        async fn published(&self, source: &mut Source) {
            self.door
                .script("cut.status", DoorEffect::Seal(SealState::Published, None));
            self.until(source, Step::Published).await;
        }
        fn abort(&self) {
            self.edit(|row| row.abort_requested = Some(json!({"reason":"operator","detail":null})));
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            *self.slot.lock().unwrap() = None;
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_reconnect_polls_once_waiting_and_mid_poll() {
        use crate::log_client::LogConnector;
        for mid_poll in [false, true] {
            let mut f = Fixture::new();
            // A stored contract violation prevents participant calls, but a
            // published source still reads move:<cut> each tick. That isolates
            // the retry loop's reconnect and timer behavior from move progress.
            f.edit(|row| {
                row.step = Step::Published;
                row.plan["_source"] = json!({"contract_violation":true});
            });
            let probe = crate::move_driver::reconnect_test::Connector::new(
                f.records.clone(),
                format!("move:{CUT}"),
                mid_poll,
            );
            let connector = Arc::new(crate::move_driver::ReconnectConnector {
                inner: probe.clone(),
                wake: f.worker.wake.clone(),
            });
            connector.connect().await.unwrap();
            f.worker.records = RecordClient::new(connector.clone());
            let task = tokio::spawn(f.source().run());
            probe.check_polling(&connector, mid_poll).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            f.worker.sql_operations.wait().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_records_each_step_before_next_call_and_releases_three_participants() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        let events = f.trace.events.lock().unwrap().clone();
        assert_eq!(
            events
                .iter()
                .filter(|e| e.op == "agent_data.prepare")
                .count(),
            3
        );
        assert!(events
            .iter()
            .filter(|e| e.op == "agent_data.export")
            .all(|e| e.step == Step::Preparing));
        assert!(events
            .iter()
            .filter(|e| e.op == "cut.seal")
            .all(|e| e.step == Step::Exported && e.seal_sent));
        assert_eq!(f.row().record_version, 1);
        assert_eq!(f.worker.health.moves_open.load(Ordering::Relaxed), 1);
        let inbox = f.worker.records.read(&format!("inbox:{TO}")).await.unwrap();
        assert_eq!(
            wire::decode_inbox_record(inbox.body.as_ref().unwrap())
                .unwrap()
                .pending
                .len(),
            1
        );
        f.claim();
        f.state(State::Activated, TO);
        f.door.script(
            "cut.handoff",
            DoorEffect::Handoff(crate::door_client::HandoffReply {
                state: HandoffState::Released,
                session_ref: None,
                code: None,
            }),
        );
        f.until(&mut source, Step::Released).await;
        for participant in &f.participants {
            assert_eq!(
                participant.tombstones().values().collect::<Vec<_>>(),
                vec![&TO.to_owned()]
            );
            assert!(participant.frozen().is_empty());
        }
        let record = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        assert_eq!(
            wire::decode_move_record(&format!("move:{CUT}"), record.body.as_ref().unwrap())
                .unwrap()
                .state,
            State::Released
        );
        assert_eq!(
            f.slot
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .cached_agent_claim(AGENT)
                .unwrap()
                .unwrap()
                .holder
                .as_deref(),
            Some(TO)
        );
        let events = f.trace.events.lock().unwrap();
        assert!(events
            .iter()
            .filter(|e| e.op == "cut.handoff" || e.op == "agent_data.release")
            .all(|e| e.step == Step::Releasing));
        assert!(events
            .iter()
            .any(|e| e.op == "cut.handoff" && e.record_version >= 2));
        assert_eq!(f.worker.health.moves_open.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn source_activated_wrong_author_never_releases() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.claim();
        f.state(State::Activated, FROM);
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Published);
        assert!(f.participants.iter().all(|p| p.tombstones().is_empty()));
        assert!(f
            .door
            .calls()
            .iter()
            .all(|c| c.request.op() != "cut.handoff"));
        assert_eq!(f.plan().source.errors[0]["code"], "author_mismatch");
        assert_eq!(f.worker.health.move_errors.load(Ordering::Relaxed), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn source_activated_changed_plan_never_releases() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.claim();
        let mut record = f.plan().record(CUT);
        record.state = State::Activated;
        record.participants[0].refs.push(MoveRef {
            ref_kind: "session".into(),
            reference: "zz".into(),
        });
        f.records.put(
            &format!("move:{CUT}"),
            2,
            wire::encode_move_record(&format!("move:{CUT}"), &record).unwrap(),
            TO,
        );
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Published);
        assert!(f
            .door
            .calls()
            .iter()
            .all(|c| c.request.op() != "cut.handoff"));
        assert_eq!(f.plan().source.errors[0]["code"], "plan_mismatch");
    }

    #[tokio::test(start_paused = true)]
    async fn source_abort_cleans_every_preboundary_checkpoint_with_and_without_a_record() {
        for (step, seal_sent, present) in [
            (Step::Preparing, false, false),
            (Step::Preparing, true, false),
            (Step::Exported, false, false),
            (Step::Exported, true, false),
            (Step::Sealed, true, false),
            (Step::Sealed, true, true),
            (Step::Published, true, true),
        ] {
            let f = Fixture::new();
            let mut source = f.source();
            if present {
                f.published(&mut source).await;
            } else if seal_sent || step != Step::Preparing {
                f.until(&mut source, Step::Exported).await;
            }
            f.edit(|r| {
                r.step = step;
                r.seal_sent = seal_sent;
            });
            f.abort();
            f.until(&mut source, Step::Aborted).await;
            assert_eq!(f.row().reason.as_deref(), Some("operator"));
            assert!(f
                .participants
                .iter()
                .all(|p| p.frozen().is_empty() && p.staged().is_empty()));
            if seal_sent {
                assert!(f.door.cut(CUT).unwrap().aborted);
            }
            if present {
                let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
                assert_eq!(
                    wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_ref().unwrap())
                        .unwrap()
                        .state,
                    State::Aborted
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_peer_abort_applies_claim_before_door_and_participant_cleanup() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        let claim = AgentClaim {
            v: 1,
            agent_id: AGENT.into(),
            holder: Some(FROM.into()),
            epoch: 2,
            cut_id: None,
            reason: None,
        };
        f.records.put(
            &format!("claim:{AGENT}"),
            2,
            serde_json::to_vec(&claim).unwrap(),
            TO,
        );
        f.state(State::Aborted, TO);
        f.until(&mut source, Step::Aborted).await;
        let cached = f
            .slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cached_agent_claim(AGENT)
            .unwrap()
            .unwrap();
        assert_eq!(cached.holder.as_deref(), Some(FROM));
        assert_eq!(cached.epoch, 2);
        assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
        let events = f.trace.events.lock().unwrap();
        let door_abort = events.iter().position(|e| e.op == "cut.abort").unwrap();
        let participant_abort = events
            .iter()
            .position(|e| e.op == "agent_data.abort")
            .unwrap();
        assert!(door_abort < participant_abort);
    }

    #[tokio::test(start_paused = true)]
    async fn source_shared_abort_cannot_unfreeze_while_receiver_holds_claim() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        let claim = AgentClaim {
            v: 1,
            agent_id: AGENT.into(),
            holder: Some(TO.into()),
            epoch: 1,
            cut_id: Some(CUT.into()),
            reason: None,
        };
        f.records.put(
            &format!("claim:{AGENT}"),
            1,
            serde_json::to_vec(&claim).unwrap(),
            TO,
        );
        f.state(State::Aborted, TO);
        source.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Published);
        assert!(
            f.participants.iter().all(|p| !p.frozen().is_empty()),
            "shared abort must not unfreeze participants while the receiver holds the claim"
        );
        assert_eq!(f.row().hold.unwrap()["code"], "claim_moved");
    }

    #[tokio::test(start_paused = true)]
    async fn source_unknown_abort_reason_and_corrupt_persisted_refs_do_not_panic() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        let mut record = f.plan().record(CUT);
        record.state = State::Aborted;
        record.reason = Some(MoveReason::Other("future_peer_reason".into()));
        f.records.put(
            &format!("move:{CUT}"),
            4,
            wire::encode_move_record(&format!("move:{CUT}"), &record).unwrap(),
            TO,
        );
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(f.row().reason.as_deref(), Some("future_peer_reason"));
        let mut participant = f.plan().participants[0].clone();
        let duplicate = participant.refs[0].clone();
        participant.refs.push(duplicate);
        assert_eq!(
            source.request(&participant).unwrap_err().code,
            "invalid_refs"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn source_digest_mismatch_abort_stuck_frozen_retries_once_on_restart() {
        for published in [false, true] {
            let f = Fixture::new();
            let mut source = f.source();
            if published {
                f.published(&mut source).await;
            } else {
                f.until(&mut source, Step::Exported).await;
            }
            f.abort();
            for _ in 0..2 {
                f.participants[1].script(
                    "agent_data.abort",
                    ParticipantEffect::Refuse(crate::move_driver::digest_refusal("abort")),
                );
            }
            for _ in 0..3 {
                f.step(&mut source).await;
                if f.row().hold.is_some() {
                    break;
                }
            }
            let row = f.row();
            assert_eq!(
                row.step,
                if published {
                    Step::Published
                } else {
                    Step::Exported
                }
            );
            let hold = row.hold.unwrap();
            assert_eq!(hold["state"], "stuck");
            assert_eq!(hold["module_id"], "b");
            assert_eq!(hold["op"], "agent_data.abort");
            assert_eq!(hold["code"], "digest_mismatch");
            assert_eq!(hold["reason"], "undo_digest_mismatch");
            assert_eq!(row.detail.unwrap(), hold);
            assert!(f.plan().source.contract_violation.is_none());
            assert!(!f.participants[1].frozen().is_empty());
            assert!(!f.participants[2].frozen().is_empty());
            let aborts = || {
                f.participants[1]
                    .calls()
                    .iter()
                    .filter(|r| matches!(r, Request::Abort(_)))
                    .count()
            };
            assert_eq!(aborts(), 1);
            let calls = f
                .participants
                .iter()
                .map(|p| p.calls().len())
                .sum::<usize>();
            for _ in 0..3 {
                f.step(&mut source).await;
            }
            assert_eq!(
                calls,
                f.participants
                    .iter()
                    .map(|p| p.calls().len())
                    .sum::<usize>()
            );
            let mut restarted = f.source();
            f.step(&mut restarted).await;
            assert_eq!(aborts(), 2);
            for _ in 0..3 {
                f.step(&mut restarted).await;
            }
            assert_eq!(aborts(), 2);
            assert!(!f.participants[1].frozen().is_empty());
            assert!(!f.row().step.is_terminal());
            assert_eq!(f.row().hold.unwrap()["reason"], "undo_digest_mismatch");
            restarted.retry_held_abort();
            f.until(&mut restarted, Step::Aborted).await;
            assert_eq!(aborts(), 3);
            assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
            assert!(f.row().hold.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn operator_abort_retry_wakes_only_the_requested_stuck_cut_once() {
        let f = Fixture::new();
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        f.abort();
        for _ in 0..2 {
            f.participants[1].script(
                "agent_data.abort",
                ParticipantEffect::Refuse(crate::move_driver::digest_refusal("abort")),
            );
        }
        f.step(&mut source).await;
        assert!(f.row().abort_stuck());
        let other_cut = "99999999999999999999999999999999";
        let mut other = f.row();
        other.cut_id = other_cut.into();
        other.agent_id = "other-agent".into();
        other.request_key = None;
        f.worker
            .sql(move |store| store.insert_agent_move(&other))
            .await
            .unwrap();
        let mut other_source = Source::new(f.worker.clone(), other_cut.into());
        other_source.abort_held = true;
        let first = tokio::spawn(source.run());
        let second = tokio::spawn(other_source.run());
        let cut = f.row().cut_id;
        f.slot
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .request_agent_move_abort(&cut, 99)
            .unwrap();
        f.worker.wake.notify_waiters();
        tokio::time::timeout(Duration::from_secs(300), async {
            loop {
                let notified = f.trace.participant_called.notified();
                if f.participants[1]
                    .calls()
                    .iter()
                    .filter(|r| matches!(r, Request::Abort(_)))
                    .count()
                    == 2
                {
                    break;
                }
                notified.await;
            }
        })
        .await
        .expect("the requested owner retries abort");
        let aborts: Vec<_> = f.participants[1]
            .calls()
            .into_iter()
            .filter_map(|r| match r {
                Request::Abort(r) => Some(r.cut_id.to_string()),
                _ => None,
            })
            .collect();
        assert_eq!(aborts, vec![f.row().cut_id.clone(), f.row().cut_id]);
        assert!(f.worker.row(other_cut).await.unwrap().abort_stuck());
        assert!(!f.worker.consume_abort_retry(other_cut).await.unwrap());
        first.abort();
        second.abort();
        let _ = first.await;
        let _ = second.await;
    }

    #[tokio::test(start_paused = true)]
    async fn source_abort_at_staged_or_later_is_cleared_and_never_unfreezes() {
        for state in [State::Staged, State::Claimed, State::Activated] {
            let f = Fixture::new();
            let mut source = f.source();
            f.published(&mut source).await;
            f.claim();
            f.state(state, TO);
            f.abort();
            f.door
                .script("cut.handoff", DoorEffect::Refuse("unavailable".into()));
            f.step(&mut source).await;
            let row = f.row();
            assert!(row.abort_requested.is_none());
            assert_eq!(row.detail.as_ref().unwrap()["code"], "invalid_state");
            assert_eq!(row.detail.as_ref().unwrap()["state"], state.as_str());
            let mut restarted = f.source();
            f.step(&mut restarted).await;
            assert!(f
                .participants
                .iter()
                .all(|p| p.calls().iter().all(|r| !matches!(r, Request::Abort(_)))));
            assert!(f.door.calls().iter().all(|c| c.request.op() != "cut.abort"));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_prepare_busy_retries_eight_times_with_paused_exponential_backoff() {
        let f = Fixture::new();
        let mut source = f.source();
        for _ in 0..9 {
            f.participants[0].script(
                "agent_data.prepare",
                ParticipantEffect::Refuse(Refusal {
                    code: "busy".into(),
                    detail: Some(json!({"refs":["ra"]})),
                    message: None,
                }),
            );
        }
        // Record the delay only from ticks that made a prepare call. A tick
        // can also finish a bookkeeping step without calling the participant
        // and return at once; counting that tick as an attempt made this test
        // fail when it ran alone in a fresh process.
        let prepares = |f: &Fixture| {
            f.participants[0]
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Prepare(_)))
                .count()
        };
        let mut delays = vec![];
        // Bounded, so a tick that never reaches prepare fails here, not hangs.
        for _ in 0..30 {
            if delays.len() == 9 {
                break;
            }
            let before = prepares(&f);
            let delay = source.tick().await.unwrap().unwrap();
            if prepares(&f) > before {
                delays.push(delay.as_secs());
            }
            tokio::time::advance(backoff(f.row().attempts)).await;
        }
        assert_eq!(delays, vec![1, 2, 4, 8, 16, 30, 30, 30, 0]);
        assert_eq!(f.row().attempts, 8);
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(f.row().reason.as_deref(), Some("participant_refused"));
        assert_eq!(f.row().detail.unwrap()["code"], "busy");
        assert_eq!(
            f.participants[0]
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Prepare(_)))
                .count(),
            9
        );
        assert!(f.door.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn source_retryable_refusals_cap_backoff_and_unknown_codes_abort_by_name() {
        for code in [
            "cut_not_ready",
            "unavailable",
            "handoff_not_covered",
            "future_code",
            "busy",
        ] {
            let f = Fixture::new();
            let mut source = f.source();
            f.until(&mut source, Step::Exported).await;
            f.door.script("cut.seal", DoorEffect::Refuse(code.into()));
            f.step(&mut source).await;
            if ["cut_not_ready", "unavailable", "handoff_not_covered"].contains(&code) {
                assert!(f.row().abort_requested.is_none());
                assert_eq!(f.row().hold.unwrap()["code"], code);
            } else {
                f.until(&mut source, Step::Aborted).await;
                assert_eq!(f.row().reason.as_deref(), Some("door_refused"));
                assert_eq!(f.row().detail.unwrap()["code"], code);
            }
        }
        assert_eq!(backoff(10), Duration::from_secs(300));
        assert_eq!(backoff(i64::MAX), Duration::from_secs(300));
    }

    #[tokio::test(start_paused = true)]
    async fn source_size_guard_aborts_before_export_or_seal() {
        let f = Fixture::new();
        let mut source = f.source();
        f.participants[0].set_prepare(
            serde_json::from_value(json!({"items":[],"external":[{
            "ref_kind":"session","ref":"ra","note":"n".repeat(wire::MOVE_BODY_CAP),"check":null}]}))
            .unwrap(),
        );
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(f.row().reason.as_deref(), Some("move_too_large"));
        assert!(f
            .participants
            .iter()
            .all(|p| p.calls().iter().all(|r| !matches!(r, Request::Export(_)))));
        assert!(f.door.calls().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn source_inbox_full_aborts_without_publication() {
        let f = Fixture::new();
        let mut source = f.source();
        let inbox = InboxRecord {
            v: 1,
            pending: (0..64)
                .map(|i| PendingMove {
                    cut_id: format!("{i:032x}"),
                    from: FROM.into(),
                    agent_id: format!("other-{i}"),
                })
                .collect(),
        };
        f.records.put(
            &format!("inbox:{TO}"),
            1,
            wire::encode_inbox_record(&inbox).unwrap(),
            TO,
        );
        f.door
            .script("cut.status", DoorEffect::Seal(SealState::Published, None));
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(f.row().reason.as_deref(), Some("receiver_inbox_full"));
        assert!(
            f.worker
                .records
                .read(&format!("move:{CUT}"))
                .await
                .unwrap()
                .absent
        );
        assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
    }

    #[tokio::test(start_paused = true)]
    async fn source_restart_reuses_exact_plan_items_and_cut_at_every_checkpoint() {
        for step in [
            Step::Preparing,
            Step::Exported,
            Step::Sealed,
            Step::Published,
            Step::Releasing,
        ] {
            let f = Fixture::new();
            let mut source = f.source();
            if matches!(step, Step::Published | Step::Releasing) {
                f.published(&mut source).await;
            } else {
                f.until(&mut source, Step::Exported).await;
                if step == Step::Sealed {
                    f.door
                        .script("cut.status", DoorEffect::Seal(SealState::Published, None));
                    f.until(&mut source, Step::Sealed).await;
                }
            }
            f.edit(|row| {
                row.step = step;
                if step == Step::Exported {
                    row.seal_sent = true;
                }
            });
            let original = f.plan();
            drop(source);
            let mut source = f.source();
            if step != Step::Releasing {
                f.published(&mut source).await;
            }
            let recovered = f.plan();
            assert_eq!(recovered.participants, original.participants);
            assert_eq!(recovered.source.items, original.source.items);
            f.claim();
            f.state(State::Activated, TO);
            f.door.script(
                "cut.handoff",
                DoorEffect::Handoff(crate::door_client::HandoffReply {
                    state: HandoffState::Released,
                    session_ref: None,
                    code: None,
                }),
            );
            f.until(&mut source, Step::Released).await;
            for p in &f.participants {
                let prepares: Vec<_> = p
                    .calls()
                    .into_iter()
                    .filter(|r| matches!(r, Request::Prepare(_)))
                    .collect();
                assert!(prepares.iter().all(|r| r == &prepares[0]));
            }
            let cut = f.door.cut(CUT).unwrap();
            assert_eq!(cut.items, original.source.items);
            assert_eq!(cut.handoff.state, HandoffState::Released);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_lost_participant_and_door_replies_settle_without_duplicate_effects() {
        for op in [
            "agent_data.prepare",
            "agent_data.export",
            "agent_data.release",
            "agent_data.abort",
        ] {
            let f = Fixture::new();
            let mut source = f.source();
            for p in &f.participants {
                p.script(op, ParticipantEffect::LoseReply(TransportError::NoReply));
            }
            f.door
                .script("cut.seal", DoorEffect::LoseReply(TransportError::NoReply));
            f.published(&mut source).await;
            if op == "agent_data.abort" {
                f.door
                    .script("cut.abort", DoorEffect::LoseReply(TransportError::NoReply));
                f.abort();
                f.until(&mut source, Step::Aborted).await;
                assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
            } else {
                f.claim();
                f.state(State::Activated, TO);
                f.door.script(
                    "cut.handoff",
                    DoorEffect::Handoff(crate::door_client::HandoffReply {
                        state: HandoffState::Released,
                        session_ref: None,
                        code: None,
                    }),
                );
                f.door.script(
                    "cut.handoff",
                    DoorEffect::LoseReply(TransportError::NoReply),
                );
                f.until(&mut source, Step::Released).await;
                for p in &f.participants {
                    assert_eq!(p.tombstones().len(), 1);
                }
            }
            for p in &f.participants {
                let requests: Vec<_> = p
                    .calls()
                    .into_iter()
                    .filter(|r| r.op().method() == op)
                    .collect();
                assert!(requests.len() >= 2);
                assert!(requests.iter().all(|r| r == &requests[0]));
                assert!(p.calls().iter().any(|r| matches!(r, Request::Status(_))));
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_lost_publication_inbox_abort_and_release_cas_replies_are_idempotent() {
        for target in ["publish", "inbox", "abort", "release"] {
            let f = Fixture::new();
            let mut source = f.source();
            if target == "publish" {
                *f.record_pipe.lose.lock().unwrap() = Some(format!("move:{CUT}"));
            }
            if target == "inbox" {
                *f.record_pipe.lose.lock().unwrap() = Some(format!("inbox:{TO}"));
            }
            f.published(&mut source).await;
            assert_eq!(f.row().record_version, 1);
            let inbox = f.worker.records.read(&format!("inbox:{TO}")).await.unwrap();
            assert_eq!(
                wire::decode_inbox_record(inbox.body.as_ref().unwrap())
                    .unwrap()
                    .pending
                    .len(),
                1
            );
            *f.record_pipe.lose.lock().unwrap() = Some(format!("move:{CUT}"));
            if target == "abort" {
                f.abort();
                f.until(&mut source, Step::Aborted).await;
                assert_eq!(f.row().record_version, 2);
            } else {
                f.claim();
                f.state(State::Activated, TO);
                f.door.script(
                    "cut.handoff",
                    DoorEffect::Handoff(crate::door_client::HandoffReply {
                        state: HandoffState::Released,
                        session_ref: None,
                        code: None,
                    }),
                );
                f.until(&mut source, Step::Released).await;
                assert_eq!(f.row().record_version, 3);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_release_enforcement_is_persisted_per_module_across_a_mid_release_restart() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.participants[0].set_enforced(false);
        f.participants[1].set_enforced(true);
        f.claim();
        f.state(State::Activated, TO);
        f.door.script(
            "cut.handoff",
            DoorEffect::Handoff(crate::door_client::HandoffReply {
                state: HandoffState::Released,
                session_ref: None,
                code: None,
            }),
        );
        f.release_gate.enabled.store(true, Ordering::Relaxed);
        let task = tokio::spawn(async move { source.tick().await });
        f.release_gate.entered.acquire().await.unwrap().forget();
        assert_eq!(f.row().step, Step::Releasing);
        assert_eq!(f.row().plan["_source"]["enforced"], json!({"a":false}));
        assert!(f.participants[1]
            .calls()
            .iter()
            .all(|r| !matches!(r, Request::Release(_))));
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        f.release_gate.enabled.store(false, Ordering::Relaxed);
        f.participants[0].set_enforced(true);
        let mut restarted = f.source();
        f.until(&mut restarted, Step::Released).await;
        assert_eq!(
            f.row().plan["_source"]["enforced"],
            json!({"a":false,"b":true,"c":true})
        );
        assert_eq!(
            f.participants[0]
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Release(_)))
                .count(),
            2
        );
        assert!(f.row().hold.is_none());
        assert_eq!(f.worker.health.move_errors.load(Ordering::Relaxed), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn source_release_contract_violation_stops_calls_without_retry_even_after_restart() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.claim();
        f.state(State::Activated, TO);
        f.door.script(
            "cut.handoff",
            DoorEffect::Handoff(crate::door_client::HandoffReply {
                state: HandoffState::Released,
                session_ref: None,
                code: None,
            }),
        );
        f.participants[1].script(
            "agent_data.release",
            ParticipantEffect::Refuse(Refusal {
                code: "cut_not_ready".into(),
                detail: Some(json!({"note":"not allowed for release"})),
                message: None,
            }),
        );
        f.step(&mut source).await;
        let row = f.row();
        assert_eq!(row.step, Step::Releasing);
        assert_eq!(row.attempts, 0);
        assert!(row.abort_requested.is_none());
        assert_eq!(row.detail.as_ref().unwrap()["contract_violation"], true);
        assert_eq!(row.detail.as_ref().unwrap()["module_id"], "b");
        assert_eq!(row.detail.as_ref().unwrap()["code"], "cut_not_ready");
        assert_eq!(row.plan["_source"]["enforced"], json!({"a":true}));
        assert_eq!(row.plan["_source"]["errors"][0]["contract_violation"], true);
        assert_eq!(f.worker.health.move_errors.load(Ordering::Relaxed), 1);
        let counts: Vec<_> = f.participants.iter().map(|p| p.calls().len()).collect();
        for _ in 0..3 {
            f.step(&mut source).await;
        }
        let mut restarted = f.source();
        f.step(&mut restarted).await;
        assert_eq!(
            f.participants
                .iter()
                .map(|p| p.calls().len())
                .collect::<Vec<_>>(),
            counts
        );
        assert_eq!(
            f.participants[1]
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Release(_)))
                .count(),
            1
        );
        assert!(f.participants[2]
            .calls()
            .iter()
            .all(|r| !matches!(r, Request::Release(_))));
        let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        assert_eq!(
            wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_ref().unwrap())
                .unwrap()
                .state,
            State::Activated
        );
    }

    #[test]
    fn participant_contract_violations_are_never_retryable_by_code() {
        for code in ["unavailable", "cut_not_ready", "handoff_not_covered"] {
            let violation = Failure::participant(
                "agent_data.release",
                "a",
                ClientError::ContractViolation {
                    op: "agent_data.release",
                    code: code.into(),
                    detail: None,
                    message: None,
                },
            );
            assert!(!violation.retryable());
            assert_eq!(violation.detail()["contract_violation"], true);
        }
        let unavailable = Failure::participant(
            "agent_data.release",
            "a",
            ClientError::Refused(Refusal {
                code: "unavailable".into(),
                detail: None,
                message: None,
            }),
        );
        assert!(unavailable.retryable());
        assert_eq!(unavailable.detail()["contract_violation"], false);
    }

    #[tokio::test(start_paused = true)]
    async fn source_releasing_refusals_hold_and_claim_failures_do_not_release() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.state(State::Activated, TO);
        f.records
            .error(&format!("claim:{AGENT}"), Some("unavailable"));
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Releasing);
        assert!(f
            .door
            .calls()
            .iter()
            .all(|c| c.request.op() != "cut.handoff"));
        f.records.error(&format!("claim:{AGENT}"), None);
        f.claim();
        f.door
            .script("cut.handoff", DoorEffect::Refuse("future_code".into()));
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Releasing);
        assert_eq!(f.row().hold.unwrap()["code"], "future_code");
        assert!(f.row().abort_requested.is_none());
        f.door.script(
            "cut.handoff",
            DoorEffect::Handoff(crate::door_client::HandoffReply {
                state: HandoffState::TakenOver,
                session_ref: None,
                code: None,
            }),
        );
        f.until(&mut source, Step::Released).await;
    }

    #[tokio::test(start_paused = true)]
    async fn source_abort_cas_losing_to_staging_never_cleans_participants() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        let mut staged = f.plan().record(CUT);
        staged.state = State::Staged;
        *f.record_pipe.race.lock().unwrap() = Some((
            wire::encode_move_record(&format!("move:{CUT}"), &staged).unwrap(),
            TO.into(),
        ));
        f.abort();
        f.step(&mut source).await;
        f.step(&mut source).await;
        assert!(f.row().abort_requested.is_none());
        assert_eq!(f.row().detail.unwrap()["code"], "invalid_state");
        assert!(f
            .participants
            .iter()
            .all(|p| p.calls().iter().all(|r| !matches!(r, Request::Abort(_)))));
        assert!(f.door.calls().iter().all(|c| c.request.op() != "cut.abort"));
    }

    #[tokio::test(start_paused = true)]
    async fn source_abort_waits_for_unknown_prepare_to_settle_even_after_restart() {
        for restart in [false, true] {
            let f = Fixture::new();
            let mut source = f.source();
            f.participants[0].script(
                "agent_data.prepare",
                ParticipantEffect::LoseReply(TransportError::NoReply),
            );
            f.participants[0].script(
                "agent_data.status",
                ParticipantEffect::FailBefore(TransportError::Unavailable),
            );
            f.step(&mut source).await;
            assert!(f.plan().source.pending.is_some());
            assert!(!f.participants[0].frozen().is_empty());
            f.abort();
            if restart {
                source = f.source();
            }
            f.until(&mut source, Step::Aborted).await;
            assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
            let calls = f.participants[0].calls();
            let abort = calls
                .iter()
                .position(|r| matches!(r, Request::Abort(_)))
                .unwrap();
            assert!(matches!(calls[abort - 1], Request::Prepare(_)));
            assert!(calls[..abort]
                .iter()
                .any(|r| matches!(r, Request::Status(_))));
            assert!(calls.iter().all(|r| !matches!(r, Request::Export(_))));
            assert!(f.row().abort_requested.is_none());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_failed_or_aborted_seal_records_door_refused_and_cleans_up() {
        for state in [SealState::Failed, SealState::Aborted] {
            let f = Fixture::new();
            let mut source = f.source();
            f.until(&mut source, Step::Exported).await;
            f.door.script(
                "cut.seal",
                DoorEffect::Seal(state, Some(json!({"cause":"bad stream"}))),
            );
            f.until(&mut source, Step::Aborted).await;
            assert_eq!(f.row().reason.as_deref(), Some("door_refused"));
            let detail = f.row().detail.unwrap();
            assert_eq!(detail["op"], "cut.status");
            assert_eq!(
                detail["code"],
                if state == SealState::Failed {
                    "failed"
                } else {
                    "aborted"
                }
            );
            assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_abort_keeps_row_open_until_claim_cache_can_be_applied() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        f.state(State::Aborted, TO);
        f.records
            .error(&format!("claim:{AGENT}"), Some("unavailable"));
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Published);
        assert!(f.participants.iter().all(|p| !p.frozen().is_empty()));
        assert!(f.door.calls().iter().all(|c| c.request.op() != "cut.abort"));
        f.records.error(&format!("claim:{AGENT}"), None);
        f.until(&mut source, Step::Aborted).await;
    }

    #[allow(
        clippy::await_holding_lock,
        reason = "holding the store mutex deliberately proves health does not acquire it"
    )]
    #[tokio::test(start_paused = true)]
    async fn source_hung_remote_call_holds_no_store_and_health_is_lock_free() {
        let f = Fixture::new();
        let mut source = f.source();
        f.published(&mut source).await;
        // The recording wrapper reads SQLite to capture checkpoints. Use the
        // raw fake here so removing the store tests only the driver's ownership.
        source.worker.records = RecordClient::new(Arc::new(FakeRecordConnector(f.records.clone())));
        f.records.hung.store(true, Ordering::Relaxed);
        let calls = f.records.calls.load(Ordering::Relaxed);
        let task = tokio::spawn(async move { source.tick().await });
        f.records.wait_for_calls(calls + 1).await;
        *f.slot.lock().unwrap() = None;
        let reopened = RegistryStore::open(&f.descriptor).unwrap();
        let mut handler = crate::ProjectsHandler::with_runtime("test".into(), clock);
        handler.health = f.worker.health.clone();
        let locked = handler.store.lock().unwrap();
        let health = subc_client_rs::ModuleHandler::health(&handler).await;
        assert_eq!(health.metrics.unwrap()["movesOpen"], 1);
        drop(locked);
        drop(reopened);
        task.abort();
    }

    /// The door ops this move sent, in order, read from the transport trace.
    /// The trace records every call before the fake door validates it, so a
    /// seal the door refuses for its shape still appears here.
    fn door_ops(f: &Fixture) -> Vec<String> {
        f.trace
            .events
            .lock()
            .unwrap()
            .iter()
            .filter(|event| event.op.starts_with("cut."))
            .map(|event| event.op.clone())
            .collect()
    }

    /// Every participant prepares no items, so the cut is empty. The fake door,
    /// like engram, refuses an empty `cut.seal` with `invalid_request` every
    /// time it is sent.
    fn empty_cut(f: &Fixture) {
        for participant in &f.participants {
            participant
                .set_prepare(serde_json::from_value(json!({"items":[],"external":[]})).unwrap());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_refused_seal_aborts_without_resealing_or_aborting_the_door() {
        let f = Fixture::new();
        empty_cut(&f);
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        f.step(&mut source).await;
        let row = f.row();
        assert!(row.abort_requested.is_some());
        assert_eq!(
            f.plan().source.seal_refused.as_deref(),
            Some("invalid_request")
        );
        f.until(&mut source, Step::Aborted).await;
        let row = f.row();
        assert_eq!(row.reason.as_deref(), Some("door_refused"));
        assert_eq!(row.detail.as_ref().unwrap()["op"], "cut.seal");
        assert_eq!(row.detail.as_ref().unwrap()["code"], "invalid_request");
        // The refused seal is the only door call: cleanup neither repeats it
        // nor aborts a cut the door never held.
        assert_eq!(door_ops(&f), ["cut.seal"]);
        assert!(f.door.cut(CUT).is_none());
        for participant in &f.participants {
            assert!(participant.frozen().is_empty());
            assert!(participant
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Abort(_))));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn source_refused_seal_cleanup_skips_the_door_after_a_mid_cleanup_restart() {
        let f = Fixture::new();
        empty_cut(&f);
        f.participants[1].script(
            "agent_data.abort",
            ParticipantEffect::Refuse(Refusal {
                code: "unavailable".into(),
                detail: None,
                message: None,
            }),
        );
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        f.step(&mut source).await;
        assert!(f.row().abort_requested.is_some());
        // Cleanup aborts participant a, then b refuses, so the cleanup stops
        // part way with a retry scheduled.
        f.step(&mut source).await;
        assert_eq!(f.row().step, Step::Exported);
        assert_eq!(f.plan().source.aborted, ["a"]);
        drop(source);
        let mut restarted = f.source();
        f.until(&mut restarted, Step::Aborted).await;
        assert_eq!(f.row().reason.as_deref(), Some("door_refused"));
        assert_eq!(door_ops(&f), ["cut.seal"]);
        assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
    }

    #[tokio::test(start_paused = true)]
    async fn source_unknown_seal_outcome_cleanup_still_reseals_then_aborts_the_door() {
        let f = Fixture::new();
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        // The door creates the cut but both replies are lost, so the client
        // reports an unknown outcome while the door holds a pending cut.
        for _ in 0..2 {
            f.door
                .script("cut.seal", DoorEffect::LoseReply(TransportError::NoReply));
        }
        f.step(&mut source).await;
        assert!(f.row().abort_requested.is_none());
        assert!(f.door.cut(CUT).is_some_and(|cut| !cut.aborted));
        let before = door_ops(&f).len();
        f.abort();
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(f.plan().source.seal_refused, None);
        let cleanup: Vec<_> = door_ops(&f)
            .split_off(before)
            .into_iter()
            .filter(|op| op == "cut.seal" || op == "cut.abort")
            .collect();
        assert_eq!(cleanup, ["cut.seal", "cut.abort"]);
        assert!(f.door.cut(CUT).unwrap().aborted);
        assert!(f.participants.iter().all(|p| p.frozen().is_empty()));
    }

    #[tokio::test(start_paused = true)]
    async fn source_cleanup_reseal_refused_for_a_cut_the_door_never_received_skips_abort() {
        let f = Fixture::new();
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        // Both sends fail before the door sees them: the outcome is unknown to
        // the driver, and the door holds no cut.
        for _ in 0..2 {
            f.door
                .script("cut.seal", DoorEffect::FailBefore(TransportError::NoReply));
        }
        f.step(&mut source).await;
        assert!(f.door.cut(CUT).is_none());
        f.abort();
        f.door.script(
            "cut.seal",
            DoorEffect::RefuseWithMessage("cut_end_mismatch".into(), "the stream moved".into()),
        );
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(
            f.plan().source.seal_refused.as_deref(),
            Some("cut_end_mismatch")
        );
        assert!(door_ops(&f).iter().all(|op| op != "cut.abort"));
        assert!(f.door.cut(CUT).is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn source_cleanup_treats_a_door_abort_not_found_as_already_gone() {
        let f = Fixture::new();
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        for _ in 0..2 {
            f.door
                .script("cut.seal", DoorEffect::LoseReply(TransportError::NoReply));
        }
        f.step(&mut source).await;
        f.abort();
        f.door
            .script("cut.abort", DoorEffect::Refuse("not_found".into()));
        f.until(&mut source, Step::Aborted).await;
        assert_eq!(
            door_ops(&f).iter().filter(|op| *op == "cut.abort").count(),
            1
        );
    }

    /// Collects the fields of each `move` target event as one line.
    struct MoveLog(Arc<Mutex<Vec<String>>>);
    impl tracing::Subscriber for MoveLog {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.push_str(&format!("{}={value:?};", field.name()));
                }
            }
            if event.metadata().target() == "move" {
                let mut fields = Fields(String::new());
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test(start_paused = true)]
    async fn source_refusals_log_once_per_code_and_keep_the_message_in_detail() {
        let f = Fixture::new();
        let lines = Arc::new(Mutex::new(Vec::new()));
        let _subscriber = tracing::subscriber::set_default(MoveLog(lines.clone()));
        let mut source = f.source();
        f.until(&mut source, Step::Exported).await;
        for _ in 0..3 {
            f.door.script(
                "cut.seal",
                DoorEffect::RefuseWithMessage("cut_not_ready".into(), "streams settling".into()),
            );
        }
        f.door.script(
            "cut.seal",
            DoorEffect::RefuseWithMessage("invalid_request".into(), "engram says no".into()),
        );
        f.until(&mut source, Step::Aborted).await;
        let row = f.row();
        assert_eq!(row.detail.as_ref().unwrap()["code"], "invalid_request");
        assert_eq!(row.detail.as_ref().unwrap()["message"], "engram says no");
        // Three retried refusals and the permanent one reached the driver.
        assert_eq!(
            door_ops(&f).iter().filter(|op| *op == "cut.seal").count(),
            4
        );
        let lines = lines.lock().unwrap();
        assert_eq!(lines.len(), 2, "{lines:?}");
        for (line, code, message) in [
            (&lines[0], "cut_not_ready", "streams settling"),
            (&lines[1], "invalid_request", "engram says no"),
        ] {
            assert!(line.contains(&format!("cut={CUT:?}")), "{line}");
            assert!(line.contains("op=\"cut.seal\""), "{line}");
            assert!(line.contains(&format!("code={code:?}")), "{line}");
            assert!(line.contains(&format!("message={message:?}")), "{line}");
            for item in &f.plan().source.items {
                assert!(!line.contains(&item.session_ref), "{line}");
            }
        }
    }

    #[test]
    fn participant_refusal_message_is_kept_in_the_stored_detail() {
        let refused = Failure::participant(
            "agent_data.prepare",
            "a",
            ClientError::Refused(Refusal {
                code: "busy".into(),
                detail: None,
                message: Some("index rebuilding".into()),
            }),
        );
        assert!(refused.refused());
        assert_eq!(refused.detail()["message"], "index rebuilding");
        let violation = Failure::participant(
            "agent_data.release",
            "a",
            ClientError::ContractViolation {
                op: "agent_data.release",
                code: "busy".into(),
                detail: None,
                message: Some("not now".into()),
            },
        );
        assert!(violation.refused());
        assert_eq!(violation.detail()["message"], "not now");
        let lost = Failure::participant("agent_data.prepare", "a", ClientError::OutcomeUnknown);
        assert!(!lost.refused());
        assert!(lost.detail().get("message").is_none());
    }
}

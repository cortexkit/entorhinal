//! Receive published moves: validate the inbox and import data, then durably
//! transfer the agent claim before any participant makes the data live.

use crate::{
    claim_codec::AgentClaim,
    door_client::CutRequest as DoorCut,
    move_driver::{
        backoff, digest_reason, participant_action, PeerRefusal, RefusalLog, Worker, POLL,
    },
    participant_client::{AgentRef, ClientError, CutRequest, ParticipantClient, Refs},
    participant_discovery::Participant,
    record_client::{CasRequest, CasSettlement, RecordError, RecordReply},
    session_move::StagedError,
    HandlerError, LivenessState, SessionKey,
};
use cortexkit_role_agent_data::errors::DriverAction;
use cortexkit_role_agent_data::{busy::BusyState, diverged::DivergedState};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    move_record::{
        self as wire, InboxRecord, MoveReason, MoveRecord, MoveRecordState as State, PendingMove,
    },
    move_store::{AgentMove, MoveSide, MoveStep as Step},
    session_carry::{is_binding_ref_kind, CarriedBinding, CarriedInstall, SESSION_BINDINGS_KIND},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};
use subc_client_rs::{CatalogList, SupervisorListResponse};

fn error(code: &str) -> HandlerError {
    HandlerError::new(code, code)
}
fn record_error(e: RecordError) -> HandlerError {
    HandlerError::new(e.code(), format!("{e:?}"))
}
fn checked_version(version: u64) -> Result<i64, HandlerError> {
    i64::try_from(version).map_err(|_| error("record_version_unknown"))
}
async fn inbox(worker: &Worker, own: &str) -> Result<(RecordReply, InboxRecord), HandlerError> {
    let subject = format!("inbox:{own}");
    let reply = worker.records.read(&subject).await.map_err(record_error)?;
    if reply.stale {
        return Err(error("unavailable"));
    }
    let body = if reply.absent {
        InboxRecord::default()
    } else {
        wire::decode_inbox_record(reply.body.as_deref().unwrap_or_default())
            .map_err(|e| error(e.code()))?
    };
    Ok((reply, body))
}

pub(crate) async fn remove_inbox(
    worker: &Worker,
    own: &str,
    cut: &str,
) -> Result<(), HandlerError> {
    loop {
        let (reply, body) = inbox(worker, own).await?;
        let next = wire::remove_inbox(&body, cut).map_err(|e| error(e.code()))?;
        if next == body {
            return Ok(());
        }
        let mut write_id = [0; 16];
        getrandom::getrandom(&mut write_id)
            .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
        let subject = format!("inbox:{own}");
        let request = CasRequest {
            body: wire::encode_inbox_record(&next).map_err(|e| error(e.code()))?,
            subject,
            expected_version: reply.version,
            write_id,
        };
        match worker.records.cas(&request).await {
            Ok(_) => return Ok(()),
            Err(e) if e.code() == "version_moved" || matches!(e, RecordError::NoReply) => {}
            Err(e) => return Err(record_error(e)),
        }
    }
}

fn pickup_row(record: &MoveRecord, entry: &PendingMove, version: i64, now: i64) -> AgentMove {
    AgentMove {
        cut_id: entry.cut_id.clone(),
        agent_id: entry.agent_id.clone(),
        side: MoveSide::Receiver,
        peer: entry.from.clone(),
        step: Step::PickedUp,
        plan: json!(record),
        request_body: None,
        record_version: version,
        claim_version: Some(record.claim_version as i64),
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
        created_at_ms: now,
        updated_at_ms: now,
    }
}

pub(crate) async fn poll_inbox(worker: &Worker) -> Result<(), HandlerError> {
    let own = worker
        .sql(|s| {
            Ok(if s.identity_log_status()?.state == "enabled" {
                s.persisted_self_author()?
            } else {
                None
            })
        })
        .await?;
    let Some(own) = own else {
        return Ok(());
    };
    let (_, body) = inbox(worker, &own).await?;
    for entry in body.pending {
        // One unavailable cut must not delay unrelated published cuts behind it.
        if let Err(e) = poll_entry(worker, &own, &entry).await {
            eprintln!("move inbox {}: retrying after {}", entry.cut_id, e.code);
        }
    }
    worker.gauges().await
}

async fn reject_entry(
    worker: &Worker,
    own: &str,
    cut: &str,
    code: &str,
) -> Result<(), HandlerError> {
    remove_inbox(worker, own, cut).await?;
    worker
        .health
        .move_inbox_rejected
        .fetch_add(1, Ordering::Relaxed);
    // Log only after removal, so repeated polls do not count or log the same rejection.
    eprintln!("move inbox {cut}: removed permanently invalid entry ({code})");
    Ok(())
}

async fn poll_entry(worker: &Worker, own: &str, entry: &PendingMove) -> Result<(), HandlerError> {
    let cut = entry.cut_id.clone();
    let existing = worker.sql(move |s| s.agent_move(&cut)).await?;
    if let Some(row) = existing.as_ref() {
        if row.step.is_terminal() || row.step == Step::Activated {
            remove_inbox(worker, own, &entry.cut_id).await?;
        }
        if row.step.is_terminal() || row.step == Step::Activated {
            return Ok(());
        }
    }
    let subject = format!("move:{}", entry.cut_id);
    let reply = worker.records.read(&subject).await.map_err(record_error)?;
    if reply.stale {
        return Ok(());
    }
    if reply.absent {
        return reject_entry(worker, own, &entry.cut_id, "record_absent").await;
    }
    let record = match wire::decode_move_record(&subject, reply.body.as_deref().unwrap_or_default())
    {
        Ok(record) => record,
        Err(e) => return reject_entry(worker, own, &entry.cut_id, e.code()).await,
    };
    if record.to != own || matches!(record.state, State::Aborted | State::Released) {
        remove_inbox(worker, own, &entry.cut_id).await?;
        return Ok(());
    }
    if existing.is_some() {
        return Ok(());
    }
    if record.state != State::Published {
        return Ok(());
    }
    let rows = worker.sql(RegistryStore::list_open_agent_moves).await?;
    if rows.len() >= 4 || rows.iter().any(|r| r.agent_id == entry.agent_id) {
        return Ok(());
    }
    let read = match worker
        .door
        .read(&DoorCut {
            cut_id: entry.cut_id.clone(),
        })
        .await
    {
        Ok(read) => read,
        Err(crate::door_client::DoorError::Refused { code, .. })
            if matches!(
                code.as_str(),
                "not_found" | "cut_aborted" | "invalid_request" | "id_reused"
            ) =>
        {
            return reject_entry(worker, own, &entry.cut_id, &code).await;
        }
        Err(e) => return Err(HandlerError::new(e.code(), format!("{e:?}"))),
    };
    if checked_version(record.claim_version).is_err() || checked_version(reply.version).is_err() {
        return reject_entry(worker, own, &entry.cut_id, "record_version_unknown").await;
    }
    let mut row = pickup_row(
        &record,
        entry,
        checked_version(reply.version)?,
        (worker.clock)(),
    );
    let mismatch = reply.author.as_deref() != Some(&entry.from)
        || record.from != entry.from
        || read.author != entry.from
        || record.agent_id != entry.agent_id;
    if mismatch {
        row.step = Step::Aborted;
        row.reason = Some("cut_author_mismatch".into());
    }
    // The earlier open-move list avoids unnecessary reads from the door
    // service. Before inserting a row, the core transaction rechecks the
    // agent slot and move cap because source admissions and inbox pickups can race.
    let result = worker.sql(move |s| Ok(s.admit_received_move(&row))).await?;
    match result {
        Ok(row) if row.step == Step::Aborted => {
            reject_entry(worker, own, &entry.cut_id, "cut_author_mismatch").await?
        }
        Ok(_) => {}
        Err(e) if matches!(e.code.as_str(), "move_in_progress" | "move_cap") => {}
        Err(e) => return Err(e.into()),
    }
    Ok(())
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Progress {
    #[serde(default)]
    aborted: Vec<String>,
    #[serde(default)]
    touched: Vec<String>,
    #[serde(default)]
    errors: Vec<Value>,
    #[serde(default)]
    retry_at_ms: i64,
    #[serde(default)]
    pending: Option<PendingCall>,
    #[serde(default)]
    take_failed: bool,
    #[serde(default)]
    contract_violation: Option<Value>,
    #[serde(default)]
    activation_fork: Option<Value>,
    #[serde(default)]
    takeover_sent: bool,
    /// Plan participants this Mac leaves out of the whole receiver side of the
    /// move: each was neither configured nor registered here when the
    /// preconditions ran, and the source sent no refs for it, so there is no
    /// data to import, check for forks, activate or undo. `None` means the
    /// preconditions have not decided yet. Once decided, every later step and
    /// every restart reads this set instead of asking the daemon again, so a
    /// module installed mid-move is never called for a cut it never saw.
    /// Rows written before this field existed decode as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skipped_absent: Option<Vec<String>>,
}
impl Progress {
    fn skips(&self, module: &str) -> bool {
        self.skipped_absent
            .as_ref()
            .is_some_and(|skipped| skipped.iter().any(|m| m == module))
    }
}

/// The plan participants this receiver calls: every one except those
/// recorded as skipped because they are absent here and carry no refs.
fn called<'a>(plan: &'a MoveRecord, progress: &Progress) -> Vec<&'a wire::MoveParticipant> {
    plan.participants
        .iter()
        .filter(|p| !progress.skips(&p.module_id))
        .collect()
}

/// Plan participants this Mac can leave out of the move: the source sent no
/// refs for them, and this Mac neither configures nor registers them, so they
/// cannot hold local data for the agent and have nothing staged to undo.
fn absent_without_refs(
    plan: &MoveRecord,
    supervisor: &SupervisorListResponse,
    catalog: &CatalogList,
) -> Vec<String> {
    let absent = |module: &str| {
        !supervisor.modules.iter().any(|m| m.module_id == module)
            && !catalog.modules.iter().any(|m| m.module_id == module)
    };
    plan.participants
        .iter()
        .filter(|p| p.refs.is_empty() && absent(&p.module_id))
        .map(|p| p.module_id.clone())
        .collect()
}

/// Participants a receiver row recorded as skipped (absent on that Mac with
/// nothing to carry), for move status. Empty for source rows and for rows
/// whose preconditions have not run yet.
pub(crate) fn skipped_participants(row: &AgentMove) -> Vec<String> {
    if row.side != MoveSide::Receiver {
        return Vec::new();
    }
    row.plan["_receiver"]["skipped_absent"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|m| m.as_str().map(str::to_owned))
        .collect()
}
#[derive(Clone, Serialize, Deserialize)]
struct PendingCall {
    module: String,
    op: String,
}

struct Failure {
    code: String,
    detail: Value,
    reason: Option<MoveReason>,
    diagnostic: bool,
    violation: bool,
    action: Option<DriverAction>,
    /// Set when the door or a participant answered with a refusal, as opposed
    /// to a lost reply or a local store or record error.
    refusal: Option<Box<PeerRefusal>>,
}
impl Failure {
    fn hold(code: &str, detail: Value) -> Self {
        Self {
            code: code.into(),
            detail,
            reason: None,
            diagnostic: false,
            violation: false,
            action: None,
            refusal: None,
        }
    }
    fn abort(reason: MoveReason, detail: Value) -> Self {
        Self {
            reason: Some(reason),
            ..Self::hold("abort", detail)
        }
    }
    fn participant(module: &str, op: &str, e: ClientError) -> Self {
        let (code, detail, violation, refused, message) = match e {
            ClientError::Refused(r) => (r.code, r.detail, false, true, r.message),
            ClientError::ContractViolation {
                code,
                detail,
                message,
                ..
            } => (code, detail, true, true, message),
            ClientError::OutcomeUnknown => ("unavailable".into(), None, false, false, None),
            ClientError::InvalidReply(message) => (
                "invalid_reply".into(),
                Some(json!({"message":message})),
                true,
                false,
                None,
            ),
        };
        let reason = digest_reason(&code, detail.as_ref());
        let mut detail = json!({"module_id":module,"op":op,"code":code,"detail":detail});
        if let Some(reason) = reason {
            detail["reason"] = json!(reason);
        }
        if let Some(message) = &message {
            detail["message"] = json!(message);
        }
        let mut failure = Self::hold(&code, detail);
        failure.violation = violation;
        if refused {
            failure.refusal = Some(Box::new(PeerRefusal { message }));
        }
        failure.action = participant_action(op, &code);
        if !violation
            && matches!(
                failure.action,
                Some(DriverAction::Abort | DriverAction::TerminalForStep)
            )
        {
            failure.reason = Some(match code.as_str() {
                "local_advanced" if op == "agent_data.import" => MoveReason::Diverged,
                "incompatible_version" => MoveReason::IncompatibleVersion,
                _ => MoveReason::ParticipantRefused,
            });
            if code == "incompatible_version" {
                failure.detail["note"] = json!("Upgrade this Mac to import the cut's format");
            }
        }
        failure
    }
}
impl From<HandlerError> for Failure {
    fn from(e: HandlerError) -> Self {
        Self::hold(&e.code, e.detail.unwrap_or(json!({"message":e.message})))
    }
}
impl From<RecordError> for Failure {
    fn from(e: RecordError) -> Self {
        Self::hold(e.code(), json!({"op":"record","error":format!("{e:?}")}))
    }
}

pub(crate) struct Receiver {
    worker: Worker,
    cut: String,
    clients: BTreeMap<String, ParticipantClient>,
    liveness: Arc<Mutex<LivenessState>>,
    abort_held: bool,
    refusals: RefusalLog,
}
impl Receiver {
    pub(crate) fn new(worker: Worker, cut: String, liveness: Arc<Mutex<LivenessState>>) -> Self {
        Self {
            worker,
            cut,
            clients: BTreeMap::new(),
            liveness,
            abort_held: false,
            refusals: RefusalLog::default(),
        }
    }
    /// Resume participant aborts after an operator repairs corrupted undo bytes.
    /// A participant's abort refusal with digest_mismatch means its undo data
    /// is corrupted. Pause cleanup until repair and operator retry or restart;
    /// restart retries once, then pauses again if the participant still refuses.
    pub(crate) fn retry_held_abort(&mut self) {
        self.abort_held = false;
    }
    pub(crate) async fn run(mut self) {
        let mut signals = self.worker.wake.subscribe();
        let mut delay = Duration::ZERO;
        loop {
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = signals.notified() => {} }
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
    async fn save(&self, row: &mut AgentMove, progress: &Progress) -> Result<(), Failure> {
        row.plan["_receiver"] = serde_json::to_value(progress)
            .map_err(|e| Failure::hold("plan_malformed", json!({"error":e.to_string()})))?;
        self.worker.save(row, false).await?;
        Ok(())
    }
    async fn read_move(&self, plan: &MoveRecord) -> Result<(MoveRecord, u64), Failure> {
        let subject = format!("move:{}", self.cut);
        let reply = self.worker.records.read(&subject).await?;
        if reply.stale || reply.absent {
            return Err(Failure::hold("unavailable", json!({"op":"record.read"})));
        }
        checked_version(reply.version)?;
        let verified = wire::decode_move_record(
            &subject,
            reply.body.as_deref().unwrap_or_default(),
        )
        .and_then(|record| {
            wire::check_record_author(&record, reply.author.as_deref().unwrap_or_default())?;
            wire::check_pinned_plan(plan, &record)?;
            Ok(record)
        });
        verified.map(|record| (record, reply.version)).map_err(|e| {
            let mut f = Failure::hold(e.code(), json!({"op":"record.read","code":e.code()}));
            f.diagnostic = true;
            f
        })
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
                Failure::hold(
                    "participant_unavailable",
                    json!({"module_id":module,"op":"route.open","error":format!("{e:?}")}),
                )
            })?;
        let client = ParticipantClient::new(&Participant {
            module_id: module.into(),
            transport,
        });
        self.clients.insert(module.into(), client.clone());
        Ok(client)
    }
    async fn discovery_lists(&self) -> Result<(SupervisorListResponse, CatalogList), Failure> {
        let supervisor = self.worker.discovery.supervisor_list().await.map_err(|e| {
            Failure::hold(
                "unavailable",
                json!({"op":"supervisor.list","error":format!("{e:?}")}),
            )
        })?;
        let catalog = self.worker.discovery.catalog_list().await.map_err(|e| {
            Failure::hold(
                "unavailable",
                json!({"op":"catalog.list","error":format!("{e:?}")}),
            )
        })?;
        Ok((supervisor, catalog))
    }
    fn request(&self, p: &wire::MoveParticipant) -> Result<CutRequest, Failure> {
        Ok(CutRequest::new(
            Refs::new(
                p.refs
                    .iter()
                    .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
                    .collect(),
            )
            .map_err(|_| Failure::hold("invalid_refs", json!({"op":"plan.read"})))?,
            cortexkit_role_agent_data::refs::CutId::new(self.cut.clone())
                .map_err(|_| Failure::hold("invalid_cut_id", json!({"op":"plan.read"})))?,
        ))
    }
    fn step(row: &mut AgentMove, progress: &mut Progress, step: Step) {
        row.step = step;
        row.attempts = 0;
        row.hold = None;
        progress.retry_at_ms = 0;
    }

    pub(crate) async fn tick(&mut self) -> Result<Option<Duration>, HandlerError> {
        let mut row = self.worker.row(&self.cut).await?;
        if row.step.is_terminal() {
            return Ok(None);
        }
        let mut plan_value = row.plan.clone();
        let progress_value = plan_value
            .as_object_mut()
            .and_then(|v| v.remove("_receiver"));
        let mut progress: Progress = progress_value
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| HandlerError::new("plan_malformed", e.to_string()))?
            .unwrap_or_default();
        let plan: MoveRecord = serde_json::from_value(plan_value)
            .map_err(|e| HandlerError::new("plan_malformed", e.to_string()))?;
        // Whether this pass ran abort cleanup, so a refusal from it backs off
        // instead of being answered with another immediate abort pass.
        let mut cleaning = false;
        let result: Result<Duration, Failure> = async {
            let (observed, version) = self.read_move(&plan).await?;
            row.record_version = checked_version(version)?;
            if observed.state == State::Aborted
                && matches!(
                    row.step,
                    Step::PickedUp | Step::Imported | Step::Staged | Step::Claimed
                )
                && row.abort_requested.is_none()
            {
                row.reason = observed.reason.clone().map(String::from);
                row.detail = observed.detail.clone();
                row.abort_requested = Some(json!({"reason":row.reason,"detail":row.detail}));
                self.save(&mut row, &progress).await?;
            }
            if progress.contract_violation.is_some() || progress.activation_fork.is_some() {
                return Ok(POLL);
            }
            if self.abort_held {
                return Ok(POLL);
            }
            // Finish the move when the source reports release, even if accepting
            // a previous move-back cut is still failing or waiting for its retry deadline.
            if row.step == Step::Activated && observed.state == State::Released {
                Self::step(&mut row, &mut progress, Step::Done);
                self.save(&mut row, &progress).await?;
                return Ok(Duration::ZERO);
            }
            if (self.worker.clock)() < progress.retry_at_ms {
                return Ok(Duration::from_millis(
                    (progress.retry_at_ms - (self.worker.clock)()) as u64,
                )
                .min(POLL));
            }
            if row.abort_requested.is_some()
                && matches!(
                    row.step,
                    Step::PickedUp | Step::Imported | Step::Staged | Step::Claimed
                )
            {
                cleaning = true;
                self.cleanup(&mut row, &plan, &mut progress).await?;
            } else {
                self.advance(&mut row, &plan, &mut progress, observed)
                    .await?;
            }
            Ok(Duration::ZERO)
        }
        .await;
        match result {
            Ok(_) if row.step.is_terminal() => Ok(None),
            Ok(delay) => Ok(Some(
                if matches!(row.step, Step::Claimed | Step::Activating | Step::Activated) {
                    POLL
                } else {
                    delay
                },
            )),
            Err(mut f) => {
                if let Some(refusal) = &f.refusal {
                    self.refusals.note(
                        &self.cut,
                        f.detail["module_id"].as_str().unwrap_or("engram"),
                        f.detail["op"].as_str().unwrap_or("unknown"),
                        &f.code,
                        refusal.message.as_deref(),
                    );
                }
                if !f.violation
                    && f.detail["op"] == "agent_data.abort"
                    && f.action == Some(DriverAction::RetryAfterOperator)
                {
                    self.abort_held = true;
                    f.detail["state"] = json!("stuck");
                    f.detail["step"] = json!(row.step);
                    f.detail["move_error"] = json!(true);
                    row.detail = Some(f.detail.clone());
                    row.hold = Some(f.detail);
                    progress.retry_at_ms = 0;
                    self.save(&mut row, &progress)
                        .await
                        .map_err(|e| HandlerError::new(&e.code, e.detail.to_string()))?;
                    return Ok(Some(POLL));
                }
                let irreversible = !row.step.before_boundary(MoveSide::Receiver);
                if irreversible {
                    f.reason = None;
                    f.diagnostic = true;
                    if f.action == Some(DriverAction::TerminalForStep) {
                        f.detail["note"] = json!(
                            "Activation cannot safely finish; operator fork resolution is required"
                        );
                        progress.activation_fork = Some(f.detail.clone());
                        row.detail = Some(f.detail.clone());
                    }
                }
                if f.violation || f.diagnostic || f.reason == Some(MoveReason::ClaimMoved) {
                    progress.errors.push(f.detail.clone());
                }
                if f.violation {
                    progress.contract_violation = Some(f.detail.clone());
                }
                if let Some(reason) = f.reason.as_ref() {
                    row.reason = Some(String::from(reason.clone()));
                    row.detail = Some(f.detail.clone());
                    row.abort_requested = Some(json!({"reason":row.reason,"detail":row.detail}));
                    if cleaning {
                        // The move is already aborting, so the next pass
                        // repeats the same cleanup calls. Back off as any
                        // other retry does (1 s doubling to 5 minutes); the
                        // attempt count and deadline are saved below, so a
                        // restart keeps waiting. Reaching a new step resets
                        // both.
                        row.attempts += 1;
                        progress.retry_at_ms = (self.worker.clock)()
                            .saturating_add(backoff(row.attempts).as_millis() as i64);
                    } else {
                        progress.retry_at_ms = 0;
                    }
                } else {
                    row.attempts += 1;
                    let delay = backoff(row.attempts);
                    progress.retry_at_ms =
                        (self.worker.clock)().saturating_add(delay.as_millis() as i64);
                    row.hold = Some(
                        json!({"code":f.code,"detail":f.detail,"move_error":f.violation || f.diagnostic}),
                    );
                    if let Some(module) = f.detail.get("module_id") {
                        row.hold
                            .as_mut()
                            .expect("this branch assigned Some(hold) immediately above")
                            ["module_id"] = module.clone();
                    }
                    for key in ["ref", "reason_code", "reason", "op", "note"] {
                        if let Some(value) = f.detail.get(key) {
                            row.hold
                                .as_mut()
                                .expect("this branch assigned Some(hold) immediately above")[key] =
                                value.clone();
                        }
                    }
                }
                self.save(&mut row, &progress)
                    .await
                    .map_err(|e| HandlerError::new(&e.code, e.detail.to_string()))?;
                Ok(Some(if f.reason.is_some() && !cleaning {
                    Duration::ZERO
                } else {
                    backoff(row.attempts).min(POLL)
                }))
            }
        }
    }

    async fn preconditions(
        &mut self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
    ) -> Result<(), Failure> {
        if !self
            .worker
            .sql(RegistryStore::move_authority_cut_over)
            .await?
        {
            return Err(Failure::abort(
                MoveReason::ReceiverPrecondition,
                json!({"code":"authority_not_cut_over"}),
            ));
        }
        if self
            .worker
            .sql(RegistryStore::identity_log_status)
            .await?
            .state
            != "enabled"
        {
            return Err(Failure::abort(
                MoveReason::ReceiverPrecondition,
                json!({"code":"identity_log_disabled"}),
            ));
        }
        self.catalog_preflight(plan).await?;
        let (supervisor, catalog) = self.discovery_lists().await?;
        // The source plans every module that declares agent data, even one with
        // no refs for this agent. A module that is not on this Mac at all cannot
        // hold local data for the agent, and with no refs the source carries none,
        // so skip it for the rest of the move. Decide once and persist the set:
        // later steps and restarts must not call a module this Mac never checked.
        // A configured but disabled or warming module is not absent: it may still
        // keep data on disk, so the move holds below until it is enabled and
        // its fork check (agent_data.diverged) can run.
        if progress.skipped_absent.is_none() {
            progress.skipped_absent = Some(absent_without_refs(plan, &supervisor, &catalog));
            self.save(row, progress).await?;
        }
        for p in called(plan, progress) {
            let configured = supervisor
                .modules
                .iter()
                .find(|m| m.module_id == p.module_id);
            let registered = catalog.modules.iter().find(|m| m.module_id == p.module_id);
            if configured.is_none() && registered.is_none() {
                return Err(Failure::abort(
                    MoveReason::ReceiverPrecondition,
                    json!({"code":"participant_unavailable","module_id":p.module_id,"absent":true}),
                ));
            }
            if !configured.is_some_and(|m| m.enabled) || !registered.is_some_and(|m| m.ready) {
                return Err(Failure::hold(
                    "participant_unavailable",
                    json!({"module_id":p.module_id,"state":configured.map(|m| &m.state),"not_ready":registered.and_then(|m| m.not_ready.as_ref())}),
                ));
            }
            let client = self.client(&p.module_id).await?;
            let description = client
                .describe()
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.describe", e))?;
            if let Some(r) = p
                .refs
                .iter()
                .find(|r| !description.ref_kinds.contains(&r.ref_kind))
            {
                return Err(Failure::abort(
                    MoveReason::ReceiverPrecondition,
                    json!({"code":"participant_unavailable","module_id":p.module_id,"ref_kind":r.ref_kind}),
                ));
            }
        }
        let id = plan.agent_id.clone();
        let agent = self
            .worker
            .sql(move |s| s.agent_row(&id))
            .await?
            .ok_or_else(|| Failure::hold("unknown_agent", json!({"agent_id":plan.agent_id})))?;
        if let Some(gone) = agent.gone() {
            return Err(Failure::abort(MoveReason::Gone, gone));
        }
        if let Some(project) = agent.project_id {
            let count = self.worker.sql(move |s| s.root_count(&project)).await?;
            if count == 0 {
                return Err(Failure::abort(
                    MoveReason::ProjectNotCheckedOut,
                    json!({"code":"project_not_checked_out"}),
                ));
            }
        }
        self.local_writers(plan).await
    }

    /// The session bindings the source carried for this move, as this Mac
    /// imported them; none when the move carries no bindings stream. A
    /// carried ref the move does not name is the source breaking the
    /// agent-data contract, and holds the move as a contract violation.
    async fn carried_bindings(&self, plan: &MoveRecord) -> Result<Vec<CarriedBinding>, Failure> {
        let carries = plan
            .participants
            .iter()
            .any(|p| p.refs.iter().any(|r| r.ref_kind == SESSION_BINDINGS_KIND));
        if !carries {
            return Ok(Vec::new());
        }
        let refs: std::collections::BTreeSet<_> = plan
            .participants
            .iter()
            .flat_map(|p| p.refs.clone())
            .filter(|r| is_binding_ref_kind(&r.ref_kind))
            .collect();
        let (cut, agent) = (self.cut.clone(), plan.agent_id.clone());
        let staged = self
            .worker
            .sql(move |s| Ok(crate::session_move::staged_bindings(s, &cut, &agent, &refs)))
            .await?;
        match staged {
            Ok(rows) => Ok(rows),
            Err(StagedError::OutsideMove(r)) => {
                let mut failure = Failure::hold(
                    "contract_violation",
                    json!({"module_id":"entorhinal","op":"agent_data.import","code":"contract_violation",
                        "ref_kind":r.ref_kind,"ref":r.reference,
                        "message":"the source carried a session binding the move does not name"}),
                );
                failure.violation = true;
                Err(failure)
            }
            Err(StagedError::Unreadable(error)) => Err(error.into()),
        }
    }

    fn count_installed(&self, installed: CarriedInstall) {
        let health = &self.worker.health;
        health
            .session_bindings_installed
            .fetch_add(installed.installed, Ordering::Relaxed);
        health
            .session_bindings_placeholders
            .fetch_add(installed.placeholders, Ordering::Relaxed);
        if installed.placeholders > 0 {
            // Once per move: the activation transaction runs once per cut.
            tracing::warn!(
                cut = %self.cut,
                placeholders = installed.placeholders,
                "carried session bindings collided with bindings on this Mac and were installed as placeholders"
            );
        }
    }

    async fn catalog_preflight(&self, plan: &MoveRecord) -> Result<(), Failure> {
        let Some(expected) = &plan.pin_digest else {
            return Ok(());
        };
        let catalog = self.worker.catalog.as_ref().ok_or_else(|| Failure::abort(MoveReason::IncompatibleVersion, json!({"code":"incompatible_version","module_id":"entorhinal","note":"Both Macs need catalog move support"})))?;
        catalog
            .preflight(&plan.agent_id, expected)
            .await
            .map(|_| ())
            .map_err(|e| match e.code.as_str() {
                "pin_digest_mismatch" => Failure::abort(
                    MoveReason::PinDigestMismatch,
                    e.detail.unwrap_or(Value::Null),
                ),
                "trust_unavailable" => Failure::hold(
                    "trust_unavailable",
                    json!({"message":e.message,"detail":e.detail}),
                ),
                "trust_missing" => {
                    Failure::abort(MoveReason::TrustMissing, e.detail.unwrap_or(Value::Null))
                }
                _ => Failure::hold(&e.code, json!({"message":e.message,"detail":e.detail})),
            })
    }

    async fn local_writers(&mut self, plan: &MoveRecord) -> Result<(), Failure> {
        let refs: Vec<_> = plan
            .participants
            .iter()
            .flat_map(|p| p.refs.clone())
            .filter(|r| is_binding_ref_kind(&r.ref_kind))
            .collect::<std::collections::BTreeSet<_>>()
            .into_iter()
            .collect();
        let owned_refs = refs.clone();
        let bindings = self
            .worker
            .sql(move |s| {
                s.session_bindings_for_refs(
                    &owned_refs
                        .iter()
                        .map(|r| (r.ref_kind.as_str(), r.reference.as_str()))
                        .collect::<Vec<_>>(),
                )
            })
            .await?;
        let mut held = std::collections::BTreeSet::new();
        for (r, binding) in refs.iter().zip(&bindings) {
            let Some(binding) = binding else {
                continue;
            };
            if binding.agent_id != plan.agent_id {
                return Err(Failure::abort(
                    MoveReason::ReceiverPrecondition,
                    json!({"code":"ref_bound_elsewhere","ref_kind":r.ref_kind,"ref":r.reference,"agent_id":binding.agent_id}),
                ));
            }
            held.insert(r.clone());
        }
        {
            let state = self.liveness.lock().unwrap_or_else(|e| e.into_inner());
            for (r, binding) in refs.iter().zip(&bindings) {
                let Some(binding) = binding else {
                    continue;
                };
                let (Some(harness), Some(session)) = (&binding.harness, &binding.session_id) else {
                    continue;
                };
                let key = SessionKey::new(harness, session);
                let entry = state.sessions.get(&key);
                if !state.stale && state.gone.contains(&key) && entry.is_none() {
                    continue;
                }
                let active = !state.stale
                    && entry.is_some_and(|e| matches!(e.state.as_str(), "active" | "idle"));
                return Err(Failure::hold(
                    "agent_busy",
                    json!({"ref":r.reference,"ref_kind":r.ref_kind,
                    "reason_code":if active {"session_active"} else {"liveness_unknown"},"note":entry.and_then(|e| e.cause.clone())}),
                ));
            }
        }
        for p in &plan.participants {
            let local: Vec<_> = p
                .refs
                .iter()
                .filter(|r| held.contains(*r))
                .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
                .collect();
            if local.is_empty() {
                continue;
            }
            let client = self.client(&p.module_id).await?;
            let answer = client
                .busy(&local)
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.busy", e))?;
            if let Some((reference, verdict)) = local
                .iter()
                .zip(&answer.refs)
                .find(|(_, r)| r.state != BusyState::Idle)
            {
                return Err(Failure::hold(
                    "agent_busy",
                    json!({"module_id":p.module_id,"ref":reference.r#ref,
                    "reason_code":verdict.reason_code,"note":verdict.reason}),
                ));
            }
        }
        Ok(())
    }

    async fn settle_pending(
        &mut self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
    ) -> Result<(), Failure> {
        if let Some(pending) = progress.pending.clone() {
            // A skipped participant has no refs, so a call left pending for it
            // (by a receiver that ran before the skip was decided) changed no
            // data worth recovering; it is never called again.
            if progress.skips(&pending.module) {
                progress.pending = None;
                return self.save(row, progress).await;
            }
            let p = plan
                .participants
                .iter()
                .find(|p| p.module_id == pending.module)
                .ok_or_else(|| Failure::hold("plan_malformed", Value::Null))?;
            let client = self.client(&p.module_id).await?;
            client
                .status(&self.request(p)?)
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.status", e))?;
            // Persisted progress names the participant calls already sent. After
            // restart, recover a lost import reply before aborting staging: the
            // import may still be changing the data that abort needs to discard.
            match pending.op.as_str() {
                "agent_data.import" => {
                    client
                        .import(&self.request(p)?)
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                }
                "agent_data.diverged" => {
                    client
                        .diverged(&self.request(p)?)
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                }
                "agent_data.abort" => {
                    client
                        .abort(&self.request(p)?)
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                    progress.aborted.push(p.module_id.clone());
                }
                "agent_data.activate" => {
                    client
                        .activate(&self.request(p)?)
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                }
                _ => {}
            }
            progress.pending = None;
            self.save(row, progress).await?;
        }
        Ok(())
    }
    async fn advance(
        &mut self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
        observed: MoveRecord,
    ) -> Result<(), Failure> {
        match row.step {
            Step::PickedUp => {
                self.settle_pending(row, plan, progress).await?;
                self.preconditions(row, plan, progress).await?;
                for p in called(plan, progress) {
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.diverged".into(),
                    });
                    if !progress.touched.contains(&p.module_id) {
                        progress.touched.push(p.module_id.clone());
                    }
                    self.save(row, progress).await?;
                    let answer = client.diverged(&self.request(p)?).await;
                    if !matches!(answer, Err(ClientError::OutcomeUnknown)) {
                        progress.pending = None;
                        self.save(row, progress).await?;
                    }
                    let answer = answer.map_err(|e| {
                        Failure::participant(&p.module_id, "agent_data.diverged", e)
                    })?;
                    if let Some((reference, r)) = p.refs.iter().zip(&answer.refs).find(|(_, r)| {
                        matches!(r.state, DivergedState::Advanced | DivergedState::Unknown)
                    }) {
                        // Preserve the participant's validated reason in the checkpoint and shared abort detail
                        // so either Mac can show it beside the participant and ref.
                        let mut detail = json!({"module_id":p.module_id,"ref":reference.reference,"state":r.state});
                        if let Some(reason) = &r.reason {
                            detail["reason"] = json!(reason);
                        }
                        return Err(Failure::abort(MoveReason::Diverged, detail));
                    }
                }
                // Diverged asks each participant whether this Mac has data past
                // the cut. A host can write while that call is in flight, so recheck
                // session-binding ownership and active local writers before import.
                self.local_writers(plan).await?;
                self.catalog_preflight(plan).await?;
                for p in called(plan, progress) {
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.import".into(),
                    });
                    self.save(row, progress).await?;
                    let answer = client.import(&self.request(p)?).await;
                    if !matches!(answer, Err(ClientError::OutcomeUnknown)) {
                        progress.pending = None;
                        self.save(row, progress).await?;
                    }
                    answer
                        .map_err(|e| Failure::participant(&p.module_id, "agent_data.import", e))?;
                }
                // Before the claim moves: every binding the source carried
                // must be one the move names.
                self.carried_bindings(plan).await?;
                Self::step(row, progress, Step::Imported);
                self.save(row, progress).await?;
            }
            Step::Imported => {
                self.catalog_preflight(plan).await?;
                if observed.state == State::Published {
                    self.cas_move(row, plan, State::Published, State::Staged)
                        .await?;
                } else if observed.state != State::Staged {
                    return Err(Failure::hold(
                        "invalid_state",
                        json!({"state":observed.state}),
                    ));
                }
                Self::step(row, progress, Step::Staged);
                self.save(row, progress).await?;
            }
            Step::Staged => {
                self.catalog_preflight(plan).await?;
                self.take_claim(row, plan, progress).await?;
                // The move-record CAS to claimed can lose to a peer abort after
                // taking the agent claim. Cleanup must return that claim to the
                // source before ending the receiver move, so ownership is not stranded.
                if observed.state != State::Claimed {
                    self.cas_move(row, plan, State::Staged, State::Claimed)
                        .await?;
                }
                Self::step(row, progress, Step::Claimed);
                self.save(row, progress).await?;
                self.apply_claim(row).await?;
            }
            Step::Claimed => {
                self.apply_claim(row).await?;
                self.local_writers(plan).await?;
                Self::step(row, progress, Step::Activating);
                self.save(row, progress).await?;
            }
            Step::Activating => {
                self.settle_pending(row, plan, progress).await?;
                for p in called(plan, progress) {
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.activate".into(),
                    });
                    self.save(row, progress).await?;
                    let answer = client.activate(&self.request(p)?).await;
                    if !matches!(answer, Err(ClientError::OutcomeUnknown)) {
                        progress.pending = None;
                        self.save(row, progress).await?;
                    }
                    answer.map_err(|e| {
                        Failure::participant(&p.module_id, "agent_data.activate", e)
                    })?;
                }
                // Resend activation even if its CAS landed before a crash. The
                // pinned participant operations are idempotent for this cut.
                if observed.state == State::Claimed {
                    self.cas_move(row, plan, State::Claimed, State::Activated)
                        .await?;
                } else if !matches!(observed.state, State::Activated | State::Released) {
                    return Err(Failure::hold(
                        "invalid_state",
                        json!({"state":observed.state}),
                    ));
                }
                let mut next = row.clone();
                let mut activated_progress = progress.clone();
                Self::step(&mut next, &mut activated_progress, Step::Activated);
                next.activated_at_ms = Some((self.worker.clock)());
                next.updated_at_ms = (self.worker.clock)();
                next.plan["_receiver"] = serde_json::to_value(&activated_progress)
                    .map_err(|e| Failure::hold("plan_malformed", json!({"error":e.to_string()})))?;
                let refs = plan
                    .participants
                    .iter()
                    .flat_map(|p| p.refs.clone())
                    .filter(|r| is_binding_ref_kind(&r.ref_kind))
                    .collect::<Vec<_>>();
                let carried = self.carried_bindings(plan).await?;
                let (activated, installed) = self
                    .worker
                    .sql(move |s| Ok(s.activate_agent_move_carrying(&next, &refs, &carried)))
                    .await?
                    .map_err(HandlerError::from)?;
                *row = activated;
                self.count_installed(installed);
                *progress = activated_progress;
                self.worker.gauges().await?;
                self.worker.wake.notify_one();
                remove_inbox(&self.worker, &plan.to, &self.cut).await?;
            }
            Step::Activated => {
                remove_inbox(&self.worker, &plan.to, &self.cut).await?;
                let activated = row
                    .activated_at_ms
                    .ok_or_else(|| Failure::hold("activated_time_missing", Value::Null))?;
                if (self.worker.clock)() < activated.saturating_add(600_000) {
                    return Ok(());
                }
                let request = DoorCut {
                    cut_id: self.cut.clone(),
                };
                let state = if progress.takeover_sent {
                    self.worker
                        .door
                        .status(&request)
                        .await
                        .map_err(door_failure)?
                        .takeover
                        .map(|p| p.state)
                        .unwrap_or(crate::door_client::TakeoverState::NotStarted)
                } else {
                    crate::door_client::TakeoverState::NotStarted
                };
                let state = if matches!(
                    state,
                    crate::door_client::TakeoverState::NotStarted
                        | crate::door_client::TakeoverState::Failed
                ) {
                    progress.takeover_sent = true;
                    self.save(row, progress).await?;
                    self.worker
                        .door
                        .takeover(&request)
                        .await
                        .map_err(door_failure)?
                        .takeover
                } else {
                    state
                };
                match state {
                    crate::door_client::TakeoverState::Taken => {
                        Self::step(row, progress, Step::TakenOver);
                        self.save(row, progress).await?;
                    }
                    crate::door_client::TakeoverState::Failed => {
                        return Err(Failure::hold("failed", json!({"op":"cut.takeover"})))
                    }
                    _ => {}
                }
            }
            _ => {}
        }
        Ok(())
    }
    async fn cas_move(
        &self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        from: State,
        to: State,
    ) -> Result<(), Failure> {
        wire::check_transition(Some(from), to, MoveSide::Receiver)
            .map_err(|e| Failure::hold(e.code(), Value::Null))?;
        let subject = format!("move:{}", self.cut);
        let mut body = plan.clone();
        body.state = to;
        if to == State::Aborted {
            body.reason = row
                .reason
                .as_ref()
                .and_then(|r| serde_json::from_value(json!(r)).ok());
            body.detail = row.detail.as_ref().map(wire::bounded_detail);
        }
        let request = CasRequest {
            subject: subject.clone(),
            expected_version: row.record_version as u64,
            body: wire::encode_move_record(&subject, &body)
                .map_err(|e| Failure::hold(e.code(), Value::Null))?,
            write_id: wire::move_write_id(&self.cut, MoveSide::Receiver, to),
        };
        match self
            .worker
            .records
            .cas_settled(&request, &plan.to, |b| {
                wire::decode_move_record(&subject, b).map_err(|e| e.code())
            })
            .await?
        {
            CasSettlement::Landed(version) => {
                row.record_version = checked_version(version)?;
                Ok(())
            }
            CasSettlement::Moved(_) => {
                Err(Failure::hold("version_moved", json!({"op":"record.cas"})))
            }
        }
    }

    async fn take_claim(
        &self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
    ) -> Result<(), Failure> {
        if row.claim_landed_version.is_some() {
            return Ok(());
        }
        let subject = format!("claim:{}", row.agent_id);
        if row.claim_take_body.is_none() {
            let reply = self.worker.records.read(&subject).await?;
            if reply.stale {
                return Err(Failure::hold("unavailable", json!({"op":"claim.read"})));
            }
            if reply.version != plan.claim_version {
                progress.take_failed = true;
                return Err(Failure::abort(
                    MoveReason::ClaimMoved,
                    json!({"op":"claim.read","code":"claim_moved","version":reply.version}),
                ));
            }
            let epoch = if reply.absent {
                0
            } else {
                let claim =
                    AgentClaim::decode(&row.agent_id, reply.body.as_deref().unwrap_or_default())
                        .map_err(|code| {
                            Failure::abort(
                                MoveReason::ClaimMoved,
                                json!({"op":"claim.read","code":code}),
                            )
                        })?;
                if claim.epoch >= 1 && claim.holder.as_deref() != Some(&plan.from) {
                    progress.take_failed = true;
                    return Err(Failure::abort(
                        MoveReason::ClaimMoved,
                        json!({"code":"claim_moved","cause":"source_not_holder"}),
                    ));
                }
                claim.epoch
            };
            let take = AgentClaim {
                v: 1,
                agent_id: row.agent_id.clone(),
                holder: Some(plan.to.clone()),
                epoch: epoch.checked_add(1).ok_or_else(|| {
                    Failure::abort(MoveReason::ClaimMoved, json!({"code":"epoch_overflow"}))
                })?,
                cut_id: Some(self.cut.clone()),
                reason: None,
            };
            row.claim_take_body =
                Some(serde_json::to_value(take).map_err(|e| {
                    Failure::hold("claim_malformed", json!({"error":e.to_string()}))
                })?);
            row.claim_take_version = Some(checked_version(plan.claim_version)?);
            self.save(row, progress).await?;
        }
        let request = CasRequest {
            subject,
            expected_version: row.claim_take_version.ok_or_else(|| {
                Failure::abort(
                    MoveReason::ClaimMoved,
                    json!({"code":"take_version_missing"}),
                )
            })? as u64,
            body: serde_json::to_vec(
                row.claim_take_body
                    .as_ref()
                    .ok_or_else(|| Failure::hold("take_body_missing", Value::Null))?,
            )
            .map_err(|e| Failure::hold("claim_malformed", json!({"error":e.to_string()})))?,
            write_id: wire::claim_write_id(&self.cut, wire::ClaimWrite::Take),
        };
        match self
            .worker
            .records
            .cas_settled(&request, &plan.to, |b| AgentClaim::decode(&row.agent_id, b))
            .await?
        {
            CasSettlement::Landed(version) => {
                row.claim_landed_version = Some(checked_version(version)?);
                self.save(row, progress).await?;
                Ok(())
            }
            CasSettlement::Moved(reply) => {
                if reply
                    .body
                    .as_deref()
                    .and_then(|b| AgentClaim::decode(&row.agent_id, b).ok())
                    .is_some_and(|claim| claim.holder.as_deref() == Some(&plan.to))
                {
                    let mut failure = Failure::hold(
                        "claim_moved",
                        json!({"op":"claim.cas","cause":"receiver_still_holder"}),
                    );
                    failure.diagnostic = true;
                    return Err(failure);
                }
                progress.take_failed = true;
                Err(Failure::abort(
                    MoveReason::ClaimMoved,
                    json!({"op":"claim.cas","code":"claim_moved","author":reply.author,"version":reply.version,"body":reply.body.and_then(|b| serde_json::from_slice::<Value>(&b).ok())}),
                ))
            }
        }
    }
    async fn return_claim(&self, row: &mut AgentMove, plan: &MoveRecord) -> Result<(), Failure> {
        let Some(version) = row.claim_landed_version else {
            return Ok(());
        };
        let mut claim: AgentClaim = serde_json::from_value(
            row.claim_take_body
                .clone()
                .ok_or_else(|| Failure::hold("take_body_missing", Value::Null))?,
        )
        .map_err(|e| Failure::hold("claim_malformed", json!({"error":e.to_string()})))?;
        claim.holder = Some(plan.from.clone());
        claim.cut_id = None;
        claim.epoch = claim
            .epoch
            .checked_add(1)
            .ok_or_else(|| Failure::hold("epoch_overflow", Value::Null))?;
        let request = CasRequest {
            subject: format!("claim:{}", row.agent_id),
            expected_version: version as u64,
            body: serde_json::to_vec(&claim)
                .map_err(|e| Failure::hold("claim_malformed", json!({"error":e.to_string()})))?,
            write_id: wire::claim_write_id(&self.cut, wire::ClaimWrite::Return),
        };
        match self
            .worker
            .records
            .cas_settled(&request, &plan.to, |b| AgentClaim::decode(&row.agent_id, b))
            .await?
        {
            CasSettlement::Landed(_) => Ok(()),
            CasSettlement::Moved(reply) => {
                if !reply.stale
                    && reply
                        .body
                        .as_deref()
                        .and_then(|b| AgentClaim::decode(&row.agent_id, b).ok())
                        .is_some_and(|c| c.holder.as_deref() == Some(&plan.from))
                {
                    Ok(())
                } else {
                    Err(Failure::hold(
                        "claim_moved",
                        json!({"op":"claim.return","version":reply.version}),
                    ))
                }
            }
        }
    }
    async fn apply_claim(&self, row: &AgentMove) -> Result<(), Failure> {
        let reply = self
            .worker
            .records
            .read(&format!("claim:{}", row.agent_id))
            .await?;
        if reply.stale {
            return Err(Failure::hold("unavailable", json!({"op":"claim.read"})));
        }
        let observation = if reply.absent {
            ClaimObservation::Absent {
                stale: false,
                self_author: reply.self_author,
            }
        } else {
            let claim =
                AgentClaim::decode(&row.agent_id, reply.body.as_deref().unwrap_or_default())
                    .map_err(|code| Failure::hold(code, json!({"op":"claim.read"})))?;
            ClaimObservation::Present {
                claim: CachedClaim {
                    holder: claim.holder,
                    epoch: claim.epoch,
                    cut_id: claim.cut_id,
                    version: checked_version(reply.version)?,
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
    async fn cleanup(
        &mut self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
    ) -> Result<(), Failure> {
        // An abort can arrive before the preconditions decided which absent
        // participants to skip. Decide here with the same rule, before any
        // participant call, so cleanup never waits on a module this Mac lacks.
        // That is safe even if an older receiver already imported: a
        // participant with no refs staged nothing, so it has nothing to undo.
        // Persisting the set makes a restart mid-cleanup skip the same modules.
        let undecided = progress.skipped_absent.is_none();
        if undecided {
            let (supervisor, catalog) = self.discovery_lists().await?;
            progress.skipped_absent = Some(absent_without_refs(plan, &supervisor, &catalog));
            self.save(row, progress).await?;
        }
        self.settle_pending(row, plan, progress).await?;
        let request = row
            .abort_requested
            .clone()
            .ok_or_else(|| Failure::hold("abort_request_missing", Value::Null))?;
        row.reason = request["reason"].as_str().map(str::to_owned);
        row.detail = request.get("detail").filter(|d| !d.is_null()).cloned();
        if row.claim_take_body.is_some() && row.claim_landed_version.is_none() {
            // The persisted failure flag is not proof that an unanswered take lost.
            // Reconcile the exact attempt even when an earlier driver marked it failed.
            if let Err(failure) = self.take_claim(row, plan, progress).await {
                if failure.reason != Some(MoveReason::ClaimMoved) || !progress.take_failed {
                    return Err(failure);
                }
            }
        }
        self.return_claim(row, plan).await?;
        let claim_reply = self
            .worker
            .records
            .read(&format!("claim:{}", row.agent_id))
            .await?;
        if claim_reply.stale {
            return Err(Failure::hold(
                "unavailable",
                json!({"op":"claim.cleanup.read"}),
            ));
        }
        if !claim_reply.absent {
            let claim = AgentClaim::decode(
                &row.agent_id,
                claim_reply.body.as_deref().unwrap_or_default(),
            )
            .map_err(|code| Failure::hold(code, json!({"op":"claim.cleanup.read"})))?;
            if claim.holder.as_deref() == Some(&plan.to) {
                return Err(Failure::hold(
                    "claim_moved",
                    json!({"op":"claim.cleanup.read","cause":"receiver_still_holder"}),
                ));
            }
        }
        for p in called(plan, progress) {
            if progress.aborted.contains(&p.module_id) {
                continue;
            }
            // Diverged checks for data past the cut; import stages received data.
            // A participant missing before either call has nothing to undo. Save
            // which modules we called so one that disappears afterward must
            // discard its staged data when it returns, before abort can finish.
            // The same holds for a participant the preconditions refused for
            // not declaring one of its ref kinds (an older version of it): it
            // was never called for this cut, and it would refuse an abort
            // naming a ref kind it does not know, so asking would never end.
            if !progress.touched.contains(&p.module_id)
                && row.detail.as_ref().is_some_and(|d| {
                    d["module_id"] == p.module_id
                        && (d["absent"] == true
                            || (d["code"] == "participant_unavailable"
                                && d["ref_kind"].is_string()))
                })
            {
                continue;
            }
            let client = self.client(&p.module_id).await?;
            progress.pending = Some(PendingCall {
                module: p.module_id.clone(),
                op: "agent_data.abort".into(),
            });
            self.save(row, progress).await?;
            client
                .abort(&self.request(p)?)
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.abort", e))?;
            progress.pending = None;
            progress.aborted.push(p.module_id.clone());
            self.save(row, progress).await?;
        }
        // The source may unfreeze as soon as it sees the shared abort. Publish
        // that state only after every receiver participant has restored its data.
        let (record, version) = self.read_move(plan).await?;
        row.record_version = checked_version(version)?;
        if record.state != State::Aborted {
            self.cas_move(row, plan, record.state, State::Aborted)
                .await?;
        }
        self.apply_claim(row).await?;
        remove_inbox(&self.worker, &plan.to, &self.cut).await?;
        Self::step(row, progress, Step::Aborted);
        self.save(row, progress).await
    }
}

fn door_failure(e: crate::door_client::DoorError) -> Failure {
    let mut failure = Failure::hold(
        e.code(),
        json!({"op":"cut.takeover","error":format!("{e:?}")}),
    );
    if let crate::door_client::DoorError::Refused { message, .. } = e {
        failure.refusal = Some(Box::new(PeerRefusal { message }));
    }
    failure
}

/// Accept is a diagnostic task, not a move checkpoint. It can outlive a finished
/// move, and even a hung door call cannot prevent release or timed takeover.
pub(crate) struct CutAccept {
    worker: Worker,
    cut: String,
}

impl CutAccept {
    pub(crate) fn new(worker: Worker, cut: String) -> Self {
        Self { worker, cut }
    }

    pub(crate) async fn run(self) {
        let mut signals = self.worker.wake.subscribe();
        let mut delay = Duration::ZERO;
        loop {
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = signals.notified() => {} }
            signals.handled();
            if self.worker.store.upgrade().is_none() {
                return;
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

    pub(crate) async fn tick(&self) -> Result<Option<Duration>, HandlerError> {
        let row = self.worker.row(&self.cut).await?;
        let Some(accept) = row.cut_accept.filter(|a| a["state"] == "pending") else {
            return Ok(None);
        };
        let now = (self.worker.clock)();
        let retry_at = accept["retry_at_ms"].as_i64().unwrap_or(0);
        if now < retry_at {
            return Ok(Some(
                Duration::from_millis((retry_at - now) as u64).min(POLL),
            ));
        }
        let own = row.plan["to"]
            .as_str()
            .ok_or_else(|| error("plan_malformed"))?;
        remove_inbox(&self.worker, own, &self.cut).await?;
        let attempts = accept["attempts"].as_i64().unwrap_or(0).saturating_add(1);
        let result = self
            .worker
            .door
            .accept(&DoorCut {
                cut_id: self.cut.clone(),
            })
            .await;
        let (state, delay) = match result {
            Ok(reply) => (json!({"state":"accepted","lifted":reply.lifted}), None),
            Err(e) if e.code() == "unavailable" => {
                let delay = backoff(attempts);
                (
                    json!({"state":"pending","code":e.code(),"attempts":attempts,
                    "retry_at_ms":(self.worker.clock)().saturating_add(delay.as_millis() as i64)}),
                    Some(delay.min(POLL)),
                )
            }
            Err(e) => (
                json!({"state":"refused","code":e.code(),"detail":format!("{e:?}")}),
                None,
            ),
        };
        let cut = self.cut.clone();
        let now = (self.worker.clock)();
        self.worker
            .sql(move |s| s.set_move_cut_accept(&cut, &state, now))
            .await?;
        self.worker.gauges().await?;
        Ok(delay)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        door_client::{Request as DoorRequest, SealRequest},
        fake_daemon::{DaemonSnapshot, FakeDaemon},
        fake_door::FakeDoor,
        fake_participant::{Effect, FakeParticipant},
        fake_record::FakeRecord,
        log_client::{LogConnector, LogTransport, TransportError},
        participant_client::{BusyRef, Refusal, Request},
        participant_discovery::{provider_role, ParticipantDiscovery},
        record_client::RecordClient,
        tests::scratch_descriptor,
    };
    use async_trait::async_trait;
    use std::sync::atomic::AtomicBool;
    use subc_client_rs::{CatalogList, SupervisorListResponse};
    const CUT: &str = "0123456789abcdef0123456789abcdef";
    const AGENT: &str = "agent_0123456789abcdef";
    const FROM: &str = "01010101010101010101010101010101";
    const TO: &str = "02020202020202020202020202020202";
    thread_local! { static NOW: std::cell::Cell<i64> = const { std::cell::Cell::new(1000) }; }
    fn clock() -> i64 {
        NOW.with(|n| n.get())
    }
    fn advance_clock() {
        NOW.with(|n| n.set(n.get() + 300_000));
    }
    fn reference() -> AgentRef {
        AgentRef::new("session", "r")
    }
    fn snapshot(ready: bool) -> DaemonSnapshot {
        DaemonSnapshot::new(
            1,
            vec![
                json!({"module_id":"data","state":"running","enabled":true,"live":true,"health":"ok"}),
            ],
            vec![
                json!({"module_id":"data","ready":ready,"not_ready":null,"control_ops":[],"roles":[provider_role()],
                "capabilities":{"provides":["agent-data/v1"],"requires":[],"must_never_reach":[]}}),
            ],
        )
    }
    struct Discovery {
        daemon: FakeDaemon,
        participant: Arc<Peer>,
        others: Mutex<BTreeMap<String, Arc<Peer>>>,
    }
    #[async_trait]
    impl ParticipantDiscovery for Discovery {
        async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
            self.daemon.supervisor_list().await
        }
        async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
            self.daemon.catalog_list().await
        }
        async fn open_participant(
            &self,
            module: &str,
        ) -> Result<Arc<dyn LogTransport>, TransportError> {
            self.daemon.open_participant(module).await?;
            Ok(self
                .others
                .lock()
                .unwrap()
                .get(module)
                .cloned()
                .unwrap_or_else(|| self.participant.clone()))
        }
    }
    struct Peer {
        fake: Arc<FakeParticipant>,
        write_after_diverged: AtomicBool,
        panic_diverged: AtomicBool,
        diverged_calls: tokio::sync::Semaphore,
        other_activated: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl LogTransport for Peer {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            if method == "agent_data.diverged" && params["cutId"] == CUT {
                self.diverged_calls.add_permits(1);
                assert!(
                    !self.panic_diverged.swap(false, Ordering::Relaxed),
                    "scripted participant panic"
                );
            }
            if method == "agent_data.activate" && params["cutId"] != CUT {
                self.other_activated.add_permits(1);
            }
            let reply = self.fake.call(method, params).await;
            if method == "agent_data.diverged"
                && self.write_after_diverged.swap(false, Ordering::Relaxed)
            {
                self.fake.set_busy(
                    reference(),
                    BusyRef::new(BusyState::Busy).with_reason_code("local_writer"),
                );
            }
            reply
        }
    }
    struct Records {
        fake: Arc<FakeRecord>,
        calls: Mutex<Vec<(String, Value)>>,
        slot: std::sync::Weak<Mutex<Option<RegistryStore>>>,
        crash: Mutex<Option<(wire::ClaimWrite, bool)>>,
        hit: tokio::sync::Semaphore,
        abort_on_claimed: AtomicBool,
        crash_activated: AtomicBool,
        stale_after_take: AtomicBool,
        take_unanswered: AtomicBool,
    }
    struct Connector(Arc<Records>);
    #[async_trait]
    impl LogConnector for Connector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for Records {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            let is_claim = method == "record.cas" && params["subject"] == format!("claim:{AGENT}");
            if method == "record.read"
                && params["subject"] == format!("claim:{AGENT}")
                && self.take_unanswered.load(Ordering::Relaxed)
                && self.stale_after_take.swap(false, Ordering::Relaxed)
            {
                return Ok(serde_json::to_vec(&json!({"result":crate::record_client::wire_reply(&RecordReply {
                    absent:true, version:0, body:None, author:None, self_author:TO.into(), stale:true,
                })})).unwrap());
            }
            if is_claim {
                let slot = self.slot.upgrade().unwrap();
                let store = slot.lock().unwrap().clone().unwrap();
                let row = store.agent_move(CUT).unwrap().unwrap();
                assert!(
                    row.claim_take_body.is_some(),
                    "take body must precede the CAS"
                );
                assert_eq!(row.claim_take_version, row.plan["claim_version"].as_i64());
            }
            let crash = {
                let mut crash = self.crash.lock().unwrap();
                if crash.as_ref().is_some_and(|(op, _)| {
                    is_claim
                        && params["write_id"]
                            == crate::log_client::encode_hex(&wire::claim_write_id(CUT, *op))
                }) {
                    crash.take()
                } else {
                    None
                }
            };
            if crash.is_some_and(|(_, after)| !after) {
                self.hit.add_permits(1);
                std::future::pending::<()>().await;
            }
            if method == "record.cas"
                && params["subject"] == format!("move:{CUT}")
                && params["write_id"]
                    == crate::log_client::encode_hex(&wire::move_write_id(
                        CUT,
                        MoveSide::Receiver,
                        State::Claimed,
                    ))
                && self.abort_on_claimed.swap(false, Ordering::Relaxed)
            {
                let subject = format!("move:{CUT}");
                let reply = self
                    .fake
                    .call("record.read", json!({"subject":subject}))
                    .await
                    .unwrap();
                let reply: Value = serde_json::from_slice(&reply).unwrap();
                let bytes =
                    crate::log_client::decode_hex(reply["result"]["body"].as_str().unwrap())
                        .unwrap();
                let mut body = wire::decode_move_record(&subject, &bytes).unwrap();
                body.state = State::Aborted;
                body.reason = Some(MoveReason::Operator);
                self.fake.put(
                    &subject,
                    reply["result"]["version"].as_u64().unwrap() + 1,
                    wire::encode_move_record(&subject, &body).unwrap(),
                    FROM,
                );
            }
            let activated = method == "record.cas"
                && params["write_id"]
                    == crate::log_client::encode_hex(&wire::move_write_id(
                        CUT,
                        MoveSide::Receiver,
                        State::Activated,
                    ));
            let unanswered_take = is_claim
                && self.stale_after_take.load(Ordering::Relaxed)
                && params["write_id"]
                    == crate::log_client::encode_hex(&wire::claim_write_id(
                        CUT,
                        wire::ClaimWrite::Take,
                    ));
            let reply = self.fake.call(method, params).await;
            if unanswered_take {
                self.take_unanswered.store(true, Ordering::Relaxed);
                return Err(TransportError::NoReply);
            }
            if crash.is_some_and(|(_, after)| after)
                || activated && self.crash_activated.swap(false, Ordering::Relaxed)
            {
                self.hit.add_permits(1);
                std::future::pending::<()>().await;
            }
            reply
        }
    }
    struct Fixture {
        worker: Worker,
        slot: Arc<Mutex<Option<RegistryStore>>>,
        records: Arc<Records>,
        discovery: Arc<Discovery>,
        door: FakeDoor,
        liveness: Arc<Mutex<LivenessState>>,
        dir: std::path::PathBuf,
    }
    impl Fixture {
        async fn new(held: bool) -> Self {
            NOW.with(|n| n.set(1000));
            let (dir, descriptor) = scratch_descriptor("move-receiver");
            let store = RegistryStore::open(&descriptor).unwrap();
            store.apply_entry("agent.cutover","{}","fixture",None,|tx| {
                tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0123456789abcdef','Receiver','R','assistant',1,1);")
            }).unwrap();
            if held {
                store.apply_entry("fixture","{}","fixture",None,|tx| tx.execute_batch(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES('session','r','agent_0123456789abcdef',NULL,NULL,'','move',1);"
                )).unwrap();
            }
            store
                .apply_entry("fixture", "{}", "fixture", None, |tx| {
                    tx.execute_batch("UPDATE identity_log_state SET state='enabled';")
                })
                .unwrap();
            store
                .cache_agent_claims(
                    &[(
                        AGENT.into(),
                        ClaimObservation::Absent {
                            stale: false,
                            self_author: TO.into(),
                        },
                    )],
                    clock(),
                )
                .unwrap();
            let slot = Arc::new(Mutex::new(Some(store)));
            let fake = FakeRecord::new([2; 16]);
            let records = Arc::new(Records {
                fake,
                calls: Mutex::new(vec![]),
                slot: Arc::downgrade(&slot),
                crash: Mutex::new(None),
                hit: tokio::sync::Semaphore::new(0),
                abort_on_claimed: AtomicBool::new(false),
                crash_activated: AtomicBool::new(false),
                stale_after_take: AtomicBool::new(false),
                take_unanswered: AtomicBool::new(false),
            });
            let participant = Arc::new(FakeParticipant::new(
                &["session"],
                if held { vec![reference()] } else { vec![] },
            ));
            participant.unknown_busy_for_unheld(true);
            let discovery = Arc::new(Discovery {
                daemon: FakeDaemon::new(snapshot(true)),
                others: Mutex::new(BTreeMap::new()),
                participant: Arc::new(Peer {
                    fake: participant,
                    write_after_diverged: AtomicBool::new(false),
                    panic_diverged: AtomicBool::new(false),
                    diverged_calls: tokio::sync::Semaphore::new(0),
                    other_activated: tokio::sync::Semaphore::new(0),
                }),
            });
            let door = FakeDoor::new([1; 16]);
            let item = crate::door_client::CutItem {
                session_ref: "a".repeat(32),
                stream: "reserved:data".into(),
                end_offset: 42,
                digest: "a".repeat(64),
            };
            door.set_stream(item.clone());
            door.script(
                "cut.seal",
                crate::fake_door::Effect::Seal(crate::door_client::SealState::Published, None),
            );
            door.client()
                .seal(&SealRequest {
                    cut_id: CUT.into(),
                    items: vec![item],
                })
                .await
                .unwrap();
            let worker = Worker {
                sql_operations: Arc::default(),
                store: Arc::downgrade(&slot),
                records: RecordClient::new(Arc::new(Connector(records.clone()))),
                door: door.for_author([2; 16]).client(),
                discovery: discovery.clone(),
                catalog: None,
                health: Arc::new(crate::HealthGauges::default()),
                watches: Arc::default(),
                writer: Arc::new(tokio::sync::Mutex::new(())),
                clock,
                wake: Arc::new(crate::move_driver::MoveWake::default()),
            };
            let f = Self {
                worker,
                slot,
                records,
                discovery,
                door,
                liveness: Arc::new(Mutex::new(LivenessState::default())),
                dir,
            };

            let record:MoveRecord = serde_json::from_value(json!({"v":1,"cut_id":CUT,"agent_id":AGENT,"from":FROM,"to":TO,"state":"published","claim_version":0,
                "participants":[{"module_id":"data","refs":[{"ref_kind":"session","ref":"r"}],"external":[]}],"reason":null,"detail":null})).unwrap();
            f.put_move(&record, FROM, 1);
            f.set_inbox(vec![PendingMove {
                cut_id: CUT.into(),
                from: FROM.into(),
                agent_id: AGENT.into(),
            }]);
            f
        }
        fn store(&self) -> RegistryStore {
            self.slot.lock().unwrap().clone().unwrap()
        }
        fn row(&self) -> AgentMove {
            self.store().agent_move(CUT).unwrap().unwrap()
        }
        fn receiver(&self) -> Receiver {
            Receiver::new(self.worker.clone(), CUT.into(), self.liveness.clone())
        }
        fn plan(&self) -> MoveRecord {
            let mut v = self.row().plan;
            v.as_object_mut().unwrap().remove("_receiver");
            serde_json::from_value(v).unwrap()
        }
        fn put_move(&self, record: &MoveRecord, author: &str, version: u64) {
            self.records.fake.put(
                &format!("move:{CUT}"),
                version,
                wire::encode_move_record(&format!("move:{CUT}"), record).unwrap(),
                author,
            );
        }
        fn set_inbox(&self, pending: Vec<PendingMove>) {
            self.records.fake.put(
                &format!("inbox:{TO}"),
                1,
                wire::encode_inbox_record(&InboxRecord { v: 1, pending }).unwrap(),
                FROM,
            );
        }
        async fn pickup(&self) {
            poll_inbox(&self.worker).await.unwrap();
        }
        async fn until(&self, receiver: &mut Receiver, step: Step) {
            for _ in 0..12 {
                if self.row().step == step {
                    return;
                }
                advance_clock();
                receiver.tick().await.unwrap();
            }
            panic!("did not reach {step:?}: {:?}", self.row());
        }
        fn edit(&self, edit: impl FnOnce(&mut AgentMove)) {
            let mut row = self.row();
            edit(&mut row);
            self.store().update_agent_move(&row).unwrap();
        }
        fn refuse(&self, op: &str, code: &str) {
            self.discovery.participant.fake.script(
                op,
                Effect::Refuse(Refusal {
                    code: code.into(),
                    detail: None,
                    message: None,
                }),
            );
        }
        async fn claim(&self) -> RecordReply {
            self.worker
                .records
                .read(&format!("claim:{AGENT}"))
                .await
                .unwrap()
        }
        fn claim_cas_count(&self) -> usize {
            self.records
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(op, p)| op == "record.cas" && p["subject"] == format!("claim:{AGENT}"))
                .count()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn receiver_fresh_unheld_refs_skip_busy_and_reach_claimed() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        assert_eq!(f.row().step, Step::PickedUp);
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Claimed).await;
        let ops: Vec<_> = f
            .discovery
            .participant
            .fake
            .calls()
            .iter()
            .map(|r| r.op().method())
            .collect();
        assert!(!ops.contains(&"agent_data.busy"));
        assert!(ops.contains(&"agent_data.diverged") && ops.contains(&"agent_data.import"));
        let claim = f.claim().await;
        assert_eq!(claim.author.as_deref(), Some(TO));
        let body = AgentClaim::decode(AGENT, claim.body.as_deref().unwrap()).unwrap();
        assert_eq!((body.holder.as_deref(), body.epoch), (Some(TO), 1));
        assert_eq!(
            f.store()
                .cached_agent_claim(AGENT)
                .unwrap()
                .unwrap()
                .holder
                .as_deref(),
            Some(TO)
        );
        assert!(matches!(
            wire::decode_move_record(
                &format!("move:{CUT}"),
                &f.worker
                    .records
                    .read(&format!("move:{CUT}"))
                    .await
                    .unwrap()
                    .body
                    .unwrap()
            )
            .unwrap()
            .state,
            State::Claimed
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn inbox_bad_entries_do_not_block_valid_pickup_and_transients_recover() {
        const BAD: &str = "abcdef0123456789abcdef0123456789";
        for case in [
            "malformed",
            "wrong_version",
            "absent",
            "version_range",
            "door_permanent",
            "door_transient",
            "record_transient",
        ] {
            let f = Fixture::new(false).await;
            let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
            let mut bad =
                wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                    .unwrap();
            bad.cut_id = BAD.into();
            bad.agent_id = "agent_abcdef0123456789".into();
            let subject = format!("move:{BAD}");
            if case != "absent" {
                let body = match case {
                    "malformed" => b"not JSON".to_vec(),
                    "wrong_version" => {
                        let mut value = json!(bad);
                        value["v"] = json!(2);
                        serde_json::to_vec(&value).unwrap()
                    }
                    _ => wire::encode_move_record(&subject, &bad).unwrap(),
                };
                f.records.fake.put(
                    &subject,
                    if case == "version_range" { u64::MAX } else { 1 },
                    body,
                    FROM,
                );
            }
            if matches!(
                case,
                "door_transient" | "record_transient" | "version_range"
            ) {
                f.door.script(
                    "cut.seal",
                    crate::fake_door::Effect::Seal(crate::door_client::SealState::Published, None),
                );
                f.door
                    .client()
                    .seal(&SealRequest {
                        cut_id: BAD.into(),
                        items: f.door.cut(CUT).unwrap().items,
                    })
                    .await
                    .unwrap();
            }
            if case == "door_transient" {
                f.door.for_author([2; 16]).script(
                    "cut.read",
                    crate::fake_door::Effect::Refuse("unavailable".into()),
                );
            }
            if case == "record_transient" {
                f.records.fake.error(&subject, Some("unavailable"));
            }
            f.set_inbox(vec![
                PendingMove {
                    cut_id: BAD.into(),
                    from: FROM.into(),
                    agent_id: bad.agent_id.clone(),
                },
                PendingMove {
                    cut_id: CUT.into(),
                    from: FROM.into(),
                    agent_id: AGENT.into(),
                },
            ]);
            f.pickup().await;
            assert_eq!(f.row().step, Step::PickedUp, "{case}");
            let transient = matches!(case, "door_transient" | "record_transient");
            assert_eq!(
                inbox(&f.worker, TO)
                    .await
                    .unwrap()
                    .1
                    .pending
                    .iter()
                    .any(|e| e.cut_id == BAD),
                transient,
                "{case}"
            );
            if case == "record_transient" {
                f.records.fake.error(&subject, None);
            }
            f.pickup().await;
            assert_eq!(f.row().step, Step::PickedUp);
            if transient {
                assert_eq!(
                    f.store().agent_move(BAD).unwrap().unwrap().step,
                    Step::PickedUp
                );
            } else {
                assert!(f.store().agent_move(BAD).unwrap().is_none());
            }
            assert_eq!(
                f.worker.health.move_inbox_rejected.load(Ordering::Relaxed),
                u64::from(!transient),
                "{case}"
            );
        }
    }

    #[tokio::test(start_paused = true)]
    async fn stale_take_settlement_is_held_then_returned_before_abort_even_with_failed_flag() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Staged).await;
        f.records.stale_after_take.store(true, Ordering::Relaxed);
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Staged);
        assert!(f.row().claim_landed_version.is_none());
        assert!(
            f.row().abort_requested.is_none(),
            "a stale read must not mark the take lost"
        );
        let claim = f.claim().await;
        assert_eq!(
            AgentClaim::decode(AGENT, claim.body.as_deref().unwrap())
                .unwrap()
                .holder
                .as_deref(),
            Some(TO)
        );
        f.edit(|row| {
            row.abort_requested = Some(json!({"reason":"operator","detail":null}));
            row.plan["_receiver"]["take_failed"] = json!(true);
        });
        f.until(&mut receiver, Step::Aborted).await;
        assert_eq!(
            AgentClaim::decode(AGENT, f.claim().await.body.as_deref().unwrap())
                .unwrap()
                .holder
                .as_deref(),
            Some(FROM)
        );
        assert_eq!(f.row().claim_landed_version, Some(1));
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_requires_source_holder_at_the_pinned_version_before_take() {
        for holder in ["03030303030303030303030303030303"] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Staged).await;
            let mut plan = f.plan();
            plan.claim_version = 1;
            plan.state = State::Staged;
            f.put_move(&plan, TO, 3);
            f.edit(|row| {
                row.plan["claim_version"] = json!(1);
                row.claim_version = Some(1);
            });
            let claim = AgentClaim {
                v: 1,
                agent_id: AGENT.into(),
                holder: Some(holder.into()),
                epoch: 1,
                cut_id: None,
                reason: None,
            };
            f.records.fake.put(
                &format!("claim:{AGENT}"),
                1,
                serde_json::to_vec(&claim).unwrap(),
                FROM,
            );
            receiver.tick().await.unwrap();
            assert_eq!(
                f.claim_cas_count(),
                0,
                "wrong holder must be refused before CAS"
            );
            assert_eq!(f.row().reason.as_deref(), Some("claim_moved"));
            f.until(&mut receiver, Step::Aborted).await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn corrupted_persisted_receiver_refs_return_a_named_error_without_panicking() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut plan = f.plan();
        let duplicate = plan.participants[0].refs[0].clone();
        plan.participants[0].refs.push(duplicate);
        assert_eq!(
            f.receiver()
                .request(&plan.participants[0])
                .unwrap_err()
                .code,
            "invalid_refs"
        );
        let client = f
            .receiver()
            .client("data")
            .await
            .unwrap_or_else(|e| panic!("{}", e.code));
        assert!(matches!(
            client.busy(&[reference(), reference()]).await,
            Err(ClientError::InvalidReply(_))
        ));
    }
    #[tokio::test(start_paused = true)]
    async fn driver_redrives_a_panicked_receiver_while_another_cut_activates() {
        const OTHER: &str = "abcdef0123456789abcdef0123456789";
        let f = Fixture::new(false).await;
        f.store().apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch(
            "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_abcdef0123456789','Other','O','assistant',1,1);"
        )).unwrap();
        let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        let mut other =
            wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                .unwrap();
        other.cut_id = OTHER.into();
        other.agent_id = "agent_abcdef0123456789".into();
        other.participants[0].refs[0].reference = "other-ref".into();
        f.records.fake.put(
            &format!("move:{OTHER}"),
            1,
            wire::encode_move_record(&format!("move:{OTHER}"), &other).unwrap(),
            FROM,
        );
        f.door.script(
            "cut.seal",
            crate::fake_door::Effect::Seal(crate::door_client::SealState::Published, None),
        );
        f.door
            .client()
            .seal(&SealRequest {
                cut_id: OTHER.into(),
                items: f.door.cut(CUT).unwrap().items,
            })
            .await
            .unwrap();
        f.set_inbox(vec![
            PendingMove {
                cut_id: CUT.into(),
                from: FROM.into(),
                agent_id: AGENT.into(),
            },
            PendingMove {
                cut_id: OTHER.into(),
                from: FROM.into(),
                agent_id: other.agent_id,
            },
        ]);
        f.discovery
            .participant
            .panic_diverged
            .store(true, Ordering::Relaxed);
        let mut handler = crate::ProjectsHandler::with_runtime("receiver-panic".into(), clock);
        handler.store = f.slot.clone();
        handler.record_client = f.worker.records.clone();
        handler.door_client = f.worker.door.clone();
        handler.participant_discovery = f.discovery.clone();
        handler.health = f.worker.health.clone();
        handler.liveness = f.liveness.clone();
        handler.move_wake = f.worker.wake.clone();
        handler.start_move_driver();
        let progressed = tokio::time::timeout(Duration::from_secs(120), async {
            f.discovery
                .participant
                .diverged_calls
                .acquire_many(2)
                .await
                .unwrap()
                .forget();
            f.discovery
                .participant
                .other_activated
                .acquire()
                .await
                .unwrap()
                .forget();
        })
        .await;
        handler.stop_move_driver().await;
        assert!(
            progressed.is_ok(),
            "driver must retry the panicked cut without blocking the other cut"
        );
        assert_eq!(f.worker.health.move_task_panics.load(Ordering::Relaxed), 1);
        assert!(matches!(
            f.store().agent_move(OTHER).unwrap().unwrap().step,
            Step::Activating | Step::Activated | Step::Done
        ));
        assert_ne!(f.row().step, Step::Aborted);
    }

    #[tokio::test]
    async fn receiver_pickup_identity_mismatches_only_insert_terminal_row_and_remove_entry() {
        for mismatch in ["record_author", "body_from", "cut_author", "agent_id"] {
            let f = Fixture::new(false).await;
            let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
            let mut record =
                wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                    .unwrap();
            let author = if mismatch == "record_author" {
                TO
            } else {
                FROM
            };
            if mismatch == "body_from" {
                record.from = TO.into();
            }
            if mismatch == "agent_id" {
                record.agent_id = "other-agent".into();
            }
            if mismatch == "cut_author" {
                f.set_inbox(vec![PendingMove {
                    cut_id: CUT.into(),
                    from: TO.into(),
                    agent_id: AGENT.into(),
                }]);
                record.from = TO.into();
            }
            f.put_move(
                &record,
                if mismatch == "cut_author" { TO } else { author },
                1,
            );
            f.records.calls.lock().unwrap().clear();
            f.pickup().await;
            assert_eq!(f.row().step, Step::Aborted, "{mismatch}");
            assert_eq!(f.row().reason.as_deref(), Some("cut_author_mismatch"));
            assert!(f.discovery.participant.fake.calls().is_empty());
            assert!(f
                .records
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(op, _)| op == "record.cas")
                .all(|(_, p)| p["subject"] == format!("inbox:{TO}")));
            assert!(inbox(&f.worker, TO).await.unwrap().1.pending.is_empty());
            assert!(f
                .door
                .calls()
                .iter()
                .all(|c| matches!(c.request, DoorRequest::Seal(_) | DoorRequest::Read(_))));
        }
    }
    #[tokio::test]
    async fn receiver_inbox_removes_terminal_activated_aborted_released_and_foreign_without_pickup()
    {
        for case in ["terminal", "activated", "aborted", "released", "foreign"] {
            let f = Fixture::new(false).await;
            if matches!(case, "terminal" | "activated") {
                f.pickup().await;
                f.edit(|r| {
                    r.step = if case == "terminal" {
                        Step::Done
                    } else {
                        Step::Activated
                    }
                });
            } else {
                let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
                let mut record = wire::decode_move_record(
                    &format!("move:{CUT}"),
                    reply.body.as_deref().unwrap(),
                )
                .unwrap();
                match case {
                    "aborted" => {
                        record.state = State::Aborted;
                        record.reason = Some(MoveReason::Operator);
                    }
                    "released" => record.state = State::Released,
                    _ => record.to = FROM.into(),
                }
                f.put_move(&record, FROM, 2);
            }
            let count = f.store().list_agent_moves().unwrap().len();
            f.pickup().await;
            assert_eq!(f.store().list_agent_moves().unwrap().len(), count);
            assert!(
                inbox(&f.worker, TO).await.unwrap().1.pending.is_empty(),
                "{case}"
            );
        }
    }
    #[tokio::test]
    async fn receiver_inbox_defers_open_agent_and_four_row_cap() {
        for same_agent in [true, false] {
            let f = Fixture::new(false).await;
            let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
            let record =
                wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                    .unwrap();
            let entry = PendingMove {
                cut_id: CUT.into(),
                from: FROM.into(),
                agent_id: AGENT.into(),
            };
            for i in 0..if same_agent { 1 } else { 4 } {
                let mut row = pickup_row(&record, &entry, 1, clock());
                row.cut_id = format!("{i:032x}");
                if !same_agent {
                    row.agent_id = format!("other-{i}");
                }
                f.store().insert_agent_move(&row).unwrap();
            }
            f.pickup().await;
            assert!(f.store().agent_move(CUT).unwrap().is_none());
            assert_eq!(inbox(&f.worker, TO).await.unwrap().1.pending.len(), 1);
            assert_eq!(f.door.calls().len(), 1);
        }
    }

    #[tokio::test]
    async fn receiver_preconditions_abort_or_hold_with_named_codes() {
        for case in [
            "authority",
            "log",
            "missing_module",
            "kind",
            "not_ready",
            "unknown_agent",
            "gone",
            "project",
            "ownership",
        ] {
            let f = Fixture::new(case == "ownership").await;
            f.pickup().await;
            match case {
                "authority" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute(
                                "DELETE FROM registry_journal WHERE op='agent.cutover'",
                                [],
                            )?;
                            Ok(())
                        })
                        .unwrap();
                }
                "log" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute_batch("UPDATE identity_log_state SET state='disabled'")
                        })
                        .unwrap();
                }
                "missing_module" => {
                    f.discovery
                        .daemon
                        .restart(DaemonSnapshot::new(2, vec![], vec![]))
                }
                "not_ready" => f.discovery.daemon.restart(snapshot(false)),
                "kind" => f.edit(|r| {
                    r.plan["participants"][0]["refs"][0]["ref_kind"] = json!("missing-kind")
                }),
                "unknown_agent" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute_batch("DELETE FROM agent")
                        })
                        .unwrap();
                }
                "gone" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute_batch(
                                "UPDATE agent SET terminal_reason='retired',terminal_at_ms=2",
                            )
                        })
                        .unwrap();
                }
                "project" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute_batch(
                                "UPDATE agent SET role='hiree',project_id='missing-project'",
                            )
                        })
                        .unwrap();
                }
                "ownership" => {
                    f.store()
                        .apply_entry("fixture", "{}", "fixture", None, |tx| {
                            tx.execute_batch("UPDATE session_binding SET agent_id='another-agent'")
                        })
                        .unwrap();
                }
                _ => unreachable!(),
            }
            if case == "kind" {
                f.put_move(&f.plan(), FROM, 2);
            }
            let mut receiver = f.receiver();
            receiver.tick().await.unwrap();
            let row = f.row();
            match case {
                "not_ready" | "unknown_agent" => {
                    assert!(row.abort_requested.is_none());
                    assert_eq!(
                        row.hold.as_ref().unwrap()["code"],
                        if case == "not_ready" {
                            "participant_unavailable"
                        } else {
                            "unknown_agent"
                        }
                    );
                }
                _ => {
                    let reason = match case {
                        "gone" => "gone",
                        "project" => "project_not_checked_out",
                        _ => "receiver_precondition",
                    };
                    assert_eq!(row.reason.as_deref(), Some(reason), "{case}");
                    f.until(&mut receiver, Step::Aborted).await;
                    assert!(inbox(&f.worker, TO).await.unwrap().1.pending.is_empty());
                }
            }
            assert!(!f
                .discovery
                .participant
                .fake
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Import(_))));
            assert_eq!(f.claim_cas_count(), 0);
        }
    }
    fn peer(fake: Arc<FakeParticipant>) -> Arc<Peer> {
        Arc::new(Peer {
            fake,
            write_after_diverged: AtomicBool::new(false),
            panic_diverged: AtomicBool::new(false),
            diverged_calls: tokio::sync::Semaphore::new(0),
            other_activated: tokio::sync::Semaphore::new(0),
        })
    }
    /// Adds a `broca` participant with the given refs to the published plan,
    /// with its own fake so every call it receives is recorded. The receiver's
    /// daemon snapshot does not list it, so it starts absent on this Mac.
    async fn add_broca(f: &Fixture, refs: Value) -> Arc<FakeParticipant> {
        let broca = Arc::new(FakeParticipant::new(&["session"], vec![]));
        f.discovery
            .others
            .lock()
            .unwrap()
            .insert("broca".into(), peer(broca.clone()));
        let subject = format!("move:{CUT}");
        let reply = f.worker.records.read(&subject).await.unwrap();
        let mut record =
            wire::decode_move_record(&subject, reply.body.as_deref().unwrap()).unwrap();
        // Participants are encoded sorted by module id, and broca sorts before data.
        record.participants.insert(
            0,
            serde_json::from_value(json!({"module_id":"broca","refs":refs,"external":[]})).unwrap(),
        );
        f.put_move(&record, FROM, 1);
        broca
    }
    /// The fixture's daemon snapshot plus a `broca` entry: configured with the
    /// given enabled flag and, when `registered`, ready in the catalog.
    fn snapshot_with_broca(enabled: bool, registered: bool) -> DaemonSnapshot {
        let mut snap = snapshot(true);
        snap.supervisor.modules.push(
            serde_json::from_value(
                json!({"module_id":"broca","state":if enabled {"running"} else {"disabled"},
                "enabled":enabled,"live":enabled,"health":"ok"}),
            )
            .unwrap(),
        );
        if registered {
            let mut entry = snap.catalog.modules[0].clone();
            entry.module_id = "broca".into();
            snap.catalog.modules.push(entry);
        }
        snap
    }
    /// Whether the receiver opened a route to `broca` or sent it any request.
    fn broca_reached(f: &Fixture, broca: &FakeParticipant) -> bool {
        let target = crate::participant_discovery::participant_target("broca");
        !broca.calls().is_empty()
            || f.discovery.daemon.calls().iter().any(
                |c| matches!(c, crate::fake_daemon::DaemonCall::Open { target: t, .. } if *t == target),
            )
    }
    fn data_ops(f: &Fixture) -> Vec<&'static str> {
        f.discovery
            .participant
            .fake
            .calls()
            .iter()
            .map(|r| r.op().method())
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_skips_an_absent_participant_with_no_refs_for_the_whole_move() {
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([])).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activated).await;
        let mut plan = f.plan();
        plan.state = State::Released;
        f.put_move(&plan, FROM, 5);
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Done);
        let ops = data_ops(&f);
        for op in [
            "agent_data.describe",
            "agent_data.diverged",
            "agent_data.import",
            "agent_data.activate",
        ] {
            assert!(ops.contains(&op), "data missed {op}: {ops:?}");
        }
        assert!(!broca_reached(&f, &broca), "{:?}", broca.calls());
        assert_eq!(skipped_participants(&f.row()), vec!["broca".to_string()]);
        assert_eq!(f.store().agent_bound_refs(AGENT).unwrap().len(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_restart_keeps_the_recorded_skip_even_once_the_participant_appears() {
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([])).await;
        f.pickup().await;
        // Hold the move just after the preconditions decided the skip.
        f.refuse("agent_data.diverged", "external_not_present");
        f.receiver().tick().await.unwrap();
        let row = f.row();
        assert_eq!(row.step, Step::PickedUp);
        assert_eq!(row.hold.as_ref().unwrap()["op"], "agent_data.diverged");
        assert_eq!(skipped_participants(&row), vec!["broca".to_string()]);
        // broca is now installed and ready here, but the move already recorded
        // that this Mac never checked it, so the restarted receiver must not call it.
        f.discovery.daemon.restart(snapshot_with_broca(true, true));
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Activated).await;
        assert!(!broca_reached(&f, &broca), "{:?}", broca.calls());
        assert!(data_ops(&f).contains(&"agent_data.activate"));
        assert_eq!(skipped_participants(&f.row()), vec!["broca".to_string()]);
    }

    /// Publishes the source's abort of the picked-up cut, as the source Mac
    /// does when it gives up before this receiver ran its preconditions.
    fn source_aborts(f: &Fixture) {
        let mut plan = f.plan();
        plan.state = State::Aborted;
        plan.reason = Some(MoveReason::Operator);
        f.put_move(&plan, FROM, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_cleanup_before_preconditions_skips_an_absent_participant_with_no_refs() {
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([])).await;
        f.pickup().await;
        source_aborts(&f);
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Aborted).await;
        assert!(!broca_reached(&f, &broca), "{:?}", broca.calls());
        let ops = data_ops(&f);
        assert!(ops.contains(&"agent_data.abort"), "{ops:?}");
        // The abort came first: the preconditions never ran.
        assert!(!ops.contains(&"agent_data.describe"), "{ops:?}");
        assert_eq!(skipped_participants(&f.row()), vec!["broca".to_string()]);
        assert_eq!(f.claim_cas_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_restart_mid_cleanup_keeps_skipping_the_absent_participant() {
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([])).await;
        f.pickup().await;
        source_aborts(&f);
        // data's abort lands but its reply is lost (twice, past the client's own
        // retry), so cleanup stops after it recorded the skip and before the
        // move is aborted.
        for _ in 0..2 {
            f.discovery.participant.fake.script(
                "agent_data.abort",
                Effect::LoseReply(TransportError::NoReply),
            );
        }
        f.receiver().tick().await.unwrap();
        let row = f.row();
        assert_eq!(row.step, Step::PickedUp);
        assert!(row.abort_requested.is_some());
        assert_eq!(row.hold.as_ref().unwrap()["op"], "agent_data.abort");
        assert_eq!(skipped_participants(&row), vec!["broca".to_string()]);
        // broca is installed now; the restarted cleanup must still skip it.
        f.discovery.daemon.restart(snapshot_with_broca(true, true));
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Aborted).await;
        assert!(!broca_reached(&f, &broca), "{:?}", broca.calls());
        assert_eq!(skipped_participants(&f.row()), vec!["broca".to_string()]);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_absent_participant_with_refs_still_aborts_as_unavailable() {
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([{"ref_kind":"session","ref":"r2"}])).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        let row = f.row();
        assert_eq!(row.reason.as_deref(), Some("receiver_precondition"));
        assert_eq!(
            row.detail,
            Some(json!({"code":"participant_unavailable","module_id":"broca","absent":true}))
        );
        assert!(skipped_participants(&row).is_empty());
        f.until(&mut receiver, Step::Aborted).await;
        assert!(!broca_reached(&f, &broca), "{:?}", broca.calls());
        assert!(!data_ops(&f).contains(&"agent_data.import"));
        assert_eq!(f.claim_cas_count(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_disabled_participant_with_no_refs_still_holds() {
        for registered in [false, true] {
            let f = Fixture::new(false).await;
            let broca = add_broca(&f, json!([])).await;
            f.discovery
                .daemon
                .restart(snapshot_with_broca(false, registered));
            f.pickup().await;
            let mut receiver = f.receiver();
            for _ in 0..3 {
                advance_clock();
                receiver.tick().await.unwrap();
                let row = f.row();
                assert_eq!(row.step, Step::PickedUp);
                assert!(row.abort_requested.is_none());
                let hold = row.hold.unwrap();
                assert_eq!(
                    (&hold["code"], &hold["module_id"]),
                    (&json!("participant_unavailable"), &json!("broca"))
                );
            }
            assert!(skipped_participants(&f.row()).is_empty());
            assert!(!data_ops(&f).contains(&"agent_data.diverged"));
            assert!(!broca_reached(&f, &broca));
        }
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_row_without_a_recorded_skip_decodes_and_skips_nothing() {
        // The progress shape the previous release wrote, with no skip field.
        let old: Progress = serde_json::from_value(json!({
            "aborted":[],"touched":["data"],"errors":[],"retry_at_ms":0,"pending":null,
            "take_failed":false,"contract_violation":null,"activation_fork":null,"takeover_sent":false
        }))
        .unwrap();
        assert!(old.skipped_absent.is_none() && !old.skips("broca"));
        assert!(serde_json::to_value(&old)
            .unwrap()
            .get("skipped_absent")
            .is_none());

        // A row an older receiver brought to activation with broca installed.
        let f = Fixture::new(false).await;
        let broca = add_broca(&f, json!([])).await;
        f.discovery.daemon.restart(snapshot_with_broca(true, true));
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activating).await;
        f.edit(|row| {
            row.plan["_receiver"]
                .as_object_mut()
                .unwrap()
                .remove("skipped_absent");
        });
        assert!(f.row().plan["_receiver"].get("skipped_absent").is_none());
        // With no recorded set nothing is skipped, and presence is not
        // re-evaluated either: broca vanishing now holds activation for it.
        f.discovery.daemon.restart(snapshot(true));
        let mut restarted = f.receiver();
        restarted.tick().await.unwrap();
        let row = f.row();
        assert_eq!(row.step, Step::Activating);
        assert_eq!(row.hold.as_ref().unwrap()["module_id"], "broca");
        assert!(skipped_participants(&row).is_empty());
        f.discovery.daemon.restart(snapshot_with_broca(true, true));
        f.until(&mut restarted, Step::Activated).await;
        assert!(broca
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Activate(_))));
    }

    #[tokio::test]
    async fn receiver_diverged_advanced_and_unknown_abort_before_import() {
        for state in [DivergedState::Advanced, DivergedState::Unknown] {
            let f = Fixture::new(true).await;
            f.pickup().await;
            f.discovery
                .participant
                .fake
                .set_diverged(reference(), state);
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Aborted).await;
            assert_eq!(f.row().reason.as_deref(), Some("diverged"));
            assert!(!f
                .discovery
                .participant
                .fake
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Import(_))));
            assert_eq!(f.claim_cas_count(), 0);
        }
    }
    #[tokio::test]
    async fn receiver_external_not_present_holds_then_imports_and_claims() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        f.refuse("agent_data.diverged", "external_not_present");
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::PickedUp);
        assert_eq!(f.row().hold.unwrap()["code"], "external_not_present");
        f.until(&mut receiver, Step::Claimed).await;
        assert!(f.row().hold.is_none());
    }
    #[tokio::test]
    async fn receiver_writer_injected_after_diverged_holds_and_reruns_diverged() {
        let f = Fixture::new(true).await;
        f.pickup().await;
        f.discovery
            .participant
            .write_after_diverged
            .store(true, Ordering::Relaxed);
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::PickedUp);
        assert_eq!(f.row().hold.unwrap()["reason_code"], "local_writer");
        assert!(!f
            .discovery
            .participant
            .fake
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Import(_))));
        f.discovery
            .participant
            .fake
            .set_busy(reference(), BusyRef::new(BusyState::Idle));
        f.until(&mut receiver, Step::Claimed).await;
        assert_eq!(
            f.discovery
                .participant
                .fake
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Diverged(_)))
                .count(),
            2
        );
    }
    #[tokio::test]
    async fn receiver_local_liveness_holds_only_already_bound_refs() {
        let f = Fixture::new(true).await;
        f.pickup().await;
        f.store()
            .apply_entry("fixture", "{}", "fixture", None, |tx| {
                tx.execute_batch("UPDATE session_binding SET harness='host',session_id='s'")
            })
            .unwrap();
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert_eq!(f.row().hold.unwrap()["reason_code"], "liveness_unknown");
        {
            let mut liveness = f.liveness.lock().unwrap();
            liveness.stale = false;
            liveness.gone.insert(SessionKey::new("host", "s"));
        }
        f.until(&mut receiver, Step::Claimed).await;
    }
    #[tokio::test]
    async fn receiver_contract_refusals_retry_busy_and_cut_not_ready_but_abort_local_advanced_and_incompatible_version(
    ) {
        for (op, code, terminal) in [
            ("agent_data.diverged", "cut_not_ready", None),
            ("agent_data.import", "busy", None),
            ("agent_data.import", "local_advanced", Some("diverged")),
            (
                "agent_data.diverged",
                "incompatible_version",
                Some("incompatible_version"),
            ),
        ] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            f.refuse(op, code);
            let mut receiver = f.receiver();
            receiver.tick().await.unwrap();
            assert_eq!(f.row().step, Step::PickedUp);
            if let Some(reason) = terminal {
                f.until(&mut receiver, Step::Aborted).await;
                assert_eq!(f.row().reason.as_deref(), Some(reason));
                assert_eq!(f.claim_cas_count(), 0);
                let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
                let record = wire::decode_move_record(
                    &format!("move:{CUT}"),
                    reply.body.as_deref().unwrap(),
                )
                .unwrap();
                assert_eq!(record.detail.unwrap()["code"], code);
            } else {
                assert_eq!(f.row().hold.unwrap()["code"], code);
                f.until(&mut receiver, Step::Claimed).await;
            }
        }
    }
    #[tokio::test]
    async fn receiver_out_of_contract_refusal_stops_even_after_restart() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        f.refuse("agent_data.import", "external_not_present");
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert!(f.row().plan["_receiver"]["contract_violation"].is_object());
        let count = f.discovery.participant.fake.calls().len();
        let mut restarted = f.receiver();
        advance_clock();
        restarted.tick().await.unwrap();
        assert_eq!(count, f.discovery.participant.fake.calls().len());
        assert_eq!(f.row().step, Step::PickedUp);
        assert_eq!(f.worker.health.move_errors.load(Ordering::Relaxed), 2);
        assert_eq!(f.claim_cas_count(), 0);
    }

    #[tokio::test]
    async fn receiver_digest_mismatch_import_aborts() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        f.refuse("agent_data.import", "digest_mismatch");
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert_eq!(f.row().reason.as_deref(), Some("participant_refused"));
        assert_eq!(f.row().detail.unwrap()["code"], "digest_mismatch");
        f.until(&mut receiver, Step::Aborted).await;
        assert_eq!(f.claim_cas_count(), 0);
        assert!(f.discovery.participant.fake.staged().is_empty());
    }

    #[tokio::test]
    async fn receiver_digest_mismatch_activate_holds_fork_across_restart() {
        for with_reason in [true, false] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Activating).await;
            let mut refusal = crate::move_driver::digest_refusal("activate");
            if !with_reason {
                refusal.detail = None;
            }
            f.discovery
                .participant
                .fake
                .script("agent_data.activate", Effect::Refuse(refusal));
            receiver.tick().await.unwrap();
            let row = f.row();
            assert_eq!(row.step, Step::Activating);
            assert!(row.abort_requested.is_none());
            assert!(row.plan["_receiver"]["contract_violation"].is_null());
            assert!(row.plan["_receiver"]["activation_fork"].is_object());
            let detail = row.detail.unwrap();
            assert_eq!(detail["module_id"], "data");
            assert_eq!(detail["op"], "agent_data.activate");
            assert_eq!(detail["code"], "digest_mismatch");
            if with_reason {
                assert_eq!(detail["reason"], "staging_digest_mismatch");
            } else {
                assert!(detail.get("reason").is_none());
            }
            let calls = f.discovery.participant.fake.calls().len();
            for _ in 0..3 {
                advance_clock();
                receiver.tick().await.unwrap();
            }
            let mut restarted = f.receiver();
            restarted.tick().await.unwrap();
            assert_eq!(calls, f.discovery.participant.fake.calls().len());
            assert_eq!(f.row().step, Step::Activating);
            assert_eq!(f.claim().await.version, 1);
            assert_eq!(f.claim_cas_count(), 1);
        }
    }

    #[tokio::test]
    async fn receiver_digest_mismatch_abort_stuck_retries_once_on_restart() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Claimed).await;
        f.edit(|r| r.abort_requested = Some(json!({"reason":"operator","detail":null})));
        for _ in 0..2 {
            f.discovery.participant.fake.script(
                "agent_data.abort",
                Effect::Refuse(crate::move_driver::digest_refusal("abort")),
            );
        }
        receiver.tick().await.unwrap();
        let row = f.row();
        assert_eq!(row.step, Step::Claimed);
        let hold = row.hold.unwrap();
        assert_eq!(hold["state"], "stuck");
        assert_eq!(hold["module_id"], "data");
        assert_eq!(hold["op"], "agent_data.abort");
        assert_eq!(hold["step"], "claimed");
        assert_eq!(hold["code"], "digest_mismatch");
        assert_eq!(hold["reason"], "undo_digest_mismatch");
        assert_eq!(row.detail.unwrap(), hold);
        assert!(row.plan["_receiver"]["contract_violation"].is_null());
        let aborts = || {
            f.discovery
                .participant
                .fake
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Abort(_)))
                .count()
        };
        assert_eq!(aborts(), 1);
        for _ in 0..3 {
            advance_clock();
            receiver.tick().await.unwrap();
        }
        assert_eq!(aborts(), 1);
        let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        assert_eq!(
            wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                .unwrap()
                .state,
            State::Claimed
        );
        let mut restarted = f.receiver();
        restarted.tick().await.unwrap();
        assert_eq!(aborts(), 2);
        for _ in 0..3 {
            advance_clock();
            restarted.tick().await.unwrap();
        }
        assert_eq!(aborts(), 2);
        assert_eq!(f.row().hold.unwrap()["reason"], "undo_digest_mismatch");
        restarted.retry_held_abort();
        f.until(&mut restarted, Step::Aborted).await;
        assert_eq!(aborts(), 3);
        assert!(f.row().hold.is_none());
    }
    #[tokio::test]
    async fn receiver_wrong_author_wrong_epoch_or_no_persisted_take_holds_claim_moved() {
        for mismatch in ["author", "epoch", "missing_take"] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Staged).await;
            let expected = AgentClaim {
                v: 1,
                agent_id: AGENT.into(),
                holder: Some(TO.into()),
                epoch: 1,
                cut_id: Some(CUT.into()),
                reason: None,
            };
            if mismatch != "missing_take" {
                f.edit(|r| {
                    r.claim_take_body = Some(serde_json::to_value(&expected).unwrap());
                    r.claim_take_version = Some(0);
                });
            }
            let mut actual = expected;
            if mismatch == "epoch" {
                actual.epoch = 2;
            }
            f.records.fake.put(
                &format!("claim:{AGENT}"),
                1,
                serde_json::to_vec(&actual).unwrap(),
                if mismatch == "author" { FROM } else { TO },
            );
            for _ in 0..3 {
                advance_clock();
                receiver.tick().await.unwrap();
            }
            assert_ne!(f.row().step, Step::Aborted);
            assert!(f.row().claim_landed_version.is_none());
            let move_reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
            assert_eq!(
                wire::decode_move_record(
                    &format!("move:{CUT}"),
                    move_reply.body.as_deref().unwrap()
                )
                .unwrap()
                .state,
                State::Staged
            );
            if mismatch == "missing_take" {
                assert_eq!(f.claim_cas_count(), 0);
            }
            assert!(!f.row().plan["_receiver"]["errors"]
                .as_array()
                .unwrap()
                .is_empty());
        }
    }
    #[tokio::test]
    async fn receiver_persisted_take_settles_by_author_and_decoded_body() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Staged).await;
        let expected =
            json!({"v":1,"agent_id":AGENT,"holder":TO,"epoch":1,"cut_id":CUT,"reason":null});
        f.edit(|r| {
            r.claim_take_body = Some(expected.clone());
            r.claim_take_version = Some(0);
        });
        f.records.fake.put(
            &format!("claim:{AGENT}"),
            1,
            serde_json::to_string_pretty(&expected)
                .unwrap()
                .into_bytes(),
            TO,
        );
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Claimed).await;
        assert_eq!(f.row().claim_landed_version, Some(1));
        assert_eq!(f.claim().await.version, 1);
    }
    #[tokio::test]
    async fn receiver_claimed_cas_losing_to_aborted_returns_claim_before_terminal_cache() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Staged).await;
        f.records.abort_on_claimed.store(true, Ordering::Relaxed);
        f.until(&mut receiver, Step::Aborted).await;
        let claim = f.claim().await;
        let body = AgentClaim::decode(AGENT, claim.body.as_deref().unwrap()).unwrap();
        assert_eq!(
            (body.holder.as_deref(), body.epoch, body.cut_id),
            (Some(FROM), 2, None)
        );
        let cached = f.store().cached_agent_claim(AGENT).unwrap().unwrap();
        assert_eq!((cached.holder.as_deref(), cached.epoch), (Some(FROM), 2));
        assert!(inbox(&f.worker, TO).await.unwrap().1.pending.is_empty());
    }
    #[tokio::test]
    async fn receiver_crashes_before_and_after_take_and_return_cas_recover_without_second_take() {
        for operation in [wire::ClaimWrite::Take, wire::ClaimWrite::Return] {
            for after in [false, true] {
                let f = Fixture::new(false).await;
                f.pickup().await;
                let mut receiver = f.receiver();
                let returning = matches!(operation, wire::ClaimWrite::Return);
                f.until(
                    &mut receiver,
                    if returning {
                        Step::Claimed
                    } else {
                        Step::Staged
                    },
                )
                .await;
                if returning {
                    f.edit(|r| {
                        r.abort_requested = Some(json!({"reason":"operator","detail":null}))
                    });
                }
                *f.records.crash.lock().unwrap() = Some((operation, after));
                let task = tokio::spawn(async move { receiver.tick().await });
                tokio::time::timeout(Duration::from_secs(5), f.records.hit.acquire())
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                task.abort();
                assert!(task.await.unwrap_err().is_cancelled());

                let mut restarted = f.receiver();
                f.until(
                    &mut restarted,
                    if returning {
                        Step::Aborted
                    } else {
                        Step::Claimed
                    },
                )
                .await;
                let claim = f.claim().await;
                let body = AgentClaim::decode(AGENT, claim.body.as_deref().unwrap()).unwrap();
                assert_eq!(claim.version, if returning { 2 } else { 1 });
                assert_eq!(body.epoch, if returning { 2 } else { 1 });
                assert_eq!(
                    body.holder.as_deref(),
                    Some(if returning { FROM } else { TO })
                );
            }
        }
    }
    #[tokio::test]
    async fn receiver_restart_after_claimed_record_cas_does_not_send_a_second_claimed_cas() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Claimed).await;
        f.edit(|r| r.step = Step::Staged);
        let count = f
            .records
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, p)| op == "record.cas" && p["subject"] == format!("move:{CUT}"))
            .count();
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Claimed).await;
        assert_eq!(f.claim().await.version, 1);
        assert_eq!(
            count,
            f.records
                .calls
                .lock()
                .unwrap()
                .iter()
                .filter(|(op, p)| op == "record.cas" && p["subject"] == format!("move:{CUT}"))
                .count()
        );
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_driver_polls_inbox_at_startup_every_thirty_seconds_and_on_reconnect() {
        check_driver_polling(false).await;
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_driver_reconnect_mid_poll_gives_exactly_one_follow_up() {
        check_driver_polling(true).await;
    }

    async fn check_driver_polling(mid_poll: bool) {
        let f = Fixture::new(false).await;
        // An empty inbox starts no per-cut tasks, whose completion or receiver
        // activation would independently wake the driver and obscure read counts.
        f.set_inbox(vec![]);
        let mut handler = crate::ProjectsHandler::with_runtime("receiver-poll".into(), clock);
        handler.store = f.slot.clone();
        handler.participant_discovery = f.discovery.clone();
        handler.door_client = f.worker.door.clone();
        let probe = crate::move_driver::reconnect_test::Connector::new(
            f.records.clone(),
            format!("inbox:{TO}"),
            mid_poll,
        );
        let connector = Arc::new(crate::move_driver::ReconnectConnector {
            inner: probe.clone(),
            wake: handler.move_wake.clone(),
        });
        handler.record_client = RecordClient::new(connector.clone());
        handler.start_move_driver();
        probe.check_polling(&connector, mid_poll).await;
        handler.stop_move_driver().await;
    }

    #[tokio::test(start_paused = true)]
    async fn receiver_reconnect_polls_once_waiting_and_mid_poll() {
        for mid_poll in [false, true] {
            let mut f = Fixture::new(false).await;
            f.pickup().await;
            f.edit(|row| row.plan["_receiver"] = json!({"contract_violation":true}));
            let probe = crate::move_driver::reconnect_test::Connector::new(
                f.records.clone(),
                format!("move:{CUT}"),
                mid_poll,
            );
            let connector = Arc::new(crate::move_driver::ReconnectConnector {
                inner: probe.clone(),
                wake: f.worker.wake.clone(),
            });
            f.worker.records = RecordClient::new(connector.clone());
            let task = tokio::spawn(f.receiver().run());
            probe.check_polling(&connector, mid_poll).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            f.worker.sql_operations.wait().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cut_accept_reconnect_polls_once_waiting_and_mid_poll() {
        for mid_poll in [false, true] {
            let mut f = Fixture::new(false).await;
            f.pickup().await;
            f.edit(|row| row.cut_accept = Some(json!({"state":"pending"})));
            // CutAccept must remove the inbox entry before accepting the door cut.
            // A failed inbox read leaves the cut pending for a 30-second retry.
            f.records
                .fake
                .error(&format!("inbox:{TO}"), Some("unavailable"));
            let probe = crate::move_driver::reconnect_test::Connector::new(
                f.records.clone(),
                format!("inbox:{TO}"),
                mid_poll,
            );
            let connector = Arc::new(crate::move_driver::ReconnectConnector {
                inner: probe.clone(),
                wake: f.worker.wake.clone(),
            });
            f.worker.records = RecordClient::new(connector.clone());
            let task = tokio::spawn(CutAccept::new(f.worker.clone(), CUT.into()).run());
            probe.check_polling(&connector, mid_poll).await;
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            f.worker.sql_operations.wait().await;
        }
    }

    #[tokio::test]
    async fn receiver_activation_checkpoint_does_not_wait_for_shared_writer() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Claimed).await;
        let _catchup = f.worker.writer.lock().await;
        tokio::time::timeout(Duration::from_secs(5), receiver.tick())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(f.row().step, Step::Activating);
    }

    #[tokio::test]
    async fn receiver_activation_rechecks_local_writers_before_boundary() {
        let f = Fixture::new(true).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Claimed).await;
        f.discovery.participant.fake.set_busy(
            reference(),
            BusyRef::new(BusyState::Busy).with_reason_code("local_writer"),
        );
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Claimed);
        assert_eq!(f.row().hold.unwrap()["reason_code"], "local_writer");
        assert!(!f
            .discovery
            .participant
            .fake
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Activate(_))));
        f.discovery
            .participant
            .fake
            .set_busy(reference(), BusyRef::new(BusyState::Idle));
        f.until(&mut receiver, Step::Activated).await;
        assert!(f.row().hold.is_none());
    }

    #[tokio::test]
    async fn receiver_activation_boundary_retries_busy_and_external_without_abort() {
        for code in [
            "busy",
            "external_not_present",
            "unavailable",
            "invalid_state",
        ] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Activating).await;
            f.refuse("agent_data.activate", code);
            receiver.tick().await.unwrap();
            let row = f.row();
            assert_eq!(row.step, Step::Activating);
            assert!(row.abort_requested.is_none());
            assert_eq!(row.hold.unwrap()["code"], code);
            assert_eq!(row.plan["_receiver"]["retry_at_ms"], clock() + 1000);
            let calls = f.discovery.participant.fake.calls().len();
            receiver.tick().await.unwrap();
            assert_eq!(calls, f.discovery.participant.fake.calls().len());
            let mut restarted = f.receiver();
            f.until(&mut restarted, Step::Activated).await;
            assert_eq!(f.claim_cas_count(), 1);
            assert!(!f
                .discovery
                .participant
                .fake
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Abort(_))));
        }
    }

    #[tokio::test]
    async fn receiver_activation_fork_and_contract_violation_hold_without_retry_across_restart() {
        for code in ["local_advanced", "unexpected_code"] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Activating).await;
            f.refuse("agent_data.activate", code);
            receiver.tick().await.unwrap();
            let row = f.row();
            assert_eq!(row.step, Step::Activating);
            assert!(row.abort_requested.is_none());
            assert_eq!(row.hold.as_ref().unwrap()["module_id"], "data");
            assert!(row.hold.as_ref().unwrap()["move_error"].as_bool().unwrap());
            if code == "local_advanced" {
                assert!(row.plan["_receiver"]["activation_fork"].is_object());
            } else {
                assert!(row.plan["_receiver"]["contract_violation"].is_object());
            }
            let calls = f.discovery.participant.fake.calls().len();
            let mut restarted = f.receiver();
            advance_clock();
            restarted.tick().await.unwrap();
            assert_eq!(calls, f.discovery.participant.fake.calls().len());
            assert_eq!(f.claim_cas_count(), 1);
        }
    }

    #[tokio::test]
    async fn receiver_partial_activation_calls_only_pinned_participants_and_resends_after_restart()
    {
        let f = Fixture::new(false).await;
        let second = Arc::new(FakeParticipant::new(&["session"], vec![]));
        let mut snap = snapshot(true);
        snap.supervisor.modules.push(serde_json::from_value(json!({"module_id":"second","state":"running","enabled":true,"live":true,"health":"ok"})).unwrap());
        let mut entry = snap.catalog.modules[0].clone();
        entry.module_id = "second".into();
        snap.catalog.modules.push(entry);
        f.discovery.daemon.restart(snap);
        f.discovery.others.lock().unwrap().insert(
            "second".into(),
            Arc::new(Peer {
                fake: second.clone(),
                write_after_diverged: AtomicBool::new(false),
                panic_diverged: AtomicBool::new(false),
                diverged_calls: tokio::sync::Semaphore::new(0),
                other_activated: tokio::sync::Semaphore::new(0),
            }),
        );
        let reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        let mut record =
            wire::decode_move_record(&format!("move:{CUT}"), reply.body.as_deref().unwrap())
                .unwrap();
        let mut p = record.participants[0].clone();
        p.module_id = "second".into();
        record.participants.push(p);
        f.put_move(&record, FROM, 1);
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activating).await;
        second.script(
            "agent_data.activate",
            Effect::Refuse(Refusal {
                code: "external_not_present".into(),
                detail: None,
                message: None,
            }),
        );
        receiver.tick().await.unwrap();
        assert!(f
            .discovery
            .participant
            .fake
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Activate(_))));
        assert_eq!(f.row().hold.unwrap()["module_id"], "second");
        assert!(f.store().request_agent_move_abort(CUT, clock()).is_err());
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Activated).await;
        for fake in [&f.discovery.participant.fake, &second] {
            assert_eq!(
                fake.calls()
                    .iter()
                    .filter(|r| matches!(r, Request::Activate(_)))
                    .count(),
                2
            );
            assert!(!fake.calls().iter().any(|r| matches!(r, Request::Abort(_))));
        }
        assert_eq!(f.store().agent_bound_refs(AGENT).unwrap().len(), 1);
    }

    #[tokio::test]
    async fn receiver_restart_after_activated_cas_resends_activate_and_records_bindings() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activating).await;
        f.records.crash_activated.store(true, Ordering::Relaxed);
        let task = tokio::spawn(async move { receiver.tick().await });
        tokio::time::timeout(Duration::from_secs(5), f.records.hit.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert_eq!(f.row().step, Step::Activating);
        assert!(f.store().agent_bound_refs(AGENT).unwrap().is_empty());
        let mut restarted = f.receiver();
        f.until(&mut restarted, Step::Activated).await;
        assert_eq!(
            f.discovery
                .participant
                .fake
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Activate(_)))
                .count(),
            2
        );
        assert_eq!(f.row().activated_at_ms, Some(clock()));
        assert_eq!(f.store().agent_bound_refs(AGENT).unwrap().len(), 1);
        assert!(inbox(&f.worker, TO).await.unwrap().1.pending.is_empty());
    }

    #[tokio::test]
    async fn receiver_moved_bindings_are_gone_replaceable_and_preserve_existing_hosts() {
        for held in [false, true] {
            let f = Fixture::new(held).await;
            if held {
                f.store().apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch("UPDATE session_binding SET harness='host',session_id='old',project_root='/old',registered_by='host';")).unwrap();
                f.liveness
                    .lock()
                    .unwrap()
                    .gone
                    .insert(SessionKey::new("host", "old"));
                f.liveness.lock().unwrap().stale = false;
            }
            let before = f
                .store()
                .session_bindings_for_refs(&[("session", "r")])
                .unwrap()[0]
                .clone();
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Activated).await;
            let binding = f
                .store()
                .session_bindings_for_refs(&[("session", "r")])
                .unwrap()[0]
                .clone()
                .unwrap();
            if let Some(before) = before {
                assert_eq!(binding, before);
            } else {
                assert!(
                    binding.harness.is_none()
                        && binding.session_id.is_none()
                        && binding.project_root.is_none()
                );
                assert_eq!(binding.registered_by, "move");
            }
            let mut handler = crate::ProjectsHandler::with_runtime("moved-bindings".into(), clock);
            handler.store = f.slot.clone();
            handler.liveness = f.liveness.clone();
            assert_eq!(
                handler.read_session_liveness(&[("session", "r")]).unwrap()[0].state,
                "gone"
            );
            f.store().apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_abcdef0123456789','Other','O','assistant',1,1);")).unwrap();
            let request = entorhinal_core::SessionRegistration {
                agent_id: "agent_abcdef0123456789",
                ref_kind: "session",
                session_ref: "r",
                harness: "host",
                session_id: "new",
                project_root: "/new",
                registered_by: "host",
                runner: None,
                network: None,
                now_ms: clock(),
            };
            let e = f.store().register_session(&request).unwrap_err();
            assert_eq!(HandlerError::from(e).code, "ref_bound_elsewhere");
            let request = entorhinal_core::SessionRegistration {
                agent_id: AGENT,
                ..request
            };
            let registered = f.store().register_session(&request).unwrap();
            assert_eq!(registered.harness.as_deref(), Some("host"));
            assert_eq!(registered.session_id.as_deref(), Some("new"));
            assert_eq!(registered.registered_by, "host");
        }
    }

    fn takeover(state: crate::door_client::TakeoverState) -> crate::fake_door::Effect {
        crate::fake_door::Effect::Takeover(crate::door_client::TakeoverProgress {
            state,
            session_ref: None,
            code: None,
        })
    }

    struct HungAcceptDoor {
        door: FakeDoor,
        hit: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl LogTransport for HungAcceptDoor {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            if method == "cut.accept" {
                self.hit.add_permits(1);
                std::future::pending::<()>().await;
            }
            self.door.call(method, params).await
        }
    }
    struct AcceptConnector(Arc<HungAcceptDoor>);
    #[async_trait]
    impl LogConnector for AcceptConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }

    #[tokio::test]
    async fn receiver_hung_accept_cannot_block_released_or_takeover() {
        for released in [true, false] {
            let mut f = Fixture::new(false).await;
            let gate = Arc::new(HungAcceptDoor {
                door: f.door.for_author([2; 16]),
                hit: tokio::sync::Semaphore::new(0),
            });
            f.worker.door =
                crate::door_client::DoorClient::new(Arc::new(AcceptConnector(gate.clone())));
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(&mut receiver, Step::Activated).await;
            let accept = CutAccept::new(f.worker.clone(), CUT.into());
            let task = tokio::spawn(async move { accept.tick().await });
            tokio::time::timeout(Duration::from_secs(5), gate.hit.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            if released {
                let mut plan = f.plan();
                plan.state = State::Released;
                f.put_move(&plan, FROM, 5);
            } else {
                NOW.with(|n| n.set(f.row().activated_at_ms.unwrap() + 600_000));
                f.door.for_author([2; 16]).script(
                    "cut.takeover",
                    takeover(crate::door_client::TakeoverState::Taken),
                );
            }
            tokio::time::timeout(Duration::from_secs(5), receiver.tick())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                f.row().step,
                if released {
                    Step::Done
                } else {
                    Step::TakenOver
                }
            );
            assert!(!task.is_finished());
            assert!(f.row().hold.is_none());
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        }
    }

    #[tokio::test]
    async fn receiver_takeover_uses_persisted_handler_clock_deadline_and_retries_failed() {
        use crate::door_client::TakeoverState;
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activated).await;
        let activated = f.row().activated_at_ms.unwrap();
        let mut restarted = f.receiver();
        NOW.with(|n| n.set(activated + 599_999));
        restarted.tick().await.unwrap();
        assert!(!f
            .door
            .calls()
            .iter()
            .any(|c| matches!(c.request, DoorRequest::Takeover(_))));
        NOW.with(|n| n.set(activated + 600_000));
        f.door
            .for_author([2; 16])
            .script("cut.takeover", takeover(TakeoverState::Failed));
        restarted.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Activated);
        assert_eq!(f.row().hold.unwrap()["code"], "failed");
        f.door
            .for_author([2; 16])
            .script("cut.takeover", takeover(TakeoverState::InProgress));
        NOW.with(|n| n.set(activated + 601_000));
        let mut restarted = f.receiver();
        restarted.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Activated);
        f.door
            .for_author([2; 16])
            .script("cut.status", takeover(TakeoverState::Taken));
        restarted.tick().await.unwrap();
        assert_eq!(f.row().step, Step::TakenOver);
        assert!(f.row().hold.is_none());
        assert_eq!(
            f.door
                .calls()
                .iter()
                .filter(|c| matches!(c.request, DoorRequest::Takeover(_)))
                .count(),
            2
        );
        assert_eq!(f.row().activated_at_ms, Some(activated));
        assert_eq!(f.plan().state, State::Published);
        let observed = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
        assert_eq!(
            wire::decode_move_record(&format!("move:{CUT}"), observed.body.as_deref().unwrap())
                .unwrap()
                .state,
            State::Activated
        );
    }

    #[tokio::test]
    async fn receiver_cut_accept_failures_never_delay_released_or_takeover() {
        use crate::door_client::TakeoverState;
        for code in ["unavailable", "cut_not_published"] {
            for released in [true, false] {
                let f = Fixture::new(false).await;
                f.pickup().await;
                let mut receiver = f.receiver();
                f.until(&mut receiver, Step::Activated).await;
                f.door
                    .for_author([2; 16])
                    .script("cut.accept", crate::fake_door::Effect::Refuse(code.into()));
                CutAccept::new(f.worker.clone(), CUT.into())
                    .tick()
                    .await
                    .unwrap();
                assert_eq!(f.row().cut_accept.as_ref().unwrap()["code"], code);
                assert!(f.row().hold.is_none());
                assert_eq!(f.worker.health.move_errors.load(Ordering::Relaxed), 1);
                if released {
                    let mut plan = f.plan();
                    plan.state = State::Released;
                    f.put_move(&plan, FROM, 5);
                } else {
                    NOW.with(|n| n.set(f.row().activated_at_ms.unwrap() + 600_000));
                    f.door
                        .for_author([2; 16])
                        .script("cut.takeover", takeover(TakeoverState::Taken));
                }
                let mut restarted = f.receiver();
                restarted.tick().await.unwrap();
                assert_eq!(
                    f.row().step,
                    if released {
                        Step::Done
                    } else {
                        Step::TakenOver
                    }
                );
                assert!(f.row().hold.is_none());
                assert_eq!(f.row().cut_accept.as_ref().unwrap()["code"], code);
                assert!(inbox(&f.worker, TO).await.unwrap().1.pending.is_empty());
                if code == "unavailable" {
                    advance_clock();
                    let accept = CutAccept::new(f.worker.clone(), CUT.into());
                    accept.tick().await.unwrap();
                    assert_eq!(
                        f.row().cut_accept.unwrap(),
                        json!({"state":"accepted","lifted":[]})
                    );
                } else {
                    assert!(f.store().list_pending_cut_accepts().unwrap().is_empty());
                }
            }
        }
    }

    #[tokio::test]
    async fn receiver_cut_accept_backoff_survives_terminal_steps_and_restart_and_caps_at_five_minutes(
    ) {
        let f = Fixture::new(false).await;
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activated).await;
        let mut plan = f.plan();
        plan.state = State::Released;
        f.put_move(&plan, FROM, 5);
        receiver.tick().await.unwrap();
        assert_eq!(f.row().step, Step::Done);
        for attempt in 1..=12 {
            let before = f
                .door
                .calls()
                .iter()
                .filter(|c| matches!(c.request, DoorRequest::Accept(_)))
                .count();
            f.door.for_author([2; 16]).script(
                "cut.accept",
                crate::fake_door::Effect::Refuse("unavailable".into()),
            );
            CutAccept::new(f.worker.clone(), CUT.into())
                .tick()
                .await
                .unwrap();
            let accept = f.row().cut_accept.unwrap();
            assert_eq!(accept["attempts"], attempt);
            assert_eq!(
                accept["retry_at_ms"],
                clock() + backoff(attempt).as_millis() as i64
            );
            assert!(f.row().hold.is_none());
            CutAccept::new(f.worker.clone(), CUT.into())
                .tick()
                .await
                .unwrap();
            assert_eq!(
                f.door
                    .calls()
                    .iter()
                    .filter(|c| matches!(c.request, DoorRequest::Accept(_)))
                    .count(),
                before + 1
            );
            assert_eq!(f.store().list_pending_cut_accepts().unwrap().len(), 1);
            NOW.with(|n| n.set(accept["retry_at_ms"].as_i64().unwrap()));
        }
        assert_eq!(backoff(12), Duration::from_secs(300));
        CutAccept::new(f.worker.clone(), CUT.into())
            .tick()
            .await
            .unwrap();
        assert_eq!(f.row().cut_accept.unwrap()["state"], "accepted");
        assert!(f.store().list_pending_cut_accepts().unwrap().is_empty());
    }

    #[tokio::test]
    async fn receiver_move_back_accepts_incoming_cut_and_lifts_earlier_session_refs() {
        let f = Fixture::new(true).await;
        let old_cut = "abcdef0123456789abcdef0123456789";
        let old_source = f.door.for_author([2; 16]);
        let item = crate::door_client::CutItem {
            session_ref: "a".repeat(32),
            stream: "reserved:data".into(),
            end_offset: 42,
            digest: "a".repeat(64),
        };
        old_source.set_stream(item.clone());
        old_source.script(
            "cut.seal",
            crate::fake_door::Effect::Seal(crate::door_client::SealState::Published, None),
        );
        old_source
            .client()
            .seal(&SealRequest {
                cut_id: old_cut.into(),
                items: vec![item],
            })
            .await
            .unwrap();
        old_source.script(
            "cut.handoff",
            crate::fake_door::Effect::Handoff(crate::door_client::HandoffReply {
                state: crate::door_client::HandoffState::Released,
                session_ref: None,
                code: None,
            }),
        );
        old_source
            .client()
            .handoff(&DoorCut {
                cut_id: old_cut.into(),
            })
            .await
            .unwrap();
        f.pickup().await;
        let mut receiver = f.receiver();
        f.until(&mut receiver, Step::Activated).await;
        CutAccept::new(f.worker.clone(), CUT.into())
            .tick()
            .await
            .unwrap();
        assert_eq!(
            f.row().cut_accept.unwrap(),
            json!({"state":"accepted","lifted":["a".repeat(32)]})
        );
        let calls = f.door.calls();
        let accept = calls
            .iter()
            .find(|c| matches!(c.request, DoorRequest::Accept(_)))
            .unwrap();
        assert_eq!(accept.request.cut_id(), CUT);
        assert_eq!(accept.author, TO);
    }

    #[tokio::test]
    async fn receiver_abort_at_every_irreversible_step_writes_nothing_and_restart_never_cleans_up()
    {
        for step in [
            Step::Activating,
            Step::Activated,
            Step::Done,
            Step::TakenOver,
        ] {
            let f = Fixture::new(false).await;
            f.pickup().await;
            let mut receiver = f.receiver();
            f.until(
                &mut receiver,
                if step == Step::Activating {
                    step
                } else {
                    Step::Activated
                },
            )
            .await;
            if step.is_terminal() {
                f.edit(|r| r.step = step);
            }
            let before = f.row();
            let reply = f
                .store()
                .request_agent_move_abort(CUT, clock() + 1)
                .unwrap_err();
            let reply = HandlerError::from(reply);
            assert_eq!(reply.code, "invalid_state");
            assert_eq!(
                reply.detail.unwrap()["step"],
                serde_json::to_value(step).unwrap()
            );
            assert_eq!(f.row(), before);
            drop(receiver);
            let mut restarted = f.receiver();
            if step == Step::Activating {
                f.until(&mut restarted, Step::Activated).await;
            }
            if !step.is_terminal() {
                let mut plan = f.plan();
                plan.state = State::Released;
                f.put_move(&plan, FROM, 5);
                restarted.tick().await.unwrap();
                assert_eq!(f.row().step, Step::Done);
            } else {
                assert!(restarted.tick().await.unwrap().is_none());
            }
            assert_eq!(f.claim_cas_count(), 1);
            assert!(!f
                .discovery
                .participant
                .fake
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Abort(_))));
        }
    }

    #[tokio::test]
    async fn receiver_abort_keeps_row_open_until_claim_observation_applies() {
        let f = Fixture::new(false).await;
        f.pickup().await;
        f.edit(|r| r.abort_requested = Some(json!({"reason":"operator","detail":null})));
        f.records
            .fake
            .error(&format!("claim:{AGENT}"), Some("unavailable"));
        let mut receiver = f.receiver();
        receiver.tick().await.unwrap();
        assert!(!f.row().step.is_terminal());
        f.records.fake.error(&format!("claim:{AGENT}"), None);
        f.until(&mut receiver, Step::Aborted).await;
        assert_eq!(
            f.store().cached_agent_claim(AGENT).unwrap().unwrap().epoch,
            0
        );
    }

    #[test]
    fn receiver_refusals_keep_the_wire_message_and_lost_replies_are_not_refusals() {
        let refused = Failure::participant(
            "data",
            "agent_data.import",
            ClientError::Refused(Refusal {
                code: "busy".into(),
                detail: None,
                message: Some("index rebuilding".into()),
            }),
        );
        assert_eq!(
            refused.refusal.as_ref().unwrap().message.as_deref(),
            Some("index rebuilding")
        );
        assert_eq!(refused.detail["message"], "index rebuilding");
        let lost = Failure::participant("data", "agent_data.import", ClientError::OutcomeUnknown);
        assert!(lost.refusal.is_none());
        assert!(lost.detail.get("message").is_none());
        let door = door_failure(crate::door_client::DoorError::Refused {
            code: "cut_not_published".into(),
            detail: None,
            message: Some("not yet".into()),
        });
        assert_eq!(
            door.refusal.as_ref().unwrap().message.as_deref(),
            Some("not yet")
        );
        assert!(door_failure(crate::door_client::DoorError::OutcomeUnknown)
            .refusal
            .is_none());
    }
}

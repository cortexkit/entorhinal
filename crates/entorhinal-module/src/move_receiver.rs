//! Receive hub-published moves, import their data, then make them live.

use crate::{
    door_client::CutRequest as DoorCut,
    hub_moves::{Decision, SeenMove, State as HubState},
    hub_residence::{MovePosition, MoveTransfer, Residence},
    move_driver::{
        backoff, digest_reason, participant_action, skipped_streams, PeerRefusal, RefusalLog,
        SkippedStreams, Worker, POLL,
    },
    participant_client::{AgentRef, ClientError, CutRequest, ImportReply, ParticipantClient, Refs},
    participant_discovery::Participant,
    session_move::StagedError,
    HandlerError, LivenessState, SessionKey,
};
use cortexkit_role_agent_data::errors::DriverAction;
use cortexkit_role_agent_data::{busy::BusyState, diverged::DivergedState};
use entorhinal_core::{
    move_record::{self as wire, MoveReason, MoveRecord, MoveRecordState as State, PendingMove},
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

mod hub;

fn error(code: &str) -> HandlerError {
    HandlerError::new(code, code)
}
fn checked_version(version: u64) -> Result<i64, HandlerError> {
    i64::try_from(version).map_err(|_| error("record_version_unknown"))
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

pub(crate) async fn poll_hub(worker: &Worker, hub: &Residence) -> Result<(), HandlerError> {
    for seen in hub.watched_moves().await {
        let cut = seen.body.cut_id.clone();
        if seen.body.receiver != hub.move_box() || seen.body.state != HubState::Published {
            continue;
        }
        let lookup = cut.clone();
        if worker.sql(move |s| s.agent_move(&lookup)).await?.is_some() {
            continue;
        }
        let plan = seen.body.participant_plan();
        let pending = PendingMove {
            cut_id: cut,
            from: seen.writer.clone(),
            agent_id: plan.agent_id.clone(),
        };
        let mut row = pickup_row(
            &plan,
            &pending,
            checked_version(seen.revision)?,
            (worker.clock)(),
        );
        row.plan["_hub_move"] = json!(seen);
        if let Err(e) = worker.sql(move |s| Ok(s.admit_received_move(&row))).await? {
            if !matches!(e.code.as_str(), "move_in_progress" | "move_cap") {
                tracing::debug!(code = e.code, "hub move not admitted");
            }
        }
    }
    Ok(())
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Progress {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub_decision: Option<Decision>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub_return: Option<MoveTransfer>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub_items: Option<Vec<crate::door_client::CutItem>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    hub_accepted: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    hub_activated: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    hub_imported: Vec<String>,
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
    /// Plan participants this machine leaves out of the whole receiver side of the
    /// move: each was neither configured nor registered here when the
    /// preconditions ran, and it was optional or had no data in the cut.
    /// `None` means the preconditions have not decided yet. Every later step and
    /// every restart reads this set instead of asking the daemon again, so a
    /// module installed mid-move is never called for a cut it never saw.
    /// Rows written before this field existed decode as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    skipped_absent: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    skipped_streams: SkippedStreams,
    /// Entries each participant reported dropping while it imported this cut,
    /// keyed by participant, then by the staged stream's `sessionRef`, then by
    /// the participant's own reason, with the count it gave. Each participant's
    /// latest import reply replaces its whole entry, so a resent import after
    /// a lost reply or a restart never counts the same drops twice. Absent
    /// when no participant reported drops, so rows without losses keep the
    /// bytes they had before this field existed.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    dropped: BTreeMap<String, ImportDrops>,
}
/// One participant's reported drops: `sessionRef` to reason to count.
type ImportDrops = BTreeMap<String, BTreeMap<String, u64>>;
impl Progress {
    fn skips(&self, module: &str) -> bool {
        self.skipped_absent
            .as_ref()
            .is_some_and(|skipped| skipped.iter().any(|m| m == module))
    }
    fn optional_skips(&self, plan: &MoveRecord) -> Vec<String> {
        plan.participants
            .iter()
            .filter(|p| p.optional && self.skips(&p.module_id))
            .map(|p| p.module_id.clone())
            .collect()
    }
    /// Keep what one participant's import reply says it dropped, replacing
    /// anything an earlier reply from the same participant recorded.
    fn record_drops(&mut self, module: &str, reply: &ImportReply) {
        let drops: ImportDrops = reply
            .items
            .iter()
            .filter_map(|item| {
                item.dropped
                    .clone()
                    .map(|dropped| (item.session_ref.clone(), dropped))
            })
            .collect();
        if drops.is_empty() {
            self.dropped.remove(module);
        } else {
            self.dropped.insert(module.to_owned(), drops);
        }
    }
}

/// What participants dropped while importing this move's data on this
/// machine, one `{participant, session_ref, reason, count}` entry per reason,
/// with reasons and counts exactly as the participants reported them. Empty
/// for source rows, for moves nothing was dropped from, and for aborted moves,
/// whose staged imports were discarded and never became the agent's data.
pub(crate) fn import_losses(row: &AgentMove) -> Vec<Value> {
    if row.side != MoveSide::Receiver || row.step == Step::Aborted {
        return Vec::new();
    }
    let Some(dropped) = row.plan["_receiver"]
        .get("dropped")
        .cloned()
        .and_then(|value| serde_json::from_value::<BTreeMap<String, ImportDrops>>(value).ok())
    else {
        return Vec::new();
    };
    let mut losses = Vec::new();
    for (participant, streams) in dropped {
        for (session_ref, reasons) in streams {
            for (reason, count) in reasons {
                losses.push(json!({
                    "participant": participant,
                    "session_ref": session_ref,
                    "reason": reason,
                    "count": count,
                }));
            }
        }
    }
    losses
}

/// The plan participants this receiver calls: every one except those
/// recorded as skipped because they are absent here.
fn called<'a>(plan: &'a MoveRecord, progress: &Progress) -> Vec<&'a wire::MoveParticipant> {
    plan.participants
        .iter()
        .filter(|p| !progress.skips(&p.module_id))
        .collect()
}

/// Plan participants this machine neither configures nor registers.
fn absent_participants<'a>(
    plan: &'a MoveRecord,
    supervisor: &SupervisorListResponse,
    catalog: &CatalogList,
) -> Vec<&'a wire::MoveParticipant> {
    let absent = |module: &str| {
        !supervisor.modules.iter().any(|m| m.module_id == module)
            && !catalog.modules.iter().any(|m| m.module_id == module)
    };
    plan.participants
        .iter()
        .filter(|p| absent(&p.module_id))
        .collect()
}

/// Participants a receiver row recorded as skipped, for move status.
/// Empty for source rows and for rows whose preconditions have not run yet.
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
        failure.violation |= failure.action.is_none();
        if !failure.violation
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
                failure.detail["note"] = json!("Upgrade this machine to import the cut's format");
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
    async fn decide_skipped_absent(
        &self,
        row: &mut AgentMove,
        plan: &MoveRecord,
        progress: &mut Progress,
        supervisor: &SupervisorListResponse,
        catalog: &CatalogList,
    ) -> Result<(), Failure> {
        if progress.skipped_absent.is_some() {
            return Ok(());
        }
        let absent = absent_participants(plan, supervisor, catalog);
        // The sealed manifest proves whether a required participant with refs
        // carries no data and names the streams lost for an optional skip.
        let items = if let Some(items) = &progress.hub_items {
            items.clone()
        } else if absent.iter().any(|p| p.optional || !p.refs.is_empty()) {
            let read = self
                .worker
                .door
                .read(&DoorCut {
                    cut_id: self.cut.clone(),
                })
                .await
                .map_err(|e| {
                    let mut failure =
                        Failure::hold(e.code(), json!({"op":"cut.read","error":format!("{e:?}")}));
                    if let crate::door_client::DoorError::Refused { message, .. } = e {
                        failure.refusal = Some(Box::new(PeerRefusal { message }));
                    }
                    failure
                })?;
            if read.author != plan.from {
                tracing::warn!(cut = %self.cut, expected_author = %plan.from, author = %read.author,
                    "sealed cut author does not match move source");
                let mut failure = Failure::hold(
                    "cut_author_mismatch",
                    json!({"op":"cut.read","expected_author":plan.from,"author":read.author}),
                );
                failure.diagnostic = true;
                return Err(failure);
            }
            read.items
        } else {
            Vec::new()
        };
        progress.skipped_absent = Some(
            absent
                .into_iter()
                .filter(|p| {
                    p.optional
                        || p.refs.is_empty()
                        || !items
                            .iter()
                            .any(|item| item.stream == format!("reserved:{}", p.module_id))
                })
                .map(|p| p.module_id.clone())
                .collect(),
        );
        progress.skipped_streams = skipped_streams(&items, &progress.optional_skips(plan));
        self.save(row, progress).await
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
        // Release metadata is local bookkeeping, never part of the shared plan.
        let hub_plan: SeenMove = if let Some(plan) = plan_value.as_object_mut() {
            plan.remove("_cut_release");
            plan.remove("_hub_move")
                .map(serde_json::from_value)
                .transpose()
                .map_err(|e| HandlerError::new("plan_malformed", e.to_string()))?
                .ok_or_else(|| error("plan_malformed"))?
        } else {
            return Err(error("plan_malformed"));
        };
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
        wire::check_pinned_plan(&hub_plan.body.participant_plan(), &plan)
            .map_err(|err| error(err.code()))?;
        let result: Result<Duration, Failure> = async {
            let (hub, seen) = self.read_hub_move(&hub_plan).await?;
            let observed = seen.body.participant_plan();
            let version = seen.revision;
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
            self.advance_hub(&hub, &seen, &hub_plan, &mut row, &plan, &mut progress)
                .await
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
                if f.code == "cut_import_pending" && f.detail["module_id"].is_string() {
                    // Engram may still be blocking reads while it installs the
                    // cut. A participant can pass up that transient refusal;
                    // retry the step even though it is not a participant code.
                    f.violation = false;
                    f.reason = None;
                    f.action = Some(DriverAction::RetryWithinStep);
                }
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
                let accepted_during_abort =
                    f.detail["op"] == "cut.abort" && f.code == "cut_accepted";
                if accepted_during_abort {
                    tracing::error!(cut_id = %self.cut, "engram accepted a cut whose hub move was aborted");
                }
                let irreversible =
                    accepted_during_abort || !row.step.before_boundary(MoveSide::Receiver);
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
                    if progress.hub_decision == Some(Decision::Abort) {
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
                Ok(Some(
                    if f.reason.is_some() && progress.hub_decision != Some(Decision::Abort) {
                        Duration::ZERO
                    } else {
                        backoff(row.attempts).min(POLL)
                    },
                ))
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
        // no data for this agent. A module that is not on this machine at all cannot
        // hold local data for the agent. Skip an optional module or one that
        // carries no data for the rest of the move. Decide once and persist the set:
        // later steps and restarts must not call a module this machine never checked.
        // A configured but disabled or warming module is not absent: it may still
        // keep data on disk, so the move holds below until it is enabled and
        // its fork check (agent_data.diverged) can run.
        self.decide_skipped_absent(row, plan, progress, &supervisor, &catalog)
            .await?;
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
        self.local_writers(plan, progress).await
    }

    /// The session bindings the source carried for this move, as this machine
    /// imported them; none when the move carries no bindings stream. A
    /// live carried ref the move does not name breaks the agent-data
    /// contract and holds the move; finished metadata need not name a ref.
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
        if !installed.skipped_finished.is_empty() {
            tracing::warn!(cut = %self.cut,
                skipped = installed.skipped_finished.len(),
                collisions = ?installed.skipped_finished,
                "finished session metadata skipped because local bindings own its host keys");
        }
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
                "carried session bindings collided with bindings on this machine and were installed as placeholders"
            );
        }
    }

    async fn catalog_preflight(&self, plan: &MoveRecord) -> Result<(), Failure> {
        let Some(expected) = &plan.pin_digest else {
            return Ok(());
        };
        let catalog = self.worker.catalog.as_ref().ok_or_else(|| Failure::abort(MoveReason::IncompatibleVersion, json!({"code":"incompatible_version","module_id":"entorhinal","note":"Both machines need catalog move support"})))?;
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

    async fn local_writers(
        &mut self,
        plan: &MoveRecord,
        progress: &Progress,
    ) -> Result<(), Failure> {
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
                let answer = state.answer(&key);
                if answer.state == "gone" {
                    continue;
                }
                let active = answer.state == "active";
                return Err(Failure::hold(
                    "agent_busy",
                    json!({"ref":r.reference,"ref_kind":r.ref_kind,
                    "reason_code":if active {"session_active"} else {"liveness_unknown"},"note":answer.cause}),
                ));
            }
        }
        for p in called(plan, progress) {
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
                    let reply = client
                        .import(&self.request(p)?)
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                    progress.record_drops(&p.module_id, &reply);
                    if progress.hub_items.is_some() {
                        progress.hub_imported.push(p.module_id.clone());
                    }
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
                    if progress.hub_decision == Some(Decision::Activate) {
                        progress.hub_activated.push(p.module_id.clone());
                    }
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
        hub: (&Residence, &SeenMove),
    ) -> Result<(), Failure> {
        let (hub, seen) = hub;
        match row.step {
            Step::PickedUp => {
                self.settle_pending(row, plan, progress).await?;
                self.preconditions(row, plan, progress).await?;
                for p in called(plan, progress) {
                    if progress.hub_imported.contains(&p.module_id) {
                        continue;
                    }
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
                        // so either machine can show it beside the participant and ref.
                        let mut detail = json!({"module_id":p.module_id,"ref":reference.reference,"state":r.state});
                        if let Some(reason) = &r.reason {
                            detail["reason"] = json!(reason);
                        }
                        return Err(Failure::abort(MoveReason::Diverged, detail));
                    }
                }
                // Diverged asks each participant whether this machine has data past
                // the cut. A host can write while that call is in flight, so recheck
                // session-binding ownership and active local writers before import.
                self.local_writers(plan, progress).await?;
                self.catalog_preflight(plan).await?;
                for p in called(plan, progress) {
                    if progress.hub_imported.contains(&p.module_id) {
                        continue;
                    }
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.import".into(),
                    });
                    self.save(row, progress).await?;
                    let answer = client.import(&self.request(p)?).await;
                    // Saved with the cleared pending call below, so the drops a
                    // reply reports are durable before the move goes on.
                    if let Ok(reply) = &answer {
                        progress.record_drops(&p.module_id, reply);
                        progress.hub_imported.push(p.module_id.clone());
                    }
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
                    let mut staged = observed.clone();
                    staged.skipped = progress.optional_skips(plan);
                    let mut body = seen.body.clone();
                    body.state = HubState::Staged;
                    body.skipped = staged.skipped;
                    row.record_version = checked_version(hub.write_move(&body, Some(seen)).await?)?;
                } else if observed.state != State::Staged && observed.state != State::Claimed {
                    return Err(Failure::hold(
                        "invalid_state",
                        json!({"state":observed.state}),
                    ));
                }
                Self::step(row, progress, Step::Staged);
                self.save(row, progress).await?;
            }
            Step::Activating => {
                self.settle_pending(row, plan, progress).await?;
                for p in called(plan, progress) {
                    if progress.hub_activated.contains(&p.module_id) {
                        continue;
                    }
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.activate".into(),
                    });
                    self.save(row, progress).await?;
                    let answer = client.activate(&self.request(p)?).await;
                    if answer.is_ok() {
                        progress.hub_activated.push(p.module_id.clone());
                    }
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
                    let mut body = seen.body.clone();
                    body.state = HubState::Activated;
                    row.record_version = checked_version(hub.write_move(&body, Some(seen)).await?)?;
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
            }
            _ => {}
        }
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
        // participant call, so cleanup never waits on a module this machine lacks.
        // A module that was absent did not stage anything to undo.
        // Persisting the set makes a restart mid-cleanup skip the same modules.
        self.settle_pending(row, plan, progress).await?;
        let request = row
            .abort_requested
            .clone()
            .ok_or_else(|| Failure::hold("abort_request_missing", Value::Null))?;
        row.reason = request["reason"].as_str().map(str::to_owned);
        row.detail = request.get("detail").filter(|d| !d.is_null()).cloned();
        for p in called(plan, progress) {
            if progress.aborted.contains(&p.module_id) || !progress.touched.contains(&p.module_id) {
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
        // Discard participant staging before asking engram to clear its
        // write block and release its reservation. After a crash, repeat
        // the idempotent door call, not the checkpointed participant aborts.
        self.abort_hub_cut().await?;
        Self::step(row, progress, Step::Aborted);
        self.save(row, progress).await
    }
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
        let attempts = accept["attempts"].as_i64().unwrap_or(0).saturating_add(1);
        // The hub driver accepted the cut before activating any participant.
        let lifted = serde_json::from_value(row.plan["_receiver"]["hub_accepted"].clone())
            .map_err(|_| error("plan_malformed"))?;
        let result: Result<_, crate::door_client::DoorError> =
            Ok(crate::door_client::AcceptReply { lifted });
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

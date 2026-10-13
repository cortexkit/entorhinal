//! Receive published moves: validate the inbox and import data, then durably
//! transfer the agent claim before any participant makes the data live.

use crate::{
    claim_codec::AgentClaim,
    door_client::CutRequest as DoorCut,
    move_driver::{backoff, Worker, POLL},
    participant_client::{AgentRef, ClientError, CutRequest, ParticipantClient, Refs},
    participant_discovery::Participant,
    record_client::{CasRequest, CasSettlement, RecordError, RecordReply},
    HandlerError, LivenessState, SessionKey,
};
use cortexkit_role_agent_data::{busy::BusyState, diverged::DivergedState};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    move_record::{
        self as wire, InboxRecord, MoveReason, MoveRecord, MoveRecordState as State, PendingMove,
    },
    move_store::{AgentMove, MoveSide, MoveStep as Step},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};

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
        plan: serde_json::to_value(record).unwrap(),
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
        let cut = entry.cut_id.clone();
        let existing = worker.sql(move |s| s.agent_move(&cut)).await?;
        if let Some(row) = existing.as_ref() {
            if row.step.is_terminal() || row.step == Step::Activated {
                remove_inbox(worker, &own, &entry.cut_id).await?;
            }
            if row.step.is_terminal() || row.step == Step::Activated {
                continue;
            }
        }
        let subject = format!("move:{}", entry.cut_id);
        let reply = worker.records.read(&subject).await.map_err(record_error)?;
        if reply.stale || reply.absent {
            continue;
        }
        let record = wire::decode_move_record(&subject, reply.body.as_deref().unwrap_or_default())
            .map_err(|e| error(e.code()))?;
        if record.to != own || matches!(record.state, State::Aborted | State::Released) {
            remove_inbox(worker, &own, &entry.cut_id).await?;
            continue;
        }
        if existing.is_some() {
            continue;
        }
        if record.state != State::Published {
            continue;
        }
        let rows = worker.sql(RegistryStore::list_open_agent_moves).await?;
        if rows.len() >= 4 || rows.iter().any(|r| r.agent_id == entry.agent_id) {
            continue;
        }
        let read = worker
            .door
            .read(&DoorCut {
                cut_id: entry.cut_id.clone(),
            })
            .await
            .map_err(|e| HandlerError::new(e.code(), format!("{e:?}")))?;
        checked_version(record.claim_version)?;
        let mut row = pickup_row(
            &record,
            &entry,
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
        // The open-row list above only avoids unnecessary door reads. The
        // core transaction rechecks the agent slot and cap against concurrent
        // source admissions and inbox pickups before inserting a row.
        let result = worker.sql(move |s| Ok(s.admit_received_move(&row))).await?;
        match result {
            Ok(row) if row.step == Step::Aborted => {
                remove_inbox(worker, &own, &entry.cut_id).await?
            }
            Ok(_) => {}
            Err(e) if matches!(e.code.as_str(), "move_in_progress" | "move_cap") => {}
            Err(e) => return Err(e.into()),
        }
    }
    worker.gauges().await
}

#[derive(Clone, Default, Serialize, Deserialize)]
struct Progress {
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
}
impl Failure {
    fn hold(code: &str, detail: Value) -> Self {
        Self {
            code: code.into(),
            detail,
            reason: None,
            diagnostic: false,
            violation: false,
        }
    }
    fn abort(reason: MoveReason, detail: Value) -> Self {
        Self {
            reason: Some(reason),
            ..Self::hold("abort", detail)
        }
    }
    fn participant(module: &str, op: &str, e: ClientError) -> Self {
        let (code, detail, violation) = match e {
            ClientError::Refused(r) => (r.code, r.detail, false),
            ClientError::ContractViolation { code, detail, .. } => (code, detail, true),
            ClientError::OutcomeUnknown => ("unavailable".into(), None, false),
            ClientError::InvalidReply(message) => (
                "invalid_reply".into(),
                Some(json!({"message":message})),
                true,
            ),
        };
        let detail = json!({"module_id":module,"op":op,"code":code,"detail":detail});
        let mut failure = Self::hold(&code, detail);
        failure.violation = violation;
        if !violation
            && !matches!(
                code.as_str(),
                "cut_not_ready" | "unavailable" | "handoff_not_covered" | "external_not_present"
            )
            && !(op == "agent_data.import" && code == "busy")
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
}
impl Receiver {
    pub(crate) fn new(worker: Worker, cut: String, liveness: Arc<Mutex<LivenessState>>) -> Self {
        Self {
            worker,
            cut,
            clients: BTreeMap::new(),
            liveness,
        }
    }
    pub(crate) async fn run(mut self) {
        let mut delay = Duration::ZERO;
        loop {
            tokio::select! { _ = tokio::time::sleep(delay) => {}, _ = self.worker.wake.notified() => {} }
            if self.worker.store.upgrade().is_none() {
                return;
            }
            match self.tick().await {
                Ok(Some(next)) => delay = next,
                Ok(None) => return,
                Err(_) => delay = POLL,
            }
        }
    }
    async fn save(&self, row: &mut AgentMove, progress: &Progress) -> Result<(), Failure> {
        row.plan["_receiver"] = serde_json::to_value(progress).unwrap();
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
    fn request(&self, p: &wire::MoveParticipant) -> CutRequest {
        CutRequest::new(
            Refs::new(
                p.refs
                    .iter()
                    .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
                    .collect(),
            )
            .unwrap(),
            cortexkit_role_agent_data::refs::CutId::new(self.cut.clone()).unwrap(),
        )
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
                row.reason = observed.reason.map(|r| {
                    serde_json::to_value(r)
                        .unwrap()
                        .as_str()
                        .unwrap()
                        .to_owned()
                });
                row.detail = observed.detail.clone();
                row.abort_requested = Some(json!({"reason":row.reason,"detail":row.detail}));
                self.save(&mut row, &progress).await?;
            }
            if progress.contract_violation.is_some() {
                return Ok(POLL);
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
            Err(f) => {
                if f.violation || f.diagnostic || f.reason == Some(MoveReason::ClaimMoved) {
                    progress.errors.push(f.detail.clone());
                }
                if f.violation {
                    progress.contract_violation = Some(f.detail.clone());
                }
                if let Some(reason) = f.reason {
                    row.reason = Some(
                        serde_json::to_value(reason)
                            .unwrap()
                            .as_str()
                            .unwrap()
                            .to_owned(),
                    );
                    row.detail = Some(f.detail.clone());
                    row.abort_requested = Some(json!({"reason":row.reason,"detail":row.detail}));
                    progress.retry_at_ms = 0;
                } else {
                    row.attempts += 1;
                    let delay = backoff(row.attempts);
                    progress.retry_at_ms =
                        (self.worker.clock)().saturating_add(delay.as_millis() as i64);
                    row.hold = Some(
                        json!({"code":f.code,"detail":f.detail,"move_error":f.violation || f.diagnostic}),
                    );
                    if let Some(module) = f.detail.get("module_id") {
                        row.hold.as_mut().unwrap()["module_id"] = module.clone();
                    }
                    for key in ["ref", "reason_code", "note"] {
                        if let Some(value) = f.detail.get(key) {
                            row.hold.as_mut().unwrap()[key] = value.clone();
                        }
                    }
                }
                self.save(&mut row, &progress)
                    .await
                    .map_err(|e| HandlerError::new(&e.code, e.detail.to_string()))?;
                Ok(Some(if f.reason.is_some() {
                    Duration::ZERO
                } else {
                    backoff(row.attempts).min(POLL)
                }))
            }
        }
    }

    async fn preconditions(&mut self, plan: &MoveRecord) -> Result<(), Failure> {
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
        for p in &plan.participants {
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

    async fn local_writers(&mut self, plan: &MoveRecord) -> Result<(), Failure> {
        let refs: Vec<_> = plan
            .participants
            .iter()
            .flat_map(|p| p.refs.clone())
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
            let p = plan
                .participants
                .iter()
                .find(|p| p.module_id == pending.module)
                .ok_or_else(|| Failure::hold("plan_malformed", Value::Null))?;
            let client = self.client(&p.module_id).await?;
            client
                .status(&self.request(p))
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.status", e))?;
            // Recover the persisted participant operation's reply after restart.
            // A lost import reply must be resolved before cleanup can safely
            // abort the participant's staging state.
            match pending.op.as_str() {
                "agent_data.import" => {
                    client
                        .import(&self.request(p))
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                }
                "agent_data.diverged" => {
                    client
                        .diverged(&self.request(p))
                        .await
                        .map_err(|e| Failure::participant(&p.module_id, &pending.op, e))?;
                }
                "agent_data.abort" => {
                    client
                        .abort(&self.request(p))
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
                self.preconditions(plan).await?;
                for p in &plan.participants {
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.diverged".into(),
                    });
                    if !progress.touched.contains(&p.module_id) {
                        progress.touched.push(p.module_id.clone());
                    }
                    self.save(row, progress).await?;
                    let answer = client.diverged(&self.request(p)).await;
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
                        return Err(Failure::abort(
                            MoveReason::Diverged,
                            json!({"module_id":p.module_id,"ref":reference.reference,"state":r.state}),
                        ));
                    }
                }
                // A host or participant can write while diverged is in flight.
                // Do not import until both ownership and local writers are checked again.
                self.local_writers(plan).await?;
                for p in &plan.participants {
                    let client = self.client(&p.module_id).await?;
                    progress.pending = Some(PendingCall {
                        module: p.module_id.clone(),
                        op: "agent_data.import".into(),
                    });
                    self.save(row, progress).await?;
                    let answer = client.import(&self.request(p)).await;
                    if !matches!(answer, Err(ClientError::OutcomeUnknown)) {
                        progress.pending = None;
                        self.save(row, progress).await?;
                    }
                    answer
                        .map_err(|e| Failure::participant(&p.module_id, "agent_data.import", e))?;
                }
                Self::step(row, progress, Step::Imported);
                self.save(row, progress).await?;
            }
            Step::Imported => {
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
                self.take_claim(row, plan, progress).await?;
                // The move-record CAS to claimed can lose to a peer abort after
                // the agent claim transferred. Return ownership to the source
                // before marking the receiver row terminal.
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
            }
            // This driver stops at the last reversible checkpoint; it does not
            // activate participant data or perform the post-activation handoff.
            _ => {
                return Ok(());
            }
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
                if claim.holder.as_deref() == Some(&plan.to)
                    && claim.cut_id.as_deref() == Some(&self.cut)
                {
                    progress.take_failed = true;
                    return Err(Failure::abort(
                        MoveReason::ClaimMoved,
                        json!({"code":"claim_moved","cause":"no_persisted_take"}),
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
            row.claim_take_body = Some(serde_json::to_value(take).unwrap());
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
            body: serde_json::to_vec(row.claim_take_body.as_ref().unwrap()).unwrap(),
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
            body: serde_json::to_vec(&claim).unwrap(),
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
        let conflicts = self
            .worker
            .sql(move |s| s.cache_agent_claims(&observations, now))
            .await?;
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
        self.settle_pending(row, plan, progress).await?;
        let request = row.abort_requested.clone().unwrap();
        row.reason = request["reason"].as_str().map(str::to_owned);
        row.detail = request.get("detail").filter(|d| !d.is_null()).cloned();
        if row.claim_take_body.is_some()
            && row.claim_landed_version.is_none()
            && !progress.take_failed
        {
            self.take_claim(row, plan, progress).await?;
        }
        self.return_claim(row, plan).await?;
        let (record, version) = self.read_move(plan).await?;
        row.record_version = checked_version(version)?;
        if record.state != State::Aborted {
            self.cas_move(row, plan, record.state, State::Aborted)
                .await?;
        }
        for p in &plan.participants {
            // A module absent before any diverged/import call cannot have staged
            // data to discard. Persist which modules were called so a module
            // that disappears after a call still must abort when it returns.
            if !progress.touched.contains(&p.module_id)
                && row
                    .detail
                    .as_ref()
                    .is_some_and(|d| d["absent"] == true && d["module_id"] == p.module_id)
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
                .abort(&self.request(p))
                .await
                .map_err(|e| Failure::participant(&p.module_id, "agent_data.abort", e))?;
            progress.pending = None;
            self.save(row, progress).await?;
        }
        self.apply_claim(row).await?;
        remove_inbox(&self.worker, &plan.to, &self.cut).await?;
        Self::step(row, progress, Step::Aborted);
        self.save(row, progress).await
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
            Ok(self.participant.clone())
        }
    }
    struct Peer {
        fake: Arc<FakeParticipant>,
        write_after_diverged: AtomicBool,
    }
    #[async_trait]
    impl LogTransport for Peer {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
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
        inbox_reads: tokio::sync::Semaphore,
        abort_on_claimed: AtomicBool,
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
            if method == "record.read" && params["subject"] == format!("inbox:{TO}") {
                self.inbox_reads.add_permits(1);
            }
            let is_claim = method == "record.cas" && params["subject"] == format!("claim:{AGENT}");
            if is_claim {
                let slot = self.slot.upgrade().unwrap();
                let store = slot.lock().unwrap().clone().unwrap();
                let row = store.agent_move(CUT).unwrap().unwrap();
                assert!(
                    row.claim_take_body.is_some(),
                    "take body must precede the CAS"
                );
                assert_eq!(row.claim_take_version, Some(0));
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
            let reply = self.fake.call(method, params).await;
            if crash.is_some_and(|(_, after)| after) {
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
                inbox_reads: tokio::sync::Semaphore::new(0),
                abort_on_claimed: AtomicBool::new(false),
            });
            let participant = Arc::new(FakeParticipant::new(
                &["session"],
                if held { vec![reference()] } else { vec![] },
            ));
            participant.unknown_busy_for_unheld(true);
            let discovery = Arc::new(Discovery {
                daemon: FakeDaemon::new(snapshot(true)),
                participant: Arc::new(Peer {
                    fake: participant,
                    write_after_diverged: AtomicBool::new(false),
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
                store: Arc::downgrade(&slot),
                records: RecordClient::new(Arc::new(Connector(records.clone()))),
                door: door.for_author([2; 16]).client(),
                discovery: discovery.clone(),
                health: Arc::new(crate::HealthGauges::default()),
                writer: Arc::new(tokio::sync::Mutex::new(())),
                clock,
                wake: Arc::new(tokio::sync::Notify::new()),
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
    async fn receiver_wrong_author_wrong_epoch_or_no_persisted_take_aborts_claim_moved() {
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
            f.until(&mut receiver, Step::Aborted).await;
            assert_eq!(f.row().reason.as_deref(), Some("claim_moved"));
            assert!(f.row().claim_landed_version.is_none());
            let move_reply = f.worker.records.read(&format!("move:{CUT}")).await.unwrap();
            assert_eq!(
                wire::decode_move_record(
                    &format!("move:{CUT}"),
                    move_reply.body.as_deref().unwrap()
                )
                .unwrap()
                .state,
                State::Aborted
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
        let f = Fixture::new(false).await;
        let mut handler = crate::ProjectsHandler::with_runtime("receiver-poll".into(), clock);
        handler.store = f.slot.clone();
        handler.participant_discovery = f.discovery.clone();
        handler.door_client = f.worker.door.clone();
        let connector = Arc::new(crate::move_driver::ReconnectConnector {
            inner: Arc::new(Connector(f.records.clone())),
            wake: handler.move_wake.clone(),
        });
        handler.record_client = RecordClient::new(connector.clone());
        handler.start_move_driver();
        f.records
            .inbox_reads
            .acquire_many(2)
            .await
            .unwrap()
            .forget();
        assert!(f.store().agent_move(CUT).unwrap().is_some());
        tokio::time::advance(Duration::from_secs(29)).await;
        assert_eq!(f.records.inbox_reads.available_permits(), 0);
        tokio::time::advance(Duration::from_secs(1)).await;
        f.records.inbox_reads.acquire().await.unwrap().forget();
        connector.connect().await.unwrap();
        f.records.inbox_reads.acquire().await.unwrap().forget();
        handler
            .move_driver_task
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .abort();
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
}

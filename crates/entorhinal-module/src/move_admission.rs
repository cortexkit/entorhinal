//! Check a move without side effects, then insert its durable checkpoint in a
//! transaction that prevents new session registrations from racing the move.

use super::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    claim_codec::AgentClaim,
    encode_result,
    participant_client::{self, AgentRef, PlannedParticipant},
    participant_discovery::discover_participants,
    HandlerError, ProjectsHandler, RouteKey,
};
use cortexkit_role_agent_data::busy::BusyState;
use entorhinal_core::{
    move_record::{MoveParticipant, MoveRef},
    move_store::{AgentMove, MoveSide, MoveStep},
    RegistryStore,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::Principal;
use tokio::time::{timeout_at, Instant};

pub(super) const MOVE: &str = "agent.move";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MoveRequest {
    agent_id: String,
    to: String,
    request_key: String,
}

fn reply(row: &AgentMove) -> Result<Vec<u8>, HandlerError> {
    encode_result(json!({"cut_id":row.cut_id, "step":row.step}))
}

impl ProjectsHandler {
    pub(super) async fn admit_move(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(MOVE, admission.flow_id.as_deref())?;
        let direct = admission.principal == Some(Principal::Direct);
        let engram = matches!(&admission.principal, Some(Principal::Reserved { module_id }) if module_id == "engram");
        if engram {
            if !self
                .with_store(RegistryStore::move_setting)?
                .lid_close_moves
            {
                return Err(HandlerError::new(
                    "caller_forbidden",
                    "lid-close moves are off",
                ));
            }
        } else if !direct && !self.move_orchestrator(&admission).await {
            return Err(HandlerError::new(
                "caller_forbidden",
                "caller cannot move agents",
            ));
        }
        let request: MoveRequest = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if request.request_key.is_empty() {
            return Err(HandlerError::new(
                "invalid_request",
                "request_key must be nonempty",
            ));
        }
        // The submitted target string, not its current resolved display name,
        // identifies a resend. A rename cannot redirect an admitted request.
        let request_body = json!({"agent_id":request.agent_id,"to":request.to});
        if let Some(bytes) = self.move_replay(&request.request_key, &request_body)? {
            return Ok(bytes);
        }
        if !self.with_store(RegistryStore::move_authority_cut_over)? {
            return Err(HandlerError::new(
                "authority_not_cut_over",
                "agent identity authority is not cut over",
            ));
        }
        if self.with_store(RegistryStore::identity_log_status)?.state != "enabled" {
            return Err(HandlerError::new(
                "identity_log_disabled",
                "moves require the identity log enabled",
            ));
        }
        let agent = self
            .with_store(|s| s.agent_row(&request.agent_id))?
            .ok_or_else(|| HandlerError::new("unknown_agent", "unknown agent"))?;
        if let Some(gone) = agent.gone() {
            return Err(HandlerError {
                code: "gone".into(),
                message: "agent is gone".into(),
                detail: Some(gone),
            });
        }
        let (mut claim_version, own) = self.move_claim(&request.agent_id).await?;
        let (peer, peer_name) = self.with_agent_store(|s| s.resolve_move_member(&request.to))?;
        if peer == own {
            return Err(HandlerError::new("invalid_request", "target is this Mac"));
        }
        if let Err(error) = self.move_open_check(&request.agent_id) {
            if error.code == "move_in_progress" {
                // Another resend can commit while claim refresh is awaiting its
                // record read. Replay that key before refusing the occupied slot.
                if let Some(bytes) = self.move_replay(&request.request_key, &request_body)? {
                    return Ok(bytes);
                }
            }
            return Err(error);
        }
        let refs = self.with_store(|s| s.agent_bound_refs(&request.agent_id))?;
        let pins = self.with_store(|s| s.catalog_pins(&request.agent_id))?;
        let pin_digest = if pins.is_empty() {
            None
        } else {
            Some(
                entorhinal_core::catalog_agent::pin_digest(&pins)
                    .map_err(|e| HandlerError::new("storage_error", e.to_string()))?,
            )
        };
        let mut available = discover_participants(self.participant_discovery.as_ref())
            .await
            .map_err(|e| HandlerError {
                code: e.code().into(),
                message: "participant unavailable".into(),
                detail: Some(e.detail),
            })?;
        if pin_digest.is_none() {
            available.retain(|p| p.module_id != "entorhinal");
        }
        let mut wire_refs: Vec<_> = refs
            .iter()
            .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
            .collect();
        if pin_digest.is_some() {
            wire_refs.push(AgentRef::new("package-closure", request.agent_id.clone()));
        }
        let plan = participant_client::plan_participants(&available, &wire_refs).await.map_err(|e| {
            let detail = match e {
                participant_client::PlanError::RefKindUnavailable(kind) => json!({"ref_kind":kind}),
                participant_client::PlanError::Participant { module_id, error } => json!({"module_id":module_id,"op":"agent_data.describe","error":format!("{error:?}")}),
            };
            HandlerError { code:"participant_unavailable".into(), message:"participant cannot cover move refs".into(), detail:Some(detail) }
        })?;
        self.move_idle(&refs, &plan).await?;
        let mut confirmation = None;
        if direct {
            let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
            if !self.route_admissions().contains_key(&key) {
                return Err(OperatorConfirmError::Declined.into());
            }
            let summary = format!(
                "move agent \"{}\" (\"{}\") to \"{}\"",
                agent.name, agent.agent_id, peer_name
            );
            tokio::select! {
                biased;
                _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
                result = timeout_at(received + super::shared_write::OPERATOR_BOUND, self.operator_confirmer.confirm_operator(&summary, key)) => {
                    result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
                }
            }
            if Instant::now() >= received + super::shared_write::OPERATOR_BOUND {
                return Err(OperatorConfirmError::Declined.into());
            }
            let recheck = async {
                let (version, refreshed_own) = self.move_claim(&request.agent_id).await?;
                if refreshed_own != own {
                    return Err(HandlerError::new(
                        "claim_integrity_failed",
                        "self author changed",
                    ));
                }
                self.move_open_check(&request.agent_id)?;
                self.move_idle(&refs, &plan).await?;
                Ok::<_, HandlerError>(version)
            };
            let rechecked = tokio::select! {
                biased;
                _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
                result = recheck => result,
            };
            claim_version = match rechecked {
                Ok(version) => version,
                Err(error) => {
                    if error.code == "move_in_progress" {
                        if let Some(bytes) =
                            self.move_replay(&request.request_key, &request_body)?
                        {
                            return Ok(bytes);
                        }
                    }
                    return Err(error);
                }
            };
            confirmation = Some(guard);
        }
        let participants: Vec<_> = plan
            .iter()
            .map(|p| MoveParticipant {
                module_id: p.client.module_id.clone(),
                refs: p
                    .refs
                    .iter()
                    .map(|r| MoveRef {
                        ref_kind: r.ref_kind.clone(),
                        reference: r.r#ref.clone(),
                    })
                    .collect(),
                external: vec![],
            })
            .collect();
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random)
            .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
        let now = (self.clock)();
        let mut move_plan = json!({"agent_id":agent.agent_id,"from":own,"to":peer,"claim_version":claim_version,"participants":participants});
        if let Some(digest) = pin_digest {
            move_plan["pin_digest"] = json!(digest);
        }
        let row = AgentMove {
            cut_id: super::log_client::encode_hex(&random),
            agent_id: request.agent_id,
            side: MoveSide::Source,
            peer: peer.clone(),
            step: MoveStep::Preparing,
            plan: move_plan,
            request_body: Some(request_body),
            record_version: 0,
            claim_version: Some(claim_version),
            claim_take_body: None,
            claim_take_version: None,
            claim_landed_version: None,
            seal_sent: false,
            abort_requested: None,
            hold: None,
            cut_accept: None,
            reason: None,
            detail: None,
            request_key: Some(request.request_key),
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: now,
            updated_at_ms: now,
        };
        if confirmation.as_ref().is_some_and(|g| *g.cancelled.borrow()) {
            return Err(OperatorConfirmError::Declined.into());
        }
        let row = self.with_agent_store(|s| s.admit_agent_move(&row, &refs))?;
        reply(&row)
    }

    fn move_replay(&self, key: &str, body: &Value) -> Result<Option<Vec<u8>>, HandlerError> {
        let Some(row) = self.with_store(|s| s.agent_move_by_request_key(key))? else {
            return Ok(None);
        };
        if row.request_body.as_ref() != Some(body) {
            return Err(HandlerError::new(
                "request_key_reused",
                "request key belongs to a different move request",
            ));
        }
        reply(&row).map(Some)
    }

    fn move_open_check(&self, agent_id: &str) -> Result<(), HandlerError> {
        let rows = self.with_store(RegistryStore::list_open_agent_moves)?;
        if let Some(row) = rows.iter().find(|row| row.agent_id == agent_id) {
            return Err(HandlerError {
                code: "move_in_progress".into(),
                message: "agent has an open move".into(),
                detail: Some(json!({"cut_id":row.cut_id,"step":row.step})),
            });
        }
        if rows.len() >= 4 {
            return Err(HandlerError::new("move_cap", "four moves are already open"));
        }
        Ok(())
    }

    async fn move_claim(&self, agent_id: &str) -> Result<(i64, String), HandlerError> {
        let refresh = self.wait_claim_refresh().await;
        let cached = self
            .with_store(|s| s.cached_agent_claim(agent_id))?
            .ok_or_else(|| HandlerError::new("claim_stale", "claim was not refreshed"))?;
        if cached.integrity_failed {
            return Err(HandlerError::new(
                "claim_integrity_failed",
                "claim has an integrity mark",
            ));
        }
        let counts =
            refresh.map_err(|_| HandlerError::new("claim_stale", "claim refresh failed"))?;
        if counts.failed != 0 || cached.stale {
            return Err(HandlerError::new("claim_stale", "claim cache is stale"));
        }
        let own = self
            .with_store(RegistryStore::persisted_self_author)?
            .ok_or_else(|| HandlerError::new("claim_stale", "self author is unknown"))?;
        if cached.epoch >= 1 && cached.holder.as_ref() != Some(&own) {
            let location = self.with_agent_store(|s| s.agent_where(agent_id))?;
            return Err(HandlerError {
                code: "not_holder".into(),
                message: "claim is held elsewhere".into(),
                detail: Some(json!({"holder":location.holder,"holder_name":location.holder_name})),
            });
        }
        let read = self
            .record_client
            .read(&format!("claim:{agent_id}"))
            .await
            .map_err(|_| HandlerError::new("claim_stale", "claim read failed"))?;
        if read.stale || read.self_author != own {
            return Err(HandlerError::new(
                "claim_stale",
                "claim read disagrees with cache",
            ));
        }
        let claim = if read.absent {
            None
        } else {
            Some(
                AgentClaim::decode(
                    agent_id,
                    read.body
                        .as_deref()
                        .ok_or_else(|| HandlerError::new("claim_stale", "claim body missing"))?,
                )
                .map_err(|_| {
                    HandlerError::new("claim_integrity_failed", "claim body is invalid")
                })?,
            )
        };
        let agrees = if cached.epoch == 0 {
            claim.as_ref().is_none_or(|c| c.epoch == 0)
        } else {
            claim.as_ref().is_some_and(|c| {
                c.epoch == cached.epoch && c.holder == cached.holder && c.cut_id == cached.cut_id
            })
        };
        if !agrees {
            return Err(HandlerError::new(
                "claim_stale",
                "claim read disagrees with cache",
            ));
        }
        let version = i64::try_from(read.version).map_err(|_| {
            HandlerError::new(
                "claim_integrity_failed",
                "claim version exceeds local range",
            )
        })?;
        Ok((version, own))
    }
}

impl ProjectsHandler {
    async fn move_idle(
        &self,
        refs: &[MoveRef],
        plan: &[PlannedParticipant],
    ) -> Result<(), HandlerError> {
        let refs_for_liveness: Vec<_> = refs
            .iter()
            .map(|r| (r.ref_kind.as_str(), r.reference.as_str()))
            .collect();
        let answers = self.read_session_liveness(&refs_for_liveness)?;
        let mut busy = Vec::new();
        for (reference, answer) in refs.iter().zip(answers) {
            if answer.state != "gone" {
                busy.push(json!({"ref_kind":reference.ref_kind,"ref":reference.reference,"participant":"agent.session_liveness",
                    "reason_code":if answer.state == "active" {"session_active"} else {"liveness_unknown"}, "reason":answer.cause}));
            }
        }
        for participant in plan {
            match participant.client.busy(&participant.refs).await {
                Ok(answer) if answer.refs.len() == participant.refs.len() => {
                    for (reference, verdict) in participant.refs.iter().zip(answer.refs) {
                        if verdict.state != BusyState::Idle {
                            busy.push(json!({"ref_kind":reference.ref_kind,"ref":reference.r#ref,"participant":participant.client.module_id,
                                "reason_code":verdict.reason_code,"reason":verdict.reason}));
                        }
                    }
                }
                answer => {
                    for reference in &participant.refs {
                        busy.push(json!({"ref_kind":reference.ref_kind,"ref":reference.r#ref,"participant":participant.client.module_id,
                            "reason_code":"busy_unknown","reason":format!("{answer:?}")}));
                    }
                    if participant.refs.is_empty() {
                        busy.push(json!({"participant":participant.client.module_id,"reason_code":"busy_unknown","reason":format!("{answer:?}")}));
                    }
                }
            }
        }
        if busy.is_empty() {
            Ok(())
        } else {
            Err(HandlerError {
                code: "agent_busy".into(),
                message: "agent is not idle".into(),
                detail: Some(json!({"refs":busy})),
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        agent_ops::{OperatorConfirmError, OperatorConfirmer},
        fake_daemon::{DaemonSnapshot, FakeDaemon},
        fake_log::FakeLog,
        fake_participant::FakeParticipant,
        fake_record::FakeRecord,
        log_client::{LogConnector, LogTransport, TransportError},
        participant_client::{BusyRef, Request},
        participant_discovery::{provider_role, ParticipantDiscovery},
        record_client::RecordClient,
        sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
        tests::{flow_stamp, reserved, scratch_descriptor},
        RouteAdmission,
    };
    use super::*;
    use async_trait::async_trait;
    use std::sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    };
    use subc_client_rs::{CatalogList, HandlerOutcome, SupervisorListResponse};
    const DIRECT: RouteKey = (80, 1);
    const ORCH: RouteKey = (81, 1);
    const ENGRAM: RouteKey = (82, 1);
    const OTHER: RouteKey = (83, 1);
    const TARGET: &str = "02020202020202020202020202020202";

    struct Prompt {
        summaries: Mutex<Vec<String>>,
        outcome: Mutex<Result<(), OperatorConfirmError>>,
        on_confirm: Mutex<Option<Box<dyn FnOnce() + Send>>>,
    }
    #[async_trait]
    impl OperatorConfirmer for Prompt {
        async fn confirm_operator(
            &self,
            summary: &str,
            _: RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.summaries.lock().unwrap().push(summary.into());
            if let Some(hook) = self.on_confirm.lock().unwrap().take() {
                hook();
            }
            *self.outcome.lock().unwrap()
        }
    }
    struct AuditedRecords {
        record: Arc<FakeRecord>,
        calls: Mutex<Vec<(String, Value)>>,
    }
    struct RecordConnector(Arc<AuditedRecords>);
    #[async_trait]
    impl LogConnector for RecordConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for AuditedRecords {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            self.record.call(method, params).await
        }
    }
    struct Discovery {
        daemon: FakeDaemon,
        participant: Arc<FakeParticipant>,
        gate: AtomicBool,
        entered: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
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
            module_id: &str,
        ) -> Result<Arc<dyn LogTransport>, TransportError> {
            self.daemon.open_participant(module_id).await?;
            Ok(self.participant.clone())
        }
    }
    // The gate sits at the participant call, not at a fake commit. Both racing
    // operations still exercise their production SQLite transactions.
    struct GatedDiscovery(Arc<Discovery>);
    struct GatedParticipant(Arc<Discovery>);
    #[async_trait]
    impl ParticipantDiscovery for GatedDiscovery {
        async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
            self.0.supervisor_list().await
        }
        async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
            self.0.catalog_list().await
        }
        async fn open_participant(
            &self,
            module_id: &str,
        ) -> Result<Arc<dyn LogTransport>, TransportError> {
            self.0.daemon.open_participant(module_id).await?;
            Ok(Arc::new(GatedParticipant(self.0.clone())))
        }
    }
    #[async_trait]
    impl LogTransport for GatedParticipant {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            if method == "agent_data.busy" && self.0.gate.load(Ordering::Relaxed) {
                self.0.entered.add_permits(1);
                self.0.release.acquire().await.unwrap().forget();
            }
            self.0.participant.call(method, params).await
        }
    }
    fn snapshot() -> DaemonSnapshot {
        DaemonSnapshot::new(
            1,
            vec![
                json!({"module_id":"data","state":"running","enabled":true,"live":true,"health":"ok"}),
            ],
            vec![
                json!({"module_id":"data","ready":true,"not_ready":null,"control_ops":[],"roles":[provider_role()],
                "capabilities":{"provides":["agent-data/v1"],"requires":[],"must_never_reach":[]}}),
            ],
        )
    }
    struct Fixture {
        handler: Arc<ProjectsHandler>,
        records: Arc<AuditedRecords>,
        discovery: Arc<Discovery>,
        prompt: Arc<Prompt>,
        resolver: Arc<FakeResolver>,
        descriptor: cortexkit_store_types::StorageDescriptor,
        dir: std::path::PathBuf,
    }
    impl Fixture {
        async fn new(stage: u8) -> Self {
            let (dir, descriptor) = scratch_descriptor("move-admission");
            let record = FakeRecord::new([1; 16]);
            let records = Arc::new(AuditedRecords {
                record,
                calls: Mutex::new(vec![]),
            });
            let discovery = Arc::new(Discovery {
                daemon: FakeDaemon::new(snapshot()),
                participant: Arc::new(FakeParticipant::new(
                    &["session"],
                    vec![AgentRef::new("session", "r")],
                )),
                gate: AtomicBool::new(false),
                entered: tokio::sync::Semaphore::new(0),
                release: tokio::sync::Semaphore::new(0),
            });
            let prompt = Arc::new(Prompt {
                summaries: Mutex::new(vec![]),
                outcome: Mutex::new(Ok(())),
                on_confirm: Mutex::new(None),
            });
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider("prefrontal-core".into()),
            );
            let mut handler = ProjectsHandler::with_log_connector(
                "0123456789abcdef".into(),
                || 700,
                Arc::new(FakeLog::default().with_author([1; 16])),
            );
            handler.record_client = RecordClient::new(Arc::new(RecordConnector(records.clone())));
            handler.participant_discovery = Arc::new(GatedDiscovery(discovery.clone()));
            handler.operator_confirmer = prompt.clone();
            handler.provider_resolver = resolver.clone();
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            for (key, principal) in [
                (DIRECT, Principal::Direct),
                (ORCH, reserved("prefrontal-core")),
                (ENGRAM, reserved("engram")),
                (OTHER, reserved("other")),
            ] {
                handler
                    .route_admissions()
                    .insert(key, RouteAdmission::from_bind(Some(principal), None));
            }
            let f = Self {
                handler: Arc::new(handler),
                records,
                discovery,
                prompt,
                resolver,
                descriptor,
                dir,
            };
            if stage >= 1 {
                let mut source = f.descriptor.clone();
                source.backend = cortexkit_store_types::StorageBackend::Sqlite {
                    path: f.dir.join("snapshot.db").to_string_lossy().into_owned(),
                };
                source.storage_namespace.push_str("-source");
                let store = RegistryStore::open(&source).unwrap();
                store
                    .apply_entry("fixture", "{}", "fixture", None, |tx| {
                        tx.execute_batch("DROP TABLE agent_name_claim; DROP TABLE agent;")
                    })
                    .unwrap();
                drop(store);
                result(f.handler.handle_request(
                    &body(
                        "agent.import",
                        json!({"snapshot_path":f.dir.join("snapshot.db"),"request_key":"cutover"}),
                    ),
                    ORCH,
                ));
            }
            if stage >= 2 {
                result(f.call(DIRECT, "identity_log.enable", json!({})).await);
            }
            f.store()
                .set_member_name(
                    entorhinal_core::shared_entry::MemberName {
                        author: TARGET.into(),
                        display_name: "Studio Mac".into(),
                    },
                    "fixture",
                )
                .unwrap();
            f
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        async fn call(&self, key: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(&body(method, params), key)
                .await
        }
        async fn create(&self) -> String {
            result(
                self.call(
                    ORCH,
                    "agent.create",
                    json!({"name":"ENTO","role":"assistant","tag":"test","request_key":"create"}),
                )
                .await,
            )["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }
        async fn move_(&self, key: RouteKey, id: &str) -> HandlerOutcome {
            self.call(
                key,
                MOVE,
                json!({"agent_id":id,"to":"Studio Mac","request_key":"move"}),
            )
            .await
        }
        fn register(
            &self,
            id: &str,
            reference: &str,
        ) -> Result<entorhinal_core::SessionBinding, entorhinal_core::agent::AgentMutationError>
        {
            self.store()
                .register_session(&entorhinal_core::SessionRegistration {
                    agent_id: id,
                    ref_kind: "session",
                    session_ref: reference,
                    harness: "host",
                    session_id: reference,
                    project_root: "/project",
                    registered_by: "host",
                    now_ms: 700,
                })
        }
        fn clear_calls(&self) {
            self.records.calls.lock().unwrap().clear();
        }
        async fn gate_entered(&self, n: u32) {
            self.discovery
                .entered
                .acquire_many(n)
                .await
                .unwrap()
                .forget();
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
    }
    fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(bytes) => {
                serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
            }
            other => panic!("expected response: {other:?}"),
        }
    }
    #[tokio::test]
    async fn served_arm_callers_confirmation_summary_and_writer_isolation() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        assert!(super::super::MUTATING_METHODS.contains(&MOVE));
        assert!(super::super::SELF_ADMITTED_METHODS.contains(&MOVE));
        assert!(!super::super::agent_operation_description(MOVE).is_empty());
        assert_eq!(
            code(f.handler.handle_request(&body(MOVE, json!({})), DIRECT)),
            "served_path_required"
        );
        f.handler.route_admissions().insert(
            OTHER,
            RouteAdmission::from_bind(Some(Principal::Direct), Some(&flow_stamp(Some("flow")))),
        );
        assert_eq!(code(f.move_(OTHER, &id).await), "flow_scope_not_admitted");
        f.handler.route_admissions().insert(
            OTHER,
            RouteAdmission::from_bind(Some(reserved("other")), None),
        );
        assert_eq!(code(f.move_(OTHER, &id).await), "caller_forbidden");
        assert_eq!(code(f.move_(ENGRAM, &id).await), "caller_forbidden");
        *f.prompt.outcome.lock().unwrap() = Err(OperatorConfirmError::PresenceUnavailable);
        assert_eq!(
            code(f.move_(DIRECT, &id).await),
            "operator_presence_unavailable"
        );
        assert_eq!(
            *f.prompt.summaries.lock().unwrap(),
            vec![format!("move agent \"ENTO\" (\"{id}\") to \"Studio Mac\"")]
        );
        *f.prompt.outcome.lock().unwrap() = Ok(());
        let generation = f.store().generation().unwrap();
        let writer = f.handler.writer.lock().await;
        let moved = result(
            tokio::time::timeout(std::time::Duration::from_secs(5), f.move_(DIRECT, &id))
                .await
                .expect("move must not acquire writer"),
        );
        drop(writer);
        assert_eq!(moved["step"], "preparing");
        assert_eq!(f.store().generation().unwrap(), generation);
        assert_eq!(
            code(
                f.register(&id, "new")
                    .map(|_| HandlerOutcome::Response(vec![]))
                    .unwrap_or_else(|e| HandlerError::from(e).into_outcome())
            ),
            "agent_moving"
        );
    }

    #[tokio::test]
    async fn engram_requires_setting_and_idle_while_orchestrator_never_prompts() {
        for caller in [ENGRAM, ORCH] {
            let f = Fixture::new(2).await;
            let id = f.create().await;
            f.store()
                .set_move_setting(entorhinal_core::move_store::MoveSetting {
                    lid_close_moves: true,
                })
                .unwrap();
            result(f.move_(caller, &id).await);
            assert!(f.prompt.summaries.lock().unwrap().is_empty());
            if caller == ORCH {
                assert_eq!(f.resolver.calls(), 1);
            }
        }
        let f = Fixture::new(2).await;
        let id = f.create().await;
        f.register(&id, "r").unwrap();
        f.store()
            .set_move_setting(entorhinal_core::move_store::MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        assert_eq!(code(f.move_(ENGRAM, &id).await), "agent_busy");
        assert!(f.store().list_agent_moves().unwrap().is_empty());
    }

    #[tokio::test]
    async fn busy_refusal_is_read_only_and_names_liveness_and_participant_reasons() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        f.register(&id, "r").unwrap();
        let reference = AgentRef::new("session", "r");
        f.discovery.participant.set_busy(
            reference.clone(),
            BusyRef::new(BusyState::Busy)
                .with_reason_code("writing")
                .with_reason("pending writes"),
        );
        let before = f.discovery.participant.held();
        f.clear_calls();
        let generation = f.store().generation().unwrap();
        let outcome = f.move_(ORCH, &id).await;
        match outcome {
            HandlerOutcome::ErrorWithDetail { code, detail, .. } => {
                assert_eq!(code, "agent_busy");
                assert_eq!(detail["refs"][0]["reason_code"], "liveness_unknown");
                assert_eq!(detail["refs"][1]["reason_code"], "writing");
            }
            other => panic!("expected detailed busy refusal: {other:?}"),
        }
        assert!(f.store().list_agent_moves().unwrap().is_empty());
        assert_eq!(f.store().generation().unwrap(), generation);
        assert_eq!(f.discovery.participant.held(), before);
        assert!(f.discovery.participant.frozen().is_empty());
        assert!(f.discovery.participant.staged().is_empty());
        assert!(f.discovery.participant.tombstones().is_empty());
        assert_eq!(
            f.discovery.participant.calls(),
            vec![
                Request::Describe(participant_client::DescribeRequest::new()),
                Request::Busy(participant_client::BusyRequest::new(
                    cortexkit_role_agent_data::refs::Refs::new(vec![reference]).unwrap(),
                ))
            ]
        );
        let calls = f.records.calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].0, "record.read_many");
        assert_eq!(
            calls[1],
            (
                "record.read".into(),
                json!({"subject":format!("claim:{id}")})
            )
        );
        assert!(calls
            .iter()
            .all(|(method, params)| ["record.read", "record.read_many"]
                .contains(&method.as_str())
                && !params.to_string().contains("move:")
                && !params.to_string().contains("inbox:")));
    }

    #[tokio::test]
    async fn preconditions_refuse_in_order_with_named_codes() {
        let f = Fixture::new(0).await;
        assert_eq!(
            code(f.move_(DIRECT, "unknown").await),
            "authority_not_cut_over"
        );
        assert!(f.records.calls.lock().unwrap().is_empty());
        let f = Fixture::new(1).await;
        assert_eq!(
            code(f.move_(DIRECT, "unknown").await),
            "identity_log_disabled"
        );
        assert!(f.records.calls.lock().unwrap().is_empty());
        let f = Fixture::new(2).await;
        f.clear_calls();
        assert_eq!(code(f.move_(ORCH, "unknown").await), "unknown_agent");
        assert!(f.records.calls.lock().unwrap().is_empty());
        let id = f.create().await;
        f.records
            .record
            .error(&format!("claim:{id}"), Some("verify_failed"));
        assert_eq!(code(f.move_(ORCH, &id).await), "claim_integrity_failed");
        f.records
            .record
            .error(&format!("claim:{id}"), Some("unavailable"));
        assert_eq!(
            code(f.move_(ORCH, &id).await),
            "claim_integrity_failed",
            "unavailable cannot erase integrity mark"
        );
        f.records.record.error(&format!("claim:{id}"), None);
        f.handler.wait_claim_refresh().await.unwrap();
        f.records.record.offline.store(true, Ordering::Relaxed);
        assert_eq!(code(f.move_(ORCH, &id).await), "claim_stale");
        f.records.record.offline.store(false, Ordering::Relaxed);
        f.records.record.put(
            &format!("claim:{id}"),
            1,
            serde_json::to_vec(
                &json!({"v":1,"agent_id":id,"holder":TARGET,"epoch":1,"cut_id":null,"reason":null}),
            )
            .unwrap(),
            TARGET,
        );
        assert_eq!(code(f.move_(ORCH, &id).await), "not_holder");
        f.records.record.put(
            &format!("claim:{id}"),
            2,
            serde_json::to_vec(
                &json!({"v":1,"agent_id":id,"holder":null,"epoch":0,"cut_id":null,"reason":null}),
            )
            .unwrap(),
            TARGET,
        );
        assert_eq!(
            code(
                f.call(
                    ORCH,
                    MOVE,
                    json!({"agent_id":id,"to":"no row","request_key":"move"})
                )
                .await
            ),
            "unknown_member"
        );
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: f.records.record.author.clone(),
                    display_name: "Self".into(),
                },
                "fixture",
            )
            .unwrap();
        assert_eq!(
            code(
                f.call(
                    ORCH,
                    MOVE,
                    json!({"agent_id":id,"to":"Self","request_key":"move"})
                )
                .await
            ),
            "invalid_request"
        );
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: "03030303030303030303030303030303".into(),
                    display_name: "Studio Mac".into(),
                },
                "fixture",
            )
            .unwrap();
        assert_eq!(code(f.move_(ORCH, &id).await), "invalid_request");
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: "03030303030303030303030303030303".into(),
                    display_name: "Other Mac".into(),
                },
                "fixture",
            )
            .unwrap();
        f.discovery.daemon.restart(DaemonSnapshot::new(2, vec![json!({"module_id":"down","state":"stopped","enabled":true,"live":false,"health":"ok"})],vec![]));
        assert_eq!(code(f.move_(ORCH, &id).await), "participant_unavailable");
        f.discovery.daemon.restart(snapshot());
        f.store()
            .apply_entry("fixture", "{}", "fixture", None, |tx| {
                tx.execute(
                    "INSERT INTO session_binding VALUES ('uncovered','r',?1,NULL,NULL,'','move',1)",
                    [&id],
                )?;
                Ok(())
            })
            .unwrap();
        assert_eq!(code(f.move_(ORCH, &id).await), "participant_unavailable");
        f.store()
            .apply_entry("fixture", "{}", "fixture", None, |tx| {
                tx.execute("DELETE FROM session_binding WHERE agent_id=?1", [&id])?;
                Ok(())
            })
            .unwrap();
        let admitted = result(f.move_(ORCH, &id).await);
        assert_eq!(
            f.store()
                .agent_move(admitted["cut_id"].as_str().unwrap())
                .unwrap()
                .unwrap()
                .claim_version,
            Some(2),
            "epoch-zero cache version is not the raw record version"
        );
        assert_eq!(
            code(
                f.call(
                    ORCH,
                    MOVE,
                    json!({"agent_id":id,"to":"Studio Mac","request_key":"other"})
                )
                .await
            ),
            "move_in_progress"
        );
        let f = Fixture::new(2).await;
        let id = f.create().await;
        result(
            f.call(
                ORCH,
                "agent.dispose",
                json!({"agent_id":id,"request_key":"dispose"}),
            )
            .await,
        );
        assert_eq!(code(f.move_(ORCH, &id).await), "gone");
    }

    #[tokio::test]
    async fn direct_rechecks_claim_open_moves_and_idle_after_confirmation() {
        for check in [4, 6, 8] {
            let f = Fixture::new(2).await;
            let id = f.create().await;
            if check == 8 {
                f.store().apply_entry("fixture","{}","fixture",None,|tx| {tx.execute("INSERT INTO session_binding VALUES ('session','r',?1,NULL,NULL,'','move',1)",[&id])?;Ok(())}).unwrap();
            }
            let store = f.store();
            let record = f.records.record.clone();
            let id_hook = id.clone();
            let participant = f.discovery.participant.clone();
            *f.prompt.on_confirm.lock().unwrap() = Some(Box::new(move || {
                match check {
                    4 => record.put(&format!("claim:{id_hook}"),1,serde_json::to_vec(&json!({"v":1,"agent_id":id_hook,"holder":TARGET,"epoch":1,"cut_id":null,"reason":null})).unwrap(),TARGET),
                    6 => {
                        let row = pending_row(&id_hook,"other");
                        store.insert_agent_move(&row).unwrap();
                    }
                    _ => {
                        participant.set_busy(
                            AgentRef::new("session", "r"),
                            BusyRef::new(BusyState::Unknown).with_reason_code("unknown"),
                        );
                    }
                }
            }));
            assert_eq!(
                code(f.move_(DIRECT, &id).await),
                match check {
                    4 => "not_holder",
                    6 => "move_in_progress",
                    _ => "agent_busy",
                }
            );
            assert_eq!(
                f.records
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|(m, _)| m == "record.read_many")
                    .count(),
                2
            );
        }
    }

    fn pending_row(id: &str, key: &str) -> AgentMove {
        AgentMove {
            cut_id: format!("{key:0<32}"),
            agent_id: id.into(),
            side: MoveSide::Receiver,
            peer: TARGET.into(),
            step: MoveStep::PickedUp,
            plan: json!({"participants":[]}),
            request_body: None,
            record_version: 1,
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
            request_key: None,
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 700,
            updated_at_ms: 700,
        }
    }

    #[tokio::test]
    async fn registration_races_real_admission_insert_from_both_orders() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        f.discovery.gate.store(true, Ordering::Relaxed);
        let handler = f.handler.clone();
        let request = body(
            MOVE,
            json!({"agent_id":id,"to":"Studio Mac","request_key":"move"}),
        );
        let task = tokio::spawn(async move { handler.handle_served_request(&request, ORCH).await });
        f.gate_entered(1).await;
        f.register(&id, "started-during-check").unwrap();
        f.discovery.release.add_permits(1);
        assert_eq!(code(task.await.unwrap()), "agent_busy");
        assert!(f.store().list_agent_moves().unwrap().is_empty());

        let f = Fixture::new(2).await;
        let id = f.create().await;
        result(f.move_(ORCH, &id).await);
        assert_eq!(
            f.register(&id, "started-after-check").unwrap_err().code,
            "agent_moving"
        );
        assert!(f.store().agent_bound_refs(&id).unwrap().is_empty());
    }

    #[tokio::test]
    async fn crash_before_commit_leaves_nothing_and_same_key_concurrent_resends_share_checkpoint() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        f.discovery.gate.store(true, Ordering::Relaxed);
        let request = body(
            MOVE,
            json!({"agent_id":id,"to":"Studio Mac","request_key":"move"}),
        );
        let handler = f.handler.clone();
        let request_for_task = request.clone();
        let task =
            tokio::spawn(
                async move { handler.handle_served_request(&request_for_task, ORCH).await },
            );
        f.gate_entered(1).await;
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(f.store().list_agent_moves().unwrap().is_empty());
        assert!(f
            .discovery
            .participant
            .calls()
            .iter()
            .all(|c| matches!(c, Request::Describe(_))));
        let mut tasks = vec![];
        for _ in 0..2 {
            let handler = f.handler.clone();
            let request = request.clone();
            tasks.push(tokio::spawn(async move {
                handler.handle_served_request(&request, ORCH).await
            }));
        }
        f.gate_entered(2).await;
        f.discovery.release.add_permits(2);
        let first = result(tasks.remove(0).await.unwrap());
        let second = result(tasks.remove(0).await.unwrap());
        assert_eq!(first, second);
        f.handler.stop_move_driver().await;
        assert_eq!(f.store().list_agent_moves().unwrap().len(), 1);
        let checkpoint = f
            .store()
            .agent_move(first["cut_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        *f.handler.store.lock().unwrap() = None;
        let restarted = RegistryStore::open(&f.descriptor).unwrap();
        assert_eq!(restarted.list_open_agent_moves().unwrap(), vec![checkpoint]);
        *f.handler.store.lock().unwrap() = Some(restarted);
        f.handler.start_move_driver();
        f.records.record.offline.store(true, Ordering::Relaxed);
        assert_eq!(
            result(f.handler.handle_served_request(&request, ORCH).await),
            first,
            "committed resend skips admission even after restart"
        );
        f.handler.stop_move_driver().await;
    }

    #[tokio::test]
    async fn request_body_is_submitted_target_and_survives_target_rename() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        let first = result(f.move_(ORCH, &id).await);
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: TARGET.into(),
                    display_name: "Renamed Mac".into(),
                },
                "fixture",
            )
            .unwrap();
        let calls = f.records.calls.lock().unwrap().len();
        assert_eq!(result(f.move_(ORCH, &id).await), first);
        assert_eq!(f.records.calls.lock().unwrap().len(), calls);
        assert_eq!(
            code(
                f.call(
                    ORCH,
                    MOVE,
                    json!({"agent_id":id,"to":"Renamed Mac","request_key":"move"})
                )
                .await
            ),
            "request_key_reused"
        );
        let f = Fixture::new(2).await;
        let id = f.create().await;
        *f.prompt.outcome.lock().unwrap() = Err(OperatorConfirmError::Declined);
        assert_eq!(code(f.move_(DIRECT, &id).await), "operator_declined");
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: TARGET.into(),
                    display_name: "Renamed Mac".into(),
                },
                "fixture",
            )
            .unwrap();
        assert_eq!(
            code(f.move_(DIRECT, &id).await),
            "unknown_member",
            "uncommitted resends re-run target resolution"
        );
        assert!(f.store().list_agent_moves().unwrap().is_empty());
    }

    #[tokio::test]
    async fn open_cap_precedes_discovery_and_receiver_registration_fence_is_transactional() {
        let f = Fixture::new(2).await;
        let id = f.create().await;
        for n in 0..4 {
            f.store()
                .insert_agent_move(&pending_row(&format!("other-{n}"), &format!("cut-{n}")))
                .unwrap();
        }
        assert_eq!(code(f.move_(ORCH, &id).await), "move_cap");
        assert!(f.discovery.daemon.calls().is_empty());
        let f = Fixture::new(2).await;
        let id = f.create().await;
        let mut row = pending_row(&id, "cut");
        row.plan = json!({"participants":[{"module_id":"data","refs":[{"ref_kind":"session","ref":"reserved"}]}]});
        f.store().insert_agent_move(&row).unwrap();
        for step in [
            MoveStep::PickedUp,
            MoveStep::Imported,
            MoveStep::Staged,
            MoveStep::Claimed,
            MoveStep::Activating,
        ] {
            row.step = step;
            f.store().update_agent_move(&row).unwrap();
            assert_eq!(f.register(&id, "r").unwrap_err().code, "agent_moving");
        }
        let other_id = result(f.call(ORCH,"agent.create",json!({"name":"OTHER","role":"assistant","tag":"test","request_key":"create-other"})).await)["agent"]["agent_id"].as_str().unwrap().to_owned();
        assert_eq!(
            f.register(&other_id, "reserved").unwrap_err().code,
            "agent_moving"
        );
        row.step = MoveStep::Activated;
        f.store().update_agent_move(&row).unwrap();
        f.register(&id, "r").unwrap();
    }

    fn code(outcome: HandlerOutcome) -> String {
        match outcome {
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                code
            }
            other => panic!("expected refusal: {other:?}"),
        }
    }
}

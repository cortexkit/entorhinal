//! Check a move without side effects, then insert its durable checkpoint in a
//! transaction that prevents new session registrations from racing the move.

use super::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    encode_result,
    participant_client::{self, AgentRef, PlannedParticipant},
    participant_discovery::discover_participants,
    sessions::MoveLiveness,
    HandlerError, ProjectsHandler, RouteKey,
};
use cortexkit_role_agent_data::busy::BusyState;
use entorhinal_core::{
    move_record::{MoveParticipant, MoveRef},
    move_store::{AgentMove, MoveSide, MoveStep},
    session_carry::{PACKAGE_CLOSURE_KIND, SESSION_BINDINGS_KIND},
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
                .move_sql(received, |handler| {
                    handler.with_store(RegistryStore::move_setting)
                })
                .await?
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
        if let Some(bytes) = self
            .move_replay(received, &request.request_key, &request_body)
            .await?
        {
            return Ok(bytes);
        }
        let agent_id = request.agent_id.clone();
        let agent = self
            .move_sql(received, move |handler| {
                if !handler.with_store(RegistryStore::move_authority_cut_over)? {
                    return Err(HandlerError::new(
                        "authority_not_cut_over",
                        "agent identity authority is not cut over",
                    ));
                }
                if handler
                    .with_store(RegistryStore::identity_log_status)?
                    .state
                    != "enabled"
                {
                    return Err(HandlerError::new(
                        "identity_log_disabled",
                        "moves require the identity log enabled",
                    ));
                }
                handler
                    .with_store(|s| s.agent_row(&agent_id))?
                    .ok_or_else(|| HandlerError::new("unknown_agent", "unknown agent"))
            })
            .await?;
        if let Some(gone) = agent.gone() {
            return Err(HandlerError {
                code: "gone".into(),
                message: "agent is gone".into(),
                detail: Some(gone),
            });
        }
        if !self.hub_backend_selected() {
            return Err(HandlerError::new(
                "hub_not_ready",
                "hub move backend is not selected",
            ));
        }
        let hub = self
            .residence()
            .ok_or_else(|| HandlerError::new("hub_not_ready", "hub move watch is not installed"))?;
        let (mut claim_version, own) = hub.move_origin(&request.agent_id).await?;
        let to = request.to.clone();
        let (peer, peer_name) = match self
            .move_sql(received, move |handler| {
                handler.with_agent_store(|s| s.resolve_move_member(&to))
            })
            .await
        {
            Err(error)
                if error.code == "unknown_member" && crate::hub_membership::box_id(&request.to) =>
            {
                (request.to.clone(), request.to.clone())
            }
            result => result?,
        };
        if hub.move_member(&peer).await?.is_none() {
            return Err(HandlerError::new(
                "unknown_member",
                "target is not a current fleet member",
            ));
        }
        let target = hub.watched_node(&peer).await?;
        if peer == own {
            return Err(HandlerError::new(
                "invalid_request",
                "target is this machine",
            ));
        }
        if let Err(error) = self.move_open_check(received, &request.agent_id).await {
            if error.code == "move_in_progress" {
                // Another resend can commit while the hub reads residence.
                // Replay that key before refusing the occupied slot.
                if let Some(bytes) = self
                    .move_replay(received, &request.request_key, &request_body)
                    .await?
                {
                    return Ok(bytes);
                }
            }
            return Err(error);
        }
        let agent_id = request.agent_id.clone();
        let refs = self
            .move_sql(received, move |handler| {
                handler.with_store(|s| s.agent_bound_refs(&agent_id))
            })
            .await?;
        // Only live session refs count toward the cap; synthetic refs, including
        // the agent ref, do not consume a session slot.
        if refs.len() > 256 {
            return Err(HandlerError {
                code: "live_ref_cap".into(),
                message: "too many live session references".into(),
                detail: Some(json!({"live_refs":refs.len(),"cap":256})),
            });
        }
        let agent_id = request.agent_id.clone();
        let pins = self
            .move_sql(received, move |handler| {
                handler.with_store(|s| s.catalog_pins(&agent_id))
            })
            .await?;
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
        // Entorhinal's own participant carries the agent's session bindings,
        // with their network history, in a stream of its own. Every enabled
        // entorhinal serves that participant, and discovery refuses the move
        // when a participant is unready, so outside test fixtures that leave
        // entorhinal out of discovery every agent with bindings carries them.
        let agent_id = request.agent_id.clone();
        let carry_bindings = self
            .move_sql(received, move |handler| {
                handler.with_store(|s| s.agent_has_bindings(&agent_id))
            })
            .await?
            && available.iter().any(|p| p.module_id == "entorhinal");
        // With no pins and no bindings entorhinal has nothing to send and
        // stays out of the move, exactly as before bindings were carried.
        if pin_digest.is_none() && !carry_bindings {
            available.retain(|p| p.module_id != "entorhinal");
        }
        let mut wire_refs: Vec<_> = refs
            .iter()
            .map(|r| AgentRef::new(r.ref_kind.clone(), r.reference.clone()))
            .collect();
        if pin_digest.is_some() {
            wire_refs.push(AgentRef::new(
                PACKAGE_CLOSURE_KIND,
                request.agent_id.clone(),
            ));
        }
        if carry_bindings {
            wire_refs.push(AgentRef::new(
                SESSION_BINDINGS_KIND,
                request.agent_id.clone(),
            ));
        }
        let plan_error = |e| {
            let detail = match e {
                participant_client::PlanError::RefKindUnavailable(kind) => json!({"ref_kind":kind}),
                participant_client::PlanError::Participant { module_id, error } => {
                    json!({"module_id":module_id,"op":"agent_data.describe","error":format!("{error:?}")})
                }
            };
            HandlerError {
                code: "participant_unavailable".into(),
                message: "participant cannot cover move refs".into(),
                detail: Some(detail),
            }
        };
        let described = participant_client::describe_participants(&available)
            .await
            .map_err(plan_error)?;
        // Agent-wide data has no session binding. Add its ref only when a
        // participant can carry it, so older participant sets stay unchanged.
        let carries_agent = described
            .iter()
            .any(|p| p.description.ref_kinds.iter().any(|kind| kind == "agent"));
        if carries_agent {
            wire_refs.push(AgentRef::new("agent", request.agent_id.clone()));
        }
        let plan =
            participant_client::plan_participants(described, &wire_refs).map_err(plan_error)?;
        // The wire contract caps each participant's fixed ref set separately.
        // Check after routing but before busy or any state-changing call, so
        // an oversized set is not misreported as a busy participant.
        if let Some(participant) = plan.iter().max_by_key(|p| p.refs.len()) {
            if participant.refs.len() > 256 {
                return Err(HandlerError {
                    code: "live_ref_cap".into(),
                    message: "too many participant references".into(),
                    detail: Some(
                        json!({"live_refs":refs.len(),"cap":256,"module_id":participant.client.module_id,"participant_refs":participant.refs.len()}),
                    ),
                });
            }
        }
        self.move_idle(received, &refs, &plan).await?;
        let mut confirmation = None;
        if direct {
            let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
            if !self.route_admissions().contains_key(&key) {
                return Err(OperatorConfirmError::Declined.into());
            }
            let mut summary = format!(
                "move agent \"{}\" (\"{}\") to \"{}\"",
                agent.name, agent.agent_id, peer_name
            );
            if let Some(target) = &target {
                for participant in plan.iter().filter(|p| p.description.optional) {
                    let module = &participant.client.module_id;
                    if !target.participants.iter().any(|p| {
                        &p.module == module
                            && participant
                                .refs
                                .iter()
                                .all(|r| p.ref_kinds.contains(&r.ref_kind))
                    }) {
                        summary.push_str(&format!("; {module} data stays on {own}"));
                    }
                }
            }
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
                let (version, refreshed_own) = hub.move_origin(&request.agent_id).await?;
                if refreshed_own != own {
                    return Err(HandlerError::new(
                        "claim_integrity_failed",
                        "self author changed",
                    ));
                }
                self.move_open_check(received, &request.agent_id).await?;
                self.move_idle(received, &refs, &plan).await?;
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
                        if let Some(bytes) = self
                            .move_replay(received, &request.request_key, &request_body)
                            .await?
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
                optional: p.description.optional,
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
        let mut move_plan = json!({"agent_id":agent.agent_id,"from":own,"to":peer,"claim_version":claim_version,"hub_gen":claim_version,"participants":participants});
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
        let row = self
            .move_sql(received, move |handler| {
                if confirmation.as_ref().is_some_and(|g| *g.cancelled.borrow()) {
                    return Err(OperatorConfirmError::Declined.into());
                }
                handler.with_agent_store(|s| s.admit_agent_move(&row, &refs))
            })
            .await?;
        reply(&row)
    }

    async fn move_replay(
        &self,
        received: Instant,
        key: &str,
        body: &Value,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        let key = key.to_owned();
        let Some(row) = self
            .move_sql(received, move |handler| {
                handler.with_store(|s| s.agent_move_by_request_key(&key))
            })
            .await?
        else {
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

    async fn move_open_check(&self, received: Instant, agent_id: &str) -> Result<(), HandlerError> {
        let rows = self
            .move_sql(received, |handler| {
                handler.with_store(RegistryStore::list_open_agent_moves)
            })
            .await?;
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
}

/// Whether `runner` is a participant in this move that holds `reference`.
fn freezes(plan: &[PlannedParticipant], runner: &str, reference: &MoveRef) -> bool {
    plan.iter().any(|participant| {
        participant.client.module_id == runner
            && participant.refs.iter().any(|held| {
                held.ref_kind == reference.ref_kind && held.r#ref == reference.reference
            })
    })
}

impl ProjectsHandler {
    async fn move_idle(
        &self,
        received: Instant,
        refs: &[MoveRef],
        plan: &[PlannedParticipant],
    ) -> Result<(), HandlerError> {
        let answers = self.read_move_liveness(received, refs).await?;
        let mut busy = Vec::new();
        for (reference, answer) in refs.iter().zip(answers) {
            let (reason_code, reason) = match answer {
                MoveLiveness::Gone => continue,
                // An idle session is safe to move only when the module that
                // runs it takes part in the move for that exact ref: its
                // `prepare` refuses while a turn runs and freezes the session
                // before answering. A host that is not a participant (such as
                // an OpenCode or Pi process) can keep writing during the move,
                // so its session must be closed first.
                MoveLiveness::Idle {
                    runner: Some(runner),
                } if freezes(plan, &runner, reference) => continue,
                MoveLiveness::Idle { .. } | MoveLiveness::Active => ("session_active", None),
                MoveLiveness::Unknown(cause) => ("liveness_unknown", cause),
            };
            busy.push(json!({"ref_kind":reference.ref_kind,"ref":reference.reference,"participant":"agent.session_liveness",
                "reason_code":reason_code, "reason":reason}));
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
        fake_participant::FakeParticipant,
        log_client::{LogTransport, TransportError},
        participant_discovery::{provider_role, ParticipantDiscovery},
        sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
        tests::{flow_stamp, reserved, scratch_descriptor},
        RouteAdmission,
    };
    use super::*;
    use async_trait::async_trait;
    use std::sync::{Arc, Mutex};
    use subc_client_rs::{CatalogList, HandlerOutcome, SupervisorListResponse};

    const DIRECT: RouteKey = (80, 1);
    const ORCH: RouteKey = (81, 1);
    const ENGRAM: RouteKey = (82, 1);
    const OTHER: RouteKey = (83, 1);
    const TARGET: &str = "02020202020202020202020202020202";

    #[derive(Default)]
    struct Prompt {
        summaries: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl OperatorConfirmer for Prompt {
        async fn confirm_operator(
            &self,
            summary: &str,
            _: RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.summaries.lock().unwrap().push(summary.into());
            Ok(())
        }
    }

    struct Discovery {
        daemon: FakeDaemon,
        participant: Arc<FakeParticipant>,
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
        discovery: Arc<Discovery>,
        prompt: Arc<Prompt>,
        descriptor: cortexkit_store_types::StorageDescriptor,
        dir: std::path::PathBuf,
    }

    impl Fixture {
        async fn new(cut_over: bool) -> Self {
            let (dir, descriptor) = scratch_descriptor("move-admission");
            let f = Self::open(dir, descriptor, false);
            if cut_over {
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
            f
        }

        async fn migrated() -> Self {
            use crate::sharing_baselines::{artifacts, force_membership, rows, BASELINE_AGENT};
            let (dir, descriptor) = scratch_descriptor("move-admission-migrated");
            let cortexkit_store_types::StorageBackend::Sqlite { path } = &descriptor.backend else {
                unreachable!();
            };
            std::fs::copy(artifacts().join("native-installed.db"), path).unwrap();
            let pre = rusqlite::Connection::open(path).unwrap();
            assert_eq!(
                rows(&pre, "SELECT agent_id FROM claim_cache"),
                vec![vec![json!(BASELINE_AGENT)]]
            );
            drop(pre);

            let f = Self::open(dir, descriptor, true);
            assert!(f.handler.hub_backend_selected());
            force_membership(&f.handler, false);
            f.handler.catalog_sql.wait().await;
            assert_eq!(f.store().identity_log_status().unwrap().state, "enabled");
            assert!(f.store().move_authority_cut_over().unwrap());
            f
        }

        fn open(
            dir: std::path::PathBuf,
            descriptor: cortexkit_store_types::StorageDescriptor,
            migrated: bool,
        ) -> Self {
            let discovery = Arc::new(Discovery {
                daemon: FakeDaemon::new(snapshot()),
                participant: Arc::new(FakeParticipant::new(&["session"], vec![])),
            });
            let prompt = Arc::new(Prompt::default());
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider("prefrontal".into()),
            );
            let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700);
            handler.participant_discovery = discovery.clone();
            handler.operator_confirmer = prompt.clone();
            handler.provider_resolver = resolver;
            let store = RegistryStore::open(&descriptor).unwrap();
            if migrated {
                crate::sharing_baselines::install_handler(&handler, store);
            } else {
                *handler.store.lock().unwrap() = Some(store);
            }
            for (key, principal) in [
                (DIRECT, Principal::Direct),
                (ORCH, reserved("prefrontal")),
                (ENGRAM, reserved("engram")),
                (OTHER, reserved("other")),
            ] {
                handler
                    .route_admissions()
                    .insert(key, RouteAdmission::from_bind(Some(principal), None));
            }
            Self {
                handler: Arc::new(handler),
                discovery,
                prompt,
                descriptor,
                dir,
            }
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

        fn install_unselected_hub(&self) {
            self.store()
                .apply_entry("fixture.hub_state", "{}", "fixture", None, |tx| {
                    tx.execute(
                        "UPDATE identity_log_state SET state='enabled',hub_log_base=0 WHERE id=1",
                        [],
                    )?;
                    tx.execute(
                        "INSERT INTO member_name(author,display_name) VALUES(?1,'Studio')",
                        [TARGET],
                    )?;
                    Ok(())
                })
                .unwrap();
            assert!(!self.handler.hub_backend_selected());
            assert!(self.handler.residence().is_none());
        }

        async fn move_(&self, key: RouteKey, id: &str) -> HandlerOutcome {
            self.call(
                key,
                MOVE,
                json!({"agent_id":id,"to":"Studio","request_key":"move"}),
            )
            .await
        }

        fn assert_no_side_effects(&self) {
            assert!(self.prompt.summaries.lock().unwrap().is_empty());
            assert!(self.discovery.daemon.calls().is_empty());
            assert!(self.store().list_agent_moves().unwrap().is_empty());
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

    fn code(outcome: HandlerOutcome) -> String {
        match outcome {
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                code
            }
            other => panic!("expected refusal: {other:?}"),
        }
    }

    fn existing_move(id: &str, key: &str, target: &str) -> AgentMove {
        AgentMove {
            cut_id: format!("{key:0<32}"),
            agent_id: id.into(),
            side: MoveSide::Source,
            peer: TARGET.into(),
            step: MoveStep::Preparing,
            plan: json!({"agent_id":id,"from":"01".repeat(16),"to":TARGET,
                "claim_version":1,"hub_gen":1,"participants":[]}),
            request_body: Some(json!({"agent_id":id,"to":target})),
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
            request_key: Some(key.into()),
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 700,
            updated_at_ms: 700,
        }
    }

    #[tokio::test]
    async fn admission_and_validation_precede_the_unselected_backend_refusal() {
        let f = Fixture::migrated().await;
        let id = crate::sharing_baselines::BASELINE_AGENT;
        assert!(super::super::MUTATING_METHODS.contains(&MOVE));
        assert!(super::super::SELF_ADMITTED_METHODS.contains(&MOVE));
        assert!(!super::super::agent_operation_description(MOVE).is_empty());
        assert_eq!(
            code(f.handler.handle_request(&body(MOVE, json!({})), DIRECT)),
            "served_path_required"
        );
        assert_eq!(code(f.move_(OTHER, id).await), "caller_forbidden");
        assert_eq!(code(f.move_(ENGRAM, id).await), "caller_forbidden");
        f.handler.route_admissions().insert(
            OTHER,
            RouteAdmission::from_bind(Some(Principal::Direct), Some(&flow_stamp(Some("flow")))),
        );
        assert_eq!(code(f.move_(OTHER, id).await), "flow_scope_not_admitted");
        for bad in [
            json!({}),
            json!({"agent_id":id,"to":"Studio","request_key":""}),
            json!({"agent_id":id,"to":"Studio","request_key":"move","extra":true}),
        ] {
            assert_eq!(code(f.call(DIRECT, MOVE, bad).await), "invalid_request");
        }
        for caller in [DIRECT, ORCH] {
            assert_eq!(code(f.move_(caller, id).await), "hub_not_ready");
        }
        f.store()
            .set_move_setting(entorhinal_core::move_store::MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        assert_eq!(code(f.move_(ENGRAM, id).await), "hub_not_ready");
        f.store()
            .apply_entry("fixture.retire", "{}", "fixture", None, |tx| {
                assert_eq!(
                    tx.execute(
                        "UPDATE agent SET terminal_reason='retired',terminal_at_ms=701 WHERE agent_id=?1",
                        [id]
                    )?,
                    1
                );
                Ok(())
            })
            .unwrap();
        for caller in [DIRECT, ENGRAM] {
            assert_eq!(code(f.move_(caller, id).await), "gone");
        }
        f.assert_no_side_effects();
    }

    #[tokio::test]
    async fn authority_identity_unknown_and_gone_keep_their_order() {
        let f = Fixture::new(false).await;
        assert_eq!(
            code(f.move_(DIRECT, "unknown").await),
            "authority_not_cut_over"
        );
        let f = Fixture::new(true).await;
        assert_eq!(
            code(f.move_(DIRECT, "unknown").await),
            "identity_log_disabled"
        );
        let id = f.create().await;
        f.install_unselected_hub();
        assert_eq!(code(f.move_(ORCH, "unknown").await), "unknown_agent");
        assert_eq!(code(f.move_(ORCH, &id).await), "hub_not_ready");
        f.assert_no_side_effects();

        let f = Fixture::new(true).await;
        let id = f.create().await;
        result(
            f.call(
                ORCH,
                "agent.dispose",
                json!({"agent_id":id,"request_key":"dispose"}),
            )
            .await,
        );
        f.install_unselected_hub();
        assert_eq!(code(f.move_(ORCH, &id).await), "gone");
        assert_eq!(code(f.move_(ORCH, "unknown").await), "unknown_agent");
        f.assert_no_side_effects();
    }

    #[tokio::test]
    async fn request_key_replay_precedes_selection_and_target_rename() {
        let f = Fixture::migrated().await;
        let id = crate::sharing_baselines::BASELINE_AGENT;
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: TARGET.into(),
                    display_name: "Studio".into(),
                },
                "fixture",
            )
            .unwrap();
        let row = existing_move(id, "move", "Studio");
        let expected = result(HandlerOutcome::Response(reply(&row).unwrap()));
        f.store().insert_agent_move(&row).unwrap();
        assert_eq!(result(f.move_(ORCH, id).await), expected);
        assert_eq!(result(f.move_(DIRECT, id).await), expected);
        f.store()
            .set_member_name(
                entorhinal_core::shared_entry::MemberName {
                    author: TARGET.into(),
                    display_name: "Renamed".into(),
                },
                "fixture",
            )
            .unwrap();
        assert_eq!(result(f.move_(ORCH, id).await), expected);
        assert_eq!(
            code(
                f.call(
                    ORCH,
                    MOVE,
                    json!({"agent_id":id,"to":"Renamed","request_key":"move"}),
                )
                .await
            ),
            "request_key_reused"
        );
        assert_eq!(code(f.move_(OTHER, id).await), "caller_forbidden");
        assert!(f.prompt.summaries.lock().unwrap().is_empty());
        assert!(f.discovery.daemon.calls().is_empty());
        assert_eq!(f.store().list_agent_moves().unwrap(), vec![row]);
    }

    #[tokio::test]
    async fn selected_backend_without_residence_watch_is_not_ready() {
        let f = Fixture::new(true).await;
        let id = f.create().await;
        f.install_unselected_hub();
        f.handler
            .health
            .hub_membership
            .store(1, std::sync::atomic::Ordering::Relaxed);
        assert!(f.handler.hub_backend_selected());
        assert!(f.handler.residence().is_none());
        assert_eq!(code(f.move_(DIRECT, &id).await), "hub_not_ready");
        f.assert_no_side_effects();
    }
}

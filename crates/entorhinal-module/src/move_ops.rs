//! Machine-local move queries and operator controls. These operations never
//! join the shared journal writer or contact a participant from the handler.

use super::agent_ops::{ConfirmationGuard, OperatorConfirmError};
use super::shared_write::OPERATOR_BOUND;
use super::{encode_result, HandlerError, ProjectsHandler, RouteKey};
use cortexkit_role_agent_data::refs::CutId;
use entorhinal_core::{
    move_record::MoveParticipant,
    move_store::MoveSetting,
    session_carry::{PACKAGE_CLOSURE_KIND, SESSION_BINDINGS_KIND},
    RegistryStore,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::Principal;
use tokio::time::{timeout_at, Instant};

pub(super) const STATUS: &str = "agent.move_status";
pub(super) const REFS: &str = "agent.move_refs";
pub(super) const MOVES: &str = "agent.moves";
pub(super) const ABORT: &str = "agent.move_abort";
pub(super) const SET_LID: &str = "agent.set_lid_close_moves";
pub(super) const READS: &[&str] = &[STATUS, REFS, MOVES];
pub(super) const WRITES: &[&str] = &[ABORT, SET_LID];

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusRequest {
    cut_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RefsRequest {
    #[serde(rename = "cutId")]
    cut_id: CutId,
}
#[derive(Deserialize)]
struct RefsPlan {
    participants: Vec<MoveParticipant>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AbortRequest {
    cut_id: String,
    request_key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SettingRequest {
    enabled: bool,
    request_key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params).map_err(|e| HandlerError::new("invalid_request", e.to_string()))
}
fn require_key(key: &str) -> Result<(), HandlerError> {
    if key.is_empty() {
        Err(HandlerError::new(
            "invalid_request",
            "request_key must be nonempty",
        ))
    } else {
        Ok(())
    }
}
fn unknown_move() -> HandlerError {
    HandlerError::new("unknown_move", "no local move has that cut_id")
}

impl ProjectsHandler {
    pub(super) fn move_refs(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: RefsRequest = parse(params)?;
        let row = self
            .with_store(|s| s.agent_move_snapshot(request.cut_id.as_str()))?
            .ok_or_else(unknown_move)?;
        // Both checkpoint formats keep the routed refs in participants. Read
        // that frozen set, not bindings that can change after admission.
        let plan: RefsPlan = serde_json::from_value(row.plan)
            .map_err(|e| HandlerError::new("plan_malformed", e.to_string()))?;
        let refs: std::collections::BTreeSet<_> = plan
            .participants
            .into_iter()
            .flat_map(|p| p.refs)
            .filter(|r| {
                !matches!(
                    r.ref_kind.as_str(),
                    "agent" | PACKAGE_CLOSURE_KIND | SESSION_BINDINGS_KIND
                )
            })
            .collect();
        encode_result(json!({"cutId":request.cut_id,"agentId":row.agent_id,"refs":refs}))
    }

    pub(super) fn move_status(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: StatusRequest = parse(params)?;
        let (view, row) = self.with_store(|s| {
            Ok((
                s.move_status_view(&request.cut_id)?,
                s.agent_move(&request.cut_id)?,
            ))
        })?;
        let mut view = view.ok_or_else(unknown_move)?;
        if let Some(row) = &row {
            show_skipped(&mut view, row);
            show_losses(&mut view, row);
        }
        elapsed(&mut view, (self.clock)());
        encode_result(view)
    }

    pub(super) fn moves(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let _: EmptyRequest = parse(params)?;
        let (mut views, rows) =
            self.with_store(|s| Ok((s.moves_view()?, s.list_agent_moves()?)))?;
        for view in &mut views {
            if let Some(row) = rows.iter().find(|r| view["cut_id"] == r.cut_id.as_str()) {
                show_skipped(view, row);
                show_losses(view, row);
            }
            elapsed(view, (self.clock)());
        }
        encode_result(json!({"moves":views}))
    }

    async fn confirm_move_control(
        &self,
        summary: &str,
        key: RouteKey,
        received: Instant,
    ) -> Result<ConfirmationGuard, HandlerError> {
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(OperatorConfirmError::Declined.into());
        }
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(received + OPERATOR_BOUND, self.operator_confirmer.confirm_operator(summary, key)) => {
                result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
            }
        }
        if Instant::now() >= received + OPERATOR_BOUND || *guard.cancelled.borrow() {
            return Err(OperatorConfirmError::Declined.into());
        }
        Ok(guard)
    }

    pub(super) async fn abort_move(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(ABORT, admission.flow_id.as_deref())?;
        let direct = admission.principal == Some(Principal::Direct);
        if !direct && !self.move_orchestrator(&admission).await {
            return Err(HandlerError::new(
                "caller_forbidden",
                "caller cannot abort moves",
            ));
        }
        let request: AbortRequest = parse(params)?;
        require_key(&request.request_key)?;
        let row = self
            .with_store(|s| s.agent_move(&request.cut_id))?
            .ok_or_else(unknown_move)?;
        if row.abort_stuck() && !direct {
            return Err(HandlerError::new(
                "caller_forbidden",
                "only Direct can retry a stuck abort with operator confirmation",
            ));
        }
        if row.step.is_terminal() && row.step != entorhinal_core::move_store::MoveStep::Aborted
            || !row.step.is_terminal() && !row.step.before_boundary(row.side)
        {
            return Err(HandlerError {
                code: "invalid_state".into(),
                message: "move cannot be aborted at this step".into(),
                detail: Some(json!({"cut_id":row.cut_id,"side":row.side,"step":row.step})),
            });
        }
        if row.step == entorhinal_core::move_store::MoveStep::Aborted
            || row.abort_requested.is_some() && !row.abort_stuck()
        {
            return self.move_status(json!({"cut_id":row.cut_id}));
        }
        let guard = if direct {
            let verb = if row.abort_stuck() {
                "retry stuck abort of"
            } else {
                "abort"
            };
            Some(
                self.confirm_move_control(
                    &format!(
                        "{verb} move \"{}\" for agent \"{}\"",
                        row.cut_id, row.agent_id
                    ),
                    key,
                    received,
                )
                .await?,
            )
        } else {
            None
        };
        if guard.as_ref().is_some_and(|g| *g.cancelled.borrow()) {
            return Err(OperatorConfirmError::Declined.into());
        }
        self.with_agent_store(|s| s.request_agent_move_abort(&request.cut_id, (self.clock)()))?;
        self.start_move_driver();
        self.move_wake.notify_waiters();
        self.move_status(json!({"cut_id":request.cut_id}))
    }

    pub(super) async fn set_lid_close_moves(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(SET_LID, admission.flow_id.as_deref())?;
        if admission.principal != Some(Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "lid-close move settings require Direct",
            ));
        }
        let request: SettingRequest = parse(params)?;
        require_key(&request.request_key)?;
        let setting = MoveSetting {
            lid_close_moves: request.enabled,
        };
        if self.with_store(RegistryStore::move_setting)? != setting {
            let _guard = self
                .confirm_move_control(
                    &format!(
                        "turn lid-close moves {} on this machine",
                        if request.enabled { "on" } else { "off" }
                    ),
                    key,
                    received,
                )
                .await?;
            self.with_store(|s| s.set_move_setting(setting))?;
        }
        encode_result(setting)
    }
}
/// The public move view leaves out the private plan, so copy out the one plan
/// fact an operator needs: which participants this receiver skipped because
/// they are not installed here and the source sent no data for them.
fn show_skipped(view: &mut Value, row: &entorhinal_core::move_store::AgentMove) {
    let skipped = super::move_receiver::skipped_participants(row);
    if !skipped.is_empty() {
        view["skipped_participants"] = json!(skipped);
    }
}
/// Copy out, from the private plan, what participants reported dropping while
/// importing on this machine. A move with any losses must not read as a clean
/// success; without losses the view gains no key at all.
fn show_losses(view: &mut Value, row: &entorhinal_core::move_store::AgentMove) {
    let losses = super::move_receiver::import_losses(row);
    if !losses.is_empty() {
        view["losses"] = json!(losses);
    }
}
pub(crate) fn elapsed(value: &mut Value, now: i64) {
    if let Some(since) = value["waiting_since_ms"].as_i64() {
        value["waiting_ms"] = json!(now.saturating_sub(since).max(0));
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        agent_ops::OperatorConfirmer,
        sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
        tests::{flow_stamp, reserved, scratch_descriptor},
        RouteAdmission, WireRequest,
    };
    use super::*;
    use async_trait::async_trait;
    use entorhinal_core::move_store::{AgentMove, MoveSide, MoveStep};
    use std::sync::{Arc, Mutex};
    use subc_client_rs::HandlerOutcome;
    const DIRECT: RouteKey = (110, 1);
    const ORCH: RouteKey = (111, 1);
    const OTHER: RouteKey = (112, 1);
    const FLOW: RouteKey = (113, 1);

    struct Prompt {
        summaries: Mutex<Vec<String>>,
        answer: Result<(), OperatorConfirmError>,
    }
    #[async_trait]
    impl OperatorConfirmer for Prompt {
        async fn confirm_operator(
            &self,
            summary: &str,
            _: RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.summaries.lock().unwrap().push(summary.into());
            self.answer
        }
    }
    struct Fixture {
        handler: ProjectsHandler,
        prompt: Arc<Prompt>,
        dir: std::path::PathBuf,
    }
    impl Fixture {
        fn new(answer: Result<(), OperatorConfirmError>) -> Self {
            let (dir, descriptor) = scratch_descriptor("move-ops");
            let prompt = Arc::new(Prompt {
                summaries: Mutex::new(vec![]),
                answer,
            });
            let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700);
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            handler.operator_confirmer = prompt.clone();
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider("orchestrator".into()),
            );
            handler.provider_resolver = resolver;
            for (key, principal) in [
                (DIRECT, Principal::Direct),
                (ORCH, reserved("orchestrator")),
                (OTHER, reserved("engram")),
            ] {
                handler
                    .route_admissions()
                    .insert(key, RouteAdmission::from_bind(Some(principal), None));
            }
            handler.route_admissions().insert(
                FLOW,
                RouteAdmission::from_bind(Some(Principal::Direct), Some(&flow_stamp(Some("flow")))),
            );
            Self {
                handler,
                prompt,
                dir,
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        async fn call(&self, method: &str, params: Value, key: RouteKey) -> HandlerOutcome {
            self.handler
                .handle_served_request(
                    &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                    key,
                )
                .await
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(body) => {
                serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
            }
            other => panic!("{other:?}"),
        }
    }
    fn code(outcome: HandlerOutcome) -> String {
        match outcome {
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                code
            }
            other => panic!("{other:?}"),
        }
    }
    fn row(cut: &str) -> AgentMove {
        AgentMove {
            cut_id: cut.into(),
            agent_id: cut.into(),
            side: MoveSide::Source,
            peer: "peer".into(),
            step: MoveStep::Preparing,
            plan: json!({"agent_id":cut,"from":"self","to":"peer","claim_version":0,"participants":[]}),
            request_body: Some(json!({"agent_id":cut,"to":"peer"})),
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
            request_key: Some(cut.into()),
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn move_refs_are_frozen_sorted_session_refs_open_to_every_principal() {
        let f = Fixture::new(Ok(()));
        let cut = "0123456789abcdef0123456789abcdef";
        let mut row = row(cut);
        row.plan["participants"] = json!([
            {"module_id":"both","refs":[
                {"ref_kind":"session","ref":"z"},
                {"ref_kind":"agent","ref":cut},
                {"ref_kind":"pi","ref":"b"},
                {"ref_kind":"session","ref":"a"}
            ],"external":[]},
            {"module_id":"duplicate","refs":[{"ref_kind":"session","ref":"a"}],"external":[]},
            {"module_id":"entorhinal","refs":[
                {"ref_kind":PACKAGE_CLOSURE_KIND,"ref":cut},
                {"ref_kind":SESSION_BINDINGS_KIND,"ref":cut}
            ],"external":[]}
        ]);
        f.store().insert_agent_move(&row).unwrap();
        let generation = f.store().generation().unwrap();
        let _writer = f.handler.writer.lock().await;
        for principal in [
            Some(Principal::Direct),
            Some(reserved("orchestrator")),
            Some(reserved("other")),
            Some(Principal::Unverified),
            None,
        ] {
            for flow in [false, true] {
                f.handler.route_admissions().insert(
                    OTHER,
                    RouteAdmission::from_bind(
                        principal.clone(),
                        flow.then(|| flow_stamp(Some("flow"))).as_ref(),
                    ),
                );
                let reply = tokio::time::timeout(
                    std::time::Duration::from_secs(5),
                    f.call(REFS, json!({"cutId":cut}), OTHER),
                )
                .await
                .expect("move_refs must bypass the shared writer");
                assert_eq!(
                    result(reply),
                    json!({"cutId":cut,"agentId":cut,"refs":[
                        {"ref_kind":"pi","ref":"b"},
                        {"ref_kind":"session","ref":"a"},
                        {"ref_kind":"session","ref":"z"}
                    ]})
                );
            }
        }
        assert_eq!(f.store().generation().unwrap(), generation);
        assert_eq!(f.store().agent_move(cut).unwrap().unwrap(), row);
    }

    #[tokio::test]
    async fn move_refs_refuse_unknown_cuts_members_and_malformed_cut_ids() {
        let f = Fixture::new(Ok(()));
        let cut = "0123456789abcdef0123456789abcdef";
        assert_eq!(
            code(f.call(REFS, json!({"cutId":cut}), OTHER).await),
            "unknown_move"
        );
        for params in [
            json!({"cutId":cut,"unexpected":true}),
            json!({"cut_id":cut}),
            json!({}),
            json!({"cutId":12}),
            json!({"cutId":""}),
            json!({"cutId":"a".repeat(31)}),
            json!({"cutId":"a".repeat(33)}),
            json!({"cutId":"A".repeat(32)}),
            json!({"cutId":"g".repeat(32)}),
        ] {
            assert_eq!(
                code(f.call(REFS, params.clone(), OTHER).await),
                "invalid_request",
                "{params}"
            );
        }
    }

    #[test]
    fn move_refs_read_committed_plan_before_parked_database_writer_is_released() {
        use std::{sync::mpsc, thread, time::Duration};

        let f = Fixture::new(Ok(()));
        let cut = "0123456789abcdef0123456789abcdef";
        f.store().insert_agent_move(&row(cut)).unwrap();
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (reply_tx, reply_rx) = mpsc::channel();
        let received = thread::scope(|scope| {
            let handler = &f.handler;
            let writer = scope.spawn(move || {
                handler
                    .with_store(|store| {
                        store.apply_entry("fixture", "{}", "test", None, |tx| {
                            tx.execute("UPDATE agent_move SET agent_id='uncommitted'", [])?;
                            parked_tx.send(()).unwrap();
                            // Keep the writer parked beyond the read deadline, but
                            // release it even when a regression blocks the read.
                            let _ = release_rx.recv_timeout(Duration::from_secs(60));
                            tx.execute_batch("SELECT * FROM missing_rollback_sentinel")
                        })
                    })
                    .unwrap_err();
            });
            parked_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            scope.spawn(move || {
                let reply = handler.handle_request(
                    &serde_json::to_vec(&json!({"method":REFS,"params":{"cutId":cut}})).unwrap(),
                    OTHER,
                );
                let _ = reply_tx.send(reply);
            });
            let received = reply_rx.recv_timeout(Duration::from_secs(30));
            release_tx.send(()).unwrap();
            writer.join().unwrap();
            received
        });
        assert_eq!(
            result(received.expect("move_refs waited for the database writer")),
            json!({"cutId":cut,"agentId":cut,"refs":[]})
        );
    }

    #[tokio::test]
    async fn served_move_controls_admit_confirm_and_bypass_shared_writer() {
        let f = Fixture::new(Ok(()));
        f.store().insert_agent_move(&row("direct")).unwrap();
        f.store().insert_agent_move(&row("relay")).unwrap();
        let generation = f.store().generation().unwrap();
        let _writer = f.handler.writer.lock().await;
        let reply = result(
            f.call(ABORT, json!({"cut_id":"direct","request_key":"k"}), DIRECT)
                .await,
        );
        assert_eq!(reply["abort_requested"]["reason"], "operator");
        let reply = result(
            f.call(ABORT, json!({"cut_id":"relay","request_key":"k"}), ORCH)
                .await,
        );
        assert_eq!(reply["abort_requested"]["reason"], "operator");
        result(
            f.call(SET_LID, json!({"enabled":true,"request_key":"k"}), DIRECT)
                .await,
        );
        result(
            f.call(
                SET_LID,
                json!({"enabled":true,"request_key":"other"}),
                DIRECT,
            )
            .await,
        );
        result(
            f.call(
                ABORT,
                json!({"cut_id":"direct","request_key":"other"}),
                DIRECT,
            )
            .await,
        );
        assert_eq!(
            f.prompt.summaries.lock().unwrap().as_slice(),
            &[
                "abort move \"direct\" for agent \"direct\"",
                "turn lid-close moves on on this machine"
            ]
        );
        assert!(f.store().move_setting().unwrap().lid_close_moves);
        assert_eq!(f.store().generation().unwrap(), generation);
        let _ = result(f.call(STATUS, json!({"cut_id":"direct"}), OTHER).await);
        assert!(!result(f.call(MOVES, json!({}), OTHER).await)["moves"]
            .as_array()
            .unwrap()
            .is_empty());
        f.handler.stop_move_driver().await;
    }

    #[tokio::test]
    async fn move_controls_refuse_wrong_callers_missing_keys_and_boundary_without_writes() {
        let f = Fixture::new(Ok(()));
        let mut boundary = row("boundary");
        boundary.step = MoveStep::Releasing;
        f.store().insert_agent_move(&boundary).unwrap();
        for method in [ABORT, SET_LID] {
            let params = if method == ABORT {
                json!({"cut_id":"boundary","request_key":"k"})
            } else {
                json!({"enabled":true,"request_key":"k"})
            };
            assert_eq!(
                code(f.call(method, params.clone(), OTHER).await),
                "caller_forbidden"
            );
            assert_eq!(
                code(f.call(method, params, FLOW).await),
                "flow_scope_not_admitted"
            );
            let params = if method == ABORT {
                json!({"cut_id":"boundary"})
            } else {
                json!({"enabled":true})
            };
            assert_eq!(
                code(f.call(method, params, DIRECT).await),
                "invalid_request"
            );
        }
        assert_eq!(
            code(
                f.call(SET_LID, json!({"enabled":true,"request_key":"k"}), ORCH)
                    .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            code(
                f.call(
                    ABORT,
                    json!({"cut_id":"boundary","request_key":"k"}),
                    DIRECT
                )
                .await
            ),
            "invalid_state"
        );
        assert_eq!(f.store().agent_move("boundary").unwrap().unwrap(), boundary);
        assert!(f.prompt.summaries.lock().unwrap().is_empty());
        assert_eq!(
            code(f.call(STATUS, json!({"cut_id":"missing"}), OTHER).await),
            "unknown_move"
        );
        assert_eq!(
            code(f.call(MOVES, json!({"unexpected":true}), OTHER).await),
            "invalid_request"
        );
        for method in [super::super::move_admission::MOVE, ABORT, SET_LID] {
            assert_eq!(
                f.handler
                    .execute(
                        WireRequest {
                            method: method.into(),
                            params: json!({})
                        },
                        DIRECT
                    )
                    .unwrap_err()
                    .code,
                "served_path_required"
            );
        }
    }

    #[tokio::test]
    async fn move_controls_presence_failure_and_aborted_noop_preserve_state() {
        let f = Fixture::new(Err(OperatorConfirmError::PresenceUnavailable));
        f.store().insert_agent_move(&row("cut")).unwrap();
        assert_eq!(
            code(
                f.call(ABORT, json!({"cut_id":"cut","request_key":"k"}), DIRECT)
                    .await
            ),
            "operator_presence_unavailable"
        );
        assert_eq!(
            code(
                f.call(SET_LID, json!({"enabled":true,"request_key":"k"}), DIRECT)
                    .await
            ),
            "operator_presence_unavailable"
        );
        assert!(f
            .store()
            .agent_move("cut")
            .unwrap()
            .unwrap()
            .abort_requested
            .is_none());
        assert!(!f.store().move_setting().unwrap().lid_close_moves);
        let mut aborted = row("aborted");
        aborted.step = MoveStep::Aborted;
        f.store().insert_agent_move(&aborted).unwrap();
        result(
            f.call(ABORT, json!({"cut_id":"aborted","request_key":"k"}), DIRECT)
                .await,
        );
        assert_eq!(f.store().agent_move("aborted").unwrap().unwrap(), aborted);
        assert_eq!(f.prompt.summaries.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn stuck_abort_retry_requires_direct_confirmation() {
        let f = Fixture::new(Ok(()));
        let mut stuck = row("stuck");
        stuck.abort_requested = Some(json!({"reason":"operator","detail":null}));
        stuck.hold = Some(
            json!({"state":"stuck","op":"agent_data.abort","module_id":"data","code":"digest_mismatch","reason":"undo_digest_mismatch"}),
        );
        f.store().insert_agent_move(&stuck).unwrap();
        assert_eq!(
            code(
                f.call(ABORT, json!({"cut_id":"stuck","request_key":"k"}), ORCH)
                    .await
            ),
            "caller_forbidden"
        );
        assert_eq!(f.store().agent_move("stuck").unwrap().unwrap(), stuck);
        result(
            f.call(ABORT, json!({"cut_id":"stuck","request_key":"k"}), DIRECT)
                .await,
        );
        assert_eq!(
            f.prompt.summaries.lock().unwrap().as_slice(),
            &["retry stuck abort of move \"stuck\" for agent \"stuck\""]
        );
        f.handler.stop_move_driver().await;
    }
}

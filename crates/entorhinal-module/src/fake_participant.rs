//! Machine-local participant double. Each instance owns its data, cut state and reply cache.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::log_client::{LogTransport, TransportError};
use crate::participant_client::*;

pub(crate) const REFUSAL_CODES: &[&str] = cortexkit_role_agent_data::errors::codes::ALL;

#[derive(Clone)]
pub(crate) enum Effect {
    Refuse(Refusal),
    FailBefore(TransportError),
    LoseReply(TransportError),
    /// Do the op, then answer with this raw result instead of the typed
    /// reply, so a test can send a reply the contract forbids.
    Reply(Value),
}

#[derive(Debug, Clone)]
pub(crate) struct CutState {
    pub(crate) refs: BTreeSet<AgentRef>,
    pub(crate) step: Step,
}

struct State {
    description: DescribeReply,
    held: BTreeSet<AgentRef>,
    unknown_busy_for_unheld: bool,
    busy: BTreeMap<AgentRef, BusyRef>,
    diverged: BTreeMap<AgentRef, DivergedRef>,
    prepare: PrepareReply,
    import: ImportReply,
    enforced: bool,
    cuts: BTreeMap<String, CutState>,
    replies: BTreeMap<(String, &'static str), (Request, Value)>,
    frozen: BTreeSet<AgentRef>,
    staged: BTreeSet<AgentRef>,
    tombstones: BTreeMap<AgentRef, String>,
    scripts: BTreeMap<String, VecDeque<Effect>>,
    calls: Vec<Request>,
    wire_calls: Vec<(String, Value)>,
}

pub(crate) struct FakeParticipant {
    state: Mutex<State>,
}

impl FakeParticipant {
    pub(crate) fn new(ref_kinds: &[&str], held: Vec<AgentRef>) -> Self {
        Self {
            state: Mutex::new(State {
                description: DescribeReply::new(
                    ref_kinds.iter().map(|kind| (*kind).into()).collect(),
                    vec![],
                    false,
                ),
                held: held.into_iter().collect(),
                unknown_busy_for_unheld: false,
                busy: BTreeMap::new(),
                diverged: BTreeMap::new(),
                prepare: PrepareReply::new(vec![], vec![]),
                import: ImportReply::new(vec![]),
                enforced: true,
                cuts: BTreeMap::new(),
                replies: BTreeMap::new(),
                frozen: BTreeSet::new(),
                staged: BTreeSet::new(),
                tombstones: BTreeMap::new(),
                scripts: BTreeMap::new(),
                calls: vec![],
                wire_calls: vec![],
            }),
        }
    }

    pub(crate) fn script(&self, op: &str, effect: Effect) {
        self.state
            .lock()
            .unwrap()
            .scripts
            .entry(op.into())
            .or_default()
            .push_back(effect);
    }
    pub(crate) fn set_prepare(&self, reply: PrepareReply) {
        self.state.lock().unwrap().prepare = reply;
    }
    pub(crate) fn set_import(&self, reply: ImportReply) {
        self.state.lock().unwrap().import = reply;
    }
    pub(crate) fn set_busy(&self, reference: AgentRef, busy: BusyRef) {
        self.state.lock().unwrap().busy.insert(reference, busy);
    }
    pub(crate) fn set_diverged(&self, reference: AgentRef, state: DivergedState) {
        self.state
            .lock()
            .unwrap()
            .diverged
            .insert(reference, DivergedRef::new(state));
    }
    pub(crate) fn set_diverged_reason(
        &self,
        reference: AgentRef,
        state: DivergedState,
        reason: impl Into<String>,
    ) {
        let entry = DivergedRef::with_reason(state, reason).expect("test reason is valid");
        self.state.lock().unwrap().diverged.insert(reference, entry);
    }
    pub(crate) fn unknown_busy_for_unheld(&self, enabled: bool) {
        self.state.lock().unwrap().unknown_busy_for_unheld = enabled;
    }
    pub(crate) fn set_enforced(&self, enforced: bool) {
        self.state.lock().unwrap().enforced = enforced;
    }
    pub(crate) fn calls(&self) -> Vec<Request> {
        self.state.lock().unwrap().calls.clone()
    }
    pub(crate) fn wire_calls(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().wire_calls.clone()
    }
    pub(crate) fn cut(&self, cut_id: &str) -> Option<CutState> {
        self.state.lock().unwrap().cuts.get(cut_id).cloned()
    }
    pub(crate) fn frozen(&self) -> BTreeSet<AgentRef> {
        self.state.lock().unwrap().frozen.clone()
    }
    pub(crate) fn staged(&self) -> BTreeSet<AgentRef> {
        self.state.lock().unwrap().staged.clone()
    }
    pub(crate) fn held(&self) -> BTreeSet<AgentRef> {
        self.state.lock().unwrap().held.clone()
    }
    /// Hold another ref, as a participant does once its host opens it.
    pub(crate) fn hold(&self, reference: AgentRef) {
        self.state.lock().unwrap().held.insert(reference);
    }
    pub(crate) fn tombstones(&self) -> BTreeMap<AgentRef, String> {
        self.state.lock().unwrap().tombstones.clone()
    }

    pub(crate) fn handle(
        &self,
        caller: &str,
        method: &str,
        params: Value,
    ) -> Result<Vec<u8>, TransportError> {
        if !cortexkit_role_agent_data::caller_admitted(caller) {
            return Err(refuse(
                cortexkit_role_agent_data::errors::codes::CALLER_FORBIDDEN,
                None,
            ));
        }
        let object = params.as_object().cloned().ok_or_else(|| {
            refuse(
                cortexkit_role_agent_data::errors::codes::INVALID_REQUEST,
                None,
            )
        })?;
        // The method belongs to the routed envelope, never to the params object.
        if object.contains_key("op") {
            return Err(refuse(
                cortexkit_role_agent_data::errors::codes::INVALID_REQUEST,
                None,
            ));
        }
        let request = Request::decode(method, &Value::Object(object))
            .map_err(|error| refuse(error.code.as_str(), error.detail))?;
        let mut state = self.state.lock().unwrap();
        state.calls.push(request.clone());
        state.wire_calls.push((method.into(), params));
        let effect = state.scripts.get_mut(method).and_then(VecDeque::pop_front);
        let recorded = request.op().changes_state()
            && request_cut(&request).is_some_and(|cut| {
                state
                    .replies
                    .contains_key(&(cut.cut_id.as_str().to_owned(), request.op().method()))
            });
        match &effect {
            Some(Effect::Refuse(refusal)) if !recorded => {
                return Err(TransportError::Refusal {
                    code: refusal.code.clone(),
                    detail: refusal.detail.clone(),
                    message: refusal.message.clone(),
                })
            }
            Some(Effect::FailBefore(error)) => return Err(error.clone()),
            _ => {}
        }
        let reply = execute(&mut state, request)?;
        match effect {
            Some(Effect::LoseReply(error)) => return Err(error),
            Some(Effect::Reply(raw)) => {
                return Ok(
                    serde_json::to_vec(&json!({"result": raw})).expect("fake reply serializes")
                )
            }
            _ => {}
        }
        Ok(serde_json::to_vec(&json!({"result": reply})).expect("fake reply serializes"))
    }
}

#[async_trait]
impl LogTransport for FakeParticipant {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.handle(cortexkit_role_agent_data::ADMITTED_CALLER, method, params)
    }
}

fn refuse(code: &str, detail: Option<Value>) -> TransportError {
    TransportError::Refusal {
        code: code.to_string(),
        detail,
        message: None,
    }
}

fn canonicalize(request: Request) -> Request {
    request.with_sorted_refs()
}

fn request_cut(request: &Request) -> Option<CutRequest> {
    Some(CutRequest::new(
        request.refs()?.clone(),
        request.cut_id()?.clone(),
    ))
}

fn request_refs(request: &Request) -> &[AgentRef] {
    request.refs().map(Refs::as_slice).unwrap_or(&[])
}

fn invalid_state(cut: &CutState) -> TransportError {
    refuse(
        cortexkit_role_agent_data::errors::codes::INVALID_STATE,
        Some(json!({"step": cut.step, "refs": cut.refs})),
    )
}

fn execute(state: &mut State, request: Request) -> Result<Value, TransportError> {
    let op = request.op();
    let cut = request_cut(&request);
    let refs: BTreeSet<_> = request_refs(&request).iter().cloned().collect();
    let key = cut
        .as_ref()
        .map(|cut| (cut.cut_id.as_str().to_owned(), op.method()));
    let canonical = canonicalize(request.clone());
    if op.changes_state() {
        if let Some((first, reply)) = key.as_ref().and_then(|key| state.replies.get(key)) {
            return if first == &canonical {
                Ok(reply.clone())
            } else {
                Err(refuse(
                    cortexkit_role_agent_data::errors::codes::ID_REUSED,
                    None,
                ))
            };
        }
    }
    let existing = cut
        .as_ref()
        .and_then(|cut| state.cuts.get(cut.cut_id.as_str()));
    if let Some(existing) = existing {
        if existing.refs != refs {
            return Err(invalid_state(existing));
        }
    }
    let step = existing.map_or(Step::None, |cut| cut.step);
    let reply = match &request {
        Request::Describe(_) => json!(state.description),
        Request::Busy(request) => {
            let mut result = Vec::new();
            for reference in &request.refs {
                if !state.held.contains(reference) {
                    if !state.unknown_busy_for_unheld {
                        return Err(refuse(
                            cortexkit_role_agent_data::errors::codes::UNKNOWN_REF,
                            Some(json!(reference)),
                        ));
                    }
                    result.push(
                        BusyRef::new(BusyState::Unknown)
                            .with_reason_code("unheld")
                            .with_reason("No local data"),
                    );
                } else {
                    result.push(
                        state
                            .busy
                            .get(reference)
                            .cloned()
                            .unwrap_or_else(|| BusyRef::new(BusyState::Idle)),
                    );
                }
            }
            json!(BusyReply::new(result))
        }
        Request::Status(_) => json!(StatusReply::new(step)),
        Request::Diverged(request) => json!(DivergedReply::new(
            request
                .refs
                .iter()
                .map(|reference| {
                    if state.held.contains(reference) {
                        state
                            .diverged
                            .get(reference)
                            .cloned()
                            .unwrap_or_else(|| DivergedRef::new(DivergedState::AtCut))
                    } else {
                        DivergedRef::new(DivergedState::AtCut)
                    }
                })
                .collect(),
        )),
        Request::Prepare(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::None)?;
            for reference in &refs {
                if !state.held.contains(reference) {
                    return Err(refuse(
                        cortexkit_role_agent_data::errors::codes::UNKNOWN_REF,
                        Some(json!(reference)),
                    ));
                }
            }
            let busy: Vec<_> = refs
                .iter()
                .filter_map(|reference| {
                    state
                        .busy
                        .get(reference)
                        .filter(|busy| busy.state != BusyState::Idle)
                        .map(|busy| {
                            json!({
                                "ref_kind": reference.ref_kind,
                                "ref": reference.r#ref,
                                "state": busy.state,
                                "reason_code": busy.reason_code,
                                "reason": busy.reason,
                            })
                        })
                })
                .collect();
            if !busy.is_empty() {
                return Err(refuse(
                    cortexkit_role_agent_data::errors::codes::BUSY,
                    Some(json!({"refs": busy})),
                ));
            }
            state.frozen.extend(refs.clone());
            json!(state.prepare)
        }
        Request::Export(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::Prepared)?;
            let prepare_key = (
                cut.as_ref().unwrap().cut_id.as_str().to_owned(),
                cortexkit_role_agent_data::ops::PREPARE,
            );
            let prepare = &state.replies[&prepare_key].1;
            let prepare: PrepareReply =
                serde_json::from_value(prepare.clone()).expect("cached prepare is typed");
            json!(ExportReply::new(
                prepare
                    .items
                    .into_iter()
                    .map(|item| ExportItem::new(item.session_ref, item.end_offset))
                    .collect(),
            ))
        }
        Request::Import(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::None)?;
            state.staged.extend(refs.clone());
            json!(state.import)
        }
        Request::Activate(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::Imported)?;
            state.held.extend(refs.clone());
            for reference in &refs {
                state.staged.remove(reference);
                state.tombstones.remove(reference);
            }
            json!(Empty::new())
        }
        Request::Abort(_) => {
            if matches!(step, Step::Activated | Step::Released) {
                return Err(invalid_state(existing.unwrap()));
            }
            for reference in &refs {
                state.frozen.remove(reference);
                state.staged.remove(reference);
            }
            json!(Empty::new())
        }
        Request::Release(request) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::Exported)?;
            for reference in &refs {
                state.frozen.remove(reference);
                state
                    .tombstones
                    .insert(reference.clone(), request.holder.clone());
            }
            json!(ReleaseReply::new(state.enforced))
        }
    };
    if op.changes_state() {
        let cut = cut.unwrap();
        let step = match request {
            Request::Prepare(_) => Step::Prepared,
            Request::Export(_) => Step::Exported,
            Request::Import(_) => Step::Imported,
            Request::Activate(_) => Step::Activated,
            Request::Abort(_) => Step::Aborted,
            Request::Release(_) => Step::Released,
            _ => unreachable!(),
        };
        state
            .cuts
            .insert(cut.cut_id.as_str().to_owned(), CutState { refs, step });
        state
            .replies
            .insert(key.unwrap(), (canonical, reply.clone()));
    }
    Ok(reply)
}

fn require_step(
    state: &State,
    cut: &CutRequest,
    actual: Step,
    required: Step,
) -> Result<(), TransportError> {
    if actual == required {
        return Ok(());
    }
    match state.cuts.get(cut.cut_id.as_str()) {
        Some(cut) => Err(invalid_state(cut)),
        None => Err(refuse(
            cortexkit_role_agent_data::errors::codes::CUT_UNKNOWN,
            None,
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::participant_discovery::Participant;

    const CUT: &str = "0123456789abcdef0123456789abcdef";
    fn reference(name: &str) -> AgentRef {
        AgentRef::new("session", name)
    }
    fn cut(refs: &[&str]) -> CutRequest {
        CutRequest::new(
            Refs::new(refs.iter().map(|r| reference(r)).collect()).unwrap(),
            CutId::new(CUT).unwrap(),
        )
    }
    fn release_request(cut: &CutRequest, holder: &str) -> ReleaseRequest {
        ReleaseRequest::new(cut.refs.clone(), cut.cut_id.clone(), holder)
    }
    fn client(fake: Arc<FakeParticipant>) -> ParticipantClient {
        ParticipantClient::new(&Participant {
            module_id: "adapter".into(),
            transport: fake,
        })
    }
    fn refusal<T: std::fmt::Debug>(result: Result<T, ClientError>, code: &str) -> Option<Value> {
        match result.unwrap_err() {
            ClientError::Refused(refusal) => {
                assert_eq!(refusal.code, code);
                refusal.detail
            }
            other => panic!("expected {code}, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn idempotency_compares_ref_sets_and_holder_and_returns_first_reply_after_later_steps() {
        let fake = Arc::new(FakeParticipant::new(
            &["session"],
            vec![reference("a"), reference("b")],
        ));
        let c = client(fake.clone());
        let original = PrepareReply::new(
            vec![],
            vec![ExternalRef::new(
                "session",
                "a",
                "Copy host data",
                json!({"v":1}),
            )],
        );
        fake.set_prepare(original.clone());
        assert_eq!(c.prepare(&cut(&["a", "b"])).await.unwrap(), original);
        fake.set_prepare(PrepareReply::new(vec![], vec![]));
        assert_eq!(c.prepare(&cut(&["b", "a"])).await.unwrap(), original);
        refusal(c.prepare(&cut(&["a"])).await, "id_reused");
        c.export(&cut(&["b", "a"])).await.unwrap();
        let release = release_request(&cut(&["a", "b"]), "machine-b");
        assert!(c.release(&release).await.unwrap().enforced);
        fake.set_enforced(false);
        assert!(
            c.release(&release_request(&cut(&["b", "a"]), "machine-b"))
                .await
                .unwrap()
                .enforced
        );
        refusal(
            c.release(&release_request(&cut(&["a", "b"]), "machine-c"))
                .await,
            "id_reused",
        );
        fake.script(
            "agent_data.prepare",
            Effect::Refuse(Refusal {
                code: "busy".into(),
                detail: None,
                message: None,
            }),
        );
        assert_eq!(c.prepare(&cut(&["a", "b"])).await.unwrap(), original);
        c.export(&cut(&["a", "b"])).await.unwrap();
        assert_eq!(
            c.status(&cut(&["b", "a"])).await.unwrap().step,
            Step::Released
        );
        assert_eq!(
            fake.tombstones().values().cloned().collect::<Vec<_>>(),
            ["machine-b", "machine-b"]
        );
    }

    #[tokio::test]
    async fn every_later_op_requires_the_fixed_ref_set() {
        let source = Arc::new(FakeParticipant::new(
            &["session"],
            vec![reference("a"), reference("b")],
        ));
        let a = client(source.clone());
        a.prepare(&cut(&["a", "b"])).await.unwrap();
        let wrong = cut(&["a"]);
        for (result, violating_op) in [
            (a.export(&wrong).await.map(|_| ()), None),
            (a.abort(&wrong).await.map(|_| ()), None),
            (
                a.status(&wrong).await.map(|_| ()),
                Some("agent_data.status"),
            ),
            (
                a.diverged(&wrong).await.map(|_| ()),
                Some("agent_data.diverged"),
            ),
        ] {
            let detail = match violating_op {
                None => refusal(result, "invalid_state").unwrap(),
                Some(expected_op) => match result.unwrap_err() {
                    ClientError::ContractViolation {
                        op, code, detail, ..
                    } => {
                        assert_eq!(op, expected_op);
                        assert_eq!(code, "invalid_state");
                        detail.unwrap()
                    }
                    other => panic!("expected contract violation for {expected_op}, got {other:?}"),
                },
            };
            assert_eq!(detail["step"], "prepared");
            assert_eq!(
                detail["refs"],
                json!([{"ref_kind":"session","ref":"a"},{"ref_kind":"session","ref":"b"}])
            );
        }
        a.export(&cut(&["a", "b"])).await.unwrap();
        refusal(
            a.release(&release_request(&wrong, "b")).await,
            "invalid_state",
        );
        let receiver = Arc::new(FakeParticipant::new(&["session"], vec![]));
        let b = client(receiver.clone());
        b.import(&cut(&["a", "b"])).await.unwrap();
        refusal(b.activate(&wrong).await, "invalid_state");
        refusal(b.abort(&wrong).await, "invalid_state");
        assert_eq!(
            receiver.staged(),
            BTreeSet::from([reference("a"), reference("b")])
        );
    }

    #[tokio::test]
    async fn source_and_receiver_step_machines_and_abort_boundaries_are_independent() {
        let source = Arc::new(FakeParticipant::new(&["session"], vec![reference("a")]));
        let receiver = Arc::new(FakeParticipant::new(&["session"], vec![]));
        let a = client(source.clone());
        let b = client(receiver.clone());
        let request = cut(&["a"]);
        assert_eq!(a.status(&request).await.unwrap().step, Step::None);
        refusal(a.export(&request).await, "cut_unknown");
        refusal(a.activate(&request).await, "cut_unknown");
        refusal(
            a.release(&release_request(&request, "machine-b")).await,
            "cut_unknown",
        );
        refusal(b.prepare(&request).await, "unknown_ref");
        assert_eq!(
            b.diverged(&request).await.unwrap().refs[0].state,
            DivergedState::AtCut
        );
        assert!(receiver.cut(CUT).is_none());
        a.prepare(&request).await.unwrap();
        assert_eq!(source.frozen(), BTreeSet::from([reference("a")]));
        assert!(receiver.frozen().is_empty());
        assert!(receiver.cut(CUT).is_none());
        assert_eq!(
            refusal(a.activate(&request).await, "invalid_state").unwrap()["step"],
            "prepared"
        );
        refusal(a.import(&request).await, "invalid_state");
        refusal(
            a.release(&release_request(&request, "machine-b")).await,
            "invalid_state",
        );
        a.export(&request).await.unwrap();
        b.import(&request).await.unwrap();
        assert_eq!(b.status(&request).await.unwrap().step, Step::Imported);
        assert_eq!(receiver.staged(), BTreeSet::from([reference("a")]));
        assert!(receiver.held().is_empty());
        b.activate(&request).await.unwrap();
        assert!(receiver.staged().is_empty());
        assert_eq!(receiver.held(), BTreeSet::from([reference("a")]));
        assert_eq!(
            refusal(b.abort(&request).await, "invalid_state").unwrap()["step"],
            "activated"
        );
        assert_eq!(source.cut(CUT).unwrap().step, Step::Exported);
        a.release(&release_request(&request, "machine-b"))
            .await
            .unwrap();
        assert_eq!(
            refusal(a.abort(&request).await, "invalid_state").unwrap()["step"],
            "released"
        );
        assert!(source.frozen().is_empty());
        assert!(receiver.tombstones().is_empty());
        assert_eq!(source.tombstones()[&reference("a")], "machine-b");
        assert_eq!(receiver.cut(CUT).unwrap().step, Step::Activated);
    }

    #[tokio::test]
    async fn abort_cleans_freezes_and_staging_and_unseen_cut_is_a_successful_noop() {
        for from in [Step::None, Step::Prepared, Step::Exported, Step::Imported] {
            let fake = Arc::new(FakeParticipant::new(&["session"], vec![reference("a")]));
            let c = client(fake.clone());
            let request = cut(&["a"]);
            match from {
                Step::Prepared => {
                    c.prepare(&request).await.unwrap();
                }
                Step::Exported => {
                    c.prepare(&request).await.unwrap();
                    c.export(&request).await.unwrap();
                }
                Step::Imported => {
                    c.import(&request).await.unwrap();
                }
                _ => {}
            }
            c.abort(&request).await.unwrap();
            c.abort(&request).await.unwrap();
            assert_eq!(c.status(&request).await.unwrap().step, Step::Aborted);
            assert!(fake.frozen().is_empty());
            assert!(fake.staged().is_empty());
            assert!(fake.tombstones().is_empty());
            assert_eq!(fake.held(), BTreeSet::from([reference("a")]));
            refusal(c.activate(&request).await, "invalid_state");
        }
    }

    #[tokio::test]
    async fn refusals_are_not_cached_and_all_contract_codes_can_be_scripted() {
        let fake = Arc::new(FakeParticipant::new(&["session"], vec![reference("a")]));
        let c = client(fake.clone());
        for code in REFUSAL_CODES {
            fake.script(
                "agent_data.prepare",
                Effect::Refuse(Refusal {
                    code: String::from(*code),
                    detail: Some(json!({"step":"none","note":"scripted refusal"})),
                    message: None,
                }),
            );
            let error = fake
                .handle(
                    "reserved:entorhinal",
                    "agent_data.prepare",
                    json!(cut(&["a"])),
                )
                .unwrap_err();
            match error {
                TransportError::Refusal {
                    code: actual_code,
                    detail,
                    ..
                } => {
                    assert_eq!(actual_code, *code);
                    assert_eq!(detail.unwrap()["note"], "scripted refusal");
                }
                other => panic!("expected scripted {code}, got {other:?}"),
            }
            assert!(fake.cut(CUT).is_none());
            assert!(fake.frozen().is_empty());
        }
        fake.set_busy(
            reference("a"),
            BusyRef::new(BusyState::Busy)
                .with_reason_code("run_active")
                .with_reason("Run active"),
        );
        let busy = refusal(c.prepare(&cut(&["a"])).await, "busy").unwrap();
        assert_eq!(busy["refs"][0]["reason_code"], "run_active");
        fake.set_busy(reference("a"), BusyRef::new(BusyState::Idle));
        c.prepare(&cut(&["a"])).await.unwrap();
        assert_eq!(fake.cut(CUT).unwrap().step, Step::Prepared);
    }

    #[tokio::test]
    async fn fresh_receiver_can_report_unknown_busy_but_accepts_diverged_and_import() {
        let fake = Arc::new(FakeParticipant::new(&["session"], vec![]));
        fake.unknown_busy_for_unheld(true);
        let c = client(fake.clone());
        let request = cut(&["a"]);
        let reply = c.busy(request.refs.as_slice()).await.unwrap();
        assert_eq!(reply.refs[0].state, BusyState::Unknown);
        assert_eq!(reply.refs[0].reason_code.as_deref(), Some("unheld"));
        assert_eq!(
            c.diverged(&request).await.unwrap().refs[0].state,
            DivergedState::AtCut
        );
        fake.set_import(ImportReply::new(vec![ImportItem::new("s", 17, "digest")]));
        assert_eq!(c.import(&request).await.unwrap().items[0].end_offset, 17);
        c.activate(&request).await.unwrap();
        assert_eq!(
            c.busy(request.refs.as_slice()).await.unwrap().refs[0].state,
            BusyState::Idle
        );
        fake.set_diverged(reference("a"), DivergedState::Advanced);
        assert_eq!(
            c.diverged(&request).await.unwrap().refs[0].state,
            DivergedState::Advanced
        );
    }

    #[test]
    fn caller_strictness_duplicates_ref_limit_and_cut_id_are_enforced() {
        let fake = FakeParticipant::new(&["session"], vec![reference("a")]);
        for caller in ["direct", "reserved:other", "session:host"] {
            assert!(
                matches!(fake.handle(caller, "agent_data.describe", json!({})), Err(TransportError::Refusal { code, .. }) if code == "caller_forbidden")
            );
        }
        for (method, params) in [
            ("agent_data.describe", json!({"extra":true})),
            (
                "agent_data.busy",
                json!({"refs":[{"ref_kind":"session","ref":"a","extra":true}]}),
            ),
            (
                "agent_data.import",
                json!({"refs":[],"cutId":CUT,"check":{"host":"x"}}),
            ),
            (
                "agent_data.busy",
                json!({"refs":[reference("a"),reference("a")]}),
            ),
            (
                "agent_data.busy",
                json!({"refs":(0..257).map(|i| reference(&i.to_string())).collect::<Vec<_>>()}),
            ),
            (
                "agent_data.prepare",
                json!({"refs":[],"cutId":"ABCDEF0123456789abcdef0123456789"}),
            ),
            ("agent_data.prepare", json!({"refs":[],"cutId":"short"})),
            ("agent_data.describe", json!({"op":"agent_data.describe"})),
        ] {
            assert!(
                matches!(fake.handle("reserved:entorhinal", method, params), Err(TransportError::Refusal { code, .. }) if code == "invalid_request")
            );
        }
        assert!(fake
            .handle(
                "reserved:entorhinal",
                "agent_data.prepare",
                json!({"refs":[],"cutId":CUT})
            )
            .is_ok());
        assert!(fake
            .handle(
                "reserved:entorhinal",
                "agent_data.export",
                json!({"refs":[],"cutId":CUT})
            )
            .is_ok());
    }
}

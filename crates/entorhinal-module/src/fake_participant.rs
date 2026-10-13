//! Machine-local participant double. Each instance owns its data, cut state and reply cache.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Mutex;

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::log_client::{LogTransport, TransportError};
use crate::participant_client::*;

pub(crate) const REFUSAL_CODES: [&str; 13] = [
    "caller_forbidden",
    "invalid_request",
    "unknown_ref",
    "id_reused",
    "invalid_state",
    "cut_unknown",
    "cut_not_ready",
    "busy",
    "not_exportable",
    "incompatible_version",
    "digest_mismatch",
    "external_not_present",
    "unavailable",
];

#[derive(Clone)]
pub(crate) enum Effect {
    Refuse(Refusal),
    FailBefore(TransportError),
    LoseReply(TransportError),
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
    diverged: BTreeMap<AgentRef, DivergedState>,
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
}

pub(crate) struct FakeParticipant {
    state: Mutex<State>,
}

impl FakeParticipant {
    pub(crate) fn new(ref_kinds: &[&str], held: Vec<AgentRef>) -> Self {
        Self {
            state: Mutex::new(State {
                description: DescribeReply {
                    ref_kinds: ref_kinds.iter().map(|kind| (*kind).into()).collect(),
                    stores: vec![],
                    external: false,
                    detail: None,
                },
                held: held.into_iter().collect(),
                unknown_busy_for_unheld: false,
                busy: BTreeMap::new(),
                diverged: BTreeMap::new(),
                prepare: PrepareReply {
                    items: vec![],
                    external: vec![],
                },
                import: ImportReply { items: vec![] },
                enforced: true,
                cuts: BTreeMap::new(),
                replies: BTreeMap::new(),
                frozen: BTreeSet::new(),
                staged: BTreeSet::new(),
                tombstones: BTreeMap::new(),
                scripts: BTreeMap::new(),
                calls: vec![],
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
        self.state.lock().unwrap().diverged.insert(reference, state);
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
    pub(crate) fn tombstones(&self) -> BTreeMap<AgentRef, String> {
        self.state.lock().unwrap().tombstones.clone()
    }

    pub(crate) fn handle(
        &self,
        caller: &str,
        method: &str,
        params: Value,
    ) -> Result<Vec<u8>, TransportError> {
        if caller != "reserved:entorhinal" {
            return Err(refuse("caller_forbidden", None));
        }
        let mut object = params
            .as_object()
            .cloned()
            .ok_or_else(|| refuse("invalid_request", None))?;
        // The method belongs to the routed envelope, never to the params object.
        if object.contains_key("op") {
            return Err(refuse("invalid_request", None));
        }
        object.insert("op".into(), Value::String(method.into()));
        let request: Request = serde_json::from_value(Value::Object(object))
            .map_err(|_| refuse("invalid_request", None))?;
        validate(&request)?;
        let mut state = self.state.lock().unwrap();
        state.calls.push(request.clone());
        let effect = state.scripts.get_mut(method).and_then(VecDeque::pop_front);
        let recorded = request.changes_state()
            && request
                .cut()
                .is_some_and(|cut| state.replies.contains_key(&(cut.cut_id, request.op())));
        match &effect {
            Some(Effect::Refuse(refusal)) if !recorded => {
                return Err(refuse(&refusal.code, refusal.detail.clone()))
            }
            Some(Effect::FailBefore(error)) => return Err(error.clone()),
            _ => {}
        }
        let reply = execute(&mut state, request)?;
        if let Some(Effect::LoseReply(error)) = effect {
            return Err(error);
        }
        Ok(serde_json::to_vec(&json!({"result": reply})).expect("fake reply serializes"))
    }
}

#[async_trait]
impl LogTransport for FakeParticipant {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.handle("reserved:entorhinal", method, params)
    }
}

fn refuse(code: &str, detail: Option<Value>) -> TransportError {
    TransportError::Refusal {
        code: code.into(),
        detail,
    }
}

fn validate(request: &Request) -> Result<(), TransportError> {
    let refs: BTreeSet<_> = request.refs().iter().collect();
    if request.refs().len() > 256 || refs.len() != request.refs().len() {
        return Err(refuse("invalid_request", None));
    }
    if let Some(cut) = request.cut() {
        if cut.cut_id.len() != 32
            || !cut
                .cut_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(refuse("invalid_request", None));
        }
    }
    Ok(())
}

fn canonicalize(mut request: Request) -> Request {
    match &mut request {
        Request::Describe(_) => {}
        Request::Busy(r) => r.refs.sort(),
        Request::Release(r) => r.refs.sort(),
        Request::Status(r)
        | Request::Prepare(r)
        | Request::Export(r)
        | Request::Diverged(r)
        | Request::Import(r)
        | Request::Activate(r)
        | Request::Abort(r) => r.refs.sort(),
    }
    request
}

fn invalid_state(cut: &CutState) -> TransportError {
    refuse(
        "invalid_state",
        Some(json!({"step": cut.step, "refs": cut.refs})),
    )
}

fn execute(state: &mut State, request: Request) -> Result<Value, TransportError> {
    let cut = request.cut();
    let refs: BTreeSet<_> = request.refs().iter().cloned().collect();
    let key = cut.as_ref().map(|cut| (cut.cut_id.clone(), request.op()));
    let canonical = canonicalize(request.clone());
    if request.changes_state() {
        if let Some((first, reply)) = key.as_ref().and_then(|key| state.replies.get(key)) {
            return if first == &canonical {
                Ok(reply.clone())
            } else {
                Err(refuse("id_reused", None))
            };
        }
    }
    let existing = cut.as_ref().and_then(|cut| state.cuts.get(&cut.cut_id));
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
                        return Err(refuse("unknown_ref", Some(json!(reference))));
                    }
                    result.push(BusyRef {
                        state: BusyState::Unknown,
                        reason_code: Some("unheld".into()),
                        reason: Some("No local data".into()),
                    });
                } else {
                    result.push(state.busy.get(reference).cloned().unwrap_or(BusyRef {
                        state: BusyState::Idle,
                        reason_code: None,
                        reason: None,
                    }));
                }
            }
            json!(BusyReply { refs: result })
        }
        Request::Status(_) => json!(StatusReply { step }),
        Request::Diverged(request) => json!(DivergedReply {
            refs: request
                .refs
                .iter()
                .map(|reference| DivergedRef {
                    state: if state.held.contains(reference) {
                        state
                            .diverged
                            .get(reference)
                            .copied()
                            .unwrap_or(DivergedState::AtCut)
                    } else {
                        DivergedState::AtCut
                    },
                })
                .collect()
        }),
        Request::Prepare(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::None)?;
            for reference in &refs {
                if !state.held.contains(reference) {
                    return Err(refuse("unknown_ref", Some(json!(reference))));
                }
            }
            let busy: Vec<_> = refs.iter().filter_map(|reference| state.busy.get(reference)
                .filter(|busy| busy.state != BusyState::Idle).map(|busy| json!({"ref_kind": reference.ref_kind, "ref": reference.reference, "state": busy.state, "reason_code": busy.reason_code, "reason": busy.reason}))).collect();
            if !busy.is_empty() {
                return Err(refuse("busy", Some(json!({"refs": busy}))));
            }
            state.frozen.extend(refs.clone());
            json!(state.prepare)
        }
        Request::Export(_) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::Prepared)?;
            let prepare =
                &state.replies[&(cut.as_ref().unwrap().cut_id.clone(), "agent_data.prepare")].1;
            let prepare: PrepareReply =
                serde_json::from_value(prepare.clone()).expect("cached prepare is typed");
            json!(ExportReply {
                items: prepare
                    .items
                    .into_iter()
                    .map(|item| ExportItem {
                        session_ref: item.session_ref,
                        end_offset: item.end_offset
                    })
                    .collect()
            })
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
            json!({})
        }
        Request::Abort(_) => {
            if matches!(step, Step::Activated | Step::Released) {
                return Err(invalid_state(existing.unwrap()));
            }
            for reference in &refs {
                state.frozen.remove(reference);
                state.staged.remove(reference);
            }
            json!({})
        }
        Request::Release(request) => {
            require_step(state, cut.as_ref().unwrap(), step, Step::Exported)?;
            for reference in &refs {
                state.frozen.remove(reference);
                state
                    .tombstones
                    .insert(reference.clone(), request.holder.clone());
            }
            json!(ReleaseReply {
                enforced: state.enforced
            })
        }
    };
    if request.changes_state() {
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
        state.cuts.insert(cut.cut_id, CutState { refs, step });
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
    match state.cuts.get(&cut.cut_id) {
        Some(cut) => Err(invalid_state(cut)),
        None => Err(refuse("cut_unknown", None)),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;
    use crate::participant_discovery::Participant;

    const CUT: &str = "0123456789abcdef0123456789abcdef";
    fn reference(name: &str) -> AgentRef {
        AgentRef {
            ref_kind: "session".into(),
            reference: name.into(),
        }
    }
    fn cut(refs: &[&str]) -> CutRequest {
        CutRequest {
            refs: refs.iter().map(|r| reference(r)).collect(),
            cut_id: CUT.into(),
        }
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
        let original = PrepareReply {
            items: vec![],
            external: vec![ExternalRef {
                ref_kind: "session".into(),
                reference: "a".into(),
                note: "Copy host data".into(),
                check: json!({"v":1}),
            }],
        };
        fake.set_prepare(original.clone());
        assert_eq!(c.prepare(&cut(&["a", "b"])).await.unwrap(), original);
        fake.set_prepare(PrepareReply {
            items: vec![],
            external: vec![],
        });
        assert_eq!(c.prepare(&cut(&["b", "a"])).await.unwrap(), original);
        refusal(c.prepare(&cut(&["a"])).await, "id_reused");
        c.export(&cut(&["b", "a"])).await.unwrap();
        let release = ReleaseRequest {
            refs: cut(&["a", "b"]).refs,
            cut_id: CUT.into(),
            holder: "mac-b".into(),
        };
        assert!(c.release(&release).await.unwrap().enforced);
        fake.set_enforced(false);
        assert!(
            c.release(&ReleaseRequest {
                refs: cut(&["b", "a"]).refs,
                ..release.clone()
            })
            .await
            .unwrap()
            .enforced
        );
        refusal(
            c.release(&ReleaseRequest {
                holder: "mac-c".into(),
                ..release
            })
            .await,
            "id_reused",
        );
        fake.script(
            "agent_data.prepare",
            Effect::Refuse(Refusal {
                code: "busy".into(),
                detail: None,
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
            ["mac-b", "mac-b"]
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
        for result in [
            a.export(&wrong).await.map(|_| ()),
            a.abort(&wrong).await.map(|_| ()),
            a.status(&wrong).await.map(|_| ()),
            a.diverged(&wrong).await.map(|_| ()),
        ] {
            let detail = refusal(result, "invalid_state").unwrap();
            assert_eq!(detail["step"], "prepared");
            assert_eq!(
                detail["refs"],
                json!([{"ref_kind":"session","ref":"a"},{"ref_kind":"session","ref":"b"}])
            );
        }
        a.export(&cut(&["a", "b"])).await.unwrap();
        refusal(
            a.release(&ReleaseRequest {
                refs: wrong.refs.clone(),
                cut_id: CUT.into(),
                holder: "b".into(),
            })
            .await,
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
            a.release(&ReleaseRequest {
                refs: request.refs.clone(),
                cut_id: CUT.into(),
                holder: "mac-b".into(),
            })
            .await,
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
            a.release(&ReleaseRequest {
                refs: request.refs.clone(),
                cut_id: CUT.into(),
                holder: "mac-b".into(),
            })
            .await,
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
        a.release(&ReleaseRequest {
            refs: request.refs.clone(),
            cut_id: CUT.into(),
            holder: "mac-b".into(),
        })
        .await
        .unwrap();
        assert_eq!(
            refusal(a.abort(&request).await, "invalid_state").unwrap()["step"],
            "released"
        );
        assert!(source.frozen().is_empty());
        assert!(receiver.tombstones().is_empty());
        assert_eq!(source.tombstones()[&reference("a")], "mac-b");
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
                    code: code.into(),
                    detail: Some(json!({"step":"none","note":"scripted refusal"})),
                }),
            );
            assert_eq!(
                refusal(c.prepare(&cut(&["a"])).await, code).unwrap()["note"],
                "scripted refusal"
            );
            assert!(fake.cut(CUT).is_none());
            assert!(fake.frozen().is_empty());
        }
        fake.set_busy(
            reference("a"),
            BusyRef {
                state: BusyState::Busy,
                reason_code: Some("run_active".into()),
                reason: Some("Run active".into()),
            },
        );
        let busy = refusal(c.prepare(&cut(&["a"])).await, "busy").unwrap();
        assert_eq!(busy["refs"][0]["reason_code"], "run_active");
        fake.set_busy(
            reference("a"),
            BusyRef {
                state: BusyState::Idle,
                reason_code: None,
                reason: None,
            },
        );
        c.prepare(&cut(&["a"])).await.unwrap();
        assert_eq!(fake.cut(CUT).unwrap().step, Step::Prepared);
    }

    #[tokio::test]
    async fn fresh_receiver_can_report_unknown_busy_but_accepts_diverged_and_import() {
        let fake = Arc::new(FakeParticipant::new(&["session"], vec![]));
        fake.unknown_busy_for_unheld(true);
        let c = client(fake.clone());
        let request = cut(&["a"]);
        let reply = c.busy(&request.refs).await.unwrap();
        assert_eq!(reply.refs[0].state, BusyState::Unknown);
        assert_eq!(reply.refs[0].reason_code.as_deref(), Some("unheld"));
        assert_eq!(
            c.diverged(&request).await.unwrap().refs[0].state,
            DivergedState::AtCut
        );
        fake.set_import(ImportReply {
            items: vec![ImportItem {
                session_ref: "s".into(),
                end_offset: 17,
                digest: "digest".into(),
            }],
        });
        assert_eq!(c.import(&request).await.unwrap().items[0].end_offset, 17);
        c.activate(&request).await.unwrap();
        assert_eq!(
            c.busy(&request.refs).await.unwrap().refs[0].state,
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

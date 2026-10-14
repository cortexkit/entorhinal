//! Agent-data/v1 participant calls using the published wire contract.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;

use cortexkit_role_agent_data::errors::{allowed_refusals, RefusalCode};
#[cfg(test)]
pub(crate) use cortexkit_role_agent_data::{
    busy::{BusyEntry as BusyRef, BusyState},
    describe::Store as StoreDescription,
    diverged::{DivergedEntry as DivergedRef, DivergedState},
    export::ExportedItem as ExportItem,
    import::ImportedItem as ImportItem,
    prepare::{ExternalItem as ExternalRef, PreparedItem as PrepareItem},
    refs::CutId,
    step::Step,
};
pub(crate) use cortexkit_role_agent_data::{
    busy::{BusyReply, BusyRequest},
    describe::{DescribeReply, DescribeRequest},
    diverged::DivergedReply,
    export::ExportReply,
    import::ImportReply,
    prepare::PrepareReply,
    refs::{Ref as AgentRef, Refs},
    release::{ReleaseReply, ReleaseRequest},
    request::{CutRequest, Request},
    status::StatusReply,
    EmptyReply as Empty, Op,
};

use crate::log_client::{LogTransport, TransportError};
use crate::participant_discovery::Participant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Refusal {
    pub(crate) code: String,
    pub(crate) detail: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) enum ClientError {
    Refused(Refusal),
    ContractViolation {
        op: &'static str,
        code: String,
        detail: Option<Value>,
    },
    OutcomeUnknown,
    InvalidReply(String),
}

fn classify_refusal(op: Op, code: String, detail: Option<Value>) -> ClientError {
    let refusal_code = RefusalCode::from_code(&code);
    if allowed_refusals(op).contains(&refusal_code) {
        ClientError::Refused(Refusal { code, detail })
    } else {
        ClientError::ContractViolation {
            op: op.method(),
            code,
            detail,
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReplyEnvelope<T> {
    result: T,
}

#[derive(Clone)]
pub(crate) struct ParticipantClient {
    pub(crate) module_id: String,
    transport: Arc<dyn LogTransport>,
    unresolved: Arc<Mutex<BTreeSet<String>>>,
}

impl ParticipantClient {
    pub(crate) fn new(participant: &Participant) -> Self {
        Self {
            module_id: participant.module_id.clone(),
            transport: participant.transport.clone(),
            unresolved: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    async fn once<T: DeserializeOwned>(&self, request: &Request) -> Result<T, ClientError> {
        let params = request.params();
        let op = request.op();
        let bytes =
            self.transport
                .call(op.method(), params)
                .await
                .map_err(|error| match error {
                    TransportError::Refusal { code, detail } => classify_refusal(op, code, detail),
                    TransportError::NoReply | TransportError::Unavailable => {
                        ClientError::OutcomeUnknown
                    }
                })?;
        serde_json::from_slice::<ReplyEnvelope<T>>(&bytes)
            .map(|reply| reply.result)
            .map_err(|error| ClientError::InvalidReply(error.to_string()))
    }

    async fn reconcile(&self, cut: &CutRequest) -> Result<(), ClientError> {
        self.once::<StatusReply>(&Request::Status(cut.clone()))
            .await
            .map_err(|error| match error {
                violation @ ClientError::ContractViolation { .. } => violation,
                _ => ClientError::OutcomeUnknown,
            })?;
        self.unresolved.lock().unwrap().remove(cut.cut_id.as_str());
        Ok(())
    }

    async fn call<T: DeserializeOwned>(&self, request: Request) -> Result<T, ClientError> {
        let cut = request.cut_id().map(|cut_id| {
            CutRequest::new(
                request
                    .refs()
                    .expect("every request with a cut id carries refs")
                    .clone(),
                cut_id.clone(),
            )
        });
        let is_status = request.op() == Op::Status;
        // A failed status probe leaves the cut unresolved across calls and client clones.
        // The next attempt must obtain status before sending another op for that cut.
        if !is_status {
            if let Some(cut) = &cut {
                let unresolved = self
                    .unresolved
                    .lock()
                    .unwrap()
                    .contains(cut.cut_id.as_str());
                if unresolved {
                    self.reconcile(cut).await?;
                }
            }
        }
        // Status cannot supply the first op's full reply. Reconcile the step, then resend
        // the identical request to retrieve the participant's durable idempotent reply.
        // Bound local recovery; a still-unknown outcome belongs to the driver's backoff.
        for _ in 0..2 {
            match self.once(&request).await {
                Err(ClientError::OutcomeUnknown) => {
                    if let Some(cut) = &cut {
                        self.unresolved
                            .lock()
                            .unwrap()
                            .insert(cut.cut_id.as_str().to_owned());
                        if !is_status {
                            self.reconcile(cut).await?;
                        }
                    }
                }
                result => {
                    if result.is_ok() && is_status {
                        if let Some(cut) = &cut {
                            self.unresolved.lock().unwrap().remove(cut.cut_id.as_str());
                        }
                    }
                    return result;
                }
            }
        }
        Err(ClientError::OutcomeUnknown)
    }

    pub(crate) async fn describe(&self) -> Result<DescribeReply, ClientError> {
        self.call(Request::Describe(DescribeRequest::new())).await
    }
    pub(crate) async fn busy(&self, refs: &[AgentRef]) -> Result<BusyReply, ClientError> {
        let request = BusyRequest::new(
            Refs::new(refs.to_vec())
                .map_err(|e| ClientError::InvalidReply(format!("invalid refs: {e}")))?,
        );
        let reply = self
            .call::<BusyReply>(Request::Busy(request.clone()))
            .await?;
        if reply.answers(&request) {
            Ok(reply)
        } else {
            Err(ClientError::InvalidReply(
                "busy reply must contain one entry per requested ref".into(),
            ))
        }
    }
    pub(crate) async fn status(&self, cut: &CutRequest) -> Result<StatusReply, ClientError> {
        self.call(Request::Status(cut.clone())).await
    }
    pub(crate) async fn prepare(&self, cut: &CutRequest) -> Result<PrepareReply, ClientError> {
        self.call(Request::Prepare(cut.clone())).await
    }
    pub(crate) async fn export(&self, cut: &CutRequest) -> Result<ExportReply, ClientError> {
        self.call(Request::Export(cut.clone())).await
    }
    pub(crate) async fn diverged(&self, cut: &CutRequest) -> Result<DivergedReply, ClientError> {
        let reply = self
            .call::<DivergedReply>(Request::Diverged(cut.clone()))
            .await?;
        if reply.answers(cut) {
            Ok(reply)
        } else {
            Err(ClientError::InvalidReply(
                "diverged reply must contain one entry per requested ref".into(),
            ))
        }
    }
    pub(crate) async fn import(&self, cut: &CutRequest) -> Result<ImportReply, ClientError> {
        self.call(Request::Import(cut.clone())).await
    }
    pub(crate) async fn activate(&self, cut: &CutRequest) -> Result<Empty, ClientError> {
        self.call(Request::Activate(cut.clone())).await
    }
    pub(crate) async fn abort(&self, cut: &CutRequest) -> Result<Empty, ClientError> {
        self.call(Request::Abort(cut.clone())).await
    }
    pub(crate) async fn release(
        &self,
        request: &ReleaseRequest,
    ) -> Result<ReleaseReply, ClientError> {
        self.call(Request::Release(request.clone())).await
    }
}

pub(crate) struct PlannedParticipant {
    pub(crate) client: ParticipantClient,
    pub(crate) refs: Vec<AgentRef>,
    pub(crate) description: DescribeReply,
}

#[derive(Debug)]
pub(crate) enum PlanError {
    Participant {
        module_id: String,
        error: ClientError,
    },
    RefKindUnavailable(String),
}

/// Describe only the available participants supplied by discovery, and retain every
/// declaring participant, not just the first owner of a kind. Empty ref sets are valid.
pub(crate) async fn plan_participants(
    available: &[Participant],
    refs: &[AgentRef],
) -> Result<Vec<PlannedParticipant>, PlanError> {
    let refs: BTreeSet<_> = refs.iter().cloned().collect();
    let available: BTreeMap<_, _> = available.iter().map(|p| (&p.module_id, p)).collect();
    let mut covered = BTreeSet::new();
    let mut plan = Vec::new();
    for participant in available.values() {
        let client = ParticipantClient::new(participant);
        let description = client
            .describe()
            .await
            .map_err(|error| PlanError::Participant {
                module_id: participant.module_id.clone(),
                error,
            })?;
        let routed: Vec<_> = refs
            .iter()
            .filter(|r| description.ref_kinds.contains(&r.ref_kind))
            .cloned()
            .collect();
        covered.extend(routed.iter().cloned());
        plan.push(PlannedParticipant {
            client,
            refs: routed,
            description,
        });
    }
    if let Some(missing) = refs.difference(&covered).next() {
        return Err(PlanError::RefKindUnavailable(missing.ref_kind.clone()));
    }
    Ok(plan)
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::sync::Mutex;

    use async_trait::async_trait;
    use serde::Serialize;
    use serde_json::json;

    use super::*;
    use crate::fake_participant::{Effect, FakeParticipant};

    const CUT: &str = "0123456789abcdef0123456789abcdef";
    const CUT_JSON: &str = r#"{"refs":[{"ref_kind":"session","ref":"s1"}],"cutId":"0123456789abcdef0123456789abcdef"}"#;
    const PREPARE_JSON: &str = r#"{"items":[{"ref_kind":"session","ref":"s1","sessionRef":"00112233445566778899aabbccddeeff","stream":"reserved:adapter","endOffset":42,"digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","format":"rows/v1","frozen":true}],"external":[{"ref_kind":"session","ref":"s1","note":"Copy the transcript with the project","check":{"host_session":"native-1","last_row":7,"future":{"version":2}}}]}"#;

    pub(super) fn reference(name: &str) -> AgentRef {
        AgentRef::new("session", name)
    }
    fn cut() -> CutRequest {
        serde_json::from_str(CUT_JSON).unwrap()
    }
    fn cut_with_refs(refs: Vec<AgentRef>, cut_id: &str) -> CutRequest {
        CutRequest::new(
            Refs::new(refs).unwrap(),
            CutId::new(cut_id).expect("test cut id is valid"),
        )
    }
    fn release_request(cut: CutRequest, holder: &str) -> ReleaseRequest {
        ReleaseRequest::new(cut.refs, cut.cut_id, holder)
    }
    fn client(fake: Arc<FakeParticipant>) -> ParticipantClient {
        ParticipantClient::new(&Participant {
            module_id: "adapter".into(),
            transport: fake,
        })
    }
    fn round_trip<T: Serialize + DeserializeOwned>(literal: &str) -> T {
        let value: T = serde_json::from_str(literal).unwrap();
        assert_eq!(
            serde_json::to_value(&value).unwrap(),
            serde_json::from_str::<Value>(literal).unwrap()
        );
        value
    }

    struct FixtureTransport {
        fixtures: Mutex<VecDeque<(&'static str, &'static str, &'static str)>>,
    }
    #[async_trait]
    impl LogTransport for FixtureTransport {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let (expected_method, expected_params, reply) = self
                .fixtures
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected op");
            assert_eq!(method, expected_method);
            assert_eq!(
                params,
                serde_json::from_str::<Value>(expected_params).unwrap()
            );
            Ok(format!("{{\"result\":{reply}}}").into_bytes())
        }
    }

    struct VectorTransport {
        method: String,
        params: Value,
        result: Value,
    }

    #[async_trait]
    impl LogTransport for VectorTransport {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            assert_eq!(method, self.method);
            assert_eq!(params, self.params);
            Ok(serde_json::to_vec(&json!({"result": self.result.clone()})).unwrap())
        }
    }

    async fn round_trip_vector(request: &Request, params: Value, result: Value, valid_reply: bool) {
        let transport = Arc::new(VectorTransport {
            method: request.op().method().into(),
            params,
            result: result.clone(),
        });
        let client = ParticipantClient::new(&Participant {
            module_id: "vector-fixture".into(),
            transport,
        });
        macro_rules! once {
            ($reply:ty) => {
                client
                    .once::<$reply>(request)
                    .await
                    .map(|reply| serde_json::to_value(reply).unwrap())
            };
        }
        let decoded = match request.op() {
            Op::Describe => once!(DescribeReply),
            Op::Busy => once!(BusyReply),
            Op::Status => once!(StatusReply),
            Op::Prepare => once!(PrepareReply),
            Op::Export => once!(ExportReply),
            Op::Diverged => once!(DivergedReply),
            Op::Import => once!(ImportReply),
            Op::Activate | Op::Abort => once!(Empty),
            Op::Release => once!(ReleaseReply),
        };
        match decoded {
            Ok(decoded) => {
                assert!(valid_reply, "accepted invalid reply vector: {result}");
                assert_eq!(decoded, result);
            }
            Err(ClientError::InvalidReply(_)) => {
                assert!(!valid_reply, "rejected valid reply vector: {result}");
            }
            Err(other) => panic!("vector call failed unexpectedly: {other:?}"),
        }
    }

    fn vector_params(vector: &Value) -> Value {
        let mut params = vector["params"].clone();
        if let Some(count) = vector["expand_refs"].as_u64() {
            params["refs"] = json!((0..count)
                .map(|index| {
                    json!({
                        "ref_kind": "broca.session",
                        "ref": format!("ses_{index}"),
                    })
                })
                .collect::<Vec<_>>());
        }
        params
    }

    fn vector_reply_for(method: &str) -> Value {
        match method {
            cortexkit_role_agent_data::ops::DESCRIBE => {
                json!({"ref_kinds":[],"stores":[],"external":false})
            }
            cortexkit_role_agent_data::ops::BUSY => json!({"refs": []}),
            cortexkit_role_agent_data::ops::STATUS => json!({"step":"none"}),
            cortexkit_role_agent_data::ops::PREPARE => {
                json!({"items": [], "external": []})
            }
            cortexkit_role_agent_data::ops::EXPORT => json!({"items": []}),
            cortexkit_role_agent_data::ops::DIVERGED => json!({"refs": []}),
            cortexkit_role_agent_data::ops::IMPORT => json!({"items": []}),
            cortexkit_role_agent_data::ops::ACTIVATE | cortexkit_role_agent_data::ops::ABORT => {
                json!({})
            }
            cortexkit_role_agent_data::ops::RELEASE => json!({"enforced": true}),
            _ => unreachable!("request vectors only contain contract operations"),
        }
    }

    #[tokio::test]
    async fn published_test_vectors_round_trip_through_client_encoding_and_decoding() {
        use cortexkit_role_agent_data::vectors;

        let requests: Value =
            serde_json::from_str(vectors::file("requests.json").unwrap()).unwrap();
        let replies: Value = serde_json::from_str(vectors::file("replies.json").unwrap()).unwrap();
        let mut requests_by_method = std::collections::BTreeMap::new();

        for vector in requests["requests"].as_array().unwrap() {
            let method = vector["method"].as_str().unwrap();
            let params = vector_params(vector);
            let request = Request::decode(method, &params)
                .unwrap_or_else(|error| panic!("{}: {error:?}", vector["name"]));
            assert_eq!(request.params(), params, "{}", vector["name"]);
            requests_by_method
                .entry(method.to_owned())
                .or_insert_with(|| (request.clone(), params.clone()));
            round_trip_vector(&request, params, vector_reply_for(method), true).await;
        }

        for vector in requests["refused"].as_array().unwrap() {
            let method = vector["method"].as_str().unwrap();
            let params = vector_params(vector);
            let error = Request::decode(method, &params).unwrap_err();
            assert_eq!(
                error.code,
                RefusalCode::InvalidRequest,
                "{}",
                vector["name"]
            );
        }

        for vector in replies["replies"].as_array().unwrap() {
            let method = vector["method"].as_str().unwrap();
            let (request, params) = &requests_by_method[method];
            round_trip_vector(request, params.clone(), vector["reply"].clone(), true).await;
        }

        for vector in replies["refused"].as_array().unwrap() {
            let method = vector["method"].as_str().unwrap();
            let (request, params) = &requests_by_method[method];
            round_trip_vector(request, params.clone(), vector["reply"].clone(), false).await;
        }
    }

    #[tokio::test]
    async fn all_ten_ops_round_trip_literal_contract_fixtures() {
        let fixtures = Arc::new(FixtureTransport {
            fixtures: Mutex::new(VecDeque::from([
                (
                    "agent_data.describe",
                    "{}",
                    r#"{"ref_kinds":["session"],"stores":[{"name":"rows","sync_class":"per_agent"}],"external":true,"detail":{"future":[1,{"x":true}]}}"#,
                ),
                (
                    "agent_data.busy",
                    r#"{"refs":[{"ref_kind":"session","ref":"s1"}]}"#,
                    r#"{"refs":[{"state":"busy","reason_code":"turn_in_flight","reason":"Turn still running"}]}"#,
                ),
                ("agent_data.status", CUT_JSON, r#"{"step":"none"}"#),
                ("agent_data.prepare", CUT_JSON, PREPARE_JSON),
                (
                    "agent_data.export",
                    CUT_JSON,
                    r#"{"items":[{"sessionRef":"00112233445566778899aabbccddeeff","endOffset":42}]}"#,
                ),
                (
                    "agent_data.diverged",
                    CUT_JSON,
                    r#"{"refs":[{"state":"at_cut"}]}"#,
                ),
                (
                    "agent_data.import",
                    CUT_JSON,
                    r#"{"items":[{"sessionRef":"00112233445566778899aabbccddeeff","endOffset":42,"digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]}"#,
                ),
                ("agent_data.activate", CUT_JSON, "{}"),
                ("agent_data.abort", CUT_JSON, "{}"),
                (
                    "agent_data.release",
                    r#"{"refs":[{"ref_kind":"session","ref":"s1"}],"cutId":"0123456789abcdef0123456789abcdef","holder":"mac-b"}"#,
                    r#"{"enforced":false}"#,
                ),
            ])),
        });
        let client = ParticipantClient::new(&Participant {
            module_id: "adapter".into(),
            transport: fixtures.clone(),
        });
        let cut = round_trip::<CutRequest>(CUT_JSON);
        let describe = client.describe().await.unwrap();
        assert_eq!(describe.detail, Some(json!({"future":[1,{"x":true}]})));
        let busy = client.busy(cut.refs.as_slice()).await.unwrap();
        assert_eq!(busy.refs[0].state, BusyState::Busy);
        assert_eq!(busy.refs[0].reason_code.as_deref(), Some("turn_in_flight"));
        assert_eq!(busy.refs[0].reason.as_deref(), Some("Turn still running"));
        assert_eq!(client.status(&cut).await.unwrap().step, Step::None);
        assert_eq!(
            client.prepare(&cut).await.unwrap(),
            round_trip::<PrepareReply>(PREPARE_JSON)
        );
        assert_eq!(
            client.export(&cut).await.unwrap(),
            round_trip::<ExportReply>(
                r#"{"items":[{"sessionRef":"00112233445566778899aabbccddeeff","endOffset":42}]}"#
            )
        );
        assert_eq!(
            client.diverged(&cut).await.unwrap(),
            round_trip::<DivergedReply>(r#"{"refs":[{"state":"at_cut"}]}"#)
        );
        assert_eq!(
            client.import(&cut).await.unwrap(),
            round_trip::<ImportReply>(
                r#"{"items":[{"sessionRef":"00112233445566778899aabbccddeeff","endOffset":42,"digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"}]}"#
            )
        );
        assert_eq!(client.activate(&cut).await.unwrap(), Empty::new());
        assert_eq!(client.abort(&cut).await.unwrap(), Empty::new());
        let release = round_trip::<ReleaseRequest>(
            r#"{"refs":[{"ref_kind":"session","ref":"s1"}],"cutId":"0123456789abcdef0123456789abcdef","holder":"mac-b"}"#,
        );
        assert!(!client.release(&release).await.unwrap().enforced);
        assert!(fixtures.fixtures.lock().unwrap().is_empty());

        for literal in [
            r#"{"op":"agent_data.describe"}"#,
            r#"{"op":"agent_data.busy","refs":[]}"#,
            r#"{"op":"agent_data.status","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.prepare","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.export","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.diverged","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.import","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.activate","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.abort","refs":[],"cutId":"0123456789abcdef0123456789abcdef"}"#,
            r#"{"op":"agent_data.release","refs":[],"cutId":"0123456789abcdef0123456789abcdef","holder":"mac-b"}"#,
        ] {
            let mut value: Value = serde_json::from_str(literal).unwrap();
            let method = value
                .as_object_mut()
                .unwrap()
                .remove("op")
                .unwrap()
                .as_str()
                .unwrap()
                .to_owned();
            let request = Request::decode(&method, &value).unwrap();
            assert_eq!(request.op().method(), method);
            assert_eq!(request.params(), value);
        }
        for literal in [
            r#"{"step":"none"}"#,
            r#"{"step":"prepared"}"#,
            r#"{"step":"exported"}"#,
            r#"{"step":"imported"}"#,
            r#"{"step":"activated"}"#,
            r#"{"step":"aborted"}"#,
            r#"{"step":"released"}"#,
        ] {
            round_trip::<StatusReply>(literal);
        }
        round_trip::<cortexkit_role_agent_data::errors::ErrorBody>(
            r#"{"code":"invalid_state","message":"state does not allow this operation","detail":{"step":"activated","future":{"x":1}}}"#,
        );
        round_trip::<BusyReply>(
            r#"{"refs":[{"state":"idle"},{"state":"unknown","reason_code":"liveness_unknown"}]}"#,
        );
        round_trip::<DivergedReply>(r#"{"refs":[{"state":"advanced"},{"state":"unknown"}]}"#);
    }

    fn rejects_extra<T: DeserializeOwned>(literal: &str) {
        let mut value: Value = serde_json::from_str(literal).unwrap();
        value
            .as_object_mut()
            .unwrap()
            .insert("unexpected".into(), json!(true));
        assert!(
            serde_json::from_value::<T>(value).is_err(),
            "{} accepted an unknown field",
            std::any::type_name::<T>()
        );
    }

    #[test]
    fn every_typed_object_rejects_unknown_fields_but_metadata_is_opaque() {
        rejects_extra::<Empty>("{}");
        rejects_extra::<AgentRef>(r#"{"ref_kind":"session","ref":"s1"}"#);
        rejects_extra::<BusyRequest>(r#"{"refs":[]}"#);
        rejects_extra::<CutRequest>(CUT_JSON);
        rejects_extra::<ReleaseRequest>(
            r#"{"refs":[],"cutId":"0123456789abcdef0123456789abcdef","holder":"b"}"#,
        );
        rejects_extra::<StoreDescription>(r#"{"name":"rows","sync_class":"per_agent"}"#);
        rejects_extra::<DescribeReply>(r#"{"ref_kinds":[],"stores":[],"external":false}"#);
        rejects_extra::<BusyRef>(r#"{"state":"idle"}"#);
        rejects_extra::<BusyReply>(r#"{"refs":[]}"#);
        rejects_extra::<StatusReply>(r#"{"step":"none"}"#);
        rejects_extra::<PrepareItem>(
            r#"{"ref_kind":"session","ref":"s1","sessionRef":"s","stream":"reserved:adapter","endOffset":0,"digest":"d","format":"v1","frozen":false}"#,
        );
        rejects_extra::<ExternalRef>(
            r#"{"ref_kind":"session","ref":"s1","note":"Copy","check":{}}"#,
        );
        rejects_extra::<PrepareReply>(PREPARE_JSON);
        rejects_extra::<ExportItem>(r#"{"sessionRef":"s","endOffset":0}"#);
        rejects_extra::<ExportReply>(r#"{"items":[]}"#);
        rejects_extra::<DivergedRef>(r#"{"state":"at_cut"}"#);
        rejects_extra::<DivergedReply>(r#"{"refs":[]}"#);
        rejects_extra::<ImportItem>(r#"{"sessionRef":"s","endOffset":0,"digest":"d"}"#);
        rejects_extra::<ImportReply>(r#"{"items":[]}"#);
        rejects_extra::<ReleaseReply>(r#"{"enforced":true}"#);
        for (op, extra) in [
            ("agent_data.diverged", "check"),
            ("agent_data.import", "format"),
            ("agent_data.activate", "check"),
        ] {
            let mut request: Value = serde_json::from_str(CUT_JSON).unwrap();
            request[extra] = json!({"should_not_be_sent":true});
            assert!(Request::decode(op, &request).is_err());
        }
        let mut reply: Value = serde_json::from_str(PREPARE_JSON).unwrap();
        reply["items"][0]["format"] = json!("new opaque format metadata");
        let reply: PrepareReply = serde_json::from_value(reply).unwrap();
        assert_eq!(reply.items[0].format, "new opaque format metadata");
        assert!(serde_json::from_str::<StatusReply>(r#"{"step":"staged"}"#).is_err());
    }

    #[tokio::test]
    async fn busy_and_diverged_replies_must_answer_every_requested_ref() {
        let fixtures = Arc::new(FixtureTransport {
            fixtures: Mutex::new(VecDeque::from([
                (
                    cortexkit_role_agent_data::ops::BUSY,
                    r#"{"refs":[{"ref_kind":"session","ref":"s1"}]}"#,
                    r#"{"refs":[]}"#,
                ),
                (
                    cortexkit_role_agent_data::ops::DIVERGED,
                    CUT_JSON,
                    r#"{"refs":[]}"#,
                ),
            ])),
        });
        let client = ParticipantClient::new(&Participant {
            module_id: "adapter".into(),
            transport: fixtures,
        });
        assert!(matches!(
            client.busy(&[reference("s1")]).await,
            Err(ClientError::InvalidReply(_))
        ));
        assert!(matches!(
            client.diverged(&cut()).await,
            Err(ClientError::InvalidReply(_))
        ));
    }

    #[tokio::test]
    async fn unknown_outcomes_probe_status_before_identical_resend() {
        for error in [TransportError::NoReply, TransportError::Unavailable] {
            for after_execution in [false, true] {
                let fake = Arc::new(FakeParticipant::new(&["session"], vec![reference("s1")]));
                fake.set_prepare(round_trip(PREPARE_JSON));
                fake.script(
                    "agent_data.prepare",
                    if after_execution {
                        Effect::LoseReply(error.clone())
                    } else {
                        Effect::FailBefore(error.clone())
                    },
                );
                let client = client(fake.clone());
                assert_eq!(
                    client.prepare(&cut()).await.unwrap(),
                    round_trip::<PrepareReply>(PREPARE_JSON)
                );
                let calls = fake.calls();
                assert_eq!(
                    calls
                        .iter()
                        .map(|request| request.op().method())
                        .collect::<Vec<_>>(),
                    [
                        "agent_data.prepare",
                        "agent_data.status",
                        "agent_data.prepare"
                    ]
                );
                assert_eq!(calls[0], calls[2]);
                fake.set_prepare(PrepareReply::new(vec![], vec![]));
                assert_eq!(
                    client.prepare(&cut()).await.unwrap(),
                    round_trip::<PrepareReply>(PREPARE_JSON)
                );
            }
        }
    }

    #[tokio::test]
    async fn failed_status_probe_does_not_resend_or_turn_unknown_into_a_refusal() {
        let fake = Arc::new(FakeParticipant::new(&["session"], vec![reference("s1")]));
        let c = client(fake.clone());
        fake.script(
            "agent_data.prepare",
            Effect::LoseReply(TransportError::NoReply),
        );
        fake.script(
            "agent_data.status",
            Effect::Refuse(Refusal {
                code: "unavailable".into(),
                detail: None,
            }),
        );
        assert!(matches!(
            c.prepare(&cut()).await,
            Err(ClientError::OutcomeUnknown)
        ));
        assert_eq!(
            fake.calls()
                .iter()
                .map(|request| request.op().method())
                .collect::<Vec<_>>(),
            ["agent_data.prepare", "agent_data.status"]
        );
        assert_eq!(fake.cut(CUT).unwrap().step, Step::Prepared);
        c.clone().prepare(&cut()).await.unwrap();
        assert_eq!(
            fake.calls()
                .iter()
                .map(|request| request.op().method())
                .collect::<Vec<_>>(),
            [
                "agent_data.prepare",
                "agent_data.status",
                "agent_data.status",
                "agent_data.prepare"
            ]
        );
    }

    #[tokio::test]
    async fn lost_replies_for_all_state_changes_are_reconciled_and_return_first_reply() {
        let source = Arc::new(FakeParticipant::new(&["session"], vec![reference("s1")]));
        let receiver = Arc::new(FakeParticipant::new(&["session"], vec![]));
        let a = client(source.clone());
        let b = client(receiver.clone());
        for (fake, op) in [
            (&source, "agent_data.prepare"),
            (&source, "agent_data.export"),
            (&source, "agent_data.release"),
            (&receiver, "agent_data.import"),
            (&receiver, "agent_data.activate"),
        ] {
            fake.script(op, Effect::LoseReply(TransportError::NoReply));
        }
        a.prepare(&cut()).await.unwrap();
        a.export(&cut()).await.unwrap();
        a.release(&release_request(cut(), "mac-b")).await.unwrap();
        b.import(&cut()).await.unwrap();
        b.activate(&cut()).await.unwrap();
        let abort = cut_with_refs(cut().refs.into_vec(), "ffffffffffffffffffffffffffffffff");
        receiver.script(
            "agent_data.abort",
            Effect::LoseReply(TransportError::NoReply),
        );
        b.abort(&abort).await.unwrap();
        for fake in [&source, &receiver] {
            for calls in fake.calls().chunks(3) {
                assert_eq!(calls.len(), 3);
                assert_eq!(calls[1].op().method(), "agent_data.status");
                assert_eq!(calls[0], calls[2]);
            }
        }
        assert_eq!(source.cut(CUT).unwrap().step, Step::Released);
        assert_eq!(receiver.cut(CUT).unwrap().step, Step::Activated);
    }

    #[tokio::test]
    async fn routing_fans_out_to_every_declaring_available_participant_only() {
        let shared = reference("s1");
        let other = AgentRef::new("memory", "m1");
        let a = Arc::new(FakeParticipant::new(&["session"], vec![shared.clone()]));
        let b = Arc::new(FakeParticipant::new(
            &["session", "memory"],
            vec![shared.clone(), other.clone()],
        ));
        let c = Arc::new(FakeParticipant::new(&["memory"], vec![other.clone()]));
        let unavailable = Arc::new(FakeParticipant::new(&["session"], vec![shared.clone()]));
        let available = vec![
            Participant {
                module_id: "c".into(),
                transport: c.clone(),
            },
            Participant {
                module_id: "b".into(),
                transport: b.clone(),
            },
            Participant {
                module_id: "a".into(),
                transport: a.clone(),
            },
        ];
        let plan = plan_participants(&available, &[shared.clone(), other.clone()])
            .await
            .unwrap();
        assert_eq!(
            plan.iter()
                .map(|p| p.client.module_id.as_str())
                .collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(plan[0].refs.as_slice(), std::slice::from_ref(&shared));
        assert_eq!(plan[1].refs, [other.clone(), shared.clone()]);
        assert_eq!(plan[2].refs, [other]);
        for p in &plan {
            p.client.busy(&p.refs).await.unwrap();
            p.client
                .prepare(&cut_with_refs(p.refs.clone(), CUT))
                .await
                .unwrap();
        }
        for (fake, expected) in [
            (a, plan[0].refs.clone()),
            (b, plan[1].refs.clone()),
            (c, plan[2].refs.clone()),
        ] {
            assert_eq!(
                fake.calls()[1].refs().unwrap().as_slice(),
                expected.as_slice()
            );
            assert_eq!(
                fake.calls()[2].refs().unwrap().as_slice(),
                expected.as_slice()
            );
        }
        assert!(unavailable.calls().is_empty());
        let missing = AgentRef::new("unsupported", "x");
        assert!(
            matches!(plan_participants(&available, &[missing]).await, Err(PlanError::RefKindUnavailable(kind)) if kind == "unsupported")
        );
    }
}

#[cfg(test)]
mod refusal_tests {
    use super::*;
    use crate::fake_participant::{Effect, FakeParticipant};
    use serde_json::json;

    fn cut() -> CutRequest {
        CutRequest::new(
            Refs::new(vec![AgentRef::new("session", "s1")]).unwrap(),
            CutId::new("0123456789abcdef0123456789abcdef").unwrap(),
        )
    }
    fn release_request(cut: CutRequest, holder: &str) -> ReleaseRequest {
        ReleaseRequest::new(cut.refs, cut.cut_id, holder)
    }

    fn client(fake: Arc<FakeParticipant>) -> ParticipantClient {
        ParticipantClient::new(&Participant {
            module_id: "adapter".into(),
            transport: fake,
        })
    }

    #[tokio::test]
    async fn allowed_refusals_per_op_preserve_code_and_opaque_detail() {
        let cases = [
            (Request::Describe(DescribeRequest::new()), "unavailable"),
            (Request::Busy(BusyRequest::new(cut().refs)), "unknown_ref"),
            (Request::Status(cut()), "invalid_request"),
            (Request::Prepare(cut()), "not_exportable"),
            (Request::Export(cut()), "cut_unknown"),
            (Request::Diverged(cut()), "external_not_present"),
            (Request::Import(cut()), "digest_mismatch"),
            (Request::Activate(cut()), "external_not_present"),
            (Request::Abort(cut()), "invalid_state"),
            (
                Request::Release(release_request(cut(), "mac-b")),
                "unknown_ref",
            ),
            (Request::Diverged(cut()), "cut_not_ready"),
            (Request::Diverged(cut()), "incompatible_version"),
            (Request::Import(cut()), "busy"),
            (Request::Import(cut()), "local_advanced"),
            (Request::Activate(cut()), "busy"),
            (Request::Activate(cut()), "local_advanced"),
        ];
        for (request, code) in cases {
            let fake = Arc::new(FakeParticipant::new(&["session"], cut().refs.into_vec()));
            let detail = json!({"note":"fixture refusal","new_metadata":{"versions":[1,2]}});
            fake.script(
                request.op().method(),
                Effect::Refuse(Refusal {
                    code: code.into(),
                    detail: Some(detail.clone()),
                }),
            );
            match client(fake.clone())
                .call::<Value>(request.clone())
                .await
                .unwrap_err()
            {
                ClientError::Refused(refusal) => {
                    assert_eq!(refusal.code, code, "{}", request.op().method());
                    assert_eq!(refusal.detail, Some(detail));
                }
                other => panic!("{} must allow {code}, got {other:?}", request.op().method()),
            }
            assert_eq!(fake.calls(), vec![request]);
        }
    }

    #[tokio::test]
    async fn disallowed_and_unknown_refusal_codes_are_contract_violations() {
        for (request, code) in [
            (Request::Release(release_request(cut(), "mac-b")), "busy"),
            (Request::Prepare(cut()), "frobnicated"),
        ] {
            let fake = Arc::new(FakeParticipant::new(&["session"], cut().refs.into_vec()));
            let detail = json!({"note":"contract violation","nested":{"future":true}});
            fake.script(
                request.op().method(),
                Effect::Refuse(Refusal {
                    code: code.into(),
                    detail: Some(detail.clone()),
                }),
            );
            match client(fake.clone())
                .call::<Value>(request.clone())
                .await
                .unwrap_err()
            {
                ClientError::ContractViolation {
                    op,
                    code: actual_code,
                    detail: actual_detail,
                } => {
                    assert_eq!(op, request.op().method());
                    assert_eq!(actual_code, code);
                    assert_eq!(actual_detail, Some(detail));
                }
                other => panic!(
                    "{} must reject {code} as a contract violation, got {other:?}",
                    request.op().method()
                ),
            }
            assert_eq!(fake.calls(), vec![request]);
        }
    }

    #[tokio::test]
    async fn status_probe_contract_violation_is_not_masked_as_unknown() {
        let fake = Arc::new(FakeParticipant::new(&["session"], cut().refs.into_vec()));
        fake.script(
            "agent_data.prepare",
            Effect::LoseReply(TransportError::NoReply),
        );
        fake.script(
            "agent_data.status",
            Effect::Refuse(Refusal {
                code: "busy".into(),
                detail: Some(json!({"step":"prepared"})),
            }),
        );
        assert!(matches!(client(fake.clone()).prepare(&cut()).await,
            Err(ClientError::ContractViolation { op: "agent_data.status", code, detail })
            if code == "busy" && detail == Some(json!({"step":"prepared"}))));
        assert_eq!(
            fake.calls()
                .iter()
                .map(|request| request.op().method())
                .collect::<Vec<_>>(),
            ["agent_data.prepare", "agent_data.status"]
        );
    }
}

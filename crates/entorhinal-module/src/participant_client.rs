//! In-tree agent-data/v1 wire types and participant calls.
//! Opaque metadata is preserved for a future switch to cortexkit-role-agent-data wire types.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;

use crate::log_client::{LogTransport, TransportError};
use crate::participant_discovery::Participant;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentRef {
    pub(crate) ref_kind: String,
    #[serde(rename = "ref")]
    pub(crate) reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Empty {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BusyRequest {
    pub(crate) refs: Vec<AgentRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutRequest {
    pub(crate) refs: Vec<AgentRef>,
    #[serde(rename = "cutId")]
    pub(crate) cut_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseRequest {
    pub(crate) refs: Vec<AgentRef>,
    #[serde(rename = "cutId")]
    pub(crate) cut_id: String,
    pub(crate) holder: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
pub(crate) enum Request {
    #[serde(rename = "agent_data.describe")]
    Describe(Empty),
    #[serde(rename = "agent_data.busy")]
    Busy(BusyRequest),
    #[serde(rename = "agent_data.status")]
    Status(CutRequest),
    #[serde(rename = "agent_data.prepare")]
    Prepare(CutRequest),
    #[serde(rename = "agent_data.export")]
    Export(CutRequest),
    #[serde(rename = "agent_data.diverged")]
    Diverged(CutRequest),
    #[serde(rename = "agent_data.import")]
    Import(CutRequest),
    #[serde(rename = "agent_data.activate")]
    Activate(CutRequest),
    #[serde(rename = "agent_data.abort")]
    Abort(CutRequest),
    #[serde(rename = "agent_data.release")]
    Release(ReleaseRequest),
}

impl Request {
    pub(crate) fn op(&self) -> &'static str {
        match self {
            Self::Describe(_) => "agent_data.describe",
            Self::Busy(_) => "agent_data.busy",
            Self::Status(_) => "agent_data.status",
            Self::Prepare(_) => "agent_data.prepare",
            Self::Export(_) => "agent_data.export",
            Self::Diverged(_) => "agent_data.diverged",
            Self::Import(_) => "agent_data.import",
            Self::Activate(_) => "agent_data.activate",
            Self::Abort(_) => "agent_data.abort",
            Self::Release(_) => "agent_data.release",
        }
    }

    pub(crate) fn refs(&self) -> &[AgentRef] {
        match self {
            Self::Describe(_) => &[],
            Self::Busy(request) => &request.refs,
            Self::Release(request) => &request.refs,
            Self::Status(request)
            | Self::Prepare(request)
            | Self::Export(request)
            | Self::Diverged(request)
            | Self::Import(request)
            | Self::Activate(request)
            | Self::Abort(request) => &request.refs,
        }
    }

    pub(crate) fn cut(&self) -> Option<CutRequest> {
        match self {
            Self::Describe(_) | Self::Busy(_) => None,
            Self::Release(request) => Some(CutRequest {
                refs: request.refs.clone(),
                cut_id: request.cut_id.clone(),
            }),
            Self::Status(request)
            | Self::Prepare(request)
            | Self::Export(request)
            | Self::Diverged(request)
            | Self::Import(request)
            | Self::Activate(request)
            | Self::Abort(request) => Some(request.clone()),
        }
    }

    pub(crate) fn changes_state(&self) -> bool {
        matches!(
            self,
            Self::Prepare(_)
                | Self::Export(_)
                | Self::Import(_)
                | Self::Activate(_)
                | Self::Abort(_)
                | Self::Release(_)
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StoreDescription {
    pub(crate) name: String,
    pub(crate) sync_class: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DescribeReply {
    pub(crate) ref_kinds: Vec<String>,
    pub(crate) stores: Vec<StoreDescription>,
    pub(crate) external: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum BusyState {
    Idle,
    Busy,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BusyRef {
    pub(crate) state: BusyState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason_code: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct BusyReply {
    pub(crate) refs: Vec<BusyRef>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Step {
    None,
    Prepared,
    Exported,
    Imported,
    Activated,
    Aborted,
    Released,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatusReply {
    pub(crate) step: Step,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareItem {
    pub(crate) ref_kind: String,
    #[serde(rename = "ref")]
    pub(crate) reference: String,
    #[serde(rename = "sessionRef")]
    pub(crate) session_ref: String,
    pub(crate) stream: String,
    #[serde(rename = "endOffset")]
    pub(crate) end_offset: u64,
    pub(crate) digest: String,
    pub(crate) format: Value,
    pub(crate) frozen: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExternalRef {
    pub(crate) ref_kind: String,
    #[serde(rename = "ref")]
    pub(crate) reference: String,
    pub(crate) note: String,
    pub(crate) check: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PrepareReply {
    pub(crate) items: Vec<PrepareItem>,
    pub(crate) external: Vec<ExternalRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExportItem {
    #[serde(rename = "sessionRef")]
    pub(crate) session_ref: String,
    #[serde(rename = "endOffset")]
    pub(crate) end_offset: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ExportReply {
    pub(crate) items: Vec<ExportItem>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum DivergedState {
    AtCut,
    Advanced,
    Unknown,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DivergedRef {
    pub(crate) state: DivergedState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DivergedReply {
    pub(crate) refs: Vec<DivergedRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImportItem {
    #[serde(rename = "sessionRef")]
    pub(crate) session_ref: String,
    #[serde(rename = "endOffset")]
    pub(crate) end_offset: u64,
    pub(crate) digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ImportReply {
    pub(crate) items: Vec<ImportItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReleaseReply {
    pub(crate) enforced: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Refusal {
    pub(crate) code: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) detail: Option<Value>,
}

#[derive(Debug, Clone)]
pub(crate) enum ClientError {
    Refused(Refusal),
    OutcomeUnknown,
    InvalidReply(String),
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
        let mut params = serde_json::to_value(request).expect("participant requests serialize");
        params
            .as_object_mut()
            .expect("request is an object")
            .remove("op");
        let bytes =
            self.transport
                .call(request.op(), params)
                .await
                .map_err(|error| match error {
                    TransportError::Refusal { code, detail } => {
                        ClientError::Refused(Refusal { code, detail })
                    }
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
            .map_err(|_| ClientError::OutcomeUnknown)?;
        self.unresolved.lock().unwrap().remove(&cut.cut_id);
        Ok(())
    }

    async fn call<T: DeserializeOwned>(&self, request: Request) -> Result<T, ClientError> {
        let cut = request.cut();
        let is_status = matches!(request, Request::Status(_));
        // A failed status probe leaves the cut unresolved across calls and client clones.
        // The next attempt must obtain status before sending another op for that cut.
        if !is_status {
            if let Some(cut) = &cut {
                let unresolved = self.unresolved.lock().unwrap().contains(&cut.cut_id);
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
                        self.unresolved.lock().unwrap().insert(cut.cut_id.clone());
                        if !is_status {
                            self.reconcile(cut).await?;
                        }
                    }
                }
                result => {
                    if result.is_ok() && is_status {
                        if let Some(cut) = &cut {
                            self.unresolved.lock().unwrap().remove(&cut.cut_id);
                        }
                    }
                    return result;
                }
            }
        }
        Err(ClientError::OutcomeUnknown)
    }

    pub(crate) async fn describe(&self) -> Result<DescribeReply, ClientError> {
        self.call(Request::Describe(Empty {})).await
    }
    pub(crate) async fn busy(&self, refs: &[AgentRef]) -> Result<BusyReply, ClientError> {
        self.call(Request::Busy(BusyRequest {
            refs: refs.to_vec(),
        }))
        .await
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
        self.call(Request::Diverged(cut.clone())).await
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
    use serde_json::json;

    use super::*;
    use crate::fake_participant::{Effect, FakeParticipant};

    const CUT: &str = "0123456789abcdef0123456789abcdef";
    const CUT_JSON: &str = r#"{"refs":[{"ref_kind":"session","ref":"s1"}],"cutId":"0123456789abcdef0123456789abcdef"}"#;
    const PREPARE_JSON: &str = r#"{"items":[{"ref_kind":"session","ref":"s1","sessionRef":"00112233445566778899aabbccddeeff","stream":"reserved:adapter","endOffset":42,"digest":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","format":"rows/v1","frozen":true}],"external":[{"ref_kind":"session","ref":"s1","note":"Copy the transcript with the project","check":{"host_session":"native-1","last_row":7,"future":{"version":2}}}]}"#;

    pub(super) fn reference(name: &str) -> AgentRef {
        AgentRef {
            ref_kind: "session".into(),
            reference: name.into(),
        }
    }
    fn cut() -> CutRequest {
        serde_json::from_str(CUT_JSON).unwrap()
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
        let busy = client.busy(&cut.refs).await.unwrap();
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
        assert_eq!(client.activate(&cut).await.unwrap(), Empty {});
        assert_eq!(client.abort(&cut).await.unwrap(), Empty {});
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
            round_trip::<Request>(literal);
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
        round_trip::<Refusal>(
            r#"{"code":"invalid_state","detail":{"step":"activated","future":{"x":1}}}"#,
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
        rejects_extra::<ReleaseRequest>(r#"{"refs":[],"cutId":"c","holder":"b"}"#);
        rejects_extra::<Request>(r#"{"op":"agent_data.prepare","refs":[],"cutId":"c"}"#);
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
        rejects_extra::<Refusal>(r#"{"code":"invalid_state","detail":{"step":"activated"}}"#);
        for (op, extra) in [
            ("agent_data.diverged", "check"),
            ("agent_data.import", "format"),
            ("agent_data.activate", "check"),
        ] {
            let mut request: Value = serde_json::from_str(CUT_JSON).unwrap();
            request["op"] = json!(op);
            request[extra] = json!({"should_not_be_sent":true});
            assert!(serde_json::from_value::<Request>(request).is_err());
        }
        let mut reply: Value = serde_json::from_str(PREPARE_JSON).unwrap();
        reply["items"][0]["format"] = json!({"new_metadata":[1,2]});
        let reply: PrepareReply = serde_json::from_value(reply).unwrap();
        assert_eq!(reply.items[0].format, json!({"new_metadata":[1,2]}));
        assert!(serde_json::from_str::<StatusReply>(r#"{"step":"staged"}"#).is_err());
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
                    calls.iter().map(Request::op).collect::<Vec<_>>(),
                    [
                        "agent_data.prepare",
                        "agent_data.status",
                        "agent_data.prepare"
                    ]
                );
                assert_eq!(calls[0], calls[2]);
                fake.set_prepare(PrepareReply {
                    items: vec![],
                    external: vec![],
                });
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
            fake.calls().iter().map(Request::op).collect::<Vec<_>>(),
            ["agent_data.prepare", "agent_data.status"]
        );
        assert_eq!(fake.cut(CUT).unwrap().step, Step::Prepared);
        c.clone().prepare(&cut()).await.unwrap();
        assert_eq!(
            fake.calls().iter().map(Request::op).collect::<Vec<_>>(),
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
        a.release(&ReleaseRequest {
            refs: cut().refs,
            cut_id: CUT.into(),
            holder: "mac-b".into(),
        })
        .await
        .unwrap();
        b.import(&cut()).await.unwrap();
        b.activate(&cut()).await.unwrap();
        let abort = CutRequest {
            cut_id: "ffffffffffffffffffffffffffffffff".into(),
            refs: cut().refs,
        };
        receiver.script(
            "agent_data.abort",
            Effect::LoseReply(TransportError::NoReply),
        );
        b.abort(&abort).await.unwrap();
        for fake in [&source, &receiver] {
            for calls in fake.calls().chunks(3) {
                assert_eq!(calls.len(), 3);
                assert_eq!(calls[1].op(), "agent_data.status");
                assert_eq!(calls[0], calls[2]);
            }
        }
        assert_eq!(source.cut(CUT).unwrap().step, Step::Released);
        assert_eq!(receiver.cut(CUT).unwrap().step, Step::Activated);
    }

    #[tokio::test]
    async fn routing_fans_out_to_every_declaring_available_participant_only() {
        let shared = reference("s1");
        let other = AgentRef {
            ref_kind: "memory".into(),
            reference: "m1".into(),
        };
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
                .prepare(&CutRequest {
                    refs: p.refs.clone(),
                    cut_id: CUT.into(),
                })
                .await
                .unwrap();
        }
        for (fake, expected) in [
            (a, plan[0].refs.clone()),
            (b, plan[1].refs.clone()),
            (c, plan[2].refs.clone()),
        ] {
            assert_eq!(fake.calls()[1].refs(), expected);
            assert_eq!(fake.calls()[2].refs(), expected);
        }
        assert!(unavailable.calls().is_empty());
        let missing = AgentRef {
            ref_kind: "unsupported".into(),
            reference: "x".into(),
        };
        assert!(
            matches!(plan_participants(&available, &[missing]).await, Err(PlanError::RefKindUnavailable(kind)) if kind == "unsupported")
        );
    }
}

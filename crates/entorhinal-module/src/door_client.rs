//! Entorhinal's cut operations on engram's `session-door` internal service.
//!
//! Wire contract: engram AGENT_SYNC.md at 8a96bc0, "The move: a durable
//! transaction" (seal/status/abort/handoff) and "Resume on the new machine"
//! (read/takeover/accept). The handler's progress objects include failure detail.
//!
//! The move driver owns call ordering and retries refusals. This client settles
//! lost transport replies with status before repeating the identical request.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use subc_protocol::RouteTarget;

use crate::log_client::{encode_hex, LogConnector, LogTransport, SubcConnector, TransportError};

pub(crate) const SERVICE_ID: &str = "session-door";
pub(crate) const OPS: [&str; 7] = [
    "cut.seal",
    "cut.status",
    "cut.read",
    "cut.handoff",
    "cut.takeover",
    "cut.abort",
    "cut.accept",
];
pub(crate) const ITEM_CAP: usize = 1024;

pub(crate) fn route_target() -> RouteTarget {
    RouteTarget::InternalService {
        module_id: "engram".into(),
        service_id: SERVICE_ID.into(),
    }
}

pub(crate) fn mint_cut_id() -> Result<String, getrandom::Error> {
    let mut bytes = [0; 16];
    getrandom::getrandom(&mut bytes)?;
    Ok(encode_hex(&bytes))
}

pub(crate) fn valid_cut_id(id: &str) -> bool {
    id.len() == 32
        && id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutItem {
    #[serde(rename = "sessionRef")]
    pub(crate) session_ref: String,
    pub(crate) stream: String,
    #[serde(rename = "endOffset")]
    pub(crate) end_offset: u64,
    pub(crate) digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CutRequest {
    #[serde(rename = "cutId")]
    pub(crate) cut_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SealRequest {
    #[serde(rename = "cutId")]
    pub(crate) cut_id: String,
    pub(crate) items: Vec<CutItem>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "op")]
pub(crate) enum Request {
    #[serde(rename = "cut.seal")]
    Seal(SealRequest),
    #[serde(rename = "cut.status")]
    Status(CutRequest),
    #[serde(rename = "cut.read")]
    Read(CutRequest),
    #[serde(rename = "cut.handoff")]
    Handoff(CutRequest),
    #[serde(rename = "cut.takeover")]
    Takeover(CutRequest),
    #[serde(rename = "cut.abort")]
    Abort(CutRequest),
    #[serde(rename = "cut.accept")]
    Accept(CutRequest),
}

impl Request {
    pub(crate) fn op(&self) -> &'static str {
        match self {
            Self::Seal(_) => OPS[0],
            Self::Status(_) => OPS[1],
            Self::Read(_) => OPS[2],
            Self::Handoff(_) => OPS[3],
            Self::Takeover(_) => OPS[4],
            Self::Abort(_) => OPS[5],
            Self::Accept(_) => OPS[6],
        }
    }

    pub(crate) fn cut_id(&self) -> &str {
        match self {
            Self::Seal(request) => &request.cut_id,
            Self::Status(request)
            | Self::Read(request)
            | Self::Handoff(request)
            | Self::Takeover(request)
            | Self::Abort(request)
            | Self::Accept(request) => &request.cut_id,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum SealState {
    Pending,
    Published,
    Failed,
    Aborted,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum HandoffState {
    NotStarted,
    InProgress,
    Released,
    TakenOver,
    Aborted,
    Failed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TakeoverState {
    NotStarted,
    InProgress,
    Taken,
    Failed,
}

// The serving handler returns the full local status from cut.seal.
pub(crate) type SealReply = StatusReply;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct StatusReply {
    pub(crate) state: SealState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) handoff: Option<HandoffReply>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) takeover: Option<TakeoverProgress>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) failure: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ReadReply {
    pub(crate) items: Vec<CutItem>,
    pub(crate) author: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HandoffReply {
    pub(crate) state: HandoffState,
    #[serde(rename = "sessionRef", skip_serializing_if = "Option::is_none")]
    pub(crate) session_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TakeoverProgress {
    pub(crate) state: TakeoverState,
    #[serde(rename = "sessionRef", skip_serializing_if = "Option::is_none")]
    pub(crate) session_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TakeoverReply {
    pub(crate) takeover: TakeoverState,
    #[serde(rename = "sessionRef")]
    pub(crate) session_ref: Option<String>,
    pub(crate) code: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AbortReply {
    pub(crate) handoff: HandoffState,
    pub(crate) state: SealState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AcceptReply {
    pub(crate) lifted: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum DoorError {
    Refused { code: String, detail: Option<Value> },
    OutcomeUnknown,
    InvalidReply(String),
}

impl DoorError {
    // Cloud outages and unsettled handoff coverage leave the cut active, so the
    // move driver can retry (AGENT_SYNC.md, handoff and Abort).
    pub(crate) fn retryable(&self) -> bool {
        matches!(self, Self::OutcomeUnknown)
            || matches!(self, Self::Refused { code, .. }
                if code == "unavailable" || code == "handoff_not_covered")
    }

    pub(crate) fn code(&self) -> &str {
        match self {
            Self::Refused { code, .. } => code,
            Self::OutcomeUnknown => "unavailable",
            Self::InvalidReply(_) => "door_protocol",
        }
    }
}

#[derive(Deserialize)]
struct Envelope<T> {
    result: T,
}

#[derive(Clone)]
pub(crate) struct DoorClient {
    connector: Arc<dyn LogConnector>,
    unresolved: Arc<Mutex<BTreeSet<String>>>,
}

impl DoorClient {
    pub(crate) fn new(connector: Arc<dyn LogConnector>) -> Self {
        Self {
            connector,
            unresolved: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    pub(crate) fn production(connector: SubcConnector) -> Self {
        Self::new(Arc::new(DoorConnector(connector)))
    }

    async fn once<T: DeserializeOwned>(&self, request: &Request) -> Result<T, DoorError> {
        let transport = self.connector.connect().await.map_err(map_transport)?;
        let mut params = serde_json::to_value(request).expect("door request serializes");
        params.as_object_mut().unwrap().remove("op");
        let bytes = transport
            .call(request.op(), params)
            .await
            .map_err(map_transport)?;
        serde_json::from_slice::<Envelope<T>>(&bytes)
            .map(|reply| reply.result)
            .map_err(|error| DoorError::InvalidReply(error.to_string()))
    }

    async fn reconcile(&self, cut_id: &str) -> Result<(), DoorError> {
        let request = Request::Status(CutRequest {
            cut_id: cut_id.into(),
        });
        // A missing cut after a lost seal reply is safe to reseal with the same ID.
        // Other refused or malformed status probes leave the outcome unresolved.
        match self.once::<StatusReply>(&request).await {
            Ok(_) => {}
            Err(DoorError::Refused { code, .. }) if code == "not_found" => {}
            Err(_) => return Err(DoorError::OutcomeUnknown),
        }
        self.unresolved.lock().unwrap().remove(cut_id);
        Ok(())
    }

    async fn call<T: DeserializeOwned>(&self, request: Request) -> Result<T, DoorError> {
        let cut_id = request.cut_id();
        let is_status = matches!(request, Request::Status(_));
        if !is_status {
            let unresolved = self.unresolved.lock().unwrap().contains(cut_id);
            if unresolved {
                self.reconcile(cut_id).await?;
            }
        }
        // Status cannot supply the refs whose write restrictions cut.accept lifted.
        // Try at most twice here; the move driver schedules any further retries.
        for _ in 0..2 {
            match self.once(&request).await {
                Err(DoorError::OutcomeUnknown) => {
                    self.unresolved.lock().unwrap().insert(cut_id.into());
                    if !is_status {
                        self.reconcile(cut_id).await?;
                    }
                }
                result => {
                    if is_status
                        && (result.is_ok()
                            || matches!(&result,
                        Err(DoorError::Refused { code, .. }) if code == "not_found"))
                    {
                        self.unresolved.lock().unwrap().remove(cut_id);
                    }
                    return result;
                }
            }
        }
        Err(DoorError::OutcomeUnknown)
    }

    pub(crate) async fn seal(&self, request: &SealRequest) -> Result<SealReply, DoorError> {
        self.call(Request::Seal(request.clone())).await
    }
    pub(crate) async fn status(&self, cut: &CutRequest) -> Result<StatusReply, DoorError> {
        self.call(Request::Status(cut.clone())).await
    }
    pub(crate) async fn read(&self, cut: &CutRequest) -> Result<ReadReply, DoorError> {
        self.call(Request::Read(cut.clone())).await
    }
    pub(crate) async fn handoff(&self, cut: &CutRequest) -> Result<HandoffReply, DoorError> {
        self.call(Request::Handoff(cut.clone())).await
    }
    pub(crate) async fn takeover(&self, cut: &CutRequest) -> Result<TakeoverReply, DoorError> {
        self.call(Request::Takeover(cut.clone())).await
    }
    pub(crate) async fn abort(&self, cut: &CutRequest) -> Result<AbortReply, DoorError> {
        self.call(Request::Abort(cut.clone())).await
    }
    pub(crate) async fn accept(&self, cut: &CutRequest) -> Result<AcceptReply, DoorError> {
        self.call(Request::Accept(cut.clone())).await
    }
}

fn map_transport(error: TransportError) -> DoorError {
    match error {
        TransportError::Refusal { code, detail } => DoorError::Refused { code, detail },
        TransportError::Unavailable | TransportError::NoReply => DoorError::OutcomeUnknown,
    }
}

struct DoorConnector(SubcConnector);

#[async_trait]
impl LogConnector for DoorConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.0
            .open_internal_service(route_target(), "agent-move")
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::VecDeque;

    const ID: &str = "00112233445566778899aabbccddeeff";
    const REF: &str = "11111111111111111111111111111111";
    const DIGEST: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    fn cut() -> CutRequest {
        CutRequest { cut_id: ID.into() }
    }
    fn seal() -> SealRequest {
        SealRequest {
            cut_id: ID.into(),
            items: vec![CutItem {
                session_ref: REF.into(),
                stream: "reserved:broca".into(),
                end_offset: 42,
                digest: DIGEST.into(),
            }],
        }
    }

    struct Stub {
        replies: Mutex<VecDeque<Result<Vec<u8>, TransportError>>>,
        calls: Mutex<Vec<(String, Value)>>,
    }
    impl Stub {
        fn new(replies: Vec<Result<Vec<u8>, TransportError>>) -> Arc<Self> {
            Arc::new(Self {
                replies: Mutex::new(replies.into()),
                calls: Mutex::new(vec![]),
            })
        }
        fn client(self: &Arc<Self>) -> DoorClient {
            DoorClient::new(Arc::new(Connector(self.clone())))
        }
    }
    struct Connector(Arc<Stub>);
    #[async_trait]
    impl LogConnector for Connector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for Stub {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.calls.lock().unwrap().push((method.into(), params));
            self.replies
                .lock()
                .unwrap()
                .pop_front()
                .expect("unexpected door call")
        }
    }
    fn reply(text: &str) -> Result<Vec<u8>, TransportError> {
        Ok(text.as_bytes().to_vec())
    }
    fn refusal(code: &str) -> TransportError {
        TransportError::Refusal {
            code: code.into(),
            detail: None,
        }
    }

    #[test]
    fn cut_ids_are_16_random_bytes_in_lowercase_hex() {
        let ids: BTreeSet<_> = (0..128).map(|_| mint_cut_id().unwrap()).collect();
        assert_eq!(ids.len(), 128);
        assert!(ids.iter().all(|id| valid_cut_id(id)));
        for invalid in [
            "",
            "a",
            "00112233445566778899AABBCCDDEEFF",
            "00112233445566778899aabbccddeefg",
        ] {
            assert!(!valid_cut_id(invalid));
        }
    }

    #[test]
    fn route_is_engrams_session_door_internal_service() {
        assert_eq!(
            route_target(),
            RouteTarget::InternalService {
                module_id: "engram".into(),
                service_id: "session-door".into(),
            }
        );
    }

    #[tokio::test]
    async fn seven_ops_match_literal_handler_wire_fixtures() {
        // Literal handler replies pin the wire independently of the in-tree fake,
        // so a matching serialization mistake in both implementations cannot pass.
        let stub = Stub::new(vec![
            reply(r#"{"result":{"state":"pending","handoff":{"state":"not_started"}}}"#),
            reply(
                r#"{"result":{"state":"failed","handoff":{"state":"not_started"},"failure":{"sessionRef":"11111111111111111111111111111111","code":"cut_end_stale"}}}"#,
            ),
            reply(
                r#"{"result":{"items":[{"sessionRef":"11111111111111111111111111111111","stream":"reserved:broca","endOffset":42,"digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}],"author":"22222222222222222222222222222222"}}"#,
            ),
            reply(
                r#"{"result":{"state":"failed","sessionRef":"11111111111111111111111111111111","code":"handoff_not_covered"}}"#,
            ),
            reply(r#"{"result":{"takeover":"in_progress","sessionRef":null,"code":null}}"#),
            reply(r#"{"result":{"handoff":"aborted","state":"published"}}"#),
            reply(r#"{"result":{"lifted":["11111111111111111111111111111111"]}}"#),
        ]);
        let client = stub.client();
        let sealed = client.seal(&seal()).await.unwrap();
        assert_eq!(sealed.state, SealState::Pending);
        assert_eq!(sealed.handoff.unwrap().state, HandoffState::NotStarted);
        let status = client.status(&cut()).await.unwrap();
        assert_eq!(status.state, SealState::Failed);
        assert_eq!(status.failure.unwrap()["code"], "cut_end_stale");
        let read = client.read(&cut()).await.unwrap();
        assert_eq!(read.items, seal().items);
        assert_eq!(read.author, "22222222222222222222222222222222");
        let handoff = client.handoff(&cut()).await.unwrap();
        assert_eq!(handoff.state, HandoffState::Failed);
        assert_eq!(handoff.code.as_deref(), Some("handoff_not_covered"));
        assert_eq!(handoff.session_ref.as_deref(), Some(REF));
        assert_eq!(
            client.takeover(&cut()).await.unwrap().takeover,
            TakeoverState::InProgress
        );
        assert_eq!(
            client.abort(&cut()).await.unwrap(),
            AbortReply {
                handoff: HandoffState::Aborted,
                state: SealState::Published
            }
        );
        assert_eq!(client.accept(&cut()).await.unwrap().lifted, vec![REF]);
        let calls = stub.calls.lock().unwrap();
        assert_eq!(
            calls.iter().map(|call| call.0.as_str()).collect::<Vec<_>>(),
            OPS
        );
        assert_eq!(
            calls[0].1,
            json!({"cutId":"00112233445566778899aabbccddeeff","items":[{"sessionRef":"11111111111111111111111111111111","stream":"reserved:broca","endOffset":42,"digest":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}]})
        );
        for (_, params) in calls.iter().skip(1) {
            assert_eq!(*params, json!({"cutId":"00112233445566778899aabbccddeeff"}));
        }
    }

    #[test]
    fn status_progress_objects_and_terminal_states_are_strict() {
        for state in ["pending", "published", "failed", "aborted"] {
            let _: StatusReply = serde_json::from_value(json!({"state":state})).unwrap();
        }
        for state in [
            "not_started",
            "in_progress",
            "released",
            "taken_over",
            "aborted",
            "failed",
        ] {
            let _: StatusReply =
                serde_json::from_value(json!({"state":"published","handoff":{"state":state}}))
                    .unwrap();
        }
        for state in ["not_started", "in_progress", "taken", "failed"] {
            let _: StatusReply =
                serde_json::from_value(json!({"state":"published","takeover":{"state":state}}))
                    .unwrap();
        }
        for invalid in [
            json!({"state":"other"}),
            json!({"state":"published","handoff":"released"}),
            json!({"state":"published","extra":true}),
            json!({"state":"published","handoff":{"state":"released","extra":true}}),
        ] {
            assert!(serde_json::from_value::<StatusReply>(invalid).is_err());
        }
        assert!(
            serde_json::from_value::<SealRequest>(json!({"cutId":ID,"items":[],"extra":true}))
                .is_err()
        );
    }

    #[tokio::test]
    async fn every_unknown_op_probes_status_then_repeats_identical_request() {
        let requests = [
            Request::Seal(seal()),
            Request::Read(cut()),
            Request::Handoff(cut()),
            Request::Takeover(cut()),
            Request::Abort(cut()),
            Request::Accept(cut()),
        ];
        for request in requests {
            let stub = Stub::new(vec![
                Err(TransportError::NoReply),
                reply(r#"{"result":{"state":"published"}}"#),
                reply(r#"{"result":{"ok":true}}"#),
            ]);
            let result: Value = stub.client().call(request.clone()).await.unwrap();
            assert_eq!(result, json!({"ok":true}));
            let calls = stub.calls.lock().unwrap();
            assert_eq!(calls.len(), 3);
            assert_eq!(calls[0], calls[2]);
            assert_eq!(calls[0].0, request.op());
            assert_eq!(calls[1], ("cut.status".into(), json!({"cutId":ID})));
        }
    }

    #[tokio::test]
    async fn failed_status_probe_keeps_cut_unresolved_across_client_clones() {
        let stub = Stub::new(vec![
            Err(TransportError::Unavailable),
            Err(refusal("unavailable")),
            reply(r#"{"result":{"state":"published"}}"#),
            reply(r#"{"result":{"handoff":"aborted","state":"published"}}"#),
        ]);
        let client = stub.client();
        assert_eq!(
            client.seal(&seal()).await.unwrap_err(),
            DoorError::OutcomeUnknown
        );
        client.clone().abort(&cut()).await.unwrap();
        let calls = stub.calls.lock().unwrap();
        assert_eq!(
            calls.iter().map(|call| call.0.as_str()).collect::<Vec<_>>(),
            ["cut.seal", "cut.status", "cut.status", "cut.abort"]
        );
    }

    #[tokio::test]
    async fn unknown_seal_without_local_row_resolves_not_found_then_resends() {
        let stub = Stub::new(vec![
            Err(TransportError::NoReply),
            Err(refusal("not_found")),
            reply(r#"{"result":{"state":"pending"}}"#),
        ]);
        assert_eq!(
            stub.client().seal(&seal()).await.unwrap().state,
            SealState::Pending
        );
        let calls = stub.calls.lock().unwrap();
        assert_eq!(calls.len(), 3);
        assert_eq!(calls[0], calls[2]);
        assert_eq!(calls[1].0, "cut.status");
    }

    #[tokio::test]
    async fn recovery_is_bounded_and_status_retries_itself() {
        let stub = Stub::new(vec![
            Err(TransportError::NoReply),
            reply(r#"{"result":{"state":"pending"}}"#),
            Err(TransportError::NoReply),
            reply(r#"{"result":{"state":"pending"}}"#),
        ]);
        assert_eq!(
            stub.client().seal(&seal()).await.unwrap_err(),
            DoorError::OutcomeUnknown
        );
        assert_eq!(stub.calls.lock().unwrap().len(), 4);
        let stub = Stub::new(vec![
            Err(TransportError::NoReply),
            reply(r#"{"result":{"state":"published"}}"#),
        ]);
        stub.client().status(&cut()).await.unwrap();
        assert!(stub
            .calls
            .lock()
            .unwrap()
            .iter()
            .all(|call| call.0 == "cut.status"));
    }

    #[tokio::test]
    async fn refusals_keep_code_detail_and_do_not_trigger_transport_recovery() {
        for code in [
            "unavailable",
            "handoff_not_covered",
            "cut_handoff_started",
            "verify_failed",
            "future_code",
        ] {
            let stub = Stub::new(vec![Err(TransportError::Refusal {
                code: code.into(),
                detail: Some(json!({"stream":"reserved:broca"})),
            })]);
            let error = stub.client().handoff(&cut()).await.unwrap_err();
            assert_eq!(
                error,
                DoorError::Refused {
                    code: code.into(),
                    detail: Some(json!({"stream":"reserved:broca"}))
                }
            );
            assert_eq!(error.code(), code);
            assert_eq!(
                error.retryable(),
                ["unavailable", "handoff_not_covered"].contains(&code)
            );
            assert_eq!(stub.calls.lock().unwrap().len(), 1);
        }
        assert!(DoorError::OutcomeUnknown.retryable());
    }

    #[tokio::test]
    async fn malformed_replies_are_not_successful_operations() {
        for text in [
            r#"{"state":"pending"}"#,
            r#"{"result":{"state":"surprise"}}"#,
            r#"{"result":{"state":"published","handoff":"released"}}"#,
        ] {
            let stub = Stub::new(vec![reply(text)]);
            assert!(matches!(
                stub.client().seal(&seal()).await,
                Err(DoorError::InvalidReply(_))
            ));
        }
    }
}

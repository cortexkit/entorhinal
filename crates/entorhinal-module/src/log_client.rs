//! Client for engram's identity log: a shared, append-only log that engram
//! signs and encrypts, so entorhinal sends and receives plaintext. The route is
//! opened on first use, never at startup.
//!
//! The contract, as engram serves it in engram-module/src/agent_sync.rs at
//! commit ee1d51918464c8e725db84a33bd4045c5982da61. That commit adds `author`
//! and hadn't reached engram's master branch when this was written. An engram
//! without it fails every read here as a protocol error, so the log must not be
//! enabled against an older engram:
//! - Service `agent-sync`, methods `identity_log.append` and
//!   `identity_log.read`. Entry ids and data travel as lowercase hex, and
//!   success replies are wrapped as `{result: ...}`.
//! - Read entries carry `signed_by_self` and `author`, the signing key's
//!   rotation-stable roster pseudonym as exactly 32 lowercase hex characters.
//!   The flag compares that author to the reader's pseudonym. Authors are only
//!   compared in memory, never persisted, logged or displayed.
//! - Size limits count decoded bytes: an entry's data is at most 256 KiB, and a
//!   read page at most 128 entries and 1 MiB.
//! - Engram checks, in order, that this device is a member, then whether this
//!   entry id was already appended, then that the log head still equals
//!   `expected_head`. An already-appended entry answers a plain `{position}`,
//!   its original position, even after the head has moved on.
//! - Engram recognises a resend only if the entry id, signer, `expected_head`,
//!   kind and data all match the first attempt, so a resend must carry the
//!   original `expected_head`, not the current head; anything else is refused
//!   `id_reused`.
//! - A refusal's extra fields (the head, a position, a size cap) arrive in the
//!   error body's `detail` field, never parsed out of the message.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{CallError, CallOptions, ConsumerOptions, SubcConsumer};
use subc_protocol::{BindIdentity, RouteTarget};

pub(crate) const APPEND: &str = "identity_log.append";
pub(crate) const READ: &str = "identity_log.read";
pub(crate) const HEAD_MOVED: &str = "head_moved";
pub(crate) const NOT_MEMBER: &str = "not_member";
pub(crate) const UNAVAILABLE: &str = "unavailable";
pub(crate) const ID_REUSED: &str = "id_reused";
pub(crate) const KEY_UNAVAILABLE: &str = "key_unavailable";
pub(crate) const VERIFY_FAILED: &str = "verify_failed";
pub(crate) const DECRYPT_FAILED: &str = "decrypt_failed";
pub(crate) const TOO_LARGE: &str = "too_large";
pub(crate) const ENTRY_DATA_CAP: usize = 256 * 1024;
pub(crate) const PAGE_ENTRIES_CAP: usize = 128;
pub(crate) const PAGE_DATA_CAP: usize = 1024 * 1024;

pub(crate) type EntryId = [u8; 16];

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum EntryKind {
    Change,
    Snapshot,
}

impl EntryKind {
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Change => "change",
            Self::Snapshot => "snapshot",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct AppendRequest {
    pub expected_head: u64,
    pub entry_id: EntryId,
    pub kind: EntryKind,
    pub data: Vec<u8>,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct AppendReply {
    pub position: u64,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct KeyId {
    pub family: String,
    pub epoch: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LogEntry {
    pub position: u64,
    pub entry_id: EntryId,
    pub signer: [u8; 32],
    pub signed_by_self: bool,
    pub author: String,
    pub key_id: KeyId,
    pub envelope_version: u64,
    // Preserve unknown kinds/versions so catch-up can stop at their position.
    pub kind: String,
    pub entry: Vec<u8>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ReadReply {
    pub head: u64,
    pub entries: Vec<LogEntry>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum LogError {
    // A missing head is still a refusal: catch up from the applied position.
    HeadMoved { head: Option<u64> },
    NotMember,
    Unavailable,
    IdReused,
    KeyUnavailable,
    VerifyFailed { position: Option<u64> },
    DecryptFailed { position: Option<u64> },
    TooLarge { field: String, cap: usize },
    NoReply,
    Protocol(String),
    Refused { code: String, detail: Option<Value> },
}

/// Maps an engram refusal code to a `LogError`, reading its extra fields (the
/// current head, the failing position, the exceeded cap) from `detail`. Every
/// refusal code entorhinal acts on is interpreted here and nowhere else.
pub(crate) fn decode_refusal(code: &str, detail: Option<Value>) -> LogError {
    match code {
        HEAD_MOVED => LogError::HeadMoved {
            head: detail.as_ref().and_then(|v| v["head"].as_u64()),
        },
        NOT_MEMBER => LogError::NotMember,
        UNAVAILABLE => LogError::Unavailable,
        ID_REUSED => LogError::IdReused,
        KEY_UNAVAILABLE => LogError::KeyUnavailable,
        VERIFY_FAILED => LogError::VerifyFailed {
            position: detail.as_ref().and_then(|v| v["position"].as_u64()),
        },
        DECRYPT_FAILED => LogError::DecryptFailed {
            position: detail.as_ref().and_then(|v| v["position"].as_u64()),
        },
        TOO_LARGE => match detail
            .as_ref()
            .and_then(|v| Some((v["field"].as_str()?, v["cap"].as_u64()?)))
        {
            Some((field, cap)) if usize::try_from(cap).is_ok() => LogError::TooLarge {
                field: field.into(),
                cap: cap as usize,
            },
            _ => LogError::Refused {
                code: code.into(),
                detail,
            },
        },
        _ => LogError::Refused {
            code: code.into(),
            detail,
        },
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TransportError {
    Refusal { code: String, detail: Option<Value> },
    Unavailable,
    NoReply,
}

impl From<TransportError> for LogError {
    fn from(error: TransportError) -> Self {
        match error {
            TransportError::Refusal { code, detail } => decode_refusal(&code, detail),
            TransportError::Unavailable => Self::Unavailable,
            TransportError::NoReply => Self::NoReply,
        }
    }
}

#[async_trait]
pub(crate) trait LogTransport: Send + Sync {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError>;
}

#[async_trait]
pub(crate) trait LogConnector: Send + Sync {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError>;
}

#[derive(Clone)]
pub(crate) struct LogClient {
    connector: Arc<dyn LogConnector>,
}

impl LogClient {
    /// Construction does no I/O. Only append and log-read can open a route.
    pub(crate) fn new(connector: Arc<dyn LogConnector>) -> Self {
        Self { connector }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, LogError> {
        let transport = self.connector.connect().await?;
        let bytes = transport.call(method, params).await?;
        let envelope: Value = serde_json::from_slice(&bytes).map_err(protocol)?;
        envelope
            .get("result")
            .cloned()
            .ok_or_else(|| LogError::Protocol("engram response has no result envelope".into()))
    }

    pub(crate) async fn append(&self, request: &AppendRequest) -> Result<AppendReply, LogError> {
        if request.data.len() > ENTRY_DATA_CAP {
            return Err(LogError::TooLarge {
                field: "entry.data".into(),
                cap: ENTRY_DATA_CAP,
            });
        }
        let reply = self
            .call(
                APPEND,
                json!({
                    "expected_head": request.expected_head,
                    "entry_id": encode_hex(&request.entry_id),
                    "entry": {"kind":request.kind.as_str(), "data":encode_hex(&request.data)},
                }),
            )
            .await?;
        let reply: AppendReply = serde_json::from_value(reply).map_err(protocol)?;
        if reply.position == 0 {
            return Err(LogError::Protocol(
                "append position must be positive".into(),
            ));
        }
        // A receipt may name an earlier position; never check expected_head here.
        Ok(reply)
    }

    pub(crate) async fn read(&self, after: u64, limit: usize) -> Result<ReadReply, LogError> {
        let limit = limit.clamp(1, PAGE_ENTRIES_CAP);
        let reply = self
            .call(READ, json!({"after":after, "limit":limit}))
            .await?;
        #[derive(Deserialize)]
        struct Page {
            head: u64,
            entries: Vec<Row>,
        }
        #[derive(Deserialize)]
        struct Row {
            position: u64,
            entry_id: String,
            signer: String,
            signed_by_self: bool,
            author: String,
            key_id: KeyId,
            envelope_version: u64,
            kind: String,
            entry: String,
        }
        let page: Page = serde_json::from_value(reply).map_err(protocol)?;
        if page.entries.len() > limit {
            return Err(LogError::Protocol(
                "engram page exceeds requested limit".into(),
            ));
        }
        let mut size = 0;
        let mut entries = Vec::with_capacity(page.entries.len());
        for row in page.entries {
            if row.author.len() != 32
                || !row
                    .author
                    .bytes()
                    .all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
            {
                return Err(protocol("author must be 32 lowercase hex characters"));
            }
            let entry = decode_hex(&row.entry)?;
            size += entry.len();
            if entry.len() > ENTRY_DATA_CAP || size > PAGE_DATA_CAP {
                return Err(LogError::Protocol(
                    "engram page exceeds decoded-byte cap".into(),
                ));
            }
            entries.push(LogEntry {
                position: row.position,
                entry_id: decode_hex(&row.entry_id)?
                    .try_into()
                    .map_err(|_| protocol("entry_id must be 16 bytes"))?,
                signer: decode_hex(&row.signer)?
                    .try_into()
                    .map_err(|_| protocol("signer must be 32 bytes"))?,
                signed_by_self: row.signed_by_self,
                author: row.author,
                key_id: row.key_id,
                envelope_version: row.envelope_version,
                kind: row.kind,
                entry,
            });
        }
        // Gaps between positions and a head lower than one already applied are
        // not checked here: only catch-up knows the last applied position, and
        // it refuses both against that recorded state.
        Ok(ReadReply {
            head: page.head,
            entries,
        })
    }
}

fn protocol(error: impl std::fmt::Display) -> LogError {
    LogError::Protocol(error.to_string())
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 15) as usize] as char);
    }
    text
}

pub(crate) fn decode_hex(text: &str) -> Result<Vec<u8>, LogError> {
    fn nibble(byte: u8) -> Result<u8, LogError> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err(protocol("expected lowercase hex")),
        }
    }
    if !text.len().is_multiple_of(2) {
        return Err(protocol("hex has odd length"));
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
        .collect()
}

/// SUBC caches the consumer connection and routes, but opens neither at startup.
pub(crate) struct SubcConnector {
    connection_file: Option<PathBuf>,
    consumer: tokio::sync::Mutex<Option<Arc<SubcTransport>>>,
    read_cwd: super::current_directory::CwdReader,
}

impl Default for SubcConnector {
    fn default() -> Self {
        Self {
            connection_file: None,
            consumer: tokio::sync::Mutex::new(None),
            read_cwd: std::env::current_dir,
        }
    }
}

impl SubcConnector {
    /// Use the supervisor's daemon, not discovery of a different live daemon.
    /// Parsing argv is local only; the connection file is not read until connect.
    pub(crate) fn from_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Self {
        let mut args = args.into_iter();
        while let Some(arg) = args.next() {
            let path = if arg == "--subc" {
                args.next().map(PathBuf::from)
            } else {
                arg.to_str()
                    .and_then(|s| s.strip_prefix("--subc="))
                    .map(PathBuf::from)
            };
            if let Some(path) = path {
                return Self {
                    connection_file: Some(path),
                    ..Self::default()
                };
            }
        }
        // Without `--subc`, fall back to the client library's default daemon
        // connection file. A supervised entorhinal always receives `--subc`;
        // the subc client library refuses to serve without a valid one.
        Self::default()
    }
}

#[async_trait]
impl LogConnector for SubcConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        let cwd =
            super::current_directory::read_current_directory(self.read_cwd).map_err(|error| {
                TransportError::Refusal {
                    code: error.code.into(),
                    detail: Some(json!({"message": error.message})),
                }
            })?;
        let mut slot = self.consumer.lock().await;
        if let Some(transport) = slot.as_ref() {
            return Ok(transport.clone());
        }
        let consumer = match &self.connection_file {
            Some(path) => SubcConsumer::connect(path, ConsumerOptions::default()).await,
            None => SubcConsumer::connect_default(ConsumerOptions::default()).await,
        }
        .map_err(|_| TransportError::Unavailable)?;
        let transport = Arc::new(SubcTransport { consumer, cwd });
        *slot = Some(transport.clone());
        Ok(transport)
    }
}

struct SubcTransport {
    consumer: SubcConsumer,
    cwd: String,
}

fn engram_target() -> RouteTarget {
    RouteTarget::InternalService {
        module_id: "engram".into(),
        service_id: "agent-sync".into(),
    }
}

#[async_trait]
impl LogTransport for SubcTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        let body = serde_json::to_vec(&json!({"method":method,"params":params}))
            .map_err(|_| TransportError::Unavailable)?;
        // Default call options identify this route with the launch secret the
        // daemon gave entorhinal at start, so the daemon verifies the caller as
        // `reserved:entorhinal`, the only principal engram lets append. They
        // present no operator identity and no session or flow scope.
        self.consumer
            .call(
                engram_target(),
                BindIdentity::new(&self.cwd, "ck-entorhinal", "identity-log"),
                body,
                CallOptions::default(),
            )
            .await
            .map_err(|error| match error {
                CallError::Module(body) => TransportError::Refusal {
                    code: body.code,
                    detail: body.detail,
                },
                CallError::OutcomeUnknown(_) => TransportError::NoReply,
                _ => TransportError::Unavailable,
            })
    }
}

#[cfg(test)]
mod tests {
    use super::super::fake_log::FailConnector;
    use super::*;
    use std::sync::Mutex;

    // This stub owns literal wire fixtures, not the fake's encoder: a symmetric
    // mistake in the client and fake must not make the wire test green.
    struct Stub {
        requests: Mutex<Vec<(String, Value)>>,
        reply: Mutex<Result<Vec<u8>, TransportError>>,
    }

    impl Stub {
        fn new(reply: Value) -> Arc<Self> {
            Arc::new(Self {
                requests: Mutex::new(Vec::new()),
                reply: Mutex::new(Ok(serde_json::to_vec(&reply).unwrap())),
            })
        }
    }

    struct StubConnector(Arc<Stub>);
    #[async_trait]
    impl LogConnector for StubConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    #[async_trait]
    impl LogTransport for Stub {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.requests.lock().unwrap().push((method.into(), params));
            self.reply.lock().unwrap().clone()
        }
    }

    fn client(stub: &Arc<Stub>) -> LogClient {
        LogClient::new(Arc::new(StubConnector(stub.clone())))
    }
    fn request() -> AppendRequest {
        AppendRequest {
            expected_head: 9,
            entry_id: [0xab; 16],
            kind: EntryKind::Snapshot,
            data: vec![0, 0xa1, 0xff],
        }
    }
    fn row() -> Value {
        json!({"position":10,"entry_id":"abababababababababababababababab",
            "signer":"5151515151515151515151515151515151515151515151515151515151515151",
            "signed_by_self":true,"author":"0123456789abcdef0123456789abcdef",
            "key_id":{"family":"bmk","epoch":7},"envelope_version":1,"kind":"snapshot","entry":"00a1ff"})
    }

    #[tokio::test]
    async fn wire_requests_and_metadata_match_engram_pin() {
        for args in [
            vec!["--subc", "target/nonexistent-daemon.json"],
            vec!["--subc=target/nonexistent-daemon.json"],
        ] {
            let connector =
                SubcConnector::from_args(args.into_iter().map(std::ffi::OsString::from));
            assert_eq!(
                connector.connection_file,
                Some(PathBuf::from("target/nonexistent-daemon.json"))
            );
            assert!(
                connector.consumer.lock().await.is_none(),
                "parsing must not open the daemon connection"
            );
        }
        assert_eq!(
            serde_json::to_value(engram_target()).unwrap(),
            json!({"kind":"internal_service","module_id":"engram","service_id":"agent-sync"})
        );
        let stub = Stub::new(json!({"result":{"position":10}}));
        let client = client(&stub);
        assert_eq!(
            client.append(&request()).await.unwrap(),
            AppendReply { position: 10 }
        );
        assert_eq!(
            stub.requests.lock().unwrap()[0],
            (
                "identity_log.append".into(),
                json!({
                    "expected_head":9,"entry_id":"abababababababababababababababab","entry":{"kind":"snapshot","data":"00a1ff"}
                })
            )
        );
        *stub.reply.lock().unwrap() =
            Ok(serde_json::to_vec(&json!({"result":{"head":10,"entries":[row()]}})).unwrap());
        let page = client.read(9, 999).await.unwrap();
        assert_eq!(
            page,
            ReadReply {
                head: 10,
                entries: vec![LogEntry {
                    position: 10,
                    entry_id: [0xab; 16],
                    signer: [0x51; 32],
                    signed_by_self: true,
                    author: "0123456789abcdef0123456789abcdef".into(),
                    key_id: KeyId {
                        family: "bmk".into(),
                        epoch: 7
                    },
                    envelope_version: 1,
                    kind: "snapshot".into(),
                    entry: vec![0, 0xa1, 0xff],
                }]
            }
        );
        assert_eq!(
            stub.requests.lock().unwrap()[1],
            ("identity_log.read".into(), json!({"after":9,"limit":128}))
        );
        *stub.reply.lock().unwrap() = Ok(br#"{"result":{"head":10,"entries":[]}}"#.to_vec());
        client.read(10, 0).await.unwrap();
        assert_eq!(
            stub.requests.lock().unwrap()[2].1,
            json!({"after":10,"limit":1})
        );
    }

    #[tokio::test]
    async fn all_refusals_decode_from_error_detail_without_aliases() {
        let stub = Stub::new(Value::Null);
        let client = client(&stub);
        let cases = [
            (
                "head_moved",
                Some(json!({"head":42})),
                LogError::HeadMoved { head: Some(42) },
            ),
            ("head_moved", None, LogError::HeadMoved { head: None }),
            (
                "head_moved",
                Some(json!({"head":"42"})),
                LogError::HeadMoved { head: None },
            ),
            ("not_member", None, LogError::NotMember),
            ("unavailable", None, LogError::Unavailable),
            ("id_reused", None, LogError::IdReused),
            ("key_unavailable", None, LogError::KeyUnavailable),
            (
                "verify_failed",
                Some(json!({"position":4})),
                LogError::VerifyFailed { position: Some(4) },
            ),
            (
                "decrypt_failed",
                Some(json!({"position":5})),
                LogError::DecryptFailed { position: Some(5) },
            ),
            (
                "too_large",
                Some(json!({"field":"entry.data","cap":262144})),
                LogError::TooLarge {
                    field: "entry.data".into(),
                    cap: 262144,
                },
            ),
            (
                "not_a_member",
                None,
                LogError::Refused {
                    code: "not_a_member".into(),
                    detail: None,
                },
            ),
        ];
        for (code, detail, expected) in cases {
            // Round-trip the actual SUBC ErrorBody: top-level extras do not exist.
            let body: subc_protocol::ErrorBody =
                serde_json::from_value(json!({"code":code,"message":"refusal","detail":detail}))
                    .unwrap();
            *stub.reply.lock().unwrap() = Err(TransportError::Refusal {
                code: body.code,
                detail: body.detail,
            });
            assert_eq!(
                client.append(&request()).await.unwrap_err(),
                expected,
                "append {code}"
            );
            assert_eq!(
                client.read(0, 128).await.unwrap_err(),
                expected,
                "read {code}"
            );
        }
        *stub.reply.lock().unwrap() = Err(TransportError::NoReply);
        assert_eq!(
            client.append(&request()).await.unwrap_err(),
            LogError::NoReply
        );
        *stub.reply.lock().unwrap() = Err(TransportError::Unavailable);
        assert_eq!(
            client.read(0, 128).await.unwrap_err(),
            LogError::Unavailable
        );
    }

    #[tokio::test]
    async fn malformed_replies_fail_closed_and_unknown_metadata_is_preserved() {
        for reply in [
            json!({"position":1}),
            json!({"result":{}}),
            json!({"result":{"position":0}}),
        ] {
            let stub = Stub::new(reply);
            assert!(matches!(
                client(&stub).append(&request()).await,
                Err(LogError::Protocol(_))
            ));
        }
        for (field, value) in [
            ("entry", json!("AA")),
            ("entry", json!("a")),
            ("signer", json!("00")),
            ("entry_id", json!("00")),
            ("key_id", json!({"epoch":1})),
            ("envelope_version", json!(null)),
        ] {
            let mut malformed = row();
            malformed[field] = value;
            let stub = Stub::new(json!({"result":{"head":10,"entries":[malformed]}}));
            assert!(
                matches!(client(&stub).read(9, 128).await, Err(LogError::Protocol(_))),
                "{field}"
            );
        }
        let stub = Stub::new(json!({"result":{"head":10,"entries":[row(),row()]}}));
        assert!(matches!(
            client(&stub).read(9, 1).await,
            Err(LogError::Protocol(_))
        ));
        let mut big = row();
        big["entry"] = json!("00".repeat(262145));
        let stub = Stub::new(json!({"result":{"head":10,"entries":[big]}}));
        assert!(matches!(
            client(&stub).read(9, 128).await,
            Err(LogError::Protocol(_))
        ));
        let mut full = row();
        full["entry"] = json!("00".repeat(262144));
        let stub = Stub::new(json!({"result":{"head":10,"entries":vec![full;5]}}));
        assert!(matches!(
            client(&stub).read(9, 128).await,
            Err(LogError::Protocol(_))
        ));
        let mut unknown = row();
        unknown["kind"] = json!("future");
        unknown["envelope_version"] = json!(99);
        let stub = Stub::new(json!({"result":{"head":10,"entries":[unknown]}}));
        let page = client(&stub).read(9, 128).await.unwrap();
        assert_eq!(page.entries[0].kind, "future");
        assert_eq!(page.entries[0].envelope_version, 99);
    }

    // Keep the body and positions valid so a missing authorship check would
    // apply the entry, rather than stall for an unrelated decoding error.
    fn catch_up_row() -> Value {
        let mut entry = row();
        entry["position"] = json!(1);
        entry["kind"] = json!("change");
        entry["entry"] = json!(encode_hex(br#"{"op":"project.shared","tables":{}}"#));
        entry
    }

    async fn assert_authorship_protocol_stalls(entry: Value, diagnostic: &str) {
        use crate::ProjectsHandler;
        use entorhinal_core::RegistryStore;
        use std::sync::atomic::Ordering;
        use tokio::time::{Duration, Instant};

        let stub = Stub::new(json!({"result":{"head":1,"entries":[entry]}}));
        let error = client(&stub).read(0, 128).await.unwrap_err();
        assert!(
            matches!(&error, LogError::Protocol(message) if message.contains(diagnostic)),
            "{error:?}"
        );

        let (dir, descriptor) = crate::tests::scratch_descriptor(&format!(
            "authorship-protocol-{}",
            crate::incarnation::new_incarnation().unwrap()
        ));
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                Ok(())
            })
            .unwrap();
        let before = store.identity_log_status().unwrap();
        let generation = store.generation().unwrap();
        let handler = ProjectsHandler::with_log_connector(
            "authorship-test".into(),
            || 700,
            Arc::new(StubConnector(stub.clone())),
        );
        let error = handler
            .catch_up(&store, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap_err();
        assert_eq!(error.code, "identity_log_stalled");
        assert!(error.message.contains(diagnostic), "{}", error.message);
        assert!(error.message.contains("at log position 1"));
        assert_eq!(handler.health.log_state.load(Ordering::Relaxed), 6);
        assert_eq!(store.identity_log_status().unwrap(), before);
        assert_eq!(store.generation().unwrap(), generation);
        assert!(store.enumerate(None).unwrap().projects.is_empty());
        drop(handler);

        // With the same valid body and repaired metadata, a fresh reader can
        // advance. The protocol error did not poison the durable progress.
        *stub.reply.lock().unwrap() = Ok(serde_json::to_vec(
            &json!({"result":{"head":1,"entries":[catch_up_row()]}}),
        )
        .unwrap());
        let handler = ProjectsHandler::with_log_connector(
            "authorship-test".into(),
            || 700,
            Arc::new(StubConnector(stub)),
        );
        handler
            .catch_up(&store, Instant::now() + Duration::from_secs(1))
            .await
            .unwrap();
        assert_eq!(
            store.identity_log_status().unwrap().last_applied_position,
            1
        );
        assert_eq!(handler.health.log_state.load(Ordering::Relaxed), 0);
        drop(handler);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn read_requires_signed_by_self_and_catch_up_stalls() {
        let mut missing = catch_up_row();
        missing.as_object_mut().unwrap().remove("signed_by_self");
        assert_authorship_protocol_stalls(missing, "signed_by_self").await;
        for value in [json!(null), json!("true"), json!(1)] {
            let mut invalid = catch_up_row();
            invalid["signed_by_self"] = value;
            assert_authorship_protocol_stalls(invalid, "boolean").await;
        }
    }

    #[tokio::test]
    async fn read_requires_author_and_catch_up_stalls() {
        let mut missing = catch_up_row();
        missing.as_object_mut().unwrap().remove("author");
        assert_authorship_protocol_stalls(missing, "author").await;
        for value in [json!(null), json!(false), json!(123)] {
            let mut invalid = catch_up_row();
            invalid["author"] = value;
            assert_authorship_protocol_stalls(invalid, "string").await;
        }
    }

    #[tokio::test]
    async fn read_rejects_noncanonical_author_and_catch_up_stalls() {
        for author in [
            "".to_string(),
            "a".repeat(31),
            "a".repeat(33),
            "A".repeat(32),
            "g".repeat(32),
            " ".repeat(32),
            "é".repeat(16),
        ] {
            let mut invalid = catch_up_row();
            invalid["author"] = json!(author);
            assert_authorship_protocol_stalls(
                invalid,
                "author must be 32 lowercase hex characters",
            )
            .await;
        }
    }

    #[tokio::test]
    async fn failing_connector_panics_on_any_log_access() {
        let connector = Arc::new(FailConnector::default());
        let client = LogClient::new(connector.clone());
        assert_eq!(connector.calls(), 0);
        let task = tokio::spawn(async move { client.read(0, 128).await });
        assert!(task.await.unwrap_err().is_panic());
        assert_eq!(connector.calls(), 1);
    }

    #[tokio::test]
    async fn startup_and_every_local_read_work_without_engram() {
        use super::super::*;
        let connector = Arc::new(FailConnector::default());
        let handler =
            ProjectsHandler::with_log_connector("offline".into(), unix_millis, connector.clone());
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/log-client-offline")
            .join(incarnation::new_incarnation().unwrap());
        std::fs::create_dir_all(&root).unwrap();
        let descriptor = StorageDescriptor {
            module_id: MODULE_ID.into(),
            storage_namespace: "default".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        handler
            .on_hello_ack(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: Some(serde_json::to_value(descriptor).unwrap()),
                machine_id: None,
            })
            .await;
        assert_eq!(handler.health().await.status, HealthStatus::Ok);
        let requests = [
            ("resolve", json!({"canonicalRoot":root})),
            ("resolve_project_id", json!({"projectId":"missing"})),
            (
                "resolve_remote",
                json!({"owner":"missing","repo":"missing"}),
            ),
            ("enumerate", json!({})),
            ("journal_tail", json!({"afterSeq":0})),
            ("trust", json!({"canonicalRoot":root})),
            ("verify", json!({})),
            ("identity_log.status", json!({})),
            (
                "resolve_root_key",
                json!({"projectId":"missing","kind":"remote","rootKey":"a/b"}),
            ),
            ("preview_attach_root", json!({"path":root})),
            (
                "agent.resolve",
                json!({"agent_id":"agent_0000000000000000"}),
            ),
            ("agent.resolve_name", json!({"name":"Missing"})),
            ("agent.list", json!({})),
            ("agent.peer_roster", json!({"workspace_id":"missing"})),
            (
                "agent.avatar_read",
                json!({"agentIds":["agent_0000000000000000"]}),
            ),
            (
                "agent.github_identity",
                json!({"agent_id":"agent_0000000000000000"}),
            ),
            ("agent.snapshot", json!({})),
            ("agent.fleet_identity", json!({})),
            (
                "agent.changes",
                json!({"incarnation":"offline","cursor":0,"wait":false}),
            ),
        ];
        for (method, params) in requests {
            let body = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
            let outcome = handler.handle_request_wait(&body, (9, 1)).await;
            let expected = match method {
                "resolve_root_key" | "preview_attach_root" => Some("identity_log_disabled"),
                "agent.peer_roster" => Some("registry_not_activated"),
                "agent.github_identity" => Some("unknown_agent"),
                _ => None,
            };
            match (outcome, expected) {
                (HandlerOutcome::Response(bytes), None) => {
                    assert!(
                        serde_json::from_slice::<Value>(&bytes).unwrap()["result"].is_object(),
                        "{method}"
                    );
                }
                (HandlerOutcome::Error { code, .. }, Some(expected)) => {
                    assert_eq!(code, expected, "{method}")
                }
                (other, _) => panic!("{method}: {other:?}"),
            }
        }
        assert_eq!(connector.calls(), 0);
        assert!(manifest().capabilities.unwrap().requires.is_empty());
        drop(handler);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod cwd_tests {
    use super::*;

    #[tokio::test]
    async fn cwd_unreadable_is_terminal_and_sends_no_frame() {
        let connector = SubcConnector {
            connection_file: Some(std::env::temp_dir().join("absent-cwd-refusal.json")),
            read_cwd: || Err(std::io::Error::other("injected cwd failure")),
            ..SubcConnector::default()
        };
        let error = match connector.connect().await {
            Ok(_) => panic!("cwd failure must refuse"),
            Err(error) => error,
        };
        assert!(matches!(error, TransportError::Refusal { code, .. } if code == "cwd_unreadable"));
        assert!(
            connector.consumer.lock().await.is_none(),
            "no consumer or route may be opened"
        );
    }
}

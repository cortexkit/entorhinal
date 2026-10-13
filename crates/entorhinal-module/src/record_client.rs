//! Generic record contract: reviewed engram record spec ct_00000000-0000-40f0.
//! Engram serving commit: not yet shipped; the release must pin its serving
//! `engram-module/src/agent_sync.rs` commit before enabling this client.
//!
//! `intent / Module operations` pins `record.read`, `record.read_many` and
//! `record.cas` on engram's `agent-sync` internal service. The shared SUBC
//! connector binds with daemon-verified `reserved:entorhinal`, no flow/session
//! scope, via `BindIdentity(cwd, ck-entorhinal, identity-log)` and default call
//! options. Replies use the existing `{result: ...}` envelope and lowercase hex.
//! `constraints / Subjects, Body, Closed refusal set` pins 256-byte subjects,
//! 64 KiB bodies and the refusal set below. `intent / record.read_many` pins
//! 1..=64 subjects, prefix paging and the 1 MiB plaintext cap.
//! `constraints / Cloud plane` pins checks: signer, signature/control head,
//! account-wide receipt lookup, then CAS. `intent / Rules 2, 4, 5` pins the
//! last-64-version resend window and stale reads from the highest confirmed
//! copy. A cloud version below that copy is refused. An unanswered write leaves
//! a persisted pending mark: offline reads refuse instead of showing its older
//! copy as current. An offline subject with neither copy nor mark is absent and
//! stale. `intent / record.read` pins `self_author` to
//! the identity log's rotation-stable, 16-byte roster pseudonym.

use super::log_client::{decode_hex, encode_hex, LogConnector, TransportError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;

pub(crate) const READ: &str = "record.read";
pub(crate) const READ_MANY: &str = "record.read_many";
pub(crate) const CAS: &str = "record.cas";
pub(crate) const SUBJECT_CAP: usize = 256;
pub(crate) const BODY_CAP: usize = 64 * 1024;
pub(crate) const MANY_CAP: usize = 64;
pub(crate) const PAGE_CAP: usize = 1024 * 1024;
pub(crate) const REFUSALS: &[&str] = &[
    "invalid_request",
    "caller_forbidden",
    "too_large",
    "key_unavailable",
    "decrypt_failed",
    "verify_failed",
    "not_member",
    "unavailable",
    "version_moved",
    "id_reused",
    "control_head_stale",
];

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum RecordError {
    Refused { code: String, detail: Option<Value> },
    NoReply,
    Protocol(String),
}

impl RecordError {
    pub(crate) fn refusal(code: &str, detail: Option<Value>) -> Self {
        if REFUSALS.contains(&code) {
            Self::Refused {
                code: code.into(),
                detail,
            }
        } else {
            Self::Protocol(format!("unknown record refusal {code}"))
        }
    }

    pub(crate) fn code(&self) -> &str {
        match self {
            Self::Refused { code, .. } => code,
            Self::NoReply => "unavailable",
            Self::Protocol(_) => "record_protocol",
        }
    }
}

impl From<TransportError> for RecordError {
    fn from(error: TransportError) -> Self {
        match error {
            TransportError::Refusal { code, detail } => Self::refusal(&code, detail),
            TransportError::Unavailable => Self::refusal("unavailable", None),
            TransportError::NoReply => Self::NoReply,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecordReply {
    pub absent: bool,
    pub version: u64,
    pub body: Option<Vec<u8>>,
    pub author: Option<String>,
    pub self_author: String,
    pub stale: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WireReply {
    absent: bool,
    version: u64,
    body: Option<String>,
    author: Option<String>,
    self_author: String,
    stale: bool,
}

pub(crate) fn valid_author(author: &str) -> bool {
    author.len() == 32
        && author
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn protocol(error: impl std::fmt::Display) -> RecordError {
    RecordError::Protocol(error.to_string())
}

pub(crate) fn validate_subject(subject: &str) -> Result<(), RecordError> {
    if subject.is_empty() {
        return Err(RecordError::refusal("invalid_request", None));
    }
    if subject.len() > SUBJECT_CAP {
        return Err(RecordError::refusal(
            "too_large",
            Some(json!({"field":"subject","cap":SUBJECT_CAP})),
        ));
    }
    Ok(())
}

fn decode_reply(value: Value) -> Result<RecordReply, RecordError> {
    if [
        "absent",
        "version",
        "body",
        "author",
        "self_author",
        "stale",
    ]
    .iter()
    .any(|key| value.get(key).is_none())
    {
        return Err(protocol("record reply is missing a required field"));
    }
    let wire: WireReply = serde_json::from_value(value).map_err(protocol)?;
    let body = wire
        .body
        .map(|s| decode_hex(&s).map_err(|error| protocol(format!("{error:?}"))))
        .transpose()?;
    if !valid_author(&wire.self_author)
        || wire.author.as_deref().is_some_and(|s| !valid_author(s))
        || body.as_ref().is_some_and(|b| b.len() > BODY_CAP)
        || (wire.absent && (wire.version != 0 || body.is_some() || wire.author.is_some()))
        || (!wire.absent && (wire.version == 0 || body.is_none() || wire.author.is_none()))
    {
        return Err(protocol("inconsistent record reply"));
    }
    Ok(RecordReply {
        absent: wire.absent,
        version: wire.version,
        body,
        author: wire.author,
        self_author: wire.self_author,
        stale: wire.stale,
    })
}

#[derive(Clone, Debug)]
pub(crate) struct CasRequest {
    pub subject: String,
    pub expected_version: u64,
    pub body: Vec<u8>,
    pub write_id: [u8; 16],
}

#[derive(Debug)]
pub(crate) enum CasSettlement {
    Landed(u64),
    Moved(RecordReply),
}

#[derive(Clone)]
pub(crate) struct RecordClient {
    connector: Arc<dyn LogConnector>,
}

/// Tests that exercise only the identity log must not open a live record route.
/// Claim tests explicitly inject their record peer instead.
#[cfg(test)]
pub(crate) struct UnavailableConnector;
#[cfg(test)]
#[async_trait::async_trait]
impl LogConnector for UnavailableConnector {
    async fn connect(&self) -> Result<Arc<dyn super::log_client::LogTransport>, TransportError> {
        Err(TransportError::Unavailable)
    }
}

impl RecordClient {
    pub(crate) fn new(connector: Arc<dyn LogConnector>) -> Self {
        Self { connector }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, RecordError> {
        // Each hop includes connection establishment in the read deadline.
        tokio::time::timeout(super::catch_up::READ_DEADLINE, async {
            let transport = self.connector.connect().await?;
            let bytes = transport.call(method, params).await?;
            let envelope: Value = serde_json::from_slice(&bytes).map_err(protocol)?;
            envelope
                .get("result")
                .cloned()
                .ok_or_else(|| protocol("record reply has no result envelope"))
        })
        .await
        .unwrap_or_else(|_| Err(RecordError::refusal("unavailable", None)))
    }

    pub(crate) async fn read(&self, subject: &str) -> Result<RecordReply, RecordError> {
        validate_subject(subject)?;
        decode_reply(self.call(READ, json!({"subject":subject})).await?)
    }

    /// Returns the answered prefix plus its first unanswered index. Refresh
    /// re-requests the suffix; it never treats an omitted row as absent.
    pub(crate) async fn read_many(
        &self,
        subjects: &[String],
    ) -> Result<(Vec<Result<RecordReply, RecordError>>, Option<usize>), RecordError> {
        if subjects.is_empty() || subjects.len() > MANY_CAP {
            return Err(RecordError::refusal("invalid_request", None));
        }
        for subject in subjects {
            validate_subject(subject)?;
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Page {
            entries: Vec<Value>,
            next: Option<usize>,
        }
        let page: Page =
            serde_json::from_value(self.call(READ_MANY, json!({"subjects":subjects})).await?)
                .map_err(protocol)?;
        if page.entries.is_empty()
            || page.entries.len() > subjects.len()
            || (page.entries.len() < subjects.len() && page.next != Some(page.entries.len()))
            || (page.entries.len() == subjects.len() && page.next.is_some())
        {
            return Err(protocol("invalid record page prefix"));
        }
        let mut total = 0;
        let mut entries = Vec::new();
        for value in page.entries {
            let reply = if let Some(error) = value.get("error") {
                let code = error["code"]
                    .as_str()
                    .ok_or_else(|| protocol("record entry has no error code"))?;
                if ![
                    "verify_failed",
                    "decrypt_failed",
                    "key_unavailable",
                    "unavailable",
                ]
                .contains(&code)
                {
                    return Err(protocol("invalid per-record refusal"));
                }
                Err(RecordError::refusal(
                    code,
                    error.get("detail").cloned().filter(|v| !v.is_null()),
                ))
            } else {
                Ok(decode_reply(value)?)
            };
            total += reply
                .as_ref()
                .ok()
                .and_then(|r| r.body.as_ref())
                .map_or(0, Vec::len);
            entries.push(reply);
        }
        if total > PAGE_CAP {
            return Err(protocol("record page exceeds plaintext cap"));
        }
        Ok((entries, page.next))
    }

    /// Resend the exact attempt first. Once its receipt has aged out of the
    /// 64-version window, only a verified subject read can establish whether it
    /// landed. Compare decoded values, not encoding choices such as whitespace.
    pub(crate) async fn cas_settled<T: PartialEq>(
        &self,
        request: &CasRequest,
        author: &str,
        decode: impl Fn(&[u8]) -> Result<T, &'static str>,
    ) -> Result<CasSettlement, RecordError> {
        match self.cas(request).await {
            Ok(version) => Ok(CasSettlement::Landed(version)),
            Err(error)
                if error.code() == "version_moved" || matches!(error, RecordError::NoReply) =>
            {
                let reply = self.read(&request.subject).await?;
                let expected =
                    decode(&request.body).map_err(|code| RecordError::refusal(code, None))?;
                let matches = reply
                    .body
                    .as_deref()
                    .and_then(|body| decode(body).ok())
                    .is_some_and(|body| body == expected);
                if !reply.absent && reply.author.as_deref() == Some(author) && matches {
                    Ok(CasSettlement::Landed(reply.version))
                } else {
                    Ok(CasSettlement::Moved(reply))
                }
            }
            Err(error) => Err(error),
        }
    }

    pub(crate) async fn cas(&self, request: &CasRequest) -> Result<u64, RecordError> {
        validate_subject(&request.subject)?;
        if request.body.len() > BODY_CAP {
            return Err(RecordError::refusal(
                "too_large",
                Some(json!({"field":"body","cap":BODY_CAP})),
            ));
        }
        let params = json!({"subject":request.subject,"expected_version":request.expected_version,"body":encode_hex(&request.body),"write_id":encode_hex(&request.write_id)});
        let reply = match self.call(CAS, params.clone()).await {
            Err(RecordError::NoReply) => self.call(CAS, params).await?,
            other => other?,
        };
        let version = reply["version"]
            .as_u64()
            .ok_or_else(|| protocol("cas reply has no version"))?;
        if version == 0 {
            return Err(protocol("cas reply has version zero"));
        }
        Ok(version)
    }
}

#[cfg(test)]
pub(crate) fn wire_reply(reply: &RecordReply) -> Value {
    json!({"absent":reply.absent,"version":reply.version,"body":reply.body.as_ref().map(|b| encode_hex(b)),"author":reply.author,"self_author":reply.self_author,"stale":reply.stale})
}

//! Pure wire codecs and coordination rules for move and inbox records.
//!
//! Record states describe the shared CAS protocol, not the machine-local driver
//! steps in `move_store`. No function here reads a store or performs a CAS.

use std::{collections::BTreeSet, fmt};

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::move_store::MoveSide;

/// Engram's generic-record body limit, in encoded bytes.
pub const BODY_CAP: usize = 64 * 1024;
/// Space reserved at publication for a later abort reason and bounded detail.
pub const ABORT_RESERVE: usize = 4096;
pub const MOVE_BODY_CAP: usize = BODY_CAP - ABORT_RESERVE;
pub const DETAIL_CAP: usize = 2048;
pub const INBOX_CAP: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MoveRecordState {
    Published,
    Staged,
    Claimed,
    Activated,
    Released,
    Aborted,
}

impl MoveRecordState {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Published => "published",
            Self::Staged => "staged",
            Self::Claimed => "claimed",
            Self::Activated => "activated",
            Self::Released => "released",
            Self::Aborted => "aborted",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoveReason {
    Operator,
    ReceiverInboxFull,
    MoveTooLarge,
    ParticipantRefused,
    DoorRefused,
    CutAuthorMismatch,
    Diverged,
    IncompatibleVersion,
    ClaimMoved,
    ProjectNotCheckedOut,
    Gone,
    ReceiverPrecondition,
    Other(String),
}

impl From<String> for MoveReason {
    fn from(reason: String) -> Self {
        match reason.as_str() {
            "operator" => Self::Operator,
            "receiver_inbox_full" => Self::ReceiverInboxFull,
            "move_too_large" => Self::MoveTooLarge,
            "participant_refused" => Self::ParticipantRefused,
            "door_refused" => Self::DoorRefused,
            "cut_author_mismatch" => Self::CutAuthorMismatch,
            "diverged" => Self::Diverged,
            "incompatible_version" => Self::IncompatibleVersion,
            "claim_moved" => Self::ClaimMoved,
            "project_not_checked_out" => Self::ProjectNotCheckedOut,
            "gone" => Self::Gone,
            "receiver_precondition" => Self::ReceiverPrecondition,
            _ => Self::Other(reason),
        }
    }
}

impl From<MoveReason> for String {
    fn from(reason: MoveReason) -> Self {
        match reason {
            MoveReason::Operator => "operator".into(),
            MoveReason::ReceiverInboxFull => "receiver_inbox_full".into(),
            MoveReason::MoveTooLarge => "move_too_large".into(),
            MoveReason::ParticipantRefused => "participant_refused".into(),
            MoveReason::DoorRefused => "door_refused".into(),
            MoveReason::CutAuthorMismatch => "cut_author_mismatch".into(),
            MoveReason::Diverged => "diverged".into(),
            MoveReason::IncompatibleVersion => "incompatible_version".into(),
            MoveReason::ClaimMoved => "claim_moved".into(),
            MoveReason::ProjectNotCheckedOut => "project_not_checked_out".into(),
            MoveReason::Gone => "gone".into(),
            MoveReason::ReceiverPrecondition => "receiver_precondition".into(),
            MoveReason::Other(reason) => reason,
        }
    }
}

impl Serialize for MoveReason {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&String::from(self.clone()))
    }
}

impl<'de> Deserialize<'de> for MoveReason {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let reason = String::deserialize(deserializer)?;
        Ok(MoveReason::from(reason))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRef {
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub reference: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExternalRef {
    pub ref_kind: String,
    #[serde(rename = "ref")]
    pub reference: String,
    pub note: String,
    /// Passed through without interpretation; the participant owns its schema.
    pub check: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveParticipant {
    pub module_id: String,
    pub refs: Vec<MoveRef>,
    pub external: Vec<ExternalRef>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct MoveRecord {
    pub v: u64,
    pub cut_id: String,
    pub agent_id: String,
    pub from: String,
    pub to: String,
    pub state: MoveRecordState,
    pub claim_version: u64,
    pub participants: Vec<MoveParticipant>,
    pub reason: Option<MoveReason>,
    pub detail: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingMove {
    pub cut_id: String,
    pub from: String,
    pub agent_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InboxRecord {
    pub v: u64,
    pub pending: Vec<PendingMove>,
}

impl Default for InboxRecord {
    fn default() -> Self {
        Self {
            v: 1,
            pending: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CodecError {
    UnknownVersion,
    CutIdMismatch,
    ParticipantUnsorted,
    ParticipantDuplicate,
    RefUnsorted,
    RefDuplicate,
    DetailTooLarge,
    MalformedBody,
    MoveTooLarge,
    BodyTooLarge,
    InboxFull,
    InboxDuplicate,
    InvalidTransition,
    AuthorMismatch,
    PlanMismatch,
}

impl CodecError {
    pub fn code(self) -> &'static str {
        match self {
            Self::UnknownVersion => "unknown_version",
            Self::CutIdMismatch => "cut_id_mismatch",
            Self::ParticipantUnsorted => "participant_unsorted",
            Self::ParticipantDuplicate => "participant_duplicate",
            Self::RefUnsorted => "ref_unsorted",
            Self::RefDuplicate => "ref_duplicate",
            Self::DetailTooLarge => "detail_too_large",
            Self::MalformedBody => "malformed_body",
            Self::MoveTooLarge => "move_too_large",
            Self::BodyTooLarge => "body_too_large",
            Self::InboxFull => "inbox_full",
            Self::InboxDuplicate => "inbox_duplicate",
            Self::InvalidTransition => "invalid_transition",
            Self::AuthorMismatch => "author_mismatch",
            Self::PlanMismatch => "plan_mismatch",
        }
    }
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.code())
    }
}

impl std::error::Error for CodecError {}

fn valid_cut_id(cut_id: &str) -> bool {
    cut_id.len() == 32
        && cut_id
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn check_version(v: u64) -> Result<(), CodecError> {
    if v == 1 {
        Ok(())
    } else {
        Err(CodecError::UnknownVersion)
    }
}

fn wire_value(body: &[u8]) -> Result<Value, CodecError> {
    if body.len() > BODY_CAP {
        return Err(CodecError::BodyTooLarge);
    }
    let value: Value = serde_json::from_slice(body).map_err(|_| CodecError::MalformedBody)?;
    let v = value
        .get("v")
        .and_then(Value::as_u64)
        .ok_or(CodecError::MalformedBody)?;
    check_version(v)?;
    Ok(value)
}

fn validate_move(subject: &str, record: &MoveRecord) -> Result<(), CodecError> {
    check_version(record.v)?;
    if subject.strip_prefix("move:") != Some(record.cut_id.as_str()) {
        return Err(CodecError::CutIdMismatch);
    }
    if !valid_cut_id(&record.cut_id) {
        return Err(CodecError::MalformedBody);
    }
    let mut modules = BTreeSet::new();
    for participant in &record.participants {
        if !modules.insert(&participant.module_id) {
            return Err(CodecError::ParticipantDuplicate);
        }
        let mut refs = BTreeSet::new();
        for reference in &participant.refs {
            if !refs.insert(reference) {
                return Err(CodecError::RefDuplicate);
            }
        }
        if participant.refs.windows(2).any(|pair| pair[0] > pair[1]) {
            return Err(CodecError::RefUnsorted);
        }
    }
    if record
        .participants
        .windows(2)
        .any(|pair| pair[0].module_id > pair[1].module_id)
    {
        return Err(CodecError::ParticipantUnsorted);
    }
    if let Some(detail) = &record.detail {
        if serde_json::to_vec(detail)
            .map_err(|_| CodecError::MalformedBody)?
            .len()
            > DETAIL_CAP
        {
            return Err(CodecError::DetailTooLarge);
        }
    }
    // Abort reasons are explanatory, so an older reader may preserve an unknown one.
    // States, versions, and other fields affect safe behavior and remain strict.
    if (record.state != MoveRecordState::Aborted
        && (record.reason.is_some() || record.detail.is_some()))
        || (record.state == MoveRecordState::Aborted && record.reason.is_none())
    {
        return Err(CodecError::MalformedBody);
    }
    Ok(())
}

fn check_move_size(state: MoveRecordState, len: usize) -> Result<(), CodecError> {
    if len > BODY_CAP {
        Err(CodecError::BodyTooLarge)
    } else if state != MoveRecordState::Aborted && len > MOVE_BODY_CAP {
        Err(CodecError::MoveTooLarge)
    } else {
        Ok(())
    }
}

/// Strict encoding: callers build aborts with `aborted_record` to bound detail.
pub fn encode_move_record(subject: &str, record: &MoveRecord) -> Result<Vec<u8>, CodecError> {
    validate_move(subject, record)?;
    let body = serde_json::to_vec(record).map_err(|_| CodecError::MalformedBody)?;
    check_move_size(record.state, body.len())?;
    Ok(body)
}

pub fn decode_move_record(subject: &str, body: &[u8]) -> Result<MoveRecord, CodecError> {
    let value = wire_value(body)?;
    // Null is meaningful on the wire; missing abort fields are not a full body.
    if value.get("reason").is_none() || value.get("detail").is_none() {
        return Err(CodecError::MalformedBody);
    }
    let record: MoveRecord = serde_json::from_slice(body).map_err(|_| CodecError::MalformedBody)?;
    validate_move(subject, &record)?;
    check_move_size(record.state, body.len())?;
    Ok(record)
}

/// Keep the full diagnostic in the local row, and put only this bounded value
/// in the shared record. Summary strings are themselves bounded so even a
/// malformed peer diagnostic cannot consume the abort reserve.
pub fn bounded_detail(detail: &Value) -> Value {
    if serde_json::to_vec(detail)
        .expect("JSON value serializes")
        .len()
        <= DETAIL_CAP
    {
        return detail.clone();
    }
    let mut summary = json!({
        "truncated": true,
        "op": summary_string(detail.get("op")),
        "code": summary_string(detail.get("code")),
    });
    if detail.get("module_id").and_then(Value::as_str).is_some() {
        summary["module_id"] = summary_string(detail.get("module_id"));
    }
    summary
}

fn summary_string(value: Option<&Value>) -> Value {
    let Some(text) = value.and_then(Value::as_str) else {
        return Value::Null;
    };
    // Three encoded strings of at most 512 bytes plus keys fit in DETAIL_CAP.
    let mut text: String = text.chars().take(512).collect();
    while serde_json::to_vec(&text).expect("string serializes").len() > 512 {
        text.pop();
    }
    Value::String(text)
}

pub fn aborted_record(
    record: &MoveRecord,
    reason: MoveReason,
    full_detail: Option<&Value>,
) -> MoveRecord {
    let mut aborted = record.clone();
    aborted.state = MoveRecordState::Aborted;
    aborted.reason = Some(reason);
    aborted.detail = full_detail.map(bounded_detail);
    aborted
}

fn validate_inbox(inbox: &InboxRecord) -> Result<(), CodecError> {
    check_version(inbox.v)?;
    if inbox.pending.len() > INBOX_CAP {
        return Err(CodecError::InboxFull);
    }
    let mut cuts = BTreeSet::new();
    for entry in &inbox.pending {
        if !valid_cut_id(&entry.cut_id) {
            return Err(CodecError::MalformedBody);
        }
        if !cuts.insert(&entry.cut_id) {
            return Err(CodecError::InboxDuplicate);
        }
    }
    Ok(())
}

pub fn encode_inbox_record(inbox: &InboxRecord) -> Result<Vec<u8>, CodecError> {
    validate_inbox(inbox)?;
    let body = serde_json::to_vec(inbox).map_err(|_| CodecError::MalformedBody)?;
    if body.len() > BODY_CAP {
        return Err(CodecError::BodyTooLarge);
    }
    Ok(body)
}

pub fn decode_inbox_record(body: &[u8]) -> Result<InboxRecord, CodecError> {
    wire_value(body)?;
    let inbox = serde_json::from_slice(body).map_err(|_| CodecError::MalformedBody)?;
    validate_inbox(&inbox)?;
    Ok(inbox)
}

/// Content idempotency is by cut_id, including a resend to a full inbox.
pub fn append_inbox(inbox: &InboxRecord, entry: PendingMove) -> Result<InboxRecord, CodecError> {
    validate_inbox(inbox)?;
    if inbox.pending.iter().any(|old| old.cut_id == entry.cut_id) {
        return Ok(inbox.clone());
    }
    let mut appended = inbox.clone();
    appended.pending.push(entry);
    encode_inbox_record(&appended)?;
    Ok(appended)
}

pub fn remove_inbox(inbox: &InboxRecord, cut_id: &str) -> Result<InboxRecord, CodecError> {
    validate_inbox(inbox)?;
    let mut removed = inbox.clone();
    removed.pending.retain(|entry| entry.cut_id != cut_id);
    Ok(removed)
}

/// `None` denotes an absent record. Claim settlement and the local activating
/// boundary remain driver preconditions; the table only checks state and side.
pub fn check_transition(
    from: Option<MoveRecordState>,
    to: MoveRecordState,
    writer: MoveSide,
) -> Result<(), CodecError> {
    use MoveRecordState::*;
    use MoveSide::{Receiver, Source};
    match (from, to, writer) {
        (None, Published, Source)
        | (Some(Published), Staged, Receiver)
        | (Some(Staged), Claimed, Receiver)
        | (Some(Claimed), Activated, Receiver)
        | (Some(Activated), Released, Source)
        | (Some(Published), Aborted, Source | Receiver)
        | (Some(Staged | Claimed), Aborted, Receiver) => Ok(()),
        _ => Err(CodecError::InvalidTransition),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordWriter {
    Source,
    Receiver,
    Either,
}

pub fn required_writer(state: MoveRecordState) -> RecordWriter {
    match state {
        MoveRecordState::Published | MoveRecordState::Released => RecordWriter::Source,
        MoveRecordState::Staged | MoveRecordState::Claimed | MoveRecordState::Activated => {
            RecordWriter::Receiver
        }
        MoveRecordState::Aborted => RecordWriter::Either,
    }
}

pub fn check_record_author(record: &MoveRecord, author: &str) -> Result<(), CodecError> {
    let matches = match required_writer(record.state) {
        RecordWriter::Source => author == record.from,
        RecordWriter::Receiver => author == record.to,
        RecordWriter::Either => author == record.from || author == record.to,
    };
    if matches {
        Ok(())
    } else {
        Err(CodecError::AuthorMismatch)
    }
}

/// Compare the immutable plan, not state or abort diagnostics. Subject/cut
/// matching is checked by the codec before this later-read check is applied.
pub fn check_pinned_plan(pinned: &MoveRecord, current: &MoveRecord) -> Result<(), CodecError> {
    if pinned.agent_id == current.agent_id
        && pinned.from == current.from
        && pinned.to == current.to
        && pinned.claim_version == current.claim_version
        && pinned.participants == current.participants
    {
        Ok(())
    } else {
        Err(CodecError::PlanMismatch)
    }
}

fn write_id(input: &str) -> [u8; 16] {
    let hash = blake3::hash(input.as_bytes());
    let mut id = [0; 16];
    id.copy_from_slice(&hash.as_bytes()[..16]);
    id
}

pub fn move_write_id(cut_id: &str, side: MoveSide, to_state: MoveRecordState) -> [u8; 16] {
    let side = match side {
        MoveSide::Source => "source",
        MoveSide::Receiver => "receiver",
    };
    write_id(&format!("move:{cut_id}:{side}:{}", to_state.as_str()))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClaimWrite {
    Take,
    Return,
}

pub fn claim_write_id(cut_id: &str, operation: ClaimWrite) -> [u8; 16] {
    let operation = match operation {
        ClaimWrite::Take => "take",
        ClaimWrite::Return => "return",
    };
    write_id(&format!("claim:{cut_id}:{operation}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const CUT: &str = "0123456789abcdef0123456789abcdef";
    const SUBJECT: &str = "move:0123456789abcdef0123456789abcdef";
    const MOVE_FIXTURE: &str = r#"{"v":1,"cut_id":"0123456789abcdef0123456789abcdef","agent_id":"agent-a","from":"mac-a","to":"mac-b","state":"published","claim_version":3,"participants":[{"module_id":"alpha","refs":[{"ref_kind":"pi-session","ref":"session-a"}],"external":[{"ref_kind":"pi-session","ref":"session-a","note":"Copy the transcript","check":{"digest":"abc","row":7}}]},{"module_id":"beta","refs":[{"ref_kind":"pi-session","ref":"session-a"}],"external":[]}],"reason":null,"detail":null}"#;
    const INBOX_FIXTURE: &str = r#"{"v":1,"pending":[{"cut_id":"0123456789abcdef0123456789abcdef","from":"mac-a","agent_id":"agent-a"}]}"#;

    fn record() -> MoveRecord {
        MoveRecord {
            v: 1,
            cut_id: CUT.into(),
            agent_id: "agent-a".into(),
            from: "mac-a".into(),
            to: "mac-b".into(),
            state: MoveRecordState::Published,
            claim_version: 3,
            participants: vec![
                MoveParticipant {
                    module_id: "alpha".into(),
                    refs: vec![MoveRef {
                        ref_kind: "pi-session".into(),
                        reference: "session-a".into(),
                    }],
                    external: vec![ExternalRef {
                        ref_kind: "pi-session".into(),
                        reference: "session-a".into(),
                        note: "Copy the transcript".into(),
                        check: json!({"digest":"abc", "row":7}),
                    }],
                },
                MoveParticipant {
                    module_id: "beta".into(),
                    refs: vec![MoveRef {
                        ref_kind: "pi-session".into(),
                        reference: "session-a".into(),
                    }],
                    external: vec![],
                },
            ],
            reason: None,
            detail: None,
        }
    }

    fn entry(cut_id: &str) -> PendingMove {
        PendingMove {
            cut_id: cut_id.into(),
            from: "mac-a".into(),
            agent_id: "agent-a".into(),
        }
    }

    fn refuses(record: &MoveRecord, error: CodecError) {
        assert_eq!(encode_move_record(SUBJECT, record), Err(error));
        let body = serde_json::to_vec(record).unwrap();
        assert_eq!(decode_move_record(SUBJECT, &body), Err(error));
        assert!(!error.code().is_empty());
    }

    #[test]
    fn move_wire_fixture_accepts_a_ref_under_two_participants() {
        let expected = record();
        assert_eq!(
            decode_move_record(SUBJECT, MOVE_FIXTURE.as_bytes()),
            Ok(expected.clone())
        );
        assert_eq!(
            encode_move_record(SUBJECT, &expected).unwrap(),
            MOVE_FIXTURE.as_bytes()
        );
        assert_eq!(
            serde_json::to_value(&expected).unwrap()["participants"][0]["external"][0]["check"],
            json!({"digest":"abc", "row":7})
        );
    }

    #[test]
    fn move_codec_accepts_literal_incompatible_version_abort() {
        let literal = r#"{"v":1,"cut_id":"0123456789abcdef0123456789abcdef","agent_id":"agent-a","from":"mac-a","to":"mac-b","state":"aborted","claim_version":0,"participants":[],"reason":"incompatible_version","detail":{"code":"incompatible_version","module_id":"adapter","note":"Upgrade this Mac"}}"#;
        let subject = "move:0123456789abcdef0123456789abcdef";
        let body = decode_move_record(subject, literal.as_bytes()).unwrap();
        assert_eq!(body.reason, Some(MoveReason::IncompatibleVersion));
        assert_eq!(
            encode_move_record(subject, &body).unwrap(),
            literal.as_bytes()
        );
    }

    #[test]
    fn move_codec_preserves_unknown_abort_reason_opaquely() {
        let literal = r#"{"v":1,"cut_id":"0123456789abcdef0123456789abcdef","agent_id":"agent-a","from":"mac-a","to":"mac-b","state":"aborted","claim_version":0,"participants":[],"reason":"some_future_reason","detail":null}"#;
        let subject = "move:0123456789abcdef0123456789abcdef";
        let body = decode_move_record(subject, literal.as_bytes()).unwrap();
        assert_eq!(
            body.reason,
            Some(MoveReason::Other("some_future_reason".into()))
        );
        assert_eq!(
            serde_json::to_value(&body).unwrap()["reason"],
            "some_future_reason"
        );
        assert_eq!(
            encode_move_record(subject, &body).unwrap(),
            literal.as_bytes()
        );
    }

    #[test]
    fn move_codec_refuses_unknown_reason_on_non_aborted_state() {
        let mut value: Value = serde_json::from_str(MOVE_FIXTURE).unwrap();
        value["reason"] = json!("some_future_reason");
        assert_eq!(
            decode_move_record(SUBJECT, &serde_json::to_vec(&value).unwrap()),
            Err(CodecError::MalformedBody)
        );
    }

    #[test]
    fn move_codec_refuses_unknown_state() {
        let mut value: Value = serde_json::from_str(MOVE_FIXTURE).unwrap();
        value["state"] = json!("future_state");
        assert_eq!(
            decode_move_record(SUBJECT, &serde_json::to_vec(&value).unwrap()),
            Err(CodecError::MalformedBody)
        );
    }

    #[test]
    fn move_codec_refuses_unknown_version_by_name() {
        let mut value = record();
        value.v = 2;
        refuses(&value, CodecError::UnknownVersion);
        assert_eq!(CodecError::UnknownVersion.code(), "unknown_version");
    }

    #[test]
    fn move_codec_refuses_subject_cut_mismatch_by_name() {
        let mut value = record();
        value.cut_id = "f".repeat(32);
        refuses(&value, CodecError::CutIdMismatch);
        assert_eq!(
            decode_move_record("inbox:mac-a", MOVE_FIXTURE.as_bytes()),
            Err(CodecError::CutIdMismatch)
        );
        assert_eq!(CodecError::CutIdMismatch.code(), "cut_id_mismatch");
    }

    #[test]
    fn move_codec_refuses_unsorted_participants_by_name() {
        let mut value = record();
        value.participants.swap(0, 1);
        refuses(&value, CodecError::ParticipantUnsorted);
        assert_eq!(
            CodecError::ParticipantUnsorted.code(),
            "participant_unsorted"
        );
    }

    #[test]
    fn move_codec_refuses_duplicate_participants_by_name() {
        let mut value = record();
        value.participants.push(value.participants[0].clone());
        refuses(&value, CodecError::ParticipantDuplicate);
        assert_eq!(
            CodecError::ParticipantDuplicate.code(),
            "participant_duplicate"
        );
    }

    #[test]
    fn move_codec_refuses_unsorted_refs_by_name() {
        let mut value = record();
        value.participants[0].refs.push(MoveRef {
            ref_kind: "a-kind".into(),
            reference: "z".into(),
        });
        refuses(&value, CodecError::RefUnsorted);
        value.participants[0].refs.sort();
        value.participants[0].refs.push(MoveRef {
            ref_kind: "pi-session".into(),
            reference: "session-0".into(),
        });
        refuses(&value, CodecError::RefUnsorted);
        assert_eq!(CodecError::RefUnsorted.code(), "ref_unsorted");
    }

    #[test]
    fn move_codec_refuses_the_same_ref_twice_within_one_participant() {
        let mut value = record();
        let duplicate = value.participants[0].refs[0].clone();
        value.participants[0].refs.push(duplicate);
        refuses(&value, CodecError::RefDuplicate);
        assert_eq!(CodecError::RefDuplicate.code(), "ref_duplicate");
    }

    #[test]
    fn move_codec_refuses_oversized_detail_by_name() {
        let mut value = aborted_record(&record(), MoveReason::Operator, None);
        value.detail = Some(json!("a".repeat(DETAIL_CAP - 2)));
        assert!(encode_move_record(SUBJECT, &value).is_ok());
        value.detail = Some(json!("a".repeat(DETAIL_CAP - 1)));
        refuses(&value, CodecError::DetailTooLarge);
        // Limits are encoded bytes, not Unicode character counts.
        value.detail = Some(json!("é".repeat(DETAIL_CAP / 2)));
        refuses(&value, CodecError::DetailTooLarge);
        assert_eq!(CodecError::DetailTooLarge.code(), "detail_too_large");
    }

    #[test]
    fn move_codec_refuses_malformed_bodies_by_name() {
        for bytes in [
            b"not JSON".as_slice(),
            b"[]",
            b"{}",
            b"{\"v\":1}",
            b"{\"v\":\"1\"}",
            b"{\"v\":-1}",
        ] {
            assert_eq!(
                decode_move_record(SUBJECT, bytes),
                Err(CodecError::MalformedBody)
            );
        }
        let fixture: Value = serde_json::from_str(MOVE_FIXTURE).unwrap();
        for key in [
            "v",
            "cut_id",
            "agent_id",
            "from",
            "to",
            "state",
            "claim_version",
            "participants",
            "reason",
            "detail",
        ] {
            let mut value = fixture.clone();
            value.as_object_mut().unwrap().remove(key);
            assert_eq!(
                decode_move_record(SUBJECT, &serde_json::to_vec(&value).unwrap()),
                Err(CodecError::MalformedBody),
                "missing {key}"
            );
        }
        for (pointer, replacement) in [
            ("/state", json!("preparing")),
            ("/claim_version", json!(-1)),
            ("/participants/0/refs/0/ref", json!(7)),
            ("/participants/0/external/0/note", json!({})),
            ("/reason", json!("operator")),
            ("/detail", json!({"code":"busy"})),
        ] {
            let mut value = fixture.clone();
            *value.pointer_mut(pointer).unwrap() = replacement;
            assert_eq!(
                decode_move_record(SUBJECT, &serde_json::to_vec(&value).unwrap()),
                Err(CodecError::MalformedBody),
                "bad {pointer}"
            );
        }
        for pointer in [
            "",
            "/participants/0",
            "/participants/0/refs/0",
            "/participants/0/external/0",
        ] {
            let mut value = fixture.clone();
            value.pointer_mut(pointer).unwrap()["unknown"] = json!(true);
            assert_eq!(
                decode_move_record(SUBJECT, &serde_json::to_vec(&value).unwrap()),
                Err(CodecError::MalformedBody),
                "unknown field at {pointer}"
            );
        }
        let mut value = record();
        value.cut_id = "A".repeat(32);
        assert_eq!(
            encode_move_record(&format!("move:{}", value.cut_id), &value),
            Err(CodecError::MalformedBody)
        );
        assert_eq!(CodecError::MalformedBody.code(), "malformed_body");
    }

    #[test]
    fn oversized_detail_becomes_a_bounded_summary_without_changing_the_row_detail() {
        let full = json!({"module_id":"alpha", "op":"agent_data.prepare", "code":"busy", "extra":"é".repeat(2048)});
        let aborted = aborted_record(&record(), MoveReason::ParticipantRefused, Some(&full));
        assert_eq!(
            aborted.detail,
            Some(
                json!({"truncated":true, "module_id":"alpha", "op":"agent_data.prepare", "code":"busy"})
            )
        );
        assert_eq!(full["extra"].as_str().unwrap().chars().count(), 2048);
        let door = json!({"op":"cut.status", "code":"failed", "extra":"x".repeat(2048)});
        assert_eq!(
            bounded_detail(&door),
            json!({"truncated":true, "op":"cut.status", "code":"failed"})
        );
        let small = json!({"op":"cut.status", "code":"failed"});
        assert_eq!(bounded_detail(&small), small);
        let hostile = json!({"module_id":"\u{0000}".repeat(2048), "op":"é".repeat(2048), "code":"\\\"".repeat(2048)});
        let summary = bounded_detail(&hostile);
        assert!(serde_json::to_vec(&summary).unwrap().len() <= DETAIL_CAP);
        assert_eq!(summary["truncated"], true);
        assert_eq!(
            bounded_detail(&json!("x".repeat(2048))),
            json!({"truncated":true,"op":null,"code":null})
        );
    }

    #[test]
    fn largest_admitted_body_always_has_room_for_an_abort() {
        assert_eq!(
            (BODY_CAP, MOVE_BODY_CAP, DETAIL_CAP, INBOX_CAP),
            (65536, 61440, 2048, 64)
        );
        let mut largest = record();
        let base_len = encode_move_record(SUBJECT, &largest).unwrap().len();
        largest.participants[0].external[0]
            .note
            .push_str(&"x".repeat(61440 - base_len));
        assert_eq!(encode_move_record(SUBJECT, &largest).unwrap().len(), 61440);
        assert_eq!(
            decode_move_record(SUBJECT, &encode_move_record(SUBJECT, &largest).unwrap()),
            Ok(largest.clone())
        );
        let mut over = largest.clone();
        over.participants[0].external[0].note.push('x');
        refuses(&over, CodecError::MoveTooLarge);
        let reasons = [
            MoveReason::Operator,
            MoveReason::ReceiverInboxFull,
            MoveReason::MoveTooLarge,
            MoveReason::ParticipantRefused,
            MoveReason::DoorRefused,
            MoveReason::CutAuthorMismatch,
            MoveReason::Diverged,
            MoveReason::ClaimMoved,
            MoveReason::ProjectNotCheckedOut,
            MoveReason::Gone,
            MoveReason::ReceiverPrecondition,
            MoveReason::IncompatibleVersion,
        ];
        let details = [
            None,
            Some(json!("x".repeat(2046))),
            Some(
                json!({"module_id":"alpha", "op":"agent_data.activate", "code":"external_not_present", "extra":"x".repeat(4096)}),
            ),
        ];
        for reason in reasons {
            for detail in &details {
                let aborted = aborted_record(&largest, reason.clone(), detail.as_ref());
                let body = encode_move_record(SUBJECT, &aborted).unwrap();
                assert!(body.len() <= 65536, "{reason:?}");
                assert_eq!(decode_move_record(SUBJECT, &body), Ok(aborted));
            }
        }
        let mut huge = aborted_record(&largest, MoveReason::Operator, None);
        let len = serde_json::to_vec(&huge).unwrap().len();
        huge.participants[0].external[0]
            .note
            .push_str(&"x".repeat(65537 - len));
        refuses(&huge, CodecError::BodyTooLarge);
    }

    #[test]
    fn inbox_wire_fixture_and_idempotent_content_edits() {
        let empty = InboxRecord::default();
        assert_eq!(
            encode_inbox_record(&empty).unwrap(),
            b"{\"v\":1,\"pending\":[]}"
        );
        let appended = append_inbox(&empty, entry(CUT)).unwrap();
        assert!(empty.pending.is_empty());
        assert_eq!(
            encode_inbox_record(&appended).unwrap(),
            INBOX_FIXTURE.as_bytes()
        );
        assert_eq!(
            decode_inbox_record(INBOX_FIXTURE.as_bytes()),
            Ok(appended.clone())
        );
        let mut resend = entry(CUT);
        resend.agent_id = "ignored-on-resend".into();
        assert_eq!(append_inbox(&appended, resend), Ok(appended.clone()));
        let second = append_inbox(&appended, entry(&"f".repeat(32))).unwrap();
        assert_eq!(
            remove_inbox(&second, CUT).unwrap().pending,
            vec![entry(&"f".repeat(32))]
        );
        assert_eq!(remove_inbox(&appended, CUT), Ok(empty.clone()));
        assert_eq!(remove_inbox(&empty, CUT), Ok(empty));
    }

    #[test]
    fn inbox_caps_at_64_and_allows_resend_when_full() {
        let mut inbox = InboxRecord::default();
        for i in 0..64 {
            inbox = append_inbox(&inbox, entry(&format!("{i:032x}"))).unwrap();
        }
        assert_eq!(inbox.pending.len(), 64);
        let bytes = encode_inbox_record(&inbox).unwrap();
        assert_eq!(decode_inbox_record(&bytes), Ok(inbox.clone()));
        assert_eq!(
            append_inbox(&inbox, inbox.pending[0].clone()),
            Ok(inbox.clone())
        );
        assert_eq!(append_inbox(&inbox, entry(CUT)), Err(CodecError::InboxFull));
        let mut over = inbox;
        over.pending.push(entry(CUT));
        assert_eq!(encode_inbox_record(&over), Err(CodecError::InboxFull));
        assert_eq!(
            decode_inbox_record(&serde_json::to_vec(&over).unwrap()),
            Err(CodecError::InboxFull)
        );
    }

    #[test]
    fn inbox_codec_refuses_unknown_malformed_duplicate_and_oversized_bodies() {
        assert_eq!(
            decode_inbox_record(b"{\"v\":2,\"pending\":[]}"),
            Err(CodecError::UnknownVersion)
        );
        for body in [
            "{}",
            "null",
            "invalid JSON",
            "{\"v\":1}",
            "{\"v\":1,\"pending\":null}",
            "{\"v\":1,\"pending\":[],\"extra\":true}",
        ] {
            assert_eq!(
                decode_inbox_record(body.as_bytes()),
                Err(CodecError::MalformedBody)
            );
        }
        let mut duplicate = InboxRecord {
            v: 1,
            pending: vec![entry(CUT), entry(CUT)],
        };
        assert_eq!(
            encode_inbox_record(&duplicate),
            Err(CodecError::InboxDuplicate)
        );
        assert_eq!(
            decode_inbox_record(&serde_json::to_vec(&duplicate).unwrap()),
            Err(CodecError::InboxDuplicate)
        );
        duplicate.pending.pop();
        duplicate.v = 2;
        assert_eq!(
            encode_inbox_record(&duplicate),
            Err(CodecError::UnknownVersion)
        );
        duplicate.v = 1;
        duplicate.pending[0].cut_id = "bad".into();
        assert_eq!(
            encode_inbox_record(&duplicate),
            Err(CodecError::MalformedBody)
        );
        let mut value: Value = serde_json::from_str(INBOX_FIXTURE).unwrap();
        value["pending"][0]["extra"] = json!(true);
        assert_eq!(
            decode_inbox_record(&serde_json::to_vec(&value).unwrap()),
            Err(CodecError::MalformedBody)
        );
        let mut huge = InboxRecord {
            v: 1,
            pending: vec![entry(CUT)],
        };
        huge.pending[0].from = "x".repeat(65536);
        assert_eq!(encode_inbox_record(&huge), Err(CodecError::BodyTooLarge));
        assert_eq!(
            decode_inbox_record(&serde_json::to_vec(&huge).unwrap()),
            Err(CodecError::BodyTooLarge)
        );
    }

    #[test]
    fn transition_table_exhaustively_refuses_every_unlisted_row() {
        use MoveRecordState::*;
        use MoveSide::{Receiver, Source};
        let allowed = [
            (None, Published, Source),
            (Some(Published), Staged, Receiver),
            (Some(Staged), Claimed, Receiver),
            (Some(Claimed), Activated, Receiver),
            (Some(Activated), Released, Source),
            (Some(Published), Aborted, Source),
            (Some(Published), Aborted, Receiver),
            (Some(Staged), Aborted, Receiver),
            (Some(Claimed), Aborted, Receiver),
        ];
        let states = [Published, Staged, Claimed, Activated, Released, Aborted];
        let mut accepted = 0;
        let mut rejected = 0;
        for from in std::iter::once(None).chain(states.into_iter().map(Some)) {
            for to in states {
                for writer in [Source, Receiver] {
                    let result = check_transition(from, to, writer);
                    if allowed.contains(&(from, to, writer)) {
                        assert_eq!(result, Ok(()), "{from:?} -> {to:?} by {writer:?}");
                        accepted += 1;
                    } else {
                        assert_eq!(
                            result,
                            Err(CodecError::InvalidTransition),
                            "{from:?} -> {to:?} by {writer:?}"
                        );
                        rejected += 1;
                    }
                }
            }
        }
        assert_eq!((accepted, rejected), (9, 75));
    }

    #[test]
    fn author_check_maps_every_state_to_its_required_writer() {
        use MoveRecordState::*;
        use RecordWriter::{Either, Receiver, Source};
        for (state, writer) in [
            (Published, Source),
            (Staged, Receiver),
            (Claimed, Receiver),
            (Activated, Receiver),
            (Released, Source),
            (Aborted, Either),
        ] {
            let mut value = record();
            value.state = state;
            assert_eq!(required_writer(state), writer);
            for (author, accepted) in [
                ("mac-a", writer != Receiver),
                ("mac-b", writer != Source),
                ("third-mac", false),
            ] {
                assert_eq!(
                    check_record_author(&value, author),
                    if accepted {
                        Ok(())
                    } else {
                        Err(CodecError::AuthorMismatch)
                    },
                    "{state:?} by {author}"
                );
            }
        }
    }

    #[test]
    fn pinned_plan_checks_each_immutable_field_including_external_data() {
        let pinned = record();
        let current = aborted_record(
            &pinned,
            MoveReason::Operator,
            Some(&json!({"code":"operator"})),
        );
        assert_eq!(check_pinned_plan(&pinned, &current), Ok(()));
        let pinned_json = serde_json::to_value(&pinned).unwrap();
        for (pointer, replacement) in [
            ("/agent_id", json!("another-agent")),
            ("/from", json!("another-source")),
            ("/to", json!("another-receiver")),
            ("/claim_version", json!(4)),
            ("/participants/0/module_id", json!("other-module")),
            ("/participants/0/refs/0/ref_kind", json!("other-kind")),
            ("/participants/0/refs/0/ref", json!("other-ref")),
            ("/participants/0/external/0/note", json!("changed note")),
            ("/participants/0/external/0/check", json!({"row":8})),
        ] {
            let mut changed = pinned_json.clone();
            *changed.pointer_mut(pointer).unwrap() = replacement;
            let changed = serde_json::from_value(changed).unwrap();
            assert_eq!(
                check_pinned_plan(&pinned, &changed),
                Err(CodecError::PlanMismatch),
                "{pointer}"
            );
        }
    }

    #[test]
    fn write_ids_are_deterministic_and_use_the_exact_blake3_inputs() {
        use MoveRecordState::*;
        use MoveSide::{Receiver, Source};
        let fixtures = [
            (
                move_write_id(CUT, Source, Published),
                "move:0123456789abcdef0123456789abcdef:source:published",
            ),
            (
                move_write_id(CUT, Receiver, Staged),
                "move:0123456789abcdef0123456789abcdef:receiver:staged",
            ),
            (
                claim_write_id(CUT, ClaimWrite::Take),
                "claim:0123456789abcdef0123456789abcdef:take",
            ),
            (
                claim_write_id(CUT, ClaimWrite::Return),
                "claim:0123456789abcdef0123456789abcdef:return",
            ),
        ];
        for (id, literal) in fixtures {
            assert_eq!(id, blake3::hash(literal.as_bytes()).as_bytes()[..16]);
        }
        let mut ids = BTreeSet::new();
        for cut in [CUT, "ffffffffffffffffffffffffffffffff"] {
            for side in [Source, Receiver] {
                for state in [Published, Staged, Claimed, Activated, Released, Aborted] {
                    let id = move_write_id(cut, side, state);
                    assert_eq!(id, move_write_id(cut, side, state));
                    assert!(ids.insert(id));
                }
            }
            for operation in [ClaimWrite::Take, ClaimWrite::Return] {
                let id = claim_write_id(cut, operation);
                assert_eq!(id, claim_write_id(cut, operation));
                assert!(ids.insert(id));
            }
        }
        assert_eq!(ids.len(), 28);
    }
}

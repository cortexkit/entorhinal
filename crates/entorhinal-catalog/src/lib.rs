//! Pure CK catalog v1 primitives. All bytes, trust decisions and time are inputs;
//! this crate performs no I/O and never installs or executes package content.

pub mod archive;
pub mod jcs;
pub mod names;
pub mod record;
pub mod resolver;
pub mod semver;
pub mod statements;
pub mod status;

use ed25519_dalek::{Signature, VerifyingKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Contract refusal codes shared by document verifiers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RefusalCode {
    NameInvalid,
    NameNotCanonical,
    ScopeReserved,
    IdBound,
    RecordInvalid,
    SignatureInvalid,
    KeyUnknown,
    KeyRevoked,
    KeyNotValid,
    RecordMismatch,
    ArchiveInvalid,
    ArchiveTooLarge,
    ManifestTooLarge,
    PathInvalid,
    PathTooLong,
    PathNotUstar,
    ManifestInvalid,
    BodyRefMissing,
    FilesMismatch,
    StatusInvalid,
    FeedRollback,
    StatusTerminal,
    FeedAfterUnavailable,
    StatementInvalid,
    KeyConflict,
    KeyReused,
    StatusSequenceConflict,
    ActionNotAllowed,
}
impl RefusalCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::NameInvalid => "name_invalid",
            Self::NameNotCanonical => "name_not_canonical",
            Self::ScopeReserved => "scope_reserved",
            Self::IdBound => "id_bound",
            Self::RecordInvalid => "record_invalid",
            Self::SignatureInvalid => "signature_invalid",
            Self::KeyUnknown => "key_unknown",
            Self::KeyRevoked => "key_revoked",
            Self::KeyNotValid => "key_not_valid",
            Self::RecordMismatch => "record_mismatch",
            Self::ArchiveInvalid => "archive_invalid",
            Self::ArchiveTooLarge => "archive_too_large",
            Self::ManifestTooLarge => "manifest_too_large",
            Self::PathInvalid => "path_invalid",
            Self::PathTooLong => "path_too_long",
            Self::PathNotUstar => "path_not_ustar",
            Self::ManifestInvalid => "manifest_invalid",
            Self::BodyRefMissing => "body_ref_missing",
            Self::FilesMismatch => "files_mismatch",
            Self::StatusInvalid => "status_invalid",
            Self::FeedRollback => "feed_rollback",
            Self::StatusTerminal => "status_terminal",
            Self::FeedAfterUnavailable => "feed_after_unavailable",
            Self::StatementInvalid => "statement_invalid",
            Self::KeyConflict => "key_conflict",
            Self::KeyReused => "key_reused",
            Self::StatusSequenceConflict => "status_sequence_conflict",
            Self::ActionNotAllowed => "action_not_allowed",
        }
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Refusal {
    pub code: RefusalCode,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub field: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<RollbackReason>,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RollbackReason {
    LowerSequence,
    SameSequenceOtherBytes,
    ChangedAtRegressed,
    KeyEpochRegressed,
}
impl Refusal {
    pub fn new(code: RefusalCode) -> Self {
        Self {
            code,
            field: None,
            reason: None,
        }
    }
    pub fn field(code: RefusalCode, field: impl Into<String>) -> Self {
        Self {
            field: Some(field.into()),
            ..Self::new(code)
        }
    }
    pub fn rollback(reason: RollbackReason) -> Self {
        Self {
            reason: Some(reason),
            ..Self::new(RefusalCode::FeedRollback)
        }
    }
}
impl std::fmt::Display for Refusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code.as_str())
    }
}
impl std::error::Error for Refusal {}

pub const MAX_SAFE_INTEGER: u64 = 9_007_199_254_740_991;
pub fn sha256_hex(bytes: impl AsRef<[u8]>) -> String {
    hex(&Sha256::digest(bytes.as_ref()))
}
pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
pub fn decode_hex<const N: usize>(s: &str) -> Option<[u8; N]> {
    if !is_hex(s, N * 2) {
        return None;
    }
    let mut out = [0; N];
    for (i, pair) in s.as_bytes().as_chunks::<2>().0.iter().enumerate() {
        let digit = |b: u8| if b <= b'9' { b - b'0' } else { b - b'a' + 10 };
        out[i] = digit(pair[0]) * 16 + digit(pair[1]);
    }
    Some(out)
}
pub fn is_hex(s: &str, len: usize) -> bool {
    s.len() == len
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
pub fn key_fingerprint(public_key: &str) -> Option<String> {
    Some(sha256_hex(decode_hex::<32>(public_key)?))
}
pub fn signing_input(domain: &str, canonical: &str) -> Vec<u8> {
    let mut bytes = domain.as_bytes().to_vec();
    bytes.push(0);
    bytes.extend_from_slice(canonical.as_bytes());
    bytes
}
pub fn verify_signature(public_key: &str, domain: &str, canonical: &str, signature: &str) -> bool {
    let Some(key) = decode_hex::<32>(public_key).and_then(|b| VerifyingKey::from_bytes(&b).ok())
    else {
        return false;
    };
    let Some(sig) = decode_hex::<64>(signature) else {
        return false;
    };
    key.verify_strict(
        &signing_input(domain, canonical),
        &Signature::from_bytes(&sig),
    )
    .is_ok()
}
pub(crate) fn positive(v: &serde_json::Value) -> Option<u64> {
    let n = v.as_f64()?;
    (n >= 1.0 && n <= MAX_SAFE_INTEGER as f64 && n.fract() == 0.0).then_some(n as u64)
}
pub(crate) fn time(s: &str) -> bool {
    let b = s.as_bytes();
    b.len() == 20
        && b.iter().enumerate().all(|(i, c)| match i {
            4 | 7 => *c == b'-',
            10 => *c == b'T',
            13 | 16 => *c == b':',
            19 => *c == b'Z',
            _ => c.is_ascii_digit(),
        })
}
pub(crate) fn shape(
    text: &str,
    domain: &str,
    fields: &[&str],
    optional: &[&str],
    code: RefusalCode,
) -> Result<serde_json::Value, Refusal> {
    let (v, source_keys) = jcs::parse_ordered(text).map_err(|_| Refusal::field(code, "json"))?;
    let o = v.as_object().ok_or_else(|| Refusal::field(code, "json"))?;
    for k in &source_keys {
        if !fields.contains(&k.as_str()) && !optional.contains(&k.as_str()) {
            return Err(Refusal::field(code, k));
        }
    }
    for k in fields {
        if !o.contains_key(*k) {
            return Err(Refusal::field(code, *k));
        }
    }
    if v["schema"] != domain {
        return Err(Refusal::field(code, "schema"));
    }
    for (key, value) in o {
        if !signed_numbers(value) {
            return Err(Refusal::field(code, key));
        }
    }
    Ok(v)
}
fn signed_numbers(v: &serde_json::Value) -> bool {
    match v {
        serde_json::Value::Number(n) => n
            .as_f64()
            .is_some_and(|n| n.fract() == 0.0 && n.abs() <= MAX_SAFE_INTEGER as f64),
        serde_json::Value::Array(a) => a.iter().all(signed_numbers),
        serde_json::Value::Object(o) => o.values().all(signed_numbers),
        _ => true,
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Kind {
    Persona,
    Skill,
    Flow,
    MemoryProfile,
}

//! Publisher statements and the registry's cross-signed status-key chain.
use crate::status::State;
use crate::{is_hex, jcs, names, positive, shape, time, verify_signature, Refusal, RefusalCode};
use serde::{Deserialize, Serialize};
use serde_json::Value;
const INVALID: RefusalCode = RefusalCode::StatementInvalid;
fn invalid(f: &str) -> Refusal {
    Refusal::field(INVALID, f)
}
fn string<'a>(s: &'a Value, f: &str) -> Result<&'a str, Refusal> {
    s[f].as_str().ok_or_else(|| invalid(f))
}
fn key<'a>(s: &'a Value, f: &str) -> Result<&'a str, Refusal> {
    let k = string(s, f)?;
    if !is_hex(k, 64) {
        return Err(invalid(f));
    }
    Ok(k)
}
fn timestamp<'a>(s: &'a Value, f: &str) -> Result<&'a str, Refusal> {
    let t = string(s, f)?;
    if !time(t) {
        return Err(invalid(f));
    }
    Ok(t)
}
fn canonical(s: &Value) -> Result<String, Refusal> {
    jcs::to_string(s).map_err(|_| invalid("json"))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Revocation {
    pub reason: String,
    pub compromise_from: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ScopeKey {
    pub public_key: String,
    pub valid_from: String,
    pub valid_until: Option<String>,
    pub revocation: Option<Revocation>,
}
impl ScopeKey {
    pub fn covers(&self, at: &str) -> bool {
        window_covers(&self.valid_from, self.valid_until.as_deref(), at)
    }
}
pub fn window_covers(from: &str, until: Option<&str>, at: &str) -> bool {
    at >= from && until.is_none_or(|u| at < u)
}
fn current(keys: &[ScopeKey], pk: &str) -> Result<(), Refusal> {
    let k = keys
        .iter()
        .find(|k| k.public_key == pk)
        .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?;
    if k.revocation.is_some() {
        return Err(Refusal::new(RefusalCode::KeyRevoked));
    }
    if k.valid_until.is_some() {
        return Err(Refusal::new(RefusalCode::KeyNotValid));
    }
    Ok(())
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryKey {
    pub key_id: String,
    pub public_key: String,
    pub statement: Option<String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryChain {
    pub registry: String,
    pub keys: Vec<RegistryKey>,
}
impl RegistryChain {
    pub fn epoch(&self, id: &str) -> Option<usize> {
        self.keys.iter().position(|k| k.key_id == id)
    }
    pub fn active_epoch(&self, at: &str) -> Option<usize> {
        self.keys
            .iter()
            .enumerate()
            .rev()
            .find(|(_, k)| {
                k.statement.as_ref().is_none_or(|s| {
                    jcs::parse(s)
                        .ok()
                        .and_then(|v| v["valid_from"].as_str().map(|x| x <= at))
                        .unwrap_or(false)
                })
            })
            .map(|(i, _)| i)
    }
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyRotation {
    pub schema: String,
    pub scope: String,
    pub old_key: String,
    pub new_key: String,
}
pub fn verify_key_rotation(
    text: &str,
    old_sig: &str,
    new_sig: &str,
    scope: &str,
    keys: &[ScopeKey],
    ever: &[String],
) -> Result<KeyRotation, Refusal> {
    let s = shape(
        text,
        "ck-key-rotation/v1",
        &["schema", "scope", "old_key", "new_key"],
        &[],
        INVALID,
    )?;
    let scoped = string(&s, "scope")?;
    if !names::part(scoped) || scoped.len() > names::SCOPE_MAX {
        return Err(invalid("scope"));
    }
    let old = key(&s, "old_key")?;
    let new = key(&s, "new_key")?;
    if new == old {
        return Err(invalid("new_key"));
    }
    if scoped != scope {
        return Err(Refusal::field(RefusalCode::RecordMismatch, "scope"));
    }
    let bytes = canonical(&s)?;
    for (pk, sig, f) in [
        (old, old_sig, "old_signature"),
        (new, new_sig, "new_signature"),
    ] {
        if !verify_signature(pk, "ck-key-rotation/v1", &bytes, sig) {
            return Err(Refusal::field(RefusalCode::SignatureInvalid, f));
        }
    }
    current(keys, old)?;
    if ever.iter().any(|k| k == new) {
        return Err(Refusal::new(RefusalCode::KeyReused));
    }
    serde_json::from_value(s).map_err(|_| invalid("json"))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct KeyCert {
    pub schema: String,
    pub scope: String,
    pub public_key: String,
    pub valid_from: String,
    pub valid_until: Option<String>,
}
pub fn verify_key_cert(text: &str, sig: &str, root: &str) -> Result<KeyCert, Refusal> {
    let s = shape(
        text,
        "ck-key-cert/v1",
        &["schema", "scope", "public_key", "valid_from", "valid_until"],
        &[],
        INVALID,
    )?;
    if s["scope"] != "cortexkit" {
        return Err(invalid("scope"));
    }
    key(&s, "public_key")?;
    let from = timestamp(&s, "valid_from")?;
    if !s["valid_until"].is_null() && timestamp(&s, "valid_until")? <= from {
        return Err(invalid("valid_until"));
    }
    if !verify_signature(root, "ck-key-cert/v1", &canonical(&s)?, sig) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid));
    }
    serde_json::from_value(s).map_err(|_| invalid("json"))
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RegistryKeyStatement {
    pub schema: String,
    pub registry: String,
    pub key_id: String,
    pub public_key: String,
    pub valid_from: String,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum KeyApply {
    Accepted,
    Duplicate,
}
/// Validate against the newest held key; mutate the chain only on acceptance.
pub fn apply_registry_key(
    pin: &mut RegistryChain,
    text: &str,
    sig: &str,
) -> Result<KeyApply, Refusal> {
    let s = shape(
        text,
        "ck-registry-key/v1",
        &["schema", "registry", "key_id", "public_key", "valid_from"],
        &[],
        INVALID,
    )?;
    for f in ["registry", "key_id"] {
        if string(&s, f)?.is_empty() {
            return Err(invalid(f));
        }
    }
    let pk = key(&s, "public_key")?;
    timestamp(&s, "valid_from")?;
    if s["registry"] != pin.registry {
        return Err(Refusal::field(RefusalCode::RecordMismatch, "registry"));
    }
    let bytes = canonical(&s)?;
    if let Some(k) = pin.keys.iter().find(|k| s["key_id"] == k.key_id) {
        // A bare pin authenticates only the key identity, not metadata received
        // with it. Leave it bare: its predecessor may not be held locally.
        return if k.statement.as_deref() == Some(&bytes)
            || (k.statement.is_none() && k.public_key == pk)
        {
            Ok(KeyApply::Duplicate)
        } else {
            Err(Refusal::field(RefusalCode::KeyConflict, "key_id"))
        };
    }
    if pin.keys.iter().any(|k| k.public_key == pk) {
        return Err(Refusal::field(RefusalCode::KeyConflict, "public_key"));
    }
    let k = pin
        .keys
        .last()
        .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?;
    if !verify_signature(&k.public_key, "ck-registry-key/v1", &bytes, sig) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid));
    }
    let statement: RegistryKeyStatement = serde_json::from_value(s).map_err(|_| invalid("json"))?;

    pin.keys.push(RegistryKey {
        key_id: statement.key_id,
        public_key: statement.public_key,
        statement: Some(bytes),
    });
    Ok(KeyApply::Accepted)
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Action {
    Withdraw,
    Unwithdraw,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StatusRequest {
    pub schema: String,
    pub action: Action,
    pub name: String,
    pub version: String,
    pub archive_sha256: String,
    pub reason: Option<String>,
    pub after_sequence: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RequestPath {
    pub action: Action,
    pub name: String,
    pub version: String,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VersionStatus {
    pub archive_sha256: String,
    pub sequence: u64,
    pub state: State,
}
#[derive(Clone, Debug, Serialize)]
pub struct NextStatus {
    pub sequence: u64,
    pub state: State,
}
pub fn apply_status_request(
    path: &RequestPath,
    text: &str,
    pk: &str,
    sig: &str,
    held: &VersionStatus,
    keys: &[ScopeKey],
) -> Result<NextStatus, Refusal> {
    let s = shape(
        text,
        "ck-status-request/v1",
        &[
            "schema",
            "action",
            "name",
            "version",
            "archive_sha256",
            "reason",
            "after_sequence",
        ],
        &[],
        INVALID,
    )?;
    let action = string(&s, "action")?;
    if action != "withdraw" && action != "unwithdraw" {
        return Err(invalid("action"));
    }
    if !names::canonical(string(&s, "name")?) {
        return Err(invalid("name"));
    }
    if string(&s, "version")?
        .parse::<crate::semver::Version>()
        .is_err()
    {
        return Err(invalid("version"));
    }
    key(&s, "archive_sha256")?;
    if !s["reason"].as_str().is_some_and(|s| !s.is_empty())
        && !(s["reason"].is_null() && action == "unwithdraw")
    {
        return Err(invalid("reason"));
    }
    positive(&s["after_sequence"]).ok_or_else(|| invalid("after_sequence"))?;
    for (f, v) in [
        (
            "action",
            serde_json::to_value(path.action).map_err(|_| invalid("action"))?,
        ),
        ("name", Value::String(path.name.clone())),
        ("version", Value::String(path.version.clone())),
    ] {
        if s[f] != v {
            return Err(Refusal::field(RefusalCode::RecordMismatch, f));
        }
    }
    if !verify_signature(pk, "ck-status-request/v1", &canonical(&s)?, sig) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid));
    }
    current(keys, pk)?;
    if s["archive_sha256"] != held.archive_sha256 {
        return Err(Refusal::field(
            RefusalCode::RecordMismatch,
            "archive_sha256",
        ));
    }
    if positive(&s["after_sequence"]) != Some(held.sequence) {
        return Err(Refusal::new(RefusalCode::StatusSequenceConflict));
    }
    if (action == "withdraw" && held.state != State::Available)
        || (action == "unwithdraw" && held.state != State::Withdrawn)
    {
        return Err(Refusal::new(RefusalCode::ActionNotAllowed));
    }
    let sequence = held
        .sequence
        .checked_add(1)
        .filter(|n| *n <= crate::MAX_SAFE_INTEGER)
        .ok_or_else(|| invalid("after_sequence"))?;
    Ok(NextStatus {
        sequence,
        state: if action == "withdraw" {
            State::Withdrawn
        } else {
            State::Available
        },
    })
}

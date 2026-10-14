//! CK catalog v1 §4.3 verification: record shape, signature, publisher trust,
//! byte digests/length, strict archive, manifest/flow identity, then file coverage.
use crate::statements::ScopeKey;
use crate::{
    archive, is_hex, jcs, key_fingerprint, names, positive, sha256_hex, shape, verify_signature,
    Kind, Refusal, RefusalCode,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub schema: String,
    pub name: String,
    pub version: String,
    pub manifest_sha256: String,
    pub archive_sha256: String,
    pub archive_bytes: u64,
    pub public_key: String,
    pub log_head: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub flow_version: Option<u64>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Requires {
    pub capabilities: Vec<String>,
    pub packages: BTreeMap<String, String>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct File {
    pub path: String,
    pub sha256: String,
    pub bytes: u64,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    pub schema: String,
    pub name: String,
    pub version: String,
    pub kind: Kind,
    pub summary: String,
    pub license: String,
    pub source: String,
    pub requires: Requires,
    pub files: Vec<File>,
    pub body: Value,
}
/// Trust is supplied for the already-selected scope. Publication time must be
/// registry-attested; a compromised key cannot supply its own earlier time.
pub struct Trust<'a> {
    pub keys: &'a [ScopeKey],
    pub published_at: Option<&'a str>,
}
#[derive(Clone, Debug)]
pub struct VerifiedPackage {
    pub record: Record,
    pub manifest: Manifest,
    pub canonical_record: String,
}
fn trusted_record(
    text: &str,
    sig: &str,
    trust: &Trust<'_>,
) -> Result<(Value, String, u64, String), Refusal> {
    let invalid = |f| Refusal::field(RefusalCode::RecordInvalid, f);
    let r = shape(
        text,
        "ck-record/v1",
        &[
            "schema",
            "name",
            "version",
            "manifest_sha256",
            "archive_sha256",
            "archive_bytes",
            "public_key",
            "log_head",
        ],
        &["flow_version"],
        RefusalCode::RecordInvalid,
    )?;
    let name = r["name"]
        .as_str()
        .filter(|n| names::canonical(n))
        .ok_or_else(|| invalid("name"))?;
    let id = names::map_name(name).map_err(|_| invalid("name"))?.id;
    if !r["version"]
        .as_str()
        .is_some_and(|v| v.parse::<crate::semver::Version>().is_ok())
    {
        return Err(invalid("version"));
    }
    for f in ["manifest_sha256", "archive_sha256", "public_key"] {
        if !r[f].as_str().is_some_and(|s| is_hex(s, 64)) {
            return Err(invalid(f));
        }
    }
    let len = positive(&r["archive_bytes"])
        .filter(|n| *n <= archive::ARCHIVE_MAX as u64)
        .ok_or_else(|| invalid("archive_bytes"))?;
    if r.get("flow_version").is_some_and(|v| positive(v).is_none()) {
        return Err(invalid("flow_version"));
    }
    let canonical = jcs::to_string(&r).map_err(|_| invalid("json"))?;
    let pk = r["public_key"]
        .as_str()
        .ok_or_else(|| invalid("public_key"))?;
    if !verify_signature(pk, "ck-record/v1", &canonical, sig) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid));
    }
    let key = trust
        .keys
        .iter()
        .find(|k| k.public_key == pk)
        .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?;
    if key
        .revocation
        .as_ref()
        .is_some_and(|rev| crate::status::revoked_at(trust.published_at, &rev.compromise_from))
    {
        return Err(Refusal::new(RefusalCode::KeyRevoked));
    }
    if trust.published_at.is_some_and(|at| !key.covers(at))
        || (trust.published_at.is_none() && key.valid_until.is_some())
    {
        return Err(Refusal::new(RefusalCode::KeyNotValid));
    }
    Ok((r, canonical, len, id))
}
/// Verify shape, signature and scope trust before the client fetches package bytes.
/// The caller must still run full record verification before publishing any blob.
pub fn verify_record_trust(text: &str, sig: &str, trust: &Trust<'_>) -> Result<Record, Refusal> {
    let (mut r, _, len, _) = trusted_record(text, sig, trust)?;
    r["archive_bytes"] = len.into();
    serde_json::from_value(r).map_err(|_| Refusal::field(RefusalCode::RecordInvalid, "json"))
}
pub fn verify_record(
    text: &str,
    sig: &str,
    manifest_bytes: &[u8],
    archive_bytes: &[u8],
    trust: &Trust<'_>,
) -> Result<VerifiedPackage, Refusal> {
    let (mut r, canonical, len, id) = trusted_record(text, sig, trust)?;
    let invalid = |f| Refusal::field(RefusalCode::RecordInvalid, f);
    let mismatch = |f| Refusal::field(RefusalCode::RecordMismatch, f);
    if r["manifest_sha256"] != sha256_hex(manifest_bytes) {
        return Err(mismatch("manifest_sha256"));
    }
    if r["archive_sha256"] != sha256_hex(archive_bytes) {
        return Err(mismatch("archive_sha256"));
    }
    if len != archive_bytes.len() as u64 {
        return Err(mismatch("archive_bytes"));
    }
    let members = archive::read(archive_bytes).map_err(|e| {
        if let Some(rule) = e.rule {
            Refusal::field(
                e.code,
                serde_json::to_value(rule)
                    .ok()
                    .and_then(|v| v.as_str().map(String::from))
                    .unwrap_or_default(),
            )
        } else {
            Refusal::new(e.code)
        }
    })?;
    if members.get("ck.json").copied() != Some(manifest_bytes) {
        return Err(mismatch("ck.json"));
    }
    let m = manifest_value(manifest_bytes)?;
    if m["name"] != r["name"] {
        return Err(mismatch("name"));
    }
    if m["version"] != r["version"] {
        return Err(mismatch("version"));
    }
    let flow = m["kind"] == "flow";
    if flow != r.get("flow_version").is_some() {
        return Err(mismatch("flow_version"));
    }
    if flow {
        let path = m["body"]["manifest"].as_str().unwrap_or("flow.json");
        let f = members
            .get(path)
            .and_then(|b| std::str::from_utf8(b).ok())
            .and_then(|s| jcs::parse(s).ok());
        if f.as_ref().is_none_or(|f| f["id"] != id) {
            return Err(mismatch("flow.json id"));
        }
        if f.as_ref().is_none_or(|f| f["version"] != r["flow_version"]) {
            return Err(mismatch("flow_version"));
        }
    }
    let manifest = validate_manifest(m)?;
    let mut listed = BTreeSet::new();
    for f in &manifest.files {
        let bytes = members.get(&f.path);
        if bytes.is_none_or(|b| sha256_hex(b) != f.sha256 || b.len() as u64 != f.bytes) {
            return Err(Refusal::field(RefusalCode::FilesMismatch, &f.path));
        }
        listed.insert(f.path.as_str());
    }
    for path in members.keys() {
        if path != "ck.json" && !listed.contains(path.as_str()) {
            return Err(Refusal::field(RefusalCode::FilesMismatch, path));
        }
    }
    r["archive_bytes"] = len.into();
    if let Some(n) = r.get("flow_version").and_then(positive) {
        r["flow_version"] = n.into();
    }
    let record = serde_json::from_value(r).map_err(|_| invalid("json"))?;
    Ok(VerifiedPackage {
        record,
        manifest,
        canonical_record: canonical,
    })
}
fn manifest_value(bytes: &[u8]) -> Result<Value, Refusal> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| jcs::parse(s).ok())
        .ok_or_else(|| Refusal::field(RefusalCode::ManifestInvalid, "json"))
}
/// Parse authenticated manifest bytes using the same rules as record verification.
/// Authentication of the exact bytes remains the caller's responsibility.
pub fn parse_manifest(bytes: &[u8]) -> Result<Manifest, Refusal> {
    validate_manifest(manifest_value(bytes)?)
}
fn validate_manifest(v: Value) -> Result<Manifest, Refusal> {
    let err = |f: &str| Refusal::field(RefusalCode::ManifestInvalid, f);
    let m: Manifest = serde_json::from_value(v).map_err(|_| err("json"))?;
    if m.summary.is_empty() || m.license.is_empty() {
        return Err(err("summary/license"));
    }
    if !m.source.split_once(':').is_some_and(|(scheme, rest)| {
        !rest.is_empty()
            && scheme.starts_with(|c: char| c.is_ascii_alphabetic())
            && scheme
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"+.-".contains(&b))
    }) || m
        .source
        .chars()
        .any(|c| c.is_whitespace() || c.is_control())
    {
        return Err(err("source"));
    }
    if m.requires
        .capabilities
        .iter()
        .collect::<BTreeSet<_>>()
        .len()
        != m.requires.capabilities.len()
    {
        return Err(err("requires.capabilities"));
    }
    if m.schema != "ck-package/v1" {
        return Err(err("schema"));
    }
    if !names::canonical(&m.name) {
        return Err(err("name"));
    }
    if m.version.parse::<crate::semver::Version>().is_err() {
        return Err(err("version"));
    }
    if !m
        .requires
        .capabilities
        .iter()
        .all(|c| crate::resolver::capability_token(c))
    {
        return Err(err("requires.capabilities"));
    }
    for (p, r) in &m.requires.packages {
        if !names::canonical(p) || r.parse::<crate::semver::Range>().is_err() {
            return Err(err("requires.packages"));
        }
    }
    let mut paths = BTreeSet::new();
    let mut lower = BTreeSet::new();
    for f in &m.files {
        if archive::check_path(&f.path).is_err()
            || f.path == "ck.json"
            || !paths.insert(f.path.as_str())
            || !lower.insert(f.path.to_ascii_lowercase())
            || !is_hex(&f.sha256, 64)
            || f.bytes > archive::ARCHIVE_MAX as u64
        {
            return Err(err("files"));
        }
    }
    let body = m.body.as_object().ok_or_else(|| err("body"))?;
    let check_file = |v: &Value| -> Result<(), Refusal> {
        let p = v.as_str().ok_or_else(|| err("body"))?;
        if !paths.contains(p) {
            return Err(err("body"));
        }
        Ok(())
    };
    let allowed: &[&str] = match m.kind {
        Kind::Skill => &["entry", "audience", "scripts"],
        Kind::Flow => &["manifest", "script"],
        Kind::Persona => &[
            "prompt",
            "extends",
            "implements",
            "delegates_to",
            "capabilities",
            "routing",
            "activation",
            "skills",
            "flows",
            "memory_profile",
            "memory_scope",
        ],
        Kind::MemoryProfile => return Ok(m),
    };
    if body.keys().any(|k| !allowed.contains(&k.as_str())) {
        return Err(err("body"));
    }
    match m.kind {
        Kind::Skill => {
            check_file(&m.body["entry"])?;
            if m.body["audience"] != "user" && m.body["audience"] != "developer" {
                return Err(err("body.audience"));
            }
            for script in m.body["scripts"]
                .as_array()
                .ok_or_else(|| err("body.scripts"))?
            {
                check_file(&script["path"])?;
                if !script["runs_with"].as_str().is_some_and(|s| !s.is_empty())
                    || script.as_object().is_none_or(|o| o.len() != 2)
                {
                    return Err(err("body.scripts"));
                }
            }
        }
        Kind::Flow => {
            check_file(&m.body["manifest"])?;
            check_file(&m.body["script"])?;
        }
        Kind::Persona => {
            check_file(&m.body["prompt"])?;
            let unique_strings = |v: &Value, check: fn(&str) -> bool| {
                v.as_array().is_some_and(|a| {
                    a.iter().all(|v| v.as_str().is_some_and(check))
                        && a.iter()
                            .filter_map(Value::as_str)
                            .collect::<BTreeSet<_>>()
                            .len()
                            == a.len()
                })
            };
            if !unique_strings(&m.body["implements"], contract_id) {
                return Err(err("body.implements"));
            }
            if !unique_strings(&m.body["skills"], names::canonical)
                || !unique_strings(&m.body["flows"], names::canonical)
            {
                return Err(err("body.skills/flows"));
            }
            let caps = m.body["capabilities"]
                .as_object()
                .ok_or_else(|| err("body.capabilities"))?;
            if caps.len() != 2
                || !unique_strings(
                    &m.body["capabilities"]["required"],
                    crate::resolver::capability_token,
                )
                || !unique_strings(
                    &m.body["capabilities"]["optional"],
                    crate::resolver::capability_token,
                )
            {
                return Err(err("body.capabilities"));
            }
            if !["project", "workspace", "global"]
                .iter()
                .any(|s| m.body["memory_scope"] == *s)
            {
                return Err(err("body.memory_scope"));
            }
            if m.body.get("routing").is_some_and(|v| !v.is_object())
                || m.body.get("activation").is_some_and(|v| {
                    v.as_array()
                        .is_none_or(|a| a.iter().any(|v| !v.is_object()))
                })
            {
                return Err(err("body.routing/activation"));
            }
            let check_ref = |v: &Value, f: &str| -> Result<(), Refusal> {
                let p = v
                    .as_str()
                    .filter(|p| names::canonical(p))
                    .ok_or_else(|| err(f))?;
                if !m.requires.packages.contains_key(p) {
                    return Err(Refusal::field(RefusalCode::BodyRefMissing, f));
                }
                Ok(())
            };
            for f in ["extends", "memory_profile"] {
                if let Some(v) = m.body.get(f) {
                    check_ref(v, f)?;
                }
            }
            for f in ["skills", "flows"] {
                for v in m.body[f].as_array().ok_or_else(|| err(f))? {
                    check_ref(v, f)?;
                }
            }
            let delegates = m.body["delegates_to"]
                .as_object()
                .ok_or_else(|| err("body.delegates_to"))?;
            for (id, v) in delegates {
                if !contract_id(id) {
                    return Err(err("body.delegates_to"));
                }
                if !v.is_null() {
                    check_ref(v, "delegates_to")?;
                }
            }
        }
        Kind::MemoryProfile => {}
    }
    Ok(m)
}
pub fn record_fingerprint(record: &Record) -> Option<String> {
    key_fingerprint(&record.public_key)
}

fn contract_id(s: &str) -> bool {
    let Some((owner, rest)) = s.split_once('/') else {
        return false;
    };
    let Some((name, major)) = rest.split_once('@') else {
        return false;
    };
    !owner.is_empty()
        && owner
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
        && !name.is_empty()
        && name
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b".-".contains(&b))
        && !major.is_empty()
        && !major.starts_with('0')
        && major.bytes().all(|b| b.is_ascii_digit())
}

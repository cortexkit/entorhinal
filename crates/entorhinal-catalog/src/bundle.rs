//! Offline starter containers reuse online ustar, record and certificate checks.
//! No packages are returned until the entire container passes verification.
use crate::{archive, jcs, names, record, statements, status, Refusal, RefusalCode};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

pub const DOMAIN: &str = "ck-starter-bundle/v1";

#[derive(Debug, Serialize)]
#[serde(untagged)]
pub enum BundleError {
    Document(Refusal),
    Archive(archive::ArchiveError),
    Member {
        code: RefusalCode,
        field: &'static str,
        member: String,
    },
}
impl From<Refusal> for BundleError {
    fn from(value: Refusal) -> Self {
        Self::Document(value)
    }
}
impl From<archive::ArchiveError> for BundleError {
    fn from(value: archive::ArchiveError) -> Self {
        Self::Archive(value)
    }
}
impl BundleError {
    pub fn code(&self) -> RefusalCode {
        match self {
            Self::Document(e) => e.code,
            Self::Archive(e) => e.code,
            Self::Member { code, .. } => *code,
        }
    }
}
impl std::fmt::Display for BundleError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code().as_str())
    }
}
impl std::error::Error for BundleError {}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Document {
    pub schema: String,
    pub created: String,
    pub records: Vec<Entry>,
}
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Entry {
    pub name: String,
    pub version: String,
    pub archive_sha256: String,
    pub record: String,
    pub record_member_sha256: String,
    pub archive: String,
}
#[derive(Clone, Debug)]
pub struct Package<'a> {
    pub package: record::VerifiedPackage,
    pub record_signature: String,
    pub status: status::HeldStatus,
    pub status_signature: String,
    pub archive: &'a [u8],
    pub manifest: &'a [u8],
    pub proof: Proof,
}
#[derive(Clone, Debug)]
pub struct VerifiedBundle<'a> {
    pub document: Document,
    pub packages: Vec<Package<'a>>,
}
fn invalid(field: &str) -> Refusal {
    Refusal::field(RefusalCode::RecordInvalid, field)
}
fn mismatch(field: &str) -> Refusal {
    Refusal::field(RefusalCode::RecordMismatch, field)
}
fn json(bytes: &[u8]) -> Result<serde_json::Value, Refusal> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| jcs::parse_signed(s).ok())
        .ok_or_else(|| invalid("json"))
}
fn canonical(value: &serde_json::Value) -> Result<String, Refusal> {
    jcs::to_string(value).map_err(|_| invalid("json"))
}

/// Metadata sufficient to re-authenticate a package's initial status after restart.
/// Component archives remain in the content store, never in this proof.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Proof {
    pub document: String,
    pub certificate: String,
    pub signature: String,
    pub member: String,
    pub member_bytes: String,
}
impl Proof {
    pub fn verify(&self, root: &str) -> Result<status::HeldStatus, BundleError> {
        let (document, key) = authenticate(
            self.document.as_bytes(),
            self.certificate.as_bytes(),
            &self.signature,
            root,
        )?;
        let entry = document
            .records
            .iter()
            .find(|e| e.record == self.member)
            .ok_or_else(|| invalid("record"))?;
        Ok(metadata(entry, self.member_bytes.as_bytes(), &key)?.status)
    }
}
fn authenticate(
    document: &[u8],
    certificate: &[u8],
    signature: &str,
    root: &str,
) -> Result<(Document, statements::ScopeKey), BundleError> {
    let value = json(document)?;
    let document: Document =
        serde_json::from_value(value.clone()).map_err(|_| invalid("bundle.json"))?;
    if document.schema != DOMAIN || !crate::time(&document.created) {
        return Err(invalid("bundle.json").into());
    }
    let cert = json(certificate)?;
    let key = statements::verify_key_cert(
        &canonical(&cert["statement"])?,
        cert["signature"].as_str().unwrap_or(""),
        root,
    )?;
    if !statements::window_covers(
        &key.valid_from,
        key.valid_until.as_deref(),
        &document.created,
    ) {
        return Err(Refusal::new(RefusalCode::KeyNotValid).into());
    }
    if !crate::verify_signature(&key.public_key, DOMAIN, &canonical(&value)?, signature) {
        return Err(Refusal::new(RefusalCode::SignatureInvalid).into());
    }
    let mut identities = BTreeSet::new();
    for entry in &document.records {
        let mapped = names::map_name(&entry.name).map_err(|_| invalid("name"))?;
        if mapped.canonical != entry.name
            || mapped.scope != "cortexkit"
            || entry.version.parse::<crate::semver::Version>().is_err()
            || !crate::is_hex(&entry.archive_sha256, 64)
            || !crate::is_hex(&entry.record_member_sha256, 64)
            || !identities.insert((&entry.name, &entry.version))
        {
            return Err(invalid("records").into());
        }
        if entry.record
            != format!(
                "records/{}/{}/{}.json",
                mapped.scope, mapped.name, entry.version
            )
        {
            return Err(invalid("record").into());
        }
        archive::check_path(&entry.record).map_err(|_| invalid("record"))?;
        if entry.archive != format!("archives/{}.tar", entry.archive_sha256) {
            return Err(mismatch("archive_sha256").into());
        }
    }
    Ok((
        document,
        statements::ScopeKey {
            public_key: key.public_key,
            valid_from: key.valid_from,
            valid_until: key.valid_until,
            revocation: None,
        },
    ))
}
struct Metadata {
    record: String,
    record_signature: String,
    status: status::HeldStatus,
    status_signature: String,
}
fn check_member(entry: &Entry, bytes: &[u8]) -> Result<(), BundleError> {
    if crate::sha256_hex(bytes) != entry.record_member_sha256 {
        return Err(BundleError::Member {
            code: RefusalCode::RecordMismatch,
            field: "record_member_sha256",
            member: entry.record.clone(),
        });
    }
    Ok(())
}
fn metadata(
    entry: &Entry,
    bytes: &[u8],
    key: &statements::ScopeKey,
) -> Result<Metadata, BundleError> {
    check_member(entry, bytes)?;
    let envelopes = json(bytes)?;
    let record_signature = envelopes["record"]["signature"]
        .as_str()
        .filter(|_| envelopes["record"]["record"].is_object())
        .ok_or_else(|| invalid("record"))?;
    let status_signature = envelopes["status"]["signature"]
        .as_str()
        .filter(|_| envelopes["status"]["record"].is_object())
        .ok_or_else(|| Refusal::field(RefusalCode::StatusInvalid, "status"))?;
    let held = status::HeldStatus::from_bundle(&canonical(&envelopes["status"]["record"])?)?;
    let status = held.record();
    let text = canonical(&envelopes["record"]["record"])?;
    let record = record::verify_record_trust(
        &text,
        record_signature,
        &record::Trust {
            keys: std::slice::from_ref(key),
            published_at: Some(&status.published_at),
        },
    )?;
    if record.name != entry.name || status.name != entry.name {
        return Err(mismatch("name").into());
    }
    if record.version != entry.version || status.version != entry.version {
        return Err(mismatch("version").into());
    }
    if record.archive_sha256 != entry.archive_sha256
        || status.archive_sha256 != entry.archive_sha256
    {
        return Err(mismatch("archive_sha256").into());
    }
    if Some(&status.publisher_key) != crate::key_fingerprint(&record.public_key).as_ref() {
        return Err(mismatch("publisher_key").into());
    }
    Ok(Metadata {
        record: text,
        record_signature: record_signature.into(),
        status: held,
        status_signature: status_signature.into(),
    })
}

/// Verify a whole starter bundle against an explicitly supplied offline root.
/// Production callers pass the trusted root embedded in the program, never a key
/// supplied by the bundle itself.
pub fn verify<'a>(bytes: &'a [u8], root: &str) -> Result<VerifiedBundle<'a>, BundleError> {
    let members = archive::read_members(bytes)?;
    let member = |path: &str| {
        members
            .get(path)
            .map(|m| m.bytes)
            .ok_or_else(|| invalid(path))
    };
    let document_bytes = member("bundle.json")?;
    let certificate = members
        .get("bundle.cert.json")
        .ok_or_else(|| Refusal::new(RefusalCode::KeyUnknown))?
        .bytes;
    let signature = members
        .get("bundle.sig")
        .and_then(|m| std::str::from_utf8(m.bytes).ok())
        .unwrap_or("");
    let (document, key) = authenticate(document_bytes, certificate, signature, root)?;
    let mut expected = BTreeSet::from(["bundle.json", "bundle.cert.json", "bundle.sig"]);
    let mut packages = Vec::new();
    for entry in &document.records {
        expected.insert(&entry.record);
        expected.insert(&entry.archive);
        let record_bytes = member(&entry.record)?;
        check_member(entry, record_bytes)?;
        let archive = members
            .get(&entry.archive)
            .map(|m| m.bytes)
            .ok_or_else(|| mismatch("archive_sha256"))?;
        if crate::sha256_hex(archive) != entry.archive_sha256 {
            return Err(mismatch("archive_sha256").into());
        }
        let metadata = metadata(entry, record_bytes, &key)?;
        let contents = archive::read(archive)?;
        let manifest = contents
            .get("ck.json")
            .copied()
            .ok_or_else(|| invalid("ck.json"))?;
        let package = record::verify_record(
            &metadata.record,
            &metadata.record_signature,
            manifest,
            archive,
            &record::Trust {
                keys: std::slice::from_ref(&key),
                published_at: Some(&metadata.status.record().published_at),
            },
        )?;
        let text = |bytes: &[u8]| {
            std::str::from_utf8(bytes)
                .map(str::to_owned)
                .map_err(|_| invalid("json"))
        };
        let proof = Proof {
            document: text(document_bytes)?,
            certificate: text(certificate)?,
            signature: signature.into(),
            member: entry.record.clone(),
            member_bytes: text(record_bytes)?,
        };
        packages.push(Package {
            package,
            record_signature: metadata.record_signature,
            status: metadata.status,
            status_signature: metadata.status_signature,
            archive,
            manifest,
            proof,
        });
    }
    if let Some((path, member)) = members
        .iter()
        .filter(|(path, _)| !expected.contains(path.as_str()))
        .min_by_key(|(_, member)| member.offset)
    {
        return Err(archive::ArchiveError {
            code: RefusalCode::ArchiveInvalid,
            rule: Some(archive::ArchiveRule::MemberUnlisted),
            offset: Some(member.offset),
            member: Some(path.clone()),
            typeflag: None,
        }
        .into());
    }
    Ok(VerifiedBundle { document, packages })
}

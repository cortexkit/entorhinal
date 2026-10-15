//! Unsigned package checks used before asking a publisher to authorize signing.
use crate::{archive, jcs, names, record, sha256_hex, Kind, Refusal, RefusalCode};
use serde_json::Value;
use std::collections::BTreeSet;

pub struct CheckedArchive {
    pub manifest: record::Manifest,
    pub manifest_sha256: String,
    pub archive_sha256: String,
    pub archive_bytes: u64,
    pub flow_version: Option<u64>,
}

/// Check everything that can be decided locally without a signature or registry state.
/// Registry rate limits, key grants, quotas and version reservations remain remote checks.
pub fn check(archive_bytes: &[u8], scope: &str) -> Result<CheckedArchive, Refusal> {
    if archive_bytes.len() > archive::ARCHIVE_MAX {
        return Err(Refusal::new(RefusalCode::ArchiveTooLarge));
    }
    let members = archive::read(archive_bytes).map_err(|e| Refusal::new(e.code))?;
    let manifest_bytes = members
        .get("ck.json")
        .ok_or_else(|| Refusal::field(RefusalCode::ManifestInvalid, "ck.json"))?;
    let manifest = record::parse_manifest(manifest_bytes)?;
    let mapped = names::map_name(&manifest.name).map_err(|e| Refusal::new(e.code))?;
    if format!("@{}", mapped.scope) != scope {
        return Err(Refusal::field(RefusalCode::RecordMismatch, "scope"));
    }
    let mut listed = BTreeSet::new();
    for file in &manifest.files {
        if members.get(&file.path).is_none_or(|bytes| {
            bytes.len() as u64 != file.bytes || sha256_hex(bytes) != file.sha256
        }) {
            return Err(Refusal::field(RefusalCode::FilesMismatch, &file.path));
        }
        listed.insert(file.path.as_str());
    }
    for path in members.keys() {
        if path != "ck.json" && !listed.contains(path.as_str()) {
            return Err(Refusal::field(RefusalCode::FilesMismatch, path));
        }
    }
    let flow_version = if manifest.kind == Kind::Flow {
        let flow = members
            .get(manifest.body["manifest"].as_str().unwrap_or_default())
            .and_then(|bytes| std::str::from_utf8(bytes).ok())
            .and_then(|text| jcs::parse(text).ok())
            .ok_or_else(|| Refusal::field(RefusalCode::RecordMismatch, "flow.json"))?;
        if flow["id"] != mapped.id {
            return Err(Refusal::field(RefusalCode::FlowIdMismatch, "flow.json id"));
        }
        Some(
            crate::positive(&flow["version"])
                .ok_or_else(|| Refusal::field(RefusalCode::RecordInvalid, "flow_version"))?,
        )
    } else {
        None
    };
    Ok(CheckedArchive {
        manifest,
        manifest_sha256: sha256_hex(manifest_bytes),
        archive_sha256: sha256_hex(archive_bytes),
        archive_bytes: archive_bytes.len() as u64,
        flow_version,
    })
}

impl CheckedArchive {
    pub fn record(&self, public_key: &str) -> record::Record {
        record::Record {
            schema: "ck-record/v1".into(),
            name: self.manifest.name.clone(),
            version: self.manifest.version.clone(),
            manifest_sha256: self.manifest_sha256.clone(),
            archive_sha256: self.archive_sha256.clone(),
            archive_bytes: self.archive_bytes,
            public_key: public_key.into(),
            log_head: Value::Null,
            flow_version: self.flow_version,
        }
    }
}

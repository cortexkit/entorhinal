//! Adapt local pins and verified manifests to the unchanged catalog resolver.
use crate::catalog_agent::{read_pins, CatalogPin, CatalogSpec};
use crate::{RegistryError, RegistryStore};
use entorhinal_catalog::record::VerifiedPackage;
use entorhinal_catalog::resolver::{self, RegistryVersion, Resolution};
use entorhinal_catalog::status::StatusRecord;
use entorhinal_catalog::{key_fingerprint, Kind};
use rusqlite::{params, OptionalExtension};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;

#[derive(Clone, Debug)]
pub enum CatalogSubject {
    Install(CatalogSpec),
    Upgrade(CatalogSpec),
    UpgradeAll,
}
#[derive(Clone, Debug, Serialize)]
pub struct CatalogResolution {
    pub closure: Vec<CatalogPin>,
    pub resolution: Resolution,
}
#[derive(Debug)]
pub enum CatalogResolveError {
    Store(RegistryError),
    Refusal(Value),
}
impl std::fmt::Display for CatalogResolveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for CatalogResolveError {}
fn refusal(code: &str) -> CatalogResolveError {
    CatalogResolveError::Refusal(json!({"code":code}))
}
impl RegistryStore {
    /// Candidates must come from record/archive verification. Held status is the
    /// authoritative eligibility overlay; an unheld candidate is not eligible.
    /// No network or content-store I/O occurs while this read snapshot is held.
    pub fn catalog_resolve(
        &self,
        agent: &str,
        subject: CatalogSubject,
        candidates: &[VerifiedPackage],
        caps: &[String],
    ) -> Result<CatalogResolution, CatalogResolveError> {
        let result = self
            .read(|conn| {
                let run = || -> Result<CatalogResolution, CatalogResolveError> {
                    let status: Option<String> = crate::agent::load_row(conn, agent)
                        .map_err(|e| CatalogResolveError::Store(e.into()))?
                        .map(|row| row.status);
                    if status.as_deref() != Some("live") {
                        return Err(refusal("gone"));
                    }
                    let pins =
                        read_pins(conn, agent).map_err(|e| CatalogResolveError::Store(e.into()))?;
                    crate::catalog_agent::validate_pins(&pins)
                        .map_err(|e| CatalogResolveError::Store(e.into()))?;
                    if let CatalogSubject::Upgrade(spec) = &subject {
                        if !pins.iter().any(|p| p.root && p.name == spec.package) {
                            return Err(refusal("root_not_found"));
                        }
                    }
                    let mut registry: BTreeMap<String, Vec<RegistryVersion>> = BTreeMap::new();
                    let mut by_version = BTreeMap::new();
                    let mut dependencies = BTreeMap::new();
                    for candidate in candidates {
                        let r = &candidate.record;
                        let m = &candidate.manifest;
                        if r.name != m.name || r.version != m.version {
                            return Err(refusal("record_mismatch"));
                        }
                        if let Some(pin) = pins
                            .iter()
                            .find(|p| p.name == r.name && p.version == r.version)
                        {
                            if pin.manifest_sha256 != r.manifest_sha256
                                || pin.archive_sha256 != r.archive_sha256
                                || Some(pin.publisher_fingerprint.clone())
                                    != key_fingerprint(&r.public_key)
                                || pin.kind != m.kind
                            {
                                return Err(refusal("record_mismatch"));
                            }
                            dependencies.insert(r.name.clone(), m.requires.packages.clone());
                        }
                        let text: Option<String> = conn
                            .query_row(
                                "SELECT canonical FROM catalog_status WHERE name=?1 AND version=?2",
                                params![r.name, r.version],
                                |row| row.get(0),
                            )
                            .optional()
                            .map_err(|e| CatalogResolveError::Store(e.into()))?;
                        let Some(text) = text else {
                            continue;
                        };
                        let held: StatusRecord =
                            serde_json::from_str(&text).map_err(|_| refusal("status_invalid"))?;
                        if held.name != r.name
                            || held.version != r.version
                            || held.archive_sha256 != r.archive_sha256
                            || Some(held.publisher_key.clone()) != key_fingerprint(&r.public_key)
                        {
                            return Err(refusal("record_mismatch"));
                        }
                        if by_version
                            .insert((r.name.clone(), r.version.clone()), candidate)
                            .is_some()
                        {
                            return Err(refusal("record_mismatch"));
                        }
                        // Step 1 accepts only skills, whose bodies have no cross-kind refs.
                        // Non-skill closure members are refused below, before a plan is ready.
                        registry
                            .entry(r.name.clone())
                            .or_default()
                            .push(RegistryVersion {
                                version: r.version.clone(),
                                kind: m.kind,
                                status: held.state,
                                requires: m.requires.clone(),
                                refs: vec![],
                            });
                    }
                    if pins.iter().any(|p| !dependencies.contains_key(&p.name)) {
                        return Err(refusal("pinned_manifest_missing"));
                    }
                    let previous = pins
                        .iter()
                        .map(|p| (p.name.clone(), p.version.clone()))
                        .collect();
                    let mut roots: BTreeMap<String, String> = pins
                        .iter()
                        .filter(|p| p.root)
                        .map(|p| {
                            (
                                p.name.clone(),
                                p.range.clone().expect("validated root range"),
                            )
                        })
                        .collect();
                    let subjects = match &subject {
                        CatalogSubject::Install(spec) | CatalogSubject::Upgrade(spec) => {
                            roots.insert(spec.package.clone(), spec.range.clone());
                            vec![spec.package.clone()]
                        }
                        CatalogSubject::UpgradeAll => roots.keys().cloned().collect(),
                    };
                    let resolution =
                        resolver::resolve_roots(&registry, &roots, &subjects, &previous, caps)
                            .map_err(|e| {
                                CatalogResolveError::Refusal(serde_json::to_value(e).unwrap())
                            })?;
                    let mut closure = Vec::new();
                    for (name, version) in &resolution.picks {
                        let package = by_version[&(name.clone(), version.clone())];
                        if package.manifest.kind != Kind::Skill {
                            return Err(CatalogResolveError::Refusal(
                                json!({"code":"kind_not_yet_supported","package":name}),
                            ));
                        }
                        closure.push(CatalogPin {
                            name: name.clone(),
                            version: version.clone(),
                            kind: package.manifest.kind,
                            manifest_sha256: package.record.manifest_sha256.clone(),
                            archive_sha256: package.record.archive_sha256.clone(),
                            publisher_fingerprint: key_fingerprint(&package.record.public_key)
                                .ok_or_else(|| refusal("record_mismatch"))?,
                            root: roots.contains_key(name),
                            range: roots.get(name).cloned(),
                        });
                    }
                    Ok(CatalogResolution {
                        closure,
                        resolution,
                    })
                };
                Ok(run())
            })
            .map_err(CatalogResolveError::Store)?;
        result
    }
}
#[cfg(test)]
mod tests;

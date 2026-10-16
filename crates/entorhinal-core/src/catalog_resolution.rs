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
                    if let Some(s) = crate::catalog_agent::blocked_status(conn, &pins).map_err(|e| CatalogResolveError::Store(e.into()))? {
                        return Err(CatalogResolveError::Refusal(json!({"code":if s["state"] == "tombstoned" {"version_tombstoned"} else {"version_revoked"},"name":s["name"],"version":s["version"],"reason":s["reason"]})));
                    }
                    if let CatalogSubject::Upgrade(spec) = &subject {
                        if !pins.iter().any(|p| p.root && p.name == spec.package) {
                            return Err(refusal("root_not_found"));
                        }
                    }
                    let mut registry: BTreeMap<String, Vec<RegistryVersion>> = BTreeMap::new();
                    let mut by_version = BTreeMap::new();
                    let mut dependencies = BTreeMap::new();
                    let mut news = BTreeMap::new();
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
                        news.insert((r.name.clone(),r.version.clone()), serde_json::to_value(&held).unwrap());
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
                                status: if held.state == entorhinal_catalog::status::State::Revoked && !crate::catalog_agent::status_blocks(&serde_json::to_value(&held).unwrap()) { entorhinal_catalog::status::State::Available } else { held.state },
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
                                if let resolver::ResolveError::NoMatchingVersion { package: name, ranges } = &e {
                                    let ranges = ranges.iter().map(|r| r.range.parse::<entorhinal_catalog::semver::Range>().expect("validated dependency range")).collect::<Vec<_>>();
                                    let matching = registry.get(name).into_iter().flatten().filter(|v| ranges.iter().all(|range| range.satisfies(&v.version.parse().expect("verified version")))).collect::<Vec<_>>();
                                    if !matching.iter().any(|v| v.status == entorhinal_catalog::status::State::Available) {
                                        if let Some(s) = matching.iter().filter_map(|v| news.get(&(name.clone(),v.version.clone()))).find(|s| crate::catalog_agent::status_blocks(s)) {
                                            return CatalogResolveError::Refusal(json!({"code":if s["state"] == "tombstoned" {"version_tombstoned"} else {"version_revoked"},"name":s["name"],"version":s["version"],"reason":s["reason"]}));
                                        }
                                    }
                                }
                                CatalogResolveError::Refusal(serde_json::to_value(e).unwrap())
                            })?;
                    let mut closure = Vec::new();
                    for (name, version) in &resolution.picks {
                        let package = by_version[&(name.clone(), version.clone())];
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
                    let requested = match &subject {
                        CatalogSubject::Install(spec) | CatalogSubject::Upgrade(spec) => {
                            spec.package.as_str()
                        }
                        CatalogSubject::UpgradeAll => "",
                    };
                    let slot_conflict =
                        crate::catalog_agent::kind_slot_conflict(&closure, requested);
                    if let Some(conflict) = slot_conflict {
                        return Err(CatalogResolveError::Refusal(
                            json!({"code":"kind_slot_taken","detail":conflict}),
                        ));
                    }
                    // Non-skill packages remain unsupported until their consumers are available.
                    // Check root slots first so conflicts are rejected as soon as those kinds
                    // become installable.
                    if let Some(pin) = closure.iter().find(|pin| pin.kind != Kind::Skill) {
                        return Err(CatalogResolveError::Refusal(
                            json!({"code":"kind_not_yet_supported","package":pin.name}),
                        ));
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

#[cfg(test)]
mod kind_slot_tests {
    use super::*;
    use crate::catalog_agent::{CatalogPin, PinWriteRequest};
    use crate::mutations::tests::Fixture;
    use entorhinal_catalog::record::{Manifest, Record, Requires};
    use entorhinal_catalog::status::State;

    fn candidate(name: &str, kind: Kind) -> VerifiedPackage {
        let name = format!("@example/{name}");
        VerifiedPackage {
            record: Record {
                schema: "ck-record/v1".into(),
                name: name.clone(),
                version: "1.0.0".into(),
                manifest_sha256: "11".repeat(32),
                archive_sha256: "22".repeat(32),
                archive_bytes: 0,
                public_key: "44".repeat(32),
                log_head: json!({}),
                flow_version: None,
            },
            manifest: Manifest {
                schema: "ck-package/v1".into(),
                name,
                version: "1.0.0".into(),
                kind,
                summary: "test".into(),
                license: "MIT".into(),
                source: "test".into(),
                requires: Requires {
                    capabilities: vec![],
                    packages: BTreeMap::new(),
                },
                files: vec![],
                body: json!({}),
            },
            canonical_record: String::new(),
        }
    }

    fn setup(
        store: &RegistryStore,
        agent: &str,
        pins: Vec<CatalogPin>,
        packages: &[VerifiedPackage],
    ) {
        let fingerprint = key_fingerprint(&"44".repeat(32)).unwrap();
        let mut pins = pins;
        for pin in &mut pins {
            pin.publisher_fingerprint = fingerprint.clone();
        }
        store
            .with_principal("test")
            .catalog_set_pins(
                PinWriteRequest {
                    agent_id: agent.into(),
                    pins,
                    request_key: None,
                },
                2,
            )
            .unwrap();
        store
            .db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT OR IGNORE INTO catalog_registry VALUES('r',0,1,'inc')",
                    [],
                )?;
                for package in packages {
                    let held = StatusRecord {
                        schema: "ck-status/v1".into(),
                        registry_key_id: "key".into(),
                        name: package.record.name.clone(),
                        version: package.record.version.clone(),
                        archive_sha256: package.record.archive_sha256.clone(),
                        publisher_key: fingerprint.clone(),
                        sequence: 1,
                        state: State::Available,
                        reason: None,
                        compromise_from: None,
                        published_at: "2026-01-01T00:00:00Z".into(),
                        changed_at: "2026-01-01T00:00:00Z".into(),
                    };
                    tx.execute(
                        "INSERT INTO catalog_status VALUES(?1,?2,'r',1,'t','t',0,?3,'signature')",
                        params![
                            package.record.name,
                            package.record.version,
                            serde_json::to_string(&held).unwrap()
                        ],
                    )?;
                }
                Ok(())
            })
            .unwrap();
    }

    #[test]
    fn catalog_prepare_refuses_second_root_before_non_skill_gate() {
        for kind in [Kind::MemoryProfile, Kind::Persona] {
            let fixture = Fixture::new("catalog-kind-slot-prepare");
            let agent = crate::catalog_agent::tests::agent(&fixture.store);
            let suffix = if kind == Kind::MemoryProfile {
                "profile"
            } else {
                "persona"
            };
            let held_name = format!("@example/held-{suffix}");
            let requested_name = format!("@example/requested-{suffix}");
            let packages = [
                candidate(&format!("held-{suffix}"), kind),
                candidate(&format!("requested-{suffix}"), kind),
            ];
            let mut held = crate::catalog_agent::tests::pin(&held_name, "1.0.0", true);
            held.kind = kind;
            setup(&fixture.store, &agent, vec![held], &packages);

            let error = fixture
                .store
                .catalog_resolve(
                    &agent,
                    CatalogSubject::Install(CatalogSpec {
                        package: requested_name.clone(),
                        range: "^1.0.0".into(),
                    }),
                    &packages,
                    &[],
                )
                .unwrap_err();
            let CatalogResolveError::Refusal(error) = error else {
                panic!("expected kind-slot refusal")
            };
            assert_eq!(error["code"], "kind_slot_taken");
            assert_eq!(
                error["detail"],
                json!({"kind":kind,"held":held_name,"requested":requested_name})
            );
        }
    }
}

//! Check the receiving machine's permissions and refresh registry status for every
//! package pinned to the moving agent.
use super::*;
use entorhinal_core::catalog_grants::{coverage, Coverage, CoveragePackage, Operation};
use std::collections::BTreeSet;

pub(super) struct Preflight {
    pub pins: Vec<CatalogPin>,
    pub certified: BTreeSet<String>,
}
fn refused(pin: &CatalogPin, reason: &str) -> HandlerError {
    HandlerError {
        code: "trust_missing".into(),
        message: "the target machine cannot authorize the moved closure".into(),
        detail: Some(json!({"publishers":[{
            "publisher":pin.name.split('/').next().unwrap(),
            "publisher_fingerprint":pin.publisher_fingerprint,
            "package":pin.name,"missing":[reason]
        }]})),
    }
}
fn network(error: crate::catalog_network::NetworkError) -> HandlerError {
    // An unauthenticated reply is not evidence of a publisher's refusal.
    HandlerError::new("trust_unavailable", format!("{error:?}"))
}
impl CatalogMove {
    pub(super) async fn current_trust(
        &self,
        pins: Vec<CatalogPin>,
    ) -> Result<Preflight, HandlerError> {
        let scopes: BTreeSet<_> = pins
            .iter()
            .map(|p| p.name.split('/').next().unwrap().to_owned())
            .collect();
        let mut certified = BTreeSet::new();
        for scope in scopes {
            let store = self.store()?;
            let local_scope = scope.clone();
            let (registry, rows) = crate::catalog_sql::run(move || {
                let url = store.catalog_registry_for_scope(&local_scope)?;
                let registry = store
                    .catalog_registries()?
                    .into_iter()
                    .find(|r| Some(&r.url) == url.as_ref());
                let rows = store.catalog_statuses(url.as_deref().unwrap_or(""))?;
                Ok::<_, HandlerError>((registry, rows))
            })
            .await?;
            let scope_pins: Vec<_> = pins
                .iter()
                .filter(|p| p.name.starts_with(&format!("{scope}/")))
                .collect();
            let (registry, rows, keys) = if let Some(registry) = &registry {
                let store = self.store()?;
                let sources = self.sources.clone();
                let local_scope = scope.clone();
                let mut trust = crate::catalog_sql::run(move || sources(store, &local_scope))
                    .await
                    .map_err(unavailable)?;
                trust.use_weak_store(self.store.clone());
                let applied = trust.apply_scope_feed().await.map_err(network)?;
                if let Some(error) = applied.refusal {
                    return Err(unavailable(error));
                }
                for (_, result) in applied.results {
                    result.map_err(unavailable)?;
                }
                let keys = if scope == "@cortexkit" {
                    trust.scope_keys(self.root.as_deref()).await.map_err(|e| {
                        if matches!(
                            e.code(),
                            "signature_invalid"
                                | "key_unknown"
                                | "key_not_valid"
                                | "record_mismatch"
                        ) {
                            refused(scope_pins[0], e.code())
                        } else {
                            network(e)
                        }
                    })?
                } else {
                    vec![]
                };
                let store = self.store()?;
                let expected = registry.clone();
                let local_scope = scope.clone();
                let (registry, rows) = crate::catalog_sql::run(move || {
                    let mapped = store.catalog_registry_for_scope(&local_scope)?;
                    let current = store
                        .catalog_registries()?
                        .into_iter()
                        .find(|r| r.url == expected.url && r.incarnation == expected.incarnation)
                        .filter(|r| mapped.as_ref() == Some(&r.url))
                        .ok_or_else(|| unavailable("registry changed during move preflight"))?;
                    let rows = store.catalog_statuses(&current.url)?;
                    Ok::<_, HandlerError>((current, rows))
                })
                .await?;
                (Some(registry), rows, keys)
            } else {
                if scope != "@cortexkit" {
                    return Err(unavailable("pinned scope has no reachable registry"));
                }
                (None, rows, vec![])
            };
            for pin in scope_pins {
                let row = rows
                    .iter()
                    .find(|r| r.name == pin.name && r.version == pin.version)
                    .ok_or_else(|| unavailable("current pinned status is unknown"))?;
                let held =
                    crate::catalog_trust::restore(row, registry.as_ref(), self.root.as_deref())
                        .map_err(|e| refused(pin, e.code))?;
                let status = held.record();
                if status.archive_sha256 != pin.archive_sha256
                    || status.publisher_key != pin.publisher_fingerprint
                {
                    return Err(refused(pin, "authenticated status differs from pin"));
                }
                if matches!(
                    status.state,
                    entorhinal_catalog::status::State::Revoked
                        | entorhinal_catalog::status::State::Tombstoned
                ) {
                    return Err(refused(pin, &format!("package is {:?}", status.state)));
                }
                if scope == "@cortexkit" {
                    let valid = if registry.is_none() {
                        row.starter_proof.is_some()
                    } else {
                        keys.iter().any(|key| {
                            entorhinal_catalog::key_fingerprint(&key.public_key).as_ref()
                                == Some(&pin.publisher_fingerprint)
                                && key.covers(&status.published_at)
                                && !key.revocation.as_ref().is_some_and(|r| {
                                    entorhinal_catalog::status::revoked_at(
                                        Some(&status.published_at),
                                        &r.compromise_from,
                                    )
                                })
                        })
                    };
                    if !valid {
                        return Err(refused(pin, "no root-certified key valid at publication"));
                    }
                    certified.insert(pin.name.clone());
                }
            }
        }
        Ok(Preflight { pins, certified })
    }
}

impl CatalogMove {
    pub(super) async fn coverage(
        &self,
        agent: &str,
        checked: &Preflight,
        staged: Option<&[PreparedPackage]>,
    ) -> Result<(), HandlerError> {
        let store = self.store()?;
        let agent = agent.to_owned();
        let pins = checked.pins.clone();
        let certified = checked.certified.clone();
        let staged = staged.map(<[PreparedPackage]>::to_vec);
        crate::catalog_sql::run(move || {
            let grants = store.publisher_grants()?;
            let live = store.agent_row(&agent)?.is_some_and(|a| a.gone().is_none());
            let blobs = if store.store_folder().join("catalog/blobs").exists() {
                Some(BlobStore::new(store.store_folder())?)
            } else { None };
            let mut missing = Vec::new();
            for pin in pins {
                let package = if let Some(package) = staged.as_ref().and_then(|p| p.iter().find(|p| p.pin == pin)) {
                    package.coverage()
                } else {
                    // Before transfer only locally available manifests can refine the
                    // preliminary grant check. Staging always checks the full closure.
                    let manifest = match blobs.as_ref().map(|b| b.read_verified(&pin.manifest_sha256)) {
                        Some(Ok(bytes)) => Some(record::parse_manifest(&bytes).map_err(unavailable)?),
                        Some(Err(error)) if error.code == "blob_unknown" => None,
                        Some(Err(error)) => return Err(unavailable(error)),
                        None => None,
                    };
                    let scripts = manifest.as_ref().map(|m| &m.body["scripts"]);
                    CoveragePackage {
                        name: pin.name.clone(), publisher_fingerprint: pin.publisher_fingerprint.clone(),
                        capabilities: manifest.as_ref().map(|m| m.requires.capabilities.clone()).unwrap_or_default(),
                        has_scripts: scripts.is_some_and(|s| s.as_object().is_some_and(|s| !s.is_empty()) || s.as_array().is_some_and(|s| !s.is_empty())),
                        root_certified: certified.contains(&pin.name),
                    }
                };
                if let Coverage::Uncovered(reasons) = coverage(&[package], &grants, Operation::Install, &agent, live) {
                    missing.push(json!({"publisher":pin.name.split('/').next().unwrap(),"publisher_fingerprint":pin.publisher_fingerprint,"package":pin.name,"missing":reasons}));
                }
            }
            if missing.is_empty() { Ok(()) } else {
                Err(HandlerError { code:"trust_missing".into(), message:"target publisher grants do not cover the moved closure".into(), detail:Some(json!({"publishers":missing})) })
            }
        }).await
    }
}

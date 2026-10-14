//! Registry replies are composed with the pure verifier; only authenticated
//! status is durable, and package bytes pass a separate publication boundary.
#![allow(dead_code)] // Verification is ready for future install and consumer callers.
use super::catalog_blob::{BlobStore, CatalogError};
use super::catalog_network::{Limits, NetworkError, RegistryClient};
use entorhinal_catalog::{jcs, names, record, statements, status, Refusal, RefusalCode};
use entorhinal_core::catalog::{CatalogRegistry, CatalogWriteError, RegistryKey};
use entorhinal_core::catalog_trust::{CatalogStatus, TrustWrite};
use entorhinal_core::RegistryStore;
use serde::Deserialize;
use serde_json::{json, Value};

/// Public key of the offline CortexKit root that certifies publishing keys.
/// None refuses root-based trust until the operator provisions the production key.
pub const CORTEXKIT_ROOT: Option<&str> = None;
impl From<Refusal> for NetworkError {
    fn from(error: Refusal) -> Self {
        CatalogError::from(error).into()
    }
}
impl From<Refusal> for CatalogError {
    fn from(error: Refusal) -> Self {
        Self {
            code: error.code.as_str(),
            message: error.to_string(),
            detail: serde_json::to_value(error).ok(),
        }
    }
}
impl From<CatalogWriteError> for CatalogError {
    fn from(error: CatalogWriteError) -> Self {
        match error {
            CatalogWriteError::Refused(code) => Self::new(code, code),
            CatalogWriteError::Store(e) => Self::new("store_error", e.to_string()),
        }
    }
}
fn store_error(e: entorhinal_core::RegistryError) -> CatalogError {
    CatalogError::new("store_error", e.to_string())
}
fn mismatch(field: &str) -> CatalogError {
    Refusal::field(RefusalCode::RecordMismatch, field).into()
}
fn strict(bytes: &[u8]) -> Result<Value, CatalogError> {
    std::str::from_utf8(bytes)
        .ok()
        .and_then(|s| jcs::parse_signed(s).ok())
        .ok_or_else(|| CatalogError::new("invalid_reply", "registry reply is not strict JSON"))
}
fn document(value: &Value) -> Result<String, CatalogError> {
    jcs::to_string(value).map_err(|_| CatalogError::new("invalid_reply", "invalid signed document"))
}
fn chain(registry: &CatalogRegistry) -> Vec<status::StatusKey> {
    registry
        .keys
        .iter()
        .map(|key| status::StatusKey {
            key_id: key.key_id.clone(),
            public_key: key.public_key.clone(),
            valid_from: key.valid_from.clone(),
        })
        .collect()
}
fn identity(expected: &CatalogRegistry, current: &CatalogRegistry) -> Result<(), CatalogError> {
    if expected.url != current.url
        || expected.incarnation != current.incarnation
        || !expected
            .keys
            .iter()
            .zip(&current.keys)
            .all(|(a, b)| a.key_id == b.key_id && a.public_key == b.public_key)
        || current.keys.len() < expected.keys.len()
    {
        return Err(CatalogError::new(
            "registry_changed",
            "registry trust was replaced during the request",
        ));
    }
    Ok(())
}
fn restore(
    row: &CatalogStatus,
    registry: &CatalogRegistry,
) -> Result<status::HeldStatus, CatalogError> {
    let held = status::HeldStatus::restore(
        &row.canonical,
        &row.signature,
        &chain(registry),
        row.signer_index,
    )?;
    let r = held.record();
    if r.name != row.name
        || r.version != row.version
        || r.sequence != row.sequence
        || r.changed_at != row.changed_at
        || r.published_at != row.published_at
        || row.registry_url != registry.url
    {
        return Err(CatalogError::new(
            "status_invalid",
            "stored status columns disagree with authenticated bytes",
        ));
    }
    Ok(held)
}
fn row(held: &status::HeldStatus, sig: &str, url: &str) -> CatalogStatus {
    let r = held.record();
    CatalogStatus {
        name: r.name.clone(),
        version: r.version.clone(),
        registry_url: url.into(),
        sequence: r.sequence,
        changed_at: r.changed_at.clone(),
        published_at: r.published_at.clone(),
        signer_index: held.signer_index(),
        canonical: held.canonical().into(),
        signature: sig.into(),
    }
}
/// Both ingestion paths enforce the same immutable version binding.
fn accept(
    registry: &CatalogRegistry,
    rows: &[CatalogStatus],
    text: &str,
    sig: &str,
    pin: &status::Pin,
) -> Result<status::AppliedStatus, CatalogError> {
    let held = rows
        .iter()
        .find(|r| r.name == pin.name && r.version == pin.version)
        .map(|r| restore(r, registry))
        .transpose()?;
    if held
        .as_ref()
        .is_some_and(|h| h.record().archive_sha256 != pin.archive_sha256)
    {
        return Err(mismatch("archive_sha256"));
    }
    let applied = status::apply_status(held.as_ref(), text, sig, &chain(registry), pin)?;
    if let Some(prior) = held {
        if prior.record().published_at != applied.held.record().published_at {
            return Err(mismatch("published_at"));
        }
        if prior.record().publisher_key != applied.held.record().publisher_key {
            return Err(mismatch("publisher_key"));
        }
    }
    Ok(applied)
}
fn scoped(scope: &str, name: &str, version: &str) -> Result<names::MappedName, CatalogError> {
    if names::map_name(name).is_err() || names::map_name(name).is_ok_and(|m| m.canonical != name) {
        return Err(CatalogError::new("name_invalid", "name must be canonical"));
    }
    let mapped = names::map_name(name)
        .map_err(|_| CatalogError::new("name_invalid", "invalid package name"))?;
    if scope != format!("@{}", mapped.scope) {
        return Err(mismatch("scope"));
    }
    if version
        .parse::<entorhinal_catalog::semver::Version>()
        .is_err()
    {
        return Err(CatalogError::new(
            "record_invalid",
            "invalid requested version",
        ));
    }
    Ok(mapped)
}
fn scope_path(scope: &str) -> Result<&str, CatalogError> {
    let part = scope.strip_prefix('@').ok_or_else(|| mismatch("scope"))?;
    let mapped = names::map_name(&format!("{scope}/p")).map_err(|_| mismatch("scope"))?;
    if mapped.canonical != format!("{scope}/p") {
        return Err(mismatch("scope"));
    }
    Ok(part)
}
pub struct CatalogTrust {
    store: RegistryStore,
    client: RegistryClient,
    scope: String,
    registry: CatalogRegistry,
    refused_links: std::collections::BTreeSet<String>,
}
impl CatalogTrust {
    pub fn for_scope(store: RegistryStore, scope: &str) -> Result<Self, CatalogError> {
        scope_path(scope)?;
        let registry = store
            .catalog_trust_transaction::<_, CatalogError>(scope, |registry, _, _| {
                Ok((registry.clone(), TrustWrite::default()))
            })?;
        let allowlist = store
            .catalog_registries()
            .map_err(store_error)?
            .into_iter()
            .map(|r| r.url)
            .collect::<Vec<_>>();
        let client = RegistryClient::new(&registry.url, &allowlist, Limits::MODULE)?;
        Ok(Self {
            store,
            client,
            scope: scope.into(),
            registry,
            refused_links: Default::default(),
        })
    }
    #[cfg(test)]
    pub(crate) fn with_client(
        store: RegistryStore,
        scope: &str,
        client: RegistryClient,
    ) -> Result<Self, CatalogError> {
        scope_path(scope)?;
        let url = store
            .catalog_registry_for_scope(scope)
            .map_err(store_error)?
            .ok_or_else(|| CatalogError::new("scope_unmapped", "scope has no registry"))?;
        let registry = store
            .catalog_registries()
            .map_err(store_error)?
            .into_iter()
            .find(|r| r.url == url)
            .ok_or_else(|| CatalogError::new("registry_unknown", "unknown registry"))?;
        Ok(Self {
            store,
            client,
            scope: scope.into(),
            registry,
            refused_links: Default::default(),
        })
    }
    pub async fn refresh_registry_keys(&mut self) -> Result<(), NetworkError> {
        let bytes = self.client.fetch_json("v1/registry/keys").await?;
        let reply = strict(&bytes)?;
        if reply["schema"] != "ck-registry-keys/v1" {
            return Err(CatalogError::new("invalid_reply", "registry key schema").into());
        }
        if reply["registry"] != self.registry.url {
            return Err(mismatch("registry").into());
        }
        let links = reply["keys"]
            .as_array()
            .ok_or_else(|| CatalogError::new("invalid_reply", "missing registry keys"))?;
        let expected = self.registry.clone();
        let scope = self.scope.clone();
        // Accepted prefix links survive a later refusal. The refusal is returned
        // after the transaction commits that prefix, never by rolling it back.
        let (registry, refusal) =
            self.store
                .catalog_trust_transaction::<_, CatalogError>(&scope, |current, _, _| {
                    identity(&expected, current)?;
                    let mut next = current.clone();
                    let mut pure = statements::RegistryChain {
                        registry: current.url.clone(),
                        keys: current
                            .keys
                            .iter()
                            .map(|k| statements::RegistryKey {
                                key_id: k.key_id.clone(),
                                public_key: k.public_key.clone(),
                                statement: k.statement.clone(),
                            })
                            .collect(),
                    };
                    let anchor = links.iter().rposition(|link| {
                        current
                            .keys
                            .iter()
                            .any(|key| link["statement"]["key_id"] == key.key_id)
                    });
                    let mut refusal = None;
                    for (position, link) in links.iter().enumerate() {
                        let result = (|| {
                            let text = document(&link["statement"])?;
                            let sig = link["signature"].as_str().ok_or_else(|| {
                                CatalogError::new(
                                    "statement_invalid",
                                    "missing registry key signature",
                                )
                            })?;
                            let known = next
                                .keys
                                .iter()
                                .position(|k| link["statement"]["key_id"] == k.key_id);
                            if known.is_none() && anchor.is_some_and(|a| position < a) {
                                return Ok(());
                            }
                            if let Some(index) = known {
                                if next.keys[index].statement.is_none() {
                                    let mut validation = pure.clone();
                                    validation.keys[index].statement = Some(text.clone());
                                    statements::apply_registry_key(&mut validation, &text, sig)?;
                                    if link["statement"]["public_key"]
                                        != next.keys[index].public_key
                                    {
                                        return Err(Refusal::new(RefusalCode::KeyConflict).into());
                                    }
                                    pure = validation;
                                    next.keys[index].statement = Some(text);
                                } else {
                                    statements::apply_registry_key(&mut pure, &text, sig)?;
                                }
                            } else {
                                statements::apply_registry_key(&mut pure, &text, sig)?;
                                let statement: statements::RegistryKeyStatement =
                                    serde_json::from_value(link["statement"].clone()).map_err(
                                        |_| {
                                            CatalogError::new(
                                                "statement_invalid",
                                                "invalid registry statement",
                                            )
                                        },
                                    )?;
                                next.keys.push(RegistryKey {
                                    chain_index: next.keys.len() as i64,
                                    key_id: statement.key_id,
                                    public_key: statement.public_key,
                                    statement: pure.keys.last().and_then(|k| k.statement.clone()),
                                    valid_from: Some(statement.valid_from),
                                });
                            }
                            Ok::<_, CatalogError>(())
                        })();
                        if let Err(e) = result {
                            refusal = Some((
                                e,
                                entorhinal_catalog::sha256_hex(link.to_string().as_bytes()),
                            ));
                            break;
                        }
                    }
                    Ok((
                        (next.clone(), refusal),
                        TrustWrite {
                            keys: Some(next.keys),
                            ..Default::default()
                        },
                    ))
                })?;
        self.registry = registry;
        if let Some((e, digest)) = refusal {
            if self.refused_links.insert(digest.clone()) {
                tracing::warn!(
                    registry = self.registry.url,
                    code = e.code,
                    document_sha256 = digest,
                    "registry key link refused"
                );
            }
            return Err(e.into());
        }
        Ok(())
    }
    pub async fn scope_keys(
        &self,
        root: Option<&str>,
    ) -> Result<Vec<statements::ScopeKey>, NetworkError> {
        let reply = strict(
            &self
                .client
                .fetch_json(&format!("v1/scopes/{}", scope_path(&self.scope)?))
                .await?,
        )?;
        scope_keys(&reply, scope_path(&self.scope)?, root).map_err(Into::into)
    }
    pub async fn verify_version(
        &mut self,
        name: &str,
        version: &str,
    ) -> Result<record::VerifiedPackage, NetworkError> {
        self.verify_version_with_root(name, version, CORTEXKIT_ROOT)
            .await
            .map_err(|mut e| {
                if let NetworkError::Failed(error) = &mut e {
                    let mut detail = error.detail.take().unwrap_or_else(|| json!({}));
                    detail["name"] = name.into();
                    detail["version"] = version.into();
                    error.detail = Some(detail);
                }
                e
            })
    }
    async fn verify_version_with_root(
        &mut self,
        name: &str,
        version: &str,
        root: Option<&str>,
    ) -> Result<record::VerifiedPackage, NetworkError> {
        let mapped = scoped(&self.scope, name, version)?;
        let reply = strict(
            &self
                .client
                .fetch_json(&format!("v1/packages/{}/{}", mapped.scope, mapped.name))
                .await?,
        )?;
        if reply["name"] != name {
            return Err(mismatch("name").into());
        }
        let versions = reply["versions"]
            .as_array()
            .ok_or_else(|| CatalogError::new("invalid_reply", "missing versions"))?;
        let entry = versions
            .iter()
            .find(|r| r["version"] == version)
            .ok_or_else(|| CatalogError::new("version_unknown", "version not served"))?;
        let record_text = document(&entry["record"])?;
        if entry["record"]["name"] != name {
            return Err(mismatch("name").into());
        }
        if entry["record"]["version"] != version {
            return Err(mismatch("version").into());
        }
        let status_text = document(&entry["status"]["record"])?;
        let status_sig = entry["status"]["signature"]
            .as_str()
            .ok_or_else(|| CatalogError::new("status_invalid", "missing status signature"))?;

        let expected = self.registry.clone();
        let pin = status::Pin {
            name: name.into(),
            version: version.into(),
            archive_sha256: entry["record"]["archive_sha256"]
                .as_str()
                .ok_or_else(|| CatalogError::new("record_invalid", "missing archive digest"))?
                .into(),
        };
        let mut refreshed = false;
        let applied = loop {
            let result = self.store.catalog_trust_transaction::<_, CatalogError>(
                &self.scope,
                |current, held, _| {
                    identity(&expected, current)?;
                    let applied = accept(current, held, &status_text, status_sig, &pin)?;
                    let write = TrustWrite {
                        statuses: vec![row(&applied.held, status_sig, &current.url)],
                        ..Default::default()
                    };
                    Ok((applied, write))
                },
            );
            match result {
                Err(error) if error.code == "key_unknown" && !refreshed => {
                    self.refresh_registry_keys().await?;
                    refreshed = true;
                }
                other => break other?,
            }
        };
        let served: record::Record = serde_json::from_value(entry["record"].clone())
            .map_err(|_| CatalogError::new("record_invalid", "invalid record"))?;
        if Some(applied.held.record().publisher_key.clone())
            != entorhinal_catalog::key_fingerprint(&served.public_key)
        {
            return Err(mismatch("publisher_key").into());
        }
        let keys = self.scope_keys(root).await?;
        let trust = record::Trust {
            keys: &keys,
            published_at: Some(&applied.held.record().published_at),
        };
        record::verify_record_trust(
            &record_text,
            entry["signature"].as_str().unwrap_or(""),
            &trust,
        )?;
        let manifest = self
            .client
            .fetch_manifest(&format!(
                "v1/packages/{}/{}/{version}/ck.json",
                mapped.scope, mapped.name
            ))
            .await?;
        let archive = self
            .client
            .fetch_archive_bytes(&served.archive_sha256)
            .await?;
        let verified = record::verify_record(
            &record_text,
            entry["signature"].as_str().unwrap_or(""),
            &manifest,
            &archive,
            &trust,
        )?;
        let folder = self.store.store_folder();
        // Recheck this version's current status and selected registry after
        // downloads. A higher status sequence or a replacement registry makes
        // the earlier acceptance stale, so its bytes must not be published.
        self.store.catalog_trust_transaction::<_, CatalogError>(
            &self.scope,
            |current, held, _| {
                identity(&expected, current)?;
                accept(current, held, &status_text, status_sig, &pin)?;
                BlobStore::new(folder)?.publish(&archive)?;
                Ok(((), TrustWrite::default()))
            },
        )?;
        Ok(verified)
    }
    pub async fn apply_scope_feed(&mut self) -> Result<FeedApply, NetworkError> {
        self.refresh_registry_keys().await?;
        let etag = self
            .store
            .catalog_feed_etag(&self.scope)
            .map_err(store_error)?;
        let after = self
            .store
            .catalog_feed_seq(&self.scope)
            .map_err(store_error)?;
        let mut mode = if after.is_some() {
            status::FeedMode::After
        } else {
            status::FeedMode::Full
        };
        loop {
            let path = format!(
                "v1/scopes/{}/status{}",
                scope_path(&self.scope)?,
                if mode == status::FeedMode::After {
                    format!("?after={}", after.unwrap())
                } else {
                    String::new()
                }
            );
            let response = match self
                .client
                .fetch_feed(
                    &path,
                    if mode == status::FeedMode::After {
                        etag.as_deref()
                    } else {
                        None
                    },
                )
                .await
            {
                Err(NetworkError::FeedAfterUnavailable) if mode == status::FeedMode::After => {
                    mode = status::FeedMode::Full;
                    continue;
                }
                other => other?,
            };
            if response.not_modified {
                self.store.catalog_trust_transaction::<_, CatalogError>(
                    &self.scope,
                    |current, _, _| {
                        identity(&self.registry, current)?;
                        Ok(((), TrustWrite::default()))
                    },
                )?;
                return Ok(FeedApply {
                    results: vec![],
                    refusal: None,
                    fetch_full: false,
                    cursor_advanced: false,
                });
            }
            let reply = strict(&response.bytes)?;
            if reply["schema"] != "ck-status-feed/v1" || reply["scope"] != scope_path(&self.scope)?
            {
                return Err(mismatch("scope").into());
            }
            let seq = reply["feed_seq"]
                .as_u64()
                .filter(|n| *n <= 9_007_199_254_740_991)
                .ok_or_else(|| CatalogError::new("invalid_reply", "invalid feed_seq"))?;
            let entries = reply["records"]
                .as_array()
                .ok_or_else(|| CatalogError::new("invalid_reply", "missing feed records"))?;

            let records = entries
                .iter()
                .map(|entry| {
                    Ok(status::FeedRecord {
                        id: entry["feed_seq"].to_string(),
                        record_text: document(&entry["record"])?,
                        signature: entry["signature"].as_str().unwrap_or("").into(),
                    })
                })
                .collect::<Result<Vec<_>, CatalogError>>()?;
            let expected = self.registry.clone();
            let scope = self.scope.clone();
            let outcome = self.store.catalog_trust_transaction::<_, CatalogError>(
                &scope,
                |current, rows, cursor| {
                    identity(&expected, current)?;
                    let mut state = status::FeedState {
                        feed_seq: cursor,
                        ..Default::default()
                    };
                    for r in rows {
                        let held = restore(r, current)?;
                        state.held.insert(format!("{}@{}", r.name, r.version), held);
                    }
                    let mut pins = std::collections::BTreeMap::new();
                    for record in &records {
                        if let Ok(s) =
                            serde_json::from_str::<status::StatusRecord>(&record.record_text)
                        {
                            if scoped(&scope, &s.name, &s.version).is_ok() {
                                pins.insert(
                                    format!("{}@{}", s.name, s.version),
                                    status::Pin {
                                        name: s.name,
                                        version: s.version,
                                        archive_sha256: s.archive_sha256,
                                    },
                                );
                            }
                        }
                    }
                    let pure = status::apply_feed(
                        &state,
                        mode,
                        &status::FeedReply::Records {
                            feed_seq: seq,
                            records: records.clone(),
                        },
                        &chain(current),
                        &pins,
                    );
                    let mut write = TrustWrite::default();
                    let mut holds = rows.to_vec();
                    let mut results = Vec::new();
                    if pure
                        .refusal
                        .as_ref()
                        .is_some_and(|r| r.field.as_deref() == Some("feed_seq"))
                    {
                        return Ok((
                            FeedApply {
                                results,
                                refusal: pure.refusal.map(Into::into),
                                fetch_full: matches!(
                                    pure.next_step,
                                    status::NextStep::FetchFullFeed
                                ),
                                cursor_advanced: false,
                            },
                            write,
                        ));
                    }
                    for (record, (_, result)) in records.iter().zip(pure.records) {
                        let result = result.map_err(CatalogError::from).and_then(|_| {
                            let s: status::StatusRecord = serde_json::from_str(&record.record_text)
                                .map_err(|_| {
                                    CatalogError::new("status_invalid", "invalid status")
                                })?;
                            let pin = status::Pin {
                                name: s.name.clone(),
                                version: s.version.clone(),
                                archive_sha256: s.archive_sha256.clone(),
                            };
                            let applied = accept(
                                current,
                                &holds,
                                &record.record_text,
                                &record.signature,
                                &pin,
                            )?;
                            let stored = row(&applied.held, &record.signature, &current.url);
                            holds.retain(|r| r.name != stored.name || r.version != stored.version);
                            holds.push(stored.clone());
                            write.statuses.push(stored);
                            Ok(applied.result)
                        });
                        results.push((record.id.clone(), result));
                    }
                    let all_ok = results.iter().all(|(_, r)| r.is_ok());
                    if all_ok {
                        write.feed_seq = Some(seq);
                        write.feed_etag = response.etag.clone();
                    }
                    let rollback = results
                        .iter()
                        .any(|(_, r)| r.as_ref().is_err_and(|e| e.code == "feed_rollback"));
                    Ok((
                        FeedApply {
                            results,
                            refusal: pure.refusal.map(Into::into),
                            fetch_full: mode == status::FeedMode::After && rollback,
                            cursor_advanced: all_ok,
                        },
                        write,
                    ))
                },
            )?;
            if outcome.fetch_full && mode == status::FeedMode::After {
                mode = status::FeedMode::Full;
                continue;
            }
            return Ok(outcome);
        }
    }
}
#[derive(Debug)]
pub struct FeedApply {
    pub results: Vec<(String, Result<status::Apply, CatalogError>)>,
    pub refusal: Option<CatalogError>,
    pub fetch_full: bool,
    pub cursor_advanced: bool,
}
#[derive(Clone, Deserialize)]
struct ScopeEntry {
    public_key: String,
    fingerprint: String,
    valid_from: String,
    valid_until: Option<String>,
    revocation: Option<statements::Revocation>,
    rotation: Option<Value>,
    cert: Option<Value>,
}
fn scope_keys(
    reply: &Value,
    scope: &str,
    root: Option<&str>,
) -> Result<Vec<statements::ScopeKey>, CatalogError> {
    if reply["scope"] != scope {
        return Err(mismatch("scope"));
    }
    if scope == "cortexkit" && root.is_none() {
        return Ok(vec![]);
    }
    let entries: Vec<ScopeEntry> = serde_json::from_value(reply["keys"].clone())
        .map_err(|_| CatalogError::new("invalid_reply", "invalid scope keys"))?;
    let mut trusted = Vec::<statements::ScopeKey>::new();
    let mut pending = Vec::new();
    let mut seen = std::collections::BTreeSet::new();
    for entry in entries {
        let certified = if scope == "cortexkit" {
            let cert = entry.cert.as_ref().ok_or_else(|| {
                CatalogError::new("key_unknown", "cortexkit key lacks a root cert")
            })?;
            Some(statements::verify_key_cert(
                &document(&cert["statement"])?,
                cert["signature"].as_str().unwrap_or(""),
                root.unwrap(),
            )?)
        } else {
            None
        };
        if !seen.insert(entry.public_key.clone())
            || entorhinal_catalog::key_fingerprint(&entry.public_key).as_ref()
                != Some(&entry.fingerprint)
        {
            return Err(mismatch("fingerprint"));
        }
        for at in std::iter::once(&entry.valid_from)
            .chain(entry.valid_until.iter())
            .chain(entry.revocation.iter().map(|r| &r.compromise_from))
        {
            super::catalog_ops::whole_second(at).map_err(|_| {
                CatalogError::new("statement_invalid", "invalid scope key timestamp")
            })?;
        }
        if entry
            .valid_until
            .as_ref()
            .is_some_and(|at| at <= &entry.valid_from)
            || entry
                .revocation
                .as_ref()
                .is_some_and(|r| r.reason.is_empty())
        {
            return Err(CatalogError::new(
                "statement_invalid",
                "invalid scope key window or revocation",
            ));
        }
        let key = statements::ScopeKey {
            public_key: entry.public_key.clone(),
            valid_from: entry.valid_from.clone(),
            valid_until: entry.valid_until.clone(),
            revocation: entry.revocation.clone(),
        };
        if scope == "cortexkit" {
            let cert = certified.unwrap();
            if cert.public_key != key.public_key
                || cert.valid_from != key.valid_from
                || cert.valid_until != key.valid_until
            {
                return Err(mismatch("cert"));
            }
            trusted.push(key);
        } else if entry.cert.is_some() {
            return Err(mismatch("cert"));
        } else if let Some(rotation) = entry.rotation {
            pending.push((key, rotation));
        } else {
            trusted.push(key);
        }
    }
    loop {
        let before = trusted.len();
        pending.retain(|(key, rotation)| {
            let Some(old) = trusted
                .iter()
                .find(|old| rotation["statement"]["old_key"] == old.public_key)
                .cloned()
            else {
                return true;
            };
            let mut validation = old;
            validation.valid_until = None;
            if validation
                .revocation
                .as_ref()
                .is_some_and(|r| r.compromise_from > key.valid_from)
            {
                validation.revocation = None;
            }
            let result = document(&rotation["statement"]).and_then(|text| {
                statements::verify_key_rotation(
                    &text,
                    rotation["old_signature"].as_str().unwrap_or(""),
                    rotation["new_signature"].as_str().unwrap_or(""),
                    scope,
                    &[validation],
                    &[],
                )
                .map_err(Into::into)
            });
            if result.as_ref().is_ok_and(|r| r.new_key == key.public_key) {
                trusted.push(key.clone());
            }
            false
        });
        if before == trusted.len() {
            break;
        }
    }
    Ok(trusted)
}
#[cfg(test)]
#[path = "catalog_trust_tests.rs"]
pub(crate) mod tests;

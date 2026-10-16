//! Operator-confirmed, machine-local registry configuration and consumer blob reads.
use super::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    catalog_blob::{BlobStore, CatalogError},
    catalog_network::registry_url,
    encode_result, HandlerError, ProjectsHandler, RouteKey,
};
use chrono::{DateTime, SecondsFormat, Utc};
use entorhinal_catalog::{key_fingerprint, names::map_name};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::Principal;
use tokio::time::{timeout_at, Instant};

pub const ADD: &str = "catalog.registry_add";
pub const REPIN: &str = "catalog.registry_repin";
pub const REMOVE: &str = "catalog.registry_remove";
pub const LIST: &str = "catalog.registry_list";
pub const READ_BLOB: &str = "catalog.read_blob";
pub const PUBLISHER_TRUST: &str = "catalog.publisher_trust";
pub const WRITES: &[&str] = &[ADD, REPIN, REMOVE, PUBLISHER_TRUST];
const CONFIRM_BOUND: std::time::Duration = std::time::Duration::from_secs(60);

impl From<CatalogError> for HandlerError {
    fn from(error: CatalogError) -> Self {
        Self {
            code: error.code.into(),
            message: error.message,
            detail: error.detail,
        }
    }
}
impl From<entorhinal_core::catalog::CatalogWriteError> for HandlerError {
    fn from(error: entorhinal_core::catalog::CatalogWriteError) -> Self {
        match error {
            entorhinal_core::catalog::CatalogWriteError::Refused("registry_has_status") => Self::new(
                "registry_has_status",
                "removal would discard status history that protects against rollback; use ck catalog registry repin to rotate the registry's key",
            ),
            entorhinal_core::catalog::CatalogWriteError::Refused(code) => Self::new(code, code),
            entorhinal_core::catalog::CatalogWriteError::Store(error) => {
                Self::new("store_error", error.to_string())
            }
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Add {
    url: String,
    #[serde(default)]
    scopes: Vec<String>,
    #[serde(default)]
    default: bool,
    key_id: String,
    public_key: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Repin {
    url: String,
    key_id: String,
    public_key: String,
    valid_from: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Remove {
    url: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadBlob {
    sha256: String,
    offset: u64,
    length: u64,
}
fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params).map_err(super::invalid_params)
}
fn canonical_url(text: &str) -> Result<String, HandlerError> {
    Ok(registry_url(text)?
        .as_str()
        .trim_end_matches('/')
        .to_string())
}
fn key_summary(verb: &str, url: &str, id: &str, key: &str) -> Result<String, HandlerError> {
    if id.is_empty() || id.chars().any(char::is_control) || url.chars().any(char::is_control) {
        return Err(HandlerError::new(
            "invalid_params",
            "key id and URL must be nonempty printable strings",
        ));
    }
    let fingerprint = key_fingerprint(key).ok_or_else(|| {
        HandlerError::new(
            "invalid_params",
            "public_key must be 64 lower-case hex digits",
        )
    })?;
    let summary = format!("{verb} {url} key {id} SHA256 {fingerprint}");
    // Reserve the longest lifecycle prompt, so any added key can also be removed.
    let remove = format!("Remove {url}, mappings and held state; key {id} SHA256 {fingerprint}");
    if summary.chars().count() > 200 || remove.chars().count() > 200 {
        return Err(HandlerError::new(
            "invalid_params",
            "complete confirmation summary exceeds 200 characters",
        ));
    }
    Ok(summary)
}
pub(crate) fn whole_second(text: &str) -> Result<(), HandlerError> {
    let parsed = DateTime::parse_from_rfc3339(text).map_err(|_| {
        HandlerError::new(
            "invalid_params",
            "valid_from must be RFC 3339 UTC whole-second Z",
        )
    })?;
    if text.len() != 20
        || !text.ends_with('Z')
        || parsed
            .with_timezone(&Utc)
            .to_rfc3339_opts(SecondsFormat::Secs, true)
            != text
    {
        return Err(HandlerError::new(
            "invalid_params",
            "valid_from must be RFC 3339 UTC whole-second Z",
        ));
    }
    Ok(())
}
fn clock_second(now: i64) -> Result<String, HandlerError> {
    DateTime::from_timestamp_millis(now)
        .map(|time| time.to_rfc3339_opts(SecondsFormat::Secs, true))
        .ok_or_else(|| HandlerError::new("invalid_params", "clock is outside RFC 3339 range"))
}
impl ProjectsHandler {
    pub(super) fn catalog_registry_list(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Empty {}
        let _: Empty = parse(params)?;
        encode_result(json!({"registries": self.with_store(|s| s.catalog_registries())?}))
    }
    pub(super) async fn catalog_registry_write(
        &self,
        method: &str,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit(method, key)?;
        let deadline = received + CONFIRM_BOUND;
        let store = self.with_store(|s| Ok(s.clone()))?;
        enum Write {
            Add(Add),
            Repin(Repin),
            Remove(Remove),
        }
        let (operation, summary) = match method {
            ADD => {
                let mut p: Add = parse(params)?;
                p.url = canonical_url(&p.url)?;
                if p.default == !p.scopes.is_empty() {
                    return Err(HandlerError::new(
                        "invalid_params",
                        "choose scopes or default",
                    ));
                }
                let mut seen = std::collections::BTreeSet::new();
                for scope in &p.scopes {
                    let mapped = map_name(&format!("{scope}/p")).map_err(|_| {
                        HandlerError::new("name_invalid", "scope must be canonical @scope")
                    })?;
                    if format!("@{}", mapped.scope) != *scope || !seen.insert(scope) {
                        return Err(HandlerError::new(
                            "name_invalid",
                            "scope must be unique and canonical",
                        ));
                    }
                }
                let summary = key_summary("Add registry", &p.url, &p.key_id, &p.public_key)?;
                (Write::Add(p), summary)
            }
            REPIN => {
                let mut p: Repin = parse(params)?;
                p.url = canonical_url(&p.url)?;
                if let Some(at) = &p.valid_from {
                    whole_second(at)?;
                }
                let summary = key_summary("Repin registry", &p.url, &p.key_id, &p.public_key)?;
                (Write::Repin(p), summary)
            }
            REMOVE => {
                let mut p: Remove = parse(params)?;
                p.url = canonical_url(&p.url)?;
                let db = store.clone();
                let registries =
                    crate::catalog_sql::until(deadline, "registry remove read", move || {
                        db.catalog_registries().map_err(HandlerError::from)
                    })
                    .await?;
                let registry = registries
                    .into_iter()
                    .find(|r| r.url == p.url)
                    .ok_or_else(|| HandlerError::new("registry_unknown", "unknown registry"))?;
                let latest = registry.keys.last().ok_or_else(|| {
                    HandlerError::new("registry_unknown", "registry has no pinned key")
                })?;
                let summary = key_summary("Remove", &p.url, &latest.key_id, &latest.public_key)?;
                let summary = summary.replacen(
                    &format!("Remove {} key", p.url),
                    &format!("Remove {}, mappings and held state; key", p.url),
                    1,
                );
                (Write::Remove(p), summary)
            }
            _ => {
                return Err(HandlerError::new(
                    "unknown_method",
                    "unknown catalog mutation",
                ))
            }
        };
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(OperatorConfirmError::Declined.into());
        }
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline,self.operator_confirmer.confirm_operator(&summary,key)) => {
                result.map_err(|_| crate::catalog_sql::expired("registry confirmation"))??;
            }
        }
        let now = (self.clock)();
        let writer = tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline,self.writer.clone().lock_owned()) => result.map_err(|_| crate::catalog_sql::expired("registry writer admission"))?,
        };
        if *guard.cancelled.borrow() {
            return Err(OperatorConfirmError::Declined.into());
        }
        if Instant::now() >= deadline {
            return Err(crate::catalog_sql::expired("registry write"));
        }
        #[cfg(test)]
        let commit_hook = self.shared_commit_hook.clone();
        crate::catalog_sql::until(deadline, "registry write", move || {
            // Cancellation of the async caller must not release either guard while SQLite writes.
            let (_writer, _confirmation) = (writer, guard);
            #[cfg(test)]
            if let Some(hook) = commit_hook {
                hook();
            }
            match operation {
                Write::Add(p) => store.catalog_registry_add(
                    &p.url,
                    &p.scopes,
                    p.default,
                    &p.key_id,
                    &p.public_key,
                    now,
                )?,
                Write::Repin(p) => {
                    let at = p.valid_from.map(Ok).unwrap_or_else(|| clock_second(now))?;
                    store.catalog_registry_repin(&p.url, &p.key_id, &p.public_key, &at)?;
                }
                Write::Remove(p) => store.catalog_registry_remove(&p.url)?,
            }
            encode_result(json!({"outcome":"ok"}))
        })
        .await
    }
    pub(super) async fn catalog_read_blob(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let deadline = received + std::time::Duration::from_secs(25);
        let admission = self.admit(READ_BLOB, key)?;
        let Some(Principal::Reserved { module_id }) = admission.principal else {
            return Err(HandlerError::new(
                "catalog_consumer_required",
                "blob reads require a catalog consumer module",
            ));
        };
        let catalog = timeout_at(deadline, self.participant_discovery.catalog_list())
            .await
            .map_err(|_| crate::catalog_sql::expired("catalog.read_blob discovery"))?
            .map_err(|_| {
                HandlerError::new("catalog_consumer_required", "daemon catalog unavailable")
            })?;
        let admitted = catalog.modules.iter().any(|entry| {
            entry.module_id == module_id
                && entry.capabilities.as_ref().is_some_and(|c| {
                    c.provides
                        .iter()
                        .any(|name| crate::catalog_consumer::parse_kind(name).is_some())
                })
        });
        if !admitted {
            return Err(HandlerError::new(
                "catalog_consumer_required",
                "module does not declare a catalog consumer kind capability",
            ));
        }
        let p: ReadBlob = parse(params)?;
        let store = self.with_store(|s| Ok(s.clone()))?;
        crate::catalog_sql::until(deadline, "catalog.read_blob", move || {
            if let Some(status) = store.catalog_blob_tombstone(&p.sha256)? {
                return Err(HandlerError {
                    code: "version_tombstoned".into(),
                    message: "blob is bound only to tombstoned versions".into(),
                    detail: Some(json!({"name":status["name"],"version":status["version"],"reason":status["reason"]})),
                });
            }
            encode_result(BlobStore::new(store.store_folder())?.read_chunk(&p.sha256, p.offset, p.length)?)
        })
        .await
    }
}
#[cfg(test)]
#[path = "catalog_ops_tests.rs"]
mod tests;

//! Operator trust uses authenticated pins, never a caller-supplied capability set.
use super::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    catalog_blob::{BlobStore, CatalogError},
    catalog_network::{Limits, RegistryClient},
    catalog_ops::PUBLISHER_TRUST,
    encode_result, HandlerError, ProjectsHandler, RouteKey,
};
use entorhinal_catalog::{archive, is_hex, names, record, sha256_hex};
use entorhinal_core::{
    catalog_grants::{Operation, PublisherGrant},
    RegistryStore,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeSet;
use tokio::time::{timeout_at, Instant};

#[derive(Clone, PartialEq, Eq)]
pub struct PublisherPin {
    pub name: String,
    pub version: String,
    pub publisher_fingerprint: String,
    pub manifest_sha256: String,
    pub archive_sha256: String,
}
/// Only a local pin implementation can supply the authenticated closure. Pins
/// and their publisher/digest bindings must not originate in request parameters.
pub trait PinSource: Send + Sync {
    fn pins(
        &self,
        store: &RegistryStore,
        agent: &str,
        publisher: &str,
    ) -> Result<Vec<PublisherPin>, CatalogError>;
}
struct UnavailablePins;
impl PinSource for UnavailablePins {
    fn pins(&self, _: &RegistryStore, _: &str, _: &str) -> Result<Vec<PublisherPin>, CatalogError> {
        Err(CatalogError::new(
            "manifest_unavailable",
            "authenticated pins are not available yet",
        ))
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TrustRequest {
    publisher_fingerprint: String,
    agent_id: String,
}
fn unavailable(pin: &PublisherPin) -> CatalogError {
    CatalogError {
        code: "manifest_unavailable",
        message: format!("manifest unavailable for {}@{}", pin.name, pin.version),
        detail: Some(json!({"name":pin.name,"version":pin.version})),
    }
}
async fn ceiling(
    store: &RegistryStore,
    pins: &[PublisherPin],
    publisher: &str,
    client: impl Fn(&str) -> Result<RegistryClient, CatalogError>,
) -> Result<Vec<String>, CatalogError> {
    if pins.is_empty() {
        return Err(CatalogError::new(
            "manifest_unavailable",
            "no authenticated pins from this publisher",
        ));
    }
    let mut tokens = BTreeSet::new();
    let blobs = BlobStore::new(store.store_folder())?;
    for pin in pins {
        let result = async {
            if pin.publisher_fingerprint != publisher || !is_hex(&pin.manifest_sha256, 64) {
                return Err(unavailable(pin));
            }
            let mapped = names::map_name(&pin.name).map_err(|_| unavailable(pin))?;
            if mapped.canonical != pin.name
                || mapped.scope == "cortexkit"
                || pin
                    .version
                    .parse::<entorhinal_catalog::semver::Version>()
                    .is_err()
            {
                return Err(unavailable(pin));
            }
            let manifest = match blobs.read_verified(&pin.archive_sha256) {
                Ok(bytes) => archive::read(&bytes)
                    .ok()
                    .and_then(|members| members.get("ck.json").map(|bytes| bytes.to_vec()))
                    .ok_or_else(|| unavailable(pin))?,
                Err(e) if e.code == "blob_unknown" => client(&format!("@{}", mapped.scope))?
                    .fetch_manifest(&format!(
                        "v1/packages/{}/{}/{}/ck.json",
                        mapped.scope, mapped.name, pin.version
                    ))
                    .await
                    .map_err(|_| unavailable(pin))?,
                Err(_) => return Err(unavailable(pin)),
            };
            if manifest.len() > 65_536 || sha256_hex(&manifest) != pin.manifest_sha256 {
                return Err(unavailable(pin));
            }
            let manifest = record::parse_manifest(&manifest).map_err(|_| unavailable(pin))?;
            if manifest.name != pin.name || manifest.version != pin.version {
                return Err(unavailable(pin));
            }
            Ok(manifest.requires.capabilities)
        }
        .await
        .map_err(|_| unavailable(pin))?;
        tokens.extend(result);
    }
    Ok(tokens.into_iter().collect())
}
impl ProjectsHandler {
    pub(super) async fn catalog_publisher_trust(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.catalog_publisher_trust_from(params, key, received, &UnavailablePins)
            .await
    }
    pub(crate) async fn catalog_publisher_trust_from(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
        source: &dyn PinSource,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit(PUBLISHER_TRUST, key)?;
        let p: TrustRequest = serde_json::from_value(params).map_err(super::invalid_params)?;
        if !is_hex(&p.publisher_fingerprint, 64) {
            return Err(HandlerError::new(
                "invalid_params",
                "publisher fingerprint must be 64 lower-case hex",
            ));
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let agent = store
            .agent_row(&p.agent_id)?
            .ok_or_else(|| HandlerError::new("unknown_agent", "unknown placement agent"))?;
        if agent.status != "live" {
            return Err(HandlerError::new("gone", "placement agent is not live"));
        }
        let pins = source.pins(&store, &p.agent_id, &p.publisher_fingerprint)?;
        let ceiling = ceiling(&store, &pins, &p.publisher_fingerprint, |scope| {
            RegistryClient::for_scope(&store, scope, Limits::MODULE)
        })
        .await?;
        let summary = format!(
            "Trust publisher {} for install at agent {}",
            p.publisher_fingerprint, p.agent_id
        );
        if summary.chars().count() > 200 {
            return Err(HandlerError::new(
                "invalid_params",
                "confirmation summary exceeds 200 characters",
            ));
        }
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(OperatorConfirmError::Declined.into());
        }
        let deadline = received + std::time::Duration::from_secs(60);
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline,self.operator_confirmer.confirm_operator(&summary,key)) => { result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??; }
        }
        let writer = tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline,self.writer.clone().lock_owned()) => result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))?,
        };
        if Instant::now() >= deadline || *guard.cancelled.borrow() {
            return Err(OperatorConfirmError::Declined.into());
        }
        let latest = source.pins(&store, &p.agent_id, &p.publisher_fingerprint)?;
        if latest != pins {
            return Err(HandlerError::new(
                "manifest_unavailable",
                "authenticated pins changed during confirmation",
            ));
        }
        let now = (self.clock)();
        let principal = json!(self.admission_for(key).principal);
        tokio::task::spawn_blocking(move || {
            let (_writer, _confirmation) = (writer, guard);
            let mut random = [0u8; 16];
            getrandom::getrandom(&mut random)
                .map_err(|e| HandlerError::new("store_error", e.to_string()))?;
            let grant = PublisherGrant {
                id: format!("pg_{}", entorhinal_catalog::hex(&random)),
                publisher_fingerprint: p.publisher_fingerprint,
                operation: Operation::Install,
                placement_agent_id: p.agent_id,
                ceiling,
                card_ref: None,
                granted_at_ms: now,
                revoked_at_ms: None,
            };
            let id = store.insert_publisher_grant(&grant, &principal)?;
            encode_result(json!({"outcome":"trusted","id":id,"operation":"install"}))
        })
        .await
        .map_err(|e| HandlerError::new("store_error", e.to_string()))?
    }
}
#[cfg(test)]
#[path = "catalog_grants_tests.rs"]
mod tests;

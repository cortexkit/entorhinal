//! Direct-only publishing: local checks, operator authorization, vault signing, durable retry.
use crate::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    HandlerError, ProjectsHandler, RouteKey,
};
use cortexkit_claustrum_client::{
    ClaustrumCredentialResolver, CredentialResolver, CredentialResolverError,
};
use entorhinal_catalog as catalog;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};
use tokio::time::{timeout_at, Instant};

mod pack;
mod storage;
#[cfg(test)]
mod tests;

pub const PUBLISH: &str = "catalog.publish";
const CREDENTIAL: &str = "signing:cortexkit-publish:1";

#[derive(Default)]
pub(crate) struct Publisher {
    serial: tokio::sync::Mutex<()>,
    #[cfg(test)]
    vault: Option<Arc<dyn CredentialResolver>>,
    #[cfg(test)]
    client: Option<Arc<crate::catalog_network::RegistryClient>>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    package_dir: String,
    registry: String,
    scope: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Signed {
    pub record: catalog::record::Record,
    pub canonical: String,
    pub signature: String,
    pub key_id: String,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PublishReply {
    record: catalog::record::Record,
    signature: String,
    status: StatusReply,
}
#[derive(Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StatusReply {
    record: catalog::status::StatusRecord,
    signature: String,
}
impl PublishReply {
    pub(crate) fn matches(&self, signed: &Signed) -> bool {
        let r = &self.status.record;
        serde_json::to_value(&self.record).ok() == serde_json::to_value(&signed.record).ok()
            && self.signature == signed.signature
            && r.schema == "ck-status/v1"
            && r.sequence == 1
            && !r.registry_key_id.is_empty()
            && r.published_at == r.changed_at
            && r.name == signed.record.name
            && r.version == signed.record.version
            && r.archive_sha256 == signed.record.archive_sha256
            && Some(r.publisher_key.clone()) == catalog::key_fingerprint(&signed.record.public_key)
            && r.state == catalog::status::State::Available
            && r.reason.is_none()
            && r.compromise_from.is_none()
            && crate::catalog_ops::whole_second(&r.published_at).is_ok()
            && crate::catalog_ops::whole_second(&r.changed_at).is_ok()
            && catalog::is_hex(&self.status.signature, 128)
    }
}
fn error(code: &str, message: impl Into<String>) -> HandlerError {
    HandlerError::new(code, message)
}
fn storage(e: impl std::fmt::Display) -> HandlerError {
    error("store_error", e.to_string())
}
fn refused(e: catalog::Refusal) -> HandlerError {
    HandlerError {
        code: e.code.as_str().into(),
        message: e.to_string(),
        detail: serde_json::to_value(e).ok(),
    }
}
fn vault_error(e: CredentialResolverError) -> HandlerError {
    use cortexkit_claustrum_client::CredentialErrorClass;
    let code = match e.class {
        CredentialErrorClass::Transient => "store_error",
        CredentialErrorClass::CredentialAbsent => "not_found",
        CredentialErrorClass::InvalidResponse => "invalid_reply",
        _ => e.code(),
    };
    let mut result = error(code, e.to_string());
    result.detail = Some(json!({"retryable": e.retryable()}));
    result
}
fn summary(checked: &catalog::publish::CheckedArchive) -> Result<String, HandlerError> {
    let kind = serde_json::to_value(checked.manifest.kind).map_err(storage)?;
    let summary = format!(
        "Publish {} {} {} SHA256 {}",
        checked.manifest.name,
        checked.manifest.version,
        kind.as_str().unwrap_or_default(),
        checked.archive_sha256
    );
    if summary.chars().count() > 200 {
        return Err(error(
            "invalid_params",
            "complete confirmation summary exceeds 200 characters",
        ));
    }
    Ok(summary)
}
fn canonical_record(record: &catalog::record::Record) -> Result<String, HandlerError> {
    catalog::jcs::to_string(&serde_json::to_value(record).map_err(storage)?)
        .map_err(|e| error("record_invalid", e.to_string()))
}
fn verify(
    signed: &Signed,
    checked: &catalog::publish::CheckedArchive,
    public: &cortexkit_claustrum_client::CredentialPublicKey,
) -> Result<(), HandlerError> {
    let expected = canonical_record(&checked.record(&public.public_key_hex))?;
    if public.algorithm != "ed25519"
        || !catalog::is_hex(&public.public_key_hex, 64)
        || !catalog::is_hex(&public.key_id, 16)
        || signed.key_id != public.key_id
        || signed.canonical != expected
        || canonical_record(&signed.record)? != expected
        || !catalog::verify_signature(
            &public.public_key_hex,
            "ck-record/v1",
            &signed.canonical,
            &signed.signature,
        )
    {
        return Err(error(
            "signature_invalid",
            "vault signature or key does not match the checked record",
        ));
    }
    Ok(())
}

impl ProjectsHandler {
    pub(super) async fn catalog_publish(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit(PUBLISH, key)?;
        let deadline = received + std::time::Duration::from_secs(60);
        let request: Request = serde_json::from_value(params).map_err(crate::invalid_params)?;
        if !Path::new(&request.package_dir).is_absolute() {
            return Err(error(
                "not_canonical",
                "package_dir must be an absolute canonical directory",
            ));
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let package_dir = request.package_dir.clone();
        let scope = request.scope.clone();
        let sql_guard = self.catalog_sql.begin();
        let (canonical, archive, checked, folder, registries) =
            crate::catalog_sql::until(deadline, "catalog publish preparation", move || {
                let _sql_guard = sql_guard;
                let canonical =
                    entorhinal_core::RegistryStore::canonical_mutation_root(&package_dir)
                        .map_err(|e| error("path_refused", e.to_string()))?;
                if canonical != package_dir {
                    return Err(error("not_canonical", "package_dir must be canonical"));
                }
                let archive = pack::pack(Path::new(&canonical))?;
                let checked = catalog::publish::check(&archive, &scope).map_err(refused)?;
                let registries = store.catalog_registries()?;
                Ok((
                    canonical,
                    archive,
                    checked,
                    store.store_folder().to_path_buf(),
                    registries,
                ))
            })
            .await?;
        let summary = summary(&checked)?;
        let registry = crate::catalog_network::registry_url(&request.registry)?;
        let registry = registry.as_str().trim_end_matches('/').to_string();
        let allowlist = registries.iter().map(|r| r.url.clone()).collect::<Vec<_>>();
        let client = Arc::new(crate::catalog_network::RegistryClient::new(
            &registry,
            &allowlist,
            crate::catalog_network::Limits::MODULE,
        )?);
        let keys = registries
            .iter()
            .find(|r| r.url == registry)
            .ok_or_else(|| error("registry_not_allowlisted", "registry is not configured"))?
            .keys
            .iter()
            .map(|k| catalog::status::StatusKey {
                key_id: k.key_id.clone(),
                public_key: k.public_key.clone(),
                valid_from: k.valid_from.clone(),
            })
            .collect::<Vec<_>>();
        #[cfg(test)]
        let client = self.catalog_publish.client.clone().unwrap_or(client);
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(OperatorConfirmError::Declined.into());
        }
        let _serial = tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline, self.catalog_publish.serial.lock()) => result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))?,
        };
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline, self.operator_confirmer.confirm_operator(&summary, key)) => {
                result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
            }
        }
        if *guard.cancelled.borrow() || Instant::now() >= deadline {
            return Err(OperatorConfirmError::Declined.into());
        }
        let vault = self.catalog_publish.resolver(Path::new(&canonical))?;
        let signed = tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline, prepare(vault.as_ref(), &folder, &registry, &request.scope, &checked, &self.catalog_sql)) => result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??,
        };
        // The authorization guard covers signing and durable storage; upload can be retried.
        let reply = client.publish(&signed, &archive).await.map_err(|e| {
            let message = match &e {
                crate::catalog_network::NetworkError::Failed(error) => error.message.clone(),
                crate::catalog_network::NetworkError::Retryable { retry_after, .. } => {
                    format!("{e}: retry after {} seconds", retry_after.delay.as_secs())
                }
                _ => e.to_string(),
            };
            HandlerError {
                code: e.code().into(),
                message: format!("{message}; repeat the identical publish to retry"),
                detail: e.detail().cloned(),
            }
        })?;
        let status =
            catalog::jcs::to_string(&serde_json::to_value(&reply.status.record).map_err(storage)?)
                .map_err(|e| error("invalid_reply", e.to_string()))?;
        let pin = catalog::status::Pin {
            name: signed.record.name.clone(),
            version: signed.record.version.clone(),
            archive_sha256: signed.record.archive_sha256.clone(),
        };
        catalog::status::apply_status(None, &status, &reply.status.signature, &keys, &pin)
            .map_err(|e| {
                error(
                    e.code.as_str(),
                    format!("publish outcome unknown: unauthenticated registry status: {e}"),
                )
            })?;
        crate::encode_result(reply)
    }
}
impl Publisher {
    fn resolver(&self, root: &Path) -> Result<Arc<dyn CredentialResolver>, HandlerError> {
        #[cfg(test)]
        if let Some(vault) = &self.vault {
            return Ok(vault.clone());
        }
        let connection = crate::log_client::subc_connection_file(std::env::args_os().skip(1))
            .ok_or_else(|| error("not_found", "supervised vault connection unavailable"))?;
        Ok(Arc::new(ClaustrumCredentialResolver::new(
            connection,
            root.to_path_buf(),
            crate::MODULE_ID,
        )))
    }
}
async fn prepare(
    vault: &dyn CredentialResolver,
    folder: &Path,
    registry: &str,
    scope: &str,
    checked: &catalog::publish::CheckedArchive,
    sql: &Arc<crate::move_driver::SqlOperations>,
) -> Result<Signed, HandlerError> {
    let public = vault.public_key(CREDENTIAL).await.map_err(vault_error)?;
    let path = storage::path(folder, registry, scope, &checked.archive_sha256);
    let held_path = path.clone();
    let guard = sql.begin();
    if let Some(signed) = crate::catalog_sql::run(move || {
        let _guard = guard;
        storage::load(&held_path)
    })
    .await?
    {
        verify(&signed, checked, &public)?;
        return Ok(signed);
    }
    if public.algorithm != "ed25519"
        || !catalog::is_hex(&public.public_key_hex, 64)
        || !catalog::is_hex(&public.key_id, 16)
    {
        return Err(error(
            "signature_invalid",
            "vault returned invalid public key",
        ));
    }
    let record = checked.record(&public.public_key_hex);
    let canonical = canonical_record(&record)?;
    let input = catalog::signing_input("ck-record/v1", &canonical);
    let signature = vault.sign(CREDENTIAL, &input).await.map_err(vault_error)?;
    let signed = Signed {
        record,
        canonical,
        signature: signature.signature_hex,
        key_id: signature.key_id,
    };
    verify(&signed, checked, &public)?;
    let saved = signed.clone();
    let guard = sql.begin();
    crate::catalog_sql::run(move || {
        let _guard = guard;
        storage::save(&path, &saved)
    })
    .await?;
    Ok(signed)
}

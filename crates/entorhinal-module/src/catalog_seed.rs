//! Starter loading grants no authority: only verified content and initial status
//! belong here. Installation and agent creation remain separate operator actions.
use crate::{
    catalog_blob::{BlobStore, CatalogError},
    catalog_trust::CORTEXKIT_ROOT,
    encode_result,
    move_driver::SqlOperations,
    HandlerError, ProjectsHandler, RouteKey,
};
use entorhinal_catalog::{archive::ARCHIVE_MAX, bundle};
use entorhinal_core::RegistryStore;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    path::Path,
};

pub const SEED: &str = "catalog.seed";
const ATTEMPT: &str = "starter-bundle.attempt.json";

impl From<bundle::BundleError> for CatalogError {
    fn from(error: bundle::BundleError) -> Self {
        Self {
            code: error.code().as_str(),
            message: error.to_string(),
            detail: serde_json::to_value(error).ok(),
        }
    }
}

#[derive(Clone)]
pub(super) struct Seeder {
    root: Option<String>,
    #[cfg(test)]
    before_seed: Option<std::sync::Arc<dyn Fn() + Send + Sync>>,
}
impl Default for Seeder {
    fn default() -> Self {
        Self {
            root: CORTEXKIT_ROOT.map(str::to_owned),
            #[cfg(test)]
            before_seed: None,
        }
    }
}
impl Seeder {
    #[cfg(test)]
    fn with_test_root(root: String) -> Self {
        Self {
            root: Some(root),
            ..Default::default()
        }
    }

    fn seed(&self, store: &RegistryStore, path: &Path) -> Result<Value, CatalogError> {
        #[cfg(test)]
        if let Some(before) = &self.before_seed {
            before();
        }
        let root = self.root.as_deref().ok_or_else(|| {
            CatalogError::new(
                "cortexkit_root_unavailable",
                "the production CortexKit root has not been provisioned",
            )
        })?;
        if !fs::metadata(path)?.is_file() {
            return Err(CatalogError::new(
                "bundle_file_invalid",
                "bundle must be a regular file",
            ));
        }
        let file = fs::File::open(path)?;
        if !file.metadata()?.is_file() {
            return Err(CatalogError::new(
                "bundle_file_invalid",
                "bundle must be a regular file",
            ));
        }
        let mut bytes = Vec::new();
        file.take(ARCHIVE_MAX as u64 + 1).read_to_end(&mut bytes)?;
        let verified = bundle::verify(&bytes, root)?;
        seed_statuses(store, &verified, root)?;
        Ok(
            json!({"seeded": verified.packages.len(), "bundle_sha256": entorhinal_catalog::sha256_hex(&bytes)}),
        )
    }
}

fn seed_statuses(
    store: &RegistryStore,
    bundle: &bundle::VerifiedBundle<'_>,
    root: &str,
) -> Result<(), CatalogError> {
    use entorhinal_catalog::status::{self, Apply};
    use entorhinal_core::catalog_trust::CatalogStatus;
    store.catalog_seed_transaction::<_, CatalogError>(|rows, registries| {
        let mut writes = Vec::new();
        for package in &bundle.packages {
            let next = package.status.record();
            let held = rows
                .iter()
                .find(|r| r.name == next.name && r.version == next.version)
                .map(|r| {
                    let registry = registries
                        .iter()
                        .find(|registry| registry.url == r.registry_url);
                    crate::catalog_trust::restore(r, registry, Some(root))
                })
                .transpose()?;
            if let Some(prior) = &held {
                let prior = prior.record();
                for (field, old, new) in [
                    (
                        "archive_sha256",
                        &prior.archive_sha256,
                        &next.archive_sha256,
                    ),
                    ("publisher_key", &prior.publisher_key, &next.publisher_key),
                    ("published_at", &prior.published_at, &next.published_at),
                ] {
                    if old != new {
                        return Err(entorhinal_catalog::Refusal::field(
                            entorhinal_catalog::RefusalCode::RecordMismatch,
                            field,
                        )
                        .into());
                    }
                }
            }
            // Bundles initialize status only. An existing hold, especially a
            // registry-signed revocation, cannot acquire bundle-chosen sequences.
            if held.is_none() && status::compare(None, &package.status)? == Apply::Applied {
                writes.push(CatalogStatus {
                    name: next.name.clone(),
                    version: next.version.clone(),
                    registry_url: String::new(),
                    sequence: next.sequence,
                    changed_at: next.changed_at.clone(),
                    published_at: next.published_at.clone(),
                    signer_index: 0,
                    canonical: package.status.canonical().into(),
                    signature: package.status_signature.clone(),
                    starter_proof: Some(
                        serde_json::to_string(&package.proof)
                            .map_err(|e| CatalogError::new("store_error", e.to_string()))?,
                    ),
                });
            }
        }
        // Fence the final trust checks and publication against concurrent feeds.
        // A refused container never reaches the content-addressed store.
        let blobs = BlobStore::new(store.store_folder())?;
        for package in &bundle.packages {
            blobs.publish(package.archive)?;
            blobs.publish(package.manifest)?;
        }
        Ok(((), writes))
    })
}

impl ProjectsHandler {
    pub(super) async fn catalog_seed(
        &self,
        params: Value,
        key: RouteKey,
        received: tokio::time::Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit(SEED, key)?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            bundle_file: String,
        }
        let request: Request = serde_json::from_value(params).map_err(crate::invalid_params)?;
        let path = std::path::PathBuf::from(&request.bundle_file);
        if !path.is_absolute() {
            return Err(HandlerError::new(
                "invalid_params",
                "bundle_file must be absolute",
            ));
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let seeder = self.catalog_seeder.clone();
        // Counted like the startup seed: after a reply times out, the job keeps
        // writing package files and holds a store handle until it ends.
        let guard = self.catalog_sql.begin();
        let value = crate::catalog_sql::until(
            received + std::time::Duration::from_secs(110),
            "catalog.seed",
            move || {
                let _guard = guard;
                seeder.seed(&store, &path).map_err(HandlerError::from)
            },
        )
        .await?;
        encode_result(value)
    }
}

fn empty_content(folder: &Path) -> Result<bool, CatalogError> {
    match fs::read_dir(folder.join("catalog/blobs")) {
        Ok(mut entries) => Ok(entries.next().transpose()?.is_none()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(true),
        Err(e) => Err(e.into()),
    }
}

fn automatic(store: &RegistryStore, seeder: &Seeder) -> Result<Option<Value>, CatalogError> {
    let catalog = store.store_folder().join("catalog");
    let path = catalog.join("starter-bundle.tar");
    if !path.try_exists()? || !empty_content(store.store_folder())? {
        return Ok(None);
    }
    fs::create_dir_all(&catalog)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&catalog, fs::Permissions::from_mode(0o700))?;
    }
    let mut options = fs::OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut attempt = match options.open(catalog.join(ATTEMPT)) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    // The attempt file records that loading started, preventing automatic retries
    // after refusal, interruption or restart.
    attempt.write_all(b"{\"outcome\":\"seed_started\"}\n")?;
    attempt.sync_all()?;
    #[cfg(unix)]
    fs::File::open(&catalog)?.sync_all()?;
    let result = seeder.seed(store, &path);
    let outcome = match &result {
        Ok(value) => json!({"outcome":"seeded", "result":value}),
        Err(error) => json!({"outcome":error.code, "detail":error.detail}),
    };
    use std::io::Seek;
    attempt.rewind()?;
    attempt.set_len(0)?;
    attempt.write_all(
        &serde_json::to_vec(&outcome).map_err(|e| CatalogError::new("blob_io", e.to_string()))?,
    )?;
    attempt.sync_all()?;
    result.map(Some)
}

/// Scheduling performs no file I/O on the startup or health task. The job is
/// counted in `sql` because it holds a store handle on the blocking pool until
/// it ends; the catalog's stop path waits for that count to reach zero.
pub(super) fn start(store: RegistryStore, sql: &std::sync::Arc<SqlOperations>) {
    schedule(store, Seeder::default(), sql);
}
fn schedule(
    store: RegistryStore,
    seeder: Seeder,
    sql: &std::sync::Arc<SqlOperations>,
) -> Option<tokio::task::JoinHandle<()>> {
    if let Ok(runtime) = tokio::runtime::Handle::try_current() {
        let guard = sql.begin();
        Some(runtime.spawn_blocking(move || {
            let _guard = guard;
            match automatic(&store, &seeder) {
                Ok(Some(_)) => {
                    tracing::info!(target: "catalog", outcome="seeded", "starter bundle")
                }
                Ok(None) => {}
                Err(error) => {
                    tracing::warn!(target: "catalog", outcome=error.code, "starter bundle")
                }
            }
        }))
    } else {
        None
    }
}

#[cfg(test)]
#[path = "catalog_seed_tests.rs"]
mod tests;

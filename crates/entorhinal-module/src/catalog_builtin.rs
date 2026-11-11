//! Startup supplies an ordinary registry row; verification has no built-in bypass.
use crate::{catalog_trust::CORTEXKIT_REGISTRY, move_driver::SqlOperations};
use entorhinal_core::RegistryStore;
use std::sync::Arc;

/// The writer may be busy or fail independently of project identity. Run off the
/// HELLO/health path and retry only at the next start, without withholding reads.
pub(super) fn start(
    store: RegistryStore,
    sql: &Arc<SqlOperations>,
) -> Option<tokio::task::JoinHandle<()>> {
    if store.is_foreign() {
        return None;
    }
    let runtime = tokio::runtime::Handle::try_current().ok()?;
    Some(runtime.spawn_blocking(sql.tracked(move || {
        let registry = CORTEXKIT_REGISTRY;
        if let Err(error) = store.catalog_registry_bootstrap(
            registry.url,
            registry.scope,
            registry.key_id,
            registry.public_key,
            crate::unix_millis(),
        ) {
            let error = crate::catalog_blob::CatalogError::from(error);
            let scopes = store
                .catalog_registries()
                .unwrap_or_default()
                .into_iter()
                .find(|row| row.url == registry.url)
                .map(|row| row.scopes)
                .unwrap_or_default();
            tracing::warn!(
                target: "catalog",
                code = error.code,
                url = registry.url,
                ?scopes,
                "built-in registry unavailable; preserving operator configuration and retrying at next startup"
            );
        }
    })))
}

#[cfg(test)]
#[path = "catalog_builtin_tests.rs"]
mod tests;

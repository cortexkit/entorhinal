//! Machine-local status refresh for scopes referenced by agent pins.
use crate::{
    catalog_agent, catalog_trust::CatalogTrust, move_driver::SqlOperations, HealthGauges,
    ProjectsHandler,
};
use entorhinal_core::RegistryStore;
use std::sync::{
    atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
use std::time::Duration;

const HOUR_MS: u64 = 3_600_000;
const ERRORS: &[&str] = &[
    "catalog_poll_failed",
    "store_unavailable",
    "storage_error",
    "store_error",
    "registry_unknown",
    "registry_changed",
    "registry_url_invalid",
    "registry_not_allowlisted",
    "invalid_reply",
    "statement_invalid",
    "key_conflict",
    "key_not_valid",
    "status_terminal",
    "json_too_large",
    "registry_transport",
    "registry_redirect_invalid",
    "registry_redirect_limit",
    "registry_http_error",
    "publish_unavailable",
    "status_key_unavailable",
    "status_unavailable",
    "feed_rollback",
    "feed_after_unavailable",
    "status_invalid",
    "signature_invalid",
    "key_unknown",
    "record_mismatch",
    "scope_unmapped",
    "registry_timeout",
];
#[derive(Default)]
pub(crate) struct Gauges {
    last_success: AtomicI64,
    succeeded: AtomicBool,
    last_error: AtomicU64,
    failures: AtomicU64,
    scopes: AtomicU64,
    #[cfg(test)]
    completed_passes: AtomicU64,
    #[cfg(test)]
    completed: Mutex<Option<tokio::sync::mpsc::UnboundedSender<CompletedPass>>>,
}
#[cfg(test)]
#[derive(Debug)]
struct CompletedPass {
    number: u64,
    result: Result<(), &'static str>,
    gauges: serde_json::Value,
}
impl Gauges {
    #[cfg(test)]
    fn completions(&self) -> tokio::sync::mpsc::UnboundedReceiver<CompletedPass> {
        let (sender, receiver) = tokio::sync::mpsc::unbounded_channel();
        assert!(self.completed.lock().unwrap().replace(sender).is_none());
        receiver
    }
    pub(crate) fn report(&self) -> serde_json::Value {
        let succeeded = self.succeeded.load(Ordering::Acquire);
        let success = self.last_success.load(Ordering::Relaxed);
        let error = self.last_error.load(Ordering::Relaxed);
        serde_json::json!({
            "catalogLastSuccessMs": succeeded.then_some(success),
            "catalogLastError": error.checked_sub(1).map(|i| ERRORS[i as usize]),
            "catalogConsecutiveFailures": self.failures.load(Ordering::Relaxed),
            "catalogScopesPolled": self.scopes.load(Ordering::Relaxed)
        })
    }
    fn finish(&self, result: Result<(), &'static str>, now: i64) -> u64 {
        let failures = match result {
            Ok(()) => {
                // Unix millisecond zero is a valid injected clock value.
                self.last_success.store(now, Ordering::Relaxed);
                self.succeeded.store(true, Ordering::Release);
                self.last_error.store(0, Ordering::Relaxed);
                self.failures.store(0, Ordering::Relaxed);
                0
            }
            Err(code) => {
                let index = ERRORS.iter().position(|c| *c == code).unwrap_or(0);
                self.last_error.store(index as u64 + 1, Ordering::Relaxed);
                self.failures
                    .fetch_add(1, Ordering::Relaxed)
                    .saturating_add(1)
            }
        };
        #[cfg(test)]
        {
            let number = self.completed_passes.fetch_add(1, Ordering::Relaxed) + 1;
            if let Some(sender) = self.completed.lock().unwrap().as_ref() {
                let _ = sender.send(CompletedPass {
                    number,
                    result,
                    gauges: self.report(),
                });
            }
        }
        failures
    }
}
fn delay(now: i64, offset: u64, failures: u64) -> Duration {
    let ms = if failures == 0 {
        HOUR_MS - (now.rem_euclid(HOUR_MS as i64) as u64 + HOUR_MS - offset) % HOUR_MS
    } else {
        HOUR_MS * (1u64 << failures.min(3)).min(6)
    };
    Duration::from_millis(ms)
}
struct Worker {
    store: Weak<Mutex<Option<RegistryStore>>>,
    sources: Arc<catalog_agent::TrustFactory>,
    health: Arc<HealthGauges>,
    clock: fn() -> i64,
    wake: Arc<tokio::sync::Notify>,
    /// Counts this worker's blocking SQL jobs. Aborting the poll task does not
    /// stop a job already on the blocking pool, and that job holds a store
    /// handle, so stopping waits for the count to reach zero.
    sql: Arc<SqlOperations>,
}
impl Worker {
    fn store(&self) -> Option<RegistryStore> {
        self.store
            .upgrade()?
            .lock()
            .unwrap()
            .clone()
            .filter(|s| !s.is_foreign())
    }
    async fn offset(&self) -> Result<u64, &'static str> {
        let mut bytes = [0; 8];
        getrandom::getrandom(&mut bytes).map_err(|_| "catalog_poll_failed")?;
        let store = self.store().ok_or("store_unavailable")?;
        let guard = self.sql.begin();
        crate::catalog_sql::run(move || {
            let _guard = guard;
            store
                .catalog_poll_offset(u64::from_le_bytes(bytes))
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        })
        .await
        .map_err(|e| e.code)
    }
    async fn pass(&self) -> Result<(), &'static str> {
        let store = self.store().ok_or("store_unavailable")?;
        let guard = self.sql.begin();
        let scopes = crate::catalog_sql::run(move || {
            let _guard = guard;
            store
                .catalog_pinned_scopes()
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        })
        .await
        .map_err(|e| e.code)?;
        let mut failure = None;
        for scope in scopes {
            self.health.catalog.scopes.fetch_add(1, Ordering::Relaxed);
            let trust = match self.store() {
                Some(store) => {
                    let sources = self.sources.clone();
                    let guard = self.sql.begin();
                    crate::catalog_sql::run(move || {
                        let _guard = guard;
                        sources(store, &scope)
                    })
                    .await
                    .map_err(|e| e.code)
                }
                None => Err("store_unavailable"),
            };
            let result = match trust {
                Ok(mut trust) => {
                    trust.use_weak_store(self.store.clone());
                    poll(&mut trust).await
                }
                Err(e) => Err(e),
            };
            if let Err(error) = result {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    async fn run(self) {
        let mut offset = None;
        loop {
            if self.store.upgrade().is_none() {
                return;
            }
            if self.store().is_none() {
                self.wake.notified().await;
                continue;
            }
            let result = match offset {
                Some(_) => self.pass().await,
                None => match self.offset().await {
                    Ok(value) => {
                        offset = Some(value);
                        self.pass().await
                    }
                    Err(e) => Err(e),
                },
            };
            let failures = self.health.catalog.finish(result, (self.clock)());
            tokio::time::sleep(delay((self.clock)(), offset.unwrap_or(0), failures)).await;
        }
    }
}
async fn poll(trust: &mut CatalogTrust) -> Result<(), &'static str> {
    let applied = trust.apply_scope_feed().await.map_err(|e| e.code())?;
    if let Some(e) = applied.refusal {
        return Err(e.code);
    }
    for (_, result) in applied.results {
        result.map_err(|e| e.code)?;
    }
    Ok(())
}
impl ProjectsHandler {
    #[cfg(test)]
    pub(super) async fn stop_catalog_poll(&self) {
        let task = self.catalog_poll_task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        // Aborting the task leaves its blocking jobs, and the startup seed's,
        // still running with a store handle; wait until they have all ended.
        self.catalog_sql.wait().await;
    }
    pub(super) fn start_catalog_poll(&self) {
        if self.health.origin.is_foreign() {
            return;
        }
        let mut task = self.catalog_poll_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            sources: self.catalog_agent.sources.clone(),
            health: self.health.clone(),
            clock: self.clock,
            wake: self.catalog_poll_wake.clone(),
            sql: self.catalog_sql.clone(),
        };
        *task = Some(tokio::spawn(worker.run()));
    }
}

#[cfg(test)]
#[path = "catalog_poll_tests.rs"]
mod tests;

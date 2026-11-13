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
const FAST_MS: u64 = 600_000;
fn fast_delay(failures: u64) -> Duration {
    Duration::from_millis(if failures == 0 {
        FAST_MS
    } else {
        (60_000 * (1u64 << failures.saturating_sub(1).min(4))).min(FAST_MS)
    })
}
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
    watches: Arc<crate::watches::Watches>,
    freshness_wake: Arc<tokio::sync::Notify>,
    snapshots: tokio::sync::Mutex<()>,
}
impl Worker {
    async fn publish_statuses(&self) -> Result<(), &'static str> {
        // The deadline and network lanes may finish together. Serialize their
        // snapshots so an older read cannot overwrite a newer watch observation.
        let _snapshot = self.snapshots.lock().await;
        let store = self.store().ok_or("store_unavailable")?;
        let statuses = crate::catalog_sql::run(self.sql.tracked(move || {
            store
                .catalog_served_statuses()
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        }))
        .await
        .map_err(|e| e.code)?;
        self.watches.observe_catalog(statuses);
        Ok(())
    }
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
        crate::catalog_sql::run(self.sql.tracked(move || {
            store
                .catalog_poll_offset(u64::from_le_bytes(bytes))
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        }))
        .await
        .map_err(|e| e.code)
    }
    async fn pass(&self) -> Result<(), &'static str> {
        let store = self.store().ok_or("store_unavailable")?;
        let scopes = crate::catalog_sql::run(self.sql.tracked(move || {
            store
                .catalog_pinned_scopes()
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        }))
        .await
        .map_err(|e| e.code)?;
        self.scopes(scopes).await
    }
    async fn fast_scopes(&self) -> Result<Vec<String>, &'static str> {
        let store = self.store().ok_or("store_unavailable")?;
        let now = (self.clock)();
        crate::catalog_sql::run(self.sql.tracked(move || {
            store.catalog_advance_freshness(now).map_err(|e| {
                crate::catalog_blob::CatalogError::new("storage_error", e.to_string())
            })?;
            let pins = store.catalog_live_flow_pins().map_err(|e| {
                crate::catalog_blob::CatalogError::new("storage_error", e.to_string())
            })?;
            let blobs = crate::catalog_blob::BlobStore::new(store.store_folder())?;
            let mut fast = Vec::new();
            for pin in pins {
                let manifest = entorhinal_catalog::record::parse_manifest(
                    &blobs.read_verified(&pin.manifest_sha256)?,
                )
                .map_err(|e| {
                    crate::catalog_blob::CatalogError::new("invalid_reply", format!("{e:?}"))
                })?;
                if manifest.kind == entorhinal_catalog::Kind::Flow
                    && manifest.name == pin.name
                    && manifest.version == pin.version
                    && !manifest.requires.capabilities.is_empty()
                {
                    fast.push(pin);
                }
            }
            store.catalog_sync_fast_lane(&fast, now).map_err(|e| {
                crate::catalog_blob::CatalogError::new("storage_error", e.to_string())
            })?;
            Ok::<_, crate::catalog_blob::CatalogError>(
                fast.iter()
                    .filter_map(|p| {
                        entorhinal_catalog::names::map_name(&p.name)
                            .ok()
                            .map(|n| format!("@{}", n.scope))
                    })
                    .collect::<std::collections::BTreeSet<_>>()
                    .into_iter()
                    .collect(),
            )
        }))
        .await
        .map_err(|e| e.code)
    }
    async fn scopes(&self, scopes: Vec<String>) -> Result<(), &'static str> {
        let mut failure = None;
        for scope in scopes {
            self.health.catalog.scopes.fetch_add(1, Ordering::Relaxed);
            let trust = match self.store() {
                Some(store) => {
                    let sources = self.sources.clone();
                    crate::catalog_sql::run(self.sql.tracked(move || sources(store, &scope)))
                        .await
                        .map_err(|e| e.code)
                }
                None => Err("store_unavailable"),
            };
            let result = match trust {
                Ok(mut trust) => {
                    trust.use_weak_store(self.store.clone());
                    trust.sql = self.sql.clone();
                    trust.clock = self.clock;
                    poll(&mut trust).await
                }
                Err(e) => Err(e),
            };
            if let Err(error) = result {
                failure = Some(error);
            }
            if let Err(error) = self.publish_statuses().await {
                failure = Some(error);
            }
        }
        failure.map_or(Ok(()), Err)
    }
    fn listen(&self) {
        if let Some(store) = self.store() {
            let wake = self.freshness_wake.clone();
            store.set_catalog_change_listener(Arc::new(move || wake.notify_one()));
        }
    }
    async fn next_freshness_deadline(&self) -> Result<Option<i64>, &'static str> {
        let store = self.store().ok_or("store_unavailable")?;
        crate::catalog_sql::run(self.sql.tracked(move || {
            store
                .catalog_next_freshness_deadline()
                .map_err(|e| crate::catalog_blob::CatalogError::new("storage_error", e.to_string()))
        }))
        .await
        .map_err(|e| e.code)
    }
    async fn run_freshness(&self) {
        loop {
            let notified = self.freshness_wake.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.store.upgrade().is_none() {
                return;
            }
            if self.store().is_none() {
                notified.await;
                continue;
            }
            self.listen();
            if let Err(code) = self.fast_scopes().await {
                tracing::warn!(
                    code,
                    "catalog freshness membership refresh failed; retaining known lanes"
                );
            }
            if let Err(code) = self.publish_statuses().await {
                tracing::warn!(code, "catalog freshness watch snapshot failed");
            }
            match self.next_freshness_deadline().await {
                Ok(Some(deadline)) => {
                    let delay = Duration::from_millis(
                        deadline.saturating_sub((self.clock)()).max(0) as u64,
                    );
                    tokio::select! { _ = notified => {}, _ = tokio::time::sleep(delay) => {} }
                }
                Ok(None) => notified.await,
                Err(code) => {
                    tracing::warn!(code, "catalog freshness deadline read failed");
                    tokio::select! { _ = notified => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
                }
            }
        }
    }
    async fn run(self) {
        // Both futures belong to the one supervised task. Aborting it stops both
        // lanes; stop_catalog_poll then awaits their tracked blocking SQL jobs.
        tokio::join!(self.run_polls(), self.run_freshness());
    }
    async fn run_polls(&self) {
        let mut offset = None;
        let mut hourly = tokio::time::Instant::now();
        let mut fast = tokio::time::Instant::now();
        let mut fast_failures: u64 = 0;
        loop {
            if self.store.upgrade().is_none() {
                return;
            }
            if self.store().is_none() {
                self.wake.notified().await;
                continue;
            }
            self.listen();
            self.freshness_wake.notify_one();
            let fast_scopes = match self.fast_scopes().await {
                Ok(scopes) => Ok(scopes),
                Err(code) => {
                    tracing::warn!(
                        code,
                        "catalog fast-lane manifest discovery failed; retaining known lanes"
                    );
                    match self.store() {
                        Some(store) => crate::catalog_sql::run(self.sql.tracked(move || {
                            store.catalog_fast_scopes().map_err(|e| {
                                crate::catalog_blob::CatalogError::new(
                                    "storage_error",
                                    e.to_string(),
                                )
                            })
                        }))
                        .await
                        .map_err(|e| e.code),
                        None => Err("store_unavailable"),
                    }
                }
            };
            if let Err(code) = self.publish_statuses().await {
                tracing::warn!(code, "catalog status watch snapshot failed");
            }
            let now = tokio::time::Instant::now();
            if now >= hourly {
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
                hourly = tokio::time::Instant::now()
                    + delay((self.clock)(), offset.unwrap_or(0), failures);
                if fast_scopes.as_ref().is_ok_and(|s| !s.is_empty()) {
                    fast_failures = if result.is_ok() {
                        0
                    } else {
                        fast_failures.saturating_add(1)
                    };
                    fast = tokio::time::Instant::now() + fast_delay(fast_failures);
                }
            } else if now >= fast {
                if let Ok(scopes) = fast_scopes {
                    if !scopes.is_empty() {
                        let result = self.scopes(scopes).await;
                        fast_failures = if result.is_ok() {
                            0
                        } else {
                            fast_failures.saturating_add(1)
                        };
                        self.health.catalog.finish(result, (self.clock)());
                        fast = tokio::time::Instant::now() + fast_delay(fast_failures);
                    }
                }
            }
            // Minute ticks discover registry scopes from flows newly pinned by
            // live agents, so network polling includes newly installed packages.
            // Freshness has its own exact-deadline timer. Hourly retries retain
            // their independent monotonic deadline and six-hour cap.
            let tick = tokio::time::Instant::now() + Duration::from_secs(60);
            let fast_due = if fast > tokio::time::Instant::now() {
                fast
            } else {
                tick
            };
            tokio::time::sleep_until(hourly.min(fast_due).min(tick)).await;
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
            watches: self.watches.clone(),
            freshness_wake: Arc::default(),
            snapshots: tokio::sync::Mutex::new(()),
        };
        *task = Some(tokio::spawn(worker.run()));
    }
}

#[cfg(test)]
#[path = "catalog_poll_tests.rs"]
mod tests;

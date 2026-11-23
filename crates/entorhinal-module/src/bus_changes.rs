//! Revision hints and acknowledged placement facts on the same bus connection.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use async_trait::async_trait;
use cortexkit_bus_client::{BusClient, BusClientOptions, Error};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::NatsRegister;
use cortexkit_bus_trait::Register;
use entorhinal_core::{placement::PlacementFact, user_profile::UserGetRequest, RegistryStore};
use serde::Serialize;
use tokio::sync::{watch, Notify};

use super::{move_driver::SqlOperations, HealthGauges, Ordering, ProjectsHandler};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
// Probe occasionally in case the daemon's catalog change notice was missed.
const CATALOG_WAIT_CAP: Duration = Duration::from_secs(15 * 60);
type Values = BTreeMap<&'static str, Vec<u8>>;

pub(super) fn ckbus_not_expected_back(error: &Error) -> bool {
    matches!(
        error,
        Error::RouteRefused {
            expected_back: Some(false),
            ..
        }
    )
}

pub(super) async fn wait_for_catalog(changes: &mut Option<watch::Receiver<Option<u64>>>) {
    let _ = tokio::time::timeout(CATALOG_WAIT_CAP, async {
        if let Some(changes) = changes {
            if changes.changed().await.is_ok() {
                return;
            }
        }
        std::future::pending().await
    })
    .await;
}

#[derive(Serialize)]
struct Generation<'a> {
    generation: i64,
    incarnation: &'a str,
}

#[derive(Serialize)]
struct Profile<'a> {
    token: &'a str,
}

#[derive(Serialize)]
struct Role {
    epoch: i64,
}

#[derive(Serialize)]
struct Catalog {
    generation: u64,
}

#[derive(Debug)]
pub(super) struct Failure {
    message: String,
    not_expected_back: bool,
}

impl Failure {
    fn connect(error: Error) -> Self {
        Self {
            not_expected_back: ckbus_not_expected_back(&error),
            message: error.to_string(),
        }
    }
}

impl From<String> for Failure {
    fn from(message: String) -> Self {
        Self {
            message,
            not_expected_back: false,
        }
    }
}

// This boundary keeps tests independent of an external NATS server. The real
// implementation only opens a provisioned register; it never creates a bucket.
#[async_trait]
pub(super) trait Sink: Send {
    async fn ready(&mut self) -> Result<(), Failure>;
    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), Failure>;
    async fn publish(&mut self, fact_id: &str, body: Vec<u8>) -> Result<(), Failure>;
    async fn connection_changed(&mut self);
}

#[derive(Default)]
struct BusSink {
    connector: super::log_client::SubcConnector,
    client: Option<BusClient>,
    changes: Option<watch::Receiver<u64>>,
    register: Option<NatsRegister>,
}

impl BusSink {
    async fn client(&mut self) -> Result<&BusClient, Failure> {
        self.client_for(super::MODULE_ID).await
    }

    async fn client_for(&mut self, module_id: &str) -> Result<&BusClient, Failure> {
        if let Some(changes) = self.changes.as_mut() {
            match changes.has_changed() {
                Ok(true) => {
                    changes.borrow_and_update();
                    self.register = None;
                }
                Err(_) => {
                    self.client = None;
                    self.register = None;
                }
                Ok(false) => {}
            }
        }
        if self.client.is_none() {
            let consumer = self
                .connector
                .consumer()
                .await
                .map_err(|e| format!("{e:?}"))?;
            let client = BusClient::connect(
                consumer,
                BusClientOptions::new(module_id).map_err(|e| e.to_string())?,
            )
            .await
            .map_err(Failure::connect)?;
            self.changes = Some(client.connection_changes());
            self.client = Some(client);
        }
        Ok(self.client.as_ref().expect("connected client"))
    }

    async fn register(&mut self) -> Result<&NatsRegister, Failure> {
        self.client().await?;
        if self.register.is_none() {
            let client = self.client.as_ref().expect("connected client");
            let bucket = AccountNames::derive(client.acct())
                .and_then(|names| names.module_bucket_name(super::MODULE_ID, "changes"))
                .map_err(|e| e.to_string())?;
            self.register = Some(
                client
                    .connection()
                    .register(bucket)
                    .await
                    .map_err(|e| e.to_string())?,
            );
        }
        Ok(self.register.as_ref().expect("opened register"))
    }
}

#[async_trait]
impl Sink for BusSink {
    async fn ready(&mut self) -> Result<(), Failure> {
        self.register().await.map(|_| ())
    }

    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), Failure> {
        let result = self
            .register()
            .await?
            .put(key, value)
            .await
            .map_err(|e| e.to_string());
        if result.is_err() {
            self.register = None;
        }
        result.map(|_| ()).map_err(Failure::from)
    }

    async fn publish(&mut self, fact_id: &str, body: Vec<u8>) -> Result<(), Failure> {
        self.client()
            .await?
            .publish_event("placement_fact", 1, None, fact_id, body)
            .await
            .map(|_| ())
            .map_err(|e| e.to_string().into())
    }

    async fn connection_changed(&mut self) {
        match self.changes.as_mut() {
            Some(changes) => {
                if changes.changed().await.is_err() {
                    self.client = None;
                    self.changes = None;
                }
                self.register = None;
            }
            None => std::future::pending().await,
        }
    }
}

struct Worker {
    store: Weak<Mutex<Option<RegistryStore>>>,
    health: Arc<HealthGauges>,
    incarnation: String,
    commits: Arc<Notify>,
    role_changed: Arc<Notify>,
    watches: Arc<super::watches::Watches>,
    providers: Arc<dyn super::sessions::ProviderResolver>,
    sql: Arc<SqlOperations>,
    #[cfg(test)]
    idle: Arc<Notify>,
    #[cfg(test)]
    failed: Arc<Notify>,
}

impl ProjectsHandler {
    fn bus_changes_worker(&self) -> Worker {
        Worker {
            store: Arc::downgrade(&self.store),
            health: self.health.clone(),
            incarnation: self.incarnation.clone(),
            commits: self.commits.clone(),
            role_changed: self.role_cache_changed.clone(),
            watches: self.watches.clone(),
            providers: self.provider_resolver.clone(),
            sql: self.bus_changes_sql.clone(),
            #[cfg(test)]
            idle: Arc::default(),
            #[cfg(test)]
            failed: Arc::default(),
        }
    }

    pub(super) fn start_bus_changes(&self) {
        if self.health.origin.is_foreign() {
            return;
        }
        let mut task = self.bus_changes_task.lock().unwrap();
        if task.is_none() {
            let worker = self.bus_changes_worker();
            let sink = BusSink {
                connector: super::log_client::SubcConnector::from_args(std::env::args_os().skip(1)),
                ..BusSink::default()
            };
            *task = Some(tokio::spawn(worker.run(sink)));
        }
    }

    #[cfg(test)]
    pub(super) fn start_bus_changes_with_sink(&self, sink: impl Sink + 'static) -> Arc<Notify> {
        let worker = self.bus_changes_worker();
        let idle = worker.idle.clone();
        let mut task = self.bus_changes_task.lock().unwrap();
        assert!(task.is_none());
        *task = Some(tokio::spawn(worker.run(sink)));
        idle
    }

    #[cfg(test)]
    pub(super) async fn stop_bus_changes(&self) {
        let task = self.bus_changes_task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.bus_changes_sql.wait().await;
    }
}

impl Worker {
    async fn placement_sql<T: Send + 'static>(
        &self,
        action: impl FnOnce(RegistryStore) -> Result<T, entorhinal_core::RegistryError> + Send + 'static,
    ) -> Result<T, String> {
        let slot = self.store.clone();
        tokio::task::spawn_blocking(self.sql.tracked(move || {
            let slot = slot.upgrade().ok_or("placement store unavailable")?;
            let store = slot
                .lock()
                .unwrap()
                .clone()
                .ok_or("placement store unavailable")?;
            drop(slot);
            if store.is_foreign() {
                return Err("placement store is foreign".into());
            }
            action(store).map_err(|e| e.to_string())
        }))
        .await
        .map_err(|e| e.to_string())?
    }

    async fn publish_placement(&self, sink: &mut impl Sink) -> Result<bool, Failure> {
        let state = self
            .placement_sql(|store| store.placement_publication())
            .await?;
        self.health
            .placement_facts_published
            .store(state.published, Ordering::Relaxed);
        self.health
            .placement_facts_unpublished
            .store(state.unpublished, Ordering::Relaxed);
        let Some(fact) = state.next else {
            return Ok(false);
        };
        let body = placement_body(&fact)?;
        sink.publish(&fact.fact_id, body).await?;
        // The read job owns no store lease by the time the bus await begins.
        // Only an acknowledged message earns a local checkpoint.
        self.placement_sql(move |store| {
            store.placement_mark_published(&fact.fact_id, super::unix_millis())
        })
        .await?;
        self.health
            .placement_facts_published
            .fetch_add(1, Ordering::Relaxed);
        self.health
            .placement_facts_unpublished
            .fetch_sub(1, Ordering::Relaxed);
        Ok(true)
    }

    async fn snapshot(&self) -> Result<Option<Values>, String> {
        let slot = self.store.clone();
        let incarnation = self.incarnation.clone();
        let values = tokio::task::spawn_blocking(self.sql.tracked(move || {
            let Some(slot) = slot.upgrade() else {
                return Ok(None);
            };
            let Some(store) = slot.lock().unwrap().clone() else {
                return Ok(None);
            };
            drop(slot);
            if store.is_foreign() {
                return Ok(None);
            }
            let profile = store
                .user_get(UserGetRequest::default())
                .map_err(|e| e.to_string())?;
            let token = profile["token"].as_str().ok_or("profile token missing")?;
            let mut values = Values::new();
            values.insert(
                "registry",
                encode(&Generation {
                    generation: store.generation().map_err(|e| e.to_string())?,
                    incarnation: &incarnation,
                })?,
            );
            values.insert(
                "agents",
                encode(&Generation {
                    generation: store.agent_change_generation().map_err(|e| e.to_string())?,
                    incarnation: &incarnation,
                })?,
            );
            values.insert("profile", encode(&Profile { token })?);
            values.insert(
                "role.connector_home",
                encode(&Role {
                    epoch: store
                        .role_read("connector_home")
                        .map_err(|e| e.to_string())?
                        .epoch,
                })?,
            );
            Ok::<_, String>(Some(values))
        }))
        .await
        .map_err(|e| e.to_string())??;
        // Catalog generation is the same process-local counter served by the
        // status watch, not a registry sequence or the identity journal head.
        values
            .map(|mut values| {
                values.insert(
                    "catalog.status",
                    encode(&Catalog {
                        generation: self.watches.catalog_generation(),
                    })?,
                );
                values.insert(
                    "fleet_epoch",
                    encode(&Generation {
                        generation: self.health.fleet_epoch.generation(),
                        incarnation: &self.incarnation,
                    })?,
                );
                Ok(values)
            })
            .transpose()
    }

    async fn run(self, mut sink: impl Sink) {
        let mut last = Values::new();
        let mut backoff = INITIAL_BACKOFF;
        let mut failed_attempts = 0u64;
        let mut daemon_catalog = self.providers.catalog_changes();
        loop {
            if let Some(changes) = &mut daemon_catalog {
                changes.mark_unchanged();
            }
            // Register before reading so commits during SQL or a bus await are
            // observed on the next pass, including notify_waiters broadcasts.
            let commits = self.commits.notified();
            let roles = self.role_changed.notified();
            let catalog = self.watches.catalog_changed().notified();
            let epoch = self.health.fleet_epoch.changed.notified();
            tokio::pin!(commits, roles, catalog, epoch);
            commits.as_mut().enable();
            roles.as_mut().enable();
            catalog.as_mut().enable();
            epoch.as_mut().enable();
            if self.store.strong_count() == 0 {
                return;
            }
            let markers = match self.snapshot().await {
                Ok(Some(values)) => publish(&mut sink, &mut last, values, &self.health).await,
                Ok(None) => {
                    tokio::time::sleep(INITIAL_BACKOFF).await;
                    continue;
                }
                Err(error) => Err(error.into()),
            };
            // Try both outputs even when one fails: a missing KV bucket must
            // not strand facts, and an EVENT outage must not strand markers.
            // Drain the whole fact backlog in this pass, because a marker
            // failure below sleeps the backoff and would otherwise release
            // only one fact per backoff interval.
            // A connect refusal also prevents placement publication. Do not
            // make a second route attempt in the same pass after that refusal.
            let result = if markers.as_ref().is_err_and(|error| error.not_expected_back) {
                markers
            } else {
                let mut placement = self.publish_placement(&mut sink).await;
                while matches!(placement, Ok(true)) {
                    placement = self.publish_placement(&mut sink).await;
                }
                if placement
                    .as_ref()
                    .is_err_and(|error| error.not_expected_back)
                {
                    placement.map(|_| ())
                } else {
                    markers.and(placement.map(|_| ()))
                }
            };
            if let Err(error) = result {
                self.health
                    .bus_changes_connected
                    .store(false, Ordering::Relaxed);
                self.health
                    .bus_changes_failures
                    .fetch_add(1, Ordering::Relaxed);
                self.health
                    .bus_changes_parked
                    .store(error.not_expected_back, Ordering::Relaxed);
                #[cfg(test)]
                self.failed.notify_one();
                failed_attempts += 1;
                if failed_attempts == 1 {
                    tracing::warn!(target: "bus_changes", error = %error.message, parked = error.not_expected_back, "change publisher unavailable (repeats are not logged)");
                }
                if error.not_expected_back {
                    wait_for_catalog(&mut daemon_catalog).await;
                    self.health
                        .bus_changes_parked
                        .store(false, Ordering::Relaxed);
                } else {
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(MAX_BACKOFF);
                }
                continue;
            }
            self.health
                .bus_changes_connected
                .store(true, Ordering::Relaxed);
            if failed_attempts != 0 {
                tracing::info!(target: "bus_changes", failed_attempts, "change publisher recovered");
                failed_attempts = 0;
            }
            backoff = INITIAL_BACKOFF;
            #[cfg(test)]
            self.idle.notify_one();
            tokio::select! {
                _ = &mut commits => {},
                _ = &mut roles => {},
                _ = &mut catalog => {},
                _ = &mut epoch => {},
                // A new connection may face a bucket ck-bus re-provisioned empty,
                // so every key is written again rather than trusting what this
                // process last put.
                _ = sink.connection_changed() => last.clear(),
            }
        }
    }
}

pub(super) fn placement_body(fact: &PlacementFact) -> Result<Vec<u8>, String> {
    let body = serde_json::to_vec(fact).map_err(|e| e.to_string())?;
    if body.len() >= 4096 {
        return Err("placement fact must be under 4 KiB".into());
    }
    Ok(body)
}

fn encode(value: &impl Serialize) -> Result<Vec<u8>, String> {
    let bytes = serde_json::to_vec(value).map_err(|e| e.to_string())?;
    if bytes.len() > 512 {
        return Err("change marker exceeds 512 bytes".into());
    }
    Ok(bytes)
}

async fn publish(
    sink: &mut impl Sink,
    last: &mut Values,
    values: Values,
    health: &HealthGauges,
) -> Result<(), Failure> {
    sink.ready().await?;
    for (key, value) in values {
        if last.get(key) == Some(&value) {
            continue;
        }
        sink.put(key, value.clone()).await?;
        last.insert(key, value);
        health.bus_changes_puts.fetch_add(1, Ordering::Relaxed);
    }
    Ok(())
}

#[cfg(test)]
pub(super) mod tests {
    use super::*;
    use crate::fake_daemon::{DaemonCall, DaemonSnapshot, FakeDaemon, WireDaemon};
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Instant};

    pub(crate) async fn disabled_ckbus(
        handler: &mut ProjectsHandler,
        dir: &std::path::Path,
    ) -> (FakeDaemon, WireDaemon, crate::log_client::SubcConnector) {
        let daemon = FakeDaemon::new(DaemonSnapshot::new(1, vec![], vec![])).with_ckbus_disabled();
        let path = dir.join("subc.json");
        let wire = WireDaemon::start(daemon.clone(), &path).await;
        let args = ["--subc".into(), path.into_os_string()];
        handler.provider_resolver = Arc::new(crate::sessions::SubcProviderResolver::from_args(
            args.clone(),
        ));
        handler.provider_resolver.resolve_provider("unused").await;
        timeout(Duration::from_secs(5), async {
            while !handler.provider_resolver.cache_health().watch_live {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        (
            daemon,
            wire,
            crate::log_client::SubcConnector::from_args(args),
        )
    }

    pub(crate) async fn assert_wire_catalog_wakes(handler: &ProjectsHandler, daemon: &FakeDaemon) {
        let mut catalog = handler.provider_resolver.catalog_changes();
        assert_eq!(*catalog.as_ref().unwrap().borrow(), Some(1));
        daemon.catalog_notice(Some(2));
        timeout(Duration::from_secs(5), wait_for_catalog(&mut catalog))
            .await
            .unwrap();
        assert_eq!(*catalog.unwrap().borrow(), Some(2));
        assert_eq!(
            daemon
                .calls()
                .iter()
                .filter(|call| matches!(call, DaemonCall::CatalogWatch))
                .count(),
            1
        );
    }

    pub(crate) struct RefusedCkbus(pub(crate) mpsc::UnboundedSender<Instant>);

    impl RefusedCkbus {
        pub(crate) fn attempt(&self) -> Error {
            self.0.send(Instant::now()).unwrap();
            Error::RouteRefused {
                code: subc_protocol::error_codes::TARGET_UNAVAILABLE.into(),
                expected_back: Some(false),
            }
        }
    }

    #[async_trait]
    impl Sink for RefusedCkbus {
        async fn ready(&mut self) -> Result<(), Failure> {
            Err(Failure::connect(self.attempt()))
        }

        async fn put(&mut self, _: &str, _: Vec<u8>) -> Result<(), Failure> {
            panic!("write requires working ck-bus")
        }

        async fn publish(&mut self, _: &str, _: Vec<u8>) -> Result<(), Failure> {
            Err(Failure::connect(self.attempt()))
        }

        async fn connection_changed(&mut self) {
            std::future::pending().await
        }
    }

    pub(crate) async fn assert_disabled_ckbus_schedule(
        daemon: &FakeDaemon,
        failed: &Notify,
        mut attempts: mpsc::UnboundedReceiver<Instant>,
    ) {
        let first = timeout(Duration::from_secs(5), attempts.recv())
            .await
            .unwrap()
            .unwrap();
        failed.notified().await;
        assert!(
            timeout(Duration::from_secs(600), attempts.recv())
                .await
                .is_err(),
            "ck-bus retried before the catalog changed"
        );

        let notice_at = Instant::now();
        assert_eq!(notice_at - first, Duration::from_secs(600));
        daemon.catalog_notice(Some(2));
        let second = timeout(Duration::from_millis(1), attempts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(second, notice_at);
        failed.notified().await;

        let parked_at = Instant::now();
        assert!(
            timeout(Duration::from_millis(899_999), attempts.recv())
                .await
                .is_err(),
            "ck-bus retried before the 15-minute cap"
        );
        let third = timeout(Duration::from_millis(2), attempts.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(third - parked_at, Duration::from_secs(900));
        failed.notified().await;
        assert_eq!(attempts.try_recv(), Err(mpsc::error::TryRecvError::Empty));
    }

    #[tokio::test]
    async fn bus_changes_wire_refusal_and_catalog_notice() {
        let (dir, _) = crate::tests::scratch_descriptor("bus-changes-wire-ckbus");
        let mut handler = ProjectsHandler::with_runtime("bus-changes".into(), || 7);
        let (daemon, _wire, connector) = disabled_ckbus(&mut handler, &dir).await;
        let mut sink = BusSink {
            connector,
            ..BusSink::default()
        };
        // The bus client shares credentials by module id across the process.
        let failure = sink
            .client_for("entorhinal-wire-publisher")
            .await
            .err()
            .expect("expected route refusal");
        assert!(failure.not_expected_back, "{failure:?}");
        assert!(failure
            .message
            .contains(subc_protocol::error_codes::TARGET_UNAVAILABLE));
        assert_wire_catalog_wakes(&handler, &daemon).await;
        handler.provider_resolver.stop().await;
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn bus_changes_waits_for_catalog_when_ckbus_disabled() {
        let (dir, descriptor) = crate::tests::scratch_descriptor("bus-changes-disabled-ckbus");
        let mut handler = ProjectsHandler::with_runtime("bus-changes".into(), || 7);
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        let daemon = FakeDaemon::new(DaemonSnapshot::new(1, vec![], vec![]));
        handler.provider_resolver = Arc::new(daemon.clone());
        let (attempts_tx, attempts) = mpsc::unbounded_channel();
        let worker = handler.bus_changes_worker();
        let failed = worker.failed.clone();
        *handler.bus_changes_task.lock().unwrap() =
            Some(tokio::spawn(worker.run(RefusedCkbus(attempts_tx))));
        assert_disabled_ckbus_schedule(&daemon, &failed, attempts).await;
        let metrics = subc_client_rs::ModuleHandler::health(&handler)
            .await
            .metrics
            .unwrap();
        assert_eq!(metrics["busChangesParkedCkbusNotExpectedBack"], true);
        assert_eq!(metrics["busChangesFailures"], 3);
        handler.stop_bus_changes().await;
        handler.provider_resolver.stop().await;
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn bus_changes_placement_body_bounded() {
        let fact = PlacementFact {
            fact_id: "x".repeat(4096),
            supersedes: None,
            project_id: "project".into(),
            workspace_id: None,
            effective_from_ms: 0,
            recorded_at_ms: 0,
        };
        assert!(placement_body(&fact).is_err());
    }

    #[test]
    fn bus_changes_marker_bytes_are_ordered_and_bounded() {
        assert_eq!(
            encode(&Generation {
                generation: 9,
                incarnation: "abc"
            })
            .unwrap(),
            br#"{"generation":9,"incarnation":"abc"}"#
        );
        assert_eq!(
            encode(&Profile { token: "u1:abc" }).unwrap(),
            br#"{"token":"u1:abc"}"#
        );
        assert_eq!(encode(&Role { epoch: 8 }).unwrap(), br#"{"epoch":8}"#);
        assert_eq!(
            encode(&Catalog { generation: 7 }).unwrap(),
            br#"{"generation":7}"#
        );
        for value in [
            encode(&Generation {
                generation: i64::MAX,
                incarnation: &"f".repeat(32),
            })
            .unwrap(),
            encode(&Profile {
                token: &format!("u1:{}", "f".repeat(64)),
            })
            .unwrap(),
            encode(&Role { epoch: i64::MAX }).unwrap(),
            encode(&Catalog {
                generation: u64::MAX,
            })
            .unwrap(),
        ] {
            assert!(value.len() < 512);
        }
        assert!(encode(&Profile {
            token: &"x".repeat(512)
        })
        .is_err());
    }
}

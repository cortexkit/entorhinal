//! Revision hints only. Consumers re-read the owning operation for actual data.

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};

use async_trait::async_trait;
use cortexkit_bus_client::{BusClient, BusClientOptions};
use cortexkit_bus_naming::AccountNames;
use cortexkit_bus_nats::NatsRegister;
use cortexkit_bus_trait::Register;
use entorhinal_core::{user_profile::UserGetRequest, RegistryStore};
use serde::Serialize;
use tokio::sync::{watch, Notify};

use super::{move_driver::SqlOperations, HealthGauges, Ordering, ProjectsHandler};

const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);
type Values = BTreeMap<&'static str, Vec<u8>>;

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

// This boundary keeps tests independent of an external NATS server. The real
// implementation only opens a provisioned register; it never creates a bucket.
#[async_trait]
pub(super) trait Sink: Send {
    async fn ready(&mut self) -> Result<(), String>;
    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), String>;
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
    async fn register(&mut self) -> Result<&NatsRegister, String> {
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
                BusClientOptions::new(super::MODULE_ID).map_err(|e| e.to_string())?,
            )
            .await
            .map_err(|e| e.to_string())?;
            self.changes = Some(client.connection_changes());
            self.client = Some(client);
        }
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
    async fn ready(&mut self) -> Result<(), String> {
        self.register().await.map(|_| ())
    }

    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        let result = self
            .register()
            .await?
            .put(key, value)
            .await
            .map_err(|e| e.to_string());
        if result.is_err() {
            self.register = None;
        }
        result.map(|_| ())
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
    sql: Arc<SqlOperations>,
    #[cfg(test)]
    idle: Arc<Notify>,
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
            sql: self.bus_changes_sql.clone(),
            #[cfg(test)]
            idle: Arc::default(),
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
                Ok(values)
            })
            .transpose()
    }

    async fn run(self, mut sink: impl Sink) {
        let mut last = Values::new();
        let mut backoff = INITIAL_BACKOFF;
        let mut failed_attempts = 0u64;
        loop {
            // Register before reading so commits during SQL or a bus await are
            // observed on the next pass, including notify_waiters broadcasts.
            let commits = self.commits.notified();
            let roles = self.role_changed.notified();
            let catalog = self.watches.catalog_changed().notified();
            tokio::pin!(commits, roles, catalog);
            commits.as_mut().enable();
            roles.as_mut().enable();
            catalog.as_mut().enable();
            if self.store.strong_count() == 0 {
                return;
            }
            let result = match self.snapshot().await {
                Ok(Some(values)) => publish(&mut sink, &mut last, values, &self.health).await,
                Ok(None) => {
                    tokio::time::sleep(INITIAL_BACKOFF).await;
                    continue;
                }
                Err(error) => Err(error),
            };
            if let Err(error) = result {
                self.health
                    .bus_changes_connected
                    .store(false, Ordering::Relaxed);
                self.health
                    .bus_changes_failures
                    .fetch_add(1, Ordering::Relaxed);
                failed_attempts += 1;
                if failed_attempts == 1 {
                    tracing::warn!(target: "bus_changes", %error, "change publisher unavailable; retrying with backoff (repeats are not logged)");
                }
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(MAX_BACKOFF);
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
                _ = sink.connection_changed() => {},
            }
        }
    }
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
) -> Result<(), String> {
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
mod tests {
    use super::*;

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

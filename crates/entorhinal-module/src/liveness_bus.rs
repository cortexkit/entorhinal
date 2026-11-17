//! Ordered session-liveness mirror. Only a live watch with a reconciliation
//! marker and a finished initial replay may replace the pushed feed. A key
//! updated after the marker replays later, so both conditions are necessary.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_nats::jetstream::kv::{Operation, Watch};
use async_trait::async_trait;
use cortexkit_bus_client::{BusClient, BusClientOptions};
use cortexkit_bus_naming::AccountNames;
use futures_util::StreamExt;
use serde::Deserialize;

use super::{HealthGauges, LivenessState, Ordering, ProjectsHandler, SessionKey};

const MARKER: &str = "_reconciled";
const INITIAL_BACKOFF: Duration = Duration::from_secs(1);
const MAX_BACKOFF: Duration = Duration::from_secs(60);

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Entry {
    harness: String,
    session_id: String,
    state: State,
    last_activity_ms: i64,
    project_id: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum State {
    Active,
    Idle,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Reconciled {
    #[serde(rename = "bootId")]
    _boot_id: String,
    #[serde(rename = "sessions")]
    _sessions: u64,
}

#[derive(Default)]
pub(super) struct Mirror {
    sessions: BTreeMap<SessionKey, Entry>,
    // Deletes have no value. Remember the identity decoded from each PUT, not
    // a split of the opaque KV key (harness and session ids may contain dots).
    keys: BTreeMap<String, SessionKey>,
    reconciled: bool,
    watch_live: bool,
    initial_replay_finished: bool,
}

pub(super) enum Read<'a> {
    Active,
    Idle,
    Gone,
    Unknown(Option<&'a str>),
}

impl LivenessState {
    /// All liveness decisions select their source here. A missing bus key is
    /// evidence of absence only after the writer's reconciliation PUT and the
    /// watch's initial replay boundary have both arrived.
    pub(super) fn read(&self, key: &SessionKey) -> Read<'_> {
        if self.bus.authoritative() {
            return match self.bus.sessions.get(key).map(|entry| &entry.state) {
                Some(State::Active) => Read::Active,
                Some(State::Idle) => Read::Idle,
                None => Read::Gone,
            };
        }
        let entry = self.sessions.get(key);
        let cause = entry.and_then(|entry| entry.cause.as_deref());
        if self.stale {
            return Read::Unknown(cause);
        }
        match entry.map(|entry| entry.state.as_str()) {
            Some("active") => Read::Active,
            Some("idle") => Read::Idle,
            None if self.gone.contains(key) => Read::Gone,
            _ => Read::Unknown(cause),
        }
    }

    pub(super) fn readable(&self) -> bool {
        self.bus.authoritative() || (!self.stale && !self.sessions.is_empty())
    }

    pub(super) fn project_activity(&self, project_id: &str, roots: &[String]) -> Option<i64> {
        if self.bus.authoritative() {
            self.bus
                .sessions
                .iter()
                .filter(|(key, entry)| {
                    matches!(self.read(key), Read::Active | Read::Idle)
                        && entry.project_id.as_deref() == Some(project_id)
                })
                .map(|(_, entry)| entry.last_activity_ms)
                .max()
        } else {
            self.sessions
                .values()
                .filter(|entry| {
                    roots.iter().any(|root| {
                        entorhinal_core::path_prefix_or_equal(
                            Path::new(root),
                            Path::new(&entry.canonical_root),
                        )
                    })
                })
                .map(|entry| entry.last_activity_ms)
                .max()
        }
    }
}

impl Mirror {
    pub(super) fn watch_started(&mut self) {
        self.watch_live = true;
    }
    pub(super) fn finish_replay(&mut self) {
        self.initial_replay_finished = true;
    }
    pub(super) fn authoritative(&self) -> bool {
        self.watch_live && self.initial_replay_finished && self.reconciled
    }

    fn clear(&mut self) {
        *self = Self::default();
    }

    pub(super) fn apply(&mut self, update: Update) -> Result<Option<SessionKey>, Failure> {
        match update {
            Update::Put(key, value) if key == MARKER => {
                serde_json::from_slice::<Reconciled>(&value).map_err(Failure::decode)?;
                self.reconciled = true;
            }
            Update::Delete(key) if key == MARKER => self.reconciled = false,
            Update::Put(key, value) if !key.starts_with('_') => {
                let entry: Entry = serde_json::from_slice(&value).map_err(Failure::decode)?;
                let identity = SessionKey::new(&entry.harness, &entry.session_id);
                if let Some(previous) = self.keys.insert(key, identity.clone()) {
                    self.sessions.remove(&previous);
                }
                self.sessions.insert(identity, entry);
            }
            Update::Delete(key) => {
                if let Some(identity) = self.keys.remove(&key) {
                    self.sessions.remove(&identity);
                    return Ok(Some(identity));
                }
                // An initial tombstone has no identity to decode. Do not infer
                // an end from absence: this bucket does not cover every host.
            }
            Update::Put(_, _) => {}
        }
        Ok(None)
    }
}

pub(super) enum Update {
    Put(String, Vec<u8>),
    Delete(String),
}

pub(super) struct Delivery {
    update: Update,
    seen_current: bool,
}

#[derive(Debug)]
pub(super) struct Failure {
    kind: String,
    message: String,
}

impl Failure {
    fn new(kind: impl Into<String>, message: impl ToString) -> Self {
        Self {
            kind: kind.into(),
            message: message.to_string(),
        }
    }
    fn decode(error: serde_json::Error) -> Self {
        Self::new("decode", error)
    }
}

// A fresh watch must replay the bucket in stream order, including tombstones.
// Keeping this boundary small lets tests drive outages without a NATS server.
#[async_trait]
pub(super) trait Source: Send {
    async fn watch_all(&mut self) -> Result<(), Failure>;
    async fn next(&mut self) -> Result<Delivery, Failure>;
}

#[derive(Default)]
struct BusSource {
    connector: super::log_client::SubcConnector,
    client: Option<BusClient>,
    changes: Option<tokio::sync::watch::Receiver<u64>>,
    watch: Option<Watch>,
}

#[async_trait]
impl Source for BusSource {
    async fn watch_all(&mut self) -> Result<(), Failure> {
        self.watch = None;
        if let Some(changes) = &mut self.changes {
            match changes.has_changed() {
                Ok(true) => {
                    changes.borrow_and_update();
                }
                Err(_) => {
                    self.client = None;
                    self.changes = None;
                }
                Ok(false) => {}
            }
        }
        if self.client.is_none() {
            let consumer = self
                .connector
                .consumer()
                .await
                .map_err(|e| Failure::new("credential", format!("{e:?}")))?;
            let client = BusClient::connect(
                consumer,
                BusClientOptions::new(super::MODULE_ID).map_err(|e| Failure::new("options", e))?,
            )
            .await
            .map_err(|e| Failure::new("connect", e))?;
            self.changes = Some(client.connection_changes());
            self.client = Some(client);
        }
        let client = self.client.as_ref().expect("connected bus client");
        let bucket = AccountNames::derive(client.acct())
            .and_then(|names| names.module_bucket_name("prefrontal-core", "session-liveness"))
            .map_err(|e| Failure::new("bucket_name", e))?;
        // Use the credentialed connection's inbox prefix and local JetStream;
        // the liveness bucket is on this machine, not a remote JetStream domain.
        let kv = async_nats::jetstream::new(client.connection().client())
            .get_key_value(bucket)
            .await
            .map_err(|e| Failure::new(format!("bucket:{:?}", e.kind()), e))?;
        self.watch = Some(
            kv.watch_with_history(">")
                .await
                .map_err(|e| Failure::new(format!("watch:{:?}", e.kind()), e))?,
        );
        Ok(())
    }

    async fn next(&mut self) -> Result<Delivery, Failure> {
        let changes = self.changes.as_mut().expect("connected change receiver");
        tokio::select! {
            result = changes.changed() => {
                if result.is_err() { self.client = None; self.changes = None; }
                Err(Failure::new("connection_changed", "bus connection replaced"))
            }
            entry = self.watch.as_mut().expect("opened watch").next() => {
                let entry = entry.ok_or_else(|| Failure::new("watch_ended", "bus watch ended"))?
                    .map_err(|e| Failure::new(format!("watch:{:?}", e.kind()), e))?;
                let update = match entry.operation {
                    Operation::Put => Update::Put(entry.key, entry.value.to_vec()),
                    Operation::Delete | Operation::Purge => Update::Delete(entry.key),
                };
                Ok(Delivery { update, seen_current: entry.seen_current })
            }
        }
    }
}

struct Worker {
    liveness: Arc<Mutex<LivenessState>>,
    health: Arc<HealthGauges>,
    upkeep: Arc<super::session_upkeep::SessionUpkeep>,
    clock: fn() -> i64,
    #[cfg(test)]
    failed: Arc<tokio::sync::Notify>,
}

impl Worker {
    fn gauges(&self, mirror: &Mirror) {
        self.health
            .liveness_bus_connected
            .store(mirror.watch_live, Ordering::Relaxed);
        self.health
            .liveness_bus_authoritative
            .store(mirror.authoritative(), Ordering::Relaxed);
        self.health
            .liveness_bus_sessions
            .store(mirror.sessions.len() as u64, Ordering::Relaxed);
    }

    fn clear(&self) {
        let mut state = self.liveness.lock().unwrap_or_else(|e| e.into_inner());
        state.bus.clear();
        self.gauges(&state.bus);
    }

    fn apply(&self, delivery: Delivery) -> Result<bool, Failure> {
        let mut state = self.liveness.lock().unwrap_or_else(|e| e.into_inner());
        let deleted = state.bus.apply(delivery.update)?;
        if delivery.seen_current {
            state.bus.finish_replay();
        }
        if let Some(key) =
            deleted.filter(|key| state.bus.authoritative() && matches!(state.read(key), Read::Gone))
        {
            self.upkeep.push(vec![entorhinal_core::GoneSession {
                harness: key.harness,
                session_id: key.session_id,
                runner: None,
                ended_at_ms: (self.clock)(),
            }]);
        }
        self.gauges(&state.bus);
        Ok(state.bus.authoritative())
    }

    async fn run(self, mut source: impl Source) {
        let mut backoff = INITIAL_BACKOFF;
        let mut outage: Option<(String, u64)> = None;
        loop {
            self.clear();
            let result = async {
                source.watch_all().await?;
                {
                    let mut state = self.liveness.lock().unwrap_or_else(|e| e.into_inner());
                    state.bus.watch_started();
                    self.gauges(&state.bus);
                }
                loop {
                    if self.apply(source.next().await?)? {
                        backoff = INITIAL_BACKOFF;
                        if let Some((kind, attempts)) = outage.take() {
                            tracing::info!(target: "liveness", kind, attempts, "bus liveness recovered");
                        }
                    }
                }
                #[allow(unreachable_code)]
                Ok::<(), Failure>(())
            }.await;
            if let Err(error) = result {
                self.clear();
                self.health
                    .liveness_bus_failures
                    .fetch_add(1, Ordering::Relaxed);
                #[cfg(test)]
                self.failed.notify_one();
                match &mut outage {
                    Some((kind, attempts)) if *kind == error.kind => *attempts += 1,
                    _ => {
                        tracing::warn!(target: "liveness", kind = error.kind, detail = error.message,
                            previous = ?outage, "bus liveness unavailable; using pushed feed (repeats are counted, not logged)");
                        outage = Some((error.kind, 1));
                    }
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(MAX_BACKOFF);
        }
    }
}

impl ProjectsHandler {
    fn liveness_bus_worker(&self) -> Worker {
        Worker {
            liveness: self.liveness.clone(),
            health: self.health.clone(),
            upkeep: self.session_upkeep.clone(),
            clock: self.clock,
            #[cfg(test)]
            failed: Arc::default(),
        }
    }

    pub(super) fn start_liveness_bus(&self) {
        if self.health.origin.is_foreign() {
            return;
        }
        let mut task = self.liveness_bus_task.lock().unwrap();
        if task.is_none() {
            let source = BusSource {
                connector: super::log_client::SubcConnector::from_args(std::env::args_os().skip(1)),
                ..BusSource::default()
            };
            *task = Some(tokio::spawn(self.liveness_bus_worker().run(source)));
        }
    }

    #[cfg(test)]
    pub(super) async fn stop_liveness_bus(&self) {
        let task = self.liveness_bus_task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.liveness_bus_worker().clear();
    }
}

#[cfg(test)]
#[path = "liveness_bus_tests.rs"]
mod tests;

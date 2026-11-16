//! Bounded process-local subscriptions. SQLite is used only for short snapshots;
//! neither waiting nor transport backpressure owns a store or history lock.

use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{atomic::Ordering, Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use entorhinal_core::{
    claim_cache::ClaimObservation, RegistryError, RegistryStore, SessionChangeKey,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{HandlerOutcome, RequestCtx};
use tokio::sync::{broadcast, Notify, Semaphore};

use crate::{HandlerError, ProjectsHandler, RouteKey, WireRequest};

pub(crate) const IDENTITY: &str = "agent.fleet_identity_watch";
pub(crate) const WHERE: &str = "agent.where_watch";
pub(crate) const CATALOG: &str = "catalog.status_watch";
pub(crate) const SESSIONS: &str = "agent.sessions_watch";
const NOTICE_LIMIT: usize = 192 * 1024;
const HISTORY_LIMIT: usize = 256;
/// Watches one route may hold open. Each holds one daemon request slot on its
/// route, so this stays well under the daemon's per-route in-flight limit, and
/// one caller (an app with many chats open, a phone) cannot crowd out others.
const WATCHES_PER_ROUTE: usize = 16;
/// Watches open across all routes. Each costs one task and a small history
/// cursor, so the bound is about runaway callers, not memory.
const WATCHES_TOTAL: usize = 64;
const QUIET: Duration = Duration::from_millis(200);
const CEILING: Duration = Duration::from_secs(1);

#[derive(Default)]
struct WhereHistory {
    seq: u64,
    batches: VecDeque<WhereBatch>,
}

struct WhereBatch {
    seq: u64,
    entries: BTreeMap<String, Value>,
    all_agents: bool,
}

#[derive(Default)]
struct CatalogHistory {
    generation: u64,
    statuses: BTreeMap<(String, String), Value>,
    batches: VecDeque<(u64, BTreeSet<(String, String)>)>,
}

pub(crate) struct Watches {
    slots: Semaphore,
    per_route: Mutex<BTreeMap<RouteKey, usize>>,
    cache_writer: Mutex<()>,
    history: Mutex<WhereHistory>,
    changed: Notify,
    catalog: Mutex<CatalogHistory>,
    catalog_changed: Notify,
    sessions_changed: broadcast::Sender<Arc<Vec<SessionChangeKey>>>,
}

impl Default for Watches {
    fn default() -> Self {
        Self {
            slots: Semaphore::new(WATCHES_TOTAL),
            per_route: Mutex::default(),
            cache_writer: Mutex::new(()),
            history: Mutex::new(WhereHistory::default()),
            changed: Notify::new(),
            catalog: Mutex::default(),
            catalog_changed: Notify::new(),
            sessions_changed: broadcast::channel(HISTORY_LIMIT).0,
        }
    }
}

impl Watches {
    pub(crate) fn catalog_generation(&self) -> u64 {
        self.catalog.lock().unwrap().generation
    }

    pub(crate) fn catalog_changed(&self) -> &Notify {
        &self.catalog_changed
    }

    /// Called with committed snapshots, after the blocking SQL job has released
    /// every store handle. Transport emission happens only in the watch task.
    pub(crate) fn observe_catalog(&self, statuses: BTreeMap<(String, String), Value>) {
        let mut history = self.catalog.lock().unwrap();
        let changes = statuses
            .keys()
            .chain(history.statuses.keys())
            .filter(|key| statuses.get(*key) != history.statuses.get(*key))
            .cloned()
            .collect::<BTreeSet<_>>();
        history.statuses = statuses;
        if changes.is_empty() {
            return;
        }
        history.generation += 1;
        let generation = history.generation;
        history.batches.push_back((generation, changes));
        if history.batches.len() > HISTORY_LIMIT {
            history.batches.pop_front();
        }
        drop(history);
        self.catalog_changed.notify_waiters();
    }
    pub(crate) fn resync_all(&self) {
        let mut history = self.history.lock().unwrap();
        history.seq += 1;
        let seq = history.seq;
        history.batches.push_back(WhereBatch {
            seq,
            entries: BTreeMap::new(),
            all_agents: true,
        });
        if history.batches.len() > HISTORY_LIMIT {
            history.batches.pop_front();
        }
        drop(history);
        self.changed.notify_waiters();
    }

    /// Claim refresh, move source and move receiver all use this seam. Serialize observations without
    /// holding the history mutex while SQLite may queue behind another writer.
    /// The refresh and move workers already call this on their blocking pool.
    pub(crate) fn cache_claims(
        &self,
        store: &RegistryStore,
        observations: &[(String, ClaimObservation)],
        now: i64,
    ) -> Result<usize, RegistryError> {
        let _writer = self.cache_writer.lock().unwrap();
        let before = observations
            .iter()
            .map(|(id, _)| Ok((id.clone(), store.cached_agent_claim(id)?)))
            .collect::<Result<BTreeMap<_, _>, RegistryError>>()?;
        let conflicts = store.cache_agent_claims(observations, now)?;
        let mut entries = BTreeMap::new();
        for (id, old) in before {
            if old != store.cached_agent_claim(&id)? {
                // A refused whereabouts read (for example an integrity mark)
                // cannot be represented as a where_many entry. Null is an
                // internal resync marker and is never sent as an entry.
                let row = store
                    .agent_where(&id)
                    .ok()
                    .and_then(|row| serde_json::to_value(row).ok());
                entries.insert(id, row.unwrap_or(Value::Null));
            }
        }
        if !entries.is_empty() {
            let mut history = self.history.lock().unwrap();
            history.seq += 1;
            let seq = history.seq;
            for (id, row) in &mut entries {
                if !row.is_null() {
                    row["agent_id"] = json!(id);
                    row["version"] = json!(seq);
                }
            }
            history.batches.push_back(WhereBatch {
                seq,
                entries,
                all_agents: false,
            });
            if history.batches.len() > HISTORY_LIMIT {
                history.batches.pop_front();
            }
            drop(history);
            self.changed.notify_waiters();
        }
        Ok(conflicts)
    }
}

#[async_trait]
pub(super) trait WatchTransport: Sync {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError>;
    async fn ended(&self);
}

#[async_trait]
impl WatchTransport for RequestCtx {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        RequestCtx::emit(self, body)
            .await
            .map_err(|error| HandlerError::new("stream_closed", error.to_string()))
    }

    async fn ended(&self) {
        self.cancelled().await;
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct IdentityRequest {
    // Like fleet_identity, malformed opaque tokens are cache misses.
    token: Option<Value>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    incarnation: String,
    seq: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WhereRequest {
    #[serde(rename = "agentIds")]
    agent_ids: Option<Vec<String>>,
    cursor: Option<Cursor>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionsRequest {
    agent_id: String,
    #[serde(default)]
    open: bool,
}

struct Slot<'a>(&'a crate::HealthGauges);
impl Drop for Slot<'_> {
    fn drop(&mut self) {
        self.0.watches_open.fetch_sub(1, Ordering::Relaxed);
    }
}

/// One route's share of the watch limit, returned when the watch ends.
struct RouteSlot<'a> {
    watches: &'a Watches,
    key: RouteKey,
}

impl Watches {
    fn take_route_slot(&self, key: RouteKey) -> Option<RouteSlot<'_>> {
        let mut held = self.per_route.lock().unwrap_or_else(|e| e.into_inner());
        let count = held.entry(key).or_default();
        if *count >= WATCHES_PER_ROUTE {
            return None;
        }
        *count += 1;
        Some(RouteSlot { watches: self, key })
    }
}

impl Drop for RouteSlot<'_> {
    fn drop(&mut self) {
        let mut held = self
            .watches
            .per_route
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if let Some(count) = held.get_mut(&self.key) {
            *count -= 1;
            if *count == 0 {
                held.remove(&self.key);
            }
        }
    }
}

pub(super) fn is_watch(method: &str) -> bool {
    matches!(method, IDENTITY | WHERE | CATALOG | SESSIONS)
}

fn decode<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))
}

impl ProjectsHandler {
    /// One termination point for cancellation, module drain and route drain.
    /// Returning Streamed lets the SDK send StreamEnd, never a dropped Response.
    pub(super) async fn serve_watch(
        &self,
        transport: &impl WatchTransport,
        body: &[u8],
        key: RouteKey,
    ) -> HandlerOutcome {
        let result = async {
            let request: WireRequest =
                decode(serde_json::from_slice(body).map_err(crate::invalid_params)?)?;
            self.admit(&request.method, key)?;
            if *self.draining.borrow() {
                return Err(HandlerError::new(
                    "module_draining",
                    "module is stopping; retry interrupted work after restart",
                ));
            }
            // Validate before consuming a slot, with exactly the existing read
            // contracts: no ambient fields and the same where_many id checks.
            let catalog = request.method == CATALOG;
            let identity = if request.method == IDENTITY || catalog {
                Some(decode::<IdentityRequest>(request.params.clone())?)
            } else {
                None
            };
            let whereabouts = if request.method == WHERE {
                let request: WhereRequest = decode(request.params.clone())?;
                if let Some(ids) = &request.agent_ids {
                    crate::claims::validate_where_ids(ids)?;
                }
                Some(request)
            } else {
                None
            };
            let sessions = if request.method == SESSIONS {
                let request: SessionsRequest = decode(request.params)?;
                let watches = Arc::downgrade(&self.watches);
                self.with_agent_store(|store| {
                    store.set_session_change_listener(Arc::new(move |keys| {
                        if let Some(watches) = watches.upgrade() {
                            let _ = watches.sessions_changed.send(Arc::new(keys));
                        }
                    }));
                    Ok(())
                })?;
                // Subscribe before the snapshot so a concurrent commit is either
                // already visible there or queued for the next notice.
                let receiver = self.watches.sessions_changed.subscribe();
                let initial = self.sessions_resync(&request)?;
                Some((request, receiver, initial))
            } else {
                None
            };
            let _route_slot = self.watches.take_route_slot(key).ok_or_else(|| {
                // Published in fleet-watch-v1's watch-capacity sample.
                HandlerError::new(
                    "watch_capacity",
                    format!("at most {WATCHES_PER_ROUTE} watches may be open"),
                )
            })?;
            let _permit = self.watches.slots.try_acquire().map_err(|_| {
                HandlerError::new(
                    "watch_capacity",
                    format!("at most {WATCHES_TOTAL} watches may be open on this machine"),
                )
            })?;
            self.health.watches_open.fetch_add(1, Ordering::Relaxed);
            let _slot = Slot(&self.health);
            if whereabouts.is_some() {
                let watches = std::sync::Arc::downgrade(&self.watches);
                self.with_agent_store(|store| {
                    store.set_where_change_listener(std::sync::Arc::new(move || {
                        if let Some(watches) = watches.upgrade() {
                            watches.resync_all();
                        }
                    }));
                    Ok(())
                })?;
            }
            let mut draining = self.draining.subscribe();
            tokio::select! {
                biased;
                _ = transport.ended() => Ok(()),
                _ = draining.wait_for(|stopping| *stopping) => Ok(()),
                result = async {
                    if let Some(request) = identity {
                        if catalog { self.catalog_status_watch(transport,request).await }
                        else { self.identity_watch(transport, request).await }
                    } else if let Some(request) = whereabouts {
                        self.where_watch(transport, request).await
                    } else if let Some((request, receiver, initial)) = sessions {
                        self.sessions_watch(transport, request, receiver, initial).await
                    } else {
                        Err(HandlerError::new("unknown_method", "not a watch operation"))
                    }
                } => result,
            }
        }
        .await;
        match result {
            Ok(()) => HandlerOutcome::Streamed,
            Err(error) => error.into_outcome(),
        }
    }

    async fn notice(
        &self,
        transport: &impl WatchTransport,
        changes: Value,
        resync: Value,
    ) -> Result<(), HandlerError> {
        let mut body = serde_json::to_vec(&changes).map_err(crate::invalid_params)?;
        let resynced = body.len() > NOTICE_LIMIT || changes["kind"] == "resync";
        if body.len() > NOTICE_LIMIT {
            body = serde_json::to_vec(&resync).map_err(crate::invalid_params)?;
        }
        transport.emit(body).await?;
        self.health.watch_notices.fetch_add(1, Ordering::Relaxed);
        if resynced {
            self.health.watch_resyncs.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }

    async fn catalog_status_watch(
        &self,
        transport: &impl WatchTransport,
        request: IdentityRequest,
    ) -> Result<(), HandlerError> {
        let notified = self.watches.catalog_changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut since = request
            .token
            .as_ref()
            .and_then(Value::as_str)
            .and_then(|token| {
                let (incarnation, generation) = token.rsplit_once(':')?;
                let generation = generation.parse::<u64>().ok()?;
                (incarnation == self.incarnation && token == format!("{incarnation}:{generation}"))
                    .then_some(generation)
            });
        loop {
            let (generation, changes) = {
                let history = self.watches.catalog.lock().unwrap();
                let changes = since
                    .filter(|seq| {
                        *seq <= history.generation
                            && history
                                .batches
                                .front()
                                .is_none_or(|(first, _)| *seq >= first - 1)
                    })
                    .map(|seq| {
                        history
                            .batches
                            .iter()
                            .filter(|(generation, _)| *generation > seq)
                            .flat_map(|(_, keys)| keys.iter().cloned())
                            .collect::<BTreeSet<_>>()
                    });
                (history.generation, changes)
            };
            let token = format!("{}:{generation}", self.incarnation);
            let resync = json!({"kind":"resync","token":token});
            match changes {
                None => self.notice(transport, resync.clone(), resync).await?,
                Some(changes) if !changes.is_empty() => {
                    let changes = changes
                        .into_iter()
                        .map(|(name, version)| json!({"name":name,"version":version}))
                        .collect::<Vec<_>>();
                    self.notice(transport,json!({"kind":"changes","token":token,"generation":generation,"changes":changes}),resync).await?;
                }
                Some(_) => {}
            }
            since = Some(generation);
            #[cfg(test)]
            self.health.watch_waiting.notify_waiters();
            (&mut notified).await;
            notified.set(self.watches.catalog_changed.notified());
            notified.as_mut().enable();
        }
    }

    async fn identity_watch(
        &self,
        transport: &impl WatchTransport,
        request: IdentityRequest,
    ) -> Result<(), HandlerError> {
        let notified = self.commits.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let since = request
            .token
            .as_ref()
            .and_then(Value::as_str)
            .and_then(|token| {
                let (incarnation, generation) = token.rsplit_once(':')?;
                let generation = generation.parse::<i64>().ok()?;
                (incarnation == self.incarnation && token == format!("{incarnation}:{generation}"))
                    .then_some(generation)
            });
        let snapshot = self
            .with_agent_store(|store| store.agent_fleet_watch_snapshot(&self.incarnation, since))?;
        let mut previous = fleet_rows(&snapshot.fleet);
        let resync = identity_resync(&snapshot.fleet);
        match snapshot.resume {
            None => self.notice(transport, resync.clone(), resync).await?,
            Some(changed) if !changed.is_empty() => {
                let agents = changed
                    .iter()
                    .filter_map(|(id, version)| {
                        previous.get(id).cloned().map(|mut row| {
                            row["version"] = json!(version);
                            row
                        })
                    })
                    .collect::<Vec<_>>();
                let removed = changed
                    .iter()
                    .filter(|(id, _)| !previous.contains_key(*id))
                    .map(|(id, version)| json!({"agentId":id,"version":version}))
                    .collect::<Vec<_>>();
                self.notice(
                    transport,
                    identity_changes(&snapshot.fleet, agents, removed),
                    resync,
                )
                .await?;
            }
            Some(_) => {}
        }
        loop {
            #[cfg(test)]
            self.health.watch_waiting.notify_waiters();
            (&mut notified).await;
            notified.set(self.commits.notified());
            notified.as_mut().enable();
            debounce(&self.commits).await;
            let snapshot = self.with_agent_store(|store| {
                store.agent_fleet_watch_snapshot(&self.incarnation, None)
            })?;
            let current = fleet_rows(&snapshot.fleet);
            let generation = &snapshot.fleet["generation"];
            let agents = current
                .iter()
                .filter(|(id, row)| previous.get(*id) != Some(*row))
                .map(|(_, row)| {
                    let mut row = row.clone();
                    row["version"] = generation.clone();
                    row
                })
                .collect::<Vec<_>>();
            let removed = previous
                .keys()
                .filter(|id| !current.contains_key(*id))
                .map(|id| json!({"agentId":id,"version":generation}))
                .collect::<Vec<_>>();
            if !agents.is_empty() || !removed.is_empty() {
                self.notice(
                    transport,
                    identity_changes(&snapshot.fleet, agents, removed),
                    identity_resync(&snapshot.fleet),
                )
                .await?;
                previous = current;
            }
        }
    }

    async fn where_watch(
        &self,
        transport: &impl WatchTransport,
        request: WhereRequest,
    ) -> Result<(), HandlerError> {
        let ids = request
            .agent_ids
            .map(|ids| ids.into_iter().collect::<BTreeSet<_>>());
        let notified = self.watches.changed.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut cursor = request.cursor;
        loop {
            let (seq, changes) = {
                let history = self.watches.history.lock().unwrap();
                let resumable = cursor.as_ref().is_some_and(|cursor| {
                    cursor.incarnation == self.incarnation
                        && cursor.seq <= history.seq
                        && history
                            .batches
                            .front()
                            .is_none_or(|first| cursor.seq >= first.seq - 1)
                });
                let mut changes = BTreeMap::new();
                let mut safe = resumable;
                if let Some(cursor) = &cursor {
                    for batch in history
                        .batches
                        .iter()
                        .filter(|batch| batch.seq > cursor.seq)
                    {
                        safe &= !batch.all_agents;
                        for (id, row) in &batch.entries {
                            if ids.as_ref().is_none_or(|ids| ids.contains(id)) {
                                safe &= !row.is_null();
                                changes.insert(id.clone(), row.clone());
                            }
                        }
                    }
                }
                (history.seq, safe.then_some(changes))
            };
            let after = json!({"incarnation":self.incarnation,"seq":seq});
            let resync = json!({"kind":"resync","cursor":after});
            match changes {
                None => self.notice(transport, resync.clone(), resync).await?,
                Some(changes) if !changes.is_empty() => {
                    let mut entries = changes.into_values().collect::<Vec<_>>();
                    for row in &mut entries {
                        if let Some(moving) = row.get_mut("moving") {
                            crate::move_ops::elapsed(moving, (self.clock)());
                        }
                    }
                    self.notice(
                        transport,
                        json!({"kind":"changes","cursor":after,"where":entries}),
                        resync,
                    )
                    .await?;
                }
                Some(_) => {}
            }
            cursor = Some(Cursor {
                incarnation: self.incarnation.clone(),
                seq,
            });
            #[cfg(test)]
            self.health.watch_waiting.notify_waiters();
            (&mut notified).await;
            notified.set(self.watches.changed.notified());
            notified.as_mut().enable();
            debounce(&self.watches.changed).await;
        }
    }

    fn sessions_resync(&self, request: &SessionsRequest) -> Result<Value, HandlerError> {
        let page = self.with_agent_store(|store| {
            store.agent_sessions_filtered(
                &request.agent_id,
                None,
                crate::sessions::SESSIONS_LIMIT_DEFAULT,
                request.open,
            )
        })?;
        Ok(if page.next.is_some() {
            json!({"kind":"resync","truncated":true})
        } else {
            json!({"kind":"resync","sessions":self.session_rows(page.sessions)})
        })
    }

    async fn sessions_watch(
        &self,
        transport: &impl WatchTransport,
        request: SessionsRequest,
        mut receiver: broadcast::Receiver<Arc<Vec<SessionChangeKey>>>,
        initial: Value,
    ) -> Result<(), HandlerError> {
        let truncated = json!({"kind":"resync","truncated":true});
        self.notice(transport, initial, truncated.clone()).await?;
        loop {
            let mut keys = BTreeSet::new();
            let mut lagged = false;
            while keys.is_empty() && !lagged {
                #[cfg(test)]
                self.health.watch_waiting.notify_waiters();
                session_keys(
                    receiver.recv().await,
                    &request.agent_id,
                    &mut keys,
                    &mut lagged,
                )?;
            }
            let ceiling = tokio::time::sleep(CEILING);
            let quiet = tokio::time::sleep(QUIET);
            tokio::pin!(ceiling, quiet);
            loop {
                tokio::select! {
                    biased;
                    _ = &mut ceiling => break,
                    _ = &mut quiet => break,
                    batch = receiver.recv() => {
                        if session_keys(batch, &request.agent_id, &mut keys, &mut lagged)? {
                            quiet.as_mut().reset(tokio::time::Instant::now() + QUIET);
                        }
                    }
                }
            }
            if lagged {
                self.notice(
                    transport,
                    self.sessions_resync(&request)?,
                    truncated.clone(),
                )
                .await?;
                continue;
            }
            let keys = keys.into_iter().collect::<Vec<_>>();
            let rows = self.with_agent_store(|store| {
                Ok(store.changed_agent_sessions(&request.agent_id, &keys, request.open)?)
            })?;
            let present = rows
                .iter()
                .filter_map(|row| Some((row.harness.clone()?, row.session.clone()?)))
                .collect::<BTreeSet<_>>();
            let removed = keys
                .into_iter()
                .filter(|key| !present.contains(key))
                .map(|(harness, session)| json!({"harness":harness,"session":session}))
                .collect::<Vec<_>>();
            self.notice(
                transport,
                json!({"kind":"changes","sessions":self.session_rows(rows),"removed":removed}),
                truncated.clone(),
            )
            .await?;
        }
    }
}

fn session_keys(
    batch: Result<Arc<Vec<SessionChangeKey>>, broadcast::error::RecvError>,
    agent: &str,
    keys: &mut BTreeSet<(String, String)>,
    lagged: &mut bool,
) -> Result<bool, HandlerError> {
    match batch {
        Ok(batch) => {
            let mut relevant = false;
            for key in batch.iter().filter(|key| key.agent_id == agent) {
                relevant = true;
                keys.insert((key.harness.clone(), key.session_id.clone()));
            }
            Ok(relevant)
        }
        Err(broadcast::error::RecvError::Lagged(_)) => {
            *lagged = true;
            Ok(true)
        }
        Err(broadcast::error::RecvError::Closed) => Err(HandlerError::new(
            "stream_closed",
            "session change source closed",
        )),
    }
}

fn fleet_rows(fleet: &Value) -> BTreeMap<String, Value> {
    fleet["agents"]
        .as_array()
        .expect("fleet rows")
        .iter()
        .map(|row| {
            (
                row["agentId"].as_str().expect("agent id").to_owned(),
                row.clone(),
            )
        })
        .collect()
}

fn identity_resync(fleet: &Value) -> Value {
    json!({"kind":"resync","token":fleet["token"]})
}

fn identity_changes(fleet: &Value, agents: Vec<Value>, removed: Vec<Value>) -> Value {
    json!({"kind":"changes","token":fleet["token"],"generation":fleet["generation"],"logPosition":fleet["logPosition"],"agents":agents,"removed":removed})
}

async fn debounce(wake: &Notify) {
    let ceiling = tokio::time::sleep(CEILING);
    tokio::pin!(ceiling);
    loop {
        tokio::select! {
            biased;
            _ = &mut ceiling => return,
            _ = tokio::time::sleep(QUIET) => return,
            _ = wake.notified() => {}
        }
    }
}

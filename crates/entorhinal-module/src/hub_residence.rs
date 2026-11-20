//! The watched residence projection and its two local write paths.
#![allow(dead_code)] // Activated by the hub cutover coordinator.

use std::{
    collections::{HashMap, HashSet},
    sync::{atomic::Ordering, Arc, RwLock, Weak},
    time::Duration,
};

use async_nats::jetstream::kv::{Entry, Operation};
use async_trait::async_trait;
use entorhinal_core::{claim_cache::HubClaimContext, hub_store::HubKvCached, RegistryStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::{watch, Notify},
    time::{timeout_at, Instant},
};

use crate::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    encode_result,
    fleet_record::{UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_identity_log::{AcceptedKv, IdentityLog, LogTransport},
    hub_membership::Sight,
    hub_transport::{check_kv_size, Feed, FeedEvent, KvDelivery},
    hub_write::{self, HubSql, LogReplan, Payload, Replanned, Step, Trust},
    role_leases, HandlerError, HealthGauges, ProjectsHandler, RouteKey,
};

pub(super) const TAKE: &str = "agent.take";
const CREATE: &str = "agent.create";
const CREATE_KV: &str = "residence.create";
const BUDGET: Duration = Duration::from_secs(5);
mod moves;
mod role_control;
#[cfg(test)]
mod tests;
pub(super) use moves::{Position as MovePosition, Transfer as MoveTransfer};
use role_control::role_key_name;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Record {
    v: u64,
    holder: String,
    #[serde(default)]
    holder_ep: u64,
    gen: i64,
    writer: String,
    forced: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    seeded: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    cut_id: Option<String>,
}

impl Record {
    fn new(box_id: &str, episode: u64, generation: i64, forced: bool) -> Self {
        Self {
            v: 1,
            holder: box_id.into(),
            holder_ep: episode,
            gen: generation,
            writer: box_id.into(),
            forced,
            seeded: None,
            cut_id: None,
        }
    }

    fn step(self, name: &str, key: String, expected: u64) -> Step {
        Step {
            name: name.into(),
            key,
            expected,
            payload: Payload::Record {
                kind: "residence".into(),
                body: json!(self),
                snapshot_id: None,
            },
        }
    }
}

/// The snapshot installer supplies these claims from the applied image, never
/// by consulting cached claims from the legacy backend. A generation-zero image row
/// identifies a migrated agent that anybody may claim at revision zero.
pub(super) trait SnapshotClaims: Send + Sync {
    fn claim(&self, agent: &str) -> Option<(Option<String>, i64)>;
    fn hub_ready(&self) -> bool;
    fn required_keys(&self) -> &[String] {
        &[]
    }
    fn role(&self, _name: &str) -> Option<(Option<String>, i64)> {
        None
    }
}

#[derive(Clone)]
pub(super) struct Runtime {
    slot: Arc<std::sync::Mutex<Option<Running>>>,
    health: Arc<HealthGauges>,
    clock: fn() -> i64,
    role_leases: Arc<role_leases::Leases>,
    move_wake: Arc<crate::move_driver::MoveWake>,
}

#[async_trait]
trait Backend: Send + Sync {
    fn membership_ready(&self) -> bool;
    fn current_member(&self, holder: &str, applied: u64) -> Option<u64>;
    fn require_known_member(
        &self,
        holder: &str,
        episode: u64,
        applied: u64,
    ) -> Result<(), HandlerError>;
    fn anchor_changes(&self) -> watch::Receiver<u64>;
    fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool;
    async fn applied(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn writer_episode(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn kv_fence_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<hub_write::KvEntry>, HandlerError>;
    async fn wait_through(&self, head: u64, deadline: Instant) -> Result<(), HandlerError>;
    async fn verify(
        &self,
        key: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError>;
    async fn record_applied(
        &self,
        record: &VerifiedRecord,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError>;
    async fn write(
        &self,
        request: &hub_write::Request<'_>,
        step: &Step,
        next: Option<hub_write::NextStep<'_>>,
        replan: Option<&dyn LogReplan>,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError>;
}

#[async_trait]
impl<T: LogTransport, U: Trust + Send + Sync + 'static> Backend for IdentityLog<T, U> {
    fn membership_ready(&self) -> bool {
        self.membership_ready()
    }
    fn current_member(&self, holder: &str, applied: u64) -> Option<u64> {
        self.current_member_episode(holder, applied)
    }
    fn require_known_member(
        &self,
        holder: &str,
        episode: u64,
        applied: u64,
    ) -> Result<(), HandlerError> {
        self.require_known_member(holder, episode, applied)
    }
    fn anchor_changes(&self) -> watch::Receiver<u64> {
        self.anchor_changes()
    }
    fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool {
        self.episode_closed(holder, episode, applied)
    }
    async fn applied(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.applied_sequence(deadline).await
    }
    async fn writer_episode(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.writer_episode(deadline).await
    }
    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.log_head(deadline).await
    }
    async fn kv_fence_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<hub_write::KvEntry>, HandlerError> {
        self.kv_fence_entry(key, deadline).await
    }
    async fn wait_through(&self, head: u64, deadline: Instant) -> Result<(), HandlerError> {
        crate::hub_identity_log::wait_for_applied(self, head, deadline).await
    }
    async fn verify(
        &self,
        key: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        // The generation is inside the sealed body. After opening it, the
        // residence rule checks whether a closed episode has a predecessor.
        self.verify_kv(key, bytes, Sight::FirstOrAfterGap, deadline)
            .await
    }
    async fn record_applied(
        &self,
        record: &VerifiedRecord,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.record_residence_applied(record, revision, deadline)
            .await
    }
    async fn write(
        &self,
        request: &hub_write::Request<'_>,
        step: &Step,
        next: Option<hub_write::NextStep<'_>>,
        replan: Option<&dyn LogReplan>,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        self.write_residence(request, step, next, replan, deadline)
            .await
    }
}

#[derive(Default)]
struct Watched {
    initial: bool,
    incarnation: u64,
    revisions: HashMap<String, u64>,
    role_seen: HashMap<String, (u64, u64)>,
    held: HashSet<String>,
    move_times: HashMap<String, i64>,
}

pub(super) struct Residence {
    backend: Arc<dyn Backend>,
    snapshot: Arc<dyn SnapshotClaims>,
    box_id: String,
    base: i64,
    sql: HubSql,
    health: Arc<HealthGauges>,
    commits: Arc<Notify>,
    watched: RwLock<Watched>,
    clock: fn() -> i64,
    role_leases: Weak<role_leases::Leases>,
    link: Notify,
    move_wake: Weak<crate::move_driver::MoveWake>,
}

pub(super) struct Running {
    pub service: Arc<Residence>,
    watch: tokio::task::JoinHandle<()>,
    recovery: tokio::task::JoinHandle<()>,
    seed: Option<tokio::task::JoinHandle<()>>,
    epochs: Option<(
        Arc<dyn crate::hub_epoch::RemovalIntake>,
        tokio::task::JoinHandle<()>,
    )>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.watch.abort();
        self.recovery.abort();
        if let Some(task) = &self.seed {
            task.abort();
        }
        if let Some((_, task)) = &self.epochs {
            task.abort();
        }
    }
}

impl ProjectsHandler {
    pub(super) fn hub_backend_selected(&self) -> bool {
        self.health
            .hub_membership
            .load(std::sync::atomic::Ordering::Acquire)
            != 0
    }

    /// Install the residence consumer on the activated hub backend. The caller
    /// supplies the live KV feed and the installed snapshot image.
    pub(super) fn activate_hub_residence<T: LogTransport, U: Trust + Send + Sync + 'static>(
        &self,
        log: Arc<IdentityLog<T, U>>,
        feed: Feed<KvDelivery>,
        snapshot: Arc<dyn SnapshotClaims>,
    ) -> Result<Arc<Residence>, HandlerError> {
        self.hub_residence_runtime().activate(log, feed, snapshot)
    }

    pub(super) fn hub_residence_runtime(&self) -> Runtime {
        Runtime {
            slot: self.hub_residence.clone(),
            health: self.health.clone(),
            clock: self.clock,
            role_leases: self.role_leases.clone(),
            move_wake: self.move_wake.clone(),
        }
    }
}

impl Runtime {
    pub(super) fn activate<T: LogTransport, U: Trust + Send + Sync + 'static>(
        &self,
        log: Arc<IdentityLog<T, U>>,
        feed: Feed<KvDelivery>,
        snapshot: Arc<dyn SnapshotClaims>,
    ) -> Result<Arc<Residence>, HandlerError> {
        let mut running = self.slot.lock().unwrap();
        if running.is_some() {
            return Err(HandlerError::new(
                "hub_not_ready",
                "hub residence watch already installed",
            ));
        }
        let epochs = crate::hub_epoch::Controller::new(log.clone())
            .zip(log.epoch_watch())
            .map(|(controller, feed)| {
                let watcher = controller.clone();
                let task = tokio::spawn(async move { watcher.watch(feed).await });
                (controller as Arc<dyn crate::hub_epoch::RemovalIntake>, task)
            });
        let service = Arc::new(Residence {
            sql: log.sql.clone(),
            box_id: log.box_id.to_string(),
            base: log.base,
            commits: log.commits.clone(),
            backend: log,
            snapshot,
            health: self.health.clone(),
            watched: RwLock::default(),
            clock: self.clock,
            role_leases: Arc::downgrade(&self.role_leases),
            link: Notify::new(),
            move_wake: Arc::downgrade(&self.move_wake),
        });
        *running = Some(Running {
            watch: tokio::spawn(service.clone().watch(feed)),
            recovery: tokio::spawn(service.clone().recover()),
            seed: None,
            epochs,
            service: service.clone(),
        });
        self.role_leases.attach(Some(service.clone()));
        self.move_wake.attach_hub(Some(&service));
        Ok(service)
    }

    pub(super) fn start_seeds<T: LogTransport, U: Trust + Send + Sync + 'static>(
        &self,
        log: Arc<IdentityLog<T, U>>,
        seeds: Arc<crate::hub_switch::snapshot::Seeds>,
    ) {
        let mut slot = self.slot.lock().unwrap();
        let Some(running) = slot.as_mut() else {
            return;
        };
        running.seed = Some(tokio::spawn(async move {
            loop {
                let notified = log.commits.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if !log.membership_ready() {
                    notified.await;
                    continue;
                }
                match seeds.write(&log, Instant::now() + BUDGET).await {
                    Ok(()) => return,
                    Err(error) if error.code == "store_unavailable" => return,
                    Err(error) => tracing::debug!(code = error.code, "snapshot seed deferred"),
                }
                tokio::select! { _ = notified => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            }
        }));
    }
}

impl ProjectsHandler {
    pub(super) async fn hub_device_removed(
        &self,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let intake = self
            .hub_residence
            .lock()
            .unwrap()
            .as_ref()
            .and_then(|running| running.epochs.as_ref().map(|(intake, _)| intake.clone()))
            .ok_or_else(not_ready)?;
        intake.deliver(bytes, deadline).await
    }

    pub(super) fn residence(&self) -> Option<Arc<Residence>> {
        self.hub_residence
            .lock()
            .unwrap()
            .as_ref()
            .map(|running| running.service.clone())
    }

    pub(super) async fn stop_hub_residence(&self) {
        self.role_leases.attach(None);
        self.move_wake.attach_hub(None);
        let running = self.hub_residence.lock().unwrap().take();
        if let Some(mut running) = running {
            running.watch.abort();
            running.recovery.abort();
            if let Some(seed) = &running.seed {
                seed.abort();
            }
            if let Some((_, epoch)) = &running.epochs {
                epoch.abort();
            }
            let _ = (&mut running.watch).await;
            let _ = (&mut running.recovery).await;
            if let Some(seed) = &mut running.seed {
                let _ = seed.await;
            }
            if let Some((_, epoch)) = &mut running.epochs {
                let _ = epoch.await;
            }
        }
        self.hub_sql.wait().await;
    }
}

fn key(agent: &str) -> String {
    format!("residence.{agent}")
}

fn bad_record(message: &str) -> HandlerError {
    HandlerError::new("record_invalid", message)
}
fn not_ready() -> HandlerError {
    HandlerError::new("hub_not_ready", "hub residence has not caught up")
}
fn deferred(error: &HandlerError) -> bool {
    matches!(
        error.code.as_str(),
        "hub_unavailable"
            | "hub_membership_stale"
            | "epoch_key_unavailable"
            | "vault_unavailable"
            | "credential_unavailable"
            | "storage_error"
            | "hub_sql_timeout"
            | "store_unavailable"
            | "writer_unknown"
    )
}

impl Residence {
    pub(super) fn ready(&self) -> bool {
        self.ready_with(&self.watched.read().unwrap())
    }

    fn ready_with(&self, watched: &Watched) -> bool {
        self.snapshot.hub_ready()
            && self.backend.membership_ready()
            && watched.initial
            && self
                .snapshot
                .required_keys()
                .iter()
                .all(|key| watched.revisions.contains_key(key) && !watched.held.contains(key))
    }

    fn fresh(&self, agent: &str, revision: Option<u64>) -> bool {
        if !self.ready() {
            return false;
        }
        let key = key(agent);
        let watched = self.watched.read().unwrap();
        watched.initial
            && !watched.held.contains(&key)
            && revision.is_none_or(|revision| watched.revisions.get(&key) == Some(&revision))
    }

    pub(super) fn with_context<R>(&self, read: impl FnOnce(&HubClaimContext<'_>) -> R) -> R {
        read(&HubClaimContext {
            local_box: &self.box_id,
            fresh: &|agent, revision| self.fresh(agent, revision),
            holder_current: &|holder, episode, position| {
                self.backend.membership_ready()
                    && position
                        .checked_sub(self.base)
                        .and_then(|sequence| self.backend.current_member(holder, sequence as u64))
                        == Some(episode)
            },
            seed_expected: &|agent| {
                self.snapshot
                    .claim(agent)
                    .is_some_and(|(_, generation)| generation > 0)
            },
        })
    }
}

pub(super) fn with_claim_context<R>(
    residence: Option<&Residence>,
    hub_selected: bool,
    read: impl FnOnce(Option<&HubClaimContext<'_>>) -> R,
) -> R {
    match residence {
        Some(residence) => residence.with_context(|context| read(Some(context))),
        None if hub_selected => read(Some(&HubClaimContext {
            local_box: "",
            fresh: &|_, _| false,
            holder_current: &|_, _, _| false,
            seed_expected: &|_| false,
        })),
        None => read(None),
    }
}

impl Residence {
    async fn agent_creator(
        &self,
        agent: &str,
        through: i64,
        deadline: Instant,
    ) -> Result<Option<(String, u64)>, HandlerError> {
        let agent = agent.to_owned();
        self.sql
            .run(deadline, "hub_agent_creator", move |store| {
                Ok(store.hub_agent_creator_through(&agent, through)?)
            })
            .await
    }

    async fn created_agent(
        &self,
        request: &str,
        deadline: Instant,
    ) -> Result<Option<(i64, entorhinal_core::agent::AgentRow)>, HandlerError> {
        let request = request.to_owned();
        self.sql
            .run(deadline, "hub_created_agent", move |store| {
                Ok(store.hub_created_agent(&request)?)
            })
            .await
    }

    fn reset_role_watch(&self) {
        let mut watched = self.watched.write().unwrap();
        watched.initial = false;
        watched.incarnation += 1;
        watched.revisions.clear();
        watched.move_times.clear();
        self.link.notify_waiters();
    }

    fn finish_initial_watch(&self) {
        let mut watched = self.watched.write().unwrap();
        if !watched.initial {
            watched.initial = true;
            self.link.notify_waiters();
            if let Some(wake) = self.move_wake.upgrade() {
                wake.hub_changed();
            }
        }
    }

    fn role_changed(&self, key: &str) {
        if let Some((name, leases)) = role_key_name(key).zip(self.role_leases.upgrade()) {
            leases.changed(name);
        }
    }

    async fn watch(self: Arc<Self>, mut feed: Feed<KvDelivery>) {
        let mut pending: HashMap<String, HeldDelivery> = HashMap::new();
        let mut anchors = self.backend.anchor_changes();
        let mut retry = tokio::time::interval(Duration::from_secs(1));
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // The log can apply a computer's removal while this watch is still
        // judging a role record that replaces that computer as holder, so
        // re-judge the record when a commit arrives.
        let committed = self.commits.notified();
        tokio::pin!(committed);
        committed.as_mut().enable();
        loop {
            if !self.sql.available() {
                return;
            }
            let lease_retry = pending
                .values()
                .filter_map(|entry| entry.lease_retry_at)
                .min();
            tokio::select! {
                event = feed.next_event() => match event {
                    FeedEvent::Connected | FeedEvent::Disconnected => {
                        pending.retain(|key, _| role_key_name(key).is_none() && !key.starts_with("move.") && !key.starts_with("node."));
                        self.reset_role_watch();
                    }
                    FeedEvent::Item(KvDelivery::Empty) => self.finish_initial_watch(),
                    FeedEvent::Item(KvDelivery::Entry(entry)) => {
                        let seen_current = entry.seen_current;
                        if pending.get(&entry.key).is_none_or(|old| old.entry.revision <= entry.revision) {
                            let arrival_head = pending.remove(&entry.key)
                                .filter(|old| old.entry.revision == entry.revision)
                                .and_then(|old| old.arrival_head);
                            self.observe_or_hold(HeldDelivery { entry, arrival_head, waiting_for_anchor: false, lease_retry_at: None }, &mut pending).await;
                        }
                        if seen_current {
                            self.finish_initial_watch();
                        }
                    }
                },
                Ok(()) = anchors.changed() => {
                    let waiting: Vec<_> = pending.extract_if(|_, entry| entry.waiting_for_anchor).collect();
                    for (_, entry) in waiting {
                        self.observe_or_hold(entry, &mut pending).await;
                    }
                }
                _ = retry.tick(), if pending.values().any(|entry| !entry.waiting_for_anchor && entry.lease_retry_at.is_none()) => {
                    let retrying: Vec<_> = pending.extract_if(|_, entry| !entry.waiting_for_anchor && entry.lease_retry_at.is_none()).collect();
                    for (_, entry) in retrying {
                        self.observe_or_hold(entry, &mut pending).await;
                    }
                }
                _ = &mut committed, if lease_retry.is_some() => {
                    committed.set(self.commits.notified());
                    committed.as_mut().enable();
                    let roles: Vec<_> = pending.extract_if(|_, entry| entry.lease_retry_at.is_some()).collect();
                    for (_, entry) in roles {
                        self.observe_or_hold(entry, &mut pending).await;
                    }
                }
                _ = async {
                    let remaining = lease_retry.expect("armed lease retry").saturating_sub(self.role_clock_ms());
                    // The executor's time source can pause in suspend, unlike the lease clock.
                    tokio::time::sleep(Duration::from_millis(remaining.min(1_000))).await;
                }, if lease_retry.is_some() => {
                    let now = self.role_clock_ms();
                    let roles: Vec<_> = pending.extract_if(|_, entry| entry.lease_retry_at.is_some_and(|at| at <= now)).collect();
                    for (_, entry) in roles {
                        self.observe_or_hold(entry, &mut pending).await;
                    }
                }
            }
        }
    }

    async fn observe_or_hold(
        &self,
        mut delivery: HeldDelivery,
        pending: &mut HashMap<String, HeldDelivery>,
    ) {
        let key = delivery.entry.key.clone();
        let deadline = Instant::now() + BUDGET;
        let mut result = self
            .observe_at(&delivery.entry, &mut delivery.arrival_head, deadline)
            .await;
        delivery.lease_retry_at = result.as_ref().err().and_then(|error| {
            (error.code == "role_lease_awaiting_expiry")
                .then(|| error.detail.as_ref()?.get("until_ms")?.as_u64())
                .flatten()
        });
        delivery.waiting_for_anchor = result
            .as_ref()
            .is_err_and(|error| error.code == "writer_unknown");
        if delivery.waiting_for_anchor || delivery.lease_retry_at.is_some() {
            let held_key = key.clone();
            let revision = delivery.entry.revision;
            if let Err(error) = self
                .sql
                .run(deadline, "hold_hub_kv", move |store| {
                    Ok(store.hold_hub_kv(&held_key, revision)?)
                })
                .await
            {
                delivery.waiting_for_anchor = false;
                delivery.lease_retry_at = None;
                result = Err(error);
            }
        }
        let mut watched = self.watched.write().unwrap();
        if let Err(error) = result {
            // Losing the log or vault is not evidence that the value is bad.
            // Fence only the affected key and keep consuming the watch.
            watched.held.insert(delivery.entry.key.clone());
            tracing::debug!(target: "hub_residence", key = delivery.entry.key, code = error.code, "holding fleet KV delivery");
            pending.insert(delivery.entry.key.clone(), delivery);
        } else {
            watched.held.remove(&delivery.entry.key);
            if key.starts_with("move.") {
                if delivery.entry.operation == Operation::Put
                    && watched.revisions.get(&key) == Some(&delivery.entry.revision)
                {
                    if let Ok(ms) =
                        i64::try_from(delivery.entry.created.unix_timestamp_nanos() / 1_000_000)
                    {
                        watched.move_times.insert(key.clone(), ms);
                    }
                } else if delivery.entry.operation != Operation::Put {
                    watched.move_times.remove(&key);
                }
            }
        }
        drop(watched);
        // Notify only the affected role. Renewal events must not wake every
        // candidate on the machine.
        self.role_changed(&key);
        if key.starts_with("move.") || key.starts_with("residence.") {
            if let Some(wake) = self.move_wake.upgrade() {
                wake.hub_changed();
            }
        }
    }

    #[cfg(test)]
    pub(super) async fn observe(
        &self,
        entry: &Entry,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.observe_at(entry, &mut None, deadline).await
    }

    async fn observe_at(
        &self,
        entry: &Entry,
        arrival_head: &mut Option<u64>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let key = entry.key.as_str();
        let revision = entry.revision;
        let value: &[u8] = match entry.operation {
            Operation::Put => &entry.value,
            Operation::Delete => b"DEL",
            Operation::Purge => b"PURGE",
        };
        let old = self.sql.cached(key, deadline).await?;
        if let Some(old) = &old {
            if revision < old.revision || (revision == old.revision && old.integrity_failed) {
                return Ok(());
            }
            if revision == old.revision && old.accepted_revision == Some(revision) {
                if old.value.as_deref() == Some(value)
                    && old.body.is_some() == (entry.operation == Operation::Put)
                {
                    self.accept(key, revision, value, old.body.as_ref(), deadline)
                        .await?;
                } else {
                    self.refuse(key, revision, "revision_bytes_differ", deadline)
                        .await?;
                }
                return Ok(());
            }
        }
        if entry.operation != Operation::Put {
            if self.delete_allowed(key, old.as_ref(), deadline).await? {
                self.accept(key, revision, value, None, deadline).await?;
            } else {
                self.refuse(key, revision, "delete_forbidden", deadline)
                    .await?;
            }
            return Ok(());
        }
        if key.starts_with("move.") || key.starts_with("node.") {
            match self
                .judge_move_signal(key, value, old.as_ref(), deadline)
                .await
            {
                Ok(accepted) => {
                    self.accept(key, revision, value, Some(&accepted.body), deadline)
                        .await?;
                    self.backend
                        .record_applied(&accepted.record, revision, deadline)
                        .await
                        .ok();
                    return Ok(());
                }
                Err(error) if deferred(&error) => return Err(error),
                Err(error) => return self.refuse(key, revision, &error.code, deadline).await,
            }
        }
        let (name, role) = if let Some(role) = key.strip_prefix("role.") {
            if !role_leases::valid_role(role) {
                return self
                    .refuse(key, revision, "invalid_role_name", deadline)
                    .await;
            }
            (role, true)
        } else if let Some(agent) = key.strip_prefix("residence.") {
            if !entorhinal_core::agent::valid_agent_id(agent) {
                return self
                    .refuse(key, revision, "invalid_agent_id", deadline)
                    .await;
            }
            (agent, false)
        } else if let Some(name) = role_key_name(key) {
            return self
                .observe_role_signal(key, name, value, revision, deadline)
                .await;
        } else {
            return Ok(());
        };
        let result = if role {
            self.judge_role(name, key, value, old.as_ref(), deadline)
                .await
        } else {
            self.judge(name, key, value, old.as_ref(), arrival_head, deadline)
                .await
        };
        match result {
            Ok(accepted) => {
                self.accept(key, revision, value, Some(&accepted.body), deadline)
                    .await?;
                if let Err(error) = self
                    .backend
                    .record_applied(&accepted.record, revision, deadline)
                    .await
                {
                    tracing::debug!(target: "hub_residence", key, revision, code = error.code, "could not settle applied residence");
                }
                Ok(())
            }
            Err(error) if deferred(&error) || error.code == "role_lease_awaiting_expiry" => {
                Err(error)
            }
            Err(error) => self.refuse(key, revision, &error.code, deadline).await,
        }
    }

    async fn accept(
        &self,
        key: &str,
        revision: u64,
        value: &[u8],
        body: Option<&Value>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let saved_key = key.to_owned();
        let value = value.to_vec();
        let body = body.cloned();
        let now = (self.clock)();
        if self
            .sql
            .run(deadline, "record_hub_kv", move |store| {
                Ok(
                    store.record_hub_kv(
                        &saved_key,
                        revision,
                        Some((&value, body.as_ref())),
                        now,
                    )?,
                )
            })
            .await?
        {
            let mut watched = self.watched.write().unwrap();
            watched.revisions.insert(key.into(), revision);
            if key.starts_with("role.") {
                let seen = watched
                    .role_seen
                    .entry(key.into())
                    .or_insert((revision, self.role_clock_ms()));
                if seen.0 != revision {
                    *seen = (revision, self.role_clock_ms());
                }
            }
        }
        Ok(())
    }

    async fn refuse(
        &self,
        key: &str,
        revision: u64,
        code: &str,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let saved_key = key.to_owned();
        let now = (self.clock)();
        if self
            .sql
            .run(deadline, "record_hub_kv", move |store| {
                Ok(store.record_hub_kv(&saved_key, revision, None, now)?)
            })
            .await?
        {
            self.watched.write().unwrap().revisions.remove(key);
            self.health
                .hub
                .kv_records_refused
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(target: "hub_residence", key, revision, code, "refused fleet KV revision");
        }
        Ok(())
    }

    async fn delete_allowed(
        &self,
        key: &str,
        old: Option<&HubKvCached>,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        if let Some(mint) = key
            .strip_prefix("wrap.")
            .and_then(|rest| rest.split('.').nth(1))
        {
            let epoch = self
                .sql
                .cached("epoch", deadline)
                .await?
                .and_then(|row| row.body);
            return Ok(epoch
                .as_ref()
                .and_then(committed_mint)
                .is_some_and(|current| current != mint));
        }
        Ok(delete_allowed_without_epoch(
            key,
            old.and_then(|row| row.body.as_ref()),
        ))
    }

    async fn judge(
        &self,
        agent: &str,
        key: &str,
        bytes: &[u8],
        old: Option<&HubKvCached>,
        arrival_head: &mut Option<u64>,
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        check_kv_size(key, bytes.len())?;
        let mut accepted = self.backend.verify(key, bytes, deadline).await?;
        let body: Record =
            serde_json::from_value(accepted.body.clone()).map_err(crate::invalid_params)?;
        let previous = old
            .and_then(|old| old.body.as_ref())
            .cloned()
            .map(serde_json::from_value::<Record>)
            .transpose()
            .map_err(crate::invalid_params)?;
        let ordinary_first = body.gen == 1 && !body.forced && body.seeded != Some(true);
        let mut creator = if ordinary_first {
            self.agent_creator(agent, i64::MAX, deadline).await?
        } else {
            None
        };
        let mut seed = self.snapshot.claim(agent);
        let predecessor_current = body.forced
            && previous.as_ref().is_some_and(|prior| {
                prior.gen.checked_add(1) == Some(body.gen)
                    && self.backend.current_member(&prior.holder, accepted.applied)
                        == Some(prior.holder_ep)
            });
        if arrival_head.is_some()
            || (ordinary_first && creator.is_none() && seed.is_none())
            || (body.seeded == Some(true) && seed.is_none())
            || predecessor_current
        {
            // KV can deliver a create, seed or forced take before the log
            // delivers the entry that makes it admissible.
            let head = if let Some(head) = *arrival_head {
                head
            } else {
                let head = self.backend.log_head(deadline).await?;
                *arrival_head = Some(head);
                head
            };
            self.backend.wait_through(head, deadline).await?;
            if ordinary_first {
                let through = i64::try_from(head)
                    .ok()
                    .and_then(|head| self.base.checked_add(head))
                    .ok_or_else(|| bad_record("hub log head is out of range"))?;
                creator = self.agent_creator(agent, through, deadline).await?;
            }
            seed = self.snapshot.claim(agent);
            accepted = self.backend.verify(key, bytes, deadline).await?;
        }
        let header = accepted.record.header();
        if body.writer != header.writer
            || body.v != 1
            || body.gen < 1
            || (body.forced && body.seeded == Some(true))
        {
            return Err(bad_record("invalid residence body"));
        }
        let consecutive = previous
            .as_ref()
            .filter(|old| old.gen.checked_add(1) == Some(body.gen));
        let is_current = |holder: &str, episode: u64| {
            self.backend.current_member(holder, accepted.applied) == Some(episode)
        };
        let writer_current = is_current(&header.writer, header.writer_ep);
        let historical =
            self.backend
                .episode_closed(&header.writer, header.writer_ep, accepted.applied)
                && consecutive.is_none();
        if !writer_current && !historical {
            self.backend.require_known_member(
                &header.writer,
                header.writer_ep,
                accepted.applied,
            )?;
        }
        if body.forced {
            if let Some(prior) = consecutive {
                self.backend.require_known_member(
                    &prior.holder,
                    prior.holder_ep,
                    accepted.applied,
                )?;
            }
        }
        if (!writer_current && !historical)
            || previous.as_ref().is_some_and(|old| old.gen >= body.gen)
        {
            return Err(bad_record(
                "residence writer is not eligible or generation did not advance",
            ));
        }
        let self_named = body.holder == header.writer && body.holder_ep == header.writer_ep;
        let valid = if body.forced {
            body.gen > 1
                && self_named
                && consecutive.is_none_or(|old| !is_current(&old.holder, old.holder_ep))
        } else if body.seeded == Some(true) {
            previous.is_none()
                && body.holder_ep == 0
                && seed == Some((Some(body.holder.clone()), body.gen))
        } else if body.gen == 1 {
            self_named
                && (creator == Some((header.writer.clone(), header.writer_ep))
                    || (creator.is_none() && seed == Some((None, 0))))
        } else {
            consecutive
                .is_none_or(|old| header.writer == old.holder && header.writer_ep == old.holder_ep)
        };
        if !valid {
            return Err(bad_record("residence body is not authorized"));
        }
        Ok(accepted)
    }

    async fn judge_role(
        &self,
        name: &str,
        key: &str,
        bytes: &[u8],
        old: Option<&HubKvCached>,
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        check_kv_size(key, bytes.len())?;
        let mut accepted = self.backend.verify(key, bytes, deadline).await?;
        let body: role_leases::Record = serde_json::from_value(accepted.body.clone())
            .map_err(|error| bad_record(&error.to_string()))?;
        let previous = old
            .and_then(|row| row.body.as_ref())
            .cloned()
            .map(serde_json::from_value::<role_leases::Record>)
            .transpose()
            .map_err(crate::invalid_params)?;
        if body.forced
            && previous.as_ref().is_some_and(|prior| {
                prior.gen.checked_add(1) == Some(body.gen)
                    && prior.holder.as_deref().is_some_and(|holder| {
                        self.backend.current_member(holder, accepted.applied)
                            == Some(prior.holder_ep)
                    })
            })
        {
            // A forced write can arrive before the membership entry closing
            // the previous holder's episode reaches this computer's log tail.
            let head = self.backend.log_head(deadline).await?;
            self.backend.wait_through(head, deadline).await?;
            accepted = self.backend.verify(key, bytes, deadline).await?;
        }
        let header = accepted.record.header();
        let current = |holder: &str, episode| {
            self.backend.current_member(holder, accepted.applied) == Some(episode)
        };
        let writer_current = current(&header.writer, header.writer_ep);
        let historical =
            self.backend
                .episode_closed(&header.writer, header.writer_ep, accepted.applied);
        if !writer_current && !historical {
            self.backend.require_known_member(
                &header.writer,
                header.writer_ep,
                accepted.applied,
            )?;
        }
        if body.forced {
            if let Some(prior) = previous
                .as_ref()
                .filter(|prior| prior.gen.checked_add(1) == Some(body.gen))
            {
                if let Some(holder) = &prior.holder {
                    self.backend
                        .require_known_member(holder, prior.holder_ep, accepted.applied)?;
                }
            }
        }
        let seed = self.snapshot.role(name);
        if !role_leases::accepts(
            &body,
            previous.as_ref(),
            header,
            writer_current,
            historical,
            seed,
            current,
        ) {
            return Err(bad_record("role body is not authorized"));
        }
        if let Some((prior, holder)) = previous
            .as_ref()
            .and_then(|prior| prior.holder.as_deref().map(|holder| (prior, holder)))
        {
            // A self-acquire is not an owner-authorized handback. Check the
            // predecessor's local lease window, not the sender's clock or roster.
            if !body.forced
                && prior.gen.checked_add(1) == Some(body.gen)
                && body.holder.as_deref() == Some(&header.writer)
                && holder != header.writer
                && !self
                    .backend
                    .episode_closed(holder, prior.holder_ep, accepted.applied)
            {
                if let Some(until_ms) =
                    self.role_lease_deadline(key, old.and_then(|row| row.accepted_revision))
                {
                    let mut error = HandlerError::new(
                        "role_lease_awaiting_expiry",
                        "previous role lease has not expired here",
                    );
                    error.detail = Some(json!({"until_ms": until_ms}));
                    return Err(error);
                }
            }
        }
        Ok(accepted)
    }

    fn role_clock_ms(&self) -> u64 {
        self.role_leases
            .upgrade()
            .map_or_else(role_leases::monotonic_ms, |leases| leases.now())
    }

    fn role_lease_deadline(&self, key: &str, revision: Option<u64>) -> Option<u64> {
        let now = self.role_clock_ms();
        let mut watched = self.watched.write().unwrap();
        let revision = revision.expect("cached role body has an accepted revision");
        let seen = watched
            .role_seen
            .entry(key.into())
            .or_insert((revision, now));
        if seen.0 != revision {
            *seen = (revision, now);
        }
        let until = seen.1.saturating_add(role_leases::LEASE_MS);
        (until > now).then_some(until)
    }
}

impl Residence {
    // Served role reads are already running inside the tracked read job.
    pub(super) fn role_on_blocking(
        &self,
        store: &RegistryStore,
        name: &str,
    ) -> Result<role_leases::Observation, HandlerError> {
        let key = format!("role.{name}");
        let row = store.hub_kv_cached(&key)?;
        let applied = crate::hub_identity_log::hub_sequence(
            self.base,
            store.identity_log_status()?.last_applied_position,
        )?;
        self.role_observation(name, &key, row, applied, None)
    }

    fn role_observation(
        &self,
        name: &str,
        key: &str,
        row: Option<HubKvCached>,
        applied: u64,
        local_episode: Option<u64>,
    ) -> Result<role_leases::Observation, HandlerError> {
        let watched = self.watched.read().unwrap();
        let revision = row.as_ref().map_or(0, |row| row.revision);
        let ready = self.ready_with(&watched)
            && !watched.held.contains(key)
            && row.as_ref().map_or_else(
                || self.snapshot.role(name).is_none_or(|(_, gen)| gen == 0),
                |row| !row.integrity_failed && watched.revisions.get(key) == Some(&revision),
            );
        let record = row
            .and_then(|row| row.body)
            .map(serde_json::from_value::<role_leases::Record>)
            .transpose()
            .map_err(crate::invalid_params)?;
        Ok(role_leases::Observation {
            ready,
            watch: (
                watched.incarnation,
                self.health
                    .hub_membership_incarnation
                    .load(Ordering::Acquire),
            ),
            revision,
            local_episode,
            holder_current: record.as_ref().is_some_and(|record| {
                record.holder.as_deref().is_some_and(|holder| {
                    crate::hub_membership::box_id(holder)
                        && self.backend.current_member(holder, applied) == Some(record.holder_ep)
                })
            }),
            holder_removed: record.as_ref().is_some_and(|record| {
                record.holder.as_deref().is_some_and(|holder| {
                    crate::hub_membership::box_id(holder)
                        && self
                            .backend
                            .episode_closed(holder, record.holder_ep, applied)
                })
            }),
            record,
        })
    }
}

#[async_trait]
impl role_leases::Backend for Residence {
    fn local_box(&self) -> &str {
        &self.box_id
    }

    async fn role_high_water(&self, name: &str, deadline: Instant) -> Result<i64, HandlerError> {
        let name = name.to_owned();
        self.sql
            .run(deadline, "role_gen_high_water", move |store| {
                Ok(store.role_gen_high_water(&name)?)
            })
            .await
    }

    async fn role(
        &self,
        name: &str,
        deadline: Instant,
    ) -> Result<role_leases::Observation, HandlerError> {
        let key = format!("role.{name}");
        let row = self.sql.cached(&key, deadline).await?;
        let applied = self.backend.applied(deadline).await?;
        let local_episode = match self.backend.writer_episode(deadline).await {
            Ok(episode) => Some(episode),
            Err(error) if error.code == "hub_not_ready" => None,
            Err(error) => return Err(error),
        };
        self.role_observation(name, &key, row, applied, local_episode)
    }

    fn link_events(&self) -> (&Notify, &Notify) {
        (&self.link, &self.commits)
    }

    async fn preferred(
        &self,
        role: &str,
        deadline: Instant,
    ) -> Result<Option<role_leases::Preferred>, HandlerError> {
        self.cached_preference(role, deadline).await
    }

    async fn handback(
        &self,
        role: &str,
        box_id: &str,
        deadline: Instant,
    ) -> Result<Option<role_leases::CandidateRequest>, HandlerError> {
        self.cached_handback(role, box_id, deadline).await
    }

    async fn reread_offer(
        &self,
        role: &str,
        target: &role_leases::Preferred,
        gen: i64,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        self.reread_role_offer(role, target, gen, deadline).await
    }

    async fn read_handback_revision(
        &self,
        role: &str,
        box_id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        self.live_handback_revision(role, box_id, deadline).await
    }

    async fn put_handback(
        &self,
        role: &str,
        gen: i64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.request_role_handback(role, gen, deadline).await
    }

    async fn delete_handback(
        &self,
        role: &str,
        box_id: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.delete_role_handback(role, box_id, revision, deadline)
            .await
    }

    async fn write(
        &self,
        role: &str,
        change: &role_leases::Change,
        deadline: Instant,
    ) -> Result<role_leases::Confirmed, HandlerError> {
        if let Some(expected) = change.vacancy_preference {
            let competing = self
                .live_preference(role, deadline)
                .await?
                .filter(|p| p.box_id != self.box_id)
                .map_or(0, |p| p.revision);
            if competing != expected {
                return Err(HandlerError::new(
                    "revision_moved",
                    "the role preference changed",
                ));
            }
        }
        let key = format!("role.{role}");
        let body = json!(change.body);
        let digest = hub_write::params_digest(change.step, &json!([key, change.expected, body]))?;
        let request = hub_write::Request {
            request_key: &change.request_key,
            params_digest: &digest,
            writer: &self.box_id,
            writer_ep: change.writer_ep,
        };
        let step = Step {
            name: change.step.into(),
            key,
            expected: change.expected,
            payload: Payload::Record {
                kind: "role".into(),
                body,
                snapshot_id: None,
            },
        };
        let landed = self
            .backend
            .write(&request, &step, None, None, deadline)
            .await?;
        let revision = landed
            .position
            .ok_or_else(|| bad_record("landed role write has no revision"))?;
        self.raise_role_gen(role, change.body.gen, deadline).await?;
        let sent_ms = self
            .sql
            .attempt(&change.request_key, deadline)
            .await?
            .map(|row| hub_write::landed_send_ms(&row, &landed))
            .transpose()?
            .flatten();
        Ok(role_leases::Confirmed { revision, sent_ms })
    }
}

struct HeldDelivery {
    entry: Entry,
    arrival_head: Option<u64>,
    waiting_for_anchor: bool,
    lease_retry_at: Option<u64>,
}

pub(super) fn committed_mint(epoch: &Value) -> Option<&str> {
    match epoch["state"].as_str()? {
        "committed" => epoch["mint_id"].as_str(),
        "pending" => epoch["base_mint_id"].as_str(),
        _ => None,
    }
}

pub(super) fn delete_allowed_without_epoch(key: &str, last: Option<&Value>) -> bool {
    let Some((kind, suffix)) = key.split_once('.') else {
        return false;
    };
    !suffix.is_empty()
        && match kind {
            "wrapreq" | "rolehandback" | "wrapjoin" => true,
            "move" => last
                .is_some_and(|body| matches!(body["state"].as_str(), Some("released" | "aborted"))),
            _ => false,
        }
}

/// Runs the normal agent mutation, captures its log entry, and rolls back the
/// transaction. A rebase uses the ID and timestamp from the persisted log entry
/// and the avatar already drawn for the request.
pub(super) type CreatePreview =
    Arc<dyn Fn(&RegistryStore, Option<&Value>) -> Result<Value, HandlerError> + Send + Sync>;

struct CreateReplan<'a> {
    residence: &'a Residence,
    preview: CreatePreview,
}

impl LogReplan for CreateReplan<'_> {
    fn wait_for_head(
        &self,
        head: u64,
        deadline: Instant,
    ) -> futures_util::future::BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(self.residence.backend.wait_through(head, deadline))
    }

    fn after_image(
        &self,
        saved: Value,
        _id: String,
        deadline: Instant,
    ) -> futures_util::future::BoxFuture<'_, Result<Replanned, HandlerError>> {
        let preview = self.preview.clone();
        Box::pin(self.residence.sql.run(
            deadline,
            "replan_agent_create",
            move |store| match preview(store, Some(&saved)) {
                Ok(body) => Ok(Replanned::Body(body)),
                Err(error) if deferred(&error) => Err(error),
                Err(error) => Ok(Replanned::Refused(error)),
            },
        ))
    }
}

// A placeholder passed only when an existing attempt already holds the
// persisted bytes: the writer resends those bytes, and this step's own
// payload (an empty delete) is never sent.
fn resume_only() -> Step {
    Step {
        name: String::new(),
        key: LOG_KEY.into(),
        expected: 0,
        payload: Payload::Delete,
    }
}

impl Residence {
    async fn request_episode(
        &self,
        attempt: Option<&entorhinal_core::hub_store::HubAttempt>,
        deadline: Instant,
    ) -> Result<u64, HandlerError> {
        if let Some(attempt) = attempt {
            let record = UnverifiedRecord::parse(&attempt.write.key, &attempt.write.value)?;
            if record.header().writer != self.box_id {
                return Err(bad_record(
                    "saved residence writer differs from this computer",
                ));
            }
            Ok(record.header().writer_ep)
        } else {
            self.backend.writer_episode(deadline).await
        }
    }

    async fn create_write(
        &self,
        request_key: &str,
        digest: &str,
        attempt: Option<&entorhinal_core::hub_store::HubAttempt>,
        preview: Option<CreatePreview>,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        if let Some(attempt) = attempt {
            if attempt.write.params_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
        } else if !self.ready() {
            return Err(not_ready());
        }
        let writer_ep = self.request_episode(attempt, deadline).await?;
        let first = if attempt.is_some() {
            resume_only()
        } else {
            let expected = self.backend.applied(deadline).await?;
            let preview = preview
                .clone()
                .ok_or_else(|| bad_record("create preview missing"))?;
            let body = self
                .sql
                .run(deadline, "preview_agent_create", move |store| {
                    preview(store, None)
                })
                .await?;
            Step {
                name: CREATE.into(),
                key: LOG_KEY.into(),
                expected,
                payload: Payload::Record {
                    kind: CREATE.into(),
                    body,
                    snapshot_id: None,
                },
            }
        };
        let next = |saved: &Value| {
            let agent = saved["agent_id"]
                .as_str()
                .filter(|id| entorhinal_core::agent::valid_agent_id(id))
                .ok_or_else(|| bad_record("saved create has no valid agent id"))?;
            Ok(Record::new(&self.box_id, writer_ep, 1, false).step(CREATE_KV, key(agent), 0))
        };
        let replan = preview.map(|preview| CreateReplan {
            residence: self,
            preview,
        });
        self.backend
            .write(
                &hub_write::Request {
                    request_key,
                    params_digest: digest,
                    writer: &self.box_id,
                    writer_ep,
                },
                &first,
                Some(&next),
                replan.as_ref().map(|plan| plan as &dyn LogReplan),
                deadline,
            )
            .await
    }

    pub(super) async fn create(
        &self,
        request_key: &str,
        digest: &str,
        preview: Option<CreatePreview>,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let attempt = self.sql.attempt(request_key, deadline).await?;
        let landed = match self
            .create_write(request_key, digest, attempt.as_ref(), preview, deadline)
            .await
        {
            Ok(landed) => landed,
            Err(error) => {
                let kv = self
                    .sql
                    .attempt(request_key, deadline)
                    .await?
                    .filter(|row| row.write.step == CREATE_KV && row.write.params_digest == digest);
                // The log proves this request created the agent, even if the
                // KV bucket (which keeps one revision) can no longer show
                // whether its first residence write landed.
                if matches!(
                    error.code.as_str(),
                    "hub_write_superseded" | "hub_write_unsettled"
                ) && kv.as_ref().is_some_and(|row| row.outcome.is_some())
                {
                    if let Some((generation, row)) =
                        self.created_agent(request_key, deadline).await?
                    {
                        return encode_result(
                            json!({"agent": row.digest(), "generation":generation}),
                        );
                    }
                }
                if error.code == "hub_unavailable" {
                    if let Some(row) = kv {
                        let record = UnverifiedRecord::parse(&row.write.key, &row.write.value)?;
                        return Err(crate::hub_transport::unsettled(&record.header().entry_id));
                    }
                }
                return Err(error);
            }
        };
        loop {
            let notified = self.commits.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some((generation, row)) = self.created_agent(request_key, deadline).await? {
                return encode_result(json!({"agent": row.digest(), "generation":generation}));
            }
            timeout_at(deadline, notified)
                .await
                .map_err(|_| crate::hub_transport::unsettled(&landed.op_id))?;
        }
    }

    async fn recover(self: Arc<Self>) {
        let log_step = format!("{}{CREATE}", hub_write::CONTINUE);
        loop {
            let notified = self.commits.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let unfinished = match self.recover_creates(&log_step).await {
                Ok(unfinished) => unfinished,
                Err(error) if error.code == "store_unavailable" => return,
                Err(error) => {
                    tracing::warn!(target: "hub_residence", code = error.code, "could not read open residence attempts");
                    true
                }
            };
            if unfinished {
                tokio::select! { _ = notified => {}, _ = tokio::time::sleep(Duration::from_secs(1)) => {} }
            } else {
                notified.await;
            }
        }
    }

    async fn recover_creates(&self, log_step: &str) -> Result<bool, HandlerError> {
        let deadline = Instant::now() + BUDGET;
        let step = log_step.to_owned();
        let open = self
            .sql
            .run(deadline, "open_hub_attempts", move |store| {
                let mut open = store.open_hub_attempts(&step)?;
                open.extend(store.open_hub_attempts(CREATE_KV)?);
                Ok(open)
            })
            .await?;
        let mut unfinished = false;
        let ready = self.backend.membership_ready();
        for attempt in &open {
            let deadline = Instant::now() + BUDGET;
            // Do not start a log append that was never seen to land. Only
            // completion of an applied create is a restart obligation.
            if attempt.write.step == log_step {
                if self
                    .created_agent(&attempt.write.request_key, deadline)
                    .await?
                    .is_none()
                {
                    continue;
                }
                let writer_ep = self.request_episode(Some(attempt), deadline).await?;
                if self.backend.episode_closed(
                    &self.box_id,
                    writer_ep,
                    self.backend.applied(deadline).await?,
                ) {
                    let error = HandlerError::new(
                        "residence_create_episode_closed",
                        "the agent was created but its writer episode closed before residence could be written",
                    );
                    if !hub_write::refuse_attempt(&self.sql, attempt, error, deadline).await? {
                        // A concurrent retry may have advanced to the KV step.
                        unfinished |= self
                            .sql
                            .attempt(&attempt.write.request_key, deadline)
                            .await?
                            .is_some_and(|row| row.outcome.is_none());
                    }
                    continue;
                }
            }
            if !ready {
                unfinished = true;
                continue;
            }
            if let Err(error) = self
                .create_write(
                    &attempt.write.request_key,
                    &attempt.write.params_digest,
                    Some(attempt),
                    None,
                    deadline,
                )
                .await
            {
                tracing::debug!(target: "hub_residence", code = error.code, "residence create has not settled yet");
                unfinished |= self
                    .sql
                    .attempt(&attempt.write.request_key, deadline)
                    .await?
                    .is_some_and(|row| row.outcome.is_none());
            }
        }
        Ok(unfinished)
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TakeInput {
    agent_id: String,
    #[serde(alias = "requestKey")]
    request_key: String,
}

impl Residence {
    async fn take_plan(
        &self,
        agent: &str,
        deadline: Instant,
    ) -> Result<(u64, Record), HandlerError> {
        let id = agent.to_owned();
        let Some(row) = self
            .sql
            .run(
                deadline,
                "agent_row",
                move |store| Ok(store.agent_row(&id)?),
            )
            .await?
        else {
            return Err(HandlerError::new("unknown_agent", "unknown agent"));
        };
        if row.gone().is_some() {
            return Err(HandlerError::new("gone", "agent is gone"));
        }
        let old = self.sql.cached(&key(agent), deadline).await?;
        if old.as_ref().is_some_and(|old| old.integrity_failed) {
            return Err(HandlerError::new(
                "claim_integrity_failed",
                "residence record was refused",
            ));
        }
        if !self.fresh(agent, old.as_ref().map(|old| old.revision)) {
            return Err(not_ready());
        }
        let episode = self.backend.writer_episode(deadline).await?;
        let Some(old) = old else {
            if self
                .agent_creator(agent, i64::MAX, deadline)
                .await?
                .is_some()
            {
                return Err(HandlerError::new(
                    "claim_pending",
                    "the creating computer must write this residence",
                ));
            }
            if self.snapshot.claim(agent) != Some((None, 0)) {
                return Err(not_ready());
            }
            return Ok((0, Record::new(&self.box_id, episode, 1, false)));
        };
        let body: Record = serde_json::from_value(
            old.body
                .ok_or_else(|| bad_record("residence body missing"))?,
        )
        .map_err(crate::invalid_params)?;
        let applied = self.backend.applied(deadline).await?;
        if self.backend.current_member(&body.holder, applied) == Some(body.holder_ep) {
            return Err(HandlerError::new(
                "holder_is_member",
                "move the agent from its current member instead",
            ));
        }
        let next = body
            .gen
            .checked_add(1)
            .ok_or_else(|| bad_record("residence generation exhausted"))?;
        Ok((old.revision, Record::new(&self.box_id, episode, next, true)))
    }

    async fn write_take(
        &self,
        input: &TakeInput,
        digest: &str,
        observed: Option<(u64, Record)>,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let attempt = self.sql.attempt(&input.request_key, deadline).await?;
        if let Some(attempt) = &attempt {
            if attempt.write.params_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
        }
        let (step, writer_ep) = if attempt.is_some() {
            (
                resume_only(),
                self.request_episode(attempt.as_ref(), deadline).await?,
            )
        } else {
            let (revision, approved) = observed.ok_or_else(not_ready)?;
            let (current, record) = self.take_plan(&input.agent_id, deadline).await?;
            if revision != current || record.holder_ep != approved.holder_ep {
                return Err(HandlerError::new(
                    "revision_moved",
                    "residence changed after confirmation",
                ));
            }
            let episode = record.holder_ep;
            (record.step(TAKE, key(&input.agent_id), revision), episode)
        };
        let landed = self
            .backend
            .write(
                &hub_write::Request {
                    request_key: &input.request_key,
                    params_digest: digest,
                    writer: &self.box_id,
                    writer_ep,
                },
                &step,
                None,
                None,
                deadline,
            )
            .await?;
        encode_result(json!({"agent_id":input.agent_id,"revision":landed.position}))
    }
}

impl ProjectsHandler {
    pub(super) fn admit_residence_take(&self, route: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(route);
        crate::refuse_flow_write(TAKE, admission.flow_id.as_deref())?;
        if admission.principal != Some(subc_protocol::Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "agent.take requires Direct",
            ));
        }
        Ok(())
    }

    pub(super) async fn take_residence(
        &self,
        params: Value,
        route: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_residence_take(route)?;
        let input: TakeInput = serde_json::from_value(params).map_err(crate::invalid_params)?;
        if !entorhinal_core::agent::valid_agent_id(&input.agent_id) || input.request_key.is_empty()
        {
            return Err(HandlerError::new(
                "invalid_request",
                "invalid agent_id or request_key",
            ));
        }
        let digest = hub_write::params_digest(TAKE, &json!({"agent_id":input.agent_id}))?;
        let service = self.residence().ok_or_else(not_ready)?;
        if service
            .sql
            .attempt(&input.request_key, received + BUDGET)
            .await?
            .is_some()
        {
            return service
                .write_take(&input, &digest, None, received + BUDGET)
                .await;
        }
        let plan = service
            .take_plan(&input.agent_id, received + BUDGET)
            .await?;
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, route)?;
        if !self.route_admissions().contains_key(&route) {
            return Err(OperatorConfirmError::Declined.into());
        }
        let summary = format!(
            "Take residence of agent '{}' at generation {} on this computer",
            input.agent_id,
            plan.1.gen - 1
        );
        let bound = received + crate::shared_write::OPERATOR_BOUND;
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(bound, self.operator_confirmer.confirm_operator(&summary, route)) => {
                result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
            }
        }
        if Instant::now() >= bound {
            return Err(OperatorConfirmError::Declined.into());
        }
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => Err(OperatorConfirmError::Declined.into()),
            result = service.write_take(&input, &digest, Some(plan), bound.min(Instant::now() + BUDGET)) => result,
        }
    }
}

//! The watched residence projection and its two local write paths.
#![allow(dead_code)] // Activated by the hub cutover coordinator.

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use async_nats::jetstream::kv::{Entry, Operation};
use async_trait::async_trait;
use entorhinal_core::{claim_cache::HubClaimContext, hub_store::HubKvCached, RegistryStore};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};

use crate::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    encode_result,
    fleet_record::{UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_identity_log::{AcceptedKv, IdentityLog, LogTransport},
    hub_membership::Sight,
    hub_transport::{check_kv_size, Feed, FeedEvent, KvDelivery},
    hub_write::{self, LogReplan, Payload, Replanned, Step, Trust},
    HandlerError, HealthGauges, ProjectsHandler, RouteKey,
};

pub(super) const TAKE: &str = "agent.take";
const CREATE: &str = "agent.create";
const CREATE_KV: &str = "residence.create";
const BUDGET: Duration = Duration::from_secs(5);

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
}

#[async_trait]
trait Backend: Send + Sync {
    fn membership_ready(&self) -> bool;
    fn current_member(&self, holder: &str, applied: u64) -> Option<u64>;
    fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool;
    fn applied(&self) -> Result<u64, HandlerError>;
    fn writer_episode(&self) -> Result<u64, HandlerError>;
    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError>;
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
    fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool {
        self.episode_closed(holder, episode, applied)
    }
    fn applied(&self) -> Result<u64, HandlerError> {
        self.applied_sequence()
    }
    fn writer_episode(&self) -> Result<u64, HandlerError> {
        self.writer_episode()
    }
    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.log_head(deadline).await
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
    revisions: HashMap<String, u64>,
    held: HashSet<String>,
}

pub(super) struct Residence {
    backend: Arc<dyn Backend>,
    snapshot: Arc<dyn SnapshotClaims>,
    box_id: String,
    base: i64,
    store: RegistryStore,
    health: Arc<HealthGauges>,
    commits: Arc<Notify>,
    watched: RwLock<Watched>,
    clock: fn() -> i64,
}

pub(super) struct Running {
    pub service: Arc<Residence>,
    watch: tokio::task::JoinHandle<()>,
    recovery: tokio::task::JoinHandle<()>,
    epochs: Option<(
        Arc<dyn crate::hub_epoch::RemovalIntake>,
        tokio::task::JoinHandle<()>,
    )>,
}

impl Drop for Running {
    fn drop(&mut self) {
        self.watch.abort();
        self.recovery.abort();
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
        let mut running = self.hub_residence.lock().unwrap();
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
            store: log.store.clone(),
            box_id: log.box_id.to_string(),
            base: log.base,
            commits: log.commits.clone(),
            backend: log,
            snapshot,
            health: self.health.clone(),
            watched: RwLock::default(),
            clock: self.clock,
        });
        *running = Some(Running {
            watch: tokio::spawn(service.clone().watch(feed)),
            recovery: tokio::spawn(service.clone().recover()),
            epochs,
            service: service.clone(),
        });
        Ok(service)
    }

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

    #[cfg(test)]
    pub(super) async fn stop_hub_residence(&self) {
        let running = self.hub_residence.lock().unwrap().take();
        if let Some(mut running) = running {
            running.watch.abort();
            running.recovery.abort();
            let _ = (&mut running.watch).await;
            let _ = (&mut running.recovery).await;
        }
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
fn timed_out(error: &HandlerError) -> bool {
    matches!(
        error.code.as_str(),
        "hub_unavailable"
            | "hub_membership_stale"
            | "epoch_key_unavailable"
            | "vault_unavailable"
            | "credential_unavailable"
            | "storage_error"
    )
}

impl Residence {
    fn ready(&self) -> bool {
        self.snapshot.hub_ready()
            && self.backend.membership_ready()
            && self.watched.read().unwrap().initial
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
    async fn watch(self: Arc<Self>, mut feed: Feed<KvDelivery>) {
        let mut pending: HashMap<String, HeldDelivery> = HashMap::new();
        let mut retry = tokio::time::interval(Duration::from_secs(1));
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                event = feed.next_event() => match event {
                    FeedEvent::Connected | FeedEvent::Disconnected => self.watched.write().unwrap().initial = false,
                    FeedEvent::Item(KvDelivery::Empty) => self.watched.write().unwrap().initial = true,
                    FeedEvent::Item(KvDelivery::Entry(entry)) => {
                        let seen_current = entry.seen_current;
                        if pending.get(&entry.key).is_none_or(|old| old.entry.revision <= entry.revision) {
                            let arrival_head = pending.remove(&entry.key)
                                .filter(|old| old.entry.revision == entry.revision)
                                .and_then(|old| old.arrival_head);
                            self.observe_or_hold(HeldDelivery { entry, arrival_head }, &mut pending).await;
                        }
                        if seen_current {
                            self.watched.write().unwrap().initial = true;
                        }
                    }
                },
                _ = retry.tick(), if !pending.is_empty() => {
                    for entry in std::mem::take(&mut pending).into_values() {
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
        let result = self
            .observe_at(
                &delivery.entry,
                &mut delivery.arrival_head,
                Instant::now() + BUDGET,
            )
            .await;
        let mut watched = self.watched.write().unwrap();
        if let Err(error) = result {
            // Losing the log or vault is not evidence that the value is bad.
            // Fence only the affected key and keep consuming the watch.
            watched.held.insert(delivery.entry.key.clone());
            tracing::debug!(target: "hub_residence", key = delivery.entry.key, code = error.code, "holding fleet KV delivery");
            pending.insert(delivery.entry.key.clone(), delivery);
        } else {
            watched.held.remove(&delivery.entry.key);
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
        let old = self.store.hub_kv_cached(key)?;
        if let Some(old) = &old {
            if revision < old.revision || (revision == old.revision && old.integrity_failed) {
                return Ok(());
            }
            if revision == old.revision {
                if old.value.as_deref() == Some(value)
                    && old.body.is_some() == (entry.operation == Operation::Put)
                {
                    self.accept(key, revision, value, old.body.as_ref())?;
                } else {
                    self.refuse(key, revision, "revision_bytes_differ")?;
                }
                return Ok(());
            }
        }
        if entry.operation != Operation::Put {
            if self.delete_allowed(key, old.as_ref())? {
                self.accept(key, revision, value, None)?;
            } else {
                self.refuse(key, revision, "delete_forbidden")?;
            }
            return Ok(());
        }
        let Some(agent) = key.strip_prefix("residence.") else {
            return Ok(());
        };
        if !entorhinal_core::agent::valid_agent_id(agent) {
            return self.refuse(key, revision, "invalid_agent_id");
        }
        match self
            .judge(agent, key, value, old.as_ref(), arrival_head, deadline)
            .await
        {
            Ok(accepted) => {
                self.accept(key, revision, value, Some(&accepted.body))?;
                if let Err(error) = self
                    .backend
                    .record_applied(&accepted.record, revision, deadline)
                    .await
                {
                    tracing::debug!(target: "hub_residence", key, revision, code = error.code, "could not settle applied residence");
                }
                Ok(())
            }
            Err(error) if timed_out(&error) => Err(error),
            Err(error) => self.refuse(key, revision, &error.code),
        }
    }

    fn accept(
        &self,
        key: &str,
        revision: u64,
        value: &[u8],
        body: Option<&Value>,
    ) -> Result<(), HandlerError> {
        if self
            .store
            .record_hub_kv(key, revision, Some((value, body)), (self.clock)())?
        {
            self.watched
                .write()
                .unwrap()
                .revisions
                .insert(key.into(), revision);
        }
        Ok(())
    }

    fn refuse(&self, key: &str, revision: u64, code: &str) -> Result<(), HandlerError> {
        if self
            .store
            .record_hub_kv(key, revision, None, (self.clock)())?
        {
            self.health
                .hub
                .kv_records_refused
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            tracing::warn!(target: "hub_residence", key, revision, code, "refused fleet KV revision");
        }
        Ok(())
    }

    fn delete_allowed(&self, key: &str, old: Option<&HubKvCached>) -> Result<bool, HandlerError> {
        if let Some(mint) = key
            .strip_prefix("wrap.")
            .and_then(|rest| rest.split('.').nth(1))
        {
            let epoch = self.store.hub_kv_cached("epoch")?.and_then(|row| row.body);
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
            self.store.hub_agent_creator(agent)?
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
                creator = self.store.hub_agent_creator_through(agent, through)?;
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
}

struct HeldDelivery {
    entry: Entry,
    arrival_head: Option<u64>,
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
            "move" => last.is_some_and(|body| {
                matches!(body["state"].as_str(), Some("activated" | "aborted"))
            }),
            _ => false,
        }
}

/// Runs the normal agent mutation, captures its log entry, and rolls back the
/// transaction. A rebase uses the ID and timestamp from the persisted log entry
/// and the avatar already drawn for the request.
pub(super) type CreatePreview<'a> =
    &'a (dyn Fn(&RegistryStore, Option<&Value>) -> Result<Value, HandlerError> + Send + Sync);

struct CreateReplan<'a> {
    residence: &'a Residence,
    preview: CreatePreview<'a>,
}

impl LogReplan for CreateReplan<'_> {
    fn wait_for_head(
        &self,
        head: u64,
        deadline: Instant,
    ) -> futures_util::future::BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(self.residence.backend.wait_through(head, deadline))
    }

    fn after_image(&self, saved: &Value) -> Result<Replanned, HandlerError> {
        match (self.preview)(&self.residence.store, Some(saved)) {
            Ok(body) => Ok(Replanned::Body(body)),
            Err(error) if timed_out(&error) => Err(error),
            Err(error) => Ok(Replanned::Refused(error)),
        }
    }
}

// This step cannot build a write; it is only passed when an existing attempt
// already supplies the persisted bytes. If that attempt has meanwhile been
// pruned, there are no bytes to resend, so the step sends nothing.
fn resume_only() -> Step {
    Step {
        name: String::new(),
        key: LOG_KEY.into(),
        expected: 0,
        payload: Payload::Delete,
    }
}

impl Residence {
    fn request_episode(
        &self,
        attempt: Option<&entorhinal_core::hub_store::HubAttempt>,
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
            self.backend.writer_episode()
        }
    }

    async fn create_write(
        &self,
        request_key: &str,
        digest: &str,
        attempt: Option<&entorhinal_core::hub_store::HubAttempt>,
        preview: Option<CreatePreview<'_>>,
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
        let writer_ep = self.request_episode(attempt)?;
        let first = if attempt.is_some() {
            resume_only()
        } else {
            let expected = self.backend.applied()?;
            let body =
                preview.ok_or_else(|| bad_record("create preview missing"))?(&self.store, None)?;
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
        preview: Option<CreatePreview<'_>>,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let attempt = self.store.hub_attempt(request_key)?;
        let landed = match self
            .create_write(request_key, digest, attempt.as_ref(), preview, deadline)
            .await
        {
            Ok(landed) => landed,
            Err(error) => {
                if error.code == "hub_unavailable" {
                    if let Some(row) = self
                        .store
                        .hub_attempt(request_key)?
                        .filter(|row| row.write.step == CREATE_KV)
                    {
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
            if let Some((generation, row)) = self.store.hub_created_agent(request_key)? {
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
        let mut open = self.store.open_hub_attempts(log_step)?;
        open.extend(self.store.open_hub_attempts(CREATE_KV)?);
        let mut unfinished = false;
        let ready = self.backend.membership_ready();
        for attempt in &open {
            // Do not start a log append that was never seen to land. Only
            // completion of an applied create is a restart obligation.
            if attempt.write.step == log_step
                && self
                    .store
                    .hub_created_agent(&attempt.write.request_key)?
                    .is_none()
            {
                continue;
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
                    Instant::now() + BUDGET,
                )
                .await
            {
                tracing::debug!(target: "hub_residence", code = error.code, "residence create has not settled yet");
                unfinished |= self
                    .store
                    .hub_attempt(&attempt.write.request_key)?
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
    fn take_plan(&self, agent: &str) -> Result<(u64, Record), HandlerError> {
        let Some(row) = self.store.agent_row(agent)? else {
            return Err(HandlerError::new("unknown_agent", "unknown agent"));
        };
        if row.gone().is_some() {
            return Err(HandlerError::new("gone", "agent is gone"));
        }
        let old = self.store.hub_kv_cached(&key(agent))?;
        if old.as_ref().is_some_and(|old| old.integrity_failed) {
            return Err(HandlerError::new(
                "claim_integrity_failed",
                "residence record was refused",
            ));
        }
        if !self.fresh(agent, old.as_ref().map(|old| old.revision)) {
            return Err(not_ready());
        }
        let episode = self.backend.writer_episode()?;
        let Some(old) = old else {
            if self.store.hub_agent_creator(agent)?.is_some() {
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
        let applied = self.backend.applied()?;
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
        let attempt = self.store.hub_attempt(&input.request_key)?;
        if let Some(attempt) = &attempt {
            if attempt.write.params_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
        }
        let (step, writer_ep) = if attempt.is_some() {
            (resume_only(), self.request_episode(attempt.as_ref())?)
        } else {
            let (revision, approved) = observed.ok_or_else(not_ready)?;
            let (current, record) = self.take_plan(&input.agent_id)?;
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
        if service.store.hub_attempt(&input.request_key)?.is_some() {
            return service
                .write_take(&input, &digest, None, received + BUDGET)
                .await;
        }
        let plan = service.take_plan(&input.agent_id)?;
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

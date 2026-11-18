//! Ordered hub identity-log apply and durable appends. Cutover owns activation
//! and the first hub snapshot; this path starts from the installed hub base.
#![allow(dead_code)]

use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};

use cortexkit_bus_naming::federation::BoxId;
use cortexkit_claustrum_client::CredentialResolver;
use entorhinal_core::{
    remote_apply::{snapshot_part_info, snapshot_supersedes_through, RemoteEntry},
    RegistryStore,
};
use futures_util::future::BoxFuture;
use serde_json::Value;
use tokio::{
    sync::Notify,
    time::{timeout_at, Instant},
};

use crate::{
    fleet_record::{vault::EpochVault, UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_epoch::EpochReader,
    hub_membership::{AnchorSource, Membership, PeerKeysRead, Sight},
    hub_transport::{Feed, FeedEvent, Hub, LogDelivery},
    hub_write::{
        self, LogReplan, Payload, Replanned, Step, Transport, Trust, Writer,
        PROJECTION_SNAPSHOT_KIND as SNAPSHOT,
    },
    HandlerError, HealthGauges, ProjectsHandler,
};

const READ_BUDGET: Duration = Duration::from_secs(25);

pub(super) trait LogTransport: Transport + Send + Sync + 'static {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery>;

    fn join_keys(
        &self,
        _prefix: &str,
        _deadline: Instant,
    ) -> impl std::future::Future<Output = Result<Vec<String>, HandlerError>> + Send {
        async {
            Err(HandlerError::new(
                "hub_unavailable",
                "join key listing unavailable",
            ))
        }
    }

    fn epoch_watch(self: &Arc<Self>) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        None
    }
}

impl LogTransport for Hub {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        self.tail(applied)
    }

    fn epoch_watch(self: &Arc<Self>) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        Some(self.epoch_watch())
    }

    async fn join_keys(
        &self,
        prefix: &str,
        deadline: Instant,
    ) -> Result<Vec<String>, HandlerError> {
        self.session(deadline)
            .await?
            .1
            .join_keys(prefix, deadline)
            .await
    }
}

pub(super) struct Link<T, U> {
    pub transport: Arc<T>,
    pub trust: Arc<U>,
    pub signer: Arc<dyn CredentialResolver>,
    pub vault: Arc<dyn EpochVault>,
    pub peers: Arc<dyn PeerKeysRead>,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Link<T, U> {
    fn writer<'a, V: Trust>(&'a self, store: &'a RegistryStore, trust: &'a V) -> Writer<'a, T, V> {
        Writer {
            store,
            transport: &self.transport,
            trust,
            signer: self.signer.as_ref(),
            vault: self.vault.as_ref(),
            replan: None,
        }
    }
}

struct MemberTrust<'a, T, U> {
    epoch: &'a U,
    epoch_reader: Option<&'a EpochReader<T>>,
    membership: &'a Membership<T>,
    store: &'a RegistryStore,
    base: i64,
}

impl<T: LogTransport, U: Trust> Trust for MemberTrust<'_, T, U> {
    async fn writer_epoch(&self, deadline: Instant) -> Result<u64, HandlerError> {
        match self.epoch_reader {
            Some(reader) => reader.fence(deadline).await,
            None => self.epoch.writer_epoch(deadline).await,
        }
    }

    async fn verification_key(&self, record: &UnverifiedRecord) -> Result<String, HandlerError> {
        self.membership.saved_key(record)
    }

    async fn verify_remote(
        &self,
        record: UnverifiedRecord,
        log_position: Option<u64>,
        deadline: Instant,
    ) -> Result<VerifiedRecord, HandlerError> {
        if let Some(sequence) = log_position {
            self.membership
                .verify_log(record, sequence, deadline)
                .await
                .map(|(_, record)| record)
        } else {
            let applied = hub_sequence(
                self.base,
                self.store.identity_log_status()?.last_applied_position,
            )?;
            self.membership
                .verify_kv(record, applied, Sight::FirstOrAfterGap, deadline)
                .await
                .map(|(record, _)| record)
        }
    }
}

pub(super) struct IdentityLog<T, U> {
    link: Arc<Link<T, U>>,
    pub(super) store: RegistryStore,
    health: Arc<HealthGauges>,
    writer: Arc<tokio::sync::Mutex<()>>,
    pub(super) commits: Arc<Notify>,
    pub(super) box_id: BoxId,
    pub(super) base: i64,
    membership: Arc<Membership<T>>,
    pub(super) epoch: Option<Arc<EpochReader<T>>>,
}

impl ProjectsHandler {
    /// Cutover calls this after installing the base and stopping legacy
    /// catch-up. The handler owns the tail task just as it owns legacy catch-up.
    pub(super) fn activate_hub_identity_log<T, U>(
        &self,
        box_id: BoxId,
        link: Link<T, U>,
    ) -> Result<IdentityLog<T, U>, HandlerError>
    where
        T: LogTransport,
        U: Trust + Send + Sync + 'static,
    {
        let store = self.with_store(|store| Ok(store.clone()))?;
        let base = store.hub_log_base()?.ok_or_else(|| {
            HandlerError::new("hub_not_ready", "hub log base has not been installed")
        })?;
        let applied = store.identity_log_status()?.last_applied_position;
        let sequence = hub_sequence(base, applied)?;
        let mut task = self.catch_up_task.lock().unwrap();
        if task.is_some() || self.health.origin.is_foreign() {
            return Err(HandlerError::new(
                "hub_not_ready",
                "cannot start the hub identity-log tail",
            ));
        }
        let link = Arc::new(link);
        let membership = Arc::new(Membership::new(
            link.transport.clone(),
            store.clone(),
            AnchorSource {
                own_box: box_id.to_string(),
                peers: link.peers.clone(),
                signer: link.signer.clone(),
            },
        ));
        let epoch = link.vault.control().map(|_| {
            Arc::new(EpochReader::new(
                link.transport.clone(),
                membership.clone(),
                store.clone(),
                link.vault.clone(),
                box_id.to_string(),
                base,
            ))
        });
        let feed = link.transport.tail(sequence).with_durable_cursor();
        let tail = Tail {
            link: link.clone(),
            store: store.clone(),
            health: self.health.clone(),
            commits: self.commits.clone(),
            base,
            applied,
            snapshot: Snapshot::default(),
            membership: membership.clone(),
            writer: self.writer.clone(),
            connected_head: None,
            epoch: epoch.clone(),
            highest_epoch: None,
        };
        self.health.hub_membership.store(1, Ordering::Release);
        self.health.log_state.store(0, Ordering::Relaxed);
        crate::enable::refresh(&self.health, &store);
        *task = Some(tokio::spawn(tail.run(feed)));
        Ok(IdentityLog {
            link,
            store,
            health: self.health.clone(),
            writer: self.writer.clone(),
            commits: self.commits.clone(),
            box_id,
            base,
            membership,
            epoch,
        })
    }
}

pub(super) struct PlannedAppend<'a> {
    pub request_key: &'a str,
    pub params_digest: &'a str,
    pub writer_ep: u64,
    /// Capture the applied sequence before reading the store to build `body`.
    pub expected: u64,
    pub body: Value,
}

pub(super) struct AcceptedKv {
    pub record: VerifiedRecord,
    pub body: Value,
    pub writer_current: bool,
    pub applied: u64,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> IdentityLog<T, U> {
    pub(super) fn epoch_watch(&self) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        self.link.transport.epoch_watch()
    }

    pub(super) fn peer_keys(&self) -> &dyn PeerKeysRead {
        self.link.peers.as_ref()
    }

    pub(super) async fn write_epoch_step(
        &self,
        request_key: &str,
        digest: &str,
        step: &Step,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let writer_ep = self.writer_episode()?;
        self.write_epoch_step_as(writer_ep, request_key, digest, step, deadline)
            .await
    }

    pub(super) async fn write_join_request(
        &self,
        writer_ep: u64,
        request_key: &str,
        expected: u64,
        body: Value,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let key = format!("wrapreq.{}", self.box_id);
        let digest = hub_write::params_digest(&key, &body)?;
        self.write_epoch_step_as(
            writer_ep,
            request_key,
            &digest,
            &Step {
                name: "wrapreq".into(),
                key,
                expected,
                payload: Payload::Record {
                    kind: "wrapreq".into(),
                    body,
                    snapshot_id: None,
                },
            },
            deadline,
        )
        .await?;
        Ok(())
    }

    async fn write_epoch_step_as(
        &self,
        writer_ep: u64,
        request_key: &str,
        digest: &str,
        step: &Step,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        self.link
            .writer(&self.store, &trust)
            .run(
                &hub_write::Request {
                    request_key,
                    params_digest: digest,
                    writer: &self.box_id.to_string(),
                    writer_ep,
                },
                step,
                None,
                deadline,
            )
            .await
    }

    pub(super) fn membership_ready(&self) -> bool {
        self.health.hub_membership.load(Ordering::Acquire) == 2
    }

    pub(super) fn holder_current(&self, holder: &str, episode: u64) -> Result<bool, HandlerError> {
        Ok(self.membership_ready()
            && self
                .membership
                .open_episode(holder, self.applied_sequence()?)
                == Some(episode)
            && crate::hub_membership::box_id(holder))
    }

    pub(super) fn current_member_episode(&self, holder: &str, applied: u64) -> Option<u64> {
        self.membership.current_member_episode(holder, applied)
    }

    pub(super) fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool {
        self.membership.episode_closed(holder, episode, applied)
    }

    pub(super) async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.link.transport.log_head(deadline).await
    }

    pub(super) fn writer_episode(&self) -> Result<u64, HandlerError> {
        if self.membership_ready() {
            if let Some(episode) = self.membership.can_write(self.applied_sequence()?) {
                return Ok(episode);
            }
        }
        Err(HandlerError::new(
            "hub_not_ready",
            "hub membership barrier has not passed",
        ))
    }

    pub(super) async fn write_residence(
        &self,
        request: &hub_write::Request<'_>,
        step: &Step,
        next: Option<hub_write::NextStep<'_>>,
        replan: Option<&dyn LogReplan>,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let _guard = timeout_at(deadline, self.writer.lock())
            .await
            .map_err(|_| {
                HandlerError::new("identity_log_contended", "waiting for the hub writer")
            })?;
        let row = self.store.hub_attempt(request.request_key)?;
        if row.as_ref().is_none_or(|row| row.outcome.is_none())
            && self.writer_episode()? != request.writer_ep
        {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "residence writer's episode has closed",
            ));
        }
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        Writer {
            replan,
            ..self.link.writer(&self.store, &trust)
        }
        .run(request, step, next, deadline)
        .await
    }

    pub(super) async fn record_residence_applied(
        &self,
        record: &VerifiedRecord,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        self.link
            .writer(&self.store, &trust)
            .record_applied(record, revision, deadline)
            .await?;
        Ok(())
    }

    pub(super) async fn verify_kv(
        &self,
        key: &str,
        bytes: &[u8],
        sight: Sight,
        deadline: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        let record = UnverifiedRecord::parse(key, bytes)?;
        let join = matches!(
            record.header().kind.as_str(),
            "epoch" | "wrap" | "wrapreq" | "wrapjoin"
        );
        let applied = self.applied_sequence()?;
        if let Some(epoch) = record.epoch() {
            if let Some(old) = self.store.hub_kv_cached(key)?.and_then(|row| row.value) {
                let previous = UnverifiedRecord::parse(key, &old)?.epoch();
                reject_rollback(previous, Some(epoch))?;
            }
        }
        let record = if join {
            self.membership
                .verify_kv(record, applied, sight, deadline)
                .await?
                .0
        } else {
            let key = match self.membership.kv_key(&record, applied, sight)? {
                Some(key) => key,
                None => {
                    let head = self.link.transport.log_head(deadline).await?;
                    self.membership.discover(head, deadline).await?;
                    wait_for_applied(self, head, deadline).await?;
                    self.membership
                        .kv_key(&record, self.applied_sequence()?, sight)?
                        .ok_or_else(crate::hub_membership::unknown_key)?
                }
            };
            if !self.membership_ready() {
                return Err(HandlerError::new(
                    "hub_membership_stale",
                    "hub membership view is catching up",
                ));
            }
            record.verify(&key)?
        };
        let applied = self.applied_sequence()?;
        let writer_current = record.header().kind != "wrapreq"
            && self
                .membership
                .open_episode(&record.header().writer, applied)
                == Some(record.header().writer_ep);
        if matches!(sight, Sight::ReplacesCached)
            && record.header().kind != "wrapreq"
            && !writer_current
        {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "replacement writer is not in its current episode",
            ));
        }
        if let (Some(reader), Some(epoch)) = (&self.epoch, record.epoch()) {
            reader.require_for_open(epoch, deadline).await?;
        }
        let body = timeout_at(deadline, record.open(self.link.vault.as_ref()))
            .await
            .map_err(|_| HandlerError::new("hub_unavailable", "fleet KV open timed out"))??;
        let body = serde_json::from_slice(&body).map_err(crate::invalid_params)?;
        crate::hub_membership::validate_kv_body(&record, &body)?;
        if !join && (!self.membership_ready() || self.applied_sequence()? != applied) {
            return Err(HandlerError::new(
                "hub_membership_stale",
                "hub membership view is catching up",
            ));
        }
        Ok(AcceptedKv {
            record,
            body,
            writer_current,
            applied,
        })
    }

    pub(super) fn applied_sequence(&self) -> Result<u64, HandlerError> {
        hub_sequence(
            self.base,
            self.store.identity_log_status()?.last_applied_position,
        )
    }

    /// Retry at the same position with the saved body. A new position needs a
    /// fresh body built against the applied store, not a copy of old rows.
    pub(super) async fn append(
        &self,
        plan: PlannedAppend<'_>,
        replan: impl Fn(&RegistryStore, &Value) -> Result<Replanned, HandlerError> + Send + Sync,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let kind = op(&plan.body)?.to_owned();
        let _guard = timeout_at(deadline, self.writer.lock())
            .await
            .map_err(|_| {
                HandlerError::new("identity_log_contended", "waiting for the hub log writer")
            })?;
        if matches!(self.health.log_state.load(Ordering::Relaxed), 4..=6) {
            return Err(HandlerError::new(
                "identity_log_stalled",
                "hub identity-log tail has stopped",
            ));
        }
        if self.membership.can_write(self.applied_sequence()?) != Some(plan.writer_ep) {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "local box cannot write in that membership episode",
            ));
        }
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        let planner = Planner {
            log: self,
            replan,
            writer_ep: plan.writer_ep,
        };
        Writer {
            replan: Some(&planner),
            ..self.link.writer(&self.store, &trust)
        }
        .run(
            &hub_write::Request {
                request_key: plan.request_key,
                params_digest: plan.params_digest,
                writer: &self.box_id.to_string(),
                writer_ep: plan.writer_ep,
            },
            &Step {
                name: kind.clone(),
                key: LOG_KEY.into(),
                expected: plan.expected,
                payload: Payload::Record {
                    kind,
                    body: plan.body,
                    snapshot_id: None,
                },
            },
            None,
            deadline,
        )
        .await
    }
}

struct Planner<'a, T, U, F> {
    log: &'a IdentityLog<T, U>,
    replan: F,
    writer_ep: u64,
}

impl<T, U, F> LogReplan for Planner<'_, T, U, F>
where
    T: LogTransport,
    U: Trust + Send + Sync + 'static,
    F: Fn(&RegistryStore, &Value) -> Result<Replanned, HandlerError> + Sync,
{
    fn wait_for_head(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(wait_for_applied(self.log, sequence, deadline))
    }

    fn after_image(&self, saved: &Value) -> Result<Replanned, HandlerError> {
        if self.log.membership.can_write(self.log.applied_sequence()?) != Some(self.writer_ep) {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "local box cannot replan in that membership episode",
            ));
        }
        let outcome = (self.replan)(&self.log.store, saved)?;
        if let Replanned::Body(body) = &outcome {
            if op(body)? != op(saved)? {
                return Err(HandlerError::new(
                    "record_invalid",
                    "re-plan changed the identity-log operation",
                ));
            }
        }
        Ok(outcome)
    }
}

pub(super) async fn wait_for_applied<T: LogTransport, U: Trust + Send + Sync + 'static>(
    log: &IdentityLog<T, U>,
    sequence: u64,
    deadline: Instant,
) -> Result<(), HandlerError> {
    let unavailable = || {
        HandlerError::new(
            "hub_unavailable",
            format!("hub tail did not apply through sequence {sequence}"),
        )
    };
    timeout_at(deadline, async {
        loop {
            // Register before reading progress so a concurrent commit cannot
            // leave the re-plan parked behind a cursor it has already reached.
            let notified = log.commits.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if log.applied_sequence()? >= sequence {
                return Ok(());
            }
            if matches!(log.health.log_state.load(Ordering::Relaxed), 4..=6) {
                return Err(unavailable());
            }
            notified.await;
        }
    })
    .await
    .map_err(|_| unavailable())?
}

fn op(body: &Value) -> Result<&str, HandlerError> {
    body.get("op")
        .and_then(Value::as_str)
        .filter(|op| !op.is_empty())
        .ok_or_else(|| HandlerError::new("record_invalid", "identity-log body has no op"))
}

pub(super) fn hub_sequence(base: i64, applied: i64) -> Result<u64, HandlerError> {
    applied
        .checked_sub(base)
        .and_then(|sequence| u64::try_from(sequence).ok())
        .ok_or_else(|| {
            HandlerError::new("identity_log_stalled", "hub cursor precedes the log base")
        })
}

fn registry_position(base: i64, sequence: u64) -> Result<i64, HandlerError> {
    i64::try_from(sequence)
        .ok()
        .filter(|sequence| *sequence > 0)
        .and_then(|sequence| base.checked_add(sequence))
        .ok_or_else(|| HandlerError::new("identity_log_stalled", "invalid hub log sequence"))
}

#[derive(Default)]
struct Snapshot {
    // Held only until all parts can be installed in one store transaction.
    entries: Vec<RemoteEntry>,
    records: Vec<VerifiedRecord>,
    identity: Option<(String, u64)>,
}

struct Tail<T, U> {
    link: Arc<Link<T, U>>,
    store: RegistryStore,
    health: Arc<HealthGauges>,
    commits: Arc<Notify>,
    base: i64,
    applied: i64,
    snapshot: Snapshot,
    membership: Arc<Membership<T>>,
    writer: Arc<tokio::sync::Mutex<()>>,
    connected_head: Option<u64>,
    epoch: Option<Arc<EpochReader<T>>>,
    highest_epoch: Option<u64>,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Tail<T, U> {
    async fn run(mut self, mut feed: Feed<LogDelivery>) {
        let mut local_keys = tokio::time::interval(Duration::from_secs(5));
        local_keys.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut connected = false;
        loop {
            tokio::select! {
                biased;
                event = feed.next_event() => match event {
                    FeedEvent::Disconnected => {
                        connected = false;
                        self.connected_head = None;
                        self.update_barrier();
                    }
                    FeedEvent::Connected => {
                        connected = true;
                        self.connected_head = None;
                        self.update_barrier();
                        if let Err(error) = self.reconnect().await {
                            if !self.retryable(&error) { self.stop(self.sequence() + 1, error); return; }
                        }
                    }
                    FeedEvent::Item(delivery) => {
                        let sequence = delivery.sequence;
                        if self.connected_head.is_none() {
                            loop {
                                match self.reconnect().await {
                                    Ok(()) => break,
                                    Err(error) if self.retryable(&error) => tokio::time::sleep(Duration::from_secs(1)).await,
                                    Err(error) => { self.stop(sequence, error); return; }
                                }
                            }
                        }
                        match self.apply(delivery).await {
                            Ok(true) => feed.applied(sequence),
                            Ok(false) => {}
                            Err(error) if self.retryable(&error) => {
                                // A head or discovery read can lose the hub
                                // while processing a delivery. Start over at
                                // the durable cursor, never skip that entry.
                                connected = false;
                                self.connected_head = None;
                                self.snapshot = Snapshot::default();
                                self.update_barrier();
                                feed = self.link.transport.tail(self.sequence()).with_durable_cursor();
                                tokio::time::sleep(Duration::from_secs(1)).await;
                            }
                            Err(error) => { self.stop(sequence, error); return; }
                        }
                    }
                },
                _ = local_keys.tick(), if connected => {
                    let refreshed = if self.connected_head.is_none() { self.reconnect().await.map(|()| false) }
                        else { self.membership.refresh(Instant::now() + READ_BUDGET).await };
                    match refreshed {
                        Ok(true) => if let Some(epoch) = &self.epoch { epoch.membership_changed.notify_one(); },
                        Err(error) if !self.retryable(&error) => { self.stop(self.sequence() + 1, error); return; }
                        _ => {}
                    }
                }
            }
            self.update_barrier();
            if self.caught_up() {
                self.publish_key().await;
            }
        }
    }

    fn sequence(&self) -> u64 {
        hub_sequence(self.base, self.applied).expect("valid applied hub position")
    }

    fn retryable(&self, error: &HandlerError) -> bool {
        matches!(
            error.code.as_str(),
            "hub_unavailable"
                | "credential_unavailable"
                | "vault_unavailable"
                | "epoch_key_unavailable"
        )
    }

    fn stop(&self, sequence: u64, error: HandlerError) {
        self.set_barrier(false);
        let state = match error.code.as_str() {
            "hub_log_gap" => 4,
            "signature_invalid" | "member_episode_invalid" | "membership_discovery_unresolved" => 5,
            _ => 6,
        };
        self.health.log_state.store(state, Ordering::Relaxed);
        self.commits.notify_waiters();
        tracing::warn!(target: "identity_log", sequence, code = error.code, message = error.message, "hub identity-log tail stopped");
    }

    async fn reconnect(&mut self) -> Result<(), HandlerError> {
        let deadline = Instant::now() + READ_BUDGET;
        let head = self.link.transport.log_head(deadline).await?;
        if head < self.sequence() {
            return Err(HandlerError::new(
                "hub_log_gap",
                "hub head is behind the applied cursor",
            ));
        }
        self.membership.discover(head, deadline).await?;
        if self.highest_epoch.is_none() {
            // Unsealed membership entries don't carry an epoch. Recover the
            // highest applied epoch from the last sealed entry at the cursor.
            for sequence in (1..=self.sequence()).rev() {
                let bytes = self
                    .link
                    .transport
                    .log_entry(sequence, deadline)
                    .await?
                    .ok_or_else(|| {
                        HandlerError::new("hub_log_gap", "missing applied hub log entry")
                    })?;
                if let Some(epoch) = UnverifiedRecord::parse(LOG_KEY, &bytes)?.epoch() {
                    self.highest_epoch = Some(epoch);
                    break;
                }
            }
        }
        self.connected_head = Some(head);
        Ok(())
    }

    fn caught_up(&self) -> bool {
        self.connected_head
            .is_some_and(|head| self.sequence() >= head)
    }

    fn update_barrier(&self) {
        let ready = self.caught_up() && self.membership.can_write(self.sequence()).is_some();
        self.set_barrier(ready);
    }

    fn set_barrier(&self, ready: bool) {
        let state = if ready { 2 } else { 1 };
        let previous = self.health.hub_membership.swap(state, Ordering::AcqRel);
        if previous != state {
            self.health
                .hub_membership_incarnation
                .fetch_add(1, Ordering::AcqRel);
            self.commits.notify_waiters();
        }
        if ready && previous != 2 {
            if let Some(epoch) = &self.epoch {
                epoch.ready.notify_one();
            }
        }
    }

    async fn publish_key(&self) {
        let Some((episode, key)) = self.membership.publication(self.sequence()) else {
            return;
        };
        if !self.store.hub_snapshot_applied(self.base).unwrap_or(false) {
            return;
        }
        if let (Some(reader), Some(request)) = (
            &self.epoch,
            self.membership
                .episode_join_request(&key.box_id, self.sequence()),
        ) {
            if !reader.admission_wrap_imported(&request).unwrap_or(false) {
                return;
            }
        }
        let Ok(_guard) = self.writer.try_lock() else {
            return;
        };
        let digest = entorhinal_catalog::sha256_hex(
            serde_json::to_vec(&key).expect("serializable member.key"),
        );
        let request_key = format!("member.key:{}:{}:{digest}", key.box_id, self.sequence());
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        let result = self
            .link
            .writer(&self.store, &trust)
            .run(
                &hub_write::Request {
                    request_key: &request_key,
                    params_digest: &digest,
                    writer: &key.box_id,
                    writer_ep: episode,
                },
                &Step {
                    name: "member.key".into(),
                    key: LOG_KEY.into(),
                    expected: self.sequence(),
                    payload: Payload::Record {
                        kind: "member.key".into(),
                        body: serde_json::to_value(&key).expect("serializable member.key"),
                        snapshot_id: None,
                    },
                },
                None,
                Instant::now() + READ_BUDGET,
            )
            .await;
        if let Err(error) = result {
            tracing::debug!(target: "identity_log", code = error.code, "hub member key not published yet");
        }
    }

    // Returns true only when this delivery has moved the durable cursor.
    async fn apply(&mut self, delivery: LogDelivery) -> Result<bool, HandlerError> {
        let position = registry_position(self.base, delivery.sequence)?;
        let next = self
            .snapshot
            .entries
            .last()
            .map_or(self.applied, |last| last.position)
            .checked_add(1);
        if let Some(missing) = next.filter(|&next| position > next) {
            return Err(HandlerError::new(
                "hub_log_gap",
                format!("missing log position {missing}"),
            ));
        }
        let unverified = UnverifiedRecord::parse(LOG_KEY, &delivery.bytes)?;
        let id = &unverified.header().entry_id;
        match self.store.journal_log_position(id)? {
            Some(stored) if stored == position && position <= self.applied => return Ok(false),
            Some(_) => return Err(position_conflict()),
            None if position <= self.applied => return Err(position_conflict()),
            None => {}
        }
        for part in &self.snapshot.entries {
            if part.position == position && part.entry_id == *id {
                return Ok(false);
            }
            if part.position == position || part.entry_id == *id {
                return Err(position_conflict());
            }
        }
        if next != Some(position) {
            return Err(position_conflict());
        }
        let deadline = Instant::now() + READ_BUDGET;
        let version = unverified.key_version().to_owned();
        let membership =
            entorhinal_core::hub_store::MEMBERSHIP_OPS.contains(&unverified.header().kind.as_str());
        let (signer, record, entry) = timeout_at(deadline, async {
            let (signer, record) = self
                .membership
                .verify_log(unverified, delivery.sequence, deadline)
                .await?;
            let floor = self
                .snapshot
                .records
                .last()
                .and_then(VerifiedRecord::epoch)
                .or(self.highest_epoch);
            reject_rollback(floor, record.epoch())?;
            if let (Some(reader), Some(e)) = (&self.epoch, record.epoch()) {
                reader.require_for_open(e, deadline).await?;
            }
            let entry = record.open(self.link.vault.as_ref()).await?;
            Ok::<_, HandlerError>((signer, record, entry))
        })
        .await
        .map_err(|_| {
            HandlerError::new(
                "hub_unavailable",
                "hub record verification or open timed out",
            )
        })??;
        let body: Value = serde_json::from_slice(&entry)
            .map_err(|error| HandlerError::new("record_invalid", error.to_string()))?;
        let header = record.header();
        if !membership && op(&body)? != header.kind {
            return Err(HandlerError::new(
                "record_invalid",
                "hub log envelope kind differs from body op",
            ));
        }
        let snapshot = header.kind == SNAPSHOT;
        let entry = RemoteEntry {
            position,
            entry_id: header.entry_id.clone(),
            author: header.writer.clone(),
            signer,
            key_id: version,
            envelope_version: 1,
            kind: if membership {
                header.kind.as_str()
            } else if snapshot {
                "snapshot"
            } else {
                "change"
            }
            .into(),
            entry,
        };
        if snapshot || self.snapshot.identity.is_some() {
            let (id, part, count) = snapshot_part_info(&entry)?;
            if self
                .snapshot
                .identity
                .get_or_insert_with(|| (id.clone(), count))
                != &(id, count)
                || part != self.snapshot.entries.len() as u64 + 1
            {
                return Err(HandlerError::new(
                    "record_invalid",
                    "hub snapshot parts are not in order",
                ));
            }
            self.snapshot.entries.push(entry);
            self.snapshot.records.push(record);
            if part != count {
                return Ok(false);
            }
            let snapshot = std::mem::take(&mut self.snapshot);
            if let Some(through) = snapshot_supersedes_through(&snapshot.entries)? {
                self.check_supersede(through, &snapshot.entries, deadline)
                    .await?;
            }
            let episodes = snapshot
                .records
                .iter()
                .map(|record| record.header().writer_ep)
                .collect::<Vec<_>>();
            self.store
                .apply_hub_remote_snapshot(&snapshot.entries, &episodes)?;
            self.highest_epoch = snapshot
                .records
                .last()
                .and_then(VerifiedRecord::epoch)
                .or(self.highest_epoch);
            self.committed(position);
            for (entry, record) in snapshot.entries.iter().zip(&snapshot.records) {
                self.settle_applied(record, hub_sequence(self.base, entry.position)?, deadline)
                    .await;
            }
        } else {
            self.store
                .apply_hub_remote_entry(&entry, header.writer_ep)?;
            self.highest_epoch = record.epoch().or(self.highest_epoch);
            self.committed(position);
            if header.kind == "member.admitted" {
                if let Some(epoch) = &self.epoch {
                    epoch.membership_changed.notify_one();
                }
            }
            self.settle_applied(&record, delivery.sequence, deadline)
                .await;
        }
        Ok(true)
    }

    async fn check_supersede(
        &self,
        through: u64,
        entries: &[RemoteEntry],
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        if self.base > 0 {
            // The hub cannot prove authorship of the engram prefix. Replacing
            // its rows could delete identities written by another box.
            return Err(HandlerError::new(
                "hub_supersede_migrated_prefix",
                "supersede cannot replace the migrated log prefix",
            ));
        }
        let author = &entries[0].author;
        if entries.iter().any(|entry| &entry.author != author) {
            return Err(HandlerError::new(
                "record_invalid",
                "supersede parts have different box authors",
            ));
        }
        for sequence in 1..=through {
            let bytes = self
                .link
                .transport
                .log_entry(sequence, deadline)
                .await?
                .ok_or_else(|| {
                    HandlerError::new(
                        "hub_log_gap",
                        format!("supersede prefix is missing sequence {sequence}"),
                    )
                })?;
            let record = UnverifiedRecord::parse(LOG_KEY, &bytes)?;
            let key = self.membership.log_key(&record, sequence)?;
            let record = record.verify(&key)?;
            if &record.header().writer != author {
                return Err(HandlerError::new(
                    "record_invalid",
                    format!("supersede replaces another box's entry at sequence {sequence}"),
                ));
            }
        }
        Ok(())
    }

    fn committed(&mut self, position: i64) {
        self.applied = position;
        self.update_barrier();
        crate::enable::refresh(&self.health, &self.store);
        if let Ok(generation) = self.store.generation() {
            self.health
                .generation
                .fetch_max(generation, Ordering::Relaxed);
        }
        self.commits.notify_waiters();
    }

    async fn settle_applied(&self, record: &VerifiedRecord, sequence: u64, deadline: Instant) {
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            store: &self.store,
            base: self.base,
        };
        if let Err(error) = self
            .link
            .writer(&self.store, &trust)
            .record_applied(record, sequence, deadline)
            .await
        {
            // Journal and cursor have already committed; an open attempt can
            // still be settled by its exact sequence on the caller's retry.
            tracing::warn!(target: "identity_log", sequence, code = error.code, "could not record outcome for an applied hub entry");
        }
    }
}

fn reject_rollback(previous: Option<u64>, next: Option<u64>) -> Result<(), HandlerError> {
    if previous
        .zip(next)
        .is_some_and(|(previous, next)| next < previous)
    {
        return Err(HandlerError::new(
            "epoch_rollback",
            "sealed record uses an older fleet epoch",
        ));
    }
    Ok(())
}

fn position_conflict() -> HandlerError {
    HandlerError::new(
        "hub_log_position_conflict",
        "hub log position and entry ID disagree with the journal or buffered snapshot",
    )
}

#[cfg(test)]
mod tests;

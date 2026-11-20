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
use futures_util::{future::BoxFuture, StreamExt};
use serde_json::Value;
use tokio::{
    sync::{watch, Notify},
    task::JoinSet,
    time::{timeout_at, Instant},
};

use crate::{
    fleet_record::{vault::EpochVault, UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_epoch::EpochReader,
    hub_membership::{AnchorSource, Membership, PeerKeysRead, Sight},
    hub_transport::{Feed, FeedEvent, Hub, LogDelivery},
    hub_write::{
        self, HubSql, LogReplan, Payload, Replanned, Step, Transport, Trust, Writer,
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

    fn epoch_watch(
        self: &Arc<Self>,
        _own_box: String,
    ) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        None
    }
}

impl LogTransport for Hub {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        self.tail(applied)
    }

    fn epoch_watch(
        self: &Arc<Self>,
        own_box: String,
    ) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        Some(self.epoch_watch(own_box))
    }

    async fn join_keys(
        &self,
        prefix: &str,
        deadline: Instant,
    ) -> Result<Vec<String>, HandlerError> {
        self.session(deadline)
            .await?
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
    fn writer<'a, V: Trust>(&'a self, sql: &'a HubSql, trust: &'a V) -> Writer<'a, T, V> {
        Writer {
            sql,
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
    sql: &'a HubSql,
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
            let applied = applied_sequence(self.sql, self.base, deadline).await?;
            self.membership
                .verify_kv(record, applied, Sight::FirstOrAfterGap, deadline)
                .await
                .map(|(record, _)| record)
        }
    }
}

pub(super) struct IdentityLog<T, U> {
    link: Arc<Link<T, U>>,
    pub(super) sql: HubSql,
    health: Arc<HealthGauges>,
    writer: Arc<tokio::sync::Mutex<()>>,
    pub(super) commits: Arc<Notify>,
    pub(super) box_id: BoxId,
    pub(super) base: i64,
    membership: Arc<Membership<T>>,
    pub(super) epoch: Option<Arc<EpochReader<T>>>,
}

impl ProjectsHandler {
    pub(super) fn hub_sql(&self) -> HubSql {
        HubSql::new(&self.store, self.hub_sql.clone())
    }

    pub(super) async fn stop_hub_tasks(&self) {
        // Stop activation before stopping the consumers it can install.
        if self.hub_switch.is_some() {
            let task = self.catch_up_task.lock().unwrap().take();
            if let Some(task) = task {
                task.abort();
                let _ = task.await;
            }
        }
        tokio::join!(self.stop_hub_tail(), self.stop_hub_residence());
    }

    pub(super) async fn stop_hub_tail(&self) {
        let task = self.hub_tail_task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.hub_sql.wait().await;
    }

    #[cfg(test)]
    pub(super) fn activate_hub_identity_log<T, U>(
        &self,
        box_id: BoxId,
        link: Link<T, U>,
    ) -> Result<IdentityLog<T, U>, HandlerError>
    where
        T: LogTransport,
        U: Trust + Send + Sync + 'static,
    {
        let sql = self.hub_sql();
        let base = sql.test_store().hub_log_base()?.ok_or_else(|| {
            HandlerError::new("hub_not_ready", "hub log base has not been installed")
        })?;
        if self.catch_up_task.lock().unwrap().is_some() || self.health.origin.is_foreign() {
            return Err(HandlerError::new(
                "hub_not_ready",
                "cannot start the hub identity-log tail",
            ));
        }
        let log = IdentityLog::new(
            sql,
            self.health.clone(),
            self.writer.clone(),
            self.commits.clone(),
            box_id,
            base,
            link,
        );
        let tail = log.start_installed_tail_for_test()?;
        *self.hub_tail_task.lock().unwrap() = Some(tail);
        Ok(log)
    }
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> IdentityLog<T, U> {
    pub(super) fn new(
        sql: HubSql,
        health: Arc<HealthGauges>,
        writer: Arc<tokio::sync::Mutex<()>>,
        commits: Arc<Notify>,
        box_id: BoxId,
        base: i64,
        link: Link<T, U>,
    ) -> Self {
        let link = Arc::new(link);
        let membership = Arc::new(Membership::new(
            link.transport.clone(),
            sql.clone(),
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
                sql.clone(),
                link.vault.clone(),
                box_id.to_string(),
                base,
                health.clone(),
            ))
        });
        Self {
            link,
            sql,
            health,
            writer,
            commits,
            box_id,
            base,
            membership,
            epoch,
        }
    }

    fn start_tail(&self, applied: i64, sequence: u64) -> tokio::task::JoinHandle<()> {
        let feed = self.link.transport.tail(sequence).with_durable_cursor();
        let tail = Tail {
            link: self.link.clone(),
            sql: self.sql.clone(),
            health: self.health.clone(),
            commits: self.commits.clone(),
            base: self.base,
            applied,
            snapshot: Snapshot::default(),
            membership: self.membership.clone(),
            writer: self.writer.clone(),
            connected_head: None,
            epoch: self.epoch.clone(),
            highest_epoch: None,
        };
        self.health.hub_membership.store(1, Ordering::Release);
        self.health.log_state.store(0, Ordering::Relaxed);
        self.health.hub.set_writer_hold(None);
        tokio::spawn(tail.run(feed))
    }

    pub(super) async fn start_installed_tail(
        &self,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, HandlerError> {
        let health = self.health.clone();
        let applied = self
            .sql
            .run(deadline, "identity_log_status", move |store| {
                let applied = store.identity_log_status()?.last_applied_position;
                crate::enable::refresh(&health, store);
                Ok(applied)
            })
            .await?;
        Ok(self.start_tail(applied, hub_sequence(self.base, applied)?))
    }

    #[cfg(test)]
    pub(super) fn start_installed_tail_for_test(
        &self,
    ) -> Result<tokio::task::JoinHandle<()>, HandlerError> {
        let store = self.sql.test_store();
        let applied = store.identity_log_status()?.last_applied_position;
        crate::enable::refresh(&self.health, &store);
        Ok(self.start_tail(applied, hub_sequence(self.base, applied)?))
    }

    pub(super) async fn discover_bootstrap_membership(
        &self,
        head: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.membership.discover(head, deadline).await
    }

    pub(super) async fn ensure_bootstrap_key(
        self: &Arc<Self>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let Some(epoch) = &self.epoch else {
            return Ok(());
        };
        if !self.has_bootstrap_epoch(deadline).await? {
            // Computers paired at cutover can import the epoch key encrypted
            // for them when it was minted. A newly paired computer asks a
            // current member to encrypt the retained epoch keys for it.
            match epoch.fence(deadline).await {
                Ok(_) if self.has_bootstrap_epoch(deadline).await? => return Ok(()),
                Ok(_) => (),
                Err(error) if error.code == "epoch_key_unavailable" => (),
                Err(error) => return Err(error),
            }
            if let Some(controller) = crate::hub_epoch::Controller::new(self.clone()) {
                controller.joins(deadline).await?;
            }
        }
        Ok(())
    }

    async fn has_bootstrap_epoch(&self, deadline: Instant) -> Result<bool, HandlerError> {
        self.sql
            .run(deadline, "hub_has_epoch", |store| {
                Ok(store.hub_has_epoch(1)?)
            })
            .await
    }

    pub(super) async fn open_bootstrap_part(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> Result<(String, VerifiedRecord, Vec<u8>), HandlerError> {
        let bytes = self
            .link
            .transport
            .log_entry(sequence, deadline)
            .await?
            .ok_or_else(|| {
                HandlerError::new(
                    "hub_migration_mismatch",
                    "snapshot prefix has a missing sequence",
                )
            })?;
        let parsed = UnverifiedRecord::parse(LOG_KEY, &bytes)?;
        let (signer, record) = self
            .membership
            .verify_log(parsed, sequence, deadline)
            .await?;
        if let (Some(reader), Some(epoch)) = (&self.epoch, record.epoch()) {
            reader.require_for_open(epoch, deadline).await?;
        }
        let plaintext = timeout_at(deadline, record.open(self.link.vault.as_ref()))
            .await
            .map_err(|_| {
                HandlerError::new("hub_unavailable", "opening snapshot part timed out")
            })??;
        Ok((signer, record, plaintext))
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

pub(super) type LocalReplan =
    Arc<dyn Fn(&RegistryStore, &Value, &str) -> Result<Replanned, HandlerError> + Send + Sync>;

#[async_trait::async_trait]
pub(super) trait SharedLog: Send + Sync {
    async fn start_installed_tail(
        &self,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, HandlerError>;
    async fn applied(&self, deadline: Instant) -> Result<u64, HandlerError>;
    async fn catch_up(&self, deadline: Instant) -> Result<(), HandlerError>;
    #[allow(clippy::too_many_arguments)]
    async fn append_local(
        &self,
        key: &str,
        digest: &str,
        expected: u64,
        body: Value,
        continuation: Option<&entorhinal_core::HubContinuation>,
        replan: LocalReplan,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError>;
}

#[async_trait::async_trait]
impl<T: LogTransport, U: Trust + Send + Sync + 'static> SharedLog for IdentityLog<T, U> {
    async fn start_installed_tail(
        &self,
        deadline: Instant,
    ) -> Result<tokio::task::JoinHandle<()>, HandlerError> {
        IdentityLog::start_installed_tail(self, deadline).await
    }
    async fn applied(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.applied_sequence(deadline).await
    }

    async fn catch_up(&self, deadline: Instant) -> Result<(), HandlerError> {
        wait_for_applied(self, self.log_head(deadline).await?, deadline).await
    }

    async fn append_local(
        &self,
        key: &str,
        digest: &str,
        expected: u64,
        body: Value,
        continuation: Option<&entorhinal_core::HubContinuation>,
        replan: LocalReplan,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let writer_ep = self.writer_episode(deadline).await?;
        let landed = self
            .append_inner(
                PlannedAppend {
                    request_key: key,
                    params_digest: digest,
                    writer_ep,
                    expected,
                    body,
                },
                replan,
                Some(continuation),
                deadline,
            )
            .await?;
        let position = landed.position.ok_or_else(position_conflict)?;
        wait_for_applied(self, position, deadline)
            .await
            .map_err(|error| {
                if error.code == "hub_unavailable" {
                    crate::hub_transport::unsettled(&landed.op_id)
                } else {
                    error
                }
            })?;
        let key = key.to_owned();
        let digest = digest.to_owned();
        self.sql
            .run(deadline, "hub_local_reply", move |store| {
                Ok(store.hub_local_reply(&key, &digest)?)
            })
            .await?
            .ok_or_else(|| {
                HandlerError::new("storage_error", "hub tail applied without a local reply")
            })
    }
}

pub(super) struct AcceptedKv {
    pub record: VerifiedRecord,
    pub body: Value,
    pub writer_current: bool,
    pub applied: u64,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> IdentityLog<T, U> {
    pub(super) fn epoch_watch(&self) -> Option<Feed<async_nats::jetstream::kv::Entry>> {
        self.link.transport.epoch_watch(self.box_id.to_string())
    }

    pub(super) fn peer_keys(&self) -> &dyn PeerKeysRead {
        self.link.peers.as_ref()
    }

    pub(super) fn anchor_changes(&self) -> watch::Receiver<u64> {
        self.membership.changes()
    }

    pub(super) fn require_known_member(
        &self,
        box_id: &str,
        episode: u64,
        applied: u64,
    ) -> Result<(), HandlerError> {
        self.membership
            .require_known_member(box_id, episode, applied)
    }

    pub(super) async fn write_epoch_step(
        &self,
        request_key: &str,
        digest: &str,
        step: &Step,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let writer_ep = self.writer_episode(deadline).await?;
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
            sql: &self.sql,
            base: self.base,
        };
        self.link
            .writer(&self.sql, &trust)
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

    pub(super) async fn holder_current(
        &self,
        holder: &str,
        episode: u64,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        Ok(self.membership_ready()
            && self
                .membership
                .open_episode(holder, self.applied_sequence(deadline).await?)
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

    pub(super) async fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<hub_write::KvEntry>, HandlerError> {
        self.link.transport.kv_entry(key, deadline).await
    }

    pub(super) async fn writer_episode(&self, deadline: Instant) -> Result<u64, HandlerError> {
        if self.membership_ready()
            || self
                .sql
                .run(deadline, "hub_log_base", |store| {
                    Ok(store.hub_log_base()?.is_none())
                })
                .await?
        {
            if let Some(episode) = self
                .membership
                .can_write(self.applied_sequence(deadline).await?)
            {
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
        let key = request.request_key.to_owned();
        let row = self
            .sql
            .run(deadline, "hub_attempt", move |store| {
                Ok(store.hub_attempt(&key)?)
            })
            .await?;
        if row.as_ref().is_none_or(|row| row.outcome.is_none())
            && self.writer_episode(deadline).await? != request.writer_ep
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
            sql: &self.sql,
            base: self.base,
        };
        Writer {
            replan,
            ..self.link.writer(&self.sql, &trust)
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
            sql: &self.sql,
            base: self.base,
        };
        self.link
            .writer(&self.sql, &trust)
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
        let applied = self.applied_sequence(deadline).await?;
        if let Some(epoch) = record.epoch() {
            let owned_key = key.to_owned();
            let old = self
                .sql
                .run(deadline, "hub_kv_cached", move |store| {
                    Ok(store.hub_kv_cached(&owned_key)?.and_then(|row| row.value))
                })
                .await?;
            if let Some(old) = old {
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
                    self.membership.discover_cached(head, deadline).await?;
                    self.membership
                        .kv_key(&record, applied.max(head), sight)?
                        .ok_or_else(|| {
                            crate::hub_membership::writer_unknown(&record.header().writer)
                        })?;
                    wait_for_applied(self, head, deadline).await?;
                    self.membership
                        .kv_key(&record, self.applied_sequence(deadline).await?, sight)?
                        .ok_or_else(|| {
                            crate::hub_membership::writer_unknown(&record.header().writer)
                        })?
                }
            };
            let verified = record.verify(&key)?;
            if !self.membership_ready() {
                return Err(HandlerError::new(
                    "hub_membership_stale",
                    "hub membership view is catching up",
                ));
            }
            verified
        };
        let applied = self.applied_sequence(deadline).await?;
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
        if !join && (!self.membership_ready() || self.applied_sequence(deadline).await? != applied)
        {
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

    pub(super) async fn applied_sequence(&self, deadline: Instant) -> Result<u64, HandlerError> {
        applied_sequence(&self.sql, self.base, deadline).await
    }

    /// Retry at the same position with the saved body. A new position needs a
    /// fresh body built against the applied store, not a copy of old rows.
    pub(super) async fn append(
        &self,
        plan: PlannedAppend<'_>,
        replan: impl Fn(&RegistryStore, &Value) -> Result<Replanned, HandlerError>
            + Send
            + Sync
            + 'static,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        self.append_inner(
            plan,
            Arc::new(move |store, body, _| replan(store, body)),
            None,
            deadline,
        )
        .await
    }

    async fn append_inner(
        &self,
        plan: PlannedAppend<'_>,
        replan: LocalReplan,
        local: Option<Option<&entorhinal_core::HubContinuation>>,
        deadline: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        let kind = op(&plan.body)?.to_owned();
        let _guard = if local.is_none() {
            Some(
                timeout_at(deadline, self.writer.lock())
                    .await
                    .map_err(|_| {
                        HandlerError::new(
                            "identity_log_contended",
                            "waiting for the hub log writer",
                        )
                    })?,
            )
        } else {
            None
        };
        if matches!(self.health.log_state.load(Ordering::Relaxed), 4..=6) {
            return Err(HandlerError::new(
                "identity_log_stalled",
                "hub identity-log tail has stopped",
            ));
        }
        if self
            .membership
            .can_write(self.applied_sequence(deadline).await?)
            != Some(plan.writer_ep)
        {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "local box cannot write in that membership episode",
            ));
        }
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            sql: &self.sql,
            base: self.base,
        };
        let planner = Planner {
            log: self,
            replan,
            writer_ep: plan.writer_ep,
        };
        let writer = Writer {
            replan: Some(&planner),
            ..self.link.writer(&self.sql, &trust)
        };
        let request = hub_write::Request {
            request_key: plan.request_key,
            params_digest: plan.params_digest,
            writer: &self.box_id.to_string(),
            writer_ep: plan.writer_ep,
        };
        let step = Step {
            name: if local.is_some() {
                format!("local:{kind}")
            } else {
                kind.clone()
            },
            key: LOG_KEY.into(),
            expected: plan.expected,
            payload: Payload::Record {
                kind,
                body: plan.body,
                snapshot_id: None,
            },
        };
        match local {
            Some(continuation) => {
                writer
                    .run_local(&request, &step, continuation, deadline)
                    .await
            }
            None => writer.run(&request, &step, None, deadline).await,
        }
    }
}

struct Planner<'a, T, U> {
    log: &'a IdentityLog<T, U>,
    replan: LocalReplan,
    writer_ep: u64,
}

impl<T, U> LogReplan for Planner<'_, T, U>
where
    T: LogTransport,
    U: Trust + Send + Sync + 'static,
{
    fn wait_for_head(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<(), HandlerError>> {
        Box::pin(wait_for_applied(self.log, sequence, deadline))
    }

    fn after_image(
        &self,
        saved: Value,
        id: String,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<Replanned, HandlerError>> {
        let replan = self.replan.clone();
        let membership = self.log.membership.clone();
        let writer_ep = self.writer_ep;
        let base = self.log.base;
        Box::pin(
            self.log
                .sql
                .run(deadline, "replan_hub_write", move |store| {
                    let applied =
                        hub_sequence(base, store.identity_log_status()?.last_applied_position)?;
                    if membership.can_write(applied) != Some(writer_ep) {
                        return Err(HandlerError::new(
                            "member_episode_invalid",
                            "local box cannot replan in that membership episode",
                        ));
                    }
                    let outcome = replan(store, &saved, &id)?;
                    if let Replanned::Body(body) | Replanned::WithLocal(body, _) = &outcome {
                        if op(body)? != op(&saved)? {
                            return Err(HandlerError::new(
                                "record_invalid",
                                "re-plan changed the identity-log operation",
                            ));
                        }
                    }
                    Ok(outcome)
                }),
        )
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
            if log.applied_sequence(deadline).await? >= sequence {
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

pub(super) async fn applied_sequence(
    sql: &HubSql,
    base: i64,
    deadline: Instant,
) -> Result<u64, HandlerError> {
    sql.run(deadline, "identity_log_status", move |store| {
        hub_sequence(base, store.identity_log_status()?.last_applied_position)
    })
    .await
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
    sql: HubSql,
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
        let mut peers = self.link.peers.clone().peer_key_updates();
        let (keys, mut key_updates) = watch::channel(());
        let (caught_up, publications) = watch::channel(None);
        // Dropping the tail also stops its two background tasks.
        let mut tasks = JoinSet::new();
        tasks.spawn(watch_own_key(self.link.signer.clone(), keys));
        tasks.spawn(
            KeyPublisher {
                link: self.link.clone(),
                sql: self.sql.clone(),
                writer: self.writer.clone(),
                base: self.base,
                membership: self.membership.clone(),
                epoch: self.epoch.clone(),
            }
            .run(publications),
        );
        let mut retry_at = None;
        let mut own_retry_at = None;
        let mut blocked = false;
        let mut held = None;
        loop {
            if !self.sql.available() {
                self.stop(self.sequence() + 1, hub_write::store_unavailable());
                return;
            }
            let anchor_change;
            tokio::select! {
                event = feed.next_event(), if retry_at.is_none() => { anchor_change = Ok(false); match event {
                    FeedEvent::Disconnected => {
                        self.connected_head = None;
                        self.snapshot = Snapshot::default();
                    }
                    FeedEvent::Connected => {
                        self.connected_head = None;
                        self.update_barrier();
                        match self.reconnect().await {
                            Ok(()) => {
                                blocked = false;
                                if let Some(epoch) = &self.epoch { epoch.membership_changed.notify_one(); }
                            }
                            Err(error) if self.retryable(&error) => {
                                retry_at = Some(Instant::now() + Duration::from_secs(1));
                            }
                            Err(error) => { self.stop(self.sequence() + 1, error); return; }
                        }
                    }
                    // Keep polling for a reconnect, but do not apply anything
                    // after the held entry. Reopening replays from the durable cursor.
                    FeedEvent::Item(_) if blocked => {}
                    FeedEvent::Item(delivery) => {
                        let sequence = delivery.sequence;
                        match self.apply(delivery).await {
                            Ok(committed) => {
                                if committed { feed.applied(sequence); }
                                if held == Some(sequence) {
                                    held = None;
                                    self.health.hub.set_writer_hold(None);
                                    self.health.log_state.store(0, Ordering::Relaxed);
                                }
                            }
                            Err(error) if error.code == "writer_unknown" => {
                                self.health.hub.set_writer_hold(error.detail.as_ref()
                                    .and_then(|detail| detail["writer"].as_str()).map(str::to_owned));
                                self.health.log_state.store(9, Ordering::Relaxed);
                                tracing::debug!(target: "identity_log", sequence, message = error.message, "hub identity-log tail held");
                                held = Some(sequence);
                                blocked = true;
                                self.connected_head = None;
                                self.commits.notify_waiters();
                            }
                            Err(error) if self.retryable(&error) => {
                                self.connected_head = None;
                                self.snapshot = Snapshot::default();
                                retry_at = Some(Instant::now() + Duration::from_secs(1));
                            }
                            Err(error) => { self.stop(sequence, error); return; }
                        }
                    }
                }},
                _ = async { tokio::time::sleep_until(retry_at.expect("scheduled replay")).await }, if retry_at.is_some() => {
                    retry_at = None;
                    feed.replay();
                    anchor_change = Ok(false);
                }
                Some(update) = peers.next() => {
                    anchor_change = match update {
                        Ok(peers) => self.membership.refresh_peers(peers, Instant::now() + READ_BUDGET).await,
                        Err(error) => Err(error),
                    };
                }
                _ = async {
                    tokio::select! {
                        Ok(()) = key_updates.changed() => {},
                        _ = async {
                            match own_retry_at {
                                Some(at) => tokio::time::sleep_until(at).await,
                                None => std::future::pending().await,
                            }
                        } => {},
                    }
                } => {
                    anchor_change = self.membership.refresh_own(Instant::now() + READ_BUDGET).await;
                    own_retry_at = anchor_change.as_ref().is_err_and(|error| self.retryable(error))
                        .then(|| Instant::now() + Duration::from_secs(1));
                }
            }
            match anchor_change {
                Ok(true) => {
                    if let Some(epoch) = &self.epoch {
                        epoch.membership_changed.notify_one();
                    }
                    if blocked {
                        self.snapshot = Snapshot::default();
                        feed.replay();
                    }
                }
                Err(error) if !self.retryable(&error) => {
                    self.stop(self.sequence() + 1, error);
                    return;
                }
                _ => {}
            }
            self.update_barrier();
            caught_up.send_replace(self.caught_up().then(|| self.sequence()));
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
                | "hub_sql_timeout"
                | "storage_error"
                | "local_identity_unavailable"
        )
    }

    fn stop(&self, sequence: u64, error: HandlerError) {
        self.set_barrier(false);
        self.health.hub.set_writer_hold(None);
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
}

async fn watch_own_key(signer: Arc<dyn CredentialResolver>, changes: watch::Sender<()>) {
    let mut version = None;
    let mut backoff = Duration::from_secs(1);
    loop {
        if let Ok(Ok(mut watch)) =
            tokio::time::timeout(READ_BUDGET, signer.watch_scoped(None)).await
        {
            while let Some(notice) = watch.next().await {
                backoff = Duration::from_secs(1);
                if version.as_ref() != Some(&notice.keys) {
                    version = Some(notice.keys);
                    changes.send_replace(());
                }
            }
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(Duration::from_secs(60));
    }
}

struct KeyPublisher<T, U> {
    link: Arc<Link<T, U>>,
    sql: HubSql,
    writer: Arc<tokio::sync::Mutex<()>>,
    base: i64,
    membership: Arc<Membership<T>>,
    epoch: Option<Arc<EpochReader<T>>>,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> KeyPublisher<T, U> {
    async fn run(self, mut cursor: watch::Receiver<Option<u64>>) {
        loop {
            if !self.sql.available() {
                return;
            }
            let sequence = *cursor.borrow_and_update();
            if let Some(sequence) = sequence {
                if self.publish_key(sequence).await {
                    tokio::select! {
                        _ = cursor.changed() => {},
                        _ = tokio::time::sleep(Duration::from_secs(1)) => {},
                    }
                    continue;
                }
            }
            if cursor.changed().await.is_err() {
                return;
            }
        }
    }

    // Return true when publication is deferred and should be retried.
    async fn publish_key(&self, sequence: u64) -> bool {
        let Some((episode, key)) = self.membership.publication(sequence) else {
            return false;
        };
        let deadline = Instant::now() + READ_BUDGET;
        let base = self.base;
        match self
            .sql
            .run(deadline, "hub_snapshot_applied", move |store| {
                Ok(store.hub_snapshot_applied(base)?)
            })
            .await
        {
            Ok(true) => {}
            Ok(false) => return false,
            Err(error) => return error.code != "store_unavailable",
        }
        if let (Some(reader), Some(request)) = (
            &self.epoch,
            self.membership.episode_join_request(&key.box_id, sequence),
        ) {
            if !reader
                .admission_wrap_imported(&request, deadline)
                .await
                .unwrap_or(false)
            {
                return true;
            }
        }
        let Ok(_guard) = self.writer.try_lock() else {
            return true;
        };
        let digest = entorhinal_catalog::sha256_hex(
            serde_json::to_vec(&key).expect("serializable member.key"),
        );
        let request_key = format!("member.key:{}:{sequence}:{digest}", key.box_id);
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            sql: &self.sql,
            base: self.base,
        };
        let result = self
            .link
            .writer(&self.sql, &trust)
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
                    expected: sequence,
                    payload: Payload::Record {
                        kind: "member.key".into(),
                        body: serde_json::to_value(&key).expect("serializable member.key"),
                        snapshot_id: None,
                    },
                },
                None,
                deadline,
            )
            .await;
        if let Err(error) = result {
            tracing::debug!(target: "identity_log", code = error.code, "hub member key not published yet");
            return true;
        }
        false
    }
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Tail<T, U> {
    // Returns true only when this delivery has moved the durable cursor.
    async fn apply(&mut self, delivery: LogDelivery) -> Result<bool, HandlerError> {
        let deadline = Instant::now() + READ_BUDGET;
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
        let owned_id = id.clone();
        match self
            .sql
            .run(deadline, "journal_log_position", move |store| {
                Ok(store.journal_log_position(&owned_id)?)
            })
            .await?
        {
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
            let health = self.health.clone();
            let entries = self
                .sql
                .apply(deadline, "apply_hub_remote_snapshot", move |store| {
                    store.apply_hub_remote_snapshot(&snapshot.entries, &episodes)?;
                    refresh_committed(&health, store);
                    Ok(snapshot.entries)
                })
                .await?;
            self.highest_epoch = snapshot
                .records
                .last()
                .and_then(VerifiedRecord::epoch)
                .or(self.highest_epoch);
            self.committed(position);
            for (entry, record) in entries.iter().zip(&snapshot.records) {
                self.settle_applied(record, hub_sequence(self.base, entry.position)?, deadline)
                    .await;
            }
        } else {
            let health = self.health.clone();
            let writer_ep = header.writer_ep;
            self.sql
                .apply(deadline, "apply_hub_remote_entry", move |store| {
                    store.apply_hub_remote_entry(&entry, writer_ep)?;
                    refresh_committed(&health, store);
                    Ok(())
                })
                .await?;
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
        self.commits.notify_waiters();
    }

    async fn settle_applied(&self, record: &VerifiedRecord, sequence: u64, deadline: Instant) {
        let trust = MemberTrust {
            epoch: self.link.trust.as_ref(),
            epoch_reader: self.epoch.as_deref(),
            membership: &self.membership,
            sql: &self.sql,
            base: self.base,
        };
        if let Err(error) = self
            .link
            .writer(&self.sql, &trust)
            .record_applied(record, sequence, deadline)
            .await
        {
            // Journal and cursor have already committed; an open attempt can
            // still be settled by its exact sequence on the caller's retry.
            tracing::warn!(target: "identity_log", sequence, code = error.code, "could not record outcome for an applied hub entry");
        }
    }
}

fn refresh_committed(health: &HealthGauges, store: &RegistryStore) {
    crate::enable::refresh(health, store);
    if let Ok(generation) = store.generation() {
        health.generation.fetch_max(generation, Ordering::Relaxed);
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

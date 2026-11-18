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
    hub_transport::{Feed, Hub, LogDelivery},
    hub_write::{
        self, LogReplan, Payload, Replanned, Step, Transport, Trust, Writer,
        PROJECTION_SNAPSHOT_KIND as SNAPSHOT,
    },
    HandlerError, HealthGauges, ProjectsHandler,
};

const READ_BUDGET: Duration = Duration::from_secs(25);

pub(super) trait LogTransport: Transport + Send + Sync + 'static {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery>;
}

impl LogTransport for Hub {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        self.tail(applied)
    }
}

pub(super) struct Link<T, U> {
    pub transport: Arc<T>,
    pub trust: Arc<U>,
    pub signer: Arc<dyn CredentialResolver>,
    pub vault: Arc<dyn EpochVault>,
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Link<T, U> {
    fn writer<'a>(&'a self, store: &'a RegistryStore) -> Writer<'a, T, U> {
        Writer {
            store,
            transport: &self.transport,
            trust: &self.trust,
            signer: self.signer.as_ref(),
            vault: self.vault.as_ref(),
            replan: None,
        }
    }

    async fn verify(
        &self,
        record: UnverifiedRecord,
    ) -> Result<(String, VerifiedRecord), HandlerError> {
        let key = self.trust.verification_key(&record).await?;
        let record = record.verify(&key)?;
        Ok((key, record))
    }
}

pub(super) struct IdentityLog<T, U> {
    link: Arc<Link<T, U>>,
    store: RegistryStore,
    health: Arc<HealthGauges>,
    writer: Arc<tokio::sync::Mutex<()>>,
    commits: Arc<Notify>,
    box_id: BoxId,
    base: i64,
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
        let feed = link.transport.tail(sequence).with_durable_cursor();
        let tail = Tail {
            link: link.clone(),
            store: store.clone(),
            health: self.health.clone(),
            commits: self.commits.clone(),
            base,
            applied,
            snapshot: Snapshot::default(),
        };
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

impl<T: LogTransport, U: Trust + Send + Sync + 'static> IdentityLog<T, U> {
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
        let planner = Planner { log: self, replan };
        Writer {
            replan: Some(&planner),
            ..self.link.writer(&self.store)
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

async fn wait_for_applied<T: LogTransport, U: Trust + Send + Sync + 'static>(
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

fn hub_sequence(base: i64, applied: i64) -> Result<u64, HandlerError> {
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
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Tail<T, U> {
    async fn run(mut self, mut feed: Feed<LogDelivery>) {
        loop {
            let delivery = feed.next().await;
            let sequence = delivery.sequence;
            match self.apply(delivery).await {
                Ok(true) => feed.applied(sequence),
                Ok(false) => {}
                Err(error) => {
                    let state = match error.code.as_str() {
                        "hub_log_gap" => 4,
                        "signature_invalid" => 5,
                        _ => 6,
                    };
                    self.health.log_state.store(state, Ordering::Relaxed);
                    self.commits.notify_waiters();
                    tracing::warn!(target: "identity_log", sequence, code = error.code, message = error.message, "hub identity-log tail stopped");
                    return;
                }
            }
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
        let (signer, record, entry) = timeout_at(deadline, async {
            let (signer, record) = self.link.verify(unverified).await?;
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
        if op(&body)? != header.kind {
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
            kind: if snapshot { "snapshot" } else { "change" }.into(),
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
            self.store.apply_remote_snapshot(&snapshot.entries, None)?;
            self.committed(position);
            for (entry, record) in snapshot.entries.iter().zip(&snapshot.records) {
                self.settle_applied(record, hub_sequence(self.base, entry.position)?, deadline)
                    .await;
            }
        } else {
            self.store.apply_remote_entry(&entry, None)?;
            self.committed(position);
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
            let (_, record) = timeout_at(deadline, self.link.verify(record))
                .await
                .map_err(|_| {
                    HandlerError::new("hub_unavailable", "supersede author check timed out")
                })??;
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
        crate::enable::refresh(&self.health, &self.store);
        if let Ok(generation) = self.store.generation() {
            self.health
                .generation
                .fetch_max(generation, Ordering::Relaxed);
        }
        self.commits.notify_waiters();
    }

    async fn settle_applied(&self, record: &VerifiedRecord, sequence: u64, deadline: Instant) {
        if let Err(error) = self
            .link
            .writer(&self.store)
            .record_applied(record, sequence, deadline)
            .await
        {
            // Journal and cursor have already committed; an open attempt can
            // still be settled by its exact sequence on the caller's retry.
            tracing::warn!(target: "identity_log", sequence, code = error.code, "could not record outcome for an applied hub entry");
        }
    }
}

fn position_conflict() -> HandlerError {
    HandlerError::new(
        "hub_log_position_conflict",
        "hub log position and entry ID disagree with the journal or buffered snapshot",
    )
}

#[cfg(test)]
mod tests;

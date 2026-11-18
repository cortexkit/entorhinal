//! Durable compare-and-set writes for the hub backend.

// This writer is used only when the registry selects the hub instead of engram.
#![allow(dead_code)]

use std::{future::Future, sync::OnceLock};

use cortexkit_claustrum_client::CredentialResolver;
use entorhinal_catalog::jcs;
use entorhinal_core::{
    hub_store::{HubAttempt, HubAttemptOutcome, HubAttemptSend, HubAttemptWrite},
    RegistryStore,
};
use futures_util::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::time::{timeout_at, Instant};

use crate::{
    fleet_record::{self, vault::EpochVault, Header, UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_transport::{self, Hub, Session},
    HandlerError,
};

const MAX_LOG_REBASES: usize = 5;
pub(super) const PROJECTION_SNAPSHOT_KIND: &str = "shared.snapshot";

pub(super) struct Request<'a> {
    pub request_key: &'a str,
    pub params_digest: &'a str,
    pub writer: &'a str,
    pub writer_ep: u64,
}

pub(super) fn params_digest(operation: &str, params: &Value) -> Result<String, HandlerError> {
    let bytes = jcs::to_string(&json!([operation, params]))
        .map_err(|error| HandlerError::new("invalid_params", error.to_string()))?;
    Ok(format!("{:x}", Sha256::digest(bytes)))
}

pub(super) struct Step {
    pub name: String,
    pub key: String,
    pub expected: u64,
    pub payload: Payload,
}

pub(super) enum Payload {
    Record {
        kind: String,
        body: Value,
        snapshot_id: Option<String>,
    },
    Delete,
}

pub(super) type NextStep<'a> = &'a (dyn Fn(&Value) -> Result<Step, HandlerError> + Sync);

pub(super) trait LogReplan: Sync {
    fn wait_for_head(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> BoxFuture<'_, Result<(), HandlerError>>;

    /// Rebuild the body against the applied head, keeping the pre-drawn IDs and
    /// timestamps from the saved body. Return `Replanned::Refused` when the
    /// operation is no longer valid there (a name now taken, a retired agent): the
    /// attempt then records that refusal as its final outcome and is never resent.
    /// An `Err` is a transient failure; the attempt stays open for a retry.
    fn after_image(&self, saved: &Value) -> Result<Replanned, HandlerError>;
    fn after_image_with_id(&self, saved: &Value, _id: &str) -> Result<Replanned, HandlerError> {
        self.after_image(saved)
    }
}

pub(super) enum Replanned {
    Body(Value),
    WithLocal(Value, Box<entorhinal_core::HubContinuation>),
    Refused(HandlerError),
}

#[derive(Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case")]
enum Outcome {
    Landed {
        position: Option<u64>,
    },
    Superseded {
        revision: u64,
    },
    HeadMoved,
    Refused {
        code: String,
        message: String,
        detail: Option<Value>,
    },
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Landed {
    pub op_id: String,
    pub position: Option<u64>,
}

pub(super) struct KvEntry {
    pub revision: u64,
    // None is a delete or purge marker, not a never-written key.
    pub value: Option<Vec<u8>>,
}

pub(super) trait Transport {
    fn log_entry(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> impl Future<Output = Result<Option<Vec<u8>>, HandlerError>> + Send;
    fn log_head(&self, deadline: Instant)
        -> impl Future<Output = Result<u64, HandlerError>> + Send;
    fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> impl Future<Output = Result<Option<KvEntry>, HandlerError>> + Send;
    fn send(
        &self,
        write: &HubAttemptWrite,
        op_id: &str,
        deadline: Instant,
    ) -> impl Future<Output = Result<Option<u64>, HandlerError>> + Send;
}

impl Transport for Session {
    async fn log_entry(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        self.log_entry(sequence, deadline).await
    }

    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.log_head(deadline).await
    }

    async fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<KvEntry>, HandlerError> {
        Ok(self.kv_entry(key, deadline).await?.map(|entry| KvEntry {
            revision: entry.revision,
            value: (entry.operation == async_nats::jetstream::kv::Operation::Put)
                .then(|| entry.value.to_vec()),
        }))
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        op_id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        let bytes = &write.value;
        if write.key == LOG_KEY {
            self.append(bytes.clone(), write.expected, op_id, deadline)
                .await
                .map(Some)
        } else if bytes.is_empty() {
            self.delete(&write.key, write.expected, op_id, deadline)
                .await
                .map(|()| None)
        } else {
            self.update_record(
                &write.key,
                bytes.clone(),
                write.expected,
                op_id,
                deadline,
                write.step == crate::role_leases::RENEW,
            )
            .await
            .map(Some)
        }
    }
}

impl Transport for Hub {
    async fn log_entry(
        &self,
        sequence: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        self.session(deadline)
            .await?
            .log_entry(sequence, deadline)
            .await
    }

    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.session(deadline).await?.log_head(deadline).await
    }

    async fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<KvEntry>, HandlerError> {
        Transport::kv_entry(&self.session(deadline).await?, key, deadline).await
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        op_id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        if !write.value.is_empty()
            && UnverifiedRecord::parse(&write.key, &write.value)?
                .header()
                .writer
                != self.box_id(deadline).await?.to_string()
        {
            return Err(HandlerError::new(
                "record_invalid",
                "fleet record writer does not match the bus box ID",
            ));
        }
        self.session(deadline)
            .await?
            .send(write, op_id, deadline)
            .await
    }
}

/// The hub backend supplies verification keys and checks which epoch may be
/// used for writing. A fence reads the epoch record from the hub, not the cache,
/// and ensures the vault holds the named key.
pub(super) trait Trust: Sync {
    fn writer_epoch(
        &self,
        deadline: Instant,
    ) -> impl Future<Output = Result<u64, HandlerError>> + Send;
    fn verification_key(
        &self,
        record: &UnverifiedRecord,
    ) -> impl Future<Output = Result<String, HandlerError>> + Send;

    fn verify_remote(
        &self,
        record: UnverifiedRecord,
        _log_position: Option<u64>,
        _deadline: Instant,
    ) -> impl Future<Output = Result<VerifiedRecord, HandlerError>> + Send {
        async move {
            let key = self.verification_key(&record).await?;
            record.verify(&key)
        }
    }
}

pub(super) struct Writer<'a, T, U> {
    pub store: &'a RegistryStore,
    pub transport: &'a T,
    pub trust: &'a U,
    pub signer: &'a dyn CredentialResolver,
    pub vault: &'a dyn EpochVault,
    pub replan: Option<&'a dyn LogReplan>,
}

enum Settlement {
    Landed(Landed),
    Superseded(u64),
    HeadMoved,
    Resend,
}

enum Rebase {
    Send(HubAttemptWrite, String),
    Refused(HandlerError),
    RowChanged,
}

pub(super) fn new_op_id() -> Result<String, HandlerError> {
    let mut bytes = [0; 16];
    getrandom::getrandom(&mut bytes)
        .map_err(|error| HandlerError::new("storage_error", error.to_string()))?;
    Ok(bytes.iter().map(|byte| format!("{byte:02x}")).collect())
}

fn process_id() -> Result<&'static str, HandlerError> {
    static PROCESS: OnceLock<String> = OnceLock::new();
    if let Some(process) = PROCESS.get() {
        return Ok(process);
    }
    let _ = PROCESS.set(new_op_id()?);
    Ok(PROCESS.get().expect("process ID initialized"))
}

pub(super) fn landed_send_ms(
    row: &HubAttempt,
    landed: &Landed,
) -> Result<Option<u64>, HandlerError> {
    let process = process_id()?;
    Ok(matching_send_ms(&row.op_ids, &landed.op_id, process))
}

pub(super) fn matching_send_ms(
    sends: &[HubAttemptSend],
    op_id: &str,
    process: &str,
) -> Option<u64> {
    sends
        .iter()
        .find(|send| send.op_id == op_id && send.process == process)
        .map(|send| send.sent_at_ms)
}

fn current_op_id(write: &HubAttemptWrite, row: &HubAttempt) -> Result<String, HandlerError> {
    if write.value.is_empty() {
        // A delete carries no envelope and is never resealed or rebased.
        return row
            .op_ids
            .first()
            .map(|send| send.op_id.clone())
            .ok_or_else(invalid_attempt);
    }
    Ok(UnverifiedRecord::parse(&write.key, &write.value)?
        .header()
        .entry_id
        .clone())
}

fn invalid_attempt() -> HandlerError {
    HandlerError::new("storage_error", "invalid hub write attempt")
}

fn conflict() -> HandlerError {
    HandlerError::new(
        "request_key_conflict",
        "request key already used with different parameters",
    )
}

fn moved() -> HandlerError {
    HandlerError::new("head_moved", "hub log position is occupied")
}

fn reply(outcome: HubAttemptOutcome) -> Result<Landed, HandlerError> {
    match serde_json::from_value(outcome.value).map_err(|_| invalid_attempt())? {
        Outcome::Landed { position } => Ok(Landed {
            op_id: outcome.op_id,
            position,
        }),
        Outcome::Superseded { revision } => {
            let mut error = HandlerError::new(
                "hub_write_superseded",
                "a newer hub revision replaced the expected value",
            );
            error.detail = Some(json!({"op_id": outcome.op_id, "revision": revision}));
            Err(error)
        }
        Outcome::HeadMoved => Err(moved()),
        Outcome::Refused {
            code,
            message,
            detail,
        } => Err(HandlerError {
            code,
            message,
            detail,
        }),
    }
}

fn recorded(op_id: String, outcome: Outcome) -> HubAttemptOutcome {
    HubAttemptOutcome {
        op_id,
        value: serde_json::to_value(outcome).expect("serializable hub outcome"),
        recorded_at_ms: crate::unix_millis(),
    }
}

pub(super) const CONTINUE: &str = "continue:";
const SNAPSHOT: &str = "snapshot:";

impl<T: Transport, U: Trust> Writer<'_, T, U> {
    /// `next` derives the residence create from the saved agent.create body,
    /// not from fresh IDs or timestamps drawn by a retrying caller.
    pub(super) async fn run(
        &self,
        request: &Request<'_>,
        first: &Step,
        next: Option<NextStep<'_>>,
        deadline: Instant,
    ) -> Result<Landed, HandlerError> {
        self.run_inner(request, first, next, None, deadline).await
    }

    pub(super) async fn run_local(
        &self,
        request: &Request<'_>,
        first: &Step,
        continuation: Option<&entorhinal_core::HubContinuation>,
        deadline: Instant,
    ) -> Result<Landed, HandlerError> {
        self.run_inner(request, first, None, continuation, deadline)
            .await
    }

    async fn run_inner(
        &self,
        request: &Request<'_>,
        first: &Step,
        next: Option<NextStep<'_>>,
        continuation: Option<&entorhinal_core::HubContinuation>,
        deadline: Instant,
    ) -> Result<Landed, HandlerError> {
        let mut rebases = 0;
        let mut resent = false;
        let mut sent = None;
        loop {
            let Some(row) = self.store.hub_attempt(request.request_key)? else {
                let (write, op_id) = self
                    .build(
                        request,
                        first,
                        next.is_some(),
                        continuation.map(|local| local.entry_id.as_str()),
                        deadline,
                    )
                    .await?;
                if self.save(&write, &op_id, None, continuation)? {
                    sent = self.send(&write, &op_id, true, deadline).await?;
                }
                continue;
            };
            if row.write.params_digest != request.params_digest {
                return Err(conflict());
            }
            if let Some(outcome) = row.outcome {
                return reply(outcome);
            }
            let write = &row.write;
            let op_id = current_op_id(write, &row)?;
            if Instant::now() >= deadline && sent.is_none() {
                return Err(hub_transport::unsettled(&op_id));
            }
            let settlement = if let Some((submitted, result)) = sent.take() {
                if submitted == *write {
                    result
                } else {
                    self.settle(&row, &op_id, deadline).await?
                }
            } else {
                self.settle(&row, &op_id, deadline).await?
            };
            // An apply may have won while settlement was reading the hub.
            if self.store.hub_attempt(request.request_key)?.as_ref() != Some(&row) {
                continue;
            }
            let terminal = match settlement {
                Settlement::Landed(landed) if write.step.starts_with(CONTINUE) => {
                    let next = next.ok_or_else(invalid_attempt)?;
                    let (_, body) = self.saved(write, deadline).await?;
                    let step = next(&body)?;
                    if step.key == LOG_KEY || !matches!(step.payload, Payload::Record { .. }) {
                        return Err(invalid_attempt());
                    }
                    let (write, op_id) = self.build(request, &step, false, None, deadline).await?;
                    let send = Self::send_record(&op_id)?;
                    if self
                        .store
                        .advance_hub_attempt(&row, &landed.op_id, &write, &send)?
                    {
                        resent = false;
                        sent = self.send(&write, &op_id, true, deadline).await?;
                    }
                    continue;
                }
                Settlement::Landed(landed) if write.step.starts_with("local:") => {
                    return Ok(landed)
                }
                Settlement::Landed(landed) => recorded(
                    landed.op_id,
                    Outcome::Landed {
                        position: landed.position,
                    },
                ),
                Settlement::Superseded(revision) => {
                    recorded(op_id, Outcome::Superseded { revision })
                }
                Settlement::HeadMoved
                    if write.step.starts_with(SNAPSHOT)
                        || self.replan.is_none()
                        || rebases == MAX_LOG_REBASES
                        || UnverifiedRecord::parse(&write.key, &write.value)?
                            .header()
                            .kind
                            == PROJECTION_SNAPSHOT_KIND =>
                {
                    recorded(op_id, Outcome::HeadMoved)
                }
                Settlement::HeadMoved => match self.rebase(request, &row, deadline).await? {
                    Rebase::Send(write, op_id) => {
                        rebases += 1;
                        resent = false;
                        sent = self.send(&write, &op_id, true, deadline).await?;
                        continue;
                    }
                    Rebase::RowChanged => continue,
                    Rebase::Refused(HandlerError {
                        code,
                        message,
                        detail,
                    }) => recorded(
                        op_id,
                        Outcome::Refused {
                            code,
                            message,
                            detail,
                        },
                    ),
                },
                Settlement::Resend => {
                    if resent {
                        return Err(hub_transport::unsettled(&op_id));
                    }
                    let (write, next_id) = self
                        .resend(request, &row, &op_id, deadline)
                        .await
                        .map_err(|e| during_settlement(e, &op_id))?;
                    if self.save(&write, &next_id, Some(&row), None)? {
                        resent = true;
                        sent = self
                            .send(&write, &next_id, false, deadline)
                            .await
                            .map_err(|e| during_settlement(e, &op_id))?;
                    }
                    continue;
                }
            };
            if self.store.record_hub_attempt_outcome(write, &terminal)? {
                return reply(terminal);
            }
        }
    }

    async fn build(
        &self,
        request: &Request<'_>,
        step: &Step,
        intermediate: bool,
        local_id: Option<&str>,
        deadline: Instant,
    ) -> Result<(HubAttemptWrite, String), HandlerError> {
        if step.name.starts_with(CONTINUE)
            || step.name.starts_with(SNAPSHOT)
            || (intermediate && step.key != LOG_KEY)
        {
            return Err(invalid_attempt());
        }
        let (value, op_id) = match &step.payload {
            Payload::Delete if step.key != LOG_KEY => {
                let cached = self.store.hub_kv_cached(&step.key)?;
                let last_body = cached
                    .as_ref()
                    .filter(|row| row.accepted_revision == Some(step.expected))
                    .and_then(|row| row.body.as_ref());
                let allowed = if let Some(mint) = step
                    .key
                    .strip_prefix("wrap.")
                    .and_then(|wrap| wrap.split('.').nth(1))
                {
                    let epoch = self.store.hub_kv_cached("epoch")?.and_then(|row| row.body);
                    epoch
                        .as_ref()
                        .and_then(crate::hub_residence::committed_mint)
                        .is_some_and(|committed| committed != mint)
                } else {
                    crate::hub_residence::delete_allowed_without_epoch(&step.key, last_body)
                };
                if !allowed || step.expected == 0 {
                    return Err(HandlerError::new(
                        "kv_delete_forbidden",
                        "fleet key cannot be deleted at this revision",
                    ));
                }
                (Vec::new(), new_op_id()?)
            }
            Payload::Delete => return Err(invalid_attempt()),
            Payload::Record {
                kind,
                body,
                snapshot_id,
            } => {
                let op_id = match snapshot_id {
                    Some(id) if step.key == LOG_KEY && !intermediate => id.clone(),
                    Some(_) => return Err(invalid_attempt()),
                    None => local_id.map(str::to_owned).map_or_else(new_op_id, Ok)?,
                };
                let header = Header {
                    kind: kind.clone(),
                    key: step.key.clone(),
                    writer: request.writer.into(),
                    writer_ep: request.writer_ep,
                    entry_id: op_id.clone(),
                };
                let epoch = self.fence(&header, deadline).await?;
                (self.prepare(&header, body, epoch, deadline).await?, op_id)
            }
        };
        let step_name = if intermediate {
            format!("{CONTINUE}{}", step.name)
        } else if matches!(
            &step.payload,
            Payload::Record {
                snapshot_id: Some(_),
                ..
            }
        ) {
            format!("{SNAPSHOT}{}", step.name)
        } else {
            step.name.clone()
        };
        Ok((
            HubAttemptWrite {
                request_key: request.request_key.into(),
                params_digest: request.params_digest.into(),
                key: step.key.clone(),
                expected: step.expected,
                value,
                step: step_name,
            },
            op_id,
        ))
    }

    fn send_record(op_id: &str) -> Result<HubAttemptSend, HandlerError> {
        Ok(HubAttemptSend {
            op_id: op_id.into(),
            sent_at_ms: crate::role_leases::monotonic_ms(),
            process: process_id()?.into(),
        })
    }

    fn save(
        &self,
        write: &HubAttemptWrite,
        op_id: &str,
        row: Option<&HubAttempt>,
        continuation: Option<&entorhinal_core::HubContinuation>,
    ) -> Result<bool, HandlerError> {
        Ok(self.store.persist_hub_attempt_with_local(
            write,
            &Self::send_record(op_id)?,
            row,
            continuation,
        )?)
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        op_id: &str,
        first_at_position: bool,
        deadline: Instant,
    ) -> Result<Option<(HubAttemptWrite, Settlement)>, HandlerError> {
        match self.transport.send(write, op_id, deadline).await {
            Ok(position)
                if !write.step.starts_with(SNAPSHOT)
                    && position
                        .map_or(write.value.is_empty(), |pos| matches_position(write, pos)) =>
            {
                Ok(Some((
                    write.clone(),
                    Settlement::Landed(Landed {
                        op_id: op_id.into(),
                        position,
                    }),
                )))
            }
            Err(error)
                if error.code == "head_moved"
                    && first_at_position
                    && !write.step.starts_with(SNAPSHOT) =>
            {
                Ok(Some((write.clone(), Settlement::HeadMoved)))
            }
            // A replacement can lose to the first send, and an independent
            // snapshot resumer can win with the same derived ID. Look up the
            // exact position even if the tail closed the row in the meantime.
            result
                if write.key == LOG_KEY
                    && (result.is_ok()
                        || result.as_ref().is_err_and(|error| {
                            matches!(error.code.as_str(), "head_moved" | "hub_write_unsettled")
                        })) =>
            {
                let Some(row) = self.store.hub_attempt(&write.request_key)? else {
                    return Err(invalid_attempt());
                };
                if row.write != *write {
                    return Ok(None);
                }
                let result = self.settle(&row, op_id, deadline).await?;
                Ok(Some((write.clone(), result)))
            }
            Ok(_) => Ok(None),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "revision_moved" | "hub_write_unsettled"
                ) =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    }

    async fn fence(&self, header: &Header, deadline: Instant) -> Result<Option<u64>, HandlerError> {
        if !header.sealed()? {
            return Ok(None);
        }
        let epoch = timeout_at(deadline, self.trust.writer_epoch(deadline))
            .await
            .map_err(|_| {
                HandlerError::new(
                    "hub_unavailable",
                    "deadline elapsed reading the writer fence",
                )
            })??;
        Ok(Some(epoch))
    }

    async fn prepare(
        &self,
        header: &Header,
        body: &Value,
        epoch: Option<u64>,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let prepared = timeout_at(
            deadline,
            fleet_record::prepare(self.signer, self.vault, header, body),
        )
        .await
        .map_err(|_| {
            HandlerError::new(
                "epoch_key_unavailable",
                "deadline elapsed preparing the fleet record",
            )
        })??;
        if prepared.epoch() != epoch {
            return Err(HandlerError::new(
                "epoch_key_unavailable",
                "vault sealed under an epoch other than the writer fence",
            ));
        }
        let bytes = prepared.into_bytes();
        if header.key != LOG_KEY {
            hub_transport::check_kv_size(&header.key, bytes.len())?;
        }
        Ok(bytes)
    }

    async fn verify(&self, record: UnverifiedRecord) -> Result<VerifiedRecord, HandlerError> {
        let key = self.trust.verification_key(&record).await?;
        record.verify(&key)
    }

    async fn saved(
        &self,
        write: &HubAttemptWrite,
        deadline: Instant,
    ) -> Result<(Header, Value), HandlerError> {
        let record = UnverifiedRecord::parse(&write.key, &write.value)?;
        timeout_at(deadline, async {
            let record = self.verify(record).await?;
            let plaintext = record.open(self.vault).await?;
            let body = serde_json::from_slice(&plaintext).map_err(|_| invalid_attempt())?;
            Ok((record.header().clone(), body))
        })
        .await
        .map_err(|_| {
            HandlerError::new(
                "epoch_key_unavailable",
                "deadline elapsed opening the saved fleet record",
            )
        })?
    }

    async fn resend(
        &self,
        request: &Request<'_>,
        row: &HubAttempt,
        op_id: &str,
        deadline: Instant,
    ) -> Result<(HubAttemptWrite, String), HandlerError> {
        let write = &row.write;
        if write.value.is_empty() {
            return Ok((write.clone(), op_id.into()));
        }
        let saved = UnverifiedRecord::parse(&write.key, &write.value)?;
        let epoch = self.fence(saved.header(), deadline).await?;
        if saved.epoch() == epoch {
            return Ok((write.clone(), op_id.into()));
        }
        if saved.epoch() > epoch {
            return Err(HandlerError::new(
                "epoch_key_unavailable",
                "writer fence is behind the saved record",
            ));
        }
        let op_id = if write.step.starts_with(SNAPSHOT) {
            op_id.into()
        } else {
            new_op_id()?
        };
        self.replace_bytes(request, write.clone(), op_id, epoch, deadline)
            .await
    }

    /// Rebase after a definite first-send compare-and-swap refusal or after
    /// finding a different entry at the unacknowledged send's position.
    async fn rebase(
        &self,
        request: &Request<'_>,
        row: &HubAttempt,
        deadline: Instant,
    ) -> Result<Rebase, HandlerError> {
        let replan = self.replan.ok_or_else(invalid_attempt)?;
        let expected = self.transport.log_head(deadline).await?;
        if expected <= row.write.expected {
            return Err(HandlerError::new(
                "hub_unavailable",
                "hub head is behind the occupied position",
            ));
        }
        replan.wait_for_head(expected, deadline).await?;
        let (mut header, saved_body) = self.saved(&row.write, deadline).await?;
        if header.writer != request.writer {
            return Err(invalid_attempt());
        }
        let op_id = new_op_id()?;
        let (body, continuation) = match replan.after_image_with_id(&saved_body, &op_id)? {
            Replanned::Body(body) => (body, None),
            Replanned::WithLocal(body, continuation) => (body, Some(continuation)),
            Replanned::Refused(error) => return Ok(Rebase::Refused(error)),
        };
        header.writer_ep = request.writer_ep;
        header.entry_id = op_id.clone();
        let saved = UnverifiedRecord::parse(&row.write.key, &row.write.value)?;
        let epoch = self.fence(&header, deadline).await?;
        if saved.epoch() > epoch {
            return Err(HandlerError::new(
                "epoch_key_unavailable",
                "writer fence is behind the saved record",
            ));
        }
        let mut write = row.write.clone();
        write.value = self.prepare(&header, &body, epoch, deadline).await?;
        write.expected = expected;
        Ok(
            if self.save(&write, &op_id, Some(row), continuation.as_deref())? {
                Rebase::Send(write, op_id)
            } else {
                Rebase::RowChanged
            },
        )
    }

    async fn replace_bytes(
        &self,
        request: &Request<'_>,
        mut write: HubAttemptWrite,
        op_id: String,
        epoch: Option<u64>,
        deadline: Instant,
    ) -> Result<(HubAttemptWrite, String), HandlerError> {
        let (mut header, body) = self.saved(&write, deadline).await?;
        if header.writer != request.writer {
            return Err(invalid_attempt());
        }
        header.writer_ep = request.writer_ep;
        header.entry_id = op_id.clone();
        write.value = self.prepare(&header, &body, epoch, deadline).await?;
        Ok((write, op_id))
    }

    async fn settle(
        &self,
        row: &HubAttempt,
        op_id: &str,
        deadline: Instant,
    ) -> Result<Settlement, HandlerError> {
        let write = &row.write;
        let pending = || hub_transport::unsettled(op_id);
        let landed = |op_id, position| Settlement::Landed(Landed { op_id, position });
        if write.key == LOG_KEY {
            let position = write.expected.checked_add(1).ok_or_else(invalid_attempt)?;
            let Some(bytes) = self
                .transport
                .log_entry(position, deadline)
                .await
                .map_err(|e| during_settlement(e, op_id))?
            else {
                return Ok(Settlement::Resend);
            };
            return Ok(
                match self
                    .matching_op(row, &bytes, Some(position), deadline)
                    .await?
                {
                    Some(id) => landed(id, Some(position)),
                    None => Settlement::HeadMoved,
                },
            );
        }
        let current = self
            .transport
            .kv_entry(&write.key, deadline)
            .await
            .map_err(|e| during_settlement(e, op_id))?;
        let delete = write.value.is_empty();
        let Some(entry) = current else {
            return if delete {
                Ok(landed(op_id.into(), None))
            } else if write.expected == 0 {
                Ok(Settlement::Resend)
            } else {
                Err(pending())
            };
        };
        if entry.revision < write.expected {
            return Err(pending());
        }
        if entry.revision == write.expected {
            return Ok(Settlement::Resend);
        }
        if delete && entry.value.is_none() {
            return Ok(landed(op_id.into(), Some(entry.revision)));
        }
        if !delete {
            if let Some(bytes) = entry.value {
                if let Some(id) = self.matching_op(row, &bytes, None, deadline).await? {
                    return Ok(landed(id, Some(entry.revision)));
                }
            }
        }
        Ok(Settlement::Superseded(entry.revision))
    }

    fn matches_header(row: &HubAttempt, header: &Header) -> Result<bool, HandlerError> {
        if row.write.key != header.key
            || row.write.value.is_empty()
            || !row.op_ids.iter().any(|op| op.op_id == header.entry_id)
        {
            return Ok(false);
        }
        let saved = UnverifiedRecord::parse(&row.write.key, &row.write.value)?;
        Ok(saved.header().kind == header.kind
            && (row.write.step.starts_with(SNAPSHOT) || saved.header().writer == header.writer))
    }

    async fn matching_op(
        &self,
        row: &HubAttempt,
        bytes: &[u8],
        log_position: Option<u64>,
        deadline: Instant,
    ) -> Result<Option<String>, HandlerError> {
        let Ok(unverified) = UnverifiedRecord::parse(&row.write.key, bytes) else {
            return Ok(None);
        };
        if !Self::matches_header(row, unverified.header())? {
            return Ok(None);
        }
        let op_id = unverified.header().entry_id.clone();
        timeout_at(deadline, async {
            let verified = self
                .trust
                .verify_remote(unverified, log_position, deadline)
                .await?;
            Ok(self
                .matches_plaintext(row, &verified, deadline)
                .await?
                .then_some(op_id.clone()))
        })
        .await
        .map_err(|_| hub_transport::unsettled(&op_id))?
    }

    async fn matches_plaintext(
        &self,
        row: &HubAttempt,
        record: &VerifiedRecord,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        if !row.write.step.starts_with(SNAPSHOT) {
            return Ok(true);
        }
        let (_, body) = self.saved(&row.write, deadline).await?;
        let expected = jcs::to_string(&body).map_err(|_| invalid_attempt())?;
        let opened = timeout_at(deadline, record.open(self.vault))
            .await
            .map_err(|_| hub_transport::unsettled(&record.header().entry_id))??;
        if record.header().kind == "member.removed" {
            // Two recovery writers can have different trusted key histories
            // for the same removal. The verified winner closes that episode.
            let observed: Value = serde_json::from_slice(&opened).map_err(|_| invalid_attempt())?;
            return Ok(["box_id", "epoch", "removal_id"]
                .into_iter()
                .all(|field| observed[field] == body[field]));
        }
        Ok(opened == expected.as_bytes())
    }

    /// Called after the log tail or KV watch verifies and applies a record.
    /// Never closes an intermediate log step: that step and its next send
    /// must move together, and a retry can still read its exact log position.
    pub(super) async fn record_applied(
        &self,
        record: &VerifiedRecord,
        position: u64,
        deadline: Instant,
    ) -> Result<usize, HandlerError> {
        let header = record.header();
        let mut matches = Vec::new();
        for row in self.store.hub_attempts_by_op(&header.entry_id)? {
            if row.outcome.is_none()
                && !row.write.step.starts_with(CONTINUE)
                && matches_position(&row.write, position)
                && Self::matches_header(&row, header)?
                && self.matches_plaintext(&row, record, deadline).await?
            {
                matches.push(row.write);
            }
        }
        let outcome = recorded(
            header.entry_id.clone(),
            Outcome::Landed {
                position: Some(position),
            },
        );
        Ok(self.store.record_hub_attempt_outcome_by_op(
            &header.key,
            position,
            &outcome,
            |current, position| {
                matches_position(current, position)
                    && matches.iter().any(|matched| {
                        current.request_key == matched.request_key
                            && current.params_digest == matched.params_digest
                            && current.expected == matched.expected
                            && current.step == matched.step
                    })
            },
        )?)
    }
}

fn during_settlement(error: HandlerError, op_id: &str) -> HandlerError {
    if error.code == "hub_unavailable" {
        hub_transport::unsettled(op_id)
    } else {
        error
    }
}

#[cfg(test)]
pub(crate) mod tests;

fn matches_position(write: &HubAttemptWrite, position: u64) -> bool {
    if write.key == LOG_KEY {
        write.expected.checked_add(1) == Some(position)
    } else {
        position > write.expected
    }
}

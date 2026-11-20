//! The signed epoch register, its writer fence, and removal-driven rotation.
#![allow(dead_code)]

mod joining;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use entorhinal_core::hub_store::{HubEpochSource, HubOwnMint};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::{
    sync::{Mutex, Notify},
    time::{timeout_at, Instant},
};

use crate::{
    fleet_record::{
        vault::{EpochControl, EpochStatus, EpochVault, PendingMint},
        UnverifiedRecord, LOG_KEY,
    },
    hub_identity_log::{wait_for_applied, IdentityLog, LogTransport},
    hub_membership::{box_id, Membership, Removed, Sight},
    hub_transport::{Feed, FeedEvent},
    hub_write::{self, Payload, Step, Transport, Trust},
    HandlerError, HealthGauges,
};

const KEY: &str = "epoch";
const RECOVERY: Duration = Duration::from_secs(60);
const BUDGET: Duration = Duration::from_secs(25);

#[derive(Clone, Copy, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum State {
    Pending,
    Committed,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
#[serde(deny_unknown_fields)]
struct DropBox {
    #[serde(rename = "box")]
    box_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    removal_id: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct Epoch {
    e: u64,
    state: State,
    minter: String,
    mint_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_e: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    base_mint_id: Option<String>,
    base_recipients: BTreeSet<String>,
    recipients: BTreeSet<String>,
    removed: Vec<DropBox>,
}

impl Epoch {
    fn validate(&self) -> Result<(), HandlerError> {
        let mut ids = BTreeSet::new();
        let mut removed = BTreeSet::new();
        let base_valid = match (self.base_e, self.base_mint_id.as_deref()) {
            (None, None) => self.e == 1 && self.base_recipients.is_empty(),
            (Some(base), Some(id)) => {
                base > 0
                    && base.checked_add(1) == Some(self.e)
                    && mint_id(id)
                    && !self.base_recipients.is_empty()
            }
            _ => false,
        };
        if !(1..=u32::MAX as u64).contains(&self.e)
            || !base_valid
            || !mint_id(&self.mint_id)
            || !self.recipients.contains(&self.minter)
            || self.recipients.len() > 64
            || self.base_recipients.len() > 64
            || !self
                .recipients
                .iter()
                .chain(&self.base_recipients)
                .all(|id| box_id(id))
            || self.removed.iter().any(|entry| {
                !box_id(&entry.box_id)
                    || !removed.insert(&entry.box_id)
                    || entry
                        .removal_id
                        .as_ref()
                        .is_some_and(|id| id.is_empty() || !ids.insert(id))
            })
        {
            return Err(invalid("invalid epoch value"));
        }
        Ok(())
    }

    fn accepts(&self, next: &Self) -> bool {
        if self.e != next.e {
            return next.e > self.e;
        }
        if self.base_e != next.base_e
            || self.base_mint_id != next.base_mint_id
            || self.base_recipients != next.base_recipients
        {
            return false;
        }
        if self.mint_id != next.mint_id {
            return self.state == State::Pending
                && self
                    .removed
                    .iter()
                    .all(|removed| next.removed.contains(removed));
        }
        if self.minter != next.minter || self.removed != next.removed {
            return false;
        }
        match (self.state, next.state) {
            (State::Pending, State::Committed) => self.recipients.is_subset(&next.recipients),
            (State::Committed, State::Committed) => {
                self.recipients.len() < next.recipients.len()
                    && self.recipients.is_subset(&next.recipients)
            }
            _ => false,
        }
    }

    fn mint(&self) -> PendingMint {
        PendingMint {
            epoch: self.e,
            mint_id: self.mint_id.clone(),
        }
    }

    fn committed_key(&self) -> Option<(u64, &str)> {
        match self.state {
            State::Committed => Some((self.e, &self.mint_id)),
            State::Pending => self.base_e.zip(self.base_mint_id.as_deref()),
        }
    }

    fn contains_removal(&self, removal_id: &str) -> bool {
        self.removed
            .iter()
            .any(|entry| entry.removal_id.as_deref() == Some(removal_id))
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemovalEvent {
    fingerprint: String,
    box_id: Option<String>,
    removal_id: String,
}

impl RemovalEvent {
    fn decode(bytes: &[u8]) -> Result<Self, HandlerError> {
        let event: Self =
            serde_json::from_slice(bytes).map_err(|_| invalid("invalid device_removed body"))?;
        if !box_id(&event.fingerprint)
            || event.box_id.as_deref().is_some_and(|id| !box_id(id))
            || event.removal_id.is_empty()
        {
            return Err(invalid("invalid device_removed identifiers"));
        }
        Ok(event)
    }
}

/// Feed the bus controller's `m_entorhinal` durable for
/// `ck.{acct}.event.callosum.device_removed.v1` into `deliver`. A successful
/// result can be acknowledged; a failure leaves the delivery for retry.
pub(super) struct Controller<T, U> {
    log: Arc<IdentityLog<T, U>>,
    reader: Arc<EpochReader<T>>,
    changing: Mutex<()>,
}

#[async_trait::async_trait]
pub(super) trait RemovalIntake: Send + Sync {
    async fn deliver(&self, bytes: &[u8], deadline: Instant) -> Result<(), HandlerError>;
}

#[async_trait::async_trait]
impl<T: LogTransport, U: Trust + Send + Sync + 'static> RemovalIntake for Controller<T, U> {
    async fn deliver(&self, bytes: &[u8], deadline: Instant) -> Result<(), HandlerError> {
        Controller::deliver(self, bytes, deadline).await
    }
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Controller<T, U> {
    pub(super) fn new(log: Arc<IdentityLog<T, U>>) -> Option<Arc<Self>> {
        Some(Arc::new(Self {
            reader: log.epoch.clone()?,
            log,
            changing: Mutex::new(()),
        }))
    }

    pub(super) async fn deliver(
        &self,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let event = RemovalEvent::decode(bytes)?;
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        // Read the register before the log head. The next mint is allowed only
        // after the previous mint's removals have been appended.
        let observed = self.reader.read(deadline).await?.ok_or_else(unavailable)?;
        let applied = self.caught_up(deadline).await?;
        if self
            .reader
            .membership
            .removal_applied("", 0, Some(&event.removal_id), applied)
        {
            return Ok(());
        }
        if observed.epoch.contains_removal(&event.removal_id) {
            return Ok(());
        }
        let peer_box = if event.box_id.is_none() {
            timeout_at(deadline, self.log.peer_keys().fleet_peers())
                .await
                .ok()
                .and_then(Result::ok)
                .and_then(|peers| {
                    peers
                        .into_iter()
                        .find_map(|(id, fp)| (fp == event.fingerprint).then_some(id))
                })
        } else {
            None
        };
        let box_id = event
            .box_id
            .or(peer_box)
            .or_else(|| {
                self.reader
                    .membership
                    .box_for_fingerprint(&event.fingerprint, applied)
            })
            .ok_or_else(|| {
                HandlerError::new("removal_box_unknown", "device removal has no known box id")
            })?;
        if box_id == self.reader.own_box {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "removed box cannot rotate its own fleet epoch",
            ));
        }
        let removal = DropBox {
            box_id,
            removal_id: Some(event.removal_id),
        };
        if let Some((_, epoch, None)) = self.reader.membership.removed_at(&removal.box_id, applied)
        {
            return self.append_removal(&removal, epoch, deadline).await;
        }
        if observed.epoch.state == State::Pending {
            return Err(HandlerError::new(
                "epoch_rotation_pending",
                "removal will be retried after the pending mint",
            ));
        }
        self.reconcile(Some(&observed), deadline).await?;
        self.append_missing(&observed, deadline).await?;
        if !observed.epoch.recipients.contains(&removal.box_id) {
            return self
                .append_removal(&removal, observed.epoch.e, deadline)
                .await;
        }
        self.reader.ensure(&observed, deadline).await?;
        self.rotate(
            Some(&observed),
            Some((&removal, &event.fingerprint)),
            deadline,
        )
        .await
    }

    /// Mint the first shared key epoch. Only the hub switch may call this (the
    /// operator's `ck projects log enable` on a fresh account, or the cutover
    /// from engram); recovery and removal handling never create epoch 1.
    pub(super) async fn bootstrap(&self, deadline: Instant) -> Result<(), HandlerError> {
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        let observed = self.reader.read(deadline).await?;
        self.reconcile(observed.as_ref(), deadline).await?;
        match observed {
            None => self.rotate(None, None, deadline).await,
            Some(observed) if observed.epoch.state == State::Committed => {
                self.reader.ensure(&observed, deadline).await
            }
            Some(_) => Err(unavailable()),
        }
    }

    async fn caught_up(&self, deadline: Instant) -> Result<u64, HandlerError> {
        let head = self.log.log_head(deadline).await?;
        wait_for_applied(&self.log, head, deadline).await?;
        self.log.applied_sequence(deadline).await
    }

    async fn append_missing(
        &self,
        current: &Observed,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        for removal in &current.epoch.removed {
            self.append_removal(removal, current.epoch.e, deadline)
                .await?;
        }
        Ok(())
    }

    async fn append_removal(
        &self,
        removal: &DropBox,
        epoch: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let identity = match &removal.removal_id {
            Some(id) => json!([id]),
            None => json!([removal.box_id, epoch]),
        };
        let digest = hub_write::params_digest("member.removed", &identity)?;
        let entry_id = &digest[..32];
        loop {
            let applied = self.caught_up(deadline).await?;
            if self.reader.membership.removal_applied(
                &removal.box_id,
                epoch,
                removal.removal_id.as_deref(),
                applied,
            ) {
                return Ok(());
            }
            let (closing_episode, closing_epoch) = if let Some((episode, old_epoch, None)) =
                self.reader.membership.removed_at(&removal.box_id, applied)
            {
                (episode, old_epoch)
            } else {
                (
                    self.reader
                        .membership
                        .open_episode(&removal.box_id, applied)
                        .ok_or_else(|| invalid("removal would close an already closed episode"))?,
                    epoch,
                )
            };
            let body = serde_json::to_value(Removed {
                box_id: removal.box_id.clone(),
                removal_id: removal.removal_id.clone(),
                epoch: closing_epoch,
                key_history: self
                    .reader
                    .membership
                    .history(&removal.box_id, closing_episode),
            })
            .expect("serializable removal");
            let request_key = format!("epoch.removed:{entry_id}:{applied}");
            match self
                .log
                .write_epoch_step(
                    &request_key,
                    &digest,
                    &Step {
                        name: "member.removed".into(),
                        key: LOG_KEY.into(),
                        expected: applied,
                        payload: Payload::Record {
                            kind: "member.removed".into(),
                            body,
                            snapshot_id: Some(entry_id.into()),
                        },
                    },
                    deadline,
                )
                .await
            {
                Ok(landed) => {
                    wait_for_applied(
                        &self.log,
                        landed
                            .position
                            .ok_or_else(|| invalid("missing removal position"))?,
                        deadline,
                    )
                    .await?;
                    return Ok(());
                }
                Err(error) if error.code == "head_moved" && Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            }
        }
    }

    async fn put(
        &self,
        request_key: &str,
        key: &str,
        expected: u64,
        body: Value,
        deadline: Instant,
    ) -> Result<u64, HandlerError> {
        let kind = key.split('.').next().expect("key kind");
        let digest = hub_write::params_digest(key, &body)?;
        self.log
            .write_epoch_step(
                request_key,
                &digest,
                &Step {
                    name: kind.into(),
                    key: key.into(),
                    expected,
                    payload: Payload::Record {
                        kind: kind.into(),
                        body,
                        snapshot_id: None,
                    },
                },
                deadline,
            )
            .await?
            .position
            .ok_or_else(|| invalid("missing epoch write revision"))
    }

    async fn rotate(
        &self,
        previous: Option<&Observed>,
        event: Option<(&DropBox, &str)>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.log.writer_episode(deadline).await?;
        let applied = self.caught_up(deadline).await?;
        let own_box = &self.reader.own_box;
        let state = previous.map(|previous| &previous.epoch);
        let (e, base_e, base_mint_id, base_recipients, mut removed) = match state {
            Some(state) if state.state == State::Pending => (
                state.e,
                state.base_e,
                state.base_mint_id.clone(),
                state.base_recipients.clone(),
                state.removed.clone(),
            ),
            Some(state) => (
                state.e + 1,
                Some(state.e),
                Some(state.mint_id.clone()),
                state.recipients.clone(),
                Vec::new(),
            ),
            None => (1, None, None, BTreeSet::new(), Vec::new()),
        };
        let roster = timeout_at(deadline, self.log.peer_keys().fleet_peers())
            .await
            .map_err(|_| busy())??;
        let mut peers: BTreeMap<_, _> = roster.iter().cloned().collect();
        if peers.len() != roster.len()
            || roster
                .iter()
                .map(|(_, fp)| fp)
                .collect::<BTreeSet<_>>()
                .len()
                != roster.len()
        {
            return Err(invalid("duplicate Callosum fleet identity"));
        }
        let fingerprint = timeout_at(deadline, self.log.peer_keys().own_fingerprint())
            .await
            .map_err(|_| busy())??;
        if !box_id(&fingerprint) {
            return Err(invalid("invalid local Callosum fingerprint"));
        }
        peers.insert(own_box.clone(), fingerprint.clone());
        let counted = peers.clone();
        if let Some((removal, fingerprint)) = event {
            peers.retain(|id, peer| {
                *id == *own_box || (id != &removal.box_id && peer != fingerprint)
            });
            if !removed.iter().any(|item| item.box_id == removal.box_id) {
                removed.push(removal.clone());
            }
        }
        for id in &base_recipients {
            if !peers.contains_key(id) && !removed.iter().any(|removal| &removal.box_id == id) {
                removed.push(DropBox {
                    box_id: id.clone(),
                    removal_id: None,
                });
            }
        }
        // A box's return to the roster does not authorize a wrap while its
        // membership removal is still in effect.
        peers.retain(|id, _| self.reader.membership.removed_at(id, applied).is_none());
        for removal in &removed {
            if !self.reader.membership.removal_applied(
                &removal.box_id,
                e,
                removal.removal_id.as_deref(),
                applied,
            ) {
                peers.remove(&removal.box_id);
            }
        }
        if !peers.contains_key(own_box) {
            return Err(HandlerError::new(
                "member_episode_invalid",
                "removed box cannot supersede a mint",
            ));
        }
        let members: Vec<_> = peers
            .values()
            .map(|fp| format!("fed-computer:{fp}"))
            .collect();
        let excluded: Vec<_> = counted
            .iter()
            .filter(|(id, _)| !peers.contains_key(*id))
            .map(|(_, fp)| format!("fed-computer:{fp}"))
            .collect();
        let vault = self.reader.control();
        let result = timeout_at(deadline, async {
            match vault.mint(&members, &excluded).await {
                Err(error) if !excluded.is_empty() && error.code == "invalid_params" => {
                    vault.mint(&members, &[]).await
                }
                result => result,
            }
        })
        .await
        .map_err(|_| busy())?;
        let minted = match result {
            Ok(minted) => minted,
            Err(error)
                if error.code == "fleet_members_mismatch"
                    && !excluded.is_empty()
                    && state.is_some_and(|state| state.state == State::Pending) =>
            {
                self.append_missing(previous.expect("pending epoch"), deadline)
                    .await?;
                return Err(error);
            }
            Err(error) => return Err(error),
        };
        let mint = minted.mint();
        let pending = Epoch {
            e,
            state: State::Pending,
            minter: own_box.clone(),
            mint_id: mint.mint_id.clone(),
            base_e,
            base_mint_id,
            base_recipients,
            recipients: peers.keys().cloned().collect(),
            removed,
        };
        let by_member: BTreeMap<_, _> = peers
            .iter()
            .filter(|(id, _)| *id != own_box)
            .map(|(id, fp)| (format!("fed-computer:{fp}"), id))
            .collect();
        let produced: BTreeSet<_> = minted.wraps.iter().map(|wrap| &wrap.member_id).collect();
        if mint.epoch != e
            || produced.len() != minted.wraps.len()
            || !by_member.keys().eq(produced)
            || pending.validate().is_err()
        {
            timeout_at(deadline, self.reader.control().discard(&mint))
                .await
                .map_err(|_| busy())??;
            return Err(invalid("vault minted an unexpected epoch or recipients"));
        }
        let expected = previous.map_or(0, |old| old.revision);
        let revision = match self
            .put(
                &format!("epoch.pending:{}", mint.mint_id),
                KEY,
                expected,
                json!(pending),
                deadline,
            )
            .await
        {
            Ok(revision) => revision,
            Err(error) => return self.settle_lost_mint(&mint, error, deadline).await,
        };
        self.reader
            .record_own_mint(
                HubOwnMint {
                    epoch: e,
                    mint_id: mint.mint_id.clone(),
                    committed: false,
                    revision,
                },
                deadline,
            )
            .await?;
        if let Some(old) =
            previous.filter(|old| old.epoch.state == State::Pending && old.epoch.minter == *own_box)
        {
            self.reader.read(deadline).await?;
            self.delete_wraps(&old.epoch.mint(), deadline).await?;
        }
        for wrapped in minted.wraps {
            let id = by_member[&wrapped.member_id];
            let key = wrap_key(e, &mint.mint_id, id);
            self.put(
                &key,
                &key,
                0,
                json!({"v":1,"epoch":e,"mint_id":mint.mint_id,"to":id,"wrap":wrapped.wrap}),
                deadline,
            )
            .await?;
        }
        let committed = Epoch {
            state: State::Committed,
            ..pending
        };
        match self
            .put(
                &format!("epoch.commit:{}", mint.mint_id),
                KEY,
                revision,
                json!(committed),
                deadline,
            )
            .await
        {
            Ok(revision) => {
                let observed = Observed {
                    revision,
                    epoch: committed,
                };
                self.reader.ensure(&observed, deadline).await?;
                self.append_missing(&observed, deadline).await
            }
            Err(error) => self.settle_lost_mint(&mint, error, deadline).await,
        }
    }

    async fn settle_lost_mint(
        &self,
        mint: &PendingMint,
        error: HandlerError,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let current = self.reader.read(deadline).await?;
        if let Some(current) = current {
            if current.epoch.mint_id == mint.mint_id {
                if current.epoch.state == State::Committed {
                    self.reader.ensure(&current, deadline).await?;
                    return self.append_missing(&current, deadline).await;
                }
            } else if current.epoch.e >= mint.epoch {
                self.reconcile(Some(&current), deadline).await?;
                if current.epoch.state == State::Committed {
                    self.reader.ensure(&current, deadline).await?;
                    if let Some(attempt) = self
                        .reader
                        .sql
                        .attempt(&format!("epoch.pending:{}", mint.mint_id), deadline)
                        .await?
                    {
                        let own = UnverifiedRecord::parse(KEY, &attempt.write.value)?;
                        let own: Epoch = serde_json::from_value(
                            own.unsealed()
                                .ok_or_else(|| invalid("missing mint attempt body"))?
                                .clone(),
                        )
                        .map_err(|_| invalid("invalid mint attempt body"))?;
                        if own
                            .removed
                            .iter()
                            .all(|removal| current.epoch.removed.contains(removal))
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
        Err(error)
    }

    async fn discard(&self, mint: &PendingMint, deadline: Instant) -> Result<(), HandlerError> {
        timeout_at(deadline, self.reader.control().discard(mint))
            .await
            .map_err(|_| busy())??;
        Ok(())
    }

    async fn delete_wraps(
        &self,
        mint: &PendingMint,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let Some(attempt) = self
            .reader
            .sql
            .attempt(&format!("epoch.pending:{}", mint.mint_id), deadline)
            .await?
        else {
            return Ok(());
        };
        let signed = UnverifiedRecord::parse(KEY, &attempt.write.value)?;
        let Some(body) = signed.unsealed() else {
            return Err(invalid("missing abandoned mint body"));
        };
        let epoch: Epoch = serde_json::from_value(body.clone())
            .map_err(|_| invalid("invalid abandoned mint body"))?;
        if epoch.minter != self.reader.own_box
            || epoch.e != mint.epoch
            || epoch.mint_id != mint.mint_id
        {
            return Err(invalid("abandoned mint does not belong to this box"));
        }
        for id in epoch
            .recipients
            .iter()
            .filter(|id| *id != &self.reader.own_box)
        {
            let key = wrap_key(epoch.e, &epoch.mint_id, id);
            let Some(entry) = self.reader.transport.kv_entry(&key, deadline).await? else {
                continue;
            };
            if entry.value.is_none() {
                continue;
            }
            let request = format!("delete.{key}");
            let digest = hub_write::params_digest(&request, &json!(entry.revision))?;
            self.log
                .write_epoch_step(
                    &request,
                    &digest,
                    &Step {
                        name: "epoch.wrap_delete".into(),
                        key,
                        expected: entry.revision,
                        payload: Payload::Delete,
                    },
                    deadline,
                )
                .await?;
        }
        Ok(())
    }

    async fn reconcile(
        &self,
        current: Option<&Observed>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let status = self.reader.status(deadline).await?;
        let saved = self
            .reader
            .sql
            .run(deadline, "hub_own_mint", |store| Ok(store.hub_own_mint()?))
            .await?;
        let pending = status.pending.as_ref().cloned().or_else(|| {
            saved
                .as_ref()
                .filter(|saved| !saved.committed)
                .map(|saved| PendingMint {
                    epoch: saved.epoch,
                    mint_id: saved.mint_id.clone(),
                })
        });
        let Some(mint) = pending else {
            return Ok(());
        };
        if let Some(current) = current {
            let epoch = &current.epoch;
            if epoch.mint_id == mint.mint_id && epoch.e == mint.epoch {
                return if epoch.state == State::Committed {
                    self.reader.ensure(current, deadline).await
                } else {
                    Ok(())
                };
            }
            if epoch.base_e == Some(mint.epoch)
                && epoch.base_mint_id.as_deref() == Some(&mint.mint_id)
            {
                // The next mint read this one committed before the local vault
                // finished. Retain that key before importing a newer epoch.
                timeout_at(deadline, self.reader.control().commit(&mint))
                    .await
                    .map_err(|_| busy())??;
                let _ = self.reader.status(deadline).await;
                self.reader
                    .sql
                    .run(deadline, "record_hub_held_epochs", move |store| {
                        store.record_hub_held_epochs(
                            HubEpochSource::Mint(&mint.mint_id),
                            &[mint.epoch],
                        )?;
                        Ok(store.clear_hub_own_mint(mint.epoch, &mint.mint_id)?)
                    })
                    .await?;
                return Ok(());
            }
        }
        let request_key = format!("epoch.pending:{}", mint.mint_id);
        if let Some(attempt) = self.reader.sql.attempt(&request_key, deadline).await? {
            if attempt.outcome.is_none()
                && attempt.write.expected == current.map_or(0, |entry| entry.revision)
            {
                // Settle the unacknowledged pending write before discarding its
                // key. If it lands, the live watch will recover the missing wraps.
                let saved = UnverifiedRecord::parse(KEY, &attempt.write.value)?;
                let body = saved
                    .unsealed()
                    .ok_or_else(|| invalid("missing pending mint body"))?;
                self.put(
                    &request_key,
                    KEY,
                    attempt.write.expected,
                    body.clone(),
                    deadline,
                )
                .await?;
                return Ok(());
            }
        }
        self.discard(&mint, deadline).await?;
        self.delete_wraps(&mint, deadline).await?;
        self.reader
            .sql
            .run(deadline, "clear_hub_own_mint", move |store| {
                Ok(store.clear_hub_own_mint(mint.epoch, &mint.mint_id)?)
            })
            .await?;
        Ok(())
    }

    async fn observe(&self, deadline: Instant) -> Result<Option<Observed>, HandlerError> {
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        let current = self.reader.read(deadline).await?;
        self.reconcile(current.as_ref(), deadline).await?;
        if let Some(current) = current
            .as_ref()
            .filter(|current| current.epoch.state == State::Committed)
        {
            // A new/re-paired box may need the join flow rather than a wrap for
            // this mint. Its absence does not stop observation of the epoch.
            if current.epoch.recipients.contains(&self.reader.own_box)
                && (current.epoch.minter == self.reader.own_box
                    || self.reader.has_epoch(1, deadline).await?)
            {
                if let Err(error) = self.reader.ensure(current, deadline).await {
                    if error.code != "epoch_key_unavailable" {
                        return Err(error);
                    }
                }
            }
            if current.epoch.minter == self.reader.own_box
                && self.log.writer_episode(deadline).await.is_ok()
            {
                self.append_missing(current, deadline).await?;
            }
        }
        Ok(current)
    }

    async fn recover(&self, revision: u64, deadline: Instant) -> Result<(), HandlerError> {
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        let Some(current) = self
            .reader
            .read(deadline)
            .await?
            .filter(|current| current.revision == revision)
        else {
            return Ok(());
        };
        self.log.writer_episode(deadline).await?;
        self.reconcile(Some(&current), deadline).await?;
        match current.epoch.state {
            State::Pending => {
                if current.epoch.minter == self.reader.own_box {
                    self.discard(&current.epoch.mint(), deadline).await?;
                }
                self.rotate(Some(&current), None, deadline).await
            }
            State::Committed => self.append_missing(&current, deadline).await,
        }
    }

    pub(super) async fn watch(&self, feed: Feed<async_nats::jetstream::kv::Entry>) {
        self.watch_for(feed, RECOVERY).await;
    }

    pub(super) async fn watch_for(
        &self,
        mut feed: Feed<async_nats::jetstream::kv::Entry>,
        recovery_delay: Duration,
    ) {
        let mut live = false;
        let mut observed = None;
        let mut retry_on_membership = false;
        let mut awaiting_writer_key = false;
        let mut anchors = self.reader.membership.changes();
        let mut joins = JoinQueue::default();
        loop {
            if !self.reader.sql.available() {
                return;
            }
            let timer = observed.as_ref().and_then(Observation::deadline);
            let join = joins.next();
            let join_deadline = join.as_ref().map(|(_, job)| job.at);
            let changed = tokio::select! {
                biased;
                event = feed.next_event() => match event {
                    FeedEvent::Disconnected => {
                        live = false;
                        observed = None;
                        retry_on_membership = false;
                        awaiting_writer_key = false;
                        joins = JoinQueue::default();
                        false
                    }
                    FeedEvent::Connected => { live = true; observed = None; joins.wake(None); true }
                    FeedEvent::Item(entry) => {
                        if entry.key == KEY {
                            joins.wake_all();
                        } else if let Some(peer) = entry.key.strip_prefix("wrapreq.").filter(|id| box_id(id)) {
                            if peer == self.reader.own_box {
                                joins.wake(None);
                            } else if entry.operation == async_nats::jetstream::kv::Operation::Put {
                                if joins.requests.insert(peer.to_owned(), entry.revision) != Some(entry.revision) {
                                    let key = Some(peer.to_owned());
                                    joins.pending.remove(&key);
                                    joins.wake(key);
                                }
                            } else {
                                joins.requests.remove(peer);
                                joins.pending.remove(&Some(peer.to_owned()));
                            }
                        } else if entry.key.strip_prefix("wrapjoin.").and_then(|key| key.split('.').next()) == Some(&self.reader.own_box) {
                            joins.wake(None);
                        }
                        entry.key == KEY || entry.key.ends_with(&format!(".{}", self.reader.own_box)) && entry.key.starts_with("wrap.")
                    }
                },
                _ = self.reader.membership_changed.notified(), if live => {
                    joins.wake_all();
                    if retry_on_membership {
                        retry_on_membership = false;
                        if let Some(watch) = observed.as_mut() { watch.retry(Instant::now()); }
                    }
                    awaiting_writer_key
                },
                Ok(()) = anchors.changed(), if live => awaiting_writer_key,
                _ = async { tokio::time::sleep_until(timer.expect("armed timer")).await }, if live && timer.is_some() => {
                    let watch = observed.as_mut().expect("armed observation");
                    watch.recovered = true;
                    if let Err(error) = self.recover(watch.revision, Instant::now() + BUDGET).await {
                        tracing::debug!(code = error.code, "fleet epoch recovery deferred");
                        if error.code == "fleet_members_mismatch" { retry_on_membership = true; }
                        else { watch.retry(Instant::now() + Duration::from_secs(5)); }
                    }
                    false
                },
                _ = self.reader.ready.notified(), if live => {
                    joins.wake_all();
                    observed.is_none()
                },
                _ = async { tokio::time::sleep_until(join_deadline.expect("scheduled join")).await },
                    if live && join_deadline.is_some() => {
                    let (peer, job) = join.expect("scheduled join");
                    joins.pending.remove(&peer);
                    if let Err(error) = self.join_work(peer.as_deref(), Instant::now() + BUDGET).await {
                        tracing::debug!(code = error.code, box_id = peer.as_deref().unwrap_or(&self.reader.own_box), "fleet join reconciliation deferred");
                        if !matches!(error.code.as_str(), "record_invalid" | "signature_invalid" | "member_episode_invalid") {
                            joins.pending.insert(peer, JoinJob {
                                at: Instant::now() + job.backoff,
                                backoff: (job.backoff * 2).min(Duration::from_secs(300)),
                            });
                        }
                    }
                    false
                },
            };
            if live && changed {
                awaiting_writer_key = false;
                let deadline = Instant::now() + BUDGET;
                match self.observe(deadline).await {
                    Ok(Some(current)) => {
                        if observed
                            .as_ref()
                            .is_none_or(|watch| watch.revision != current.revision)
                        {
                            let applied = if current.epoch.state != State::Pending
                                && !current.epoch.removed.is_empty()
                            {
                                self.log.applied_sequence(deadline).await.ok()
                            } else {
                                None
                            };
                            let needs_recovery = current.epoch.state == State::Pending
                                || current.epoch.removed.iter().any(|removal| {
                                    applied.is_some_and(|applied| {
                                        !self.reader.membership.removal_applied(
                                            &removal.box_id,
                                            current.epoch.e,
                                            removal.removal_id.as_deref(),
                                            applied,
                                        )
                                    })
                                });
                            observed = Some(Observation::new(
                                current.revision,
                                Instant::now(),
                                needs_recovery.then_some(recovery_delay),
                            ));
                            retry_on_membership = false;
                        }
                    }
                    Ok(None) => observed = None,
                    Err(error) => {
                        awaiting_writer_key = error.code == "writer_unknown";
                        tracing::debug!(code = error.code, "fleet epoch observation deferred")
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
struct JoinJob {
    at: Instant,
    backoff: Duration,
}

#[derive(Default)]
struct JoinQueue {
    requests: BTreeMap<String, u64>,
    // None is this computer's import/request job; each other box retries on its own schedule.
    pending: BTreeMap<Option<String>, JoinJob>,
}

impl JoinQueue {
    fn next(&self) -> Option<(Option<String>, JoinJob)> {
        self.pending
            .iter()
            .min_by_key(|(_, job)| job.at)
            .map(|(key, job)| (key.clone(), *job))
    }

    fn wake(&mut self, peer: Option<String>) {
        self.pending.entry(peer).or_insert_with(|| JoinJob {
            at: Instant::now(),
            backoff: Duration::from_secs(1),
        });
    }

    fn wake_all(&mut self) {
        self.wake(None);
        for peer in self.requests.keys().cloned().collect::<Vec<_>>() {
            self.wake(Some(peer));
        }
    }
}

struct Observation {
    revision: u64,
    due: Option<Instant>,
    recovered: bool,
}

impl Observation {
    fn new(revision: u64, now: Instant, recovery: Option<Duration>) -> Self {
        Self {
            revision,
            due: recovery.map(|delay| now + delay),
            recovered: false,
        }
    }

    fn deadline(&self) -> Option<Instant> {
        if self.recovered {
            None
        } else {
            self.due
        }
    }

    fn retry(&mut self, at: Instant) {
        self.due = Some(at);
        self.recovered = false;
    }
}

fn mint_id(value: &str) -> bool {
    value.len() == 22
        && URL_SAFE_NO_PAD
            .decode(value)
            .is_ok_and(|bytes| bytes.len() == 16)
}

fn invalid(message: &str) -> HandlerError {
    HandlerError::new("record_invalid", message)
}

fn unavailable() -> HandlerError {
    HandlerError::new(
        "epoch_key_unavailable",
        "writer does not hold the committed fleet epoch",
    )
}

fn busy() -> HandlerError {
    HandlerError::new("hub_unavailable", "fleet epoch operation timed out")
}

fn wrap_key(e: u64, mint: &str, box_id: &str) -> String {
    format!("wrap.{e}.{mint}.{box_id}")
}

#[derive(Clone)]
struct Observed {
    revision: u64,
    epoch: Epoch,
}

/// All paths that read the epoch register share the same accepted predecessor.
/// The writer uses a direct read even when the watch has already seen the value.
pub(super) struct EpochReader<T> {
    pub transport: Arc<T>,
    pub membership: Arc<Membership<T>>,
    pub sql: hub_write::HubSql,
    pub vault: Arc<dyn EpochVault>,
    pub own_box: String,
    pub base: i64,
    reading: Mutex<()>,
    reading_status: Mutex<()>,
    importing: Mutex<()>,
    health: Arc<HealthGauges>,
    pub ready: Notify,
    pub membership_changed: Notify,
}

impl<T: Transport> EpochReader<T> {
    pub(super) fn new(
        transport: Arc<T>,
        membership: Arc<Membership<T>>,
        sql: hub_write::HubSql,
        vault: Arc<dyn EpochVault>,
        own_box: String,
        base: i64,
        health: Arc<HealthGauges>,
    ) -> Self {
        Self {
            transport,
            membership,
            sql,
            vault,
            own_box,
            base,
            reading: Mutex::new(()),
            reading_status: Mutex::new(()),
            importing: Mutex::new(()),
            health,
            ready: Notify::new(),
            membership_changed: Notify::new(),
        }
    }

    fn control(&self) -> &dyn EpochControl {
        self.vault
            .control()
            .expect("epoch lifecycle requires a vault controller")
    }

    async fn status(&self, deadline: Instant) -> Result<EpochStatus, HandlerError> {
        let _guard = timeout_at(deadline, self.reading_status.lock())
            .await
            .map_err(|_| busy())?;
        let result = match timeout_at(deadline, self.control().status()).await {
            Ok(result) => result,
            Err(_) => Err(busy()),
        };
        self.health.fleet_epoch.set_highest_held(
            result
                .as_ref()
                .ok()
                .and_then(|status| status.held.iter().copied().max()),
        );
        result
    }

    async fn applied(&self, deadline: Instant) -> Result<u64, HandlerError> {
        crate::hub_identity_log::applied_sequence(&self.sql, self.base, deadline).await
    }

    async fn has_epoch(&self, epoch: u64, deadline: Instant) -> Result<bool, HandlerError> {
        self.sql
            .run(deadline, "hub_has_epoch", move |store| {
                Ok(store.hub_has_epoch(epoch)?)
            })
            .await
    }

    async fn holds_epoch(
        &self,
        epoch: u64,
        mint: &str,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        let mint = mint.to_owned();
        self.sql
            .run(deadline, "hub_holds_epoch", move |store| {
                Ok(store.hub_holds_epoch(epoch, &mint)?)
            })
            .await
    }

    async fn record_own_mint(
        &self,
        mint: HubOwnMint,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        self.sql
            .run(deadline, "record_hub_own_mint", move |store| {
                Ok(store.record_hub_own_mint(&mint)?)
            })
            .await
    }

    async fn read(&self, deadline: Instant) -> Result<Option<Observed>, HandlerError> {
        let _guard = timeout_at(deadline, self.reading.lock())
            .await
            .map_err(|_| busy())?;
        let observed = self.read_record(deadline).await;
        match &observed {
            Ok(entry) => self.health.fleet_epoch.set_current(
                entry
                    .as_ref()
                    .and_then(|entry| entry.epoch.committed_key().map(|(e, _)| e)),
            ),
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "record_invalid"
                        | "signature_invalid"
                        | "member_episode_invalid"
                        | "writer_unknown"
                ) =>
            {
                self.health.fleet_epoch.set_current(None);
            }
            Err(_) => {}
        }
        observed
    }

    async fn read_record(&self, deadline: Instant) -> Result<Option<Observed>, HandlerError> {
        let previous = self.sql.cached(KEY, deadline).await?;
        let Some(entry) = self.transport.kv_entry(KEY, deadline).await? else {
            return if previous.is_none() {
                Ok(None)
            } else {
                Err(invalid("epoch register disappeared"))
            };
        };
        if let Some(old) = &previous {
            if entry.revision < old.revision {
                return Err(invalid("epoch register revision went backwards"));
            }
            if entry.revision == old.revision {
                if old.integrity_failed {
                    return Err(invalid("epoch register failed integrity checks"));
                }
                if old.accepted_revision == Some(entry.revision) {
                    if old.value != entry.value {
                        return Err(invalid("epoch register failed integrity checks"));
                    }
                    let epoch = serde_json::from_value(
                        old.body
                            .clone()
                            .ok_or_else(|| invalid("missing epoch body"))?,
                    )
                    .map_err(|_| invalid("invalid stored epoch"))?;
                    return Ok(Some(Observed {
                        revision: entry.revision,
                        epoch,
                    }));
                }
            }
        }
        let decoded = async {
            let bytes = entry
                .value
                .as_ref()
                .ok_or_else(|| invalid("epoch cannot be deleted"))?;
            let old = previous
                .and_then(|row| row.body)
                .map(serde_json::from_value::<Epoch>)
                .transpose()
                .map_err(|_| invalid("invalid stored epoch"))?;
            let sight = if old.is_some() {
                Sight::ReplacesCached
            } else {
                Sight::FirstOrAfterGap
            };
            let (record, _) = self
                .membership
                .verify_kv(
                    UnverifiedRecord::parse(KEY, bytes)?,
                    self.applied(deadline).await?,
                    sight,
                    deadline,
                )
                .await?;
            let body = record
                .unsealed()
                .ok_or_else(|| invalid("epoch must be unsealed"))?;
            let epoch: Epoch =
                serde_json::from_value(body.clone()).map_err(|_| invalid("invalid epoch body"))?;
            epoch.validate()?;
            let join = old.as_ref().is_some_and(|old| {
                old.e == epoch.e
                    && old.mint_id == epoch.mint_id
                    && old.state == State::Committed
                    && epoch.state == State::Committed
            });
            if (!join && epoch.state == State::Pending && record.header().writer != epoch.minter)
                || old.as_ref().is_some_and(|old| !old.accepts(&epoch))
            {
                return Err(invalid("invalid epoch transition"));
            }
            let bytes = bytes.clone();
            let body = body.clone();
            let revision = entry.revision;
            self.sql
                .run(deadline, "record_hub_kv", move |store| {
                    Ok(store.record_hub_kv(
                        KEY,
                        revision,
                        Some((&bytes, Some(&body))),
                        crate::unix_millis(),
                    )?)
                })
                .await?;
            Ok(Some(Observed {
                revision: entry.revision,
                epoch,
            }))
        }
        .await;
        if let Err(error) = &decoded {
            let revision = entry.revision;
            match error.code.as_str() {
                "writer_unknown" => {
                    self.sql
                        .run(deadline, "hold_hub_kv", move |store| {
                            Ok(store.hold_hub_kv(KEY, revision)?)
                        })
                        .await?
                }
                "record_invalid" | "signature_invalid" | "member_episode_invalid" => {
                    self.sql
                        .run(deadline, "record_hub_kv", move |store| {
                            Ok(store.record_hub_kv(KEY, revision, None, crate::unix_millis())?)
                        })
                        .await?;
                }
                _ => {}
            }
        }
        decoded
    }

    pub(super) async fn fence(&self, deadline: Instant) -> Result<u64, HandlerError> {
        let observed = self.read(deadline).await?.ok_or_else(unavailable)?;
        let (e, mint) = observed.epoch.committed_key().ok_or_else(unavailable)?;
        if !self.holds_epoch(e, mint, deadline).await? {
            // A pending value has no new committed key to import. The named
            // base is usable only with previously recorded provenance.
            if observed.epoch.state != State::Committed {
                return Err(unavailable());
            }
            self.ensure(&observed, deadline).await?;
        }
        self.holds_epoch(e, mint, deadline)
            .await?
            .then_some(e)
            .ok_or_else(unavailable)
    }

    pub(super) async fn require_for_open(
        &self,
        epoch: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        if !self.has_epoch(epoch, deadline).await? {
            // A box without its first epoch needs a join wrap, not just the
            // latest mint's key.
            if !self.has_epoch(1, deadline).await? {
                return Err(unavailable());
            }
            self.fence(deadline).await?;
            if !self.has_epoch(epoch, deadline).await? {
                return Err(unavailable());
            }
        }
        Ok(())
    }

    pub(super) async fn admission_wrap_imported(
        &self,
        request_id: &str,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        if self
            .sql
            .attempt(&format!("join.request:{request_id}"), deadline)
            .await?
            .is_none()
        {
            // A wiped box can restore keys with a new request in the same episode.
            return Ok(true);
        }
        let key = format!("wrapjoin.{}.{request_id}", self.own_box);
        Ok(self
            .sql
            .cached(&key, deadline)
            .await?
            .is_some_and(|row| row.body.is_some()))
    }

    async fn ensure(&self, observed: &Observed, deadline: Instant) -> Result<(), HandlerError> {
        let epoch = &observed.epoch;
        if epoch.state != State::Committed {
            return Err(unavailable());
        }
        let _guard = timeout_at(deadline, self.importing.lock())
            .await
            .map_err(|_| busy())?;
        if self.holds_epoch(epoch.e, &epoch.mint_id, deadline).await? {
            return Ok(());
        }
        if epoch.minter == self.own_box {
            timeout_at(deadline, self.control().commit(&epoch.mint()))
                .await
                .map_err(|_| busy())??;
            let _ = self.status(deadline).await;
            self.record_own_mint(
                HubOwnMint {
                    epoch: epoch.e,
                    mint_id: epoch.mint_id.clone(),
                    committed: true,
                    revision: observed.revision,
                },
                deadline,
            )
            .await?;
        } else {
            let key = wrap_key(epoch.e, &epoch.mint_id, &self.own_box);
            let entry = self
                .transport
                .kv_entry(&key, deadline)
                .await?
                .and_then(|entry| entry.value)
                .ok_or_else(unavailable)?;
            let (record, _) = self
                .membership
                .verify_kv(
                    UnverifiedRecord::parse(&key, &entry)?,
                    self.applied(deadline).await?,
                    Sight::FirstOrAfterGap,
                    deadline,
                )
                .await?;
            let body = record
                .unsealed()
                .ok_or_else(|| invalid("wrap must be unsealed"))?;
            if record.header().writer != epoch.minter
                || body["v"] != 1
                || body["epoch"] != epoch.e
                || body["mint_id"] != epoch.mint_id
                || body["to"] != self.own_box
            {
                return Err(invalid("wrap does not match the committed mint"));
            }
            let wrap = body
                .get("wrap")
                .ok_or_else(|| invalid("missing epoch wrap"))?;
            let imported = timeout_at(deadline, self.control().import(wrap))
                .await
                .map_err(|_| busy())??;
            let _ = self.status(deadline).await;
            if imported.epochs.as_slice() != [epoch.e]
                && !(imported.already_held && imported.epochs.is_empty())
            {
                return Err(invalid("vault imported a different fleet epoch"));
            }
        }
        let e = epoch.e;
        let mint_id = epoch.mint_id.clone();
        self.sql
            .run(deadline, "record_hub_held_epochs", move |store| {
                Ok(store.record_hub_held_epochs(HubEpochSource::Mint(&mint_id), &[e])?)
            })
            .await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const A: &str = "0123456789abcdef0123456789abcdef";
    const B: &str = "fedcba9876543210fedcba9876543210";
    const C: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    struct EpochOnly(Vec<u8>);

    impl Transport for EpochOnly {
        async fn log_entry(&self, _: u64, _: Instant) -> Result<Option<Vec<u8>>, HandlerError> {
            Ok(None)
        }
        async fn log_head(&self, _: Instant) -> Result<u64, HandlerError> {
            Ok(0)
        }
        async fn kv_entry(
            &self,
            key: &str,
            _: Instant,
        ) -> Result<Option<hub_write::KvEntry>, HandlerError> {
            Ok((key == KEY).then(|| hub_write::KvEntry {
                revision: 5,
                value: Some(self.0.clone()),
            }))
        }
        async fn send(
            &self,
            _: &entorhinal_core::hub_store::HubAttemptWrite,
            _: &str,
            _: Instant,
        ) -> Result<Option<u64>, HandlerError> {
            unreachable!()
        }
    }

    #[derive(Default)]
    struct LiveRoster(std::sync::Mutex<Vec<crate::hub_membership::PeerAnchor>>);

    #[async_trait::async_trait]
    impl crate::hub_membership::PeerKeysRead for LiveRoster {
        async fn peer_keys_read(
            &self,
        ) -> Result<Vec<crate::hub_membership::PeerAnchor>, HandlerError> {
            Ok(self.0.lock().unwrap().clone())
        }
        async fn own_fingerprint(&self) -> Result<String, HandlerError> {
            Ok("self-fingerprint".into())
        }
    }

    async fn read_epoch_after_anchor_arrives(
        bad_signature: bool,
    ) -> (HandlerError, Result<Option<Observed>, HandlerError>) {
        use crate::hub_write::tests::TestKeys;
        use cortexkit_claustrum_client::ClaustrumCredentialResolver;

        let (path, descriptor) = crate::tests::scratch_descriptor("epoch-anchor-arrives");
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        store.apply_entry("agent.cutover", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled',hub_log_base=0,last_applied_position=0,last_seen_head=0", [])?;
            Ok(())
        }).unwrap();
        let own = Arc::new(TestKeys::new());
        let minter = Arc::new(TestKeys::with_seed(51));
        let minter_signer = ClaustrumCredentialResolver::with_target(minter.clone(), 0);
        let epoch = Epoch {
            e: 1,
            state: State::Committed,
            minter: C.into(),
            mint_id: URL_SAFE_NO_PAD.encode([5; 16]),
            base_e: None,
            base_mint_id: None,
            base_recipients: BTreeSet::new(),
            recipients: [C].map(str::to_owned).into(),
            removed: vec![],
        };
        let mut bytes = crate::fleet_record::prepare(
            &minter_signer,
            own.as_ref(),
            &crate::fleet_record::Header {
                kind: KEY.into(),
                key: KEY.into(),
                writer: C.into(),
                writer_ep: 0,
                entry_id: "00000000000000000000000000000051".into(),
            },
            &json!(epoch),
        )
        .await
        .unwrap()
        .into_bytes();
        let roster = Arc::new(LiveRoster::default());
        if bad_signature {
            let mut envelope: Value = serde_json::from_slice(&bytes).unwrap();
            envelope["sig"] = json!("00".repeat(64));
            bytes = entorhinal_catalog::jcs::to_string(&envelope)
                .unwrap()
                .into_bytes();
            roster
                .0
                .lock()
                .unwrap()
                .push(crate::hub_membership::tests::anchor(C, &minter));
        }
        let transport = Arc::new(EpochOnly(bytes));
        let slot = Arc::new(std::sync::Mutex::new(Some(store)));
        let sql = hub_write::HubSql::new(&slot, Arc::default());
        let membership = Arc::new(Membership::new(
            transport.clone(),
            sql.clone(),
            crate::hub_membership::AnchorSource {
                own_box: A.into(),
                peers: roster.clone(),
                signer: Arc::new(ClaustrumCredentialResolver::with_target(own.clone(), 0)),
            },
        ));
        let deadline = Instant::now() + BUDGET;
        membership.discover(0, deadline).await.unwrap();
        let reader = EpochReader::new(
            transport,
            membership.clone(),
            sql,
            own,
            A.into(),
            0,
            Arc::new(HealthGauges::default()),
        );
        let first = reader
            .read(deadline)
            .await
            .err()
            .expect("initial revision must not verify");
        *roster.0.lock().unwrap() = vec![crate::hub_membership::tests::anchor(C, &minter)];
        if !bad_signature {
            membership.discover(0, deadline).await.unwrap();
        }
        let second = reader.read(deadline).await;
        drop(reader);
        drop(membership);
        drop(slot);
        let _ = std::fs::remove_dir_all(&path);
        (first, second)
    }

    #[tokio::test]
    async fn hub_audit_regression_epoch_rereads_unchanged_revision_after_anchor_arrives() {
        let (first, second) = read_epoch_after_anchor_arrives(false).await;
        assert!(
            second.as_ref().is_ok_and(Option::is_some),
            "epoch register stays refused: {:?}",
            second.err().map(|e| (e.code, e.message))
        );
        assert_eq!(first.code, "writer_unknown");
    }

    #[tokio::test]
    async fn hub_audit_regression_epoch_keeps_bad_signature_refused() {
        let (first, second) = read_epoch_after_anchor_arrives(true).await;
        assert_eq!(first.code, "signature_invalid");
        assert_eq!(second.err().unwrap().code, "record_invalid");
    }

    fn pending() -> Epoch {
        Epoch {
            e: 2,
            state: State::Pending,
            minter: A.into(),
            mint_id: URL_SAFE_NO_PAD.encode([1; 16]),
            base_e: Some(1),
            base_mint_id: Some(URL_SAFE_NO_PAD.encode([2; 16])),
            base_recipients: [A, B].map(str::to_owned).into(),
            recipients: [A].map(str::to_owned).into(),
            removed: vec![DropBox {
                box_id: B.into(),
                removal_id: Some("100:permanent:7".into()),
            }],
        }
    }

    #[test]
    fn hub_epoch_accepts_only_commit_join_and_pending_supersession_at_same_epoch() {
        let pending = pending();
        pending.validate().unwrap();
        let committed = Epoch {
            state: State::Committed,
            ..pending.clone()
        };
        assert!(pending.accepts(&committed));
        assert!(!committed.accepts(&pending));
        assert!(!committed.accepts(&committed));
        let mut joined = committed.clone();
        joined.recipients.insert(C.into());
        assert!(committed.accepts(&joined));
        assert!(!joined.accepts(&committed));
        let mut winner = Epoch {
            mint_id: URL_SAFE_NO_PAD.encode([3; 16]),
            ..pending.clone()
        };
        assert!(pending.accepts(&winner));
        winner.state = State::Committed;
        assert!(pending.accepts(&winner));
        assert!(!committed.accepts(&winner));
        winner.removed.clear();
        assert!(!pending.accepts(&winner));
        let mut forged = joined;
        forged.base_mint_id = Some(URL_SAFE_NO_PAD.encode([4; 16]));
        assert!(!committed.accepts(&forged));
    }

    #[test]
    fn hub_epoch_removal_intake_decodes_callosum_vectors_with_or_without_box_id() {
        // A permanent removal and a supersession, each carrying a box id. Copied
        // from callosum's test-vectors/device-removed-v1/box-id-cases.json (tag
        // device-removed-vectors-v1.1).
        let one = RemovalEvent::decode(br#"{"fingerprint":"72cd6e8422c407fb6d098690f1130b7d","removal_id":"100:permanent:7","box_id":"0123456789abcdef0123456789abcdef"}"#).unwrap();
        let two = RemovalEvent::decode(br#"{"fingerprint":"75877bb41d393b5fb8455ce60ecd8dda","removal_id":"200:9:8","box_id":"0123456789abcdef0123456789abcdef"}"#).unwrap();
        assert_eq!(one.box_id.as_deref(), Some(A));
        assert_ne!(one.removal_id, two.removal_id);
        let old = RemovalEvent::decode(
            br#"{"fingerprint":"72cd6e8422c407fb6d098690f1130b7d","removal_id":"100:permanent:7"}"#,
        )
        .unwrap();
        assert!(old.box_id.is_none());
        assert_eq!(old.removal_id, one.removal_id);
    }
}

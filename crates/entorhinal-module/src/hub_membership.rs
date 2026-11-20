//! Membership discovery verifies entries without applying them or moving the log cursor.
#![allow(dead_code)]

pub(super) mod peers;
#[cfg(test)]
pub(super) mod tests;

use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, RwLock},
    time::Duration,
};

use async_trait::async_trait;
use cortexkit_claustrum_client::{CredentialResolver, PublicKeyAlgorithm};
use entorhinal_core::hub_store::{HubAnchor, MEMBERSHIP_OPS};
use futures_util::{
    stream::{self, BoxStream},
    StreamExt,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::{
    sync::{watch, Mutex},
    time::{timeout_at, Instant},
};

use crate::{
    fleet_record::{self, UnverifiedRecord, VerifiedRecord, LOG_KEY},
    hub_write::Transport,
    HandlerError,
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Key {
    pub version: String,
    #[serde(rename = "pub")]
    pub public: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct MemberKey {
    pub box_id: String,
    pub fingerprint: String,
    pub version: String,
    #[serde(rename = "pub")]
    pub public: String,
    pub history: Vec<Key>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Removed {
    pub box_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub removal_id: Option<String>,
    pub epoch: u64,
    pub key_history: Vec<Key>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admitted {
    box_id: String,
    after_removal_id: String,
    request_id: String,
}

pub(super) fn box_id(value: &str) -> bool {
    fleet_record::is_hex(value, 32)
}

fn invalid(message: &str) -> HandlerError {
    HandlerError::new("record_invalid", message)
}

fn episode_invalid() -> HandlerError {
    HandlerError::new(
        "member_episode_invalid",
        "record writer was not authorized in that episode",
    )
}

pub(super) fn unknown_key() -> HandlerError {
    HandlerError::new(
        "signature_invalid",
        "no trusted fleet record key for the writer episode",
    )
}

pub(super) fn writer_unknown(writer: &str) -> HandlerError {
    HandlerError {
        code: "writer_unknown".into(),
        message: format!("no trusted fleet record key or current roster entry for writer {writer}"),
        detail: Some(serde_json::json!({"writer":writer})),
    }
}

#[derive(Clone, Debug)]
enum Event {
    Key(MemberKey),
    Removed(Removed),
    Admitted(Admitted),
}

fn valid_key(key: &Key) -> bool {
    entorhinal_catalog::decode_hex::<32>(&key.public)
        .is_some_and(|public| peers::key_id(&public) == key.version)
}

fn valid_history(keys: &[Key]) -> bool {
    let mut versions = BTreeSet::new();
    keys.iter()
        .all(|key| valid_key(key) && versions.insert(&key.version))
}

impl Event {
    fn decode(record: &UnverifiedRecord) -> Result<Self, HandlerError> {
        let body = record
            .unsealed()
            .ok_or_else(|| invalid("membership must be unsealed"))?;
        let bad_body = || invalid("invalid hub membership body");
        fn decode<T: serde::de::DeserializeOwned>(body: &Value) -> Option<T> {
            T::deserialize(body).ok()
        }
        match record.header().kind.as_str() {
            "member.key" => {
                let key: MemberKey = decode(body).ok_or_else(bad_body)?;
                if key.box_id != record.header().writer
                    || key.version != record.key_version()
                    || key.fingerprint.is_empty()
                    || !valid_history(&key.history)
                    || !key
                        .history
                        .iter()
                        .any(|item| item.version == key.version && item.public == key.public)
                {
                    return Err(bad_body());
                }
                Ok(Self::Key(key))
            }
            "member.removed" => {
                let removed: Removed = decode(body).ok_or_else(bad_body)?;
                if !box_id(&removed.box_id)
                    || removed.epoch == 0
                    || removed.removal_id.as_deref() == Some("")
                    || !valid_history(&removed.key_history)
                {
                    return Err(bad_body());
                }
                Ok(Self::Removed(removed))
            }
            "member.admitted" => {
                let admitted: Admitted = decode(body).ok_or_else(bad_body)?;
                if !box_id(&admitted.box_id)
                    || admitted.after_removal_id.is_empty()
                    || admitted.request_id.is_empty()
                {
                    return Err(bad_body());
                }
                Ok(Self::Admitted(admitted))
            }
            _ => Err(bad_body()),
        }
    }
}

struct Candidate {
    record: UnverifiedRecord,
    event: Event,
}

impl Candidate {
    fn parse(bytes: &[u8]) -> Option<Self> {
        if !UnverifiedRecord::membership_kind(bytes) {
            return None;
        }
        let record = UnverifiedRecord::parse(LOG_KEY, bytes).ok()?;
        let event = Event::decode(&record).ok()?;
        Some(Self { record, event })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct BoxState {
    episode: u64,
    closed: Option<Closure>,
    admission_request: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Closure {
    epoch: u64,
    removal_id: Option<String>,
}

#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Timeline {
    boxes: BTreeMap<String, BTreeMap<u64, BoxState>>,
    removals: BTreeMap<String, u64>,
    anonymous: BTreeMap<(String, u64), u64>,
    admissions: BTreeMap<(String, String), String>,
}

impl Timeline {
    fn duplicate_before(&self, removal: &Removed, sequence: u64) -> bool {
        let previous = if let Some(id) = &removal.removal_id {
            self.removals.get(id)
        } else {
            self.anonymous.get(&(removal.box_id.clone(), removal.epoch))
        };
        previous.is_some_and(|first| *first < sequence)
    }
    // The writer is checked against the state preceding its own entry.
    fn before(&self, box_id: &str, sequence: u64) -> BoxState {
        self.boxes
            .get(box_id)
            .and_then(|events| events.range(..sequence).next_back())
            .map(|(_, state)| state.clone())
            .unwrap_or_default()
    }

    fn transition(&mut self, sequence: u64, event: &Event) -> Result<Option<u64>, HandlerError> {
        match event {
            Event::Key(_) => Ok(None),
            Event::Removed(removal) => {
                let mut state = self.before(&removal.box_id, sequence);
                if self.duplicate_before(removal, sequence) {
                    return Ok(None);
                }
                let linked = if let Some(closed) = &state.closed {
                    if closed.epoch != removal.epoch
                        || closed.removal_id.is_some()
                        || removal.removal_id.is_none()
                    {
                        return Err(invalid("removal does not close or link an episode"));
                    }
                    true
                } else {
                    false
                };
                state.closed = Some(Closure {
                    epoch: removal.epoch,
                    removal_id: removal.removal_id.clone(),
                });
                if let Some(id) = &removal.removal_id {
                    self.removals.insert(id.clone(), sequence);
                }
                self.anonymous
                    .entry((removal.box_id.clone(), removal.epoch))
                    .or_insert(sequence);
                let closes = (!linked).then_some(state.episode);
                self.boxes
                    .entry(removal.box_id.clone())
                    .or_default()
                    .insert(sequence, state);
                Ok(closes)
            }
            Event::Admitted(admission) => {
                let id = (admission.box_id.clone(), admission.after_removal_id.clone());
                if self.admissions.contains_key(&id) {
                    return Ok(None);
                }
                let state = self.before(&admission.box_id, sequence);
                if state.closed.and_then(|closed| closed.removal_id).as_ref()
                    != Some(&admission.after_removal_id)
                {
                    return Err(invalid("admission does not follow the named removal"));
                }
                self.admissions.insert(id, admission.request_id.clone());
                self.boxes
                    .entry(admission.box_id.clone())
                    .or_default()
                    .insert(
                        sequence,
                        BoxState {
                            episode: state.episode + 1,
                            closed: None,
                            admission_request: Some(admission.request_id.clone()),
                        },
                    );
                Ok(None)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct PeerAnchor {
    pub box_id: String,
    pub fingerprint: String,
    pub generation: Option<u64>,
    pub fleet_record_ed25519: Key,
}

/// Return current computer peers only, with the key bound to Callosum's machine id.
#[async_trait]
pub(super) trait PeerKeysRead: Send + Sync {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError>;
    async fn own_fingerprint(&self) -> Result<String, HandlerError>;

    // Replace this polling source with Callosum's peer-key watch when available.
    fn peer_key_updates(
        self: Arc<Self>,
    ) -> BoxStream<'static, Result<Vec<PeerAnchor>, HandlerError>>
    where
        Self: 'static,
    {
        let every = Duration::from_secs(5);
        let mut ticks = tokio::time::interval_at(Instant::now() + every, every);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        stream::unfold((self, ticks), |(source, mut ticks)| async move {
            ticks.tick().await;
            let peers = source.peer_keys_read().await;
            Some((peers, (source, ticks)))
        })
        .boxed()
    }

    async fn fleet_peers(&self) -> Result<Vec<(String, String)>, HandlerError> {
        Ok(self
            .peer_keys_read()
            .await?
            .into_iter()
            .map(|peer| (peer.box_id, peer.fingerprint))
            .collect())
    }
}

pub(super) struct AnchorSource {
    pub peers: Arc<dyn PeerKeysRead>,
    pub own_box: String,
    pub signer: Arc<dyn CredentialResolver>,
}

impl AnchorSource {
    async fn own_anchor(&self) -> Result<PeerAnchor, HandlerError> {
        let (fingerprint, public) = tokio::try_join!(self.peers.own_fingerprint(), async {
            self.signer
                .public_key(fleet_record::CREDENTIAL_ID)
                .await
                .map_err(|error| {
                    HandlerError::new(error.code(), "fleet record public key unavailable")
                })
        })?;
        if public.algorithm != PublicKeyAlgorithm::Ed25519 {
            return Err(invalid("fleet anchor is not Ed25519"));
        }
        Ok(PeerAnchor {
            box_id: self.own_box.clone(),
            fingerprint,
            generation: None,
            fleet_record_ed25519: Key {
                version: public.key_id,
                public: public.public_key_hex,
            },
        })
    }

    fn combine(
        &self,
        mut peers: Vec<PeerAnchor>,
        own: PeerAnchor,
    ) -> Result<Vec<PeerAnchor>, HandlerError> {
        peers.retain(|peer| peer.box_id != self.own_box);
        peers.push(own);
        let mut boxes = BTreeSet::new();
        if peers.iter().any(|peer| {
            !box_id(&peer.box_id)
                || peer.fingerprint.is_empty()
                || !valid_key(&peer.fleet_record_ed25519)
                || !boxes.insert(&peer.box_id)
        }) {
            return Err(invalid("invalid current fleet anchor reply"));
        }
        peers.sort_by(|left, right| left.box_id.cmp(&right.box_id));
        Ok(peers)
    }

    async fn read(&self, deadline: Instant) -> Result<Vec<PeerAnchor>, HandlerError> {
        timeout_at(deadline, async {
            let (peers, own) = tokio::try_join!(self.peers.peer_keys_read(), self.own_anchor())?;
            self.combine(peers, own)
        })
        .await
        .map_err(|_| HandlerError::new("hub_unavailable", "fleet anchor read timed out"))?
    }
}

type Episode = (String, u64);
type Keys = BTreeMap<Episode, BTreeMap<String, String>>;

fn insert(keys: &mut Keys, episode: &Episode, key: &Key) -> Result<(), HandlerError> {
    let versions = keys.entry(episode.clone()).or_default();
    if versions
        .get(&key.version)
        .is_some_and(|public| public != &key.public)
    {
        return Err(invalid("conflicting fleet key history for one episode"));
    }
    versions.insert(key.version.clone(), key.public.clone());
    Ok(())
}

fn lookup<'a>(keys: &'a Keys, episode: &Episode, version: &str) -> Option<&'a str> {
    keys.get(episode)?.get(version).map(String::as_str)
}

#[derive(Debug, Default, PartialEq, Eq)]
struct Proof {
    timeline: Timeline,
    anchors: Keys,
    history: Keys,
    published: BTreeMap<Episode, u64>,
    last_publication: BTreeMap<Episode, (u64, MemberKey)>,
    fingerprints: BTreeMap<String, BTreeMap<u64, String>>,
    verified: BTreeMap<u64, [u8; 32]>,
}

impl Proof {
    fn check_episode(
        &self,
        record: &UnverifiedRecord,
        sequence: u64,
    ) -> Result<Episode, HandlerError> {
        let header = record.header();
        let state = self.timeline.before(&header.writer, sequence);
        if state.closed.is_some() || state.episode != header.writer_ep {
            return Err(episode_invalid());
        }
        Ok((header.writer.clone(), state.episode))
    }

    fn key(&self, record: &UnverifiedRecord, episode: &Episode, sequence: u64) -> Option<&str> {
        let version = record.key_version();
        if episode.1 == 0 || record.header().kind == "member.key" {
            lookup(&self.anchors, episode, version)
                .or_else(|| lookup(&self.history, episode, version))
        } else if self
            .published
            .get(episode)
            .is_some_and(|first| *first < sequence)
        {
            lookup(&self.history, episode, version)
        } else {
            None
        }
    }

    fn log_key(&self, record: &UnverifiedRecord, sequence: u64) -> Result<String, HandlerError> {
        let episode = self.check_episode(record, sequence)?;
        if MEMBERSHIP_OPS.contains(&record.header().kind.as_str()) {
            Event::decode(record)?;
        }
        let key = self.key(record, &episode, sequence).ok_or_else(|| {
            if lookup(&self.anchors, &episode, record.key_version()).is_some()
                || lookup(&self.history, &episode, record.key_version()).is_some()
            {
                // This key is known but cannot authorize an ordinary entry
                // before its publication in the new episode.
                unknown_key()
            } else {
                writer_unknown(&record.header().writer)
            }
        })?;
        if MEMBERSHIP_OPS.contains(&record.header().kind.as_str())
            && self.verified.get(&sequence) != Some(&record.signing_id())
        {
            return Err(unknown_key());
        }
        Ok(key.to_owned())
    }
}

fn scoped_anchors(timeline: &Timeline, head: u64, current: &[PeerAnchor]) -> Vec<HubAnchor> {
    current
        .iter()
        .filter_map(|peer| {
            let state = timeline.before(&peer.box_id, head.saturating_add(1));
            state.closed.is_none().then(|| HubAnchor {
                box_id: peer.box_id.clone(),
                episode: state.episode,
                version: peer.fleet_record_ed25519.version.clone(),
                public_key: entorhinal_catalog::decode_hex::<32>(&peer.fleet_record_ed25519.public)
                    .expect("validated anchor")
                    .to_vec(),
            })
        })
        .collect()
}

fn derive(
    timeline: &Timeline,
    candidates: &BTreeMap<u64, Candidate>,
    anchors: impl IntoIterator<Item = HubAnchor>,
) -> Result<Proof, HandlerError> {
    let mut proof = Proof {
        timeline: timeline.clone(),
        ..Proof::default()
    };
    for anchor in anchors {
        insert(
            &mut proof.anchors,
            &(anchor.box_id, anchor.episode),
            &Key {
                version: anchor.version,
                public: entorhinal_catalog::hex(&anchor.public_key),
            },
        )?;
    }
    // Only the latest directly anchored publication can supply a box's own history.
    let mut publications = BTreeMap::new();
    for (&sequence, candidate) in candidates {
        if let Event::Key(key) = &candidate.event {
            let Ok(episode) = proof.check_episode(&candidate.record, sequence) else {
                continue;
            };
            if lookup(&proof.anchors, &episode, &key.version) == Some(&key.public)
                && candidate.record.signed_by(&key.public)
            {
                publications.insert(episode, key);
            }
        }
    }
    for (episode, key) in publications {
        for key in &key.history {
            if lookup(&proof.anchors, &episode, &key.version)
                .is_some_and(|public| public != key.public)
            {
                return Err(invalid("fleet key history conflicts with an anchor"));
            }
            insert(&mut proof.history, &episode, key)?;
        }
    }

    // Removal copies can lead to other removal copies. Start with roots each
    // time; a cycle of copies without an independent signature proves nothing.
    while proof.verified.len() < candidates.len() {
        let before = proof.verified.len();
        for (&sequence, candidate) in candidates {
            if proof.verified.contains_key(&sequence) {
                continue;
            }
            let Ok(episode) = proof.check_episode(&candidate.record, sequence) else {
                continue;
            };
            if !proof
                .key(&candidate.record, &episode, sequence)
                .is_some_and(|key| candidate.record.signed_by(key))
            {
                continue;
            }
            match &candidate.event {
                Event::Key(key) => {
                    if proof.key(&candidate.record, &episode, sequence) != Some(&key.public) {
                        continue;
                    }
                    proof
                        .published
                        .entry(episode)
                        .and_modify(|first| *first = (*first).min(sequence))
                        .or_insert(sequence);
                    proof
                        .fingerprints
                        .entry(key.fingerprint.clone())
                        .or_default()
                        .insert(sequence, key.box_id.clone());
                    let latest = proof
                        .last_publication
                        .entry((key.box_id.clone(), candidate.record.header().writer_ep))
                        .or_insert_with(|| (sequence, key.clone()));
                    if latest.0 < sequence {
                        *latest = (sequence, key.clone());
                    }
                }
                Event::Removed(removed) => {
                    let target = timeline.before(&removed.box_id, sequence);
                    if target.closed.is_none() && !timeline.duplicate_before(removed, sequence) {
                        for key in &removed.key_history {
                            insert(
                                &mut proof.history,
                                &(removed.box_id.clone(), target.episode),
                                key,
                            )?;
                        }
                    }
                }
                Event::Admitted(_) => {}
            }
            proof
                .verified
                .insert(sequence, candidate.record.signing_id());
        }
        if before == proof.verified.len() {
            break;
        }
    }

    let mut next = Timeline::default();
    proof.verified.retain(|sequence, _| {
        next.transition(*sequence, &candidates[sequence].event)
            .is_ok()
    });
    proof.timeline = next;
    Ok(proof)
}

fn resolve(
    candidates: &BTreeMap<u64, Candidate>,
    persisted: &[HubAnchor],
    current: &[PeerAnchor],
    head: u64,
) -> Result<(Proof, Vec<HubAnchor>), HandlerError> {
    let mut timeline = Timeline::default();
    for _ in 0..=candidates.len() {
        let scoped = scoped_anchors(&timeline, head, current);
        let proof = derive(
            &timeline,
            candidates,
            persisted.iter().chain(&scoped).cloned(),
        )?;
        if proof.timeline == timeline {
            return Ok((proof, scoped));
        }
        timeline = proof.timeline;
    }
    Err(HandlerError::new(
        "membership_discovery_unresolved",
        "membership discovery did not converge",
    ))
}

#[derive(Default)]
struct Scan {
    loaded: bool,
    through: u64,
    candidates: BTreeMap<u64, Candidate>,
}

#[derive(Default)]
struct View {
    proof: Proof,
    current: Vec<PeerAnchor>,
    through: Option<u64>,
}

pub(super) struct Membership<T> {
    transport: Arc<T>,
    sql: crate::hub_write::HubSql,
    source: AnchorSource,
    scan: Mutex<Scan>,
    view: RwLock<View>,
    changed: watch::Sender<u64>,
}

impl<T: Transport> Membership<T> {
    pub(super) fn new(
        transport: Arc<T>,
        sql: crate::hub_write::HubSql,
        source: AnchorSource,
    ) -> Self {
        Self {
            transport,
            sql,
            source,
            scan: Mutex::default(),
            view: RwLock::default(),
            changed: watch::channel(0).0,
        }
    }

    pub(super) async fn discover(&self, head: u64, deadline: Instant) -> Result<(), HandlerError> {
        let current = self.source.read(deadline).await?;
        self.scan_with(head, Some(current), deadline).await
    }

    pub(super) fn changes(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }

    pub(super) async fn discover_cached(
        &self,
        head: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let initialized = self.view.read().unwrap().through.is_some();
        if initialized {
            self.scan_with(head, None, deadline).await
        } else {
            self.discover(head, deadline).await
        }
    }

    pub(super) async fn refresh_peers(
        &self,
        peers: Vec<PeerAnchor>,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        let Some(mut own) = self.current_anchor(&self.source.own_box) else {
            return Ok(false);
        };
        own.fingerprint = timeout_at(deadline, self.source.peers.own_fingerprint())
            .await
            .map_err(|_| {
                HandlerError::new("hub_unavailable", "local fingerprint read timed out")
            })??;
        self.refresh_with(self.source.combine(peers, own)?, deadline)
            .await
    }

    pub(super) async fn refresh_own(&self, deadline: Instant) -> Result<bool, HandlerError> {
        let peers = self.view.read().unwrap().current.clone();
        if peers.is_empty() {
            return Ok(false);
        }
        let own = timeout_at(deadline, self.source.own_anchor())
            .await
            .map_err(|_| HandlerError::new("hub_unavailable", "local anchor read timed out"))??;
        self.refresh_with(self.source.combine(peers, own)?, deadline)
            .await
    }

    async fn refresh_with(
        &self,
        current: Vec<PeerAnchor>,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        if self.view.read().unwrap().current != current {
            let head = self.transport.log_head(deadline).await?;
            self.scan_with(head, Some(current), deadline).await?;
            return Ok(true);
        }
        Ok(false)
    }

    async fn scan_with(
        &self,
        head: u64,
        current: Option<Vec<PeerAnchor>>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let mut scan = timeout_at(deadline, self.scan.lock())
            .await
            .map_err(|_| HandlerError::new("hub_unavailable", "membership discovery busy"))?;
        if !scan.loaded {
            let saved = self
                .sql
                .run(deadline, "hub_membership_scan", |store| {
                    Ok(store.hub_membership_scan()?)
                })
                .await?;
            scan.through = saved.through;
            for (sequence, bytes) in saved.candidates {
                if let Some(candidate) = Candidate::parse(&bytes) {
                    scan.candidates.insert(sequence, candidate);
                }
            }
            scan.loaded = true;
        }
        while scan.through < head {
            let start = scan.through;
            let end = head.min(start.saturating_add(128));
            let mut through = start;
            let mut parsed = Vec::new();
            let mut raw = Vec::new();
            let result = async {
                for sequence in start + 1..=end {
                    let bytes = self
                        .transport
                        .log_entry(sequence, deadline)
                        .await?
                        .ok_or_else(|| {
                            HandlerError::new(
                                "hub_log_gap",
                                format!("membership discovery missing sequence {sequence}"),
                            )
                        })?;
                    if let Some(candidate) = Candidate::parse(&bytes) {
                        raw.push((sequence, bytes));
                        parsed.push((sequence, candidate));
                    }
                    through = sequence;
                }
                Ok::<_, HandlerError>(())
            }
            .await;
            if through > start {
                self.sql
                    .run(deadline, "checkpoint_hub_membership_scan", move |store| {
                        Ok(store.checkpoint_hub_membership_scan(through, &raw)?)
                    })
                    .await?;
                scan.through = through;
                scan.candidates.extend(parsed);
            }
            result?;
        }
        let through = scan.through;
        let current = {
            let view = self.view.read().unwrap();
            let current = current.unwrap_or_else(|| view.current.clone());
            if view.through == Some(through) && view.current == current {
                return Ok(());
            }
            current
        };
        let anchors = self
            .sql
            .run(deadline, "hub_anchors", |store| Ok(store.hub_anchors()?))
            .await?;
        let result = resolve(&scan.candidates, &anchors, &current, through);
        match result {
            Ok((proof, scoped)) => {
                self.sql
                    .run(deadline, "persist_hub_anchors", move |store| {
                        Ok(store.persist_hub_anchors(&scoped)?)
                    })
                    .await?;
                let mut view = self.view.write().unwrap();
                let changed = view.current != current || view.proof != proof;
                *view = View {
                    proof,
                    current,
                    through: Some(through),
                };
                if changed {
                    self.changed.send_modify(|generation| *generation += 1);
                }
                Ok(())
            }
            Err(error) => {
                *self.view.write().unwrap() = View::default();
                Err(error)
            }
        }
    }

    pub(super) fn through(&self) -> Option<u64> {
        self.view.read().unwrap().through
    }

    pub(super) fn current_anchor(&self, box_id: &str) -> Option<PeerAnchor> {
        self.view
            .read()
            .unwrap()
            .current
            .iter()
            .find(|peer| peer.box_id == box_id)
            .cloned()
    }

    pub(super) fn log_key(
        &self,
        record: &UnverifiedRecord,
        sequence: u64,
    ) -> Result<String, HandlerError> {
        self.view.read().unwrap().proof.log_key(record, sequence)
    }

    pub(super) async fn verify_log(
        &self,
        record: UnverifiedRecord,
        sequence: u64,
        deadline: Instant,
    ) -> Result<(String, VerifiedRecord), HandlerError> {
        let mut key = self.log_key(&record, sequence);
        if key.as_ref().is_err_and(|error| {
            matches!(
                error.code.as_str(),
                "writer_unknown" | "signature_invalid" | "member_episode_invalid"
            )
        }) {
            let head = self.transport.log_head(deadline).await?;
            self.discover_cached(head, deadline).await?;
            key = self.log_key(&record, sequence);
        }
        let key = key?;
        let verified = record.verify(&key)?;
        Ok((key, verified))
    }

    pub(super) fn saved_key(&self, record: &UnverifiedRecord) -> Result<String, HandlerError> {
        let header = record.header();
        let view = self.view.read().unwrap();
        let episode = (header.writer.clone(), header.writer_ep);
        lookup(&view.proof.anchors, &episode, record.key_version())
            .or_else(|| lookup(&view.proof.history, &episode, record.key_version()))
            .map(str::to_owned)
            .ok_or_else(|| writer_unknown(&header.writer))
    }

    pub(super) fn open_episode(&self, box_id: &str, applied: u64) -> Option<u64> {
        let view = self.view.read().unwrap();
        view.through?;
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        state.closed.is_none().then_some(state.episode)
    }

    pub(super) fn current_member_episode(&self, box_id: &str, applied: u64) -> Option<u64> {
        let view = self.view.read().unwrap();
        view.through?;
        if !view.current.iter().any(|peer| peer.box_id == box_id) {
            return None;
        }
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        state.closed.is_none().then_some(state.episode)
    }

    pub(super) fn require_known_member(
        &self,
        box_id: &str,
        episode: u64,
        applied: u64,
    ) -> Result<(), HandlerError> {
        let view = self.view.read().unwrap();
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        if !self::box_id(box_id)
            || view.current.iter().any(|peer| peer.box_id == box_id)
            || episode < state.episode
            || (episode == state.episode && state.closed.is_some())
        {
            return Ok(());
        }
        Err(writer_unknown(box_id))
    }

    pub(super) fn episode_closed(&self, box_id: &str, episode: u64, applied: u64) -> bool {
        let view = self.view.read().unwrap();
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        episode < state.episode || (episode == state.episode && state.closed.is_some())
    }

    pub(super) fn can_write(&self, applied: u64) -> Option<u64> {
        let view = self.view.read().unwrap();
        view.through?;
        let box_id = &self.source.own_box;
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        if state.closed.is_some() {
            return None;
        }
        if state.episode != 0
            && !view
                .proof
                .published
                .get(&(box_id.clone(), state.episode))
                .is_some_and(|sequence| *sequence <= applied)
        {
            return None;
        }
        Some(state.episode)
    }

    pub(super) fn removed_at(
        &self,
        box_id: &str,
        applied: u64,
    ) -> Option<(u64, u64, Option<String>)> {
        let view = self.view.read().unwrap();
        let state = view
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1));
        state
            .closed
            .map(|closed| (state.episode, closed.epoch, closed.removal_id))
    }

    pub(super) fn removal_applied(
        &self,
        box_id: &str,
        epoch: u64,
        id: Option<&str>,
        applied: u64,
    ) -> bool {
        let view = self.view.read().unwrap();
        let history = &view.proof.timeline;
        let position = match id {
            Some(id) => history.removals.get(id),
            None => history.anonymous.get(&(box_id.to_owned(), epoch)),
        };
        position.is_some_and(|position| *position <= applied)
    }

    pub(super) fn admission_request(&self, box_id: &str, removal_id: &str) -> Option<String> {
        self.view
            .read()
            .unwrap()
            .proof
            .timeline
            .admissions
            .get(&(box_id.to_owned(), removal_id.to_owned()))
            .cloned()
    }

    pub(super) fn episode_join_request(&self, box_id: &str, applied: u64) -> Option<String> {
        self.view
            .read()
            .unwrap()
            .proof
            .timeline
            .before(box_id, applied.saturating_add(1))
            .admission_request
    }

    pub(super) fn box_for_fingerprint(&self, fingerprint: &str, applied: u64) -> Option<String> {
        let view = self.view.read().unwrap();
        view.proof
            .fingerprints
            .get(fingerprint)?
            .range(..=applied)
            .next_back()
            .map(|(_, id)| id.clone())
    }
}

#[derive(Clone, Copy)]
pub(super) enum Sight {
    FirstOrAfterGap,
    ReplacesCached,
    ReconfirmsCached,
}

impl<T: Transport> Membership<T> {
    pub(super) fn publication(&self, applied: u64) -> Option<(u64, MemberKey)> {
        let view = self.view.read().unwrap();
        if view.through? > applied {
            return None;
        }
        let peer = view
            .current
            .iter()
            .find(|peer| peer.box_id == self.source.own_box)?;
        let state = view
            .proof
            .timeline
            .before(&peer.box_id, applied.saturating_add(1));
        if state.closed.is_some() {
            return None;
        }
        let episode = (peer.box_id.clone(), state.episode);
        let key = &peer.fleet_record_ed25519;
        if view
            .proof
            .last_publication
            .get(&episode)
            .is_some_and(|(_, last)| {
                last.version == key.version
                    && last.public == key.public
                    && last.fingerprint == peer.fingerprint
            })
        {
            return None;
        }
        let mut history = BTreeMap::new();
        for keys in [&view.proof.history, &view.proof.anchors] {
            if let Some(keys) = keys.get(&episode) {
                history.extend(keys.clone());
            }
        }
        Some((
            state.episode,
            MemberKey {
                box_id: peer.box_id.clone(),
                fingerprint: peer.fingerprint.clone(),
                version: key.version.clone(),
                public: key.public.clone(),
                history: history
                    .into_iter()
                    .map(|(version, public)| Key { version, public })
                    .collect(),
            },
        ))
    }

    pub(super) fn history(&self, box_id: &str, episode: u64) -> Vec<Key> {
        let view = self.view.read().unwrap();
        let episode = (box_id.to_owned(), episode);
        let mut history = BTreeMap::new();
        for keys in [&view.proof.history, &view.proof.anchors] {
            if let Some(keys) = keys.get(&episode) {
                history.extend(keys.clone());
            }
        }
        history
            .into_iter()
            .map(|(version, public)| Key { version, public })
            .collect()
    }

    pub(super) fn kv_key(
        &self,
        record: &UnverifiedRecord,
        applied: u64,
        sight: Sight,
    ) -> Result<Option<String>, HandlerError> {
        let view = self.view.read().unwrap();
        if view.through.is_none() {
            return Ok(None);
        }
        let header = record.header();
        if header.kind == "wrapreq" {
            if header.key.strip_prefix("wrapreq.") != Some(&header.writer) {
                return Err(invalid("wrap request writer differs from its box id"));
            }
            return Ok(view
                .current
                .iter()
                .find(|peer| {
                    peer.box_id == header.writer
                        && peer.fleet_record_ed25519.version == record.key_version()
                })
                .map(|peer| peer.fleet_record_ed25519.public.clone()));
        }
        let state = view
            .proof
            .timeline
            .before(&header.writer, applied.saturating_add(1));
        let current = state.closed.is_none() && state.episode == header.writer_ep;
        if header.writer_ep > state.episode {
            return Ok(None);
        }
        if !current && matches!(sight, Sight::ReplacesCached) {
            return Err(episode_invalid());
        }
        let episode = (header.writer.clone(), header.writer_ep);
        Ok(if header.writer_ep == 0 {
            lookup(&view.proof.anchors, &episode, record.key_version())
                .or_else(|| lookup(&view.proof.history, &episode, record.key_version()))
        } else if view
            .proof
            .published
            .get(&episode)
            .is_some_and(|first| *first <= applied)
        {
            lookup(&view.proof.history, &episode, record.key_version())
        } else {
            None
        }
        .map(str::to_owned))
    }

    pub(super) async fn verify_kv(
        &self,
        record: UnverifiedRecord,
        applied: u64,
        sight: Sight,
        deadline: Instant,
    ) -> Result<(VerifiedRecord, Option<u64>), HandlerError> {
        let (key, arrival_head) = match self.kv_key(&record, applied, sight)? {
            Some(key) => (key, None),
            None => {
                // Hold this unverified value (don't apply or refuse it yet)
                // while key discovery scans to the log head seen on its
                // arrival. The roster may still be missing the writer.
                let head = self.transport.log_head(deadline).await?;
                self.discover_cached(head, deadline).await?;
                (
                    self.kv_key(&record, applied.max(head), sight)?
                        .ok_or_else(|| writer_unknown(&record.header().writer))?,
                    Some(head),
                )
            }
        };
        Ok((record.verify(&key)?, arrival_head))
    }
}

pub(super) fn validate_kv_body(record: &VerifiedRecord, body: &Value) -> Result<(), HandlerError> {
    let header = record.header();
    for field in ["writer", "author"] {
        if let Some(value) = body.get(field) {
            if !value.as_str().is_some_and(box_id)
                || (field == "writer" && value.as_str() != Some(&header.writer))
            {
                return Err(invalid("invalid fleet KV writer or author"));
            }
        }
    }
    if matches!(header.kind.as_str(), "residence" | "role")
        && !body["holder"].as_str().is_some_and(|holder| {
            box_id(holder)
                || holder
                    .strip_prefix("engram:")
                    .is_some_and(|author| !author.is_empty())
        })
    {
        return Err(invalid("invalid fleet KV holder"));
    }
    Ok(())
}

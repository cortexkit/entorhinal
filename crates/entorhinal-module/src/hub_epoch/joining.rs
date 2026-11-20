//! Requests and responses for a box that needs retained fleet epoch keys.

use super::*;
use crate::{
    fleet_record::{is_hex, VerifiedRecord},
    hub_membership::PeerAnchor,
    hub_write::KvEntry,
};
use entorhinal_catalog::sha256_hex;

const MAX_EPOCHS: u64 = 256;
// Base64url plus the signed envelope still fit inside the 12 KiB KV limit.
const PART_BYTES: usize = 8 * 1024;
// Allow for base64 in the vault's wrap JSON, even at the 1 MiB plaintext limit.
const MAX_PARTS: usize = 256;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Request {
    v: u8,
    request_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    after_removal_id: Option<String>,
}

impl Request {
    fn decode(key: &str, bytes: &[u8], anchor: &PeerAnchor) -> Result<Self, HandlerError> {
        let record = UnverifiedRecord::parse(key, bytes)?;
        if record.header().writer != anchor.box_id
            || record.key_version() != anchor.fleet_record_ed25519.version
        {
            return Err(invalid("wrap request is not from the current box anchor"));
        }
        let request: Self = body(&record.verify(&anchor.fleet_record_ed25519.public)?)?;
        if request.v != 1
            || !is_hex(&request.request_id, 32)
            || request.after_removal_id.as_deref() == Some("")
        {
            return Err(invalid("invalid wrap request"));
        }
        Ok(request)
    }

    fn write_id(&self) -> String {
        format!("join.request:{}", self.request_id)
    }
}

#[derive(Deserialize, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
struct Manifest {
    v: u8,
    responder: String,
    parts: usize,
    sha256: String,
    through_epoch: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Part {
    v: u8,
    count: usize,
    sha256: String,
    data: String,
}

struct Assembly {
    entry: KvEntry,
    manifest: Manifest,
    wrap: Value,
}

fn request_key(box_id: &str) -> String {
    format!("wrapreq.{box_id}")
}

fn manifest_key(box_id: &str, request: &Request) -> String {
    format!("wrapjoin.{box_id}.{}", request.request_id)
}

fn body<T: serde::de::DeserializeOwned>(record: &VerifiedRecord) -> Result<T, HandlerError> {
    record
        .unsealed()
        .and_then(|body| T::deserialize(body).ok())
        .ok_or_else(|| invalid("invalid unsealed join record body"))
}

fn encode(value: &impl Serialize) -> Value {
    serde_json::to_value(value).expect("serializable join record")
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Controller<T, U> {
    pub(crate) async fn joins(&self, deadline: Instant) -> Result<(), HandlerError> {
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        let Some(current) = self.join_epoch(deadline).await? else {
            return Ok(());
        };
        if self.join_self(&current, deadline).await?
            || self.log.writer_episode(deadline).await.is_err()
        {
            return Ok(());
        }
        for peer in self.join_peers(deadline).await? {
            if let Err(error) = self.serve_join(&peer, deadline).await {
                tracing::debug!(
                    code = error.code,
                    box_id = peer.box_id,
                    "fleet join response deferred"
                );
            }
        }
        Ok(())
    }

    pub(super) async fn join_work(
        &self,
        peer_box: Option<&str>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let _guard = timeout_at(deadline, self.changing.lock())
            .await
            .map_err(|_| busy())?;
        let Some(current) = self.join_epoch(deadline).await? else {
            return Ok(());
        };
        match peer_box {
            None => {
                self.join_self(&current, deadline).await?;
            }
            Some(box_id) if self.log.writer_episode(deadline).await.is_ok() => {
                if let Some(peer) = self
                    .join_peers(deadline)
                    .await?
                    .iter()
                    .find(|peer| peer.box_id == box_id)
                {
                    self.serve_join(peer, deadline).await?;
                }
            }
            Some(_) => {}
        }
        Ok(())
    }

    async fn join_epoch(&self, deadline: Instant) -> Result<Option<Observed>, HandlerError> {
        let head = self.log.log_head(deadline).await?;
        if self
            .reader
            .membership
            .through()
            .is_none_or(|seen| seen < head)
        {
            self.reader.membership.discover(head, deadline).await?;
        }
        // A joining computer is sent only committed shared keys. A key a computer has
        // just created stays pending until its hub record wins, and may still be
        // discarded, so it is never wrapped for a joiner.
        Ok(self
            .reader
            .read(deadline)
            .await?
            .filter(|current| current.epoch.committed_key().is_some()))
    }

    async fn join_peers(&self, deadline: Instant) -> Result<Vec<PeerAnchor>, HandlerError> {
        let peers = timeout_at(deadline, self.log.peer_keys().peer_keys_read())
            .await
            .map_err(|_| busy())??;
        Ok(peers
            .iter()
            .filter(|peer| {
                peer.box_id != self.reader.own_box
                    && box_id(&peer.box_id)
                    && box_id(&peer.fingerprint)
                    && peers
                        .iter()
                        .filter(|other| other.box_id == peer.box_id)
                        .count()
                        == 1
            })
            .cloned()
            .collect())
    }

    async fn join_self(&self, current: &Observed, deadline: Instant) -> Result<bool, HandlerError> {
        let own = &self.reader.own_box;
        let timeline = &self.reader.membership;
        let head = timeline.through().ok_or_else(busy)?;
        let removed = timeline.removed_at(own, head);
        // Wait for the durable removal's id before asking to be readmitted.
        if removed.as_ref().is_some_and(|(_, _, id)| id.is_none()) {
            return Ok(true);
        }
        let after = removed.as_ref().and_then(|(_, _, id)| id.as_deref());
        let key = request_key(own);
        let entry = self.reader.transport.kv_entry(&key, deadline).await?;
        let Some(anchor) = timeline.current_anchor(own) else {
            return Ok(true);
        };
        let mut held = self.join_held(deadline).await?;
        let e = current.epoch.committed_key().expect("committed base").0;
        let request = entry
            .as_ref()
            .and_then(|entry| entry.value.as_deref())
            .and_then(|bytes| Request::decode(&key, bytes, &anchor).ok());
        let request = match request {
            Some(request)
                if self
                    .reader
                    .sql
                    .attempt(&request.write_id(), deadline)
                    .await?
                    .is_some()
                    && (request.after_removal_id.as_deref() == after
                        || (after.is_none()
                            && request.after_removal_id.as_ref().is_some_and(|id| {
                                timeline.admission_request(own, id).as_deref()
                                    == Some(&request.request_id)
                            }))) =>
            {
                request
            }
            _ if after.is_none() && (1..=e).all(|epoch| held.contains(&epoch)) => return Ok(false),
            _ => {
                self.request_join(
                    removed,
                    entry.as_ref().map_or(0, |entry| entry.revision),
                    deadline,
                )
                .await?;
                return Ok(true);
            }
        };
        let revision = entry.expect("verified join request has revision").revision;
        let manifest_key = manifest_key(own, &request);
        let saved = self.reader.sql.cached(&manifest_key, deadline).await?;
        let imported = saved
            .and_then(|row| row.body)
            .and_then(|body| Manifest::deserialize(body).ok())
            .is_some_and(|manifest| {
                (1..=manifest.through_epoch).all(|epoch| held.contains(&epoch))
            });
        if !imported {
            let Some(assembly) = self.assemble(&manifest_key, deadline).await? else {
                return Ok(true);
            };
            let observed = self.reader.read(deadline).await?.ok_or_else(unavailable)?;
            let through = assembly.manifest.through_epoch;
            if observed
                .epoch
                .committed_key()
                .is_none_or(|(e, _)| through > e)
            {
                return Err(invalid("join wrap names an uncommitted epoch"));
            }
            held = self
                .import_join(&manifest_key, &request, &assembly, deadline)
                .await?;
        }

        // A single intervening rotation can be filled from its per-mint wrap.
        let latest = self.reader.read(deadline).await?.ok_or_else(unavailable)?;
        let target = latest.epoch.committed_key().ok_or_else(unavailable)?.0;
        if !held.contains(&target)
            && latest.epoch.state == State::Committed
            && latest.epoch.recipients.contains(own)
        {
            match self.reader.ensure(&latest, deadline).await {
                Ok(()) => held = self.join_held(deadline).await?,
                Err(error) if error.code == "epoch_key_unavailable" => {}
                Err(error) => return Err(error),
            }
        }
        if after.is_some() {
            return Ok(true);
        }
        if !(1..=target).all(|epoch| held.contains(&epoch)) {
            // Once discovery sees admission, an ordinary request can fill any
            // key gap, even one that prevents applying the admission record.
            self.request_join(None, revision, deadline).await?;
            return Ok(true);
        }
        if self.log.writer_episode(deadline).await.is_err() {
            return Ok(true);
        }
        self.clean_join(own, &key, revision, deadline).await?;
        Ok(false)
    }

    async fn join_held(&self, deadline: Instant) -> Result<BTreeSet<u64>, HandlerError> {
        let status = self.reader.status(deadline).await?;
        self.tracked_epochs(status.held, deadline).await
    }

    async fn tracked_epochs(
        &self,
        epochs: impl IntoIterator<Item = u64>,
        deadline: Instant,
    ) -> Result<BTreeSet<u64>, HandlerError> {
        let mut held = BTreeSet::new();
        for e in epochs {
            if self.reader.has_epoch(e, deadline).await? {
                held.insert(e);
            }
        }
        Ok(held)
    }

    async fn request_join(
        &self,
        removed: Option<(u64, u64, Option<String>)>,
        expected: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let (writer_ep, after_removal_id) = removed
            .map(|(episode, _, id)| (episode, id))
            .unwrap_or((self.log.writer_episode(deadline).await.unwrap_or(0), None));
        let request = Request {
            v: 1,
            request_id: hub_write::new_op_id()?,
            after_removal_id,
        };
        self.log
            .write_join_request(
                writer_ep,
                &request.write_id(),
                expected,
                encode(&request),
                deadline,
            )
            .await
    }
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Controller<T, U> {
    async fn serve_join(&self, peer: &PeerAnchor, deadline: Instant) -> Result<(), HandlerError> {
        let box_id = &peer.box_id;
        let key = request_key(box_id);
        let Some(entry) = self.reader.transport.kv_entry(&key, deadline).await? else {
            return Ok(());
        };
        let Some(bytes) = &entry.value else {
            return Ok(());
        };
        let request = Request::decode(&key, bytes, peer)?;
        let manifest = manifest_key(box_id, &request);
        let epoch = loop {
            let Some(mut current) = self
                .eligible_join(box_id, &request, entry.revision, deadline)
                .await?
            else {
                return Ok(());
            };
            if !self.is_current_peer(peer, deadline).await? {
                return Ok(());
            }
            if current.epoch.recipients.contains(box_id) {
                break current;
            }
            current.epoch.recipients.insert(box_id.clone());
            current.epoch.validate()?;
            let result = self
                .put(
                    &format!(
                        "join.recipient:{box_id}:{}:{}",
                        request.request_id, current.revision
                    ),
                    KEY,
                    current.revision,
                    json!(current.epoch),
                    deadline,
                )
                .await;
            match result {
                Ok(_) => continue,
                Err(error) if raced(&error) && Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            }
        };

        if self
            .reader
            .transport
            .kv_entry(&manifest, deadline)
            .await?
            .is_none()
        {
            self.reader.ensure(&epoch, deadline).await?;
            let Some(latest) = self
                .eligible_join(box_id, &request, entry.revision, deadline)
                .await?
            else {
                return Ok(());
            };
            if latest.revision != epoch.revision || !self.is_current_peer(peer, deadline).await? {
                return Ok(());
            }
            let wrap = {
                let _guard = timeout_at(deadline, self.reader.importing.lock())
                    .await
                    .map_err(|_| busy())?;
                let vault = self.reader.control();
                timeout_at(deadline, async {
                    let status = self.reader.status(deadline).await?;
                    if status.current != Some(epoch.epoch.e)
                        || !(1..=epoch.epoch.e).all(|e| status.held.contains(&e))
                    {
                        return Err(unavailable());
                    }
                    vault
                        .wrap_for(&format!("fed-computer:{}", peer.fingerprint))
                        .await
                })
                .await
                .map_err(|_| busy())??
            };
            self.publish_join(
                box_id,
                entry.revision,
                &manifest,
                &wrap,
                epoch.epoch.e,
                deadline,
            )
            .await?;
        }
        if request.after_removal_id.is_some() && self.assemble(&manifest, deadline).await?.is_some()
        {
            self.admit(peer, &request, entry.revision, deadline).await?;
        }
        Ok(())
    }

    async fn is_current_peer(
        &self,
        peer: &PeerAnchor,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        let peers = timeout_at(deadline, self.log.peer_keys().peer_keys_read())
            .await
            .map_err(|_| busy())??;
        let mut matching = peers.iter().filter(|other| other.box_id == peer.box_id);
        Ok(matching.next() == Some(peer) && matching.next().is_none())
    }

    async fn eligible_join(
        &self,
        box_id: &str,
        request: &Request,
        revision: u64,
        deadline: Instant,
    ) -> Result<Option<Observed>, HandlerError> {
        // Read the register before the log head: its removal may not be logged yet.
        let Some(current) = self.reader.read(deadline).await? else {
            return Ok(None);
        };
        let applied = self.caught_up(deadline).await?;
        let timeline = &self.reader.membership;
        let outstanding = timeline.removed_at(box_id, applied);
        let removal_matches = match outstanding {
            Some((_, _, Some(id))) => request.after_removal_id.as_deref() == Some(&id),
            Some((_, _, None)) => false,
            None => request.after_removal_id.is_none(),
        };
        if current.epoch.state != State::Committed
            || !removal_matches
            || current.epoch.removed.iter().any(|removed| {
                removed.box_id == box_id
                    && !timeline.removal_applied(
                        box_id,
                        current.epoch.e,
                        removed.removal_id.as_deref(),
                        applied,
                    )
            })
            || !self.request_current(box_id, revision, deadline).await?
        {
            return Ok(None);
        }
        Ok(Some(current))
    }

    async fn request_current(
        &self,
        box_id: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<bool, HandlerError> {
        Ok(self
            .reader
            .transport
            .kv_entry(&request_key(box_id), deadline)
            .await?
            .is_some_and(|entry| entry.revision == revision && entry.value.is_some()))
    }

    async fn publish_join(
        &self,
        joiner: &str,
        request_revision: u64,
        key: &str,
        wrap: &Value,
        through_epoch: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let responder = &self.reader.own_box;
        let bytes = serde_json::to_vec(wrap).expect("serializable vault wrap");
        let count = bytes.len().div_ceil(PART_BYTES);
        if !(1..=MAX_PARTS).contains(&count) {
            return Err(invalid("fleet join wrap exceeds the part limit"));
        }
        let sha256 = sha256_hex(&bytes);
        let prefix = format!("{key}.{responder}.");
        // Each signed, zero-based part carries the count and complete wrap's
        // digest along with its own base64url data. Never splice two producers.
        for (index, bytes) in bytes.chunks(PART_BYTES).enumerate() {
            let part = format!("{prefix}{index}");
            let body = Part {
                v: 1,
                count,
                sha256: sha256.clone(),
                data: URL_SAFE_NO_PAD.encode(bytes),
            };
            let expected = self
                .reader
                .transport
                .kv_entry(&part, deadline)
                .await?
                .map_or(0, |entry| entry.revision);
            self.put(
                &format!("join.part:{part}:{expected}:{sha256}"),
                &part,
                expected,
                encode(&body),
                deadline,
            )
            .await?;
        }
        let manifest = Manifest {
            v: 1,
            responder: responder.clone(),
            parts: count,
            sha256,
            through_epoch,
        };
        let mut own_manifest = None;
        if self
            .request_current(joiner, request_revision, deadline)
            .await?
        {
            match self
                .put(
                    &format!("join.manifest:{key}:{responder}:{}", manifest.sha256),
                    key,
                    0,
                    encode(&manifest),
                    deadline,
                )
                .await
            {
                Ok(revision) => own_manifest = Some(revision),
                Err(error) if error.code == "hub_write_unsettled" => {
                    let Some(revision) = error.detail.as_ref().and_then(|d| d["revision"].as_u64())
                    else {
                        return Err(error);
                    };
                    let Some(current) = self
                        .assemble(key, deadline)
                        .await?
                        .filter(|current| current.entry.revision >= revision)
                    else {
                        return Err(error);
                    };
                    // Reconcile with the signed manifest and parts that are
                    // usable now, not with a guess about the earlier publish.
                    if current.manifest == manifest {
                        own_manifest = Some(current.entry.revision);
                    } else if current.manifest.responder == *responder {
                        // Part keys are scoped to the request and responder.
                        // Deleting ours here would also delete the current response.
                        return Err(error);
                    }
                }
                Err(error) if raced(&error) => {}
                Err(error) => return Err(error),
            }
            if own_manifest.is_some()
                && self
                    .request_current(joiner, request_revision, deadline)
                    .await?
            {
                return Ok(());
            }
        }
        if let Some(revision) = own_manifest {
            self.delete_join(key, revision, deadline).await?;
        }
        self.clean_join_parts(&prefix, deadline).await
    }

    async fn admit(
        &self,
        peer: &PeerAnchor,
        request: &Request,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let Some(id) = &request.after_removal_id else {
            return Ok(());
        };
        let box_id = &peer.box_id;
        let digest = hub_write::params_digest("member.admitted", &json!([box_id, id]))?;
        let entry_id = &digest[..32];
        loop {
            if self
                .eligible_join(box_id, request, revision, deadline)
                .await?
                .is_none()
                || !self.is_current_peer(peer, deadline).await?
            {
                return Ok(());
            }
            let applied = self.log.applied_sequence(deadline).await?;
            if self
                .reader
                .membership
                .removed_at(box_id, applied)
                .and_then(|(_, _, id)| id)
                .as_ref()
                != Some(id)
            {
                return Ok(());
            }
            match self.log.write_epoch_step(
                &format!("join.admit:{entry_id}:{applied}"),
                &digest,
                &Step {
                    name: "member.admitted".into(),
                    key: LOG_KEY.into(),
                    expected: applied,
                    payload: Payload::Record {
                        kind: "member.admitted".into(),
                        body: json!({"box_id":box_id,"after_removal_id":id,"request_id":request.request_id}),
                        snapshot_id: Some(entry_id.into()),
                    },
                },
                deadline,
            ).await {
                Ok(_) => return Ok(()),
                Err(error) if error.code == "head_moved" && Instant::now() < deadline => continue,
                Err(error) => return Err(error),
            }
        }
    }
}

fn raced(error: &HandlerError) -> bool {
    matches!(
        error.code.as_str(),
        "revision_moved" | "hub_write_superseded"
    )
}

impl<T: LogTransport, U: Trust + Send + Sync + 'static> Controller<T, U> {
    async fn assemble(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<Assembly>, HandlerError> {
        let Some(entry) = self.reader.transport.kv_entry(key, deadline).await? else {
            return Ok(None);
        };
        let Some(bytes) = &entry.value else {
            return Ok(None);
        };
        let signed = self.verify_join_record(key, bytes, deadline).await?;
        let manifest: Manifest = body(&signed)?;
        if manifest.v != 1
            || manifest.responder != signed.header().writer
            || !(1..=MAX_PARTS).contains(&manifest.parts)
            || !(1..=MAX_EPOCHS).contains(&manifest.through_epoch)
            || !is_hex(&manifest.sha256, 64)
        {
            return Err(invalid("invalid fleet join manifest"));
        }
        let mut joined = Vec::new();
        for index in 0..manifest.parts {
            let part_key = format!("{key}.{}.{index}", manifest.responder);
            let Some(bytes) = self
                .reader
                .transport
                .kv_entry(&part_key, deadline)
                .await?
                .and_then(|entry| entry.value)
            else {
                return Ok(None);
            };
            let part_signed = self.verify_join_record(&part_key, &bytes, deadline).await?;
            let part: Part = body(&part_signed)?;
            if part.v != 1
                || part.count != manifest.parts
                || part.sha256 != manifest.sha256
                || part_signed.header().writer != manifest.responder
                || part_signed.header().writer_ep != signed.header().writer_ep
            {
                return Err(invalid("part does not match fleet join manifest"));
            }
            let bytes = URL_SAFE_NO_PAD
                .decode(part.data)
                .map_err(|_| invalid("invalid join part encoding"))?;
            if bytes.is_empty() || bytes.len() > PART_BYTES {
                return Err(invalid("invalid join part size"));
            }
            joined.extend(bytes);
        }
        if sha256_hex(&joined) != manifest.sha256 {
            return Err(invalid("fleet join wrap digest differs from manifest"));
        }
        let wrap =
            serde_json::from_slice(&joined).map_err(|_| invalid("invalid fleet join wrap"))?;
        Ok(Some(Assembly {
            entry,
            manifest,
            wrap,
        }))
    }

    async fn verify_join_record(
        &self,
        key: &str,
        bytes: &[u8],
        deadline: Instant,
    ) -> Result<VerifiedRecord, HandlerError> {
        crate::hub_transport::check_kv_size(key, bytes.len())?;
        self.reader
            .membership
            .verify_kv(
                UnverifiedRecord::parse(key, bytes)?,
                self.log.applied_sequence(deadline).await?,
                Sight::FirstOrAfterGap,
                deadline,
            )
            .await
            .map(|(record, _)| record)
    }

    async fn import_join(
        &self,
        key: &str,
        request: &Request,
        assembly: &Assembly,
        deadline: Instant,
    ) -> Result<BTreeSet<u64>, HandlerError> {
        let _guard = timeout_at(deadline, self.reader.importing.lock())
            .await
            .map_err(|_| busy())?;
        let vault = self.reader.control();
        let through = assembly.manifest.through_epoch;
        let held = timeout_at(deadline, async {
            let before: BTreeSet<_> = self
                .reader
                .status(deadline)
                .await?
                .held
                .into_iter()
                .collect();
            let imported = vault.import(&assembly.wrap).await?;
            let after: BTreeSet<_> = self
                .reader
                .status(deadline)
                .await?
                .held
                .into_iter()
                .collect();
            let expected = 1..=through;
            if imported.epochs.iter().any(|e| !expected.contains(e))
                || after.difference(&before).any(|e| !expected.contains(e))
                || !expected.clone().all(|e| after.contains(&e))
            {
                return Err(invalid(
                    "vault did not import the manifest's committed epochs",
                ));
            }
            Ok(after)
        })
        .await
        .map_err(|_| busy())??;
        let request_id = request.request_id.clone();
        let revision = assembly.entry.revision;
        let bytes = assembly.entry.value.clone().expect("manifest bytes");
        let key = key.to_owned();
        let manifest = encode(&assembly.manifest);
        self.reader
            .sql
            .run(deadline, "record_hub_held_epochs", move |store| {
                store.record_hub_held_epochs(
                    HubEpochSource::Join(&request_id),
                    &(1..=through).collect::<Vec<_>>(),
                )?;
                store.record_hub_kv(
                    &key,
                    revision,
                    Some((&bytes, Some(&manifest))),
                    crate::unix_millis(),
                )?;
                Ok(())
            })
            .await?;
        self.tracked_epochs(held, deadline).await
    }

    async fn clean_join(
        &self,
        own: &str,
        request_key: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        // Superseded requests and crashed responders can leave parts behind.
        // Keep the request until all of this box's responses have been removed.
        let keys = self
            .reader
            .transport
            .join_keys(&format!("wrapjoin.{own}."), deadline)
            .await?;
        if self.request_current(own, revision, deadline).await? {
            self.delete_join_keys(keys, deadline).await?;
            self.delete_join(request_key, revision, deadline).await?;
        }
        Ok(())
    }

    async fn clean_join_parts(&self, prefix: &str, deadline: Instant) -> Result<(), HandlerError> {
        let keys = self.reader.transport.join_keys(prefix, deadline).await?;
        self.delete_join_keys(keys, deadline).await
    }

    async fn delete_join_keys(
        &self,
        keys: Vec<String>,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        for key in keys {
            if let Some(entry) = self.reader.transport.kv_entry(&key, deadline).await? {
                if entry.value.is_some() {
                    self.delete_join(&key, entry.revision, deadline).await?;
                }
            }
        }
        Ok(())
    }

    async fn delete_join(
        &self,
        key: &str,
        expected: u64,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let request = format!("join.delete:{key}:{expected}");
        let digest = hub_write::params_digest(&request, &Value::Null)?;
        match self
            .log
            .write_epoch_step(
                &request,
                &digest,
                &Step {
                    name: "wrapjoin.delete".into(),
                    key: key.into(),
                    expected,
                    payload: Payload::Delete,
                },
                deadline,
            )
            .await
        {
            Ok(_) => Ok(()),
            Err(error) if raced(&error) => Ok(()),
            Err(error) => Err(error),
        }
    }
}

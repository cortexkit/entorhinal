//! Entorhinal's session-bindings participant: the agent-move participant that
//! carries every session binding of the moving agent, ended ones included,
//! with its whole network history, to the receiving Mac.
//!
//! The shared move record names each binding only by `(ref_kind, ref)`. On
//! its own that lets the receiver insert placeholders with no host session,
//! runner or history: the apps cannot list the agent's sessions there until
//! a host registers them again, and content an agent made offline reads as
//! `unknown` network, which allows its links. So entorhinal sends the rows
//! itself, in its own engram stream beside the catalog package closure, under
//! ref kind `session-bindings` with the agent id as the ref. The move record's
//! format does not change: this is one more ref of an existing participant.
//!
//! * Source `prepare` reads the rows and history in one snapshot while the
//!   open source move refuses new registrations, writes the stream (see
//!   `stream.rs`) and admits it through the session door in chunks.
//! * Receiver `import` reads the stream back, checks its digest, format and
//!   rows, and keeps the verified bytes. The move driver then checks that
//!   every carried ref belongs to the move, before the claim is taken.
//! * Receiver `activate` rechecks the kept bytes. The driver installs the rows
//!   in the same transaction that records activation (see
//!   `entorhinal_core::session_carry`).
mod stream;

use crate::{
    catalog_move::CatalogMove,
    door_client::{CutItem, CutRequest as DoorCut, DoorClient},
    HandlerError, HealthGauges, ProjectsHandler,
};
use cortexkit_role_agent_data::{
    describe::{DescribeReply, Store},
    refs::Ref,
    request::CutRequest,
    Op,
};
use entorhinal_catalog::{hex, sha256_hex};
use entorhinal_core::{
    move_record::MoveRef,
    move_store::{AgentMove, MoveSide},
    session_carry::{CarriedBinding, PACKAGE_CLOSURE_KIND, SESSION_BINDINGS_KIND},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{atomic::Ordering, Arc, Mutex, Weak},
};
use tokio::time::Duration;

const KIND: &str = SESSION_BINDINGS_KIND;
/// The stream format, as `prepare` reports it and as the stream's first
/// line names it.
pub(crate) const FORMAT: &str = "session-bindings/v1";
/// Entorhinal's reserved engram stream, shared with the package closure;
/// each cut item still has its own session ref.
const STREAM: &str = "reserved:entorhinal";
/// The session door's per-call cap on raw bytes, the same as the package
/// closure's: both `session.admit` bytes and `session.read` limits.
const CALL_CAP: usize = 1_048_576;

fn invalid(message: impl Into<String>) -> HandlerError {
    HandlerError::new("digest_mismatch", message)
}
fn unavailable(error: impl std::fmt::Debug) -> HandlerError {
    HandlerError::new("unavailable", format!("{error:?}"))
}
/// The door session that holds one cut's bindings. Derived from the ref
/// kind, not the format version, so a receiver finds the item of a newer
/// format and can refuse it as `incompatible_version` from its first line.
fn session_ref(agent: &str, cut: &str) -> String {
    sha256_hex(format!("{KIND}:{agent}:{cut}").as_bytes())[..32].into()
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Checkpoint {
    agent: String,
    step: String,
    item: CutItem,
}

/// Test-only changes to what a source writes, standing in for a source Mac
/// that runs another version or misbehaves.
#[cfg(test)]
#[derive(Clone, Copy, Default)]
pub(crate) struct SourceFault {
    pub(crate) magic: Option<&'static [u8]>,
    pub(crate) extra: Option<fn() -> CarriedBinding>,
}

pub(crate) struct SessionMove {
    store: Weak<Mutex<Option<RegistryStore>>>,
    door: DoorClient,
    health: Arc<HealthGauges>,
    lock: Arc<tokio::sync::Mutex<()>>,
    request_timeout: Duration,
    #[cfg(test)]
    fault: SourceFault,
}

impl ProjectsHandler {
    pub(crate) fn session_move_context(&self) -> SessionMove {
        SessionMove {
            store: Arc::downgrade(&self.store),
            door: self.door_client.clone(),
            health: self.health.clone(),
            lock: self.session_move_lock.clone(),
            request_timeout: Duration::from_secs(30),
            #[cfg(test)]
            fault: SourceFault::default(),
        }
    }
}

/// Answer one agent-data request for entorhinal's participant. A request
/// naming only the package closure goes to the catalog participant exactly as
/// before; one naming only the agent's bindings goes to [`SessionMove`]; one
/// naming both is split, each half answered by its owner, and the replies
/// joined in the request's ref order.
pub(crate) async fn execute_participant(
    catalog: &CatalogMove,
    bindings: &SessionMove,
    method: &str,
    params: Value,
) -> Result<Value, HandlerError> {
    if method == "agent_data.describe" {
        let _: cortexkit_role_agent_data::describe::DescribeRequest =
            serde_json::from_value(params)
                .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        return Ok(serde_json::to_value(DescribeReply::new(
            vec![PACKAGE_CLOSURE_KIND.into(), KIND.into()],
            vec![
                Store::new("package-closure/v1", "agent"),
                Store::new(FORMAT, "agent"),
            ],
            false,
        ))
        .expect("a describe reply always serializes"));
    }
    let refs: Vec<Ref> = params
        .get("refs")
        .cloned()
        .map(serde_json::from_value)
        .transpose()
        .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?
        .unwrap_or_default();
    let ours = refs.iter().filter(|r| r.ref_kind == KIND).count();
    if ours == 0 {
        return catalog.execute(method, params).await;
    }
    if ours == refs.len() {
        return bindings.execute(method, params).await;
    }
    let split = |kind_is_ours: bool| {
        let mut part = params.clone();
        part["refs"] = json!(refs
            .iter()
            .filter(|r| (r.ref_kind == KIND) == kind_is_ours)
            .collect::<Vec<_>>());
        part
    };
    // Abort must reach both halves even when one refuses, so neither keeps
    // state the other has given up.
    if method == "agent_data.abort" {
        let closure = catalog.execute(method, split(false)).await;
        let carried = bindings.execute(method, split(true)).await;
        closure?;
        carried?;
        return Ok(json!({}));
    }
    let closure = catalog.execute(method, split(false)).await?;
    let carried = bindings.execute(method, split(true)).await?;
    Ok(join(method, &refs, closure, carried))
}

/// Join the two halves' replies, keeping per-ref lists in request order.
fn join(method: &str, refs: &[Ref], closure: Value, carried: Value) -> Value {
    let list = |key: &str| {
        let mut closure = closure[key]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter();
        let mut carried = carried[key]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter();
        refs.iter()
            .filter_map(|r| {
                if r.ref_kind == KIND {
                    carried.next()
                } else {
                    closure.next()
                }
            })
            .collect::<Vec<_>>()
    };
    match method {
        "agent_data.busy" | "agent_data.diverged" => json!({"refs":list("refs")}),
        "agent_data.prepare" => json!({"items":list("items"),"external":[]}),
        "agent_data.export" | "agent_data.import" => json!({"items":list("items")}),
        "agent_data.release" => json!({"enforced":
            closure["enforced"] == true && carried["enforced"] == true}),
        // Status reports the half that is further behind, so a caller that
        // resends after a lost reply reaches both halves.
        "agent_data.status" => {
            const ORDER: [&str; 7] = [
                "aborted",
                "none",
                "prepared",
                "imported",
                "exported",
                "activated",
                "released",
            ];
            let rank = |v: &Value| ORDER.iter().position(|s| v["step"] == *s).unwrap_or(0);
            if rank(&closure) <= rank(&carried) {
                closure
            } else {
                carried
            }
        }
        _ => json!({}),
    }
}

impl SessionMove {
    #[cfg(test)]
    pub(crate) fn with_fault(mut self, fault: SourceFault) -> Self {
        self.fault = fault;
        self
    }

    fn store(&self) -> Result<RegistryStore, HandlerError> {
        self.store
            .upgrade()
            .and_then(|s| s.lock().unwrap().clone())
            .ok_or_else(|| unavailable("session move store is gone"))
    }

    pub(crate) async fn execute(&self, method: &str, params: Value) -> Result<Value, HandlerError> {
        let op = Op::ALL
            .into_iter()
            .find(|op| op.method() == method)
            .ok_or_else(|| HandlerError::new("invalid_request", "unknown agent-data operation"))?;
        tokio::time::timeout(self.request_timeout, self.execute_inner(method, params))
            .await
            .unwrap_or_else(|_| Err(unavailable("session bindings request deadline exceeded")))
            .map_err(|mut error| {
                // A code this op may not answer with would make the driver
                // treat this Mac's own participant as broken; answer
                // `unavailable` instead, which the driver retries.
                if !cortexkit_role_agent_data::errors::allowed_refusals(op).contains(
                    &cortexkit_role_agent_data::errors::RefusalCode::from_code(&error.code),
                ) {
                    error.code = "unavailable".into();
                }
                error
            })
    }

    async fn execute_inner(&self, method: &str, params: Value) -> Result<Value, HandlerError> {
        if method == "agent_data.busy" {
            let request: cortexkit_role_agent_data::busy::BusyRequest =
                serde_json::from_value(params)
                    .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
            for r in request.refs.as_slice() {
                check_ref(r)?;
            }
            // The bindings are records, not running sessions. Whether each
            // session is running is checked by the move itself, through
            // session liveness and the session's own participant.
            return Ok(json!({"refs":request.refs.as_slice().iter()
                .map(|_| json!({"state":"idle"})).collect::<Vec<_>>()}));
        }
        let cut: CutRequest = if method == "agent_data.release" {
            let release: cortexkit_role_agent_data::release::ReleaseRequest =
                serde_json::from_value(params)
                    .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
            CutRequest::new(release.refs, release.cut_id)
        } else {
            serde_json::from_value(params)
                .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?
        };
        let refs = cut.refs.as_slice();
        if refs.len() != 1 {
            return Err(HandlerError::new(
                "invalid_request",
                "one session-bindings ref required",
            ));
        }
        let agent = check_ref(&refs[0])?.to_owned();
        let id = cut.cut_id.as_str().to_owned();
        let saved = self.load(&id)?;
        if saved.as_ref().is_some_and(|s| s.agent != agent) {
            return Err(HandlerError::new("id_reused", "cut belongs to another ref"));
        }
        if method == "agent_data.status" {
            return Ok(json!({"step":saved.map_or("none".into(), |s| s.step)}));
        }
        // One state-changing request per Mac at a time. A second one, such
        // as a retry racing a request its caller gave up on, is refused
        // `unavailable`, which the driver retries.
        let _guard = self
            .lock
            .clone()
            .try_lock_owned()
            .map_err(|_| unavailable("a session bindings request is already running"))?;
        let saved = self.load(&id)?;
        let row = self.agent_move(&id).await?;
        if row.agent_id != agent {
            return Err(HandlerError::new(
                "id_reused",
                "cut belongs to another agent",
            ));
        }
        self.advance(method, &id, &agent, &row, saved).await
    }

    async fn agent_move(&self, cut: &str) -> Result<AgentMove, HandlerError> {
        let store = self.store()?;
        let cut = cut.to_owned();
        crate::catalog_sql::run(move || {
            store
                .agent_move(&cut)
                .map_err(HandlerError::from)?
                .ok_or_else(|| HandlerError::new("cut_unknown", "no move has this cut"))
        })
        .await
    }

    fn side(row: &AgentMove, side: MoveSide) -> Result<(), HandlerError> {
        if row.side != side {
            return Err(HandlerError::new(
                "invalid_state",
                "this op belongs to the other side of the move",
            ));
        }
        Ok(())
    }

    async fn advance(
        &self,
        method: &str,
        id: &str,
        agent: &str,
        row: &AgentMove,
        mut saved: Option<Checkpoint>,
    ) -> Result<Value, HandlerError> {
        let step = saved
            .as_ref()
            .map_or_else(|| "none".to_owned(), |s| s.step.clone());
        let step = step.as_str();
        match method {
            "agent_data.prepare" => {
                Self::side(row, MoveSide::Source)?;
                if saved.is_none() {
                    let state = self.write_stream(id, agent).await?;
                    saved = Some(state);
                }
                let state = saved.as_mut().expect("prepared above");
                if !matches!(state.step.as_str(), "prepared" | "exported") {
                    return Err(HandlerError::new(
                        "invalid_state",
                        "cut is not a source cut",
                    ));
                }
                if state.step == "prepared" {
                    self.upload(id, &state.item).await?;
                }
                Ok(json!({"items":[{"ref_kind":KIND,"ref":agent,
                    "sessionRef":state.item.session_ref,"stream":STREAM,
                    "endOffset":state.item.end_offset,"digest":state.item.digest,
                    "format":FORMAT,"frozen":true}],"external":[]}))
            }
            "agent_data.export" => {
                Self::side(row, MoveSide::Source)?;
                let mut state = saved
                    .ok_or_else(|| HandlerError::new("cut_unknown", "prepare the cut first"))?;
                if !matches!(state.step.as_str(), "prepared" | "exported") {
                    return Err(HandlerError::new("invalid_state", "cut is not prepared"));
                }
                if state.step == "prepared" {
                    self.cover(&state.item).await?;
                    state.step = "exported".into();
                    self.save(id, &state)?;
                }
                Ok(json!({"items":[{"sessionRef":state.item.session_ref,
                    "endOffset":state.item.end_offset}]}))
            }
            "agent_data.release" => {
                Self::side(row, MoveSide::Source)?;
                let mut state =
                    saved.ok_or_else(|| HandlerError::new("cut_unknown", "cut is unknown"))?;
                if !matches!(state.step.as_str(), "exported" | "released") {
                    return Err(HandlerError::new("invalid_state", "cut cannot release"));
                }
                state.step = "released".into();
                self.save(id, &state)?;
                Ok(json!({"enforced":true}))
            }
            "agent_data.diverged" => {
                Self::side(row, MoveSide::Receiver)?;
                // Always `at_cut`: activation never replaces a binding this
                // Mac already has, so there is no local copy it could
                // overwrite.
                Ok(json!({"refs":[{"state":"at_cut"}]}))
            }
            "agent_data.import" => {
                Self::side(row, MoveSide::Receiver)?;
                if let Some(state) = saved.filter(|_| matches!(step, "imported" | "activated")) {
                    return Ok(json!({"items":[{"sessionRef":state.item.session_ref,
                        "endOffset":state.item.end_offset,"digest":state.item.digest}]}));
                }
                if step != "none" {
                    return Err(HandlerError::new("invalid_state", "cut cannot import"));
                }
                let read = self
                    .door
                    .read(&DoorCut { cut_id: id.into() })
                    .await
                    .map_err(|_| HandlerError::new("cut_not_ready", "cut is not readable"))?;
                let item = read
                    .items
                    .into_iter()
                    .find(|i| i.session_ref == session_ref(agent, id) && i.stream == STREAM)
                    .ok_or_else(|| invalid("cut has no session bindings"))?;
                let dir = self.directory(id)?;
                crate::catalog_sql::run({
                    let dir = dir.clone();
                    move || private_dir(&dir)
                })
                .await?;
                self.download(id, &item, &dir.join("payload")).await?;
                let payload = dir.join("payload");
                let owner = agent.to_owned();
                crate::catalog_sql::run(move || stream::decode(&payload, &owner).map(|_| ()))
                    .await?;
                let state = Checkpoint {
                    agent: agent.into(),
                    step: "imported".into(),
                    item,
                };
                self.save(id, &state)?;
                Ok(json!({"items":[{"sessionRef":state.item.session_ref,
                    "endOffset":state.item.end_offset,"digest":state.item.digest}]}))
            }
            "agent_data.activate" => {
                Self::side(row, MoveSide::Receiver)?;
                let mut state =
                    saved.ok_or_else(|| HandlerError::new("cut_unknown", "cut is not imported"))?;
                if !matches!(state.step.as_str(), "imported" | "activated") {
                    return Err(HandlerError::new("invalid_state", "cut is not imported"));
                }
                // The kept bytes must still be the ones import verified.
                let payload = self.directory(id)?.join("payload");
                let digest = state.item.digest.clone();
                crate::catalog_sql::run(move || check_digest(&payload, &digest)).await?;
                if state.step != "activated" {
                    state.step = "activated".into();
                    self.save(id, &state)?;
                }
                Ok(json!({}))
            }
            "agent_data.abort" => {
                if matches!(step, "activated" | "released") {
                    return Err(HandlerError::new("invalid_state", "cut cannot abort"));
                }
                let state = Checkpoint {
                    agent: agent.into(),
                    step: "aborted".into(),
                    item: saved.map(|s| s.item).unwrap_or_else(|| CutItem {
                        session_ref: session_ref(agent, id),
                        stream: STREAM.into(),
                        end_offset: 0,
                        digest: sha256_hex([]),
                    }),
                };
                self.save(id, &state)?;
                Ok(json!({}))
            }
            _ => Err(HandlerError::new(
                "invalid_request",
                "unknown agent-data operation",
            )),
        }
    }

    /// Read the agent's bindings and write this cut's stream once. Later
    /// prepares for the same cut reuse it, so the item never changes.
    async fn write_stream(&self, id: &str, agent: &str) -> Result<Checkpoint, HandlerError> {
        let store = self.store()?;
        let dir = self.directory(id)?;
        let (cut, owner) = (id.to_owned(), agent.to_owned());
        #[cfg(test)]
        let fault = self.fault;
        let (end_offset, digest, count) = crate::catalog_sql::run(move || {
            #[allow(unused_mut)]
            let mut rows = store
                .export_session_bindings(&cut, &owner)
                .map_err(HandlerError::from)?;
            #[allow(unused_mut)]
            let mut magic = stream::MAGIC;
            #[cfg(test)]
            {
                if let Some(extra) = fault.extra {
                    rows.push(extra());
                }
                if let Some(other) = fault.magic {
                    magic = other;
                }
            }
            private_dir(&dir)?;
            let (length, digest) = stream::encode(&dir.join("payload"), magic, &owner, &rows)?;
            Ok::<_, HandlerError>((length, digest, rows.len() as u64))
        })
        .await?;
        let state = Checkpoint {
            agent: agent.into(),
            step: "prepared".into(),
            item: CutItem {
                session_ref: session_ref(agent, id),
                stream: STREAM.into(),
                end_offset,
                digest,
            },
        };
        self.save(id, &state)?;
        self.health
            .session_bindings_carried
            .fetch_add(count, Ordering::Relaxed);
        Ok(state)
    }

    fn directory(&self, cut: &str) -> Result<PathBuf, HandlerError> {
        if !crate::door_client::valid_cut_id(cut) {
            return Err(HandlerError::new("invalid_request", "invalid cut id"));
        }
        Ok(cut_directory(&self.store()?, cut))
    }

    fn load(&self, cut: &str) -> Result<Option<Checkpoint>, HandlerError> {
        match fs::read(self.directory(cut)?.join("checkpoint.json")) {
            Ok(bytes) => serde_json::from_slice(&bytes)
                .map(Some)
                .map_err(unavailable),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(unavailable(e)),
        }
    }

    fn save(&self, cut: &str, state: &Checkpoint) -> Result<(), HandlerError> {
        let dir = self.directory(cut)?;
        private_dir(&dir)?;
        let path = dir.join("checkpoint.json");
        let temporary = path.with_extension("tmp");
        let mut file = File::create(&temporary).map_err(unavailable)?;
        file.write_all(&serde_json::to_vec(state).map_err(unavailable)?)
            .map_err(unavailable)?;
        file.sync_all().map_err(unavailable)?;
        fs::rename(temporary, &path).map_err(unavailable)?;
        #[cfg(unix)]
        File::open(&dir)
            .and_then(|f| f.sync_all())
            .map_err(unavailable)?;
        Ok(())
    }

    async fn cover(&self, item: &CutItem) -> Result<(), HandlerError> {
        let cover = self
            .door
            .session("session.cover", json!({"sessionRef":item.session_ref}))
            .await
            .map_err(unavailable)?;
        if cover["durableOffset"].as_u64() != Some(item.end_offset)
            || cover["cloudOffset"].as_u64().unwrap_or(0) < item.end_offset
        {
            return Err(unavailable(
                "session bindings stream is not covered in the cloud yet",
            ));
        }
        Ok(())
    }

    /// Admit the stream through the session door in calls of at most
    /// [`CALL_CAP`] bytes. A resend after a lost reply finds the door ahead
    /// of it, confirms the admitted prefix is the same bytes, and goes on.
    async fn upload(&self, id: &str, item: &CutItem) -> Result<(), HandlerError> {
        let mut file = File::open(self.directory(id)?.join("payload")).map_err(unavailable)?;
        let mut offset = 0;
        let mut buffer = vec![0; CALL_CAP];
        loop {
            let n = file.read(&mut buffer).map_err(unavailable)?;
            if n == 0 {
                break;
            }
            let bytes = &buffer[..n];
            match self
                .door
                .session(
                    "session.admit",
                    json!({"sessionRef":item.session_ref,"fromOffset":offset,"bytes":hex(bytes)}),
                )
                .await
            {
                Ok(reply) if reply["durableOffset"].as_u64() == Some(offset + n as u64) => {}
                Err(crate::door_client::DoorError::Refused { code, detail, .. })
                    if code == "offset_mismatch"
                        && detail
                            .as_ref()
                            .and_then(|d| d["expectedOffset"].as_u64())
                            .is_some_and(|end| {
                                end >= offset + n as u64 && end <= item.end_offset
                            }) =>
                {
                    let reply = self
                        .door
                        .session(
                            "session.read",
                            json!({"sessionRef":item.session_ref,"fromOffset":offset,"limit":n}),
                        )
                        .await
                        .map_err(unavailable)?;
                    if reply["bytes"].as_str() != Some(hex(bytes).as_str()) {
                        return Err(HandlerError::new(
                            "invalid_state",
                            "admitted session bindings prefix differs",
                        ));
                    }
                }
                other => {
                    return Err(unavailable(format!(
                        "session bindings admission: {other:?}"
                    )))
                }
            }
            offset += n as u64;
        }
        self.cover(item).await
    }

    /// Read the cut's stream back in calls of at most [`CALL_CAP`] bytes,
    /// hashing as it goes, and refuse it unless the whole matches the
    /// item's digest.
    async fn download(&self, id: &str, item: &CutItem, path: &Path) -> Result<(), HandlerError> {
        if item.end_offset > stream::STREAM_CAP {
            return Err(invalid("session bindings stream exceeds its cap"));
        }
        let mut file = File::create(path).map_err(unavailable)?;
        let mut hash = Sha256::new();
        let mut offset = 0;
        while offset < item.end_offset {
            let limit = (item.end_offset - offset).min(CALL_CAP as u64);
            let reply = self
                .door
                .session(
                    "session.read",
                    json!({"sessionRef":item.session_ref,"fromOffset":offset,"limit":limit,"cutId":id}),
                )
                .await
                .map_err(unavailable)?;
            if reply["fromOffset"].as_u64() != Some(offset)
                || reply["endOffset"].as_u64() != Some(offset + limit)
                || reply["streamEnd"].as_u64() != Some(item.end_offset)
                || reply["digest"].as_str() != Some(item.digest.as_str())
            {
                return Err(invalid("cut read metadata differs"));
            }
            let bytes = unhex(
                reply["bytes"]
                    .as_str()
                    .ok_or_else(|| invalid("cut read has no bytes"))?,
            )?;
            if bytes.len() as u64 != limit {
                return Err(invalid("short session bindings read"));
            }
            hash.update(&bytes);
            file.write_all(&bytes).map_err(unavailable)?;
            offset += limit;
        }
        if hex(&hash.finalize()) != item.digest {
            let mut error = invalid("session bindings cut hash differs");
            error.detail = Some(json!({"reason":"staging_digest_mismatch"}));
            return Err(error);
        }
        file.sync_all().map_err(unavailable)?;
        Ok(())
    }
}

fn unhex(text: &str) -> Result<Vec<u8>, HandlerError> {
    if !text.len().is_multiple_of(2) {
        return Err(invalid("invalid hexadecimal bytes"));
    }
    (0..text.len())
        .step_by(2)
        .map(|i| {
            text.get(i..i + 2)
                .filter(|pair| {
                    pair.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                })
                .and_then(|pair| u8::from_str_radix(pair, 16).ok())
                .ok_or_else(|| invalid("invalid hexadecimal bytes"))
        })
        .collect()
}

fn cut_directory(store: &RegistryStore, cut: &str) -> PathBuf {
    store.store_folder().join("session-moves").join(cut)
}

fn check_digest(path: &Path, digest: &str) -> Result<(), HandlerError> {
    let bytes = fs::read(path).map_err(unavailable)?;
    if sha256_hex(&bytes) != digest {
        let mut error = invalid("kept session bindings differ from the imported cut");
        error.detail = Some(json!({"reason":"staging_digest_mismatch"}));
        return Err(error);
    }
    Ok(())
}

fn check_ref(r: &Ref) -> Result<&str, HandlerError> {
    if r.ref_kind != KIND || r.r#ref.is_empty() {
        return Err(HandlerError::new(
            "invalid_request",
            "invalid session-bindings ref",
        ));
    }
    Ok(&r.r#ref)
}

fn private_dir(path: &Path) -> Result<(), HandlerError> {
    fs::create_dir_all(path).map_err(unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(unavailable)?;
    }
    Ok(())
}

/// Why the bindings a receiver kept for a cut cannot be installed.
pub(crate) enum StagedError {
    /// A carried row names a ref the move does not: the source broke the
    /// agent-data contract by sending data outside the move.
    OutsideMove(MoveRef),
    /// The kept bytes are missing, unreadable or no longer verify.
    Unreadable(HandlerError),
}

/// The bindings this Mac imported for receiver move `cut` of `agent`,
/// rechecked against the import's digest, each of whose refs must be one of
/// the move's `binding_refs`. Runs on a blocking thread.
pub(crate) fn staged_bindings(
    store: &RegistryStore,
    cut: &str,
    agent: &str,
    binding_refs: &BTreeSet<MoveRef>,
) -> Result<Vec<CarriedBinding>, StagedError> {
    let dir = cut_directory(store, cut);
    let state: Checkpoint = fs::read(dir.join("checkpoint.json"))
        .map_err(unavailable)
        .and_then(|bytes| serde_json::from_slice(&bytes).map_err(unavailable))
        .map_err(StagedError::Unreadable)?;
    if state.agent != agent || !matches!(state.step.as_str(), "imported" | "activated") {
        return Err(StagedError::Unreadable(HandlerError::new(
            "invalid_state",
            "session bindings were not imported for this cut",
        )));
    }
    let payload = dir.join("payload");
    check_digest(&payload, &state.item.digest).map_err(StagedError::Unreadable)?;
    let rows = stream::decode(&payload, agent).map_err(StagedError::Unreadable)?;
    if let Some(row) = rows.iter().find(|row| {
        !binding_refs.contains(&MoveRef {
            ref_kind: row.ref_kind.clone(),
            reference: row.session_ref.clone(),
        })
    }) {
        return Err(StagedError::OutsideMove(MoveRef {
            ref_kind: row.ref_kind.clone(),
            reference: row.session_ref.clone(),
        }));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn r(kind: &str, reference: &str) -> Ref {
        serde_json::from_value(json!({"ref_kind":kind,"ref":reference})).unwrap()
    }

    #[test]
    fn joined_replies_keep_request_ref_order() {
        let refs = [r(PACKAGE_CLOSURE_KIND, "a"), r(KIND, "a")];
        let joined = join(
            "agent_data.prepare",
            &refs,
            json!({"items":[{"n":"closure"}],"external":[]}),
            json!({"items":[{"n":"bindings"}],"external":[]}),
        );
        assert_eq!(
            joined,
            json!({"items":[{"n":"closure"},{"n":"bindings"}],"external":[]})
        );
        let status = join(
            "agent_data.status",
            &refs,
            json!({"step":"activated"}),
            json!({"step":"imported"}),
        );
        assert_eq!(status, json!({"step":"imported"}));
        let release = join(
            "agent_data.release",
            &refs,
            json!({"enforced":true}),
            json!({"enforced":true}),
        );
        assert_eq!(release, json!({"enforced":true}));
    }
}

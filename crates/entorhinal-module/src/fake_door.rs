//! In-memory session door with per-device streams/latches and shared cut manifests.
//!
//! Rules follow engram AGENT_SYNC.md at 8a96bc0: "The move: a durable
//! transaction", "Abort", "Recovery", and "Resume on the new machine".

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};

use crate::door_client::*;
use crate::log_client::{encode_hex, LogConnector, LogTransport, TransportError};

#[derive(Debug, Clone)]
pub(crate) enum Effect {
    Refuse(String),
    FailBefore(TransportError),
    LoseReply(TransportError),
    Seal(SealState, Option<Value>),
    Handoff(HandoffReply),
    Takeover(TakeoverProgress),
}

#[derive(Debug, Clone)]
pub(crate) struct DoorCall {
    pub(crate) author: String,
    pub(crate) request: Request,
}

#[derive(Debug, Clone)]
pub(crate) struct Cut {
    pub(crate) author: String,
    pub(crate) items: Vec<CutItem>,
    pub(crate) state: SealState,
    pub(crate) handoff: HandoffReply,
    pub(crate) failure: Option<Value>,
    pub(crate) aborted: bool,
    takeovers: BTreeMap<String, TakeoverProgress>,
}

#[derive(Default)]
struct State {
    payloads: BTreeMap<(String, String), Vec<u8>>,
    session_calls: Vec<(String, Value)>,
    streams: BTreeMap<(String, String, String), CutItem>,
    cuts: BTreeMap<String, Cut>,
    handed_off: BTreeMap<(String, String), String>,
    scripts: BTreeMap<(String, String), VecDeque<Effect>>,
    calls: Vec<DoorCall>,
}

#[derive(Clone)]
pub(crate) struct FakeDoor {
    author: String,
    state: Arc<Mutex<State>>,
}

impl FakeDoor {
    pub(crate) fn new(author: [u8; 16]) -> Self {
        Self {
            author: encode_hex(&author),
            state: Arc::new(Mutex::new(State::default())),
        }
    }

    pub(crate) fn for_author(&self, author: [u8; 16]) -> Self {
        Self {
            author: encode_hex(&author),
            state: self.state.clone(),
        }
    }

    pub(crate) fn client(&self) -> DoorClient {
        DoorClient::new(Arc::new(self.clone()))
    }

    pub(crate) fn set_stream(&self, item: CutItem) {
        self.state.lock().unwrap().streams.insert(
            (
                self.author.clone(),
                item.session_ref.clone(),
                item.stream.clone(),
            ),
            item,
        );
    }

    pub(crate) fn script(&self, op: &str, effect: Effect) {
        assert!(
            OPS.contains(&op) || ["session.admit", "session.read", "session.cover"].contains(&op)
        );
        self.state
            .lock()
            .unwrap()
            .scripts
            .entry((self.author.clone(), op.into()))
            .or_default()
            .push_back(effect);
    }

    pub(crate) fn calls(&self) -> Vec<DoorCall> {
        self.state.lock().unwrap().calls.clone()
    }
    pub(crate) fn session_calls(&self) -> Vec<(String, Value)> {
        self.state.lock().unwrap().session_calls.clone()
    }
    pub(crate) fn replace_payload(&self, cut: &str, bytes: Vec<u8>, rehash: bool) {
        let mut state = self.state.lock().unwrap();
        let record = state.cuts.get_mut(cut).unwrap();
        let item = record
            .items
            .iter_mut()
            .find(|i| i.stream == "reserved:entorhinal")
            .unwrap();
        if rehash {
            item.digest = entorhinal_catalog::sha256_hex(&bytes);
            item.end_offset = bytes.len() as u64;
        }
        let key = (record.author.clone(), item.session_ref.clone());
        state.payloads.insert(key, bytes);
    }
    pub(crate) fn cut(&self, cut_id: &str) -> Option<Cut> {
        self.state.lock().unwrap().cuts.get(cut_id).cloned()
    }

    pub(crate) fn admit(&self, session_ref: &str) -> Result<(), TransportError> {
        let state = self.state.lock().unwrap();
        // Handoff permanently stops writes on this device until a move back is
        // accepted (AGENT_SYNC.md, move step 6 and Recovery).
        if let Some(cut_id) = state
            .handed_off
            .get(&(self.author.clone(), session_ref.into()))
        {
            return Err(refuse(
                "container_handed_off",
                Some(json!({"cutId":cut_id})),
            ));
        }
        // Pending and published cuts still block source writes. A failed seal
        // unblocks them (AGENT_SYNC.md, move steps 3-4).
        if let Some((cut_id, _)) = state.cuts.iter().find(|(_, cut)| {
            cut.author == self.author
                && !cut.aborted
                && cut.handoff.state == HandoffState::NotStarted
                && matches!(cut.state, SealState::Pending | SealState::Published)
                && cut.items.iter().any(|item| item.session_ref == session_ref)
        }) {
            return Err(refuse("cut_pending_handoff", Some(json!({"cutId":cut_id}))));
        }
        Ok(())
    }

    pub(crate) fn handle(
        &self,
        caller: &str,
        method: &str,
        params: Value,
    ) -> Result<Vec<u8>, TransportError> {
        // The move contract admits only the daemon-attested entorhinal principal.
        if caller != "reserved:entorhinal" {
            return Err(refuse("caller_forbidden", None));
        }
        if method.starts_with("session.") {
            if method == "session.admit" {
                self.admit(params["sessionRef"].as_str().unwrap_or_default())?;
            }
            let mut state = self.state.lock().unwrap();
            state.session_calls.push((method.into(), params.clone()));
            let effect = state
                .scripts
                .get_mut(&(self.author.clone(), method.into()))
                .and_then(VecDeque::pop_front);
            match &effect {
                Some(Effect::Refuse(code)) => return Err(refuse(code, None)),
                Some(Effect::FailBefore(e)) => return Err(e.clone()),
                _ => {}
            }
            let reply = session(&mut state, &self.author, method, &params)?;
            if let Some(Effect::LoseReply(e)) = effect {
                return Err(e);
            }
            return Ok(serde_json::to_vec(&json!({"result":reply})).unwrap());
        }
        let mut object = params
            .as_object()
            .cloned()
            .ok_or_else(|| refuse("invalid_request", None))?;
        if object.contains_key("op") {
            return Err(refuse("invalid_request", None));
        }
        object.insert("op".into(), Value::String(method.into()));
        let request: Request = serde_json::from_value(Value::Object(object))
            .map_err(|_| refuse("invalid_request", None))?;
        validate(&request)?;
        let mut state = self.state.lock().unwrap();
        state.calls.push(DoorCall {
            author: self.author.clone(),
            request: request.clone(),
        });
        let effect = state
            .scripts
            .get_mut(&(self.author.clone(), method.into()))
            .and_then(VecDeque::pop_front);
        match &effect {
            Some(Effect::Refuse(code)) => return Err(refuse(code, None)),
            Some(Effect::FailBefore(error)) => return Err(error.clone()),
            _ => {}
        }
        let reply = execute(&mut state, &self.author, &request, effect.as_ref())?;
        if let Some(Effect::LoseReply(error)) = effect {
            return Err(error);
        }
        Ok(serde_json::to_vec(&json!({"result":reply})).unwrap())
    }
}

fn session(
    state: &mut State,
    author: &str,
    method: &str,
    params: &Value,
) -> Result<Value, TransportError> {
    let session = params["sessionRef"]
        .as_str()
        .filter(|s| s.len() == 32)
        .ok_or_else(|| refuse("invalid_request", None))?;
    let key = (author.to_string(), session.to_string());
    match method {
        "session.admit" => {
            let bytes = crate::catalog_move::stream_bytes(
                params["bytes"]
                    .as_str()
                    .ok_or_else(|| refuse("invalid_request", None))?,
            )
            .map_err(|_| refuse("invalid_request", None))?;
            // Engram's own per-call cap, written out here rather than shared
            // with entorhinal's constant, so a wrong client cap fails a test.
            if bytes.len() > 1_048_576 {
                return Err(refuse("record_too_large", Some(json!({"cap":1_048_576}))));
            }
            let payload = state.payloads.entry(key).or_default();
            if params["fromOffset"].as_u64() != Some(payload.len() as u64) {
                return Err(refuse(
                    "offset_mismatch",
                    Some(json!({"expectedOffset":payload.len()})),
                ));
            }
            payload.extend(bytes);
            let end = payload.len();
            let item = CutItem {
                session_ref: session.into(),
                stream: "reserved:entorhinal".into(),
                end_offset: end as u64,
                digest: entorhinal_catalog::sha256_hex(payload),
            };
            state
                .streams
                .insert((author.into(), session.into(), item.stream.clone()), item);
            Ok(json!({"durableOffset":end,"cloudOffset":end}))
        }
        "session.cover" => {
            let end = state.payloads.get(&key).map_or(0, Vec::len);
            Ok(json!({"durableOffset":end,"cloudOffset":end}))
        }
        "session.read" => {
            let (owner, end, digest) = if let Some(cut) = params["cutId"].as_str() {
                let record = state
                    .cuts
                    .get(cut)
                    .filter(|r| r.state == SealState::Published && !r.aborted)
                    .ok_or_else(|| refuse("not_found", None))?;
                let item = record
                    .items
                    .iter()
                    .find(|i| i.session_ref == session && i.stream == "reserved:entorhinal")
                    .ok_or_else(|| refuse("cut_item_missing", None))?;
                (
                    record.author.clone(),
                    item.end_offset as usize,
                    Some(item.digest.clone()),
                )
            } else {
                (
                    author.into(),
                    state.payloads.get(&key).map_or(0, Vec::len),
                    None,
                )
            };
            let payload = state
                .payloads
                .get(&(owner, session.into()))
                .ok_or_else(|| refuse("unavailable", None))?;
            let from = params["fromOffset"].as_u64().unwrap() as usize;
            let limit = params["limit"].as_u64().unwrap() as usize;
            if limit > 1_048_576 {
                return Err(refuse("record_too_large", Some(json!({"cap":1_048_576}))));
            }
            if from > end {
                return Err(refuse("offset_out_of_range", None));
            }
            let stop = end.min(from.saturating_add(limit));
            let bytes = payload
                .get(from..stop)
                .ok_or_else(|| refuse("unavailable", None))?;
            let mut reply = json!({"bytes":entorhinal_catalog::hex(bytes),"fromOffset":from,"endOffset":stop,"streamEnd":end});
            if let Some(digest) = digest {
                reply["digest"] = json!(digest);
            }
            Ok(reply)
        }
        _ => Err(refuse("invalid_request", None)),
    }
}

#[async_trait]
impl LogConnector for FakeDoor {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl LogTransport for FakeDoor {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.handle("reserved:entorhinal", method, params)
    }
}

fn refuse(code: &str, detail: Option<Value>) -> TransportError {
    TransportError::Refusal {
        code: code.into(),
        detail,
    }
}

fn valid_hex(value: &str, bytes: usize) -> bool {
    value.len() == bytes * 2
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

fn validate(request: &Request) -> Result<(), TransportError> {
    if !valid_cut_id(request.cut_id()) {
        return Err(refuse("invalid_request", None));
    }
    if let Request::Seal(seal) = request {
        // Stream labels identify modules, refs are 16-byte hex and digests are
        // SHA-256 hex (AGENT_SYNC.md, move step 3; cut_ops::parse_items).
        if seal.items.len() > ITEM_CAP {
            return Err(refuse("too_large", None));
        }
        if seal.items.is_empty() {
            return Err(refuse("invalid_request", None));
        }
        let mut keys = BTreeSet::new();
        for item in &seal.items {
            let module = item.stream.strip_prefix("reserved:").unwrap_or("");
            if module.is_empty()
                || !module
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
                || !valid_hex(&item.session_ref, 16)
                || !valid_hex(&item.digest, 32)
            {
                return Err(refuse("invalid_request", None));
            }
            if !keys.insert((&item.session_ref, &item.stream)) {
                return Err(refuse("cut_item_duplicate", None));
            }
        }
    }
    Ok(())
}

fn handoff(state: HandoffState) -> HandoffReply {
    HandoffReply {
        state,
        session_ref: None,
        code: None,
    }
}
fn takeover(state: TakeoverState) -> TakeoverProgress {
    TakeoverProgress {
        state,
        session_ref: None,
        code: None,
    }
}

fn local_status(cut: &Cut) -> Value {
    json!(StatusReply {
        state: cut.state,
        handoff: Some(cut.handoff.clone()),
        takeover: None,
        failure: cut.failure.clone()
    })
}

fn remote_status(cut: &Cut, author: &str) -> Value {
    json!(StatusReply {
        state: SealState::Published,
        handoff: None,
        takeover: cut.takeovers.get(author).cloned(),
        failure: None
    })
}

fn latch(state: &mut State, cut_id: &str) {
    let cut = &state.cuts[cut_id];
    for item in &cut.items {
        state.handed_off.insert(
            (cut.author.clone(), item.session_ref.clone()),
            cut_id.into(),
        );
    }
}

fn apply_effect(state: &mut State, cut_id: &str, author: &str, effect: Option<&Effect>) {
    let cut = state.cuts.get_mut(cut_id).unwrap();
    match effect {
        Some(Effect::Seal(next, failure)) if cut.state == SealState::Pending => {
            cut.state = *next;
            cut.failure = failure.clone();
            if *next == SealState::Aborted {
                cut.aborted = true;
                cut.handoff = handoff(HandoffState::Aborted);
            }
        }
        Some(Effect::Handoff(next))
            if !matches!(
                cut.handoff.state,
                HandoffState::Released | HandoffState::TakenOver | HandoffState::Aborted
            ) =>
        {
            cut.handoff = next.clone();
        }
        Some(Effect::Takeover(next))
            if cut
                .takeovers
                .get(author)
                .is_none_or(|progress| progress.state != TakeoverState::Taken) =>
        {
            cut.takeovers.insert(author.into(), next.clone());
        }
        _ => {}
    }
}

fn execute(
    state: &mut State,
    author: &str,
    request: &Request,
    effect: Option<&Effect>,
) -> Result<Value, TransportError> {
    let cut_id = request.cut_id();
    if let Request::Seal(seal) = request {
        if let Some(cut) = state.cuts.get(cut_id) {
            // A matching seal replay reports its current state even if a stream
            // has since advanced (AGENT_SYNC.md, move step 3).
            if cut.author != author || cut.items != seal.items {
                return Err(refuse("id_reused", None));
            }
        } else {
            for item in &seal.items {
                let local = state
                    .streams
                    .get(&(author.into(), item.session_ref.clone(), item.stream.clone()))
                    .ok_or_else(|| refuse("cut_item_unknown", None))?;
                if local.end_offset != item.end_offset {
                    return Err(refuse(
                        "cut_end_mismatch",
                        Some(json!({"stream":item.stream,"endOffset":local.end_offset})),
                    ));
                }
                if local.digest != item.digest {
                    return Err(refuse(
                        "cut_digest_mismatch",
                        Some(json!({"stream":item.stream})),
                    ));
                }
            }
            state.cuts.insert(
                cut_id.into(),
                Cut {
                    author: author.into(),
                    items: seal.items.clone(),
                    state: SealState::Pending,
                    handoff: handoff(HandoffState::NotStarted),
                    failure: None,
                    aborted: false,
                    takeovers: BTreeMap::new(),
                },
            );
        }
        apply_effect(state, cut_id, author, effect);
        return Ok(local_status(&state.cuts[cut_id]));
    }
    let cut = state
        .cuts
        .get(cut_id)
        .ok_or_else(|| refuse("not_found", None))?;
    match request {
        Request::Status(_) => {
            if cut.author != author && cut.state != SealState::Published {
                return Err(refuse("not_found", None));
            }
            apply_effect(state, cut_id, author, effect);
            let cut = &state.cuts[cut_id];
            Ok(if cut.author == author {
                local_status(cut)
            } else {
                remote_status(cut, author)
            })
        }
        Request::Read(_) => {
            // Other devices see only published cuts (AGENT_SYNC.md, Resume step 1).
            // cut_ops::read also lets the sealer read its own local items early.
            if cut.author != author && cut.state != SealState::Published {
                return Err(refuse("not_found", None));
            }
            Ok(json!(ReadReply {
                items: cut.items.clone(),
                author: cut.author.clone()
            }))
        }
        Request::Handoff(_) => {
            // Only the sealer can release its containers, and only after publication
            // proves their bytes reached the cloud (AGENT_SYNC.md, move step 6).
            if cut.author != author {
                return Err(refuse("not_found", None));
            }
            if cut.state != SealState::Published {
                return Err(refuse("cut_not_published", None));
            }
            if cut.aborted {
                return Err(refuse("cut_handoff_started", None));
            }
            let taken = cut
                .takeovers
                .values()
                .any(|progress| progress.state == TakeoverState::Taken);
            let cut = state.cuts.get_mut(cut_id).unwrap();
            if !matches!(
                cut.handoff.state,
                HandoffState::Released | HandoffState::TakenOver
            ) {
                cut.handoff = handoff(if taken {
                    HandoffState::TakenOver
                } else {
                    HandoffState::InProgress
                });
            }
            latch(state, cut_id);
            apply_effect(state, cut_id, author, effect);
            Ok(json!(state.cuts[cut_id].handoff))
        }
        Request::Takeover(_) => {
            // Takeover requires another device's published cut; it cannot acquire
            // this device's own cut (AGENT_SYNC.md, Resume step 5).
            if cut.author == author {
                return Err(refuse(
                    if cut.state == SealState::Published {
                        "invalid_request"
                    } else {
                        "cut_not_published"
                    },
                    None,
                ));
            }
            if cut.state != SealState::Published {
                return Err(refuse("not_found", None));
            }
            let cut = state.cuts.get_mut(cut_id).unwrap();
            let progress = cut
                .takeovers
                .entry(author.into())
                .or_insert_with(|| takeover(TakeoverState::InProgress));
            if progress.state == TakeoverState::Failed {
                *progress = takeover(TakeoverState::InProgress);
            }
            apply_effect(state, cut_id, author, effect);
            let progress = &state.cuts[cut_id].takeovers[author];
            Ok(json!(TakeoverReply {
                takeover: progress.state,
                session_ref: progress.session_ref.clone(),
                code: progress.code.clone()
            }))
        }
        Request::Abort(_) => {
            // Once either handoff or takeover starts, abort cannot restore ownership.
            // Earlier aborts keep published cuts readable but cancel pending ones
            // permanently (AGENT_SYNC.md, Abort).
            if cut.author != author {
                return Err(refuse("not_found", None));
            }
            if !matches!(
                cut.handoff.state,
                HandoffState::NotStarted | HandoffState::Aborted
            ) || !cut.takeovers.is_empty()
            {
                return Err(refuse("cut_handoff_started", None));
            }
            let cut = state.cuts.get_mut(cut_id).unwrap();
            if cut.state == SealState::Pending {
                cut.state = SealState::Aborted;
            }
            cut.aborted = true;
            cut.handoff = handoff(HandoffState::Aborted);
            Ok(json!(AbortReply {
                handoff: HandoffState::Aborted,
                state: cut.state
            }))
        }
        Request::Accept(_) => {
            // A move back re-enables writes only for its incoming containers on this
            // device. A repeat may return empty after those restrictions are gone
            // (AGENT_SYNC.md, Moving back; cut_ops::execute).
            if cut.author == author {
                return Err(refuse(
                    if cut.state == SealState::Published {
                        "invalid_request"
                    } else {
                        "cut_not_published"
                    },
                    None,
                ));
            }
            if cut.state != SealState::Published {
                return Err(refuse("cut_not_published", None));
            }
            let refs: BTreeSet<_> = cut
                .items
                .iter()
                .map(|item| item.session_ref.clone())
                .collect();
            let lifted: Vec<_> = refs
                .into_iter()
                .filter(|reference| {
                    state
                        .handed_off
                        .remove(&(author.into(), reference.clone()))
                        .is_some()
                })
                .collect();
            Ok(json!(AcceptReply { lifted }))
        }
        Request::Seal(_) => unreachable!(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const ID: &str = "00112233445566778899aabbccddeeff";
    const BACK: &str = "ffeeddccbbaa99887766554433221100";
    const REF: &str = "11111111111111111111111111111111";
    const OTHER: &str = "22222222222222222222222222222222";

    fn item(reference: &str) -> CutItem {
        CutItem {
            session_ref: reference.into(),
            stream: "reserved:broca".into(),
            end_offset: 42,
            digest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        }
    }
    fn cut(id: &str) -> CutRequest {
        CutRequest { cut_id: id.into() }
    }
    fn seal(id: &str, items: Vec<CutItem>) -> SealRequest {
        SealRequest {
            cut_id: id.into(),
            items,
        }
    }
    fn fixture() -> (FakeDoor, FakeDoor, SealRequest) {
        let source = FakeDoor::new([1; 16]);
        let receiver = source.for_author([2; 16]);
        source.set_stream(item(REF));
        (source, receiver, seal(ID, vec![item(REF)]))
    }
    async fn publish(source: &FakeDoor, request: &SealRequest) {
        source.client().seal(request).await.unwrap();
        source.script("cut.status", Effect::Seal(SealState::Published, None));
        assert_eq!(
            source
                .client()
                .status(&cut(&request.cut_id))
                .await
                .unwrap()
                .state,
            SealState::Published
        );
    }
    fn error_code(error: TransportError) -> String {
        match error {
            TransportError::Refusal { code, .. } => code,
            other => panic!("unexpected {other:?}"),
        }
    }
    fn seal_code(door: &FakeDoor, request: &SealRequest) -> String {
        error_code(
            door.handle("reserved:entorhinal", "cut.seal", json!(request))
                .unwrap_err(),
        )
    }

    #[test]
    fn only_reserved_entorhinal_is_admitted_for_every_op() {
        let door = FakeDoor::new([1; 16]);
        for caller in [
            "direct",
            "reserved:engram",
            "reserved:broca",
            "entorhinal",
            "session:entorhinal",
            "flow:entorhinal",
        ] {
            for op in OPS {
                assert_eq!(
                    error_code(door.handle(caller, op, json!({"cutId":ID})).unwrap_err()),
                    "caller_forbidden"
                );
            }
        }
        assert!(door.calls().is_empty());
    }

    #[test]
    fn item_syntax_is_strict_and_empty_cuts_are_refused() {
        let (door, _, request) = fixture();
        let mut invalid = vec![seal(ID, vec![])];
        for stream in [
            "broca",
            "reserved:",
            "reserved:a:b",
            "reserved:a b",
            "reserved:a/b",
        ] {
            let mut item = item(REF);
            item.stream = stream.into();
            invalid.push(seal(ID, vec![item]));
        }
        for field in ["cutId", "sessionRef", "digest", "stream", "endOffset"] {
            let mut value = json!(&request);
            if field == "cutId" {
                value[field] = json!("ABCDEF");
            } else {
                value["items"][0][field] = json!(false);
            }
            assert_eq!(
                error_code(
                    door.handle("reserved:entorhinal", "cut.seal", value)
                        .unwrap_err()
                ),
                "invalid_request"
            );
        }
        for mut item in [item("not-hex"), item(REF), item(REF)] {
            if item.session_ref == REF {
                item.digest = "A".repeat(64);
            }
            invalid.push(seal(ID, vec![item]));
        }
        for request in invalid {
            assert_eq!(seal_code(&door, &request), "invalid_request");
        }
        for params in [
            json!({"cutId":ID,"extra":true}),
            json!({"cutId":ID,"op":"cut.status"}),
        ] {
            assert_eq!(
                error_code(
                    door.handle("reserved:entorhinal", "cut.status", params)
                        .unwrap_err()
                ),
                "invalid_request"
            );
        }
        assert!(door.cut(ID).is_none());
    }

    #[tokio::test]
    async fn seal_enforces_1024_item_cap_at_exact_boundary() {
        let door = FakeDoor::new([1; 16]);
        let items: Vec<_> = (0..ITEM_CAP).map(|i| item(&format!("{i:032x}"))).collect();
        for item in &items {
            door.set_stream(item.clone());
        }
        let mut too_many = items.clone();
        too_many.push(item(OTHER));
        assert_eq!(seal_code(&door, &seal(ID, too_many)), "too_large");
        assert!(door.cut(ID).is_none());
        assert_eq!(
            door.client().seal(&seal(ID, items)).await.unwrap().state,
            SealState::Pending
        );
    }

    #[test]
    fn duplicate_items_are_refused_without_a_seal_or_fence() {
        let (door, _, _) = fixture();
        assert_eq!(
            seal_code(&door, &seal(ID, vec![item(REF), item(REF)])),
            "cut_item_duplicate"
        );
        assert!(door.cut(ID).is_none());
        assert!(door.admit(REF).is_ok());
    }
    #[test]
    fn unknown_items_are_refused_without_a_seal_or_fence() {
        let (door, _, _) = fixture();
        assert_eq!(
            seal_code(&door, &seal(ID, vec![item(OTHER)])),
            "cut_item_unknown"
        );
        assert!(door.cut(ID).is_none());
        assert!(door.admit(REF).is_ok());
    }
    #[test]
    fn mismatched_end_is_refused_without_a_seal_or_fence() {
        let (door, _, _) = fixture();
        let mut item = item(REF);
        item.end_offset = 41;
        assert_eq!(seal_code(&door, &seal(ID, vec![item])), "cut_end_mismatch");
        assert!(door.cut(ID).is_none());
        assert!(door.admit(REF).is_ok());
    }
    #[test]
    fn mismatched_digest_is_refused_without_a_seal_or_fence() {
        let (door, _, _) = fixture();
        let mut item = item(REF);
        item.digest = "b".repeat(64);
        assert_eq!(
            seal_code(&door, &seal(ID, vec![item])),
            "cut_digest_mismatch"
        );
        assert!(door.cut(ID).is_none());
        assert!(door.admit(REF).is_ok());
    }

    #[tokio::test]
    async fn seal_replay_returns_current_state_and_different_items_refuse_id_reused() {
        let (source, _, request) = fixture();
        publish(&source, &request).await;
        let mut changed = request.clone();
        changed.items[0].end_offset = 43;
        assert_eq!(seal_code(&source, &changed), "id_reused");
        source.set_stream(changed.items[0].clone());
        assert_eq!(
            source.client().seal(&request).await.unwrap().state,
            SealState::Published
        );
        assert_eq!(source.cut(ID).unwrap().items, request.items);
    }

    #[tokio::test]
    async fn admits_are_fenced_from_seal_through_publication_until_handoff_or_abort() {
        let (source, receiver, request) = fixture();
        source.client().seal(&request).await.unwrap();
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "cut_pending_handoff"
        );
        assert!(source.admit(OTHER).is_ok());
        assert!(receiver.admit(REF).is_ok());
        publish(&source, &request).await;
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "cut_pending_handoff"
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::InProgress
        );
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "container_handed_off"
        );
        let (source, _, request) = fixture();
        publish(&source, &request).await;
        assert_eq!(
            source.client().abort(&cut(ID)).await.unwrap().state,
            SealState::Published
        );
        assert!(source.admit(REF).is_ok());
        assert_eq!(
            source.client().abort(&cut(ID)).await.unwrap().handoff,
            HandoffState::Aborted
        );
    }

    #[tokio::test]
    async fn read_returns_sealer_and_remote_reads_require_a_published_manifest() {
        let (source, receiver, request) = fixture();
        source.client().seal(&request).await.unwrap();
        let local = source.client().read(&cut(ID)).await.unwrap();
        assert_eq!(local.author, "01010101010101010101010101010101");
        assert_eq!(local.items, request.items);
        let local_status = source.client().status(&cut(ID)).await.unwrap();
        assert_eq!(local_status.state, SealState::Pending);
        assert_eq!(
            local_status.handoff.unwrap().state,
            HandoffState::NotStarted
        );
        assert!(local_status.takeover.is_none());
        assert_eq!(
            receiver.client().read(&cut(ID)).await.unwrap_err().code(),
            "not_found"
        );
        assert_eq!(
            receiver.client().status(&cut(ID)).await.unwrap_err().code(),
            "not_found"
        );
        publish(&source, &request).await;
        assert_eq!(receiver.client().read(&cut(ID)).await.unwrap(), local);
        let status = receiver.client().status(&cut(ID)).await.unwrap();
        assert_eq!(status.state, SealState::Published);
        assert!(status.handoff.is_none());
        receiver.script("cut.read", Effect::Refuse("verify_failed".into()));
        assert_eq!(
            receiver.client().read(&cut(ID)).await.unwrap_err().code(),
            "verify_failed"
        );
    }

    #[tokio::test]
    async fn failed_and_aborted_seals_are_terminal_and_release_pending_fences() {
        for terminal in [SealState::Failed, SealState::Aborted] {
            let (source, receiver, request) = fixture();
            source.client().seal(&request).await.unwrap();
            source.script(
                "cut.status",
                Effect::Seal(
                    terminal,
                    Some(json!({"sessionRef":REF,"code":"lease_held_elsewhere"})),
                ),
            );
            assert_eq!(
                source.client().status(&cut(ID)).await.unwrap().state,
                terminal
            );
            assert!(source.admit(REF).is_ok());
            assert_eq!(
                receiver.client().read(&cut(ID)).await.unwrap_err().code(),
                "not_found"
            );
            source.script("cut.seal", Effect::Seal(SealState::Published, None));
            assert_eq!(
                source.client().seal(&request).await.unwrap().state,
                terminal
            );
        }
        let (source, receiver, request) = fixture();
        source.client().seal(&request).await.unwrap();
        assert_eq!(
            source.client().abort(&cut(ID)).await.unwrap().state,
            SealState::Aborted
        );
        source.script("cut.status", Effect::Seal(SealState::Published, None));
        assert_eq!(
            source.client().status(&cut(ID)).await.unwrap().state,
            SealState::Aborted
        );
        assert_eq!(
            receiver.client().read(&cut(ID)).await.unwrap_err().code(),
            "not_found"
        );
    }

    #[tokio::test]
    async fn handoff_requires_local_publication_and_retries_failed_progress() {
        let (source, receiver, request) = fixture();
        source.client().seal(&request).await.unwrap();
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap_err().code(),
            "cut_not_published"
        );
        publish(&source, &request).await;
        assert_eq!(
            receiver
                .client()
                .handoff(&cut(ID))
                .await
                .unwrap_err()
                .code(),
            "not_found"
        );
        for code in ["unavailable", "handoff_not_covered"] {
            source.script("cut.handoff", Effect::Refuse(code.into()));
            assert!(source
                .client()
                .handoff(&cut(ID))
                .await
                .unwrap_err()
                .retryable());
        }
        source.script(
            "cut.handoff",
            Effect::Handoff(HandoffReply {
                state: HandoffState::Failed,
                session_ref: Some(REF.into()),
                code: Some("unavailable".into()),
            }),
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::Failed
        );
        source.script(
            "cut.handoff",
            Effect::Handoff(handoff(HandoffState::Released)),
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::Released
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::Released
        );
    }

    #[tokio::test]
    async fn takeover_refuses_self_and_waking_source_reports_taken_over() {
        let (source, receiver, request) = fixture();
        publish(&source, &request).await;
        assert_eq!(
            source.client().takeover(&cut(ID)).await.unwrap_err().code(),
            "invalid_request"
        );
        receiver.script(
            "cut.takeover",
            Effect::Takeover(takeover(TakeoverState::Failed)),
        );
        assert_eq!(
            receiver.client().takeover(&cut(ID)).await.unwrap().takeover,
            TakeoverState::Failed
        );
        receiver.script(
            "cut.takeover",
            Effect::Takeover(takeover(TakeoverState::Taken)),
        );
        assert_eq!(
            receiver.client().takeover(&cut(ID)).await.unwrap().takeover,
            TakeoverState::Taken
        );
        assert_eq!(
            receiver.client().takeover(&cut(ID)).await.unwrap().takeover,
            TakeoverState::Taken
        );
        assert_eq!(
            receiver
                .client()
                .status(&cut(ID))
                .await
                .unwrap()
                .takeover
                .unwrap()
                .state,
            TakeoverState::Taken
        );
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "cut_pending_handoff"
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::TakenOver
        );
        assert_eq!(
            source.client().handoff(&cut(ID)).await.unwrap().state,
            HandoffState::TakenOver
        );
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "container_handed_off"
        );
    }

    #[tokio::test]
    async fn abort_refuses_after_handoff_or_takeover_has_started() {
        for take_over in [false, true] {
            let (source, receiver, request) = fixture();
            publish(&source, &request).await;
            if take_over {
                receiver.client().takeover(&cut(ID)).await.unwrap();
            } else {
                source.client().handoff(&cut(ID)).await.unwrap();
            }
            assert_eq!(
                source.client().abort(&cut(ID)).await.unwrap_err().code(),
                "cut_handoff_started"
            );
            assert!(!source.cut(ID).unwrap().aborted);
        }
    }

    #[tokio::test]
    async fn accept_refuses_self_sealed_and_unpublished_cuts() {
        let (source, receiver, request) = fixture();
        source.client().seal(&request).await.unwrap();
        assert_eq!(
            receiver.client().accept(&cut(ID)).await.unwrap_err().code(),
            "cut_not_published"
        );
        publish(&source, &request).await;
        assert_eq!(
            source.client().accept(&cut(ID)).await.unwrap_err().code(),
            "invalid_request"
        );
        assert!(receiver
            .client()
            .accept(&cut(ID))
            .await
            .unwrap()
            .lifted
            .is_empty());
    }

    #[tokio::test]
    async fn accept_on_move_back_lifts_only_incoming_local_latches_and_replay_may_be_empty() {
        let (source, receiver, mut request) = fixture();
        source.set_stream(item(OTHER));
        request.items.push(item(OTHER));
        publish(&source, &request).await;
        source.client().handoff(&cut(ID)).await.unwrap();
        receiver.set_stream(item(REF));
        publish(&receiver, &seal(BACK, vec![item(REF)])).await;
        source.client().read(&cut(BACK)).await.unwrap();
        assert_eq!(
            error_code(source.admit(REF).unwrap_err()),
            "container_handed_off"
        );
        assert_eq!(
            source.client().accept(&cut(BACK)).await.unwrap().lifted,
            vec![REF]
        );
        assert!(source.admit(REF).is_ok());
        assert_eq!(
            error_code(source.admit(OTHER).unwrap_err()),
            "container_handed_off"
        );
        assert!(source
            .client()
            .accept(&cut(BACK))
            .await
            .unwrap()
            .lifted
            .is_empty());
        assert_eq!(
            error_code(source.admit(OTHER).unwrap_err()),
            "container_handed_off"
        );
    }

    #[tokio::test]
    async fn lost_seal_and_abort_replies_reconcile_and_repeat_without_reapplying() {
        let (source, _, request) = fixture();
        source.script("cut.seal", Effect::LoseReply(TransportError::NoReply));
        source.client().seal(&request).await.unwrap();
        source.script("cut.abort", Effect::LoseReply(TransportError::NoReply));
        source.client().abort(&cut(ID)).await.unwrap();
        assert!(source.admit(REF).is_ok());
        let calls = source.calls();
        assert_eq!(
            calls
                .iter()
                .map(|call| call.request.op())
                .collect::<Vec<_>>(),
            [
                "cut.seal",
                "cut.status",
                "cut.seal",
                "cut.abort",
                "cut.status",
                "cut.abort"
            ]
        );
        assert_eq!(calls[0].request, calls[2].request);
        assert!(calls
            .iter()
            .all(|call| call.author == "01010101010101010101010101010101"));
        assert_eq!(source.cut(ID).unwrap().state, SealState::Aborted);
    }
}

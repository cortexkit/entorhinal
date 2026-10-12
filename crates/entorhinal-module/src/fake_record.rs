//! In-process generic record peer, not a crypto emulator. It admits only
//! entorhinal, limits subjects and bodies, pages batch reads, remembers writes
//! for 64 versions, and preserves confirmed copies and pending writes on restart.
//! Citations name sections of reviewed engram record spec ct_00000000-0000-40f0.
//! Wire fields and limits follow `intent / Module operations` and `constraints /
//! Subjects, Body, Closed refusal set`, using the typed client's validators.
//! Signature and decryption failures are injected at the verified-read boundary;
//! the fake does not implement encryption or replay the device roster.
//! A scripted `control_head_stale` represents failure after the peer's one
//! recovery retry (`constraints / Control-head recovery`), not its first retry.

use super::{
    log_client::{decode_hex, encode_hex, LogConnector, LogTransport, TransportError},
    record_client::*,
};
use async_trait::async_trait;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Clone)]
struct Stored {
    version: u64,
    body: Vec<u8>,
    author: String,
}
#[derive(Clone)]
struct Receipt {
    subject: String,
    expected: u64,
    body: Vec<u8>,
    author: String,
    version: u64,
}
#[derive(Default)]
struct Cloud {
    rows: HashMap<String, Stored>,
    receipts: HashMap<String, Receipt>,
}
#[derive(Clone, PartialEq, Eq)]
struct Pending {
    id: String,
    expected: u64,
}
#[derive(Default)]
struct Local {
    copies: HashMap<String, Stored>,
    pending: HashMap<String, Pending>,
}

pub(crate) struct FakeRecord {
    cloud: Arc<Mutex<Cloud>>,
    local: Arc<Mutex<Local>>,
    pub author: String,
    pub offline: AtomicBool,
    pub hung: AtomicBool,
    pub drop_reply: AtomicBool,
    pub member: AtomicBool,
    pub keys: AtomicBool,
    pub scoped: AtomicBool,
    pub calls: AtomicUsize,
    pub batches: Mutex<Vec<Vec<String>>>,
    changed: tokio::sync::Notify,
    principal: Mutex<String>,
    errors: Mutex<HashMap<String, String>>,
}

impl FakeRecord {
    pub(crate) fn new(author: [u8; 16]) -> Arc<Self> {
        Arc::new(Self {
            cloud: Arc::new(Mutex::new(Cloud::default())),
            local: Arc::new(Mutex::new(Local::default())),
            author: encode_hex(&author),
            offline: AtomicBool::new(false),
            hung: AtomicBool::new(false),
            drop_reply: AtomicBool::new(false),
            member: AtomicBool::new(true),
            keys: AtomicBool::new(true),
            scoped: AtomicBool::new(false),
            calls: AtomicUsize::new(0),
            batches: Mutex::new(Vec::new()),
            changed: tokio::sync::Notify::new(),
            principal: Mutex::new("reserved:entorhinal".into()),
            errors: Mutex::new(HashMap::new()),
        })
    }

    /// Preserve the highest confirmed record and any unanswered write on
    /// restart, so an offline read cannot silently regress (intent, Rules 4-5).
    pub(crate) fn restart(&self) -> Arc<Self> {
        let mut next = Self::new([0; 16]);
        let next_mut = Arc::get_mut(&mut next).unwrap();
        next_mut.author = self.author.clone();
        next_mut.cloud = self.cloud.clone();
        next_mut.local = self.local.clone();
        next_mut
            .offline
            .store(self.offline.load(Ordering::Relaxed), Ordering::Relaxed);
        next
    }

    pub(crate) async fn wait_for_calls(&self, count: usize) {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.calls.load(Ordering::Relaxed) >= count {
                return;
            }
            changed.await;
        }
    }

    pub(crate) fn error(&self, subject: &str, code: Option<&str>) {
        let mut errors = self.errors.lock().unwrap();
        if let Some(code) = code {
            errors.insert(subject.into(), code.into());
        } else {
            errors.remove(subject);
        }
    }

    pub(crate) fn put(&self, subject: &str, version: u64, body: Vec<u8>, author: &str) {
        self.cloud.lock().unwrap().rows.insert(
            subject.into(),
            Stored {
                version,
                body,
                author: author.into(),
            },
        );
    }

    fn read(&self, subject: &str) -> Result<RecordReply, TransportError> {
        // When the cloud or its device-roster fetch is unavailable, use the
        // confirmed local copy with stale=true unless a write is pending
        // (intent, Rules 4-5).
        let mut local = self.local.lock().unwrap();
        let stale = self.offline.load(Ordering::Relaxed);
        if stale {
            if let Some(mark) = local.pending.get(subject) {
                return Err(refuse(
                    "unavailable",
                    Some(json!({"pending_write_id":mark.id})),
                ));
            }
        } else {
            if let Some(code) = self.errors.lock().unwrap().get(subject) {
                return Err(refuse(code, None));
            }
            let row = self.cloud.lock().unwrap().rows.get(subject).cloned();
            let version = row.as_ref().map_or(0, |r| r.version);
            if version < local.copies.get(subject).map_or(0, |r| r.version) {
                return Err(refuse("verify_failed", None));
            }
            if local
                .pending
                .get(subject)
                .is_some_and(|mark| version > mark.expected)
            {
                local.pending.remove(subject);
            }
            if let Some(row) = row {
                local.copies.insert(subject.into(), row);
            }
        }
        let row = local.copies.get(subject);
        Ok(RecordReply {
            absent: row.is_none(),
            version: row.map_or(0, |r| r.version),
            body: row.map(|r| r.body.clone()),
            author: row.map(|r| r.author.clone()),
            self_author: self.author.clone(),
            stale,
        })
    }

    fn cas(
        &self,
        subject: &str,
        expected: u64,
        body: Vec<u8>,
        id: String,
    ) -> Result<Value, TransportError> {
        let mark = Pending {
            id: id.clone(),
            expected,
        };
        let prior = {
            let mut local = self.local.lock().unwrap();
            let prior = local.pending.get(subject).cloned();
            if prior.as_ref().is_none_or(|p| expected >= p.expected) {
                local.pending.insert(subject.into(), mark.clone());
            }
            prior
        };
        // A cached record cannot establish that a new write landed: every
        // compare-and-swap requires a cloud reply (intent, Rule 3).
        if self.offline.load(Ordering::Relaxed) {
            return Err(refuse("unavailable", None));
        }
        let outcome = (|| {
            // Membership and scripted verification errors precede receipts.
            // Receipts precede CAS so a retry replays its original version even
            // when a later write changed the record (constraints, Cloud plane).
            if !self.member.load(Ordering::Relaxed) {
                return Err(refuse("not_member", None));
            }
            if let Some(code) = self.errors.lock().unwrap().get(subject) {
                return Err(refuse(code, None));
            }
            let mut cloud = self.cloud.lock().unwrap();
            if let Some(receipt) = cloud.receipts.get(&id) {
                if receipt.subject != subject
                    || receipt.expected != expected
                    || receipt.body != body
                    || receipt.author != self.author
                {
                    return Err(refuse("id_reused", None));
                }
                return Ok(receipt.version);
            }
            let row = cloud.rows.get(subject);
            let version = row.map_or(0, |r| r.version);
            if version != expected {
                return Err(refuse(
                    "version_moved",
                    Some(json!({"current_version":version,"author":row.map(|r| &r.author)})),
                ));
            }
            let version = version
                .checked_add(1)
                .ok_or_else(|| refuse("unavailable", None))?;
            cloud.rows.insert(
                subject.into(),
                Stored {
                    version,
                    body: body.clone(),
                    author: self.author.clone(),
                },
            );
            cloud.receipts.insert(
                id.clone(),
                Receipt {
                    subject: subject.into(),
                    expected,
                    body: body.clone(),
                    author: self.author.clone(),
                    version,
                },
            );
            // Keep receipts for this subject's last 64 versions in the same
            // transaction as the new version (constraints, Cloud plane).
            cloud
                .receipts
                .retain(|_, r| r.subject != subject || r.version > version.saturating_sub(64));
            Ok(version)
        })();
        match outcome {
            Ok(version) => {
                if self.drop_reply.swap(false, Ordering::Relaxed) {
                    return Err(TransportError::NoReply);
                }
                let mut local = self.local.lock().unwrap();
                if local.pending.get(subject) == Some(&mark) {
                    local.pending.remove(subject);
                }
                if local
                    .copies
                    .get(subject)
                    .is_none_or(|r| r.version <= version)
                {
                    local.copies.insert(
                        subject.into(),
                        Stored {
                            version,
                            body,
                            author: self.author.clone(),
                        },
                    );
                }
                Ok(json!({"version":version}))
            }
            Err(error) => {
                let mut local = self.local.lock().unwrap();
                if local.pending.get(subject) == Some(&mark) {
                    if let Some(prior) = prior {
                        local.pending.insert(subject.into(), prior);
                    } else {
                        local.pending.remove(subject);
                    }
                }
                Err(error)
            }
        }
    }
}

fn refuse(code: &str, detail: Option<Value>) -> TransportError {
    TransportError::Refusal {
        code: code.into(),
        detail,
    }
}
fn subject(params: &Value) -> Result<&str, TransportError> {
    let s = params["subject"]
        .as_str()
        .ok_or_else(|| refuse("invalid_request", None))?;
    check_subject(s)?;
    Ok(s)
}
fn check_subject(s: &str) -> Result<(), TransportError> {
    validate_subject(s).map_err(|e| match e {
        RecordError::Refused { code, detail } => TransportError::Refusal { code, detail },
        _ => refuse("invalid_request", None),
    })
}

// Open connections share the peer's error and hang settings; changing those
// settings affects the next call even without reconnecting.
#[derive(Clone)]
pub(crate) struct FakeRecordConnector(pub Arc<FakeRecord>);
#[async_trait]
impl LogConnector for FakeRecordConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(self.0.clone())
    }
}

#[async_trait]
impl LogTransport for FakeRecord {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.changed.notify_waiters();
        if self.hung.load(Ordering::Relaxed) {
            std::future::pending::<()>().await;
        }
        // Refuse all callers except daemon-attested entorhinal before looking
        // at their request bodies (constraints, Callers).
        if *self.principal.lock().unwrap() != "reserved:entorhinal"
            || self.scoped.load(Ordering::Relaxed)
        {
            return Err(refuse("caller_forbidden", None));
        }
        let object = params
            .as_object()
            .ok_or_else(|| refuse("invalid_request", None))?;
        let allowed: &[&str] = match method {
            READ => &["subject"],
            READ_MANY => &["subjects"],
            CAS => &["subject", "expected_version", "body", "write_id"],
            _ => return Err(refuse("invalid_request", None)),
        };
        if object.keys().any(|k| !allowed.contains(&k.as_str())) {
            return Err(refuse("invalid_request", None));
        }
        if !self.keys.load(Ordering::Relaxed) {
            return Err(refuse("key_unavailable", None));
        }
        let result = match method {
            READ => wire_reply(&self.read(subject(&params)?)?),
            READ_MANY => {
                let subjects = params["subjects"]
                    .as_array()
                    .ok_or_else(|| refuse("invalid_request", None))?;
                if subjects.is_empty() || subjects.len() > MANY_CAP {
                    return Err(refuse("invalid_request", None));
                }
                let subjects: Vec<&str> = subjects
                    .iter()
                    .map(|s| s.as_str().ok_or_else(|| refuse("invalid_request", None)))
                    .collect::<Result<_, _>>()?;
                for s in &subjects {
                    check_subject(s)?;
                }
                self.batches
                    .lock()
                    .unwrap()
                    .push(subjects.iter().map(|s| (*s).into()).collect());
                let mut bytes = 0;
                let mut entries = Vec::new();
                for s in &subjects {
                    let reply = self.read(s);
                    let size = reply
                        .as_ref()
                        .ok()
                        .and_then(|r| r.body.as_ref())
                        .map_or(0, Vec::len);
                    // Return a non-empty prefix of the requested subjects;
                    // plaintext bodies may total at most 1 MiB (intent, read_many).
                    if !entries.is_empty() && bytes + size > PAGE_CAP {
                        break;
                    }
                    bytes += size;
                    entries.push(match reply {
                        Ok(reply) => wire_reply(&reply),
                        Err(TransportError::Refusal { code, detail }) => {
                            json!({"error":{"code":code,"detail":detail}})
                        }
                        Err(_) => unreachable!(),
                    });
                }
                json!({"next":if entries.len() == subjects.len() { None } else { Some(entries.len()) },"entries":entries})
            }
            CAS => {
                let s = subject(&params)?;
                let body = decode_hex(
                    params["body"]
                        .as_str()
                        .ok_or_else(|| refuse("invalid_request", None))?,
                )
                .map_err(|_| refuse("invalid_request", None))?;
                if body.len() > BODY_CAP {
                    return Err(refuse(
                        "too_large",
                        Some(json!({"field":"body","cap":BODY_CAP})),
                    ));
                }
                let id = params["write_id"]
                    .as_str()
                    .ok_or_else(|| refuse("invalid_request", None))?;
                if !valid_author(id) {
                    return Err(refuse("invalid_request", None));
                }
                let expected = params["expected_version"]
                    .as_u64()
                    .ok_or_else(|| refuse("invalid_request", None))?;
                self.cas(s, expected, body, id.into())?
            }
            _ => unreachable!(),
        };
        Ok(serde_json::to_vec(&json!({"result":result})).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn client(peer: &Arc<FakeRecord>) -> RecordClient {
        RecordClient::new(Arc::new(FakeRecordConnector(peer.clone())))
    }
    fn request(n: u64) -> CasRequest {
        CasRequest {
            subject: "claim:a".into(),
            expected_version: n,
            body: vec![n as u8],
            write_id: [n as u8; 16],
        }
    }
    async fn raw(peer: &FakeRecord, method: &str, params: Value) -> Result<Value, TransportError> {
        peer.call(method, params)
            .await
            .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
    }
    fn code(error: TransportError) -> String {
        match error {
            TransportError::Refusal { code, .. } => code,
            other => panic!("unexpected {other:?}"),
        }
    }

    #[tokio::test]
    async fn record_resend_replays_before_cas_and_enforces_account_wide_id_and_window() {
        let peer = FakeRecord::new([1; 16]);
        let client = client(&peer);
        assert_eq!(client.cas(&request(0)).await.unwrap(), 1);
        assert_eq!(client.cas(&request(1)).await.unwrap(), 2);
        assert_eq!(client.cas(&request(0)).await.unwrap(), 1);
        let mut reused = request(0);
        reused.body.push(99);
        assert_eq!(client.cas(&reused).await.unwrap_err().code(), "id_reused");
        reused = request(0);
        reused.subject = "other".into();
        assert_eq!(client.cas(&reused).await.unwrap_err().code(), "id_reused");
        peer.member.store(false, Ordering::Relaxed);
        assert_eq!(
            client.cas(&request(0)).await.unwrap_err().code(),
            "not_member"
        );
        peer.member.store(true, Ordering::Relaxed);
        for n in 2..65 {
            assert_eq!(client.cas(&request(n)).await.unwrap(), n + 1);
        }
        assert_eq!(
            client.cas(&request(0)).await.unwrap_err(),
            RecordError::Refused {
                code: "version_moved".into(),
                detail: Some(json!({"current_version":65,"author":peer.author}))
            }
        );
        assert_eq!(client.cas(&request(1)).await.unwrap(), 2);
        let mut forgotten = request(0);
        forgotten.expected_version = 65;
        forgotten.body.clear();
        assert_eq!(client.cas(&forgotten).await.unwrap(), 66);
        let row = client.read("claim:a").await.unwrap();
        assert_eq!(row.body, Some(vec![]));
        assert_eq!(row.version, 66);
        assert_eq!(row.author, Some(peer.author.clone()));
        assert_eq!(row.self_author, peer.author);
    }
    #[tokio::test]
    async fn record_client_resends_lost_reply_with_identical_attempt() {
        let peer = FakeRecord::new([1; 16]);
        peer.drop_reply.store(true, Ordering::Relaxed);
        assert_eq!(client(&peer).cas(&request(0)).await.unwrap(), 1);
        assert_eq!(peer.calls.load(Ordering::Relaxed), 2);
        assert_eq!(client(&peer).read("claim:a").await.unwrap().version, 1);
    }
    #[tokio::test]
    async fn record_pending_mark_survives_restart_and_low_read_then_clears_on_higher_read() {
        let peer = FakeRecord::new([1; 16]);
        assert_eq!(client(&peer).cas(&request(0)).await.unwrap(), 1);
        peer.offline.store(true, Ordering::Relaxed);
        assert_eq!(
            client(&peer).cas(&request(1)).await.unwrap_err().code(),
            "unavailable"
        );
        let peer = peer.restart();
        let err = client(&peer).read("claim:a").await.unwrap_err();
        assert_eq!(
            err,
            RecordError::Refused {
                code: "unavailable".into(),
                detail: Some(json!({"pending_write_id":encode_hex(&[1;16])}))
            }
        );
        peer.offline.store(false, Ordering::Relaxed);
        assert_eq!(client(&peer).read("claim:a").await.unwrap().version, 1);
        peer.offline.store(true, Ordering::Relaxed);
        assert_eq!(
            client(&peer).read("claim:a").await.unwrap_err().code(),
            "unavailable"
        );
        peer.offline.store(false, Ordering::Relaxed);
        // Refusing a newer CAS restores the pending write whose offline
        // attempt was already unanswered, rather than allowing stale reads.
        assert_eq!(
            client(&peer).cas(&request(3)).await.unwrap_err().code(),
            "version_moved"
        );
        peer.offline.store(true, Ordering::Relaxed);
        assert_eq!(
            client(&peer).read("claim:a").await.unwrap_err().code(),
            "unavailable"
        );
        peer.offline.store(false, Ordering::Relaxed);
        peer.put("claim:a", 2, vec![8], &peer.author);
        assert_eq!(
            client(&peer)
                .read_many(&["claim:a".into()])
                .await
                .unwrap()
                .0[0]
                .as_ref()
                .unwrap()
                .version,
            2
        );
        peer.offline.store(true, Ordering::Relaxed);
        let row = client(&peer).read("claim:a").await.unwrap();
        assert!(row.stale);
        assert_eq!(row.version, 2);
    }
    #[tokio::test]
    async fn record_stale_copies_absent_and_floor_rules() {
        let peer = FakeRecord::new([1; 16]);
        let client = client(&peer);
        peer.offline.store(true, Ordering::Relaxed);
        let absent = client.read("new").await.unwrap();
        assert!(absent.absent && absent.stale);
        assert_eq!(absent.version, 0);
        assert!(absent.body.is_none() && absent.author.is_none());
        peer.offline.store(false, Ordering::Relaxed);
        assert_eq!(client.cas(&request(0)).await.unwrap(), 1);
        peer.offline.store(true, Ordering::Relaxed);
        assert_eq!(client.read("claim:a").await.unwrap().version, 1);
        peer.offline.store(false, Ordering::Relaxed);
        peer.cloud.lock().unwrap().rows.remove("claim:a");
        assert_eq!(
            client.read("claim:a").await.unwrap_err().code(),
            "verify_failed"
        );
        peer.put("claim:a", 2, vec![4], &peer.author);
        assert_eq!(client.read("claim:a").await.unwrap().version, 2);
        peer.put("claim:a", 1, vec![3], &peer.author);
        assert_eq!(
            client.read("claim:a").await.unwrap_err().code(),
            "verify_failed"
        );
    }
    #[tokio::test]
    async fn record_fake_and_client_enforce_admission_bounds_and_closed_refusals() {
        assert_eq!(
            (SUBJECT_CAP, BODY_CAP, MANY_CAP, PAGE_CAP),
            (256, 65_536, 64, 1_048_576)
        );
        let peer = FakeRecord::new([1; 16]);
        let client = client(&peer);
        for caller in ["direct", "reserved:other", "session:x", "flow:x"] {
            *peer.principal.lock().unwrap() = caller.into();
            assert_eq!(
                code(raw(&peer, CAS, json!({})).await.unwrap_err()),
                "caller_forbidden"
            );
        }
        *peer.principal.lock().unwrap() = "reserved:entorhinal".into();
        peer.scoped.store(true, Ordering::Relaxed);
        assert_eq!(
            code(raw(&peer, READ, json!({})).await.unwrap_err()),
            "caller_forbidden"
        );
        peer.scoped.store(false, Ordering::Relaxed);
        for (params, expected) in [
            (json!({"subject":""}), "invalid_request"),
            (json!({"subject":"a".repeat(257)}), "too_large"),
            (json!({"subject":"ok","unknown":1}), "invalid_request"),
        ] {
            assert_eq!(code(raw(&peer, READ, params).await.unwrap_err()), expected);
        }
        assert!(client.read(&"a".repeat(256)).await.unwrap().absent);
        assert_eq!(
            client.read(&"é".repeat(129)).await.unwrap_err().code(),
            "too_large"
        );
        for n in [0, 65] {
            let subjects = vec!["s".to_string(); n];
            assert_eq!(
                client.read_many(&subjects).await.unwrap_err().code(),
                "invalid_request"
            );
            assert_eq!(
                code(
                    raw(&peer, READ_MANY, json!({"subjects":subjects}))
                        .await
                        .unwrap_err()
                ),
                "invalid_request"
            );
        }
        for body in ["A0", "a", "gg"] {
            assert_eq!(code(raw(&peer,CAS,json!({"subject":"s","expected_version":0,"body":body,"write_id":encode_hex(&[1;16])})).await.unwrap_err()),"invalid_request");
        }
        let params = json!({"subject":"s","expected_version":0,"body":"00".repeat(BODY_CAP+1),"write_id":encode_hex(&[1;16])});
        assert_eq!(
            code(raw(&peer, CAS, params).await.unwrap_err()),
            "too_large"
        );
        let mut req = request(0);
        req.body = vec![0; BODY_CAP + 1];
        assert_eq!(client.cas(&req).await.unwrap_err().code(), "too_large");
        req.body = vec![0; 65_536];
        client.cas(&req).await.unwrap();
        peer.keys.store(false, Ordering::Relaxed);
        assert_eq!(
            client.read("s").await.unwrap_err().code(),
            "key_unavailable"
        );
        for code in REFUSALS {
            assert_eq!(RecordError::refusal(code, None).code(), *code);
        }
        assert_eq!(
            RecordError::refusal("epoch_moved", None).code(),
            "record_protocol"
        );
    }
    #[tokio::test]
    async fn record_many_pages_64_subjects_and_keeps_per_entry_errors() {
        let peer = FakeRecord::new([1; 16]);
        let client = client(&peer);
        let subjects: Vec<String> = (0..64).map(|n| format!("s:{n}")).collect();
        for subject in &subjects {
            peer.put(subject, 1, vec![7; BODY_CAP], &peer.author);
        }
        let (first, next) = client.read_many(&subjects).await.unwrap();
        assert_eq!(first.len(), 16);
        assert_eq!(next, Some(16));
        let (second, next) = client.read_many(&subjects[16..32]).await.unwrap();
        assert_eq!(second.len(), 16);
        assert_eq!(next, None);
        peer.error(&subjects[0], Some("verify_failed"));
        peer.error(&subjects[1], Some("decrypt_failed"));
        let (rows, next) = client.read_many(&subjects[..3]).await.unwrap();
        assert_eq!(next, None);
        assert_eq!(rows[0].as_ref().unwrap_err().code(), "verify_failed");
        assert_eq!(rows[1].as_ref().unwrap_err().code(), "decrypt_failed");
        assert_eq!(rows[2].as_ref().unwrap().version, 1);
    }
    struct Reply(Value);
    #[async_trait]
    impl LogConnector for Reply {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(Arc::new(Reply(self.0.clone())))
        }
    }
    #[async_trait]
    impl LogTransport for Reply {
        async fn call(&self, _: &str, _: Value) -> Result<Vec<u8>, TransportError> {
            Ok(serde_json::to_vec(&json!({"result":self.0})).unwrap())
        }
    }
    #[tokio::test]
    async fn record_client_refuses_malformed_pages_and_inconsistent_replies() {
        let good = wire_reply(&RecordReply {
            absent: true,
            version: 0,
            body: None,
            author: None,
            self_author: encode_hex(&[1; 16]),
            stale: false,
        });
        for page in [
            json!({"entries":[],"next":0}),
            json!({"entries":[good.clone()],"next":0}),
            json!({"entries":[good.clone(),good.clone(),good.clone()],"next":null}),
            json!({"entries":[good.clone(),good.clone()],"next":1}),
        ] {
            let c = RecordClient::new(Arc::new(Reply(page)));
            assert_eq!(
                c.read_many(&["a".into(), "b".into()])
                    .await
                    .unwrap_err()
                    .code(),
                "record_protocol"
            );
        }
        let mut bad = good;
        bad["version"] = json!(1);
        let c = RecordClient::new(Arc::new(Reply(bad)));
        assert_eq!(c.read("a").await.unwrap_err().code(), "record_protocol");
    }
}

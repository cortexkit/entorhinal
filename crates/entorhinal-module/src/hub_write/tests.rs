use super::*;
use std::{
    collections::{HashMap, VecDeque},
    path::PathBuf,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use cortexkit_claustrum_client::{
    ClaustrumCredentialResolver, CredentialGetTarget, CredentialResolverError,
    CredentialResolverFuture,
};
use cortexkit_store_types::StorageDescriptor;
use ed25519_dalek::{Signer, SigningKey};
use entorhinal_catalog::{hex, sha256_hex};

use crate::fleet_record::{SealedBody, CREDENTIAL_ID};

const BOX: &str = "00000000000000000000000000000001";
const RESIDENCE: &str = "residence.agent-1";
const REQUEST: Request<'static> = Request {
    request_key: "request",
    params_digest: "digest",
    writer: BOX,
    writer_ep: 0,
};

pub(crate) struct TestKeys {
    epoch: AtomicU64,
    signing: SigningKey,
    sealed: Mutex<Vec<Seal>>,
}

type Seal = (SealedBody, Vec<u8>, Vec<u8>);

impl TestKeys {
    pub(crate) fn new() -> Self {
        Self {
            epoch: AtomicU64::new(1),
            signing: SigningKey::from_bytes(&[42; 32]),
            sealed: Mutex::default(),
        }
    }

    fn key_version(&self) -> String {
        sha256_hex(self.signing.verifying_key().as_bytes())[..16].to_owned()
    }
}

type VaultFuture<'a> = CredentialResolverFuture<'a, Result<Value, CredentialResolverError>>;

impl CredentialGetTarget for TestKeys {
    fn credential_get<'a>(&'a self, _: &'a str, _: Option<u64>, _: bool) -> VaultFuture<'a> {
        panic!("private key read")
    }

    fn credential_public_key<'a>(&'a self, id: &'a str) -> VaultFuture<'a> {
        Box::pin(async move {
            assert_eq!(id, CREDENTIAL_ID);
            Ok(
                json!({"algorithm":"ed25519", "key_id":self.key_version(), "public_key_hex":hex(self.signing.verifying_key().as_bytes())}),
            )
        })
    }

    fn credential_sign<'a>(&'a self, id: &'a str, payload: &'a [u8]) -> VaultFuture<'a> {
        Box::pin(async move {
            assert_eq!(id, CREDENTIAL_ID);
            Ok(
                json!({"key_id":self.key_version(), "signature_b64":STANDARD.encode(self.signing.sign(payload).to_bytes())}),
            )
        })
    }
}

#[async_trait]
impl EpochVault for TestKeys {
    async fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<SealedBody, HandlerError> {
        let mut sealed = self.sealed.lock().unwrap();
        let digest = Sha256::digest(plaintext);
        let value = json!({"epoch":self.epoch.load(Ordering::Relaxed), "nonce":URL_SAFE_NO_PAD.encode(&digest[..24]), "ciphertext":URL_SAFE_NO_PAD.encode(digest)});
        sealed.push((
            serde_json::from_value(value.clone()).unwrap(),
            aad.to_vec(),
            plaintext.to_vec(),
        ));
        Ok(serde_json::from_value(value).unwrap())
    }

    async fn open(&self, aad: &[u8], body: &SealedBody) -> Result<Vec<u8>, HandlerError> {
        Ok(self
            .sealed
            .lock()
            .unwrap()
            .iter()
            .find(|(sealed, bound, _)| sealed == body && bound == aad)
            .unwrap()
            .2
            .clone())
    }
}

impl Trust for TestKeys {
    async fn writer_epoch(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(self.epoch.load(Ordering::Relaxed))
    }

    async fn verification_key(&self, _: &UnverifiedRecord) -> Result<String, HandlerError> {
        Ok(hex(self.signing.verifying_key().as_bytes()))
    }
}

#[derive(Debug)]
enum Action {
    HoldThroughDeadline,
    CommitLoseAck,
    OriginalFirst,
    ReplacementFirst,
    CompetingAppend(Vec<u8>),
}

#[derive(Default)]
struct HubState {
    logs: Vec<Vec<u8>>,
    kv: HashMap<String, KvEntry>,
    revision: u64,
    offline: bool,
    held: Option<HubAttemptWrite>,
    sends: Vec<(HubAttemptWrite, String)>,
    actions: VecDeque<Action>,
}

impl HubState {
    fn check_link(&self) -> Result<(), HandlerError> {
        if self.offline {
            Err(HandlerError::new("hub_unavailable", "test link lost"))
        } else {
            Ok(())
        }
    }

    fn commit(&mut self, write: &HubAttemptWrite) -> Result<Option<u64>, HandlerError> {
        if write.key == LOG_KEY {
            if write.expected != self.logs.len() as u64 {
                return Err(moved());
            }
            self.logs.push(write.value.clone());
            return Ok(Some(self.logs.len() as u64));
        }
        if self.kv.get(&write.key).map_or(0, |entry| entry.revision) != write.expected {
            return Err(HandlerError::new("revision_moved", "CAS refused"));
        }
        self.revision += 1;
        let value = (!write.value.is_empty()).then(|| write.value.clone());
        let ack = value.as_ref().map(|_| self.revision);
        self.kv.insert(
            write.key.clone(),
            KvEntry {
                revision: self.revision,
                value,
            },
        );
        Ok(ack)
    }
}

struct Recorder<'a> {
    store: &'a RegistryStore,
    hub: &'a Mutex<HubState>,
}

impl Transport for Recorder<'_> {
    async fn log_entry(&self, seq: u64, _: Instant) -> Result<Option<Vec<u8>>, HandlerError> {
        let hub = self.hub.lock().unwrap();
        hub.check_link()?;
        Ok(hub.logs.get((seq - 1) as usize).cloned())
    }

    async fn log_head(&self, _: Instant) -> Result<u64, HandlerError> {
        let hub = self.hub.lock().unwrap();
        hub.check_link()?;
        Ok(hub.logs.len() as u64)
    }

    async fn kv_entry(&self, key: &str, _: Instant) -> Result<Option<KvEntry>, HandlerError> {
        let hub = self.hub.lock().unwrap();
        hub.check_link()?;
        Ok(hub.kv.get(key).map(|entry| KvEntry {
            revision: entry.revision,
            value: entry.value.clone(),
        }))
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        let row = self
            .store
            .hub_attempt(&write.request_key)
            .unwrap()
            .expect("send reached hub before attempt was persisted");
        assert_eq!(&row.write, write);
        assert!(row.outcome.is_none());
        assert!(row
            .op_ids
            .iter()
            .any(|op| op.op_id == id && !op.process.is_empty()));
        {
            let mut hub = self.hub.lock().unwrap();
            hub.check_link()?;
            hub.sends.push((write.clone(), id.into()));
            match hub.actions.pop_front() {
                None => return hub.commit(write),
                Some(Action::OriginalFirst) => {
                    let old = hub.held.take().unwrap();
                    hub.commit(&old).unwrap();
                    return hub.commit(write);
                }
                Some(Action::ReplacementFirst) => {
                    let old = hub.held.take().unwrap();
                    let ack = hub.commit(write)?;
                    assert!(hub.commit(&old).is_err());
                    return Ok(ack);
                }
                Some(Action::CompetingAppend(bytes)) => {
                    hub.logs.push(bytes);
                    return hub.commit(write);
                }
                Some(Action::HoldThroughDeadline) => hub.held = Some(write.clone()),
                Some(Action::CommitLoseAck) => {
                    hub.commit(write)?;
                }
            }
            hub.offline = true;
        }
        tokio::time::sleep_until(deadline).await;
        Err(hub_transport::unsettled(id))
    }
}

struct Fixture {
    dir: PathBuf,
    descriptor: StorageDescriptor,
    keys: Arc<TestKeys>,
    signer: ClaustrumCredentialResolver,
    hub: Mutex<HubState>,
}

impl Fixture {
    fn new(name: &str) -> Self {
        let (dir, mut descriptor) = crate::tests::scratch_descriptor(name);
        descriptor.storage_namespace = dir.file_name().unwrap().to_str().unwrap().into();
        let keys = Arc::new(TestKeys::new());
        let signer = ClaustrumCredentialResolver::with_target(keys.clone(), 0);
        Self {
            dir,
            descriptor,
            keys,
            signer,
            hub: Mutex::default(),
        }
    }

    fn store(&self) -> RegistryStore {
        RegistryStore::open(&self.descriptor).unwrap()
    }

    fn recorder<'a>(&'a self, store: &'a RegistryStore) -> Recorder<'a> {
        Recorder {
            store,
            hub: &self.hub,
        }
    }

    fn writer<'a>(&'a self, transport: &'a Recorder<'a>) -> Writer<'a, Recorder<'a>, TestKeys> {
        Writer {
            store: transport.store,
            transport,
            trust: &self.keys,
            signer: &self.signer,
            vault: self.keys.as_ref(),
        }
    }

    fn action(&self, action: Action) {
        self.hub.lock().unwrap().actions.push_back(action);
    }

    fn reconnect(&self) {
        self.hub.lock().unwrap().offline = false;
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn deadline() -> Instant {
    Instant::now() + Duration::from_secs(1)
}

fn step(key: &str, expected: u64, body: Value) -> Step {
    let kind = if key == LOG_KEY {
        "agent.create"
    } else {
        key.split('.').next().unwrap()
    };
    Step {
        name: kind.into(),
        key: key.into(),
        expected,
        payload: Payload::Record {
            kind: kind.into(),
            body,
            snapshot_id: None,
        },
    }
}

fn row(store: &RegistryStore) -> HubAttempt {
    store.hub_attempt(REQUEST.request_key).unwrap().unwrap()
}

fn assert_unsettled(error: HandlerError, op_id: &str) {
    assert_eq!(error.code, "hub_write_unsettled");
    assert_eq!(error.detail, Some(json!({"op_id":op_id})));
}

#[tokio::test(start_paused = true)]
async fn hub_write_rotation_reseals_lost_ack_log_and_kv_at_same_expected() {
    for key in [LOG_KEY, RESIDENCE] {
        for original_first in [true, false] {
            let f = Fixture::new("hub-rotation");
            let store = f.store();
            let transport = f.recorder(&store);
            let writer = f.writer(&transport);
            let body = json!({"agent_id":"agent-1", "created_at_ms":42});
            let plan = step(key, 0, body.clone());
            f.action(Action::HoldThroughDeadline);
            let error = writer
                .run(&REQUEST, &plan, None, deadline())
                .await
                .unwrap_err();
            let first = row(&store);
            let old_id = first.op_ids[0].op_id.clone();
            assert_unsettled(error, &old_id);
            assert!(first.outcome.is_none());

            f.keys.epoch.store(2, Ordering::Relaxed);
            f.reconnect();
            f.action(if original_first {
                Action::OriginalFirst
            } else {
                Action::ReplacementFirst
            });
            let retry = step(key, 0, json!({"agent_id":"different", "created_at_ms":99}));
            let landed = writer
                .run(&REQUEST, &retry, None, deadline())
                .await
                .unwrap();
            let second = row(&store);
            assert_eq!(second.op_ids.len(), 2);
            assert!(second.op_ids.contains(&first.op_ids[0]));
            assert_eq!(second.write.expected, 0);
            assert_eq!(
                writer.saved(&second.write, deadline()).await.unwrap().1,
                body
            );
            let resealed = UnverifiedRecord::parse(key, &second.write.value).unwrap();
            assert_eq!(resealed.epoch(), Some(2));
            assert_ne!(resealed.header().entry_id, old_id);
            let expected_id = if original_first {
                &old_id
            } else {
                &resealed.header().entry_id
            };
            assert_eq!(&landed.op_id, expected_id);
            let outcome = second.outcome.unwrap();
            assert_eq!(&outcome.op_id, expected_id);
            assert_eq!(outcome.value, json!({"status":"landed", "position":1}));
            let hub = f.hub.lock().unwrap();
            assert_eq!(hub.sends.len(), 2);
            assert_eq!(hub.logs.len() as u64 + hub.revision, 1);
            assert_eq!(hub.sends[0].0, first.write);
            assert_eq!(hub.sends[1].0, second.write);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn hub_write_restart_second_cas_settles_superseded_without_a_write() {
    let f = Fixture::new("hub-cas-restart");
    let plan = step(RESIDENCE, 0, json!({"gen":1}));
    let op_id = {
        let store = f.store();
        let transport = f.recorder(&store);
        let writer = f.writer(&transport);
        f.action(Action::CommitLoseAck);
        let error = writer
            .run(&REQUEST, &plan, None, deadline())
            .await
            .unwrap_err();
        let mut first = row(&store);
        let op_id = first.op_ids.pop().unwrap().op_id;
        assert_unsettled(error, &op_id);
        let second = writer
            .build(
                &REQUEST,
                &step(RESIDENCE, 1, json!({"gen":2})),
                false,
                deadline(),
            )
            .await
            .unwrap()
            .0;
        assert_eq!(f.hub.lock().unwrap().commit(&second).unwrap(), Some(2));
        op_id
    };
    f.reconnect();
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    for _ in 0..2 {
        let error = writer
            .run(&REQUEST, &plan, None, deadline())
            .await
            .unwrap_err();
        assert_eq!(error.code, "hub_write_superseded");
        assert_eq!(error.detail, Some(json!({"op_id":op_id,"revision":2})));
        f.hub.lock().unwrap().offline = true;
    }
    let different = Request {
        params_digest: "different",
        ..REQUEST
    };
    assert_eq!(
        writer
            .run(&different, &plan, None, deadline())
            .await
            .unwrap_err()
            .code,
        "request_key_conflict"
    );
    assert_eq!(f.hub.lock().unwrap().sends.len(), 1);
    assert_eq!(f.hub.lock().unwrap().revision, 2);
    assert_eq!(
        row(&store).outcome.unwrap().value,
        json!({"status":"superseded","revision":2})
    );
}

#[tokio::test(start_paused = true)]
async fn hub_write_agent_create_advances_same_row_and_watch_settles_by_op_after_restart() {
    fn residence(body: &Value) -> Result<Step, HandlerError> {
        let key = format!("residence.{}", body["agent_id"].as_str().unwrap());
        Ok(step(
            &key,
            0,
            json!({"gen":1,"created_at_ms":body["created_at_ms"]}),
        ))
    }
    let f = Fixture::new("hub-create");
    let log = step(LOG_KEY, 0, json!({"agent_id":"agent-1","created_at_ms":88}));
    let initial = {
        let store = f.store();
        let transport = f.recorder(&store);
        let writer = f.writer(&transport);
        f.action(Action::CommitLoseAck);
        let error = writer
            .run(&REQUEST, &log, Some(&residence), deadline())
            .await
            .unwrap_err();
        let initial = row(&store);
        assert_unsettled(error, &initial.op_ids[0].op_id);
        assert_eq!(initial.write.key, LOG_KEY);
        assert!(initial.outcome.is_none());
        initial
    };
    let retry = step(LOG_KEY, 0, json!({"agent_id":"agent-2","created_at_ms":99}));
    f.reconnect();
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    f.action(Action::CommitLoseAck);
    let error = writer
        .run(&REQUEST, &retry, Some(&residence), deadline())
        .await
        .unwrap_err();
    let pending = row(&store);
    let id = current_op_id(&pending.write, &pending).unwrap();
    assert_unsettled(error, &id);
    assert_eq!(pending.write.key, RESIDENCE);
    assert_eq!(pending.write.step, "residence");
    assert!(!pending.write.step.starts_with(CONTINUE));
    assert!(pending.outcome.is_none());
    assert!(pending.op_ids.contains(&initial.op_ids[0]));
    assert_eq!(pending.op_ids.len(), 2);
    assert_eq!(
        writer.saved(&pending.write, deadline()).await.unwrap().1,
        json!({"gen":1,"created_at_ms":88})
    );
    let old = writer
        .verify(UnverifiedRecord::parse(LOG_KEY, &initial.write.value).unwrap())
        .await
        .unwrap();
    assert_eq!(writer.record_applied(&old, 1, deadline()).await.unwrap(), 0);
    assert_eq!(row(&store), pending);

    drop(store);
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    let seen = writer
        .verify(UnverifiedRecord::parse(RESIDENCE, &pending.write.value).unwrap())
        .await
        .unwrap();
    assert_eq!(
        writer.record_applied(&seen, 1, deadline()).await.unwrap(),
        1
    );
    assert_eq!(
        writer.record_applied(&seen, 1, deadline()).await.unwrap(),
        0
    );
    assert_eq!(
        writer
            .run(&REQUEST, &retry, Some(&residence), deadline())
            .await
            .unwrap(),
        Landed {
            op_id: id,
            position: Some(1)
        }
    );
    let hub = f.hub.lock().unwrap();
    assert_eq!(hub.sends.len(), 2);
    assert_eq!((hub.logs.len(), hub.revision), (1, 1));
}

#[tokio::test(start_paused = true)]
async fn hub_write_log_reads_exact_lost_ack_position_and_rebases_only_on_other_entry() {
    let f = Fixture::new("hub-log-rebase");
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    let body = json!({"agent_id":"agent-1","created_at_ms":42});
    let plan = step(LOG_KEY, 0, body.clone());
    let foreign = writer
        .build(
            &REQUEST,
            &step(LOG_KEY, 0, json!({"agent_id":"other"})),
            false,
            deadline(),
        )
        .await
        .unwrap()
        .0
        .value;
    f.action(Action::CompetingAppend(foreign.clone()));
    f.action(Action::CommitLoseAck);
    let error = writer
        .run(&REQUEST, &plan, None, deadline())
        .await
        .unwrap_err();
    let pending = row(&store);
    let id = current_op_id(&pending.write, &pending).unwrap();
    assert_unsettled(error, &id);
    assert_eq!(pending.op_ids.len(), 2);
    assert_eq!(pending.write.expected, 1);
    assert_eq!(
        writer.saved(&pending.write, deadline()).await.unwrap().1,
        body
    );
    f.hub.lock().unwrap().logs.push(foreign);
    f.reconnect();
    assert_eq!(
        writer.run(&REQUEST, &plan, None, deadline()).await.unwrap(),
        Landed {
            op_id: id,
            position: Some(2)
        }
    );
    let hub = f.hub.lock().unwrap();
    assert_eq!(hub.logs.len(), 3);
    assert_eq!(hub.sends.len(), 2);
    assert_ne!(hub.sends[0].1, hub.sends[1].1);
}

#[tokio::test(start_paused = true)]
async fn hub_write_snapshot_keeps_derived_id_and_matches_an_independent_resumer() {
    let f = Fixture::new("hub-snapshot");
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    let id = "0000000000000000000000000000000f";
    let plan = Step {
        name: "part-1".into(),
        key: LOG_KEY.into(),
        expected: 0,
        payload: Payload::Record {
            kind: "identity.snapshot".into(),
            body: json!({"part":1}),
            snapshot_id: Some(id.into()),
        },
    };
    f.action(Action::HoldThroughDeadline);
    assert_unsettled(
        writer
            .run(&REQUEST, &plan, None, deadline())
            .await
            .unwrap_err(),
        id,
    );
    f.keys.epoch.store(2, Ordering::Relaxed);
    f.reconnect();
    let another_writer = Request {
        writer: "00000000000000000000000000000002",
        ..REQUEST
    };
    let independent = writer
        .build(&another_writer, &plan, false, deadline())
        .await
        .unwrap()
        .0
        .value;
    f.action(Action::CompetingAppend(independent));
    assert_eq!(
        writer.run(&REQUEST, &plan, None, deadline()).await.unwrap(),
        Landed {
            op_id: id.into(),
            position: Some(1)
        }
    );
    let hub = f.hub.lock().unwrap();
    assert_eq!(hub.logs.len(), 1);
    assert_eq!(hub.sends.len(), 2);
    assert_eq!(hub.sends[0].1, hub.sends[1].1);
    assert_eq!(
        UnverifiedRecord::parse(LOG_KEY, &hub.sends[1].0.value)
            .unwrap()
            .epoch(),
        Some(2)
    );
    assert_eq!(row(&store).op_ids.len(), 1);
}

#[tokio::test(start_paused = true)]
async fn hub_write_retry_same_revision_resends_exact_bytes_and_keeps_first_send_time() {
    let f = Fixture::new("hub-resend");
    let store = f.store();
    let transport = f.recorder(&store);
    let writer = f.writer(&transport);
    let plan = step(RESIDENCE, 0, json!({"gen":1}));
    f.action(Action::HoldThroughDeadline);
    let error = writer
        .run(&REQUEST, &plan, None, deadline())
        .await
        .unwrap_err();
    let sent = row(&store).op_ids;
    assert_unsettled(error, &sent[0].op_id);
    f.reconnect();
    let landed = writer.run(&REQUEST, &plan, None, deadline()).await.unwrap();
    assert_eq!(landed.op_id, sent[0].op_id);
    assert_eq!(row(&store).op_ids, sent);
    let hub = f.hub.lock().unwrap();
    assert_eq!(hub.sends.len(), 2);
    assert_eq!(hub.sends[0], hub.sends[1]);
}

#[tokio::test(start_paused = true)]
async fn hub_write_delete_settles_conditionally_without_deleting_newer_values() {
    for replacement in [false, true] {
        let f = Fixture::new("hub-delete");
        let store = f.store();
        let transport = f.recorder(&store);
        let writer = f.writer(&transport);
        let put = step("wrapreq.box", 0, json!({"request_id":1}));
        let initial = writer
            .build(&REQUEST, &put, false, deadline())
            .await
            .unwrap()
            .0;
        assert_eq!(f.hub.lock().unwrap().commit(&initial).unwrap(), Some(1));
        let delete = Step {
            name: "delete".into(),
            expected: 1,
            key: put.key.clone(),
            payload: Payload::Delete,
        };
        f.action(Action::CommitLoseAck);
        let error = writer
            .run(&REQUEST, &delete, None, deadline())
            .await
            .unwrap_err();
        let original = row(&store);
        let id = &original.op_ids[0].op_id;
        assert_unsettled(error, id);
        if replacement {
            let next = step(&put.key, 2, json!({"request_id":2}));
            let next = writer
                .build(&REQUEST, &next, false, deadline())
                .await
                .unwrap()
                .0;
            assert_eq!(f.hub.lock().unwrap().commit(&next).unwrap(), Some(3));
        }
        f.reconnect();
        let result = writer.run(&REQUEST, &delete, None, deadline()).await;
        if replacement {
            let error = result.unwrap_err();
            assert_eq!(error.code, "hub_write_superseded");
            assert_eq!(error.detail, Some(json!({"op_id":id,"revision":3})));
            assert!(f.hub.lock().unwrap().kv[&put.key].value.is_some());
        } else {
            assert_eq!(
                result.unwrap(),
                Landed {
                    op_id: id.clone(),
                    position: Some(2)
                }
            );
        }
        assert_eq!(f.hub.lock().unwrap().sends.len(), 1);
        assert_eq!(f.hub.lock().unwrap().sends[0].0.expected, 1);
    }
}

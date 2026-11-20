use std::{path::PathBuf, sync::atomic::AtomicU64};

use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use entorhinal_core::hub_store::{HubAttemptSend, HubAttemptWrite};

use super::*;
use crate::{fleet_record::Header, hub_write::tests::TestKeys};

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ROLE_KEY: &str = "role.connector_home";
const AGENT: &str = "agent_0000000000000042";
const CREATE_ID: &str = "00000000000000000000000000000042";

struct Image;

impl SnapshotClaims for Image {
    fn claim(&self, _: &str) -> Option<(Option<String>, i64)> {
        None
    }
    fn hub_ready(&self) -> bool {
        true
    }
}

struct TestBackend {
    keys: Arc<TestKeys>,
    applied: AtomicU64,
    writes: AtomicU64,
    reply_code: &'static str,
}

#[async_trait]
impl Backend for TestBackend {
    fn membership_ready(&self) -> bool {
        true
    }
    fn current_member(&self, holder: &str, applied: u64) -> Option<u64> {
        (!self.episode_closed(holder, 0, applied)).then_some(0)
    }
    fn require_known_member(&self, _: &str, _: u64, _: u64) -> Result<(), HandlerError> {
        Ok(())
    }
    fn anchor_changes(&self) -> watch::Receiver<u64> {
        watch::channel(0).1
    }
    fn episode_closed(&self, holder: &str, episode: u64, applied: u64) -> bool {
        holder == A && episode == 0 && applied >= 2
    }
    async fn applied(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(self.applied.load(Ordering::Relaxed))
    }
    async fn writer_episode(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(0)
    }
    async fn log_head(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(self.applied.load(Ordering::Relaxed))
    }
    async fn wait_through(&self, _: u64, _: Instant) -> Result<(), HandlerError> {
        Ok(())
    }
    async fn kv_entry(
        &self,
        _: &str,
        _: Instant,
    ) -> Result<Option<hub_write::KvEntry>, HandlerError> {
        unreachable!()
    }
    async fn verify(
        &self,
        key: &str,
        bytes: &[u8],
        _: Instant,
    ) -> Result<AcceptedKv, HandlerError> {
        let record = UnverifiedRecord::parse(key, bytes)?.verify(&self.keys.public_hex())?;
        let body = serde_json::from_slice(&record.open(self.keys.as_ref()).await?).unwrap();
        let applied = self.applied.load(Ordering::Relaxed);
        Ok(AcceptedKv {
            writer_current: self.current_member(&record.header().writer, applied)
                == Some(record.header().writer_ep),
            record,
            body,
            applied,
        })
    }
    async fn record_applied(
        &self,
        _: &VerifiedRecord,
        _: u64,
        _: Instant,
    ) -> Result<(), HandlerError> {
        Ok(())
    }
    async fn write(
        &self,
        _: &hub_write::Request<'_>,
        _: &Step,
        _: Option<hub_write::NextStep<'_>>,
        _: Option<&dyn LogReplan>,
        _: Instant,
    ) -> Result<hub_write::Landed, HandlerError> {
        self.writes.fetch_add(1, Ordering::Relaxed);
        Err(HandlerError::new(self.reply_code, "test write reply"))
    }
}

struct Fixture {
    residence: Residence,
    slot: Arc<std::sync::Mutex<Option<RegistryStore>>>,
    backend: Arc<TestBackend>,
    clock: Arc<AtomicU64>,
    _leases: Arc<role_leases::Leases>,
    path: PathBuf,
}

impl Fixture {
    fn new(name: &str, reply_code: &'static str) -> Self {
        let (path, descriptor) = crate::tests::scratch_descriptor(name);
        let store = RegistryStore::open(&descriptor).unwrap();
        let backend = Arc::new(TestBackend {
            keys: Arc::new(TestKeys::new()),
            applied: AtomicU64::new(1),
            writes: AtomicU64::new(0),
            reply_code,
        });
        let clock = Arc::new(AtomicU64::new(1_000));
        let mut leases = role_leases::Leases::default();
        let lease_clock = clock.clone();
        leases.sample_clock(move || lease_clock.load(Ordering::Relaxed));
        let leases = Arc::new(leases);
        let slot = Arc::new(std::sync::Mutex::new(Some(store)));
        let residence = Residence {
            backend: backend.clone(),
            snapshot: Arc::new(Image),
            box_id: A.into(),
            base: 0,
            sql: HubSql::new(&slot, Arc::default()),
            health: Arc::new(HealthGauges::default()),
            commits: Arc::new(Notify::new()),
            watched: RwLock::default(),
            clock: || 0,
            role_leases: Arc::downgrade(&leases),
            link: Notify::new(),
            move_wake: Weak::new(),
        };
        Self {
            residence,
            slot,
            backend,
            clock,
            _leases: leases,
            path,
        }
    }

    async fn sign(&self, key: &str, kind: &str, writer: &str, id: &str, body: Value) -> Vec<u8> {
        let signer = ClaustrumCredentialResolver::with_target(self.backend.keys.clone(), 0);
        let header = Header {
            kind: kind.into(),
            key: key.into(),
            writer: writer.into(),
            writer_ep: 0,
            entry_id: id.into(),
        };
        crate::fleet_record::prepare(&signer, self.backend.keys.as_ref(), &header, &body)
            .await
            .unwrap()
            .into_bytes()
    }

    async fn role_entry(&self, holder: &str, revision: u64) -> Entry {
        let record = role_leases::Record::new(holder, 0, revision as i64, false);
        let value = self
            .sign(
                ROLE_KEY,
                "role",
                holder,
                &format!("{revision:032x}"),
                json!(record),
            )
            .await;
        Entry {
            bucket: "residence".into(),
            key: ROLE_KEY.into(),
            value: value.into(),
            revision,
            delta: 0,
            created: async_nats::datetime::DateTime::UNIX_EPOCH,
            operation: Operation::Put,
            seen_current: true,
        }
    }

    async fn applied_create(&self, step: &str) -> HubAttemptWrite {
        let mut write = HubAttemptWrite {
            request_key: "create".into(),
            params_digest: "digest".into(),
            key: LOG_KEY.into(),
            expected: 0,
            value: self
                .sign(LOG_KEY, CREATE, A, CREATE_ID, json!({"agent_id":AGENT}))
                .await,
            step: format!("{}{CREATE}", hub_write::CONTINUE),
        };
        let send = |op_id: &str| HubAttemptSend {
            op_id: op_id.into(),
            sent_at_ms: 1,
            process: "test".into(),
        };
        let store = &self.residence.sql.test_store();
        assert!(store
            .persist_hub_attempt(&write, &send(CREATE_ID), None)
            .unwrap());
        if step == CREATE_KV {
            let prior = store.hub_attempt("create").unwrap().unwrap();
            let id = "00000000000000000000000000000043";
            write.key = key(AGENT);
            write.value = self
                .sign(
                    &write.key,
                    "residence",
                    A,
                    id,
                    json!(Record::new(A, 0, 1, false)),
                )
                .await;
            write.step = CREATE_KV.into();
            assert!(store
                .advance_hub_attempt(&prior, CREATE_ID, &write, &send(id))
                .unwrap());
        }
        let payload = json!({"entry":{"row":{
            "agent_id":AGENT, "name":"Created", "name_version":1, "name_normalization_version":1,
            "tag":"test", "labels":[], "role":"assistant", "status":"active",
            "created_at_ms":1,"updated_at_ms":1,"agent_generation":1
        }}});
        store.apply_entry(CREATE, &payload.to_string(), "test", None, |tx| {
            tx.execute("UPDATE registry_journal SET entry_id=?1,log_position=1 WHERE seq=last_insert_rowid()", [CREATE_ID])?;
            tx.execute("INSERT INTO hub_log_writer(position,entry_id,box,episode) VALUES(1,?1,?2,0)", [CREATE_ID,A])?;
            Ok(())
        }).unwrap();
        assert!(store.hub_created_agent("create").unwrap().is_some());
        write
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        self.slot.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

#[tokio::test]
async fn hub_audit_regression_closed_create_episode_concludes_recovery() {
    let f = Fixture::new("closed-create-episode", "member_episode_invalid");
    let log_step = format!("{}{CREATE}", hub_write::CONTINUE);
    f.applied_create(&log_step).await;
    f.backend.applied.store(2, Ordering::Relaxed);
    assert!(
        !f.residence.recover_creates(&log_step).await.unwrap(),
        "closed create episode kept the recovery loop armed"
    );
    assert_eq!(f.backend.writes.load(Ordering::Relaxed), 0);
    let outcome = f
        .residence
        .sql
        .test_store()
        .hub_attempt("create")
        .unwrap()
        .unwrap()
        .outcome
        .unwrap();
    assert_eq!(outcome.value["code"], "residence_create_episode_closed");
}

#[tokio::test]
async fn hub_audit_regression_create_reply_keeps_applied_agent_when_claim_is_unsettled() {
    let f = Fixture::new("settled-agent-create", "hub_write_unsettled");
    f.applied_create(CREATE_KV).await;
    let saved = f
        .residence
        .sql
        .test_store()
        .hub_attempt("create")
        .unwrap()
        .unwrap();
    hub_write::refuse_attempt(
        &f.residence.sql,
        &saved,
        HandlerError::new("hub_write_unsettled", "KV history unavailable"),
        Instant::now() + BUDGET,
    )
    .await
    .unwrap();
    let bytes = f
        .residence
        .create("create", "digest", None, Instant::now() + BUDGET)
        .await
        .unwrap();
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reply["result"]["agent"]["agent_id"], AGENT);
}

#[tokio::test]
async fn hub_audit_regression_role_acquisition_held_until_expiry_or_applied_removal() {
    for removed in [false, true] {
        let f = Fixture::new("early-role-acquire", "unexpected_write");
        let deadline = Instant::now() + BUDGET;
        f.residence
            .observe(&f.role_entry(A, 1).await, deadline)
            .await
            .unwrap();
        let early = HeldDelivery {
            entry: f.role_entry(B, 2).await,
            arrival_head: None,
            waiting_for_anchor: false,
            lease_retry_at: None,
        };
        let mut pending = HashMap::new();
        f.residence.observe_or_hold(early, &mut pending).await;
        assert!(
            pending.contains_key(ROLE_KEY),
            "accepted early acquisition without expiry or a removal"
        );
        assert_eq!(
            pending[ROLE_KEY].lease_retry_at,
            Some(1_000 + role_leases::LEASE_MS)
        );
        let held = f
            .residence
            .sql
            .test_store()
            .hub_kv_cached(ROLE_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(held.revision, 2);
        assert_eq!(held.accepted_revision, Some(1));
        assert!(!held.integrity_failed);
        let at = if removed {
            f.backend.applied.store(2, Ordering::Relaxed);
            1_001
        } else {
            1_000 + role_leases::LEASE_MS
        };
        f.clock.store(at, Ordering::Relaxed);
        let early = pending.remove(ROLE_KEY).unwrap();
        f.residence.observe_or_hold(early, &mut pending).await;
        assert!(pending.is_empty());
        let cached = f
            .residence
            .sql
            .test_store()
            .hub_kv_cached(ROLE_KEY)
            .unwrap()
            .unwrap();
        assert_eq!(cached.accepted_revision, Some(2));
        assert!(!cached.integrity_failed);
    }
}

#[tokio::test]
async fn hub_audit_regression_role_acquisition_after_expiry_is_not_held() {
    let f = Fixture::new("ordinary-role-expiry", "unexpected_write");
    let deadline = Instant::now() + BUDGET;
    f.residence
        .observe(&f.role_entry(A, 1).await, deadline)
        .await
        .unwrap();
    f.clock.fetch_add(role_leases::LEASE_MS, Ordering::Relaxed);
    f.residence
        .observe(&f.role_entry(B, 2).await, deadline)
        .await
        .unwrap();
    assert_eq!(
        f.residence
            .sql
            .test_store()
            .hub_kv_cached(ROLE_KEY)
            .unwrap()
            .unwrap()
            .accepted_revision,
        Some(2)
    );
}

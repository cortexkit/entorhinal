use super::*;
use crate::{
    fleet_record::UnverifiedRecord,
    hub_identity_log::{IdentityLog, Link, LogTransport, PlannedAppend},
    hub_write::{self, tests::TestKeys, KvEntry, Replanned, Transport},
    ProjectsHandler,
};
use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{hub_store::HubAttemptWrite, RegistryStore, SharedSettlement};
use std::{future::pending, sync::Mutex as StdMutex};
use subc_client_rs::HandlerOutcome;
use tokio::sync::{oneshot, Barrier};

const A: &str = "0123456789abcdef0123456789abcdef";
const B: &str = "fedcba9876543210fedcba9876543210";
const C: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const BASE: i64 = 17;

#[tokio::test]
async fn hub_reader_regression_epoch_feed_only_receives_epoch_and_join_keys() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let leaf = rig.a.leaf.clone();
    let mut feed = Feed::epoch(leaf.health.clone(), move |_| {
        let leaf = leaf.clone();
        async move {
            Binding {
                session: leaf.session(),
                changes: None,
            }
            .epoch_watch(A)
            .await
        }
    });
    assert!(matches!(
        within(feed.next_event()).await,
        FeedEvent::Connected
    ));
    for key in [
        "role.connector_home",
        "residence.agent_0000000000000042",
        "epoch",
    ] {
        rig.kv.put(key, "record".into()).await.unwrap();
    }
    let request = format!("wrapreq.{B}");
    rig.kv.put(&request, "request".into()).await.unwrap();
    for key in [
        format!("wrap.2.mint.{B}"),
        format!("wrapjoin.{B}.request"),
        format!("wrap.2.mint.{A}"),
        format!("wrapjoin.{A}.request"),
    ] {
        rig.kv.put(&key, "wrap".into()).await.unwrap();
    }
    assert!(matches!(within(feed.next()).await, KvDelivery::CaughtUp));
    for key in [
        "epoch".into(),
        request,
        format!("wrap.2.mint.{A}"),
        format!("wrapjoin.{A}.request"),
    ] {
        assert!(matches!(within(feed.next()).await, KvDelivery::Entry(entry) if entry.key == key));
    }
}

struct Leaf {
    box_id: BoxId,
    client: async_nats::Client,
    health: Arc<HealthGauges>,
    gate: StdMutex<Option<Gate>>,
    refuse_log_kind_after: StdMutex<Option<(&'static str, usize)>>,
    sends: StdMutex<Vec<(u64, String, Vec<u8>)>>,
    reads: StdMutex<Vec<u64>>,
    requests: StdMutex<Vec<String>>,
    refuse_kv: std::sync::atomic::AtomicBool,
    epoch_gate: StdMutex<Option<(String, Gate)>>,
    head_gate: StdMutex<Option<(oneshot::Sender<()>, oneshot::Receiver<()>)>>,
}

enum Gate {
    Race(Arc<Barrier>),
    LoseAck(oneshot::Sender<()>, oneshot::Receiver<()>),
    HoldKv {
        held: oneshot::Sender<()>,
        release: oneshot::Receiver<()>,
        settled: oneshot::Sender<Result<u64, String>>,
    },
}

impl Leaf {
    fn session(&self) -> Session {
        Session::new(self.client.clone(), self.health.clone())
    }
}

impl LogTransport for Leaf {
    async fn kv_published_at(
        &self,
        key: &str,
        revision: u64,
        deadline: Instant,
    ) -> Result<Option<i64>, HandlerError> {
        self.session()
            .kv_published_at(key, revision, deadline)
            .await
    }
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        let leaf = self.clone();
        Feed::log(self.health.clone(), applied, move |cursor| {
            Binding {
                session: leaf.session(),
                changes: None,
            }
            .tail(cursor.unwrap())
        })
    }

    async fn join_keys(
        &self,
        prefix: &str,
        deadline: Instant,
    ) -> Result<Vec<String>, HandlerError> {
        self.requests.lock().unwrap().push(format!("keys:{prefix}"));
        self.session().join_keys(prefix, deadline).await
    }
}

impl Transport for Leaf {
    async fn log_entry(
        &self,
        position: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        self.reads.lock().unwrap().push(position);
        self.requests
            .lock()
            .unwrap()
            .push(format!("log:{position}"));
        self.session().log_entry(position, deadline).await
    }

    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.requests.lock().unwrap().push("head".into());
        let gate = self.head_gate.lock().unwrap().take();
        if let Some((entered, release)) = gate {
            entered.send(()).unwrap();
            let _ = release.await;
        }
        self.session().log_head(deadline).await
    }

    async fn kv_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<KvEntry>, HandlerError> {
        self.requests.lock().unwrap().push(format!("read:{key}"));
        Transport::kv_entry(&self.session(), key, deadline).await
    }

    async fn kv_fence_entry(
        &self,
        key: &str,
        deadline: Instant,
    ) -> Result<Option<KvEntry>, HandlerError> {
        self.requests.lock().unwrap().push(format!("fence:{key}"));
        self.session().kv_fence_entry(key, deadline).await
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        self.requests
            .lock()
            .unwrap()
            .push(format!("write:{}", write.key));
        if write.key != LOG_SUBJECT {
            if self.refuse_kv.load(Ordering::Acquire) {
                return Err(unavailable("fixture stopped residence before send"));
            }
            if write.value.is_empty() {
                self.session()
                    .delete(&write.key, write.expected, id, deadline)
                    .await?;
                return Ok(None);
            }
            let gate = {
                let mut configured = self.epoch_gate.lock().unwrap();
                if configured.as_ref().is_some_and(|(key, _)| {
                    write.key.starts_with(key) || write.request_key.starts_with(key)
                }) {
                    configured.take().map(|(_, gate)| gate)
                } else {
                    None
                }
            };
            if let Some(Gate::Race(barrier)) = &gate {
                barrier.wait().await;
            }
            if let Some(Gate::HoldKv {
                held,
                release,
                settled,
            }) = gate
            {
                let (reply, waiting) = oneshot::channel();
                let session = self.session();
                let write = write.clone();
                let id = id.to_owned();
                tokio::spawn(async move {
                    held.send(()).unwrap();
                    release.await.unwrap();
                    let outcome = session
                        .update(
                            &write.key,
                            write.value,
                            write.expected,
                            &id,
                            Instant::now() + Duration::from_secs(5),
                        )
                        .await;
                    settled
                        .send(outcome.as_ref().copied().map_err(|e| e.code.clone()))
                        .unwrap();
                    let _ = reply.send(outcome.map(Some));
                });
                return waiting.await.unwrap();
            }
            let revision = self
                .session()
                .update(
                    &write.key,
                    write.value.clone(),
                    write.expected,
                    id,
                    deadline,
                )
                .await?;
            if let Some(Gate::LoseAck(committed, release)) = gate {
                committed.send(()).unwrap();
                release.await.unwrap();
            }
            return Ok(Some(revision));
        }
        let record = UnverifiedRecord::parse(LOG_SUBJECT, &write.value).unwrap();
        assert_eq!(record.header().writer, self.box_id.to_string());
        assert_eq!(record.header().entry_id, id);
        if let Some((kind, remaining)) = self.refuse_log_kind_after.lock().unwrap().as_mut() {
            if record.header().kind == *kind {
                if *remaining == 0 {
                    return Err(HandlerError::new(
                        "hub_write_refused",
                        "fixture refused the next entry",
                    ));
                }
                *remaining -= 1;
            }
        }
        self.sends
            .lock()
            .unwrap()
            .push((write.expected, id.into(), write.value.clone()));
        let gate = self.gate.lock().unwrap().take();
        if let Some(Gate::Race(barrier)) = &gate {
            barrier.wait().await;
        }
        let position = self
            .session()
            .append(write.value.clone(), write.expected, id, deadline)
            .await?;
        if let Some(Gate::LoseAck(committed, release)) = gate {
            // The real hub committed the publish. Withhold its acknowledgement
            // until the other computer has appended at the next position.
            committed.send(()).unwrap();
            release.await.unwrap();
            return Err(unsettled(id));
        }
        Ok(Some(position))
    }
}

#[async_trait::async_trait]
impl crate::agent_reads::FeedClock for FrozenFeedClock {
    async fn sleep(&self, _: Duration) {
        pending::<()>().await;
    }
}

struct FrozenFeedClock;

async fn leaf(
    rig: &mut Rig,
    hub_port: u16,
    name: &str,
    id: &str,
    health: Arc<HealthGauges>,
) -> Arc<Leaf> {
    let prefix = format!("ckdel.{id}.entorhinal");
    let ports = rig
        .start(
            name,
            &format!(
                r#"
        server_name: {name}
        leafnodes {{ remotes: [{{ url: "nats-leaf://127.0.0.1:{hub_port}" }}] }}
        authorization {{ users: [{{ user: test, pass: test, permissions: {{
            publish: ["$JS.hub.API.>", "ck.fleet.idlog", "$JS.FC.>"],
            subscribe: ["{prefix}.>"]
        }} }}] }}
    "#
            ),
            false,
        )
        .await;
    let client = async_nats::ConnectOptions::new()
        .user_and_password("test".into(), "test".into())
        .custom_inbox_prefix(prefix)
        .connect(format!("127.0.0.1:{}", ports.client))
        .await
        .unwrap();
    let leaf = Arc::new(Leaf {
        box_id: id.parse().unwrap(),
        client,
        health,
        gate: StdMutex::default(),
        refuse_log_kind_after: StdMutex::default(),
        sends: StdMutex::default(),
        reads: StdMutex::default(),
        head_gate: StdMutex::default(),
        requests: StdMutex::default(),
        refuse_kv: std::sync::atomic::AtomicBool::new(false),
        epoch_gate: StdMutex::default(),
    });
    within(async {
        while leaf.session().log().await.is_err() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    leaf
}

fn store(rig: &Rig, name: &str) -> RegistryStore {
    RegistryStore::open(&store_descriptor(rig, name)).unwrap()
}

fn store_descriptor(rig: &Rig, name: &str) -> StorageDescriptor {
    let dir = rig.dir.join(name);
    fs::create_dir_all(&dir).unwrap();
    StorageDescriptor {
        module_id: crate::MODULE_ID.into(),
        storage_namespace: format!("{}-{name}", rig.dir.file_name().unwrap().to_string_lossy()),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: dir.join("store.db").to_str().unwrap().into(),
        },
    }
}

fn handler(store: RegistryStore, id: &str) -> ProjectsHandler {
    store.apply_entry("agent.cutover", "{}", "test", None, |tx| {
        tx.execute("UPDATE identity_log_state SET state='enabled',hub_log_base=?1,last_applied_position=?1,last_seen_head=?1", [BASE])?;
        Ok(())
    }).unwrap();
    let mut handler = ProjectsHandler::with_runtime(id.into(), || 700);
    handler.feed_clock = Arc::new(FrozenFeedClock);
    *handler.store.lock().unwrap() = Some(store);
    handler
}

fn activate(
    handler: &ProjectsHandler,
    leaf: Arc<Leaf>,
    keys: &Arc<TestKeys>,
) -> Arc<IdentityLog<Leaf, TestKeys>> {
    Arc::new(
        handler
            .activate_hub_identity_log(
                leaf.box_id.clone(),
                Link {
                    transport: leaf,
                    trust: keys.clone(),
                    signer: Arc::new(ClaustrumCredentialResolver::with_target(keys.clone(), 0)),
                    vault: keys.clone(),
                    peers: Arc::new(crate::hub_membership::tests::Roster(
                        [A, B, C]
                            .map(|id| crate::hub_membership::tests::anchor(id, keys))
                            .to_vec(),
                    )),
                },
            )
            .unwrap(),
    )
}

fn body(method: &str, params: Value) -> Vec<u8> {
    serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
}

fn result(outcome: HandlerOutcome) -> Value {
    let HandlerOutcome::Response(bytes) = outcome else {
        panic!("identity read did not succeed: {outcome:?}")
    };
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
}

async fn through(handler: &ProjectsHandler, sequence: u64) {
    let position = BASE + sequence as i64;
    loop {
        let notified = handler.commits.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        if handler
            .with_store(RegistryStore::identity_log_status)
            .unwrap()
            .last_applied_position
            >= position
        {
            return;
        }
        notified.await;
    }
}

#[derive(Clone)]
struct AgentOperation {
    method: &'static str,
    params: Value,
    time: i64,
    new_id: Option<String>,
}

impl AgentOperation {
    fn create(index: u64, name: String) -> Self {
        Self {
            method: "agent.create",
            params: json!({"name":name,"role":"assistant","tag":"test","request_key":format!("request-{index}")}),
            time: 700 + index as i64,
            new_id: Some(format!("agent_{index:016x}")),
        }
    }

    fn key(&self) -> &str {
        self.params["request_key"].as_str().unwrap()
    }

    fn preview(
        &self,
        store: &RegistryStore,
        time: i64,
        id: Option<&str>,
    ) -> Result<Value, HandlerError> {
        // Run the real mutation against this replica's store. Capture its
        // serialized log entry, then roll back without committing or publishing.
        let captured = Arc::new(StdMutex::new(None));
        let slot = captured.clone();
        let head = store.identity_log_status()?.last_applied_position;
        let response = store.shared_attempt(
            format!("test-preview-{}", self.key()),
            head,
            move |bytes| {
                *slot.lock().unwrap() = Some(bytes);
                SharedSettlement::Rollback {
                    sent: false,
                    code: "test_preview".into(),
                    message: "capture without committing".into(),
                }
            },
            || {
                store
                    .with_principal("test")
                    .agent_mutation_with_id(self.method, self.params.clone(), time, id)
                    .map_err(HandlerError::from)
            },
        );
        let error = response.expect_err("preview must not commit");
        if error.code != "test_preview" {
            return Err(error);
        }
        let bytes = captured.lock().unwrap().take().unwrap();
        Ok(serde_json::from_slice(&bytes).unwrap())
    }

    fn plan<'a>(&'a self, node: &'a Node) -> Invocation<'a> {
        let expected = node
            .log
            .sql
            .test_store()
            .identity_log_status()
            .map(|status| {
                crate::hub_identity_log::hub_sequence(node.log.base, status.last_applied_position)
                    .unwrap()
            })
            .unwrap();
        let store = node.handler.with_store(|store| Ok(store.clone())).unwrap();
        let body = self
            .preview(&store, self.time, self.new_id.as_deref())
            .unwrap();
        Invocation {
            operation: self,
            expected,
            body,
            log: &node.log,
        }
    }
}

struct Invocation<'a> {
    operation: &'a AgentOperation,
    expected: u64,
    body: Value,
    log: &'a IdentityLog<Leaf, TestKeys>,
}

impl Invocation<'_> {
    async fn append(&self) -> Result<hub_write::Landed, HandlerError> {
        self.append_before(Instant::now() + Duration::from_secs(5))
            .await
    }

    async fn append_before(&self, deadline: Instant) -> Result<hub_write::Landed, HandlerError> {
        let operation = self.operation.clone();
        let request_key = operation.key().to_owned();
        let digest = hub_write::params_digest(operation.method, &operation.params).unwrap();
        self.log
            .append(
                PlannedAppend {
                    request_key: &request_key,
                    params_digest: &digest,
                    writer_ep: 0,
                    expected: self.expected,
                    body: self.body.clone(),
                },
                move |store, saved| {
                    let time = saved["row"]["updated_at_ms"].as_i64().unwrap();
                    let id = operation
                        .new_id
                        .as_ref()
                        .map(|_| saved["agent_id"].as_str().unwrap());
                    match operation.preview(store, time, id) {
                        Ok(body) => Ok(Replanned::Body(body)),
                        Err(error) if error.code == "name_conflict" => {
                            Ok(Replanned::Refused(error))
                        }
                        Err(error) => Err(error),
                    }
                },
                deadline,
            )
            .await
    }
}

struct Node {
    handler: ProjectsHandler,
    leaf: Arc<Leaf>,
    log: Arc<IdentityLog<Leaf, TestKeys>>,
}

struct IdentityRig {
    a: Node,
    b: Node,
    keys: Arc<TestKeys>,
    stream: jetstream::stream::Stream,
    kv: kv::Store,
    hub_port: u16,
    leaf_port: u16,
    _processes: Rig,
}

impl IdentityRig {
    fn arm_race(&self) {
        let race = Arc::new(Barrier::new(2));
        for leaf in [&self.a.leaf, &self.b.leaf] {
            *leaf.gate.lock().unwrap() = Some(Gate::Race(race.clone()));
        }
    }

    async fn new() -> Option<Self> {
        match Command::new("nats-server").arg("-v").output() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                eprintln!("nats-server not installed; skipping live hub identity-log rig");
                return None;
            }
            result => {
                let version = result.unwrap();
                assert!(version.status.success());
                eprintln!(
                    "identity-log rig: {}",
                    String::from_utf8_lossy(&version.stdout).trim()
                );
            }
        }
        let mut rig = Rig::new();
        let hub_ports = rig
            .start(
                "hub",
                &format!(
                    r#"
        server_name: hub
        jetstream {{ domain: hub, store_dir: {:?} }}
        leafnodes {{ listen: "127.0.0.1:-1", no_advertise: true }}
    "#,
                    rig.dir.join("log").to_string_lossy()
                ),
                true,
            )
            .await;
        let admin = jetstream::new(
            async_nats::connect(format!("127.0.0.1:{}", hub_ports.client))
                .await
                .unwrap(),
        );
        let stream = admin
            .create_stream(jetstream::stream::Config {
                name: LOG_STREAM.into(),
                subjects: vec![LOG_SUBJECT.into()],
                allow_direct: true,
                duplicate_window: Duration::from_secs(120),
                ..Default::default()
            })
            .await
            .unwrap();
        let kv = admin
            .create_key_value(kv::Config {
                bucket: BUCKET.into(),
                history: 1,
                max_value_size: MAX_KV_VALUE,
                max_bytes: MAX_KV_BYTES,
                ..Default::default()
            })
            .await
            .unwrap();

        let a = handler(store(&rig, "box-a"), A);
        let b = handler(store(&rig, "box-b"), B);
        let leaf_port = hub_ports.leaf.unwrap();
        let leaf_a = leaf(&mut rig, leaf_port, "leaf-a", A, a.health.clone()).await;
        let leaf_b = leaf(&mut rig, leaf_port, "leaf-b", B, b.health.clone()).await;
        let keys = Arc::new(TestKeys::new());
        let log_a = activate(&a, leaf_a.clone(), &keys);
        let log_b = activate(&b, leaf_b.clone(), &keys);
        // Finish the empty-log startup scan before recording exact reads spent
        // on contention and lost acknowledgments below.
        within(async {
            while !log_a.membership_ready() || !log_b.membership_ready() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        Some(Self {
            a: Node {
                handler: a,
                leaf: leaf_a,
                log: log_a,
            },
            b: Node {
                handler: b,
                leaf: leaf_b,
                log: log_b,
            },
            keys,
            stream,
            kv,
            hub_port: hub_ports.client,
            leaf_port,
            _processes: rig,
        })
    }
}

mod epochs;
mod residence;

#[tokio::test(flavor = "current_thread")]
async fn hub_tail_sql_does_not_delay_runtime_timer() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let node = &rig.a;
    let operation = AgentOperation::create(42, "Timer".into());
    let invocation = operation.plan(node);
    let entry_id = "00000000000000000000000000000042";
    let signer = ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0);
    let bytes = crate::fleet_record::prepare(
        &signer,
        rig.keys.as_ref(),
        &crate::fleet_record::Header {
            kind: operation.method.into(),
            key: LOG_SUBJECT.into(),
            writer: A.into(),
            writer_ep: 0,
            entry_id: entry_id.into(),
        },
        &invocation.body,
    )
    .await
    .unwrap()
    .into_bytes();

    let path = rig._processes.dir.join("box-a/store.db");
    let (locked, waiting) = oneshot::channel();
    let (release, released) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        locked.send(()).unwrap();
        // Always release, even if an inline SQL apply stalls the runtime.
        let _ = released.recv_timeout(Duration::from_millis(750));
        conn.execute_batch("ROLLBACK").unwrap();
    });
    waiting.await.unwrap();
    let (read, read_started) = oneshot::channel();
    let read = StdMutex::new(Some(read));
    let store = node.handler.with_store(|store| Ok(store.clone())).unwrap();
    store.set_read_hook(Some(Arc::new(move || {
        if let Some(read) = read.lock().unwrap().take() {
            let _ = read.send(Instant::now());
        }
    })));
    node.leaf
        .session()
        .append(bytes, 0, entry_id, Instant::now() + Duration::from_secs(5))
        .await
        .unwrap();
    let started = within(read_started).await.unwrap();
    tokio::time::sleep_until(started + Duration::from_millis(50)).await;
    let elapsed = started.elapsed();
    let _ = release.send(());
    writer.join().unwrap();
    store.set_read_hook(None);
    assert!(
        elapsed < Duration::from_millis(300),
        "50 ms timer took {elapsed:?} while the hub tail applied an entry"
    );
    within(through(&node.handler, 1)).await;
}

#[tokio::test(flavor = "current_thread")]
async fn hub_tail_network_wait_does_not_keep_handler_store_open() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let node = handler(store(&rig._processes, "drop-during-head"), A);
    let descriptor = store_descriptor(&rig._processes, "drop-during-head");
    let (entered, waiting) = oneshot::channel();
    let (release, released) = oneshot::channel();
    *rig.a.leaf.head_gate.lock().unwrap() = Some((entered, released));
    let log = activate(&node, rig.a.leaf.clone(), &rig.keys);
    within(waiting).await.unwrap();
    drop(node);
    // No yield between drop and open: an abort has not yet dropped the tail future.
    let reopened = RegistryStore::open(&descriptor)
        .expect("the parked hub tail must not hold the writer lease");
    drop(release);
    drop(log);
    drop(reopened);
}

#[tokio::test]
async fn hub_identity_log_two_leaf_rig_applies_and_wakes_then_rebases_and_settles_lost_ack() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let (a, b, leaf_a, leaf_b, keys) = (
        &rig.a.handler,
        &rig.b.handler,
        &rig.a.leaf,
        &rig.b.leaf,
        &rig.keys,
    );
    let operations = (1..=5)
        .map(|index| AgentOperation::create(index, format!("Agent-{index}")))
        .collect::<Vec<_>>();

    let cursor = b.with_store(RegistryStore::generation).unwrap();
    let changes = body(
        "agent.changes",
        json!({"cursor":cursor,"incarnation":b.incarnation}),
    );
    let mut waiter1 = std::pin::pin!(b.handle_request_wait(&changes, (41, 1)));
    let mut waiter2 = std::pin::pin!(b.handle_request_wait(&changes, (42, 1)));
    assert!(futures_util::poll!(waiter1.as_mut()).is_pending());
    assert!(futures_util::poll!(waiter2.as_mut()).is_pending());
    let first_plan = operations[0].plan(&rig.a);
    let landed = first_plan.append().await.unwrap();
    assert_eq!(landed.position, Some(1));
    let first = rig.stream.direct_get(1).await.unwrap();
    assert_eq!(first.subject.as_str(), LOG_SUBJECT);
    assert_eq!(
        first
            .headers
            .get(NATS_EXPECTED_LAST_SEQUENCE)
            .unwrap()
            .as_str(),
        "0"
    );
    assert_eq!(
        first.headers.get(NATS_MESSAGE_ID).unwrap().as_str(),
        landed.op_id
    );
    let (one, two) = within(async { tokio::join!(waiter1, waiter2) }).await;
    let page = result(one);
    assert_eq!(page, result(two));
    assert_eq!(page["entries"].as_array().unwrap().len(), 1);
    assert_eq!(page["entries"][0]["agent_id"], first_plan.body["agent_id"]);
    assert_eq!(page["entries"][0]["row"]["created_at_ms"], 701);
    assert_eq!(
        b.with_store(|store| store.journal_log_position(&landed.op_id))
            .unwrap(),
        Some(BASE + 1)
    );
    assert_eq!(
        result(b.handle_request(&body("agent.fleet_identity", json!({})), (41, 1)))["logPosition"],
        BASE + 1
    );
    within(through(a, 1)).await;

    let left_plan = operations[1].plan(&rig.a);
    let right_plan = operations[2].plan(&rig.b);
    rig.arm_race();
    let (left, right) =
        within(async { tokio::join!(left_plan.append(), right_plan.append()) }).await;
    let (left, right) = (left.unwrap(), right.unwrap());
    let mut positions = [left.position.unwrap(), right.position.unwrap()];
    positions.sort();
    assert_eq!(positions, [2, 3]);
    let (loser, plan) = if left.position == Some(3) {
        (leaf_a, left_plan.body)
    } else {
        (leaf_b, right_plan.body)
    };
    let sends: Vec<_> = loser
        .sends
        .lock()
        .unwrap()
        .iter()
        .filter(|send| send.0 > 0)
        .cloned()
        .collect();
    assert_eq!(sends.iter().map(|send| send.0).collect::<Vec<_>>(), [1, 2]);
    assert_ne!(sends[0].1, sends[1].1);
    for (i, (_, _, bytes)) in sends.into_iter().enumerate() {
        let signed = UnverifiedRecord::parse(LOG_SUBJECT, &bytes).unwrap();
        let key = hub_write::Trust::verification_key(keys.as_ref(), &signed)
            .await
            .unwrap();
        let plaintext = signed
            .verify(&key)
            .unwrap()
            .open(keys.as_ref())
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&plaintext).unwrap();
        assert_eq!(body["agent_id"], plan["agent_id"]);
        assert_eq!(body["row"]["created_at_ms"], plan["row"]["created_at_ms"]);
        if i == 0 {
            assert_eq!(body, plan);
        } else {
            assert_ne!(body["claims"][0]["claim_id"], plan["claims"][0]["claim_id"]);
        }
    }
    assert!(leaf_a.reads.lock().unwrap().is_empty() && leaf_b.reads.lock().unwrap().is_empty());
    within(async { tokio::join!(through(a, 3), through(b, 3)) }).await;

    let (committed_tx, committed_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *leaf_a.gate.lock().unwrap() = Some(Gate::LoseAck(committed_tx, release_rx));
    let lost_plan = operations[3].plan(&rig.a);
    let (lost, later) = within(async {
        tokio::join!(lost_plan.append(), async {
            committed_rx.await.unwrap();
            tokio::join!(through(a, 4), through(b, 4));
            // With its ACK still withheld, only the tail can settle A's row.
            // Wait for that settlement separately from the journal apply.
            while a
                .with_store(|store| store.hub_attempt("request-4"))
                .unwrap()
                .unwrap()
                .outcome
                .is_none()
            {
                sleep(Duration::from_millis(10)).await;
            }
            let later = operations[4].plan(&rig.b).append().await.unwrap();
            release_tx.send(()).unwrap();
            later
        })
    })
    .await;
    let lost = lost.unwrap();
    assert_eq!((lost.position, later.position), (Some(4), Some(5)));
    assert_eq!(*leaf_a.reads.lock().unwrap(), [4]);
    assert_eq!(lost_plan.append().await.unwrap(), lost);
    let info = rig.stream.info().await.unwrap();
    assert_eq!((info.state.messages, info.state.last_sequence), (5, 5));
    within(async { tokio::join!(through(a, 5), through(b, 5)) }).await;
    for handler in [a, b] {
        let feed = handler
            .with_store(|store| Ok(store.agent_changes(0, None)?))
            .unwrap();
        let mut ids = feed
            .entries
            .into_iter()
            .map(|entry| entry.agent_id)
            .collect::<Vec<_>>();
        ids.sort();
        assert_eq!(
            ids,
            (1..=5)
                .map(|i| format!("agent_{i:016x}"))
                .collect::<Vec<_>>()
        );
        assert_eq!(handler.health.log_applied.load(Ordering::Relaxed), BASE + 5);
    }
}

#[tokio::test]
async fn hub_identity_log_replans_same_name_create_as_terminal_name_conflict() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let left = AgentOperation::create(1, "One Name".into());
    let right = AgentOperation::create(2, "One Name".into());
    let left_plan = left.plan(&rig.a);
    let right_plan = right.plan(&rig.b);
    assert_eq!(left_plan.expected, right_plan.expected);
    rig.arm_race();
    let (left_result, right_result) =
        within(async { tokio::join!(left_plan.append(), right_plan.append()) }).await;
    let (winner, loser, refusal, loser_node) = match (left_result, right_result) {
        (Ok(_), Err(refusal)) => (&left, &right_plan, refusal, &rig.b),
        (Err(refusal), Ok(_)) => (&right, &left_plan, refusal, &rig.a),
        results => panic!("expected exactly one landed create and one name_conflict: {results:?}"),
    };
    assert_eq!(refusal.code, "name_conflict", "{refusal:?}");
    assert_eq!(
        refusal.message,
        "name 'One Name' is claimed in assistant/global"
    );
    let row = loser_node
        .handler
        .with_store(|store| store.hub_attempt(loser.operation.key()))
        .unwrap()
        .unwrap();
    assert_eq!(row.op_ids.len(), 1);
    assert!(
        row.outcome.is_some(),
        "the validation refusal must close the attempt"
    );
    let replay = loser.append().await.unwrap_err();
    assert_eq!(
        (replay.code, replay.message, replay.detail),
        (refusal.code, refusal.message, refusal.detail)
    );
    for node in [&rig.a, &rig.b] {
        assert_eq!(node.leaf.sends.lock().unwrap().len(), 1);
        within(through(&node.handler, 1)).await;
        let snapshot = node
            .handler
            .with_store(|store| Ok(store.agent_snapshot()?))
            .unwrap();
        assert_eq!(snapshot.agents.len(), 1);
        assert_eq!(
            snapshot.agents[0].agent_id,
            winner.new_id.as_deref().unwrap()
        );
        assert_eq!(snapshot.claims.len(), 1);
    }
    let info = rig.stream.info().await.unwrap();
    assert_eq!((info.state.messages, info.state.last_sequence), (1, 1));
}

#[tokio::test]
async fn hub_identity_log_replans_concurrent_edits_to_keep_both_agent_fields() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let create = AgentOperation::create(1, "Edited Agent".into());
    let id = create.new_id.as_deref().unwrap();
    assert_eq!(
        create.plan(&rig.a).append().await.unwrap().position,
        Some(1)
    );
    within(async { tokio::join!(through(&rig.a.handler, 1), through(&rig.b.handler, 1)) }).await;
    let tag = AgentOperation {
        method: "agent.update_tag",
        params: json!({"agent_id":id,"tag":"new tag","request_key":"tag"}),
        time: 710,
        new_id: None,
    };
    let labels = AgentOperation {
        method: "agent.set_labels",
        params: json!({"agent_id":id,"labels":["new label"],"request_key":"labels"}),
        time: 711,
        new_id: None,
    };
    let tag = tag.plan(&rig.a);
    let labels = labels.plan(&rig.b);
    assert_eq!(tag.body["row"]["labels"], json!([]));
    assert_eq!(labels.body["row"]["tag"], "test");
    rig.arm_race();
    let (tag, labels) = within(async { tokio::join!(tag.append(), labels.append()) }).await;
    let mut positions = [
        tag.unwrap().position.unwrap(),
        labels.unwrap().position.unwrap(),
    ];
    positions.sort();
    assert_eq!(positions, [2, 3]);
    for node in [&rig.a, &rig.b] {
        within(through(&node.handler, 3)).await;
        let snapshot = node
            .handler
            .with_store(|store| Ok(store.agent_snapshot()?))
            .unwrap();
        assert_eq!(snapshot.agents.len(), 1);
        let agent = &snapshot.agents[0];
        assert_eq!(agent.tag, "new tag");
        assert_eq!(agent.labels, ["new label"]);
        assert_eq!(agent.agent_generation, 3);
    }
}

#[tokio::test]
async fn hub_identity_log_rebase_deadline_leaves_attempt_open_without_a_second_send() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let a = &rig.a;
    let catch_up = a.handler.hub_tail_task.lock().unwrap().take().unwrap();
    catch_up.abort();
    assert!(catch_up.await.unwrap_err().is_cancelled());

    let competing = AgentOperation::create(1, "Landed on B".into());
    let stale = AgentOperation::create(2, "Stale on A".into());
    assert_eq!(
        competing.plan(&rig.b).append().await.unwrap().position,
        Some(1)
    );
    let stale = stale.plan(a);
    assert_eq!(stale.expected, 0);
    let error = stale
        .append_before(Instant::now() + Duration::from_secs(1))
        .await
        .unwrap_err();
    assert_eq!(error.code, "hub_unavailable", "{error:?}");
    assert_eq!(error.message, "hub tail did not apply through sequence 1");
    assert_eq!(a.leaf.sends.lock().unwrap().len(), 1);
    let row = a
        .handler
        .with_store(|store| store.hub_attempt(stale.operation.key()))
        .unwrap()
        .unwrap();
    assert_eq!(row.write.expected, 0);
    assert_eq!(row.op_ids.len(), 1);
    assert!(row.outcome.is_none());
}

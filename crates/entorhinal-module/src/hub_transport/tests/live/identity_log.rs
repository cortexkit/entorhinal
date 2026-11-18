use super::*;
use crate::{
    fleet_record::UnverifiedRecord,
    hub_identity_log::{IdentityLog, Link, LogTransport},
    hub_write::{self, tests::TestKeys, KvEntry, Transport},
    ProjectsHandler,
};
use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{hub_store::HubAttemptWrite, RegistryStore};
use std::{future::pending, sync::Mutex as StdMutex};
use subc_client_rs::HandlerOutcome;
use tokio::sync::{oneshot, Barrier};

const A: &str = "0123456789abcdef0123456789abcdef";
const B: &str = "fedcba9876543210fedcba9876543210";
const BASE: i64 = 17;

struct Leaf {
    box_id: BoxId,
    client: async_nats::Client,
    health: Arc<HealthGauges>,
    gate: StdMutex<Option<Gate>>,
    sends: StdMutex<Vec<(u64, String, Vec<u8>)>>,
    reads: StdMutex<Vec<u64>>,
}

enum Gate {
    Race(Arc<Barrier>),
    LoseAck(oneshot::Sender<()>, oneshot::Receiver<()>),
}

impl Leaf {
    fn session(&self) -> Session {
        Session::new(self.client.clone(), self.health.clone())
    }
}

impl LogTransport for Leaf {
    fn tail(self: &Arc<Self>, applied: u64) -> Feed<LogDelivery> {
        let leaf = self.clone();
        Feed::log(self.health.clone(), applied, move |cursor| {
            Binding {
                box_id: leaf.box_id.clone(),
                session: leaf.session(),
                changes: None,
            }
            .tail(cursor.unwrap())
        })
    }
}

impl Transport for Leaf {
    async fn log_entry(
        &self,
        position: u64,
        deadline: Instant,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        self.reads.lock().unwrap().push(position);
        self.session().log_entry(position, deadline).await
    }

    async fn log_head(&self, deadline: Instant) -> Result<u64, HandlerError> {
        self.session().log_head(deadline).await
    }

    async fn kv_entry(&self, _: &str, _: Instant) -> Result<Option<KvEntry>, HandlerError> {
        unreachable!("identity log never reads KV")
    }

    async fn send(
        &self,
        write: &HubAttemptWrite,
        id: &str,
        deadline: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        assert_eq!(write.key, LOG_SUBJECT);
        let record = UnverifiedRecord::parse(LOG_SUBJECT, &write.value).unwrap();
        assert_eq!(record.header().writer, self.box_id.to_string());
        assert_eq!(record.header().entry_id, id);
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
    let listen = port();
    let prefix = format!("ckdel.{id}.entorhinal");
    rig.start(
        name,
        &format!(
            r#"
        listen: 127.0.0.1:{listen}
        server_name: {name}
        leafnodes {{ remotes: [{{ url: "nats-leaf://127.0.0.1:{hub_port}" }}] }}
        authorization {{ users: [{{ user: test, pass: test, permissions: {{
            publish: ["$JS.hub.API.>", "ck.fleet.idlog", "$JS.FC.>"],
            subscribe: ["{prefix}.>"]
        }} }}] }}
    "#
        ),
        listen,
    )
    .await;
    let client = async_nats::ConnectOptions::new()
        .user_and_password("test".into(), "test".into())
        .custom_inbox_prefix(prefix)
        .connect(format!("127.0.0.1:{listen}"))
        .await
        .unwrap();
    let leaf = Arc::new(Leaf {
        box_id: id.parse().unwrap(),
        client,
        health,
        gate: StdMutex::default(),
        sends: StdMutex::default(),
        reads: StdMutex::default(),
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
    let dir = rig.dir.join(name);
    fs::create_dir_all(&dir).unwrap();
    RegistryStore::open(&StorageDescriptor {
        module_id: crate::MODULE_ID.into(),
        storage_namespace: format!("{}-{name}", rig.dir.file_name().unwrap().to_string_lossy()),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: dir.join("store.db").to_str().unwrap().into(),
        },
    })
    .unwrap()
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

fn plan(store: &RegistryStore, index: u64) -> Value {
    let before = store.generation().unwrap();
    store.with_principal("test").agent_mutation_with_id(
        "agent.create",
        json!({"name":format!("Agent-{index}"), "role":"assistant", "tag":"test", "request_key":format!("request-{index}")}),
        700 + index as i64,
        Some(&format!("agent_{index:016x}")),
    ).unwrap();
    let entries = store.agent_changes(before, None).unwrap().entries;
    assert_eq!(entries.len(), 1);
    serde_json::to_value(&entries[0]).unwrap()
}

fn activate(
    handler: &ProjectsHandler,
    leaf: Arc<Leaf>,
    keys: &Arc<TestKeys>,
) -> IdentityLog<Leaf, TestKeys> {
    handler
        .activate_hub_identity_log(
            leaf.box_id.clone(),
            Link {
                transport: leaf,
                trust: keys.clone(),
                signer: Arc::new(ClaustrumCredentialResolver::with_target(keys.clone(), 0)),
                vault: keys.clone(),
            },
        )
        .unwrap()
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

async fn append(log: &IdentityLog<Leaf, TestKeys>, body: &Value) -> hub_write::Landed {
    let request = body["row"]["request_key"].as_str().unwrap();
    let digest = hub_write::params_digest("agent.create", body).unwrap();
    log.append(
        request,
        &digest,
        0,
        body.clone(),
        Instant::now() + Duration::from_secs(5),
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn hub_identity_log_two_leaf_rig_applies_and_wakes_then_rebases_and_settles_lost_ack() {
    match Command::new("nats-server").arg("-v").output() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("nats-server not installed; skipping live hub identity-log rig");
            return;
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
    let hub = port();
    let leaf_port = port();
    rig.start(
        "hub",
        &format!(
            r#"
        listen: 127.0.0.1:{hub}
        server_name: hub
        jetstream {{ domain: hub, store_dir: {:?} }}
        leafnodes {{ listen: 127.0.0.1:{leaf_port}, no_advertise: true }}
    "#,
            rig.dir.join("log").to_string_lossy()
        ),
        hub,
    )
    .await;
    let admin = jetstream::new(
        async_nats::connect(format!("127.0.0.1:{hub}"))
            .await
            .unwrap(),
    );
    let mut stream = admin
        .create_stream(jetstream::stream::Config {
            name: LOG_STREAM.into(),
            subjects: vec![LOG_SUBJECT.into()],
            allow_direct: true,
            duplicate_window: Duration::from_secs(120),
            ..Default::default()
        })
        .await
        .unwrap();

    let a = handler(store(&rig, "box-a"), A);
    let b = handler(store(&rig, "box-b"), B);
    let leaf_a = leaf(&mut rig, leaf_port, "leaf-a", A, a.health.clone()).await;
    let leaf_b = leaf(&mut rig, leaf_port, "leaf-b", B, b.health.clone()).await;
    let keys = Arc::new(TestKeys::new());
    let log_a = activate(&a, leaf_a.clone(), &keys);
    let log_b = activate(&b, leaf_b.clone(), &keys);
    let planner = store(&rig, "planner");
    planner
        .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let plans = (1..=5)
        .map(|index| plan(&planner, index))
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
    let landed = append(&log_a, &plans[0]).await;
    assert_eq!(landed.position, Some(1));
    let first = stream.direct_get(1).await.unwrap();
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
    assert_eq!(page["entries"][0]["agent_id"], plans[0]["agent_id"]);
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
    within(through(&a, 1)).await;

    let race = Arc::new(Barrier::new(2));
    for leaf in [&leaf_a, &leaf_b] {
        *leaf.gate.lock().unwrap() = Some(Gate::Race(race.clone()));
    }
    let (left, right) =
        within(async { tokio::join!(append(&log_a, &plans[1]), append(&log_b, &plans[2])) }).await;
    let mut positions = [left.position.unwrap(), right.position.unwrap()];
    positions.sort();
    assert_eq!(positions, [2, 3]);
    let (loser, plan) = if left.position == Some(3) {
        (&leaf_a, &plans[1])
    } else {
        (&leaf_b, &plans[2])
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
    for (_, _, bytes) in sends {
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
        assert_eq!(serde_json::from_slice::<Value>(&plaintext).unwrap(), *plan);
    }
    assert!(leaf_a.reads.lock().unwrap().is_empty() && leaf_b.reads.lock().unwrap().is_empty());
    within(async { tokio::join!(through(&a, 3), through(&b, 3)) }).await;

    let (committed_tx, committed_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    *leaf_a.gate.lock().unwrap() = Some(Gate::LoseAck(committed_tx, release_rx));
    let (lost, later) = within(async {
        tokio::join!(append(&log_a, &plans[3]), async {
            committed_rx.await.unwrap();
            tokio::join!(through(&a, 4), through(&b, 4));
            // The apply hook may already have closed A's row. The writer still
            // must read its exact position, not the stream's newer last entry.
            let row = a
                .with_store(|store| store.hub_attempt("request-4"))
                .unwrap()
                .unwrap();
            assert!(row.outcome.is_some());
            let later = append(&log_b, &plans[4]).await;
            release_tx.send(()).unwrap();
            later
        })
    })
    .await;
    assert_eq!((lost.position, later.position), (Some(4), Some(5)));
    assert_eq!(*leaf_a.reads.lock().unwrap(), [4]);
    assert_eq!(append(&log_a, &plans[3]).await, lost);
    let info = stream.info().await.unwrap();
    assert_eq!((info.state.messages, info.state.last_sequence), (5, 5));
    within(async { tokio::join!(through(&a, 5), through(&b, 5)) }).await;
    for handler in [&a, &b] {
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

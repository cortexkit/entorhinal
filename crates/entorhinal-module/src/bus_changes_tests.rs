use super::*;
use crate::bus_changes::{Failure, Sink};
use crate::ModuleHandler;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, Notify};
use tokio::time::{timeout, Duration};

#[derive(Default)]
pub(super) struct Control {
    fail: AtomicBool,
    attempts: AtomicU64,
    attempted: Notify,
    block: AtomicBool,
    entered: Notify,
    release: Notify,
    reconnected: Notify,
    publish_fail: AtomicBool,
    publish_block: AtomicBool,
    publish_attempted: Notify,
    publish_release: Notify,
}

struct FakeSink {
    control: Arc<Control>,
    puts: mpsc::UnboundedSender<(String, Vec<u8>)>,
    events: mpsc::UnboundedSender<(String, Vec<u8>)>,
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn ready(&mut self) -> Result<(), Failure> {
        self.control.attempts.fetch_add(1, Ordering::Relaxed);
        self.control.attempted.notify_one();
        if self.control.fail.load(Ordering::Relaxed) {
            return Err("bucket not provisioned".to_string().into());
        }
        Ok(())
    }

    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), Failure> {
        if self.control.block.swap(false, Ordering::Relaxed) {
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        self.puts.send((key.into(), value)).unwrap();
        Ok(())
    }

    async fn connection_changed(&mut self) {
        self.control.reconnected.notified().await
    }

    async fn publish(&mut self, fact_id: &str, body: Vec<u8>) -> Result<(), Failure> {
        self.events.send((fact_id.into(), body)).unwrap();
        self.control.publish_attempted.notify_one();
        if self.control.publish_block.swap(false, Ordering::Relaxed) {
            self.control.publish_release.notified().await;
        }
        if self.control.publish_fail.load(Ordering::Relaxed) {
            return Err("EVENT unavailable".to_string().into());
        }
        Ok(())
    }
}

pub(super) struct Published {
    control: Arc<Control>,
    puts: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    events: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    idle: Arc<Notify>,
}

impl Published {
    pub(super) fn start(handler: &ProjectsHandler, control: Arc<Control>) -> Self {
        let (puts, receiver) = mpsc::unbounded_channel();
        let (events, event_receiver) = mpsc::unbounded_channel();
        let idle = handler.start_bus_changes_with_sink(FakeSink {
            control: control.clone(),
            puts,
            events,
        });
        Self {
            control,
            puts: receiver,
            events: event_receiver,
            idle,
        }
    }

    pub(super) async fn idle(&self) {
        timeout(Duration::from_secs(5), self.idle.notified())
            .await
            .expect("publisher idle");
    }

    pub(super) async fn event(&mut self) -> (String, Vec<u8>) {
        timeout(Duration::from_secs(5), self.events.recv())
            .await
            .unwrap()
            .unwrap()
    }

    async fn take(&mut self, expected: &[&str]) -> BTreeMap<String, Value> {
        let mut values = BTreeMap::new();
        for _ in expected {
            let (key, bytes) = timeout(Duration::from_secs(5), self.puts.recv())
                .await
                .unwrap()
                .unwrap();
            assert!(bytes.len() < 512, "{key}: {} bytes", bytes.len());
            assert!(values
                .insert(key, serde_json::from_slice(&bytes).unwrap())
                .is_none());
        }
        assert_eq!(
            values.keys().map(String::as_str).collect::<Vec<_>>(),
            expected
        );
        values
    }

    fn quiet(&mut self) {
        assert!(matches!(
            self.puts.try_recv(),
            Err(mpsc::error::TryRecvError::Empty)
        ));
    }
}

#[tokio::test]
async fn bus_changes_startup_publishes_zero_role_epoch_after_migration_only_on_own_store() {
    use crate::sharing_baselines::{artifacts, install_handler, rows, StoreFile, BOX_A, NOW};

    let source = artifacts().join("disabled.db");
    for (machine, foreign) in [(BOX_A, false), ("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb", true)] {
        let file = StoreFile::copy(&source);
        let conn = Connection::open(file.path()).unwrap();
        assert_eq!(
            rows(
                &conn,
                "SELECT source,epoch FROM role_cache WHERE role='connector_home'"
            ),
            vec![vec![json!("cloud"), json!(3)]]
        );
        conn.execute(
            "INSERT INTO store_origin VALUES(?1,?2)",
            params![BOX_A, NOW],
        )
        .unwrap();
        drop(conn);

        let handler = ProjectsHandler::with_runtime(machine.into(), || NOW);
        handler.health.origin.set_daemon(Some(machine.into()));
        let store = file.open();
        assert!(rows(&file.read(), "SELECT role FROM role_cache").is_empty());
        install_handler(&handler, store);
        handler.catalog_sql.wait().await;
        assert_eq!(handler.health.origin.is_foreign(), foreign);

        if foreign {
            handler.start_bus_changes();
            assert!(handler.bus_changes_task.lock().unwrap().is_none());
        } else {
            let mut published = Published::start(&handler, Arc::default());
            published.idle().await;
            let roles = std::iter::from_fn(|| published.puts.try_recv().ok())
                .filter_map(|(key, body)| (key == "role.connector_home").then_some(body))
                .collect::<Vec<_>>();
            assert_eq!(roles, vec![br#"{"epoch":0}"#.to_vec()]);
        }
        crate::tests::stop_store_tasks(&handler).await;
    }
}

fn placement_commit(
    store: &RegistryStore,
    id: &str,
    action: impl Fn() -> Result<Vec<u8>, entorhinal_core::RegistryError>,
) {
    let author = "ab".repeat(16);
    let preview = std::sync::Arc::new(std::sync::Mutex::new(None));
    let captured = preview.clone();
    store
        .preview_hub_write(
            id.into(),
            move |entry, _| *captured.lock().unwrap() = Some(entry),
            action,
        )
        .expect_err("preview rolls back before the hub entry is applied");
    let entry = entorhinal_core::remote_apply::RemoteEntry {
        position: store.identity_log_status().unwrap().last_applied_position + 1,
        entry_id: id.into(),
        author: author.clone(),
        signer: author,
        key_id: "key".into(),
        envelope_version: 1,
        kind: "change".into(),
        entry: preview.lock().unwrap().take().expect("placement preview"),
    };
    store.apply_hub_remote_entry(&entry, 1).unwrap();
}

fn backfill(f: &Fixture) {
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    store
        .apply_entry("identity_log.enable", "{}", "operator", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled', hub_log_base=0 WHERE id=1",
                [],
            )?;
            tx.execute(
                "INSERT INTO self_identity VALUES(1,?1,1)",
                ["ab".repeat(16)],
            )?;
            Ok(())
        })
        .unwrap();
    placement_commit(&store, "backfill", || {
        store
            .with_principal("test")
            .placement_backfill(Default::default(), 702)
    });
}

fn assign(f: &Fixture) {
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    placement_commit(&store, "assign", || {
        store.with_principal("test").assign_workspace_at(
            entorhinal_core::AssignWorkspaceRequest {
                project_id: "served-project".into(),
                workspace_id: "other".into(),
                ..Default::default()
            },
            703,
        )
    });
}

fn publication(f: &Fixture) -> entorhinal_core::placement::PublicationState {
    f.handler.with_store(|s| s.placement_publication()).unwrap()
}

#[tokio::test]
async fn bus_changes_placement_pos_order_and_exact_bodies() {
    let f = Fixture::new("bus-placement-order").await;
    backfill(&f);
    assign(&f);
    let facts = f
        .handler
        .with_store(|s| s.placement_read(Default::default(), INCARNATION))
        .unwrap()
        .facts;
    assert_eq!(facts.len(), 2);
    let mut published = Published::start(&f.handler, Arc::default());
    for fact in facts {
        let (id, body) = published.event().await;
        assert_eq!(id, fact.fact_id);
        assert_eq!(body, serde_json::to_vec(&fact).unwrap());
        assert!(body.len() < 4096);
        assert!(String::from_utf8(body)
            .unwrap()
            .contains("\"supersedes\":null"));
    }
    published.idle().await;
    assert_eq!(publication(&f).published, 2);
    assert_eq!(publication(&f).unpublished, 0);
    let health = f.handler.health().await.metrics.unwrap();
    assert_eq!(health["placementFactsPublished"], 2);
    assert_eq!(health["placementFactsUnpublished"], 0);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_failed_publish_retries_only_marks_after_ack() {
    let f = Fixture::new("bus-placement-retry").await;
    backfill(&f);
    let control = Arc::new(Control::default());
    control.publish_fail.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    let first = published.event().await;
    assert_eq!(publication(&f).published, 0);
    assert_eq!(publication(&f).unpublished, 1);
    published
        .control
        .publish_block
        .store(true, Ordering::Relaxed);
    published
        .control
        .publish_fail
        .store(false, Ordering::Relaxed);
    assert_eq!(published.event().await, first);
    f.handler.bus_changes_sql.wait().await;
    assert_eq!(publication(&f).published, 0);
    // No lease survives the bus await: even closing and reopening the store
    // is safe while the server acknowledgement is pending.
    f.handler.catalog_sql.wait().await;
    f.handler.store.lock().unwrap().take();
    let store = RegistryStore::open(&f.descriptor).unwrap();
    *f.handler.store.lock().unwrap() = Some(store);
    published.control.publish_release.notify_one();
    published.idle().await;
    assert_eq!(publication(&f).published, 1);
    assert_eq!(publication(&f).unpublished, 0);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_restart_does_not_republish() {
    let f = Fixture::new("bus-placement-restart").await;
    backfill(&f);
    let mut first = Published::start(&f.handler, Arc::default());
    first.event().await;
    first.idle().await;
    f.handler.stop_bus_changes().await;
    f.handler.catalog_sql.wait().await;
    f.handler.store.lock().unwrap().take();
    *f.handler.store.lock().unwrap() = Some(RegistryStore::open(&f.descriptor).unwrap());
    let mut restarted = Published::start(&f.handler, Arc::default());
    restarted.take(KEYS).await;
    restarted.idle().await;
    assert!(matches!(
        restarted.events.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    assert_eq!(publication(&f).published, 1);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_failure_keeps_markers_updating() {
    let f = Fixture::new("bus-placement-markers").await;
    backfill(&f);
    let control = Arc::new(Control::default());
    control.publish_fail.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    let initial = published.take(KEYS).await;
    published.event().await;
    assign(&f);
    f.handler.commits.notify_waiters();
    let updated = published.take(&["registry"]).await;
    assert!(
        updated["registry"]["generation"].as_i64() > initial["registry"]["generation"].as_i64()
    );
    assert_eq!(publication(&f).published, 0);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_publishes_while_kv_bucket_fails() {
    let f = Fixture::new("bus-placement-kv-failure").await;
    backfill(&f);
    let control = Arc::new(Control::default());
    control.fail.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    published.event().await;
    // The next pass observes the durable checkpoint even though KV still fails.
    timeout(Duration::from_secs(5), async {
        loop {
            if publication(&f).published == 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(publication(&f).unpublished, 0);
    published.quiet();
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_drains_backlog_while_kv_fails() {
    let f = Fixture::new("bus-placement-drain").await;
    backfill(&f);
    assign(&f);
    let control = Arc::new(Control::default());
    control.fail.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    published.event().await;
    published.event().await;
    // Both facts went out in the first pass, before any backoff retry
    // could call ready() a second time.
    assert_eq!(published.control.attempts.load(Ordering::Relaxed), 1);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_placement_remote_apply_wakes_publisher() {
    let f = Fixture::new("bus-placement-remote").await;
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    store
        .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled', hub_log_base=0",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let mut published = Published::start(&f.handler, Arc::default());
    published.take(KEYS).await;
    published.idle().await;
    let fact = entorhinal_core::placement::PlacementFact {
        fact_id: "e:remote:served-project".into(),
        supersedes: None,
        project_id: "served-project".into(),
        workspace_id: None,
        effective_from_ms: 900,
        recorded_at_ms: 900,
    };
    let mut row = serde_json::to_value(&fact).unwrap();
    row["pos"] = json!(1);
    store.apply_hub_remote_entry(&entorhinal_core::remote_apply::RemoteEntry {
        position: 1,
        entry_id: "55".repeat(16),
        author: "ab".repeat(16),
        signer: "ab".repeat(16),
        key_id: "key".into(),
        envelope_version: 1,
        kind: "change".into(),
        entry: serde_json::to_vec(&json!({"op":"project.shared","tables":{"placement_fact":{"upsert":[row],"delete":[]}}})).unwrap(),
    }, 1).unwrap();
    f.handler.commits.notify_waiters();
    drop(store);
    let (id, body) = published.event().await;
    assert_eq!(id, fact.fact_id);
    assert_eq!(body, serde_json::to_vec(&fact).unwrap());
    published.idle().await;
    assert_eq!(publication(&f).published, 1);
    f.handler.stop_bus_changes().await;
}

const KEYS: &[&str] = &[
    "agents",
    "catalog.status",
    "fleet_epoch",
    "profile",
    "registry",
    "role.connector_home",
];

#[tokio::test]
async fn fleet_epoch_current_served_records_and_marker() {
    let f = Fixture::new("fleet-epoch-current").await;
    let mut publisher = Published::start(&f.handler, Arc::default());
    let initial = publisher.take(KEYS).await;
    assert_eq!(
        initial["fleet_epoch"],
        json!({"generation":0,"incarnation":INCARNATION})
    );
    publisher.idle().await;
    let method = crate::fleet_epoch_current::METHOD;
    let (record, reply) = f.record(method, json!({})).await;
    assert_eq!(reply, json!({"result":{}}));
    published_file("fleet-epoch-current-v1", "before-hub.json", vec![record]);

    let cache = &f.handler.health.fleet_epoch;
    cache.set_current(Some(2));
    let (key, bytes) = timeout(Duration::from_secs(5), publisher.puts.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(key, "fleet_epoch");
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap(),
        json!({"generation":1,"incarnation":INCARNATION})
    );
    published_file(
        "fleet-epoch-current-v1",
        "marker-bump.json",
        vec![format!(
            "  {{\"key\":{},\"value_bytes\":{}}}",
            json!(key),
            json!(String::from_utf8(bytes).unwrap())
        )],
    );
    let (record, reply) = f.record(method, json!({})).await;
    assert_eq!(reply, json!({"result":{"current":2}}));
    published_file("fleet-epoch-current-v1", "held-unknown.json", vec![record]);

    cache.set_highest_held(Some(1));
    assert_eq!(
        publisher.take(&["fleet_epoch"]).await["fleet_epoch"]["generation"],
        2
    );
    let (record, reply) = f.record(method, json!({})).await;
    assert_eq!(reply, json!({"result":{"current":2,"highest_held":1}}));
    published_file("fleet-epoch-current-v1", "held-behind.json", vec![record]);

    cache.set_highest_held(Some(2));
    assert_eq!(
        publisher.take(&["fleet_epoch"]).await["fleet_epoch"]["generation"],
        3
    );
    let (record, reply) = f.record(method, json!({})).await;
    assert_eq!(reply, json!({"result":{"current":2,"highest_held":2}}));
    published_file("fleet-epoch-current-v1", "held-current.json", vec![record]);

    cache.set_current(None);
    assert_eq!(
        publisher.take(&["fleet_epoch"]).await["fleet_epoch"]["generation"],
        4
    );
    let (record, reply) = f.record(method, json!({})).await;
    assert_eq!(reply, json!({"result":{"highest_held":2}}));
    published_file(
        "fleet-epoch-current-v1",
        "current-unknown.json",
        vec![record],
    );

    let (record, reply) = f.record(method, json!({"unknown":true})).await;
    assert_eq!(reply["code"], "invalid_request");
    published_file("fleet-epoch-current-v1", "unknown-param.json", vec![record]);
    publisher.quiet();
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_reconnect_rewrites_every_key() {
    let f = Fixture::new("bus-reconnect").await;
    let mut published = Published::start(&f.handler, Arc::default());
    let first = published.take(KEYS).await;
    published.idle().await;
    published.quiet();
    // Nothing changed in the store, but the new connection may face an empty,
    // re-provisioned bucket, so all six markers must be written again.
    published.control.reconnected.notify_one();
    assert_eq!(published.take(KEYS).await, first);
    published.idle().await;
    published.quiet();
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_startup_write_kinds_and_unchanged_wakes() {
    let f = Fixture::new("bus-markers").await;
    let source = f.snapshot();
    seed_agent(&source, HEAD, "Ada", None, false, false);
    drop(source);
    assert!(matches!(f.import().await, HandlerOutcome::Response(_)));
    let mut published = Published::start(&f.handler, Arc::default());
    let initial = published.take(KEYS).await;
    published.idle().await;
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    assert_eq!(
        initial["registry"],
        json!({"generation":store.generation().unwrap(),"incarnation":INCARNATION})
    );
    let feed = store.agent_changes(0, None).unwrap();
    assert_eq!(
        initial["agents"],
        json!({"generation":feed.entries.last().unwrap().seq,"incarnation":INCARNATION})
    );
    assert_eq!(
        initial["profile"],
        json!({"token":store.user_get(Default::default()).unwrap()["token"]})
    );
    assert_eq!(initial["role.connector_home"], json!({"epoch":0}));
    assert_eq!(initial["catalog.status"], json!({"generation":0}));
    drop(store);

    f.success(
        DIRECT,
        "assign_workspace",
        json!({"projectId":"served-project","workspaceId":"other","requestKey":"placement"}),
    )
    .await;
    let assigned = published.take(&["registry"]).await;
    published.idle().await;
    assert!(
        assigned["registry"]["generation"].as_i64() > initial["registry"]["generation"].as_i64()
    );
    published.quiet();

    let renamed = f
        .success(
            CORE,
            "agent.rename",
            json!({"agent_id":HEAD,"name":"Ada Lovelace","request_key":"rename"}),
        )
        .await;
    let changes = published.take(&["agents", "registry"]).await;
    published.idle().await;
    assert_eq!(changes["agents"]["generation"], renamed["generation"]);
    assert_eq!(changes["registry"]["generation"], renamed["generation"]);
    published.quiet();

    f.success(
        CORE,
        "user.set",
        json!({"field":"name","value":"Mira","request_key":"profile"}),
    )
    .await;
    let changes = published.take(&["profile", "registry"]).await;
    published.idle().await;
    assert_ne!(changes["profile"], initial["profile"]);
    let reply = f.success(CORE, "user.get", json!({})).await;
    assert_eq!(changes["profile"], json!({"token":reply["token"]}));
    published.quiet();

    f.handler.watches.observe_catalog(BTreeMap::from([(
        ("@example/flow".into(), "1.0.0".into()),
        json!({"state":"revoked","blocked":true}),
    )]));
    assert_eq!(
        published.take(&["catalog.status"]).await["catalog.status"],
        json!({"generation":1})
    );
    published.idle().await;
    f.handler.commits.notify_waiters();
    published.idle().await;
    published.quiet();
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_missing_bucket_retries_and_recovers_without_blocking_reads_or_health() {
    let f = Fixture::new("bus-outage").await;
    let control = Arc::new(Control::default());
    control.fail.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    timeout(
        Duration::from_secs(5),
        published.control.attempted.notified(),
    )
    .await
    .unwrap();
    // Both outputs are attempted before recording an outage. The placement
    // read may still be on the blocking pool after the fake KV attempt wakes us.
    timeout(Duration::from_secs(1), async {
        while f
            .handler
            .health
            .bus_changes_failures
            .load(Ordering::Relaxed)
            == 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(!f
        .handler
        .health
        .bus_changes_connected
        .load(Ordering::Relaxed));
    let health = timeout(Duration::from_secs(1), f.handler.health())
        .await
        .unwrap();
    assert_eq!(health.metrics.unwrap()["busChangesConnected"], false);
    assert!(f.success(READER, "enumerate", json!({})).await["generation"].is_i64());
    assert_eq!(
        f.handler
            .health
            .bus_changes_failures
            .load(Ordering::Relaxed),
        1
    );
    published.quiet();
    published.control.fail.store(false, Ordering::Relaxed);
    published.take(KEYS).await;
    published.idle().await;
    assert_eq!(published.control.attempts.load(Ordering::Relaxed), 2);
    let health = f.handler.health().await.metrics.unwrap();
    assert_eq!(health["busChangesPuts"], 6);
    assert_eq!(health["busChangesFailures"], 1);
    assert_eq!(health["busChangesConnected"], true);
    f.handler.stop_bus_changes().await;
}

#[tokio::test]
async fn bus_changes_pending_put_releases_store_and_serializes_newer_values() {
    let f = Fixture::new("bus-pending-put").await;
    let control = Arc::new(Control::default());
    control.block.store(true, Ordering::Relaxed);
    let mut published = Published::start(&f.handler, control);
    timeout(Duration::from_secs(5), published.control.entered.notified())
        .await
        .unwrap();
    f.handler.bus_changes_sql.wait().await;
    f.handler.catalog_sql.wait().await;
    // Reopening the fenced store would fail if the bus task retained any clone
    // of it while its put was pending, even after the blocking job finished.
    f.handler.store.lock().unwrap().take();
    let store = RegistryStore::open(&f.descriptor).expect("bus await released the store lease");
    let old_generation = store.generation().unwrap();
    *f.handler.store.lock().unwrap() = Some(store);
    let health = timeout(Duration::from_secs(1), f.handler.health())
        .await
        .unwrap();
    assert_eq!(health.metrics.unwrap()["busChangesPuts"], 0);
    f.success(
        DIRECT,
        "assign_workspace",
        json!({"projectId":"served-project","workspaceId":"newer","requestKey":"newer"}),
    )
    .await;
    published.control.release.notify_one();
    let initial = published.take(KEYS).await;
    assert_eq!(initial["registry"]["generation"], old_generation);
    let latest = published.take(&["registry"]).await;
    assert!(latest["registry"]["generation"].as_i64().unwrap() > old_generation);
    published.idle().await;
    published.quiet();
    f.handler.stop_bus_changes().await;
}

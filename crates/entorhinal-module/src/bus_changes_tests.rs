use super::*;
use crate::bus_changes::Sink;
use crate::ModuleHandler;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use tokio::sync::{mpsc, Notify};
use tokio::time::{timeout, Duration};

#[derive(Default)]
struct Control {
    fail: AtomicBool,
    attempts: AtomicU64,
    attempted: Notify,
    block: AtomicBool,
    entered: Notify,
    release: Notify,
}

struct FakeSink {
    control: Arc<Control>,
    puts: mpsc::UnboundedSender<(String, Vec<u8>)>,
}

#[async_trait::async_trait]
impl Sink for FakeSink {
    async fn ready(&mut self) -> Result<(), String> {
        self.control.attempts.fetch_add(1, Ordering::Relaxed);
        self.control.attempted.notify_one();
        if self.control.fail.load(Ordering::Relaxed) {
            return Err("bucket not provisioned".into());
        }
        Ok(())
    }

    async fn put(&mut self, key: &str, value: Vec<u8>) -> Result<(), String> {
        if self.control.block.swap(false, Ordering::Relaxed) {
            self.control.entered.notify_one();
            self.control.release.notified().await;
        }
        self.puts.send((key.into(), value)).unwrap();
        Ok(())
    }

    async fn connection_changed(&mut self) {
        std::future::pending().await
    }
}

struct Published {
    control: Arc<Control>,
    puts: mpsc::UnboundedReceiver<(String, Vec<u8>)>,
    idle: Arc<Notify>,
}

impl Published {
    fn start(handler: &ProjectsHandler, control: Arc<Control>) -> Self {
        let (puts, receiver) = mpsc::unbounded_channel();
        let idle = handler.start_bus_changes_with_sink(FakeSink {
            control: control.clone(),
            puts,
        });
        Self {
            control,
            puts: receiver,
            idle,
        }
    }

    async fn idle(&self) {
        timeout(Duration::from_secs(5), self.idle.notified())
            .await
            .expect("publisher idle");
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

const KEYS: &[&str] = &[
    "agents",
    "catalog.status",
    "profile",
    "registry",
    "role.connector_home",
];

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
async fn bus_changes_role_take_and_refresh_only_publish_cached_epoch() {
    let f = RoleFixture::new("bus-role", false).await;
    let mut published = Published::start(&f.handler, Arc::default());
    published.take(KEYS).await;
    published.idle().await;
    let bytes = f.handler.take_role(json!({"role":"connector_home","expected_epoch":0,"reason":"home","request_id":"0123456789abcdef0123456789abcdef"}), ROLE_DIRECT, tokio::time::Instant::now()).await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["epoch"],
        1
    );
    assert_eq!(
        published.take(&["role.connector_home"]).await["role.connector_home"],
        json!({"epoch":1})
    );
    published.idle().await;
    f.record.put("role:connector_home", 2, serde_json::to_vec(&json!({"v":1,"role":"connector_home","holder":ROLE_OTHER_AUTHOR,"epoch":2,"reason":"moved"})).unwrap(), ROLE_OTHER_AUTHOR);
    f.refresh().await;
    assert_eq!(
        published.take(&["role.connector_home"]).await["role.connector_home"],
        json!({"epoch":2})
    );
    published.idle().await;
    f.refresh().await;
    published.idle().await;
    published.quiet();
    f.handler.stop_bus_changes().await;
    f.handler.stop_role_refresh().await;
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
    assert_eq!(health["busChangesPuts"], 5);
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

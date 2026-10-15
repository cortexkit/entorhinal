use super::*;
use crate::catalog_network::{Limits, RegistryClient};
use crate::catalog_trust::tests::vectors;
use crate::fake_registry::{FakeRegistry, Reply};
use serde_json::{json, Value};
fn clock() -> i64 {
    600_000
}
struct Fixture {
    root: std::path::PathBuf,
    descriptor: cortexkit_store_types::StorageDescriptor,
    slot: Arc<Mutex<Option<RegistryStore>>>,
    fake: FakeRegistry,
    health: Arc<HealthGauges>,
}
impl Fixture {
    async fn new() -> Self {
        let (root, descriptor) = crate::tests::scratch_descriptor("catalog-poll");
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("agent.cutover", "{}", "module", None, |_| Ok(()))
            .unwrap();
        let reply: Value = serde_json::from_slice(
            &store
                .agent_mutation(
                    "agent.create",
                    json!({"role":"assistant","name":"Poll","tag":"catalog","request_key":"poll"}),
                    1,
                )
                .unwrap(),
        )
        .unwrap();
        let agent = reply["result"]["agent"]["agent_id"].as_str().unwrap();
        store
            .with_principal("test")
            .catalog_set_pins(
                entorhinal_core::catalog_agent::PinWriteRequest {
                    agent_id: agent.into(),
                    pins: vec![entorhinal_core::catalog_agent::CatalogPin {
                        name: "@example/hello-skill".into(),
                        version: "1.0.0".into(),
                        kind: entorhinal_catalog::Kind::Skill,
                        manifest_sha256: "11".repeat(32),
                        archive_sha256: "22".repeat(32),
                        publisher_fingerprint: "33".repeat(32),
                        root: true,
                        range: Some("^1".into()),
                    }],
                    request_key: None,
                },
                2,
            )
            .unwrap();
        store
            .catalog_registry_add(
                "https://registry.example.invalid",
                &["@example".into()],
                false,
                "test-registry-1",
                vectors("statements")["keys"]["registry_status"][0]["public_key_hex"]
                    .as_str()
                    .unwrap(),
                1,
            )
            .unwrap();
        store.catalog_poll_offset(600_000).unwrap();
        let fake = FakeRegistry::start().await;
        fake.serve_vectors();
        let f = Self {
            root,
            descriptor,
            slot: Arc::new(Mutex::new(Some(store))),
            fake,
            health: Arc::default(),
        };
        f.feed();
        f
    }
    fn feed(&self) {
        let feed = Reply {
            headers: vec![("ETag".into(), "\"poll-1\"".into())],
            ..Reply::bytes(
                json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[]})
                    .to_string(),
            )
        };
        self.fake.serve("/v1/scopes/example/status", feed.clone());
        self.fake.serve("/v1/scopes/example/status?after=1", feed);
    }
    fn worker(&self) -> Worker {
        let url = self.fake.url.clone();
        Worker {
            store: Arc::downgrade(&self.slot),
            health: self.health.clone(),
            clock,
            wake: Arc::default(),
            sources: Arc::new(move |store, scope| {
                CatalogTrust::with_client(
                    store,
                    scope,
                    RegistryClient::loopback(&url, std::slice::from_ref(&url), Limits::MODULE),
                )
            }),
        }
    }
    async fn done(&self) {
        self.health.catalog.done.notified().await;
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.slot.lock().unwrap().take();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
#[tokio::test(start_paused = true)]
async fn startup_hourly_offset_and_backoff_recovers() {
    for (failures, hours) in [(0, 1), (1, 2), (2, 4), (3, 6), (4, 6), (100, 6)] {
        assert_eq!(
            delay(clock(), 600_000, failures),
            Duration::from_secs(hours * 3600)
        );
    }
    let f = Fixture::new().await;
    assert!(f.health.catalog.report()["catalogLastSuccessMs"].is_null());
    let task = tokio::spawn(f.worker().run());
    f.done().await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], 1);
    assert_eq!(f.health.catalog.report()["catalogLastSuccessMs"], 600_000);
    tokio::time::advance(Duration::from_millis(HOUR_MS - 1)).await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    f.done().await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], 2);
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 503,
            ..Reply::bytes("")
        },
    );
    for (hours, count) in [(1, 1), (2, 2), (4, 3), (6, 4)] {
        tokio::time::advance(Duration::from_secs(hours * 3600)).await;
        f.done().await;
        assert_eq!(
            f.health.catalog.report()["catalogConsecutiveFailures"],
            count
        );
        assert!(f.health.catalog.report()["catalogLastError"].is_string());
    }
    f.feed();
    tokio::time::advance(Duration::from_secs(6 * 3600)).await;
    f.done().await;
    assert_eq!(f.health.catalog.report()["catalogConsecutiveFailures"], 0);
    assert!(f.health.catalog.report()["catalogLastError"].is_null());
    let count = f.health.catalog.report()["catalogScopesPolled"]
        .as_u64()
        .unwrap();
    tokio::time::advance(Duration::from_secs(3600)).await;
    f.done().await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], count + 1);
    task.abort();
    task.await.unwrap_err();
    assert_eq!(delay(0, 600_000, 0), Duration::from_secs(600));
    assert_eq!(delay(600_000, 600_000, 0), Duration::from_secs(3600));
    assert_eq!(delay(-1, 600_000, 0), Duration::from_millis(600_001));
}
#[tokio::test]
async fn stored_offset_survives_reopen_and_not_modified_changes_nothing() {
    let f = Fixture::new().await;
    assert_eq!(f.worker().offset().unwrap(), 600_000);
    f.worker().pass().await.unwrap();
    f.slot.lock().unwrap().take();
    let store = RegistryStore::open(&f.descriptor).unwrap();
    assert_eq!(store.catalog_poll_offset(999_999).unwrap(), 600_000);
    *f.slot.lock().unwrap() = Some(store.clone());
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 304,
            headers: vec![("ETag".into(), "\"ignored\"".into())],
            ..Reply::bytes("")
        },
    );
    f.worker().pass().await.unwrap();
    assert_eq!(store.catalog_feed_seq("@example").unwrap(), Some(1));
    assert_eq!(
        store.catalog_feed_etag("@example").unwrap().as_deref(),
        Some("\"poll-1\"")
    );
    assert!(store
        .catalog_statuses("https://registry.example.invalid")
        .unwrap()
        .is_empty());
    assert!(f.fake.requests.lock().unwrap().iter().any(|r| r
        .contains("If-None-Match: \"poll-1\"")
        || r.contains("if-none-match: \"poll-1\"")));
}
#[tokio::test]
async fn gated_http_holds_neither_store_nor_handler_lease() {
    let f = Fixture::new().await;
    let gate = Arc::new(tokio::sync::Notify::new());
    let entered = Arc::new(tokio::sync::Notify::new());
    f.fake.serve(
        "/v1/scopes/example/status",
        Reply {
            release: Some(gate.clone()),
            entered: Some(entered.clone()),
            ..Reply::bytes(
                json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[]})
                    .to_string(),
            )
        },
    );
    let worker = f.worker();
    let task = tokio::spawn(async move { worker.pass().await });
    entered.notified().await;
    f.slot.lock().unwrap().take();
    let reopened =
        RegistryStore::open(&f.descriptor).expect("an HTTP await retained the store lease");
    drop(reopened);
    gate.notify_one();
    assert_eq!(task.await.unwrap(), Err("store_unavailable"));
}

#[tokio::test(start_paused = true)]
async fn hello_ack_starts_catalog_poll_once() {
    use subc_client_rs::ModuleHandler;
    let f = Fixture::new().await;
    let sources = f.worker().sources;
    f.slot.lock().unwrap().take();
    let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), clock);
    handler.catalog_agent.sources = sources;
    handler
        .on_hello_ack(&crate::tests::hello_ack(&f.descriptor))
        .await;
    tokio::time::timeout(
        Duration::from_secs(1),
        handler.health.catalog.done.notified(),
    )
    .await
    .expect("HELLO_ACK did not schedule the startup status poll");
    assert_eq!(handler.health.catalog.report()["catalogScopesPolled"], 1);
    handler.start_catalog_poll();
    assert_eq!(handler.health.catalog.report()["catalogScopesPolled"], 1);
    assert_eq!(
        handler.health().await.metrics.unwrap()["catalogLastSuccessMs"],
        600_000
    );
    crate::tests::stop_store_tasks(&handler).await;
    handler.store.lock().unwrap().take();
}

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
            sql: Arc::default(),
            sources: Arc::new(move |store, scope| {
                CatalogTrust::with_client(
                    store,
                    scope,
                    RegistryClient::loopback(&url, std::slice::from_ref(&url), Limits::MODULE),
                )
            }),
        }
    }
}
async fn completed_pass(
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<CompletedPass>,
    number: u64,
    result: Result<(), &'static str>,
) -> Value {
    let paused_at = tokio::time::Instant::now();
    // Real time only bounds a broken test; it never drives the simulated schedule.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    let completion = loop {
        match receiver.try_recv() {
            Ok(completion) => break completion,
            Err(tokio::sync::mpsc::error::TryRecvError::Disconnected) => {
                panic!("poll completion channel closed before pass {number}")
            }
            Err(tokio::sync::mpsc::error::TryRecvError::Empty) => {}
        }
        assert!(
            std::time::Instant::now() < deadline,
            "poll pass {number} did not complete"
        );
        // Keep socket I/O runnable instead of letting paused Tokio time jump to
        // the HTTP timeout. Only explicit advance calls drive the poll schedule.
        tokio::task::yield_now().await;
    };
    assert_eq!(completion.number, number, "unexpected completed poll pass");
    assert_eq!(completion.result, result, "poll pass {number}");
    assert_eq!(
        tokio::time::Instant::now(),
        paused_at,
        "HTTP completion advanced the paused clock"
    );
    completion.gauges
}
#[test]
fn completed_passes_are_buffered_with_their_results_and_gauges() {
    let gauges = Gauges::default();
    let mut completions = gauges.completions();
    gauges.finish(Ok(()), 100);
    gauges.finish(Err("status_unavailable"), 200);
    gauges.finish(Ok(()), 300);
    for (number, result, success, failures) in [
        (1, Ok(()), 100, 0),
        (2, Err("status_unavailable"), 100, 1),
        (3, Ok(()), 300, 0),
    ] {
        let pass = completions.try_recv().unwrap();
        assert_eq!(pass.number, number);
        assert_eq!(pass.result, result);
        assert_eq!(pass.gauges["catalogLastSuccessMs"], success);
        assert_eq!(pass.gauges["catalogConsecutiveFailures"], failures);
    }
    assert!(matches!(
        completions.try_recv(),
        Err(tokio::sync::mpsc::error::TryRecvError::Empty)
    ));
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
    let mut completions = f.health.catalog.completions();
    assert!(f.health.catalog.report()["catalogLastSuccessMs"].is_null());
    let task = tokio::spawn(f.worker().run());
    let first = completed_pass(&mut completions, 1, Ok(())).await;
    assert_eq!(first["catalogScopesPolled"], 1);
    assert_eq!(first["catalogLastSuccessMs"], 600_000);
    tokio::time::advance(Duration::from_millis(HOUR_MS - 1)).await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    let hourly = completed_pass(&mut completions, 2, Ok(())).await;
    assert_eq!(hourly["catalogScopesPolled"], 2);
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 503,
            ..Reply::bytes("")
        },
    );
    for (hours, count) in [(1, 1), (2, 2), (4, 3), (6, 4)] {
        tokio::time::advance(Duration::from_secs(hours * 3600)).await;
        let failed = completed_pass(&mut completions, count + 2, Err("registry_http_error")).await;
        assert_eq!(failed["catalogConsecutiveFailures"], count);
        assert_eq!(failed["catalogLastError"], "registry_http_error");
    }
    f.feed();
    tokio::time::advance(Duration::from_secs(6 * 3600)).await;
    let recovered = completed_pass(&mut completions, 7, Ok(())).await;
    assert_eq!(recovered["catalogConsecutiveFailures"], 0);
    assert!(recovered["catalogLastError"].is_null());
    let count = recovered["catalogScopesPolled"].as_u64().unwrap();
    tokio::time::advance(Duration::from_secs(3600)).await;
    let hourly = completed_pass(&mut completions, 8, Ok(())).await;
    assert_eq!(hourly["catalogScopesPolled"], count + 1);
    task.abort();
    task.await.unwrap_err();
    assert_eq!(delay(0, 600_000, 0), Duration::from_secs(600));
    assert_eq!(delay(600_000, 600_000, 0), Duration::from_secs(3600));
    assert_eq!(delay(-1, 600_000, 0), Duration::from_millis(600_001));
}
#[tokio::test]
async fn stored_offset_survives_reopen_and_not_modified_changes_nothing() {
    let f = Fixture::new().await;
    assert_eq!(f.worker().offset().await.unwrap(), 600_000);
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
    let entry = entered.notified();
    tokio::pin!(entry);
    entry.as_mut().enable();
    let mut task = tokio::spawn(async move { worker.pass().await });
    tokio::select! {
        () = &mut entry => {},
        result = &mut task => panic!("HTTP poll completed before reaching its reply gate: {result:?}"),
    }
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
    let mut completions = handler.health.catalog.completions();
    handler.catalog_agent.sources = sources;
    handler
        .on_hello_ack(&crate::tests::hello_ack(&f.descriptor))
        .await;
    let first = completed_pass(&mut completions, 1, Ok(())).await;
    assert_eq!(first["catalogScopesPolled"], 1);
    handler.start_catalog_poll();
    assert_eq!(handler.health.catalog.report()["catalogScopesPolled"], 1);
    assert_eq!(
        handler.health().await.metrics.unwrap()["catalogLastSuccessMs"],
        600_000
    );
    crate::tests::stop_store_tasks(&handler).await;
    handler.store.lock().unwrap().take();
}

#[tokio::test(start_paused = true)]
async fn catalog_poll_clock_sql_does_not_occupy_runtime() {
    use crate::catalog_sql::tests::{finish, poll, responsive, started, tracked, HeldSql};
    let f = finish(Fixture::new()).await;
    let worker = f.worker();
    let store = worker.store().unwrap();
    let handler = ProjectsHandler::with_runtime("poll-held-sql".into(), clock);
    *handler.store.lock().unwrap() = Some(store.clone());
    let held = HeldSql::new(store).await;
    let (request, entered) = tracked(worker.offset());
    let mut request = Box::pin(request);
    started(request.as_mut(), &entered).await;
    assert!(poll(request.as_mut()).is_pending());
    responsive(&handler).await;
    held.finish().await;
    assert_eq!(finish(request).await.unwrap(), 600_000);
}

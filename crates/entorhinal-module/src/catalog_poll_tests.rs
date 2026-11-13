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
    agent: String,
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
            agent: agent.into(),
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
            watches: Arc::default(),
            freshness_wake: Arc::default(),
            snapshots: tokio::sync::Mutex::new(()),
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
impl Drop for Fixture {
    fn drop(&mut self) {
        self.slot.lock().unwrap().take();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn catalog_poll_deadline_marks_stale_and_notifies_while_http_is_pending() {
    use entorhinal_core::catalog_trust::{CatalogStatus, TrustWrite};
    use std::{future::Future, pin::Pin, task::Poll};
    static NOW: AtomicI64 = AtomicI64::new(1_499_000);
    fn deadline_clock() -> i64 {
        NOW.load(Ordering::Relaxed)
    }
    struct Sink {
        sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        cancel: tokio::sync::watch::Sender<bool>,
    }
    #[async_trait::async_trait]
    impl crate::watches::WatchTransport for Sink {
        async fn emit(&self, bytes: Vec<u8>) -> Result<(), crate::HandlerError> {
            self.sender.send(bytes).unwrap();
            Ok(())
        }
        async fn ended(&self) {
            let _ = self.cancel.subscribe().wait_for(|ended| *ended).await;
        }
    }
    async fn next_notice(
        mut watch: Pin<&mut impl Future<Output = subc_client_rs::HandlerOutcome>>,
        receiver: &mut tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) -> Value {
        let limit = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            std::future::poll_fn(|cx| {
                assert!(
                    watch.as_mut().poll(cx).is_pending(),
                    "watch ended before freshness notice"
                );
                Poll::Ready(())
            })
            .await;
            if let Ok(bytes) = receiver.try_recv() {
                return serde_json::from_slice(&bytes).unwrap();
            }
            assert!(std::time::Instant::now() < limit,"freshness deadline passed without a status-watch notice while HTTP remained pending");
            tokio::task::yield_now().await;
        }
    }
    NOW.store(1_499_000, Ordering::Relaxed);
    let f = Fixture::new().await;
    let store = f.slot.lock().unwrap().as_ref().unwrap().clone();
    let records = vectors("record");
    let package = &records["packages"]["flow"];
    let manifest = package["manifest_text"].as_str().unwrap().as_bytes();
    crate::catalog_blob::BlobStore::new(store.store_folder())
        .unwrap()
        .publish(manifest)
        .unwrap();
    let pin: entorhinal_core::catalog_agent::CatalogPin = serde_json::from_value(json!({"name":"@example/hello-flow","version":"1.0.0","kind":"flow","manifest_sha256":package["manifest_sha256"],"archive_sha256":package["archive_sha256"],"publisher_fingerprint":records["keys"]["trusted"]["fingerprint"],"root":true,"range":"^1"})).unwrap();
    store
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.agent.clone(),
                pins: vec![pin.clone()],
                request_key: None,
            },
            600_000,
        )
        .unwrap();
    store
        .catalog_sync_fast_lane(std::slice::from_ref(&pin), 600_000)
        .unwrap();
    let status = vectors("status")["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["record"]["name"] == pin.name)
        .unwrap()
        .clone();
    store
        .catalog_trust_transaction::<_, entorhinal_core::catalog::CatalogWriteError>(
            "@example",
            |registry, _, _| {
                Ok((
                    (),
                    TrustWrite {
                        statuses: vec![CatalogStatus {
                            name: pin.name.clone(),
                            version: pin.version.clone(),
                            registry_url: registry.url.clone(),
                            sequence: status["record"]["sequence"].as_u64().unwrap(),
                            changed_at: status["record"]["changed_at"].as_str().unwrap().into(),
                            published_at: status["record"]["published_at"].as_str().unwrap().into(),
                            signer_index: 0,
                            canonical: entorhinal_catalog::jcs::to_string(&status["record"])
                                .unwrap(),
                            signature: status["signature"].as_str().unwrap().into(),
                            starter_proof: None,
                        }],
                        confirmed_at_ms: Some(600_000),
                        ..Default::default()
                    },
                ))
            },
        )
        .unwrap();
    assert_eq!(
        store.catalog_next_freshness_deadline().unwrap(),
        Some(1_500_000)
    );
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    f.fake.serve(
        "/v1/scopes/example/status",
        Reply {
            entered: Some(entered.clone()),
            release: Some(release),
            ..Reply::bytes(
                json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[]})
                    .to_string(),
            )
        },
    );
    let mut worker = f.worker();
    worker.clock = deadline_clock;
    let sql = worker.sql.clone();
    let mut handler = ProjectsHandler::with_runtime("deadline-watch".into(), deadline_clock);
    handler.store = f.slot.clone();
    handler.watches = worker.watches.clone();
    let key = (991, 1);
    handler.route_admissions().insert(
        key,
        crate::RouteAdmission::from_bind(Some(subc_protocol::Principal::Direct), None),
    );
    let task = tokio::spawn(worker.run());
    crate::catalog_sql::tests::finish(entered.notified()).await;
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sink = Sink {
        sender,
        cancel: tokio::sync::watch::channel(false).0,
    };
    let body = json!({"method":crate::watches::CATALOG,"params":{}})
        .to_string()
        .into_bytes();
    let watch = handler.serve_watch(&sink, &body, key);
    tokio::pin!(watch);
    assert_eq!(
        next_notice(watch.as_mut(), &mut receiver).await["kind"],
        "resync"
    );
    NOW.store(1_500_000, Ordering::Relaxed);
    tokio::time::advance(Duration::from_secs(1)).await;
    let changed = next_notice(watch.as_mut(), &mut receiver).await;
    assert_eq!(changed["kind"], "changes");
    assert_eq!(
        changed["changes"],
        json!([{"name":pin.name,"version":pin.version}])
    );
    assert_eq!(
        store.catalog_served_statuses().unwrap()[&(pin.name.clone(), pin.version.clone())]
            ["stale_since_ms"],
        1_500_000
    );
    assert_eq!(
        f.health.catalog.completed_passes.load(Ordering::Relaxed),
        0,
        "the held HTTP pass must not have completed"
    );
    store
        .catalog_trust_transaction::<_, entorhinal_core::catalog::CatalogWriteError>(
            "@example",
            |_, _, _| {
                Ok((
                    (),
                    TrustWrite {
                        confirmed_at_ms: Some(1_500_000),
                        confirmed_scope: true,
                        ..Default::default()
                    },
                ))
            },
        )
        .unwrap();
    let cleared = next_notice(watch.as_mut(), &mut receiver).await;
    assert_eq!(cleared["kind"], "changes");
    assert!(
        store.catalog_served_statuses().unwrap()[&(pin.name.clone(), pin.version.clone())]
            .get("stale_since_ms")
            .is_none()
    );
    assert_eq!(
        store.catalog_next_freshness_deadline().unwrap(),
        Some(2_400_000)
    );
    store
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.agent.clone(),
                pins: vec![],
                request_key: None,
            },
            1_500_000,
        )
        .unwrap();
    assert_eq!(
        next_notice(watch.as_mut(), &mut receiver).await["kind"],
        "changes"
    );
    assert_eq!(store.catalog_next_freshness_deadline().unwrap(), None);
    sink.cancel.send_replace(true);
    assert!(matches!(
        watch.await,
        subc_client_rs::HandlerOutcome::Streamed
    ));
    task.abort();
    let _ = task.await;
    sql.wait().await;
}

#[tokio::test(start_paused = true)]
async fn catalog_poll_flow_ten_minute_lane_and_minute_backoff() {
    let f = Fixture::new().await;
    let store = f.slot.lock().unwrap().as_ref().unwrap().clone();
    let record = vectors("record");
    let package = &record["packages"]["flow"];
    let bytes = package["manifest_text"].as_str().unwrap().as_bytes();
    crate::catalog_blob::BlobStore::new(store.store_folder())
        .unwrap()
        .publish(bytes)
        .unwrap();
    let agent = store.catalog_live_flow_pins().unwrap();
    assert!(agent.is_empty());
    store
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.agent.clone(),
                pins: vec![entorhinal_core::catalog_agent::CatalogPin {
                    name: "@example/hello-flow".into(),
                    version: "1.0.0".into(),
                    kind: entorhinal_catalog::Kind::Flow,
                    manifest_sha256: entorhinal_catalog::sha256_hex(bytes),
                    archive_sha256: package["archive_sha256"].as_str().unwrap().into(),
                    publisher_fingerprint: record["keys"]["trusted"]["fingerprint"]
                        .as_str()
                        .unwrap()
                        .into(),
                    root: true,
                    range: Some("^1".into()),
                }],
                request_key: None,
            },
            100,
        )
        .unwrap();
    let mut completions = f.health.catalog.completions();
    let worker = f.worker();
    let sql = worker.sql.clone();
    let task = tokio::spawn(worker.run());
    completed_pass(&mut completions, 1, Ok(())).await;
    tokio::time::advance(Duration::from_millis(FAST_MS - 1)).await;
    assert_eq!(f.health.catalog.report()["catalogScopesPolled"], 1);
    tokio::time::advance(Duration::from_millis(1)).await;
    completed_pass(&mut completions, 2, Ok(())).await;
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 503,
            ..Reply::bytes("")
        },
    );
    let mut number = 3;
    for ms in [600_000, 60_000, 120_000, 240_000, 480_000, 600_000] {
        tokio::time::advance(Duration::from_millis(ms)).await;
        completed_pass(&mut completions, number, Err("registry_http_error")).await;
        number += 1;
    }
    f.feed();
    tokio::time::advance(Duration::from_millis(600_000)).await;
    completed_pass(&mut completions, number, Ok(())).await;
    task.abort();
    let _ = task.await;
    sql.wait().await;
    let mut no_caps: Value = serde_json::from_slice(bytes).unwrap();
    no_caps["requires"]["capabilities"] = json!([]);
    let mut pin = store.catalog_live_flow_pins().unwrap().remove(0);
    pin.manifest_sha256 = crate::catalog_blob::BlobStore::new(store.store_folder())
        .unwrap()
        .publish(no_caps.to_string().as_bytes())
        .unwrap();
    store
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.agent.clone(),
                pins: vec![pin],
                request_key: None,
            },
            101,
        )
        .unwrap();
    assert!(
        f.worker().fast_scopes().await.unwrap().is_empty(),
        "a flow with no required capabilities stays on the hourly lane"
    );
    for (failure, ms) in [
        (0, 600_000),
        (1, 60_000),
        (2, 120_000),
        (3, 240_000),
        (4, 480_000),
        (5, 600_000),
        (99, 600_000),
    ] {
        assert_eq!(fast_delay(failure), Duration::from_millis(ms));
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
    let worker = f.worker();
    let sql = worker.sql.clone();
    let task = tokio::spawn(worker.run());
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
    // Aborting the task leaves its blocking SQL jobs (the freshness timer's
    // among them) running on side threads; wait for them before the fixture
    // deletes the store's folder.
    sql.wait().await;
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

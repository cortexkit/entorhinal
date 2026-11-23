use super::*;

const DIRECT: RouteKey = (51, 1);
const OTHER: RouteKey = (52, 1);
const CODE: &str = "legacy_sharing_unsupported";

fn assert_message(text: &str) {
    for point in [
        "shares through engram",
        "carries over neither its shared data nor its local-only data",
        "Move the store folder aside and keep it; do not delete it",
        "With a new, empty registry",
        "if this account's registry already lives on the hub, this computer will join it on its own",
        "`ck projects log enable` to join at once",
        "import agents first and run `ck projects log enable`",
        "`ck projects log enable --without-agents`, then confirm the prompt",
    ] {
        assert!(text.contains(point), "missing {point:?} in {text:?}");
    }
}

#[derive(Clone, Default)]
struct Logs(Arc<Mutex<Vec<u8>>>);

impl std::io::Write for Logs {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

async fn request(
    handler: &ProjectsHandler,
    route: RouteKey,
    method: &str,
    params: Value,
) -> subc_protocol::ErrorBody {
    let body = serde_json::to_vec(&json!({"method": method, "params": params})).unwrap();
    match handler.handle_served_request(&body, route).await {
        HandlerOutcome::Error { code, message } => subc_protocol::ErrorBody::new(code, message),
        other => panic!("expected {method} refusal, got {other:?}"),
    }
}

#[test]
fn oldway_refusal_module_startup_and_served_requests() {
    // A current-thread runtime lets the scoped subscriber also see the retry
    // task's store log without installing a subscriber for unrelated tests.
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    for lease_held in [false, true] {
        let log = Logs::default();
        let output = log.clone();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(move || output.clone())
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            let (dir, descriptor) = tests::scratch_descriptor("oldway-refusal-module");
            let predecessor = RegistryStore::open(&descriptor).unwrap();
            predecessor
                .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                    tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                    Ok(())
                })
                .unwrap();
            // Opened while still disabled, this handle holds the real lease
            // until dropped; the refused module never opts in to the fixture.
            let holder = lease_held.then_some(predecessor);
            let served = ProjectsHandler::new().unwrap().into_served();
            let handler = &served.0;
            runtime.block_on(async {
                served.on_hello_ack(&tests::hello_ack(&descriptor)).await;
                if lease_held {
                    let opening = served.health().await;
                    assert_eq!(opening.status, HealthStatus::Degraded);
                    assert!(opening.detail.unwrap().contains("lease is held"));
                    drop(holder);
                    tokio::time::timeout(Duration::from_secs(5), async {
                        while handler.health.store_opening.load(Ordering::Acquire) {
                            tokio::time::sleep(Duration::from_millis(10)).await;
                        }
                    })
                    .await
                    .expect("lease retry settled");
                }
                let health = served.health().await;
                assert_eq!(health.status, HealthStatus::Failing);
                assert!(handler.health.store_failed.load(Ordering::Relaxed));
                let detail = health.detail.unwrap();
                assert!(detail.starts_with("store_failed: legacy_sharing_unsupported:"));
                assert_message(&detail);
                assert!(handler.store.lock().unwrap().is_none());

                for (key, principal) in [(DIRECT, Principal::Direct), (OTHER, Principal::Unverified)] {
                    let bind = RouteBindRequest::new(
                        RouteHandle::detached(key.0, key.1),
                        subc_protocol::RouteTarget::ManagementSurface { module_id: MODULE_ID.into() },
                        subc_protocol::BindIdentity::new("test", "operator"),
                    ).with_principal(principal);
                    served.on_bind(&bind).await;
                }

                let error = handler.identity_log_status().unwrap_err();
                assert_eq!(error.code, CODE);
                assert_message(&error.message);
                let status = request(handler, DIRECT, "identity_log.status", json!({})).await;
                assert_eq!(status.code, CODE);
                assert_eq!(status.message, error.message);

                let take = json!({"role":"x","expected_epoch":0,"request_id":"11".repeat(16),"reason":"run coordinator"});
                let refused = request(handler, DIRECT, roles::TAKE, take.clone()).await;
                assert_eq!(refused.code, CODE);
                assert_eq!(refused.message, error.message);
                assert_eq!(request(handler, OTHER, roles::TAKE, take).await.code, "caller_forbidden");
                assert_eq!(request(handler, DIRECT, roles::TAKE, json!({})).await.code, "invalid_request");

                // The served handler sends agent.move_abort's initial row
                // lookup through its blocking SQL job.
                let worker = request(handler, DIRECT, move_ops::ABORT, json!({"cut_id":"missing","request_key":"abort"})).await;
                assert_eq!(worker.code, CODE);
                assert_eq!(worker.message, error.message);
            });

            let (exit, stderr) = cli::log_status_against(|method, params| {
                assert_eq!(method, "identity_log.status");
                runtime.block_on(request(handler, DIRECT, method, params))
            });
            assert_eq!(exit, std::process::ExitCode::from(3));
            assert!(stderr.starts_with("legacy_sharing_unsupported: "));
            assert_message(&stderr);

            let logged = String::from_utf8(log.0.lock().unwrap().clone()).unwrap();
            let line = logged
                .lines()
                .find(|line| line.contains("ERROR store:"))
                .unwrap_or_else(|| panic!("no store error in {logged:?}"));
            assert!(line.contains(CODE), "{line}");
            assert_eq!(
                line.contains("opening projects storage after"),
                lease_held,
                "{line}"
            );
            assert_message(line);

            runtime.block_on(tests::stop_store_tasks(handler));
            drop(served);
            std::fs::remove_dir_all(dir).unwrap();
        });
    }
}

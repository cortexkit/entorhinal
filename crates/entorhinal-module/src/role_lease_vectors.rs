use super::*;
use crate::role_leases::{View, ACK, WATCH};
use std::sync::atomic::{AtomicU64, Ordering};

const CONNECTOR: &str = "connector_home";
const PROJECT: &str = "dreamer.pj-fern_01";
const WORKSPACE: &str = "dreamer.workspace_02";
const GLOBAL: &str = "dreamer.global";

fn output(op: &str, params: &Value, principal: &Value, frames: &[Frame]) -> String {
    let frames = frames
        .iter()
        .map(|frame| {
            let kind = match frame.header.ty {
                FrameType::StreamData => "StreamData",
                FrameType::StreamEnd => "StreamEnd",
                FrameType::Response => "Response",
                FrameType::Error => "Error",
                other => panic!("unexpected frame {other:?}"),
            };
            let body = if frame.body.is_empty() {
                "null"
            } else {
                std::str::from_utf8(&frame.body).unwrap()
            };
            format!("{{\"type\":{},\"body\":{body}}}", json!(kind))
        })
        .collect::<Vec<_>>()
        .join(",");
    format!(
        "  {{\"op\":{},\"request\":{params},\"principal\":{principal},\"frames\":[{frames}]}}",
        json!(op)
    )
}

async fn bind(
    module: &mut ModuleHarness,
    channel: u16,
    principal: Principal,
    scope: Option<&crate::ScopeStamp>,
) -> RouteHandle {
    let request = crate::RouteBindRequest::new(
        RouteHandle::detached(channel, 1),
        RouteTarget::ManagementSurface {
            module_id: crate::MODULE_ID.into(),
        },
        BindIdentity::new("test", "lease-fixtures"),
    )
    .with_principal(principal);
    module
        .bind_route(match scope {
            Some(scope) => request.with_scope(scope.clone()),
            None => request,
        })
        .await
}

async fn frame(module: &mut ModuleHarness, corr: u64, kind: FrameType) -> Frame {
    let frame = tokio::time::timeout(Duration::from_secs(2), module.next_frame())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(frame.header.corr, corr);
    assert_eq!(
        frame.header.ty,
        kind,
        "frame {corr}: {}",
        String::from_utf8_lossy(&frame.body)
    );
    frame
}

fn publish(file: &str, records: Vec<String>) {
    published_file("role-lease-v1", file, records);
}

#[tokio::test(start_paused = true)]
async fn served_role_lease_v1_records_match_published_files() {
    let mut fixture = Fixture::new("served-role-lease").await;
    let clock = Arc::new(AtomicU64::new(100_000));
    let now = clock.clone();
    let leases = Arc::get_mut(&mut fixture.handler.role_leases).unwrap();
    leases.sample_clock(move || now.load(Ordering::Relaxed));
    let sources = [
        (CONNECTOR, 7, Some(145_000), false),
        (WORKSPACE, 4, Some(145_000), false),
        (GLOBAL, 9, Some(120_000), true),
        (PROJECT, 0, None, false),
    ]
    .map(|(role, gen, until, handback)| {
        let (tx, rx) = watch::channel(View {
            gen,
            until_ms: until,
            handback,
        });
        leases.sample_view(role, rx);
        tx
    });
    let handler = Arc::new(std::mem::replace(
        &mut fixture.handler,
        ProjectsHandler::with_runtime(INCARNATION.into(), || 700),
    ));
    let served = handler.clone();
    let mut module = ModuleHarness::start(move |_| ServedHandler(served), []).await;
    let plexus = bind(&mut module, 1811, reserved("plexus"), None).await;
    let mc = bind(&mut module, 1812, reserved("magic-context"), None).await;
    let second_mc = bind(&mut module, 1813, reserved("magic-context"), None).await;
    let direct = bind(&mut module, 1814, Principal::Direct, None).await;
    let scope: crate::ScopeStamp = serde_json::from_value(json!({
        "owner":{"kind":"reserved","module_id":"magic-context"},
        "ref":"head:agent_0000000000000001", "scope_epoch":7, "kind":"head",
        "attributes":{"flow_id":"lease-test-flow"}, "owner_authorized":true,
    }))
    .unwrap();
    let flow = bind(&mut module, 1815, reserved("magic-context"), Some(&scope)).await;
    let p = json!({"kind":"reserved","moduleId":"plexus"});
    let m = json!({"kind":"reserved","moduleId":"magic-context"});

    let mut ack_records = Vec::new();
    for (file, role, route, principal) in [
        ("holding.json", CONNECTOR, plexus, &p),
        ("not-holding.json", PROJECT, mc, &m),
        ("handback.json", GLOBAL, mc, &m),
        ("lapsed.json", WORKSPACE, mc, &m),
    ] {
        let params = json!({"role":role});
        let corr = module
            .send_request(route, request(WATCH, params.clone()))
            .await;
        let first = frame(&mut module, corr, FrameType::StreamData).await;
        let notice: Value = serde_json::from_slice(&first.body).unwrap();
        let ack = json!({"role":role,"seq":notice["seq"]});
        if role == PROJECT {
            let wrong = module
                .send_request(second_mc, request(ACK, ack.clone()))
                .await;
            let refused = frame(&mut module, wrong, FrameType::Error).await;
            let mut principal = principal.clone();
            principal["differentRoute"] = json!(true);
            ack_records.push(output(ACK, &ack, &principal, &[refused]));
        }
        let id = module.send_request(route, request(ACK, ack.clone())).await;
        let acked = frame(&mut module, id, FrameType::Response).await;
        if role == CONNECTOR {
            ack_records.push(output(ACK, &ack, principal, &[acked]));
        }
        if role == WORKSPACE {
            clock.store(145_000, Ordering::Relaxed);
            tokio::time::sleep(Duration::from_millis(45_001)).await;
            assert_eq!(handler.role_leases.held(), 0);
        }
        module.cancel_request(route, corr).await;
        let last = frame(&mut module, corr, FrameType::StreamEnd).await;
        let frames = if role == WORKSPACE {
            vec![first, last]
        } else {
            vec![first]
        };
        publish(file, vec![output(WATCH, &params, principal, &frames)]);
    }
    publish("ack.json", ack_records);

    let mut refusals = Vec::new();
    for (route, role, principal) in [
        (mc, "dreamer..global", m.clone()),
        (mc, CONNECTOR, m.clone()),
        (plexus, GLOBAL, p.clone()),
        (direct, GLOBAL, json!({"kind":"direct"})),
        (
            flow,
            GLOBAL,
            json!({"kind":"reserved","moduleId":"magic-context","flowScoped":true}),
        ),
    ] {
        let params = json!({"role":role});
        let corr = module
            .send_request(route, request(WATCH, params.clone()))
            .await;
        let refused = frame(&mut module, corr, FrameType::Error).await;
        refusals.push(output(WATCH, &params, &principal, &[refused]));
    }
    publish("refusals.json", refusals);
    drop(sources);
    module.shutdown().await;
    crate::tests::stop_store_tasks(&handler).await;
}

#[tokio::test(start_paused = true)]
async fn served_role_lease_v1_non_hub_records_match_published_files() {
    // Tokio suppresses automatic clock jumps while blocking work is pending.
    // Hold that inhibition across the entire exchange, including the gaps
    // between a completed SQL job and delivery of its reply.
    let (release_clock, clock_held) = tokio::sync::oneshot::channel::<()>();
    let keep_clock = tokio::task::spawn_blocking(move || clock_held.blocking_recv());
    let mut fixture = Fixture::new("served-role-lease-non-hub").await;
    let start = tokio::time::Instant::now();
    let clock = Arc::new(AtomicU64::new(100_000));
    let now = clock.clone();
    Arc::get_mut(&mut fixture.handler.role_leases)
        .unwrap()
        .sample_clock(move || now.load(Ordering::Relaxed));
    let handler = Arc::new(std::mem::replace(
        &mut fixture.handler,
        ProjectsHandler::with_runtime(INCARNATION.into(), || 700),
    ));
    let served = handler.clone();
    let mut module = ModuleHarness::start(move |_| ServedHandler(served), []).await;
    let plexus = bind(&mut module, 1821, reserved("plexus"), None).await;
    let mc = bind(&mut module, 1822, reserved("magic-context"), None).await;
    let p = json!({"kind":"reserved","moduleId":"plexus"});
    let m = json!({"kind":"reserved","moduleId":"magic-context"});
    let mut disabled = Vec::new();

    // Capture the renewal only after the first notice is acknowledged and the
    // lease task (which reads its local role view on its own schedule, apart
    // from the notice) has finished its first SQL pass; advancing time before
    // that would race the pass's deadline.
    for (role, route, principal) in [(CONNECTOR, plexus, &p), (GLOBAL, mc, &m)] {
        let pass = handler.role_leases.local_pass_complete.notified();
        tokio::pin!(pass);
        pass.as_mut().enable();
        let params = json!({"role":role});
        let corr = module
            .send_request(route, request(WATCH, params.clone()))
            .await;
        let first = frame(&mut module, corr, FrameType::StreamData).await;
        let notice = ack_frame(&mut module, route, role, &first).await;
        assert_eq!(notice["holding"], true);
        assert_eq!(notice["gen"], 1);
        pass.await;
        handler.hub_sql.wait().await;
        clock.fetch_add(20_000, Ordering::Relaxed);
        tokio::time::advance(Duration::from_millis(20_001)).await;
        let renewed = frame(&mut module, corr, FrameType::StreamData).await;
        let notice = ack_frame(&mut module, route, role, &renewed).await;
        assert_eq!(notice["issued_at_ms"], clock.load(Ordering::Relaxed));
        assert_eq!(notice["valid_for_ms"], 45_000);
        module.cancel_request(route, corr).await;
        frame(&mut module, corr, FrameType::StreamEnd).await;
        disabled.push(output(WATCH, &params, principal, &[first, renewed]));
    }
    assert_eq!(handler.role_leases.held(), 0);
    publish("sharing-disabled.json", disabled);

    // The published sharing samples each start from a fresh store.
    let (fresh_dir, descriptor) = crate::tests::scratch_descriptor("served-role-lease-engram");
    let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
    *handler.store.lock().unwrap() = Some(store.clone());
    store
        .apply_entry("fixture.lease", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let set_claim = |stale| {
        store
            .cache_roles(
                &[(
                    CONNECTOR.into(),
                    ClaimObservation::Present {
                        self_author: SELF_AUTHOR.into(),
                        claim: CachedClaim {
                            holder: Some(SELF_AUTHOR.into()),
                            epoch: 12,
                            cut_id: None,
                            version: 36,
                            read_at_ms: 700,
                            stale,
                            integrity_failed: false,
                        },
                    },
                )],
                700,
            )
            .unwrap();
        handler.role_cache_changed.notify_waiters();
    };
    set_claim(false);
    let params = json!({"role":CONNECTOR});
    let corr = module
        .send_request(plexus, request(WATCH, params.clone()))
        .await;
    let claim = frame(&mut module, corr, FrameType::StreamData).await;
    let notice = ack_frame(&mut module, plexus, CONNECTOR, &claim).await;
    assert_eq!(notice["gen"], 12);
    assert_eq!(notice["holding"], true);
    set_claim(true);
    let stale = frame(&mut module, corr, FrameType::StreamData).await;
    let notice = ack_frame(&mut module, plexus, CONNECTOR, &stale).await;
    assert_eq!(notice["gen"], 12);
    assert_eq!(notice["holding"], false);
    module.cancel_request(plexus, corr).await;
    frame(&mut module, corr, FrameType::StreamEnd).await;

    let dreamer = json!({"role":GLOBAL});
    let corr = module
        .send_request(mc, request(WATCH, dreamer.clone()))
        .await;
    let local = frame(&mut module, corr, FrameType::StreamData).await;
    let notice = ack_frame(&mut module, mc, GLOBAL, &local).await;
    assert_eq!(notice["gen"], 1);
    assert_eq!(notice["holding"], true);
    publish(
        "engram-sharing.json",
        vec![
            output(WATCH, &params, &p, &[claim, stale]),
            output(WATCH, &dreamer, &m, &[local]),
        ],
    );
    module.cancel_request(mc, corr).await;
    frame(&mut module, corr, FrameType::StreamEnd).await;
    store
        .record_hub_kv(
            "role.dreamer.global",
            1,
            Some((b"verified role", Some(&json!({"gen":12})))),
            700,
        )
        .unwrap();
    let corr = module
        .send_request(mc, request(WATCH, dreamer.clone()))
        .await;
    let regrant = frame(&mut module, corr, FrameType::StreamData).await;
    let notice = ack_frame(&mut module, mc, GLOBAL, &regrant).await;
    assert_eq!(notice["gen"], 13);
    assert_eq!(notice["holding"], true);
    publish(
        "local-after-hub.json",
        vec![output(WATCH, &dreamer, &m, &[regrant])],
    );
    assert!(start.elapsed() >= Duration::from_secs(40));
    module.shutdown().await;
    release_clock.send(()).unwrap();
    keep_clock.await.unwrap().unwrap();
    crate::tests::stop_store_tasks(&handler).await;
    handler.store.lock().unwrap().take();
    drop(store);
    let _ = std::fs::remove_dir_all(fresh_dir);
}

async fn ack_frame(
    module: &mut ModuleHarness,
    route: RouteHandle,
    role: &str,
    notice: &Frame,
) -> Value {
    let notice: Value = serde_json::from_slice(&notice.body).unwrap();
    let id = module
        .send_request(
            route,
            request(ACK, json!({"role":role,"seq":notice["seq"]})),
        )
        .await;
    frame(module, id, FrameType::Response).await;
    notice
}

#[tokio::test(start_paused = true)]
async fn served_role_lease_v1_yield_records_match_published_files() {
    use crate::role_leases::{tests::sample_holder, YIELD};

    for (file, withdrawn) in [
        ("yield-transferred.json", false),
        ("yield-withdrawn.json", true),
    ] {
        let mut fixture = Fixture::new("served-role-yield").await;
        let hub = sample_holder(Arc::get_mut(&mut fixture.handler.role_leases).unwrap());
        let handler = Arc::new(std::mem::replace(
            &mut fixture.handler,
            ProjectsHandler::with_runtime(INCARNATION.into(), || 700),
        ));
        let served = handler.clone();
        let mut module = ModuleHarness::start(move |_| ServedHandler(served), []).await;
        let route = bind(&mut module, 1831, reserved("plexus"), None).await;
        let watch = module
            .send_request(route, request(WATCH, json!({"role":CONNECTOR})))
            .await;
        for (gen, holding) in [(0, false), (1, false), (1, true)] {
            let notice = frame(&mut module, watch, FrameType::StreamData).await;
            let notice = ack_frame(&mut module, route, CONNECTOR, &notice).await;
            assert_eq!(notice["gen"], gen);
            assert_eq!(notice["holding"], holding);
        }
        hub.request_sample_handback();
        let notice = frame(&mut module, watch, FrameType::StreamData).await;
        assert_eq!(
            ack_frame(&mut module, route, CONNECTOR, &notice).await["handback"],
            true
        );

        if withdrawn {
            hub.withdraw_sample_handback();
        }
        let params = json!({"role":CONNECTOR,"gen":1});
        let call = module
            .send_request(route, request(YIELD, params.clone()))
            .await;
        loop {
            let next = tokio::time::timeout(Duration::from_secs(2), module.next_frame())
                .await
                .unwrap()
                .unwrap();
            if next.header.corr == call {
                assert_eq!(next.header.ty, FrameType::Response);
                let value: Value = serde_json::from_slice(&next.body).unwrap();
                assert_eq!(value["result"]["yielded"], !withdrawn);
                publish(
                    file,
                    vec![output(
                        YIELD,
                        &params,
                        &json!({"kind":"reserved","moduleId":"plexus"}),
                        &[next],
                    )],
                );
                break;
            }
            assert_eq!(next.header.corr, watch);
            assert_eq!(next.header.ty, FrameType::StreamData);
        }
        module.shutdown().await;
        crate::tests::stop_store_tasks(&handler).await;
    }
}

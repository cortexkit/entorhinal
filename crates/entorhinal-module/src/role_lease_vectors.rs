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
    assert_eq!(frame.header.ty, kind);
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
    fixture
        .handler
        .role_leases
        .sample_clock(move || now.load(Ordering::Relaxed));
    let sources = [
        (CONNECTOR, 7, 145_000, false),
        (WORKSPACE, 4, 145_000, false),
        (GLOBAL, 9, 120_000, true),
    ]
    .map(|(role, gen, until, handback)| {
        let (tx, rx) = watch::channel(View {
            gen,
            until_ms: Some(until),
            handback,
        });
        fixture.handler.role_leases.sample_view(role, rx);
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

use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    fleet_record::{self, Header},
    hub_membership,
    hub_residence::{self, SnapshotClaims},
    RouteAdmission, RouteKey,
};
use std::{collections::HashMap, sync::atomic::AtomicUsize};

const ROUTE: RouteKey = (20, 1);
const MIGRATED: &str = "agent_0000000000000040";

#[derive(Default)]
struct Image(HashMap<String, (Option<String>, i64)>);
impl SnapshotClaims for Image {
    fn claim(&self, agent: &str) -> Option<(Option<String>, i64)> {
        self.0.get(agent).cloned()
    }
    fn hub_ready(&self) -> bool {
        true
    }
    fn role(&self, name: &str) -> Option<(Option<String>, i64)> {
        self.0.get(&format!("role.{name}")).cloned()
    }
}

const SAMPLE_SENTINEL: &str = "engram:11111111111111111111111111111111";

async fn cutover_sample(handler: &ProjectsHandler, op: &str, params: Value) -> String {
    let outcome = handler
        .handle_served_request(&body(op, params.clone()), ROUTE)
        .await;
    let HandlerOutcome::Response(bytes) = outcome else {
        panic!("sample {op} refused: {outcome:?}");
    };
    format!(
        "  {{\"op\":{},\"request\":{},\"reply\":{}}}",
        json!(op),
        params,
        String::from_utf8(bytes).unwrap()
    )
}

#[tokio::test]
async fn hub_cutover_holder_samples() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let handler = &mut rig.b.handler;
    handler.operator_confirmer = Arc::new(RoleConfirm::default());
    bind(handler, subc_protocol::Principal::Direct);
    let other = "agent_0000000000000041";
    migrate_identity(&rig.b, MIGRATED, "Engram held");
    migrate_identity(&rig.b, other, "Hub held");
    let image = Arc::new(Image(HashMap::from([
        (MIGRATED.into(), (Some(SAMPLE_SENTINEL.into()), 5)),
        (other.into(), (Some(A.into()), 1)),
        (
            "role.dreamer.workspace_02".into(),
            (Some(SAMPLE_SENTINEL.into()), 5),
        ),
    ])));
    residence(&rig.b, image);
    for (id, holder, gen) in [(MIGRATED, SAMPLE_SENTINEL, 5), (other, A, 1)] {
        let mut seeded = record(B, holder, gen, false);
        seeded["seeded"] = json!(true);
        let revision = put(&rig, id, 0, B, seeded).await;
        assert!(!applied(&rig.b, id, revision).await.integrity_failed);
        until(|| ready_for(&rig.b, id)).await;
    }
    let handler = &rig.b.handler;
    use crate::served_vectors::published_file;
    let location = [other, MIGRATED];
    let mut records = Vec::new();
    for id in location {
        records.push(cutover_sample(handler, "agent.where", json!({"agent_id":id})).await);
    }
    published_file("agent-sessions-v1", "where-hub.json", records);
    published_file(
        "agent-sessions-v1",
        "where-many-hub.json",
        vec![cutover_sample(handler, "agent.where_many", json!({"agentIds":location})).await],
    );
    published_file(
        "agent-identity-reads-v1",
        "fleet-identity-hub.json",
        vec![cutover_sample(handler, "agent.fleet_identity", json!({})).await],
    );

    let role = "dreamer.global";
    let key = format!("role.{role}");
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "role".into(),
            key: key.clone(),
            writer: A.into(),
            writer_ep: 0,
            entry_id: "56".repeat(16),
        },
        &json!({"v":1,"holder":A,"holder_ep":0,"writer":A,"gen":1,"lease_ms":60000}),
    )
    .await
    .unwrap();
    rig.kv
        .update(&key, signed.into_bytes().into(), 0)
        .await
        .unwrap();
    until(|| role_read(handler, role)["holder"] == A && role_read(handler, role)["stale"] == false)
        .await;
    let legacy_role = "dreamer.workspace_02";
    let legacy_key = format!("role.{legacy_role}");
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0), rig.keys.as_ref(),
        &Header { kind:"role".into(), key:legacy_key.clone(), writer:A.into(), writer_ep:0, entry_id:"57".repeat(16) },
        &json!({"v":1,"holder":SAMPLE_SENTINEL,"holder_ep":0,"writer":A,"gen":5,"seeded":true,"lease_ms":60000}),
    ).await.unwrap();
    rig.kv
        .update(&legacy_key, signed.into_bytes().into(), 0)
        .await
        .unwrap();
    until(|| {
        let read = role_read(handler, legacy_role);
        read["holder"] == SAMPLE_SENTINEL && read["stale"] == false
    })
    .await;
    for (op, file) in [
        ("role.read", "read-hub.json"),
        ("role.fence", "fence-hub.json"),
    ] {
        let mut records = Vec::new();
        for name in [role, legacy_role] {
            records.push(cutover_sample(handler, op, json!({"role":name})).await);
        }
        published_file("role-claims-v1", file, records);
    }
    published_file("role-claims-v1", "handback-requested.json", vec![cutover_sample(handler, "role.take", json!({"role":role,"expected_epoch":1,"request_id":"23".repeat(16),"reason":"switch computers"})).await]);
}

#[derive(Default)]
struct Confirm(AtomicUsize);
#[async_trait::async_trait]
impl OperatorConfirmer for Confirm {
    async fn confirm_operator(
        &self,
        summary: &str,
        route: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        assert_eq!(route, ROUTE);
        assert!(summary.contains("residence"));
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

fn bind(handler: &ProjectsHandler, principal: subc_protocol::Principal) {
    handler.route_admissions().insert(
        ROUTE,
        RouteAdmission {
            principal: Some(principal),
            ..Default::default()
        },
    );
}

fn residence(node: &Node, image: Arc<Image>) {
    let leaf = node.leaf.clone();
    let feed = Feed::kv(node.handler.health.clone(), move |_| {
        Binding {
            session: leaf.session(),
            changes: None,
        }
        .residence_watch()
    })
    .with_durable_cursor();
    node.handler
        .activate_hub_residence(node.log.clone(), feed, image)
        .unwrap();
}

#[tokio::test]
async fn hub_bucket_reset_fences_both_feeds_and_keeps_verified_reads_stale() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (_issuer, connector) =
        super::super::watch_credentials::start(&rig._processes.dir, rig.hub_port, B).await;
    let handler = &rig.b.handler;
    bind(handler, subc_protocol::Principal::Direct);
    let hub = Hub::new(connector, handler.health.clone());
    hub.activate();
    let residence = handler
        .activate_hub_residence(
            rig.b.log.clone(),
            hub.residence_watch(),
            Arc::new(Image::default()),
        )
        .unwrap();
    let mut epochs = hub.epoch_watch(B.into());
    assert!(matches!(
        within(epochs.next_event()).await,
        FeedEvent::Connected
    ));
    assert!(matches!(
        within(epochs.next_event()).await,
        FeedEvent::Item(KvDelivery::CaughtUp)
    ));

    let role = "dreamer.global";
    let key = format!("role.{role}");
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "role".into(),
            key: key.clone(),
            writer: A.into(),
            writer_ep: 0,
            entry_id: hub_write::new_op_id().unwrap(),
        },
        &json!({"v":1,"holder":A,"holder_ep":0,"writer":A,"gen":1,"lease_ms":60000}),
    )
    .await
    .unwrap();
    rig.kv.put(&key, signed.into_bytes().into()).await.unwrap();
    until(|| role_read(handler, role)["stale"] == false && role_read(handler, role)["holder"] == A)
        .await;
    let store = store_of(handler);
    let cached = store.hub_kv_cached(&key).unwrap().unwrap();
    let admin = jetstream::new(
        async_nats::connect(format!("127.0.0.1:{}", rig.hub_port))
            .await
            .unwrap(),
    );
    admin.delete_key_value(BUCKET).await.unwrap();
    let replacement = admin
        .create_key_value(kv::Config {
            bucket: BUCKET.into(),
            history: 1,
            max_value_size: MAX_KV_VALUE,
            max_bytes: MAX_KV_BYTES,
            ..Default::default()
        })
        .await
        .unwrap();
    replacement.delete(&key).await.unwrap();

    let reset = tokio::time::timeout(Duration::from_secs(30), epochs.next_event())
        .await
        .unwrap();
    assert!(matches!(reset, FeedEvent::BucketReset));
    assert!(!residence.ready());
    let read = role_read(handler, role);
    assert_eq!(read["holder"], A);
    assert_eq!(read["epoch"], 1);
    assert_eq!(read["stale"], true);
    assert_eq!(store.hub_kv_cached(&key).unwrap(), Some(cached));
    assert_eq!(
        error(role_take(handler, role, 1, "00000000000000000000000000000001").await),
        BUCKET_RESET
    );
    let create = AgentOperation::create(99, "Reset blocked this write".into());
    assert_eq!(
        create.plan(&rig.b).append().await.unwrap_err().code,
        BUCKET_RESET
    );
    assert_eq!(
        rig.b
            .log
            .writer_episode(Instant::now() + Duration::from_secs(5))
            .await
            .unwrap_err()
            .code,
        BUCKET_RESET
    );
    let mut metrics = json!({});
    handler.health.hub.report(&mut metrics);
    assert_eq!(metrics["hubBucketResets"], 1);
    assert_eq!(metrics["hubResetBucket"], BUCKET);
    assert_eq!(metrics["hubKvWatchLive"], false);
}

#[tokio::test]
async fn hub_reader_regression_residence_replays_after_transient_watch_error() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let agent = "agent_0000000000000042";
    migrate_identity(&rig.b, agent, "Blip");
    let image = Arc::new(Image(HashMap::from([(agent.into(), (Some(A.into()), 1))])));
    let (inject, injected) = tokio::sync::oneshot::channel();
    let leaf = rig.b.leaf.clone();
    let mut injected = Some(injected);
    let feed = Feed::kv(rig.b.handler.health.clone(), move |_| {
        let leaf = leaf.clone();
        let injected = injected.take();
        async move {
            let kv = leaf.session().kv().await?;
            let watch = kv
                .watch_with_history(">")
                .await
                .map_err(|error| Failure::kind("kv_watch", error.kind()))?;
            let values = kv
                .status()
                .await
                .map_err(|error| Failure::kind("kv_status", error.kind()))?
                .values();
            let entries = test_snapshot(watch, values == 0);
            let errors = futures_util::stream::iter(injected).then(|injected| async {
                injected.await.unwrap();
                Err(Failure::code("kv_delivery:MissingHeartbeat"))
            });
            Ok(Subscription::new(
                None,
                futures_util::stream::select(entries, errors),
            ))
        }
    })
    .with_durable_cursor();
    rig.b
        .handler
        .activate_hub_residence(rig.b.log.clone(), feed, image)
        .unwrap();
    let mut seeded = record(B, A, 1, false);
    seeded["seeded"] = json!(true);
    let revision = put(&rig, agent, 0, B, seeded).await;
    assert!(!applied(&rig.b, agent, revision).await.integrity_failed);
    until(|| ready_for(&rig.b, agent)).await;
    inject.send(()).unwrap();
    until(|| !ready_for(&rig.b, agent)).await;
    tokio::time::timeout(Duration::from_secs(5), until(|| ready_for(&rig.b, agent)))
        .await
        .expect("idle residence did not replay the already-verified claim");
}

#[tokio::test]
async fn hub_reader_regression_residence_and_role_control_retry_when_writer_is_listed() {
    use crate::hub_membership::tests::{anchor, ChangingRoster};
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let roster = Arc::new(ChangingRoster(tokio::sync::watch::channel(Vec::new()).0));
    let handler = handler(store(&rig._processes, "unlisted-writer"), B);
    let leaf = rig.b.leaf.clone();
    let log = Arc::new(
        handler
            .activate_hub_identity_log(
                leaf.box_id.clone(),
                Link {
                    transport: leaf.clone(),
                    trust: rig.keys.clone(),
                    signer: Arc::new(ClaustrumCredentialResolver::with_target(
                        rig.keys.clone(),
                        0,
                    )),
                    vault: rig.keys.clone(),
                    peers: roster.clone(),
                },
            )
            .unwrap(),
    );
    let node = Node { handler, leaf, log };
    let agent = "agent_0000000000000042";
    migrate_identity(&node, agent, "Blip");
    residence(
        &node,
        Arc::new(Image(HashMap::from([(agent.into(), (Some(C.into()), 1))]))),
    );
    let mut seeded = record(C, C, 1, false);
    seeded["seeded"] = json!(true);
    let revision = put(&rig, agent, 0, C, seeded).await;
    let preference_key = "roleprefer.connector_home";
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "roleprefer".into(),
            key: preference_key.into(),
            writer: C.into(),
            writer_ep: 0,
            entry_id: "59".repeat(16),
        },
        &json!({"v":1,"box":C,"writer":C}),
    )
    .await
    .unwrap();
    let preference_revision = rig
        .kv
        .put(preference_key, signed.into_bytes().into())
        .await
        .unwrap();
    let store = store_of(&node.handler);
    let preference = || store.hub_kv_cached(preference_key).unwrap();
    let held = applied(&node, agent, revision).await;
    assert!(
        !held.integrity_failed,
        "an unlisted writer is not a forged claim"
    );
    assert_eq!(held.accepted_revision, None);
    until(|| preference().is_some()).await;
    assert!(
        !preference().unwrap().integrity_failed,
        "an unlisted writer is not a forged preference"
    );
    assert!(!ready_for(&node, agent));
    roster.0.send_replace(vec![anchor(C, &rig.keys)]);
    until(|| {
        ready_for(&node, agent)
            && preference().unwrap().accepted_revision == Some(preference_revision)
    })
    .await;
    assert_eq!(
        applied(&node, agent, revision).await.accepted_revision,
        Some(revision)
    );
}

async fn until(mut pred: impl FnMut() -> bool) {
    within(async {
        while !pred() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

async fn until_async<F: std::future::Future<Output = bool>>(mut pred: impl FnMut() -> F) {
    within(async {
        while !pred().await {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

fn store_of(handler: &ProjectsHandler) -> RegistryStore {
    handler.with_store(|store| Ok(store.clone())).unwrap()
}

fn where_(handler: &ProjectsHandler, agent: &str) -> HandlerOutcome {
    handler.handle_request(&body("agent.where", json!({"agent_id":agent})), ROUTE)
}

fn error(outcome: HandlerOutcome) -> String {
    match outcome {
        HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => code,
        other => panic!("expected refusal, got {other:?}"),
    }
}

async fn take(handler: &ProjectsHandler, agent: &str, request: &str) -> HandlerOutcome {
    handler
        .handle_active_request(
            &body(
                hub_residence::TAKE,
                json!({"agent_id":agent, "request_key":request}),
            ),
            ROUTE,
        )
        .await
}

fn ready_for(node: &Node, agent: &str) -> bool {
    match where_(&node.handler, agent) {
        HandlerOutcome::Response(bytes) => {
            serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["stale"] == false
        }
        _ => false,
    }
}

fn migrate_identity(node: &Node, agent: &str, name: &str) {
    let store = store_of(&node.handler);
    store.with_principal("test").agent_mutation_with_id("agent.create",
        json!({"role":"assistant","tag":"test","name":name,"request_key":format!("migrate-{agent}")}), 700, Some(agent)).unwrap();
}

fn attach_session(node: &Node, agent: &str) {
    // Seed an existing binding so the session route can query the new claim.
    // Registering through the route would be refused while the agent's first
    // claim is still being written, which this test doesn't exercise.
    store_of(&node.handler)
        .register_session(&entorhinal_core::SessionRegistration {
            agent_id: agent,
            ref_kind: "runner",
            session_ref: "sample",
            harness: "test",
            session_id: "session",
            project_root: None,
            registered_by: "orchestrator",
            runner: Some("runner"),
            role: None,
            network: None,
            now_ms: 700,
        })
        .unwrap();
}

fn session(node: &Node) -> Value {
    result(node.handler.handle_request(
        &body(
            "agent.session_residence",
            json!({"runner":"runner","session":"session"}),
        ),
        ROUTE,
    ))
}

async fn put(rig: &IdentityRig, agent: &str, revision: u64, writer: &str, record: Value) -> u64 {
    put_residence(rig, agent, revision, writer, record, false).await
}

async fn put_residence(
    rig: &IdentityRig,
    agent: &str,
    revision: u64,
    writer: &str,
    record: Value,
    corrupt_signature: bool,
) -> u64 {
    let key = format!("residence.{agent}");
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "residence".into(),
            key: key.clone(),
            writer: writer.into(),
            writer_ep: 0,
            entry_id: hub_write::new_op_id().unwrap(),
        },
        &record,
    )
    .await
    .unwrap();
    let mut bytes = signed.into_bytes();
    if corrupt_signature {
        let mut envelope: Value = serde_json::from_slice(&bytes).unwrap();
        envelope["sig"] = json!("0".repeat(128));
        bytes = entorhinal_catalog::jcs::to_string(&envelope)
            .unwrap()
            .into_bytes();
    }
    rig.kv.update(key, bytes.into(), revision).await.unwrap()
}

fn record(writer: &str, holder: &str, gen: i64, forced: bool) -> Value {
    json!({"v":1,"writer":writer,"holder":holder,"holder_ep":0,"gen":gen,"forced":forced})
}

async fn applied(
    node: &Node,
    agent: &str,
    revision: u64,
) -> entorhinal_core::hub_store::HubKvCached {
    let store = store_of(&node.handler);
    let key = format!("residence.{agent}");
    until(|| {
        store
            .hub_kv_cached(&key)
            .unwrap()
            .is_some_and(|row| row.revision == revision)
    })
    .await;
    store.hub_kv_cached(&key).unwrap().unwrap()
}

#[tokio::test]
async fn hub_residence_create_restarts_pending_and_preserves_session_precedence() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    migrate_identity(&rig.a, MIGRATED, "Migrated");
    let image = Arc::new(Image(HashMap::from([(MIGRATED.into(), (None, 0))])));
    residence(&rig.a, image.clone());
    residence(&rig.b, image.clone());
    until(|| ready_for(&rig.a, MIGRATED)).await;
    bind(
        &rig.a.handler,
        subc_protocol::Principal::Reserved {
            module_id: "prefrontal".into(),
        },
    );
    rig.a.leaf.refuse_kv.store(true, Ordering::Release);
    let request =
        json!({"role":"assistant","name":"Pending","tag":"test","request_key":"pending-create"});
    let out = rig
        .a
        .handler
        .handle_active_request(&body("agent.create", request.clone()), ROUTE)
        .await;
    assert_eq!(error(out), "hub_write_unsettled");
    within(through(&rig.a.handler, 1)).await;
    within(through(&rig.b.handler, 1)).await;
    let local_store = store_of(&rig.a.handler);
    let (_, row) = local_store
        .hub_created_agent("pending-create")
        .unwrap()
        .unwrap();
    let agent = row.agent_id;
    assert_eq!(
        store_of(&rig.b.handler).hub_agent_creator(&agent).unwrap(),
        Some((A.into(), 0))
    );
    attach_session(&rig.b, &agent);
    assert_eq!(result(where_(&rig.b.handler, &agent))["stale"], true);
    assert_eq!(session(&rig.b)["state"], "unknown");
    bind(&rig.b.handler, subc_protocol::Principal::Direct);
    assert_eq!(
        error(take(&rig.b.handler, &agent, "cannot-take-pending").await),
        "claim_pending"
    );

    let Node {
        handler: old_handler,
        leaf,
        log,
    } = rig.a;
    old_handler.stop_hub_residence().await;
    crate::tests::stop_catch_up(&old_handler).await;
    drop((old_handler, log, local_store));
    let mut restarted = ProjectsHandler::with_runtime(A.into(), || 800);
    restarted.feed_clock = Arc::new(FrozenFeedClock);
    *restarted.store.lock().unwrap() = Some(store(&rig._processes, "box-a"));
    rig.a = Node {
        log: activate(&restarted, leaf.clone(), &rig.keys),
        leaf,
        handler: restarted,
    };
    rig.a.leaf.refuse_kv.store(false, Ordering::Release);
    residence(&rig.a, image);
    until(|| ready_for(&rig.b, &agent)).await;
    let location = result(where_(&rig.b.handler, &agent));
    assert_eq!(location["holder"], A);
    assert_eq!(location["epoch"], 1);
    assert_eq!(session(&rig.b)["state"], "elsewhere");
    bind(
        &rig.a.handler,
        subc_protocol::Principal::Reserved {
            module_id: "prefrontal".into(),
        },
    );
    let replay = result(
        rig.a
            .handler
            .handle_active_request(&body("agent.create", request), ROUTE)
            .await,
    );
    assert_eq!(replay["agent"]["agent_id"], agent);
    assert_eq!(
        rig.a
            .leaf
            .session()
            .log_head(Instant::now() + Duration::from_secs(2))
            .await
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn hub_residence_forced_take_removed_and_engram_holders() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let confirm = Arc::new(Confirm::default());
    rig.b.handler.operator_confirmer = confirm.clone();
    let lost = "agent_0000000000000041";
    let old_episode = "agent_0000000000000042";
    migrate_identity(&rig.b, MIGRATED, "Engram held");
    migrate_identity(&rig.b, lost, "Removed held");
    migrate_identity(&rig.b, old_episode, "Past episode");
    attach_session(&rig.b, lost);
    let engram = "engram:old-author";
    let image = Arc::new(Image(HashMap::from([
        (MIGRATED.into(), (Some(engram.into()), 5)),
        (lost.into(), (Some(A.into()), 1)),
    ])));
    residence(&rig.b, image);
    for (id, holder, gen) in [(MIGRATED, engram, 5), (lost, A, 1)] {
        let mut seeded = record(B, holder, gen, false);
        seeded["seeded"] = json!(true);
        let revision = put(&rig, id, 0, B, seeded).await;
        assert!(!applied(&rig.b, id, revision).await.integrity_failed);
    }
    bind(
        &rig.b.handler,
        subc_protocol::Principal::Reserved {
            module_id: "prefrontal".into(),
        },
    );
    assert_eq!(
        error(take(&rig.b.handler, MIGRATED, "not-direct").await),
        "caller_forbidden"
    );
    bind(&rig.b.handler, subc_protocol::Principal::Direct);
    assert_eq!(
        error(take(&rig.b.handler, lost, "still-member").await),
        "holder_is_member"
    );
    assert_eq!(confirm.0.load(Ordering::Relaxed), 0);
    assert_eq!(session(&rig.b)["state"], "elsewhere");

    let anchor = hub_membership::tests::anchor(A, &rig.keys);
    let removal = json!({"box_id":A,"epoch":2,"removal_id":"remove-a","key_history":[anchor.fleet_record_ed25519]});
    let id = hub_write::new_op_id().unwrap();
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "member.removed".into(),
            key: LOG_SUBJECT.into(),
            writer: B.into(),
            writer_ep: 0,
            entry_id: id.clone(),
        },
        &removal,
    )
    .await
    .unwrap();
    rig.b
        .leaf
        .session()
        .append(
            signed.into_bytes(),
            0,
            &id,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    within(through(&rig.b.handler, 1)).await;
    assert_eq!(session(&rig.b)["state"], "unknown");
    assert!(result(where_(&rig.b.handler, lost))
        .get("holder_box")
        .is_none());
    for (agent, label, expected) in [(lost, "removed", 2), (MIGRATED, "engram", 6)] {
        let revision = result(take(&rig.b.handler, agent, label).await)["revision"]
            .as_u64()
            .unwrap();
        let row = applied(&rig.b, agent, revision).await;
        assert_eq!(row.body.unwrap(), record(B, B, expected, true));
        assert_eq!(result(where_(&rig.b.handler, agent))["status"], "held_here");
    }
    assert_eq!(confirm.0.load(Ordering::Relaxed), 2);
    result(take(&rig.b.handler, lost, "removed").await);
    assert_eq!(confirm.0.load(Ordering::Relaxed), 2);
    assert_eq!(session(&rig.b)["state"], "here");

    // Even without a cached predecessor, the reader can verify a signature
    // from the closed episode without letting the old holder serve the agent.
    // Re-admission does not revive that authority.
    let late = put(&rig, old_episode, 0, A, record(A, A, 3, true)).await;
    assert!(!applied(&rig.b, old_episode, late).await.integrity_failed);
    assert!(result(where_(&rig.b.handler, old_episode))
        .get("holder_box")
        .is_none());
    let id = hub_write::new_op_id().unwrap();
    let admission = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "member.admitted".into(),
            key: LOG_SUBJECT.into(),
            writer: B.into(),
            writer_ep: 0,
            entry_id: id.clone(),
        },
        &json!({"box_id":A,"after_removal_id":"remove-a","request_id":"rejoin-a"}),
    )
    .await
    .unwrap();
    rig.b
        .leaf
        .session()
        .append(
            admission.into_bytes(),
            1,
            &id,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    within(through(&rig.b.handler, 2)).await;
    assert!(result(where_(&rig.b.handler, old_episode))
        .get("holder_box")
        .is_none());
    let revision = result(take(&rig.b.handler, old_episode, "take-past-episode").await)["revision"]
        .as_u64()
        .unwrap();
    assert_eq!(
        applied(&rig.b, old_episode, revision).await.body.unwrap(),
        record(B, B, 4, true)
    );
}

#[tokio::test]
async fn hub_residence_watch_refuses_forged_forced_values_and_continues_other_keys() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let ids = (0..6)
        .map(|n| format!("agent_{n:016x}"))
        .collect::<Vec<_>>();
    for (n, id) in ids.iter().enumerate() {
        migrate_identity(&rig.b, id, &format!("Reader case {n}"));
    }
    let image = Arc::new(Image(
        ids[2..5]
            .iter()
            .map(|id| (id.clone(), (Some(A.into()), 1)))
            .collect(),
    ));
    residence(&rig.b, image);
    for (i, writer, holder, corrupt_signature) in [
        (0, C, C, true),
        (1, B, A, false),
        (2, C, C, true),
        (3, B, A, false),
    ] {
        let id = &ids[i];
        let mut previous = 0;
        if i >= 2 {
            let mut seeded = record(A, A, 1, false);
            seeded["seeded"] = json!(true);
            previous = put(&rig, id, 0, A, seeded.clone()).await;
            assert_eq!(applied(&rig.b, id, previous).await.body, Some(seeded));
        }
        let forged = put_residence(
            &rig,
            id,
            previous,
            writer,
            record(writer, holder, 2, true),
            corrupt_signature,
        )
        .await;
        let refused = applied(&rig.b, id, forged).await;
        assert!(
            refused.integrity_failed,
            "case {i} must mark only its own row"
        );
        assert_eq!(
            refused.accepted_revision,
            (previous != 0).then_some(previous)
        );
        assert_eq!(error(where_(&rig.b.handler, id)), "claim_integrity_failed");
    }
    let agent = &ids[4];
    let mut seeded = record(A, A, 1, false);
    seeded["seeded"] = json!(true);
    let a = put(&rig, agent, 0, A, seeded).await;
    assert_eq!(applied(&rig.b, agent, a).await.body.unwrap()["holder"], A);
    let b = put(&rig, agent, a, A, record(A, B, 2, false)).await;
    assert_eq!(applied(&rig.b, agent, b).await.body.unwrap()["holder"], B);
    let c = put(&rig, agent, b, B, record(B, C, 3, false)).await;
    assert_eq!(applied(&rig.b, agent, c).await.body.unwrap()["holder"], C);
    assert_eq!(result(where_(&rig.b.handler, agent))["epoch"], 3);

    attach_session(&rig.b, agent);
    let key = format!("residence.{agent}");
    let current = rig.kv.entry(&key).await.unwrap().unwrap();
    let service = rig.b.handler.residence().unwrap();
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut older = current.clone();
    older.operation = kv::Operation::Delete;
    older.revision = b;
    service.observe(&older, deadline).await.unwrap();
    service.observe(&current, deadline).await.unwrap();
    assert_eq!(result(where_(&rig.b.handler, agent))["epoch"], 3);
    let mut different = current.clone();
    different.value = b"changed bytes at same revision"[..].into();
    service.observe(&different, deadline).await.unwrap();
    service.observe(&current, deadline).await.unwrap();
    assert_eq!(
        error(where_(&rig.b.handler, agent)),
        "claim_integrity_failed"
    );
    rig.kv.purge_expect_revision(&key, Some(c)).await.unwrap();
    let tombstone = rig.kv.entry(&key).await.unwrap().unwrap().revision;
    let refused = applied(&rig.b, agent, tombstone).await;
    assert!(refused.integrity_failed);
    assert_eq!(refused.body.unwrap()["holder"], C);
    assert_eq!(session(&rig.b)["state"], "unknown");
    let refused_registration = rig
        .b
        .handler
        .register_session(
            json!({
                "agent_id":agent,"ref_kind":"runner","ref":"sample","harness":"test",
                "session_id":"session","runner":"runner"
            }),
            crate::sessions::SessionRole::Orchestrator,
        )
        .unwrap_err();
    assert_eq!(refused_registration.code, "claim_integrity_failed");
    let landed = put(&rig, agent, tombstone, C, record(C, B, 4, false)).await;
    assert!(!applied(&rig.b, agent, landed).await.integrity_failed);
    // The SQLite row commits before the watcher publishes its fresh revision.
    until(|| ready_for(&rig.b, agent)).await;
    assert_eq!(session(&rig.b)["state"], "here");
    assert_eq!(
        rig.b
            .handler
            .health
            .hub
            .kv_records_refused
            .load(Ordering::Relaxed),
        6
    );
}

#[derive(Default)]
struct RoleConfirm(AtomicUsize);

#[async_trait::async_trait]
impl OperatorConfirmer for RoleConfirm {
    async fn confirm_operator(
        &self,
        summary: &str,
        route: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        assert_eq!(route, ROUTE);
        assert!(summary.contains("Take role"));
        self.0.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

async fn role_take(
    handler: &ProjectsHandler,
    role: &str,
    epoch: i64,
    request_id: &str,
) -> HandlerOutcome {
    handler.handle_active_request(&body("role.take", json!({
        "role": role, "expected_epoch": epoch, "request_id": request_id, "reason": "switch computers",
    })), ROUTE).await
}

fn role_read(handler: &ProjectsHandler, role: &str) -> Value {
    result(handler.handle_request(&body("role.read", json!({"role":role})), ROUTE))
}

#[tokio::test]
async fn lease_handback_role_take_replays_preference_and_force_takes_removed_holder() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let confirm = Arc::new(RoleConfirm::default());
    rig.b.handler.operator_confirmer = confirm.clone();
    residence(&rig.b, Arc::new(Image::default()));
    bind(&rig.b.handler, subc_protocol::Principal::Direct);

    let role = "dreamer.global";
    let key = format!("role.{role}");
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "role".into(),
            key: key.clone(),
            writer: A.into(),
            writer_ep: 0,
            entry_id: hub_write::new_op_id().unwrap(),
        },
        &json!({"v":1,"holder":A,"holder_ep":0,"writer":A,"gen":1,"lease_ms":60000}),
    )
    .await
    .unwrap();
    rig.kv
        .update(&key, signed.into_bytes().into(), 0)
        .await
        .unwrap();
    until(|| {
        role_read(&rig.b.handler, role)["stale"] == false
            && role_read(&rig.b.handler, role)["epoch"] == 1
    })
    .await;

    let request = "00000000000000000000000000000001";
    let store = store_of(&rig.b.handler);
    let preference_reply = json!({"epoch":1,"held_here":false,"handback_requested":true});
    assert_eq!(
        result(role_take(&rig.b.handler, role, 1, request).await),
        preference_reply
    );
    let prefer = store.hub_attempt(request).unwrap().unwrap();
    assert_eq!(prefer.write.key, "roleprefer.dreamer.global");
    assert_eq!(
        result(role_take(&rig.b.handler, role, 1, request).await),
        preference_reply
    );
    assert_eq!(
        error(role_take(&rig.b.handler, role, 2, request).await),
        "request_key_conflict"
    );
    assert_eq!(confirm.0.load(Ordering::Relaxed), 1);
    assert_eq!(role_read(&rig.b.handler, role)["holder"], A);

    let anchor = hub_membership::tests::anchor(A, &rig.keys);
    let removal = json!({"box_id":A,"epoch":2,"removal_id":"remove-role-holder","key_history":[anchor.fleet_record_ed25519]});
    let id = hub_write::new_op_id().unwrap();
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "member.removed".into(),
            key: LOG_SUBJECT.into(),
            writer: B.into(),
            writer_ep: 0,
            entry_id: id.clone(),
        },
        &removal,
    )
    .await
    .unwrap();
    rig.b
        .leaf
        .session()
        .append(
            signed.into_bytes(),
            0,
            &id,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    within(through(&rig.b.handler, 1)).await;
    let forced_id = "00000000000000000000000000000002";
    let forced_reply = json!({"epoch":2,"held_here":true});
    assert_eq!(
        result(role_take(&rig.b.handler, role, 1, forced_id).await),
        forced_reply
    );
    until(|| role_read(&rig.b.handler, role)["holder"] == B).await;
    let cached = store.hub_kv_cached(&key).unwrap().unwrap();
    assert_eq!(
        cached.body.unwrap(),
        json!({"v":1,"holder":B,"holder_ep":0,"writer":B,"gen":2,"forced":true,"lease_ms":60000})
    );
    assert_eq!(
        store.hub_attempt(forced_id).unwrap().unwrap().write.key,
        key
    );
    assert_eq!(
        result(role_take(&rig.b.handler, role, 1, forced_id).await),
        forced_reply
    );
    assert_eq!(
        result(role_take(&rig.b.handler, role, 1, request).await),
        preference_reply
    );
    assert_eq!(confirm.0.load(Ordering::Relaxed), 2);
}

struct RoleNotices(tokio::sync::mpsc::UnboundedSender<Value>);

#[async_trait::async_trait]
impl crate::watches::WatchTransport for RoleNotices {
    async fn emit(&self, body: Vec<u8>) -> Result<(), crate::HandlerError> {
        self.0.send(serde_json::from_slice(&body).unwrap()).unwrap();
        Ok(())
    }
    async fn ended(&self) {
        std::future::pending().await
    }
}

#[tokio::test]
async fn lease_handback_role_take_preference_steers_a_vacant_role() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let confirm = Arc::new(RoleConfirm::default());
    rig.b.handler.operator_confirmer = confirm.clone();
    for node in [&rig.a, &rig.b] {
        residence(node, Arc::new(Image::default()));
    }
    bind(&rig.b.handler, subc_protocol::Principal::Direct);
    let role = "dreamer.pj-vacant";
    until(|| {
        role_read(&rig.a.handler, role)["stale"] == false
            && role_read(&rig.b.handler, role)["stale"] == false
    })
    .await;
    let request_id = "00000000000000000000000000000003";
    let reply = json!({"epoch":0,"held_here":false,"handback_requested":true});
    assert_eq!(
        result(role_take(&rig.b.handler, role, 0, request_id).await),
        reply
    );
    assert!(role_read(&rig.b.handler, role)["holder"].is_null());
    let route = (21, 1);
    for node in [&rig.a, &rig.b] {
        node.handler.route_admissions().insert(
            route,
            RouteAdmission {
                principal: Some(crate::tests::reserved("magic-context")),
                ..Default::default()
            },
        );
    }
    let (a_tx, mut a_rx) = tokio::sync::mpsc::unbounded_channel();
    let (b_tx, mut b_rx) = tokio::sync::mpsc::unbounded_channel();
    let a_sink = RoleNotices(a_tx);
    let b_sink = RoleNotices(b_tx);
    let holds = async {
        loop {
            tokio::select! {
                notice = a_rx.recv() => {
                    let notice = notice.unwrap();
                    assert_eq!(notice["holding"], false);
                    rig.a.handler.ack_role_lease(json!({"role":role,"seq":notice["seq"]}), route).unwrap();
                }
                notice = b_rx.recv() => {
                    let notice = notice.unwrap();
                    rig.b.handler.ack_role_lease(json!({"role":role,"seq":notice["seq"]}), route).unwrap();
                    if notice["holding"] == true {
                        assert_eq!(notice["gen"], 1);
                        break;
                    }
                }
            }
        }
        assert_eq!(role_read(&rig.b.handler, role)["holder"], B);
        assert_eq!(
            result(role_take(&rig.b.handler, role, 0, request_id).await),
            reply
        );
        assert_eq!(confirm.0.load(Ordering::Relaxed), 1);
    };
    within(async {
        tokio::select! {
            biased;
            result = rig.a.handler.serve_role_lease(&a_sink, role.into(), route) => panic!("A's watch ended: {result:?}"),
            result = rig.b.handler.serve_role_lease(&b_sink, role.into(), route) => panic!("B's watch ended: {result:?}"),
            _ = holds => {}
        }
    }).await;
}

mod moves;

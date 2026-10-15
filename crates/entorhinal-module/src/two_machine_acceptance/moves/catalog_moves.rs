use super::*;
use crate::{
    catalog_blob::BlobStore,
    catalog_consumer::{self, ConsumerDiscovery},
    catalog_trust::tests::vectors,
};
use base64::Engine;
use entorhinal_core::catalog_agent::{CatalogPin, PinWriteRequest};
use tokio::time::Instant;

struct Consumer {
    store: RegistryStore,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl LogTransport for Consumer {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.calls.lock().unwrap().push(method.into());
        if method == "catalog_consumer.prefetch" {
            for pin in params["packages"].as_array().unwrap() {
                for field in ["manifest_sha256", "archive_sha256"] {
                    let digest = pin[field].as_str().unwrap();
                    let bytes = BlobStore::new(self.store.store_folder())
                        .unwrap()
                        .read_verified(digest)
                        .unwrap();
                    assert_eq!(entorhinal_catalog::sha256_hex(bytes), digest);
                }
            }
        }
        let reply = match method {
            "catalog_consumer.check" => json!({"ok":true,"card_section":"Verified skill cache"}),
            "catalog_consumer.prefetch" => json!({"ready":true}),
            _ => panic!("unexpected catalog consumer operation"),
        };
        Ok(serde_json::to_vec(&json!({"result":reply})).unwrap())
    }
}
struct Consumers(Arc<Consumer>);
impl Consumers {
    fn snapshot() -> DaemonSnapshot {
        DaemonSnapshot::new(
            1,
            vec![
                json!({"module_id":WRITER_MODULE,"enabled":true,"state":"running","live":true,"health":"ok"}),
            ],
            vec![
                json!({"module_id":WRITER_MODULE,"ready":true,"control_ops":[],"roles":[{"role":"internal_service","service_id":catalog_consumer::SERVICE,"transport":"bulk","agent_facing":false,"operations":["catalog_consumer.check","catalog_consumer.prefetch"]}],"capabilities":{"provides":[catalog_consumer::SKILL],"requires":[],"must_never_reach":[]}}),
            ],
        )
    }
}
#[async_trait]
impl ConsumerDiscovery for Consumers {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        Ok(Self::snapshot().supervisor)
    }
    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        Ok(Self::snapshot().catalog)
    }
    async fn open(&self, module: &str) -> Result<Arc<dyn LogTransport>, TransportError> {
        assert_eq!(module, WRITER_MODULE);
        Ok(self.0.clone())
    }
}
fn package(large: bool) -> (CatalogPin, Vec<u8>) {
    let v = vectors("record");
    let p = &v["packages"]["skill"];
    let mut archive = base64::engine::general_purpose::STANDARD
        .decode(p["archive_base64"].as_str().unwrap())
        .unwrap();
    if large {
        archive.resize(3_145_700, 0);
    }
    (
        CatalogPin {
            name: "@example/hello-skill".into(),
            version: "1.0.0".into(),
            kind: entorhinal_catalog::Kind::Skill,
            manifest_sha256: p["manifest_sha256"].as_str().unwrap().into(),
            archive_sha256: entorhinal_catalog::sha256_hex(&archive),
            publisher_fingerprint: v["keys"]["trusted"]["fingerprint"].as_str().unwrap().into(),
            root: true,
            range: Some("^1".into()),
        },
        archive,
    )
}
fn grant(mac: &Mac, pin: &CatalogPin, id: &str) {
    mac.store()
        .insert_publisher_grant(
            &entorhinal_core::catalog_grants::PublisherGrant {
                id: id.into(),
                publisher_fingerprint: pin.publisher_fingerprint.clone(),
                operation: entorhinal_core::catalog_grants::Operation::Install,
                placement_agent_id: AGENT.into(),
                ceiling: vec!["cap/example.read@1".into()],
                card_ref: None,
                granted_at_ms: 1,
                revoked_at_ms: None,
            },
            &json!({"principal":"operator"}),
        )
        .unwrap();
}
async fn pins(mac: &Mac, pins: Vec<CatalogPin>, key: &str) {
    drive(mac.machine.handler.catalog_pins_write_wait(
        PinWriteRequest {
            agent_id: AGENT.into(),
            pins,
            request_key: Some(key.into()),
        },
        CORE,
        Instant::now(),
    ))
    .await
    .unwrap();
}
async fn catalog_pair(
    receiver_trusted: bool,
    catch_up: bool,
    large: bool,
) -> (Pair, CatalogPin, Vec<u8>, Arc<Consumer>) {
    let mut pair = Pair::new(&[]).await;
    let (pin, archive) = package(large);
    for mac in [&mut pair.a, &mut pair.b] {
        let consumer = Arc::new(Consumer {
            store: mac.store(),
            calls: Mutex::new(vec![]),
        });
        mac.machine.handler.catalog_agent.consumers = Arc::new(Consumers(consumer));
        mac.discovery
            .daemon
            .restart(snapshot(&[("entorhinal", "package-closure")], true, true));
        *mac.discovery.catalog.lock().unwrap() =
            Some(Arc::new(mac.machine.handler.catalog_move_context()));
    }
    pins(&pair.a, vec![pin.clone()], "pins-1").await;
    if catch_up {
        pair.b.machine.catch_up().await;
    }
    BlobStore::new(pair.a.store().store_folder())
        .unwrap()
        .publish(&archive)
        .unwrap();
    grant(&pair.a, &pin, "source-only");
    if receiver_trusted {
        grant(&pair.b, &pin, "receiver-only");
    }
    let consumer = Arc::new(Consumer {
        store: pair.b.store(),
        calls: Mutex::new(vec![]),
    });
    pair.b.machine.handler.catalog_agent.consumers = Arc::new(Consumers(consumer.clone()));
    *pair.b.discovery.catalog.lock().unwrap() =
        Some(Arc::new(pair.b.machine.handler.catalog_move_context()));
    (pair, pin, archive, consumer)
}
fn no_claim_take(mac: &Mac) {
    assert!(
        !mac.records
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|(method, p)| method == "record.cas" && p["subject"] == format!("claim:{AGENT}")),
        "receiver took a claim before refusing"
    );
}
#[tokio::test(start_paused = true)]
async fn catalog_move_transfers_verified_blobs_prefetches_ready_and_chunks() {
    let (pair, pin, archive, consumer) = catalog_pair(true, true, true).await;
    let local_permissions = pair.b.store().publisher_grants().unwrap();
    let journal = pair.b.store().agent_snapshot().unwrap().generation;
    let cut = pair.published("catalog-full").await;
    assert_eq!(
        pair.a.row(&cut).plan["pin_digest"],
        pair.a.store().catalog_pin_digest(AGENT).unwrap()
    );
    pair.finish(&cut).await;
    let blobs = BlobStore::new(pair.b.store().store_folder()).unwrap();
    assert_eq!(blobs.read_verified(&pin.archive_sha256).unwrap(), archive);
    assert!(blobs.read_verified(&pin.manifest_sha256).is_ok());
    let generations = pair.b.store().catalog_agent_generations(AGENT).unwrap();
    assert_eq!(generations.len(), 1);
    assert_eq!(generations[0].state, "ready");
    assert_eq!(generations[0].closure, vec![pin]);
    assert_eq!(
        serde_json::to_value(pair.b.store().publisher_grants().unwrap()).unwrap(),
        serde_json::to_value(local_permissions).unwrap()
    );
    assert_eq!(pair.b.store().agent_snapshot().unwrap().generation, journal);
    assert!(
        pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty(),
        "synthetic closure leaked into session bindings"
    );
    assert!(consumer
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|m| m == "catalog_consumer.prefetch"));
    let admits = pair
        .a
        .door
        .session_calls()
        .into_iter()
        .filter(|(m, _)| m == "session.admit")
        .collect::<Vec<_>>();
    assert!(admits.len() >= 2);
    assert!(admits
        .iter()
        .all(|(_, p)| p["bytes"].as_str().unwrap().len() / 2 <= 3_145_692));
}
#[tokio::test(start_paused = true)]
async fn catalog_move_pin_digest_catches_up_before_import() {
    let (pair, pin, _, _) = catalog_pair(true, false, false).await;
    assert!(pair.b.store().catalog_pins(AGENT).unwrap().is_empty());
    let cut = pair.published("catalog-catchup").await;
    pair.finish(&cut).await;
    assert_eq!(pair.b.store().catalog_pins(AGENT).unwrap(), vec![pin]);
    assert_eq!(
        pair.b.store().catalog_agent_generations(AGENT).unwrap()[0].state,
        "ready"
    );
}
#[tokio::test(start_paused = true)]
async fn catalog_move_persistent_pin_mismatch_aborts_before_claim() {
    let (pair, mut pin, _, _) = catalog_pair(true, true, false).await;
    let cut = pair.published("catalog-mismatch").await;
    pin.range = Some("*".into());
    pins(&pair.a, vec![pin], "pins-after-admission").await;
    pair.b.machine.catch_up().await;
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    let row = pair.b.row(&cut);
    assert_eq!(row.reason.as_deref(), Some("pin_digest_mismatch"));
    assert_ne!(
        row.detail.as_ref().unwrap()["source_pin_digest"],
        row.detail.as_ref().unwrap()["receiver_pin_digest"]
    );
    no_claim_take(&pair.b);
    assert!(!pair.b.store().store_folder().join("catalog/moves").exists());
}
#[tokio::test(start_paused = true)]
async fn catalog_move_missing_local_trust_aborts_before_staging_including_lid_close() {
    for lid in [false, true] {
        let (pair, pin, _, _) = catalog_pair(false, true, false).await;
        if lid {
            pair.a
                .store()
                .set_move_setting(entorhinal_core::move_store::MoveSetting {
                    lid_close_moves: true,
                })
                .unwrap();
            pair.a.machine.handler.route_admissions().insert(
                CORE,
                crate::RouteAdmission::from_bind(
                    Some(subc_protocol::Principal::Reserved {
                        module_id: "engram".into(),
                    }),
                    None,
                ),
            );
        }
        let cut = pair
            .published(if lid { "catalog-lid" } else { "catalog-trust" })
            .await;
        pair.b.pickup().await;
        pair.b.receiver_until(&cut, MoveStep::Aborted).await;
        let row = pair.b.row(&cut);
        assert_eq!(row.reason.as_deref(), Some("trust_missing"));
        assert_eq!(
            row.detail.as_ref().unwrap()["publishers"][0]["publisher_fingerprint"],
            pin.publisher_fingerprint
        );
        assert_eq!(
            row.detail.as_ref().unwrap()["publishers"][0]["publisher"],
            "@example"
        );
        assert!(row.hold.is_none());
        assert!(!pair.b.store().store_folder().join("catalog/moves").exists());
        assert!(pair
            .b
            .store()
            .catalog_agent_generations(AGENT)
            .unwrap()
            .is_empty());
        no_claim_take(&pair.b);
    }
}

async fn tampered_move(rehash: bool) {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let cut = pair.published("catalog-tamper").await;
    let item = pair
        .a
        .door
        .cut(&cut)
        .unwrap()
        .items
        .into_iter()
        .find(|i| i.stream == "reserved:entorhinal")
        .unwrap();
    let reply = pair
        .a
        .door
        .client()
        .session(
            "session.read",
            json!({"sessionRef":item.session_ref,"fromOffset":0,"limit":item.end_offset}),
        )
        .await
        .unwrap();
    let mut bytes = crate::catalog_move::stream_bytes(reply["bytes"].as_str().unwrap()).unwrap();
    *bytes.last_mut().unwrap() ^= 1;
    pair.a.door.replace_payload(&cut, bytes, rehash);
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    let row = pair.b.row(&cut);
    assert_eq!(row.detail.as_ref().unwrap()["code"], "digest_mismatch");
    if !rehash {
        assert_eq!(
            row.detail.as_ref().unwrap()["reason"],
            "staging_digest_mismatch"
        );
    }
    assert!(!pair.b.store().store_folder().join("catalog/blobs").exists());
    assert!(pair
        .b
        .store()
        .catalog_agent_generations(AGENT)
        .unwrap()
        .is_empty());
    no_claim_take(&pair.b);
}
#[tokio::test(start_paused = true)]
async fn catalog_move_tampered_cut_digest_refuses_before_publish() {
    tampered_move(false).await;
}
#[tokio::test(start_paused = true)]
async fn catalog_move_tampered_pinned_blob_refuses_even_with_matching_cut_hash() {
    tampered_move(true).await;
}

struct Confirm;
#[async_trait]
impl crate::agent_ops::OperatorConfirmer for Confirm {
    async fn confirm_operator(
        &self,
        _: &str,
        _: RouteKey,
    ) -> Result<(), crate::agent_ops::OperatorConfirmError> {
        Ok(())
    }
}
#[tokio::test(start_paused = true)]
async fn catalog_move_operator_trusts_real_stored_pins_then_retries() {
    let (mut pair, pin, archive, _) = catalog_pair(false, true, false).await;
    let cut = pair.published("catalog-before-trust").await;
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    pair.a.source_until(&cut, MoveStep::Aborted).await;
    // Publisher trust grants only the capabilities in the selected package's
    // manifest. A cached archive lets the command verify them against the
    // shared pin's digest without accepting a caller-supplied capability list.
    BlobStore::new(pair.b.store().store_folder())
        .unwrap()
        .publish(&archive)
        .unwrap();
    pair.b.machine.handler.operator_confirmer = Arc::new(Confirm);
    pair.b.machine.handler.route_admissions().insert(
        DIRECT,
        crate::RouteAdmission::from_bind(Some(subc_protocol::Principal::Direct), None),
    );
    let request = json!({"method":crate::catalog_ops::PUBLISHER_TRUST,"params":{"agent_id":AGENT,"publisher_fingerprint":pin.publisher_fingerprint}});
    let reply = drive(
        pair.b
            .machine
            .handler
            .handle_served_request(&serde_json::to_vec(&request).unwrap(), DIRECT),
    )
    .await;
    let HandlerOutcome::Response(body) = reply else {
        panic!("trust command refused: {reply:?}")
    };
    assert_eq!(
        serde_json::from_slice::<Value>(&body).unwrap()["result"]["operation"],
        "install"
    );
    let cut = pair.published("catalog-after-trust").await;
    pair.finish(&cut).await;
    assert_eq!(
        pair.b.store().catalog_agent_generations(AGENT).unwrap()[0].state,
        "ready"
    );
}
#[tokio::test(start_paused = true)]
async fn catalog_move_legacy_target_wait_is_visible_and_operator_can_abort() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let cut = pair.published("catalog-legacy").await;
    elapse(12_345);
    let mut source = pair.a.source(&cut);
    drive(source.tick()).await.unwrap();
    assert_eq!(pair.a.row(&cut).step, MoveStep::Published);
    let status = pair
        .a
        .machine
        .call("agent.move_status", json!({"cut_id":cut}))
        .await
        .unwrap();
    assert_eq!(status["status"], "waiting_for_target");
    assert_eq!(
        status["message"],
        "waiting for the target Mac to pick up the move"
    );
    assert_eq!(status["waiting_ms"], 12_345);
    let where_ = pair.a.where_().await;
    assert_eq!(where_["moving"]["status"], "waiting_for_target");
    assert_eq!(where_["moving"]["waiting_ms"], 12_345);
    pair.a.abort(&cut);
    pair.a.source_until(&cut, MoveStep::Aborted).await;
    assert_eq!(pair.a.record(&cut).await.state, MoveState::Aborted);
}
#[tokio::test(start_paused = true)]
async fn catalog_move_back_uses_a_new_session_container() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let first = pair.published("catalog-outbound").await;
    pair.finish(&first).await;
    let second = pair.b.admit_to("Desk Mac", "catalog-return").await.unwrap();
    pair.b
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    pair.b.source_until(&second, MoveStep::Published).await;
    let item = |cut: &str| {
        pair.a
            .door
            .cut(cut)
            .unwrap()
            .items
            .into_iter()
            .find(|i| i.stream == "reserved:entorhinal")
            .unwrap()
            .session_ref
    };
    assert_ne!(item(&first), item(&second));
    pair.a.pickup().await;
    pair.a.receiver_until(&second, MoveStep::Activated).await;
    pair.b.source_until(&second, MoveStep::Released).await;
    pair.a.receiver_until(&second, MoveStep::Done).await;
}

#[tokio::test(start_paused = true)]
async fn catalog_move_root_trust_uses_receiver_root_not_source_grants() {
    use crate::{
        catalog_network::{Limits, RegistryClient},
        catalog_trust::CatalogTrust,
        fake_registry::{FakeRegistry, Reply},
    };
    let (mut pair, mut pin, _, _) = catalog_pair(false, true, false).await;
    let registry = FakeRegistry::start().await;
    let vectors = vectors("statements");
    let cert = &vectors["ck-key-cert/v1"]["cases"][0]["served"];
    let statement = &cert["statement"];
    let pk = statement["public_key"].as_str().unwrap();
    pin.name = "@cortexkit/root-skill".into();
    pin.publisher_fingerprint = entorhinal_catalog::key_fingerprint(pk).unwrap();
    pins(&pair.a, vec![pin.clone()], "root-pins").await;
    pair.b.machine.catch_up().await;
    pair.b
        .store()
        .catalog_registry_add(
            "https://registry.example.invalid",
            &["@cortexkit".into()],
            false,
            "registry-key",
            vectors["keys"]["registry_status"][0]["public_key_hex"]
                .as_str()
                .unwrap(),
            100,
        )
        .unwrap();
    let reply = json!({"scope":"cortexkit","keys":[{"public_key":pk,"fingerprint":pin.publisher_fingerprint,"valid_from":statement["valid_from"],"valid_until":statement["valid_until"],"rotation":null,"revocation":null,"cert":cert}]});
    registry.serve("/v1/scopes/cortexkit", Reply::bytes(reply.to_string()));
    let url = registry.url.clone();
    pair.b.machine.handler.catalog_agent.sources = Arc::new(move |store, scope| {
        CatalogTrust::with_client(
            store,
            scope,
            RegistryClient::loopback(&url, std::slice::from_ref(&url), Limits::MODULE),
        )
    });
    let digest = pair.b.store().catalog_pin_digest(AGENT).unwrap();
    let context = pair.b.machine.handler.catalog_move_context();
    let error = drive(context.preflight(AGENT, &digest)).await.unwrap_err();
    assert_eq!(error.code, "trust_missing");
    assert_eq!(
        error.detail.unwrap()["publishers"][0]["publisher_fingerprint"],
        pin.publisher_fingerprint
    );
    let context = context.with_test_root(
        vectors["keys"]["root"]["public_key_hex"]
            .as_str()
            .unwrap()
            .into(),
    );
    assert_eq!(
        drive(context.preflight(AGENT, &digest)).await.unwrap(),
        vec![pin.clone()]
    );
    let mut corrupted = reply;
    corrupted["keys"][0]["cert"]["signature"] = json!("00".repeat(64));
    registry.serve("/v1/scopes/cortexkit", Reply::bytes(corrupted.to_string()));
    assert_eq!(
        drive(context.preflight(AGENT, &digest))
            .await
            .unwrap_err()
            .code,
        "trust_missing"
    );
}

#[tokio::test(start_paused = true)]
async fn catalog_move_rechecks_trust_before_claim_and_discards_ready_generation() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let cut = pair.published("catalog-trust-recheck").await;
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Staged).await;
    assert_eq!(
        pair.b.store().catalog_agent_generations(AGENT).unwrap()[0].state,
        "ready"
    );
    pair.b
        .store()
        .revoke_publisher_grant("receiver-only", clock(), &json!({"principal":"operator"}))
        .unwrap();
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    assert_eq!(pair.b.row(&cut).reason.as_deref(), Some("trust_missing"));
    no_claim_take(&pair.b);
    assert!(pair
        .b
        .store()
        .catalog_agent_generations(AGENT)
        .unwrap()
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn catalog_move_participant_rechecks_trust_before_import_staging() {
    let (pair, _, _, _) = catalog_pair(false, true, false).await;
    let cut = pair.published("catalog-import-trust").await;
    pair.b.pickup().await;
    let context = pair.b.machine.handler.catalog_move_context();
    let request = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":cut});
    for op in ["agent_data.diverged", "agent_data.import"] {
        let error = context.execute(op, request.clone()).await.unwrap_err();
        assert_eq!(error.code, "invalid_request");
        assert_eq!(error.detail.unwrap()["reason"], "trust_missing");
        assert!(!pair.b.store().store_folder().join("catalog/moves").exists());
    }
}

#[tokio::test(start_paused = true)]
async fn catalog_move_lost_admit_reply_and_fresh_context_reuse_immutable_bytes() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    pair.a.door.script(
        "session.admit",
        DoorEffect::LoseReply(TransportError::NoReply),
    );
    let cut = pair.published("catalog-lost-admit").await;
    let before = pair.a.door.cut(&cut).unwrap().items;
    *pair.a.discovery.catalog.lock().unwrap() =
        Some(Arc::new(pair.a.machine.handler.catalog_move_context()));
    let context = pair.a.machine.handler.catalog_move_context();
    let request = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":cut});
    let prepare = context
        .execute("agent_data.prepare", request.clone())
        .await
        .unwrap();
    assert_eq!(prepare["items"][0]["digest"], before[0].digest);
    context.execute("agent_data.export", request).await.unwrap();
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Imported).await;
    *pair.b.discovery.catalog.lock().unwrap() =
        Some(Arc::new(pair.b.machine.handler.catalog_move_context()));
    pair.finish(&cut).await;
    assert_eq!(
        pair.b
            .store()
            .catalog_agent_generations(AGENT)
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test(start_paused = true)]
async fn catalog_move_lease_errors_remain_retryable_participant_refusals() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let cut = pair
        .a
        .admit_to("Studio Mac", "catalog-lease")
        .await
        .unwrap();
    let context = pair.a.machine.handler.catalog_move_context();
    let request = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":cut});
    for code in [
        "lease_unavailable",
        "lease_acquiring",
        "lease_held_elsewhere",
    ] {
        pair.a
            .door
            .script("session.admit", DoorEffect::Refuse(code.into()));
        let error = context
            .execute("agent_data.prepare", request.clone())
            .await
            .unwrap_err();
        assert_eq!(error.code, "unavailable");
        assert_eq!(
            crate::move_driver::participant_action("agent_data.prepare", &error.code),
            Some(cortexkit_role_agent_data::errors::DriverAction::Retry)
        );
    }
    context
        .execute("agent_data.prepare", request.clone())
        .await
        .unwrap();
    pair.a.door.script(
        "session.cover",
        DoorEffect::Refuse("lease_unavailable".into()),
    );
    let error = context
        .execute("agent_data.export", request)
        .await
        .unwrap_err();
    assert_eq!(error.code, "unavailable");
    assert_eq!(
        crate::move_driver::participant_action("agent_data.export", &error.code),
        Some(cortexkit_role_agent_data::errors::DriverAction::Retry)
    );
}

#[tokio::test(start_paused = true)]
async fn catalog_move_none_status_and_aborted_cut_cannot_import() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let context = pair.b.machine.handler.catalog_move_context();
    let unknown = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":"edededededededededededededededed"});
    assert_eq!(
        context.execute("agent_data.status", unknown).await.unwrap(),
        json!({"step":"none"})
    );
    let cut = pair.published("catalog-abort-none").await;
    pair.b.pickup().await;
    let request = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":cut});
    context
        .execute("agent_data.abort", request.clone())
        .await
        .unwrap();
    let fresh = pair.b.machine.handler.catalog_move_context();
    assert_eq!(
        fresh
            .execute("agent_data.status", request.clone())
            .await
            .unwrap(),
        json!({"step":"aborted"})
    );
    assert_eq!(
        fresh
            .execute("agent_data.import", request)
            .await
            .unwrap_err()
            .code,
        "invalid_state"
    );
    assert!(pair
        .b
        .store()
        .catalog_agent_generations(AGENT)
        .unwrap()
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn catalog_move_content_store_errors_use_allowed_retry_refusals() {
    let (pair, _, _, _) = catalog_pair(true, true, false).await;
    let cut = pair
        .a
        .admit_to("Studio Mac", "catalog-blob-io")
        .await
        .unwrap();
    let root = pair.a.store().store_folder().join("catalog/blobs");
    std::fs::remove_dir_all(&root).unwrap();
    std::fs::write(root, b"not a directory").unwrap();
    let request = json!({"refs":[{"ref_kind":"package-closure","ref":AGENT}],"cutId":cut});
    let error = pair
        .a
        .machine
        .handler
        .catalog_move_context()
        .execute("agent_data.prepare", request)
        .await
        .unwrap_err();
    assert_eq!(error.code, "unavailable");
    assert_eq!(
        crate::move_driver::participant_action("agent_data.prepare", &error.code),
        Some(cortexkit_role_agent_data::errors::DriverAction::Retry)
    );
}

use super::*;
use crate::tests::{flow_stamp, reserved, scratch_descriptor};
use crate::RouteAdmission;
use base64::Engine;
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;

fn vector() -> Value {
    serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/starter-bundle.json"
    ))
    .unwrap()
}
fn fixture(
    label: &str,
) -> (
    std::path::PathBuf,
    RegistryStore,
    std::path::PathBuf,
    cortexkit_store_types::StorageDescriptor,
) {
    let (root, descriptor) = scratch_descriptor(label);
    let store = RegistryStore::open(&descriptor).unwrap();
    let path = store.store_folder().join("catalog/starter-bundle.tar");
    fs::create_dir_all(path.parent().unwrap()).unwrap();
    fs::write(
        &path,
        base64::engine::general_purpose::STANDARD
            .decode(vector()["valid"]["bundle_archive_base64"].as_str().unwrap())
            .unwrap(),
    )
    .unwrap();
    (root, store, path, descriptor)
}
fn test_seeder() -> Seeder {
    Seeder::with_test_root(
        vector()["keys"]["root"]["public_key_hex"]
            .as_str()
            .unwrap()
            .into(),
    )
}
/// The production seeder trusts only the compiled CortexKit root, so a bundle
/// signed under the TEST-ONLY root is refused and leaves nothing behind.
#[test]
fn production_seeder_refuses_test_only_bundle_and_writes_nothing() {
    let (root, store, path, _) = fixture("seed-production-root");
    assert_eq!(
        Seeder::default().seed(&store, &path).unwrap_err().code,
        "signature_invalid"
    );
    assert!(!path.parent().unwrap().join("blobs").exists());
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn seed_content_is_idempotent_and_creates_no_pins_or_authority() {
    let (root, store, path, _) = fixture("seed-content");
    store
        .apply_entry("agent.cutover", "{}", "module", None, |_| Ok(()))
        .unwrap();
    let agent: Value = serde_json::from_slice(&store.agent_mutation("agent.create", json!({"role":"assistant","name":"Ada","tag":"seed","request_key":"seed-test-agent"}), 1).unwrap()).unwrap();
    let agent = agent["result"]["agent"]["agent_id"].as_str().unwrap();
    let generation = store.generation().unwrap();
    let seeder = test_seeder();
    let first = seeder.seed(&store, &path).unwrap();
    assert!(
        store.catalog_pins(agent).unwrap().is_empty(),
        "seeding must not create a pin"
    );
    let statuses = store.catalog_statuses("").unwrap();
    assert_eq!(statuses.len(), 2);
    assert_eq!(seeder.seed(&store, &path).unwrap(), first);
    assert_eq!(
        serde_json::to_value(store.catalog_statuses("").unwrap()).unwrap(),
        serde_json::to_value(statuses).unwrap()
    );
    assert!(
        store.catalog_pins(agent).unwrap().is_empty(),
        "seeding must not create a pin"
    );
    assert!(store.catalog_pinned_scopes().unwrap().is_empty());
    assert!(store.catalog_agent_generations(agent).unwrap().is_empty());
    assert!(store.publisher_grants().unwrap().is_empty());
    assert!(store.catalog_registries().unwrap().is_empty());
    assert_eq!(
        store.generation().unwrap(),
        generation,
        "seeding must not append a journal entry"
    );
    let blobs = BlobStore::new(store.store_folder()).unwrap();
    for package in vector()["valid"]["packages"].as_array().unwrap() {
        assert!(blobs
            .read_verified(package["archive_sha256"].as_str().unwrap())
            .is_ok());
        assert!(blobs
            .read_verified(
                package["record"]["value"]["manifest_sha256"]
                    .as_str()
                    .unwrap()
            )
            .is_ok());
    }
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn automatic_refusal_is_recorded_and_not_retried() {
    let (root, store, path, descriptor) = fixture("seed-refusal");
    // The production root refuses this TEST-ONLY bundle; that refusal is
    // recorded once and never retried on later starts.
    assert_eq!(
        automatic(&store, &Seeder::default()).unwrap_err().code,
        "signature_invalid"
    );
    let attempt = path.parent().unwrap().join(ATTEMPT);
    let recorded: Value = serde_json::from_slice(&fs::read(&attempt).unwrap()).unwrap();
    assert_eq!(recorded["outcome"], "signature_invalid");
    fs::write(&path, b"bad tar").unwrap();
    drop(store);
    let store = RegistryStore::open(&descriptor).unwrap();
    assert!(automatic(&store, &test_seeder()).unwrap().is_none());
    assert_eq!(
        serde_json::from_slice::<Value>(&fs::read(attempt).unwrap()).unwrap(),
        recorded
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
#[tokio::test]
async fn seed_is_direct_only_and_has_no_confirmation() {
    let (root, store, path, _) = fixture("seed-admission");
    let mut handler = ProjectsHandler::with_runtime("seed-test".into(), || 0);
    handler.catalog_seeder = test_seeder();
    *handler.store.lock().unwrap() = Some(store);
    let route = (281, 1);
    let body = serde_json::to_vec(&json!({"method":SEED,"params":{"bundle_file":path}})).unwrap();
    for principal in [
        None,
        Some(reserved("prefrontal-core")),
        Some(reserved("other")),
        Some(Principal::Unverified),
    ] {
        handler
            .route_admissions()
            .insert(route, RouteAdmission::from_bind(principal, None));
        let outcome = handler.handle_served_request(&body, route).await;
        assert!(
            matches!(outcome, HandlerOutcome::Error { ref code, .. } if code == "write_not_permitted"),
            "{outcome:?}"
        );
    }
    let stamp = flow_stamp(Some("flow_seed"));
    handler.route_admissions().insert(
        route,
        RouteAdmission::from_bind(Some(Principal::Direct), Some(&stamp)),
    );
    assert!(
        matches!(handler.handle_served_request(&body, route).await, HandlerOutcome::ErrorWithDetail { ref code, .. } | HandlerOutcome::Error { ref code, .. } if code == "flow_scope_not_admitted")
    );
    handler.route_admissions().insert(
        route,
        RouteAdmission::from_bind(Some(Principal::Direct), None),
    );
    assert!(matches!(
        handler.handle_served_request(&body, route).await,
        HandlerOutcome::Response(_)
    ));
    handler.store.lock().unwrap().take();
    fs::remove_dir_all(root).unwrap();
}

fn signed(value: Value, key: &Value) -> Value {
    use ed25519_dalek::{Signer, SigningKey};
    let seed = entorhinal_catalog::decode_hex(key["seed_hex"].as_str().unwrap()).unwrap();
    let signer = SigningKey::from_bytes(&seed);
    assert_eq!(
        entorhinal_catalog::hex(signer.verifying_key().as_bytes()),
        key["public_key_hex"]
    );
    let text = entorhinal_catalog::jcs::to_string(&value).unwrap();
    let input = entorhinal_catalog::signing_input(value["schema"].as_str().unwrap(), &text);
    json!({"record":value,"signature":entorhinal_catalog::hex(&signer.sign(&input).to_bytes())})
}
fn bundle_at_sequence(sequence: u64) -> Vec<u8> {
    use entorhinal_catalog::{archive, jcs, sha256_hex};
    let v = vector();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(v["valid"]["bundle_archive_base64"].as_str().unwrap())
        .unwrap();
    let mut members: std::collections::BTreeMap<String, Vec<u8>> = archive::read_members(&bytes)
        .unwrap()
        .into_iter()
        .map(|(p, m)| (p, m.bytes.to_vec()))
        .collect();
    let mut doc: Value = serde_json::from_slice(&members["bundle.json"]).unwrap();
    for entry in doc["records"].as_array_mut().unwrap() {
        let path = entry["record"].as_str().unwrap().to_string();
        let mut member: Value = serde_json::from_slice(&members[&path]).unwrap();
        member["status"]["record"]["sequence"] = sequence.into();
        member["status"] = signed(
            member["status"]["record"].clone(),
            &v["keys"]["registry_status"],
        );
        let bytes = jcs::to_string(&member).unwrap().into_bytes();
        entry["record_member_sha256"] = sha256_hex(&bytes).into();
        members.insert(path, bytes);
    }
    let sig = signed(doc.clone(), &v["keys"]["publish"])["signature"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    members.insert("bundle.sig".into(), sig);
    members.insert(
        "bundle.json".into(),
        jcs::to_string(&doc).unwrap().into_bytes(),
    );
    // Reuse the package writer with an empty manifest, then remove that one
    // header to make a container rather than inventing another ustar writer.
    let mut files = vec![archive::Member::regular("ck.json", b"")];
    files.extend(members.iter().map(|(p, b)| archive::Member::regular(p, b)));
    let archive = archive::write(&files).unwrap();
    archive[512..].to_vec()
}

#[test]
fn every_refused_bundle_leaves_content_and_status_empty() {
    let (root, store, path, _) = fixture("seed-bad-bundles");
    for case in vector()["cases"].as_array().unwrap() {
        fs::write(
            &path,
            base64::engine::general_purpose::STANDARD
                .decode(case["bundle_archive_base64"].as_str().unwrap())
                .unwrap(),
        )
        .unwrap();
        let refusal = test_seeder().seed(&store, &path).unwrap_err();
        assert_eq!(refusal.code, case["expect"]["code"]);
        assert!(store.catalog_statuses("").unwrap().is_empty());
        assert!(!path.parent().unwrap().join("blobs").exists());
    }
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn seeded_statuses_fence_lower_and_conflicting_online_feeds_after_restart() {
    use crate::{
        catalog_network::{Limits, RegistryClient},
        catalog_trust::CatalogTrust,
        fake_registry::{FakeRegistry, Reply},
    };
    let (root, store, path, descriptor) = fixture("seed-rollback");
    fs::write(&path, bundle_at_sequence(3)).unwrap();
    test_seeder().seed(&store, &path).unwrap();
    let initial = store.catalog_statuses("").unwrap();
    drop(store);
    let store = RegistryStore::open(&descriptor).unwrap();
    let v = vector();
    let url = "https://registry.example.invalid";
    store
        .catalog_registry_add(
            url,
            &["@cortexkit".into()],
            false,
            "test-registry-1",
            v["keys"]["registry_status"]["public_key_hex"]
                .as_str()
                .unwrap(),
            1,
        )
        .unwrap();
    let fake = FakeRegistry::start().await;
    fake.serve(
        "/v1/registry/keys",
        Reply::bytes(json!({"schema":"ck-registry-keys/v1","registry":url,"keys":[]}).to_string()),
    );
    let mut trust = CatalogTrust::with_client(
        store.clone(),
        "@cortexkit",
        RegistryClient::loopback(&fake.url, std::slice::from_ref(&fake.url), Limits::MODULE),
    )
    .unwrap()
    .with_test_root(v["keys"]["root"]["public_key_hex"].as_str().unwrap().into());
    for (sequence, reason) in [(2, Value::Null), (3, json!("changed bytes"))] {
        let mut status: Value = serde_json::from_str(&initial[0].canonical).unwrap();
        status["sequence"] = sequence.into();
        status["reason"] = reason;
        let signed = signed(status, &v["keys"]["registry_status"]);
        fake.serve("/v1/scopes/cortexkit/status", Reply::bytes(json!({"schema":"ck-status-feed/v1","scope":"cortexkit","feed_seq":1,"records":[{"feed_seq":1,"record":signed["record"],"signature":signed["signature"]}]}).to_string()));
        let outcome = trust.apply_scope_feed().await.unwrap();
        assert!(!outcome.cursor_advanced);
        assert_eq!(
            outcome.results[0].1.as_ref().unwrap_err().code,
            "feed_rollback"
        );
        assert_eq!(
            store.catalog_statuses(url).unwrap()[0].canonical,
            initial[0].canonical
        );
        assert!(store.catalog_feed_seq("@cortexkit").unwrap().is_none());
    }
    // An identical online record can replace the attestation with its registry
    // authentication while preserving sequence, archive hash and publisher identity.
    let status: Value = serde_json::from_str(&initial[0].canonical).unwrap();
    let signed = signed(status, &v["keys"]["registry_status"]);
    fake.serve("/v1/scopes/cortexkit/status", Reply::bytes(json!({"schema":"ck-status-feed/v1","scope":"cortexkit","feed_seq":1,"records":[{"feed_seq":1,"record":signed["record"],"signature":signed["signature"]}]}).to_string()));
    assert!(trust.apply_scope_feed().await.unwrap().cursor_advanced);
    assert!(store.catalog_statuses(url).unwrap()[0]
        .starter_proof
        .is_none());
    drop(trust);
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "current_thread")]
async fn startup_seed_does_not_block_health() {
    use std::sync::Arc;
    let (root, store, _, _) = fixture("seed-health");
    let handler = ProjectsHandler::with_runtime("seed-health".into(), || 0);
    *handler.store.lock().unwrap() = Some(store.clone());
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, wait_release) = std::sync::mpsc::channel();
    let wait_release = std::sync::Mutex::new(wait_release);
    let mut seeder = test_seeder();
    seeder.before_seed = Some(Arc::new(move || {
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        wait_release
            .lock()
            .unwrap()
            .recv_timeout(std::time::Duration::from_secs(10))
            .unwrap();
    }));
    let task = schedule(store.clone(), seeder, &handler.catalog_sql).unwrap();
    wait_entered.await.unwrap();
    use subc_client_rs::ModuleHandler;
    let health = tokio::time::timeout(std::time::Duration::from_secs(1), handler.health()).await;
    release.send(()).unwrap();
    assert!(health.is_ok(), "background bundle load blocked health");
    task.await.unwrap();
    handler.store.lock().unwrap().take();
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

/// Stopping the catalog must not return while a startup seed job still holds a
/// store handle on the blocking pool. Otherwise a caller that deletes the store
/// folder next is refused on Windows, which keeps open files undeletable.
#[tokio::test(flavor = "current_thread")]
async fn stopping_the_catalog_waits_for_a_running_seed_job() {
    use std::future::Future;
    use std::sync::Arc;
    use std::task::{Context, Waker};
    let (root, store, _, _) = fixture("seed-stop-waits");
    let handler = ProjectsHandler::with_runtime("seed-stop-waits".into(), || 0);
    let (entered, wait_entered) = tokio::sync::oneshot::channel();
    let entered = std::sync::Mutex::new(Some(entered));
    let (release, wait_release) = std::sync::mpsc::channel::<()>();
    let wait_release = std::sync::Mutex::new(wait_release);
    let mut seeder = test_seeder();
    seeder.before_seed = Some(Arc::new(move || {
        entered.lock().unwrap().take().unwrap().send(()).unwrap();
        let _ = wait_release.lock().unwrap().recv();
    }));
    let task = schedule(store.clone(), seeder, &handler.catalog_sql).unwrap();
    drop(store);
    wait_entered.await.unwrap();

    let mut stop = Box::pin(handler.stop_catalog_poll());
    assert!(
        stop.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending(),
        "stop returned while the seed job still held the store"
    );
    release.send(()).unwrap();
    task.await.unwrap();
    stop.await;
    fs::remove_dir_all(root).unwrap();
}

#[test]
fn automatic_seed_requires_a_present_bundle_and_empty_content() {
    let (root, store, path, _) = fixture("seed-automatic-gates");
    fs::remove_file(&path).unwrap();
    assert!(automatic(&store, &test_seeder()).unwrap().is_none());
    assert!(!path.parent().unwrap().join(ATTEMPT).exists());
    fs::write(&path, b"not a bundle").unwrap();
    BlobStore::new(store.store_folder())
        .unwrap()
        .publish(b"existing content")
        .unwrap();
    assert!(automatic(&store, &test_seeder()).unwrap().is_none());
    assert!(!path.parent().unwrap().join(ATTEMPT).exists());
    drop(store);
    fs::remove_dir_all(root).unwrap();
}
#[test]
fn malformed_saved_starter_proof_is_a_refusal_not_a_panic() {
    let (root, store, path, _) = fixture("seed-corrupt-proof");
    test_seeder().seed(&store, &path).unwrap();
    store
        .catalog_seed_transaction::<_, CatalogError>(|rows, _| {
            let mut corrupt = rows[0].clone();
            corrupt.starter_proof = Some("not json".into());
            Ok(((), vec![corrupt]))
        })
        .unwrap();
    assert_eq!(
        test_seeder().seed(&store, &path).unwrap_err().code,
        "status_invalid"
    );
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test(start_paused = true)]
async fn catalog_seed_sql_is_off_runtime_and_bounded() {
    use crate::catalog_sql::tests::{finish, poll, responsive, started, tracked, HeldSql};
    let (root, store, path, _) = fixture("seed-held-sql");
    let mut handler = ProjectsHandler::with_runtime("seed-held-sql".into(), || 0);
    handler.catalog_seeder = test_seeder();
    *handler.store.lock().unwrap() = Some(store.clone());
    let route = (281, 1);
    handler.route_admissions().insert(
        route,
        RouteAdmission::from_bind(Some(Principal::Direct), None),
    );
    let held = HeldSql::new(store.clone()).await;
    let (request, entered) = tracked(handler.catalog_seed(
        json!({"bundle_file":path}),
        route,
        tokio::time::Instant::now(),
    ));
    let mut request = Box::pin(request);
    started(request.as_mut(), &entered).await;
    assert!(poll(request.as_mut()).is_pending());
    responsive(&handler).await;
    tokio::time::advance(std::time::Duration::from_millis(110_001)).await;
    let answer = poll(request.as_mut());
    held.finish().await;
    assert!(
        matches!(answer, std::task::Poll::Ready(Err(error)) if error.code == "catalog_timeout")
    );
    drop(request);
    // Repeating the same bundle joins the durable, content-addressed result.
    let reply = finish(handler.catalog_seed(
        json!({"bundle_file":path}),
        route,
        tokio::time::Instant::now(),
    ))
    .await
    .unwrap();
    assert!(serde_json::from_slice::<Value>(&reply).unwrap()["result"].is_object());
    assert_eq!(store.catalog_statuses("").unwrap().len(), 2);
    drop(handler);
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

use super::*;
use crate::{
    catalog_network::{Limits, RegistryClient},
    catalog_trust::CatalogTrust,
    ProjectsHandler,
};
use base64::Engine;
use cortexkit_store_types::{StorageBackend, StorageDescriptor};
use rusqlite::Connection;
use serde_json::{json, Value};
use subc_client_rs::ModuleHandler;

async fn startup(descriptor: &StorageDescriptor, machine: Option<&str>) -> RegistryStore {
    let handler = ProjectsHandler::with_runtime("builtin-registry".into(), || 100);
    let mut ack = crate::tests::hello_ack(descriptor);
    ack.machine_id = machine.map(|id| serde_json::from_value(json!(id)).unwrap());
    handler.on_hello_ack(&ack).await;
    assert!(handler
        .health
        .store_ready
        .load(std::sync::atomic::Ordering::Relaxed));
    crate::tests::stop_store_tasks(&handler).await;
    handler.with_store(|s| Ok(s.clone())).unwrap()
}

fn conn(descriptor: &StorageDescriptor) -> Connection {
    let StorageBackend::Sqlite { path } = &descriptor.backend else {
        unreachable!()
    };
    Connection::open(path).unwrap()
}

fn add(store: &RegistryStore, url: &str, scope: &str, now: i64) {
    let registry = CORTEXKIT_REGISTRY;
    store
        .catalog_registry_add(
            url,
            &[scope.into()],
            false,
            registry.key_id,
            registry.public_key,
            now,
        )
        .unwrap();
}

#[tokio::test]
async fn builtin_registry_fresh_startup_verifies_signed_cortexkit_package() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-fresh");
    let store = startup(&descriptor, None).await;
    let rows = store.catalog_registries().unwrap();
    assert_eq!(
        rows.len(),
        1,
        "startup must materialize the built-in registry"
    );
    let row = &rows[0];
    assert_eq!(row.url, "https://catalog.cortexkit.io");
    assert_eq!(row.scopes, ["@cortexkit"]);
    assert!(!row.is_default);
    assert_eq!(row.keys.len(), 1);
    assert_eq!(row.keys[0].key_id, "genesis");
    assert_eq!(
        row.keys[0].public_key,
        "57e7fcba3ee643f26196fc3f36174515e2aa01e4e0b00bda0e8f42d62cba36a4"
    );
    assert_eq!(row.keys[0].chain_index, 0);
    assert!(row.keys[0].statement.is_none());
    assert!(row.keys[0].valid_from.is_none());
    assert!(CatalogTrust::for_scope(store.clone(), "@cortexkit").is_ok());
    assert_eq!(
        CatalogTrust::for_scope(store.clone(), "@other")
            .err()
            .unwrap()
            .code,
        "scope_unmapped"
    );
    assert!(store
        .catalog_registry_for_scope("@other")
        .unwrap()
        .is_none());

    let vectors: Value = serde_json::from_str(include_str!(
        "../../entorhinal-catalog/tests/vectors/ck-catalog-v1/starter-bundle.json"
    ))
    .unwrap();
    let package = &vectors["valid"]["packages"][1];
    let name = package["name"].as_str().unwrap();
    let record = &package["record_file"]["record"];
    let status = &package["record_file"]["status"];
    let archive = base64::engine::general_purpose::STANDARD
        .decode(package["archive_base64"].as_str().unwrap())
        .unwrap();
    let members = entorhinal_catalog::archive::read(&archive).unwrap();
    let manifest = members.get("ck.json").unwrap().to_vec();
    let fake = crate::fake_registry::FakeRegistry::start().await;
    use crate::fake_registry::Reply;
    fake.serve(
        "/v1/registry/keys",
        Reply::bytes(
            json!({"schema":"ck-registry-keys/v1","registry":row.url,"keys":[]}).to_string(),
        ),
    );
    fake.serve("/v1/packages/cortexkit/starter-skill", Reply::bytes(json!({"name":name,"versions":[{"version":"1.0.0","record":record["record"],"signature":record["signature"],"status":status}]}).to_string()));
    fake.serve(
        "/v1/packages/cortexkit/starter-skill/1.0.0/ck.json",
        Reply::bytes(manifest),
    );
    fake.serve(
        &format!(
            "/v1/archives/{}",
            package["archive_sha256"].as_str().unwrap()
        ),
        Reply::bytes(archive),
    );
    let key = &vectors["keys"]["publish"];
    let cert = &vectors["valid"]["certificate"];
    fake.serve("/v1/scopes/cortexkit", Reply::bytes(json!({"scope":"cortexkit","keys":[{"public_key":key["public_key_hex"],"fingerprint":key["fingerprint"],"valid_from":cert["statement"]["valid_from"],"valid_until":cert["statement"]["valid_until"],"revocation":null,"rotation":null,"cert":{"statement":cert["statement"],"signature":cert["signature"]}}]}).to_string()));
    let trust = || {
        CatalogTrust::with_client(
            store.clone(),
            "@cortexkit",
            RegistryClient::loopback(&fake.url, std::slice::from_ref(&fake.url), Limits::MODULE),
        )
        .unwrap()
        .with_test_root(
            vectors["keys"]["root"]["public_key_hex"]
                .as_str()
                .unwrap()
                .into(),
        )
    };

    // The published fixtures have TEST-ONLY signers. Inject only their root first:
    // the unchanged genesis anchor must still refuse their registry status.
    assert_eq!(
        trust()
            .verify_version(name, "1.0.0")
            .await
            .unwrap_err()
            .code(),
        "key_unknown"
    );
    assert!(!fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.contains("archives") || p.ends_with("ck.json")));
    assert_eq!(store.catalog_registries().unwrap(), rows);

    // Explicit repin uses the ordinary operator write, not a production bypass.
    // Only this test swaps to the fixture's public TEST-ONLY status key.
    store
        .catalog_registry_repin(
            row.url.as_str(),
            "test-registry-1",
            vectors["keys"]["registry_status"]["public_key_hex"]
                .as_str()
                .unwrap(),
            "2026-10-01T00:00:00Z",
        )
        .unwrap();
    let (verified, first_party) = trust()
        .verify_version_with_provenance(name, "1.0.0")
        .await
        .unwrap();
    assert!(first_party);
    assert_eq!(verified.record.name, name);
    assert_eq!(
        store.catalog_registries().unwrap()[0].incarnation,
        row.incarnation
    );
    assert_eq!(
        store
            .catalog_registry_remove(&row.url)
            .unwrap_err()
            .to_string(),
        "registry_has_status"
    );
    drop(fake);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn builtin_registry_preserves_operator_mapping_and_repin_without_audit_or_journal() {
    for url in [CORTEXKIT_REGISTRY.url, "https://operator.example"] {
        let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-existing");
        let store = RegistryStore::open(&descriptor).unwrap();
        add(&store, url, "@cortexkit", 29);
        let before = store.catalog_registries().unwrap();
        let journal = store.generation().unwrap();
        let audit = store.catalog_audit().unwrap().len();
        drop(store);
        let store = startup(&descriptor, None).await;
        assert_eq!(store.catalog_registries().unwrap(), before);
        assert_eq!(store.generation().unwrap(), journal);
        assert_eq!(store.catalog_audit().unwrap().len(), audit);
        store
            .catalog_registry_repin(url, "rotated", &"ab".repeat(32), "2026-11-01T00:00:00Z")
            .unwrap();
        let repinned = store.catalog_registries().unwrap();
        drop(store);
        let store = startup(&descriptor, None).await;
        assert_eq!(store.catalog_registries().unwrap(), repinned);
        assert_eq!(store.generation().unwrap(), journal);
        assert_eq!(store.catalog_audit().unwrap().len(), audit);
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn builtin_registry_remove_restart_and_explicit_add_clears_opt_out() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-opt-out");
    let store = startup(&descriptor, None).await;
    store
        .catalog_registry_remove(CORTEXKIT_REGISTRY.url)
        .unwrap();
    drop(store);
    let store = startup(&descriptor, None).await;
    assert!(store.catalog_registries().unwrap().is_empty());
    assert_eq!(
        conn(&descriptor)
            .query_row(
                "SELECT COUNT(*) FROM catalog_registry_opt_out WHERE scope='@cortexkit'",
                [],
                |r| r.get::<_, i64>(0)
            )
            .unwrap(),
        1
    );
    add(&store, CORTEXKIT_REGISTRY.url, "@cortexkit", 101);
    assert_eq!(
        conn(&descriptor)
            .query_row("SELECT COUNT(*) FROM catalog_registry_opt_out", [], |r| r
                .get::<_, i64>(
                0
            ))
            .unwrap(),
        0
    );
    let restored = store.catalog_registries().unwrap();
    drop(store);
    let store = startup(&descriptor, None).await;
    assert_eq!(store.catalog_registries().unwrap(), restored);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn builtin_registry_foreign_store_materializes_nothing() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-foreign");
    let store = RegistryStore::open(&descriptor).unwrap();
    store.check_store_origin(Some("old-machine"), 1).unwrap();
    drop(store);
    let store = startup(&descriptor, Some("new-machine")).await;
    assert!(store.is_foreign());
    assert!(store.catalog_registries().unwrap().is_empty());
    assert_eq!(store.generation().unwrap(), 0);
    assert!(store.catalog_audit().unwrap().is_empty());
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn builtin_registry_url_collision_preserves_other_scope_and_startup_completes() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-url-collision");
    let store = RegistryStore::open(&descriptor).unwrap();
    add(&store, CORTEXKIT_REGISTRY.url, "@operator", 29);
    let before = store.catalog_registries().unwrap();
    drop(store);
    let store = startup(&descriptor, None).await;
    assert_eq!(store.catalog_registries().unwrap(), before);
    assert!(store
        .catalog_registry_for_scope("@cortexkit")
        .unwrap()
        .is_none());
    assert_eq!(store.generation().unwrap(), 0);
    assert!(store.catalog_audit().unwrap().is_empty());
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn builtin_registry_write_failure_keeps_startup_ready_and_retries_next_start() {
    let (dir, descriptor) = crate::tests::scratch_descriptor("builtin-write-failure");
    drop(RegistryStore::open(&descriptor).unwrap());
    conn(&descriptor).execute_batch("CREATE TRIGGER refuse_registry BEFORE INSERT ON catalog_registry BEGIN SELECT RAISE(ABORT,'test storage refusal'); END;").unwrap();
    let store = startup(&descriptor, None).await;
    assert!(store.catalog_registries().unwrap().is_empty());
    drop(store);
    conn(&descriptor)
        .execute_batch("DROP TRIGGER refuse_registry;")
        .unwrap();
    let store = startup(&descriptor, None).await;
    assert_eq!(store.catalog_registries().unwrap().len(), 1);
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

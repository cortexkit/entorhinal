use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    catalog_trust::tests::vectors,
    fake_registry::{FakeRegistry, Reply},
    RouteAdmission,
};
use async_trait::async_trait;
use base64::Engine;
use entorhinal_core::catalog_grants::{coverage, Coverage, CoveragePackage};
use std::sync::{Arc, Mutex};
use subc_protocol::Principal;
const ROUTE: RouteKey = (181, 1);
struct Pins(Vec<PublisherPin>);
impl PinSource for Pins {
    fn pins(&self, _: &RegistryStore, _: &str, _: &str) -> Result<Vec<PublisherPin>, CatalogError> {
        Ok(self.0.clone())
    }
}
struct Prompt {
    answer: Result<(), OperatorConfirmError>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.calls.lock().unwrap().push(summary.into());
        self.answer
    }
}
struct Fixture {
    root: std::path::PathBuf,
    descriptor: cortexkit_store_types::StorageDescriptor,
    handler: ProjectsHandler,
    agent: String,
    prompt: Arc<Prompt>,
}
impl Fixture {
    fn new(answer: Result<(), OperatorConfirmError>) -> Self {
        let (root, descriptor) = crate::tests::scratch_descriptor("publisher-grants");
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        let body = store
            .agent_mutation(
                "agent.create",
                json!({"name":"Agent","role":"assistant","tag":"test","request_key":"create"}),
                100,
            )
            .unwrap();
        let agent = serde_json::from_slice::<Value>(&body).unwrap()["result"]["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_string();
        let prompt = Arc::new(Prompt {
            answer,
            calls: Mutex::new(vec![]),
        });
        let mut handler = ProjectsHandler::with_runtime("grant-test".into(), || 100);
        handler.operator_confirmer = prompt.clone();
        *handler.store.lock().unwrap() = Some(store);
        handler.route_admissions().insert(
            ROUTE,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        Self {
            root,
            descriptor,
            handler,
            agent,
            prompt,
        }
    }
    fn store(&self) -> RegistryStore {
        self.handler.with_store(|s| Ok(s.clone())).unwrap()
    }
    fn pins(&self) -> Pins {
        let v = vectors("record");
        let p = &v["packages"]["skill"];
        let archive = base64::engine::general_purpose::STANDARD
            .decode(p["archive_base64"].as_str().unwrap())
            .unwrap();
        BlobStore::new(self.store().store_folder())
            .unwrap()
            .publish(&archive)
            .unwrap();
        Pins(vec![PublisherPin {
            name: "@example/hello-skill".into(),
            version: "1.0.0".into(),
            publisher_fingerprint: v["keys"]["trusted"]["fingerprint"].as_str().unwrap().into(),
            manifest_sha256: p["manifest_sha256"].as_str().unwrap().into(),
            archive_sha256: p["archive_sha256"].as_str().unwrap().into(),
        }])
    }
    async fn trust(&self, source: &dyn PinSource) -> Result<Value, HandlerError> {
        let body = self.handler.catalog_publisher_trust_from(json!({"publisher_fingerprint":vectors("record")["keys"]["trusted"]["fingerprint"],"agent_id":self.agent}),ROUTE,Instant::now(),source).await?;
        Ok(serde_json::from_slice::<Value>(&body).unwrap()["result"].clone())
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
#[tokio::test]
async fn publisher_trust_authenticates_pins_and_is_install_only() {
    let f = Fixture::new(Ok(()));
    assert_eq!(
        f.trust(&StoredPins).await.unwrap_err().code,
        "manifest_unavailable"
    );
    assert_eq!(
        f.trust(&Pins(vec![])).await.unwrap_err().code,
        "manifest_unavailable"
    );
    let pins = f.pins();
    let result = f.trust(&pins).await.unwrap();
    assert_eq!(result["operation"], "install");
    assert!(f.prompt.calls.lock().unwrap()[0].contains("for install at agent"));
    let grant = f.store().publisher_grants().unwrap().remove(0);
    assert_eq!(grant.operation, Operation::Install);
    let mut corrupt = pins;
    corrupt.0[0].manifest_sha256 = "ab".repeat(32);
    let e = f.trust(&corrupt).await.unwrap_err();
    assert_eq!(e.code, "manifest_unavailable");
    assert_eq!(e.detail.unwrap()["name"], "@example/hello-skill");
    assert_eq!(f.store().catalog_audit().unwrap().len(), 1);
}
#[tokio::test]
async fn publisher_trust_admission_decline_and_disposed_placement() {
    let f = Fixture::new(Err(OperatorConfirmError::Declined));
    let pins = f.pins();
    for principal in [
        Principal::Unverified,
        Principal::Reserved {
            module_id: "cingulate".into(),
        },
    ] {
        f.handler
            .route_admissions()
            .insert(ROUTE, RouteAdmission::from_bind(Some(principal), None));
        assert_eq!(
            f.trust(&pins).await.unwrap_err().code,
            "write_not_permitted"
        );
    }
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(
            Some(Principal::Direct),
            Some(&crate::tests::flow_stamp(Some("f"))),
        ),
    );
    assert_eq!(
        f.trust(&pins).await.unwrap_err().code,
        "flow_scope_not_admitted"
    );
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(Some(Principal::Direct), None),
    );
    assert!(f.trust(&pins).await.is_err());
    assert!(f.store().publisher_grants().unwrap().is_empty());
    f.store()
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":f.agent,"request_key":"dispose"}),
            101,
        )
        .unwrap();
    assert_eq!(f.trust(&pins).await.unwrap_err().code, "gone");
}
#[tokio::test]
async fn publisher_manifest_http_digest_strict_identity_and_cap() {
    let f = Fixture::new(Ok(()));
    let fake = FakeRegistry::start().await;
    let p = &vectors("record")["packages"]["skill"];
    let pin = PublisherPin {
        name: "@example/hello-skill".into(),
        version: "1.0.0".into(),
        publisher_fingerprint: vectors("record")["keys"]["trusted"]["fingerprint"]
            .as_str()
            .unwrap()
            .into(),
        manifest_sha256: p["manifest_sha256"].as_str().unwrap().into(),
        archive_sha256: p["archive_sha256"].as_str().unwrap().into(),
    };
    let path = "/v1/packages/example/hello-skill/1.0.0/ck.json";
    let factory = |_: &str| {
        Ok(RegistryClient::loopback(
            &fake.url,
            std::slice::from_ref(&fake.url),
            Limits::MODULE,
        ))
    };
    fake.serve(path, Reply::bytes(p["manifest_text"].as_str().unwrap()));
    ceiling(
        &f.store(),
        std::slice::from_ref(&pin),
        &pin.publisher_fingerprint,
        &factory,
    )
    .await
    .unwrap();
    fake.serve(path, Reply::bytes(vec![b'x'; 65_537]));
    assert_eq!(
        ceiling(
            &f.store(),
            std::slice::from_ref(&pin),
            &pin.publisher_fingerprint,
            &factory
        )
        .await
        .unwrap_err()
        .code,
        "manifest_unavailable"
    );
    for bytes in [
        b"{\"name\":\"a\",\"name\":\"b\"}".to_vec(),
        p["manifest_text"]
            .as_str()
            .unwrap()
            .replace("hello-skill", "other")
            .into_bytes(),
        p["manifest_text"]
            .as_str()
            .unwrap()
            .replace("1.0.0", "2.0.0")
            .into_bytes(),
    ] {
        let mut pin = pin.clone();
        pin.manifest_sha256 = sha256_hex(&bytes);
        fake.serve(path, Reply::bytes(bytes));
        assert_eq!(
            ceiling(
                &f.store(),
                std::slice::from_ref(&pin),
                &pin.publisher_fingerprint,
                &factory
            )
            .await
            .unwrap_err()
            .code,
            "manifest_unavailable"
        );
    }
}
#[tokio::test]
async fn grants_coverage_refresh_revoke_inventory_audit_and_local_only() {
    let f = Fixture::new(Ok(()));
    let store = f.store();
    let mut grant = PublisherGrant {
        id: "pg_test".into(),
        publisher_fingerprint: "ab".repeat(32),
        operation: Operation::Install,
        placement_agent_id: f.agent.clone(),
        ceiling: vec![],
        card_ref: None,
        granted_at_ms: 100,
        revoked_at_ms: None,
    };
    store
        .insert_publisher_grant(&grant, &json!({"kind":"direct"}))
        .unwrap();
    let mut p = CoveragePackage {
        name: "@example/p".into(),
        publisher_fingerprint: grant.publisher_fingerprint.clone(),
        capabilities: vec![],
        has_scripts: false,
        root_certified: false,
    };
    let cover = |p: &CoveragePackage, op, agent_live| {
        coverage(
            std::slice::from_ref(p),
            &store.publisher_grants().unwrap(),
            op,
            &f.agent,
            agent_live,
        )
    };
    assert_eq!(cover(&p, Operation::Install, true), Coverage::Covered);
    assert!(matches!(
        coverage(
            std::slice::from_ref(&p),
            &store.publisher_grants().unwrap(),
            Operation::Install,
            "another-agent",
            true
        ),
        Coverage::Uncovered(_)
    ));
    assert!(matches!(
        cover(&p, Operation::Upgrade, true),
        Coverage::Uncovered(_)
    ));
    assert!(matches!(
        cover(&p, Operation::Install, false),
        Coverage::Uncovered(_)
    ));
    p.capabilities.push("cap/a.b@1".into());
    assert!(matches!(
        cover(&p, Operation::Install, true),
        Coverage::Uncovered(_)
    ));
    grant.id = "pg_new".into();
    grant.ceiling = vec!["cap/a.b@1".into()];
    assert_eq!(
        store
            .insert_publisher_grant(&grant, &json!({"kind":"direct"}))
            .unwrap(),
        "pg_test"
    );
    assert_eq!(cover(&p, Operation::Install, true), Coverage::Covered);
    p.has_scripts = true;
    assert!(matches!(
        cover(&p, Operation::Install, true),
        Coverage::Uncovered(_)
    ));
    p.has_scripts = false;
    p.publisher_fingerprint = "cd".repeat(32);
    assert!(matches!(
        cover(&p, Operation::Install, true),
        Coverage::Uncovered(_)
    ));
    p.publisher_fingerprint = grant.publisher_fingerprint.clone();
    let list = f.handler.permissions_list(json!({})).unwrap();
    let list: Value = serde_json::from_slice(&list).unwrap();
    assert_eq!(list["result"]["grants"][0]["scope"], "catalog_publisher");
    let revoke = f
        .handler
        .permissions_revoke(json!({"id":"pg_test"}), ROUTE)
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&revoke).unwrap()["result"]["outcome"],
        "revoked"
    );
    assert!(matches!(
        cover(&p, Operation::Install, true),
        Coverage::Uncovered(_)
    ));
    assert!(!store
        .revoke_publisher_grant("pg_test", 101, &json!({"kind":"direct"}))
        .unwrap());
    assert_eq!(store.publisher_grants().unwrap().len(), 1);
    let list: Value =
        serde_json::from_slice(&f.handler.permissions_list(json!({})).unwrap()).unwrap();
    assert!(list["result"]["grants"].as_array().unwrap().is_empty());
    let audit = store.catalog_audit().unwrap();
    assert_eq!(
        audit
            .iter()
            .map(|r| r["kind"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["grant_insert", "grant_refresh", "grant_revoke"]
    );
    grant.ceiling = vec!["cap/a.c@1".into()];
    assert_eq!(
        store
            .insert_publisher_grant(&grant, &json!({"kind":"direct"}))
            .unwrap(),
        "pg_test"
    );
    assert_eq!(
        store.publisher_grants().unwrap()[0].ceiling,
        ["cap/a.b@1", "cap/a.c@1"]
    );
    assert_eq!(cover(&p, Operation::Install, true), Coverage::Covered);
    let generation = store.generation().unwrap();
    store.rebuild().unwrap();
    assert_eq!(store.publisher_grants().unwrap().len(), 1);
    assert_eq!(store.generation().unwrap(), generation);
    let changes = store.agent_changes(0, None).unwrap();
    assert!(!serde_json::to_string(&changes).unwrap().contains("pg_test"));
    store
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":f.agent,"request_key":"end"}),
            102,
        )
        .unwrap();
    let list: Value =
        serde_json::from_slice(&f.handler.permissions_list(json!({})).unwrap()).unwrap();
    assert!(list["result"]["grants"].as_array().unwrap().is_empty());
    assert!(matches!(
        store.insert_publisher_grant(&grant, &json!({})),
        Err(entorhinal_core::catalog::CatalogWriteError::Refused("gone"))
    ));
}

#[tokio::test]
async fn grants_reopen_keep_revoked_history_and_no_root_publisher_grant() {
    let f = Fixture::new(Ok(()));
    let grant = PublisherGrant {
        id: "pg_history".into(),
        publisher_fingerprint: "ab".repeat(32),
        operation: Operation::Install,
        placement_agent_id: f.agent.clone(),
        ceiling: vec![],
        card_ref: None,
        granted_at_ms: 100,
        revoked_at_ms: None,
    };
    let mut missing = grant.clone();
    missing.placement_agent_id = "no-agent".into();
    assert!(matches!(
        f.store()
            .insert_publisher_grant(&missing, &json!({"kind":"direct"})),
        Err(entorhinal_core::catalog::CatalogWriteError::Refused(
            "unknown_agent"
        ))
    ));
    f.store()
        .insert_publisher_grant(&grant, &json!({"kind":"direct"}))
        .unwrap();
    f.store()
        .revoke_publisher_grant(&grant.id, 101, &json!({"kind":"direct"}))
        .unwrap();
    f.handler.store.lock().unwrap().take();
    let reopened = RegistryStore::open(&f.descriptor).unwrap();
    assert_eq!(
        reopened.publisher_grants().unwrap()[0].revoked_at_ms,
        Some(101)
    );
    assert_eq!(reopened.catalog_audit().unwrap().len(), 2);
    assert!(!reopened
        .revoke_publisher_grant(&grant.id, 102, &json!({"kind":"direct"}))
        .unwrap());
    let root = CoveragePackage {
        name: "@cortexkit/p".into(),
        publisher_fingerprint: "cd".repeat(32),
        capabilities: vec![],
        has_scripts: false,
        root_certified: true,
    };
    assert_eq!(
        coverage(
            std::slice::from_ref(&root),
            &[],
            Operation::Install,
            &f.agent,
            true
        ),
        Coverage::Covered
    );
    let mut scripted = root;
    scripted.has_scripts = true;
    assert!(
        matches!(coverage(&[scripted], &[], Operation::Install, &f.agent, true), Coverage::Uncovered(reasons) if reasons[0].contains("scripts"))
    );
}
#[tokio::test]
async fn publisher_pins_must_stay_authenticated_through_confirmation() {
    struct MovingPins {
        initial: Vec<PublisherPin>,
        calls: std::sync::atomic::AtomicUsize,
    }
    impl PinSource for MovingPins {
        fn pins(
            &self,
            _: &RegistryStore,
            _: &str,
            _: &str,
        ) -> Result<Vec<PublisherPin>, CatalogError> {
            let mut pins = self.initial.clone();
            if self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst) != 0 {
                pins[0].manifest_sha256 = "ab".repeat(32);
            }
            Ok(pins)
        }
    }
    let f = Fixture::new(Ok(()));
    let pins = f.pins();
    let moving = MovingPins {
        initial: pins.0,
        calls: std::sync::atomic::AtomicUsize::new(0),
    };
    assert_eq!(
        f.trust(&moving).await.unwrap_err().code,
        "manifest_unavailable"
    );
    assert!(f.store().publisher_grants().unwrap().is_empty());
    assert!(f.store().catalog_audit().unwrap().is_empty());
    assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
}

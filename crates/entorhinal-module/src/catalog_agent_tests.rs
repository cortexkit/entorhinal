use super::*;
use crate::catalog_network::{Limits, RegistryClient};
use crate::catalog_trust::tests::{signed, vectors};
use crate::fake_cingulate::{Effect, FakeCingulate};
use crate::fake_registry::{FakeRegistry, Reply};
use crate::log_client::{LogTransport, TransportError};
use crate::tool_provider::tests::{error_code, result, Fixture as Tools};
use async_trait::async_trait;
use std::sync::atomic::{AtomicBool, Ordering};
use subc_client_rs::{CatalogList, SupervisorListResponse};

const NAME: &str = "@example/hello-skill";
const URL: &str = "https://registry.example.invalid";
fn consumer_role() -> Value {
    json!({"role":"internal_service","service_id":catalog_consumer::SERVICE,"transport":"bulk","agent_facing":false,"operations":["catalog_consumer.check","catalog_consumer.prefetch"]})
}
struct TestCapabilities;
impl LocalCapabilities for TestCapabilities {
    fn tokens(&self, _: &CatalogList) -> Vec<String> {
        vec!["cap/example.read@1".into()]
    }
}

fn script_free_package(registry: &FakeRegistry) -> Value {
    use base64::Engine;
    let records = vectors("record");
    let package = &records["packages"]["skill"];
    let mut manifest: Value =
        serde_json::from_str(package["manifest_text"].as_str().unwrap()).unwrap();
    manifest["body"]["scripts"] = json!([]);
    manifest["files"]
        .as_array_mut()
        .unwrap()
        .retain(|f| f["path"] == "SKILL.md");
    let manifest_text = manifest.to_string();
    let original = base64::engine::general_purpose::STANDARD
        .decode(package["archive_base64"].as_str().unwrap())
        .unwrap();
    let mut archive = Vec::new();
    for (path, content) in [
        ("ck.json", manifest_text.as_str()),
        (
            "SKILL.md",
            package["members"][1]["content"].as_str().unwrap(),
        ),
    ] {
        let mut header = original[..512].to_vec();
        header[..100].fill(0);
        header[..path.len()].copy_from_slice(path.as_bytes());
        header[124..136].copy_from_slice(format!("{:011o}\0", content.len()).as_bytes());
        header[148..156].fill(b' ');
        let sum: u32 = header.iter().map(|b| u32::from(*b)).sum();
        header[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        archive.extend(header);
        archive.extend_from_slice(content.as_bytes());
        archive.resize(archive.len().div_ceil(512) * 512, 0);
    }
    archive.extend([0; 1024]);
    let mut record = records["valid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["record"]["name"] == NAME)
        .unwrap()["record"]
        .clone();
    record["archive_sha256"] = json!(entorhinal_catalog::sha256_hex(&archive));
    record["archive_bytes"] = json!(archive.len());
    record["manifest_sha256"] = json!(entorhinal_catalog::sha256_hex(manifest_text.as_bytes()));
    let record_doc = signed(record.clone(), &records["keys"]["trusted"]);
    let mut status = vectors("status")["records"][0]["record"].clone();
    status["archive_sha256"] = record["archive_sha256"].clone();
    let doc = signed(status, &vectors("statements")["keys"]["registry_status"][0]);
    registry.serve("/v1/packages/example/hello-skill",Reply::bytes(json!({"name":NAME,"versions":[{"version":"1.0.0","record":record,"signature":record_doc["signature"],"status":doc}]}).to_string()));
    registry.serve(
        "/v1/packages/example/hello-skill/1.0.0/ck.json",
        Reply::bytes(manifest_text),
    );
    registry.serve(
        &format!(
            "/v1/archives/{}",
            record["archive_sha256"].as_str().unwrap()
        ),
        Reply::bytes(archive),
    );
    doc
}

struct FakeConsumer {
    store: RegistryStore,
    check_reply: Mutex<Value>,
    ready: AtomicBool,
    calls: Mutex<Vec<(String, Value)>>,
}
#[async_trait]
impl LogTransport for FakeConsumer {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        self.calls
            .lock()
            .unwrap()
            .push((method.into(), params.clone()));
        for p in params["packages"].as_array().unwrap() {
            let digest = p["archive_sha256"].as_str().unwrap();
            let bytes = crate::catalog_blob::BlobStore::new(self.store.store_folder())
                .unwrap()
                .read_chunk(digest, 0, 1048576)
                .unwrap();
            use base64::Engine;
            let bytes = base64::engine::general_purpose::STANDARD
                .decode(bytes["bytes"].as_str().unwrap())
                .unwrap();
            assert_eq!(
                entorhinal_catalog::sha256_hex(bytes),
                digest,
                "consumer verifies the actual archive"
            );
        }
        let result = match method {
            "catalog_consumer.check" => self.check_reply.lock().unwrap().clone(),
            "catalog_consumer.prefetch" => json!({"ready":self.ready.load(Ordering::SeqCst)}),
            _ => panic!("unexpected consumer method {method}"),
        };
        Ok(serde_json::to_vec(&json!({"result":result})).unwrap())
    }
}
struct FakeConsumers {
    consumer: Arc<FakeConsumer>,
    available: AtomicBool,
    ready: AtomicBool,
    capability: Mutex<String>,
    extra: Mutex<Option<Arc<FakeConsumer>>>,
    has_role: AtomicBool,
}
#[async_trait]
impl ConsumerDiscovery for FakeConsumers {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        let mut configured = vec![
            json!({"module_id":crate::WRITER_MODULE,"enabled":true,"state":"running","live":true,"health":"ok"}),
        ];
        if self.extra.lock().unwrap().is_some() {
            configured.push(json!({"module_id":"zeta","enabled":true,"state":"running","live":true,"health":"ok"}));
        }
        Ok(crate::fake_daemon::DaemonSnapshot::new(1, configured, vec![]).supervisor)
    }
    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        let roles = if self.has_role.load(Ordering::SeqCst) {
            vec![consumer_role()]
        } else {
            vec![]
        };
        let mut modules = if self.available.load(Ordering::SeqCst) {
            vec![
                json!({"module_id":crate::WRITER_MODULE,"ready":self.ready.load(Ordering::SeqCst),"roles":roles,"control_ops":[],"capabilities":{"provides":[*self.capability.lock().unwrap()],"requires":[]}}),
            ]
        } else {
            vec![]
        };
        if self.extra.lock().unwrap().is_some() {
            modules.push(json!({"module_id":"zeta","ready":true,"roles":roles,"control_ops":[],"capabilities":{"provides":[catalog_consumer::SKILL],"requires":[]}}));
        }
        Ok(crate::fake_daemon::DaemonSnapshot::new(1, vec![], modules).catalog)
    }
    async fn open(&self, module: &str) -> Result<Arc<dyn LogTransport>, TransportError> {
        if !self.has_role.load(Ordering::SeqCst) {
            return Err(TransportError::Refusal {
                code: "service_not_provided".into(),
                detail: Some(json!({"service_id":catalog_consumer::SERVICE})),
            });
        }
        if module == "zeta" {
            return Ok(self.extra.lock().unwrap().as_ref().unwrap().clone());
        }
        assert_eq!(module, crate::WRITER_MODULE);
        Ok(self.consumer.clone())
    }
}
struct Fixture {
    tools: Tools,
    registry: FakeRegistry,
    consumers: Arc<FakeConsumers>,
    cingulate: FakeCingulate,
}
fn clock() -> i64 {
    100
}
impl Fixture {
    async fn new() -> Self {
        let mut tools = Tools::new();
        tools.handler.clock = clock;
        tools.admit(Some(&tools.stamp(0)));
        let store = tools.handler.with_store(|s| Ok(s.clone())).unwrap();
        let registry = FakeRegistry::start().await;
        registry.serve_vectors();
        let status = script_free_package(&registry);
        let feed = json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[{"feed_seq":1,"record":status["record"],"signature":status["signature"]}]});
        registry.serve("/v1/scopes/example/status", Reply::bytes(feed.to_string()));
        registry.serve(
            "/v1/scopes/example/status?after=1",
            Reply::bytes(feed.to_string()),
        );
        store
            .catalog_registry_add(
                URL,
                &["@example".into()],
                false,
                "test-registry-1",
                vectors("statements")["keys"]["registry_status"][0]["public_key_hex"]
                    .as_str()
                    .unwrap(),
                100,
            )
            .unwrap();
        let url = registry.url.clone();
        tools.handler.catalog_agent.sources = Arc::new(move |store, scope| {
            crate::catalog_trust::CatalogTrust::with_client(
                store,
                scope,
                RegistryClient::loopback(&url, std::slice::from_ref(&url), Limits::MODULE),
            )
        });
        let consumer = Arc::new(FakeConsumer {
            store,
            check_reply: Mutex::new(json!({"ok":true,"card_section":"Verified skill section"})),
            ready: AtomicBool::new(true),
            calls: Mutex::default(),
        });
        let consumers = Arc::new(FakeConsumers {
            consumer,
            available: AtomicBool::new(true),
            ready: AtomicBool::new(true),
            capability: Mutex::new(catalog_consumer::SKILL.into()),
            extra: Mutex::default(),
            has_role: AtomicBool::new(true),
        });
        tools.handler.catalog_agent.consumers = consumers.clone();
        tools.handler.catalog_agent.capabilities = Arc::new(TestCapabilities);
        let cingulate = FakeCingulate::new();
        cingulate.set_now(100);
        tools.handler.consent_connector = Arc::new(cingulate.clone());
        Self {
            tools,
            registry,
            consumers,
            cingulate,
        }
    }
    fn store(&self) -> RegistryStore {
        self.tools.handler.with_store(|s| Ok(s.clone())).unwrap()
    }
    fn spec(&self, key: &str) -> Value {
        json!({"spec":{"operation":"install","target":{"agent_id":self.tools.agents[0]},"package":NAME,"range":"^1.0.0"},"request_key":key})
    }
    async fn prepare(&self, key: &str) -> Value {
        let prepared = result(self.tools.call("catalog_prepare", self.spec(key)).await);
        assert_eq!(prepared["state"], "pending");
        self.tools.handler.catalog_agent.finished.notified().await;
        result(
            self.tools
                .call("catalog_plan", json!({"plan_id":prepared["plan_id"]}))
                .await,
        )
    }
    async fn install(&self, plan: &Value) -> subc_client_rs::HandlerOutcome {
        self.tools
            .call("catalog_install", json!({"plan_id":plan["plan_id"]}))
            .await
    }
    async fn op(
        &self,
        principal: subc_protocol::Principal,
        method: &str,
        params: Value,
    ) -> subc_client_rs::HandlerOutcome {
        let key = (133, 1);
        self.tools
            .handler
            .route_admissions()
            .insert(key, crate::RouteAdmission::from_bind(Some(principal), None));
        self.tools
            .handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                key,
            )
            .await
    }
    fn grant(&self) {
        let record = vectors("record");
        self.store()
            .insert_publisher_grant(
                &entorhinal_core::catalog_grants::PublisherGrant {
                    id: "test-grant".into(),
                    publisher_fingerprint: record["keys"]["trusted"]["fingerprint"]
                        .as_str()
                        .unwrap()
                        .into(),
                    operation: entorhinal_core::catalog_grants::Operation::Install,
                    placement_agent_id: self.tools.agents[0].clone(),
                    ceiling: vec!["cap/example.read@1".into()],
                    card_ref: Some("prior-card".into()),
                    granted_at_ms: 1,
                    revoked_at_ms: None,
                },
                &json!("test"),
            )
            .unwrap();
    }
}

#[tokio::test]
async fn catalog_prepare_install_card_pins_ready_ack_active() {
    let f = Fixture::new().await;
    let plan = f.prepare("first").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    assert_eq!(plan["closure"][0]["name"], NAME);
    assert_eq!(plan["closure"][0]["trust"], "first_contact");
    assert!(plan["moves"].is_object());
    f.cingulate
        .script("consent.await", Effect::Answer("approve_once"));
    let installed = result(f.install(&plan).await);
    let generation = installed["generation_id"].as_str().unwrap();
    assert_eq!(f.cingulate.card_count(), 1);
    let card = f
        .cingulate
        .calls()
        .into_iter()
        .find(|c| c.method == "consent.request")
        .unwrap()
        .params;
    assert_eq!(card["facts"][0]["value"], "Verified skill section");
    assert_eq!(card["facts"][0]["label"], "Skills (prefrontal-core)");
    assert_eq!(
        f.store().catalog_pins(&f.tools.agents[0]).unwrap()[0].archive_sha256,
        plan["closure"][0]["archive_sha256"]
    );
    let generations = result(f.tools.call("catalog_generation", json!({})).await);
    assert_eq!(generations["generations"][0]["state"], "ready");
    let read = result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            AGENT_GENERATION,
            json!({"agent_id":f.tools.agents[0]}),
        )
        .await,
    );
    assert_eq!(read["ready"][0]["generation_id"], generation);
    assert_eq!(
        read["ready"][0]["pin_digest"],
        f.store().catalog_pin_digest(&f.tools.agents[0]).unwrap()
    );
    let ack = json!({"agent_id":f.tools.agents[0],"generation_id":generation});
    let active = result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            ACK_ACTIVATION,
            ack.clone(),
        )
        .await,
    );
    assert_eq!(active["generation"]["state"], "active");
    assert_eq!(
        result(
            f.op(
                crate::tests::reserved(crate::WRITER_MODULE),
                ACK_ACTIVATION,
                ack
            )
            .await
        ),
        active
    );
    assert!(f.store().verify().unwrap().ok);
}

#[tokio::test]
async fn catalog_covered_install_no_card_and_retry_once() {
    let f = Fixture::new().await;
    f.grant();
    let plan = f.prepare("covered").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    let before = f.store().generation().unwrap();
    let installed = result(f.install(&plan).await);
    let after = f.store().generation().unwrap();
    assert!(after > before);
    assert_eq!(result(f.install(&plan).await), installed);
    assert_eq!(f.store().generation().unwrap(), after);
    assert_eq!(
        f.store()
            .catalog_agent_generations(&f.tools.agents[0])
            .unwrap()
            .len(),
        1
    );
    assert_eq!(f.cingulate.card_count(), 0);
}

#[tokio::test]
async fn catalog_always_publisher_then_covers_next_install() {
    let f = Fixture::new().await;
    let plan = f.prepare("always").await;
    f.cingulate
        .script("consent.await", Effect::Answer("approve_always_publisher"));
    result(f.install(&plan).await);
    let grants = f.store().publisher_grants().unwrap();
    assert_eq!(grants.len(), 1);
    assert_eq!(grants[0].placement_agent_id, f.tools.agents[0]);
    assert_eq!(
        grants[0].operation,
        entorhinal_core::catalog_grants::Operation::Install
    );
    assert_eq!(grants[0].ceiling, vec!["cap/example.read@1"]);
    let second = f.prepare("second").await;
    assert_eq!(second["state"], "ready", "{second}");
    result(f.install(&second).await);
    assert_eq!(f.cingulate.card_count(), 1);
    assert_eq!(
        f.store()
            .catalog_agent_generations(&f.tools.agents[0])
            .unwrap()
            .len(),
        2
    );
}

#[tokio::test]
async fn catalog_stale_base_pins_before_and_after_card() {
    let f = Fixture::new().await;
    let plan = f.prepare("stale").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    // Another approved plan can change pins while this plan awaits user approval.
    f.cingulate.script("consent.await", Effect::FailBefore);
    assert_eq!(error_code(f.install(&plan).await), "consent_pending");
    let pins = vec![serde_json::from_value(
        plan["closure"][0]
            .as_object()
            .unwrap()
            .iter()
            .filter(|(k, _)| {
                !["trust", "capabilities", "has_scripts", "root_certified"].contains(&k.as_str())
            })
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect::<Map<_, _>>()
            .into(),
    )
    .unwrap()];
    f.store()
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.tools.agents[0].clone(),
                pins,
                request_key: None,
            },
            101,
        )
        .unwrap();
    f.cingulate
        .script("consent.await", Effect::Answer("approve_once"));
    assert_eq!(error_code(f.install(&plan).await), "plan_stale");
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f.store().publisher_grants().unwrap().is_empty());
    assert_eq!(f.cingulate.card_count(), 1);
    assert_eq!(error_code(f.install(&plan).await), "plan_stale");
}

#[tokio::test]
async fn catalog_status_change_after_readiness_is_stale() {
    let f = Fixture::new().await;
    let plan = f.prepare("status-stale").await;
    let statuses = f.store().catalog_statuses(URL).unwrap();
    let mut changed: Value = serde_json::from_str(&statuses[0].canonical).unwrap();
    changed["sequence"] = json!(2);
    changed["state"] = json!("withdrawn");
    changed["changed_at"] = json!("2026-10-15T10:00:00Z");
    changed["reason"] = json!("Publisher withdrew this version");
    let doc = signed(
        changed,
        &vectors("statements")["keys"]["registry_status"][0],
    );
    f.registry.serve("/v1/scopes/example/status?after=1",Reply::bytes(json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":2,"records":[{"feed_seq":2,"record":doc["record"],"signature":doc["signature"]}]}).to_string()));
    let mut trust = (f.tools.handler.catalog_agent.sources)(f.store(), "@example").unwrap();
    let refreshed = trust.apply_scope_feed().await.unwrap();
    assert!(
        refreshed.results.iter().all(|(_, r)| r.is_ok()),
        "{refreshed:?}"
    );
    assert_eq!(error_code(f.install(&plan).await), "plan_stale");
    assert_eq!(f.cingulate.card_count(), 0);
}

#[tokio::test]
async fn catalog_consumer_refusal_names_module_and_package() {
    let f = Fixture::new().await;
    *f.consumers.consumer.check_reply.lock().unwrap() = json!({"ok":false,"refusals":[{"package":NAME,"version":"1.0.0","code":"skill_invalid","message":"unsupported runner"}]});
    let plan = f.prepare("refused").await;
    assert_eq!(plan["state"], "failed");
    assert_eq!(plan["refusal"]["code"], "consumer_refused");
    assert_eq!(plan["refusal"]["detail"]["module_id"], crate::WRITER_MODULE);
    assert_eq!(plan["refusal"]["detail"]["refusals"][0]["package"], NAME);
    assert_eq!(error_code(f.install(&plan).await), "plan_failed");
    assert_eq!(f.cingulate.card_count(), 0);
}

#[tokio::test]
async fn catalog_missing_unready_and_wrong_kind_consumer_fail_plan() {
    for mode in ["missing", "unready", "wrong-kind"] {
        let f = Fixture::new().await;
        match mode {
            "missing" => f.consumers.available.store(false, Ordering::SeqCst),
            "unready" => f.consumers.ready.store(false, Ordering::SeqCst),
            _ => *f.consumers.capability.lock().unwrap() = catalog_consumer::FLOW.into(),
        }
        let plan = f.prepare(mode).await;
        assert_eq!(plan["state"], "failed", "{plan}");
        assert_eq!(plan["refusal"]["code"], "consumer_unavailable");
        assert_eq!(plan["refusal"]["detail"]["module_id"], crate::WRITER_MODULE);
        assert_eq!(f.cingulate.card_count(), 0);
    }
}

#[tokio::test]
async fn catalog_refused_consumer_route_fails_plan_and_holds_generation() {
    let f = Fixture::new().await;
    assert_eq!(
        consumer_role(),
        json!({"role":"internal_service","service_id":"catalog-consumer","transport":"bulk","agent_facing":false,"operations":["catalog_consumer.check","catalog_consumer.prefetch"]})
    );
    f.consumers.has_role.store(false, Ordering::SeqCst);
    let plan = f.prepare("no-service-role").await;
    assert_eq!(plan["state"], "failed");
    assert_eq!(plan["refusal"]["code"], "consumer_unavailable");
    assert_eq!(plan["refusal"]["detail"]["module_id"], crate::WRITER_MODULE);
    assert_eq!(
        plan["refusal"]["detail"]["route_refusal"]["code"],
        "service_not_provided"
    );
    assert_eq!(
        plan["refusal"]["detail"]["route_refusal"]["detail"]["service_id"],
        catalog_consumer::SERVICE
    );
    assert_eq!(f.cingulate.card_count(), 0);
    f.consumers.has_role.store(true, Ordering::SeqCst);
    f.grant();
    let plan = f.prepare("role-lost-after-check").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    f.consumers.has_role.store(false, Ordering::SeqCst);
    let installed = result(f.install(&plan).await);
    let id = installed["generation_id"].as_str().unwrap();
    let generation = f
        .store()
        .catalog_generation(id, &f.tools.agents[0])
        .unwrap();
    assert_eq!(generation.state, "approved");
    let error = generation.error.unwrap();
    assert_eq!(error["code"], "consumer_unavailable");
    assert_eq!(error["detail"]["module_id"], crate::WRITER_MODULE);
    assert_eq!(
        error["detail"]["route_refusal"]["code"],
        "service_not_provided"
    );
    f.consumers.has_role.store(true, Ordering::SeqCst);
    assert_eq!(result(f.install(&plan).await), installed);
    assert_eq!(
        f.store()
            .catalog_generation(id, &f.tools.agents[0])
            .unwrap()
            .state,
        "ready"
    );
}

#[tokio::test]
async fn catalog_prefetch_requires_all_checked_consumers_and_retries() {
    let f = Fixture::new().await;
    f.grant();
    let plan = f.prepare("prefetch").await;
    f.consumers.consumer.ready.store(false, Ordering::SeqCst);
    let installed = result(f.install(&plan).await);
    let generation = installed["generation_id"].as_str().unwrap();
    let row = f
        .store()
        .catalog_generation(generation, &f.tools.agents[0])
        .unwrap();
    assert_eq!(row.state, "approved");
    assert_eq!(
        row.error.unwrap()["detail"]["module_id"],
        crate::WRITER_MODULE
    );
    assert_eq!(
        error_code(
            f.op(
                crate::tests::reserved(crate::WRITER_MODULE),
                ACK_ACTIVATION,
                json!({"agent_id":f.tools.agents[0],"generation_id":generation})
            )
            .await
        ),
        "invalid_transition"
    );
    f.consumers.consumer.ready.store(true, Ordering::SeqCst);
    f.consumers.available.store(false, Ordering::SeqCst);
    result(f.install(&plan).await);
    assert_eq!(
        f.store()
            .catalog_generation(generation, &f.tools.agents[0])
            .unwrap()
            .state,
        "approved"
    );
    f.consumers.available.store(true, Ordering::SeqCst);
    assert_eq!(result(f.install(&plan).await), installed);
    let row = f
        .store()
        .catalog_generation(generation, &f.tools.agents[0])
        .unwrap();
    assert_eq!(row.state, "ready");
    assert!(row.error.is_none());
    assert_eq!(f.cingulate.card_count(), 0);
}

#[tokio::test]
async fn catalog_prepare_idempotence_validation_and_admission() {
    let f = Fixture::new().await;
    let plan = f.prepare("repeat").await;
    assert_eq!(
        error_code(
            f.tools
                .call(
                    "catalog_install",
                    json!({"plan_id":plan["plan_id"],"request_key":"not-in-schema"})
                )
                .await
        ),
        "invalid_request"
    );
    let repeated = result(f.tools.call("catalog_prepare", f.spec("repeat")).await);
    assert_eq!(repeated["plan_id"], plan["plan_id"]);
    let mut other = f.spec("repeat");
    other["spec"]["range"] = json!("^2.0.0");
    assert_eq!(
        error_code(f.tools.call("catalog_prepare", other).await),
        "request_key_conflict"
    );
    for (field, value, code) in [
        ("package", json!("bad name"), "invalid_request"),
        ("range", json!("not-semver"), "invalid_request"),
        ("operation", json!("hire"), "operation_not_yet_supported"),
        (
            "target",
            json!({"tier":"global"}),
            "operation_not_yet_supported",
        ),
        (
            "target",
            json!({"agent_id":f.tools.agents[1]}),
            "caller_forbidden",
        ),
    ] {
        let mut args = f.spec("invalid");
        args["spec"][field] = value;
        assert_eq!(
            error_code(f.tools.call("catalog_prepare", args).await),
            code
        );
    }
    f.tools.admit(None);
    for (tool, args) in [
        ("catalog_prepare", f.spec("no-stamp")),
        ("catalog_plan", json!({"plan_id":plan["plan_id"]})),
        ("catalog_install", json!({"plan_id":plan["plan_id"]})),
        ("catalog_generation", json!({})),
    ] {
        assert_eq!(
            error_code(f.tools.call(tool, args).await),
            "caller_forbidden"
        );
    }
    f.tools.admit(Some(&f.tools.stamp(1)));
    assert_eq!(
        error_code(
            f.tools
                .call("catalog_plan", json!({"plan_id":plan["plan_id"]}))
                .await
        ),
        "caller_forbidden"
    );
    assert_eq!(error_code(f.install(&plan).await), "caller_forbidden");
    for principal in [
        subc_protocol::Principal::Direct,
        subc_protocol::Principal::Unverified,
        crate::tests::reserved("prefrontal"),
        crate::tests::reserved("basal-ganglia"),
    ] {
        for op in [AGENT_GENERATION, ACK_ACTIVATION] {
            let params = if op == AGENT_GENERATION {
                json!({"agent_id":f.tools.agents[0]})
            } else {
                json!({"agent_id":f.tools.agents[0],"generation_id":"unknown"})
            };
            assert_eq!(
                error_code(f.op(principal.clone(), op, params).await),
                "caller_forbidden"
            );
        }
    }
}

#[tokio::test]
async fn catalog_production_capabilities_fail_closed_by_token() {
    let mut f = Fixture::new().await;
    f.tools.handler.catalog_agent.capabilities = Arc::new(DaemonCapabilities::default());
    let plan = f.prepare("empty-caps").await;
    assert_eq!(plan["state"], "failed");
    assert_eq!(plan["refusal"]["code"], "no_matching_version");
    assert_eq!(
        plan["refusal"]["detail"]["missing_capabilities"][0],
        json!({"package":NAME,"capability":"cap/example.read@1"})
    );
    assert!(f.consumers.consumer.calls.lock().unwrap().is_empty());
    assert_eq!(f.cingulate.card_count(), 0);
}

#[test]
fn catalog_daemon_capabilities_use_only_ready_valid_tokens_and_warn_once() {
    struct Warnings(Arc<Mutex<Vec<String>>>);
    impl tracing::Subscriber for Warnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields(String);
            impl tracing::field::Visit for Fields {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.push_str(&format!("{}={value:?};", field.name()));
                }
            }
            if *event.metadata().level() == tracing::Level::WARN {
                let mut fields = Fields(String::new());
                event.record(&mut fields);
                self.0.lock().unwrap().push(fields.0);
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }
    let module = |name, ready, provides| json!({"module_id":name,"ready":ready,"roles":[],"control_ops":[],"capabilities":{"provides":provides,"requires":[]}});
    let catalog = crate::fake_daemon::DaemonSnapshot::new(
        1,
        vec![],
        vec![
            module(
                "first",
                true,
                json!([
                    "cap-code-read/v1",
                    "cap-shell/v1",
                    "cap-code-read-extra/v1",
                    catalog_consumer::SKILL
                ]),
            ),
            module(
                "second",
                true,
                json!(["cap-code-read/v1", "cap-code-read-extra/v1"]),
            ),
            module("unready", false, json!(["cap-unready/v1"])),
        ],
    )
    .catalog;
    let source = DaemonCapabilities::default();
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let _subscriber = tracing::subscriber::set_default(Warnings(warnings.clone()));
    for _ in 0..2 {
        assert_eq!(
            source.tokens(&catalog),
            vec!["cap/code.read@1", "cap/shell@1"]
        );
    }
    let captured = warnings.lock().unwrap();
    assert_eq!(captured.len(), 1, "{captured:?}");
    assert!(captured[0].contains("cap-code-read-extra/v1"));
}

#[tokio::test]
async fn catalog_consumer_sections_must_be_nonblank_human_text_and_fit() {
    for section in [
        json!({"title":"not text"}),
        json!([]),
        json!(""),
        json!("  \n"),
        json!("x".repeat(crate::consent::CATALOG_CARD_MAX_BYTES)),
    ] {
        let f = Fixture::new().await;
        let too_large = section
            .as_str()
            .is_some_and(|s| s.len() >= crate::consent::CATALOG_CARD_MAX_BYTES);
        *f.consumers.consumer.check_reply.lock().unwrap() =
            json!({"ok":true,"card_section":section});
        let plan = f.prepare("bad-section").await;
        assert_eq!(plan["state"], "failed", "{plan}");
        assert_eq!(
            plan["refusal"]["code"],
            if too_large {
                "card_too_large"
            } else {
                "consumer_contract_violation"
            }
        );
        assert_eq!(plan["refusal"]["detail"]["module_id"], crate::WRITER_MODULE);
        assert_eq!(f.cingulate.card_count(), 0);
    }
}

#[test]
fn catalog_card_envelope_exact_byte_boundary() {
    let mut card = json!({"facts":[{"label":"Skills","value":""}]});
    let overhead = serde_json::to_vec(&json!({"method":"consent.request","params":card}))
        .unwrap()
        .len();
    card["facts"][0]["value"] = json!("x".repeat(1_310_720 - overhead));
    let sent = crate::consent_client::request_bytes("consent.request", &card).unwrap();
    assert_eq!(sent.len(), 1_310_720);
    assert_eq!(
        sent,
        serde_json::to_vec(&json!({"method":"consent.request","params":card})).unwrap()
    );
    crate::consent::check_catalog_card_size(&card).unwrap();
    let mut extra = card["facts"][0]["value"].as_str().unwrap().to_owned();
    extra.push('x');
    card["facts"][0]["value"] = json!(extra);
    assert_eq!(
        crate::consent::check_catalog_card_size(&card)
            .unwrap_err()
            .code,
        "card_too_large"
    );
}

#[tokio::test]
async fn catalog_third_party_scripts_force_card_despite_grant() {
    let f = Fixture::new().await;
    f.registry.serve_vectors();
    let status = &vectors("status")["records"][0];
    f.registry.serve("/v1/scopes/example/status",Reply::bytes(json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[{"feed_seq":1,"record":status["record"],"signature":status["signature"]}]}).to_string()));
    f.grant();
    let plan = f.prepare("scripts").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    assert_eq!(plan["closure"][0]["has_scripts"], true);
    f.cingulate
        .script("consent.await", Effect::Answer("approve_once"));
    result(f.install(&plan).await);
    assert_eq!(f.cingulate.card_count(), 1);
}

#[tokio::test]
async fn catalog_pending_and_corrupt_bytes_never_raise_card() {
    let f = Fixture::new().await;
    let created = result(f.tools.call("catalog_prepare", f.spec("pending")).await);
    assert_eq!(error_code(f.install(&created).await), "plan_not_ready");
    f.tools.handler.catalog_agent.finished.notified().await;
    let f = Fixture::new().await;
    let status = script_free_package(&f.registry);
    f.registry.serve(
        &format!(
            "/v1/archives/{}",
            status["record"]["archive_sha256"].as_str().unwrap()
        ),
        Reply::bytes(b"corrupt archive".to_vec()),
    );
    let plan = f.prepare("corrupt").await;
    assert_eq!(plan["state"], "failed");
    assert_eq!(plan["refusal"]["code"], "record_mismatch");
    assert!(f.consumers.consumer.calls.lock().unwrap().is_empty());
    assert_eq!(f.cingulate.card_count(), 0);
}

#[tokio::test]
async fn catalog_commit_revalidates_base_under_writer_lock() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    let f = Fixture::new().await;
    let plan = f.prepare("commit-stale").await;
    f.cingulate
        .script("consent.await", Effect::Answer("approve_always_publisher"));
    let lock = f.tools.handler.writer.lock().await;
    let mut install = Box::pin(f.install(&plan));
    assert!(install
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    assert_eq!(f.cingulate.card_count(), 1);
    let detail = f
        .store()
        .catalog_prepared(plan["plan_id"].as_str().unwrap(), &f.tools.agents[0])
        .unwrap();
    f.store()
        .with_principal("test")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: f.tools.agents[0].clone(),
                pins: detail.packages.into_iter().map(|p| p.pin).collect(),
                request_key: None,
            },
            101,
        )
        .unwrap();
    drop(lock);
    assert_eq!(error_code(install.await), "plan_stale");
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f.store().publisher_grants().unwrap().is_empty());
}

#[tokio::test]
async fn catalog_commit_revalidates_publisher_coverage_under_writer_lock() {
    use std::{
        future::Future,
        task::{Context, Waker},
    };
    let f = Fixture::new().await;
    f.grant();
    let plan = f.prepare("grant-revoked").await;
    let lock = f.tools.handler.writer.lock().await;
    let mut install = Box::pin(f.install(&plan));
    assert!(install
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    f.store()
        .revoke_publisher_grant("test-grant", 101, &json!("test"))
        .unwrap();
    drop(lock);
    assert_eq!(error_code(install.await), "plan_stale");
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn catalog_atomic_pin_failure_rolls_back_generation_grant_and_receipt() {
    let f = Fixture::new().await;
    let plan = f.prepare("rollback").await;
    let sql = rusqlite::Connection::open(f.tools.root.join("store.db")).unwrap();
    sql.execute_batch("CREATE TRIGGER fail_pins BEFORE INSERT ON catalog_pin BEGIN SELECT RAISE(ABORT,'injected pin failure'); END;").unwrap();
    f.cingulate
        .script("consent.await", Effect::Answer("approve_always_publisher"));
    assert_eq!(error_code(f.install(&plan).await), "storage_error");
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f.store().publisher_grants().unwrap().is_empty());
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f.store().catalog_audit().unwrap().is_empty());
    sql.execute_batch("DROP TRIGGER fail_pins").unwrap();
    result(f.install(&plan).await);
    assert_eq!(f.cingulate.card_count(), 1);
    assert_eq!(f.store().publisher_grants().unwrap().len(), 1);
    assert_eq!(
        f.store()
            .catalog_agent_generations(&f.tools.agents[0])
            .unwrap()
            .len(),
        1
    );
}

#[tokio::test]
async fn catalog_shared_install_rolls_back_log_refusal() {
    let mut f = Fixture::new().await;
    let log = crate::fake_log::FakeLog::default();
    f.tools.handler.log_client = log.client();
    f.store()
        .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
            Ok(())
        })
        .unwrap();
    f.grant();
    let plan = f.prepare("shared").await;
    log.on_append(crate::fake_log::Action::Refuse {
        code: "id_reused".into(),
        detail: None,
    });
    assert_eq!(error_code(f.install(&plan).await), "identity_log_invariant");
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert_eq!(log.head(), 0);
}

#[tokio::test]
async fn catalog_shared_install_appends_identity_log_once() {
    let mut f = Fixture::new().await;
    let log = crate::fake_log::FakeLog::default();
    f.tools.handler.log_client = log.client();
    f.store()
        .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
            Ok(())
        })
        .unwrap();
    f.grant();
    let plan = f.prepare("shared-success").await;
    let installed = result(f.install(&plan).await);
    assert_eq!(log.head(), 1);
    assert_eq!(result(f.install(&plan).await), installed);
    assert_eq!(log.head(), 1);
    assert_eq!(f.store().catalog_pins(&f.tools.agents[0]).unwrap().len(), 1);
    assert!(f.store().verify().unwrap().ok);
}

#[tokio::test]
async fn catalog_deny_is_terminal_and_does_not_install() {
    let f = Fixture::new().await;
    let plan = f.prepare("deny").await;
    f.cingulate.script("consent.await", Effect::Answer("deny"));
    assert_eq!(error_code(f.install(&plan).await), "consent_declined");
    assert_eq!(error_code(f.install(&plan).await), "consent_declined");
    assert_eq!(f.cingulate.card_count(), 1);
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
    assert!(f
        .store()
        .catalog_agent_generations(&f.tools.agents[0])
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn catalog_generations_hide_superseded_and_activation_needs_no_network() {
    let f = Fixture::new().await;
    f.grant();
    let first = f.prepare("old").await;
    let old = result(f.install(&first).await);
    result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            ACK_ACTIVATION,
            json!({"agent_id":f.tools.agents[0],"generation_id":old["generation_id"]}),
        )
        .await,
    );
    let second = f.prepare("new").await;
    let new = result(f.install(&second).await);
    let before = f.consumers.consumer.calls.lock().unwrap().len();
    f.consumers.available.store(false, Ordering::SeqCst);
    result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            ACK_ACTIVATION,
            json!({"agent_id":f.tools.agents[0],"generation_id":new["generation_id"]}),
        )
        .await,
    );
    let generations = result(f.tools.call("catalog_generation", json!({})).await);
    assert_eq!(generations["generations"].as_array().unwrap().len(), 1);
    assert_eq!(
        generations["generations"][0]["generation_id"],
        new["generation_id"]
    );
    assert_eq!(generations["generations"][0]["state"], "active");
    let core = result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            AGENT_GENERATION,
            json!({"agent_id":f.tools.agents[0]}),
        )
        .await,
    );
    assert_eq!(core["active"]["generation_id"], new["generation_id"]);
    assert_eq!(f.consumers.consumer.calls.lock().unwrap().len(), before);
}

#[tokio::test]
async fn catalog_all_agent_roles_need_authorized_nonflow_nonworker_stamps() {
    let f = Fixture::new().await;
    for role in 0..4 {
        let stamp = f.tools.stamp(role);
        f.tools.admit(Some(&stamp));
        assert_eq!(
            result(f.tools.call("catalog_generation", json!({})).await),
            json!({"generations":[]})
        );
        let mut unauthorized = stamp.clone();
        unauthorized.owner_authorized = false;
        let mut flow = stamp.clone();
        flow.attributes.flow_id = Some("flow".into());
        let mut worker = stamp.clone();
        worker.kind = subc_protocol::scope::ScopeKind::Worker;
        let mut run = stamp.clone();
        run.attributes.run_id = Some("run".into());
        run.attributes.delegates = false;
        for stamp in [unauthorized, flow, worker, run] {
            f.tools.admit(Some(&stamp));
            assert_eq!(
                error_code(f.tools.call("catalog_generation", json!({})).await),
                "caller_forbidden"
            );
        }
    }
}

#[tokio::test]
async fn catalog_losing_one_of_two_checked_consumers_holds_generation() {
    let f = Fixture::new().await;
    f.grant();
    let extra = Arc::new(FakeConsumer {
        store: f.store(),
        check_reply: Mutex::new(json!({"ok":true,"card_section":"Additional skill checks"})),
        ready: AtomicBool::new(true),
        calls: Mutex::default(),
    });
    *f.consumers.extra.lock().unwrap() = Some(extra.clone());
    let plan = f.prepare("two-consumers").await;
    assert_eq!(plan["state"], "ready", "{plan}");
    assert_eq!(plan["card"]["facts"][0]["value"], "Verified skill section");
    assert_eq!(plan["card"]["facts"][1]["value"], "Additional skill checks");
    *f.consumers.extra.lock().unwrap() = None;
    let installed = result(f.install(&plan).await);
    let id = installed["generation_id"].as_str().unwrap();
    let generation = f
        .store()
        .catalog_generation(id, &f.tools.agents[0])
        .unwrap();
    assert_eq!(generation.state, "approved");
    assert_eq!(generation.error.unwrap()["detail"]["module_id"], "zeta");
    *f.consumers.extra.lock().unwrap() = Some(extra.clone());
    assert_eq!(result(f.install(&plan).await), installed);
    assert_eq!(
        f.store()
            .catalog_generation(id, &f.tools.agents[0])
            .unwrap()
            .state,
        "ready"
    );
    assert_eq!(
        extra
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == "catalog_consumer.prefetch")
            .count(),
        1
    );
}

fn forged_commit(f: &Fixture, plan: &Value) -> entorhinal_core::agent::consent::ConsentCommit {
    use entorhinal_core::agent::consent::*;
    let id = plan["plan_id"].as_str().unwrap();
    let arguments = json!({"plan_id":id});
    let target = f
        .store()
        .catalog_install_target(id, &f.tools.agents[0])
        .unwrap();
    ConsentCommit {
        request: ConsentRequest {
            identity: RequestIdentity {
                agent_id: f.tools.agents[0].clone(),
                tool: "catalog_install".into(),
                request_key: id.into(),
            },
            args_digest: cortexkit_role_tool_provider::catalog::composition_digest(&arguments)
                .unwrap(),
            scope_ref: f.tools.stamp(0).scope_ref,
            scope_epoch: 7,
            created_at_ms: 100,
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        },
        authorization: Authorization::Card {
            id: "unrecorded".into(),
            answer: "approve_once".into(),
        },
        action: "catalog_install".into(),
        target: json!(ConsentWrite {
            tool: "catalog_install".into(),
            arguments,
            target,
            grant_class: None,
            scope_id: None
        }),
        granted: None,
        now_ms: 100,
    }
}
#[tokio::test]
async fn catalog_commit_requires_recorded_approval() {
    let f = Fixture::new().await;
    let plan = f.prepare("recorded").await;
    let error = f
        .store()
        .with_principal("agent")
        .with_consent_commit(forged_commit(&f, &plan))
        .catalog_install(plan["plan_id"].as_str().unwrap(), 100)
        .unwrap_err();
    let error = HandlerError::from(error);
    assert_eq!(error.code, "consent_lost");
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
}
#[tokio::test]
async fn catalog_commit_requires_exact_approved_target() {
    let f = Fixture::new().await;
    f.grant();
    let plan = f.prepare("binding").await;
    let mut commit = forged_commit(&f, &plan);
    commit.authorization = entorhinal_core::agent::consent::Authorization::PublisherGrants {
        ids: vec!["test-grant".into()],
    };
    commit.target["target"]["prepared"]["checks"][0]["card_section"] =
        json!("Text different from the approved plan");
    let error = f
        .store()
        .with_principal("agent")
        .with_consent_commit(commit)
        .catalog_install(plan["plan_id"].as_str().unwrap(), 100)
        .unwrap_err();
    assert_eq!(HandlerError::from(error).code, "plan_stale");
    assert!(f
        .store()
        .catalog_pins(&f.tools.agents[0])
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn catalog_always_choice_is_install_only_and_never_root_certified() {
    let f = Fixture::new().await;
    let plan = f.prepare("options").await;
    let id = plan["plan_id"].as_str().unwrap();
    let mut stored = f.store().catalog_plan(id, &f.tools.agents[0]).unwrap();
    let mut detail = f.store().catalog_prepared(id, &f.tools.agents[0]).unwrap();
    let has_always = |card: Value| {
        card["options"]
            .as_array()
            .unwrap()
            .iter()
            .any(|o| o["id"] == "approve_always_publisher")
    };
    assert!(has_always(
        preview_card(&f.store(), &stored, &detail).unwrap()
    ));
    stored.operation = "upgrade".into();
    assert!(!has_always(
        preview_card(&f.store(), &stored, &detail).unwrap()
    ));
    stored.operation = "install".into();
    detail.packages[0].root_certified = true;
    assert!(!has_always(
        preview_card(&f.store(), &stored, &detail).unwrap()
    ));
}

fn serve_status_news(f: &Fixture, state: &str, seq: u64) {
    let mut status: Value =
        serde_json::from_str(&f.store().catalog_statuses(URL).unwrap()[0].canonical).unwrap();
    status["state"] = json!(state);
    status["sequence"] = json!(seq);
    status["reason"] = json!("registry security notice");
    status["compromise_from"] = if state == "revoked" {
        status["published_at"].clone()
    } else {
        Value::Null
    };
    let signed = signed(status, &vectors("statements")["keys"]["registry_status"][0]);
    let feed = json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":seq,"records":[{"feed_seq":seq,"record":signed["record"],"signature":signed["signature"]}]});
    let after = f.store().catalog_feed_seq("@example").unwrap().unwrap();
    f.registry.serve(
        &format!("/v1/scopes/example/status?after={after}"),
        Reply::bytes(feed.to_string()),
    );
    f.registry.serve(
        &format!("/v1/scopes/example/status?after={seq}"),
        Reply::bytes(feed.to_string()),
    );
}
#[tokio::test]
async fn catalog_status_news_views_and_prepare_named_refusal() {
    let f = Fixture::new().await;
    f.grant();
    let plan = f.prepare("status-plan").await;
    let installed = result(f.install(&plan).await);
    let id = installed["generation_id"].as_str().unwrap();
    let agent = &f.tools.agents[0];
    let available = result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            AGENT_GENERATION,
            json!({"agent_id":agent}),
        )
        .await,
    );
    assert!(available["ready"][0].get("pinned_status").is_none());
    let before_news = f.store().catalog_generation(id, agent).unwrap();
    serve_status_news(&f, "withdrawn", 2);
    let mut trust = (f.tools.handler.catalog_agent.sources)(f.store(), "@example").unwrap();
    refresh(&mut trust).await.unwrap();
    assert!(generation_reply(&before_news)
        .get("pinned_status")
        .is_none());
    let withdrawn = result(f.tools.call("catalog_generation", json!({})).await);
    assert_eq!(withdrawn["generations"][0]["state"], "ready");
    assert_eq!(
        withdrawn["generations"][0]["pinned_status"][0]["state"],
        "withdrawn"
    );
    assert_eq!(
        withdrawn["generations"][0]["pinned_status"][0]["reason"],
        "registry security notice"
    );
    result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            ACK_ACTIVATION,
            json!({"agent_id":agent,"generation_id":id}),
        )
        .await,
    );
    serve_status_news(&f, "revoked", 3);
    refresh(&mut trust).await.unwrap();
    let degraded = result(
        f.op(
            crate::tests::reserved(crate::WRITER_MODULE),
            AGENT_GENERATION,
            json!({"agent_id":agent}),
        )
        .await,
    );
    assert_eq!(degraded["active"]["state"], "degraded");
    assert_eq!(
        degraded["active"]["error"],
        json!({"code":"pinned_revoked","name":NAME,"version":"1.0.0","reason":"registry security notice"})
    );
    let refused = f.prepare("revoked-plan").await;
    assert_eq!(refused["state"], "failed");
    assert_eq!(refused["refusal"]["code"], "version_revoked");
    assert_eq!(refused["refusal"]["detail"]["name"], NAME);
}

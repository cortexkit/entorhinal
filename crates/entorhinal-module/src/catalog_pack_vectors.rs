//! Automation-pack samples use verified script-flow bytes and the real served
//! generation, context and held-watch handlers. All identities and clocks are fixed.
use super::*;
use crate::catalog_trust::tests::{signed, vectors};
use crate::HandlerError;
use entorhinal_core::catalog_agent::{
    CatalogPin, CatalogSpec, CatalogTarget, PreparedPackage, PreparedPlan,
};

async fn record(
    f: &Fixture,
    records: &mut Vec<String>,
    case: &str,
    method: &str,
    params: Value,
    route: RouteKey,
) -> Value {
    let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
    let (outcome, bytes) = match f.handler.handle_served_request(&request, route).await {
        HandlerOutcome::Response(bytes) => ("response", String::from_utf8(bytes).unwrap()),
        other => ("error", refusal(other).to_string()),
    };
    records.push(format!(
        "  {{\"case\":{},\"request_bytes\":{},\"outcome\":{},\"reply_bytes\":{}}}",
        json!(case),
        json!(String::from_utf8(request).unwrap()),
        json!(outcome),
        json!(bytes)
    ));
    serde_json::from_str(&bytes).unwrap()
}

struct Watch {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    cancel: tokio::sync::watch::Sender<bool>,
}
#[async_trait::async_trait]
impl crate::watches::WatchTransport for Watch {
    async fn emit(&self, bytes: Vec<u8>) -> Result<(), HandlerError> {
        self.sender.send(bytes).unwrap();
        Ok(())
    }
    async fn ended(&self) {
        let _ = self.cancel.subscribe().wait_for(|done| *done).await;
    }
}

fn watch_record(records: &mut Vec<String>, case: &str, params: Value, bytes: Vec<u8>) {
    let request = json!({"method":"catalog.status_watch","params":params}).to_string();
    records.push(format!(
        "  {{\"case\":{},\"request_bytes\":{},\"outcome\":\"notice\",\"reply_bytes\":{}}}",
        json!(case),
        json!(request),
        json!(String::from_utf8(bytes).unwrap())
    ));
}

#[tokio::test]
async fn catalog_packs_v1_records_match_published_files() {
    use crate::catalog_network::{Limits, RegistryClient};
    use crate::fake_registry::{FakeRegistry, Reply};
    const NAME: &str = "@example/hello-flow";
    const GENERATION: &str = "generation:automation-pack";
    let f = Fixture::new("catalog-pack-vectors").await;
    let source = f.snapshot();
    seed_agent(
        &source,
        ASSISTANT,
        "Automation assistant",
        None,
        false,
        false,
    );
    drop(source);
    assert!(matches!(f.import().await, HandlerOutcome::Response(_)));
    let store = f.handler.with_store(|s| Ok(s.clone())).unwrap();
    let registry = FakeRegistry::start().await;
    registry.serve_vectors();
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
    let mut trust = crate::catalog_trust::CatalogTrust::with_client(
        store.clone(),
        "@example",
        RegistryClient::loopback(
            &registry.url,
            std::slice::from_ref(&registry.url),
            Limits::MODULE,
        ),
    )
    .unwrap();
    trust.clock = || 100;
    let verified = trust.verify_version(NAME, "1.0.0").await.unwrap();
    assert_eq!(verified.manifest.kind, entorhinal_catalog::Kind::Flow);
    assert!(verified.manifest.body.get("script").is_some());
    assert!(verified.manifest.body.get("module").is_none());
    let signed_record = vectors("record")["valid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["record"]["name"] == NAME)
        .unwrap()
        .clone();
    assert_eq!(
        serde_json::to_value(&verified.record).unwrap(),
        signed_record["record"]
    );
    published_file(
        "catalog-packs-v1",
        "flow-record.json",
        vec![format!(
            "  {}",
            json!({"record":verified.record,"signature":signed_record["signature"]})
        )],
    );
    let pin = CatalogPin {
        name: NAME.into(),
        version: "1.0.0".into(),
        kind: entorhinal_catalog::Kind::Flow,
        manifest_sha256: verified.record.manifest_sha256.clone(),
        archive_sha256: verified.record.archive_sha256.clone(),
        publisher_fingerprint: entorhinal_catalog::record::record_fingerprint(&verified.record)
            .unwrap(),
        root: true,
        range: Some("^1.0.0".into()),
    };
    store
        .with_principal("samples")
        .catalog_set_pins(
            entorhinal_core::catalog_agent::PinWriteRequest {
                agent_id: ASSISTANT.into(),
                pins: vec![pin.clone()],
                request_key: None,
            },
            100,
        )
        .unwrap();
    store
        .catalog_sync_fast_lane(std::slice::from_ref(&pin), 100)
        .unwrap();
    store
        .catalog_create_plan(
            "plan:automation-pack",
            ASSISTANT,
            CatalogTarget::Agent(ASSISTANT.into()),
            "install",
            CatalogSpec {
                package: NAME.into(),
                range: "^1.0.0".into(),
            },
            100,
        )
        .unwrap();
    store
        .catalog_save_prepared(
            "plan:automation-pack",
            ASSISTANT,
            &PreparedPlan {
                packages: vec![PreparedPackage {
                    pin: pin.clone(),
                    trust: "first_contact".into(),
                    capabilities: verified.manifest.requires.capabilities.clone(),
                    has_scripts: true,
                    root_certified: false,
                }],
                moves: json!({}),
                checks: vec![],
            },
        )
        .unwrap();
    store
        .catalog_plan_ready("plan:automation-pack", ASSISTANT, vec![pin.clone()], 200)
        .unwrap();
    store
        .catalog_approve_plan("plan:automation-pack", ASSISTANT, GENERATION, 300)
        .unwrap();
    store
        .catalog_generation_ready(GENERATION, ASSISTANT)
        .unwrap();
    f.success(
        CORE,
        "catalog.ack_activation",
        json!({"agent_id":ASSISTANT,"generation_id":GENERATION}),
    )
    .await;
    let mut generations = Vec::new();
    let fresh = record(
        &f,
        &mut generations,
        "fresh-flow-generation",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
        CORE,
    )
    .await;
    assert!(fresh["result"]["active"].get("pinned_status").is_none());
    let mut context = Vec::new();
    record(
        &f,
        &mut context,
        "fresh-flow-context",
        "context.session",
        json!({"agent_id":ASSISTANT}),
        READER,
    )
    .await;
    f.handler
        .watches
        .observe_catalog(store.catalog_served_statuses().unwrap());
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let sink = Watch {
        sender,
        cancel: tokio::sync::watch::channel(false).0,
    };
    let body = json!({"method":"catalog.status_watch","params":{}})
        .to_string()
        .into_bytes();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    let opening = tokio::select! { biased; _ = &mut watch => panic!("watch ended"), bytes = receiver.recv() => bytes.unwrap() };
    let mut watches = Vec::new();
    watch_record(&mut watches, "opening-resync", json!({}), opening);
    store.catalog_advance_freshness(900_100).unwrap();
    f.handler
        .watches
        .observe_catalog(store.catalog_served_statuses().unwrap());
    let changed = tokio::select! { biased; _ = &mut watch => panic!("watch ended"), bytes = receiver.recv() => bytes.unwrap() };
    watch_record(&mut watches, "stale-pin-changes", json!({}), changed);
    let stale = record(
        &f,
        &mut generations,
        "stale-available-flow-generation",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
        CORE,
    )
    .await;
    assert_eq!(stale["result"]["active"]["state"], "active");
    assert_eq!(
        stale["result"]["active"]["pinned_status"][0]["stale_since_ms"],
        900_100
    );
    record(
        &f,
        &mut context,
        "stale-flow-context",
        "context.session",
        json!({"agent_id":ASSISTANT}),
        READER,
    )
    .await;
    // A foreign token gets a resync, while the first watch stays open for
    // the clearing and revocation notices that follow.
    let (foreign_sender, mut foreign_receiver) = tokio::sync::mpsc::unbounded_channel();
    let foreign_sink = Watch {
        sender: foreign_sender,
        cancel: tokio::sync::watch::channel(false).0,
    };
    let params = json!({"token":"foreign:2"});
    let body = json!({"method":"catalog.status_watch","params":params})
        .to_string()
        .into_bytes();
    let foreign = f.handler.serve_watch(&foreign_sink, &body, READER);
    tokio::pin!(foreign);
    let bytes = tokio::select! { biased; _ = &mut foreign => panic!("watch ended"), bytes = foreign_receiver.recv() => bytes.unwrap() };
    watch_record(&mut watches, "foreign-token-resync", params, bytes);
    foreign_sink.cancel.send_replace(true);
    assert!(matches!(foreign.await, HandlerOutcome::Streamed));
    // A complete feed with no new records confirms the held status, which
    // clears the stale marker; the open watch reports that as a change.
    registry.serve(
        "/v1/scopes/example/status",
        Reply::bytes(
            json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":1,"records":[]})
                .to_string(),
        ),
    );
    trust.clock = || 950_000;
    trust.apply_scope_feed().await.unwrap();
    f.handler
        .watches
        .observe_catalog(store.catalog_served_statuses().unwrap());
    let cleared = tokio::select! { biased; _ = &mut watch => panic!("watch ended"), bytes = receiver.recv() => bytes.unwrap() };
    watch_record(&mut watches, "stale-cleared-changes", json!({}), cleared);
    let mut status = vectors("status")["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["record"]["name"] == NAME)
        .unwrap()["record"]
        .clone();
    status["state"] = json!("revoked");
    status["sequence"] = json!(2);
    status["reason"] = json!("publisher security notice");
    status["compromise_from"] = status["published_at"].clone();
    let doc = signed(status, &vectors("statements")["keys"]["registry_status"][0]);
    let feed = json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":2,"records":[{"feed_seq":2,"record":doc["record"],"signature":doc["signature"]}]});
    registry.serve("/v1/scopes/example/status", Reply::bytes(feed.to_string()));
    trust.clock = || 1_000_000;
    trust.apply_scope_feed().await.unwrap();
    f.handler
        .watches
        .observe_catalog(store.catalog_served_statuses().unwrap());
    let revocation = tokio::select! { biased; _ = &mut watch => panic!("watch ended"), bytes = receiver.recv() => bytes.unwrap() };
    watch_record(&mut watches, "revoked-pin-changes", json!({}), revocation);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
    let revoked = record(
        &f,
        &mut generations,
        "revoked-flow-generation",
        "catalog.agent_generation",
        json!({"agent_id":ASSISTANT}),
        CORE,
    )
    .await;
    assert_eq!(revoked["result"]["active"]["state"], "degraded");
    assert_eq!(
        revoked["result"]["active"]["error"]["code"],
        "pinned_revoked"
    );
    let blocked = record(
        &f,
        &mut generations,
        "revoked-flow-activation-blocked",
        "catalog.ack_activation",
        json!({"agent_id":ASSISTANT,"generation_id":GENERATION}),
        CORE,
    )
    .await;
    assert_eq!(blocked["code"], "activation_blocked");
    published_file("catalog-packs-v1", "generations.json", generations);
    published_file("catalog-packs-v1", "context.json", context);
    published_file("catalog-packs-v1", "status-watch.json", watches);
    let folder =
        PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../test-vectors/catalog-packs-v1");
    let sums = [
        "context.json",
        "flow-record.json",
        "generations.json",
        "status-watch.json",
    ]
    .map(|name| {
        format!(
            "{}  {name}\n",
            entorhinal_catalog::sha256_hex(std::fs::read(folder.join(name)).unwrap())
        )
    })
    .concat();
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::write(folder.join("SHA256SUMS"), sums).unwrap();
    } else {
        assert_eq!(
            std::fs::read_to_string(folder.join("SHA256SUMS")).unwrap(),
            sums
        );
    }
}

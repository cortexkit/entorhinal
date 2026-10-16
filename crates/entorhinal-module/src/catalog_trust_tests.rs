use super::*;
use crate::fake_registry::{FakeRegistry, Reply};
use base64::Engine;
use ed25519_dalek::{Signer, SigningKey};
const URL: &str = "https://registry.example.invalid";
const NAME: &str = "@example/hello-skill";
const VERSION: &str = "1.0.0";
pub(crate) fn vectors(file: &str) -> Value {
    serde_json::from_str(match file {
        "statements" => {
            include_str!("../../entorhinal-catalog/tests/vectors/ck-catalog-v1/statements.json")
        }
        "status" => {
            include_str!("../../entorhinal-catalog/tests/vectors/ck-catalog-v1/status.json")
        }
        "record" => {
            include_str!("../../entorhinal-catalog/tests/vectors/ck-catalog-v1/record.json")
        }
        _ => panic!("unknown vectors"),
    })
    .unwrap()
}
pub(crate) fn signed(value: Value, key: &Value) -> Value {
    let seed: [u8; 32] = entorhinal_catalog::decode_hex(key["seed_hex"].as_str().unwrap()).unwrap();
    let signer = SigningKey::from_bytes(&seed);
    assert_eq!(
        entorhinal_catalog::hex(signer.verifying_key().as_bytes()),
        key["public_key_hex"]
    );
    let text = document(&value).unwrap();
    let sig = signer.sign(&entorhinal_catalog::signing_input(
        value["schema"].as_str().unwrap(),
        &text,
    ));
    json!({"record":value,"signature":entorhinal_catalog::hex(&sig.to_bytes())})
}
fn status_doc() -> Value {
    let v = vectors("status");
    json!({"record":v["records"][0]["record"],"signature":v["records"][0]["signature"]})
}
fn package_doc() -> Value {
    let v = vectors("record");
    let r = v["valid"]
        .as_array()
        .unwrap()
        .iter()
        .find(|r| r["record"]["name"] == NAME)
        .unwrap();
    json!({"name":NAME,"versions":[{"version":VERSION,"record":r["record"],"signature":r["signature"],"status":status_doc()}]})
}
fn serve_package(fake: &FakeRegistry, reply: &Value) {
    fake.serve(
        "/v1/packages/example/hello-skill",
        Reply::bytes(reply.to_string()),
    );
}
fn serve_feed(fake: &FakeRegistry, seq: u64, docs: Vec<Value>, path: &str) {
    let entries = docs
        .iter()
        .enumerate()
        .map(|(n, doc)| json!({"feed_seq":n+1,"record":doc["record"],"signature":doc["signature"]}))
        .collect::<Vec<_>>();
    fake.serve(path,Reply::bytes(json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":seq,"records":entries}).to_string()));
}
struct Fixture {
    root: std::path::PathBuf,
    descriptor: cortexkit_store_types::StorageDescriptor,
    fake: FakeRegistry,
}
impl Fixture {
    async fn new(key: usize) -> Self {
        let (root, descriptor) = crate::tests::scratch_descriptor("catalog-trust");
        let fake = FakeRegistry::start().await;
        fake.serve_vectors();
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .catalog_registry_add(
                URL,
                &["@example".into(), "@cortexkit".into()],
                false,
                &format!("test-registry-{}", key + 1),
                vectors("statements")["keys"]["registry_status"][key]["public_key_hex"]
                    .as_str()
                    .unwrap(),
                100,
            )
            .unwrap();
        Self {
            root,
            descriptor,
            fake,
        }
    }
    fn trust(&self) -> CatalogTrust {
        CatalogTrust::with_client(
            RegistryStore::open(&self.descriptor).unwrap(),
            "@example",
            RegistryClient::loopback(
                &self.fake.url,
                std::slice::from_ref(&self.fake.url),
                Limits::MODULE,
            ),
        )
        .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn error(e: NetworkError, code: &str, field: Option<&str>) {
    assert_eq!(e.code(), code, "{e:?}");
    if let Some(field) = field {
        assert_eq!(e.detail().unwrap()["field"], field);
    }
}
#[tokio::test]
async fn chain_walk_validates_held_links_and_persists_cutoffs() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    trust.refresh_registry_keys().await.unwrap();
    assert_eq!(trust.registry.keys.len(), 3);
    assert_eq!(
        trust.registry.keys[2].valid_from.as_deref(),
        Some("2027-05-01T00:00:00Z")
    );
    trust.refresh_registry_keys().await.unwrap();
    let mut reply: Value =
        serde_json::from_slice(&f.fake.replies.lock().unwrap()["/v1/registry/keys"].body).unwrap();
    reply["keys"][0]["statement"]["valid_from"] = "2027-01-02T00:00:00Z".into();
    f.fake
        .serve("/v1/registry/keys", Reply::bytes(reply.to_string()));
    error(
        trust.refresh_registry_keys().await.unwrap_err(),
        "key_conflict",
        Some("key_id"),
    );
    assert_eq!(trust.registry.keys.len(), 3);
    let key = &vectors("statements")["keys"]["registry_status"][1];
    for (at, expected) in [
        ("2027-04-30T23:59:59Z", None),
        ("2027-05-01T00:00:00Z", Some("key_not_valid")),
    ] {
        let mut doc = status_doc()["record"].clone();
        doc["registry_key_id"] = "test-registry-2".into();
        doc["changed_at"] = at.into();
        doc["sequence"] = if expected.is_some() {
            3.into()
        } else {
            2.into()
        };
        let mut package = package_doc();
        package["versions"][0]["status"] = signed(doc, key);
        serve_package(&f.fake, &package);
        let result = trust.verify_version(NAME, VERSION).await;
        if let Some(code) = expected {
            error(result.unwrap_err(), code, None);
        } else {
            result.unwrap();
        }
    }
}
#[tokio::test]
async fn chain_null_anchor_and_refused_successor() {
    for key in [1, 2] {
        let f = Fixture::new(key).await;
        let mut trust = f.trust();
        trust.refresh_registry_keys().await.unwrap();
        assert_eq!(trust.registry.keys.len(), 3 - key);
        assert!(trust.registry.keys[0].statement.is_some());
        assert!(trust.registry.keys[0].valid_from.is_none());
    }
    let f = Fixture::new(1).await;
    let mut trust = f.trust();
    let mut reply: Value =
        serde_json::from_slice(&f.fake.replies.lock().unwrap()["/v1/registry/keys"].body).unwrap();
    reply["keys"][0]["statement"]["registry"] = "https://other.invalid".into();
    f.fake
        .serve("/v1/registry/keys", Reply::bytes(reply.to_string()));
    error(
        trust.refresh_registry_keys().await.unwrap_err(),
        "record_mismatch",
        Some("registry"),
    );
    assert!(trust.registry.keys[0].statement.is_none());
    let mut reply: Value =
        serde_json::from_slice(&f.fake.replies.lock().unwrap()["/v1/registry/keys"].body).unwrap();
    reply["keys"][0]["statement"]["registry"] = URL.into();
    reply["keys"][0]["statement"]["public_key"] =
        vectors("statements")["keys"]["registry_status"][0]["public_key_hex"].clone();
    f.fake
        .serve("/v1/registry/keys", Reply::bytes(reply.to_string()));
    error(
        trust.refresh_registry_keys().await.unwrap_err(),
        "key_conflict",
        None,
    );
    assert!(trust.registry.keys[0].statement.is_none());
}
#[tokio::test]
async fn registry_statement_vectors_over_http() {
    for case in vectors("statements")["ck-registry-key/v1"]["cases"]
        .as_array()
        .unwrap()
    {
        let f = Fixture::new(0).await;
        let mut trust = f.trust();
        let pinned: statements::RegistryChain =
            serde_json::from_value(case["pinned"].clone()).unwrap();
        trust
            .store()
            .unwrap()
            .catalog_trust_transaction::<_, CatalogError>("@example", |_, _, _| {
                Ok((
                    (),
                    TrustWrite {
                        keys: Some(
                            pinned
                                .keys
                                .iter()
                                .enumerate()
                                .map(|(n, k)| RegistryKey {
                                    chain_index: n as i64,
                                    key_id: k.key_id.clone(),
                                    public_key: k.public_key.clone(),
                                    statement: k.statement.clone(),
                                    valid_from: None,
                                })
                                .collect(),
                        ),
                        ..Default::default()
                    },
                ))
            })
            .unwrap();
        trust.registry = trust
            .store()
            .unwrap()
            .catalog_registries()
            .unwrap()
            .remove(0);
        let text = case["statement_text"].as_str().unwrap();
        let envelope = format!("{{\"schema\":\"ck-registry-keys/v1\",\"registry\":\"{URL}\",\"keys\":[{{\"statement\":{text},\"signature\":{}}}]}}",case["signature"]);
        f.fake.serve("/v1/registry/keys", Reply::bytes(envelope));
        let result = trust.refresh_registry_keys().await;
        if let Some(expected) = case["expect"]["code"].as_str() {
            assert_eq!(result.unwrap_err().code(), expected, "{}", case["name"]);
        } else {
            result.unwrap();
        }
    }
}
#[tokio::test]
async fn verify_status_identity_binding_and_separate_blob_publication() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    error(
        trust
            .verify_version("@other/hello-skill", VERSION)
            .await
            .unwrap_err(),
        "record_mismatch",
        Some("scope"),
    );
    assert!(f.fake.calls.lock().unwrap().is_empty());
    let mut package = package_doc();
    package["versions"][0]["record"]["name"] = "@example/other".into();
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "record_mismatch",
        Some("name"),
    );
    assert!(trust
        .store()
        .unwrap()
        .catalog_statuses(URL)
        .unwrap()
        .is_empty());
    let mut package = package_doc();
    package["versions"][0]["record"]["version"] = "2.0.0".into();
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "record_mismatch",
        Some("version"),
    );
    assert!(trust
        .store()
        .unwrap()
        .catalog_statuses(URL)
        .unwrap()
        .is_empty());
    package = package_doc();
    package["versions"][0]["status"]["signature"] = "00".repeat(64).into();
    serve_package(&f.fake, &package);
    let e = trust.verify_version(NAME, VERSION).await.unwrap_err();
    assert_eq!(e.detail().unwrap()["name"], NAME);
    error(e, "signature_invalid", None);
    assert!(trust
        .store()
        .unwrap()
        .catalog_statuses(URL)
        .unwrap()
        .is_empty());
    serve_package(&f.fake, &package_doc());
    trust.verify_version(NAME, VERSION).await.unwrap();
    let held = trust.store().unwrap().catalog_statuses(URL).unwrap()[0]
        .canonical
        .clone();
    let mut other_publisher = package_doc();
    let other_key = &vectors("statements")["keys"]["publishers"][1];
    other_publisher["versions"][0]["record"]["public_key"] = other_key["public_key_hex"].clone();
    other_publisher["versions"][0]["signature"] =
        signed(other_publisher["versions"][0]["record"].clone(), other_key)["signature"].clone();
    serve_package(&f.fake, &other_publisher);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "record_mismatch",
        Some("publisher_key"),
    );
    for field in ["archive_sha256", "publisher_key", "published_at"] {
        let mut package = package_doc();
        let mut doc = status_doc()["record"].clone();
        doc["sequence"] = 2.into();
        doc[field] = if field == "published_at" {
            "2026-10-13T09:00:00Z".into()
        } else {
            "ab".repeat(32).into()
        };
        if field == "archive_sha256" {
            package["versions"][0]["record"][field] = doc[field].clone();
        }
        package["versions"][0]["status"] =
            signed(doc, &vectors("statements")["keys"]["registry_status"][0]);
        serve_package(&f.fake, &package);
        f.fake.calls.lock().unwrap().clear();
        error(
            trust.verify_version(NAME, VERSION).await.unwrap_err(),
            "record_mismatch",
            Some(field),
        );
        assert!(!f
            .fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .any(|p| p.starts_with("/v1/archives/")));
        assert_eq!(
            trust.store().unwrap().catalog_statuses(URL).unwrap()[0].canonical,
            held
        );
    }
    let mut tombstone = status_doc()["record"].clone();
    tombstone["sequence"] = 3.into();
    tombstone["state"] = "tombstoned".into();
    tombstone["reason"] = "removed".into();
    let mut package = package_doc();
    package["versions"][0]["status"] = signed(
        tombstone,
        &vectors("statements")["keys"]["registry_status"][0],
    );
    serve_package(&f.fake, &package);
    let digest = package["versions"][0]["record"]["archive_sha256"]
        .as_str()
        .unwrap();
    f.fake.serve(
        &format!("/v1/archives/{digest}"),
        Reply {
            status: 410,
            ..Reply::bytes(vec![])
        },
    );
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "registry_http_error",
        None,
    );
    assert_eq!(
        serde_json::from_str::<Value>(
            &trust.store().unwrap().catalog_statuses(URL).unwrap()[0].canonical
        )
        .unwrap()["state"],
        "tombstoned"
    );
    serve_package(&f.fake, &package_doc());
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "feed_rollback",
        None,
    );
}
#[tokio::test]
async fn feed_mixed_unheld_binding_guards_and_full_fallback() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    serve_feed(&f.fake, 1, vec![status_doc()], "/v1/scopes/example/status");
    assert!(trust.apply_scope_feed().await.unwrap().cursor_advanced);
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.contains("packages") || p.contains("archives")));
    let before = trust.store().unwrap().catalog_statuses(URL).unwrap()[0]
        .canonical
        .clone();
    for field in ["published_at", "publisher_key"] {
        let mut doc = status_doc()["record"].clone();
        doc["sequence"] = 2.into();
        doc[field] = if field == "published_at" {
            "2026-10-13T00:00:00Z".into()
        } else {
            "ab".repeat(32).into()
        };
        serve_feed(
            &f.fake,
            2,
            vec![signed(
                doc,
                &vectors("statements")["keys"]["registry_status"][0],
            )],
            "/v1/scopes/example/status?after=1",
        );
        let outcome = trust.apply_scope_feed().await.unwrap();
        assert!(!outcome.cursor_advanced);
        assert_eq!(
            outcome.results[0]
                .1
                .as_ref()
                .unwrap_err()
                .detail
                .as_ref()
                .unwrap()["field"],
            field
        );
        assert_eq!(
            trust.store().unwrap().catalog_statuses(URL).unwrap()[0].canonical,
            before
        );
    }
    let mut other = status_doc()["record"].clone();
    other["name"] = "@example/unheld".into();
    let mut other = signed(other, &vectors("statements")["keys"]["registry_status"][0]);
    other["signature"] = "00".repeat(64).into();
    serve_feed(
        &f.fake,
        2,
        vec![status_doc(), other],
        "/v1/scopes/example/status?after=1",
    );
    let outcome = trust.apply_scope_feed().await.unwrap();
    assert!(!outcome.cursor_advanced);
    assert_eq!(
        outcome.results[1].1.as_ref().unwrap_err().code,
        "signature_invalid"
    );
    assert_eq!(
        trust.store().unwrap().catalog_feed_seq("@example").unwrap(),
        Some(1)
    );
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 410,
            ..Reply::bytes(json!({"error":{"code":"feed_after_unavailable"}}).to_string())
        },
    );
    assert!(trust.apply_scope_feed().await.unwrap().cursor_advanced);
    serve_feed(&f.fake, 0, vec![], "/v1/scopes/example/status?after=1");
    f.fake.calls.lock().unwrap().clear();
    assert!(trust.apply_scope_feed().await.unwrap().cursor_advanced);
    assert_eq!(
        &*f.fake.calls.lock().unwrap(),
        &[
            "/v1/registry/keys",
            "/v1/scopes/example/status?after=1",
            "/v1/scopes/example/status"
        ]
    );
}
#[tokio::test]
async fn unknown_status_key_fetches_once_and_cannot_self_authorize() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let mut package = package_doc();
    let mut doc = status_doc()["record"].clone();
    doc["registry_key_id"] = "unknown".into();
    package["versions"][0]["status"] = signed(doc, &vectors("statements")["keys"]["publishers"][0]);
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "key_unknown",
        None,
    );
    assert_eq!(
        f.fake
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|p| p.as_str() == "/v1/registry/keys")
            .count(),
        1
    );
    assert!(trust
        .store()
        .unwrap()
        .catalog_statuses(URL)
        .unwrap()
        .is_empty());
}
#[tokio::test]
async fn stored_status_reopen_checks_columns_and_rollback() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    serve_package(&f.fake, &package_doc());
    trust.verify_version(NAME, VERSION).await.unwrap();
    drop(trust);
    let mut trust = f.trust();
    trust.verify_version(NAME, VERSION).await.unwrap();
    let rows = trust.store().unwrap().catalog_statuses(URL).unwrap();
    trust
        .store()
        .unwrap()
        .catalog_trust_transaction::<_, CatalogError>("@example", |_, _, _| {
            let mut bad = rows[0].clone();
            bad.sequence += 1;
            Ok((
                (),
                TrustWrite {
                    statuses: vec![bad],
                    ..Default::default()
                },
            ))
        })
        .unwrap();
    drop(trust);
    let mut trust = f.trust();
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "status_invalid",
        None,
    );
}
#[tokio::test]
async fn scope_rotations_keep_lineage_and_bound_revocation() {
    let f = Fixture::new(0).await;
    let trust = f.trust();
    let v = vectors("statements");
    let old = &v["keys"]["publishers"][0];
    let new = &v["keys"]["publishers"][1];
    let third = &v["keys"]["publishers"][2];
    let rotation = |from: &Value, to: &Value| {
        let statement = json!({"schema":"ck-key-rotation/v1","scope":"example","old_key":from["public_key_hex"],"new_key":to["public_key_hex"]});
        json!({"statement":statement,"old_signature":signed(statement.clone(),from)["signature"],"new_signature":signed(statement.clone(),to)["signature"]})
    };
    let entry = |key: &Value, from: &str, until: Option<&str>, rev: Value, rotation: Value| json!({"public_key":key["public_key_hex"],"fingerprint":key["fingerprint"],"valid_from":from,"valid_until":until,"revocation":rev,"rotation":rotation,"cert":null});
    let base = json!({"scope":"example","keys":[entry(old,"2026-01-01T00:00:00Z",Some("2026-06-01T00:00:00Z"),Value::Null,Value::Null),entry(new,"2026-06-01T00:00:00Z",Some("2026-08-01T00:00:00Z"),Value::Null,rotation(old,new)),entry(third,"2026-08-01T00:00:00Z",None,Value::Null,rotation(new,third))]});
    for (compromise, count) in [
        (None, 3),
        (Some("2026-07-01T00:00:00Z"), 3),
        (Some("2026-06-01T00:00:00Z"), 1),
    ] {
        let mut reply = base.clone();
        if let Some(at) = compromise {
            reply["keys"][0]["revocation"] = json!({"reason":"compromised","compromise_from":at});
        }
        f.fake
            .serve("/v1/scopes/example", Reply::bytes(reply.to_string()));
        assert_eq!(trust.scope_keys(None).await.unwrap().len(), count);
    }
    for signature in ["old_signature", "new_signature"] {
        let mut reply = base.clone();
        reply["keys"][1]["rotation"][signature] = "00".repeat(64).into();
        f.fake
            .serve("/v1/scopes/example", Reply::bytes(reply.to_string()));
        assert_eq!(trust.scope_keys(None).await.unwrap().len(), 1);
    }
    let mut reply = base.clone();
    reply["keys"].as_array_mut().unwrap().remove(0);
    f.fake
        .serve("/v1/scopes/example", Reply::bytes(reply.to_string()));
    assert!(trust.scope_keys(None).await.unwrap().is_empty());
    let mut bad = base.clone();
    bad["keys"][0]["fingerprint"] = "ab".repeat(32).into();
    f.fake
        .serve("/v1/scopes/example", Reply::bytes(bad.to_string()));
    error(
        trust.scope_keys(None).await.unwrap_err(),
        "record_mismatch",
        Some("fingerprint"),
    );
    bad["scope"] = "other".into();
    f.fake
        .serve("/v1/scopes/example", Reply::bytes(bad.to_string()));
    error(
        trust.scope_keys(None).await.unwrap_err(),
        "record_mismatch",
        Some("scope"),
    );
}
#[tokio::test]
async fn cert_vectors_over_http_and_record_key_windows() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    trust.scope = "@cortexkit".into();
    let v = vectors("statements");
    let root = v["keys"]["root"]["public_key_hex"].as_str().unwrap();
    for case in v["ck-key-cert/v1"]["cases"].as_array().unwrap() {
        let cert = &case["served"];
        let statement = &cert["statement"];
        let pk = statement["public_key"].as_str().unwrap();
        let reply = json!({"scope":"cortexkit","keys":[{"public_key":pk,"fingerprint":entorhinal_catalog::key_fingerprint(pk).unwrap_or_default(),"valid_from":statement["valid_from"],"valid_until":statement["valid_until"],"rotation":null,"revocation":null,"cert":cert}]});
        f.fake
            .serve("/v1/scopes/cortexkit", Reply::bytes(reply.to_string()));
        let result = trust.scope_keys(Some(root)).await;
        if case["expect"]["code"].is_string() {
            assert!(result.is_err(), "{}", case["name"]);
        } else {
            assert_eq!(result.unwrap().len(), 1);
        }
        assert!(trust.scope_keys(None).await.unwrap().is_empty());
    }
    let cert = &v["ck-key-cert/v1"]["cases"][0]["served"];
    let statement = &cert["statement"];
    let reply = json!({"scope":"cortexkit","keys":[{"public_key":statement["public_key"],"fingerprint":entorhinal_catalog::key_fingerprint(statement["public_key"].as_str().unwrap()).unwrap(),"valid_from":statement["valid_from"],"valid_until":statement["valid_until"],"rotation":null,"revocation":null,"cert":cert}]});
    f.fake
        .serve("/v1/scopes/cortexkit", Reply::bytes(reply.to_string()));
    let keys = trust.scope_keys(Some(root)).await.unwrap();
    let mut expanded = reply.clone();
    expanded["keys"][0]["valid_from"] = "2026-09-01T00:00:00Z".into();
    f.fake
        .serve("/v1/scopes/cortexkit", Reply::bytes(expanded.to_string()));
    error(
        trust.scope_keys(Some(root)).await.unwrap_err(),
        "record_mismatch",
        Some("cert"),
    );
    let package = package_doc();
    let bytes = vectors("record")["packages"]["skill"]["manifest_text"]
        .as_str()
        .unwrap()
        .as_bytes()
        .to_vec();
    let archive = base64::engine::general_purpose::STANDARD
        .decode(
            vectors("record")["packages"]["skill"]["archive_base64"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    let mut rec = package["versions"][0]["record"].clone();
    rec["public_key"] = statement["public_key"].clone();
    let rec = signed(rec, &v["keys"]["cortexkit_publish"][0]);
    for (at, valid) in [
        ("2026-09-30T23:59:59Z", false),
        ("2026-10-01T00:00:00Z", true),
        ("2027-03-31T23:59:59Z", true),
        ("2027-04-01T00:00:00Z", false),
    ] {
        let result = record::verify_record(
            &document(&rec["record"]).unwrap(),
            rec["signature"].as_str().unwrap(),
            &bytes,
            &archive,
            &record::Trust {
                keys: &keys,
                published_at: Some(at),
            },
        );
        if valid {
            result.unwrap();
        } else {
            assert_eq!(result.unwrap_err().code, RefusalCode::KeyNotValid);
        }
    }
    let mut revoked = keys.clone();
    revoked[0].revocation = Some(statements::Revocation {
        reason: "compromised".into(),
        compromise_from: "2026-10-14T09:00:00Z".into(),
    });
    for at in [
        None,
        Some("2026-10-14T09:00:00Z"),
        Some("2026-10-15T09:00:00Z"),
    ] {
        assert_eq!(
            record::verify_record(
                &document(&rec["record"]).unwrap(),
                rec["signature"].as_str().unwrap(),
                &bytes,
                &archive,
                &record::Trust {
                    keys: &revoked,
                    published_at: at
                }
            )
            .unwrap_err()
            .code,
            RefusalCode::KeyRevoked
        );
    }
    record::verify_record(
        &document(&rec["record"]).unwrap(),
        rec["signature"].as_str().unwrap(),
        &bytes,
        &archive,
        &record::Trust {
            keys: &revoked,
            published_at: Some("2026-10-14T08:59:59Z"),
        },
    )
    .unwrap();
    for (sig, code) in [
        (rec["signature"].as_str().unwrap(), RefusalCode::KeyUnknown),
        ("00", RefusalCode::SignatureInvalid),
    ] {
        assert_eq!(
            record::verify_record(
                &document(&rec["record"]).unwrap(),
                sig,
                &bytes,
                &archive,
                &record::Trust {
                    keys: &[],
                    published_at: Some("2026-10-14T09:00:00Z")
                }
            )
            .unwrap_err()
            .code,
            code
        );
    }
}
#[test]
fn production_root_accepts_only_its_certificates() {
    let root = CORTEXKIT_ROOT.expect("the production root is compiled in");
    // The certificate the root signed for the first @cortexkit publishing key.
    let issued = json!({
        "public_key": "cf865c91b3dc0ae0103d417720c35d64b766427364e94ae811962d55b8120c68",
        "schema": "ck-key-cert/v1",
        "scope": "cortexkit",
        "valid_from": "2026-10-16T00:00:00Z",
        "valid_until": "2027-10-16T00:00:00Z",
    });
    let signature = "7926ed099ff19a2f6a80f573dbfff67dadda7f12b2beb1433d007aee9d842ea182ea2fb5e3c9aad196cc9f078b22529b2f25096bc124ff337bb9ec945fdaae0c";
    let key = statements::verify_key_cert(&document(&issued).unwrap(), signature, root)
        .expect("the compiled root verifies the publishing key it certified");
    assert_eq!(key.public_key, issued["public_key"]);
    // Certificates signed by the TEST-ONLY root must never verify in production.
    let v = vectors("statements");
    let cert = &v["ck-key-cert/v1"]["cases"][0]["served"];
    assert!(
        statements::verify_key_cert(
            &document(&cert["statement"]).unwrap(),
            cert["signature"].as_str().unwrap(),
            root
        )
        .is_err(),
        "production root must not be the TEST-ONLY root"
    );
}
#[tokio::test]
async fn in_flight_verification_rechecks_status_repin_and_registry_identity() {
    for race in ["status", "repin", "replacement"] {
        let f = Fixture::new(0).await;
        let mut trust = f.trust();
        let entered = std::sync::Arc::new(tokio::sync::Notify::new());
        let release = std::sync::Arc::new(tokio::sync::Notify::new());
        f.fake.serve(
            "/v1/packages/example/hello-skill",
            Reply {
                entered: Some(entered.clone()),
                release: Some(release.clone()),
                ..Reply::bytes(package_doc().to_string())
            },
        );
        let store = trust.store().unwrap().clone();
        let task = tokio::spawn(async move { trust.verify_version(NAME, VERSION).await });
        entered.notified().await;
        match race {
            "status" => {
                serve_feed(
                    &f.fake,
                    2,
                    vec![
                        json!({"record":vectors("status")["records"][1]["record"],"signature":vectors("status")["records"][1]["signature"]}),
                    ],
                    "/v1/scopes/example/status",
                );
                let client = RegistryClient::loopback(
                    &f.fake.url,
                    std::slice::from_ref(&f.fake.url),
                    Limits::MODULE,
                );
                let mut other =
                    CatalogTrust::with_client(store.clone(), "@example", client).unwrap();
                other.apply_scope_feed().await.unwrap();
            }
            "repin" => {
                store
                    .catalog_registry_repin(
                        URL,
                        "replacement",
                        vectors("statements")["keys"]["registry_status"][1]["public_key_hex"]
                            .as_str()
                            .unwrap(),
                        "2026-10-14T09:00:00Z",
                    )
                    .unwrap();
            }
            _ => {
                store.catalog_registry_remove(URL).unwrap();
                store
                    .catalog_registry_add(
                        URL,
                        &["@example".into()],
                        false,
                        "test-registry-1",
                        vectors("statements")["keys"]["registry_status"][1]["public_key_hex"]
                            .as_str()
                            .unwrap(),
                        101,
                    )
                    .unwrap();
            }
        }
        release.notify_one();
        error(
            task.await.unwrap().unwrap_err(),
            match race {
                "status" => "feed_rollback",
                "repin" => "key_not_valid",
                _ => "registry_changed",
            },
            None,
        );
        assert_eq!(
            BlobStore::new(store.store_folder())
                .unwrap()
                .read_verified(
                    package_doc()["versions"][0]["record"]["archive_sha256"]
                        .as_str()
                        .unwrap()
                )
                .unwrap_err()
                .code,
            "blob_unknown"
        );
    }
}

#[tokio::test]
async fn record_trust_precedes_malformed_archive_and_never_fetches_untrusted_bytes() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let mut package = package_doc();
    package["versions"][0]["signature"] = "00".repeat(64).into();
    let digest = package["versions"][0]["record"]["archive_sha256"]
        .as_str()
        .unwrap();
    f.fake.serve(
        &format!("/v1/archives/{digest}"),
        Reply::bytes(b"not a tar archive"),
    );
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "signature_invalid",
        None,
    );
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.contains("archives") || p.ends_with("ck.json")));
    assert_eq!(
        trust.store().unwrap().catalog_statuses(URL).unwrap().len(),
        1
    );
    assert_eq!(
        BlobStore::new(trust.store().unwrap().store_folder())
            .unwrap()
            .read_verified(digest)
            .unwrap_err()
            .code,
        "blob_unknown"
    );
}
#[tokio::test]
async fn rotation_statement_client_cases_over_http() {
    let f = Fixture::new(0).await;
    let trust = f.trust();
    let v = vectors("statements");
    for case in v["ck-key-rotation/v1"]["cases"].as_array().unwrap() {
        let served = &case["served"];
        let statement = &served["statement"];
        let mut keys = case["scope_keys"].as_array().unwrap().clone();
        for key in &mut keys {
            key["fingerprint"] =
                entorhinal_catalog::key_fingerprint(key["public_key"].as_str().unwrap())
                    .unwrap()
                    .into();
            key["rotation"] = Value::Null;
            key["cert"] = Value::Null;
        }
        let pk = statement["new_key"].as_str().unwrap();
        keys.push(json!({"public_key":pk,"fingerprint":entorhinal_catalog::key_fingerprint(pk).unwrap(),"valid_from":"2026-06-01T00:00:00Z","valid_until":null,"revocation":null,"rotation":served,"cert":null}));
        f.fake.serve(
            "/v1/scopes/example",
            Reply::bytes(json!({"scope":"example","keys":keys}).to_string()),
        );
        let result = trust.scope_keys(None).await;
        let expected = case["expect"]["ok"] == true
            || matches!(
                case["name"].as_str(),
                Some("old_key_retired" | "new_key_was_in_a_scope" | "old_key_revoked")
            );
        let kept = result
            .as_ref()
            .is_ok_and(|keys| keys.iter().any(|key| key.public_key == pk));
        assert_eq!(kept, expected, "{}: {result:?}", case["name"]);
    }
}
#[tokio::test]
async fn cortexkit_http_uses_only_explicit_root_and_refuses_production_slot() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    trust.scope = "@cortexkit".into();
    let v = vectors("statements");
    let key = &v["keys"]["cortexkit_publish"][0];
    let root = v["keys"]["root"]["public_key_hex"].as_str().unwrap();
    let bytes = vectors("record")["packages"]["skill"]["manifest_text"]
        .as_str()
        .unwrap()
        .replace("@example/hello-skill", "@cortexkit/hello-skill")
        .into_bytes();
    let original = base64::engine::general_purpose::STANDARD
        .decode(
            vectors("record")["packages"]["skill"]["archive_base64"]
                .as_str()
                .unwrap(),
        )
        .unwrap();
    let members = entorhinal_catalog::archive::read(&original).unwrap();
    let mut archive_members = vec![entorhinal_catalog::archive::Member::regular(
        "ck.json", &bytes,
    )];
    for (name, bytes) in &members {
        if *name != "ck.json" {
            archive_members.push(entorhinal_catalog::archive::Member::regular(name, bytes));
        }
    }
    let archive = entorhinal_catalog::archive::write(&archive_members).unwrap();
    let mut package = package_doc();
    let rec = &mut package["versions"][0]["record"];
    rec["name"] = "@cortexkit/hello-skill".into();
    rec["public_key"] = key["public_key_hex"].clone();
    rec["manifest_sha256"] = entorhinal_catalog::sha256_hex(&bytes).into();
    rec["archive_sha256"] = entorhinal_catalog::sha256_hex(&archive).into();
    rec["archive_bytes"] = archive.len().into();
    package["versions"][0]["signature"] = signed(rec.clone(), key)["signature"].clone();
    package["name"] = "@cortexkit/hello-skill".into();
    let mut stat = status_doc()["record"].clone();
    stat["name"] = package["name"].clone();
    stat["archive_sha256"] = package["versions"][0]["record"]["archive_sha256"].clone();
    stat["publisher_key"] = key["fingerprint"].clone();
    package["versions"][0]["status"] = signed(stat, &v["keys"]["registry_status"][0]);
    let cert = &v["ck-key-cert/v1"]["cases"][0]["served"];
    f.fake.serve("/v1/scopes/cortexkit",Reply::bytes(json!({"scope":"cortexkit","keys":[{"public_key":key["public_key_hex"],"fingerprint":key["fingerprint"],"valid_from":cert["statement"]["valid_from"],"valid_until":cert["statement"]["valid_until"],"revocation":null,"rotation":null,"cert":cert}]}).to_string()));
    f.fake.serve(
        "/v1/packages/cortexkit/hello-skill",
        Reply::bytes(package.to_string()),
    );
    f.fake.serve(
        "/v1/packages/cortexkit/hello-skill/1.0.0/ck.json",
        Reply::bytes(bytes),
    );
    f.fake.serve(
        &format!("/v1/archives/{}", entorhinal_catalog::sha256_hex(&archive)),
        Reply::bytes(archive),
    );
    // The compiled production root must not accept a publishing key certified by
    // the TEST-ONLY root (the published test key the test vectors are signed
    // with), and no manifest or archive may be downloaded before that check.
    error(
        trust
            .verify_version("@cortexkit/hello-skill", VERSION)
            .await
            .unwrap_err(),
        "signature_invalid",
        None,
    );
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.contains("archives") || p.ends_with("ck.json")));
    let (verified, first_party) = trust
        .with_test_root(root.into())
        .verify_version_with_provenance("@cortexkit/hello-skill", VERSION)
        .await
        .unwrap();
    assert!(first_party);
    assert_eq!(verified.record.name, "@cortexkit/hello-skill");
}
#[tokio::test]
async fn archive_barrier_cannot_publish_after_newer_status() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let digest = package_doc()["versions"][0]["record"]["archive_sha256"]
        .as_str()
        .unwrap()
        .to_string();
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    let original = f.fake.replies.lock().unwrap()[&format!("/v1/archives/{digest}")].clone();
    f.fake.serve(
        &format!("/v1/archives/{digest}"),
        Reply {
            entered: Some(entered.clone()),
            release: Some(release.clone()),
            ..original
        },
    );
    let store = trust.store().unwrap().clone();
    let task = tokio::spawn(async move { trust.verify_version(NAME, VERSION).await });
    entered.notified().await;
    let mut other = CatalogTrust::with_client(
        store.clone(),
        "@example",
        RegistryClient::loopback(
            &f.fake.url,
            std::slice::from_ref(&f.fake.url),
            Limits::MODULE,
        ),
    )
    .unwrap();
    serve_feed(
        &f.fake,
        2,
        vec![
            json!({"record":vectors("status")["records"][1]["record"],"signature":vectors("status")["records"][1]["signature"]}),
        ],
        "/v1/scopes/example/status",
    );
    other.apply_scope_feed().await.unwrap();
    release.notify_one();
    error(task.await.unwrap().unwrap_err(), "feed_rollback", None);
    assert_eq!(
        BlobStore::new(store.store_folder())
            .unwrap()
            .read_verified(&digest)
            .unwrap_err()
            .code,
        "blob_unknown"
    );
    assert_eq!(store.catalog_statuses(URL).unwrap()[0].sequence, 2);
}

#[tokio::test]
async fn feed_etag_304_preserves_cursor_and_status() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    serve_feed(&f.fake, 1, vec![status_doc()], "/v1/scopes/example/status");
    f.fake
        .replies
        .lock()
        .unwrap()
        .get_mut("/v1/scopes/example/status")
        .unwrap()
        .headers
        .push(("ETag".into(), "\"feed-one\"".into()));
    trust.apply_scope_feed().await.unwrap();
    assert_eq!(
        trust
            .store()
            .unwrap()
            .catalog_feed_etag("@example")
            .unwrap()
            .as_deref(),
        Some("\"feed-one\"")
    );
    let before = trust.store().unwrap().catalog_statuses(URL).unwrap()[0]
        .canonical
        .clone();
    f.fake.serve(
        "/v1/scopes/example/status?after=1",
        Reply {
            status: 304,
            ..Reply::bytes(vec![])
        },
    );
    assert!(!trust.apply_scope_feed().await.unwrap().cursor_advanced);
    assert!(f
        .fake
        .requests
        .lock()
        .unwrap()
        .last()
        .unwrap()
        .to_ascii_lowercase()
        .contains("if-none-match: \"feed-one\""));
    assert_eq!(
        trust.store().unwrap().catalog_statuses(URL).unwrap()[0].canonical,
        before
    );
    assert_eq!(
        trust.store().unwrap().catalog_feed_seq("@example").unwrap(),
        Some(1)
    );
}

#[tokio::test]
async fn same_clock_same_key_readd_refuses_old_incarnation() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let original = trust.registry.incarnation.clone();
    assert!(entorhinal_catalog::is_hex(&original, 32));
    assert!(serde_json::to_value(&trust.registry)
        .unwrap()
        .get("incarnation")
        .is_none());
    let entered = std::sync::Arc::new(tokio::sync::Notify::new());
    let release = std::sync::Arc::new(tokio::sync::Notify::new());
    f.fake.serve(
        "/v1/packages/example/hello-skill",
        Reply {
            entered: Some(entered.clone()),
            release: Some(release.clone()),
            ..Reply::bytes(package_doc().to_string())
        },
    );
    let store = trust.store().unwrap().clone();
    let task = tokio::spawn(async move { trust.verify_version(NAME, VERSION).await });
    entered.notified().await;
    store.catalog_registry_remove(URL).unwrap();
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
    assert_ne!(store.catalog_registries().unwrap()[0].incarnation, original);
    release.notify_one();
    error(task.await.unwrap().unwrap_err(), "registry_changed", None);
    assert!(store.catalog_statuses(URL).unwrap().is_empty());
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.starts_with("/v1/archives/")));
}

#[tokio::test]
async fn feed_records_two_held_and_one_unheld_without_package_requests() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    trust.verify_version(NAME, VERSION).await.unwrap();
    trust
        .verify_version("@example/hello-flow", VERSION)
        .await
        .unwrap();
    let status = vectors("status");
    let flow = status["records"]
        .as_array()
        .unwrap()
        .iter()
        .find(|s| s["record"]["name"] == "@example/hello-flow")
        .unwrap();
    let mut unheld = status_doc()["record"].clone();
    unheld["name"] = "@example/not-downloaded".into();
    serve_feed(
        &f.fake,
        3,
        vec![
            status_doc(),
            json!({"record":flow["record"],"signature":flow["signature"]}),
            signed(unheld, &vectors("statements")["keys"]["registry_status"][0]),
        ],
        "/v1/scopes/example/status",
    );
    f.fake.calls.lock().unwrap().clear();
    assert!(trust.apply_scope_feed().await.unwrap().cursor_advanced);
    assert_eq!(
        trust.store().unwrap().catalog_statuses(URL).unwrap().len(),
        3
    );
    assert_eq!(
        trust.store().unwrap().catalog_feed_seq("@example").unwrap(),
        Some(3)
    );
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.contains("packages") || p.contains("archives")));
}
#[tokio::test]
async fn publisher_windows_use_status_time_through_http() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let base: Value =
        serde_json::from_slice(&f.fake.replies.lock().unwrap()["/v1/scopes/example"].body).unwrap();
    for (from, until, allowed) in [
        ("2026-10-14T09:00:01Z", None, false),
        ("2026-10-14T09:00:00Z", None, true),
        ("2026-01-01T00:00:00Z", Some("2026-10-14T09:00:01Z"), true),
        ("2026-01-01T00:00:00Z", Some("2026-10-14T09:00:00Z"), false),
    ] {
        let mut reply = base.clone();
        reply["keys"][0]["valid_from"] = from.into();
        reply["keys"][0]["valid_until"] = json!(until);
        f.fake
            .serve("/v1/scopes/example", Reply::bytes(reply.to_string()));
        f.fake.calls.lock().unwrap().clear();
        let result = trust.verify_version(NAME, VERSION).await;
        if allowed {
            result.unwrap();
        } else {
            error(result.unwrap_err(), "key_not_valid", None);
            assert!(!f
                .fake
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|p| p.contains("archives") || p.ends_with("ck.json")));
        }
    }
}

#[tokio::test]
async fn status_refusal_precedes_package_shape_and_invalid_status_does_not_fetch_keys() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let mut package = package_doc();
    package["versions"][0]["record"]["public_key"] = 42.into();
    package["versions"][0]["status"]["signature"] = "00".repeat(64).into();
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "signature_invalid",
        None,
    );
    assert!(trust
        .store()
        .unwrap()
        .catalog_statuses(URL)
        .unwrap()
        .is_empty());
    package["versions"][0]["status"]["signature"] = status_doc()["signature"].clone();
    serve_package(&f.fake, &package);
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "record_invalid",
        None,
    );
    assert_eq!(
        trust.store().unwrap().catalog_statuses(URL).unwrap().len(),
        1
    );
    let mut package = package_doc();
    package["versions"][0]["status"]["record"]["registry_key_id"] = "unknown".into();
    package["versions"][0]["status"]["record"]["sequence"] = 0.into();
    serve_package(&f.fake, &package);
    f.fake.calls.lock().unwrap().clear();
    error(
        trust.verify_version(NAME, VERSION).await.unwrap_err(),
        "status_invalid",
        None,
    );
    assert!(!f
        .fake
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|path| path == "/v1/registry/keys"));
}

/// Checks that each refused registry-key document is logged only once, however
/// often it is refused. The test reads the trust walker's own record of logged
/// documents (`refused_links`, the SHA-256 digests of documents already
/// logged) instead of counting emitted tracing events. Counting events was
/// flaky in parallel runs, because tracing caches each callsite's interest
/// globally and another test thread can register it first without this test's
/// subscriber.
#[tokio::test]
async fn refused_chain_links_log_once_per_distinct_document() {
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let mut reply: Value =
        serde_json::from_slice(&f.fake.replies.lock().unwrap()["/v1/registry/keys"].body).unwrap();
    reply["keys"][0]["signature"] = "00".repeat(64).into();
    f.fake
        .serve("/v1/registry/keys", Reply::bytes(reply.to_string()));
    for _ in 0..2 {
        error(
            trust.refresh_registry_keys().await.unwrap_err(),
            "signature_invalid",
            None,
        );
    }
    assert_eq!(trust.refused_links.len(), 1);
    reply["keys"][0]["signature"] = "01".repeat(64).into();
    f.fake
        .serve("/v1/registry/keys", Reply::bytes(reply.to_string()));
    error(
        trust.refresh_registry_keys().await.unwrap_err(),
        "signature_invalid",
        None,
    );
    assert_eq!(trust.refused_links.len(), 2);
}

#[tokio::test(start_paused = true)]
async fn trust_status_sql_does_not_occupy_runtime() {
    use crate::catalog_sql::tests::{finish, poll, responsive, started, tracked, HeldSql};
    let f = Fixture::new(0).await;
    let mut trust = f.trust();
    let store = trust.store().unwrap();
    let handler = crate::ProjectsHandler::with_runtime("trust-held-sql".into(), || 0);
    *handler.store.lock().unwrap() = Some(store.clone());
    let held = HeldSql::new(store.clone()).await;
    let (refresh, entered) = tracked(trust.refresh_registry_keys());
    let mut refresh = Box::pin(refresh);
    started(refresh.as_mut(), &entered).await;
    assert!(poll(refresh.as_mut()).is_pending());
    responsive(&handler).await;
    held.finish().await;
    finish(refresh).await.unwrap();

    let held = HeldSql::new(store.clone()).await;
    let (verify, entered) = tracked(trust.verify_version(NAME, VERSION));
    let mut verify = Box::pin(verify);
    started(verify.as_mut(), &entered).await;
    assert!(poll(verify.as_mut()).is_pending());
    responsive(&handler).await;
    held.finish().await;
    finish(verify).await.unwrap();

    serve_feed(&f.fake, 1, vec![status_doc()], "/v1/scopes/example/status");
    let held = HeldSql::new(store).await;
    let (feed, entered) = tracked(trust.apply_scope_feed());
    let mut feed = Box::pin(feed);
    started(feed.as_mut(), &entered).await;
    assert!(poll(feed.as_mut()).is_pending());
    responsive(&handler).await;
    held.finish().await;
    finish(feed).await.unwrap();
}

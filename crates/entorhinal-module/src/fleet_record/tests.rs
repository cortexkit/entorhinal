use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use cortexkit_claustrum_client::{
    ClaustrumCredentialResolver, CredentialGetTarget, CredentialResolverError,
    CredentialResolverFuture,
};
use ed25519_dalek::{Signer, SigningKey};
use entorhinal_catalog::{hex, sha256_hex};
use serde_json::json;

use super::{
    vault::{SubcEpochVault, VaultCall},
    *,
};

type VaultFuture<'a> = CredentialResolverFuture<'a, Result<Value, CredentialResolverError>>;
const FIXTURE_SEED: u8 = 0x51;
const FIXTURE_KEY_ID: &str = "4b735ac174e40636";
const ROTATED_SEED: u8 = 0x52;

enum Call {
    PublicKey(u8),
    Sign {
        seed: u8,
        reply_key_id: String,
        input: Option<String>,
    },
    Epoch {
        request: String,
        response: String,
    },
}

struct VaultScript(Mutex<VecDeque<Call>>);

impl VaultScript {
    fn new(calls: impl IntoIterator<Item = Call>) -> Arc<Self> {
        Arc::new(Self(Mutex::new(calls.into_iter().collect())))
    }

    fn next(&self) -> Call {
        self.0
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected call to the vault")
    }

    fn done(&self) {
        assert!(
            self.0.lock().unwrap().is_empty(),
            "expected more vault calls"
        );
    }

    fn signer(self: &Arc<Self>) -> ClaustrumCredentialResolver {
        ClaustrumCredentialResolver::with_target(self.clone(), 0)
    }

    fn epoch_vault(self: &Arc<Self>) -> SubcEpochVault {
        SubcEpochVault::with_call(self.clone())
    }
}

fn signing_key(seed: u8) -> SigningKey {
    SigningKey::from_bytes(&[seed; 32])
}

fn key_id(seed: u8) -> String {
    sha256_hex(signing_key(seed).verifying_key().as_bytes())[..16].to_owned()
}

impl CredentialGetTarget for VaultScript {
    fn credential_get<'a>(&'a self, _: &'a str, _: Option<u64>, _: bool) -> VaultFuture<'a> {
        panic!("the codec must never read private keys")
    }

    fn credential_public_key<'a>(&'a self, id: &'a str) -> VaultFuture<'a> {
        Box::pin(async move {
            assert_eq!(id, "signing:fleet-record:1");
            let Call::PublicKey(seed) = self.next() else {
                panic!("unexpected public key read")
            };
            Ok(
                json!({"algorithm":"ed25519", "key_id":key_id(seed), "public_key_hex":hex(signing_key(seed).verifying_key().as_bytes())}),
            )
        })
    }

    fn credential_sign<'a>(&'a self, id: &'a str, payload: &'a [u8]) -> VaultFuture<'a> {
        Box::pin(async move {
            assert_eq!(id, "signing:fleet-record:1");
            let Call::Sign {
                seed,
                reply_key_id,
                input,
            } = self.next()
            else {
                panic!("unexpected sign")
            };
            if let Some(input) = input {
                assert_eq!(payload, input.as_bytes());
            }
            let signature = signing_key(seed).sign(payload);
            Ok(
                json!({"key_id":reply_key_id, "signature_b64":STANDARD.encode(signature.to_bytes())}),
            )
        })
    }
}

#[async_trait]
impl VaultCall for VaultScript {
    async fn send(&self, _: &'static str, request_bytes: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
        let Call::Epoch { request, response } = self.next() else {
            panic!("unexpected epoch operation")
        };
        assert_eq!(request_bytes, request.as_bytes());
        Ok(response.into_bytes())
    }
}

fn cases() -> Value {
    serde_json::from_str(include_str!(
        "../../tests/fixtures/fleet-record-envelope-v1/cases.json"
    ))
    .unwrap()
}

fn text<'a>(value: &'a Value, field: &str) -> &'a str {
    value[field].as_str().unwrap()
}

fn header(sample: &Value) -> Header {
    let aad: Value = serde_json::from_str(text(sample, "aad_utf8")).unwrap();
    Header {
        kind: text(&aad, "kind").into(),
        key: text(&aad, "key").into(),
        writer: text(&aad, "writer").into(),
        writer_ep: aad["writer_ep"].as_u64().unwrap(),
        entry_id: text(&aad, "entry_id").into(),
    }
}

fn sample_seal(sample: &Value) -> Call {
    let envelope: Value = serde_json::from_str(text(sample, "envelope_utf8")).unwrap();
    Call::Epoch {
        request: format!(r#"{{"method":"fleet.epoch_seal","params":{{"aad":"{}","plaintext":"{}"}}}}"#,
            URL_SAFE_NO_PAD.encode(text(sample,"aad_utf8")), URL_SAFE_NO_PAD.encode(text(sample,"plaintext_utf8"))),
        response: json!({"result":{"ciphertext":envelope["ciphertext"], "epoch":envelope["epoch"], "nonce":envelope["nonce"]}}).to_string(),
    }
}

fn sample_open(sample: &Value) -> Call {
    let envelope: Value = serde_json::from_str(text(sample, "envelope_utf8")).unwrap();
    Call::Epoch {
        request: format!(
            r#"{{"method":"fleet.epoch_open","params":{{"aad":"{}","ciphertext":{},"epoch":{},"nonce":{}}}}}"#,
            URL_SAFE_NO_PAD.encode(text(sample, "aad_utf8")),
            envelope["ciphertext"],
            envelope["epoch"],
            envelope["nonce"]
        ),
        response:
            json!({"result":{"plaintext":URL_SAFE_NO_PAD.encode(text(sample,"plaintext_utf8"))}})
                .to_string(),
    }
}

#[tokio::test]
async fn fleet_record_matches_independent_envelope_samples() {
    let vectors = cases();
    let public_key = text(&vectors, "public_key_hex");
    for sample in [&vectors["log"], &vectors["residence"]] {
        let vault = VaultScript::new([
            Call::PublicKey(FIXTURE_SEED),
            sample_seal(sample),
            Call::Sign {
                seed: FIXTURE_SEED,
                reply_key_id: FIXTURE_KEY_ID.into(),
                input: Some(text(sample, "signing_input_utf8").into()),
            },
            sample_open(sample),
        ]);
        let epoch_vault = vault.epoch_vault();
        let header = header(sample);
        let plaintext = text(sample, "plaintext_utf8");
        let expected = text(sample, "envelope_utf8").as_bytes();
        let prepared = prepare(
            &vault.signer(),
            &epoch_vault,
            &header,
            &serde_json::from_str(plaintext).unwrap(),
        )
        .await
        .unwrap();
        assert_eq!(prepared.into_bytes(), expected);

        let incoming = UnverifiedRecord::parse(&header.key, expected).unwrap();
        assert_eq!(incoming.key_version(), FIXTURE_KEY_ID);
        assert_eq!(incoming.header(), &header);
        assert_eq!(incoming.input, text(sample, "signing_input_utf8"));
        let incoming = incoming.verify(public_key).unwrap();
        assert_eq!(
            incoming.open(&epoch_vault).await.unwrap(),
            plaintext.as_bytes()
        );
        vault.done();
    }
}

#[tokio::test]
async fn fleet_record_rejects_tampered_samples_before_open() {
    let vectors = cases();
    let vault = VaultScript::new([]);
    for sample in vectors["negative"].as_array().unwrap() {
        let result = async {
            UnverifiedRecord::parse(
                text(sample, "destination"),
                text(sample, "envelope_utf8").as_bytes(),
            )?
            .verify(text(&vectors, "public_key_hex"))?
            .open(&vault.epoch_vault())
            .await
        }
        .await;
        assert_eq!(
            result.unwrap_err().code,
            text(sample, "error"),
            "altered {}",
            text(sample, "altered")
        );
    }
    let good = text(&vectors["residence"], "envelope_utf8");
    assert_eq!(
        UnverifiedRecord::parse("residence.agent-18", good.as_bytes())
            .unwrap_err()
            .code,
        "record_invalid"
    );
    vault.done();
}

#[tokio::test]
async fn fleet_record_control_kinds_are_signed_without_an_epoch() {
    let vectors = cases();
    let mut header = header(&vectors["log"]);
    for (kind, key) in [
        ("member.key", LOG_KEY),
        ("member.removed", LOG_KEY),
        ("member.admitted", LOG_KEY),
        ("epoch", "epoch"),
        ("wrap", "wrap.mint.box"),
        ("wrapreq", "wrapreq.box"),
        ("wrapjoin", "wrapjoin.box.request"),
    ] {
        header.kind = kind.into();
        header.key = key.into();
        let vault = VaultScript::new([
            Call::PublicKey(FIXTURE_SEED),
            Call::Sign {
                seed: FIXTURE_SEED,
                reply_key_id: FIXTURE_KEY_ID.into(),
                input: None,
            },
        ]);
        let mut body = json!({"\u{e000}": "BMP", "\u{10000}": [null, 0.000001]});
        if kind == "member.key" {
            // A key announcement must name the vault's current signing key.
            body["version"] = FIXTURE_KEY_ID.into();
            body["pub"] = text(&vectors, "public_key_hex").into();
        }
        let prepared = prepare(&vault.signer(), &vault.epoch_vault(), &header, &body)
            .await
            .unwrap();
        assert_eq!(prepared.epoch(), None);
        let bytes = prepared.into_bytes();
        let encoded: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(encoded["body"], body);
        for name in ["epoch", "ciphertext", "nonce"] {
            assert!(encoded.get(name).is_none(), "{kind} has a {name} field");
        }
        let read = UnverifiedRecord::parse(key, &bytes)
            .unwrap()
            .verify(text(&vectors, "public_key_hex"))
            .unwrap();
        // The literal pins JCS's UTF-16 key order; a key announcement adds
        // ASCII members that sort ahead of both.
        let expected = if kind == "member.key" {
            format!(
                "{{\"pub\":\"{}\",\"version\":\"{FIXTURE_KEY_ID}\",\"\u{10000}\":[null,0.000001],\"\u{e000}\":\"BMP\"}}",
                text(&vectors, "public_key_hex")
            )
        } else {
            "{\"\u{10000}\":[null,0.000001],\"\u{e000}\":\"BMP\"}".to_owned()
        };
        assert_eq!(
            read.open(&vault.epoch_vault()).await.unwrap(),
            expected.as_bytes()
        );
        vault.done();
    }
}

#[test]
fn fleet_record_refuses_ambiguous_or_noncanonical_envelopes() {
    let vectors = cases();
    let good = text(&vectors["residence"], "envelope_utf8");
    let expected_key = "residence.agent-17";
    let duplicate = good.replace("\"epoch\":3,", "\"epoch\":3,\"epoch\":3,");
    let mixed = good.replacen('{', "{\"body\":null,", 1);
    let noncanonical = format!("{good}\n");
    for bytes in [duplicate, mixed, noncanonical] {
        assert_eq!(
            UnverifiedRecord::parse(expected_key, bytes.as_bytes())
                .unwrap_err()
                .code,
            "record_invalid"
        );
    }
}

#[tokio::test]
async fn fleet_record_epoch_rpc_replays_vault_samples() {
    // Exact bodies from the claustrum (credential vault) repository's
    // fleet-epoch-vectors-v1 tag, file
    // crates/credentials-module/tests/fixtures/fleet-epoch-v1/cases.json.
    // The wire response wraps each body in a `result` field.
    let seal = r#"{"method":"fleet.epoch_seal","params":{"aad":"cmVjb3JkOmlkZW50aXR5Ojc","plaintext":"ZmxlZXQgc2FtcGxlIHBsYWludGV4dA"}}"#;
    let vault = VaultScript::new([
        Call::Epoch {
            request: seal.into(),
            response: r#"{"result":{"ciphertext":"SE_5Sjamf8Gf7w6OFP65Ue---EIPRps5oQPt0EscLBNzB-47Sww","epoch":1,"nonce":"63-10RwCM-PbIM3o9PqroTlLsdPsNBP_"}}"#.into(),
        },
        Call::Epoch {
            request: r#"{"method":"fleet.epoch_open","params":{"aad":"cmVjb3JkOmlkZW50aXR5Ojc","ciphertext":"SE_5Sjamf8Gf7w6OFP65Ue---EIPRps5oQPt0EscLBNzB-47Sww","epoch":1,"nonce":"63-10RwCM-PbIM3o9PqroTlLsdPsNBP_"}}"#.into(),
            response: r#"{"result":{"plaintext":"ZmxlZXQgc2FtcGxlIHBsYWludGV4dA"}}"#.into(),
        },
        Call::Epoch {
            request: seal.into(),
            response: r#"{"result":{"error":{"class":"transient","code":"fleet_epoch_unavailable"}}}"#.into(),
        },
        Call::Epoch {
            request: seal.into(),
            response: r#"{"result":{"error":{"class":"transient","code":"store_error"}}}"#.into(),
        },
    ]);
    let aad = b"record:identity:7";
    let plaintext = b"fleet sample plaintext";
    let epoch_vault = vault.epoch_vault();
    let sealed = epoch_vault.seal(aad, plaintext).await.unwrap();
    assert_eq!(sealed.epoch, 1);
    assert_eq!(epoch_vault.open(aad, &sealed).await.unwrap(), plaintext);
    assert_eq!(
        epoch_vault.seal(aad, plaintext).await.unwrap_err().code,
        "epoch_key_unavailable"
    );
    // A vault store failure is retried by hub readers and writers, like an
    // outage, rather than stopping them.
    assert_eq!(
        epoch_vault.seal(aad, plaintext).await.unwrap_err().code,
        "vault_unavailable"
    );
    vault.done();
}

#[tokio::test]
async fn hub_join_vault_requests_use_excluded_and_wrap_for_wire_shapes() {
    let vault = VaultScript::new([
        Call::Epoch {
            request: r#"{"method":"fleet.epoch_mint","params":{"excluded":["fed-computer:peer"],"members":["fed-computer:self"]}}"#.into(),
            response: r#"{"result":{"epoch":2,"mint_id":"mint","wraps":[]}}"#.into(),
        },
        Call::Epoch {
            request: r#"{"method":"fleet.epoch_mint","params":{"excluded":["fed-computer:peer"],"members":["fed-computer:self"]}}"#.into(),
            response: r#"{"result":{"error":{"class":"permanent","code":"invalid_params"}}}"#.into(),
        },
        Call::Epoch {
            request: r#"{"method":"fleet.epoch_mint","params":{"members":["fed-computer:self"]}}"#.into(),
            response: r#"{"result":{"error":{"class":"permanent","code":"fleet_members_mismatch"}}}"#.into(),
        },
        Call::Epoch {
            request: r#"{"method":"fleet.epoch_wrap_for","params":{"member_id":"fed-computer:peer"}}"#.into(),
            response: r#"{"result":{"wrap":{"ct":"ciphertext","enc":"encapsulation","from":"fed-computer:self","to":"fed-computer:peer","v":1}}}"#.into(),
        },
    ]);
    let epoch_vault = vault.epoch_vault();
    let control = epoch_vault.control().unwrap();
    let members = ["fed-computer:self".into()];
    assert_eq!(
        control
            .mint(&members, &["fed-computer:peer".into()])
            .await
            .unwrap()
            .epoch,
        2
    );
    assert_eq!(
        control
            .mint(&members, &["fed-computer:peer".into()])
            .await
            .err()
            .unwrap()
            .code,
        "invalid_params"
    );
    assert_eq!(
        control.mint(&members, &[]).await.err().unwrap().code,
        "fleet_members_mismatch"
    );
    assert_eq!(
        control.wrap_for("fed-computer:peer").await.unwrap()["ct"],
        "ciphertext"
    );
    vault.done();
}

#[tokio::test]
async fn fleet_record_key_rotation_rebuilds_with_the_reread_key() {
    let vectors = cases();
    let sample = &vectors["residence"];
    let rotated_id = key_id(ROTATED_SEED);
    let input = text(sample, "signing_input_utf8");
    let vault = VaultScript::new([
        Call::PublicKey(FIXTURE_SEED),
        sample_seal(sample),
        Call::Sign {
            seed: ROTATED_SEED,
            reply_key_id: rotated_id.clone(),
            input: Some(input.into()),
        },
        Call::PublicKey(ROTATED_SEED),
        sample_seal(sample),
        Call::Sign {
            seed: ROTATED_SEED,
            reply_key_id: rotated_id.clone(),
            input: Some(input.replace(FIXTURE_KEY_ID, &rotated_id)),
        },
    ]);
    let header = header(sample);
    let record = prepare(
        &vault.signer(),
        &vault.epoch_vault(),
        &header,
        &serde_json::from_str(text(sample, "plaintext_utf8")).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(record.epoch(), Some(3));
    let read = UnverifiedRecord::parse(&header.key, &record.into_bytes()).unwrap();
    assert_eq!(read.key_version(), rotated_id);
    assert_eq!(
        read.verify(&hex(signing_key(ROTATED_SEED).verifying_key().as_bytes()))
            .unwrap()
            .epoch(),
        Some(3)
    );
    vault.done();
}

#[tokio::test]
async fn fleet_record_wrong_sign_reply_never_reaches_attempt_or_publish() {
    use entorhinal_core::{
        hub_store::{HubAttemptSend, HubAttemptWrite},
        RegistryStore,
    };

    let vectors = cases();
    let sample = &vectors["residence"];
    let header = header(sample);
    let plaintext = serde_json::from_str(text(sample, "plaintext_utf8")).unwrap();
    let (dir, mut descriptor) = crate::tests::scratch_descriptor("fleet-record-guard");
    descriptor.storage_namespace = dir.file_name().unwrap().to_str().unwrap().into();
    let store = RegistryStore::open(&descriptor).unwrap();
    let mut published = Vec::new();
    for (request_key, seed, reply_id) in [
        ("wrong-id", FIXTURE_SEED, key_id(ROTATED_SEED)),
        ("wrong-signature", ROTATED_SEED, FIXTURE_KEY_ID.into()),
    ] {
        let vault = VaultScript::new((0..2).flat_map(|_| {
            [
                Call::PublicKey(FIXTURE_SEED),
                sample_seal(sample),
                Call::Sign {
                    seed,
                    reply_key_id: reply_id.clone(),
                    input: Some(text(sample, "signing_input_utf8").into()),
                },
            ]
        }));
        let outcome = async {
            let prepared =
                prepare(&vault.signer(), &vault.epoch_vault(), &header, &plaintext).await?;
            let write = HubAttemptWrite {
                request_key: request_key.into(),
                params_digest: "params-digest".into(),
                key: header.key.clone(),
                expected: 0,
                value: prepared.into_bytes(),
                step: "residence".into(),
            };
            let send = HubAttemptSend {
                op_id: header.entry_id.clone(),
                sent_at_ms: 44,
                process: "test-process".into(),
            };
            assert!(store.persist_hub_attempt(&write, &send, None).unwrap());
            published.push(write.value);
            Ok::<_, HandlerError>(())
        }
        .await;
        assert!(
            store.hub_attempt(request_key).unwrap().is_none(),
            "persisted a mismatched candidate for {request_key}"
        );
        assert!(
            published.is_empty(),
            "published a mismatched candidate for {request_key}"
        );
        assert_eq!(outcome.unwrap_err().code, "signature_invalid");
        vault.done();
    }
    drop(store);
    std::fs::remove_dir_all(dir).unwrap();
}

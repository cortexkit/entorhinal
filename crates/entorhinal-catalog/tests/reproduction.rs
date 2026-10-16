use c::{
    archive::{self, Member},
    jcs, record, resolver, semver, statements, status,
};
use ed25519_dalek::{Signer, SigningKey};
use entorhinal_catalog as c;
use serde::Serialize;
use serde_json::{json, Value};
use std::{collections::BTreeMap, fs, path::PathBuf};
fn dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/vectors/ck-catalog-v1")
}
fn load(name: &str) -> Value {
    jcs::parse(&fs::read_to_string(dir().join(name)).unwrap()).unwrap()
}
fn s(v: &Value) -> &str {
    v.as_str().unwrap()
}
fn a(v: &Value) -> &Vec<Value> {
    v.as_array().unwrap()
}
fn outcome<T: Serialize, E: Serialize>(result: Result<T, E>) -> Value {
    let (ok, body) = match result {
        Ok(v) => (true, serde_json::to_value(v).unwrap()),
        Err(e) => (false, serde_json::to_value(e).unwrap()),
    };
    let mut out = body.as_object().cloned().unwrap_or_default();
    out.insert("ok".into(), json!(ok));
    Value::Object(out)
}
fn success<T, E: Serialize>(result: Result<T, E>) -> Value {
    outcome(result.map(|_| json!({})))
}
fn signing_key(v: &Value) -> SigningKey {
    let seed = c::decode_hex::<32>(s(&v["seed_hex"])).unwrap();
    assert_eq!(c::sha256_hex(s(&v["seed_text"])), v["seed_hex"]);
    let key = SigningKey::from_bytes(&seed);
    let pk = c::hex(&key.verifying_key().to_bytes());
    assert_eq!(pk, v["public_key_hex"]);
    assert_eq!(c::key_fingerprint(&pk).unwrap(), v["fingerprint"]);
    key
}
fn signed(doc: &Value, field: &str, key: &SigningKey, signature_field: &str) {
    let canonical = jcs::to_string(&doc[field]).unwrap();
    assert_eq!(canonical, doc["canonical"]);
    assert_eq!(c::sha256_hex(&canonical), doc["canonical_sha256"]);
    let input = c::signing_input(s(&doc[field]["schema"]), &canonical);
    assert_eq!(c::hex(&input), doc["signing_input_hex"]);
    assert_eq!(c::hex(&key.sign(&input).to_bytes()), doc[signature_field]);
}
fn base64(s: &str) -> Vec<u8> {
    let mut out = Vec::new();
    let mut acc = 0u32;
    let mut bits = 0;
    for b in s.bytes().take_while(|b| *b != b'=') {
        let n = match b {
            b'A'..=b'Z' => b - b'A',
            b'a'..=b'z' => b - b'a' + 26,
            b'0'..=b'9' => b - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => panic!("bad vector base64"),
        };
        acc = (acc << 6) | n as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    out
}

fn starter_case(name: &str) {
    let vector = load("starter-bundle.json");
    let case = a(&vector["cases"])
        .iter()
        .find(|case| case["name"] == name)
        .unwrap();
    let bytes = base64(s(&case["bundle_archive_base64"]));
    assert_eq!(
        success(c::bundle::verify(
            &bytes,
            s(&vector["keys"]["root"]["public_key_hex"])
        )),
        case["expect"],
        "{name}"
    );
}
#[test]
fn starter_bundle_signature() {
    starter_case("bad_bundle_signature");
}
#[test]
fn starter_bundle_root_certificate() {
    let vector = load("starter-bundle.json");
    let bytes = base64(s(&vector["valid"]["bundle_archive_base64"]));
    assert_eq!(
        success(c::bundle::verify(
            &bytes,
            s(&vector["keys"]["registry_status"]["public_key_hex"])
        )),
        json!({"ok":false,"code":"signature_invalid"})
    );
    starter_case("bundle_key_not_root_certified");
}
#[test]
fn starter_bundle_unlisted_member() {
    starter_case("extra_unlisted_file");
}
#[test]
fn starter_bundle_record_member_binding() {
    starter_case("record_member_bytes_changed");
}
#[test]
fn starter_bundle_valid() {
    let vector = load("starter-bundle.json");
    let bytes = base64(s(&vector["valid"]["bundle_archive_base64"]));
    assert_eq!(
        c::sha256_hex(&bytes),
        vector["valid"]["bundle_archive_sha256"]
    );
    let root = s(&vector["keys"]["root"]["public_key_hex"]);
    let verified = c::bundle::verify(&bytes, root).unwrap();
    assert_eq!(verified.packages.len(), 2);
    for (package, expected) in verified
        .packages
        .iter()
        .zip(a(&vector["valid"]["packages"]))
    {
        assert_eq!(
            serde_json::to_value(&package.package.record).unwrap(),
            expected["record"]["value"]
        );
        assert_eq!(
            serde_json::to_value(package.status.record()).unwrap(),
            expected["status"]["value"]
        );
        assert_eq!(package.archive, base64(s(&expected["archive_base64"])));
        assert_eq!(
            package.proof.verify(root).unwrap().canonical(),
            package.status.canonical()
        );
    }
}
#[test]
fn starter_bundle_other_refusals() {
    for name in [
        "archive_sha256_mismatch",
        "record_missing_status",
        "non_ustar_entry",
        "path_escaping_entry",
    ] {
        starter_case(name);
    }
}
/// Persona bodies are closed: memory authorization is decided by the host, so a
/// persona that still declares `memory_scope` is refused. The valid starter
/// bundle above carries a persona without it, which covers the accepted side.
#[test]
fn starter_bundle_persona_manifest_cases() {
    let vector = load("starter-bundle.json");
    let cases = a(&vector["persona_manifest_cases"]);
    assert!(!cases.is_empty());
    for case in cases {
        let got = success(record::parse_manifest(s(&case["manifest_text"]).as_bytes()));
        assert_eq!(got["ok"], case["expect"]["ok"], "{}", case["name"]);
        assert_eq!(got["code"], case["expect"]["code"], "{}", case["name"]);
    }
}
fn package_members(v: &Value) -> Vec<Member<'_>> {
    a(&v["members"])
        .iter()
        .map(|m| Member::regular(s(&m["path"]), s(&m["content"]).as_bytes()))
        .collect()
}
#[test]
fn reproduce_readme() {
    let readme = fs::read(dir().join("README.md")).unwrap();
    assert_eq!(
        c::sha256_hex(&readme),
        "132414d99c639927ece46fe5b204c84c9a0d2cb150bdce38e7899d13baeefef5"
    );
    let text = String::from_utf8(readme).unwrap();
    let mut checked = 0;
    for line in text
        .lines()
        .filter(|l| l.starts_with("| ") && l.contains(".json |"))
    {
        let cols: Vec<_> = line.split('|').map(str::trim).collect();
        if cols[2].starts_with('`') {
            assert_eq!(
                c::sha256_hex(fs::read(dir().join(cols[1])).unwrap()),
                cols[2].trim_matches('`')
            );
            checked += 1;
        }
    }
    assert_eq!(checked, 7);
}
#[test]
fn reproduce_source() {
    let bytes = fs::read(dir().join("SOURCE.txt")).unwrap();
    assert_eq!(
        c::sha256_hex(&bytes),
        "a67e6a0539bacee290d536b5d52dd72f5e40ba012b3fc8fc3a72fe8dab6fac1d"
    );
    assert!(String::from_utf8(bytes).unwrap().contains("610329529"));
}
#[test]
fn reproduce_generator() {
    assert_eq!(
        c::sha256_hex(fs::read(dir().join("generate.ts")).unwrap()),
        "0b796b2f48289e6b3842eafead93579e19bc82e8dc62e76d61644c017154c652"
    );
}
#[test]
fn reproduce_generator_test() {
    assert_eq!(
        c::sha256_hex(fs::read(dir().join("generate.test.ts")).unwrap()),
        "78ebd3a78d4a37eae8f7c28a8a337b9d8c71c36e656e6ed3f7c4495c5e0d3d9f"
    );
}
#[test]
fn reproduce_names() {
    let v = load("names.json");
    for case in a(&v["valid"]) {
        let n = c::names::map_name(s(&case["input"])).unwrap();
        for (actual, expected) in [
            (json!(n.canonical), &case["canonical"]),
            (json!(n.short), &case["short"]),
            (json!(n.short_bytes), &case["short_bytes"]),
            (json!(n.sha256), &case["sha256_of_canonical"]),
            (json!(n.form), &case["form"]),
            (json!(n.id), &case["basal_id"]),
            (json!(n.id.len()), &case["basal_id_bytes"]),
            (
                json!(s(&case["input"]) == n.canonical),
                &case["input_is_canonical"],
            ),
        ] {
            assert_eq!(&actual, expected, "{}", case["input"]);
        }
    }
    for case in a(&v["refused"]) {
        assert_eq!(
            serde_json::to_value(c::names::map_name(s(&case["input"])).unwrap_err()).unwrap(),
            json!({"code":case["code"],"part":case["part"]})
        );
    }
    for case in a(&v["binding"]) {
        assert_eq!(
            outcome(c::names::bind_name(
                &serde_json::from_value(case["table"].clone()).unwrap(),
                s(&case["publish"])
            )),
            case["expect"]
        );
    }
    for case in a(&v["scope_claims"]) {
        assert_eq!(
            success(c::names::claim_scope(s(&case["scope"]))),
            case["expect"]
        );
    }
}
#[test]
fn reproduce_record() {
    let v = load("record.json");
    let k = signing_key(&v["keys"]["trusted"]);
    signing_key(&v["keys"]["untrusted"]);
    for case in a(&v["jcs"]) {
        let canonical = jcs::canonicalize(s(&case["input_json"])).unwrap();
        assert_eq!(canonical, case["canonical"]);
        assert_eq!(c::hex(canonical.as_bytes()), case["canonical_utf8_hex"]);
        assert_eq!(c::sha256_hex(&canonical), case["sha256"]);
    }
    for pkg in v["packages"].as_object().unwrap().values() {
        let bytes = archive::write(&package_members(pkg)).unwrap();
        assert_eq!(bytes, base64(s(&pkg["archive_base64"])));
        assert_eq!(c::sha256_hex(&bytes), pkg["archive_sha256"]);
        assert_eq!(json!(bytes.len()), pkg["archive_bytes"]);
        assert_eq!(
            c::sha256_hex(s(&pkg["manifest_text"])),
            pkg["manifest_sha256"]
        );
        for m in a(&pkg["members"]) {
            assert_eq!(c::sha256_hex(s(&m["content"])), m["sha256"]);
            assert_eq!(json!(s(&m["content"]).len()), m["bytes"]);
        }
    }
    let keys = vec![statements::ScopeKey {
        public_key: c::hex(&k.verifying_key().to_bytes()),
        valid_from: "2026-01-01T00:00:00Z".into(),
        valid_until: None,
        revocation: None,
    }];
    let trust = record::Trust {
        keys: &keys,
        published_at: None,
    };
    for doc in a(&v["valid"]) {
        signed(doc, "record", &k, "signature");
        let pkg = &v["packages"][s(&doc["package"])];
        let bytes = base64(s(&pkg["archive_base64"]));
        let verified = record::verify_record(
            s(&doc["canonical"]),
            s(&doc["signature"]),
            s(&pkg["manifest_text"]).as_bytes(),
            &bytes,
            &trust,
        )
        .unwrap();
        assert_eq!(
            record::record_fingerprint(&verified.record).unwrap(),
            doc["key_fingerprint"]
        );
    }
    for case in a(&v["cases"]) {
        let pkg = &v["packages"][s(&case["package"])];
        let bytes = base64(s(&pkg["archive_base64"]));
        assert_eq!(
            success(record::verify_record(
                s(&case["served_text"]),
                s(&case["signature"]),
                s(&pkg["manifest_text"]).as_bytes(),
                &bytes,
                &trust
            )),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}
#[test]
fn reproduce_resolver() {
    let v = load("resolver.json");
    for case in a(&v["ranges"]) {
        let range = s(&case["range"]);
        let r = range.parse::<semver::Range>().unwrap();
        assert_eq!(
            json!(r.satisfies(&s(&case["version"]).parse().unwrap())),
            case["satisfies"],
            "{range}"
        );
    }
    for case in a(&v["invalid_ranges"]) {
        assert!(s(&case["range"]).parse::<semver::Range>().is_err());
        assert_eq!(case["code"], "range_invalid");
    }
    let versions: Vec<semver::Version> = a(&v["precedence_ascending"])
        .iter()
        .map(|v| s(v).parse().unwrap())
        .collect();
    for pair in versions.windows(2) {
        assert!(pair[0] < pair[1]);
    }
    for case in a(&v["invalid_versions"]) {
        assert!(s(&case["version"]).parse::<semver::Version>().is_err());
        assert_eq!(case["code"], "version_invalid");
    }
    for token in a(&v["capability_tokens"]["valid"]) {
        assert!(resolver::capability_token(s(token)));
    }
    for token in a(&v["capability_tokens"]["invalid"]) {
        assert!(!resolver::capability_token(s(token)));
    }
    for case in a(&v["cases"]) {
        let registry = serde_json::from_value(case["registry"].clone()).unwrap();
        let caps: Vec<String> = serde_json::from_value(case["local_capabilities"].clone()).unwrap();
        let pins: Option<BTreeMap<String, String>> = case
            .get("current_pins")
            .map(|v| serde_json::from_value(v.clone()).unwrap());
        let result = if case.get("roots").is_some() {
            let roots = serde_json::from_value(case["roots"].clone()).unwrap();
            let subjects: Vec<String> =
                serde_json::from_value(case["subject_roots"].clone()).unwrap();
            let pins = pins.as_ref().unwrap();
            assert_eq!(
                json!(resolver::synthetic_root_ranges(&registry, &roots, &subjects, pins).unwrap()),
                case["synthetic_root"]["requires"]["packages"],
                "{} synthetic root",
                case["name"]
            );
            resolver::resolve_roots(&registry, &roots, &subjects, pins, &caps)
        } else {
            let root = serde_json::from_value(case["root"].clone()).unwrap();
            resolver::resolve(&registry, &root, &caps, pins.as_ref())
        };
        let got = match result {
            Ok(r) => outcome::<_, Value>(Ok(r)),
            Err(e) => json!({"ok":false,"refusal":e}),
        };
        assert_eq!(got, case["expect"], "{}", case["name"]);
    }
}

#[test]
fn reproduce_capability_manifest_mapping() {
    let v = load("resolver.json");
    for token in a(&v["capability_tokens"]["valid"]) {
        assert!(resolver::capability_token(s(token)), "{}", s(token));
    }
    for token in a(&v["capability_tokens"]["invalid"]) {
        assert!(!resolver::capability_token(s(token)), "{}", s(token));
        assert_eq!(
            serde_json::to_value(resolver::capability_to_manifest(s(token)).unwrap_err()).unwrap(),
            json!({"code": "capability_not_a_token"})
        );
    }

    let mapping = &v["capability_manifest_mapping"];
    for pair in a(&mapping["pairs"]) {
        let token = s(&pair["token"]);
        let capability = s(&pair["capability"]);
        assert_eq!(resolver::capability_to_manifest(token).unwrap(), capability);
        assert_eq!(resolver::manifest_to_capability(capability).unwrap(), token);
        assert!(subc_protocol::manifest::is_valid_capability_identifier(
            capability
        ));
    }

    let too_long_token = s(&mapping["token_too_long"]);
    assert!(resolver::capability_token(too_long_token));
    assert_eq!(
        resolver::capability_to_manifest(too_long_token),
        Err(resolver::CapabilityError::TokenTooLong)
    );
    let too_long_name = format!("cap-{}/v1", "a".repeat(61));
    assert_eq!(
        resolver::manifest_to_capability(&too_long_name),
        Err(resolver::CapabilityError::TokenTooLong)
    );
    for capability in a(&mapping["capability_not_a_token"]) {
        assert_eq!(
            serde_json::to_value(resolver::manifest_to_capability(s(capability)).unwrap_err())
                .unwrap(),
            json!({"code": "capability_not_a_token"}),
            "{}",
            s(capability)
        );
    }
}

#[test]
fn reproduce_statements() {
    let v = load("statements.json");
    let root = signing_key(&v["keys"]["root"]);
    let publishers: Vec<_> = a(&v["keys"]["publishers"])
        .iter()
        .map(signing_key)
        .collect();
    let registries: Vec<_> = a(&v["keys"]["registry_status"])
        .iter()
        .map(signing_key)
        .collect();
    for k in a(&v["keys"]["cortexkit_publish"]) {
        signing_key(k);
    }
    let r = &v["ck-registry-key/v1"];
    for (i, doc) in a(&r["chain"]).iter().enumerate() {
        signed(doc, "statement", &registries[i], "signature");
    }
    for case in a(&r["cases"]) {
        let mut pin = serde_json::from_value(case["pinned"].clone()).unwrap();
        let got = statements::apply_registry_key(
            &mut pin,
            s(&case["statement_text"]),
            s(&case["signature"]),
        )
        .map(|result| json!({"result":result}));
        assert_eq!(outcome(got), case["expect"], "{}", case["name"]);
    }
    let r = &v["ck-key-rotation/v1"];
    signed(&r["valid"], "statement", &publishers[0], "old_signature");
    signed(&r["valid"], "statement", &publishers[2], "new_signature");
    for case in a(&r["cases"]) {
        let served = &case["served"];
        let got = statements::verify_key_rotation(
            &served["statement"].to_string(),
            s(&served["old_signature"]),
            s(&served["new_signature"]),
            s(&case["scope"]),
            &serde_json::from_value::<Vec<_>>(case["scope_keys"].clone()).unwrap(),
            &serde_json::from_value::<Vec<_>>(case["ever_in_any_scope"].clone()).unwrap(),
        )
        .map(|_| json!({"result":"accepted"}));
        assert_eq!(outcome(got), case["expect"], "{}", case["name"]);
    }
    let r = &v["ck-key-cert/v1"];
    for doc in a(&r["valid"]) {
        signed(doc, "statement", &root, "signature");
    }
    for case in a(&r["cases"]) {
        let served = &case["served"];
        let got = statements::verify_key_cert(
            &served["statement"].to_string(),
            s(&served["signature"]),
            s(&v["keys"]["root"]["public_key_hex"]),
        )
        .map(|_| json!({"result":"certified"}));
        assert_eq!(outcome(got), case["expect"], "{}", case["name"]);
    }
    for case in a(&r["window_cases"]) {
        let doc = a(&r["valid"])
            .iter()
            .find(|d| d["name"] == case["cert"])
            .unwrap();
        let cert = &doc["statement"];
        let covers = statements::window_covers(
            s(&cert["valid_from"]),
            cert["valid_until"].as_str(),
            s(&case["published_at"]),
        );
        assert_eq!(
            if covers {
                json!({"ok":true})
            } else {
                json!({"ok":false,"code":"key_not_valid"})
            },
            case["expect"]
        );
    }
    let r = &v["ck-status-request/v1"];
    for doc in a(&r["valid"]) {
        signed(doc, "statement", &publishers[0], "signature");
    }
    for case in a(&r["cases"]) {
        let req = &case["request"];
        let got = statements::apply_status_request(
            &serde_json::from_value(case["path"].clone()).unwrap(),
            &req["statement"].to_string(),
            s(&req["public_key"]),
            s(&req["signature"]),
            &serde_json::from_value(case["current"].clone()).unwrap(),
            &serde_json::from_value::<Vec<_>>(case["scope_keys"].clone()).unwrap(),
        )
        .map(|next| json!({"result":"applied","next":next}));
        assert_eq!(outcome(got), case["expect"], "{}", case["name"]);
    }
}
fn held(text: &str, records: &[Value], chain: &[status::StatusKey]) -> status::HeldStatus {
    let doc = records.iter().find(|d| d["canonical"] == text).unwrap();
    let r = &doc["record"];
    let pin = status::Pin {
        name: s(&r["name"]).into(),
        version: s(&r["version"]).into(),
        archive_sha256: s(&r["archive_sha256"]).into(),
    };
    status::apply_status(None, text, s(&doc["signature"]), chain, &pin)
        .unwrap()
        .held
}
#[test]
fn reproduce_status() {
    let v = load("status.json");
    let key = signing_key(&v["registry_keys"]["test-registry-1"]);
    let chain = vec![status::StatusKey {
        key_id: "test-registry-1".into(),
        public_key: c::hex(&key.verifying_key().to_bytes()),
        valid_from: None,
    }];
    let records = a(&v["records"]);
    for doc in records {
        signed(doc, "record", &key, "signature");
    }
    for (from, next) in v["allowed_next_state"].as_object().unwrap() {
        let from: status::State = serde_json::from_value(json!(from)).unwrap();
        for to in [
            status::State::Available,
            status::State::Withdrawn,
            status::State::Revoked,
            status::State::Tombstoned,
        ] {
            assert_eq!(from.permits(to), a(next).contains(&json!(to)));
        }
    }
    for case in a(&v["apply_cases"]) {
        let held = case["held"].as_str().map(|t| held(t, records, &chain));
        let got = status::apply_status(
            held.as_ref(),
            s(&case["incoming_text"]),
            s(&case["signature"]),
            &chain,
            &serde_json::from_value(case["pinned"].clone()).unwrap(),
        )
        .map(|a| json!({"result":a.result}));
        assert_eq!(outcome(got), case["expect"], "{}", case["name"]);
    }
    for case in a(&v["revocation_time"]) {
        assert_eq!(
            json!(status::revoked_at(
                case["published_at"].as_str(),
                s(&case["compromise_from"])
            )),
            case["blocked"]
        );
    }
    assert!(status::revoked_at(None, "2026-10-20T00:00:00Z"));
    for row in a(&v["effects"]) {
        assert_eq!(
            serde_json::to_value(status::effect(
                serde_json::from_value(row["kind"].clone()).unwrap(),
                serde_json::from_value(row["state"].clone()).unwrap()
            ))
            .unwrap(),
            *row
        );
    }
    let mut pins = BTreeMap::new();
    for doc in records {
        let r = &doc["record"];
        pins.insert(
            format!("{}@{}", s(&r["name"]), s(&r["version"])),
            status::Pin {
                name: s(&r["name"]).into(),
                version: s(&r["version"]).into(),
                archive_sha256: s(&r["archive_sha256"]).into(),
            },
        );
    }
    for case in a(&v["feed"]["views"]) {
        let log: Vec<_> = a(&v["feed"]["log"])
            .iter()
            .filter(|w| w["feed_seq"].as_u64().unwrap() <= case["log_up_to"].as_u64().unwrap())
            .map(|w| {
                let doc = records.iter().find(|d| d["id"] == w["id"]).unwrap();
                status::FeedWrite {
                    feed_seq: w["feed_seq"].as_u64().unwrap(),
                    id: s(&w["id"]).into(),
                    record: serde_json::from_value(doc["record"].clone()).unwrap(),
                }
            })
            .collect();
        let got = match status::select_feed(
            &log,
            v["feed"]["oldest_servable_after"].as_u64().unwrap(),
            case["after"].as_u64(),
        ) {
            Ok(view) => json!({"status":200,"feed_seq":view.feed_seq,"records":view.records}),
            Err(e) => json!({"status":410,"code":e.code}),
        };
        assert_eq!(got, case["expect"], "{}", case["name"]);
    }
    let example = &v["feed"]["example_reply"];
    let doc = records.iter().find(|d| d["id"] == "f3").unwrap();
    assert_eq!(
        example["body"],
        json!({"schema":"ck-status-feed/v1","scope":"example","feed_seq":7,"records":[{"feed_seq":7,"record":doc["record"],"signature":doc["signature"]}]})
    );
    for case in a(&v["feed"]["client_cases"]) {
        let mut state = status::FeedState {
            feed_seq: case["state"]["feed_seq"].as_u64(),
            held: BTreeMap::new(),
        };
        for (p, text) in case["state"]["held"].as_object().unwrap() {
            state.held.insert(p.clone(), held(s(text), records, &chain));
        }
        let reply = if case["reply"]["status"] == 410 {
            status::FeedReply::Unavailable
        } else {
            status::FeedReply::Records {
                feed_seq: case["reply"]["feed_seq"].as_u64().unwrap(),
                records: serde_json::from_value(case["reply"]["records"].clone()).unwrap(),
            }
        };
        let got = status::apply_feed(
            &state,
            if case["mode"] == "full" {
                status::FeedMode::Full
            } else {
                status::FeedMode::After
            },
            &reply,
            &chain,
            &pins,
        );
        let results:Vec<_>=got.records.into_iter().map(|(id,r)|json!({"id":id,"result":outcome(r.map(|result|json!({"result":result})))})).collect();
        let holds: BTreeMap<_, _> = got
            .next_state
            .held
            .iter()
            .map(|(p, h)| (p, h.canonical()))
            .collect();
        assert_eq!(
            json!({"refusal":got.refusal,"records":results,"next_state":{"feed_seq":got.next_state.feed_seq,"held":holds},"next_step":got.next_step}),
            case["expect"],
            "{}",
            case["name"]
        );
    }
}
// This fixture writer deliberately exposes malformed headers only in tests.
fn fixture_header(name: &[u8], prefix: &[u8], size: usize) -> [u8; 512] {
    let mut h = [0; 512];
    h[..name.len()].copy_from_slice(name);
    h[345..345 + prefix.len()].copy_from_slice(prefix);
    h[100..108].copy_from_slice(b"0000644\0");
    h[108..116].copy_from_slice(b"0000000\0");
    h[116..124].copy_from_slice(b"0000000\0");
    h[124..136].copy_from_slice(format!("{size:011o}\0").as_bytes());
    h[136..148].copy_from_slice(b"00000000000\0");
    h[156] = b'0';
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    checksum(&mut h);
    h
}
fn checksum(h: &mut [u8]) {
    h[148..156].fill(b' ');
    let sum: usize = h.iter().map(|b| *b as usize).sum();
    h[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
}
fn fixture_entry(name: &[u8], prefix: &[u8], data: &[u8], kind: u8, link: &[u8]) -> Vec<u8> {
    let mut h = fixture_header(name, prefix, data.len());
    h[156] = kind;
    h[157..157 + link.len()].copy_from_slice(link);
    checksum(&mut h);
    let mut out = h.to_vec();
    out.extend_from_slice(data);
    out.resize(out.len().div_ceil(512) * 512, 0);
    out
}
fn archive_case(name: &str, base: &[u8]) -> Vec<u8> {
    let end = base.len() - 1024;
    let mut out = base.to_vec();
    let extra = match name {
        "symlink" => Some(fixture_entry(b"evil", b"", b"", b'2', b"/etc/passwd")),
        "hard_link" => Some(fixture_entry(b"again.json", b"", b"", b'1', b"ck.json")),
        "directory_entry" => Some(fixture_entry(b"scripts/", b"", b"", b'5', b"")),
        "dot_dot_path" => Some(fixture_entry(b"../evil", b"", b"x\n", b'0', b"")),
        "absolute_path" => Some(fixture_entry(b"/etc/passwd", b"", b"x\n", b'0', b"")),
        "non_ascii_path" => Some(fixture_entry(b"\xe9.md", b"", b"x\n", b'0', b"")),
        "path_over_255_bytes" => Some(fixture_entry(&[b'n'; 100], &[b'p'; 155], b"x\n", b'0', b"")),
        "duplicate_member" => Some(fixture_entry(b"SKILL.md", b"", b"again\n", b'0', b"")),
        "case_collision" => Some(fixture_entry(b"skill.md", b"", b"again\n", b'0', b"")),
        _ => None,
    };
    if let Some(extra) = extra {
        out.truncate(end);
        out.extend(extra);
        out.resize(out.len() + 1024, 0);
        return out;
    }
    match name {
        "valid_with_trailing_zero_padding" => out.resize(out.len() + 2048, 0),
        "header_checksum_off_by_one" => {
            let sum =
                usize::from_str_radix(std::str::from_utf8(&out[148..154]).unwrap(), 8).unwrap() + 1;
            out[148..156].copy_from_slice(format!("{sum:06o}\0 ").as_bytes());
        }
        "header_altered_after_checksum" => out[0] = b'd',
        "gnu_magic" => {
            out[257..263].copy_from_slice(b"ustar ");
            out[263..265].copy_from_slice(b" \0");
            checksum(&mut out[..512]);
        }
        "wrong_version" => {
            out[263..265].copy_from_slice(b"01");
            checksum(&mut out[..512]);
        }
        "v7_no_magic" => {
            out[257..265].fill(0);
            checksum(&mut out[..512]);
        }
        "base256_size" => {
            let size =
                usize::from_str_radix(std::str::from_utf8(&out[124..135]).unwrap(), 8).unwrap();
            out[124..136].fill(0);
            out[124] = 0x80;
            let bytes = (size as u64).to_be_bytes();
            out[130..136].copy_from_slice(&bytes[2..]);
            checksum(&mut out[..512]);
        }
        "size_with_leading_space" => {
            let size =
                usize::from_str_radix(std::str::from_utf8(&out[124..135]).unwrap(), 8).unwrap();
            out[124..136].copy_from_slice(format!(" {size:010o}\0").as_bytes());
            checksum(&mut out[..512]);
        }
        "terminator_missing" => out.truncate(end),
        "terminator_one_block" => out.truncate(end + 512),
        "member_after_single_zero_block" => {
            out.truncate(end + 512);
            out.extend(fixture_entry(b"late.txt", b"", b"late\n", b'0', b""));
            out.resize(out.len() + 1024, 0);
        }
        "non_zero_after_terminator" => {
            out.resize(out.len() + 512, 0);
            out[base.len() + 100] = 1;
        }
        "pax_extended_header" => {
            out = fixture_entry(b"PaxHeaders/ck.json", b"", b"16 path=ck.json\n", b'x', b"");
            out.extend_from_slice(base);
        }
        "gnu_long_name" => {
            out = fixture_entry(b"././@LongLink", b"", b"ck.json\0", b'L', b"");
            out.extend_from_slice(base);
        }
        "truncated_member" => {
            out.truncate(end);
            out.extend_from_slice(&fixture_header(b"big.txt", b"", 4096));
            out.extend_from_slice(b"0123456789");
        }
        "ck_json_missing" => {
            let size =
                usize::from_str_radix(std::str::from_utf8(&out[124..135]).unwrap(), 8).unwrap();
            out = out[512 + size.div_ceil(512) * 512..].to_vec();
        }
        "members_at_limit" | "too_many_members" => {
            out.truncate(end);
            for i in 1..=if name == "members_at_limit" { 997 } else { 998 } {
                out.extend(fixture_entry(
                    format!("pad/{i:04}").as_bytes(),
                    b"",
                    b"",
                    b'0',
                    b"",
                ));
            }
            out.resize(out.len() + 1024, 0);
        }
        _ => panic!("unhandled archive vector {name}"),
    }
    out
}
#[test]
fn reproduce_archive() {
    let v = load("archive.json");
    let packages = load("record.json");
    let members = package_members(&packages["packages"]["skill"]);
    let base = archive::write(&members).unwrap();
    assert_eq!(base, base64(s(&v["valid"]["archive_base64"])));
    assert_eq!(c::sha256_hex(&base), v["valid"]["archive_sha256"]);
    assert_eq!(json!(base.len()), v["valid"]["archive_bytes"]);
    let read = archive::read(&base).unwrap();
    for m in a(&v["valid"]["members"]) {
        let bytes = read[s(&m["path"])];
        assert_eq!(c::sha256_hex(bytes), m["sha256"]);
        assert_eq!(json!(bytes.len()), m["bytes"]);
    }
    for case in a(&v["cases"]) {
        let name = s(&case["name"]);
        let bytes = archive_case(name, &base);
        if let Some(encoded) = case["archive_base64"].as_str() {
            assert_eq!(bytes, base64(encoded), "{name}");
        }
        assert_eq!(c::sha256_hex(&bytes), case["archive_sha256"], "{name}");
        assert_eq!(json!(bytes.len()), case["archive_bytes"]);
        assert_eq!(
            outcome(archive::read(&bytes).map(|m| json!({"member_count":m.len()}))),
            case["expect"],
            "{name}"
        );
        if case["expect"]["ok"] == true {
            let padding = if name == "valid_with_trailing_zero_padding" {
                2048
            } else {
                0
            };
            let paths: Vec<_> = (1..=if name == "members_at_limit" { 997 } else { 0 })
                .map(|i| format!("pad/{i:04}"))
                .collect();
            let mut ms = members.clone();
            ms.extend(paths.iter().map(|p| Member::regular(p, b"")));
            let mut written = archive::write(&ms).unwrap();
            written.resize(written.len() + padding, 0);
            assert_eq!(written, bytes);
        }
        if !case["build"].is_null() {
            assert_eq!(case["build"]["base"], "valid");
            assert_eq!(case["build"]["append_empty_members"]["first"], "pad/0001");
            assert_eq!(
                case["build"]["append_empty_members"]["last"],
                if name == "members_at_limit" {
                    "pad/0997"
                } else {
                    "pad/0998"
                }
            );
        }
    }
    for case in a(&v["path_cases"]) {
        let path = s(&case["path"]);
        let result = archive::check_path(path);
        let got = match result {
            Ok(()) => json!({"ok":true}),
            Err(rule) => json!({"ok":false,"rule":rule}),
        };
        assert_eq!(got, case["expect"]);
        assert_eq!(json!(path.len()), case["path_bytes"]);
        let split = if result.is_ok() {
            archive::ustar_split(path).map(|(prefix, name)| json!({"prefix":prefix,"name":name}))
        } else {
            None
        };
        assert_eq!(json!(split), case["ustar_split"]);
    }
}

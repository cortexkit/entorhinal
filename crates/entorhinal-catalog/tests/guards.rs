use c::{
    archive::{self, Member},
    jcs,
    status::{self, Pin, StatusKey},
    RefusalCode,
};
use ed25519_dalek::{Signer, SigningKey};
use entorhinal_catalog as c;
use serde_json::{json, Value};
use std::collections::BTreeMap;
fn key(n: u8) -> SigningKey {
    let v = jcs::parse(include_str!("vectors/ck-catalog-v1/statements.json")).unwrap();
    SigningKey::from_bytes(
        &c::decode_hex::<32>(
            v["keys"]["registry_status"][n as usize - 1]["seed_hex"]
                .as_str()
                .unwrap(),
        )
        .unwrap(),
    )
}
fn sig(v: &Value, k: &SigningKey) -> String {
    let canonical = jcs::to_string(v).unwrap();
    c::hex(
        &k.sign(&c::signing_input(v["schema"].as_str().unwrap(), &canonical))
            .to_bytes(),
    )
}
fn status_record(kid: &str, seq: u64, at: &str) -> Value {
    json!({"schema":"ck-status/v1","registry_key_id":kid,"name":"@example/hello","version":"1.0.0","archive_sha256":"a".repeat(64),"publisher_key":"b".repeat(64),"sequence":seq,"state":"available","reason":null,"compromise_from":null,"published_at":"2026-01-01T00:00:00Z","changed_at":at})
}
fn pin() -> Pin {
    Pin {
        name: "@example/hello".into(),
        version: "1.0.0".into(),
        archive_sha256: "a".repeat(64),
    }
}
fn chain() -> Vec<StatusKey> {
    vec![
        StatusKey {
            key_id: "r1".into(),
            public_key: c::hex(&key(1).verifying_key().to_bytes()),
            valid_from: None,
        },
        StatusKey {
            key_id: "r2".into(),
            public_key: c::hex(&key(2).verifying_key().to_bytes()),
            valid_from: Some("2026-09-01T00:00:00Z".into()),
        },
        StatusKey {
            key_id: "r3".into(),
            public_key: c::hex(&key(3).verifying_key().to_bytes()),
            valid_from: Some("2026-07-01T00:00:00Z".into()),
        },
    ]
}
#[test]
fn duplicate_keys_are_refused() {
    for text in [
        r#"{"x":1,"x":2}"#,
        r#"{"nested":{"x":1,"\u0078":2}}"#,
        r#"[{"a":null,"a":null}]"#,
    ] {
        assert!(jcs::parse(text).is_err(), "accepted duplicate: {text}");
    }
}
#[test]
fn signed_json_requires_integer_tokens() {
    let vectors = jcs::parse(include_str!("vectors/ck-catalog-v1/record.json")).unwrap();
    let doc = &vectors["valid"][0];
    let pkg = &vectors["packages"][doc["package"].as_str().unwrap()];
    let members: Vec<_> = pkg["members"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| {
            Member::regular(
                m["path"].as_str().unwrap(),
                m["content"].as_str().unwrap().as_bytes(),
            )
        })
        .collect();
    let bytes = archive::write(&members).unwrap();
    let manifest = pkg["manifest_text"].as_str().unwrap().as_bytes();
    let signer = SigningKey::from_bytes(
        &c::decode_hex::<32>(vectors["keys"]["trusted"]["seed_hex"].as_str().unwrap()).unwrap(),
    );
    let keys = [c::statements::ScopeKey {
        public_key: c::hex(&signer.verifying_key().to_bytes()),
        valid_from: "2026-01-01T00:00:00Z".into(),
        valid_until: None,
        revocation: None,
    }];
    let trust = c::record::Trust {
        keys: &keys,
        published_at: None,
    };
    let record_text = doc["record"].to_string();
    let status = status_record("r1", 1, "2026-06-01T00:00:00Z");
    let cert = json!({"schema":"ck-key-cert/v1","scope":"cortexkit","public_key":keys[0].public_key,"valid_from":"2026-01-01T00:00:00Z","valid_until":null});
    for token in ["1.0", "1e2", "1E0", "0e0", "-0"] {
        // These tokens are valid general JSON, but their spelling is forbidden in signed documents.
        assert!(jcs::parse(token).is_ok(), "{token}");
        for text in [
            token.to_owned(),
            format!(r#"{{"n":{token}}}"#),
            format!(r#"[{{"n":[{token}]}}]"#),
        ] {
            assert_eq!(
                jcs::parse_signed(&text),
                Err(jcs::JsonError::InvalidJson),
                "{text}"
            );
        }
        let text = record_text.replace(
            "\"log_head\":null",
            &format!(r#""log_head":{{"n":[{token}]}}"#),
        );
        for signature in [doc["signature"].as_str().unwrap(), "bad signature"] {
            assert_eq!(
                c::record::verify_record(&text, signature, manifest, &bytes, &trust).unwrap_err(),
                c::Refusal::field(RefusalCode::RecordInvalid, "json"),
                "record: {token}"
            );
        }
        let text = status
            .to_string()
            .replace("\"sequence\":1", &format!(r#""sequence":{token}"#));
        assert_eq!(
            status::apply_status(None, &text, &sig(&status, &key(1)), &chain(), &pin())
                .unwrap_err(),
            c::Refusal::field(RefusalCode::StatusInvalid, "json"),
            "status: {token}"
        );
        let text = cert
            .to_string()
            .replace("\"valid_until\":null", &format!(r#""valid_until":{token}"#));
        assert_eq!(
            c::statements::verify_key_cert(&text, &sig(&cert, &signer), &keys[0].public_key)
                .unwrap_err(),
            c::Refusal::field(RefusalCode::StatementInvalid, "json"),
            "statement: {token}"
        );
    }
    for token in ["0", "-1", "100"] {
        let number = jcs::parse_signed(token).unwrap();
        let nested = json!({"n":[number]});
        let text = record_text.replace("\"log_head\":null", &format!(r#""log_head":{nested}"#));
        let mut record = doc["record"].clone();
        record["log_head"] = nested.clone();
        assert_eq!(jcs::parse_signed(&text).unwrap(), record);
        let verified =
            c::record::verify_record(&text, &sig(&record, &signer), manifest, &bytes, &trust)
                .unwrap();
        assert_eq!(verified.record.log_head, nested);
    }
    for text in [
        r#"{"1.0":"-0","nested":["1e2","1E0","0e0"]}"#,
        r#"{"escaped":"\"1e2\" \\ -0","n":100}"#,
    ] {
        assert_eq!(jcs::parse_signed(text).unwrap(), jcs::parse(text).unwrap());
    }
    for text in [r#"{"n":0,"n":1}"#, r#"{"n":01}"#, r#"{"n":-}"#] {
        assert_eq!(jcs::parse_signed(text), Err(jcs::JsonError::InvalidJson));
    }
}
#[test]
fn tar_header_checksum_is_verified() {
    let mut bytes = archive::write(&[Member::regular("ck.json", b"{}")]).unwrap();
    bytes[100] ^= 1;
    assert_eq!(
        archive::read(&bytes).unwrap_err().rule,
        Some(archive::ArchiveRule::HeaderChecksum)
    );
}
#[test]
fn status_key_epoch_cannot_regress() {
    let chain = chain();
    let a = status_record("r3", 2, "2026-05-01T00:00:00Z");
    let held = status::apply_status(None, &a.to_string(), &sig(&a, &key(3)), &chain, &pin())
        .unwrap()
        .held;
    let b = status_record("r2", 3, "2026-06-01T00:00:00Z");
    let e = status::apply_status(
        Some(&held),
        &b.to_string(),
        &sig(&b, &key(2)),
        &chain,
        &pin(),
    )
    .unwrap_err();
    assert_eq!(e.code, RefusalCode::FeedRollback);
    assert_eq!(e.reason, Some(c::RollbackReason::KeyEpochRegressed));
}
#[test]
fn retired_status_keys_are_not_valid() {
    let chain = chain();
    for (id, n) in [("r1", 1), ("r2", 2)] {
        let v = status_record(id, 4, "2026-07-01T00:00:00Z");
        assert_eq!(
            status::apply_status(None, &v.to_string(), &sig(&v, &key(n)), &chain, &pin())
                .unwrap_err()
                .code,
            RefusalCode::KeyNotValid
        );
    }
    let v = status_record("r3", 4, "2026-07-01T00:00:00Z");
    assert_eq!(
        status::apply_status(None, &v.to_string(), &sig(&v, &key(3)), &chain, &pin())
            .unwrap()
            .held
            .signer_index(),
        2
    );
}
#[test]
fn record_refusal_order_is_preserved() {
    let v = json!({"schema":"ck-record/v1","name":"@example/hello","version":"1.0.0","manifest_sha256":"0".repeat(64),"archive_sha256":"0".repeat(64),"archive_bytes":1024,"public_key":c::hex(&key(1).verifying_key().to_bytes()),"log_head":null});
    let trust = c::record::Trust {
        keys: &[],
        published_at: None,
    };
    assert_eq!(
        c::record::verify_record(
            &v.to_string(),
            &"0".repeat(128),
            b"invalid manifest",
            b"invalid tar",
            &trust
        )
        .unwrap_err()
        .code,
        RefusalCode::SignatureInvalid
    );
    assert_eq!(
        c::record::verify_record(
            &v.to_string(),
            &sig(&v, &key(1)),
            b"invalid manifest",
            b"invalid tar",
            &trust
        )
        .unwrap_err()
        .code,
        RefusalCode::KeyUnknown
    );
    let mut bad = v;
    bad["schema"] = json!("wrong");
    assert_eq!(
        c::record::verify_record(&bad.to_string(), &"0".repeat(128), b"", b"", &trust)
            .unwrap_err()
            .code,
        RefusalCode::RecordInvalid
    );
}
#[test]
fn resolver_is_deterministic() {
    use c::resolver::*;
    let rv = |v: &str| RegistryVersion {
        version: v.into(),
        kind: c::Kind::Skill,
        status: status::State::Available,
        requires: c::record::Requires {
            capabilities: vec![],
            packages: BTreeMap::new(),
        },
        refs: vec![],
    };
    let root = Root {
        package: "@example/root".into(),
        range: "*".into(),
    };
    for versions in [
        vec![rv("1.0.0"), rv("2.0.0"), rv("1.1.0")],
        vec![rv("2.0.0"), rv("1.1.0"), rv("1.0.0")],
    ] {
        let registry = BTreeMap::from([(root.package.clone(), versions)]);
        assert_eq!(
            resolve(&registry, &root, &[], None).unwrap().picks[&root.package],
            "2.0.0"
        );
    }
}
#[test]
fn status_time_regression_precedes_terminal() {
    let keys = &chain()[..1];
    let mut a = status_record("r1", 2, "2026-06-01T00:00:00Z");
    a["state"] = json!("tombstoned");
    a["reason"] = json!("gone");
    let held = status::apply_status(None, &a.to_string(), &sig(&a, &key(1)), keys, &pin())
        .unwrap()
        .held;
    let b = status_record("r1", 3, "2026-05-01T00:00:00Z");
    assert_eq!(
        status::apply_status(Some(&held), &b.to_string(), &sig(&b, &key(1)), keys, &pin())
            .unwrap_err()
            .reason,
        Some(c::RollbackReason::ChangedAtRegressed)
    );
}
#[test]
fn jcs_rfc8785_numbers_and_strict_json() {
    let cases = [
        ("-0", "0"),
        ("1e-6", "0.000001"),
        ("1e-7", "1e-7"),
        ("1e20", "100000000000000000000"),
        ("1e21", "1e+21"),
        ("1e23", "1e+23"),
        ("333333333.33333329", "333333333.3333333"),
        ("4.50", "4.5"),
        ("2e-3", "0.002"),
        ("5e-324", "5e-324"),
        ("1.7976931348623157e308", "1.7976931348623157e+308"),
        ("9007199254740992", "9007199254740992"),
    ];
    for (input, out) in cases {
        assert_eq!(jcs::canonicalize(input).unwrap(), out, "{input}");
    }
    for input in [
        r#""\ud800""#,
        r#""\udc00""#,
        "NaN",
        "1e999",
        "[1,]",
        "{} trailing",
        "{\"a\":1,}",
    ] {
        assert!(jcs::parse(input).is_err(), "{input}");
    }
}
#[test]
fn writer_refuses_forbidden_inputs() {
    use archive::{ArchiveRule as R, MemberKind};
    let ck = Member::regular("ck.json", b"{}");
    let long = format!("{}/{}", "a".repeat(156), "b".repeat(100));
    let no_split = "a".repeat(101);
    for (p, code) in [
        ("../x", RefusalCode::PathInvalid),
        ("/x", RefusalCode::PathInvalid),
        ("a//b", RefusalCode::PathInvalid),
        (&long, RefusalCode::PathTooLong),
        (&no_split, RefusalCode::PathNotUstar),
    ] {
        assert_eq!(
            archive::write(&[ck.clone(), Member::regular(p, b"")])
                .unwrap_err()
                .code,
            code
        );
    }
    for (inputs, rule) in [
        (vec![ck.clone(), ck.clone()], R::PathDuplicate),
        (
            vec![ck.clone(), Member::regular("CK.json", b"")],
            R::PathCollision,
        ),
        (vec![Member::regular("x", b"")], R::CkJsonMissing),
        (
            vec![Member::regular("x", b""), ck.clone()],
            R::CkJsonNotFirst,
        ),
    ] {
        assert_eq!(archive::write(&inputs).unwrap_err().rule, Some(rule));
    }
    for kind in [
        MemberKind::Directory,
        MemberKind::Link,
        MemberKind::Device,
        MemberKind::Fifo,
        MemberKind::Extension,
    ] {
        assert_eq!(
            archive::write(&[
                ck.clone(),
                Member {
                    path: "x",
                    bytes: b"",
                    kind
                }
            ])
            .unwrap_err()
            .rule,
            Some(R::MemberType)
        );
    }
    let paths: Vec<_> = (0..1000).map(|i| format!("pad/{i}")).collect();
    let mut inputs = vec![ck.clone()];
    inputs.extend(paths.iter().map(|p| Member::regular(p, b"")));
    assert_eq!(
        archive::write(&inputs).unwrap_err().rule,
        Some(R::TooManyMembers)
    );
    let huge = vec![0; archive::ARCHIVE_MAX];
    assert_eq!(
        archive::write(&[ck.clone(), Member::regular("x", &huge)])
            .unwrap_err()
            .code,
        RefusalCode::ArchiveTooLarge
    );
    assert_eq!(
        archive::read(&huge.iter().copied().chain([0]).collect::<Vec<_>>())
            .unwrap_err()
            .code,
        RefusalCode::ArchiveTooLarge
    );
    let manifest = vec![0; archive::MANIFEST_MAX + 1];
    assert_eq!(
        archive::write(&[Member::regular("ck.json", &manifest)])
            .unwrap_err()
            .code,
        RefusalCode::ManifestTooLarge
    );
}

#[test]
fn jcs_rfc8785_binary64_appendix_b() {
    let cases = [
        (0x0000000000000000, "0"),
        (0x8000000000000000, "0"),
        (0x0000000000000001, "5e-324"),
        (0x8000000000000001, "-5e-324"),
        (0x7fefffffffffffff, "1.7976931348623157e+308"),
        (0xffefffffffffffff, "-1.7976931348623157e+308"),
        (0x4340000000000000, "9007199254740992"),
        (0xc340000000000000, "-9007199254740992"),
        (0x4430000000000000, "295147905179352830000"),
        (0x44b52d02c7e14af5, "9.999999999999997e+22"),
        (0x44b52d02c7e14af6, "1e+23"),
        (0x44b52d02c7e14af7, "1.0000000000000001e+23"),
        (0x444b1ae4d6e2ef4e, "999999999999999700000"),
        (0x444b1ae4d6e2ef4f, "999999999999999900000"),
        (0x444b1ae4d6e2ef50, "1e+21"),
        (0x3eb0c6f7a0b5ed8c, "9.999999999999997e-7"),
        (0x3eb0c6f7a0b5ed8d, "0.000001"),
        (0x41b3de4355555553, "333333333.3333332"),
        (0x41b3de4355555554, "333333333.33333325"),
        (0x41b3de4355555555, "333333333.3333333"),
        (0x41b3de4355555556, "333333333.3333334"),
        (0x41b3de4355555557, "333333333.33333343"),
        (0xbecbf647612f3696, "-0.0000033333333333333333"),
        (0x43143ff3c1cb0959, "1424953923781206.2"),
    ];
    for (bits, expected) in cases {
        assert_eq!(
            jcs::to_string(&json!(f64::from_bits(bits))).unwrap(),
            expected,
            "{bits:016x}"
        );
    }
}

#[test]
fn verifies_all_package_kinds_and_checks_manifest_files() {
    let k = key(1);
    let keys = vec![c::statements::ScopeKey {
        public_key: c::hex(&k.verifying_key().to_bytes()),
        valid_from: "2026-01-01T00:00:00Z".into(),
        valid_until: None,
        revocation: None,
    }];
    let trust = c::record::Trust {
        keys: &keys,
        published_at: Some("2026-06-01T00:00:00Z"),
    };
    for (kind, body) in [
        (
            "skill",
            json!({"entry":"body.txt","audience":"user","scripts":[]}),
        ),
        (
            "persona",
            json!({"prompt":"body.txt","implements":[],"delegates_to":{},"capabilities":{"required":[],"optional":[]},"skills":[],"flows":[],"memory_scope":"project"}),
        ),
        ("memory-profile", json!({"arbitrary":{"value":true}})),
        ("flow", json!({"manifest":"body.txt","script":"script.sh"})),
    ] {
        let data = if kind == "flow" {
            br#"{"id":"example--hello","version":1}"#.as_slice()
        } else {
            b"hello".as_slice()
        };
        let script = b"echo hello\n";
        let mut files =
            vec![json!({"path":"body.txt","sha256":c::sha256_hex(data),"bytes":data.len()})];
        if kind == "flow" {
            files.push(
                json!({"path":"script.sh","sha256":c::sha256_hex(script),"bytes":script.len()}),
            );
        }
        let manifest=json!({"schema":"ck-package/v1","name":"@example/hello","version":"1.0.0","kind":kind,"summary":"Example package","license":"MIT","source":"https://example.invalid/hello","requires":{"capabilities":[],"packages":{}},"files":files,"body":body}).to_string();
        let mut members = vec![
            Member::regular("ck.json", manifest.as_bytes()),
            Member::regular("body.txt", data),
        ];
        if kind == "flow" {
            members.push(Member::regular("script.sh", script));
        }
        let archive = archive::write(&members).unwrap();
        let mut r = json!({"schema":"ck-record/v1","name":"@example/hello","version":"1.0.0","manifest_sha256":c::sha256_hex(&manifest),"archive_sha256":c::sha256_hex(&archive),"archive_bytes":archive.len(),"public_key":keys[0].public_key,"log_head":{"future":true}});
        if kind == "flow" {
            r["flow_version"] = json!(1);
        }
        let verified = c::record::verify_record(
            &r.to_string(),
            &sig(&r, &k),
            manifest.as_bytes(),
            &archive,
            &trust,
        )
        .unwrap();
        assert_eq!(serde_json::to_value(verified.manifest.kind).unwrap(), kind);
        let mut bad: Value = jcs::parse(&manifest).unwrap();
        bad["files"][0]["sha256"] = json!("0".repeat(64));
        let bad = bad.to_string();
        let mut bad_members = vec![
            Member::regular("ck.json", bad.as_bytes()),
            Member::regular("body.txt", data),
        ];
        if kind == "flow" {
            bad_members.push(Member::regular("script.sh", script));
        }
        let bad_archive = archive::write(&bad_members).unwrap();
        r["manifest_sha256"] = json!(c::sha256_hex(&bad));
        r["archive_sha256"] = json!(c::sha256_hex(&bad_archive));
        r["archive_bytes"] = json!(bad_archive.len());
        assert_eq!(
            c::record::verify_record(
                &r.to_string(),
                &sig(&r, &k),
                bad.as_bytes(),
                &bad_archive,
                &trust
            )
            .unwrap_err()
            .code,
            RefusalCode::FilesMismatch
        );
        let revoked = vec![c::statements::ScopeKey {
            revocation: Some(c::statements::Revocation {
                reason: "compromised".into(),
                compromise_from: "2026-06-01T00:00:00Z".into(),
            }),
            ..keys[0].clone()
        }];
        assert_eq!(
            c::record::verify_record(
                &r.to_string(),
                &sig(&r, &k),
                bad.as_bytes(),
                &bad_archive,
                &c::record::Trust {
                    keys: &revoked,
                    published_at: None
                }
            )
            .unwrap_err()
            .code,
            RefusalCode::KeyRevoked
        );
    }
}
#[test]
fn statement_and_status_shape_refusals_are_typed() {
    for text in [
        "null",
        "[]",
        r#"{"schema":"wrong"}"#,
        r#"{"schema":"a","schema":"b"}"#,
    ] {
        assert_eq!(
            status::apply_status(None, text, "", &[], &pin())
                .unwrap_err()
                .code,
            RefusalCode::StatusInvalid
        );
        assert_eq!(
            c::statements::verify_key_cert(text, "", "")
                .unwrap_err()
                .code,
            RefusalCode::StatementInvalid
        );
        assert_eq!(
            c::statements::verify_key_rotation(text, "", "", "example", &[], &[])
                .unwrap_err()
                .code,
            RefusalCode::StatementInvalid
        );
        let mut registry = c::statements::RegistryChain {
            registry: "https://example.invalid".into(),
            keys: vec![],
        };
        assert_eq!(
            c::statements::apply_registry_key(&mut registry, text, "")
                .unwrap_err()
                .code,
            RefusalCode::StatementInvalid
        );
        let path = c::statements::RequestPath {
            action: c::statements::Action::Withdraw,
            name: "@example/hello".into(),
            version: "1.0.0".into(),
        };
        let current = c::statements::VersionStatus {
            archive_sha256: "0".repeat(64),
            sequence: 1,
            state: status::State::Available,
        };
        assert_eq!(
            c::statements::apply_status_request(&path, text, "", "", &current, &[])
                .unwrap_err()
                .code,
            RefusalCode::StatementInvalid
        );
    }
}

#[test]
fn several_roots_validate_subjects_and_do_not_leak_synthetic_requesters() {
    use c::resolver::{self, ResolveError};
    let roots = BTreeMap::from([("@x/a".into(), "^1".into())]);
    let registry = BTreeMap::new();
    let pins = BTreeMap::new();
    assert!(
        matches!(resolver::synthetic_root_ranges(&registry,&roots,&["@x/b".into()],&pins),Err(ResolveError::PackageNotFound {package}) if package=="@x/b")
    );
    assert!(
        matches!(resolver::synthetic_root_ranges(&registry,&roots,&[],&pins),Err(ResolveError::PackageNotFound {package}) if package=="@x/a")
    );
    let registry=serde_json::from_value(json!({"@x/a":[{"version":"1.0.0","kind":"skill","status":"available","requires":{"capabilities":[],"packages":{}}}]})).unwrap();
    let roots = BTreeMap::from([("@x/a".into(), "^2".into())]);
    let error =
        resolver::resolve_roots(&registry, &roots, &["@x/a".into()], &pins, &[]).unwrap_err();
    assert_eq!(
        serde_json::to_value(error).unwrap(),
        json!({"code":"no_matching_version","package":"@x/a","ranges":[{"requested_by":"(root)","range":"^2"}]})
    );
}

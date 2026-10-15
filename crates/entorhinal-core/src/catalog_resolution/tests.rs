use super::*;
use crate::catalog_agent::tests::{agent, pin};
use crate::catalog_agent::PinWriteRequest;
use crate::mutations::tests::Fixture;
use entorhinal_catalog::record::{Manifest, Record, Requires};
use entorhinal_catalog::resolver::Direction;
use entorhinal_catalog::status::State;
fn spec(name: &str) -> CatalogSpec {
    CatalogSpec {
        package: format!("@acme/{name}"),
        range: "^1".into(),
    }
}
fn candidate(name: &str, version: &str, deps: &[(&str, &str)]) -> VerifiedPackage {
    VerifiedPackage {
        record: Record {
            schema: "ck-record/v1".into(),
            name: format!("@acme/{name}"),
            version: version.into(),
            manifest_sha256: "11".repeat(32),
            archive_sha256: "22".repeat(32),
            archive_bytes: 0,
            public_key: "44".repeat(32),
            log_head: json!({}),
            flow_version: None,
        },
        manifest: Manifest {
            schema: "ck-package/v1".into(),
            name: format!("@acme/{name}"),
            version: version.into(),
            kind: Kind::Skill,
            summary: "test".into(),
            license: "MIT".into(),
            source: "test".into(),
            requires: Requires {
                capabilities: vec![],
                packages: deps
                    .iter()
                    .map(|(n, r)| (format!("@acme/{n}"), r.to_string()))
                    .collect(),
            },
            files: vec![],
            body: json!({}),
        },
        canonical_record: String::new(),
    }
}
fn setup(
    store: &RegistryStore,
    agent: &str,
    mut pins: Vec<CatalogPin>,
    versions: &[VerifiedPackage],
) {
    for pin in &mut pins {
        pin.publisher_fingerprint = key_fingerprint(&"44".repeat(32)).unwrap();
    }
    store
        .with_principal("test")
        .catalog_set_pins(
            PinWriteRequest {
                agent_id: agent.into(),
                pins,
                request_key: None,
            },
            2,
        )
        .unwrap();
    store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "INSERT OR IGNORE INTO catalog_registry VALUES('r',0,1,'inc')",
                [],
            )?;
            for v in versions {
                let held = StatusRecord {
                    schema: "ck-status/v1".into(),
                    registry_key_id: "key".into(),
                    name: v.record.name.clone(),
                    version: v.record.version.clone(),
                    archive_sha256: v.record.archive_sha256.clone(),
                    publisher_key: key_fingerprint(&v.record.public_key).unwrap(),
                    sequence: 1,
                    state: State::Available,
                    reason: None,
                    compromise_from: None,
                    published_at: "2026-01-01T00:00:00Z".into(),
                    changed_at: "2026-01-01T00:00:00Z".into(),
                };
                tx.execute(
                    "INSERT INTO catalog_status VALUES(?1,?2,'r',1,'t','t',0,?3,'signature')",
                    params![
                        v.record.name,
                        v.record.version,
                        serde_json::to_string(&held).unwrap()
                    ],
                )?;
            }
            Ok(())
        })
        .unwrap();
}
fn pins() -> Vec<CatalogPin> {
    vec![
        pin("@acme/a", "1.0.0", true),
        pin("@acme/d", "1.0.0", false),
    ]
}
#[test]
fn install_keeps_existing_root_and_dependency() {
    let f = Fixture::new("catalog-resolve-install");
    let id = agent(&f.store);
    let versions = vec![
        candidate("a", "1.0.0", &[("d", "^1")]),
        candidate("a", "1.1.0", &[("d", "^1")]),
        candidate("d", "1.0.0", &[]),
        candidate("d", "1.1.0", &[]),
        candidate("b", "1.0.0", &[]),
    ];
    setup(&f.store, &id, pins(), &versions);
    let result = f
        .store
        .catalog_resolve(&id, CatalogSubject::Install(spec("b")), &versions, &[])
        .unwrap();
    assert_eq!(result.resolution.picks["@acme/a"], "1.0.0");
    assert_eq!(result.resolution.picks["@acme/d"], "1.0.0");
    assert_eq!(
        result.resolution.moves.as_ref().unwrap()["@acme/a"].direction as u8,
        Direction::Same as u8
    );
    assert_eq!(
        result.resolution.moves.as_ref().unwrap()["@acme/b"].direction as u8,
        Direction::Added as u8
    );
    assert_eq!(result.resolution.picks.len(), 3);
    assert_eq!(result.resolution.moves.unwrap().len(), 3);
}
#[test]
fn named_upgrade_moves_private_dependency_but_keeps_shared_dependency() {
    let f = Fixture::new("catalog-resolve-upgrade");
    let id = agent(&f.store);
    let versions = vec![
        candidate("a", "1.0.0", &[("d", "^1"), ("e", "^1")]),
        candidate("a", "1.1.0", &[("d", "^1"), ("e", "^1")]),
        candidate("c", "1.0.0", &[("d", "^1")]),
        candidate("d", "1.0.0", &[]),
        candidate("d", "1.1.0", &[]),
        candidate("e", "1.0.0", &[]),
        candidate("e", "1.1.0", &[]),
    ];
    let mut current = pins();
    current.push(pin("@acme/c", "1.0.0", true));
    current.push(pin("@acme/e", "1.0.0", false));
    setup(&f.store, &id, current, &versions);
    let result = f
        .store
        .catalog_resolve(&id, CatalogSubject::Upgrade(spec("a")), &versions, &[])
        .unwrap();
    assert_eq!(result.resolution.picks["@acme/a"], "1.1.0");
    assert_eq!(result.resolution.picks["@acme/e"], "1.1.0");
    assert_eq!(result.resolution.picks["@acme/d"], "1.0.0");
}
#[test]
fn exact_pin_conflict_names_package_version_and_subject_range() {
    let f = Fixture::new("catalog-resolve-conflict");
    let id = agent(&f.store);
    let versions = vec![
        candidate("a", "1.0.0", &[("d", "^1")]),
        candidate("d", "1.0.0", &[]),
        candidate("d", "1.1.0", &[]),
        candidate("b", "1.0.0", &[("d", "~1.1")]),
    ];
    setup(&f.store, &id, pins(), &versions);
    let Err(CatalogResolveError::Refusal(error)) =
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("b")), &versions, &[])
    else {
        panic!("pin conflict not refused")
    };
    assert_eq!(
        error,
        json!({"code":"pin_conflict","package":"@acme/d","pinned_version":"1.0.0","requested_by":"@acme/b@1.0.0","range":"~1.1"})
    );
}
#[test]
fn upgrade_all_moves_everything_and_reports_down_and_removed() {
    let f = Fixture::new("catalog-resolve-all");
    let id = agent(&f.store);
    let versions = vec![
        candidate("a", "1.0.0", &[("d", "^1")]),
        candidate("a", "1.1.0", &[]),
        candidate("d", "1.0.0", &[]),
        candidate("d", "1.1.0", &[]),
        candidate("c", "1.0.0", &[]),
    ];
    let mut current = pins();
    let mut c = pin("@acme/c", "2.0.0", true);
    c.range = Some("^1".into());
    current.push(c);
    let mut versions = versions;
    versions.push(candidate("c", "2.0.0", &[]));
    setup(&f.store, &id, current, &versions);
    let result = f
        .store
        .catalog_resolve(&id, CatalogSubject::UpgradeAll, &versions, &[])
        .unwrap();
    assert_eq!(result.resolution.picks["@acme/a"], "1.1.0");
    assert_eq!(
        result.resolution.moves.as_ref().unwrap()["@acme/d"].direction as u8,
        Direction::Removed as u8
    );
    assert_eq!(
        result.resolution.moves.as_ref().unwrap()["@acme/c"].direction as u8,
        Direction::Down as u8
    );
}
#[test]
fn held_status_and_capabilities_control_eligibility_and_kind_gate() {
    let f = Fixture::new("catalog-resolve-held");
    let id = agent(&f.store);
    let mut versions = vec![candidate("a", "1.0.0", &[]), candidate("a", "1.1.0", &[])];
    versions[1].manifest.requires.capabilities = vec!["skill/search/v1".into()];
    setup(&f.store, &id, vec![], &versions);
    assert_eq!(
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("a")), &versions, &[])
            .unwrap()
            .resolution
            .picks["@acme/a"],
        "1.0.0"
    );
    assert_eq!(
        f.store
            .catalog_resolve(
                &id,
                CatalogSubject::Install(spec("a")),
                &versions,
                &["skill/search/v1".into()]
            )
            .unwrap()
            .resolution
            .picks["@acme/a"],
        "1.1.0"
    );
    f.store.db.with_conn_fenced(|tx|tx.execute("UPDATE catalog_status SET canonical=json_set(canonical,'$.state','withdrawn') WHERE version='1.1.0'",[])).unwrap();
    assert_eq!(
        f.store
            .catalog_resolve(
                &id,
                CatalogSubject::Install(spec("a")),
                &versions,
                &["skill/search/v1".into()]
            )
            .unwrap()
            .resolution
            .picks["@acme/a"],
        "1.0.0"
    );
    versions[0].manifest.kind = Kind::Flow;
    let Err(CatalogResolveError::Refusal(error)) =
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("a")), &versions, &[])
    else {
        panic!("kind gate did not refuse")
    };
    assert_eq!(
        error,
        json!({"code":"kind_not_yet_supported","package":"@acme/a"})
    );
}

#[test]
fn resolver_refuses_missing_live_root_manifests_unheld_and_mismatched_records() {
    let f = Fixture::new("catalog-resolve-refusals");
    let id = agent(&f.store);
    let versions = vec![candidate("a", "1.0.0", &[])];
    setup(&f.store, &id, vec![], &versions);
    let error = |result: Result<CatalogResolution, CatalogResolveError>, code: &str| {
        let Err(CatalogResolveError::Refusal(value)) = result else {
            panic!("expected {code}")
        };
        assert_eq!(value["code"], code);
    };
    error(
        f.store.catalog_resolve(
            "missing",
            CatalogSubject::Install(spec("a")),
            &versions,
            &[],
        ),
        "gone",
    );
    error(
        f.store
            .catalog_resolve(&id, CatalogSubject::Upgrade(spec("a")), &versions, &[]),
        "root_not_found",
    );
    let mut bad = versions.clone();
    bad[0].manifest.name = "@acme/b".into();
    error(
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("a")), &bad, &[]),
        "record_mismatch",
    );
    bad = versions.clone();
    bad[0].record.archive_sha256 = "00".repeat(32);
    error(
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("a")), &bad, &[]),
        "record_mismatch",
    );
    error(
        f.store.catalog_resolve(
            &id,
            CatalogSubject::Install(spec("a")),
            &[versions[0].clone(), versions[0].clone()],
            &[],
        ),
        "record_mismatch",
    );
    let mut unheld = versions.clone();
    unheld.push(candidate("a", "1.9.0", &[]));
    assert_eq!(
        f.store
            .catalog_resolve(&id, CatalogSubject::Install(spec("a")), &unheld, &[])
            .unwrap()
            .resolution
            .picks["@acme/a"],
        "1.0.0"
    );
    f.store
        .with_principal("test")
        .catalog_set_pins(
            PinWriteRequest {
                agent_id: id.clone(),
                pins: vec![pin("@acme/a", "1.0.0", true)],
                request_key: None,
            },
            2,
        )
        .unwrap();
    error(
        f.store
            .catalog_resolve(&id, CatalogSubject::UpgradeAll, &[], &[]),
        "pinned_manifest_missing",
    );
}

#[test]
fn pinned_candidate_bytes_and_kind_cannot_change() {
    let f = Fixture::new("catalog-pinned-candidate");
    let id = agent(&f.store);
    let versions = vec![candidate("a", "1.0.0", &[])];
    setup(
        &f.store,
        &id,
        vec![pin("@acme/a", "1.0.0", true)],
        &versions,
    );
    for field in ["manifest", "archive", "publisher", "kind"] {
        let mut bad = versions.clone();
        match field {
            "manifest" => bad[0].record.manifest_sha256 = "ff".repeat(32),
            "archive" => bad[0].record.archive_sha256 = "ff".repeat(32),
            "publisher" => bad[0].record.public_key = "ff".repeat(32),
            _ => bad[0].manifest.kind = Kind::Flow,
        }
        let Err(CatalogResolveError::Refusal(error)) =
            f.store
                .catalog_resolve(&id, CatalogSubject::UpgradeAll, &bad, &[])
        else {
            panic!("pinned identity changed: {field}")
        };
        assert_eq!(error["code"], "record_mismatch");
    }
}

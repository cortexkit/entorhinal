use super::*;

#[test]
fn user_profile_time_zone_local_database_and_scope() {
    let f = Fixture::new();
    for zone in ["Europe/Istanbul", "US/Pacific", "UTC", "Etc/GMT+3"] {
        let got =
            f.set(json!({"field":"time_zone","value":format!("  {zone}  "),"request_key":zone}));
        assert_eq!(got["user"]["time_zone"], zone);
    }
    for value in [json!("Future/Unlisted_City"), json!("utc"), json!(7)] {
        let req =
            serde_json::from_value(json!({"field":"time_zone","value":value,"request_key":"bad"}))
                .unwrap();
        assert_eq!(
            code(
                f.store
                    .with_principal("reserved:callosum")
                    .user_set(req, 701, None)
                    .unwrap_err()
            ),
            "invalid_value"
        );
    }
    let req = serde_json::from_value(json!({"field":"time_zone","value":"UTC","scope":{"kind":"workspace","id":"studio"},"request_key":"scoped"})).unwrap();
    assert_eq!(
        code(
            f.store
                .with_principal("reserved:callosum")
                .user_set(req, 701, None)
                .unwrap_err()
        ),
        "invalid_request"
    );
}

#[test]
fn user_profile_time_zone_tokens_clear_noop_and_rebuild() {
    let f = Fixture::new();
    let empty = f.get();
    let set = f.set(json!({"field":"time_zone","value":"US/Pacific","request_key":"set"}));
    assert_eq!(set["user"], json!({"time_zone":"US/Pacific"}));
    assert_ne!(empty["token"], set["token"]);
    f.verify();
    let head = f.store.generation().unwrap();
    let noop = f.set(json!({"field":"time_zone","value":" US/Pacific ","request_key":"noop"}));
    assert_eq!(noop, set);
    assert_eq!(head, f.store.generation().unwrap());
    let clear = f.set(json!({"field":"time_zone","value":null,"request_key":"clear"}));
    assert_eq!(clear["user"], json!({}));
    assert_ne!(set["token"], clear["token"]);
    assert_eq!(empty["token"], clear["token"]);
    f.verify();
    let head = f.store.generation().unwrap();
    assert_eq!(
        f.set(json!({"field":"time_zone","value":null,"request_key":"clear-noop"})),
        clear
    );
    assert_eq!(head, f.store.generation().unwrap());
}

#[test]
fn user_profile_time_zone_remote_grammar_not_database() {
    let f = Fixture::new();
    f.enable();
    let unknown = "Future/Unlisted_City";
    assert!(unknown.parse::<chrono_tz::Tz>().is_err());
    let body = json!({"op":"user.set","field":"time_zone","value":unknown,"principal":"reserved:callosum"});
    f.store
        .apply_remote_entry(&entry(1, body.clone(), false), None)
        .unwrap();
    assert_eq!(f.get()["user"]["time_zone"], unknown);
    f.verify();
    for bad in [
        "",
        "/UTC",
        "UTC/",
        "Future//City",
        "Future/Some City",
        "Future/Citý",
        "Future/City.",
        " UTC",
        "UTC\n",
        &"A".repeat(65),
    ] {
        let mut invalid = body.clone();
        invalid["value"] = json!(bad);
        assert!(
            f.store
                .apply_remote_entry(&entry(2, invalid, false), None)
                .is_err(),
            "{bad:?}"
        );
        assert_eq!(
            f.store.identity_log_status().unwrap().last_applied_position,
            1
        );
    }
    for (i, zone) in ["A".repeat(64), "Future_Zone/A1+-".into()]
        .iter()
        .enumerate()
    {
        let mut valid = body.clone();
        valid["value"] = json!(zone);
        f.store
            .apply_remote_entry(&entry(i as i64 + 2, valid, false), None)
            .unwrap();
        assert_eq!(f.get()["user"]["time_zone"], *zone);
    }
    let snapshot = f
        .store
        .read(|c| Ok(SharedState::capture(c)?.snapshot(vec![])))
        .unwrap();
    let receiver = Fixture::new();
    receiver.enable();
    receiver
        .store
        .apply_remote_entry(&entry(1, json!(snapshot), true), None)
        .unwrap();
    assert_eq!(receiver.get(), f.get());
    receiver.verify();
    f.verify();
}

#[test]
fn user_profile_time_zone_legacy_entry_bytes_unchanged() {
    let req: UserSetRequest =
        serde_json::from_value(json!({"field":"name","value":" Mira ","request_key":"legacy"}))
            .unwrap();
    let entry = req.normalized().unwrap().entry("reserved:prefrontal-core");
    assert_eq!(serde_json::to_vec(&entry).unwrap(), br#"{"op":"user.set","field":"name","value":"Mira","principal":"reserved:prefrontal-core"}"#);
    assert_eq!(canonical_bytes(&serde_json::to_value(entry).unwrap()), br#"{"field":"name","op":"user.set","principal":"reserved:prefrontal-core","value":"Mira"}"#);
}

#[test]
fn user_profile_time_zone_shared_entry_round_trip() {
    let source = Fixture::new();
    let receiver = Fixture::new();
    source.enable();
    receiver.enable();
    for (i, value) in [json!(" US/Pacific "), Value::Null].into_iter().enumerate() {
        let expected = source
            .set(json!({"field":"time_zone","value":value,"request_key":format!("share-{i}")}));
        let bytes = source.store.read(|c| c.query_row("SELECT entry FROM registry_journal WHERE op='user.set' AND stream='shared' ORDER BY seq DESC LIMIT 1", [], |r| r.get::<_,Vec<u8>>(0))).unwrap();
        let body: Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(body["field"], "time_zone");
        assert_eq!(
            body["value"],
            if value.is_null() {
                Value::Null
            } else {
                json!("US/Pacific")
            }
        );
        receiver
            .store
            .apply_remote_entry(&entry(i as i64 + 1, body, false), None)
            .unwrap();
        assert_eq!(receiver.get(), expected);
        source.verify();
        receiver.verify();
    }
}

#[test]
fn user_profile_time_zone_migration_preserves_existing_profile() {
    let f = Fixture::new();
    std::fs::create_dir_all(f._cleanup.0.join("prior-zone")).unwrap();
    let descriptor = StorageDescriptor {
        module_id: "entorhinal".into(),
        storage_namespace: "prior-zone".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: f
                ._cleanup
                .0
                .join("prior-zone/store.db")
                .to_string_lossy()
                .into(),
        },
    };
    let prior = crate::MIGRATIONS
        .iter()
        .position(|migration| migration.statements == V23_USER_TIME_ZONE)
        .unwrap();
    assert_eq!(
        crate::MIGRATIONS[prior].version,
        crate::MIGRATIONS[prior - 1].version + 1
    );
    let old =
        RegistryStore::open_with_migrations(&descriptor, &crate::MIGRATIONS[..prior]).unwrap();
    assert!(old
        .read(|c| c.prepare("SELECT time_zone FROM user_profile").map(|_| ()))
        .is_err());
    old.apply_entry("user.set", r#"{"entry":{"op":"user.set","field":"name","value":"Mira","principal":"reserved:prefrontal-core"}}"#, "test", None, |tx| tx.execute("INSERT INTO user_profile(id,name) VALUES(1,'Mira')", []).map(|_| ())).unwrap();
    drop(old);
    let upgraded = RegistryStore::open(&descriptor).unwrap();
    assert_eq!(
        upgraded.user_get(UserGetRequest::default()).unwrap()["user"],
        json!({"name":"Mira"})
    );
    assert!(upgraded.verify().unwrap().ok);
    upgraded.rebuild().unwrap();
    assert!(upgraded.verify().unwrap().ok);
    let request =
        serde_json::from_value(json!({"field":"time_zone","value":"UTC","request_key":"upgraded"}))
            .unwrap();
    upgraded
        .with_principal("reserved:prefrontal-core")
        .user_set(request, 701, None)
        .unwrap();
    assert_eq!(
        upgraded.user_get(UserGetRequest::default()).unwrap()["user"],
        json!({"name":"Mira","time_zone":"UTC"})
    );
    assert!(upgraded.verify().unwrap().ok);
}

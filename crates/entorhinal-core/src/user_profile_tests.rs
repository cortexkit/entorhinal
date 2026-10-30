use super::*;
use crate::{
    remote_apply::RemoteEntry,
    shared_entry::{canonical_bytes, SharedState},
    RegisterRequest,
};
use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT: AtomicU64 = AtomicU64::new(0);
struct Fixture {
    store: RegistryStore,
    _cleanup: crate::scratch_cleanup::ScratchCleanup,
}
impl Fixture {
    fn new() -> Self {
        let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/user-profile")
            .join(format!(
                "{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
        std::fs::create_dir_all(&root).unwrap();
        let store = RegistryStore::open(&StorageDescriptor {
            module_id: "entorhinal".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into(),
            },
        })
        .unwrap();
        for id in ["pj-one", "pj-two"] {
            store
                .register(RegisterRequest {
                    project_id: Some(id.into()),
                    name: id.into(),
                    workspace_id: Some("studio".into()),
                    ..Default::default()
                })
                .unwrap();
        }
        Self {
            store,
            _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
        }
    }
    fn get(&self) -> Value {
        self.store.user_get(UserGetRequest::default()).unwrap()
    }
    fn set(&self, value: Value) -> Value {
        let req = serde_json::from_value(value).unwrap();
        let bytes = self
            .store
            .with_principal("reserved:prefrontal-core")
            .user_set(req, 700, None)
            .unwrap();
        serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
    }
    fn enable(&self) {
        self.store
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])
                    .map(|_| ())
            })
            .unwrap();
    }
    fn verify(&self) {
        let report = self.store.verify().unwrap();
        assert!(report.ok, "{report:?}");
        let before = self.get();
        self.store.rebuild().unwrap();
        assert_eq!(self.get(), before);
        assert!(self.store.verify().unwrap().ok);
    }
}
fn entry(position: i64, value: Value, snapshot: bool) -> RemoteEntry {
    RemoteEntry {
        position,
        entry_id: format!("user-{position}"),
        author: "01".repeat(16),
        signer: "other".into(),
        key_id: "key".into(),
        envelope_version: crate::remote_apply::ENVELOPE_VERSION,
        kind: if snapshot { "snapshot" } else { "change" }.into(),
        entry: canonical_bytes(&value),
    }
}
fn code(e: RegistryError) -> String {
    if let RegistryError::Domain { code, .. } = e {
        code
    } else {
        panic!("{e}")
    }
}

#[test]
fn user_profile_tokens_scopes_idempotency_and_rebuild() {
    let f = Fixture::new();
    let empty = f.get();
    assert_eq!(empty["user"], json!({}));
    assert_eq!(empty["expertise"], json!([]));
    let first = f.set(
        json!({"field":"name","value":"  Mira  ","request_key":"name","actor":"Mira via core"}),
    );
    assert_eq!(first["user"], json!({"name":"Mira"}));
    assert_ne!(first["token"], empty["token"]);
    assert!(first.get("actor").is_none());
    assert!(first.get("generation").is_none());
    assert!(first.get("noop").is_none());
    let second = f.set(json!({"field":"pronouns","value":"they/them","request_key":"pronouns"}));
    assert_eq!(
        f.set(
            json!({"field":"name","value":"  Mira  ","request_key":"name","actor":"Mira via core"})
        ),
        first
    );
    let reused = serde_json::from_value(
        json!({"field":"name","value":"Mira","request_key":"name","actor":"Mira via core"}),
    )
    .unwrap();
    assert_eq!(
        code(
            f.store
                .with_principal("core")
                .user_set(reused, 701, None)
                .unwrap_err()
        ),
        "request_key_reused"
    );
    let head = f.store.generation().unwrap();
    let noop = f.set(json!({"field":"name","value":"Mira","request_key":"noop"}));
    assert_eq!(noop, second);
    assert_eq!(f.store.generation().unwrap(), head);
    f.set(json!({"field":"expertise","scope":{"kind":"project","id":"pj-one"},"value":"Project practice","request_key":"project"}));
    let all=f.set(json!({"field":"expertise","scope":{"kind":"workspace","id":"studio"},"value":"Workspace practice","request_key":"workspace"}));
    assert_eq!(all["expertise"][0]["scope"]["kind"], "workspace");
    let scoped = f
        .store
        .user_get(UserGetRequest {
            project_id: Some("pj-one".into()),
            token: all["token"].as_str().map(str::to_owned),
        })
        .unwrap();
    assert_eq!(scoped["unchanged"], false);
    assert_eq!(scoped["user"], all["user"]);
    assert_eq!(scoped["effective_expertise"]["text"], "Project practice");
    let unscoped = f
        .store
        .user_get(UserGetRequest {
            project_id: None,
            token: scoped["token"].as_str().map(str::to_owned),
        })
        .unwrap();
    assert_eq!(unscoped, all);
    for (project, kind, text) in [
        ("pj-one", "project", "Project practice"),
        ("pj-two", "workspace", "Workspace practice"),
    ] {
        let req = UserGetRequest {
            project_id: Some(project.into()),
            token: None,
        };
        let got = f.store.user_get(req.clone()).unwrap();
        assert_eq!(got["effective_expertise"]["from"]["kind"], kind);
        assert_eq!(got["effective_expertise"]["text"], text);
        assert_eq!(
            f.store
                .user_get(UserGetRequest {
                    token: got["token"].as_str().map(str::to_owned),
                    ..req
                })
                .unwrap(),
            json!({"token":got["token"],"unchanged":true})
        );
    }
    let old = f
        .store
        .user_get(UserGetRequest {
            project_id: Some("pj-two".into()),
            token: None,
        })
        .unwrap();
    f.store
        .assign_workspace(crate::AssignWorkspaceRequest {
            project_id: "pj-two".into(),
            workspace_id: "other".into(),
            ..Default::default()
        })
        .unwrap();
    let moved = f
        .store
        .user_get(UserGetRequest {
            project_id: Some("pj-two".into()),
            token: old["token"].as_str().map(str::to_owned),
        })
        .unwrap();
    assert_eq!(moved["unchanged"], false);
    assert!(moved.get("effective_expertise").is_none());
    assert_eq!(f.get()["token"], all["token"]);
    for id in ["missing", "pj-implicit-123"] {
        assert_eq!(
            code(
                f.store
                    .user_get(UserGetRequest {
                        project_id: Some(id.into()),
                        token: all["token"].as_str().map(str::to_owned)
                    })
                    .unwrap_err()
            ),
            "unknown_project"
        );
    }
    let audit=f.store.read(|c|c.query_row("SELECT actor,principal FROM registry_journal WHERE op='user.set' ORDER BY seq LIMIT 1",[],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))).unwrap();
    assert_eq!(
        audit,
        ("Mira via core".into(), "reserved:prefrontal-core".into())
    );
    f.verify();
    f.store
        .db
        .with_conn_fenced(|tx| tx.execute("UPDATE user_profile SET name='corrupted'", []))
        .unwrap();
    assert!(!f.store.verify().unwrap().ok);
    f.store.rebuild().unwrap();
    assert_eq!(f.get()["user"]["name"], "Mira");
    let cleared = f.set(json!({"field":"name","value":null,"request_key":"clear"}));
    assert!(cleared["user"].get("name").is_none());
    f.verify();
}

#[test]
fn user_profile_excludes_private_entries_from_both_feeds() {
    let f = Fixture::new();
    for enabled in [false, true] {
        if enabled {
            f.enable();
        }
        let head = f.store.generation().unwrap();
        f.set(json!({"field":"name","value":if enabled {"Mira"} else {"Ari"},"request_key":format!("name-{enabled}")}));
        let journal = f.store.journal_tail(head, 100).unwrap();
        assert!(
            journal.entries.is_empty(),
            "profile leaked into project feed: {journal:?}"
        );
        let agents = f.store.agent_changes(head, None).unwrap();
        assert!(
            agents.entries.is_empty(),
            "profile leaked into agent feed: {agents:?}"
        );
        let shared: f64 = f
            .store
            .read(|c| {
                c.query_row(
                    "SELECT count(*) FROM registry_journal WHERE seq>?1 AND stream='shared'",
                    [head],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(shared, if enabled { 1.0 } else { 0.0 });
    }
    f.verify();
    f.set(json!({"field":"expertise","scope":{"kind":"workspace","id":"studio"},"value":"Private skill","request_key":"private-expertise"}));
    let snapshot = f
        .store
        .read(|c| Ok(SharedState::capture(c)?.snapshot(vec![])))
        .unwrap();
    let mut body = json!(snapshot);
    body["snapshot_id"] = json!("profile-join");
    body["part"] = json!(1);
    body["parts"] = json!(1);
    let joined = Fixture::new();
    joined.enable();
    let head = joined.store.generation().unwrap();
    joined
        .store
        .apply_remote_snapshot(&[entry(1, body, true)], None)
        .unwrap();
    assert_eq!(joined.get()["user"]["name"], "Mira");
    for row in joined.store.journal_tail(head, 100).unwrap().entries {
        assert!(
            !row.payload_json.contains("user_profile"),
            "snapshot exposed profile rows"
        );
        assert!(
            !row.payload_json.contains("Mira"),
            "snapshot exposed personal text"
        );
        assert!(
            !row.payload_json.contains("user_expertise"),
            "snapshot exposed expertise rows"
        );
        assert!(
            !row.payload_json.contains("Private skill"),
            "snapshot exposed expertise text"
        );
    }
    assert!(joined
        .store
        .agent_changes(head, None)
        .unwrap()
        .entries
        .is_empty());
    joined.verify();
}

#[test]
fn user_profile_remote_validation_snapshots_cascades_and_replay() {
    let a = Fixture::new();
    let b = Fixture::new();
    a.enable();
    b.enable();
    a.set(json!({"field":"expertise","scope":{"kind":"workspace","id":"studio"},"value":"Workspace skill","request_key":"ws"}));
    a.set(json!({"field":"expertise","scope":{"kind":"project","id":"pj-one"},"value":"Project skill","request_key":"pj"}));
    a.set(json!({"field":"name","value":"Mira","request_key":"name"}));
    let rows = a
        .store
        .read(|c| {
            c.prepare("SELECT entry FROM registry_journal WHERE op='user.set' ORDER BY seq")?
                .query_map([], |r| r.get::<_, Vec<u8>>(0))?
                .collect::<rusqlite::Result<Vec<_>>>()
        })
        .unwrap();
    for (i, bytes) in rows.iter().enumerate() {
        b.store
            .apply_remote_entry(
                &entry(i as i64 + 1, serde_json::from_slice(bytes).unwrap(), false),
                None,
            )
            .unwrap();
    }
    assert_eq!(a.get(), b.get());
    b.verify();
    let invalid =
        json!({"op":"user.set","field":"name","value":"bad\nline","principal":"reserved:callosum"});
    assert!(b
        .store
        .apply_remote_entry(&entry(4, invalid, false), None)
        .is_err());
    assert_eq!(
        b.store.identity_log_status().unwrap().last_applied_position,
        3
    );
    let snapshot = a
        .store
        .read(|c| Ok(SharedState::capture(c)?.snapshot(vec![])))
        .unwrap();
    let c = Fixture::new();
    c.enable();
    c.store
        .apply_remote_entry(&entry(1, json!(snapshot), true), None)
        .unwrap();
    assert_eq!(c.get(), a.get());
    c.verify();
    let mut bad = snapshot.clone();
    bad.tables.get_mut("user_profile").unwrap().upsert[0]
        .insert("name".into(), json!("bad\u{2066}"));
    assert!(c.store.db.with_conn_fenced(|tx| bad.apply(tx, 1)).is_err());
    let mut empty = snapshot.clone();
    let row = &mut empty.tables.get_mut("user_profile").unwrap().upsert[0];
    for field in [
        "name",
        "address_as",
        "pronouns",
        "computer_savviness",
        "verbosity",
    ] {
        row.insert(field.into(), Value::Null);
    }
    assert!(c
        .store
        .db
        .with_conn_fenced(|tx| empty.apply(tx, 1))
        .is_err());
    let deletion = json!({"op":"project.shared","tables":{"project":{"upsert":[],"delete":[{"project_id":"pj-one"}]},"project_workspace":{"upsert":[],"delete":[{"project_id":"pj-one"}]}}});
    b.store
        .apply_remote_entry(&entry(4, deletion, false), None)
        .unwrap();
    assert_eq!(b.get()["expertise"].as_array().unwrap().len(), 1);
    b.verify();
    let deletion = json!({"op":"project.shared","tables":{"workspace":{"upsert":[],"delete":[{"workspace_id":"studio"}]},"project_workspace":{"upsert":[],"delete":[{"project_id":"pj-two"}]}}});
    b.store
        .apply_remote_entry(&entry(5, deletion, false), None)
        .unwrap();
    assert_eq!(b.get()["expertise"], json!([]));
    b.verify();
    a.store
        .remove(crate::RemoveRequest {
            project_id: Some("pj-one".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(a.get()["expertise"].as_array().unwrap().len(), 1);
    a.verify();
}

#[test]
fn user_profile_migration_and_local_workspace_cascade() {
    let f = Fixture::new();
    std::fs::create_dir_all(f._cleanup.0.join("prior")).unwrap();
    let descriptor = StorageDescriptor {
        module_id: "entorhinal".into(),
        storage_namespace: "prior-test".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: f._cleanup.0.join("prior/store.db").to_string_lossy().into(),
        },
    };
    let prior = crate::MIGRATIONS.len() - 1;
    assert_eq!(
        crate::MIGRATIONS[prior].version,
        crate::MIGRATIONS[prior - 1].version + 1
    );
    let old =
        RegistryStore::open_with_migrations(&descriptor, &crate::MIGRATIONS[..prior]).unwrap();
    assert_eq!(
        old.read(|c| c.query_row(
            "SELECT count(*) FROM sqlite_master WHERE name='user_profile'",
            [],
            |r| r.get::<_, i64>(0)
        ))
        .unwrap(),
        0
    );
    drop(old);
    let upgraded = RegistryStore::open(&descriptor).unwrap();
    assert_eq!(
        upgraded.user_get(UserGetRequest::default()).unwrap()["user"],
        json!({})
    );
    f.set(json!({"field":"expertise","scope":{"kind":"workspace","id":"studio"},"value":"Skill","request_key":"skill"}));
    f.store
        .remove(crate::RemoveRequest {
            workspace_id: Some("studio".into()),
            ..Default::default()
        })
        .unwrap();
    assert_eq!(f.get()["expertise"], json!([]));
    f.verify();
}

#[test]
fn user_profile_text_rules_count_characters_and_validate_remote_changes() {
    let f = Fixture::new();
    for (field, limit) in [
        ("name", 64),
        ("address_as", 64),
        ("pronouns", 32),
        ("expertise", 300),
    ] {
        for (suffix, value, valid) in [
            ("max", json!("é".repeat(limit)), true),
            ("over", json!("é".repeat(limit + 1)), false),
            ("empty", json!("  "), false),
            ("type", json!(1), false),
        ] {
            let mut req =
                json!({"field":field,"value":value,"request_key":format!("{field}-{suffix}")});
            if field == "expertise" {
                req["scope"] = json!({"kind":"workspace","id":"studio"});
            }
            let out = f.store.with_principal("core").user_set(
                serde_json::from_value(req).unwrap(),
                1,
                None,
            );
            assert_eq!(out.is_ok(), valid, "{field} {suffix}");
        }
    }
    for (i, c) in [
        '\n', '\t', '\r', '\0', '\u{7f}', '\u{85}', '\u{202a}', '\u{202e}', '\u{2066}', '\u{2069}',
        '\u{2028}', '\u{2029}',
    ]
    .iter()
    .enumerate()
    {
        let req: UserSetRequest = serde_json::from_value(
            json!({"field":"name","value":format!("{c}Mira"),"request_key":format!("bad-{i}")}),
        )
        .unwrap();
        assert_eq!(
            code(
                f.store
                    .with_principal("core")
                    .user_set(req, 1, None)
                    .unwrap_err()
            ),
            "invalid_value"
        );
    }
    for value in [json!(0), json!(4), json!(1.5), json!("2"), json!(true)] {
        let req = serde_json::from_value(
            json!({"field":"computer_savviness","value":value,"request_key":"bad-int"}),
        )
        .unwrap();
        assert_eq!(
            code(
                f.store
                    .with_principal("core")
                    .user_set(req, 1, None)
                    .unwrap_err()
            ),
            "invalid_value"
        );
    }
    f.enable();
    for (i, (field, value, scope)) in [
        ("name", json!("x".repeat(65)), None),
        ("pronouns", json!("bad\ttext"), None),
        ("verbosity", json!("other"), None),
        (
            "expertise",
            json!("skill"),
            Some(json!({"kind":"project","id":"pj-implicit-1"})),
        ),
        (
            "expertise",
            json!("skill"),
            Some(json!({"kind":"workspace","id":"absent"})),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        let mut user =
            json!({"op":"user.set","field":field,"value":value,"principal":"reserved:callosum"});
        if let Some(scope) = scope {
            user["scope"] = scope;
        }
        assert!(
            f.store
                .apply_remote_entry(&entry(1, user, false), None)
                .is_err(),
            "case {i}"
        );
    }
    f.verify();
}

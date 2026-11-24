use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    enable_state::HubSnapshot, remote_apply::RemoteEntry, AddRootRequest, RegisterRequest,
    RegistryStore, ResolveRemoteStatus, SetOwnedRemotesRequest,
};
use rusqlite::Connection;
use serde_json::Value;

#[cfg(windows)]
#[path = "support/windows_acl.rs"]
mod windows_acl;

static COUNTER: AtomicU64 = AtomicU64::new(0);

struct Fixture {
    store: RegistryStore,
    db: Connection,
    scratch: PathBuf,
    _cleanup: ScratchCleanup,
}

impl Fixture {
    fn new() -> Self {
        let scratch = std::env::temp_dir().join(format!(
            "entorhinal-git-{}-{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&scratch).unwrap();
        let scratch = scratch.canonicalize().unwrap();
        let path = scratch.join("store.db");
        let mut store = RegistryStore::open(&StorageDescriptor {
            module_id: "entorhinal".into(),
            storage_namespace: scratch.to_str().unwrap().into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: path.to_str().unwrap().into(),
            },
        })
        .unwrap();
        store.set_root_records(true);
        let db = Connection::open(path).unwrap();
        Self {
            store,
            db,
            _cleanup: ScratchCleanup(scratch.clone()),
            scratch,
        }
    }

    fn repo(&self, name: &str, remote: &str) -> String {
        let root = self.scratch.join(name);
        fs::create_dir_all(root.join(".git")).unwrap();
        fs::write(
            root.join(".git/config"),
            format!("[remote \"origin\"]\nurl = https://github.com/{remote}.git\n"),
        )
        .unwrap();
        RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap()
    }

    fn register(&self, project: &str, roots: &[&str], label: Option<&str>) {
        self.store
            .register(RegisterRequest {
                project_id: Some(project.into()),
                name: project.into(),
                roots: roots.iter().map(|r| r.to_string()).collect(),
                label: label.map(str::to_string),
                ..Default::default()
            })
            .unwrap();
    }

    fn enabled(&self, enabled: bool) {
        // Test key selection without a log publisher or a saved enable plan.
        self.db
            .execute(
                "UPDATE identity_log_state SET state=?1,hub_log_base=?2",
                rusqlite::params![
                    if enabled { "enabled" } else { "disabled" },
                    enabled.then_some(0)
                ],
            )
            .unwrap();
    }

    fn keys(&self) -> Vec<(String, String, String, i64)> {
        self.db.prepare("SELECT project_id,kind,root_key,created_at FROM project_root_key ORDER BY project_id,kind,root_key").unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?))).unwrap().map(Result::unwrap).collect()
    }

    fn mappings(&self) -> Vec<(String, Option<String>, Option<String>)> {
        self.db.prepare("SELECT canonical_root,root_key_kind,root_key FROM project_root ORDER BY canonical_root").unwrap()
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?))).unwrap().map(Result::unwrap).collect()
    }

    fn record(&self, root: &str) -> Value {
        let reply = self.store.resolve(root).unwrap();
        serde_json::to_value(&reply.root_fields.unwrap().root_records[0]).unwrap()
    }

    fn install_hub_snapshot(&self) {
        let holder = "ab".repeat(16);
        self.store
            .begin_hub_enable(&"cd".repeat(16), &holder)
            .unwrap();
        let plan = self.store.hub_enable_plan().unwrap().unwrap();
        let image: HubSnapshot = serde_json::from_str(&plan.preimage).unwrap();
        let expected: Vec<Value> = self.keys().into_iter().map(|(project, kind, key, created_at)|
            serde_json::json!({"project_id":project,"kind":kind,"root_key":key,"created_at":created_at})).collect();
        assert_eq!(serde_json::to_value(&image.tables["project_root_key"]).unwrap(), serde_json::json!(expected),
            "snapshot must carry the original key rows, not only leave the local database unchanged");
        let part = RemoteEntry {
            position: 1,
            entry_id: "01".repeat(16),
            author: holder.clone(),
            signer: "test".into(),
            key_id: "test".into(),
            envelope_version: 1,
            kind: "snapshot".into(),
            entry: plan.preimage.into_bytes(),
        };
        self.store
            .install_hub_snapshot(&image, &[(part, 0)], &holder)
            .unwrap();
    }
}

struct ScratchCleanup(PathBuf);

impl Drop for ScratchCleanup {
    fn drop(&mut self) {
        // Declared as the fixture's last field, so it drops after the store
        // and its SQLite connections have closed: Windows refuses to delete a
        // directory holding an open file.
        let _ = fs::remove_dir_all(&self.0);
    }
}

struct Unreadable {
    #[cfg(unix)]
    path: PathBuf,
    #[cfg(unix)]
    permissions: fs::Permissions,
    #[cfg(windows)]
    acl: Option<windows_acl::SavedDacl>,
}

impl Unreadable {
    fn config(root: &str) -> Self {
        let path = Path::new(root).join(".git/config");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let permissions = fs::metadata(&path).unwrap().permissions();
            fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();
            assert!(
                fs::read_to_string(&path).is_err(),
                "mode 000 must actually deny config reads; run tests as an unprivileged user"
            );
            Self { path, permissions }
        }
        #[cfg(windows)]
        {
            let acl = windows_acl::deny_read_for_current_user(&path).unwrap();
            assert!(
                fs::read_to_string(&path).is_err(),
                "deny-read ACL must deny config reads"
            );
            Self { acl: Some(acl) }
        }
    }
}

impl Drop for Unreadable {
    fn drop(&mut self) {
        #[cfg(unix)]
        fs::set_permissions(&self.path, self.permissions.clone()).unwrap();
        #[cfg(windows)]
        windows_acl::restore(self.acl.take().unwrap()).unwrap();
    }
}

#[test]
fn unreadable_config_reports_os_error_and_no_remotes() {
    let f = Fixture::new();
    let root = f.repo("root", "owner/unreadable");
    f.register("P", &[&root], None);
    assert!(f.record(&root).get("remotesError").is_none());
    let _unreadable = Unreadable::config(&root);
    let error = fs::read_to_string(Path::new(&root).join(".git/config"))
        .unwrap_err()
        .to_string();
    let record = f.record(&root);
    assert_eq!(record["remotes"], serde_json::json!([]));
    assert_eq!(record["remotesError"], error);
}

#[test]
fn unreadable_config_register_and_add_root_mint_no_key() {
    let f = Fixture::new();
    f.enabled(true);
    let root = f.repo("first", "owner/first");
    let _unreadable = Unreadable::config(&root);
    f.register("P", &[&root], Some("must-not-fallback"));
    assert!(
        f.keys().is_empty(),
        "unreadable register must mint neither remote nor label key"
    );
    let added = f.repo("added", "owner/added");
    let _added_unreadable = Unreadable::config(&added);
    f.store
        .add_root(AddRootRequest {
            project_id: "P".into(),
            root: added,
            label: Some("must-not-fallback".into()),
            actor: None,
        })
        .unwrap();
    assert!(f.keys().is_empty(), "unreadable add-root must mint no key");
    assert!(f
        .mappings()
        .iter()
        .all(|(_, kind, key)| kind.is_none() && key.is_none()));
    let keyed = f.repo("keyed", "owner/keyed");
    f.register("K", &[&keyed], None);
    let before = (f.keys(), f.mappings());
    let _keyed_unreadable = Unreadable::config(&keyed);
    f.register("K", &[&keyed], Some("do-not-rekey"));
    assert_eq!(
        (f.keys(), f.mappings()),
        before,
        "existing key must remain unchanged"
    );
}

#[test]
fn unreadable_config_hub_snapshot_carries_key_rows_and_keeps_mappings() {
    let f = Fixture::new();
    f.enabled(true);
    let keyed = f.repo("a-keyed", "owner/carried");
    let second = f.repo("b-empty", "owner/second");
    fs::write(
        Path::new(&second).join(".git/config"),
        "[core]\nbare = false\n",
    )
    .unwrap();
    f.register("P", &[&keyed, &second], None);
    let unkeyed = f.repo("c-unkeyed", "owner/unkeyed");
    let _unkeyed_unreadable = Unreadable::config(&unkeyed);
    f.register("U", &[&unkeyed], Some("no-fallback"));
    let before = (f.keys(), f.mappings());
    assert_eq!(before.0.len(), 1);
    let _unreadable = Unreadable::config(&keyed);
    f.enabled(false);
    f.install_hub_snapshot();
    assert_eq!(
        (f.keys(), f.mappings()),
        before,
        "hub install must preserve carried rows, timestamps and local mappings"
    );
}

#[test]
fn unreadable_config_hub_snapshot_preserves_key_shared_by_two_roots() {
    for unreadable_name in ["a-first", "z-last"] {
        let f = Fixture::new();
        f.enabled(true);
        let first = f.repo("a-first", "owner/shared");
        let last = f.repo("z-last", "owner/shared");
        f.register("P", &[&first, &last], None);
        let before = (f.keys(), f.mappings());
        assert_eq!(before.0.len(), 1);
        let unreadable = if unreadable_name == "a-first" {
            &first
        } else {
            &last
        };
        let _unreadable = Unreadable::config(unreadable);
        f.enabled(false);
        f.install_hub_snapshot();
        assert_eq!(
            (f.keys(), f.mappings()),
            before,
            "hub install must preserve one durable row and both local mappings"
        );
    }
}

#[test]
fn unreadable_config_existing_key_refuses_register_from_another_project() {
    let f = Fixture::new();
    f.enabled(true);
    let carried = f.repo("a-carried", "owner/collision");
    f.register("P", &[&carried], None);
    let _unreadable = Unreadable::config(&carried);
    let foreign = f.repo("z-foreign", "owner/collision");
    let before = (
        f.keys(),
        f.mappings(),
        serde_json::to_value(f.store.journal_tail(0, 100).unwrap()).unwrap(),
    );
    let error = f
        .store
        .register(RegisterRequest {
            project_id: Some("Q".into()),
            name: "Q".into(),
            roots: vec![foreign],
            ..Default::default()
        })
        .expect_err("a durable key must still prevent a new project from claiming it");
    assert!(error.to_string().starts_with("root_key_exists"), "{error}");
    assert_eq!(
        (
            f.keys(),
            f.mappings(),
            serde_json::to_value(f.store.journal_tail(0, 100).unwrap()).unwrap()
        ),
        before
    );
}

#[test]
fn unreadable_config_resolve_remote_skips_only_bad_root() {
    let f = Fixture::new();
    let bad = f.repo("a-bad", "owner/bad");
    f.register("A", &[&bad], None);
    let good = f.repo("z-good", "owner/good");
    f.register("Z", &[&good], None);
    let _unreadable = Unreadable::config(&bad);
    assert!(matches!(
        f.store.resolve_remote("owner", "bad").unwrap().status,
        ResolveRemoteStatus::None
    ));
    assert!(
        matches!(f.store.resolve_remote("owner", "good").unwrap().status, ResolveRemoteStatus::Found { project_id, .. } if project_id == "Z")
    );
}

#[test]
fn unreadable_config_owned_remotes_refusal_writes_nothing() {
    let f = Fixture::new();
    let root = f.repo("root", "owner/root");
    f.register("P", &[&root], None);
    let _unreadable = Unreadable::config(&root);
    let before = serde_json::to_value(f.store.journal_tail(0, 100).unwrap()).unwrap();
    let error = f
        .store
        .set_owned_remotes(SetOwnedRemotesRequest {
            root,
            remotes: Some(vec!["upstream".into()]),
            actor: None,
            request_key: None,
        })
        .unwrap_err();
    assert!(
        error.to_string().starts_with("remotes_unreadable"),
        "{error}"
    );
    assert_eq!(
        serde_json::to_value(f.store.journal_tail(0, 100).unwrap()).unwrap(),
        before
    );
    assert_eq!(
        f.db.query_row("SELECT COUNT(*) FROM root_owned_remotes", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

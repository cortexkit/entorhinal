use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    enable_state::EnablePart, RegisterRequest, RegistryError, RegistryStore, SetOwnedRemotesRequest,
};
use serde_json::Value;
use std::{
    fs,
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicUsize, Ordering},
};

static NEXT: AtomicUsize = AtomicUsize::new(0);
const TOKEN: &str = "cortexkit-root-incarnation";

struct Fixture {
    // Fields drop in declaration order; close SQLite before removing its files.
    store: RegistryStore,
    _scratch: ScratchDir,
    root: String,
}

struct ScratchDir(PathBuf);

impl Drop for ScratchDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn git(root: &Path, args: &[&str]) {
    let output = Command::new("git")
        .arg("-C")
        .arg(root)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

impl Fixture {
    fn new() -> Self {
        let dir = std::env::temp_dir().join(format!(
            "entorhinal-rebind-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed),
        ));
        fs::create_dir_all(dir.join("checkout")).unwrap();
        let dir = dir.canonicalize().unwrap();
        let descriptor = StorageDescriptor {
            module_id: "rebind-test".into(),
            storage_namespace: "test".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: dir.join("store.db").to_str().unwrap().into(),
            },
        };
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        let root =
            RegistryStore::canonical_mutation_root(dir.join("checkout").to_str().unwrap()).unwrap();
        Self {
            store,
            _scratch: ScratchDir(dir),
            root,
        }
    }

    fn init(&self) {
        git(Path::new(&self.root), &["init", "-q"]);
        git(
            Path::new(&self.root),
            &[
                "remote",
                "add",
                "origin",
                "https://github.com/rebind/repo.git",
            ],
        );
        git(
            Path::new(&self.root),
            &[
                "remote",
                "add",
                "upstream",
                "https://github.com/rebind/upstream.git",
            ],
        );
    }

    fn register(&self) {
        self.store
            .register(RegisterRequest {
                project_id: Some("project".into()),
                name: "Project".into(),
                roots: vec![self.root.clone()],
                ..Default::default()
            })
            .unwrap();
    }

    fn record(&self) -> Value {
        serde_json::to_value(self.store.trust(&self.root).unwrap()).unwrap()["rootRecords"][0]
            .clone()
    }
}

fn result(bytes: Vec<u8>) -> Value {
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
}

#[test]
fn rebind_replaced_checkout_preserves_project_and_requires_approval() {
    let f = Fixture::new();
    f.init();
    f.register();
    f.store.bind_root(&f.root, "entorhinal").unwrap();
    f.store.approve_root(&f.root, "operator").unwrap();
    f.store
        .set_owned_remotes(SetOwnedRemotesRequest {
            root: f.root.clone(),
            remotes: Some(vec!["upstream".into()]),
            ..Default::default()
        })
        .unwrap();
    let plan = f.store.prepare_enable("rebind-test", 100).unwrap();
    let parts: Vec<_> = plan
        .bodies
        .into_iter()
        .enumerate()
        .map(|(i, data)| EnablePart {
            entry_id: [i as u8 + 1; 16],
            data,
        })
        .collect();
    f.store.begin_enable(&parts, &plan.backfill).unwrap();
    f.store
        .finish_enable(100, "direct", Some("machine"))
        .unwrap();
    let before = f.record();
    assert!(
        before["rootKey"].is_object(),
        "preservation needs a mapped key"
    );
    let head = f.store.generation().unwrap();
    fs::remove_dir_all(Path::new(&f.root).join(".git")).unwrap();
    f.init();
    assert_eq!(f.record()["identity"], "replaced");
    let reply = result(
        f.store
            .with_principal("direct")
            .rebind_root(&f.root, "entorhinal")
            .unwrap(),
    );
    assert_eq!(reply["projectId"], "project");
    assert_eq!(reply["canonicalRoot"], f.root);
    assert_eq!(reply["oldIdentity"], "replaced");
    assert_eq!(reply["identity"], "bound");
    assert_eq!(reply["approval"]["state"], "unapproved");
    assert_eq!(reply["noop"], false);
    let rebound = f.record();
    assert_ne!(rebound["registrationEpoch"], before["registrationEpoch"]);
    assert_ne!(rebound["incarnation"], before["incarnation"]);
    assert_eq!(rebound["rootKey"], before["rootKey"]);
    assert_eq!(rebound["remotes"], before["remotes"]);
    assert_eq!(f.store.resolve(&f.root).unwrap().project_id, "project");
    let journal = f.store.journal_tail(head, 10).unwrap().entries;
    assert_eq!(journal.len(), 1);
    assert_eq!(journal[0].op, "bind_root");
    assert_eq!(journal[0].actor, "entorhinal");
    assert_eq!(journal[0].principal.as_deref(), Some("direct"));
    let payload: Value = serde_json::from_str(&journal[0].payload_json).unwrap();
    assert_eq!(payload["replacedEpoch"], before["registrationEpoch"]);
    assert_eq!(payload["registrationEpoch"], rebound["registrationEpoch"]);
    let head = f.store.generation().unwrap();
    let noop = result(f.store.rebind_root(&f.root, "entorhinal").unwrap());
    assert_eq!(noop["noop"], true);
    assert_eq!(noop["oldIdentity"], "bound");
    assert_eq!(f.store.generation().unwrap(), head);
    assert!(f.store.verify().unwrap().ok);
    assert!(f.store.rebuild().unwrap().replay.ok);
    assert_eq!(f.record(), rebound);
    assert!(f.store.verify().unwrap().ok);
    f.store
        .set_project_approval(&f.root, "operator", true)
        .unwrap();
    assert_eq!(f.record()["approval"]["state"], "approved");
}

#[test]
fn rebind_bound_checkout_is_noop_without_journal_or_token_changes() {
    let f = Fixture::new();
    f.init();
    f.register();
    f.store.bind_root(&f.root, "entorhinal").unwrap();
    f.store.approve_root(&f.root, "operator").unwrap();
    let record = f.record();
    let head = f.store.generation().unwrap();
    let token = fs::read(Path::new(&f.root).join(".git").join(TOKEN)).unwrap();
    let reply = result(
        f.store
            .rebind_root(&format!("{}/.", f.root), "entorhinal")
            .unwrap(),
    );
    assert_eq!(reply["canonicalRoot"], f.root);
    assert_eq!(reply["projectId"], "project");
    assert_eq!(reply["oldIdentity"], "bound");
    assert_eq!(reply["identity"], "bound");
    assert_eq!(reply["noop"], true);
    assert_eq!(reply["approval"]["state"], "approved");
    assert_eq!(f.record(), record);
    assert_eq!(
        fs::read(Path::new(&f.root).join(".git").join(TOKEN)).unwrap(),
        token
    );
    assert_eq!(f.store.generation().unwrap(), head);
    assert!(f.store.journal_tail(head, 10).unwrap().entries.is_empty());
}

#[test]
fn rebind_unbound_registered_checkout_binds_without_approval() {
    let f = Fixture::new();
    f.init();
    f.register();
    assert_eq!(f.record()["identity"], "unbound");
    let reply = result(f.store.rebind_root(&f.root, "entorhinal").unwrap());
    assert_eq!(reply["oldIdentity"], "unbound");
    assert_eq!(reply["identity"], "bound");
    assert_eq!(reply["approval"]["state"], "unapproved");
    assert_eq!(reply["noop"], false);
    assert!(f.store.verify().unwrap().ok);
}

#[test]
fn rebind_unregistered_checkout_refuses_before_token_creation() {
    let f = Fixture::new();
    f.init();
    let head = f.store.generation().unwrap();
    assert!(matches!(
        f.store.rebind_root(&f.root, "entorhinal"),
        Err(RegistryError::Domain { code, .. }) if code == "not_found"
    ));
    assert!(!Path::new(&f.root).join(".git").join(TOKEN).exists());
    assert_eq!(f.store.generation().unwrap(), head);
    assert!(f.store.enumerate(None).unwrap().projects.is_empty());
}

#[test]
fn rebind_unverifiable_checkout_refuses_without_binding() {
    let f = Fixture::new();
    f.register();
    let head = f.store.generation().unwrap();
    assert_eq!(f.record()["identity"], "unverifiable");
    let error = f.store.rebind_root(&f.root, "entorhinal").unwrap_err();
    assert!(matches!(error, RegistryError::Domain { code, message, .. }
        if code == "root_identity_unverifiable" && message.contains(".git")));
    assert!(f.record()["registrationEpoch"].is_null());
    assert_eq!(f.store.generation().unwrap(), head);
    f.init();
    fs::write(Path::new(&f.root).join(".git").join(TOKEN), "malformed\n").unwrap();
    assert!(matches!(f.store.rebind_root(&f.root, "entorhinal"),
        Err(RegistryError::Domain { code, message, .. })
        if code == "root_identity_unverifiable" && message.contains("malformed")));
    assert!(f.record()["registrationEpoch"].is_null());
    assert_eq!(f.store.generation().unwrap(), head);
    assert!(f.store.verify().unwrap().ok);
}

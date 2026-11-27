use super::*;
use crate::{
    agent_ops::{OperatorConfirmError, OperatorConfirmer},
    tests::{flow_stamp, reserved},
    RouteAdmission, RouteKey, WRITER_MODULE,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;
const DIRECT: RouteKey = (501, 1);
const CORE: RouteKey = (502, 1);
#[derive(Default)]
struct Prompt {
    summaries: Mutex<Vec<String>>,
    decline: bool,
    unavailable: bool,
    on_prompt: Mutex<Option<Box<dyn FnOnce() + Send>>>,
}
#[async_trait::async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.summaries.lock().unwrap().push(summary.into());
        if let Some(f) = self.on_prompt.lock().unwrap().take() {
            f();
        }
        if self.unavailable {
            Err(OperatorConfirmError::PresenceUnavailable)
        } else if self.decline {
            Err(OperatorConfirmError::Declined)
        } else {
            Ok(())
        }
    }
}
struct Fixture {
    handler: ProjectsHandler,
    dir: std::path::PathBuf,
    prompt: Arc<Prompt>,
}
impl Fixture {
    fn new(decline: bool) -> Self {
        Self::with_prompt(Prompt {
            decline,
            ..Default::default()
        })
    }
    fn with_prompt(prompt: Prompt) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor("memory-context");
        let prompt = Arc::new(prompt);
        let handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700)
            .with_operator_confirmer(prompt.clone());
        let store = entorhinal_core::RegistryStore::open(&descriptor).unwrap();
        store
            .register(entorhinal_core::RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        *handler.store.lock().unwrap() = Some(store);
        for (key, principal) in [(DIRECT, Principal::Direct), (CORE, reserved(WRITER_MODULE))] {
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
        }
        Self {
            handler,
            dir,
            prompt,
        }
    }
    async fn call(&self, key: RouteKey, op: &str, params: Value) -> Result<Value, String> {
        match self.outcome(key, op, params).await {
            HandlerOutcome::Response(bytes) => {
                Ok(serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
            }
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                Err(code)
            }
            _ => panic!("unexpected handler outcome"),
        }
    }
    async fn outcome(&self, key: RouteKey, op: &str, params: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":op,"params":params})).unwrap(),
                key,
            )
            .await
    }
    fn count(&self) -> usize {
        self.prompt.summaries.lock().unwrap().len()
    }

    fn store(&self) -> entorhinal_core::RegistryStore {
        self.handler.with_store(|store| Ok(store.clone())).unwrap()
    }

    fn checkout(&self, name: &str) -> String {
        let path = self.dir.join(name);
        std::fs::create_dir_all(path.join(".git")).unwrap();
        std::fs::write(path.join(".git/config"), "[core]\n").unwrap();
        entorhinal_core::RegistryStore::canonical_mutation_root(path.to_str().unwrap()).unwrap()
    }

    fn unapproved_root(&self) -> String {
        let root = self.checkout("checkout");
        let store = self.store();
        store
            .add_root(entorhinal_core::AddRootRequest {
                project_id: "P".into(),
                root: root.clone(),
                label: None,
                actor: None,
            })
            .unwrap();
        store.bind_root(&root, "fixture").unwrap();
        root
    }

    fn batch_project(&self, id: &str, bound: bool) -> String {
        let root = self.checkout(id);
        let store = self.store();
        store
            .register(entorhinal_core::RegisterRequest {
                project_id: Some(id.into()),
                name: format!("Project {id}"),
                roots: vec![root.clone()],
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        if bound {
            store.bind_root(&root, "fixture").unwrap();
        }
        root
    }

    async fn approve_projects(&self, params: Value) -> Result<Value, String> {
        self.call(DIRECT, "approve_projects", params).await
    }

    fn workspace_folder(&self, name: &str) -> String {
        let path = self.dir.join(name);
        std::fs::create_dir(&path).unwrap();
        crate::tests::canonical_test_path(path)
            .to_str()
            .unwrap()
            .into()
    }

    fn workspace_approval_rows(&self) -> i64 {
        rusqlite::Connection::open(self.dir.join("store.db"))
            .unwrap()
            .query_row("SELECT COUNT(*) FROM workspace_root_approval", [], |row| {
                row.get(0)
            })
            .unwrap()
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        std::fs::remove_dir_all(&self.dir).unwrap();
    }
}

fn approval(store: &entorhinal_core::RegistryStore, root: &str) -> &'static str {
    store
        .trust(root)
        .unwrap()
        .root_fields
        .unwrap()
        .root_records
        .into_iter()
        .find(|record| record.root == root)
        .unwrap()
        .approval
        .state
}

#[tokio::test]
async fn workspace_approval_set_refuses_without_presence_and_writes_nothing() {
    let f = Fixture::with_prompt(Prompt {
        unavailable: true,
        ..Default::default()
    });
    let root = f.workspace_folder("workspace-root");
    let store = f.store();
    let before = store.generation().unwrap();
    let params = json!({"workspaceId":"W","root":root});
    assert_eq!(
        f.call(DIRECT, "set_workspace_root", params)
            .await
            .unwrap_err(),
        "operator_presence_unavailable"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    let after = f
        .call(CORE, "enumerate", json!({"workspaceId":"W"}))
        .await
        .unwrap();
    assert!(after["workspaces"][0]["root"].is_null());
    assert!(after["workspaces"][0].get("approval").is_none());
    assert_eq!(f.workspace_approval_rows(), 0);
}

#[tokio::test]
async fn workspace_approval_set_and_approve_commit_together_and_retries_do_not_prompt() {
    let f = Fixture::new(false);
    let root = f.workspace_folder("workspace-root");
    let before = f.store().generation().unwrap();
    let request = json!({"workspaceId":"W","root":root,"requestKey":"set-1"});
    let set = f
        .call(DIRECT, "set_workspace_root", request.clone())
        .await
        .unwrap();
    assert_eq!(set["root"], root);
    assert_eq!(
        set["approval"],
        json!({"journalSeq":before+2,"state":"approved"})
    );
    assert_eq!(set["generation"], before + 2);
    assert_eq!(set["noop"], false);
    assert_eq!(f.workspace_approval_rows(), 1);
    assert_eq!(f.count(), 1);
    let summaries = f.prompt.summaries.lock().unwrap().clone();
    assert!(
        summaries[0].contains("W") && summaries[0].contains(&root),
        "{summaries:?}"
    );
    assert_eq!(
        f.call(DIRECT, "set_workspace_root", request).await.unwrap(),
        set
    );
    assert_eq!(f.count(), 1);
    let noop = f
        .call(
            DIRECT,
            "set_workspace_root",
            json!({"workspaceId":"W","root":root}),
        )
        .await
        .unwrap();
    assert_eq!(noop["noop"], true);
    assert_eq!(noop["approval"], set["approval"]);
    assert_eq!(f.store().generation().unwrap(), before + 2);
}

#[tokio::test]
async fn workspace_approval_set_rejects_file_id_changed_during_prompt() {
    let f = Fixture::new(false);
    let root = f.workspace_folder("workspace-root");
    let moved = f.dir.join("moved-folder");
    let store = f.store();
    let before = store.generation().unwrap();
    let original = entorhinal_core::file_id(std::path::Path::new(&root)).unwrap();
    let target = root.clone();
    *f.prompt.on_prompt.lock().unwrap() = Some(Box::new(move || {
        std::fs::rename(&target, moved).unwrap();
        std::fs::create_dir(&target).unwrap();
    }));
    let request = json!({"workspaceId":"W","root":root});
    assert_eq!(
        f.call(DIRECT, "set_workspace_root", request)
            .await
            .unwrap_err(),
        "operator_approval_stale"
    );
    assert_eq!(f.count(), 1);
    assert_ne!(
        entorhinal_core::file_id(std::path::Path::new(&root)).as_deref(),
        Some(original.as_str())
    );
    assert_eq!(store.generation().unwrap(), before);
    assert!(store.enumerate(Some("W")).unwrap().workspaces[0]
        .root
        .is_none());
    assert_eq!(f.workspace_approval_rows(), 0);
}

#[tokio::test]
async fn workspace_approval_approve_rejects_a_changed_workspace_root_during_prompt() {
    let f = Fixture::new(false);
    let first = f.workspace_folder("first");
    let second = f.workspace_folder("second");
    let store = f.store();
    let set = |root| entorhinal_core::SetWorkspaceRootRequest {
        workspace_id: "W".into(),
        root: Some(root),
        ..Default::default()
    };
    store.set_workspace_root(set(first)).unwrap();
    let before = store.generation().unwrap();
    let during_prompt = store.clone();
    let changed = second.clone();
    *f.prompt.on_prompt.lock().unwrap() = Some(Box::new(move || {
        during_prompt.set_workspace_root(set(changed)).unwrap();
    }));
    assert_eq!(
        f.call(DIRECT, "approve_workspace_root", json!({"workspaceId":"W"}))
            .await
            .unwrap_err(),
        "operator_approval_stale"
    );
    assert_eq!(
        store.generation().unwrap(),
        before + 1,
        "only the concurrent root change was written"
    );
    assert_eq!(
        store.enumerate(Some("W")).unwrap().workspaces[0].root,
        Some(second)
    );
    assert_eq!(f.workspace_approval_rows(), 0);
}

#[tokio::test]
async fn workspace_approval_clear_and_unapprove_do_not_prompt_or_leave_approval() {
    let f = Fixture::new(false);
    let root = f.workspace_folder("workspace-root");
    let set = json!({"workspaceId":"W","root":root});
    f.call(DIRECT, "set_workspace_root", set.clone())
        .await
        .unwrap();
    let revoked = f
        .call(
            DIRECT,
            "unapprove_workspace_root",
            json!({"workspaceId":"W"}),
        )
        .await
        .unwrap();
    assert_eq!(revoked["root"], root);
    assert_eq!(
        revoked["approval"],
        json!({"state":"unapproved","journalSeq":null})
    );
    assert_eq!(f.count(), 1);
    assert_eq!(f.workspace_approval_rows(), 0);

    let approved = f
        .call(DIRECT, "approve_workspace_root", json!({"workspaceId":"W"}))
        .await
        .unwrap();
    assert_eq!(approved["approval"]["state"], "approved");
    assert_eq!(f.workspace_approval_rows(), 1);
    let cleared = f
        .call(
            DIRECT,
            "set_workspace_root",
            json!({"workspaceId":"W","root":null}),
        )
        .await
        .unwrap();
    assert!(cleared["root"].is_null());
    assert!(cleared.get("approval").is_none());
    assert!(cleared.get("unapprovedReason").is_none());
    assert_eq!(f.count(), 2);
    assert_eq!(f.workspace_approval_rows(), 0);
    assert!(f.store().verify().unwrap().ok);
}

#[tokio::test]
async fn workspace_approval_reserved_prefrontal_refuses_every_write_without_side_effects() {
    let f = Fixture::new(false);
    let old = f.workspace_folder("old");
    let new = f.workspace_folder("new");
    let store = f.store();
    store
        .set_workspace_root(entorhinal_core::SetWorkspaceRootRequest {
            workspace_id: "W".into(),
            root: Some(old.clone()),
            ..Default::default()
        })
        .unwrap();
    let before = store.generation().unwrap();
    for (op, req) in [
        ("set_workspace_root", json!({"workspaceId":"W","root":new})),
        ("set_workspace_root", json!({"workspaceId":"W","root":null})),
        ("approve_workspace_root", json!({"workspaceId":"W"})),
    ] {
        assert_eq!(
            f.call(CORE, op, req).await.unwrap_err(),
            "write_not_permitted",
            "{op}"
        );
    }
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(f.workspace_approval_rows(), 0);
    assert_eq!(
        store.enumerate(Some("W")).unwrap().workspaces[0].root,
        Some(old)
    );
    f.call(DIRECT, "approve_workspace_root", json!({"workspaceId":"W"}))
        .await
        .unwrap();
    let before = store.generation().unwrap();
    assert_eq!(
        f.call(CORE, "unapprove_workspace_root", json!({"workspaceId":"W"}))
            .await
            .unwrap_err(),
        "write_not_permitted"
    );
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(f.workspace_approval_rows(), 1);
    assert_eq!(f.count(), 1);
}

#[tokio::test]
async fn folder_presence_approve_refuses_without_presence_and_plain_execute() {
    let f = Fixture::with_prompt(Prompt {
        unavailable: true,
        ..Default::default()
    });
    let root = f.unapproved_root();
    let store = f.store();
    let before = store.generation().unwrap();
    let params = json!({"root": root});
    let unserved = f.handler.execute(
        crate::WireRequest {
            method: "approve_root".into(),
            params: params.clone(),
        },
        DIRECT,
    );
    assert_eq!(unserved.unwrap_err().code, "operator_presence_unavailable");
    assert_eq!(f.count(), 0);
    assert_eq!(
        f.call(DIRECT, "approve_root", params).await.unwrap_err(),
        "operator_presence_unavailable"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &root), "unapproved");
}

#[tokio::test]
async fn folder_presence_approve_checks_then_commits_and_unapprove_does_not_prompt() {
    let f = Fixture::new(false);
    let unregistered = f.checkout("unregistered");
    let root = f.unapproved_root();
    let store = f.store();
    let before = store.generation().unwrap();
    assert_eq!(
        f.call(DIRECT, "approve_root", json!({"root":unregistered}))
            .await
            .unwrap_err(),
        "root_not_bound"
    );
    assert_eq!(f.count(), 0);
    assert_eq!(store.generation().unwrap(), before);
    f.call(DIRECT, "approve_project", json!({"canonicalRoot":root}))
        .await
        .unwrap();
    let summaries = f.prompt.summaries.lock().unwrap().clone();
    assert_eq!(summaries.len(), 1);
    assert!(summaries[0].contains("Project") && summaries[0].contains(&root));
    assert_eq!(store.generation().unwrap(), before + 1);
    assert_eq!(approval(&store, &root), "approved");
    f.call(DIRECT, "unapprove_project", json!({"canonicalRoot":root}))
        .await
        .unwrap();
    assert_eq!(f.count(), 1);
    assert_eq!(approval(&store, &root), "unapproved");
}

fn batch_params(roots: &[(&str, &str)]) -> Value {
    json!({
        "canonicalRoots": roots.iter().map(|(_, root)| root).collect::<Vec<_>>(),
        "expected": roots.iter().map(|(project, root)| json!({"projectId":project,"root":root})).collect::<Vec<_>>(),
        "requestKey":"batch"
    })
}

#[tokio::test]
async fn folder_presence_batch_approves_three_projects_and_replays_retry() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let c = f.batch_project("C", true);
    let store = f.store();
    let before = store.generation().unwrap();
    let mut params = batch_params(&[("A", &a), ("B", &b), ("C", &c)]);
    params["canonicalRoots"]
        .as_array_mut()
        .unwrap()
        .push(json!(&a));
    let reply = f.approve_projects(params.clone()).await.unwrap();
    assert_eq!(
        reply,
        json!({
            "approved":[{"projectId":"A","name":"Project A","roots":[a]},
                        {"projectId":"B","name":"Project B","roots":[b]},
                        {"projectId":"C","name":"Project C","roots":[c]}],
            "alreadyApproved":[], "generation":before + 4,
        })
    );
    assert_eq!(
        *f.prompt.summaries.lock().unwrap(),
        ["Approve 3 project folders listed in the terminal"]
    );
    for root in [&a, &b, &c] {
        assert_eq!(approval(&store, root), "approved");
    }
    assert_eq!(f.approve_projects(params).await.unwrap(), reply);
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before + 4);
    assert!(store.verify().unwrap().ok);
    assert!(store.rebuild().unwrap().replay.ok);
    assert_eq!(approval(&store, &b), "approved");
}

#[tokio::test]
async fn folder_presence_batch_skips_approved_projects_and_roots_and_remains_local_on_hub() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let second = f.checkout("B-second");
    let third = f.checkout("B-third");
    let store = f.store();
    for root in [&second, &third] {
        store
            .add_root(entorhinal_core::AddRootRequest {
                project_id: "B".into(),
                root: root.clone(),
                label: None,
                actor: None,
            })
            .unwrap();
        store.bind_root(root, "fixture").unwrap();
    }
    for root in [&a, &b] {
        store.approve_root(root, "fixture").unwrap();
    }
    store
        .apply_entry("identity_log.enable", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled', hub_log_base=0, hub_ready=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    let before = store.generation().unwrap();
    let mut params = batch_params(&[("B", &second), ("B", &third)]);
    params["canonicalRoots"] = json!([a, b]);
    let reply = f.approve_projects(params.clone()).await.unwrap();
    assert_eq!(
        reply,
        json!({"approved":[{"projectId":"B","name":"Project B","roots":[second,third]}],
        "alreadyApproved":[{"projectId":"A","name":"Project A"}],"generation":before+3})
    );
    assert_eq!(
        *f.prompt.summaries.lock().unwrap(),
        ["Approve 2 project folders listed in the terminal"]
    );
    let shared: i64 = rusqlite::Connection::open(f.dir.join("store.db"))
        .unwrap()
        .query_row(
            "SELECT COUNT(*) FROM registry_journal WHERE seq>?1 AND stream='shared'",
            [before],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(shared, 0);
    assert_eq!(f.approve_projects(params.clone()).await.unwrap(), reply);
    params["expected"] = json!([]);
    params["requestKey"] = json!("nothing-to-do");
    let noop = json!({"approved":[],"alreadyApproved":[{"projectId":"A","name":"Project A"},
        {"projectId":"B","name":"Project B"}],"generation":before+3});
    assert_eq!(f.approve_projects(params.clone()).await.unwrap(), noop);
    assert_eq!(f.approve_projects(params).await.unwrap(), noop);
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before + 3);
}

#[tokio::test]
async fn folder_presence_batch_names_all_blocked_folders_without_prompting() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", false);
    let c = f.batch_project("C", false);
    std::fs::remove_dir_all(std::path::Path::new(&c).join(".git")).unwrap();
    let store = f.store();
    let before = store.generation().unwrap();
    let mut params = batch_params(&[("A", &a)]);
    params["canonicalRoots"] = json!([a, b, c]);
    let HandlerOutcome::Error { code, message, .. } =
        f.outcome(DIRECT, "approve_projects", params).await
    else {
        panic!("expected a refusal")
    };
    assert_eq!(code, "root_not_bound");
    // Refusals quote folders as typed, not Debug-escaped, so Windows
    // backslashes appear once.
    assert!(
        message.contains(&format!("\"{b}\" is unbound")),
        "{message}"
    );
    assert!(
        message.contains(&format!("\"{c}\" is unverifiable")),
        "{message}"
    );
    assert_eq!(f.count(), 0);
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &a), "unapproved");
}

#[tokio::test]
async fn folder_presence_batch_decline_writes_nothing() {
    let f = Fixture::new(true);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let store = f.store();
    let before = store.generation().unwrap();
    assert_eq!(
        f.approve_projects(batch_params(&[("A", &a), ("B", &b)]))
            .await
            .unwrap_err(),
        "operator_declined"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &a), "unapproved");
    assert_eq!(approval(&store, &b), "unapproved");
}

#[tokio::test]
async fn folder_presence_batch_stale_in_one_project_writes_nothing_in_either() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let store = f.store();
    let before = store.generation().unwrap();
    let marker = std::path::Path::new(&b).join(".git/cortexkit-root-incarnation");
    *f.prompt.on_prompt.lock().unwrap() =
        Some(Box::new(move || std::fs::remove_file(marker).unwrap()));
    assert_eq!(
        f.approve_projects(batch_params(&[("A", &a), ("B", &b)]))
            .await
            .unwrap_err(),
        "operator_approval_stale"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &a), "unapproved");
    assert_eq!(approval(&store, &b), "unapproved");
}

#[tokio::test]
async fn folder_presence_batch_root_added_after_preview_is_refused_before_prompt() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let params = batch_params(&[("A", &a), ("B", &b)]);
    let added = f.checkout("B-added");
    let store = f.store();
    store
        .add_root(entorhinal_core::AddRootRequest {
            project_id: "B".into(),
            root: added.clone(),
            label: None,
            actor: None,
        })
        .unwrap();
    store.bind_root(&added, "fixture").unwrap();
    let before = store.generation().unwrap();
    assert_eq!(
        f.approve_projects(params).await.unwrap_err(),
        "operator_approval_stale"
    );
    assert_eq!(f.count(), 0);
    assert_eq!(store.generation().unwrap(), before);
    for root in [&a, &b, &added] {
        assert_eq!(approval(&store, root), "unapproved");
    }
}

#[tokio::test]
async fn folder_presence_batch_last_approval_failure_rolls_back_every_project() {
    let f = Fixture::new(false);
    let a = f.batch_project("A", true);
    let b = f.batch_project("B", true);
    let c = f.batch_project("C", true);
    let store = f.store();
    let before = store.generation().unwrap();
    // Fail only after two approvals have been inserted. If each project had
    // its own transaction, those first two approvals would survive the error.
    rusqlite::Connection::open(f.dir.join("store.db"))
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_third_approval BEFORE INSERT ON root_approval
         WHEN (SELECT COUNT(*) FROM root_approval) = 2
         BEGIN SELECT RAISE(ABORT, 'test third approval failure'); END;",
        )
        .unwrap();
    assert_eq!(
        f.approve_projects(batch_params(&[("A", &a), ("B", &b), ("C", &c)]))
            .await
            .unwrap_err(),
        "storage_error"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    for root in [&a, &b, &c] {
        assert_eq!(approval(&store, root), "unapproved");
    }
}

#[tokio::test]
async fn folder_presence_replaced_checkout_is_stale_without_journal_changes() {
    let f = Fixture::new(false);
    let root = f.unapproved_root();
    let store = f.store();
    let before = store.generation().unwrap();
    let marker = std::path::Path::new(&root).join(".git/cortexkit-root-incarnation");
    let original = std::fs::read_to_string(&marker).unwrap();
    let replacement = if original.starts_with('a') { 'b' } else { 'a' };
    *f.prompt.on_prompt.lock().unwrap() = Some(Box::new(move || {
        std::fs::write(marker, format!("{}\n", replacement.to_string().repeat(64))).unwrap();
    }));
    assert_eq!(
        f.call(DIRECT, "approve_root", json!({"root":root}))
            .await
            .unwrap_err(),
        "operator_approval_stale"
    );
    assert_eq!(f.count(), 1);
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &root), "unapproved");
}

#[tokio::test]
async fn folder_presence_register_approval_is_one_transaction() {
    let f = Fixture::new(false);
    let root = f.checkout("new");
    let store = f.store();
    let before = store.generation().unwrap();
    let params = json!({"projectId":"New","name":"New","roots":[root],"requestKey":"new"});
    let reader = store.clone();
    *f.prompt.on_prompt.lock().unwrap() = Some(Box::new(move || {
        assert_eq!(reader.generation().unwrap(), before);
    }));
    let reply = f.call(DIRECT, "register", params.clone()).await.unwrap();
    assert_eq!(f.count(), 1);
    assert_eq!(reply["projectId"], "New");
    assert_eq!(
        reply["folderApprovals"],
        json!([{"root":root,"state":"approved"}])
    );
    assert_eq!(approval(&store, &root), "approved");
    assert_eq!(
        store
            .journal_tail(before, 10)
            .unwrap()
            .entries
            .into_iter()
            .map(|row| row.op)
            .collect::<Vec<_>>(),
        ["register", "bind_root", "approve_root"]
    );
    assert_eq!(reply["generation"], store.generation().unwrap());
    assert_eq!(f.call(DIRECT, "register", params).await.unwrap(), reply);
    assert_eq!(f.count(), 1);
    assert!(store.verify().unwrap().ok);
    store.rebuild().unwrap();
    assert_eq!(approval(&store, &root), "approved");

    let failed = f.checkout("failed");
    // Make approval fail after registration wrote its provisional row. A
    // separate registration transaction would leave that row committed.
    rusqlite::Connection::open(f.dir.join("store.db")).unwrap().execute_batch(
        "CREATE TRIGGER fail_approval BEFORE INSERT ON root_approval BEGIN SELECT RAISE(ABORT, 'test approval failure'); END;",
    ).unwrap();
    let before = store.generation().unwrap();
    assert_eq!(
        f.call(
            DIRECT,
            "register",
            json!({"projectId":"Failed","name":"Failed","roots":[failed]})
        )
        .await
        .unwrap_err(),
        "storage_error"
    );
    assert_eq!(store.generation().unwrap(), before);
    assert_ne!(store.trust(&failed).unwrap().project_id, "Failed");
}

#[tokio::test]
async fn folder_presence_register_non_git_is_confirmed_but_unapproved() {
    let f = Fixture::new(false);
    let root = f.dir.join("plain");
    std::fs::create_dir(&root).unwrap();
    let root =
        entorhinal_core::RegistryStore::canonical_mutation_root(root.to_str().unwrap()).unwrap();
    let store = f.store();
    let before = store.generation().unwrap();
    let reply = f
        .call(DIRECT, "register", json!({"name":"Plain","roots":[root]}))
        .await
        .unwrap();
    assert_eq!(f.count(), 1);
    assert_eq!(reply["folderApprovals"][0]["root"], root);
    assert_eq!(reply["folderApprovals"][0]["state"], "unapproved");
    assert!(reply["folderApprovals"][0]["reason"]
        .as_str()
        .is_some_and(|reason| !reason.is_empty()));
    assert_eq!(approval(&store, &root), "unapproved");
    assert_eq!(store.generation().unwrap(), before + 1);
}

#[tokio::test]
async fn folder_presence_prefrontal_mutation_admission_preserves_derived_parent() {
    let f = Fixture::new(false);
    let root = f.unapproved_root();
    let store = f.store();
    let before = store.generation().unwrap();
    for method in [
        "approve_root",
        "unapprove_root",
        "approve_project",
        "approve_projects",
        "unapprove_project",
        "register",
        "attach_root",
        "add_root",
        "set_workspace_root",
    ] {
        assert_eq!(
            f.call(CORE, method, json!({"root":root,"canonicalRoot":root}))
                .await
                .unwrap_err(),
            "write_not_permitted",
            "{method}"
        );
    }
    assert_eq!(store.generation().unwrap(), before);
    assert_eq!(approval(&store, &root), "unapproved");
    let container = f.dir.join("workers");
    std::fs::create_dir(&container).unwrap();
    let container = crate::tests::canonical_test_path(container);
    let record = store
        .trust(&root)
        .unwrap()
        .root_fields
        .unwrap()
        .root_records
        .remove(0);
    let params = json!({"projectId":"P","root":root,"container":container,"registrationEpoch":record.registration_epoch,"incarnation":record.incarnation.unwrap().value});
    assert_eq!(
        f.call(CORE, "attach_derived_parent", params.clone())
            .await
            .unwrap_err(),
        "binding_unapproved"
    );
    assert_eq!(store.generation().unwrap(), before);
    store.approve_root(&root, "fixture").unwrap();
    let before = store.generation().unwrap();
    f.call(CORE, "attach_derived_parent", params).await.unwrap();
    assert_eq!(store.generation().unwrap(), before + 1);
    assert_eq!(f.count(), 0);
}
fn document(revision: i64) -> Value {
    json!({"workspace_id":"W","expected_revision":revision,"charter":"Purpose","document":"Policy","request_key":format!("doc-{revision}")})
}
#[tokio::test]
async fn memory_direct_domain_and_document_writes_require_touch_id() {
    for decline in [true, false] {
        let f = Fixture::new(decline);
        let req = json!({"project_id":"P","domain":"@example/software","request_key":"domain"});
        let result = f.call(DIRECT, "project.set_domain", req.clone()).await;
        assert_eq!(f.count(), 1);
        assert_eq!(
            f.prompt.summaries.lock().unwrap()[0],
            "set domain of project Project to @example/software"
        );
        if decline {
            assert_eq!(result.unwrap_err(), "operator_declined");
        } else {
            assert_eq!(result.unwrap()["domain"], "@example/software");
            f.call(DIRECT, "project.set_domain", req).await.unwrap();
            assert_eq!(f.count(), 1);
        }
        let result = f.call(DIRECT, "workspace.set_document", document(0)).await;
        assert_eq!(f.count(), 2);
        let summary = f.prompt.summaries.lock().unwrap()[1].clone();
        assert!(
            summary.contains("workspace W revision 1 (charter 7 bytes, document 6 bytes, sha256 ")
        );
        assert!(summary.chars().count() <= 200);
        if decline {
            assert_eq!(result.unwrap_err(), "operator_declined");
        } else {
            let accepted = result.unwrap();
            assert_eq!(accepted["revision"], 1);
            assert_eq!(accepted["accepted_via"], "direct");
            f.call(DIRECT, "workspace.set_document", document(0))
                .await
                .unwrap();
            assert_eq!(f.count(), 2);
            let mut stale = document(0);
            stale["request_key"] = json!("stale");
            assert_eq!(
                f.call(DIRECT, "workspace.set_document", stale)
                    .await
                    .unwrap_err(),
                "revision_conflict"
            );
            assert_eq!(f.count(), 2);
            assert!(f.handler.with_store(|s| Ok(s.verify()?.ok)).unwrap());
        }
    }
}
#[tokio::test]
async fn memory_documents_refuse_every_non_direct_principal_and_flow_before_decode() {
    let f = Fixture::new(false);
    for (n, principal) in [
        None,
        Some(reserved(WRITER_MODULE)),
        Some(reserved("other")),
        Some(Principal::Direct),
    ]
    .into_iter()
    .enumerate()
    {
        let key = (510 + n as u16, 1);
        for flow in [false, true] {
            let stamp = flow_stamp(Some("flow"));
            f.handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal.clone(), flow.then_some(&stamp)),
            );
            let error = f
                .call(key, "workspace.set_document", Value::Null)
                .await
                .unwrap_err();
            assert_eq!(
                error,
                if flow {
                    "flow_scope_not_admitted"
                } else if matches!(principal, Some(Principal::Direct)) {
                    "invalid_params"
                } else {
                    "direct_caller_required"
                }
            );
            if flow {
                assert_eq!(
                    f.call(key, "project.set_domain", Value::Null)
                        .await
                        .unwrap_err(),
                    "flow_scope_not_admitted"
                );
            }
        }
    }
    assert_eq!(f.count(), 0);
    f.call(
        CORE,
        "project.set_domain",
        json!({"project_id":"P","domain":null,"request_key":"clear"}),
    )
    .await
    .unwrap();
    assert_eq!(f.count(), 0);
}
#[tokio::test]
async fn memory_validation_and_long_summaries_refuse_before_prompt() {
    let f = Fixture::new(false);
    for (field, value, code) in [
        ("document", json!("x".repeat(65537)), "document_too_large"),
        ("charter", json!("\0"), "document_invalid_text"),
        ("workspace_id", json!("unknown"), "workspace_unknown"),
    ] {
        let mut req = document(0);
        req[field] = value;
        assert_eq!(
            f.call(DIRECT, "workspace.set_document", req)
                .await
                .unwrap_err(),
            code
        );
    }
    f.handler
        .with_store(|s| {
            s.register(entorhinal_core::RegisterRequest {
                project_id: Some("P".into()),
                name: "x".repeat(200),
                ..Default::default()
            })
            .map(|_| ())
        })
        .unwrap();
    assert_eq!(
        f.call(
            DIRECT,
            "project.set_domain",
            json!({"project_id":"P","domain":"@example/software","request_key":"long"})
        )
        .await
        .unwrap_err(),
        "operator_summary_too_long"
    );
    assert_eq!(f.count(), 0);
}

#[tokio::test]
async fn memory_context_reads_are_open_to_every_principal_including_flows() {
    let f = Fixture::new(false);
    for (n, principal) in [
        None,
        Some(Principal::Direct),
        Some(reserved(WRITER_MODULE)),
        Some(reserved("other")),
    ]
    .into_iter()
    .enumerate()
    {
        let key = (600 + n as u16, 1);
        for flow in [false, true] {
            let stamp = flow_stamp(Some("read-flow"));
            f.handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal.clone(), flow.then_some(&stamp)),
            );
            let folder = crate::tests::canonical_test_path(&f.dir)
                .to_string_lossy()
                .into_owned();
            assert_eq!(
                f.call(key, "context.project", json!({"folder":folder}))
                    .await
                    .unwrap()["project_id"],
                Value::Null
            );
            assert_eq!(
                f.call(key, "workspace.document", json!({"workspace_id":"W"}))
                    .await
                    .unwrap_err(),
                "revision_unknown"
            );
            assert_eq!(
                f.call(key, "context.session", json!({"agent_id":"unknown"}))
                    .await
                    .unwrap_err(),
                "unknown_agent"
            );
        }
    }
    assert_eq!(f.count(), 0);
}

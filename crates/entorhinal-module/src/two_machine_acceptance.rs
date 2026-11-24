//! Serves local registry workloads through admitted routes and checks the journal directly.

use super::*;
use crate::fake_log::{FailConnector, FakeLog};
use crate::log_client::LogConnector;
use rusqlite::{types::Value as Cell, Connection, OpenFlags};
use std::{future::Future, path::Path};

const DIRECT: RouteKey = (90, 1);
const CORE: RouteKey = (91, 1);
const SHARED: &[&str] = &[
    "placement_fact",
    "placement_state",
    "project",
    "workspace",
    "workspace_document",
    "project_workspace",
    "project_alias",
    "agent",
    "agent_name_claim",
    "project_root_key",
    "member_name",
    "member_box",
    "catalog_pin_set",
    "catalog_pin",
];
const LOCAL: &[&str] = &[
    "project_root",
    "derived_root_parent",
    "root_binding",
    "retired_binding",
    "root_approval",
    "root_owned_remotes",
    "workspace_root",
];

// A paused Tokio clock must not jump to network deadlines while SQLite is on
// its worker thread. Keep this task runnable, with no real-time measurements.
async fn drive<F: Future>(future: F) -> F::Output {
    let now = tokio::time::Instant::now();
    tokio::pin!(future);
    let result = loop {
        tokio::select! {
            result = &mut future => break result,
            () = tokio::task::yield_now() => {},
        }
    };
    assert_eq!(tokio::time::Instant::now(), now);
    result
}

struct Machine {
    dir: PathBuf,
    descriptor: StorageDescriptor,
    handler: ProjectsHandler,
}

impl Machine {
    fn new(label: &str, connector: Arc<dyn LogConnector>) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let handler = Self::open(&descriptor, connector);
        Self {
            dir: crate::tests::canonical_test_path(dir),
            descriptor,
            handler,
        }
    }

    fn open(descriptor: &StorageDescriptor, connector: Arc<dyn LogConnector>) -> ProjectsHandler {
        let handler = ProjectsHandler::with_log_connector("acceptance".into(), || 700, connector);
        let mut store = RegistryStore::open(descriptor).unwrap();
        store.set_root_records(true);
        install_store(
            &handler.store,
            &handler.health,
            &handler.catalog_sql,
            Ok(store),
        );
        for (key, principal) in [
            (DIRECT, Principal::Direct),
            (
                CORE,
                Principal::Reserved {
                    module_id: WRITER_MODULE.into(),
                },
            ),
        ] {
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
        }
        handler
    }

    async fn restart(&mut self, connector: Arc<dyn LogConnector>) {
        crate::tests::stop_store_tasks(&self.handler).await;
        self.handler.store.lock().unwrap().take();
        self.handler = Self::open(&self.descriptor, connector);
    }

    fn store(&self) -> RegistryStore {
        self.handler.with_store(|s| Ok(s.clone())).unwrap()
    }

    fn conn(&self) -> Connection {
        let StorageBackend::Sqlite { path } = &self.descriptor.backend else {
            unreachable!()
        };
        Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }

    fn query(&self, sql: &str) -> Vec<Vec<Cell>> {
        let conn = self.conn();
        let mut stmt = conn.prepare(sql).unwrap();
        let columns = stmt.column_count();
        let mut rows = stmt
            .query_map([], |r| (0..columns).map(|i| r.get(i)).collect())
            .unwrap()
            .collect::<rusqlite::Result<Vec<Vec<Cell>>>>()
            .unwrap();
        // All columns participate in sorting, including composite primary keys.
        rows.sort_by_key(|row| format!("{row:?}"));
        rows
    }

    fn rows(&self, table: &str) -> Vec<Vec<Cell>> {
        self.query(&format!("SELECT * FROM {table}"))
    }

    fn shared(&self) -> BTreeMap<String, Vec<Vec<Cell>>> {
        SHARED
            .iter()
            .map(|table| {
                let sql = if *table == "project_alias" {
                    "SELECT * FROM project_alias WHERE substr(old_id,1,13) <> 'pj-implicit1-'"
                        .into()
                } else {
                    format!("SELECT * FROM {table}")
                };
                ((*table).into(), self.query(&sql))
            })
            .collect()
    }

    fn local(&self) -> BTreeMap<String, Vec<Vec<Cell>>> {
        LOCAL
            .iter()
            .map(|table| ((*table).into(), self.rows(table)))
            .chain(std::iter::once((
                "implicit_aliases".into(),
                self.query(
                    "SELECT * FROM project_alias WHERE substr(old_id,1,13) = 'pj-implicit1-'",
                ),
            )))
            .collect()
    }

    async fn bytes_on(
        &self,
        route: RouteKey,
        method: &str,
        params: Value,
    ) -> Result<Vec<u8>, String> {
        let body = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
        match drive(self.handler.handle_served_request(&body, route)).await {
            HandlerOutcome::Response(bytes) => Ok(bytes),
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                Err(code)
            }
            HandlerOutcome::Streamed => panic!("unexpected streamed reply"),
        }
    }

    async fn call(&self, method: &str, params: Value) -> Result<Value, String> {
        let route = if method.starts_with("agent.") {
            CORE
        } else {
            DIRECT
        };
        self.bytes_on(route, method, params)
            .await
            .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
    }

    async fn register(&self, id: &str, root: &str, workspace: Option<&str>) -> Value {
        self.call(
            "register",
            json!({"projectId":id,"name":id,"roots":[root],"workspaceId":workspace}),
        )
        .await
        .unwrap()
    }

    fn repo(&self, name: &str, remotes: &[(&str, &str)]) -> String {
        let path = self.dir.join(name);
        repo(&path, remotes);
        crate::tests::canonical_test_path(path)
            .to_string_lossy()
            .into_owned()
    }

    async fn approve(&self, root: &str) -> Value {
        self.call("approve_root", json!({"root":root}))
            .await
            .unwrap();
        let reply = self
            .call("resolve", json!({"canonicalRoot":root}))
            .await
            .unwrap();
        let record = &reply["rootRecords"][0];
        assert_eq!(record["approval"]["state"], "approved");
        json!({"root":root,"incarnation":record["incarnation"]["value"],"registrationEpoch":record["registrationEpoch"]})
    }

    async fn clean_rebuild(&self) {
        let shared = self.shared();
        let local = self.local();
        let members = self.rows("workspace_member");
        let journal = self.rows("registry_journal");
        let report = self.call("verify", json!({})).await.unwrap();
        assert_eq!(report["replay"]["ok"], true, "{report}");
        let report = self.call("rebuild", json!({})).await.unwrap();
        assert_eq!(report["replay"]["ok"], true, "{report}");
        assert_eq!(self.shared(), shared);
        assert_eq!(self.local(), local);
        assert_eq!(self.rows("workspace_member"), members);
        assert_eq!(self.rows("registry_journal"), journal);
    }
}

impl Drop for Machine {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        let removed = std::fs::remove_dir_all(&self.dir);
        // A second panic while a test is already failing aborts the whole test
        // binary (on Windows, 0xc0000409), hiding every later result. So while
        // unwinding, only report the leftover folder; otherwise fail loudly,
        // since a folder Windows can't delete means a store handle outlived
        // the store.
        if std::thread::panicking() {
            if let Err(error) = removed {
                eprintln!("scratch folder {} not removed: {error}", self.dir.display());
            }
        } else {
            removed.unwrap();
        }
    }
}

fn git(path: &Path, args: &[&str]) {
    let output = std::process::Command::new("git")
        .current_dir(path)
        .args(args)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
}

fn repo(path: &Path, remotes: &[(&str, &str)]) {
    std::fs::create_dir_all(path).unwrap();
    git(path, &["init", "-q"]);
    for (name, key) in remotes {
        git(
            path,
            &[
                "remote",
                "add",
                name,
                &format!("https://github.com/{key}.git"),
            ],
        );
    }
}

#[tokio::test(start_paused = true)]
async fn hub_cutover_restart_never_calls_engram() {
    let fail = Arc::new(FailConnector::default());
    let mut m = Machine::new("hub-cutover-restart", fail.clone());
    m.store().role_read("connector_home").unwrap();
    m.store().apply_entry("fixture.cutover", "{}", "test", None, |tx| {
        tx.execute("UPDATE identity_log_state SET state='enabled',hub_log_base=17,hub_ready=1,last_applied_position=17,last_seen_head=17", [])?;
        tx.execute("INSERT INTO self_identity VALUES(1,?1,1)", ["ab".repeat(16)])?;
        Ok(())
    }).unwrap();
    m.restart(fail.clone()).await;
    m.handler.record_client = crate::record_client::RecordClient::new(fail.clone());
    assert!(m.handler.hub_backend_selected());
    m.handler.start_catch_up();
    m.handler.start_move_driver();
    m.handler.move_wake.notify_one();
    for _ in 0..3 {
        tokio::time::advance(Duration::from_secs(30)).await;
        for _ in 0..32 {
            tokio::task::yield_now().await;
        }
    }
    crate::tests::stop_store_tasks(&m.handler).await;
    assert_eq!(fail.calls(), 0);
}

#[tokio::test(start_paused = true)]
async fn disabled_workload_keeps_local_journal_and_never_connects() {
    let fail = Arc::new(FailConnector::default());
    let m = Machine::new("disabled-acceptance", fail.clone());
    let root = m.repo("remote", &[("origin", "disabled/one")]);
    let second = m.repo("second", &[("origin", "disabled/two")]);
    let label = m.repo("label", &[]);
    m.register("P", &root, Some("W")).await;
    m.call(
        "register",
        json!({"projectId":"L","name":"Label","roots":[label],"label":"local"}),
    )
    .await
    .unwrap();
    m.call("add_root", json!({"projectId":"P","root":second}))
        .await
        .unwrap();
    let binding = m.approve(&root).await;
    let container = m.dir.join("workers");
    std::fs::create_dir_all(&container).unwrap();
    m.call("attach_derived_parent", json!({"projectId":"P","root":root,"incarnation":binding["incarnation"],"registrationEpoch":binding["registrationEpoch"],"container":container})).await.unwrap();
    m.call(
        "set_owned_remotes",
        json!({"root":root,"remotes":["origin"],"request_key":"owned"}),
    )
    .await
    .unwrap();
    m.call("unapprove_root", json!({"root":root}))
        .await
        .unwrap();
    m.call(
        "assign_workspace",
        json!({"projectId":"L","workspaceId":"W"}),
    )
    .await
    .unwrap();
    m.call(
        "set_workspace_root",
        json!({"workspaceId":"W","root":m.dir}),
    )
    .await
    .unwrap();
    m.call("remove_root", json!({"projectId":"P","root":second}))
        .await
        .unwrap();
    assert_eq!(
        m.call("remove_root", json!({"projectId":"P","root":root}))
            .await
            .unwrap_err(),
        "last_root"
    );
    let before = m.rows("registry_journal");
    for (path, kind, key) in [
        (&root, "remote", "disabled/one"),
        (&label, "label", "local"),
        (&root, "remote", "missing/key"),
    ] {
        assert_eq!(
            m.call(
                "attach_root",
                json!({"path":path,"projectId":"P","label":key})
            )
            .await
            .unwrap_err(),
            "identity_log_disabled"
        );
        assert_eq!(
            m.call(
                "resolve_root_key",
                json!({"projectId":"P","kind":kind,"rootKey":key})
            )
            .await
            .unwrap_err(),
            "identity_log_disabled"
        );
    }
    assert_eq!(m.rows("registry_journal"), before);
    m.call("remove", json!({"projectId":"L"})).await.unwrap();
    m.call("remove", json!({"workspaceId":"W"})).await.unwrap();
    let implicit_root = m.repo("implicit", &[("origin", "disabled/implicit")]);
    m.call("upgrade_implicit", json!({"implicitId":entorhinal_core::implicit_project_id(&implicit_root),"projectId":"U","name":"Upgraded","roots":[implicit_root]})).await.unwrap();
    let seeded = m.repo("seeded", &[("origin", "disabled/seeded")]);
    m.call("seed_import", json!({"source":"mc","exclude_home_scoped":false,"payload":{"pairs":[{"canonicalRoot":seeded,"mcIdentity":"Seed","name":"Seed"}]}})).await.unwrap();
    let snapshot = m.dir.join("empty-agents.db");
    Connection::open(&snapshot)
        .unwrap()
        .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
        .unwrap();
    m.call(
        "agent.import",
        json!({"snapshot_path":snapshot,"request_key":"disabled-import"}),
    )
    .await
    .unwrap();
    let created = m.call("agent.create", json!({"role":"assistant","name":"Legacy","tag":"helper","request_key":"disabled-create"})).await.unwrap();
    let id = &created["agent"]["agent_id"];
    m.call(
        "agent.rename",
        json!({"agent_id":id,"name":"Legacy Renamed","request_key":"disabled-rename"}),
    )
    .await
    .unwrap();
    m.call(
        "agent.update_tag",
        json!({"agent_id":id,"tag":"changed","request_key":"disabled-tag"}),
    )
    .await
    .unwrap();
    m.call(
        "agent.set_labels",
        json!({"agent":id,"labels":["local"],"request_key":"disabled-labels"}),
    )
    .await
    .unwrap();
    m.call(
        "agent.dispose",
        json!({"agent_id":id,"request_key":"disabled-dispose"}),
    )
    .await
    .unwrap();
    assert!(m.rows("project_root_key").is_empty());
    assert!(m.query("SELECT seq FROM registry_journal WHERE op LIKE 'root_key.%' OR stream <> 'local' OR origin <> 'here' OR entry_id IS NOT NULL OR log_position IS NOT NULL OR entry IS NOT NULL").is_empty());
    let ops = m.query("SELECT op FROM registry_journal");
    for op in [
        "register",
        "bind_root",
        "add_root",
        "approve_root",
        "attach_derived_parent",
        "set_owned_remotes",
        "unapprove_root",
        "assign_workspace",
        "set_workspace_root",
        "remove_root",
        "remove",
        "upgrade_implicit",
        "seed_import",
        "agent.cutover",
        "agent.create",
        "agent.rename",
        "agent.update_tag",
        "agent.set_labels",
        "agent.dispose",
    ] {
        assert!(
            ops.contains(&vec![Cell::Text(op.into())]),
            "missing legacy op {op}"
        );
    }
    assert_eq!(fail.calls(), 0);
    assert_eq!(m.store().identity_log_status().unwrap().state, "disabled");
    m.clean_rebuild().await;
}

mod moves {
    mod agent_data_request_vectors;
    mod basal_samples;
    mod move_refs;
    mod occipital_samples;
}

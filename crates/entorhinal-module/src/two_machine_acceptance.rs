//! Acceptance through the serving path: independent stores, admitted routes,
//! and one serialized wire log. SQL assertions inspect the persisted cells,
//! independently of the replication encoder and replay comparator.

use super::*;
use crate::fake_log::{FailConnector, FakeLog};
use crate::log_client::{LogConnector, LogTransport, TransportError};
use rusqlite::{types::Value as Cell, Connection, OpenFlags};
use std::{future::Future, path::Path, sync::atomic::AtomicUsize};

#[path = "member_box_acceptance.rs"]
mod member_boxes;

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
        Self::open_with_record(descriptor, connector, None)
    }

    fn new_with_record(
        label: &str,
        connector: Arc<dyn LogConnector>,
        record: crate::record_client::RecordClient,
    ) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let handler = Self::open_with_record(&descriptor, connector, Some(record));
        Self {
            dir: crate::tests::canonical_test_path(dir),
            descriptor,
            handler,
        }
    }

    fn open_with_record(
        descriptor: &StorageDescriptor,
        connector: Arc<dyn LogConnector>,
        record: Option<crate::record_client::RecordClient>,
    ) -> ProjectsHandler {
        let mut handler =
            ProjectsHandler::with_log_connector("acceptance".into(), || 700, connector);
        if let Some(record) = record {
            handler.record_client = record;
        }
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

    /// Stops the old handler's background tasks before reopening the store.
    /// Claim refresh reads the store on blocking threads once sharing is on,
    /// and a pass left running would keep the database open, so Windows could
    /// not delete the scratch folder at the end of the test.
    async fn restart(&mut self, connector: Arc<dyn LogConnector>) {
        crate::tests::stop_store_tasks(&self.handler).await;
        self.handler.store.lock().unwrap().take();
        self.handler = Self::open(&self.descriptor, connector);
    }

    async fn restart_legacy(&mut self, connector: Arc<dyn LogConnector>) {
        crate::tests::allow_legacy_store(&self.descriptor);
        self.restart(connector).await;
    }

    fn restore(
        label: &str,
        snapshot: &Path,
        connector: Arc<dyn LogConnector>,
        prepare_open: impl FnOnce(&StorageDescriptor),
    ) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            unreachable!()
        };
        // The snapshot is a closed SQLite backup, not an open store whose WAL
        // might be missing. Restore it before opening the new handler.
        std::fs::copy(snapshot, path).unwrap();
        prepare_open(&descriptor);
        let handler = Self::open(&descriptor, connector);
        Self {
            dir: crate::tests::canonical_test_path(dir),
            descriptor,
            handler,
        }
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

    async fn enable(&self) {
        assert_eq!(
            self.call("identity_log.enable", json!({})).await.unwrap()["state"],
            "enabled"
        );
    }
    async fn enable_without_agents(&self) {
        assert_eq!(
            self.call("identity_log.enable", json!({"without_agents":true}))
                .await
                .unwrap()["state"],
            "enabled"
        );
    }

    async fn catch_up(&self) {
        let _writer = self.handler.writer.lock().await;
        drive(self.handler.catch_up(
            &self.store(),
            tokio::time::Instant::now() + Duration::from_secs(25),
        ))
        .await
        .unwrap();
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

async fn pair(label: &str, log: Arc<dyn LogConnector>) -> (Machine, Machine) {
    let a = Machine::new(&format!("{label}-a"), log.clone());
    let b = Machine::new(&format!("{label}-b"), log);
    a.enable_without_agents().await;
    b.enable().await;
    (a, b)
}

#[tokio::test(start_paused = true)]
async fn move_root_to_new_project_is_one_shared_write_and_keeps_remote_checkout_unkeyed() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("move-root", log.clone()).await;
    let kept = a.repo("kept", &[("origin", "move/kept")]);
    let moved = a.repo("moved", &[("origin", "move/separated")]);
    a.register("P", &kept, Some("W")).await;
    a.call("add_root", json!({"projectId":"P","root":moved}))
        .await
        .unwrap();
    a.call("approve_root", json!({"root":moved})).await.unwrap();
    b.catch_up().await;
    let clone = b.repo("clone", &[("origin", "move/separated")]);
    b.call("attach_root", json!({"path":clone})).await.unwrap();
    b.approve(&clone).await;
    let local_binding = b.rows("root_binding");
    let local_approval = b.rows("root_approval");
    let head = log.head();
    let cursor = a.store().generation().unwrap();
    let feed_params = json!({"incarnation":"acceptance","cursor":cursor,"wait":false});
    let before_feed = a.call("agent.changes", feed_params.clone()).await.unwrap();
    let request = json!({"fromProjectId":"P","root":moved,"name":"Separated"});
    assert_eq!(
        a.bytes_on(CORE, "move_root_to_new_project", request.clone())
            .await
            .unwrap_err(),
        "write_not_permitted"
    );
    let flow = (99, 1);
    a.handler.route_admissions().insert(
        flow,
        RouteAdmission::from_bind(
            Some(Principal::Direct),
            Some(&crate::tests::flow_stamp(Some("flow"))),
        ),
    );
    assert_eq!(
        a.bytes_on(flow, "move_root_to_new_project", request.clone())
            .await
            .unwrap_err(),
        "flow_scope_not_admitted"
    );
    assert_eq!(log.head(), head);
    let result = a.call("move_root_to_new_project", request).await.unwrap();
    let project = result["projectId"].as_str().unwrap();
    assert_eq!(result["generation"], cursor + 4);
    assert_eq!(log.head(), head + 1);
    let after_feed = a.call("agent.changes", feed_params).await.unwrap();
    assert_eq!(
        serde_json::to_vec(&before_feed["entries"]).unwrap(),
        serde_json::to_vec(&after_feed["entries"]).unwrap()
    );
    assert_eq!(before_feed["cursor"], cursor);
    assert_eq!(after_feed["cursor"], a.store().generation().unwrap());
    assert_eq!(after_feed["generation"], after_feed["cursor"]);
    assert_eq!(after_feed["cursor"], cursor + 5);
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    for machine in [&a, &b] {
        assert_eq!(machine.query("SELECT project_id,kind,root_key FROM project_root_key WHERE root_key='move/separated'"), vec![vec![Cell::Text(project.into()), Cell::Text("remote".into()), Cell::Text("move/separated".into())]]);
        let resolved = machine
            .call("resolve_project_id", json!({"projectId":project}))
            .await
            .unwrap();
        assert!(resolved["workspaceId"].is_null());
        assert_eq!(machine.call("verify", json!({})).await.unwrap()["ok"], true);
        machine.clean_rebuild().await;
    }
    assert_eq!(a.store().resolve(&moved).unwrap().project_id, project);
    assert_eq!(a.store().resolve(&kept).unwrap().project_id, "P");
    let record = a
        .store()
        .trust(&moved)
        .unwrap()
        .root_fields
        .unwrap()
        .root_records
        .remove(0);
    assert_eq!(record.approval.state, "unapproved");
    assert_eq!(record.identity, "bound");
    assert_eq!(
        b.query("SELECT canonical_root,project_id,root_key_kind,root_key FROM project_root"),
        vec![vec![
            Cell::Text(clone.clone()),
            Cell::Text("P".into()),
            Cell::Null,
            Cell::Null
        ]]
    );
    assert_eq!(b.rows("root_binding"), local_binding);
    assert_eq!(b.rows("root_approval"), local_approval);
    assert_eq!(b.store().resolve(&clone).unwrap().project_id, "P");
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

#[tokio::test(start_paused = true)]
async fn s3_names_and_boxes_survive_supersede_for_late_joiner() {
    let root = FakeLog::default();
    let a_log = Arc::new(root.with_author([0x11; 16]));
    let b_log = Arc::new(root.with_author([0x22; 16]));
    let a = Machine::new("name-a", a_log.clone());
    let b = Machine::new("name-b", b_log);
    a.enable_without_agents().await;
    b.enable_without_agents().await;
    assert_eq!(
        a.store().persisted_self_author().unwrap(),
        Some("11".repeat(16))
    );
    // This fixture has no daemon box id, so joining does not need to learn its author.
    assert_eq!(b.store().persisted_self_author().unwrap(), None);
    assert_eq!(
        b.call("member.set_name", json!({"display_name":"Joiner"}))
            .await
            .unwrap_err(),
        "self_author_unknown"
    );
    assert!(b.rows("self_identity").is_empty());
    a.call("member.set_name", json!({"display_name":"  Studio  "}))
        .await
        .unwrap();
    a.call(
        "agent.create",
        json!({"request_key":"named-agent","name":"Ada","tag":"test","role":"assistant"}),
    )
    .await
    .unwrap();
    b.catch_up().await;
    assert_eq!(
        b.rows("member_name"),
        vec![vec![
            Cell::Text("11".repeat(16)),
            Cell::Text("Studio".into())
        ]]
    );

    // Populate the mapping through the shared writer before replacing the prefix.
    let studio_box = "ab".repeat(16);
    a.handler.health.origin.set_daemon(Some(studio_box.clone()));
    let writer = a.handler.writer.clone().lock_owned().await;
    drive(a.handler.catch_up_worker().publish_member_box(
        &a.store(),
        writer,
        tokio::time::Instant::now() + Duration::from_secs(25),
    ))
    .await
    .unwrap();
    b.catch_up().await;

    // Reset the enable flag to disabled without dropping shared rows, as a
    // restored copy must re-enable by superseding its own existing log prefix.
    a.store()
        .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='disabled'", [])?;
            Ok(())
        })
        .unwrap();
    a.enable().await;
    let page = a_log.client().read(0, 128).await.unwrap();
    let snapshot = page.entries.last().unwrap();
    let value: Value = serde_json::from_slice(&snapshot.entry).unwrap();
    assert!(value["supersedes_through"].as_u64().unwrap() > 0);
    assert_eq!(
        value["member_name"],
        json!({"author":"11".repeat(16),"display_name":"Studio"})
    );
    assert_eq!(
        value["member_box"],
        json!({"author":"11".repeat(16),"box_id":studio_box})
    );
    let c = Machine::new("name-late", Arc::new(root.with_author([0x33; 16])));
    c.enable_without_agents().await;
    assert_eq!(c.rows("member_name"), b.rows("member_name"));
    assert_eq!(c.rows("member_box"), b.rows("member_box"));
    assert_eq!(c.store().persisted_self_author().unwrap(), None);
    a.clean_rebuild().await;
    b.clean_rebuild().await;
    c.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn s3_joiner_first_own_write_learns_author_before_name() {
    let root = FakeLog::default();
    let a = Machine::new("first-own-a", Arc::new(root.with_author([0x11; 16])));
    let b = Machine::new("first-own-b", Arc::new(root.with_author([0x22; 16])));
    a.enable_without_agents().await;
    b.enable_without_agents().await;
    assert_eq!(b.store().persisted_self_author().unwrap(), None);
    b.call("register", json!({"projectId":"B","name":"B","roots":[]}))
        .await
        .unwrap();
    assert_eq!(
        b.store().persisted_self_author().unwrap(),
        Some("22".repeat(16))
    );
    b.call("member.set_name", json!({"display_name":"Joiner"}))
        .await
        .unwrap();
    a.catch_up().await;
    assert_eq!(
        a.rows("member_name"),
        vec![vec![
            Cell::Text("22".repeat(16)),
            Cell::Text("Joiner".into())
        ]]
    );
}

// A client merging several machines' fleet replies picks the one with the
// highest `logPosition`. This pins that the field is the shared position each
// machine has applied: a machine that hasn't caught up reports less, and both
// report the log head once they agree.
#[tokio::test(start_paused = true)]
async fn fleet_identity_reports_the_shared_log_position_each_machine_applied() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("fleet-position", log.clone()).await;
    async fn fleet(m: &Machine) -> Value {
        m.call("agent.fleet_identity", json!({})).await.unwrap()
    }
    a.call(
        "agent.create",
        json!({"role":"assistant","name":"Ada","tag":"helper","request_key":"create"}),
    )
    .await
    .unwrap();
    let (ahead, behind) = (fleet(&a).await, fleet(&b).await);
    assert_eq!(ahead["logPosition"], log.head());
    assert!(behind["logPosition"].as_i64() < ahead["logPosition"].as_i64());
    b.catch_up().await;
    let (a_reply, b_reply) = (fleet(&a).await, fleet(&b).await);
    assert_eq!(a_reply["logPosition"], log.head());
    assert_eq!(b_reply["logPosition"], log.head());
    assert_eq!(a_reply["agents"], b_reply["agents"]);
}

#[tokio::test(start_paused = true)]
async fn two_machines_converge_with_placement_and_agent_change_ops() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("convergence", log.clone()).await;
    let p1 = a.repo("p1", &[("origin", "converge/p1")]);
    let p2 = a.repo("p2", &[("origin", "converge/p2")]);
    a.register("P1", &p1, Some("W")).await;
    b.catch_up().await;
    let b_root = b.repo("p1-local", &[("origin", "converge/p1")]);
    b.call("attach_root", json!({"path":b_root})).await.unwrap();
    let binding = b.approve(&b_root).await;
    b.call(
        "set_owned_remotes",
        json!({"root":b_root,"remotes":["origin"]}),
    )
    .await
    .unwrap();
    let container = b.dir.join("workers");
    std::fs::create_dir_all(&container).unwrap();
    b.call("attach_derived_parent", json!({"projectId":"P1","root":b_root,"incarnation":binding["incarnation"],"registrationEpoch":binding["registrationEpoch"],"container":container})).await.unwrap();
    b.call(
        "set_workspace_root",
        json!({"workspaceId":"W","root":b.dir}),
    )
    .await
    .unwrap();
    let local = b.local();
    let cursor = b.store().generation().unwrap();
    a.register("P2", &p2, None).await;
    let created = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Ada","tag":"helper","request_key":"create"}),
        )
        .await
        .unwrap();
    let id = created["agent"]["agent_id"].as_str().unwrap();
    a.call(
        "agent.rename",
        json!({"agent_id":id,"name":"Grace","request_key":"rename"}),
    )
    .await
    .unwrap();
    a.call("remove", json!({"projectId":"P2"})).await.unwrap();
    b.catch_up().await;
    assert_eq!(b.shared(), a.shared());
    assert_eq!(
        b.rows("workspace_member"),
        vec![vec![
            Cell::Text("W".into()),
            Cell::Text("local".into()),
            Cell::Text("".into()),
            Cell::Text("P1".into())
        ]]
    );
    assert_eq!(b.local(), local, "unrelated local cells must not travel");
    let feed = b
        .call(
            "agent.changes",
            json!({"incarnation":"acceptance","cursor":cursor,"wait":false}),
        )
        .await
        .unwrap();
    let entries = feed["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0]["op"], "agent.create");
    assert_eq!(entries[1]["op"], "agent.rename");
    assert_eq!(entries[0]["row"]["name"], "Ada");
    assert_eq!(entries[1]["row"]["name"], "Grace");
    assert_eq!(feed["cursor"], b.store().generation().unwrap());
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position as u64,
        log.head()
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn restored_pre_enable_store_supersedes_and_three_members_converge() {
    use std::task::{Context, Waker};

    const ADA: &str = "agent_0000000000000001";
    const LIN: &str = "agent_0000000000000002";
    let log = FakeLog::default();
    let author_a = Arc::new(log.with_author([0x11; 16]));
    let author_b = Arc::new(log.with_author([0x22; 16]));
    let author_d = Arc::new(log.with_author([0x33; 16]));
    let a = Machine::new("incident-a", author_a.clone());
    let root = a.repo("p", &[("origin", "incident/p")]);
    a.register("P", &root, Some("W")).await;
    let backup = a.dir.join("pre-enable.db");
    // VACUUM INTO includes committed WAL contents without copying the live
    // store's database files piecemeal.
    a.conn()
        .execute("VACUUM INTO ?1", [backup.to_str().unwrap()])
        .unwrap();
    let before_shared = a.shared();
    let before_local = a.local();
    let before_journal = a.rows("registry_journal");

    a.enable_without_agents().await;
    assert_eq!(log.head(), 1);
    let original = drive(author_a.client().read(0, 128)).await.unwrap();
    assert_eq!(original.entries.len(), 1);
    let bootstrap: Value = serde_json::from_slice(&original.entries[0].entry).unwrap();
    assert!(
        bootstrap.get("agents").is_none(),
        "empty agent lists are omitted on the wire"
    );
    assert!(a.rows("agent").is_empty());
    assert!(bootstrap.get("supersedes_through").is_none());
    assert_eq!(
        a.query("SELECT COUNT(*) FROM registry_journal WHERE op='agent.cutover'"),
        vec![vec![Cell::Integer(1)]]
    );
    let b = Machine::new("incident-b", author_b.clone());
    b.enable().await;
    assert_eq!(b.shared(), a.shared());
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        1
    );
    let initial = b.call("agent.snapshot", json!({})).await.unwrap();
    assert!(initial["agents"].as_array().unwrap().is_empty());
    assert!(initial["claims"].as_array().unwrap().is_empty());
    let cursor = initial["generation"].as_i64().unwrap();
    let body = serde_json::to_vec(&json!({"method":"agent.changes","params":{
        "incarnation":"acceptance","cursor":cursor,"wait":true
    }}))
    .unwrap();
    let waiter = b.handler.handle_request_wait(&body, CORE);
    tokio::pin!(waiter);
    assert!(waiter
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let before = tokio::time::Instant::now();

    let a2 = Machine::restore("incident-a2", &backup, author_a.clone(), |_| {});
    assert_eq!(a2.shared(), before_shared);
    assert_eq!(a2.local(), before_local);
    assert_eq!(a2.rows("registry_journal"), before_journal);
    assert_eq!(a2.store().identity_log_status().unwrap().state, "disabled");
    assert_eq!(
        a2.query("SELECT COUNT(*) FROM registry_journal WHERE op='agent.cutover'"),
        vec![vec![Cell::Integer(0)]]
    );

    // Import real frozen core tables, including a released name claim. The
    // restored copy has neither the zero-agent marker nor the enabled state.
    let snapshot_path = a2.dir.join("core-agents.db");
    let source = Connection::open(&snapshot_path).unwrap();
    for migration in [
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/076_agent_registry.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/082_wake_delivery.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/109_agent_generation.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
        include_str!("../../entorhinal-core/tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
    ] {
        source.execute_batch(migration).unwrap();
    }
    source.execute_batch("INSERT INTO agent(agent_id,name,tag,role,name_version,generation,labels_json,created_at_ms,updated_at_ms)
        VALUES('agent_0000000000000001','Ada','helper','assistant',2,2,'[\"imported\"]',10,20);
        INSERT INTO agent(agent_id,name,tag,role,project_id,created_at_ms,updated_at_ms)
        VALUES('agent_0000000000000002','Lin','lead','head','P',10,10);
        INSERT INTO agent_name_claim(claim_id,agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms,released_at_ms) VALUES
        (1,'agent_0000000000000001','assistant','global','grace','Grace',10,20),
        (2,'agent_0000000000000001','assistant','global','ada','Ada',20,NULL),
        (3,'agent_0000000000000002','workspace','W','lin','Lin',10,NULL);").unwrap();
    drop(source);
    let imported = a2
        .call(
            "agent.import",
            json!({
                "snapshot_path":snapshot_path,"request_key":"incident-import"
            }),
        )
        .await
        .unwrap();
    assert_eq!(imported["agents_imported"], 2);
    assert_eq!(imported["claims_imported"], 3);
    assert_eq!(log.head(), 1, "agent.import must remain a local operation");
    let snapshot = a2.call("agent.snapshot", json!({})).await.unwrap();
    a2.enable().await;
    assert_eq!(log.head(), 2);
    let enabled = a2.store().identity_log_status().unwrap();
    assert_eq!(enabled.last_applied_position, 2);
    assert_eq!(enabled.last_seen_head, 2);
    let supersede = drive(author_a.client().read(0, 128)).await.unwrap();
    assert_eq!(supersede.entries.len(), 2);
    assert_eq!(supersede.entries[0], original.entries[0]);
    let entry = &supersede.entries[1];
    assert_eq!(entry.position, 2);
    assert_eq!(entry.kind, "snapshot");
    assert!(entry.signed_by_self);
    assert_eq!(entry.author, original.entries[0].author);
    let part: Value = serde_json::from_slice(&entry.entry).unwrap();
    assert_eq!(part["supersedes_through"], 1);
    assert_eq!(part["part"], 1);
    assert_eq!(part["parts"], 1);
    assert_eq!(part["agents"].as_array().unwrap().len(), 2);
    let journal =
        a2.query("SELECT payload_json FROM registry_journal WHERE op='identity_log.enable'");
    let Cell::Text(payload) = &journal[0][0] else {
        panic!("missing enable payload")
    };
    assert_eq!(
        serde_json::from_str::<Value>(payload).unwrap()["supersedes_through"],
        1
    );
    assert_eq!(
        a2.query("SELECT COUNT(*) FROM registry_journal WHERE op='agent.cutover'"),
        vec![vec![Cell::Integer(1)]]
    );
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        1
    );
    assert!(waiter
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());

    b.catch_up().await;
    assert_eq!(
        tokio::time::Instant::now(),
        before,
        "catch-up must wake the waiter before its timeout"
    );
    // The waiter's scans finish on the blocking pool, so it is awaited, not
    // polled once; paused time must not move, so the catch-up woke it.
    let HandlerOutcome::Response(bytes) =
        tokio::time::timeout(std::time::Duration::from_secs(60), waiter.as_mut())
            .await
            .expect("the parked agent.changes waiter never returned")
    else {
        panic!("supersede did not notify the parked agent.changes waiter")
    };
    assert_eq!(
        tokio::time::Instant::now(),
        before,
        "the waiter returned on its timeout, not on the catch-up"
    );
    let waited: Value = serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone();
    let feed = b
        .call(
            "agent.changes",
            json!({
                "incarnation":"acceptance","cursor":cursor,"wait":false
            }),
        )
        .await
        .unwrap();
    assert_eq!(
        waited, feed,
        "the pre-supersede cursor and waiter see the same after-images"
    );
    let entries = feed["entries"].as_array().unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(
        entries
            .iter()
            .map(|e| e["agent_id"].as_str().unwrap())
            .collect::<Vec<_>>(),
        vec![ADA, LIN]
    );
    for (entry, row) in entries.iter().zip(snapshot["agents"].as_array().unwrap()) {
        assert_eq!(entry["op"], "agent.import");
        assert_eq!(&entry["row"], row);
        assert_eq!(entry["agent_generation"], row["agent_generation"]);
        let claims: Vec<_> = snapshot["claims"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|claim| claim["agent_id"] == row["agent_id"])
            .cloned()
            .collect();
        assert_eq!(entry["claims"], json!(claims));
        assert!(entry["seq"].as_i64().unwrap() > cursor);
    }
    assert_eq!(entries[0]["row"]["name"], "Ada");
    assert_eq!(entries[0]["row"]["agent_generation"], 2);
    assert_eq!(entries[0]["row"]["labels"], json!(["imported"]));
    assert_eq!(entries[0]["claims"][0]["released_at_ms"], 20);
    assert_eq!(entries[0]["claims"][1]["released_at_ms"], Value::Null);
    assert_eq!(feed["cursor"], b.store().generation().unwrap());
    assert_eq!(
        b.handler.health.generation.load(Ordering::Relaxed),
        b.store().generation().unwrap()
    );
    assert_eq!(b.shared(), a2.shared());
    assert_eq!(
        b.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        2
    );

    // The actual reserved core route must be able to mutate imported identity;
    // success on a direct store call would not prove serving-path admission.
    let renamed = b
        .call(
            "agent.rename",
            json!({
                "agent_id":ADA,"name":"Katherine","request_key":"incident-rename"
            }),
        )
        .await
        .unwrap();
    assert_eq!(renamed["agent"]["name"], "Katherine");
    assert_eq!(renamed["agent"]["agent_generation"], 3);
    assert_eq!(log.head(), 3);
    let tail = drive(author_b.client().read(2, 128)).await.unwrap();
    assert_eq!(tail.entries.len(), 1);
    assert_eq!(tail.entries[0].position, 3);
    assert_eq!(tail.entries[0].kind, "change");
    assert!(tail.entries[0].signed_by_self);
    assert_ne!(tail.entries[0].author, entry.author);
    let change: Value = serde_json::from_slice(&tail.entries[0].entry).unwrap();
    assert_eq!(change["op"], "agent.rename");
    assert_eq!(change["row"]["name"], "Katherine");
    let d = Machine::new("incident-d", author_d);
    d.enable().await;
    a2.catch_up().await;
    for member in [&a2, &b, &d] {
        let status = member.store().identity_log_status().unwrap();
        assert_eq!(status.state, "enabled");
        assert_eq!(status.last_applied_position, 3);
        assert_eq!(status.last_seen_head, 3);
        assert_eq!(member.shared(), b.shared());
        assert_eq!(member.rows("workspace_member"), b.rows("workspace_member"));
        assert_eq!(member.rows("agent").len(), 2);
        assert_eq!(member.rows("agent_name_claim").len(), 4);
        assert_eq!(member.query("SELECT name,name_version,agent_generation FROM agent WHERE agent_id='agent_0000000000000001'"),
            vec![vec![Cell::Text("Katherine".into()), Cell::Integer(3), Cell::Integer(3)]]);
        member.clean_rebuild().await;
        assert_eq!(
            member
                .store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            3
        );
    }
    assert_eq!(
        log.head(),
        3,
        "verify and rebuild must not publish shared entries"
    );
}

#[tokio::test(start_paused = true)]
async fn remote_cascades_retire_bindings_move_keys_and_clear_workspace_paths() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("cascade", log).await;
    let root = a.repo("p", &[("origin", "cascade/p")]);
    a.register("P", &root, Some("W")).await;
    b.catch_up().await;
    let local = b.repo("p", &[("origin", "cascade/p")]);
    b.call("attach_root", json!({"path":local})).await.unwrap();
    let implicit = entorhinal_core::implicit_project_id(&local);
    assert_eq!(
        b.call("resolve_project_id", json!({"projectId":implicit}))
            .await
            .unwrap()["projectId"],
        "P"
    );
    assert!(!b.rows("root_binding").is_empty());
    a.call("remove", json!({"projectId":"P"})).await.unwrap();
    b.catch_up().await;
    for table in [
        "project_root",
        "root_binding",
        "root_approval",
        "project_alias",
        "project_root_key",
    ] {
        assert!(b.rows(table).is_empty(), "outright removal left {table}");
    }
    b.clean_rebuild().await;

    let moved = a.repo("moved", &[("origin", "cascade/moved")]);
    let successor = a.repo("successor", &[("origin", "cascade/successor")]);
    a.register("M", &moved, Some("W")).await;
    a.register("S", &successor, None).await;
    b.catch_up().await;
    let checkout = b.repo("moved", &[("origin", "cascade/moved")]);
    b.call("attach_root", json!({"path":checkout}))
        .await
        .unwrap();
    let binding = b.approve(&checkout).await;
    a.call("remove", json!({"projectId":"M","successorProjectId":"S"}))
        .await
        .unwrap();
    b.catch_up().await;
    let resolved = b
        .call(
            "resolve",
            json!({"canonicalRoot":checkout,"executionBinding":binding}),
        )
        .await
        .unwrap();
    assert_eq!(resolved["projectId"], "S");
    assert_eq!(resolved["bindingStatus"], "retired");
    assert_eq!(
        resolved["rootRecords"][0]["approval"]["state"],
        "unapproved"
    );
    assert!(b.rows("root_binding").is_empty());
    assert!(b.rows("root_approval").is_empty());
    assert_eq!(
        b.query("SELECT project_id,root_key_kind,root_key FROM project_root"),
        vec![vec![
            Cell::Text("S".into()),
            Cell::Text("remote".into()),
            Cell::Text("cascade/moved".into())
        ]]
    );
    assert_eq!(
        b.call(
            "resolve_project_id",
            json!({"projectId":entorhinal_core::implicit_project_id(&checkout)})
        )
        .await
        .unwrap()["projectId"],
        "S"
    );
    let epoch = binding["registrationEpoch"].as_str().unwrap();
    assert!(b
        .query("SELECT registration_epoch,reason FROM retired_binding")
        .contains(&vec![
            Cell::Text(epoch.into()),
            Cell::Text("removed".into())
        ]));
    b.clean_rebuild().await;

    b.call(
        "set_workspace_root",
        json!({"workspaceId":"W","root":b.dir}),
    )
    .await
    .unwrap();
    assert_eq!(b.rows("workspace_root").len(), 1);
    a.call("remove", json!({"workspaceId":"W"})).await.unwrap();
    b.catch_up().await;
    assert!(b.rows("workspace_root").is_empty());
    assert!(b.rows("workspace_member").is_empty());
    b.clean_rebuild().await;
    a.call(
        "assign_workspace",
        json!({"projectId":"S","workspaceId":"W"}),
    )
    .await
    .unwrap();
    b.catch_up().await;
    assert_eq!(
        b.call("resolve", json!({"canonicalRoot":checkout}))
            .await
            .unwrap()["workspaceRoot"],
        Value::Null
    );
    assert!(b.rows("workspace_root").is_empty());
    assert_eq!(b.shared(), a.shared());
    b.clean_rebuild().await;
}

// Park exactly the first two appends after arming. Both writers have prepared
// against the same head before either CAS executes, so races are reproducible.
struct RacingLog {
    log: FakeLog,
    remaining: AtomicUsize,
    barrier: tokio::sync::Barrier,
    attempts: Mutex<Vec<Value>>,
}

impl RacingLog {
    fn new() -> Self {
        Self {
            log: FakeLog::default(),
            remaining: AtomicUsize::new(0),
            barrier: tokio::sync::Barrier::new(2),
            attempts: Mutex::new(vec![]),
        }
    }
    fn arm(&self) {
        self.remaining.store(2, Ordering::SeqCst);
    }
}

#[derive(Clone)]
struct RaceConnector(Arc<RacingLog>);

#[async_trait]
impl LogConnector for RaceConnector {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl LogTransport for RaceConnector {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        if method == log_client::APPEND
            && self
                .0
                .remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                .is_ok()
        {
            self.0.attempts.lock().unwrap().push(params.clone());
            self.0.barrier.wait().await;
        }
        self.0.log.call(method, params).await
    }
}

async fn racing_pair(label: &str) -> (Machine, Machine, Arc<RacingLog>) {
    let log = Arc::new(RacingLog::new());
    let (a, b) = pair(label, Arc::new(RaceConnector(log.clone()))).await;
    log.arm();
    (a, b, log)
}

fn same_head(log: &RacingLog) {
    let attempts = log.attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(attempts[0]["expected_head"], attempts[1]["expected_head"]);
    assert_ne!(attempts[0]["entry_id"], attempts[1]["entry_id"]);
}

#[tokio::test(start_paused = true)]
async fn conflicting_registers_commit_once_and_refuse_root_key_exists() {
    let (a, b, log) = racing_pair("conflicting").await;
    let ra = a.repo("p", &[("origin", "races/same")]);
    let rb = b.repo("q", &[("origin", "races/same")]);
    let (x, y) = drive(async {
        tokio::join!(
            a.call("register", json!({"projectId":"P","name":"P","roots":[ra]})),
            b.call("register", json!({"projectId":"Q","name":"Q","roots":[rb]})),
        )
    })
    .await;
    assert_eq!(usize::from(x.is_ok()) + usize::from(y.is_ok()), 1);
    let refusal = if x.is_err() {
        x.unwrap_err()
    } else {
        y.unwrap_err()
    };
    assert_eq!(refusal, "root_key_exists");
    same_head(&log);
    assert_eq!(log.log.head(), 2, "snapshot plus exactly one register");
    a.catch_up().await;
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert_eq!(a.rows("project").len(), 1);
    assert_eq!(a.rows("project_root_key").len(), 1);
    assert_eq!(
        a.rows("project_root").len() + b.rows("project_root").len(),
        1
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn independent_appends_both_commit_at_dense_positions() {
    let (a, b, log) = racing_pair("independent").await;
    let (x, y) = drive(async {
        tokio::join!(
            a.call(
                "register",
                json!({"projectId":"P","name":"P","roots":[],"requestKey":"a"})
            ),
            b.call(
                "register",
                json!({"projectId":"Q","name":"Q","roots":[],"requestKey":"b"})
            ),
        )
    })
    .await;
    x.unwrap();
    y.unwrap();
    same_head(&log);
    let page = log.log.client().read(0, 128).await.unwrap();
    assert_eq!(page.head, 3);
    assert_eq!(
        page.entries.iter().map(|e| e.position).collect::<Vec<_>>(),
        [1, 2, 3]
    );
    assert_eq!(
        page.entries
            .iter()
            .map(|e| e.entry_id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        3
    );
    a.catch_up().await;
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert_eq!(a.rows("project").len(), 2);
    for m in [&a, &b] {
        assert_eq!(m.query("SELECT log_position FROM registry_journal WHERE stream='shared' AND log_position >= 2 ORDER BY log_position"), vec![vec![Cell::Integer(2)], vec![Cell::Integer(3)]]);
        m.clean_rebuild().await;
    }
}

#[derive(Default)]
struct Absent {
    calls: AtomicUsize,
}

#[async_trait]
impl LogConnector for Absent {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Err(TransportError::NoReply)
    }
}

#[tokio::test(start_paused = true)]
async fn absent_engram_all_reads_startup_and_same_key_core_retry() {
    let log = Arc::new(FakeLog::default());
    let mut m = Machine::new("absent", log.clone());
    m.enable_without_agents().await;
    let root = m.repo("p", &[("origin", "absent/p")]);
    m.register("P", &root, Some("ws-W")).await;
    let created = m
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Reader","tag":"ok","request_key":"reader-create"}),
        )
        .await
        .unwrap();
    let id = created["agent"]["agent_id"].as_str().unwrap();
    let absent = Arc::new(Absent::default());
    m.restart_legacy(absent.clone()).await;
    assert_eq!(m.handler.health().await.status, HealthStatus::Ok);
    assert_eq!(
        absent.calls.load(Ordering::SeqCst),
        0,
        "startup must be lazy"
    );
    for (method, params) in [
        ("resolve", json!({"canonicalRoot":root})),
        ("resolve_project_id", json!({"projectId":"P"})),
        ("resolve_remote", json!({"owner":"absent","repo":"p"})),
        (
            "resolve_root_key",
            json!({"projectId":"P","kind":"remote","rootKey":"absent/p"}),
        ),
        ("preview_attach_root", json!({"path":root})),
        ("enumerate", json!({})),
        ("trust", json!({"canonicalRoot":root})),
        ("journal_tail", json!({"after":0})),
        ("verify", json!({})),
        ("identity_log.status", json!({})),
        ("agent.resolve", json!({"agent_id":id})),
        ("agent.resolve_name", json!({"name":"Reader"})),
        ("agent.list", json!({})),
        ("agent.peer_roster", json!({"workspace_id":"ws-W"})),
        ("agent.avatar_read", json!({"agentIds":[id]})),
        ("agent.github_identity", json!({"agent_id":id})),
        ("agent.fleet_identity", json!({})),
        ("agent.snapshot", json!({})),
        (
            "agent.changes",
            json!({"incarnation":"acceptance","cursor":0,"wait":false}),
        ),
    ] {
        m.call(method, params)
            .await
            .unwrap_or_else(|code| panic!("read {method}: {code}"));
    }
    assert_eq!(
        absent.calls.load(Ordering::SeqCst),
        0,
        "reads must not dial engram"
    );
    let request = json!({"projectId":"Retry","name":"Retry","roots":[],"requestKey":"core-retry"});
    let before = m.rows("registry_journal");
    let shared = m.shared();
    let local = m.local();
    let head = log.head();
    assert_eq!(
        m.bytes_on(CORE, "register", request.clone())
            .await
            .unwrap_err(),
        "engram_unavailable"
    );
    assert!(absent.calls.load(Ordering::SeqCst) > 0);
    assert_eq!(m.rows("registry_journal"), before);
    assert_eq!(m.shared(), shared);
    assert_eq!(m.local(), local);
    assert_eq!(
        m.store().identity_log_status().unwrap().pending_write_count,
        0
    );
    m.restart_legacy(log.clone()).await;
    let committed = m.bytes_on(CORE, "register", request.clone()).await.unwrap();
    let journal = m.rows("registry_journal");
    m.restart_legacy(Arc::new(FailConnector::default())).await;
    assert_eq!(
        m.bytes_on(CORE, "register", request).await.unwrap(),
        committed
    );
    assert_eq!(m.rows("registry_journal"), journal);
    assert_eq!(log.head(), head + 1);
    assert_eq!(
        m.query("SELECT op FROM registry_journal WHERE request_key='core-retry'"),
        vec![vec![Cell::Text("register".into())]]
    );
    m.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn absent_engram_local_writes_keep_keys_and_workspace_timestamps() {
    let log = Arc::new(FakeLog::default());
    let mut m = Machine::new("local-absent", log.clone());
    m.enable_without_agents().await;
    let root = m.repo("first", &[("origin", "local/p")]);
    m.register("P", &root, Some("W")).await;
    let clone = m.repo("clone", &[("origin", "local/p")]);
    m.call("attach_root", json!({"path":clone})).await.unwrap();
    let fail = Arc::new(FailConnector::default());
    m.restart_legacy(fail.clone()).await;
    let shared = m.shared();
    let head = log.head();
    m.approve(&clone).await;
    m.call(
        "set_owned_remotes",
        json!({"root":clone,"remotes":["origin"]}),
    )
    .await
    .unwrap();
    let implicit = entorhinal_core::implicit_project_id(&clone);
    assert!(m
        .query("SELECT old_id FROM project_alias")
        .contains(&vec![Cell::Text(implicit.clone())]));
    m.call("remove_root", json!({"projectId":"P","root":clone}))
        .await
        .unwrap();
    assert!(!m
        .query("SELECT old_id FROM project_alias")
        .contains(&vec![Cell::Text(implicit)]));
    m.call("remove_root", json!({"projectId":"P","root":root}))
        .await
        .unwrap();
    assert!(m.rows("project_root").is_empty());
    assert!(m.rows("root_binding").is_empty());
    assert!(m.rows("root_approval").is_empty());
    assert_eq!(m.rows("retired_binding").len(), 2);
    // Both local writes use the same injected clock as agent writes, without
    // waiting for or asserting anything about the real wall clock.
    for (time, name) in [(100, "workspace-one"), (200, "workspace-two")] {
        let path = m.dir.join(name);
        std::fs::create_dir_all(&path).unwrap();
        m.handler.clock = if time == 100 { || 100 } else { || 200 };
        m.call("set_workspace_root", json!({"workspaceId":"W","root":path}))
            .await
            .unwrap();
        assert_eq!(
            m.query("SELECT updated_at FROM workspace_root"),
            vec![vec![Cell::Integer(time)]]
        );
    }
    assert_eq!(
        m.query("SELECT created_at FROM registry_journal WHERE op='set_workspace_root'"),
        vec![vec![Cell::Integer(100)], vec![Cell::Integer(200)]]
    );
    assert_eq!(m.shared(), shared);
    assert_eq!(log.head(), head);
    assert_eq!(fail.calls(), 0);
    m.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn attach_matches_remote_selects_smallest_key_and_sorts_two_clones() {
    let log = Arc::new(FakeLog::default());
    let a = Machine::new("attach-a", log.clone());
    let b = Machine::new("attach-b", log);
    let root = a.repo("p", &[("origin", "z/z"), ("other", "a/a")]);
    // Install the override before enabling, so backfill must choose from both
    // effective owned remotes, not just the default origin.
    a.call(
        "register",
        json!({"projectId":"P","name":"P","roots":[root]}),
    )
    .await
    .unwrap();
    a.call(
        "set_owned_remotes",
        json!({"root":root,"remotes":["origin","other"]}),
    )
    .await
    .unwrap();
    a.enable_without_agents().await;
    b.enable().await;
    assert_eq!(
        a.query("SELECT project_id,kind,root_key FROM project_root_key"),
        vec![vec![
            Cell::Text("P".into()),
            Cell::Text("remote".into()),
            Cell::Text("a/a".into())
        ]]
    );
    // Rebuild must use the saved key/mapping, not reread the git config.
    std::fs::remove_file(Path::new(&root).join(".git/config")).unwrap();
    a.clean_rebuild().await;
    let z = b.repo("z-clone", &[("origin", "a/a"), ("other", "z/z")]);
    let aa = b.repo("a-clone", &[("origin", "a/a")]);
    let before = b.rows("registry_journal");
    assert_eq!(
        b.call(
            "register",
            json!({"projectId":"Duplicate","name":"duplicate","roots":[z]})
        )
        .await
        .unwrap_err(),
        "root_key_exists"
    );
    assert_eq!(b.rows("registry_journal"), before);
    for path in [&z, &aa] {
        let reply = b.call("attach_root", json!({"path":path})).await.unwrap();
        assert_eq!(reply["projectId"], "P");
        let resolved = b
            .call("resolve", json!({"canonicalRoot":path}))
            .await
            .unwrap();
        assert_eq!(
            resolved["rootRecords"][0]["approval"]["state"],
            "unapproved"
        );
    }
    assert_eq!(
        b.call(
            "resolve_root_key",
            json!({"projectId":"P","kind":"remote","rootKey":"a/a"})
        )
        .await
        .unwrap()["roots"],
        json!([aa, z])
    );
    let label = a.repo("label", &[]);
    a.call(
        "register",
        json!({"projectId":"Label","name":"Label","roots":[label],"label":"a/a"}),
    )
    .await
    .unwrap();
    b.catch_up().await;
    let local_label = b.repo("label", &[]);
    b.call(
        "attach_root",
        json!({"path":local_label,"projectId":"Label","label":"a/a"}),
    )
    .await
    .unwrap();
    assert_eq!(
        b.call(
            "resolve_root_key",
            json!({"projectId":"Label","kind":"remote","rootKey":"a/a"})
        )
        .await
        .unwrap()["roots"],
        json!([])
    );
    assert_eq!(
        b.call(
            "resolve_root_key",
            json!({"projectId":"P","kind":"label","rootKey":"a/a"})
        )
        .await
        .unwrap()["roots"],
        json!([])
    );
    assert_eq!(
        b.call(
            "resolve_root_key",
            json!({"projectId":"Label","kind":"label","rootKey":"a/a"})
        )
        .await
        .unwrap()["roots"],
        json!([local_label])
    );
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn same_path_different_repository_keeps_implicit_alias_local() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("same-path", log).await;
    let path = a.repo("checkout", &[("origin", "path/a")]);
    a.register("P", &path, None).await;
    b.catch_up().await;
    // Replacing only the repository metadata models two machines' different
    // filesystems at the exact same canonical path, without changing cwd/env.
    std::fs::remove_dir_all(Path::new(&path).join(".git")).unwrap();
    repo(Path::new(&path), &[("origin", "path/b")]);
    b.register("Q", &path, None).await;
    a.catch_up().await;
    let implicit = entorhinal_core::implicit_project_id(&path);
    assert_eq!(
        b.call("resolve", json!({"canonicalRoot":path}))
            .await
            .unwrap()["projectId"],
        "Q"
    );
    assert_eq!(
        b.call("resolve_project_id", json!({"projectId":implicit}))
            .await
            .unwrap()["projectId"],
        "Q"
    );
    assert_eq!(
        a.call("resolve_project_id", json!({"projectId":implicit}))
            .await
            .unwrap()["projectId"],
        "P"
    );
    assert_eq!(a.shared(), b.shared());
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn same_name_and_path_minted_id_refuses_project_id_occupied() {
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("same-name", log).await;
    let path = a.repo("checkout", &[("origin", "name/a")]);
    let first = a
        .call("register", json!({"name":"Same","roots":[path]}))
        .await
        .unwrap();
    b.catch_up().await;
    std::fs::remove_dir_all(Path::new(&path).join(".git")).unwrap();
    repo(Path::new(&path), &[("origin", "name/b")]);
    let before = b.shared();
    let journal = b.rows("registry_journal");
    assert_eq!(
        b.call("register", json!({"name":"Same","roots":[path]}))
            .await
            .unwrap_err(),
        "project_id_occupied"
    );
    assert_eq!(b.shared(), before);
    assert_eq!(b.rows("registry_journal"), journal);
    assert!(b.rows("project_root").is_empty());
    assert_eq!(
        a.call("enumerate", json!({})).await.unwrap()["projects"][0]["projectId"],
        first["projectId"]
    );
    assert_eq!(a.rows("project_root").len(), 1);
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn two_machines_carry_and_correct_once_and_refuse_forged_claim_owner() {
    let log = Arc::new(FakeLog::default());
    let mut a = Machine::new("carry-correct-a", log.clone());
    let b = Machine::new("carry-correct-b", log.clone());
    let confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    a.handler.operator_confirmer = confirmer.clone();
    a.call(
        "register",
        json!({"projectId":"P","name":"P","roots":[],"workspaceId":"W1"}),
    )
    .await
    .unwrap();
    let source = Connection::open(a.dir.join("source.db")).unwrap();
    for migration in crate::served_vectors::MIGRATIONS {
        source.execute_batch(migration).unwrap();
    }
    source.execute_batch("INSERT INTO agent(agent_id,name,tag,role,project_id,workspace_id,created_at_ms,updated_at_ms) VALUES
        ('agent_0000000000000001','Head','','head','P','P',10,10),
        ('agent_0000000000000002','Hiree','','hiree','P','W1',10,10);
        INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES
        ('agent_0000000000000001','workspace','P','head','Head',10),
        ('agent_0000000000000002','workspace','W1','hiree','Hiree',10);").unwrap();
    drop(source);
    a.call(
        "agent.import",
        json!({"snapshot_path":a.dir.join("source.db"),"request_key":"import"}),
    )
    .await
    .unwrap();
    a.enable().await;
    b.enable().await;
    assert_eq!(a.shared(), b.shared());
    let cursor = b.store().generation().unwrap();
    let before = log.head();
    let correction = a
        .bytes_on(
            DIRECT,
            "agent.repair_workspaces",
            json!({"request_key":"correct"}),
        )
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&correction).unwrap()["result"]["cleared"],
        2
    );
    assert_eq!(log.head(), before + 1);
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    a.call(
        "assign_workspace",
        json!({"projectId":"P","workspaceId":"W2","requestKey":"carry"}),
    )
    .await
    .unwrap();
    assert_eq!(log.head(), before + 2);
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 2);
    let page = log.client().read(before, 128).await.unwrap();
    assert_eq!(page.entries.len(), 2);
    let entries: Vec<Value> = page
        .entries
        .iter()
        .map(|entry| serde_json::from_slice(&entry.entry).unwrap())
        .collect();
    for entry in &entries {
        assert_eq!(entry["op"], "project.shared");
        assert_eq!(entry["agents"].as_array().unwrap().len(), 2);
        assert!(entry["agents"]
            .as_array()
            .unwrap()
            .iter()
            .all(|agent| agent["op"] == "agent.set_namespace"
                && agent["row"]["workspace_id"].is_null()));
    }
    assert!(entries[0]["tables"].as_object().unwrap().is_empty());
    let feed = b
        .call(
            "agent.changes",
            json!({"incarnation":"acceptance","cursor":cursor,"wait":false}),
        )
        .await
        .unwrap();
    assert_eq!(feed["entries"].as_array().unwrap().len(), 4);
    assert!(a.store().verify().unwrap().ok);
    assert!(b.store().verify().unwrap().ok);
    a.clean_rebuild().await;
    b.clean_rebuild().await;
    let state = b.shared();
    let head = b.store().generation().unwrap();
    let mut forged = entries[1].clone();
    forged["agents"][0]["claims"][0]["agent_id"] = json!("agent_0000000000000002");
    log.client()
        .append(&log_client::AppendRequest {
            expected_head: log.head(),
            entry_id: [0xf1; 16],
            kind: log_client::EntryKind::Change,
            data: serde_json::to_vec(&forged).unwrap(),
        })
        .await
        .unwrap();
    let error = drive(b.handler.catch_up(
        &b.store(),
        tokio::time::Instant::now() + Duration::from_secs(25),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code, "identity_log_stalled");
    assert_eq!(
        shared_write::health_state(b.handler.health.log_state.load(Ordering::Relaxed)),
        "apply_failed"
    );
    assert!(
        error
            .message
            .contains("agent entry changes another agent's claim"),
        "{}",
        error.message
    );
    assert_eq!(b.shared(), state);
    assert_eq!(b.store().generation().unwrap(), head);
}

mod moves {
    mod agent_data_request_vectors;
    mod move_refs;
}

#[tokio::test(start_paused = true)]
async fn consent_workspace_and_hire_replicate_without_local_authority_rows() {
    use crate::fake_cingulate::{Effect, FakeCingulate};
    use entorhinal_core::agent::consent::Authorization;
    let log = FakeLog::default();
    let mut a = Machine::new("consent-shared-a", Arc::new(log.with_author([0x11; 16])));
    let b = Machine::new("consent-shared-b", Arc::new(log.with_author([0x22; 16])));
    a.enable_without_agents().await;
    b.enable_without_agents().await;
    let assistant = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Janitor","tag":"helper","request_key":"assistant"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let fake = FakeCingulate::new();
    fake.set_now(700);
    a.handler.consent_connector = Arc::new(fake.clone());
    let stamp = |id: &str| {
        serde_json::from_value::<subc_protocol::scope::ScopeStamp>(json!({"owner":{"kind":"reserved","module_id":"prefrontal"},"ref":format!("head:{id}"),"scope_epoch":7,"kind":"head","attributes":{"agent_id":id},"owner_authorized":true})).unwrap()
    };
    let tool = (80, 2);
    a.handler.route_admissions().insert(
        tool,
        RouteAdmission::from_bind(
            Some(Principal::Reserved {
                module_id: "prefrontal".into(),
            }),
            Some(&stamp(&assistant)),
        ),
    );
    fake.script("consent.await", Effect::Answer("approve_once"));
    let body = serde_json::to_vec(&json!({"name":"projects_create_workspace","arguments":{"workspace_id":"W","name":"Workspace","request_key":"workspace"}})).unwrap();
    let outcome = drive(a.handler.handle_served_request(&body, tool)).await;
    assert!(
        matches!(outcome, HandlerOutcome::Response(_)),
        "{outcome:?}"
    );
    b.catch_up().await;
    assert_eq!(a.rows("workspace"), b.rows("workspace"));
    a.call(
        "register",
        json!({"projectId":"P","name":"Project","workspaceId":"W","roots":[]}),
    )
    .await
    .unwrap();
    fake.script("consent.await", Effect::Answer("approve_once"));
    let body = serde_json::to_vec(&json!({"name":"agents_create_head","arguments":{"name":"Head","tag":"head","project_id":"P","request_key":"head"}})).unwrap();
    let outcome = drive(a.handler.handle_served_request(&body, tool)).await;
    assert!(
        matches!(outcome, HandlerOutcome::Response(_)),
        "{outcome:?}"
    );
    let head = a
        .store()
        .agent_snapshot()
        .unwrap()
        .agents
        .into_iter()
        .find(|row| row.role == "head")
        .unwrap()
        .agent_id;
    a.handler.route_admissions().insert(
        tool,
        RouteAdmission::from_bind(
            Some(Principal::Reserved {
                module_id: "prefrontal".into(),
            }),
            Some(&stamp(&head)),
        ),
    );
    fake.script("consent.await", Effect::Answer("approve_always_project"));
    let body = serde_json::to_vec(&json!({"name":"agents_hire","arguments":{"name":"Worker","tag":"worker","labels":["build"],"request_key":"hire"}})).unwrap();
    let outcome = drive(a.handler.handle_served_request(&body, tool)).await;
    assert!(
        matches!(outcome, HandlerOutcome::Response(_)),
        "{outcome:?}"
    );
    b.catch_up().await;
    assert_eq!(a.rows("agent"), b.rows("agent"));
    let snapshot = b.store().agent_snapshot().unwrap();
    assert_eq!(snapshot.agents.len(), 3);
    for row in &snapshot.agents {
        crate::shared_write::assert_wild_avatar(&serde_json::to_value(&row.avatar).unwrap());
        let changes = b.store().agent_changes(0, None).unwrap();
        let entries: Vec<_> = changes
            .entries
            .iter()
            .filter(|entry| entry.agent_id == row.agent_id)
            .collect();
        assert_eq!(entries.len(), 1, "avatar must arrive in the create entry");
        assert_eq!(entries[0].op, "agent.create");
        assert_eq!(entries[0].row.avatar, row.avatar);
        assert_eq!(row.agent_generation, 1);
    }
    assert!(
        matches!(b.store().agent_changes(0,None).unwrap().entries.last().unwrap().authorization,Some(Authorization::Card { ref answer, .. }) if answer == "approve_always_project")
    );
    for table in [
        "consent_request",
        "consent_write_plan",
        "consent_grant",
        "agent_audit",
    ] {
        assert!(b.rows(table).is_empty(), "local table {table} was synced");
    }
    assert_eq!(a.rows("consent_grant").len(), 1);
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn catalog_pins_round_trip_shared_writer_and_stay_out_of_agent_feed() {
    use entorhinal_core::catalog_agent::{CatalogPin, PinWriteRequest};
    let log = Arc::new(FakeLog::default());
    let (a, b) = pair("catalog-pins", log.clone()).await;
    let created = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Ada","tag":"catalog","request_key":"create"}),
        )
        .await
        .unwrap();
    let id = created["agent"]["agent_id"].as_str().unwrap();
    b.catch_up().await;
    let cursor = b.store().agent_snapshot().unwrap().generation;
    let pins = vec![CatalogPin {
        name: "@acme/a".into(),
        version: "1.0.0".into(),
        kind: entorhinal_catalog::Kind::Skill,
        manifest_sha256: "11".repeat(32),
        archive_sha256: "22".repeat(32),
        publisher_fingerprint: "33".repeat(32),
        root: true,
        range: Some("^1".into()),
    }];
    let req = PinWriteRequest {
        agent_id: id.into(),
        pins: pins.clone(),
        request_key: Some("pins".into()),
    };
    let reply = drive(a.handler.catalog_pins_write_wait(
        req.clone(),
        CORE,
        tokio::time::Instant::now(),
    ))
    .await
    .unwrap();
    assert_eq!(
        reply,
        drive(
            a.handler
                .catalog_pins_write_wait(req, CORE, tokio::time::Instant::now())
        )
        .await
        .unwrap()
    );
    b.catch_up().await;
    assert_eq!(a.store().catalog_pins(id).unwrap(), pins);
    assert_eq!(b.store().catalog_pins(id).unwrap(), pins);
    assert_eq!(
        a.store().catalog_pin_digest(id).unwrap(),
        b.store().catalog_pin_digest(id).unwrap()
    );
    let feed = b.store().agent_changes(cursor, Some(1)).unwrap();
    assert!(
        feed.entries.is_empty(),
        "catalog pin escaped into identity feed"
    );
    assert!(feed.cursor > cursor);
    assert_eq!(a.shared(), b.shared());
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn two_machines_receive_every_avatar_manifest_entry() {
    let log = Arc::new(FakeLog::default());
    let mut a = Machine::new("avatar-manifest-a", log.clone());
    let b = Machine::new("avatar-manifest-b", log.clone());
    let confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    a.handler.operator_confirmer = confirmer.clone();
    let source = a.dir.join("empty-agents.db");
    Connection::open(&source)
        .unwrap()
        .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
        .unwrap();
    a.call(
        "agent.import",
        json!({"snapshot_path":source,"request_key":"import"}),
    )
    .await
    .unwrap();
    let ada = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Ada","tag":"helper","request_key":"ada"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .clone();
    let grace = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Grace","tag":"helper","request_key":"grace"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .clone();
    a.enable().await;
    b.enable().await;
    assert_eq!(a.shared(), b.shared());
    let cursor = b.store().generation().unwrap();
    let head = log.head();
    // New agents already have avatars, so exercise manifest replication by
    // explicitly replacing those defaults rather than skipping seeded rows.
    let params = json!({"request_key":"avatars","replace":true,"entries":[
        {"agent_id":ada,"avatar_genome":"a".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1},
        {"agent_id":grace,"avatar_genome":"b".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1}
    ]});
    let first = a
        .bytes_on(DIRECT, "agent.apply_avatar_manifest", params.clone())
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&first).unwrap()["result"]["applied"],
        2
    );
    assert_eq!(
        log.head(),
        head + 2,
        "each avatar uses a standard shared write"
    );
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    let entries = b.store().agent_changes(cursor, None).unwrap().entries;
    assert_eq!(entries.len(), 2);
    for (id, genome) in [(&ada, "a".repeat(32_768)), (&grace, "b".repeat(32_768))] {
        let change = entries
            .iter()
            .find(|change| json!(change.agent_id) == *id)
            .unwrap();
        assert_eq!(change.op, "agent.set_avatar");
        let avatar = change.row.avatar.as_ref().unwrap();
        assert_eq!(avatar.genome, genome);
        assert_eq!(avatar.avatar_type, "creature.wild");
        assert_eq!(avatar.version, Some(1));
    }
    assert_eq!(
        a.bytes_on(DIRECT, "agent.apply_avatar_manifest", params)
            .await
            .unwrap(),
        first
    );
    assert_eq!(log.head(), head + 2);
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 1);
    assert!(a.store().verify().unwrap().ok);
    assert!(b.store().verify().unwrap().ok);
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[derive(Clone)]
struct InterruptAvatarLog {
    log: Arc<FakeLog>,
    remaining: Arc<AtomicUsize>,
}

// First-poll checks observe async suspension without measuring elapsed time.
fn first_poll<F: Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

async fn avatar_incident_pair() -> (Machine, Machine, Arc<FakeLog>, Value, String) {
    let log = Arc::new(FakeLog::default());
    let mut a = Machine::new("avatar-incident-a", log.clone());
    let b = Machine::new("avatar-incident-b", log.clone());
    a.handler.operator_confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    let resolver = sessions::FakeResolver::new();
    for capability in [sessions::ORCHESTRATOR_CAPABILITY, sessions::HOST_CAPABILITY] {
        resolver.set(
            capability,
            sessions::ProviderResolution::Provider(WRITER_MODULE.into()),
        );
    }
    a.handler.provider_resolver = resolver;
    a.store()
        .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let mut entries = Vec::new();
    for index in 0..32 {
        let agent = a.call("agent.create", json!({"role":"assistant","name":format!("Avatar {index}"),"tag":"test","request_key":format!("create-{index}")})).await.unwrap();
        entries.push(json!({"agent_id":agent["agent"]["agent_id"],"avatar_genome":"ab".repeat(16_384),"avatar_type":"creature.wild","avatar_version":1}));
    }
    let id = entries[0]["agent_id"].as_str().unwrap().to_owned();
    a.enable().await;
    b.enable().await;
    (
        a,
        b,
        log,
        json!({"request_key":"avatars","replace":true,"entries":entries}),
        id,
    )
}

fn registration_body(id: &str) -> Vec<u8> {
    serde_json::to_vec(&json!({"method":sessions::REGISTER_SESSION,"params":{
        "agent_id":id,"ref_kind":"pi","ref":"avatar-ref","harness":"pi","session_id":"avatar-session","runner":"pi","project_root":"/project"
    }})).unwrap()
}

#[tokio::test(start_paused = true)]
async fn avatar_batch_keeps_one_runtime_worker_available_and_local_writes_bounded() {
    assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
    let (a, b, log, params, id) = avatar_incident_pair().await;
    a.bytes_on(
        CORE,
        sessions::REGISTER_SESSION,
        serde_json::from_slice::<Value>(&registration_body(&id)).unwrap()["params"].clone(),
    )
    .await
    .unwrap();
    let head = log.head();
    let calls = log.calls();
    for _ in 0..32 {
        log.on_append(crate::fake_log::Action::Hold);
    }
    let batch_body =
        serde_json::to_vec(&json!({"method":"agent.apply_avatar_manifest","params":params}))
            .unwrap();
    let mut batch = Box::pin(a.handler.handle_served_request(&batch_body, DIRECT));
    for index in 0..32 {
        drive(async {
            tokio::select! {
                biased;
                () = log.wait_for_calls(calls + (index + 1) * 2) => {},
                result = &mut batch => panic!("batch finished before append {index}: {result:?}"),
            }
        })
        .await;
        let frozen = tokio::time::Instant::now();
        let body = registration_body(&id);
        let mut registration = Box::pin(a.handler.handle_served_request(&body, CORE));
        let unrelated_body = serde_json::to_vec(&json!({"method":"register","params":{"projectId":"unrelated","name":"unrelated","roots":[],"requestKey":"unrelated"}})).unwrap();
        let mut unrelated = Box::pin(a.handler.handle_served_request(&unrelated_body, DIRECT));
        assert!(first_poll(registration.as_mut()).is_pending());
        assert!(first_poll(unrelated.as_mut()).is_pending());
        for (method, params) in [
            ("resolve", json!({"canonicalRoot":a.dir})),
            ("enumerate", json!({})),
            (
                sessions::SESSION_FOR_REF,
                json!({"ref_kind":"pi","ref":"avatar-ref"}),
            ),
            ("agent.fleet_identity", json!({})),
            ("agent.avatar_read", json!({"agentIds":[id]})),
            (
                "agent.session_liveness",
                json!({"refs":[{"ref_kind":"pi","ref":"avatar-ref"}]}),
            ),
        ] {
            let mut read = Box::pin(a.bytes_on(CORE, method, params));
            assert!(
                matches!(first_poll(read.as_mut()), std::task::Poll::Ready(Ok(_))),
                "{method} waited during append {index}"
            );
        }
        let mut health = Box::pin(a.handler.health());
        assert!(
            first_poll(health.as_mut()).is_ready(),
            "health queued behind append"
        );
        if index == 0 {
            tokio::time::advance(Duration::from_secs(25) + Duration::from_millis(1)).await;
            assert!(
                matches!(first_poll(registration.as_mut()), std::task::Poll::Ready(HandlerOutcome::Error { ref code, .. }) if code == "identity_log_contended")
            );
            assert!(
                matches!(first_poll(unrelated.as_mut()), std::task::Poll::Ready(HandlerOutcome::Error { ref code, .. }) if code == "identity_log_contended")
            );
        } else {
            assert_eq!(tokio::time::Instant::now(), frozen);
        }
        log.release_next().unwrap();
    }
    assert!(matches!(
        drive(&mut batch).await,
        HandlerOutcome::Response(_)
    ));
    assert_eq!(log.head(), head + 32);
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert!(a.store().verify().unwrap().ok);
    assert!(b.store().verify().unwrap().ok);
}

#[tokio::test(start_paused = true)]
async fn avatar_batch_drain_interrupts_append_without_waiting_for_network_and_resumes() {
    let (mut a, b, log, params, _id) = avatar_incident_pair().await;
    let head = log.head();
    let calls = log.calls();
    log.on_append(crate::fake_log::Action::Hold);
    let mut batch = Box::pin(a.bytes_on(DIRECT, "agent.apply_avatar_manifest", params.clone()));
    drive(async {
        tokio::select! {
            biased;
            () = log.wait_for_calls(calls + 2) => {},
            result = &mut batch => panic!("batch finished before append: {result:?}"),
        }
    })
    .await;
    let frozen = tokio::time::Instant::now();
    a.handler
        .on_draining(
            subc_client_rs::RouteCloseReason::Restart,
            std::time::SystemTime::now(),
        )
        .await;
    assert!(
        matches!(first_poll(batch.as_mut()), std::task::Poll::Ready(Err(ref code)) if code == "module_draining"),
        "drain must interrupt the batch without advancing the clock"
    );
    drop(batch);
    drop(drive(a.handler.writer.lock()).await);
    assert_eq!(tokio::time::Instant::now(), frozen);
    assert_eq!(
        a.store().identity_log_status().unwrap().pending_write_count,
        1
    );
    assert_eq!(log.head(), head);
    // A lost append reply can arrive after cancellation. Replay must settle it
    // from the log, then resume only the remaining manifest entries.
    log.release_next().unwrap();
    b.catch_up().await;
    a.restart_legacy(log.clone()).await;
    a.handler.operator_confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    a.catch_up().await;
    let body = serde_json::to_vec(&json!({"method":"agent.apply_avatar_manifest","params":params}))
        .unwrap();
    let resumed = drive(a.handler.handle_served_request(&body, DIRECT)).await;
    let HandlerOutcome::Response(bytes) = resumed else {
        panic!("{resumed:?}")
    };
    let result: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(result["result"]["skipped"].as_array().unwrap().len(), 1);
    assert_eq!(result["result"]["written"].as_array().unwrap().len(), 31);
    assert_eq!(log.head(), head + 32);
    b.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert!(b.store().verify().unwrap().ok);
    crate::tests::stop_store_tasks(&a.handler).await;
    crate::tests::stop_store_tasks(&b.handler).await;
}

#[tokio::test(start_paused = true)]
async fn cold_fleet_and_avatar_reads_do_not_wait_for_background_log_read() {
    let (mut a, b, log, _, id) = avatar_incident_pair().await;
    a.restart_legacy(log.clone()).await;
    let calls = log.calls();
    log.on_read(crate::fake_log::Action::Hang);
    a.handler.start_catch_up();
    drive(log.wait_for_calls(calls + 1)).await;
    assert!(a.handler.writer.try_lock().is_err());
    for method in [
        "agent.fleet_identity",
        "agent.avatar_read",
        "resolve",
        "enumerate",
    ] {
        let params = match method {
            "agent.avatar_read" => json!({"agentIds":[id]}),
            "resolve" => json!({"canonicalRoot":a.dir}),
            _ => json!({"token":"old-incarnation:0"}),
        };
        let mut read = Box::pin(a.bytes_on(DIRECT, method, params));
        assert!(
            matches!(first_poll(read.as_mut()), std::task::Poll::Ready(Ok(_))),
            "cold {method} waited for the log"
        );
        assert_eq!(log.calls(), calls + 1, "read contacted the shared log");
    }
    crate::tests::stop_store_tasks(&a.handler).await;
    crate::tests::stop_store_tasks(&b.handler).await;
}

#[async_trait]
impl LogConnector for InterruptAvatarLog {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait]
impl LogTransport for InterruptAvatarLog {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        if method == log_client::APPEND
            && self
                .remaining
                .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                == Ok(1)
        {
            // A definitive refusal stops settlement immediately without advancing
            // the paused clock; the next attempt uses the healthy log again.
            self.log.on_append(crate::fake_log::Action::Refuse {
                code: "id_reused".into(),
                detail: None,
            });
        }
        self.log.call(method, params).await
    }
}

#[tokio::test(start_paused = true)]
async fn two_machines_resume_interrupted_avatar_manifest_without_duplicates() {
    let log = Arc::new(FakeLog::default());
    let connector = Arc::new(InterruptAvatarLog {
        log: log.clone(),
        remaining: Arc::new(AtomicUsize::new(0)),
    });
    let mut a = Machine::new("avatar-resume-a", connector.clone());
    let b = Machine::new("avatar-resume-b", log.clone());
    let confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    a.handler.operator_confirmer = confirmer.clone();
    let source = a.dir.join("empty-agents.db");
    Connection::open(&source)
        .unwrap()
        .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
        .unwrap();
    a.call(
        "agent.import",
        json!({"snapshot_path":source,"request_key":"import"}),
    )
    .await
    .unwrap();
    let ada = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Ada","tag":"helper","request_key":"ada"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .clone();
    let grace = a
        .call(
            "agent.create",
            json!({"role":"assistant","name":"Grace","tag":"helper","request_key":"grace"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .clone();
    a.enable().await;
    b.enable().await;
    let cursor = b.store().generation().unwrap();
    let head = log.head();
    let params = json!({"request_key":"avatars","replace":true,"entries":[
        {"agent_id":ada,"avatar_genome":"a".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1},
        {"agent_id":grace,"avatar_genome":"b".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1}
    ]});
    connector.remaining.store(2, Ordering::SeqCst);
    let body = serde_json::to_vec(&json!({"method":"agent.apply_avatar_manifest","params":params}))
        .unwrap();
    let outcome = drive(a.handler.handle_served_request(&body, DIRECT)).await;
    let HandlerOutcome::ErrorWithDetail {
        message, detail, ..
    } = outcome
    else {
        panic!("expected interruption")
    };
    assert_eq!(detail["batch"]["written"].as_array().unwrap().len(), 1);
    assert_eq!(detail["batch"]["remaining"].as_array().unwrap().len(), 1);
    assert!(
        message.contains("written:")
            && message.contains("remaining:")
            && message.contains("Ada")
            && message.contains("Grace")
    );
    assert_eq!(log.head(), head + 1);
    b.catch_up().await;
    assert_eq!(
        b.store().agent_changes(cursor, None).unwrap().entries.len(),
        1
    );
    let resumed = a
        .bytes_on(DIRECT, "agent.apply_avatar_manifest", params.clone())
        .await
        .unwrap();
    let reply: Value = serde_json::from_slice(&resumed).unwrap();
    assert_eq!(reply["result"]["written"].as_array().unwrap().len(), 2);
    assert!(reply["result"]["remaining"].as_array().unwrap().is_empty());
    assert_eq!(log.head(), head + 2);
    b.catch_up().await;
    let entries = b.store().agent_changes(cursor, None).unwrap().entries;
    assert_eq!(entries.len(), 2);
    assert!(entries.iter().all(|e| e.op == "agent.set_avatar"));
    assert_ne!(entries[0].agent_id, entries[1].agent_id);
    assert_eq!(a.shared(), b.shared());
    assert_eq!(
        confirmer.summaries.lock().unwrap().len(),
        2,
        "interrupted batch confirms its remaining work once"
    );
    assert_eq!(
        a.bytes_on(DIRECT, "agent.apply_avatar_manifest", params)
            .await
            .unwrap(),
        resumed
    );
    assert_eq!(log.head(), head + 2);
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 2);
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn memory_policy_round_trips_between_two_machines_and_rebuilds() {
    let log = Arc::new(FakeLog::default());
    let connector: Arc<dyn LogConnector> = log;
    let mut a = Machine::new("memory-policy-a", connector.clone());
    let mut b = Machine::new("memory-policy-b", connector);
    for machine in [&mut a, &mut b] {
        machine.handler.operator_confirmer = Arc::new(crate::served_vectors::Approve {
            summaries: Mutex::new(vec![]),
        });
    }
    a.enable_without_agents().await;
    b.enable_without_agents().await;
    let root = a.repo("project", &[]);
    a.call("register",json!({"projectId":"P","name":"Project","workspaceId":"W","roots":[root],"domain":"@example/software"})).await.unwrap();
    b.catch_up().await;
    assert_eq!(
        b.query("SELECT domain FROM project WHERE project_id='P'"),
        vec![vec![Cell::Text("@example/software".into())]]
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
    for revision in 0..3 {
        let writer = if revision == 0 { &a } else { &b };
        writer.call("workspace.set_document",json!({"workspace_id":"W","expected_revision":revision,"charter":if revision==2 {"\t".repeat(4096)}else{"Purpose".into()},"document":if revision==2 {"\"".repeat(65536)}else{format!("Policy {revision}")},"request_key":format!("document-{revision}")})).await.unwrap();
        a.catch_up().await;
        b.catch_up().await;
        assert_eq!(a.rows("workspace_document"), b.rows("workspace_document"));
        assert_eq!(
            a.store().workspace_document("W", None).unwrap().revision,
            revision + 1
        );
        a.clean_rebuild().await;
        b.clean_rebuild().await;
    }
    b.call(
        "project.set_domain",
        json!({"project_id":"P","domain":"@example/research","request_key":"change-domain"}),
    )
    .await
    .unwrap();
    a.catch_up().await;
    assert_eq!(
        a.query("SELECT domain FROM project WHERE project_id='P'"),
        vec![vec![Cell::Text("@example/research".into())]]
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
    b.call(
        "project.set_domain",
        json!({"project_id":"P","domain":null,"request_key":"clear-domain"}),
    )
    .await
    .unwrap();
    a.catch_up().await;
    assert_eq!(a.shared(), b.shared());
    assert_eq!(
        a.query("SELECT domain FROM project WHERE project_id='P'"),
        vec![vec![Cell::Null]]
    );
    assert!(b.rows("project_root").is_empty());
    let context = a.store().context_project(&root, "fixed").unwrap();
    assert_eq!(
        context["stamp"]["logPosition"],
        a.store()
            .identity_log_status()
            .unwrap()
            .last_applied_position
    );
    assert_eq!(
        context["members_digest"],
        entorhinal_catalog::sha256_hex("P")
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn adopt_store_shared_restore_rejoins_with_new_author_and_installs_agents() {
    let log = FakeLog::default();
    let a = Machine::new("origin-source", Arc::new(log.with_author([0x11; 16])));
    let root = a.repo("project", &[("origin", "origin/project")]);
    a.register("P", &root, Some("W")).await;
    a.enable_without_agents().await;
    a.call(
        "agent.create",
        json!({"role":"head","name":"Ada","tag":"head","project_id":"P","request_key":"head"}),
    )
    .await
    .unwrap();
    a.store()
        .check_store_origin(Some("11111111111111111111111111111111"), 1)
        .unwrap();
    let backup = a.dir.join("restore.db");
    a.conn()
        .execute("VACUUM INTO ?1", [backup.to_str().unwrap()])
        .unwrap();
    let b = Machine::restore(
        "origin-restored",
        &backup,
        Arc::new(log.with_author([0x22; 16])),
        crate::tests::allow_legacy_store,
    );
    let store = b.store();
    store
        .check_store_origin(Some("22222222222222222222222222222222"), 2)
        .unwrap();
    assert!(store.is_foreign());
    store
        .adopt_store("22222222222222222222222222222222", false, 3)
        .unwrap();
    assert!(store.enable_registry_is_empty().unwrap());
    store.rebuild().unwrap();
    assert!(store.enable_registry_is_empty().unwrap());
    assert!(store.verify().unwrap().ok);
    assert!(store
        .journal_tail(0, 1000)
        .unwrap()
        .entries
        .iter()
        .all(|e| e.op != "store.adopt"));
    b.enable().await;
    assert!(!b.rows("agent").is_empty());
    assert_eq!(b.shared(), a.shared());
    b.call("register", json!({"projectId":"B","name":"B","roots":[]}))
        .await
        .unwrap();
    b.call("member.set_name", json!({"display_name":"New machine"}))
        .await
        .unwrap();
    assert_eq!(
        b.store().persisted_self_author().unwrap().as_deref(),
        Some("22222222222222222222222222222222")
    );
    b.clean_rebuild().await;
}

#[tokio::test(start_paused = true)]
async fn placement_facts_two_machine_snapshot_retry_skew_and_rebuild() {
    let log = FakeLog::default();
    let mut a = Machine::new("placement-a", Arc::new(log.clone()));
    a.handler.operator_confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: std::sync::Mutex::new(vec![]),
    });
    a.handler.clock = || 900;
    a.store()
        .with_principal("test")
        .register_at(
            entorhinal_core::RegisterRequest {
                name: "minted".into(),
                workspace_id: Some("W1".into()),
                ..Default::default()
            },
            900,
        )
        .unwrap();
    let initial = a.call("enumerate", json!({})).await.unwrap();
    let project = initial["projects"][0]["projectId"]
        .as_str()
        .unwrap()
        .to_owned();
    a.store()
        .with_principal("test")
        .assign_workspace_at(
            entorhinal_core::AssignWorkspaceRequest {
                project_id: project.clone(),
                workspace_id: "W2".into(),
                ..Default::default()
            },
            900,
        )
        .unwrap();
    assert_eq!(
        a.call("placement.read", json!({})).await.unwrap()["facts"],
        json!([])
    );
    a.enable_without_agents().await;
    let first = a
        .call("placement.backfill", json!({"requestKey":"backfill"}))
        .await
        .unwrap();
    assert_eq!(first["facts"], 2);
    let history = a.call("placement.read", json!({})).await.unwrap();
    let author = a.call("node.self", json!({})).await.unwrap();
    let identity = author["author"].as_str().unwrap();
    assert_eq!(
        history["facts"][0]["fact_id"],
        format!("b:{identity}:1:{project}")
    );
    assert_eq!(history["facts"][0]["workspace_id"], "W1");
    assert_eq!(history["facts"][1]["workspace_id"], "W2");
    // Superseding snapshots must carry the same facts as the change entry.
    let prefix = log.client().read(0, 128).await.unwrap();
    let remote = |entry: &crate::log_client::LogEntry| entorhinal_core::remote_apply::RemoteEntry {
        position: entry.position as i64,
        entry_id: crate::log_client::encode_hex(&entry.entry_id),
        author: entry.author.clone(),
        signer: crate::log_client::encode_hex(&entry.signer),
        key_id: json!({"family":entry.key_id.family,"epoch":entry.key_id.epoch}).to_string(),
        envelope_version: entry.envelope_version as i64,
        kind: entry.kind.clone(),
        entry: entry.entry.clone(),
    };
    let replay_prefix = prefix.entries.iter().map(remote).collect::<Vec<_>>();
    let plan = a
        .store()
        .prepare_supersede(&"ef".repeat(16), 900, prefix.head, &replay_prefix)
        .unwrap();
    assert_eq!(plan.bodies.len(), 1);
    let image: Value = serde_json::from_slice(&plan.bodies[0]).unwrap();
    assert_eq!(
        image["tables"]["placement_fact"]["upsert"]
            .as_array()
            .unwrap()
            .len(),
        2
    );
    assert_eq!(
        image["tables"]["placement_state"]["upsert"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    log.client()
        .append(&crate::log_client::AppendRequest {
            expected_head: prefix.head,
            entry_id: [0xee; 16],
            kind: crate::log_client::EntryKind::Snapshot,
            data: plan.bodies.into_iter().next().unwrap(),
        })
        .await
        .unwrap();
    a.catch_up().await;
    let mut b = Machine::new("placement-b", Arc::new(log.with_author([0x22; 16])));
    b.enable().await;
    assert_eq!(
        b.query("SELECT COUNT(*) FROM registry_journal WHERE op='shared.snapshot' AND instr(CAST(entry AS TEXT),'placement_fact')>0"),
        vec![vec![Cell::Integer(1)]]
    );
    assert_eq!(a.rows("placement_fact"), b.rows("placement_fact"));
    assert_eq!(a.rows("placement_state"), b.rows("placement_state"));
    assert_eq!(b.call("placement.read", json!({})).await.unwrap(), history);
    b.handler.clock = || 100;
    let params = json!({"projectId":project,"workspaceId":"W3","requestKey":"skew"});
    let response = b.call("assign_workspace", params.clone()).await.unwrap();
    assert_eq!(b.call("assign_workspace", params).await.unwrap(), response);
    a.catch_up().await;
    assert_eq!(a.rows("placement_fact"), b.rows("placement_fact"));
    let page = a.call("placement.read", json!({})).await.unwrap();
    assert_eq!(page["facts"].as_array().unwrap().len(), 3);
    assert_eq!(page["facts"][2]["recorded_at_ms"], 100);
    assert_eq!(page["facts"][2]["effective_from_ms"], 900);
    let entries = log.client().read(0, 128).await.unwrap();
    let committed = entries.entries.last().unwrap();
    assert_eq!(
        page["facts"][2]["fact_id"],
        format!(
            "e:{}:{project}",
            crate::log_client::encode_hex(&committed.entry_id)
        )
    );
    a.clean_rebuild().await;
    b.clean_rebuild().await;
    assert_eq!(
        a.call("placement.backfill", json!({})).await.unwrap_err(),
        "placement_already_backfilled"
    );
    assert!(a.bytes_on(CORE, "placement.read", json!({})).await.is_ok());
    assert_eq!(
        a.bytes_on(CORE, "placement.backfill", json!({}))
            .await
            .unwrap_err(),
        "write_not_permitted"
    );
    for params in [
        json!({"limit":1025}),
        json!({"limit":0}),
        json!({"bogus":1}),
        json!({"after":"bad"}),
    ] {
        assert_eq!(
            a.call("placement.read", params).await.unwrap_err(),
            "invalid_request"
        );
    }
    let page1 = a.call("placement.read", json!({"limit":1})).await.unwrap();
    let page2 = a
        .call("placement.read", json!({"limit":2,"after":page1["next"]}))
        .await
        .unwrap();
    assert_eq!(page1["through"], 1);
    assert_eq!(page2["through"], 3);
    assert_eq!(page2["next"], Value::Null);
    let foreign = b
        .call("placement.read", json!({"after":page1["next"],"limit":1}))
        .await
        .unwrap();
    // Give the other process a different incarnation before exercising restart.
    b.handler.incarnation = "other-process".into();
    let restarted = b
        .call("placement.read", json!({"after":page1["next"],"limit":1}))
        .await
        .unwrap();
    assert_eq!(foreign["through"], 2);
    assert_eq!(restarted["through"], 1);
}

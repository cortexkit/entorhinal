//! Acceptance through the serving path: independent stores, admitted routes,
//! and one serialized wire log. SQL assertions inspect the persisted cells,
//! independently of the replication encoder and replay comparator.

use super::*;
use crate::fake_log::{FailConnector, FakeLog};
use crate::log_client::{LogConnector, LogTransport, TransportError};
use rusqlite::{types::Value as Cell, Connection, OpenFlags};
use std::{future::Future, path::Path, sync::atomic::AtomicUsize};

const DIRECT: RouteKey = (90, 1);
const CORE: RouteKey = (91, 1);
const SHARED: &[&str] = &[
    "project",
    "workspace",
    "project_workspace",
    "project_alias",
    "agent",
    "agent_name_claim",
    "project_root_key",
    "member_name",
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
        install_store(&handler.store, &handler.health, Ok(store));
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

    fn restart(&mut self, connector: Arc<dyn LogConnector>) {
        self.handler.store.lock().unwrap().take();
        self.handler = Self::open(&self.descriptor, connector);
    }

    fn restore(label: &str, snapshot: &Path, connector: Arc<dyn LogConnector>) -> Self {
        let (dir, descriptor) = crate::tests::scratch_descriptor(label);
        let StorageBackend::Sqlite { path } = &descriptor.backend else {
            unreachable!()
        };
        // The snapshot is a closed SQLite backup, not an open store whose WAL
        // might be missing. Restore it before opening the new handler.
        std::fs::copy(snapshot, path).unwrap();
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
        std::fs::remove_dir_all(&self.dir).unwrap();
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
async fn s3_names_replicate_to_where_and_survive_supersede_for_late_joiner() {
    use crate::{
        fake_record::{FakeRecord, FakeRecordConnector},
        record_client::RecordClient,
    };
    let root = FakeLog::default();
    let a_log = Arc::new(root.with_author([0x11; 16]));
    let b_log = Arc::new(root.with_author([0x22; 16]));
    let a_record = FakeRecord::new([0x11; 16]);
    let b_record = FakeRecord::new([0x22; 16]);
    b_record.offline.store(true, Ordering::Relaxed);
    let a = Machine::new_with_record(
        "name-a",
        a_log.clone(),
        RecordClient::new(Arc::new(FakeRecordConnector(a_record))),
    );
    let b = Machine::new_with_record(
        "name-b",
        b_log.clone(),
        RecordClient::new(Arc::new(FakeRecordConnector(b_record.clone()))),
    );
    a.enable_without_agents().await;
    b.enable_without_agents().await;
    assert_eq!(
        a.store().persisted_self_author().unwrap(),
        Some("11".repeat(16))
    );
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
    let created = a
        .call(
            "agent.create",
            json!({"request_key":"named-agent","name":"Ada","tag":"test","role":"assistant"}),
        )
        .await
        .unwrap();
    let agent = created["agent"]["agent_id"].as_str().unwrap();
    b.catch_up().await;
    assert_eq!(
        b.rows("member_name"),
        vec![vec![
            Cell::Text("11".repeat(16)),
            Cell::Text("Studio".into())
        ]]
    );
    let claim = serde_json::to_vec(&json!({"v":1,"agent_id":agent,"holder":"11".repeat(16),"epoch":1,"cut_id":null,"reason":null})).unwrap();
    b_record.put(
        &format!("claim:{agent}"),
        1,
        claim.clone(),
        &"11".repeat(16),
    );
    b_record.offline.store(false, Ordering::Relaxed);
    b.bytes_on(DIRECT, "agent.refresh_claims", json!({}))
        .await
        .unwrap();
    let where_reply = b
        .call("agent.where", json!({"agent_id":agent}))
        .await
        .unwrap();
    assert_eq!(where_reply["status"], "held_elsewhere");
    assert_eq!(where_reply["holder_name"], "Studio");

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
    let c_record = FakeRecord::new([0x33; 16]);
    c_record.put(&format!("claim:{agent}"), 1, claim, &"11".repeat(16));
    c_record.offline.store(true, Ordering::Relaxed);
    let c = Machine::new_with_record(
        "name-late",
        Arc::new(root.with_author([0x33; 16])),
        RecordClient::new(Arc::new(FakeRecordConnector(c_record.clone()))),
    );
    c.enable_without_agents().await;
    assert_eq!(c.rows("member_name"), b.rows("member_name"));
    assert_eq!(c.store().persisted_self_author().unwrap(), None);
    c_record.offline.store(false, Ordering::Relaxed);
    c.bytes_on(DIRECT, "agent.refresh_claims", json!({}))
        .await
        .unwrap();
    assert_eq!(
        c.call("agent.where", json!({"agent_id":agent}))
            .await
            .unwrap()["holder_name"],
        "Studio"
    );
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
    use std::task::{Context, Poll, Waker};

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

    let a2 = Machine::restore("incident-a2", &backup, author_a.clone());
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
    let Poll::Ready(HandlerOutcome::Response(bytes)) = waiter
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
    else {
        panic!("supersede did not notify the parked agent.changes waiter")
    };
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
    m.restart(absent.clone());
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
    m.restart(log.clone());
    let committed = m.bytes_on(CORE, "register", request.clone()).await.unwrap();
    let journal = m.rows("registry_journal");
    m.restart(Arc::new(FailConnector::default()));
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
    m.restart(fail.clone());
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
    use super::*;
    use crate::{
        door_client::{CutItem, Request as DoorRequest, SealState},
        fake_daemon::{DaemonSnapshot, FakeDaemon},
        fake_door::{Effect as DoorEffect, FakeDoor},
        fake_participant::{Effect, FakeParticipant},
        fake_record::FakeRecord,
        move_driver::{SqlOperations, Worker},
        move_receiver::{poll_inbox, CutAccept, Receiver},
        move_source::Source,
        participant_client::{AgentRef, BusyRef, BusyState, PrepareReply, Refusal, Request},
        participant_discovery::{provider_role, ParticipantDiscovery},
        record_client::{RecordClient, RecordReply},
        sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
    };
    use async_trait::async_trait;
    use entorhinal_core::{
        claim_cache::ClaimObservation,
        move_record::{self as wire, MoveRecord, MoveRecordState as MoveState},
        move_store::{AgentMove, MoveStep},
        SessionRegistration,
    };
    use subc_client_rs::{CatalogList, SupervisorListResponse};

    use std::collections::BTreeSet;

    const AGENT: &str = "agent_0123456789abcdef";
    const A: &str = "01010101010101010101010101010101";
    const B: &str = "02020202020202020202020202020202";
    thread_local! { static NOW: std::cell::Cell<i64> = const { std::cell::Cell::new(1000) }; }
    fn clock() -> i64 {
        NOW.with(|n| n.get())
    }
    fn elapse(ms: i64) {
        NOW.with(|n| n.set(n.get() + ms));
    }

    fn snapshot(modules: &[(&str, &str)], ready: bool, role: bool) -> DaemonSnapshot {
        DaemonSnapshot::new(1,
            modules.iter().map(|(module, _)| json!({"module_id":module,"state":"running","enabled":true,"live":true,"health":"ok"})).collect(),
            modules.iter().map(|(module, _)| json!({"module_id":module,"ready":ready,"not_ready":null,"control_ops":[],"roles":if role { vec![provider_role()] } else { vec![] },
                "capabilities":{"provides":["agent-data/v1"],"requires":[],"must_never_reach":[]}})).collect())
    }

    struct PeerGate {
        trap: Mutex<Option<(String, String, bool)>>,
        hit: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }
    struct PeerTransport {
        module: String,
        fake: Arc<FakeParticipant>,
        gate: Arc<PeerGate>,
    }
    #[async_trait]
    impl LogTransport for PeerTransport {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let trap = {
                let mut t = self.gate.trap.lock().unwrap();
                if t.as_ref()
                    .is_some_and(|(module, op, _)| module == &self.module && op == method)
                {
                    t.take()
                } else {
                    None
                }
            };
            if trap.as_ref().is_some_and(|(_, _, after)| !after) {
                self.gate.hit.add_permits(1);
                self.gate.release.acquire().await.unwrap().forget();
            }
            let reply = self.fake.call(method, params).await;
            if trap.is_some_and(|(_, _, after)| after) {
                self.gate.hit.add_permits(1);
                self.gate.release.acquire().await.unwrap().forget();
            }
            reply
        }
    }
    struct Discovery {
        daemon: FakeDaemon,
        peers: BTreeMap<String, Arc<FakeParticipant>>,
        gate: Arc<PeerGate>,
    }
    #[async_trait]
    impl ParticipantDiscovery for Discovery {
        async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
            self.daemon.supervisor_list().await
        }
        async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
            self.daemon.catalog_list().await
        }
        async fn open_participant(
            &self,
            module: &str,
        ) -> Result<Arc<dyn LogTransport>, TransportError> {
            self.daemon.open_participant(module).await?;
            Ok(Arc::new(PeerTransport {
                module: module.into(),
                fake: self.peers[module].clone(),
                gate: self.gate.clone(),
            }))
        }
    }

    struct Trap {
        method: String,
        subject: String,
        after: bool,
    }
    struct Records {
        fake: Arc<FakeRecord>,
        calls: Mutex<Vec<(String, Value)>>,
        trap: Mutex<Option<Trap>>,
        append_barrier: Mutex<Option<Arc<tokio::sync::Barrier>>>,
        hit: tokio::sync::Semaphore,
        release: tokio::sync::Semaphore,
    }
    #[async_trait]
    impl LogTransport for Records {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            self.calls
                .lock()
                .unwrap()
                .push((method.into(), params.clone()));
            let trap = {
                let mut trap = self.trap.lock().unwrap();
                if trap
                    .as_ref()
                    .is_some_and(|t| t.method == method && params["subject"] == t.subject)
                {
                    trap.take()
                } else {
                    None
                }
            };
            if trap.as_ref().is_some_and(|t| !t.after) {
                self.hit.add_permits(1);
                self.release.acquire().await.unwrap().forget();
            }
            let barrier = if method == "record.cas"
                && params["subject"]
                    .as_str()
                    .is_some_and(|s| s.starts_with("inbox:"))
            {
                self.append_barrier.lock().unwrap().take()
            } else {
                None
            };
            if let Some(barrier) = barrier {
                barrier.wait().await;
            }
            let reply = self.fake.call(method, params).await;
            if trap.as_ref().is_some_and(|t| t.after) {
                self.hit.add_permits(1);
                self.release.acquire().await.unwrap().forget();
                return Err(TransportError::Unavailable);
            }
            reply
        }
    }
    struct Connector(Arc<Records>);
    #[async_trait]
    impl LogConnector for Connector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }

    struct Mac {
        machine: Machine,
        log: Arc<dyn LogConnector>,
        records: Arc<Records>,
        discovery: Arc<Discovery>,
        door: FakeDoor,
    }
    impl Mac {
        fn configure(&mut self) {
            self.machine.handler.clock = clock;
            self.machine.handler.participant_discovery = self.discovery.clone();
            self.machine.handler.door_client = self.door.client();
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider(WRITER_MODULE.into()),
            );
            self.machine.handler.provider_resolver = resolver;
        }
        fn worker(&self) -> Worker {
            let h = &self.machine.handler;
            Worker {
                store: Arc::downgrade(&h.store),
                sql_operations: Arc::new(SqlOperations::default()),
                records: h.record_client.clone(),
                door: h.door_client.clone(),
                discovery: h.participant_discovery.clone(),
                health: h.health.clone(),
                writer: h.writer.clone(),
                clock,
                wake: h.move_wake.clone(),
            }
        }
        async fn restart(&mut self) {
            self.machine.handler.stop_move_driver().await;
            self.machine.handler.store.lock().unwrap().take();
            self.machine.handler = Machine::open_with_record(
                &self.machine.descriptor,
                self.log.clone(),
                Some(RecordClient::new(Arc::new(Connector(self.records.clone())))),
            );
            self.configure();
        }
        fn store(&self) -> RegistryStore {
            self.machine.store()
        }
        fn row(&self, cut: &str) -> AgentMove {
            self.store().agent_move(cut).unwrap().unwrap()
        }
        fn source(&self, cut: &str) -> Source {
            Source::new(self.worker(), cut.into())
        }
        fn receiver(&self, cut: &str) -> Receiver {
            Receiver::new(
                self.worker(),
                cut.into(),
                self.machine.handler.liveness.clone(),
            )
        }
        async fn admit_to(&self, to: &str, key: &str) -> Result<String, String> {
            // Admission is the real handler arm. The test owns scheduling so it
            // can stop at durable checkpoints without racing the polling task.
            let bytes = drive(self.machine.handler.admit_move(
                json!({"agent_id":AGENT,"to":to,"request_key":key}),
                CORE,
                tokio::time::Instant::now(),
            ))
            .await
            .map_err(|e| e.code)?;
            Ok(
                serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["cut_id"]
                    .as_str()
                    .unwrap()
                    .into(),
            )
        }
        async fn pickup(&self) {
            drive(poll_inbox(&self.worker())).await.unwrap();
        }
        fn abort(&self, cut: &str) {
            self.store().request_agent_move_abort(cut, clock()).unwrap();
        }
        async fn where_(&self) -> Value {
            self.machine
                .call("agent.where", json!({"agent_id":AGENT}))
                .await
                .unwrap()
        }
        async fn claim(&self) -> RecordReply {
            self.machine
                .handler
                .record_client
                .read(&format!("claim:{AGENT}"))
                .await
                .unwrap()
        }
        async fn record(&self, cut: &str) -> MoveRecord {
            let subject = format!("move:{cut}");
            let reply = self
                .machine
                .handler
                .record_client
                .read(&subject)
                .await
                .unwrap();
            wire::decode_move_record(&subject, reply.body.as_deref().unwrap()).unwrap()
        }
        fn register(
            &self,
            agent: &str,
            kind: &str,
            reference: &str,
        ) -> Result<entorhinal_core::SessionBinding, entorhinal_core::agent::AgentMutationError>
        {
            self.store().register_session(&SessionRegistration {
                agent_id: agent,
                ref_kind: kind,
                session_ref: reference,
                harness: "host",
                session_id: reference,
                project_root: "/project",
                registered_by: "host",
                now_ms: clock(),
            })
        }
        fn refuse(&self, module: &str, op: &str, code: &str) {
            self.discovery.peers[module].script(
                op,
                Effect::Refuse(Refusal {
                    code: code.into(),
                    detail: None,
                }),
            );
        }
        async fn source_until(&self, cut: &str, target: MoveStep) {
            if target == MoveStep::Released {
                self.door.script(
                    "cut.handoff",
                    DoorEffect::Handoff(crate::door_client::HandoffReply {
                        state: crate::door_client::HandoffState::Released,
                        session_ref: None,
                        code: None,
                    }),
                );
            }
            let mut driver = self.source(cut);
            for _ in 0..24 {
                if self.row(cut).step == target {
                    return;
                }
                let before = self.row(cut).step;
                drive(driver.tick()).await.unwrap();
                if self.row(cut).step == before {
                    elapse(300_000);
                }
            }
            let row = self.row(cut);
            panic!(
                "source did not reach {target:?}: step={:?} reason={:?} detail={:?}",
                row.step, row.reason, row.detail
            );
        }
        async fn receiver_until(&self, cut: &str, target: MoveStep) {
            let mut driver = self.receiver(cut);
            for _ in 0..24 {
                if self.row(cut).step == target {
                    return;
                }
                let before = self.row(cut).step;
                drive(driver.tick()).await.unwrap();
                if self.row(cut).step == before {
                    elapse(300_000);
                }
            }
            let row = self.row(cut);
            panic!(
                "receiver did not reach {target:?}: step={:?} reason={:?} detail={:?}",
                row.step, row.reason, row.detail
            );
        }
    }

    struct Pair {
        a: Mac,
        b: Mac,
        modules: Vec<(String, String)>,
    }
    impl Pair {
        async fn new(modules: &[(&str, &str)]) -> Self {
            Self::with_receiver_refs(modules, false).await
        }
        async fn with_receiver_refs(modules: &[(&str, &str)], receiver_held: bool) -> Self {
            NOW.with(|n| n.set(1000));
            let log = FakeLog::default();
            let record = FakeRecord::new([1; 16]);
            let mut b_record = record.restart();
            Arc::get_mut(&mut b_record).unwrap().author = B.into();
            let door = FakeDoor::new([1; 16]);
            let mut macs = Vec::new();
            for (index, author, fake) in [(1, A, record), (2, B, b_record)] {
                let log: Arc<dyn LogConnector> = Arc::new(log.with_author([index; 16]));
                let records = Arc::new(Records {
                    fake,
                    calls: Mutex::new(vec![]),
                    trap: Mutex::new(None),
                    append_barrier: Mutex::new(None),
                    hit: tokio::sync::Semaphore::new(0),
                    release: tokio::sync::Semaphore::new(0),
                });
                let machine = Machine::new_with_record(
                    "move-acceptance",
                    log.clone(),
                    RecordClient::new(Arc::new(Connector(records.clone()))),
                );
                let discovery = Arc::new(Discovery { daemon:FakeDaemon::new(snapshot(modules, true, true)), gate:Arc::new(PeerGate { trap:Mutex::new(None),hit:tokio::sync::Semaphore::new(0), release:tokio::sync::Semaphore::new(0) }), peers:modules.iter().enumerate().map(|(module_index,(module, kind))| {
                    let reference = AgentRef::new(*kind, format!("ref-{kind}"));
                    let peer = Arc::new(FakeParticipant::new(&[*kind], if index == 1 || receiver_held { vec![reference] } else { vec![] }));
                    peer.unknown_busy_for_unheld(true);
                    let session_ref = format!("{:032x}", module_index+16);
                    let item = CutItem { session_ref:session_ref.clone(), stream:format!("reserved:{module}"), end_offset:42, digest:"a".repeat(64) };
                    door.for_author([index;16]).set_stream(item.clone());
                    peer.set_prepare(serde_json::from_value(json!({"items":[{"ref_kind":kind,"ref":format!("ref-{kind}"),"sessionRef":session_ref,"stream":item.stream,"endOffset":42,"digest":item.digest,"format":"v1","frozen":true}],"external":[]})).unwrap());
                    (module.to_string(), peer)
                }).collect() });
                let mut mac = Mac {
                    machine,
                    log,
                    records,
                    discovery,
                    door: door.for_author([index; 16]),
                };
                mac.configure();
                // Both stores begin with the same assistant identity and an
                // identity-log authority marker. Bindings and move checkpoints
                // remain local. Setup precedes generation snapshots so later
                // assertions detect any identity-journal append by the move.
                mac.store().apply_entry("agent.cutover", "{}", "fixture", None, |tx| tx.execute_batch(
                    "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0123456789abcdef','ENTO','E','assistant',1,1); UPDATE identity_log_state SET state='enabled';"
                )).unwrap();
                mac.store()
                    .cache_agent_claims(
                        &[(
                            AGENT.into(),
                            ClaimObservation::Absent {
                                stale: false,
                                self_author: author.into(),
                            },
                        )],
                        clock(),
                    )
                    .unwrap();
                for (member, name) in [(A, "Desk Mac"), (B, "Studio Mac")] {
                    mac.store()
                        .set_member_name(
                            entorhinal_core::shared_entry::MemberName {
                                author: member.into(),
                                display_name: name.into(),
                            },
                            "fixture",
                        )
                        .unwrap();
                }
                if index == 1 {
                    for kind in modules
                        .iter()
                        .map(|(_, kind)| *kind)
                        .collect::<BTreeSet<_>>()
                    {
                        mac.store().apply_entry("fixture", "{}", "fixture", None, |tx| { tx.execute("INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES(?1,?2,?3,NULL,NULL,'','move',1)", (kind, format!("ref-{kind}"), AGENT))?; Ok(()) }).unwrap();
                    }
                }
                macs.push(mac);
            }
            let b = macs.pop().unwrap();
            let a = macs.pop().unwrap();
            Self {
                a,
                b,
                modules: modules
                    .iter()
                    .map(|(m, k)| (m.to_string(), k.to_string()))
                    .collect(),
            }
        }
        async fn published(&self, key: &str) -> String {
            self.a
                .door
                .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
            let cut = self.a.admit_to(B, key).await.unwrap();
            self.a.source_until(&cut, MoveStep::Published).await;
            cut
        }
        async fn finish(&self, cut: &str) {
            self.b.pickup().await;
            self.b.receiver_until(cut, MoveStep::Activated).await;
            drive(CutAccept::new(self.b.worker(), cut.into()).tick())
                .await
                .unwrap();
            self.a.source_until(cut, MoveStep::Released).await;
            self.b.receiver_until(cut, MoveStep::Done).await;
        }
        fn clean(&self) {
            for mac in [&self.a, &self.b] {
                for peer in mac.discovery.peers.values() {
                    assert!(peer.frozen().is_empty());
                    assert!(peer.staged().is_empty());
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn full_move_three_participants_preserves_refs_claim_and_local_journals() {
        let f = Pair::new(&[
            ("broca", "broca-session"),
            ("pi", "pi-session"),
            ("thalamus", "host-session"),
        ])
        .await;
        let generations = (
            f.a.store().generation().unwrap(),
            f.b.store().generation().unwrap(),
        );
        let cut = f.published("full").await;
        f.finish(&cut).await;
        assert_eq!(f.a.record(&cut).await.state, MoveState::Released);
        let claim: Value =
            serde_json::from_slice(f.b.claim().await.body.as_deref().unwrap()).unwrap();
        assert_eq!(claim["holder"], B);
        assert_eq!(claim["epoch"], 1);
        // Read the source's persisted cache before any further handler call.
        // Release must have installed the receiver's claim without a refresh request.
        let source_cache = f.a.store().cached_agent_claim(AGENT).unwrap();
        assert_eq!(
            source_cache
                .as_ref()
                .and_then(|claim| claim.holder.as_deref()),
            Some(B),
            "release must refresh the source claim cache to the receiver"
        );
        assert_eq!(source_cache.map(|claim| claim.epoch), Some(1));
        let a_where = f.a.where_().await;
        assert_eq!(a_where["status"], "held_elsewhere", "{a_where}");
        assert_eq!(a_where["holder"], B);
        assert_eq!(f.b.where_().await["status"], "held_here");
        assert_eq!(
            f.a.register(AGENT, "pi-session", "new").unwrap_err().code,
            "held_elsewhere"
        );
        for (module, kind) in &f.modules {
            let expected = BTreeSet::from([AgentRef::new(kind, format!("ref-{kind}"))]);
            assert_eq!(
                f.a.discovery.peers[module].cut(&cut).unwrap().refs,
                expected
            );
            assert_eq!(
                f.b.discovery.peers[module].cut(&cut).unwrap().refs,
                expected
            );
            assert_eq!(
                f.a.discovery.peers[module].tombstones()
                    [&AgentRef::new(kind, format!("ref-{kind}"))],
                B
            );
        }
        assert_eq!(
            generations,
            (
                f.a.store().generation().unwrap(),
                f.b.store().generation().unwrap()
            )
        );
        f.clean();
    }

    #[tokio::test(start_paused = true)]
    async fn shared_kind_fans_out_and_move_back_without_host_carries_every_ref() {
        let mut f = Pair::new(&[("alpha", "pi-session"), ("beta", "pi-session")]).await;
        let cut = f.published("out").await;
        f.finish(&cut).await;
        let record = f.a.record(&cut).await;
        assert_eq!(record.participants.len(), 2);
        assert_eq!(record.participants[0].refs, record.participants[1].refs);
        for module in ["alpha", "beta"] {
            for (mac, ops) in [
                (
                    &f.a,
                    vec![
                        "agent_data.prepare",
                        "agent_data.export",
                        "agent_data.release",
                    ],
                ),
                (
                    &f.b,
                    vec![
                        "agent_data.diverged",
                        "agent_data.import",
                        "agent_data.activate",
                    ],
                ),
            ] {
                for op in ops {
                    assert!(
                        mac.discovery.peers[module]
                            .calls()
                            .iter()
                            .any(|r| r.op().method() == op),
                        "missing {module} {op}"
                    );
                }
            }
        }
        assert_eq!(
            f.b.machine
                .query("SELECT harness,session_id,registered_by FROM session_binding"),
            vec![vec![Cell::Null, Cell::Null, Cell::Text("move".into())]]
        );
        assert_eq!(
            f.b.machine
                .call(
                    "agent.session_liveness",
                    json!({"refs":[{"ref_kind":"pi-session","ref":"ref-pi-session"}]})
                )
                .await
                .unwrap()["refs"][0]["state"],
            "gone"
        );
        let old_bindings = f.a.machine.rows("session_binding");
        f.a.restart().await;
        f.b.restart().await;
        f.b.door
            .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
        let back = f.b.admit_to(A, "back").await.unwrap();
        f.b.source_until(&back, MoveStep::Published).await;
        f.a.pickup().await;
        f.a.receiver_until(&back, MoveStep::Activated).await;
        drive(CutAccept::new(f.a.worker(), back.clone()).tick())
            .await
            .unwrap();
        f.b.source_until(&back, MoveStep::Released).await;
        f.a.receiver_until(&back, MoveStep::Done).await;
        assert_eq!(f.a.machine.rows("session_binding"), old_bindings);
        for module in ["alpha", "beta"] {
            assert_eq!(
                f.b.discovery.peers[module].cut(&back).unwrap().refs,
                f.a.discovery.peers[module].cut(&cut).unwrap().refs
            );
            assert!(f.a.discovery.peers[module].tombstones().is_empty());
        }
        assert!(f.a.door.calls().iter().any(
            |c| c.author == A && matches!(&c.request,DoorRequest::Accept(r) if r.cut_id==back)
        ));
        for item in f.a.door.cut(&cut).unwrap().items {
            assert!(f.a.door.admit(&item.session_ref).is_ok());
        }
        assert_eq!(f.a.where_().await["status"], "held_here");
        assert_eq!(f.b.where_().await["holder"], A);
    }

    fn put_claim(
        mac: &Mac,
        version: u64,
        holder: Option<&str>,
        epoch: i64,
        cut: Option<&str>,
        author: &str,
    ) {
        mac.records.fake.put(&format!("claim:{AGENT}"),version,serde_json::to_vec(&json!({"v":1,"agent_id":AGENT,"holder":holder,"epoch":epoch,"cut_id":cut,"reason":null})).unwrap(),author);
    }
    #[tokio::test(start_paused = true)]
    async fn version_three_epoch_zero_uses_record_version_not_cache_version() {
        let f = Pair::new(&[("pi", "pi-session")]).await;
        put_claim(&f.a, 3, None, 0, None, A);
        let cut = f.published("epoch-zero").await;
        assert_eq!(f.a.row(&cut).claim_version, Some(3));
        f.finish(&cut).await;
        let claim = f.b.claim().await;
        assert_eq!(claim.version, 4);
        assert_eq!(
            serde_json::from_slice::<Value>(claim.body.as_deref().unwrap()).unwrap()["epoch"],
            1
        );
    }
    #[tokio::test(start_paused = true)]
    async fn held_elsewhere_and_busy_admission_have_no_move_side_effects() {
        for code in [
            "not_holder",
            "agent_busy",
            "unknown_member",
            "invalid_request",
            "claim_stale",
        ] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let target = match code {
                "not_holder" => {
                    put_claim(&f.a, 1, Some(B), 1, None, B);
                    B
                }
                "agent_busy" => {
                    f.a.discovery.peers["pi"].set_busy(
                        AgentRef::new("pi-session", "ref-pi-session"),
                        BusyRef::new(BusyState::Unknown).with_reason_code("run_active"),
                    );
                    B
                }
                "unknown_member" => "03030303030303030303030303030303",
                "claim_stale" => {
                    f.a.store().apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_1111111111111111','Other','','assistant',1,1);")).unwrap();
                    f.a.records
                        .fake
                        .error("claim:agent_1111111111111111", Some("unavailable"));
                    B
                }
                _ => {
                    f.a.store()
                        .set_member_name(
                            entorhinal_core::shared_entry::MemberName {
                                author: "03030303030303030303030303030303".into(),
                                display_name: "Studio Mac".into(),
                            },
                            "fixture",
                        )
                        .unwrap();
                    "Studio Mac"
                }
            };
            let admission = f.a.machine.handler.admit_move(
                json!({"agent_id":AGENT,"to":target,"request_key":code}),
                CORE,
                tokio::time::Instant::now(),
            );
            tokio::pin!(admission);
            let mut outcome = None;
            // Bound cooperative polls rather than advancing the paused clock:
            // SQLite's blocking worker must finish before network deadlines jump.
            for _ in 0..1_000_000 {
                tokio::select! {
                    biased;
                    result = &mut admission => {
                        outcome = Some(result);
                        break;
                    }
                    () = tokio::task::yield_now() => {}
                }
            }
            let outcome = outcome.expect("admission exceeded its bounded cooperative poll budget");
            assert!(
                f.a.store().list_agent_moves().unwrap().is_empty(),
                "{code} admission must remain read-only"
            );
            assert_eq!(
                outcome
                    .map(|_| "admitted".to_owned())
                    .unwrap_or_else(|e| e.code),
                code
            );
            assert!(f.a.door.calls().is_empty());
            assert!(f.a.discovery.peers["pi"]
                .calls()
                .iter()
                .all(|r| matches!(r, Request::Describe(_) | Request::Busy(_))));
            assert!(f
                .a
                .records
                .calls
                .lock()
                .unwrap()
                .iter()
                .all(|(op, p)| op == "record.read_many"
                    || op == "record.read" && p["subject"] == format!("claim:{AGENT}")));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn moved_claim_aborts_without_take_and_claimed_abort_returns_epoch_two_to_a() {
        for claimed in [false, true] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published("abort-claim").await;
            f.b.pickup().await;
            f.b.receiver_until(
                &cut,
                if claimed {
                    MoveStep::Claimed
                } else {
                    MoveStep::Staged
                },
            )
            .await;
            if claimed {
                f.b.abort(&cut);
            } else {
                put_claim(&f.a, 1, Some(A), 1, None, A);
            }
            f.b.receiver_until(&cut, MoveStep::Aborted).await;
            f.a.source_until(&cut, MoveStep::Aborted).await;
            assert_eq!(
                f.b.row(&cut).reason.as_deref(),
                Some(if claimed { "operator" } else { "claim_moved" })
            );
            let body: Value =
                serde_json::from_slice(f.b.claim().await.body.as_deref().unwrap()).unwrap();
            assert_eq!(body["holder"], A);
            assert_eq!(body["epoch"], if claimed { 2 } else { 1 });
            assert_eq!(f.a.where_().await["status"], "held_here");
            assert_eq!(f.b.where_().await["status"], "held_elsewhere");
            assert_eq!(
                f.b.register(AGENT, "pi-session", "new").unwrap_err().code,
                "held_elsewhere"
            );
            if !claimed {
                assert!(!f
                    .b
                    .records
                    .calls
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|(op, p)| op == "record.cas" && p["subject"] == format!("claim:{AGENT}")));
            }
            f.clean();
        }
    }
    #[tokio::test(start_paused = true)]
    async fn abort_matrix_cleans_every_pre_boundary_checkpoint_across_restarts() {
        for source_step in [
            MoveStep::Preparing,
            MoveStep::Exported,
            MoveStep::Sealed,
            MoveStep::Published,
        ] {
            let mut f = Pair::new(&[("pi", "pi-session")]).await;
            f.a.door
                .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
            let cut = f.a.admit_to(B, "source-abort").await.unwrap();
            f.a.source_until(&cut, source_step).await;
            f.a.abort(&cut);
            f.a.restart().await;
            f.a.source_until(&cut, MoveStep::Aborted).await;
            f.b.pickup().await;
            assert!(f.b.store().list_agent_moves().unwrap().is_empty());
            assert_eq!(f.a.row(&cut).reason.as_deref(), Some("operator"));
            if let Some(seal) = f.a.door.cut(&cut) {
                for item in seal.items {
                    assert!(f.a.door.admit(&item.session_ref).is_ok());
                }
            }
            f.clean();
        }
        for step in [
            MoveStep::PickedUp,
            MoveStep::Imported,
            MoveStep::Staged,
            MoveStep::Claimed,
        ] {
            let mut f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published("receiver-abort").await;
            f.b.pickup().await;
            f.b.receiver_until(&cut, step).await;
            f.b.abort(&cut);
            f.b.restart().await;
            f.b.receiver_until(&cut, MoveStep::Aborted).await;
            f.a.restart().await;
            f.a.source_until(&cut, MoveStep::Aborted).await;
            assert_eq!(f.a.record(&cut).await.state, MoveState::Aborted);
            assert_eq!(f.a.row(&cut).reason.as_deref(), Some("operator"));
            f.clean();
        }
    }
    #[tokio::test(start_paused = true)]
    async fn source_abort_request_at_staged_cannot_unfreeze_receiver_take() {
        let f = Pair::new(&[("pi", "pi-session")]).await;
        let cut = f.published("staged-race").await;
        f.b.pickup().await;
        f.b.receiver_until(&cut, MoveStep::Staged).await;
        f.a.abort(&cut);
        f.b.receiver_until(&cut, MoveStep::Claimed).await;
        drive(f.a.source(&cut).tick()).await.unwrap();
        let row = f.a.row(&cut);
        assert!(row.abort_requested.is_none());
        assert_eq!(row.detail.unwrap()["code"], "invalid_state");
        assert!(!f.a.discovery.peers["pi"].frozen().is_empty());
        f.finish(&cut).await;
    }
    #[tokio::test(start_paused = true)]
    async fn activation_hold_survives_crash_and_every_boundary_abort_is_read_only() {
        let mut f = Pair::new(&[("alpha", "a-session"), ("beta", "b-session")]).await;
        let cut = f.published("boundary").await;
        f.b.pickup().await;
        f.b.receiver_until(&cut, MoveStep::Activating).await;
        f.b.refuse("beta", "agent_data.activate", "external_not_present");
        drive(f.b.receiver(&cut).tick()).await.unwrap();
        assert_eq!(f.b.row(&cut).step, MoveStep::Activating);
        assert_eq!(f.b.row(&cut).hold.unwrap()["code"], "external_not_present");
        assert_eq!(
            f.b.discovery.peers["alpha"].cut(&cut).unwrap().step,
            crate::participant_client::Step::Activated
        );
        let before = f.b.row(&cut);
        assert_eq!(
            f.b.machine
                .bytes_on(
                    CORE,
                    "agent.move_abort",
                    json!({"cut_id":cut,"request_key":"boundary"})
                )
                .await
                .unwrap_err(),
            "invalid_state"
        );
        assert_eq!(f.b.row(&cut), before);
        f.b.restart().await;
        elapse(300_000);
        f.finish(&cut).await;
        for mac in [&f.a, &f.b] {
            let before = mac.row(&cut);
            assert_eq!(
                mac.machine
                    .bytes_on(
                        CORE,
                        "agent.move_abort",
                        json!({"cut_id":cut,"request_key":"terminal"})
                    )
                    .await
                    .unwrap_err(),
                "invalid_state"
            );
            assert_eq!(mac.row(&cut), before);
            assert!(!mac
                .discovery
                .peers
                .values()
                .any(|p| p.calls().iter().any(|r| matches!(r, Request::Abort(_)))));
        }
        f.a.restart().await;
        f.b.restart().await;
        assert!(f.a.store().list_open_agent_moves().unwrap().is_empty());
        assert!(f.b.store().list_open_agent_moves().unwrap().is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn restarted_source_and_receiver_resume_at_every_recorded_step() {
        let mut f = Pair::new(&[("pi", "pi-session")]).await;
        f.a.door
            .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
        let cut = f.a.admit_to(B, "restarts").await.unwrap();
        for step in [
            MoveStep::Preparing,
            MoveStep::Exported,
            MoveStep::Sealed,
            MoveStep::Published,
        ] {
            f.a.source_until(&cut, step).await;
            f.a.restart().await;
            assert_eq!(
                f.a.register(AGENT, "pi-session", "new").unwrap_err().code,
                "agent_moving"
            );
        }
        f.b.pickup().await;
        for step in [
            MoveStep::PickedUp,
            MoveStep::Imported,
            MoveStep::Staged,
            MoveStep::Claimed,
            MoveStep::Activating,
            MoveStep::Activated,
        ] {
            f.b.receiver_until(&cut, step).await;
            f.b.restart().await;
            if step != MoveStep::Activated {
                assert_eq!(
                    f.b.register(AGENT, "pi-session", "new").unwrap_err().code,
                    "agent_moving"
                );
            }
        }
        f.a.records
            .fake
            .error(&format!("claim:{AGENT}"), Some("unavailable"));
        drive(f.a.source(&cut).tick()).await.unwrap();
        assert_eq!(f.a.row(&cut).step, MoveStep::Releasing);
        f.a.restart().await;
        f.a.records.fake.error(&format!("claim:{AGENT}"), None);
        elapse(300_000);
        f.a.source_until(&cut, MoveStep::Released).await;
        f.a.restart().await;
        f.b.receiver_until(&cut, MoveStep::Done).await;
        f.b.restart().await;
        assert_eq!(f.a.record(&cut).await.state, MoveState::Released);
        let claim = f.b.claim().await;
        assert_eq!(claim.version, 1);
        assert_eq!(
            f.a.register(AGENT, "pi-session", "new").unwrap_err().code,
            "held_elsewhere"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn crashes_on_both_sides_of_take_and_return_cas_never_strand_or_retake_claim() {
        for returning in [false, true] {
            for after in [false, true] {
                let mut f = Pair::new(&[("pi", "pi-session")]).await;
                let cut = f.published("cas-crash").await;
                f.b.pickup().await;
                f.b.receiver_until(
                    &cut,
                    if returning {
                        MoveStep::Claimed
                    } else {
                        MoveStep::Staged
                    },
                )
                .await;
                if returning {
                    f.b.abort(&cut);
                }
                *f.b.records.trap.lock().unwrap() = Some(Trap {
                    method: "record.cas".into(),
                    subject: format!("claim:{AGENT}"),
                    after,
                });
                let mut driver = f.b.receiver(&cut);
                let mut future = Box::pin(driver.tick());
                drive(async { tokio::select! { result=&mut future => panic!("CAS did not reach crash gate: {result:?}"), permit=f.b.records.hit.acquire() => permit.unwrap().forget() } }).await;
                drop(future);
                drop(driver);
                f.b.restart().await;
                if returning {
                    f.b.receiver_until(&cut, MoveStep::Aborted).await;
                    f.a.source_until(&cut, MoveStep::Aborted).await;
                } else {
                    f.finish(&cut).await;
                }
                let claim = f.b.claim().await;
                assert_eq!(claim.version, if returning { 2 } else { 1 });
                let body: Value = serde_json::from_slice(claim.body.as_deref().unwrap()).unwrap();
                assert_eq!(body["holder"], if returning { A } else { B });
                assert_eq!(body["epoch"], if returning { 2 } else { 1 });
                f.clean();
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn operator_abort_during_prepare_pending_seal_and_lost_publish_recovers_cleanup() {
        for phase in ["prepare", "seal", "publish"] {
            let mut f = Pair::new(&[("alpha", "a-session"), ("beta", "b-session")]).await;
            let cut = f.a.admit_to(B, phase).await.unwrap();
            if phase == "prepare" {
                *f.a.discovery.gate.trap.lock().unwrap() =
                    Some(("alpha".into(), "agent_data.prepare".into(), true));
                let mut driver = f.a.source(&cut);
                let mut future = Box::pin(driver.tick());
                drive(async {tokio::select! { result=&mut future=>panic!("prepare gate not reached: {result:?}"), permit=f.a.discovery.gate.hit.acquire()=>permit.unwrap().forget() }}).await;
                f.a.abort(&cut);
                assert!(!f.a.discovery.peers["alpha"].frozen().is_empty());
                assert!(!f
                    .a
                    .discovery
                    .peers
                    .values()
                    .any(|p| p.calls().iter().any(|r| matches!(r, Request::Abort(_)))));
                drop(future);
                drop(driver);
            } else {
                f.a.source_until(&cut, MoveStep::Exported).await;
                if phase == "seal" {
                    drive(f.a.source(&cut).tick()).await.unwrap();
                    assert!(f.a.row(&cut).seal_sent);
                    assert_eq!(f.a.door.cut(&cut).unwrap().state, SealState::Pending);
                } else {
                    f.a.door
                        .script("cut.status", DoorEffect::Seal(SealState::Published, None));
                    f.a.source_until(&cut, MoveStep::Sealed).await;
                    *f.a.records.trap.lock().unwrap() = Some(Trap {
                        method: "record.cas".into(),
                        subject: format!("move:{cut}"),
                        after: true,
                    });
                    let mut driver = f.a.source(&cut);
                    let mut future = Box::pin(driver.tick());
                    drive(async {tokio::select! {result=&mut future=>panic!("publish gate not reached: {result:?}"),permit=f.a.records.hit.acquire()=>permit.unwrap().forget()}}).await;
                    drop(future);
                    drop(driver);
                    assert_eq!(f.a.record(&cut).await.state, MoveState::Published);
                }
                f.a.abort(&cut);
            }
            f.a.restart().await;
            elapse(300_000);
            *f.a.discovery.gate.trap.lock().unwrap() =
                Some(("alpha".into(), "agent_data.abort".into(), true));
            let mut driver = f.a.source(&cut);
            let mut future = Box::pin(async {
                for _ in 0..8 {
                    driver.tick().await.unwrap();
                    elapse(300_000);
                }
                panic!("cleanup gate not reached in {phase}")
            });
            drive(async {tokio::select! {()=&mut future=>panic!("cleanup unexpectedly completed"),permit=f.a.discovery.gate.hit.acquire()=>permit.unwrap().forget()}}).await;
            drop(future);
            drop(driver);
            f.a.restart().await;
            f.a.source_until(&cut, MoveStep::Aborted).await;
            assert_eq!(f.a.row(&cut).reason.as_deref(), Some("operator"));
            for peer in f.a.discovery.peers.values() {
                assert!(peer.frozen().is_empty());
            }
            if let Some(seal) = f.a.door.cut(&cut) {
                for item in seal.items {
                    assert!(f.a.door.admit(&item.session_ref).is_ok());
                }
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn takeover_and_accept_diagnostics_do_not_delay_finish_across_restart() {
        for takeover in [false, true] {
            for code in ["cut_not_published", "unavailable"] {
                let mut f = Pair::new(&[("pi", "pi-session")]).await;
                let cut = f.published("accept").await;
                f.b.pickup().await;
                f.b.receiver_until(&cut, MoveStep::Activated).await;
                f.b.door
                    .script("cut.accept", DoorEffect::Refuse(code.into()));
                drive(CutAccept::new(f.b.worker(), cut.clone()).tick())
                    .await
                    .unwrap();
                assert!(f.b.row(&cut).hold.is_none());
                assert_eq!(f.b.row(&cut).cut_accept.unwrap()["code"], code);
                for reply in [
                    f.b.machine
                        .call("agent.move_status", json!({"cut_id":cut}))
                        .await
                        .unwrap(),
                    f.b.machine.call("agent.moves", json!({})).await.unwrap(),
                    f.b.where_().await,
                ] {
                    assert!(reply.to_string().contains(code), "{reply}");
                }
                f.b.restart().await;
                if takeover {
                    let deadline = f.b.row(&cut).activated_at_ms.unwrap() + 600_000;
                    elapse(deadline - clock() - 1);
                    drive(f.b.receiver(&cut).tick()).await.unwrap();
                    assert!(!f.b.door.calls().iter().any(|call| matches!(&call.request,DoorRequest::Takeover(request) if request.cut_id==cut)));
                    assert_eq!(f.b.row(&cut).step, MoveStep::Activated);
                    f.b.door.script(
                        "cut.takeover",
                        DoorEffect::Takeover(crate::door_client::TakeoverProgress {
                            state: crate::door_client::TakeoverState::Taken,
                            session_ref: None,
                            code: None,
                        }),
                    );
                    elapse(1);
                    f.b.receiver_until(&cut, MoveStep::TakenOver).await;
                } else {
                    f.a.source_until(&cut, MoveStep::Released).await;
                    f.b.receiver_until(&cut, MoveStep::Done).await;
                }
                assert!(f.b.row(&cut).hold.is_none());
                if code == "unavailable" {
                    elapse(300_000);
                    drive(CutAccept::new(f.b.worker(), cut.clone()).tick())
                        .await
                        .unwrap();
                    assert_eq!(f.b.row(&cut).cut_accept.unwrap()["state"], "accepted");
                }
                if takeover {
                    f.b.door
                        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
                    let back = f.b.admit_to(A, "post-takeover").await.unwrap();
                    assert_eq!(f.b.row(&back).step, MoveStep::Preparing);
                    f.a.source_until(&cut, MoveStep::Released).await;
                }
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn receiver_ref_ownership_and_registration_windows_are_transactional() {
        for conflict in [false, true] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            f.b.store().apply_entry("fixture","{}","fixture",None,|tx| tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_1111111111111111','Other','','assistant',1,1);")).unwrap();
            if conflict {
                f.b.register("agent_1111111111111111", "pi-session", "ref-pi-session")
                    .unwrap();
            }
            let cut = f.published("ownership").await;
            f.b.pickup().await;
            if conflict {
                f.b.receiver_until(&cut, MoveStep::Aborted).await;
                assert_eq!(f.b.row(&cut).detail.unwrap()["code"], "ref_bound_elsewhere");
                assert!(!f.b.discovery.peers["pi"]
                    .calls()
                    .iter()
                    .any(|r| matches!(r, Request::Import(_))));
                f.a.source_until(&cut, MoveStep::Aborted).await;
            } else {
                assert_eq!(
                    f.b.register("agent_1111111111111111", "pi-session", "ref-pi-session")
                        .unwrap_err()
                        .code,
                    "agent_moving"
                );
                f.finish(&cut).await;
                assert_eq!(
                    f.b.register("agent_1111111111111111", "pi-session", "ref-pi-session")
                        .unwrap_err()
                        .code,
                    "ref_bound_elsewhere"
                );
                let binding = f.b.register(AGENT, "pi-session", "ref-pi-session").unwrap();
                assert_eq!(binding.harness.as_deref(), Some("host"));
                assert_eq!(binding.session_id.as_deref(), Some("ref-pi-session"));
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn receiver_inbox_discards_sixty_four_abandoned_moves_without_creating_rows() {
        let f = Pair::new(&[("pi", "pi-session")]).await;
        for n in 0..64 {
            let cut = f.published(&format!("abandoned-{n}")).await;
            f.a.abort(&cut);
            f.a.source_until(&cut, MoveStep::Aborted).await;
        }
        let subject = format!("inbox:{B}");
        let reply =
            f.b.machine
                .handler
                .record_client
                .read(&subject)
                .await
                .unwrap();
        assert_eq!(
            wire::decode_inbox_record(reply.body.as_deref().unwrap())
                .unwrap()
                .pending
                .len(),
            64
        );
        f.b.pickup().await;
        let reply =
            f.b.machine
                .handler
                .record_client
                .read(&subject)
                .await
                .unwrap();
        assert!(wire::decode_inbox_record(reply.body.as_deref().unwrap())
            .unwrap()
            .pending
            .is_empty());
        assert!(f.b.store().list_agent_moves().unwrap().is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn receiver_diverged_lagging_identity_and_external_data_obey_pickup_policy() {
        for mode in ["diverged", "lagging", "external"] {
            let f = Pair::with_receiver_refs(&[("pi", "pi-session")], mode == "diverged").await;
            let cut = f.published(mode).await;
            if mode == "lagging" {
                f.b.store()
                    .apply_entry("fixture", "{}", "fixture", None, |tx| {
                        tx.execute_batch("DELETE FROM agent;")
                    })
                    .unwrap();
            }
            f.b.pickup().await;
            if mode == "diverged" {
                f.b.discovery.peers["pi"].set_diverged(
                    AgentRef::new("pi-session", "ref-pi-session"),
                    crate::participant_client::DivergedState::Advanced,
                );
            }
            if mode == "external" {
                f.b.refuse("pi", "agent_data.diverged", "external_not_present");
            }
            drive(f.b.receiver(&cut).tick()).await.unwrap();
            if mode == "diverged" {
                f.b.receiver_until(&cut, MoveStep::Aborted).await;
                f.a.source_until(&cut, MoveStep::Aborted).await;
                assert_eq!(f.b.row(&cut).reason.as_deref(), Some("diverged"));
                assert!(!f.b.discovery.peers["pi"]
                    .calls()
                    .iter()
                    .any(|r| matches!(r, Request::Import(_))));
            } else {
                assert_eq!(f.b.row(&cut).step, MoveStep::PickedUp);
                assert_eq!(
                    f.b.row(&cut).hold.unwrap()["code"],
                    if mode == "lagging" {
                        "unknown_agent"
                    } else {
                        "external_not_present"
                    }
                );
                if mode == "lagging" {
                    f.b.store().apply_entry("fixture","{}","fixture",None,|tx| tx.execute_batch("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0123456789abcdef','ENTO','E','assistant',1,1);")).unwrap();
                }
                elapse(300_000);
                f.finish(&cut).await;
                assert!(
                    !f.b.discovery.peers["pi"]
                        .calls()
                        .iter()
                        .any(|r| matches!(r, Request::Busy(_))),
                    "unheld refs must skip busy"
                );
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn retry_refusals_busy_nine_failed_seal_and_unknown_code_have_named_outcomes() {
        for mode in [
            "unavailable",
            "handoff_not_covered",
            "busy",
            "failed",
            "frobnicated",
        ] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            f.a.door.script(
                "cut.seal",
                DoorEffect::Seal(
                    if mode == "failed" {
                        SealState::Failed
                    } else {
                        SealState::Published
                    },
                    None,
                ),
            );
            let cut = f.a.admit_to(B, mode).await.unwrap();
            if mode == "unavailable" {
                for _ in 0..2 {
                    f.a.refuse("pi", "agent_data.prepare", mode);
                }
            }
            if mode == "busy" {
                for _ in 0..9 {
                    f.a.refuse("pi", "agent_data.prepare", mode);
                }
            }
            if mode == "frobnicated" {
                f.a.refuse("pi", "agent_data.prepare", mode);
            }
            if ["busy", "failed", "frobnicated"].contains(&mode) {
                f.a.source_until(&cut, MoveStep::Aborted).await;
                let row = f.a.row(&cut);
                assert_eq!(
                    row.reason.as_deref(),
                    Some(if mode == "failed" {
                        "door_refused"
                    } else {
                        "participant_refused"
                    })
                );
                assert_eq!(row.detail.unwrap()["code"], mode);
                if mode == "busy" {
                    assert_eq!(
                        f.a.discovery.peers["pi"]
                            .calls()
                            .iter()
                            .filter(|r| matches!(r, Request::Prepare(_)))
                            .count(),
                        9
                    );
                }
                f.clean();
            } else {
                f.a.source_until(&cut, MoveStep::Published).await;
                f.b.pickup().await;
                f.b.receiver_until(&cut, MoveStep::Activated).await;
                if mode == "handoff_not_covered" {
                    for _ in 0..2 {
                        f.a.door
                            .script("cut.handoff", DoorEffect::Refuse(mode.into()));
                    }
                }
                f.a.source_until(&cut, MoveStep::Released).await;
                f.b.receiver_until(&cut, MoveStep::Done).await;
                if mode == "handoff_not_covered" {
                    assert_eq!(f.a.door.calls().iter().filter(|call| matches!(&call.request,DoorRequest::Handoff(request) if request.cut_id==cut)).count(),3);
                }
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn discovery_missing_not_ready_role_and_unlisted_modules_obey_pinned_plan() {
        for mode in ["missing", "not-ready", "no-role", "unlisted"] {
            let mut f = Pair::new(&[("pi", "pi-session")]).await;
            if mode == "no-role" {
                f.a.discovery
                    .daemon
                    .restart(snapshot(&[("pi", "pi-session")], true, false));
                assert_eq!(
                    f.a.admit_to(B, mode).await.unwrap_err(),
                    "participant_unavailable"
                );
                assert!(f.a.store().list_agent_moves().unwrap().is_empty());
                continue;
            }
            let cut = f.published(mode).await;
            f.b.discovery.daemon.restart(snapshot(
                if mode == "missing" {
                    &[]
                } else {
                    &[("pi", "pi-session")]
                },
                mode != "not-ready",
                true,
            ));
            let extra = Arc::new(FakeParticipant::new(&["pi-session"], vec![]));
            if mode == "unlisted" {
                let mut peers = f.b.discovery.peers.clone();
                peers.insert("extra".into(), extra.clone());
                f.b.discovery = Arc::new(Discovery {
                    daemon: FakeDaemon::new(snapshot(
                        &[("pi", "pi-session"), ("extra", "pi-session")],
                        true,
                        true,
                    )),
                    peers,
                    gate: f.b.discovery.gate.clone(),
                });
                f.b.configure();
            }
            f.b.pickup().await;
            drive(f.b.receiver(&cut).tick()).await.unwrap();
            match mode {
                "missing" => {
                    f.b.receiver_until(&cut, MoveStep::Aborted).await;
                    assert_eq!(f.b.row(&cut).detail.unwrap()["module_id"], "pi");
                    f.a.source_until(&cut, MoveStep::Aborted).await;
                }
                "not-ready" => {
                    assert_eq!(f.b.row(&cut).step, MoveStep::PickedUp);
                    assert!(f.b.row(&cut).hold.is_some());
                    f.b.discovery
                        .daemon
                        .restart(snapshot(&[("pi", "pi-session")], true, true));
                    elapse(300_000);
                    f.finish(&cut).await;
                }
                _ => {
                    f.finish(&cut).await;
                    assert!(extra.calls().is_empty());
                }
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn wrong_author_and_foreign_to_pickup_do_not_touch_participants_or_move_record() {
        for mode in ["author", "to"] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published(mode).await;
            let mut record = f.a.record(&cut).await;
            if mode == "to" {
                record.to = "03030303030303030303030303030303".into();
            }
            f.a.records.fake.put(
                &format!("move:{cut}"),
                2,
                wire::encode_move_record(&format!("move:{cut}"), &record).unwrap(),
                if mode == "author" {
                    "03030303030303030303030303030303"
                } else {
                    A
                },
            );
            f.b.pickup().await;
            if mode == "author" {
                assert_eq!(f.b.row(&cut).reason.as_deref(), Some("cut_author_mismatch"));
            } else {
                assert!(f.b.store().agent_move(&cut).unwrap().is_none());
            }
            assert!(f.b.discovery.peers["pi"].calls().is_empty());
            assert!(!f
                .b
                .records
                .calls
                .lock()
                .unwrap()
                .iter()
                .any(|(op, p)| op == "record.cas" && p["subject"] == format!("move:{cut}")));
        }
    }
    #[tokio::test(start_paused = true)]
    async fn forged_activated_author_or_plan_cannot_release_source_participants() {
        for mode in ["author", "to", "agent"] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published(mode).await;
            f.b.pickup().await;
            f.b.receiver_until(&cut, MoveStep::Activated).await;
            let good = f.b.record(&cut).await;
            let mut forged = good.clone();
            if mode == "to" {
                forged.to = "03030303030303030303030303030303".into();
            }
            if mode == "agent" {
                forged.agent_id = "agent_1111111111111111".into();
            }
            f.a.records.fake.put(
                &format!("move:{cut}"),
                5,
                wire::encode_move_record(&format!("move:{cut}"), &forged).unwrap(),
                if mode == "author" { A } else { B },
            );
            drive(f.a.source(&cut).tick()).await.unwrap();
            assert_eq!(f.a.row(&cut).step, MoveStep::Published);
            assert!(f.a.discovery.peers["pi"].tombstones().is_empty());
            assert!(!f.a.discovery.peers["pi"].frozen().is_empty());
            assert!(f.a.where_().await.get("moveErrors").is_some());
            f.a.records.fake.put(
                &format!("move:{cut}"),
                6,
                wire::encode_move_record(&format!("move:{cut}"), &good).unwrap(),
                B,
            );
            elapse(300_000);
            f.finish(&cut).await;
        }
    }
    #[tokio::test(start_paused = true)]
    async fn oversized_prepare_aborts_before_export_or_seal_and_abort_detail_is_bounded() {
        let f = Pair::new(&[("pi", "pi-session")]).await;
        let prepare:PrepareReply=serde_json::from_value(json!({"items":[],"external":[{"ref_kind":"pi-session","ref":"ref-pi-session","note":"x".repeat(61_440),"check":{}}]})).unwrap();
        f.a.discovery.peers["pi"].set_prepare(prepare);
        let cut = f.a.admit_to(B, "size").await.unwrap();
        f.a.source_until(&cut, MoveStep::Aborted).await;
        assert_eq!(f.a.row(&cut).reason.as_deref(), Some("move_too_large"));
        assert!(f.a.door.calls().is_empty());
        assert!(!f.a.discovery.peers["pi"]
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Export(_))));
        f.clean();
        let f = Pair::new(&[("pi", "pi-session")]).await;
        let cut = f.published("detail").await;
        f.b.pickup().await;
        f.b.discovery.peers["pi"].script(
            "agent_data.import",
            Effect::Refuse(Refusal {
                code: "incompatible_version".into(),
                detail: Some(json!({"found":"x".repeat(10_000),"required":"v2"})),
            }),
        );
        f.b.receiver_until(&cut, MoveStep::Aborted).await;
        f.a.source_until(&cut, MoveStep::Aborted).await;
        let record = f.a.record(&cut).await;
        assert_eq!(record.state, MoveState::Aborted);
        assert_eq!(record.detail.unwrap()["truncated"], true);
        assert!(f.b.row(&cut).detail.unwrap().to_string().len() > 2048);
        f.clean();
    }
    #[tokio::test(start_paused = true)]
    async fn admission_registration_orders_same_key_and_unique_index_use_real_transactions() {
        for before_insert in [false, true] {
            let mut f = Pair::new(&[("pi", "pi-session")]).await;
            if before_insert {
                *f.a.discovery.gate.trap.lock().unwrap() =
                    Some(("pi".into(), "agent_data.busy".into(), true));
                let mut future = Box::pin(f.a.admit_to(B, "racing"));
                drive(async {tokio::select! {result=&mut future=>panic!("admission gate not reached: {result:?}"),permit=f.a.discovery.gate.hit.acquire()=>permit.unwrap().forget()}}).await;
                f.a.register(AGENT, "pi-session", "new").unwrap();
                drop(future);
                assert!(f.a.store().list_agent_moves().unwrap().is_empty());
                f.a.restart().await;
                assert_eq!(f.a.admit_to(B, "racing").await.unwrap_err(), "agent_busy");
            } else {
                *f.a.records.trap.lock().unwrap() = Some(Trap {
                    method: "record.read".into(),
                    subject: format!("claim:{AGENT}"),
                    after: false,
                });
                let mut first = Box::pin(f.a.admit_to(B, "same"));
                drive(async {tokio::select! {result=&mut first=>panic!("preflight gate not reached: {result:?}"),permit=f.a.records.hit.acquire()=>permit.unwrap().forget()}}).await;
                let cut = f.a.admit_to(B, "same").await.unwrap();
                f.a.records.release.add_permits(1);
                assert_eq!(drive(&mut first).await.unwrap(), cut);
                drop(first);
                assert_eq!(f.a.store().list_agent_moves().unwrap().len(), 1);
                assert_eq!(
                    f.a.register(AGENT, "pi-session", "new").unwrap_err().code,
                    "agent_moving"
                );
                let mut second = f.a.row(&cut);
                second.cut_id = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into();
                second.request_key = None;
                assert!(f.a.store().insert_agent_move(&second).is_err());
                second.side = entorhinal_core::move_store::MoveSide::Receiver;
                second.step = MoveStep::PickedUp;
                assert!(f.a.store().insert_agent_move(&second).is_err());
                f.a.store()
                    .set_member_name(
                        entorhinal_core::shared_entry::MemberName {
                            author: B.into(),
                            display_name: "Renamed Mac".into(),
                        },
                        "fixture",
                    )
                    .unwrap();
                assert_eq!(f.a.admit_to(B, "same").await.unwrap(), cut);
                f.a.restart().await;
                assert_eq!(f.a.admit_to(B, "same").await.unwrap(), cut);
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn hung_shared_writer_cannot_delay_admission_or_full_move() {
        let f = Pair::new(&[("pi", "pi-session")]).await;
        let _writer = f.a.machine.handler.writer.lock().await;
        let generations = (
            f.a.store().generation().unwrap(),
            f.b.store().generation().unwrap(),
        );
        let cut = f.published("writer").await;
        f.finish(&cut).await;
        assert_eq!(
            generations,
            (
                f.a.store().generation().unwrap(),
                f.b.store().generation().unwrap()
            )
        );
    }

    #[tokio::test(start_paused = true)]
    async fn boundary_abort_matrix_writes_nothing_then_restart_completes() {
        for step in [
            MoveStep::Activating,
            MoveStep::Activated,
            MoveStep::Done,
            MoveStep::TakenOver,
            MoveStep::Releasing,
            MoveStep::Released,
        ] {
            let mut f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published("boundary-matrix").await;
            f.b.pickup().await;
            f.b.receiver_until(
                &cut,
                if step == MoveStep::Activating {
                    step
                } else {
                    MoveStep::Activated
                },
            )
            .await;
            if step == MoveStep::Releasing {
                f.a.records
                    .fake
                    .error(&format!("claim:{AGENT}"), Some("unavailable"));
                drive(f.a.source(&cut).tick()).await.unwrap();
            } else if step == MoveStep::TakenOver {
                f.b.door.script(
                    "cut.takeover",
                    DoorEffect::Takeover(crate::door_client::TakeoverProgress {
                        state: crate::door_client::TakeoverState::Taken,
                        session_ref: None,
                        code: None,
                    }),
                );
                elapse(600_000);
                f.b.receiver_until(&cut, step).await;
            } else if matches!(step, MoveStep::Released | MoveStep::Done) {
                f.a.source_until(&cut, MoveStep::Released).await;
                if step == MoveStep::Done {
                    f.b.receiver_until(&cut, step).await;
                }
            }
            let mac = if matches!(step, MoveStep::Releasing | MoveStep::Released) {
                &f.a
            } else {
                &f.b
            };
            assert_eq!(mac.row(&cut).step, step);
            let before = mac.row(&cut);
            assert_eq!(
                mac.store()
                    .request_agent_move_abort(&cut, clock())
                    .unwrap_err()
                    .code,
                "invalid_state"
            );
            assert_eq!(mac.row(&cut), before);
            assert_eq!(
                mac.machine
                    .bytes_on(
                        CORE,
                        "agent.move_abort",
                        json!({"cut_id":cut,"request_key":"boundary"})
                    )
                    .await
                    .unwrap_err(),
                "invalid_state"
            );
            assert_eq!(mac.row(&cut), before);
            f.a.records.fake.error(&format!("claim:{AGENT}"), None);
            f.a.restart().await;
            f.b.restart().await;
            elapse(300_000);
            if !f.b.row(&cut).step.is_terminal() {
                f.b.receiver_until(&cut, MoveStep::Activated).await;
            }
            f.a.source_until(&cut, MoveStep::Released).await;
            if f.b.row(&cut).step != MoveStep::TakenOver {
                f.b.receiver_until(&cut, MoveStep::Done).await;
            }
            for mac in [&f.a, &f.b] {
                assert!(!mac.discovery.peers["pi"]
                    .calls()
                    .iter()
                    .any(|r| matches!(r, Request::Abort(_))));
            }
            assert_eq!(f.b.claim().await.version, 1);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn settlement_requires_persisted_take_exact_body_and_receiver_author() {
        for mode in ["matching", "wrong-author", "wrong-epoch", "no-take"] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let cut = f.published(mode).await;
            f.b.pickup().await;
            f.b.receiver_until(&cut, MoveStep::Staged).await;
            if mode != "no-take" {
                let mut row = f.b.row(&cut);
                row.claim_take_body = Some(
                    json!({"v":1,"agent_id":AGENT,"holder":B,"epoch":1,"cut_id":cut,"reason":null}),
                );
                row.claim_take_version = Some(0);
                f.b.store().update_agent_move(&row).unwrap();
            }
            put_claim(
                &f.b,
                1,
                Some(B),
                if mode == "wrong-epoch" { 2 } else { 1 },
                Some(&cut),
                if mode == "wrong-author" { A } else { B },
            );
            if mode == "matching" {
                f.finish(&cut).await;
                assert_eq!(f.b.claim().await.version, 1);
            } else {
                let mut receiver = f.b.receiver(&cut);
                for _ in 0..3 {
                    elapse(300_000);
                    receiver.tick().await.unwrap();
                }
                // Without proof of our own claim take, returning ownership
                // could overwrite another writer. Publishing abort while the
                // receiver still owns the claim could let both Macs resume writes.
                assert_eq!(f.b.row(&cut).step, MoveStep::Staged);
                assert_eq!(f.b.record(&cut).await.state, MoveState::Staged);
                assert!(f.b.row(&cut).hold.is_some());
                assert!(!f.b.discovery.peers["pi"]
                    .calls()
                    .iter()
                    .any(|r| matches!(r, Request::Activate(_))));
            }
        }
    }
    #[tokio::test(start_paused = true)]
    async fn writer_appearing_after_diverged_holds_import_and_diverged_runs_again() {
        let f = Pair::with_receiver_refs(&[("pi", "pi-session")], true).await;
        f.b.store().apply_entry("fixture","{}","fixture",None,|tx|tx.execute_batch("INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES('pi-session','ref-pi-session','agent_0123456789abcdef',NULL,NULL,'','move',1);")).unwrap();
        let cut = f.published("writer-after-diverged").await;
        f.b.pickup().await;
        *f.b.discovery.gate.trap.lock().unwrap() =
            Some(("pi".into(), "agent_data.diverged".into(), true));
        let mut driver = f.b.receiver(&cut);
        let mut future = Box::pin(driver.tick());
        drive(async {tokio::select! {result=&mut future=>panic!("diverged gate not reached: {result:?}"),permit=f.b.discovery.gate.hit.acquire()=>permit.unwrap().forget()}}).await;
        let reference = AgentRef::new("pi-session", "ref-pi-session");
        f.b.discovery.peers["pi"].set_busy(
            reference.clone(),
            BusyRef::new(BusyState::Busy).with_reason_code("run_active"),
        );
        f.b.discovery.gate.release.add_permits(1);
        drive(&mut future).await.unwrap();
        drop(future);
        drop(driver);
        assert_eq!(f.b.row(&cut).step, MoveStep::PickedUp);
        assert!(f.b.row(&cut).hold.is_some());
        assert!(!f.b.discovery.peers["pi"]
            .calls()
            .iter()
            .any(|r| matches!(r, Request::Import(_))));
        f.b.discovery.peers["pi"].set_busy(reference, BusyRef::new(BusyState::Idle));
        elapse(300_000);
        f.finish(&cut).await;
        assert_eq!(
            f.b.discovery.peers["pi"]
                .calls()
                .iter()
                .filter(|r| matches!(r, Request::Diverged(_)))
                .count(),
            2
        );
    }
    #[tokio::test(start_paused = true)]
    async fn lost_append_with_receiver_finishing_before_retry_never_readds_inbox_entry() {
        let mut f = Pair::new(&[("pi", "pi-session")]).await;
        f.a.door
            .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
        let cut = f.a.admit_to(B, "lost-append").await.unwrap();
        f.a.source_until(&cut, MoveStep::Sealed).await;
        *f.a.records.trap.lock().unwrap() = Some(Trap {
            method: "record.cas".into(),
            subject: format!("inbox:{B}"),
            after: true,
        });
        let mut driver = f.a.source(&cut);
        let mut future = Box::pin(driver.tick());
        drive(async {tokio::select! {result=&mut future=>panic!("append gate not reached: {result:?}"),permit=f.a.records.hit.acquire()=>permit.unwrap().forget()}}).await;
        f.b.pickup().await;
        f.b.receiver_until(&cut, MoveStep::Activated).await;
        f.a.records.release.add_permits(1);
        drive(&mut future).await.unwrap();
        let after_retry =
            f.a.machine
                .handler
                .record_client
                .read(&format!("inbox:{B}"))
                .await
                .unwrap();
        assert!(
            wire::decode_inbox_record(after_retry.body.as_deref().unwrap())
                .unwrap()
                .pending
                .is_empty()
        );
        drop(future);
        drop(driver);
        f.a.restart().await;
        f.a.source_until(&cut, MoveStep::Released).await;
        f.b.receiver_until(&cut, MoveStep::Done).await;
        f.a.restart().await;
        drive(f.a.source(&cut).tick()).await.unwrap();
        f.b.pickup().await;
        let inbox =
            f.a.machine
                .handler
                .record_client
                .read(&format!("inbox:{B}"))
                .await
                .unwrap();
        assert!(wire::decode_inbox_record(inbox.body.as_deref().unwrap())
            .unwrap()
            .pending
            .is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn lost_participant_and_record_replies_settle_without_duplicate_claim_epochs() {
        for op in [
            "agent_data.prepare",
            "agent_data.export",
            "agent_data.import",
            "agent_data.activate",
            "agent_data.release",
            "record.cas",
        ] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            if op == "record.cas" {
                f.a.records.fake.drop_reply.store(true, Ordering::Relaxed);
            } else {
                let mac = if ["agent_data.import", "agent_data.activate"].contains(&op) {
                    &f.b
                } else {
                    &f.a
                };
                mac.discovery.peers["pi"].script(op, Effect::LoseReply(TransportError::NoReply));
            }
            let cut = f.published(op).await;
            f.finish(&cut).await;
            assert_eq!(f.b.claim().await.version, 1);
            if op != "record.cas" {
                let mac = if ["agent_data.import", "agent_data.activate"].contains(&op) {
                    &f.b
                } else {
                    &f.a
                };
                assert!(mac.discovery.peers["pi"]
                    .calls()
                    .iter()
                    .any(|r| matches!(r, Request::Status(_))));
            }
            f.clean();
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_sources_append_one_inbox_without_overwriting_each_other() {
        let f = Pair::with_receiver_refs(&[("pi", "pi-session")], true).await;
        let target = "03030303030303030303030303030303";
        for mac in [&f.a, &f.b] {
            mac.store()
                .set_member_name(
                    entorhinal_core::shared_entry::MemberName {
                        author: target.into(),
                        display_name: "Third Mac".into(),
                    },
                    "fixture",
                )
                .unwrap();
            mac.door
                .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
        }
        let a = f.a.admit_to(target, "append-a").await.unwrap();
        let b = f.b.admit_to(target, "append-b").await.unwrap();
        f.a.source_until(&a, MoveStep::Sealed).await;
        f.b.source_until(&b, MoveStep::Sealed).await;
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        *f.a.records.append_barrier.lock().unwrap() = Some(barrier.clone());
        *f.b.records.append_barrier.lock().unwrap() = Some(barrier);
        let mut a_driver = f.a.source(&a);
        let mut b_driver = f.b.source(&b);
        let (one, two) = drive(async { tokio::join!(a_driver.tick(), b_driver.tick()) }).await;
        one.unwrap();
        two.unwrap();
        let inbox =
            f.a.machine
                .handler
                .record_client
                .read(&format!("inbox:{target}"))
                .await
                .unwrap();
        let pending = wire::decode_inbox_record(inbox.body.as_deref().unwrap())
            .unwrap()
            .pending;
        assert_eq!(
            pending
                .iter()
                .map(|p| p.cut_id.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from([a, b])
        );
        assert_eq!(pending.len(), 2);
    }
    #[tokio::test(start_paused = true)]
    async fn largest_admitted_body_still_fits_operator_abort_and_oversized_refusal() {
        for refusal in [false, true] {
            let f = Pair::new(&[("pi", "pi-session")]).await;
            let base = json!({"v":1,"cut_id":"0".repeat(32),"agent_id":AGENT,"from":A,"to":B,"state":"published","claim_version":0,"participants":[{"module_id":"pi","refs":[{"ref_kind":"pi-session","ref":"ref-pi-session"}],"external":[{"ref_kind":"pi-session","ref":"ref-pi-session","note":"","check":null}]}],"reason":null,"detail":null});
            let padding = 61_440 - serde_json::to_vec(&base).unwrap().len();
            f.a.discovery.peers["pi"].set_prepare(serde_json::from_value(json!({"items":[{"ref_kind":"pi-session","ref":"ref-pi-session","sessionRef":"00000000000000000000000000000010","stream":"reserved:pi","endOffset":42,"digest":"a".repeat(64),"format":"v1","frozen":true}],"external":[{"ref_kind":"pi-session","ref":"ref-pi-session","note":"n".repeat(padding),"check":null}]})).unwrap());
            let cut = f.published("largest").await;
            let published =
                f.a.machine
                    .handler
                    .record_client
                    .read(&format!("move:{cut}"))
                    .await
                    .unwrap();
            assert_eq!(published.body.as_deref().unwrap().len(), 61_440);
            if refusal {
                f.b.pickup().await;
                f.b.discovery.peers["pi"].script(
                    "agent_data.import",
                    Effect::Refuse(Refusal {
                        code: "incompatible_version".into(),
                        detail: Some(json!({"found":"f".repeat(10_000),"required":"v2"})),
                    }),
                );
                f.b.receiver_until(&cut, MoveStep::Aborted).await;
            } else {
                f.a.abort(&cut);
            }
            f.a.source_until(&cut, MoveStep::Aborted).await;
            let aborted =
                f.a.machine
                    .handler
                    .record_client
                    .read(&format!("move:{cut}"))
                    .await
                    .unwrap();
            assert!(aborted.body.as_deref().unwrap().len() <= 65_536);
            assert_eq!(
                wire::decode_move_record(&format!("move:{cut}"), aborted.body.as_deref().unwrap())
                    .unwrap()
                    .state,
                MoveState::Aborted
            );
            f.clean();
        }
    }
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
    let head = a
        .call(
            "agent.create",
            json!({"role":"head","name":"Head","tag":"head","project_id":"P","request_key":"head"}),
        )
        .await
        .unwrap()["agent"]["agent_id"]
        .as_str()
        .unwrap()
        .to_owned();
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

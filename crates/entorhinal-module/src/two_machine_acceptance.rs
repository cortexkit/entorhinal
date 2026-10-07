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
            dir: dir.canonicalize().unwrap(),
            descriptor,
            handler,
        }
    }

    fn open(descriptor: &StorageDescriptor, connector: Arc<dyn LogConnector>) -> ProjectsHandler {
        let handler = ProjectsHandler::with_log_connector("acceptance".into(), || 700, connector);
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
            dir: dir.canonicalize().unwrap(),
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
        path.canonicalize().unwrap().to_string_lossy().into_owned()
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

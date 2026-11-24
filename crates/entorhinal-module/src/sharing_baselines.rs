//! Frozen sharing stores and byte comparisons across the schema upgrade.

use super::*;
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use std::{collections::BTreeMap, fs, path::Path};

#[path = "sharing_baselines/acceptance.rs"]
mod acceptance;
#[path = "sharing_baselines/record.rs"]
pub(crate) mod record;
#[path = "sharing_baselines/reply_exceptions.rs"]
mod reply_exceptions;

pub(crate) const NOW: i64 = 700;
pub(crate) const BOX_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIRECT: RouteKey = (73, 1);
const SESSIONS: RouteKey = (73, 2);
const WRITER: RouteKey = (73, 3);
pub(crate) const BASELINE_AGENT: &str = "agent_0000000000000001";
const STORES: &[(&str, &str, Option<i64>)] = &[
    ("legacy-zero", "enabled", Some(3)),
    ("legacy-supersede", "enabled", Some(8)),
    ("legacy-catch-up", "enabled", Some(8)),
    ("native-installed", "enabled", Some(0)),
    ("native-enabling", "enabling", None),
    ("disabled", "disabled", None),
];
const SNAPSHOT_OPS: &str = "'root_key.backfill','shared.snapshot','hub.snapshot'";
// Keep the manifest explicit: a missing derived table must not silently
// disappear from the comparisons when the replay implementation changes.
const DERIVED: &[&str] = &[
    "hub_membership",
    "placement_fact",
    "placement_state",
    "user_expertise",
    "user_profile",
    "catalog_pin",
    "catalog_pin_set",
    "member_name",
    "member_box",
    "agent_name_claim",
    "agent",
    "workspace_document",
    "workspace_member",
    "project_workspace",
    "project_alias",
    "derived_root_parent",
    "root_owned_remotes",
    "project_root_key",
    "project_root",
    "project",
    "workspace_root",
    "workspace",
    "root_binding",
    "retired_binding",
    "root_approval",
];

pub(crate) fn artifacts() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/sharing-baselines")
}

pub(crate) struct StoreFile {
    dir: PathBuf,
    descriptor: StorageDescriptor,
}

impl StoreFile {
    pub(crate) fn new(label: &str) -> Self {
        let (dir, descriptor) = tests::scratch_descriptor(label);
        Self { dir, descriptor }
    }

    pub(crate) fn copy(source: &Path) -> Self {
        for suffix in ["-wal", "-shm"] {
            assert!(
                !source.with_extension(format!("db{suffix}")).exists(),
                "sidecar for {}",
                source.display()
            );
        }
        let file = Self::new("sharing-baseline-copy");
        fs::copy(source, file.path()).unwrap();
        file
    }

    pub(crate) fn path(&self) -> &str {
        let StorageBackend::Sqlite { path } = &self.descriptor.backend else {
            unreachable!()
        };
        path
    }

    pub(crate) fn open(&self) -> RegistryStore {
        RegistryStore::open(&self.descriptor).unwrap()
    }

    pub(crate) fn read(&self) -> Connection {
        Connection::open_with_flags(self.path(), OpenFlags::SQLITE_OPEN_READ_ONLY).unwrap()
    }
}

impl Drop for StoreFile {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.dir);
    }
}

pub(crate) fn admit(handler: &ProjectsHandler) {
    for (route, principal) in [
        (DIRECT, Principal::Direct),
        (
            SESSIONS,
            Principal::Reserved {
                module_id: "phone".into(),
            },
        ),
        (
            WRITER,
            Principal::Reserved {
                module_id: "prefrontal".into(),
            },
        ),
    ] {
        handler.route_admissions().insert(
            route,
            RouteAdmission {
                principal: Some(principal),
                ..Default::default()
            },
        );
    }
}

fn open_handler(file: &StoreFile, own: &str) -> ProjectsHandler {
    let handler = ProjectsHandler::with_runtime(own.into(), || NOW);
    install_handler(&handler, file.open());
    handler
}

pub(crate) fn install_handler(handler: &ProjectsHandler, store: RegistryStore) {
    admit(handler);
    let tick = Instant::now();
    install_store_with_time(
        &handler.store,
        &handler.health,
        &handler.catalog_sql,
        Ok(store),
        tick,
        || tick,
    );
    assert!(handler.health.store_ready.load(Ordering::Acquire));
}

pub(crate) async fn call(handler: &ProjectsHandler, method: &str, params: Value) -> String {
    let route = match method {
        "agent.session_residence" => SESSIONS,
        "agent.create" | "agent.import" => WRITER,
        _ => DIRECT,
    };
    let request = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
    match handler.handle_served_request(&request, route).await {
        HandlerOutcome::Response(bytes) => String::from_utf8(bytes).unwrap(),
        HandlerOutcome::Error { code, message } => {
            json!({"error":{"code":code,"message":message}}).to_string()
        }
        HandlerOutcome::ErrorWithDetail {
            code,
            message,
            detail,
        } => json!({"error":{"code":code,"message":message,"detail":detail}}).to_string(),
        outcome => panic!("{method}: unexpected outcome {outcome:?}"),
    }
}

pub(crate) fn rows(conn: &Connection, sql: &str) -> Vec<Vec<Value>> {
    let mut stmt = conn.prepare(sql).unwrap();
    let n = stmt.column_count();
    stmt.query_map([], |row| {
        (0..n)
            .map(|i| {
                Ok(match row.get_ref(i)? {
                    ValueRef::Null => Value::Null,
                    ValueRef::Integer(a) => json!(a),
                    ValueRef::Real(a) => json!(a),
                    ValueRef::Text(a) => json!(std::str::from_utf8(a).unwrap()),
                    ValueRef::Blob(a) => json!({"hex": log_client::encode_hex(a)}),
                })
            })
            .collect::<rusqlite::Result<Vec<_>>>()
    })
    .unwrap()
    .collect::<rusqlite::Result<_>>()
    .unwrap()
}

fn rebuild_prefixes(source: &Path) -> String {
    let file = StoreFile::copy(source);
    let snapshots = rows(
        &file.read(),
        &format!("SELECT seq,op FROM registry_journal WHERE op IN ({SNAPSHOT_OPS}) ORDER BY seq"),
    );
    let mut prefixes = Vec::new();
    for snapshot in snapshots {
        let before = snapshot[0].as_i64().unwrap();
        let file = StoreFile::copy(source);
        Connection::open(file.path())
            .unwrap()
            .execute("DELETE FROM registry_journal WHERE seq>=?1", [before])
            .unwrap();
        file.open().rebuild().unwrap();
        let conn = file.read();
        let tables: serde_json::Map<String, Value> = DERIVED
            .iter()
            .map(|table| {
                let mut data = rows(&conn, &format!("SELECT * FROM {table}"));
                data.sort_by_cached_key(|row| serde_json::to_string(row).unwrap());
                ((*table).into(), json!(data))
            })
            .collect();
        prefixes.push(json!({"before_seq":before,"snapshot_op":snapshot[1],"tables":tables}));
    }
    format!("{}\n", serde_json::to_string_pretty(&prefixes).unwrap())
}

pub(crate) async fn served_reads(
    handler: &ProjectsHandler,
    file: &StoreFile,
    roles: &[String],
) -> String {
    let conn = file.read();
    let agents = rows(&conn, "SELECT agent_id FROM agent ORDER BY agent_id")
        .into_iter()
        .map(|row| row[0].clone())
        .collect::<Vec<_>>();
    let sessions = rows(
        &conn,
        "SELECT DISTINCT runner,session_id FROM session_binding ORDER BY runner,session_id",
    );
    drop(conn);
    let mut requests = Vec::new();
    if !agents.is_empty() {
        requests.push(("agent.where_many", json!({"agentIds":agents})));
    }
    for role in roles {
        for method in ["role.read", "role.fence"] {
            requests.push((method, json!({"role":role})));
        }
    }
    for session in sessions {
        requests.push((
            "agent.session_residence",
            json!({"runner":session[0],"session":session[1]}),
        ));
    }
    let mut out = String::new();
    for (method, params) in requests {
        let reply = call(handler, method, params.clone()).await;
        out.push_str(&format!(
            "{{\"method\":\"{method}\",\"params\":{params},\"reply\":{reply}}}\n"
        ));
    }
    out
}

pub(crate) fn force_membership(handler: &ProjectsHandler, selected: bool) {
    handler
        .health
        .hub_membership
        .store(u64::from(selected), Ordering::Release);
    assert_eq!(handler.hub_backend_selected(), selected);
    assert_eq!(
        handler.health.hub_membership.load(Ordering::Acquire),
        u64::from(selected)
    );
    assert!(handler.hub_residence.lock().unwrap().is_none());
}

async fn store_replies(source: &Path, installed: bool, forced: bool) -> String {
    let file = StoreFile::copy(source);
    // Keep probing the recorded roles even after migration erases those rows.
    let roles = rows(&file.read(), "SELECT role FROM role_cache ORDER BY role")
        .into_iter()
        .map(|row| row[0].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
    let handler = open_handler(&file, BOX_A);
    assert_eq!(handler.hub_backend_selected(), installed);
    assert!(handler.hub_residence.lock().unwrap().is_none());
    if forced {
        force_membership(&handler, false);
    }
    let mut replies = served_reads(&handler, &file, &roles).await;
    let refresh = call(&handler, "agent.refresh_claims", json!({})).await;
    replies.push_str(&format!(
        "{{\"method\":\"agent.refresh_claims\",\"params\":{{}},\"reply\":{refresh}}}\n"
    ));
    replies
}

async fn refresh_replies(source: &Path) -> String {
    let mut out = String::new();
    for selected in [false, true] {
        let file = StoreFile::copy(source);
        let handler = open_handler(&file, BOX_A);
        force_membership(&handler, selected);
        let reply = call(&handler, "agent.refresh_claims", json!({})).await;
        out.push_str(&format!("{{\"selected\":{selected},\"reply\":{reply}}}\n"));
    }
    out
}

async fn status_replies(source: &Path) -> (String, String) {
    let file = StoreFile::copy(source);
    let handler = open_handler(&file, BOX_A);
    let reply = call(&handler, "identity_log.status", json!({})).await;
    let value: Value = serde_json::from_str(&reply).unwrap();
    let child = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "cli::sharing_baseline_cli_render_child",
            "--ignored",
            "--nocapture",
        ])
        .env("ENTORHINAL_BASELINE_STATUS", value["result"].to_string())
        .output()
        .unwrap();
    assert!(child.status.success(), "{child:?}");
    let output = String::from_utf8(child.stdout).unwrap();
    let output = output
        .split_once("running 1 test\n")
        .unwrap()
        .1
        .split_once("test cli::sharing_baseline_cli_render_child ... ok\n")
        .unwrap()
        .0;
    (format!("{reply}\n"), output.to_owned())
}

async fn history_outputs(dir: &Path, name: &str, base: Option<i64>) -> Vec<(String, String)> {
    let source = dir.join(format!("{name}.db"));
    let mut outputs = vec![("prefixes.json".into(), rebuild_prefixes(&source))];
    for (mode, force) in [("default", false), ("unselected", true)] {
        outputs.push((
            format!("{mode}.jsonl"),
            store_replies(&source, base.is_some(), force).await,
        ));
    }
    if matches!(name, "disabled" | "native-enabling" | "native-installed") {
        outputs.push(("refresh.jsonl".into(), refresh_replies(&source).await));
        let (rpc, cli) = status_replies(&source).await;
        outputs.push(("status.json".into(), rpc));
        outputs.push(("status.txt".into(), cli));
    }
    outputs
}

pub(crate) fn check_store(file: &StoreFile, store: &RegistryStore, state: &str, base: Option<i64>) {
    assert_eq!(store.identity_log_status().unwrap().state, state);
    assert_eq!(store.hub_log_base().unwrap(), base);
    let verify = store.verify().unwrap();
    assert!(verify.ok, "{verify:?}");
    let conn = file.read();
    assert_schema_version(&conn, 28);
    assert_eq!(
        rows(&conn, "SELECT COUNT(*) FROM identity_log_state"),
        vec![vec![json!(1)]]
    );
    assert!(rows(&conn, "SELECT role FROM role_cache").is_empty());
    assert_eq!(store.role_gen_high_water("connector_home").unwrap(), 13);
}

fn assert_schema_version(conn: &Connection, version: i64) {
    assert_eq!(
        rows(
            conn,
            "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace='projects'"
        ),
        vec![vec![json!(version)]]
    );
}

fn check_legacy_rows(conn: &Connection, name: &str) {
    assert_schema_version(conn, 27);
    assert_eq!(
        rows(
            conn,
            "SELECT role,source,epoch,version FROM role_cache WHERE role='connector_home'"
        ),
        vec![vec![
            json!("connector_home"),
            json!("cloud"),
            json!(3),
            json!(5)
        ]]
    );
    assert_eq!(
        rows(conn, "SELECT agent_id,epoch,version FROM claim_cache"),
        vec![vec![json!(BASELINE_AGENT), json!(3), json!(5)]]
    );
    if name == "disabled" {
        assert_eq!(
            rows(
                conn,
                "SELECT source,epoch FROM role_cache WHERE role='local_role'"
            ),
            vec![vec![json!("local"), json!(1)]]
        );
        assert_eq!(
            rows(
                conn,
                "SELECT source,epoch FROM role_cache WHERE role='enrolled'"
            ),
            vec![vec![Value::Null, json!(0)]]
        );
        assert_eq!(
            rows(conn, "SELECT epoch FROM role_take_request"),
            vec![vec![json!(1)]]
        );
    }
    if name == "native-enabling" {
        assert_eq!(
            rows(conn, "SELECT enable_backfill FROM identity_log_state"),
            vec![vec![Value::Null]]
        );
    }
    if name == "native-installed" || name == "switch-observer" {
        let pending = rows(conn, "SELECT continuation FROM pending_entry");
        let [row] = pending.as_slice() else {
            panic!("expected one pending write, got {pending:?}");
        };
        let continuation: Value = serde_json::from_str(row[0].as_str().unwrap()).unwrap();
        assert_eq!(continuation["request_key"], "unanswered");
        assert_eq!(continuation["params_digest"], "d0".repeat(32));
        assert!(
            serde_json::from_str::<Value>(continuation["reply"].as_str().unwrap())
                .unwrap()
                .get("result")
                .is_some()
        );
    }
}

pub(crate) fn check_pending(file: &StoreFile, store: &RegistryStore) {
    let status = store.identity_log_status().unwrap();
    assert_eq!(status.pending_write_count, 1);
    let found = rows(
        &file.read(),
        "SELECT entry_id,expected_head FROM pending_entry",
    );
    let [row] = found.as_slice() else {
        panic!("expected one pending write, got {found:?}");
    };
    assert_eq!(row[0], "51".repeat(16));
    assert_eq!(row[1], status.last_applied_position);
}

fn projection(conn: &Connection, table: &str, columns: &[Vec<Value>]) -> Vec<Vec<Value>> {
    let names = columns
        .iter()
        .map(|col| format!("\"{}\"", col[1].as_str().unwrap()))
        .collect::<Vec<_>>()
        .join(",");
    let mut cells = rows(conn, &format!("SELECT {names} FROM \"{table}\""));
    cells.sort_by_cached_key(|row| serde_json::to_string(row).unwrap());
    cells
}

fn table_info(conn: &Connection, table: &str) -> Vec<Vec<Value>> {
    rows(conn, &format!("PRAGMA table_info(\"{table}\")"))
}

fn schema(conn: &Connection) -> Vec<Vec<Value>> {
    rows(
        conn,
        "SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name",
    )
}

fn migrate_history(file: &StoreFile, name: &str) -> RegistryStore {
    let conn = file.read();
    check_legacy_rows(&conn, name);
    let before_schema = schema(&conn);
    let mut expected_schema = before_schema.clone();
    expected_schema
        .retain(|item| !matches!(item[2].as_str(), Some("role_take_request" | "claim_cache")));
    let mut before = BTreeMap::new();
    for item in &expected_schema {
        if item[0] != "table" {
            continue;
        }
        let table = item[1].as_str().unwrap();
        let mut columns = table_info(&conn, table);
        let old_count = columns.len();
        let removed: &[&str] = match table {
            "identity_log_state" => &["enable_backfill", "engram_frozen", "hub_quieting"],
            "pending_entry" => &["continuation"],
            _ => &[],
        };
        columns.retain(|col| !removed.contains(&col[1].as_str().unwrap()));
        assert_eq!(
            old_count - columns.len(),
            removed.len(),
            "{name}: V27 {table}"
        );
        for (index, col) in columns.iter_mut().enumerate() {
            col[0] = json!(index);
        }
        let data = match table {
            // Opening a writer advances the lease fence independently of migrations.
            "cortexkit_fence" | "role_cache" => Vec::new(),
            _ => projection(&conn, table, &columns),
        };
        before.insert(table.to_owned(), (columns, data));
    }
    drop(conn);

    let store = file.open();
    let conn = file.read();
    let mut after_schema = schema(&conn);
    // SQLite rewrites these two CREATE TABLE texts. Compare their retained
    // columns below, but compare every other schema object byte-for-byte.
    for items in [&mut expected_schema, &mut after_schema] {
        for item in items {
            if item[0] == "table"
                && matches!(
                    item[1].as_str(),
                    Some("identity_log_state" | "pending_entry")
                )
            {
                item[3] = Value::Null;
            }
        }
    }
    assert_eq!(after_schema, expected_schema, "{name}: migrated schema");
    for (table, (columns, data)) in before {
        assert_eq!(
            table_info(&conn, &table),
            columns,
            "{name}: {table} columns"
        );
        if table == "cortexkit_fence" {
            continue;
        }
        let mut actual = projection(&conn, &table, &columns);
        if table == "cortexkit_schema_version" {
            let count = actual.len();
            actual.retain(|row| !(row[0] == "projects" && row[1] == 28));
            assert_eq!(count - actual.len(), 1, "{name}: new migration row");
        }
        assert_eq!(actual, data, "{name}: {table} rows");
    }
    store
}

fn check_history(file: &StoreFile, name: &str, state: &str, base: Option<i64>) {
    let store = migrate_history(file, name);
    check_store(file, &store, state, base);
    let conn = file.read();
    check_rebuild(&conn, file.path());
    let enable = rows(&conn, "SELECT actor,payload_json,stream,entry FROM registry_journal WHERE op='identity_log.enable'");
    let expected = match name {
        "legacy-zero" => Some(("operator", "{}")),
        "legacy-supersede" => Some(("operator", "{\"supersedes_through\":1}")),
        "legacy-catch-up" | "native-installed" => Some(("log", "{}")),
        _ => None,
    };
    assert_eq!(
        enable,
        expected
            .into_iter()
            .map(|(a, p)| vec![json!(a), json!(p), json!("local"), Value::Null])
            .collect::<Vec<_>>()
    );
    if name.starts_with("legacy-") {
        let migration = rows(&conn, "SELECT log_position,stream,origin,json_extract(CAST(entry AS TEXT),'$.excluded') FROM registry_journal WHERE op='log.migrated'");
        let excluded = if name == "legacy-zero" {
            "[]".into()
        } else {
            format!("[\"{}\"]", "b".repeat(32))
        };
        assert_eq!(
            migration,
            vec![vec![
                json!(base),
                json!("shared"),
                json!("log"),
                json!(excluded)
            ]]
        );
    }
    if matches!(name, "legacy-zero" | "legacy-supersede") {
        assert_eq!(rows(&conn, "SELECT stream,origin,json_extract(CAST(entry AS TEXT),'$.op') FROM registry_journal WHERE op='root_key.backfill'"), vec![vec![json!("shared"), json!("here"), json!("project.shared")]]);
    }
    if name == "legacy-zero" {
        assert_eq!(rows(&conn, "SELECT stream,entry IS NULL FROM registry_journal WHERE op='hub.snapshot' ORDER BY seq"), vec![vec![json!("local"), json!(1)], vec![json!("shared"), json!(0)]]);
    }
    if name == "legacy-catch-up" {
        assert_eq!(rows(&conn, "SELECT json_extract(payload_json,'$.snapshot_part.part'),json_extract(payload_json,'$.snapshot_part.parts'),json_extract(payload_json,'$.snapshot_part.supersedes_through'),stream,origin FROM registry_journal WHERE op='shared.snapshot' ORDER BY seq"),
            [(1,1,None),(1,1,Some(1)),(1,2,None),(2,2,None)].into_iter().map(|(part,count,supersedes)| vec![json!(part),json!(count),json!(supersedes),json!("shared"),json!("log")]).collect::<Vec<_>>());
        assert_eq!(rows(&conn, "SELECT op FROM registry_journal WHERE stream='shared' AND origin='log' AND op IN ('member.set_box','member.set_name','project.shared') ORDER BY seq"),
            ["member.set_box", "member.set_name", "project.shared"].into_iter().map(|op| vec![json!(op)]).collect::<Vec<_>>());
        assert_eq!(
            rows(
                &conn,
                "SELECT COUNT(*) FROM registry_journal WHERE op='agent.import' AND actor='log'"
            ),
            vec![vec![json!(2)]]
        );
    }
    if name == "native-installed" {
        check_pending(file, &store);
        assert_eq!(rows(&conn, "SELECT request_key,expected,outcome FROM hub_attempt WHERE request_key='outstanding-hub-attempt'"), vec![vec![json!("outstanding-hub-attempt"), json!(83), Value::Null]]);
        assert_eq!(rows(&conn, "SELECT op_id,sent_at_ms FROM hub_attempt_send WHERE request_key='outstanding-hub-attempt' ORDER BY sent_at_ms"), vec![vec![json!("31".repeat(16)),json!(600)],vec![json!("32".repeat(16)),json!(650)]]);
        assert_eq!(
            rows(
                &conn,
                "SELECT epoch,source_kind,source_id FROM hub_held_epoch ORDER BY epoch"
            ),
            [1, 2]
                .into_iter()
                .map(|epoch| vec![json!(epoch), json!("join"), json!("baseline-join")])
                .collect::<Vec<_>>()
        );
    }
    if name == "native-enabling" {
        let plan = store.hub_enable_plan().unwrap().unwrap();
        assert_eq!(plan.enable_id, "a1".repeat(16));
        assert_eq!(
            plan.snapshot_digest,
            entorhinal_catalog::sha256_hex(plan.preimage.as_bytes())
        );
        assert_eq!(
            serde_json::from_str::<Value>(&plan.preimage).unwrap()["engram_through"],
            0
        );
    }
}

fn check_rebuild(expected: &Connection, source: &str) {
    let empty = StoreFile::new("sharing-history-rebuild");
    drop(empty.open());
    let conn = Connection::open(empty.path()).unwrap();
    let columns = table_info(expected, "registry_journal")
        .iter()
        .map(|column| column[1].as_str().unwrap().to_owned())
        .collect::<Vec<_>>()
        .join(",");
    conn.execute("ATTACH DATABASE ?1 AS history", [source])
        .unwrap();
    conn.execute(
        &format!("INSERT INTO registry_journal ({columns}) SELECT {columns} FROM history.registry_journal ORDER BY seq"),
        [],
    ).unwrap();
    drop(conn);
    empty.open().rebuild().unwrap();
    let rebuilt = empty.read();
    for table in DERIVED {
        let columns = table_info(expected, table);
        assert_eq!(
            projection(&rebuilt, table, &columns),
            projection(expected, table, &columns),
            "rebuild of {table}"
        );
    }
}

#[tokio::test]
async fn sharing_baseline_history_replies_and_prefixes() {
    let dir = artifacts();
    for (name, state, base) in STORES {
        let source = dir.join(format!("{name}.db"));
        let frozen = fs::read(&source).unwrap();
        let file = StoreFile::copy(&source);
        check_history(&file, name, state, *base);
        for (kind, actual) in history_outputs(&dir, name, *base).await {
            let path = dir.join(format!("{name}.{kind}"));
            let mut expected = fs::read_to_string(&path).unwrap();
            if kind == "unselected.jsonl" || (kind == "default.jsonl" && base.is_none()) {
                reply_exceptions::apply(&path, name, *state == "disabled", &mut expected);
            }
            assert_eq!(expected, actual, "{}", path.display());
        }
        assert_eq!(fs::read(source).unwrap(), frozen);
    }
}

#[test]
fn sharing_baseline_migration_preserves_observer_and_adoption_clears_pending() {
    let source = artifacts().join("switch-observer.db");
    let frozen = fs::read(&source).unwrap();
    let file = StoreFile::copy(&source);
    let store = migrate_history(&file, "switch-observer");
    check_store(&file, &store, "enabled", Some(17));
    check_pending(&file, &store);
    check_rebuild(&file.read(), file.path());
    assert_eq!(fs::read(source).unwrap(), frozen);

    let source = artifacts().join("native-installed.db");
    let file = StoreFile::copy(&source);
    let store = file.open();
    check_pending(&file, &store);
    store.check_store_origin(Some(BOX_A), NOW).unwrap();
    let other = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
    store.check_store_origin(Some(other), NOW).unwrap();
    assert!(store.is_foreign());
    store.adopt_store(other, false, NOW).unwrap();
    assert!(!store.is_foreign());
    assert_eq!(store.identity_log_status().unwrap().pending_write_count, 0);
}

#[test]
fn sharing_baseline_verify_rejects_corrupt_migration_marker_rows() {
    let source = artifacts().join("legacy-zero.db");
    for (field, error) in [
        ("entry=NULL", "is shared but carries no entry bytes"),
        ("entry=X'7b'", "EOF while parsing an object"),
        ("payload_json='{'", "EOF while parsing an object"),
        ("created_at='unreadable'", "unreadable journal time"),
    ] {
        let file = StoreFile::copy(&source);
        let store = file.open();
        assert!(store.verify().unwrap().ok);
        let conn = Connection::open(file.path()).unwrap();
        assert_eq!(conn.execute(
            &format!("UPDATE registry_journal SET {field} WHERE op='log.migrated' AND stream='shared'"),
            [],
        ).unwrap(), 1);
        drop(conn);
        let failed = store.verify().unwrap_err();
        assert!(failed.to_string().contains(error), "{field}: {failed}");
    }
}

//! Frozen inputs and byte comparisons for the pre-migration sharing stores.

use super::*;
use rusqlite::{types::ValueRef, Connection, OpenFlags};
use std::{fs, path::Path};

#[path = "sharing_baselines/record.rs"]
pub(crate) mod record;

pub(crate) const NOW: i64 = 700;
pub(crate) const BOX_A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const DIRECT: RouteKey = (73, 1);
const SESSIONS: RouteKey = (73, 2);
const WRITER: RouteKey = (73, 3);
const BASELINE_AGENT: &str = "agent_0000000000000001";
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
    admit(&handler);
    let tick = Instant::now();
    install_store_with_time(
        &handler.store,
        &handler.health,
        &handler.catalog_sql,
        Ok(file.open()),
        tick,
        || tick,
    );
    assert!(handler.health.store_ready.load(Ordering::Acquire));
    handler
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

fn force_membership(handler: &ProjectsHandler, selected: bool) {
    handler
        .health
        .hub_membership
        .store(u64::from(selected), Ordering::Release);
    assert_eq!(handler.hub_backend_selected(), selected);
    assert!(handler.hub_residence.lock().unwrap().is_none());
}

async fn store_replies(source: &Path, installed: bool, forced: bool) -> String {
    let file = StoreFile::copy(source);
    let handler = open_handler(&file, BOX_A);
    assert_eq!(handler.hub_backend_selected(), installed);
    assert!(handler.hub_residence.lock().unwrap().is_none());
    if forced {
        force_membership(&handler, false);
    }
    let roles = rows(&file.read(), "SELECT role FROM role_cache ORDER BY role")
        .into_iter()
        .map(|row| row[0].as_str().unwrap().to_owned())
        .collect::<Vec<_>>();
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
    assert_eq!(
        rows(
            &conn,
            "SELECT MAX(version) FROM cortexkit_schema_version WHERE namespace='projects'"
        ),
        vec![vec![json!(27)]]
    );
    assert_eq!(
        rows(&conn, "SELECT COUNT(*) FROM identity_log_state"),
        vec![vec![json!(1)]]
    );
    assert_eq!(
        rows(
            &conn,
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
        rows(&conn, "SELECT agent_id,epoch,version FROM claim_cache"),
        vec![vec![json!(BASELINE_AGENT), json!(3), json!(5)]]
    );
    assert_eq!(store.role_gen_high_water("connector_home").unwrap(), 13);
}

pub(crate) fn check_pending(file: &StoreFile, store: &RegistryStore) {
    let status = store.identity_log_status().unwrap();
    assert_eq!(status.pending_write_count, 1);
    let found = rows(
        &file.read(),
        "SELECT entry_id,expected_head,continuation FROM pending_entry",
    );
    let [row] = found.as_slice() else {
        panic!("expected one pending write, got {found:?}");
    };
    assert_eq!(row[0], "51".repeat(16));
    assert_eq!(row[1], status.last_applied_position);
    let continuation: Value = serde_json::from_str(row[2].as_str().unwrap()).unwrap();
    assert_eq!(continuation["request_key"], "unanswered");
    assert_eq!(continuation["params_digest"], "d0".repeat(32));
    assert!(
        serde_json::from_str::<Value>(continuation["reply"].as_str().unwrap())
            .unwrap()
            .get("result")
            .is_some()
    );
}

fn check_history(file: &StoreFile, name: &str, state: &str, base: Option<i64>) {
    let store = file.open();
    check_store(file, &store, state, base);
    let conn = file.read();
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
    if name == "disabled" {
        assert_eq!(
            rows(
                &conn,
                "SELECT source,epoch FROM role_cache WHERE role='local_role'"
            ),
            vec![vec![json!("local"), json!(1)]]
        );
        assert_eq!(
            rows(&conn, "SELECT epoch FROM role_take_request"),
            vec![vec![json!(1)]]
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
        assert_eq!(
            rows(&conn, "SELECT enable_backfill FROM identity_log_state"),
            vec![vec![Value::Null]]
        );
    }
}

#[tokio::test]
async fn sharing_baseline_history_replies_and_prefixes() {
    let dir = artifacts();
    for (name, state, base) in STORES {
        let file = StoreFile::copy(&dir.join(format!("{name}.db")));
        check_history(&file, name, state, *base);
        for (kind, actual) in history_outputs(&dir, name, *base).await {
            let path = dir.join(format!("{name}.{kind}"));
            let mut expected = fs::read_to_string(&path).unwrap();
            if kind == "unselected.jsonl" || (kind == "default.jsonl" && base.is_none()) {
                // Preserve the recorded replies and substitute only the former
                // cache hit for the first agent in each observer without a hub.
                let disabled = *state == "disabled";
                let agent = "agent_0000000000000001";
                let old_where = json!({"agent_id":agent,"cut_id":null,"epoch":3,
                    "holder":"11111111111111111111111111111111","holder_name":null,
                    "known":true,"read_at_ms":700,"stale":false,"status":"held_elsewhere"});
                let mut old_with_box = old_where.clone();
                old_with_box["holder_box"] = json!(BOX_A);
                let new_where = json!({"agent_id":agent,"cut_id":null,"epoch":0,"holder":null,
                    "holder_name":null,"known":disabled,"read_at_ms":null,"stale":!disabled,"status":"unclaimed"});
                let old_residence = json!({"agent_id":agent,"epoch":3,
                    "holder":"11111111111111111111111111111111","read_at_ms":700,"state":"elsewhere"});
                let new_residence = if disabled {
                    json!({"agent_id":agent,"epoch":0,"state":"unclaimed"})
                } else {
                    json!({"agent_id":agent,"state":"unknown"})
                };
                expected = expected
                    .replace(&old_where.to_string(), &new_where.to_string())
                    .replace(&old_with_box.to_string(), &new_where.to_string())
                    .replace(&old_residence.to_string(), &new_residence.to_string());
                // The historical stores still contain the frozen observations.
                // Startup no longer marks those rows stale; only their stale
                // bit differs until the migration purges the cache.
                expected = expected
                    .split_inclusive('\n')
                    .map(|line| {
                        let row: Value = serde_json::from_str(line).unwrap();
                        if matches!(row["method"].as_str(), Some("role.read" | "role.fence"))
                            && matches!(
                                row["params"]["role"].as_str(),
                                Some("connector_home" | "local_role")
                            )
                            && row["reply"]["result"]["epoch"]
                                .as_i64()
                                .is_some_and(|e| e > 0)
                        {
                            assert_eq!(row["reply"]["result"]["stale"], true);
                            line.replacen("\"stale\":true", "\"stale\":false", 1)
                        } else {
                            line.to_owned()
                        }
                    })
                    .collect();
            }
            assert_eq!(expected, actual, "{}", path.display());
        }
    }
}

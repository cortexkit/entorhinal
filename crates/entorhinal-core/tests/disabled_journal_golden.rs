use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    AddRootRequest, AssignWorkspaceRequest, RegisterRequest, RegistryStore, RemoveRequest,
    SetWorkspaceRootRequest,
};
use rusqlite::{params, Connection};
use serde_json::{json, Value};
use std::{fs, path::Path, process::Command};

const GOLDEN: &str = include_str!("fixtures/disabled-journal-e247ed6.json");

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

// This workload was run unchanged in a detached worktree at e247ed6 with:
// DISABLED_GOLDEN_OUTPUT=<fixture path> cargo test --manifest-path
// target/disabled-baseline/Cargo.toml -p entorhinal-core --test
// disabled_journal_golden --locked -- --nocapture
// Agent clocks are fixed; journal creation times are not part of the fixture.
// Only filesystem prefixes, randomly minted binding tokens and epochs are
// replaced in raw strings; JSON is never parsed
// and re-encoded for comparison, so changes in key order or whitespace fail.
#[test]
fn disabled_workload_matches_pre_identity_log_reply_and_journal_bytes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../target")
        .join(format!("disabled-golden-{}", std::process::id()));
    fs::create_dir_all(&root).unwrap();
    let root = root.canonicalize().unwrap();
    for name in ["one", "two", "workspace"] {
        fs::create_dir_all(root.join(name)).unwrap();
    }
    for name in ["one", "two"] {
        git(&root.join(name), &["init", "-q"]);
        git(
            &root.join(name),
            &[
                "remote",
                "add",
                "origin",
                &format!("https://github.com/golden/{name}.git"),
            ],
        );
    }
    let descriptor = StorageDescriptor {
        module_id: "disabled-golden".into(),
        storage_namespace: "test".into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: root.join("store.db").to_string_lossy().into(),
        },
    };
    let store = RegistryStore::open(&descriptor).unwrap();
    let writer = store.with_principal("reserved:prefrontal-core");
    let one = root.join("one").to_string_lossy().into_owned();
    let two = root.join("two").to_string_lossy().into_owned();
    let mut replies = vec![
        writer
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                roots: vec![one.clone()],
                actor: Some("operator".into()),
                request_key: Some("register".into()),
                ..Default::default()
            })
            .unwrap(),
        writer
            .add_root(AddRootRequest {
                project_id: "P".into(),
                root: two.clone(),
                actor: Some("operator".into()),
                ..Default::default()
            })
            .unwrap(),
        writer
            .set_workspace_root(SetWorkspaceRootRequest {
                workspace_id: "W".into(),
                root: Some(root.join("workspace").to_string_lossy().into()),
                actor: Some("operator".into()),
                request_key: Some("workspace-root".into()),
            })
            .unwrap(),
        writer
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "P".into(),
                workspace_id: "W2".into(),
                workspace_name: Some("Second".into()),
                actor: Some("operator".into()),
                request_key: Some("assign".into()),
            })
            .unwrap(),
        // The module binds unbound checkouts after project writes; approve_root
        // requires that same preparation when using the core API directly.
        writer.bind_root(&one, "operator").unwrap(),
        writer.bind_root(&two, "operator").unwrap(),
        writer.approve_root(&one, "operator").unwrap(),
    ];
    let conn = Connection::open(root.join("store.db")).unwrap();
    let mut replacements = vec![(root.to_string_lossy().into_owned(), "$ROOT".to_owned())];
    for (i, path) in [&one, &two].iter().enumerate() {
        let (incarnation, epoch): (String, String) = conn
            .query_row(
                "SELECT incarnation,registration_epoch FROM root_binding WHERE canonical_root=?1",
                [path],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        replacements.push((incarnation, format!("$INCARNATION{}", i + 1)));
        replacements.push((epoch, format!("$EPOCH{}", i + 1)));
    }
    replies.push(
        writer
            .remove(RemoveRequest {
                project_id: Some("P".into()),
                request_key: Some("remove".into()),
                actor: Some("operator".into()),
                ..Default::default()
            })
            .unwrap(),
    );
    let snapshot = root.join("snapshot.db");
    let source = Connection::open(&snapshot).unwrap();
    for migration in [
        include_str!("fixtures/core-agent-migrations/076_agent_registry.sql"),
        include_str!("fixtures/core-agent-migrations/080_agent_github_identity.sql"),
        include_str!("fixtures/core-agent-migrations/082_wake_delivery.sql"),
        include_str!("fixtures/core-agent-migrations/109_agent_generation.sql"),
        include_str!("fixtures/core-agent-migrations/112_agent_avatar.sql"),
        include_str!("fixtures/core-agent-migrations/126_agent_labels.sql"),
    ] {
        source.execute_batch(migration).unwrap();
    }
    for (id, name) in [
        ("agent_0000000000000001", "Ada"),
        ("agent_0000000000000002", "Grace"),
    ] {
        source.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,?2,'helper','assistant',10,20)", params![id, name]).unwrap();
        source.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES(?1,'assistant','global',?2,?3,30)", params![id, name.to_ascii_lowercase(), name]).unwrap();
    }
    drop(source);
    replies.push(
        writer
            .agent_import(
                json!({"snapshot_path":snapshot,"request_key":"import"}),
                500,
            )
            .unwrap(),
    );
    replies.push(writer.agent_mutation("agent.rename", json!({"agent_id":"agent_0000000000000001","name":"Augusta","request_key":"rename","actor":"operator"}), 600).unwrap());
    replies.push(writer.agent_mutation("agent.dispose", json!({"agent_id":"agent_0000000000000002","request_key":"dispose","actor":"operator"}), 700).unwrap());
    let normalize = |mut text: String| {
        for (from, to) in &replacements {
            text = text.replace(from, to);
        }
        text
    };
    let replies: Vec<String> = replies
        .into_iter()
        .map(|b| normalize(String::from_utf8(b).unwrap()))
        .collect();
    let journal: Vec<Value> = conn.prepare("SELECT seq,op,payload_json,actor,request_key FROM registry_journal ORDER BY seq").unwrap()
        .query_map([], |r| Ok(json!({"seq":r.get::<_,i64>(0)?,"op":r.get::<_,String>(1)?,"payload_json":normalize(r.get(2)?),"actor":r.get::<_,String>(3)?,"request_key":r.get::<_,Option<String>>(4)?}))).unwrap()
        .collect::<rusqlite::Result<_>>().unwrap();
    let actual =
        serde_json::to_string_pretty(&json!({"replies":replies,"journal":journal})).unwrap() + "\n";
    if let Ok(path) = std::env::var("DISABLED_GOLDEN_OUTPUT") {
        // Generation is confined to the historical checkout. Never bless HEAD.
        assert!(!conn.prepare("SELECT stream FROM registry_journal").is_ok());
        fs::write(path, actual).unwrap();
    } else {
        assert_eq!(
            conn.query_row("SELECT state FROM identity_log_state", [], |r| r
                .get::<_, String>(0))
                .unwrap(),
            "disabled"
        );
        assert_eq!(actual.as_bytes(), GOLDEN.as_bytes());
        let nondefault: i64 = conn.query_row("SELECT COUNT(*) FROM registry_journal WHERE stream != 'local' OR origin != 'here' OR entry_id IS NOT NULL OR log_position IS NOT NULL OR entry IS NOT NULL", [], |r| r.get(0)).unwrap();
        assert_eq!(
            nondefault, 0,
            "disabled writes must keep new journal columns at their defaults"
        );
    }
    drop(conn);
    drop(store);
    fs::remove_dir_all(root).unwrap();
}

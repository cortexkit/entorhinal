//! Exercises caller authorization, identity import, and restart/backup
//! invalidation through the serving handler on disposable closed SQLite copies.

use super::*;
use crate::fake_log::FailConnector;
use rusqlite::{Connection, OpenFlags};
use std::path::Path;

const DIRECT: RouteKey = (190, 1);
const CORE: RouteKey = (191, 1);

type RehearsalResult<T> = Result<T, Box<dyn std::error::Error>>;

#[derive(Default, serde::Serialize)]
struct Report {
    inputs: Value,
    steps: Vec<Step>,
    setup_error: Option<String>,
}

#[derive(serde::Serialize)]
struct Step {
    name: String,
    request: Value,
    reply: Value,
    assertions: Vec<Value>,
}

impl Report {
    fn check(&mut self, name: &str, expected: Value, actual: Value) {
        let pass = expected == actual;
        self.steps.last_mut().unwrap().assertions.push(json!({
            "name": name, "expected": expected, "actual": actual, "pass": pass,
        }));
    }

    fn local(&mut self, name: &str, request: Value, reply: Value) {
        self.steps.push(Step {
            name: name.into(),
            request,
            reply,
            assertions: Vec::new(),
        });
    }

    async fn call(
        &mut self,
        handler: &ProjectsHandler,
        name: &str,
        principal: RouteKey,
        method: &str,
        params: Value,
    ) -> RehearsalResult<(Value, Vec<u8>)> {
        let request = json!({"method": method, "params": params});
        let outcome = handler
            .handle_served_request(&serde_json::to_vec(&request)?, principal)
            .await;
        let (reply, bytes) = match outcome {
            HandlerOutcome::Response(bytes) => (serde_json::from_slice(&bytes)?, bytes),
            HandlerOutcome::Error { code, message } => (
                json!({"error": {"code": code, "message": message, "detail": null}}),
                vec![],
            ),
            HandlerOutcome::ErrorWithDetail {
                code,
                message,
                detail,
            } => (
                json!({"error": {"code": code, "message": message, "detail": detail}}),
                vec![],
            ),
            HandlerOutcome::Streamed => (json!({"unexpected_streamed_reply": true}), vec![]),
        };
        self.local(name, json!({
            "principal": if principal == DIRECT { "Direct" } else { "Reserved(prefrontal-core)" },
            "body": request,
        }), reply.clone());
        if !bytes.is_empty() {
            self.steps.last_mut().unwrap().reply["wire_utf8"] = json!(std::str::from_utf8(&bytes)?);
        }
        Ok((reply, bytes))
    }

    fn success(&mut self, reply: &Value) {
        self.check(
            "successful response",
            json!(true),
            json!(reply.get("result").is_some()),
        );
    }

    fn refusal(&mut self, reply: &Value, code: &str) {
        self.check("refusal code", json!(code), reply["error"]["code"].clone());
    }
}

fn open(descriptor: &StorageDescriptor) -> RehearsalResult<ProjectsHandler> {
    let handler = ProjectsHandler::with_log_connector(
        crate::incarnation::new_incarnation()
            .map_err(|error| format!("cannot mint process incarnation: {error}"))?,
        unix_millis,
        Arc::new(FailConnector::default()),
    );
    let mut store = RegistryStore::open(descriptor)?;
    store.set_root_records(true);
    install_store(&handler.store, &handler.health, Ok(store));
    for (route, principal) in [
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
            .insert(route, RouteAdmission::from_bind(Some(principal), None));
    }
    Ok(handler)
}

fn required_path(name: &str) -> RehearsalResult<PathBuf> {
    std::env::var_os(name)
        .map(PathBuf::from)
        .ok_or_else(|| format!("{name} must be set").into())
}

fn sha256(path: &Path) -> RehearsalResult<String> {
    // Use the platform digest utility rather than adding a production dependency
    // for an opt-in evidence harness.
    let output = if cfg!(target_os = "macos") {
        std::process::Command::new("shasum")
            .args(["-a", "256"])
            .arg(path)
            .output()?
    } else {
        std::process::Command::new("sha256sum").arg(path).output()?
    };
    if !output.status.success() {
        return Err(format!(
            "sha256 failed for {}: {}",
            path.display(),
            String::from_utf8_lossy(&output.stderr)
        )
        .into());
    }
    let digest = String::from_utf8(output.stdout)?
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned();
    if digest.len() != 64 || !digest.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("invalid SHA-256 utility output".into());
    }
    Ok(digest)
}

fn safe_input(path: &Path) -> RehearsalResult<PathBuf> {
    let path = path.canonicalize()?;
    let live = required_path("HOME")?.join(".local/share/cortexkit/entorhinal");
    let live = live.canonicalize().unwrap_or(live);
    if path.starts_with(live) {
        return Err(format!("refusing live entorhinal input: {}", path.display()).into());
    }
    Ok(path)
}

fn copy_input(source: &Path, destination: &Path) -> RehearsalResult<Value> {
    let source = safe_input(source)?;
    let digest = sha256(&source)?;
    std::fs::copy(&source, destination)?;
    if sha256(destination)? != digest {
        return Err(format!("input changed while copying {}", source.display()).into());
    }
    Ok(json!({"path": source, "sha256": digest, "copy": destination}))
}

fn read_only(path: &Path) -> RehearsalResult<Connection> {
    Ok(Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
    )?)
}

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn copy_store(source: &Path, label: &str) -> RehearsalResult<(Scratch, StorageDescriptor, Value)> {
    let source = safe_input(source)?;
    let (dir, descriptor) = crate::tests::scratch_descriptor(label);
    let scratch = Scratch(dir.canonicalize()?);
    let store_input = copy_input(&source, &scratch.0.join("store.db"))?;
    let mut leases = Vec::new();
    for entry in std::fs::read_dir(source.parent().ok_or("store has no parent")?)? {
        let entry = entry?;
        if entry
            .path()
            .extension()
            .is_some_and(|extension| extension == "lease")
        {
            leases.push(copy_input(
                &entry.path(),
                &scratch.0.join(entry.file_name()),
            )?);
        }
    }
    if leases.is_empty() {
        return Err(format!(
            "no writer .lease beside {}; copy it with the store",
            source.display()
        )
        .into());
    }
    Ok((
        scratch,
        descriptor,
        json!({"store": store_input, "leases": leases}),
    ))
}

fn source_rows(conn: &Connection, sql: &str) -> RehearsalResult<Value> {
    let strings = conn
        .prepare(sql)?
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Value::Array(
        strings
            .iter()
            .map(|row| serde_json::from_str(row))
            .collect::<Result<Vec<_>, _>>()?,
    ))
}

fn expected_snapshot(path: &Path) -> RehearsalResult<Value> {
    let conn = read_only(path)?;
    let agents: i64 = conn.query_row("SELECT COUNT(*) FROM agent", [], |row| row.get(0))?;
    let claims: i64 = conn.query_row("SELECT COUNT(*) FROM agent_name_claim", [], |row| {
        row.get(0)
    })?;
    // Read the frozen core columns directly; using entorhinal's importer to form
    // the expectation would hide a dropped or incorrectly decoded source field.
    // Keys are the agent wire names, so core's `avatar_type` column is `type`.
    let rows = source_rows(
        &conn,
        "SELECT json_object(
        'agent_id',agent_id,'name',name,'name_version',name_version,
        'name_normalization_version',name_normalization_version,'tag',tag,
        'labels',json(labels_json),'role',role,'project_id',project_id,'workspace_id',workspace_id,
        'avatar',CASE WHEN avatar_genome IS NULL THEN NULL ELSE json_object(
            'genome',avatar_genome,'type',avatar_type,'version',avatar_version) END,
        'github_identity',json(github_identity_json),
        'status',CASE WHEN terminal_reason IS NULL THEN 'live' WHEN terminal_reason='deleted'
            THEN 'retired' ELSE terminal_reason END,
        'merged_into',merged_into_agent_id,'supervisor_agent_id',NULL,'request_key',NULL,
        'created_at_ms',created_at_ms,'updated_at_ms',updated_at_ms,'terminal_at_ms',terminal_at_ms,
        'agent_generation',generation) FROM agent ORDER BY agent_id COLLATE BINARY",
    )?;
    let claim_rows = source_rows(
        &conn,
        "SELECT json_object(
        'claim_id',claim_id,'agent_id',agent_id,'namespace_kind',namespace_kind,
        'namespace_key',namespace_key,'normalized_name',normalized_name,
        'name_normalization_version',name_normalization_version,'display_name',display_name,
        'claimed_at_ms',claimed_at_ms,'released_at_ms',released_at_ms)
        FROM agent_name_claim ORDER BY claim_id",
    )?;
    Ok(json!({"agent_count": agents, "claim_count": claims, "agents": rows, "claims": claim_rows}))
}

fn integer(value: &Value, field: &str) -> RehearsalResult<i64> {
    value[field]
        .as_i64()
        .ok_or_else(|| format!("missing integer {field} in {value}").into())
}

async fn snapshot(
    report: &mut Report,
    handler: &ProjectsHandler,
    name: &str,
) -> RehearsalResult<Value> {
    let (reply, _) = report
        .call(handler, name, DIRECT, "agent.snapshot", json!({}))
        .await?;
    report.success(&reply);
    Ok(reply["result"].clone())
}

async fn unchanged(
    report: &mut Report,
    handler: &ProjectsHandler,
    generation: i64,
) -> RehearsalResult<()> {
    let reply = snapshot(report, handler, "head after import attempt").await?;
    report.check(
        "unchanged generation",
        json!(generation),
        reply["generation"].clone(),
    );
    Ok(())
}

async fn rehearse(report: &mut Report) -> RehearsalResult<()> {
    let store_path = required_path("ENTO_REHEARSAL_STORE")?;
    let snapshot_path = required_path("ENTO_REHEARSAL_SNAPSHOT")?;
    let older_path = required_path("ENTO_REHEARSAL_OLDER")?;
    // Check every input before opening any database. Symlink targets are checked
    // as well, so a harmless-looking copy path cannot name the deployed store.
    for path in [&store_path, &snapshot_path, &older_path] {
        let canonical = safe_input(path)?;
        let wal = PathBuf::from(format!("{}-wal", canonical.display()));
        if wal.exists() && std::fs::metadata(&wal)?.len() != 0 {
            return Err(format!(
                "{} has a nonempty WAL; supply a closed SQLite backup",
                canonical.display()
            )
            .into());
        }
    }
    let (_a_dir, a_descriptor, store_input) = copy_store(&store_path, "cut-rehearsal-a")?;
    let (_b_dir, b_descriptor, older_input) = copy_store(&older_path, "cut-rehearsal-b")?;
    let snapshot_copy = _a_dir.0.join("core-snapshot.db");
    let snapshot_input = copy_input(&snapshot_path, &snapshot_copy)?;
    report.inputs = json!({"store": store_input, "snapshot": snapshot_input, "older": older_input});
    let expected = expected_snapshot(&snapshot_copy)?;
    report.local(
        "source SQL expectations",
        json!({"source": snapshot_copy}),
        expected.clone(),
    );

    let a = open(&a_descriptor)?;
    let initial = snapshot(report, &a, "initial A snapshot").await?;
    let i1 = initial["incarnation"].clone();
    let g0 = integer(&initial, "generation")?;
    report.check(
        "empty pre-import agents",
        json!([]),
        initial["agents"].clone(),
    );
    report.check(
        "nonempty I1",
        json!(true),
        json!(i1.as_str().is_some_and(|s| !s.is_empty())),
    );

    let (direct, _) = report
        .call(
            &a,
            "direct import refused",
            DIRECT,
            "agent.import",
            json!({"snapshot_path": snapshot_copy, "request_key": "cut-rehearsal-direct"}),
        )
        .await?;
    report.refusal(&direct, "direct_identity_write_not_admitted");
    unchanged(report, &a, g0).await?;
    let params = json!({"snapshot_path": snapshot_copy, "actor": "prefrontal-core", "request_key": "cut-rehearsal-import"});
    let (imported, bytes) = report
        .call(
            &a,
            "attested core import",
            CORE,
            "agent.import",
            params.clone(),
        )
        .await?;
    report.success(&imported);
    report.check(
        "agents imported",
        expected["agent_count"].clone(),
        imported["result"]["agents_imported"].clone(),
    );
    report.check(
        "claims imported",
        expected["claim_count"].clone(),
        imported["result"]["claims_imported"].clone(),
    );
    let g1 = integer(&imported["result"], "generation")?;
    let (retried, retry_bytes) = report
        .call(&a, "same key retry", CORE, "agent.import", params.clone())
        .await?;
    report.success(&retried);
    report.check(
        "byte-identical import retry",
        json!(std::str::from_utf8(&bytes)?),
        json!(std::str::from_utf8(&retry_bytes)?),
    );
    unchanged(report, &a, g1).await?;
    let mut new_key = params;
    new_key["request_key"] = json!("cut-rehearsal-second-import");
    let (refused, _) = report
        .call(&a, "new key import refused", CORE, "agent.import", new_key)
        .await?;
    report.refusal(&refused, "import_already_done");
    unchanged(report, &a, g1).await?;

    let after = snapshot(report, &a, "post-import A snapshot").await?;
    report.check(
        "snapshot agents match source",
        expected["agents"].clone(),
        after["agents"].clone(),
    );
    report.check(
        "snapshot claims match source",
        expected["claims"].clone(),
        after["claims"].clone(),
    );
    report.check(
        "snapshot generation G1",
        json!(g1),
        after["generation"].clone(),
    );
    report.check(
        "same incarnation I1",
        i1.clone(),
        after["incarnation"].clone(),
    );
    report.check("import advances generation", json!(true), json!(g1 > g0));

    let mut cursor = 0;
    let mut entries = Vec::new();
    while cursor < g1 {
        let (reply, _) = report
            .call(
                &a,
                "A change page",
                DIRECT,
                "agent.changes",
                json!({"incarnation": i1, "cursor": cursor, "wait": false, "limit": 2}),
            )
            .await?;
        report.success(&reply);
        let page = &reply["result"];
        report.check("feed generation G1", json!(g1), page["generation"].clone());
        report.check(
            "feed incarnation I1",
            i1.clone(),
            page["incarnation"].clone(),
        );
        let next = integer(page, "cursor")?;
        report.check(
            "feed cursor progresses within head",
            json!(true),
            json!(next > cursor && next <= g1),
        );
        entries.extend(
            page["entries"]
                .as_array()
                .ok_or("missing change entries")?
                .iter()
                .cloned(),
        );
        if next <= cursor || next > g1 {
            break;
        }
        cursor = next;
    }
    report.local(
        "complete agent feed",
        json!({"from": 0, "through": g1}),
        json!(entries),
    );
    report.check("feed reaches G1", json!(g1), json!(cursor));
    report.check(
        "exactly N agent.import entries",
        expected["agent_count"].clone(),
        json!(entries
            .iter()
            .filter(|entry| entry["op"] == "agent.import")
            .count()),
    );
    report.check(
        "no agent.cutover entries",
        json!(0),
        json!(entries
            .iter()
            .filter(|entry| entry["op"] == "agent.cutover")
            .count()),
    );
    report.check(
        "no other agent operations",
        json!(true),
        json!(entries.iter().all(|entry| entry["op"] == "agent.import")),
    );

    project_probes(report, &a).await?;
    let (verified, _) = report
        .call(&a, "verify A store", DIRECT, "verify", json!({}))
        .await?;
    report.success(&verified);
    report.check("verify ok", json!(true), verified["result"]["ok"].clone());
    report.check(
        "clean replay",
        json!(true),
        verified["result"]["replay"]["ok"].clone(),
    );
    report.check(
        "no replay differences",
        json!([]),
        verified["result"]["replay"]["tables"].clone(),
    );
    drop(a);
    let a2 = open(&a_descriptor)?;
    let restarted = snapshot(report, &a2, "restarted A2 snapshot").await?;
    let i3 = restarted["incarnation"].clone();
    report.check(
        "restart mints new incarnation",
        json!(true),
        json!(i3 != i1),
    );
    report.check(
        "restart retains G1",
        json!(g1),
        restarted["generation"].clone(),
    );
    let (stale, _) = report
        .call(
            &a2,
            "A2 rejects old incarnation",
            DIRECT,
            "agent.changes",
            json!({"incarnation": i1, "cursor": g1, "wait": false}),
        )
        .await?;
    report.refusal(&stale, "snapshot_required");

    let b = open(&b_descriptor)?;
    let (stale, _) = report
        .call(
            &b,
            "B rejects old incarnation",
            DIRECT,
            "agent.changes",
            json!({"incarnation": i1, "cursor": g1, "wait": false}),
        )
        .await?;
    report.refusal(&stale, "snapshot_required");
    let restored = snapshot(report, &b, "restored B snapshot").await?;
    let i2 = restored["incarnation"].clone();
    let g2 = integer(&restored, "generation")?;
    report.check("restore differs from I1", json!(true), json!(i2 != i1));
    report.check("restore differs from I3", json!(true), json!(i2 != i3));
    report.check("restore has lower generation", json!(true), json!(g2 < g1));
    let b_conn = read_only(&_b_dir.0.join("store.db"))?;
    let b_head: i64 = b_conn.query_row(
        "SELECT COALESCE(MAX(seq),0) FROM registry_journal",
        [],
        |row| row.get(0),
    )?;
    let b_agents: i64 = b_conn.query_row("SELECT COUNT(*) FROM agent", [], |row| row.get(0))?;
    report.check(
        "B snapshot has B SQL head",
        json!(b_head),
        restored["generation"].clone(),
    );
    report.check(
        "B snapshot has B agents",
        json!(b_agents),
        json!(restored["agents"]
            .as_array()
            .ok_or("missing restored agents")?
            .len()),
    );
    Ok(())
}

async fn project_probes(report: &mut Report, handler: &ProjectsHandler) -> RehearsalResult<()> {
    // `list` is the CLI verb; its deployed wire method is `enumerate`.
    let (listed, _) = report
        .call(handler, "project list", DIRECT, "enumerate", json!({}))
        .await?;
    report.success(&listed);
    // Choose a remote from the registered checkouts, not the shared key table:
    // that table is populated only when identity-log replication is enabled,
    // while the identity import must also work before replication.
    let remote = listed["result"]["projects"]
        .as_array()
        .ok_or("missing listed projects")?
        .iter()
        .flat_map(|project| project["rootRecords"].as_array().into_iter().flatten())
        .flat_map(|root| root["remotes"].as_array().into_iter().flatten())
        .find(|remote| remote["owned"] == true)
        .ok_or("store needs a registered root with an owned remote for resolve_remote")?;
    let owner = remote["owner"]
        .as_str()
        .ok_or("owned remote has no owner")?;
    let repo = remote["repo"]
        .as_str()
        .ok_or("owned remote has no repository")?;
    let (resolved, _) = report
        .call(
            handler,
            "resolve owned project remote",
            DIRECT,
            "resolve_remote",
            json!({"owner": owner, "repo": repo}),
        )
        .await?;
    report.success(&resolved);
    report.check(
        "owned remote resolves to registered identity",
        json!(true),
        json!(matches!(
            resolved["result"]["status"].as_str(),
            Some("found" | "ambiguous")
        )),
    );
    let mut after = 0;
    loop {
        let (journal, _) = report
            .call(
                handler,
                "project journal page",
                DIRECT,
                "journal_tail",
                json!({"afterSeq": after, "limit": 1000}),
            )
            .await?;
        report.success(&journal);
        let entries = journal["result"]["entries"]
            .as_array()
            .ok_or("missing project journal entries")?;
        report.check(
            "project journal excludes agent entries",
            json!(true),
            json!(entries.iter().all(|entry| entry["op"]
                .as_str()
                .is_some_and(|op| !op.starts_with("agent.")))),
        );
        let head = integer(&journal["result"], "generation")?;
        let next = if entries.len() == 1000 {
            integer(entries.last().unwrap(), "seq")?
        } else {
            head
        };
        report.check(
            "project journal cursor progresses",
            json!(true),
            json!(next > after || next == head),
        );
        if next >= head || next <= after {
            break;
        }
        after = next;
    }
    Ok(())
}

fn check_inputs_unchanged(report: &mut Report) -> RehearsalResult<()> {
    let mut files = Vec::new();
    for store in ["store", "older"] {
        files.push(report.inputs[store]["store"].clone());
        files.extend(
            report.inputs[store]["leases"]
                .as_array()
                .ok_or("missing input leases")?
                .iter()
                .cloned(),
        );
    }
    files.push(report.inputs["snapshot"].clone());
    report.local(
        "input copies leave sources untouched",
        json!({"operation": "sha256 input files"}),
        Value::Null,
    );
    for file in files {
        let path = Path::new(file["path"].as_str().ok_or("missing input path")?);
        report.check(
            &format!("unchanged {}", path.display()),
            file["sha256"].clone(),
            json!(sha256(path)?),
        );
    }
    Ok(())
}

fn write_report(out: &Path, report: &Report) -> RehearsalResult<()> {
    let parent = out
        .parent()
        .filter(|path| !path.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let parent = safe_input(parent)?;
    let nonce =
        crate::incarnation::new_incarnation().map_err(|error| format!("report nonce: {error}"))?;
    let temporary = parent.join(format!(".cut-rehearsal-{nonce}.json"));
    // Replace the report rather than truncating it: an existing hard link at
    // OUT must not modify an input file that shares the same underlying bytes.
    let result = (|| -> RehearsalResult<()> {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        std::io::Write::write_all(&mut file, &serde_json::to_vec_pretty(report)?)?;
        file.sync_all()?;
        std::fs::rename(&temporary, out)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temporary);
    }
    result
}

#[tokio::test]
#[ignore = "evidence harness; needs ENTO_REHEARSAL_* paths"]
async fn cut_barrier_rehearsal() {
    let out = required_path("ENTO_REHEARSAL_OUT").unwrap_or_else(|error| panic!("{error}"));
    // Never let the report replace an input, even if OUT is an alias for it.
    if out.exists() {
        let canonical_out = safe_input(&out).unwrap_or_else(|error| panic!("{error}"));
        for name in [
            "ENTO_REHEARSAL_STORE",
            "ENTO_REHEARSAL_SNAPSHOT",
            "ENTO_REHEARSAL_OLDER",
        ] {
            let input = required_path(name).unwrap_or_else(|error| panic!("{error}"));
            let canonical_input = safe_input(&input).unwrap_or_else(|error| panic!("{error}"));
            assert_ne!(
                canonical_out, canonical_input,
                "ENTO_REHEARSAL_OUT must not replace {name}"
            );
            if canonical_out
                .extension()
                .is_some_and(|extension| extension == "lease")
            {
                panic!("ENTO_REHEARSAL_OUT must not replace a writer lease");
            }
        }
    }
    let mut report = Report::default();
    if let Err(error) = rehearse(&mut report).await {
        report.setup_error = Some(error.to_string());
    }
    if !report.inputs.is_null() {
        if let Err(error) = check_inputs_unchanged(&mut report) {
            report.setup_error = Some(error.to_string());
        }
    }
    let failed: Vec<String> = report
        .steps
        .iter()
        .flat_map(|step| {
            step.assertions
                .iter()
                .filter(|assertion| assertion["pass"] != true)
                .map(|assertion| format!("{}: {}", step.name, assertion["name"]))
        })
        .collect();
    write_report(&out, &report).unwrap_or_else(|error| {
        panic!("cannot write ENTO_REHEARSAL_OUT {}: {error}", out.display())
    });
    assert!(
        report.setup_error.is_none() && failed.is_empty(),
        "rehearsal failed; report={}; error={:?}; failed assertions={failed:?}",
        out.display(),
        report.setup_error
    );
    println!(
        "cut barrier evidence: {}; {} steps, {} assertions",
        out.display(),
        report.steps.len(),
        report
            .steps
            .iter()
            .map(|step| step.assertions.len())
            .sum::<usize>()
    );
}

use super::*;
use sha2::{Digest, Sha256};

const DIRECTORY: &str = "agent-data-requests-v1";
const HARNESS: &str = "opencode";
const RUNNER: &str = "thalamus-harness";
const SESSION: &str = "session-example-001";
const PROJECT_ROOT: &str = "/workspace/agent-data-example";
const INCARNATION: &str = "0123456789abcdef";
const KIND: &str = "opencode-session";

// The deprecated generator is needed to publish requests for stored legacy refs.
#[allow(deprecated)]
fn session_refs() -> [(&'static str, String); 2] {
    use cortexkit_role_llm_runner::{session_ref, session_ref_v1, SessionIdentity};
    [
        (
            "old-format.json",
            session_ref_v1(HARNESS, PROJECT_ROOT, SESSION),
        ),
        (
            "new-format.json",
            session_ref(&SessionIdentity {
                runner: RUNNER,
                harness: HARNESS,
                session: SESSION,
            }),
        ),
    ]
}

async fn record(machine: &Machine, method: &str, params: Value) -> (String, Value) {
    let request = serde_json::to_string(&json!({"method":method,"params":params})).unwrap();
    let reply = machine.bytes_on(CORE, method, params).await.unwrap();
    let decoded = serde_json::from_slice(&reply).unwrap();
    (
        format!(
            "  {{\"request\":{request},\"principal\":{},\"reply\":{}}}",
            json!({"kind":"reserved","moduleId":WRITER_MODULE}),
            String::from_utf8(reply).unwrap(),
        ),
        decoded,
    )
}

async fn registered_session(machine: &Machine, reference: &str) -> Vec<String> {
    let (registration, reply) = record(
        machine,
        "agent.register_session",
        json!({
            "agent_id": AGENT,
            "ref_kind": KIND,
            "ref": reference,
            "harness": HARNESS,
            "session_id": SESSION,
            "project_root": PROJECT_ROOT,
            "runner": RUNNER,
        }),
    )
    .await;
    assert_eq!(reply["result"]["ref"], reference);
    let (lookup, reply) = record(
        machine,
        "agent.session_for_ref",
        json!({"ref_kind": KIND, "ref": reference}),
    )
    .await;
    assert_eq!(reply["result"]["agent_id"], AGENT);
    assert_eq!(reply["result"]["ref"], reference);
    assert_eq!(reply["result"]["runner"], RUNNER);
    vec![registration, lookup]
}

async fn capture(reference: &str, abort: bool) -> Vec<String> {
    let mut pair = Pair::new(&[]).await;
    let mut daemons = Vec::new();
    let mut wires = Vec::new();
    for (source, node) in [(true, &mut pair.a), (false, &mut pair.b)] {
        node.machine.handler.incarnation = INCARNATION.into();
        let resolver = FakeResolver::new();
        for capability in [ORCHESTRATOR_CAPABILITY, crate::sessions::HOST_CAPABILITY] {
            resolver.set(
                capability,
                ProviderResolution::Provider(WRITER_MODULE.into()),
            );
        }
        node.machine.handler.provider_resolver = resolver;
        let agent_ref = AgentRef::new(KIND, reference);
        let participant = Arc::new(FakeParticipant::new(
            &[KIND],
            if source { vec![agent_ref] } else { vec![] },
        ));
        let stream = CutItem {
            session_ref: "00000000000000000000000000000010".into(),
            stream: format!("reserved:{RUNNER}"),
            end_offset: 42,
            digest: "a".repeat(64),
        };
        node.door.set_stream(stream.clone());
        participant.set_prepare(
            serde_json::from_value(json!({"items":[{
            "ref_kind":KIND,"ref":reference,"sessionRef":stream.session_ref,
            "stream":stream.stream,"endOffset":42,"digest":stream.digest,
            "format":"v1","frozen":true
        }],"external":[]}))
            .unwrap(),
        );
        let daemon =
            FakeDaemon::new(snapshot(&[(RUNNER, KIND)], true, true)).with_participant(participant);
        let connection_file = node.machine.dir.join("participant-subc.json");
        wires.push(crate::fake_daemon::WireDaemon::start(daemon.clone(), &connection_file).await);
        let connector = Arc::new(crate::log_client::SubcConnector::from_args([
            std::ffi::OsString::from("--subc"),
            connection_file.into_os_string(),
        ]));
        node.machine.handler.participant_discovery = Arc::new(
            crate::participant_discovery::SubcParticipantDiscovery::new(connector),
        );
        daemons.push(daemon);
    }
    let mut records = registered_session(&pair.a.machine, reference).await;
    // A completed session keeps its binding, but admission requires a fresh
    // liveness answer that no session process is still running.
    {
        let mut liveness = pair.a.machine.handler.liveness.lock().unwrap();
        liveness.stale = false;
        liveness
            .gone
            .insert(crate::SessionKey::new(HARNESS, SESSION));
    }
    let cut = pair.a.admit_to(B, "example-move").await.unwrap();
    if abort {
        pair.a.source_until(&cut, MoveStep::Exported).await;
        pair.a.abort(&cut);
        pair.a.source_until(&cut, MoveStep::Aborted).await;
        records.clear();
    } else {
        pair.a
            .door
            .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
        pair.a.source_until(&cut, MoveStep::Published).await;
        pair.finish(&cut).await;
    }
    // Admission mints a random cut ID. Replace only that string in the captured
    // bytes; never parse and re-encode a request to produce the published body.
    let normalized_cut = if abort {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    } else {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    for (side, daemon) in [if abort { "source-abort" } else { "source" }, "receiver"]
        .into_iter()
        .zip(&daemons)
    {
        let calls = daemon.request_bytes();
        let expected: &[&str] = match side {
            "source" => &["describe", "busy", "prepare", "export", "release"],
            "receiver" if !abort => &["describe", "diverged", "import", "activate"],
            "source-abort" => &["describe", "busy", "prepare", "export", "abort"],
            _ => &[],
        };
        assert_eq!(calls.len(), expected.len());
        for (bytes, op) in calls.into_iter().zip(expected) {
            let request: Value = serde_json::from_slice(&bytes).unwrap();
            assert_eq!(request["method"], format!("agent_data.{op}"));
            if *op != "describe" {
                assert_eq!(
                    request["params"]["refs"],
                    json!([{"ref_kind":KIND,"ref":reference}])
                );
            }
            if !matches!(*op, "describe" | "busy") {
                assert_eq!(request["params"]["cutId"], cut);
            }
            let request = String::from_utf8(bytes)
                .unwrap()
                .replace(&cut, normalized_cut);
            records.push(format!(
                "  {{\"side\":{},\"request\":{request}}}",
                json!(side)
            ));
        }
    }
    records
}

#[test]
fn served_vectors_agent_data_requests_v1() {
    // A separate process isolates the SDK's cached supervised launch identity
    // from concurrent tests and from the developer's daemon environment.
    let mut child = std::process::Command::new(std::env::current_exe().unwrap());
    let probe = format!(
        "{}::served_vectors_agent_data_requests_probe",
        module_path!().split_once("::").unwrap().1,
    );
    child
        .env_clear()
        .env("SUBC_MODULE_ID", "entorhinal")
        .env("SUBC_LAUNCH_NONCE", "fake-launch-secret")
        .args(["--exact", &probe, "--ignored", "--nocapture"]);
    if let Some(value) = std::env::var_os("ENTO_WRITE_SERVED_VECTORS") {
        child.env("ENTO_WRITE_SERVED_VECTORS", value);
    }
    #[cfg(windows)]
    for name in ["SYSTEMROOT", "PATH", "TEMP", "TMP"] {
        if let Some(value) = std::env::var_os(name) {
            child.env(name, value);
        }
    }
    let output = child.output().unwrap();
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
}

#[tokio::test(start_paused = true)]
#[ignore = "run with isolated launch identity by served_vectors_agent_data_requests_v1"]
async fn served_vectors_agent_data_requests_probe() {
    let mut sums = String::new();
    for (name, reference) in session_refs() {
        let mut records = capture(&reference, false).await;
        records.extend(capture(&reference, true).await);
        let bytes = format!("[\n{}\n]\n", records.join(",\n"));
        publish(name, bytes.as_bytes());
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes.as_bytes())));
    }
    publish("SHA256SUMS", sums.as_bytes());
}

fn publish(name: &str, bytes: &[u8]) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors")
        .join(DIRECTORY)
        .join(name);
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        println!("wrote {DIRECTORY}/{name}");
    } else {
        assert_eq!(
            std::fs::read(&path).unwrap(),
            bytes,
            "{DIRECTORY}/{name} differs from the real requests"
        );
    }
}

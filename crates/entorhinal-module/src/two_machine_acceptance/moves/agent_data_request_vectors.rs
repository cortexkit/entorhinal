use super::super::*;
use crate::{
    fake_daemon::{DaemonSnapshot, FakeDaemon, WireDaemon},
    fake_participant::FakeParticipant,
    participant_client::{AgentRef, CutRequest, ParticipantClient, Refs, ReleaseRequest},
    participant_discovery::{
        provider_role, Participant, ParticipantDiscovery, SubcParticipantDiscovery,
    },
    sessions::{FakeResolver, ProviderResolution, HOST_CAPABILITY, ORCHESTRATOR_CAPABILITY},
};
use cortexkit_role_agent_data::refs::CutId;
use sha2::{Digest, Sha256};

const DIRECTORY: &str = "agent-data-requests-v1";
const HARNESS: &str = "opencode";
const RUNNER: &str = "thalamus-harness";
const SESSION: &str = "session-example-001";
const PROJECT_ROOT: &str = "/workspace/agent-data-example";
const INCARNATION: &str = "0123456789abcdef";
const KIND: &str = "opencode-session";
const AGENT: &str = "agent_0123456789abcdef";
const TO: &str = "02020202020202020202020202020202";
const RECEIVER_BOX: &str = "abcdef0123456789abcdef0123456789";

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

async fn participant(
    reference: &str,
    source: bool,
    connection_file: &Path,
) -> (ParticipantClient, FakeDaemon, WireDaemon) {
    let agent_ref = AgentRef::new(KIND, reference);
    let participant = Arc::new(FakeParticipant::new(
        &[KIND],
        if source { vec![agent_ref] } else { vec![] },
    ));
    let stream = crate::door_client::CutItem {
        session_ref: "00000000000000000000000000000010".into(),
        stream: format!("reserved:{RUNNER}"),
        end_offset: 42,
        digest: "a".repeat(64),
    };
    participant.set_prepare(
        serde_json::from_value(json!({"items":[{
            "ref_kind":KIND,"ref":reference,"sessionRef":stream.session_ref,
            "stream":stream.stream,"endOffset":42,"digest":stream.digest,
            "format":"v1","frozen":true
        }],"external":[]}))
        .unwrap(),
    );
    let daemon = FakeDaemon::new(DaemonSnapshot::new(
        1,
        vec![json!({"module_id":RUNNER,"state":"running","enabled":true,"live":true,"health":"ok"})],
        vec![json!({"module_id":RUNNER,"ready":true,"not_ready":null,"control_ops":[],"roles":[provider_role()],
            "capabilities":{"provides":["agent-data/v1"],"requires":[],"must_never_reach":[]}})],
    ))
    .with_participant(participant);
    let wire = WireDaemon::start(daemon.clone(), connection_file).await;
    let connector = Arc::new(crate::log_client::SubcConnector::from_args([
        std::ffi::OsString::from("--subc"),
        connection_file.as_os_str().to_owned(),
    ]));
    let discovery = SubcParticipantDiscovery::new(connector);
    let transport = drive(discovery.open_participant(RUNNER)).await.unwrap();
    let client = ParticipantClient::new(&Participant {
        module_id: RUNNER.into(),
        transport,
    });
    (client, daemon, wire)
}

async fn capture(reference: &str, abort: bool, receiver_box: Option<&str>) -> Vec<String> {
    let mut machine = Machine::new("agent-data-request-vector");
    machine.handler.clock = || 1000;
    machine.handler.incarnation = INCARNATION.into();
    let resolver = FakeResolver::new();
    for capability in [ORCHESTRATOR_CAPABILITY, HOST_CAPABILITY] {
        resolver.set(
            capability,
            ProviderResolution::Provider(WRITER_MODULE.into()),
        );
    }
    machine.handler.provider_resolver = resolver;
    machine
        .store()
        .apply_entry("agent.cutover", "{}", "fixture", None, |_| Ok(()))
        .unwrap();
    machine
        .store()
        .apply_entry("fixture.agent", "{}", "fixture", None, |tx| {
            tx.execute(
                "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Agent','R','assistant',1,1)",
                [AGENT],
            )?;
            Ok(())
        })
        .unwrap();
    let mut records = registered_session(&machine, reference).await;
    let (source, source_daemon, _source_wire) = participant(
        reference,
        true,
        &machine.dir.join("source-participant.json"),
    )
    .await;
    let (receiver, receiver_daemon, _receiver_wire) = participant(
        reference,
        false,
        &machine.dir.join("receiver-participant.json"),
    )
    .await;
    let cut = crate::hub_write::new_op_id().unwrap();
    let request = CutRequest::new(
        Refs::new(vec![AgentRef::new(KIND, reference)]).unwrap(),
        CutId::new(cut.clone()).unwrap(),
    );

    drive(source.describe()).await.unwrap();
    drive(source.busy(&[AgentRef::new(KIND, reference)]))
        .await
        .unwrap();
    drive(source.prepare(&request)).await.unwrap();
    drive(source.export(&request)).await.unwrap();
    if abort {
        drive(source.abort(&request)).await.unwrap();
        records.clear();
    } else {
        let release = ReleaseRequest::new(request.refs.clone(), request.cut_id.clone(), TO);
        let release = if let Some(box_id) = receiver_box {
            release.with_holder_box(box_id).unwrap()
        } else {
            release
        };
        drive(source.release(&release)).await.unwrap();
        drive(receiver.describe()).await.unwrap();
        drive(receiver.diverged(&request)).await.unwrap();
        drive(receiver.import(&request)).await.unwrap();
        drive(receiver.activate(&request)).await.unwrap();
    }

    let normalized_cut = if abort {
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
    } else {
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    };
    for (side, daemon) in [if abort { "source-abort" } else { "source" }, "receiver"]
        .into_iter()
        .zip([source_daemon, receiver_daemon])
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
            if *op == "release" {
                assert_eq!(request["params"]["holder"], TO);
                assert_eq!(
                    request["params"].get("holderBox"),
                    receiver_box.map(|id| json!(id)).as_ref()
                );
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
        let mut records = capture(&reference, false, None).await;
        let aborted = capture(&reference, true, None).await;
        records.extend(aborted.iter().cloned());
        let bytes = format!("[\n{}\n]\n", records.join(",\n"));
        publish(name, bytes.as_bytes());
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes.as_bytes())));
        if name == "new-format.json" {
            let mut records = capture(&reference, false, Some(RECEIVER_BOX)).await;
            records.extend(aborted);
            let bytes = format!("[\n{}\n]\n", records.join(",\n"));
            publish("with-holder-box.json", bytes.as_bytes());
        }
    }
    publish("SHA256SUMS", sums.as_bytes());
}

fn publish(name: &str, bytes: &[u8]) {
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors")
        .join(DIRECTORY)
        .join(name);
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        if std::fs::read(&path).is_ok_and(|existing| existing == bytes) {
            return;
        }
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

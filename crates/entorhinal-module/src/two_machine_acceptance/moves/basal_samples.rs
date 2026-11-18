//! Replay Basal's tagged bodies through the participant client, then check how
//! the move driver handles Basal's import losses and absence on the receiver.

use std::path::Path;

use cortexkit_role_agent_data::errors::{
    allowed_refusals, driver_action_for, DriverAction, ErrorBody, RefusalCode,
};
use cortexkit_role_agent_data::ops::{ACTIVATE, DESCRIBE, EXPORT, IMPORT, PREPARE, RELEASE};
use cortexkit_role_agent_data::prepare::PrepareReply;
use entorhinal_core::move_record::{MoveReason, MoveRef};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::*;
use crate::log_client::service_call_error;
use crate::move_driver::participant_action;
use crate::move_receiver::skipped_participants;
use crate::participant_client::{ClientError, ParticipantClient};
use crate::participant_discovery::Participant;

const MODULE: &str = "basal";
const RECEIVER_CLEAN: &str = "receiver-clean.json";
const RECEIVER_EMPTY: &str = "receiver-empty.json";
const RECEIVER_FLOWS: &str = "receiver-flows.json";
const RECEIVER_REFUSALS: &str = "receiver-refusals.json";
const SOURCE_EMPTY: &str = "source-empty.json";
const SOURCE_FLOWS: &str = "source-flows.json";
const SOURCE_REFUSALS: &str = "source-refusals.json";
const FILES: [&str; 8] = [
    "README.md",
    RECEIVER_CLEAN,
    RECEIVER_EMPTY,
    RECEIVER_FLOWS,
    RECEIVER_REFUSALS,
    SOURCE_EMPTY,
    SOURCE_FLOWS,
    SOURCE_REFUSALS,
];
// Pin the manifest from tag basal-agent-data-vectors-v1 as well as each file it lists.
const SUMS_SHA256: &str = "6d7084b347830584f2aaf07555f9f26f9359ff279054b0953dba74c87f2f8dd5";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    name: String,
    request: SampleRequest,
    reply: Option<Value>,
    error: Option<Value>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRequest {
    method: String,
    params: Value,
}

type Samples = BTreeMap<&'static str, Vec<Sample>>;

fn load_samples() -> Samples {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/basal-agent-data-v1");
    let read =
        |name: &str| std::fs::read(root.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    let sums = read("SHA256SUMS");
    assert_eq!(
        format!("{:x}", Sha256::digest(&sums)),
        SUMS_SHA256,
        "SHA256SUMS differs from the tagged list"
    );
    let sums = std::str::from_utf8(&sums).expect("SHA256SUMS is UTF-8");
    assert_eq!(sums.lines().count(), FILES.len(), "SHA256SUMS file count");

    let mut samples = BTreeMap::new();
    for (name, line) in FILES.into_iter().zip(sums.lines()) {
        let (hash, listed) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("SHA256SUMS: invalid line {line:?}"));
        assert_eq!(listed, name, "SHA256SUMS: unexpected file");
        let bytes = read(name);
        assert_eq!(
            format!("{:x}", Sha256::digest(&bytes)),
            hash,
            "{name} differs from the tagged bytes"
        );
        if name.ends_with(".json") {
            samples.insert(
                name,
                serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: {e}")),
            );
        }
    }
    samples
}

struct SampleTransport {
    at: String,
    expected: SampleRequest,
    answer: Mutex<Option<Result<Vec<u8>, TransportError>>>,
}

#[async_trait]
impl LogTransport for SampleTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        assert_eq!(method, self.expected.method, "{}.request.method", self.at);
        assert_eq!(params, self.expected.params, "{}.request.params", self.at);
        self.answer
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| panic!("{}: the client sent the request twice", self.at))
    }
}

async fn send(client: &ParticipantClient, request: &Request) -> Result<Value, ClientError> {
    fn encode(reply: impl Serialize) -> Value {
        serde_json::to_value(reply).expect("a decoded reply serializes")
    }
    Ok(match request {
        Request::Describe(_) => encode(client.describe().await?),
        Request::Busy(busy) => encode(client.busy(busy.refs.as_slice()).await?),
        Request::Status(cut) => encode(client.status(cut).await?),
        Request::Prepare(cut) => encode(client.prepare(cut).await?),
        Request::Export(cut) => encode(client.export(cut).await?),
        Request::Diverged(cut) => encode(client.diverged(cut).await?),
        Request::Import(cut) => encode(client.import(cut).await?),
        Request::Activate(cut) => encode(client.activate(cut).await?),
        Request::Abort(cut) => encode(client.abort(cut).await?),
        Request::Release(release) => encode(client.release(release).await?),
    })
}

fn expected_refusal(name: &str) -> Option<(&'static str, &'static str, DriverAction)> {
    Some(match name {
        "prepare-approval-open" | "prepare-run-active" => {
            (PREPARE, "busy", DriverAction::RetryLater)
        }
        "release-before-export" => (RELEASE, "invalid_state", DriverAction::Abort),
        "describe-wrong-caller" => (DESCRIBE, "caller_forbidden", DriverAction::Abort),
        "activate-run-active" => (ACTIVATE, "busy", DriverAction::RetryLater),
        // In the receiver's import step, a terminal local_advanced refusal
        // aborts the move as diverged, before the claim changes hands.
        "import-flow-id-clash" => (IMPORT, "local_advanced", DriverAction::TerminalForStep),
        _ => return None,
    })
}

fn refusal(body: &subc_protocol::ErrorBody) -> Refusal {
    Refusal {
        code: body.code.clone(),
        detail: body.detail.clone(),
        message: Some(body.message.clone()),
    }
}

async fn replay(file: &str, sample: &Sample) {
    let at = format!("{file}[{}]", sample.name);
    let method = &sample.request.method;
    let request = Request::decode(method, &sample.request.params)
        .unwrap_or_else(|e| panic!("{at}.request ({method}) does not decode: {e:?}"));
    let expected = expected_refusal(&sample.name);
    assert_eq!(
        expected.is_some(),
        matches!(file, SOURCE_REFUSALS | RECEIVER_REFUSALS),
        "{at}: unexpected exchange name"
    );
    let (answer, expected_error) = match (&sample.reply, &sample.error, expected) {
        (Some(reply), None, None) => (Ok(serde_json::to_vec(reply).unwrap()), None),
        (None, Some(error), Some((op, code, action))) => {
            assert_eq!(method, op, "{at}.request.method");
            let body: subc_protocol::ErrorBody =
                serde_json::from_value(error.clone()).unwrap_or_else(|e| panic!("{at}.error: {e}"));
            assert_eq!(body.code, code, "{at}.error.code");
            assert_eq!(serde_json::to_value(&body).unwrap(), *error, "{at}.error");
            // The driver preserves refusal details as given. Also check the
            // contract's typed detail for codes with a prescribed shape.
            let contract: ErrorBody =
                serde_json::from_value(error.clone()).unwrap_or_else(|e| panic!("{at}.error: {e}"));
            contract
                .typed_detail()
                .unwrap_or_else(|e| panic!("{at}.error.detail: {e}"));
            let expected = refusal(&body);
            (
                Err(service_call_error(subc_client_rs::CallError::Module(body))),
                Some((expected, action)),
            )
        }
        _ => panic!("{at}: expected exactly one of reply or error for this exchange"),
    };
    let transport = Arc::new(SampleTransport {
        at: at.clone(),
        expected: sample.request.clone(),
        answer: Mutex::new(Some(answer)),
    });
    let client = ParticipantClient::new(&Participant {
        module_id: MODULE.into(),
        transport: transport.clone(),
    });
    let decoded = send(&client, &request).await;
    assert!(
        transport.answer.lock().unwrap().is_none(),
        "{at}: the client did not send the request: {decoded:?}"
    );
    match expected_error {
        None => assert_eq!(
            decoded.unwrap_or_else(|e| panic!("{at}.reply: {e:?}")),
            sample.reply.as_ref().unwrap()["result"],
            "{at}.reply.result"
        ),
        Some((expected, action)) => {
            let Err(ClientError::Refused(received)) = decoded else {
                panic!(
                    "{at}.error.code: expected an allowed refusal {expected:?}, got {decoded:?}"
                );
            };
            assert_eq!(received, expected, "{at}.error refusal");
            let op = request.op();
            let code = RefusalCode::from_code(&received.code);
            assert!(allowed_refusals(op).contains(&code), "{at}.error.code");
            assert_eq!(
                driver_action_for(op, &code),
                action,
                "{at}.error.code: contract action"
            );
            assert_eq!(
                participant_action(op.method(), &received.code),
                Some(action),
                "{at}.error.code: driver action"
            );
        }
    }
}

fn sample<'a>(samples: &'a Samples, file: &str, name: &str) -> &'a Sample {
    samples[file]
        .iter()
        .find(|sample| sample.name == name)
        .unwrap_or_else(|| panic!("{file}: missing {name}"))
}

fn result<'a>(samples: &'a Samples, file: &str, name: &str) -> &'a Value {
    &sample(samples, file, name)
        .reply
        .as_ref()
        .unwrap_or_else(|| panic!("{file}[{name}].reply: expected success"))["result"]
}

async fn published_basal(samples: &Samples, flows: bool, absent: bool) -> (Pair, String) {
    // A session in another participant lets the door seal a cut even when
    // Basal has no streams for the agent.
    let pair = super::agent_refs::pair_with_refs(
        &[("broca", &["session"]), (MODULE, &["agent"])],
        &[AgentRef::new("session", "ref-session")],
    )
    .await;
    let source = &pair.a.discovery.peers[MODULE];
    let describe = result(samples, SOURCE_FLOWS, "describe");
    assert!(
        describe.get("optional").is_none(),
        "{SOURCE_FLOWS}[describe].reply.result.optional must be absent"
    );
    source.script(DESCRIBE, Effect::Reply(describe.clone()));
    let (file, prepare, export) = if flows {
        (SOURCE_FLOWS, "prepare-with-flows", "export-with-flows")
    } else {
        (SOURCE_EMPTY, "prepare-empty", "export-empty")
    };
    let prepared: PrepareReply = serde_json::from_value(result(samples, file, prepare).clone())
        .unwrap_or_else(|e| panic!("{file}[{prepare}].reply.result: {e}"));
    assert_eq!(
        prepared.items.len(),
        usize::from(flows),
        "{file}[{prepare}].reply.result.items"
    );
    for item in &prepared.items {
        assert!(
            item.frozen,
            "{file}[{prepare}].reply.result.items[0].frozen"
        );
        pair.a.door.set_stream(CutItem {
            session_ref: item.session_ref.clone(),
            stream: item.stream.clone(),
            end_offset: item.end_offset,
            digest: item.digest.clone(),
        });
    }
    source.set_prepare(prepared);
    source.script(EXPORT, Effect::Reply(result(samples, file, export).clone()));
    source.script(
        RELEASE,
        Effect::Reply(result(samples, SOURCE_FLOWS, "release-enforced").clone()),
    );
    if absent {
        pair.b
            .discovery
            .daemon
            .restart(snapshot(&[("broca", "session")], true, true));
    }

    let cut = pair.published("basal-sample").await;
    let record = pair.a.record(&cut).await;
    let plan = record
        .participants
        .iter()
        .find(|p| p.module_id == MODULE)
        .expect("source-flows.json[describe] must plan Basal");
    assert!(
        !plan.optional,
        "{SOURCE_FLOWS}[describe] must plan required Basal"
    );
    assert_eq!(
        plan.refs,
        [MoveRef {
            ref_kind: "agent".into(),
            reference: AGENT.into(),
        }],
        "{SOURCE_FLOWS}[describe].reply.result.ref_kinds must plan the agent ref"
    );
    assert!(source
        .calls()
        .iter()
        .any(|call| matches!(call, Request::Export(_))));
    let sealed = pair.a.door.cut(&cut).unwrap();
    assert_eq!(
        sealed
            .items
            .iter()
            .filter(|item| item.stream == "reserved:basal")
            .count(),
        usize::from(flows),
        "{file}[{export}].reply.result.items: Basal streams in the sealed cut"
    );
    (pair, cut)
}

async fn move_with_absent_basal(samples: &Samples, flows: bool) {
    let (pair, cut) = published_basal(samples, flows, true).await;
    if flows {
        pair.b.pickup().await;
        pair.b.receiver_until(&cut, MoveStep::Aborted).await;
        pair.a.source_until(&cut, MoveStep::Aborted).await;
        let record = pair.a.record(&cut).await;
        assert_eq!(record.state, MoveState::Aborted);
        assert_eq!(record.reason, Some(MoveReason::ReceiverPrecondition));
        assert_eq!(
            record.detail,
            Some(json!({"code":"participant_unavailable","module_id":MODULE,"absent":true}))
        );
        assert!(skipped_participants(&pair.b.row(&cut)).is_empty());
    } else {
        pair.finish(&cut).await;
        assert_eq!(skipped_participants(&pair.b.row(&cut)), [MODULE]);
    }
    assert!(pair.b.discovery.peers[MODULE].calls().is_empty());
    pair.clean();
}

async fn move_with_basal_import_drops(samples: &Samples) {
    let (pair, cut) = published_basal(samples, true, false).await;
    let imported = result(samples, RECEIVER_FLOWS, "import-with-drops");
    pair.b.discovery.peers[MODULE].set_import(serde_json::from_value(imported.clone()).unwrap());
    pair.finish(&cut).await;

    let status = pair
        .b
        .machine
        .call("agent.move_status", json!({"cut_id":cut}))
        .await
        .unwrap();
    assert_eq!(
        status["losses"],
        json!([{
            "participant": MODULE,
            "session_ref": "d5acbf637b6119cbd2a2b3f32cc6f140",
            "reason": "queued_event",
            "count": 1
        }]),
        "{RECEIVER_FLOWS}[import-with-drops].reply.result.items[0].dropped must reach move status"
    );
    pair.clean();
}

async fn move_with_basal_import_clash(samples: &Samples) {
    let (pair, cut) = published_basal(samples, true, false).await;
    let clash = sample(samples, RECEIVER_REFUSALS, "import-flow-id-clash");
    let body = serde_json::from_value(clash.error.clone().unwrap()).unwrap();
    pair.b.discovery.peers[MODULE].script(IMPORT, Effect::Refuse(refusal(&body)));
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    pair.a.source_until(&cut, MoveStep::Aborted).await;
    let record = pair.a.record(&cut).await;
    assert_eq!(record.state, MoveState::Aborted);
    assert_eq!(record.reason, Some(MoveReason::Diverged));
    let detail = record.detail.unwrap();
    assert_eq!(detail["module_id"], MODULE);
    assert_eq!(detail["op"], IMPORT);
    assert_eq!(
        detail["code"], "local_advanced",
        "{RECEIVER_REFUSALS}[import-flow-id-clash].error.code must abort receiver import"
    );
    pair.clean();
}

#[tokio::test(start_paused = true)]
async fn basal_agent_data_samples_match_entorhinal_move_driver() {
    let samples = load_samples();
    for (&file, exchanges) in &samples {
        for exchange in exchanges {
            replay(file, exchange).await;
        }
    }
    move_with_basal_import_drops(&samples).await;
    move_with_basal_import_clash(&samples).await;
    move_with_absent_basal(&samples, true).await;
    move_with_absent_basal(&samples, false).await;
}

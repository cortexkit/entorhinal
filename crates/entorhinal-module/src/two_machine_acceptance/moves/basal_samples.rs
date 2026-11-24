//! Replay Basal's tagged agent-data exchanges through the participant client.

use std::{
    collections::BTreeMap,
    path::Path,
    sync::{Arc, Mutex},
};

use cortexkit_role_agent_data::errors::{
    allowed_refusals, driver_action_for, DriverAction, ErrorBody, RefusalCode,
};
use cortexkit_role_agent_data::ops::{ACTIVATE, DESCRIBE, IMPORT, PREPARE, RELEASE};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::log_client::{service_call_error, LogTransport, TransportError};
use crate::move_driver::participant_action;
use crate::participant_client::{ClientError, ParticipantClient, Refusal, Request};

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

#[tokio::test(start_paused = true)]
async fn basal_agent_data_samples_match_entorhinal_move_driver() {
    let samples = load_samples();
    for (&file, exchanges) in &samples {
        for exchange in exchanges {
            replay(file, exchange).await;
        }
    }
}

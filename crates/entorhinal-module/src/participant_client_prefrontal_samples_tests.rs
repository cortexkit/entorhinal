//! Replays the published Prefrontal `agent-data/v1` samples through the move
//! driver's participant client.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cortexkit_role_agent_data::errors::{
    allowed_refusals, driver_action_for, DriverAction, ErrorBody, RefusalCode,
};
use cortexkit_role_agent_data::ops::{DESCRIBE, PREPARE};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};

use crate::log_client::{service_call_error, LogTransport, TransportError};
use crate::move_driver::participant_action;
use crate::participant_client::{
    describe_participants, plan_participants, AgentRef, ClientError, Op, ParticipantClient,
    Refusal, Request,
};
use crate::participant_discovery::Participant;

const MODULE: &str = "prefrontal";

const SUMS_SHA256: &str = "295e44df026b96029162bd4cf3cc002b0adc14ae8bb041509e0e734ab5aa3f19";
// The published checksum list does not cover its README.
const README_SHA256: &str = "2a15601b79293bde77c4717eb314d660397e8ad98d90d781fc7c20576ddcb4dc";

fn samples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/prefrontal-agent-data-v1")
}

fn read_bytes(name: &str) -> Vec<u8> {
    std::fs::read(samples_dir().join(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn check_digest(name: &str, bytes: &[u8], expected: &str) {
    assert_eq!(
        format!("{:x}", Sha256::digest(bytes)),
        expected,
        "{name} differs from the published bytes"
    );
}

#[test]
fn prefrontal_agent_data_samples_match_their_published_checksums() {
    let sums = read_bytes("SHA256SUMS");
    check_digest("SHA256SUMS", &sums, SUMS_SHA256);
    check_digest("README.md", &read_bytes("README.md"), README_SHA256);
    for line in std::str::from_utf8(&sums).unwrap().lines() {
        let (sum, name) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("not a SHA256SUMS line: {line:?}"));
        check_digest(name, &read_bytes(name), sum);
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    name: String,
    node: String,
    request: SampleRequest,
    reply: SampleReply,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRequest {
    method: String,
    params: Value,
}

#[derive(Deserialize)]
#[serde(rename_all = "lowercase")]
enum SampleReply {
    Result(Value),
    Error(Value),
}

fn read_samples() -> Vec<Sample> {
    serde_json::from_slice(&read_bytes("replies.json")).expect("published reply samples are JSON")
}

// The route checks the request the client actually sends before returning
// the corresponding captured reply.
struct SampleTransport {
    at: String,
    expected: SampleRequest,
    answer: Mutex<Option<Result<Vec<u8>, TransportError>>>,
}

#[async_trait]
impl LogTransport for SampleTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        assert_eq!(method, self.expected.method, "{}: method", self.at);
        assert_eq!(params, self.expected.params, "{}: params", self.at);
        self.answer
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| panic!("{}: the client called twice", self.at))
    }
}

fn sample_participant(
    sample: &Sample,
    answer: Result<Vec<u8>, TransportError>,
) -> (Participant, Arc<SampleTransport>) {
    let transport = Arc::new(SampleTransport {
        at: format!("{}/{}", sample.node, sample.name),
        expected: sample.request.clone(),
        answer: Mutex::new(Some(answer)),
    });
    let participant = Participant {
        module_id: MODULE.into(),
        transport: transport.clone(),
    };
    (participant, transport)
}

fn reply_value<T: Serialize>(reply: T) -> Value {
    serde_json::to_value(reply).expect("a decoded reply serializes")
}

// Use the operation-specific methods, not serde alone: the client also
// checks busy and diverged against the requested refs and classifies refusals.
async fn replay(
    sample: &Sample,
    answer: Result<Vec<u8>, TransportError>,
) -> Result<Value, ClientError> {
    let request = Request::decode(&sample.request.method, &sample.request.params)
        .unwrap_or_else(|error| panic!("{}: request does not decode: {error:?}", sample.name));
    let (participant, transport) = sample_participant(sample, answer);
    let client = ParticipantClient::new(&participant);
    let result = match request {
        Request::Describe(_) => client.describe().await.map(reply_value),
        Request::Busy(busy) => client.busy(busy.refs.as_slice()).await.map(reply_value),
        Request::Status(cut) => client.status(&cut).await.map(reply_value),
        Request::Prepare(cut) => client.prepare(&cut).await.map(reply_value),
        Request::Export(cut) => client.export(&cut).await.map(reply_value),
        Request::Diverged(cut) => client.diverged(&cut).await.map(reply_value),
        Request::Import(cut) => client.import(&cut).await.map(reply_value),
        Request::Activate(cut) => client.activate(&cut).await.map(reply_value),
        Request::Abort(cut) => client.abort(&cut).await.map(reply_value),
        Request::Release(release) => client.release(&release).await.map(reply_value),
    };
    assert!(
        transport.answer.lock().unwrap().is_none(),
        "{}: the client did not call the route",
        transport.at
    );
    result
}

fn result_envelope(result: &Value) -> Vec<u8> {
    serde_json::to_vec(&json!({ "result": result })).unwrap()
}

#[tokio::test]
async fn prefrontal_agent_data_samples_replies_decode_through_the_client_unchanged() {
    let samples = read_samples();
    let mut methods = BTreeSet::new();
    let mut failures = Vec::new();
    for sample in &samples {
        let SampleReply::Result(published) = &sample.reply else {
            continue;
        };
        let method = sample.request.method.as_str();
        methods.insert(method);
        match replay(sample, Ok(result_envelope(published))).await {
            Ok(decoded) if decoded == *published => {}
            Ok(decoded) => failures.push(format!(
                "{method} ({}): decode changes {published} to {decoded}",
                sample.name
            )),
            Err(error) => failures.push(format!(
                "{method} ({}): the client rejects the reply: {error:?}",
                sample.name
            )),
        }
    }
    assert_eq!(
        methods,
        Op::ALL.into_iter().map(|op| op.method()).collect(),
        "successful samples must cover every participant operation"
    );
    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

#[tokio::test]
async fn prefrontal_agent_data_samples_prepare_busy_is_retry_later() {
    let samples = read_samples();
    let mut errors = samples.iter().filter_map(|sample| match &sample.reply {
        SampleReply::Error(error) => Some((sample, error)),
        SampleReply::Result(_) => None,
    });
    let (sample, published) = errors.next().expect("published prepare refusal");
    assert!(errors.next().is_none(), "expected only one refusal sample");
    assert_eq!(sample.name, "prepare-busy");
    assert_eq!(sample.request.method, PREPARE);

    let contract: ErrorBody = serde_json::from_value(published.clone()).unwrap();
    assert!(contract
        .typed_detail()
        .expect("busy detail decodes")
        .is_some());
    let body: subc_protocol::ErrorBody = serde_json::from_value(published.clone()).unwrap();
    assert_eq!(reply_value(&body), *published, "SDK module error body");
    let expected = Refusal {
        code: "busy".into(),
        detail: body.detail.clone(),
        message: Some(body.message.clone()),
    };
    let answer = service_call_error(subc_client_rs::CallError::Module(body));
    let decoded = replay(sample, Err(answer)).await;
    let Err(ClientError::Refused(received)) = decoded else {
        panic!("prepare-busy: expected an allowed refusal, got {decoded:?}");
    };
    assert_eq!(received, expected, "prepare-busy: client refusal");
    let op = Op::Prepare;
    let code = RefusalCode::from_code(&received.code);
    assert!(allowed_refusals(op).contains(&code));
    assert_eq!(driver_action_for(op, &code), DriverAction::RetryLater);
    assert_eq!(
        participant_action(op.method(), &received.code),
        Some(DriverAction::RetryLater)
    );
}

#[tokio::test]
async fn prefrontal_agent_data_samples_describe_plans_required_head() {
    let samples = read_samples();
    let sample = samples
        .iter()
        .find(|sample| sample.request.method == DESCRIBE)
        .expect("published describe sample");
    let SampleReply::Result(published) = &sample.reply else {
        panic!("describe sample must succeed");
    };
    let head = AgentRef::new("agent", "head");
    // Advertising Prefrontal is not enough to synthesize a head ref. Routing
    // delivers only refs actually supplied to the planner.
    for refs in [&[][..], std::slice::from_ref(&head)] {
        let (participant, _) = sample_participant(sample, Ok(result_envelope(published)));
        let described = describe_participants(&[participant]).await.unwrap();
        let plan = plan_participants(described, refs).unwrap();
        assert_eq!(plan.len(), 1);
        let entry = &plan[0];
        assert_eq!(entry.client.module_id, MODULE);
        assert!(!entry.description.optional, "Prefrontal is required");
        assert_eq!(entry.description.ref_kinds, ["agent"]);
        assert_eq!(entry.refs, refs);
    }
}

#[tokio::test]
async fn prefrontal_agent_data_samples_admission_adds_required_agent_ref() {
    let samples = read_samples();
    let sample = samples
        .iter()
        .find(|sample| sample.request.method == DESCRIBE)
        .expect("published describe sample");
    let SampleReply::Result(published) = &sample.reply else {
        panic!("describe sample must succeed");
    };
    let agent_id = "head".to_owned();
    let (participant, _) = sample_participant(sample, Ok(result_envelope(published)));
    let described = describe_participants(&[participant]).await.unwrap();
    let mut wire_refs = Vec::new();
    let carries_agent = described
        .iter()
        .any(|p| p.description.ref_kinds.iter().any(|kind| kind == "agent"));
    if carries_agent {
        wire_refs.push(AgentRef::new("agent", agent_id.clone()));
    }
    let plan = plan_participants(described, &wire_refs).unwrap();
    assert_eq!(plan.len(), 1);
    assert_eq!(plan[0].client.module_id, MODULE);
    assert!(!plan[0].description.optional, "Prefrontal is required");
    assert_eq!(plan[0].refs, [AgentRef::new("agent", agent_id)]);
}

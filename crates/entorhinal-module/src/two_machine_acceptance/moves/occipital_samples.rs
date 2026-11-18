//! Replay Occipital's tagged bodies through the client and exercise admission
//! and the receiver's absent-participant decision with the pinned replies.

use std::path::Path;

use cortexkit_role_agent_data::errors::{
    allowed_refusals, driver_action_for, DriverAction, RefusalCode,
};
use cortexkit_role_agent_data::ops::{BUSY, DESCRIBE, EXPORT, RELEASE};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use subc_protocol::Principal;

use super::*;
use crate::log_client::service_call_error;
use crate::move_driver::participant_action;
use crate::move_receiver::skipped_participants;
use crate::participant_client::{ClientError, ParticipantClient};
use crate::participant_discovery::Participant;

const MODULE: &str = "occipital";
const SOURCE: &str = "source.json";
const RELEASE_FILE: &str = "release.json";
const EMPTY: &str = "empty-agent.json";
const REFUSALS: &str = "refusals.json";
const FILES: [&str; 5] = [SOURCE, "receiver.json", RELEASE_FILE, EMPTY, REFUSALS];
// Pin the list itself so editing both a sample and its checksum still fails.
const SUMS_SHA256: &str = "283d6831e56ab3b4242ab73fb103fc76bff9c18db29b9d24831d01109e9b3269";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Sample {
    op: String,
    principal: Principal,
    request: SampleRequest,
    reply: Value,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct SampleRequest {
    method: String,
    params: Value,
}

fn load_samples() -> BTreeMap<&'static str, Vec<Sample>> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/occipital-agent-data-v1");
    let read =
        |name: &str| std::fs::read(root.join(name)).unwrap_or_else(|e| panic!("{name}: {e}"));
    let sums = read("SHA256SUMS");
    assert_eq!(
        format!("{:x}", Sha256::digest(&sums)),
        SUMS_SHA256,
        "SHA256SUMS differs from the tagged list"
    );
    let sums = std::str::from_utf8(&sums).expect("SHA256SUMS is UTF-8");
    assert_eq!(
        sums.lines().count(),
        FILES.len(),
        "SHA256SUMS: unexpected file count"
    );
    FILES
        .into_iter()
        .zip(sums.lines())
        .map(|(name, line)| {
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
            (
                name,
                serde_json::from_slice(&bytes).unwrap_or_else(|e| panic!("{name}: {e}")),
            )
        })
        .collect()
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

async fn replay(at: &str, sample: &Sample, refusal: Option<(&str, &str)>) {
    let expected_principal = if refusal == Some((DESCRIBE, "caller_forbidden")) {
        "broca"
    } else {
        "entorhinal"
    };
    assert_eq!(
        sample.principal,
        Principal::Reserved {
            module_id: expected_principal.into()
        },
        "{at}.principal"
    );
    let request = Request::decode(&sample.op, &sample.request.params)
        .unwrap_or_else(|e| panic!("{at}.request ({}) does not decode: {e:?}", sample.op));

    let (answer, expected_error) = match refusal {
        Some((method, code)) => {
            assert_eq!(sample.op, method, "{at}.op");
            let body: subc_protocol::ErrorBody = serde_json::from_value(sample.reply.clone())
                .unwrap_or_else(|e| panic!("{at}.reply: {e}"));
            assert_eq!(body.code, code, "{at}.reply.code");
            // The SDK reads explicit JSON null for detail as None.
            assert_eq!(
                sample.reply,
                json!({"code":body.code,"message":body.message,"detail":null}),
                "{at}.reply error frame"
            );
            let expected_error = Refusal {
                code: code.into(),
                detail: None,
                message: Some(body.message.clone()),
            };
            (
                Err(service_call_error(subc_client_rs::CallError::Module(body))),
                Some(expected_error),
            )
        }
        None => (Ok(serde_json::to_vec(&sample.reply).unwrap()), None),
    };
    let transport = Arc::new(SampleTransport {
        at: at.into(),
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
            sample.reply["result"],
            "{at}.reply.result"
        ),
        Some(expected) => {
            let Err(ClientError::Refused(received)) = decoded else {
                panic!(
                    "{at}.reply.code: expected an allowed refusal {expected:?}, got {decoded:?}"
                );
            };
            assert_eq!(received, expected, "{at}.reply refusal");
            let op = request.op();
            let code = RefusalCode::from_code(&received.code);
            assert!(
                allowed_refusals(op).contains(&code),
                "{at}.reply.code: {code} is not allowed for {}",
                op.method()
            );
            // All four samples reject this attempt, rather than asking for a retry.
            assert_eq!(
                driver_action_for(op, &code),
                DriverAction::Abort,
                "{at}.reply.code: contract action"
            );
            assert_eq!(
                participant_action(op.method(), &received.code),
                Some(DriverAction::Abort),
                "{at}.reply.code: driver action"
            );
        }
    }
}

async fn move_with_empty_occipital(samples: &BTreeMap<&str, Vec<Sample>>, absent: bool) {
    let describe = &samples[SOURCE][0].reply["result"];
    let prepare = &samples[EMPTY][0].reply["result"];
    let import = &samples[EMPTY][1].reply["result"];
    let release = &samples[RELEASE_FILE][0].reply["result"];

    // The door requires at least one stream to seal a cut. A session in
    // another participant supplies it without giving Occipital any rows.
    let pair = super::agent_refs::pair_with_refs(
        &[("broca", &["session"]), (MODULE, &["agent"])],
        &[AgentRef::new("session", "ref-session")],
    )
    .await;
    let source = &pair.a.discovery.peers[MODULE];
    let receiver = &pair.b.discovery.peers[MODULE];
    source.script(DESCRIBE, Effect::Reply(describe.clone()));
    source.set_prepare(serde_json::from_value(prepare.clone()).unwrap());
    source.script(RELEASE, Effect::Reply(release.clone()));
    receiver.set_import(serde_json::from_value(import.clone()).unwrap());
    if absent {
        pair.b
            .discovery
            .daemon
            .restart(snapshot(&[("broca", "session")], true, true));
    }

    let cut = pair.published("occipital-empty-agent").await;
    let record = pair.a.record(&cut).await;
    let plan = record
        .participants
        .iter()
        .find(|p| p.module_id == MODULE)
        .expect("source.json[0]: describe must plan Occipital");
    assert!(
        plan.optional,
        "source.json[0].reply.result.optional must reach the move record"
    );
    assert_eq!(plan.refs.len(), 1, "source.json[0].reply.result.ref_kinds");
    assert_eq!(
        plan.refs[0].ref_kind, "agent",
        "source.json[0].reply.result.ref_kinds"
    );
    assert_eq!(
        plan.refs[0].reference, AGENT,
        "admission must add the moved agent's ref"
    );
    let sealed = pair.a.door.cut(&cut).unwrap();
    assert!(
        sealed
            .items
            .iter()
            .all(|item| item.stream != "reserved:occipital"),
        "empty-agent.json[0].reply.result.items must not seal an Occipital stream"
    );

    pair.finish(&cut).await;
    let skipped = skipped_participants(&pair.b.row(&cut));
    if absent {
        assert!(
            receiver.calls().is_empty(),
            "an absent, itemless Occipital must not be called"
        );
        assert_eq!(skipped, [MODULE]);
    } else {
        assert!(
            receiver
                .calls()
                .iter()
                .any(|r| matches!(r, Request::Import(_))),
            "empty-agent.json[1].reply.result.items must allow import"
        );
        assert!(skipped.is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn occipital_agent_data_samples_match_entorhinal_move_driver() {
    let samples = load_samples();
    let refusals = [
        (DESCRIBE, "caller_forbidden"),
        (BUSY, "invalid_request"),
        (EXPORT, "cut_unknown"),
        (RELEASE, "id_reused"),
    ];
    assert_eq!(
        samples[REFUSALS].len(),
        refusals.len(),
        "{REFUSALS}: refusal count"
    );
    for name in FILES {
        for (index, sample) in samples[name].iter().enumerate() {
            let refusal = (name == REFUSALS).then(|| refusals[index]);
            replay(&format!("{name}[{index}]"), sample, refusal).await;
        }
    }
    assert_eq!(
        samples[SOURCE][0].reply["result"]["optional"], true,
        "source.json[0].reply.result.optional"
    );
    assert_eq!(
        samples[SOURCE][0].reply["result"]["ref_kinds"],
        json!(["agent"]),
        "source.json[0].reply.result.ref_kinds"
    );
    assert_eq!(
        samples[RELEASE_FILE][0].reply["result"]["enforced"], true,
        "release.json[0].reply.result.enforced"
    );
    for (index, sample) in samples[EMPTY].iter().enumerate() {
        assert_eq!(
            sample.reply["result"]["items"],
            json!([]),
            "empty-agent.json[{index}].reply.result.items"
        );
    }
    move_with_empty_occipital(&samples, false).await;
    move_with_empty_occipital(&samples, true).await;
}

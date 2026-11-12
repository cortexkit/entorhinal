//! Magic Context (ck-mc) publishes the `agent-data/v1` bodies its participant
//! accepts and produces; a byte-exact copy lives in
//! `tests/fixtures/mc-agent-data-samples-v1/`, with its tag and commit in
//! `SOURCE.txt` there. These tests send every sample through the same client
//! path a move uses: the request entorhinal would send must be the sampled
//! request, every reply must pass the client's strict decode unchanged, and
//! every refusal must arrive through the SDK error mapping as a refusal the
//! contract allows for that op, with the driver action listed below. A sample
//! entorhinal would reject or read differently fails with its file name.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cortexkit_role_agent_data::errors::{DriverAction, ErrorBody};
use cortexkit_role_agent_data::ops::{
    ABORT, ACTIVATE, BUSY, DESCRIBE, DIVERGED, EXPORT, IMPORT, PREPARE, RELEASE, STATUS,
};
use serde::Serialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_client_rs::{CatalogList, SupervisorListResponse};

use super::*;
use crate::fake_daemon::{DaemonSnapshot, FakeDaemon};
use crate::log_client::service_call_error;
use crate::move_driver::participant_action;
use crate::participant_discovery::{discover_participants, provider_role, ParticipantDiscovery};

/// The SHA-256 of the published `SHA256SUMS`. Pinning the list itself means a
/// changed sample fails even when its line in the list was edited to match.
const SUMS_SHA256: &str = "79ea37226e971b8ffcd86c7fc5b9bba00515d332ea8721d184fe4020167e4226";

/// Files in the folder that are not samples: the README and the list are
/// covered by `SHA256SUMS` (the README is, the list is pinned above), and
/// `SOURCE.txt` is entorhinal's own note of where the copy came from.
const NOT_SAMPLES: &[&str] = &["README.md", "SHA256SUMS", "SOURCE.txt"];

/// Every successful reply sample and the op its README section names. Each
/// has a `<stem>-request.json` and a `<stem>-reply.json`.
const REPLIES: &[(&str, &str)] = &[
    ("wire-describe-no-runner", DESCRIBE),
    ("service-describe-runner", DESCRIBE),
    ("wire-busy-idle", BUSY),
    ("service-busy-historian", BUSY),
    ("service-busy-view", BUSY),
    ("service-busy-unknown", BUSY),
    ("wire-prepare", PREPARE),
    ("wire-prepare-to-abort", PREPARE),
    ("source-prepare", PREPARE),
    ("wire-export", EXPORT),
    ("wire-diverged", DIVERGED),
    ("wire-import", IMPORT),
    ("wire-import-repeat", IMPORT),
    ("wire-release", RELEASE),
    ("wire-activate", ACTIVATE),
    ("wire-abort", ABORT),
    ("service-abort", ABORT),
    ("wire-status-none", STATUS),
    ("wire-status-prepared", STATUS),
    ("wire-status-exported", STATUS),
    ("wire-status-imported", STATUS),
    ("wire-status-released", STATUS),
    ("wire-status-activated", STATUS),
    ("wire-status-aborted", STATUS),
    ("service-status-none", STATUS),
    ("service-status-prepared", STATUS),
    ("service-status-exported", STATUS),
    ("service-status-imported", STATUS),
    ("service-status-released", STATUS),
    ("service-status-activated", STATUS),
    ("service-status-aborted", STATUS),
];

/// One refusal sample as the README describes it, and what entorhinal's
/// driver must do with it.
struct RefusalSample {
    stem: &'static str,
    method: &'static str,
    code: &'static str,
    /// The action the contract's table gives this op and code.
    action: DriverAction,
    /// `None` when the README says the `detail` member is absent; otherwise
    /// the members the README says it carries.
    detail: Option<&'static [&'static str]>,
}

const fn refusal(
    stem: &'static str,
    method: &'static str,
    code: &'static str,
    action: DriverAction,
    detail: Option<&'static [&'static str]>,
) -> RefusalSample {
    RefusalSample {
        stem,
        method,
        code,
        action,
        detail,
    }
}

/// Every refusal sample: `<stem>-refusal.json`, with a `<stem>-request.json`
/// except for `service-invalid-request`, whose input is deliberately not a
/// contract request and is not published.
const REFUSALS: &[RefusalSample] = &[
    refusal(
        "receiver-busy",
        IMPORT,
        "busy",
        DriverAction::RetryLater,
        Some(&["refs"]),
    ),
    refusal(
        "receiver-cut-not-ready",
        IMPORT,
        "cut_not_ready",
        DriverAction::RetryWithinStep,
        None,
    ),
    refusal(
        "receiver-digest-mismatch",
        IMPORT,
        "digest_mismatch",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "receiver-incompatible-version",
        IMPORT,
        "incompatible_version",
        DriverAction::Abort,
        Some(&["found", "required"]),
    ),
    refusal(
        "receiver-local-advanced",
        IMPORT,
        "local_advanced",
        DriverAction::TerminalForStep,
        None,
    ),
    refusal(
        "receiver-not-exportable",
        PREPARE,
        "not_exportable",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "receiver-participant-failed",
        IMPORT,
        "participant_failed",
        DriverAction::Abort,
        Some(&["reason"]),
    ),
    refusal(
        "service-caller-forbidden",
        DESCRIBE,
        "caller_forbidden",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "service-cut-unknown",
        EXPORT,
        "cut_unknown",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "service-id-reused",
        ABORT,
        "id_reused",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "service-invalid-request",
        DESCRIBE,
        "invalid_request",
        DriverAction::Abort,
        None,
    ),
    refusal(
        "service-moves-off-defensive",
        DESCRIBE,
        "unavailable",
        DriverAction::Retry,
        None,
    ),
    refusal(
        "service-unknown-ref",
        BUSY,
        "unknown_ref",
        DriverAction::Abort,
        Some(&["ref", "ref_kind"]),
    ),
    refusal(
        "source-export-record_too_large",
        EXPORT,
        "participant_failed",
        DriverAction::Abort,
        Some(&["reason"]),
    ),
    refusal(
        "source-export-response_too_large",
        EXPORT,
        "participant_failed",
        DriverAction::Abort,
        Some(&["reason"]),
    ),
    refusal(
        "wire-invalid-state",
        ABORT,
        "invalid_state",
        DriverAction::Abort,
        Some(&["step"]),
    ),
];

fn samples_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/mc-agent-data-samples-v1")
}

fn read_bytes(name: &str) -> Vec<u8> {
    std::fs::read(samples_dir().join(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

fn read_json(name: &str) -> Value {
    serde_json::from_slice(&read_bytes(name)).unwrap_or_else(|error| panic!("{name}: {error}"))
}

/// Decodes a sampled request with the contract's own request types, which is
/// the decoder ck-mc runs, and checks that entorhinal encodes the decoded
/// request back to the same params. The transport below then requires the
/// client to send exactly those params, so the body entorhinal sends for this
/// situation is one ck-mc's decoder accepts.
fn sample_request(name: &str) -> (Request, Value) {
    let body = read_json(name);
    let members: BTreeSet<_> = body
        .as_object()
        .unwrap_or_else(|| panic!("{name}: a request body is an object"))
        .keys()
        .map(String::as_str)
        .collect();
    assert_eq!(
        members,
        BTreeSet::from(["method", "params"]),
        "{name}: a request body is exactly method and params"
    );
    let method = body["method"]
        .as_str()
        .unwrap_or_else(|| panic!("{name}: method is a string"));
    let params = body["params"].clone();
    let request = Request::decode(method, &params)
        .unwrap_or_else(|error| panic!("{name}: the contract decoder refuses it: {error:?}"));
    assert_eq!(
        request.params(),
        params,
        "{name}: entorhinal encodes this request differently"
    );
    (request, params)
}

/// Stands in for the opened agent-data route: it checks the client sends the
/// sampled op and params, then answers once with the sample.
struct SampleTransport {
    name: String,
    method: &'static str,
    params: Value,
    answer: Mutex<Option<Result<Vec<u8>, TransportError>>>,
}

#[async_trait]
impl LogTransport for SampleTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        assert_eq!(
            method, self.method,
            "{}: the client sent another op",
            self.name
        );
        assert_eq!(
            params, self.params,
            "{}: the client's params differ from the sampled request",
            self.name
        );
        self.answer
            .lock()
            .unwrap()
            .take()
            .unwrap_or_else(|| panic!("{}: the client sent the request twice", self.name))
    }
}

/// Sends `request` through the client method a move uses for its op, including
/// the extra checks `busy` and `diverged` make, and returns the decoded reply
/// serialized back to JSON so a caller can see whether decoding dropped or
/// changed a member.
async fn send(
    name: &str,
    request: &Request,
    params: Value,
    answer: Result<Vec<u8>, TransportError>,
) -> Result<Value, ClientError> {
    let transport = Arc::new(SampleTransport {
        name: name.to_owned(),
        method: request.op().method(),
        params,
        answer: Mutex::new(Some(answer)),
    });
    let client = ParticipantClient::new(&Participant {
        module_id: "magic-context".into(),
        transport,
    });
    fn value<T: Serialize>(reply: T) -> Value {
        serde_json::to_value(reply).expect("a decoded reply serializes")
    }
    Ok(match request {
        Request::Describe(_) => value(client.describe().await?),
        Request::Busy(busy) => value(client.busy(busy.refs.as_slice()).await?),
        Request::Status(cut) => value(client.status(cut).await?),
        Request::Prepare(cut) => value(client.prepare(cut).await?),
        Request::Export(cut) => value(client.export(cut).await?),
        Request::Diverged(cut) => value(client.diverged(cut).await?),
        Request::Import(cut) => value(client.import(cut).await?),
        Request::Activate(cut) => value(client.activate(cut).await?),
        Request::Abort(cut) => value(client.abort(cut).await?),
        Request::Release(release) => value(client.release(release).await?),
    })
}

#[tokio::test]
async fn mc_agent_data_samples_replies_decode_through_the_client_unchanged() {
    let mut failures = Vec::new();
    for (stem, method) in REPLIES {
        let request_name = format!("{stem}-request.json");
        let reply_name = format!("{stem}-reply.json");
        let (request, params) = sample_request(&request_name);
        assert_eq!(
            request.op().method(),
            *method,
            "{request_name}: the README names another op"
        );
        // A sample is the reply's `result` value; the route carries it inside
        // the `{"result": ...}` envelope the client decodes strictly.
        let mut wire = b"{\"result\":".to_vec();
        wire.extend_from_slice(&read_bytes(&reply_name));
        wire.push(b'}');
        let sample = read_json(&reply_name);
        match send(&reply_name, &request, params, Ok(wire)).await {
            Ok(decoded) if decoded == sample => {}
            Ok(decoded) => failures.push(format!(
                "{reply_name}: decoding drops or changes members, giving {decoded}"
            )),
            Err(error) => failures.push(format!(
                "{reply_name}: entorhinal rejects this {method} reply: {error:?}"
            )),
        }
        if let Some((_, step)) = stem.split_once("-status-") {
            assert_eq!(sample["step"], step, "{reply_name}: the step its name says");
        }
    }
    assert!(
        failures.is_empty(),
        "replies entorhinal does not accept as published:\n{}",
        failures.join("\n")
    );
}

#[tokio::test]
async fn mc_agent_data_samples_refusals_are_allowed_with_the_expected_driver_action() {
    let mut failures = Vec::new();
    for sample in REFUSALS {
        let name = format!("{}-refusal.json", sample.stem);
        let request_name = format!("{}-request.json", sample.stem);
        let (request, params) = if sample.stem == "service-invalid-request" {
            assert!(
                !samples_dir().join(&request_name).exists(),
                "{request_name}: the README says this input is not published"
            );
            (Request::Describe(DescribeRequest::new()), json!({}))
        } else {
            sample_request(&request_name)
        };
        assert_eq!(
            request.op().method(),
            sample.method,
            "{request_name}: the README names another op"
        );

        // A refusal reaches entorhinal as the SDK's module error body; keeping
        // the re-encoded body equal to the sample shows no member was dropped.
        let published = read_json(&name);
        let body: subc_protocol::ErrorBody = match serde_json::from_value(published.clone()) {
            Ok(body) => body,
            Err(error) => {
                failures.push(format!("{name}: not an error body: {error}"));
                continue;
            }
        };
        if serde_json::to_value(&body).unwrap() != published {
            failures.push(format!(
                "{name}: members other than code, message and detail"
            ));
        }
        if body.code != sample.code {
            failures.push(format!(
                "{name}: code {} where the README says {}",
                body.code, sample.code
            ));
        }
        fn members(detail: &Value) -> Option<BTreeSet<&str>> {
            detail
                .as_object()
                .map(|object| object.keys().map(String::as_str).collect())
        }
        let detail_matches = match (sample.detail, &body.detail) {
            (None, None) => true,
            (Some(expected), Some(detail)) => {
                members(detail) == Some(expected.iter().copied().collect())
            }
            _ => false,
        };
        if !detail_matches {
            failures.push(format!(
                "{name}: the README gives detail members {:?}, the body has {:?}",
                sample.detail, body.detail
            ));
        }

        // The contract's typed details decode strictly; a sample that breaks
        // one is a contract defect even where the driver keeps detail opaque.
        let contract: ErrorBody = serde_json::from_value(published.clone())
            .unwrap_or_else(|error| panic!("{name}: the contract refuses the body: {error}"));
        for (kind, result) in [
            ("typed", contract.typed_detail().map(|_| ())),
            (
                "digest_mismatch",
                contract.digest_mismatch_detail().map(|_| ()),
            ),
            (
                "participant_failed",
                contract.participant_failed_detail().map(|_| ()),
            ),
        ] {
            if let Err(error) = result {
                failures.push(format!("{name}: {kind} detail does not decode: {error}"));
            }
        }

        let expected = Refusal {
            code: sample.code.into(),
            detail: body.detail.clone(),
            message: Some(body.message.clone()),
        };
        // The driver picks its action from the op it sent and the code it
        // received, so take both from the call, not from the table.
        let received = body.code.clone();
        let answer = Err(service_call_error(subc_client_rs::CallError::Module(body)));
        match send(&name, &request, params, answer).await {
            Err(ClientError::Refused(refusal)) if refusal == expected => {}
            Err(ClientError::ContractViolation { op, code, .. }) => failures.push(format!(
                "{name}: entorhinal treats {code} as a contract violation of {op}"
            )),
            other => failures.push(format!(
                "{name}: expected the refusal {expected:?}, the client returned {other:?}"
            )),
        }
        let method = request.op().method();
        let action = participant_action(method, &received);
        if action != Some(sample.action) {
            failures.push(format!(
                "{name}: the driver takes {action:?} for {received} on {method}, expected {:?}",
                sample.action
            ));
        }
    }
    assert!(
        failures.is_empty(),
        "refusals entorhinal classifies differently from the README:\n{}",
        failures.join("\n")
    );
}

#[test]
fn mc_agent_data_samples_match_their_published_checksums() {
    let sums = read_bytes("SHA256SUMS");
    assert_eq!(
        format!("{:x}", Sha256::digest(&sums)),
        SUMS_SHA256,
        "SHA256SUMS differs from the published list"
    );
    let sums = String::from_utf8(sums).expect("SHA256SUMS is UTF-8");
    let mut listed = BTreeSet::new();
    for line in sums.lines() {
        let (sum, name) = line
            .split_once("  ")
            .unwrap_or_else(|| panic!("not a SHA256SUMS line: {line:?}"));
        assert_eq!(
            format!("{:x}", Sha256::digest(read_bytes(name))),
            sum,
            "{name} differs from the published bytes"
        );
        listed.insert(name.to_owned());
    }

    let present: BTreeSet<String> = std::fs::read_dir(samples_dir())
        .unwrap()
        .map(|entry| entry.unwrap().file_name().into_string().unwrap())
        .collect();
    let mut expected_present = listed.clone();
    expected_present.extend(["SHA256SUMS".to_owned(), "SOURCE.txt".to_owned()]);
    assert_eq!(
        present, expected_present,
        "the folder holds exactly the published files and SOURCE.txt"
    );

    // Every published sample is exercised by one of the tables above.
    let mut tabled: BTreeSet<String> = NOT_SAMPLES
        .iter()
        .filter(|name| listed.contains(**name))
        .map(|name| (*name).to_owned())
        .collect();
    for (stem, _) in REPLIES {
        tabled.insert(format!("{stem}-request.json"));
        tabled.insert(format!("{stem}-reply.json"));
    }
    for sample in REFUSALS {
        tabled.insert(format!("{}-refusal.json", sample.stem));
        if sample.stem != "service-invalid-request" {
            tabled.insert(format!("{}-request.json", sample.stem));
        }
    }
    assert_eq!(tabled, listed, "every published sample is tested");
}

#[test]
fn mc_agent_data_samples_identical_import_replays_the_first_reply_bytes() {
    // The README: an identical accepted request answers with the first reply's
    // bytes, which is what lets the client resend after a lost reply.
    assert_eq!(
        read_bytes("wire-import-repeat-request.json"),
        read_bytes("wire-import-request.json")
    );
    assert_eq!(
        read_bytes("wire-import-repeat-reply.json"),
        read_bytes("wire-import-reply.json")
    );
}

/// A daemon whose catalog lists ck-mc as an agent-data provider but which
/// refuses to open its route, as the daemon does for a ck-mc built with moves
/// off: that build declares no agent-data service, and the caller sees
/// `target_unavailable` instead of any agent-data body.
struct MovesOffDaemon(FakeDaemon);

fn target_unavailable() -> TransportError {
    TransportError::Refusal {
        code: "target_unavailable".into(),
        detail: None,
        message: Some("the target does not serve this route".into()),
    }
}

#[async_trait]
impl ParticipantDiscovery for MovesOffDaemon {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        self.0.supervisor_list().await
    }
    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        self.0.catalog_list().await
    }
    async fn open_participant(
        &self,
        _module_id: &str,
    ) -> Result<Arc<dyn LogTransport>, TransportError> {
        Err(target_unavailable())
    }
}

#[tokio::test]
async fn mc_agent_data_samples_moves_off_route_refusal_is_handled_as_today() {
    // At discovery: the move is refused as participant_unavailable, naming the
    // route refusal, before any agent-data request is sent. (A build that also
    // stops declaring agent-data/v1 is not a participant at all; discovery
    // skips it without opening a route.)
    let daemon = MovesOffDaemon(FakeDaemon::new(DaemonSnapshot::new(
        1,
        vec![json!({
            "module_id": "magic-context", "enabled": true, "state": "running",
            "live": true, "health": "ok",
        })],
        vec![json!({
            "module_id": "magic-context", "ready": true, "roles": [provider_role()],
            "control_ops": [],
            "capabilities": {"provides": [cortexkit_role_agent_data::PROVIDES], "requires": []},
        })],
    )));
    let refused = discover_participants(&daemon)
        .await
        .err()
        .expect("a participant whose route is refused blocks discovery");
    assert_eq!(refused.code(), "participant_unavailable");
    assert_eq!(
        refused.detail,
        json!({
            "module_id": "magic-context", "state": "running",
            "route_refusal": {"code": "target_unavailable", "detail": null},
        })
    );

    // At call time, on a route opened before ck-mc restarted with moves off:
    // `target_unavailable` is outside the agent-data refusal set, so the
    // client reports a contract violation, and the driver's table maps the
    // unknown code to Abort rather than a retry.
    for stem in ["wire-describe-no-runner", "wire-import"] {
        let (request, params) = sample_request(&format!("{stem}-request.json"));
        let method = request.op().method();
        match send(stem, &request, params, Err(target_unavailable())).await {
            Err(ClientError::ContractViolation { op, code, .. }) => {
                assert_eq!((op, code.as_str()), (method, "target_unavailable"));
            }
            other => panic!("{stem}: target_unavailable gave {other:?}"),
        }
        assert_eq!(
            participant_action(method, "target_unavailable"),
            Some(DriverAction::Abort)
        );
    }
}

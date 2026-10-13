//! Independent test oracle for the JSON bodies sent to Cingulate. The upstream
//! decoder is not published as a dependency, so this ports its generic-consent
//! rules from cingulate-decode 0.4.0 at Cingulate commit 7a1c32c. The source is
//! `crates/cingulate-decode/src/lib.rs` in the cortexkit/cingulate repository;
//! all `lib.rs` citations below refer to that file at that revision. Requester
//! identity admission, phone answering and permission-ask kinds are outside the
//! generic JSON decoder and are not part of this oracle.

use serde::Deserialize;
use serde_json::Value;

// Cingulate decoder lib.rs:17-46: nested objects reject unknown fields,
// and their members must be strings.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct OptionBody {
    id: String,
    label: String,
    effect: String,
    detail: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    kind: String,
    label: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fact {
    label: String,
    value: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Preview {
    label: String,
    text: String,
}

// Cingulate decoder lib.rs:62-117: reject unknown request fields; accept
// optional uninterpreted JSON, i64 expiry milliseconds and default-empty facts.
// Retain package/flow payload fields so their use on a consent card is rejected
// by the same semantic rule as upstream, not by an unknown-field check.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Request {
    package: Option<Value>,
    flow_install: Option<serde_json::Map<String, Value>>,
    flow_decision: Option<serde_json::Map<String, Value>>,
    kind: String,
    title: String,
    prompt: String,
    options: Vec<OptionBody>,
    default: String,
    urgency: String,
    dedup_key: Option<String>,
    requester_ref: Option<Value>,
    on_expiry: String,
    material_damage: bool,
    scope: Option<Value>,
    duration: Option<Value>,
    args_digest: Option<String>,
    late_execution: String,
    session_ref: Option<String>,
    parent_session_ref: Option<String>,
    target: Target,
    #[serde(default)]
    facts: Vec<Fact>,
    preview: Option<Preview>,
    expires_in_ms: i64,
    #[serde(rename = "requestedSchema")]
    requested_schema: Option<Value>,
    binding: Option<Value>,
    at_console: Option<String>,
}

pub(crate) fn decode(body: Value) -> Result<Value, &'static str> {
    // Cingulate decoder lib.rs:119-145: a present requester_ref, including null, is strictly
    // printable ASCII. at_console permits null but not a blank string.
    if let Some(reference) = body.get("requester_ref") {
        if !reference.as_str().is_some_and(|s| {
            (1..=128).contains(&s.len()) && s.bytes().all(|b| (b' '..=b'~').contains(&b))
        }) {
            return Err("elicitation_invalid_request");
        }
    }
    let request: Request =
        serde_json::from_value(body.clone()).map_err(|_| "elicitation_invalid_request")?;
    if request
        .at_console
        .as_ref()
        .is_some_and(|s| s.trim().is_empty())
    {
        return Err("elicitation_invalid_request");
    }
    // Cingulate decoder lib.rs:216-235,255-261: foreign bodies and choose effects are forbidden
    // on consent; this port deliberately handles only kind consent.
    if request.kind != "consent" {
        return Err("elicitation_unknown_kind");
    }
    if request.package.is_some()
        || request.flow_install.is_some()
        || request.flow_decision.is_some()
        || request.options.iter().any(|o| o.effect == "choose")
    {
        return Err("elicitation_invalid_request");
    }
    // Cingulate decoder lib.rs:296-312: nonblank title/prompt and option labels;
    // nonempty option ids that are unique within the card.
    if request.title.trim().is_empty()
        || request.prompt.trim().is_empty()
        || request.options.is_empty()
    {
        return Err("elicitation_invalid_request");
    }
    let mut ids = std::collections::HashSet::new();
    for option in &request.options {
        if option.id.trim().is_empty()
            || option.label.trim().is_empty()
            || !ids.insert(option.id.as_str())
        {
            return Err("elicitation_invalid_request");
        }
    }
    // Cingulate decoder lib.rs:313-333: the optional requestedSchema must
    // enumerate exactly the offered option ids.
    if let Some(schema) = request.requested_schema {
        let mut schema_ids = schema
            .pointer("/properties/decision/enum")
            .and_then(Value::as_array)
            .and_then(|a| a.iter().map(Value::as_str).collect::<Option<Vec<_>>>())
            .ok_or("elicitation_invalid_request")?;
        let mut offered = request
            .options
            .iter()
            .map(|o| o.id.as_str())
            .collect::<Vec<_>>();
        schema_ids.sort_unstable();
        offered.sort_unstable();
        if schema_ids != offered {
            return Err("elicitation_invalid_request");
        }
    }
    // Cingulate decoder lib.rs:334-353: the default must decline, expiry must
    // deny, urgency is low/normal/high, late_execution is execute/notify_only,
    // and expiry is between one millisecond and 24 hours.
    let default = request
        .options
        .iter()
        .find(|o| o.id == request.default)
        .ok_or("elicitation_invalid_request")?;
    if default.effect != "decline" || request.on_expiry != "deny" {
        return Err("elicitation_fail_closed_required");
    }
    if !matches!(request.urgency.as_str(), "low" | "normal" | "high")
        || !matches!(request.late_execution.as_str(), "execute" | "notify_only")
        || !(1..=86_400_000).contains(&request.expires_in_ms)
    {
        return Err("elicitation_invalid_request");
    }
    // Cingulate decoder lib.rs:355-365: require either scope or nonblank
    // args_digest, plus nonblank target kind and label.
    if request.scope.is_none()
        && request
            .args_digest
            .as_ref()
            .is_none_or(|s| s.trim().is_empty())
        || request.target.kind.trim().is_empty()
        || request.target.label.trim().is_empty()
    {
        return Err("elicitation_invalid_request");
    }
    // These fields are shape-checked, not semantically restricted by the decoder.
    let _ = (
        request.dedup_key,
        request.requester_ref,
        request.material_damage,
        request.duration,
        request.session_ref,
        request.parent_session_ref,
        request.binding,
    );
    for option in request.options {
        let _ = option.detail;
    }
    for fact in request.facts {
        let _ = (fact.label, fact.value);
    }
    if let Some(preview) = request.preview {
        let _ = (preview.label, preview.text);
    }
    // Cingulate decoder lib.rs:178-183: requestedSchema is checked then removed,
    // so an answer-schema change does not replace an otherwise identical card.
    let mut result = body;
    result.as_object_mut().unwrap().remove("requestedSchema");
    Ok(result)
}

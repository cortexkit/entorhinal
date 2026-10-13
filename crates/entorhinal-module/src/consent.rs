//! Consent acquisition and read-only recovery, separate from authorized writes.
//!
//! A decision is returned to the tool executor, which commits the mutation (or
//! refusal), audit and terminal request together. Feed capture never executes it.

use std::{
    collections::HashSet,
    sync::{Arc, Mutex},
};

use cortexkit_role_tool_provider::catalog::composition_digest;
use entorhinal_core::{
    agent::{
        consent::{ConsentRequest, RecordedSettlement, RequestIdentity, RequestState},
        AgentMutationError,
    },
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use subc_protocol::scope::ScopeStamp;
use tokio::time::{timeout_at, Instant};

use crate::consent_client::{
    bounded_call, ConsentConnector, ConsentError, ConsentRoute, Owner, AWAIT_MS,
};

pub(super) const CARD_LIFETIME_MS: i64 = 600_000;
pub(super) const AMBIGUOUS_LIFETIME_MS: i64 = 660_000;
pub(super) const RETENTION_MS: i64 = 86_400_000;

fn domain_error(code: &str, message: &str) -> AgentMutationError {
    AgentMutationError {
        code: code.into(),
        message: message.into(),
        detail: None,
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Binding {
    pub agent_id: String,
    pub tool: String,
    pub request_key: String,
    pub args_digest: String,
}

impl Binding {
    pub fn for_request(row: &ConsentRequest) -> Self {
        Self {
            agent_id: row.identity.agent_id.clone(),
            tool: row.identity.tool.clone(),
            request_key: row.identity.request_key.clone(),
            args_digest: row.args_digest.clone(),
        }
    }
    fn identity(&self) -> RequestIdentity {
        RequestIdentity {
            agent_id: self.agent_id.clone(),
            tool: self.tool.clone(),
            request_key: self.request_key.clone(),
        }
    }
}

/// Canonicalize the tuple as a JSON array, not a delimiter-joined string: keys
/// may contain any printable ASCII, including whichever delimiter was chosen.
pub(super) fn external_key(key: &RequestIdentity) -> String {
    let digest = composition_digest(&json!([key.agent_id, key.tool, key.request_key]))
        .expect("string tuple canonicalizes");
    format!("ent1:{digest}")
}

pub(super) struct CardContext {
    pub caller_label: String,
    pub target_kind: String,
    pub target_label: String,
    pub target_scope: Value,
}

fn always_choice(tool: &str) -> Option<&'static str> {
    match tool {
        "projects_register" | "agents_create_head" => Some("approve_always_workspace"),
        "agents_hire" => Some("approve_always_project"),
        _ => None,
    }
}

pub(super) fn render_card(
    row: &ConsentRequest,
    context: &CardContext,
) -> Result<Value, AgentMutationError> {
    let action = match row.identity.tool.as_str() {
        "projects_create_workspace" => "create workspace",
        "projects_register" => "register project",
        "agents_create_head" => "create project head",
        "agents_hire" => "hire agent",
        _ => {
            return Err(domain_error(
                "invalid_request",
                "this action has no consent card",
            ))
        }
    };
    let mut options = vec![json!({"id":"approve_once", "label":"Approve once", "effect":"grant"})];
    if let Some(choice) = always_choice(&row.identity.tool) {
        let label = if choice == "approve_always_workspace" {
            "Approve always for this workspace"
        } else {
            "Approve always for this project"
        };
        options.push(json!({"id":choice, "label":label, "effect":"grant"}));
    }
    options.push(json!({"id":"deny", "label":"Deny", "effect":"decline"}));
    let key = external_key(&row.identity);
    Ok(json!({
        "kind":"consent",
        "title":format!("{}: {} {}", context.caller_label, action, context.target_label),
        "prompt":format!("{} requests permission to {} {}.", context.caller_label, action, context.target_label),
        "target":{"kind":context.target_kind, "label":context.target_label},
        "urgency":"high", "options":options, "default":"deny", "on_expiry":"deny",
        "material_damage":false, "late_execution":"notify_only", "expires_in_ms":CARD_LIFETIME_MS,
        "scope":{"action":row.identity.tool, "target":context.target_scope},
        "dedup_key":key, "requester_ref":key, "binding":Binding::for_request(row),
    }))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ConsentDecision {
    Approved(RecordedSettlement),
    Refused(&'static str),
    Pending(Option<String>),
    Replay(Value),
}

// The lease spans the executor's terminal commit, not just its Cingulate wait.
// Releasing it earlier would let a concurrent retry consume the same answer.
type RequestSlot = (String, String, String);
#[derive(Default)]
pub(super) struct ConsentCoordinator {
    active: Arc<Mutex<HashSet<RequestSlot>>>,
}

pub(super) struct ConsentAttempt {
    pub decision: ConsentDecision,
    _lease: Option<RequestLease>,
}
struct RequestLease {
    active: Arc<Mutex<HashSet<RequestSlot>>>,
    key: RequestSlot,
}
impl Drop for RequestLease {
    fn drop(&mut self) {
        self.active
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.key);
    }
}

pub(super) struct ConsentCall<'a> {
    pub store: &'a RegistryStore,
    pub connector: &'a dyn ConsentConnector,
    pub stamp: &'a ScopeStamp,
    pub deadline: Instant,
    pub now_ms: &'a (dyn Fn() -> i64 + Send + Sync),
}

impl ConsentCoordinator {
    /// Call only after caller, key, target and cap validation. The supplied row
    /// describes that validated target; stamp identity comes from route admission.
    pub async fn obtain(
        &self,
        call: ConsentCall<'_>,
        row: &ConsentRequest,
        context: &CardContext,
    ) -> Result<ConsentAttempt, AgentMutationError> {
        let store = call.store;
        row.identity.validate()?;
        let owner = Owner::from_stamp(call.stamp)
            .map_err(|_| domain_error("caller_forbidden", "invalid scope epoch"))?;
        if call.stamp.attributes.agent_id.as_deref() != Some(row.identity.agent_id.as_str())
            || row.scope_ref != owner.scope_ref
            || row.scope_epoch != owner.epoch
        {
            return Err(domain_error(
                "caller_forbidden",
                "request must use the stamped caller and scope",
            ));
        }
        let saved = store.consent_request(&row.identity)?;
        if saved
            .as_ref()
            .is_some_and(|saved| saved.args_digest != row.args_digest)
        {
            return Err(domain_error(
                "request_key_conflict",
                "request key has different arguments",
            ));
        }
        let key = (
            row.identity.agent_id.clone(),
            row.identity.tool.clone(),
            row.identity.request_key.clone(),
        );
        let lease = {
            let mut active = self.active.lock().unwrap_or_else(|e| e.into_inner());
            active.insert(key.clone()).then(|| RequestLease {
                active: self.active.clone(),
                key,
            })
        };
        if lease.is_none() {
            return Ok(ConsentAttempt {
                decision: ConsentDecision::Pending(saved.and_then(|saved| saved.elicitation_id)),
                _lease: None,
            });
        }
        let card = render_card(row, context)?;
        let (saved, inserted) = store.consent_transaction(|tx| {
            let inserted = tx.request(&row.identity)?.is_none();
            Ok((tx.begin_request(row)?, inserted))
        })?;
        let decision = resolve(&call, &saved, card, inserted).await?;
        Ok(ConsentAttempt {
            decision,
            _lease: lease,
        })
    }
}

fn recorded_decision(row: &ConsentRequest, now: i64) -> Option<ConsentDecision> {
    let answer = row.settlement.as_ref()?;
    if now.saturating_sub(answer.settled_at_ms) >= RETENTION_MS {
        return Some(ConsentDecision::Refused("consent_lost"));
    }
    Some(match (answer.state.as_str(), answer.choice.as_deref()) {
        ("answered", Some("approve_once")) => ConsentDecision::Approved(answer.clone()),
        ("answered", Some(choice)) if Some(choice) == always_choice(&row.identity.tool) => {
            ConsentDecision::Approved(answer.clone())
        }
        ("answered", Some("deny")) => ConsentDecision::Refused("consent_declined"),
        ("expired", _) => ConsentDecision::Refused("consent_expired"),
        _ => ConsentDecision::Refused("consent_lost"),
    })
}

async fn resolve(
    call: &ConsentCall<'_>,
    row: &ConsentRequest,
    card: Value,
    inserted: bool,
) -> Result<ConsentDecision, AgentMutationError> {
    if row.state != RequestState::Awaiting {
        return row
            .outcome
            .clone()
            .map(ConsentDecision::Replay)
            .ok_or_else(|| domain_error("consent_lost", "terminal request has no outcome"));
    }
    let owner = Owner::from_stamp(call.stamp)
        .map_err(|_| domain_error("caller_forbidden", "invalid scope epoch"))?;
    if row.scope_ref != owner.scope_ref || row.scope_epoch != owner.epoch {
        return Ok(ConsentDecision::Refused("consent_lost"));
    }
    if let Some(decision) = recorded_decision(row, (call.now_ms)()) {
        return Ok(decision);
    }
    let route = match timeout_at(
        call.deadline,
        call.connector.open(call.stamp, call.deadline),
    )
    .await
    {
        Ok(Ok(route)) => route,
        _ if inserted => {
            call.store
                .consent_transaction(|tx| tx.delete_awaiting(&row.identity))?;
            return Ok(ConsentDecision::Refused("consent_unavailable"));
        }
        _ => return Ok(unresolved(row, (call.now_ms)())),
    };
    let result = resolve_on_route(call, row, card, inserted, route.as_ref(), &owner).await;
    // Closing has no bearing on the decision and shares the same call deadline.
    let _ = timeout_at(call.deadline, route.close()).await;
    result
}

fn unresolved(row: &ConsentRequest, now: i64) -> ConsentDecision {
    if row.elicitation_id.is_none()
        && now.saturating_sub(row.created_at_ms) >= AMBIGUOUS_LIFETIME_MS
    {
        ConsentDecision::Refused("consent_lost")
    } else {
        ConsentDecision::Pending(row.elicitation_id.clone())
    }
}

async fn resolve_on_route(
    call: &ConsentCall<'_>,
    row: &ConsentRequest,
    card: Value,
    inserted: bool,
    route: &dyn ConsentRoute,
    owner: &Owner,
) -> Result<ConsentDecision, AgentMutationError> {
    let mut current = row.clone();
    if inserted {
        match bounded_call(route, "consent.request", card, call.deadline).await {
            Ok(reply) => {
                let Some(id) = reply
                    .get("elicitation_id")
                    .and_then(Value::as_str)
                    .filter(|id| !id.is_empty())
                else {
                    return Ok(unresolved(row, (call.now_ms)()));
                };
                call.store
                    .consent_transaction(|tx| tx.set_elicitation(&row.identity, id))?;
                current.elicitation_id = Some(id.to_owned());
            }
            Err(ConsentError::Refused(code)) if code != "cancelled" => {
                call.store
                    .consent_transaction(|tx| tx.delete_awaiting(&row.identity))?;
                return Ok(ConsentDecision::Refused("consent_unavailable"));
            }
            // A cancelled or unreadable request reply may follow a committed card.
            // Resending could raise another card after the first one settled.
            Err(_) => return Ok(unresolved(row, (call.now_ms)())),
        }
    }
    let Some(id) = current.elicitation_id.as_deref() else {
        let capture = capture_feed(call.store, route, owner, call.deadline).await;
        if !capture.complete {
            return Ok(unresolved(row, (call.now_ms)()));
        }
        let saved = call
            .store
            .consent_request(&row.identity)?
            .expect("awaiting row exists");
        return Ok(recorded_decision(&saved, (call.now_ms)())
            .unwrap_or_else(|| unresolved(&saved, (call.now_ms)())));
    };
    if !inserted {
        match bounded_call(
            route,
            "consent.get",
            json!({"elicitation_id":id}),
            call.deadline,
        )
        .await
        {
            Ok(record) => match inspect_record(call.store, &current, &record, (call.now_ms)())? {
                RecordDecision::Settled(decision) => return Ok(decision),
                RecordDecision::Mismatch => {
                    return Ok(ConsentDecision::Pending(Some(id.to_owned())))
                }
                RecordDecision::Pending => {}
            },
            Err(ConsentError::Refused(code))
                if code == "elicitation_not_found" || code == "elicitation_not_owner" =>
            {
                return Ok(ConsentDecision::Refused("consent_lost"))
            }
            Err(_) => return Ok(ConsentDecision::Pending(Some(id.to_owned()))),
        }
    }
    let awaited = bounded_call(
        route,
        "consent.await",
        json!({"elicitation_id":id, "timeout_ms":AWAIT_MS}),
        call.deadline,
    )
    .await;
    if let Ok(record) = awaited {
        match inspect_record(call.store, &current, &record, (call.now_ms)())? {
            RecordDecision::Settled(decision) => return Ok(decision),
            RecordDecision::Mismatch => return Ok(ConsentDecision::Pending(Some(id.to_owned()))),
            RecordDecision::Pending => {}
        }
    }
    // Pending (or an ambiguous wait) must detach before releasing the held call.
    // Detach may have raced an answer; that answer belongs to this same call.
    if let Ok(record) = bounded_call(
        route,
        "consent.detach",
        json!({"elicitation_id":id}),
        call.deadline,
    )
    .await
    {
        if let RecordDecision::Settled(decision) =
            inspect_record(call.store, &current, &record, (call.now_ms)())?
        {
            return Ok(decision);
        }
    }
    Ok(ConsentDecision::Pending(Some(id.to_owned())))
}

enum RecordDecision {
    Pending,
    Mismatch,
    Settled(ConsentDecision),
}

fn inspect_record(
    store: &RegistryStore,
    row: &ConsentRequest,
    record: &Value,
    now: i64,
) -> Result<RecordDecision, AgentMutationError> {
    if record.get("binding") != Some(&json!(Binding::for_request(row)))
        || record["elicitation_id"].as_str() != row.elicitation_id.as_deref()
    {
        return Ok(RecordDecision::Mismatch);
    }
    if record["state"] == "pending" {
        return Ok(RecordDecision::Pending);
    }
    let Some(answer) = settlement(record) else {
        return Ok(RecordDecision::Mismatch);
    };
    store.consent_transaction(|tx| {
        tx.record_settlement(
            &row.identity,
            &row.args_digest,
            &row.scope_ref,
            row.scope_epoch,
            &answer,
        )
    })?;
    let saved = store
        .consent_request(&row.identity)?
        .expect("awaiting request exists");
    Ok(RecordDecision::Settled(
        recorded_decision(&saved, now).unwrap_or(ConsentDecision::Refused("consent_lost")),
    ))
}

fn settlement(record: &Value) -> Option<RecordedSettlement> {
    // Retention tombstones have no binding and their state is not an answer.
    if record["kind"] != "consent" || !record.get("binding")?.is_object() {
        return None;
    }
    let state = record["state"].as_str()?;
    if !matches!(state, "answered" | "expired" | "withdrawn") {
        return None;
    }
    Some(RecordedSettlement {
        state: state.to_owned(),
        choice: if state == "answered" {
            Some(record["answered_choice_id"].as_str()?.to_owned())
        } else {
            None
        },
        elicitation_id: record["elicitation_id"].as_str()?.to_owned(),
        settled_at_ms: record["answered_at_ms"].as_i64()?,
    })
}

#[derive(Debug)]
pub(super) struct CaptureReport {
    pub cursor: i64,
    pub complete: bool,
}

/// Capture is safe before a terminal commit and as best-effort maintenance after
/// it. It changes neither request state nor outcome. All network work is bounded
/// by the original tool call's deadline, including acknowledgement.
pub(super) async fn capture_feed(
    store: &RegistryStore,
    route: &dyn ConsentRoute,
    owner: &Owner,
    deadline: Instant,
) -> CaptureReport {
    let Ok(mut cursor) = store.consent_feed_cursor(&owner.scope_ref, owner.epoch) else {
        return CaptureReport {
            cursor: 0,
            complete: false,
        };
    };
    loop {
        let page =
            match bounded_call(route, "consent.answers", json!({"since":cursor}), deadline).await {
                Ok(page) => page,
                Err(_) => {
                    return CaptureReport {
                        cursor,
                        complete: false,
                    }
                }
            };
        let Some(records) = page["records"].as_array() else {
            return CaptureReport {
                cursor,
                complete: false,
            };
        };
        let Some(next) = page["cursor"].as_i64().filter(|next| *next >= cursor) else {
            return CaptureReport {
                cursor,
                complete: false,
            };
        };
        let continuation = page.get("continuation");
        if continuation.is_some_and(|value| value.as_i64() != Some(next) || next <= cursor) {
            return CaptureReport {
                cursor,
                complete: false,
            };
        }
        let commit = store.consent_transaction(|tx| {
            for record in records {
                let Some(answer) = settlement(record) else {
                    continue;
                };
                let Ok(binding) = serde_json::from_value::<Binding>(record["binding"].clone())
                else {
                    continue;
                };
                let key = binding.identity();
                if key.validate().is_err() {
                    continue;
                }
                tx.record_settlement(
                    &key,
                    &binding.args_digest,
                    &owner.scope_ref,
                    owner.epoch,
                    &answer,
                )?;
            }
            tx.save_feed_cursor(&owner.scope_ref, owner.epoch, next)
        });
        if commit.is_err() {
            return CaptureReport {
                cursor,
                complete: false,
            };
        }
        cursor = next;
        match bounded_call(route, "consent.ack", json!({"cursor":cursor}), deadline).await {
            Ok(reply) if reply["ok"] == true => {}
            _ => {
                return CaptureReport {
                    cursor,
                    complete: false,
                }
            }
        }
        if continuation.is_none() {
            return CaptureReport {
                cursor,
                complete: true,
            };
        }
    }
}

#[cfg(test)]
#[path = "consent_tests.rs"]
mod tests;

use super::*;
use crate::consent::{CardContext, ConsentCall, ConsentDecision};
use cortexkit_role_tool_provider::catalog::composition_digest;
use entorhinal_core::agent::consent::{
    AuditRow, Authorization, ConsentCommit, ConsentRequest, RequestIdentity, RequestState,
    StandingGrant,
};
use entorhinal_core::RegistryStore;
use tokio::time::Instant;

fn pending(id: Option<String>) -> HandlerError {
    HandlerError {
        code: "consent_pending".into(),
        message: "consent has not settled; retry with the same request key".into(),
        detail: Some(json!({"elicitation_id":id})),
    }
}

fn replay(value: Value) -> Result<Vec<u8>, HandlerError> {
    if let Some(error) = value.get("error") {
        return Err(HandlerError {
            code: error["code"].as_str().unwrap_or("consent_stale").into(),
            message: error["message"]
                .as_str()
                .unwrap_or("request refused")
                .into(),
            detail: error.get("detail").cloned().filter(|v| !v.is_null()),
        });
    }
    encode_result(value)
}

impl ProjectsHandler {
    pub(super) async fn execute_agent_tool(
        &self,
        tool: AgentTool,
        mut arguments: Map<String, Value>,
        admission: RouteAdmission,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let stamp = admission.scope.as_ref().expect("admitted agent scope");
        let caller = stamp
            .attributes
            .agent_id
            .as_deref()
            .expect("admitted agent");
        let store = self.with_store(|s| Ok(s.clone()))?;
        if !tool.writes {
            let mut hires = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                let body = store.agent_read(
                    "agent.list",
                    json!({"role":"hiree","limit":200,"cursor":cursor}),
                )?;
                let page: Value =
                    serde_json::from_slice::<Value>(&body).expect("core reply")["result"].clone();
                for agent in page["agents"].as_array().expect("agent list") {
                    let row = store.agent_row(agent["agent_id"].as_str().unwrap())?;
                    if row.is_some_and(|row| {
                        row.status == "live" && row.supervisor_agent_id.as_deref() == Some(caller)
                    }) {
                        hires.push(agent.clone());
                    }
                }
                cursor = page["next_cursor"].as_str().map(str::to_owned);
                if cursor.is_none() {
                    break;
                }
            }
            return encode_result(json!({"agents":hires}));
        }
        let identity = RequestIdentity {
            agent_id: caller.into(),
            tool: tool.name.into(),
            request_key: arguments
                .remove("request_key")
                .expect("validated key")
                .as_str()
                .unwrap()
                .into(),
        };
        let arguments = Value::Object(arguments);
        let digest = composition_digest(&arguments).expect("canonical arguments");
        let saved = store.consent_request(&identity)?;
        if let Some(saved) = &saved {
            if saved.args_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
            if saved.state != RequestState::Awaiting {
                return replay(saved.outcome.clone().expect("terminal outcome"));
            }
        }
        let mut checked = arguments.as_object().expect("tool arguments").clone();
        checked.insert("request_key".into(), json!(identity.request_key));
        validate_arguments(tool, &checked)?;
        let Some(lease) = self.consent_coordinator.try_lease(&identity) else {
            return Err(pending(saved.and_then(|r| r.elicitation_id)));
        };
        let plan = if let Some(saved) = &saved {
            match store.consent_write_plan(&identity)? {
                Some(plan) => plan,
                None => {
                    return self.refuse_agent_write(
                        &store,
                        saved,
                        tool.name,
                        &Value::Null,
                        Authorization::Card {
                            id: saved.elicitation_id.clone().unwrap_or_default(),
                            answer: "deny".into(),
                        },
                        HandlerError::new(
                            "consent_lost",
                            "awaiting request has no validated target",
                        ),
                    )
                }
            }
        } else {
            store.precheck_consent_write(caller, tool.name, arguments, (self.clock)())?
        };
        let row = ConsentRequest {
            identity,
            args_digest: digest,
            scope_ref: stamp.scope_ref.clone(),
            scope_epoch: i64::try_from(stamp.scope_epoch)
                .map_err(|_| HandlerError::new("caller_forbidden", "scope epoch out of range"))?,
            created_at_ms: (self.clock)(),
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        };
        let grant = if saved.is_none() {
            match plan.grant_class.clone().zip(plan.scope_id.as_deref()) {
                Some((class, scope)) => store.covering_consent_grant(caller, class, scope)?,
                None => None,
            }
        } else {
            None
        };
        let mut attempt = None;
        let mut granted = None;
        let authorization = if tool.name == "agents_retire_hire" {
            Authorization::OwnHire
        } else if let Some(grant) = grant {
            Authorization::Grant { id: grant.id }
        } else {
            let caller_name = store.agent_row(caller)?.expect("admitted caller").name;
            let context = CardContext {
                caller_label: caller_name,
                target_kind: plan.target["kind"].as_str().unwrap().into(),
                target_label: plan.target["label"].as_str().unwrap().into(),
                target_scope: plan.target.clone(),
            };
            attempt = Some(
                self.consent_coordinator
                    .obtain_leased(
                        ConsentCall {
                            store: &store,
                            connector: self.consent_connector.as_ref(),
                            stamp,
                            deadline,
                            now_ms: &self.clock,
                        },
                        &row,
                        &context,
                        Some(&plan),
                        lease,
                    )
                    .await?,
            );
            match &attempt.as_ref().unwrap().decision {
                ConsentDecision::Pending(id) => return Err(pending(id.clone())),
                ConsentDecision::Replay(outcome) => return replay(outcome.clone()),
                ConsentDecision::Refused("consent_unavailable") => {
                    return Err(HandlerError::new(
                        "consent_unavailable",
                        "Cingulate is unavailable",
                    ))
                }
                ConsentDecision::Refused(code) => {
                    let saved = store
                        .consent_request(&row.identity)?
                        .expect("consent request retained");
                    let auth = Authorization::Card {
                        id: saved.elicitation_id.unwrap_or_default(),
                        answer: saved
                            .settlement
                            .and_then(|s| s.choice)
                            .unwrap_or_else(|| "deny".into()),
                    };
                    return self.refuse_agent_write(
                        &store,
                        &row,
                        tool.name,
                        &plan.target,
                        auth,
                        HandlerError::new(code, "consent did not authorize the write"),
                    );
                }
                ConsentDecision::Approved(answer) => {
                    if matches!(
                        answer.choice.as_deref(),
                        Some("approve_always_workspace" | "approve_always_project")
                    ) {
                        granted = Some(StandingGrant {
                            id: format!("grant:{}", crate::consent::external_key(&row.identity)),
                            holder_agent_id: caller.into(),
                            class: plan
                                .grant_class
                                .clone()
                                .expect("always option has grant class"),
                            scope_id: plan.scope_id.clone().expect("always option has scope"),
                            granted_at_ms: (self.clock)(),
                            card_ref: answer.elicitation_id.clone(),
                            revoked_at_ms: None,
                        });
                    }
                    Authorization::Card {
                        id: answer.elicitation_id.clone(),
                        answer: answer.choice.clone().expect("approved choice"),
                    }
                }
            }
        };
        // Exclude other calls on this request key until the database commits
        // its applied or refused outcome. Card calls hold that exclusion in
        // the attempt; grant and retirement calls keep the original lease.
        let commit = ConsentCommit {
            request: row.clone(),
            authorization: authorization.clone(),
            action: tool.name.into(),
            target: json!(plan),
            granted,
            now_ms: (self.clock)(),
        };
        let result = self
            .consent_write_wait(plan.clone(), commit, deadline)
            .await;
        let outcome = match result {
            Ok(body) => {
                let envelope: Value =
                    serde_json::from_slice(&body).map_err(crate::invalid_params)?;
                replay(envelope["result"].clone())
            }
            Err(error)
                if matches!(
                    error.code.as_str(),
                    "consent_stale"
                        | "gone"
                        | "hire_cap_reached"
                        | "not_your_hire"
                        | "name_conflict"
                        | "head_exists"
                        | "invalid_role_shape"
                        | "project_exists"
                        | "workspace_exists"
                ) =>
            {
                let error = if matches!(
                    error.code.as_str(),
                    "gone" | "hire_cap_reached" | "not_your_hire" | "consent_stale"
                ) {
                    error
                } else {
                    HandlerError::new("consent_stale", error.message)
                };
                self.refuse_agent_write(&store, &row, tool.name, &plan.target, authorization, error)
            }
            Err(error) => Err(error),
        };
        drop(attempt);
        outcome
    }

    fn refuse_agent_write(
        &self,
        store: &RegistryStore,
        row: &ConsentRequest,
        action: &str,
        target: &Value,
        authorization: Authorization,
        error: HandlerError,
    ) -> Result<Vec<u8>, HandlerError> {
        let outcome =
            json!({"error":{"code":error.code,"message":error.message,"detail":error.detail}});
        let saved_outcome = store.consent_transaction(|tx| {
            let saved = tx.begin_request(row)?;
            if saved.state != RequestState::Awaiting {
                return Ok(saved.outcome.expect("terminal outcome"));
            }
            tx.finish_request(
                &row.identity,
                &row.args_digest,
                RequestState::Refused,
                &outcome,
            )?;
            tx.append_audit(&AuditRow {
                row_id: 0,
                time_ms: (self.clock)(),
                agent_id: row.identity.agent_id.clone(),
                action: action.into(),
                target: target.clone(),
                authorization,
                outcome: outcome.clone(),
            })?;
            Ok(outcome)
        })?;
        replay(saved_outcome)
    }
}

#[cfg(test)]
#[path = "tool_writes_tests.rs"]
mod tests;

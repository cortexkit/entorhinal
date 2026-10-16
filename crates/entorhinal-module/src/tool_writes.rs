use super::*;
use crate::consent::{CardContext, ConsentCall, ConsentDecision};
use cortexkit_role_tool_provider::catalog::composition_digest;
use entorhinal_core::agent::consent::{
    AuditRow, Authorization, ConsentCommit, ConsentRequest, GrantClass, RequestIdentity,
    RequestState, StandingGrant,
};
use entorhinal_core::agent::{consent::ConsentWrite, AgentMutationError};
use entorhinal_core::RegistryStore;
use tokio::time::Instant;

pub(crate) fn pending(id: Option<String>) -> HandlerError {
    HandlerError {
        code: "consent_pending".into(),
        message: "consent has not settled; retry with the same request key".into(),
        detail: Some(json!({"elicitation_id":id})),
    }
}

fn consent_lost(message: impl Into<String>) -> HandlerError {
    HandlerError::new("consent_lost", message)
}

fn approval_grant(
    plan: &ConsentWrite,
    choice: &str,
) -> Result<Option<(GrantClass, String)>, HandlerError> {
    if choice == "approve_once" {
        return Ok(None);
    }
    let scope = plan.scope_id.as_deref().filter(|s| !s.is_empty());
    let valid = match (choice, plan.tool.as_str(), plan.grant_class.as_ref()) {
        ("approve_always_workspace", "projects_register", Some(GrantClass::RegisterProject)) => {
            scope == plan.target["workspace_id"].as_str()
        }
        ("approve_always_workspace", "agents_create_head", Some(GrantClass::CreateHead)) => {
            scope == plan.target["placement"]["workspace_id"].as_str()
        }
        ("approve_always_project", "agents_hire", Some(GrantClass::Hire)) => {
            scope == plan.target["placement"]["project_id"].as_str()
        }
        _ => false,
    };
    if !valid || scope.is_none() {
        return Err(HandlerError::new(
            "invalid_reply",
            "approval choice does not match this action's grant class and scope",
        ));
    }
    Ok(plan.grant_class.clone().zip(scope.map(str::to_owned)))
}

fn load_request(
    store: &RegistryStore,
    identity: &RequestIdentity,
) -> Result<Option<ConsentRequest>, HandlerError> {
    store
        .consent_request(identity)
        .map_err(|error| consent_lost(format!("stored consent request could not be read: {error}")))
}

fn terminal_outcome(row: &ConsentRequest) -> Result<Value, HandlerError> {
    row.outcome
        .clone()
        .ok_or_else(|| consent_lost("stored terminal consent request has no outcome"))
}

fn core_result(body: &[u8]) -> Result<Value, HandlerError> {
    let envelope: Value = serde_json::from_slice(body).map_err(|error| {
        HandlerError::new(
            "invalid_reply",
            format!("core returned invalid JSON: {error}"),
        )
    })?;
    envelope
        .get("result")
        .filter(|result| result.is_object())
        .cloned()
        .ok_or_else(|| HandlerError::new("invalid_reply", "core reply has no result object"))
}

fn validate_plan(plan: &ConsentWrite, tool: AgentTool, digest: &str) -> Result<(), HandlerError> {
    if plan.tool != tool.name || composition_digest(&plan.arguments).ok().as_deref() != Some(digest)
    {
        return Err(consent_lost(
            "stored consent plan does not match its request arguments",
        ));
    }
    let required = match tool.name {
        "projects_create_workspace" => Some(&plan.target["workspace_id"]),
        "projects_register" => Some(&plan.target["dir"]),
        "agents_create_head" => Some(&plan.target["placement"]["project_id"]),
        _ => None,
    };
    if required.is_some_and(|value| value.as_str().is_none()) {
        return Err(consent_lost(
            "stored consent plan is missing its write target",
        ));
    }
    Ok(())
}

pub(crate) fn replay(value: Value) -> Result<Vec<u8>, HandlerError> {
    if !value.is_object() {
        return Err(consent_lost("stored consent outcome is not an object"));
    }
    if let Some(error) = value.get("error") {
        return Err(HandlerError {
            code: error["code"]
                .as_str()
                .filter(|code| !code.is_empty())
                .ok_or_else(|| consent_lost("stored refusal has no error code"))?
                .into(),
            message: error["message"]
                .as_str()
                .ok_or_else(|| consent_lost("stored refusal has no error message"))?
                .into(),
            detail: error.get("detail").cloned().filter(|v| !v.is_null()),
        });
    }
    encode_tool_reply(value)
}

impl ProjectsHandler {
    pub(super) async fn execute_agent_tool(
        &self,
        tool: AgentTool,
        mut arguments: Map<String, Value>,
        admission: RouteAdmission,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let stamp = admission
            .scope
            .as_ref()
            .expect("admit_agent_tool established a stamped agent scope for this call");
        let caller = stamp
            .attributes
            .agent_id
            .as_deref()
            .expect("admit_agent_tool established a nonempty stamped caller agent id");
        let store = self.with_store(|s| Ok(s.clone()))?;
        if !tool.writes {
            let mut hires = Vec::new();
            let mut cursor: Option<String> = None;
            loop {
                let body = store.agent_read(
                    "agent.list",
                    json!({"role":"hiree","limit":200,"cursor":cursor}),
                )?;
                let page = core_result(&body)?;
                let agents = page["agents"].as_array().ok_or_else(|| {
                    HandlerError::new("invalid_reply", "core agent.list reply has no agents array")
                })?;
                for agent in agents {
                    let id = agent["agent_id"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .ok_or_else(|| {
                            HandlerError::new(
                                "invalid_reply",
                                "core agent.list row has no string agent_id",
                            )
                        })?;
                    let row = store.agent_row(id)?;
                    if row.is_some_and(|row| {
                        row.status == "live" && row.supervisor_agent_id.as_deref() == Some(caller)
                    }) {
                        hires.push(agent.clone());
                    }
                }
                cursor = match page.get("next_cursor") {
                    None => None,
                    Some(value) => Some(
                        value
                            .as_str()
                            .filter(|cursor| !cursor.is_empty())
                            .ok_or_else(|| {
                                HandlerError::new(
                                    "invalid_reply",
                                    "core agent.list next_cursor must be a nonempty string",
                                )
                            })?
                            .to_owned(),
                    ),
                };
                if cursor.is_none() {
                    break;
                }
            }
            return encode_tool_reply(json!({"agents":hires}));
        }
        let identity = RequestIdentity {
            agent_id: caller.into(),
            tool: tool.name.into(),
            // validate_request_key has already checked this field; refusing here
            // too keeps a missing or non-string key from ever reaching a write.
            request_key: match arguments.remove("request_key") {
                Some(Value::String(key)) => key,
                _ => {
                    return Err(HandlerError::new(
                        "invalid_request_key",
                        "request_key must be a string",
                    ))
                }
            },
        };
        let arguments = Value::Object(arguments);
        let digest = composition_digest(&arguments).expect("arguments is a serde_json::Value parsed from valid JSON, which has a canonical encoding");
        let saved = load_request(&store, &identity)?;
        if let Some(saved) = &saved {
            if saved.args_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
            if saved.state != RequestState::Awaiting {
                return replay(terminal_outcome(saved)?);
            }
        }
        let mut checked = arguments
            .as_object()
            .expect("arguments were constructed as Value::Object in this call")
            .clone();
        checked.insert("request_key".into(), json!(identity.request_key));
        validate_arguments(tool, &checked)?;
        let Some(lease) = self.consent_coordinator.try_lease(&identity) else {
            return Err(pending(saved.and_then(|r| r.elicitation_id)));
        };
        let plan = if let Some(saved) = &saved {
            match store.consent_write_plan(&identity) {
                Ok(Some(plan)) => plan,
                missing => {
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
                            format!(
                                "awaiting request has no readable validated target: {missing:?}"
                            ),
                        ),
                    )
                }
            }
        } else {
            store.precheck_consent_write(caller, tool.name, arguments, (self.clock)())?
        };
        let validated = (|| {
            validate_plan(&plan, tool, &digest)?;
            let target_kind = plan.target["kind"]
                .as_str()
                .ok_or_else(|| consent_lost("stored consent plan has no string target.kind"))?;
            let target_label = plan.target["label"]
                .as_str()
                .ok_or_else(|| consent_lost("stored consent plan has no string target.label"))?;
            Ok::<_, HandlerError>((target_kind, target_label))
        })();
        let (target_kind, target_label) = match validated {
            Ok(target) => target,
            Err(error) => {
                if let Some(saved) = saved.as_ref() {
                    return self.refuse_agent_write(
                        &store,
                        saved,
                        tool.name,
                        &plan.target,
                        Authorization::Card {
                            id: saved.elicitation_id.clone().unwrap_or_default(),
                            answer: saved
                                .settlement
                                .as_ref()
                                .and_then(|s| s.choice.clone())
                                .unwrap_or_default(),
                        },
                        error,
                    );
                }
                return Err(error);
            }
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
            let caller_name = store
                .agent_row(caller)?
                .ok_or_else(|| consent_lost("requesting agent row disappeared before consent"))?
                .name;
            let context = CardContext {
                caller_label: caller_name,
                target_kind: target_kind.into(),
                target_label: target_label.into(),
                target_scope: plan.target.clone(),
            };
            let obtained = self
                .consent_coordinator
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
                .await;
            attempt = Some(match obtained {
                Ok(attempt) => attempt,
                Err(error) => {
                    let error = HandlerError::from(error);
                    if matches!(error.code.as_str(), "invalid_reply" | "consent_lost") {
                        return self.refuse_agent_write(
                            &store,
                            &row,
                            tool.name,
                            &plan.target,
                            Authorization::Card {
                                id: saved
                                    .as_ref()
                                    .and_then(|s| s.elicitation_id.clone())
                                    .unwrap_or_default(),
                                answer: "deny".into(),
                            },
                            error,
                        );
                    }
                    return Err(error);
                }
            });
            match &attempt
                .as_ref()
                .expect(
                    "this call assigned a consent attempt immediately before reading its decision",
                )
                .decision
            {
                ConsentDecision::Pending(id) => return Err(pending(id.clone())),
                ConsentDecision::Replay(outcome) => return replay(outcome.clone()),
                ConsentDecision::Refused("consent_unavailable") => {
                    return Err(HandlerError::new(
                        "consent_unavailable",
                        "Cingulate is unavailable",
                    ))
                }
                ConsentDecision::Refused(code) => {
                    let saved = load_request(&store, &row.identity)?.ok_or_else(|| {
                        consent_lost("consent request disappeared before its refusal was recorded")
                    })?;
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
                    let approval = (|| {
                        let choice = answer
                            .choice
                            .as_deref()
                            .filter(|choice| {
                                matches!(
                                    *choice,
                                    "approve_once"
                                        | "approve_always_workspace"
                                        | "approve_always_project"
                                )
                            })
                            .ok_or_else(|| {
                                HandlerError::new(
                                    "invalid_reply",
                                    "approved consent settlement has no approval choice",
                                )
                            })?;
                        if answer.elicitation_id.is_empty() {
                            return Err(HandlerError::new(
                                "invalid_reply",
                                "approved consent settlement has no card id",
                            ));
                        }
                        let grant =
                            approval_grant(&plan, choice)?.map(|(class, scope_id)| StandingGrant {
                                id: format!(
                                    "grant:{}",
                                    crate::consent::external_key(&row.identity)
                                ),
                                holder_agent_id: caller.into(),
                                class,
                                scope_id,
                                granted_at_ms: (self.clock)(),
                                card_ref: answer.elicitation_id.clone(),
                                revoked_at_ms: None,
                            });
                        Ok::<_, HandlerError>((choice.to_owned(), grant))
                    })();
                    let (choice, grant) = match approval {
                        Ok(approval) => approval,
                        Err(error) => {
                            return self.refuse_agent_write(
                                &store,
                                &row,
                                tool.name,
                                &plan.target,
                                Authorization::Card {
                                    id: answer.elicitation_id.clone(),
                                    answer: answer.choice.clone().unwrap_or_default(),
                                },
                                error,
                            )
                        }
                    };
                    granted = grant;
                    Authorization::Card {
                        id: answer.elicitation_id.clone(),
                        answer: choice,
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
            Ok(body) => replay(core_result(&body)?).map_err(|error| {
                if error.code == "consent_lost" {
                    HandlerError::new("invalid_reply", error.message)
                } else {
                    error
                }
            }),
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
                        | "invalid_reply"
                        | "consent_lost"
                ) =>
            {
                let error = if matches!(
                    error.code.as_str(),
                    "gone"
                        | "hire_cap_reached"
                        | "not_your_hire"
                        | "consent_stale"
                        | "invalid_reply"
                        | "consent_lost"
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

    pub(crate) fn refuse_agent_write(
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
            let saved = tx.begin_request(row).map_err(|error| {
                if error.code == "request_key_conflict" {
                    error
                } else {
                    AgentMutationError {
                        code: "consent_lost".into(),
                        message: format!(
                            "stored consent request could not be read or retained: {error}"
                        ),
                        detail: None,
                    }
                }
            })?;
            if saved.state != RequestState::Awaiting {
                return saved.outcome.ok_or_else(|| AgentMutationError {
                    code: "consent_lost".into(),
                    message: "stored terminal consent request has no outcome".into(),
                    detail: None,
                });
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

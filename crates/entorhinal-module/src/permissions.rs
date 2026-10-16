//! Local permission inventory, audit reads and operator-only hire limits.
//! None of these operations contact Cingulate. Revocation gives the shared
//! writer a short deadline; permission reads never join its queue.

use super::agent_ops::{ConfirmationGuard, OperatorConfirmError};
use super::shared_write::OPERATOR_BOUND;
use super::{encode_result, HandlerError, ProjectsHandler, RouteKey};
use entorhinal_core::agent::consent::{GrantClass, StandingGrant};
use entorhinal_core::RegistryStore;
use serde::Deserialize;
use serde_json::{json, Value};
use subc_protocol::Principal;
use tokio::time::{timeout_at, Instant};

pub(super) const CAPABILITY: &str = "permissions/v1";
pub(super) const LIST: &str = "permissions.list";
pub(super) const REVOKE: &str = "permissions.revoke";
pub(super) const AUDIT: &str = "agent.audit_list";
pub(super) const SET_CAP: &str = "agent.set_hire_cap";
const REVOKE_BOUND: std::time::Duration = std::time::Duration::from_secs(4);
const CAP_WRITE_BOUND: std::time::Duration = std::time::Duration::from_secs(4);

fn revoke_unavailable() -> HandlerError {
    HandlerError::new(
        "unavailable",
        "a shared write is in progress; retry permission revocation",
    )
}

fn revoke_on_store(
    store: &RegistryStore,
    params: Value,
    now: i64,
    principal: Value,
) -> Result<Vec<u8>, HandlerError> {
    let request: RevokeRequest = parse(params)?;
    let mut found = false;
    for grant in store
        .consent_grants()
        .map_err(|error| HandlerError::new("storage_error", error.to_string()))?
    {
        if grant.id == request.id
            && live(store, &grant)
                .map_err(|error| HandlerError::new("storage_error", error.to_string()))?
        {
            found = true;
            break;
        }
    }
    let revoked = if found {
        store.consent_transaction(|tx| tx.revoke_grant(&request.id, now))?
    } else {
        store.revoke_publisher_grant(&request.id, now, &principal)?
    };
    encode_result(json!({"outcome":if revoked { "revoked" } else { "not_found" }}))
}

pub(super) fn admit(method: &str, principal: Option<&Principal>) -> Result<(), HandlerError> {
    let (module, code, message) = match method {
        LIST | REVOKE => (
            Some("cingulate"),
            "permissions_operator_required",
            "permissions operations need reserved:cingulate or the local operator's direct route",
        ),
        AUDIT => (
            Some("callosum"),
            "audit_operator_required",
            "agent.audit_list needs reserved:callosum or the local operator's direct route",
        ),
        SET_CAP => (
            None,
            "direct_caller_required",
            "hire caps require the local operator's direct route",
        ),
        _ => return Ok(()),
    };
    if matches!(principal, Some(Principal::Direct))
        || matches!(principal, Some(Principal::Reserved { module_id }) if Some(module_id.as_str()) == module)
    {
        return Ok(());
    }
    Err(HandlerError::new(code, message))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListRequest {
    agent_id: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RevokeRequest {
    id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AuditRequest {
    since_ms: Option<i64>,
    limit: Option<i64>,
    cursor: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CapRequest {
    project_id: String,
    cap: i64,
}

fn parse<T: serde::de::DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params).map_err(|e| HandlerError::new("invalid_request", e.to_string()))
}

fn live(
    store: &RegistryStore,
    grant: &StandingGrant,
) -> Result<bool, entorhinal_core::RegistryError> {
    Ok(store
        .covering_consent_grant(&grant.holder_agent_id, grant.class.clone(), &grant.scope_id)?
        .is_some())
}

impl ProjectsHandler {
    pub(super) fn permissions_list(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: ListRequest = parse(params)?;
        let grants = self.with_store(|store| {
            let subjects = store.enumerate(None)?;
            let mut rows = Vec::new();
            let mut holders = std::collections::HashMap::new();
            for grant in store.consent_grants()? {
                if request
                    .agent_id
                    .as_ref()
                    .is_some_and(|id| *id != grant.holder_agent_id)
                    || grant.revoked_at_ms.is_some()
                {
                    continue;
                }
                let holder_live = if let Some(live) = holders.get(&grant.holder_agent_id) {
                    *live
                } else {
                    let live = store
                        .agent_row(&grant.holder_agent_id)?
                        .is_some_and(|a| a.status == "live");
                    holders.insert(grant.holder_agent_id.clone(), live);
                    live
                };
                if !holder_live {
                    continue;
                }
                let (scope, subject, action) = match grant.class {
                    GrantClass::RegisterProject | GrantClass::CreateHead => {
                        let Some(workspace) = subjects
                            .workspaces
                            .iter()
                            .find(|w| w.workspace_id == grant.scope_id)
                        else {
                            continue;
                        };
                        if grant.class == GrantClass::RegisterProject {
                            (
                                "workspace_projects",
                                &workspace.name,
                                "register projects in workspace",
                            )
                        } else {
                            (
                                "workspace_heads",
                                &workspace.name,
                                "create project heads in workspace",
                            )
                        }
                    }
                    GrantClass::Hire => {
                        let Some(project) = subjects
                            .projects
                            .iter()
                            .find(|p| p.project_id == grant.scope_id)
                        else {
                            continue;
                        };
                        ("project_hires", &project.name, "hire agents in project")
                    }
                };
                rows.push(json!({
                    "id":grant.id, "agent_id":grant.holder_agent_id,
                    "holder":{"kind":"agent","agent_id":grant.holder_agent_id},
                    "module":super::MODULE_ID, "subject_label":subject,
                    "scope":scope, "scope_label":format!("{action} {subject:?}"),
                    "granted_at_ms":grant.granted_at_ms, "card_ref":grant.card_ref,
                }));
            }
            for grant in store.publisher_grants()? {
                if grant.revoked_at_ms.is_some()
                    || request.agent_id.as_ref().is_some_and(|id| *id != grant.placement_agent_id)
                    || !store.agent_row(&grant.placement_agent_id)?.is_some_and(|a| a.status == "live") {
                    continue;
                }
                rows.push(json!({
                    "id":grant.id,"agent_id":grant.placement_agent_id,
                    "holder":{"kind":"agent","agent_id":grant.placement_agent_id},
                    "module":super::MODULE_ID,"subject_label":grant.publisher_fingerprint,
                    "scope":"catalog_publisher",
                    "scope_label":format!("{} packages from publisher {}",grant.operation.as_str(),grant.publisher_fingerprint),
                    "granted_at_ms":grant.granted_at_ms,"card_ref":grant.card_ref,
                }));
            }
            Ok(rows)
        })?;
        encode_result(json!({"grants":grants}))
    }

    pub(super) fn permissions_revoke(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        revoke_on_store(
            &store,
            params,
            (self.clock)(),
            json!(self.admission_for(key).principal),
        )
    }

    pub(super) async fn permissions_revoke_wait(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit(REVOKE, key)?;
        let deadline = received + REVOKE_BOUND;
        // Refuse before starting SQL when the shared writer is occupied. The
        // caller has a five-second timeout and does not retry automatically.
        let writer = timeout_at(deadline, self.writer.clone().lock_owned())
            .await
            .map_err(|_| revoke_unavailable())?;
        let store = self.with_store(|s| Ok(s.clone()))?;
        let now = (self.clock)();
        let principal = json!(self.admission_for(key).principal);
        let worker = tokio::task::spawn_blocking(move || {
            let _writer = writer;
            #[cfg(test)]
            tokio::task::block_in_place(|| {});
            revoke_on_store(&store, params, now, principal)
        });
        timeout_at(deadline, worker)
            .await
            .map_err(|_| revoke_unavailable())?
            .map_err(|error| HandlerError::new("storage_error", error.to_string()))?
    }

    pub(super) fn agent_audit_list(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: AuditRequest = parse(params)?;
        let page = self.with_agent_store(|s| {
            s.agent_audit_page(request.since_ms, request.limit, request.cursor.as_deref())
        })?;
        encode_result(page)
    }

    pub(super) async fn set_hire_cap(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admission_for(key);
        admit(SET_CAP, admission.principal.as_ref())?;
        super::refuse_flow_write(SET_CAP, admission.flow_id.as_deref())?;
        let request: CapRequest = parse(params)?;
        if request.cap < 0 {
            return Err(HandlerError::new(
                "invalid_request",
                "hire cap must be non-negative",
            ));
        }
        let project = self
            .with_store(|s| s.enumerate(None))?
            .projects
            .into_iter()
            .find(|p| p.project_id == request.project_id)
            .ok_or_else(|| HandlerError::new("not_found", "unknown project"))?;
        let summary = format!(
            "set hire cap for project {:?} ({:?}) to {}",
            project.name, project.project_id, request.cap
        );
        let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(OperatorConfirmError::Declined.into());
        }
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(received + OPERATOR_BOUND, self.operator_confirmer.confirm_operator(&summary, key)) => {
                result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
            }
        }
        if Instant::now() >= received + OPERATOR_BOUND || *guard.cancelled.borrow() {
            return Err(OperatorConfirmError::Declined.into());
        }
        // Confirmation may take minutes; bound the local SQL phase separately,
        // without extending the receipt-relative operator deadline.
        let deadline = (Instant::now() + CAP_WRITE_BOUND).min(received + OPERATOR_BOUND);
        let unavailable = || {
            HandlerError::new(
                "unavailable",
                "a shared write is in progress; retry setting the hire cap",
            )
        };
        let writer = tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
            result = timeout_at(deadline, self.writer.clone().lock_owned()) => result.map_err(|_| unavailable())?,
        };
        if Instant::now() >= deadline || *guard.cancelled.borrow() {
            return Err(OperatorConfirmError::Declined.into());
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let sql = tokio::task::spawn_blocking(move || {
            // Keep confirmation exclusion until SQL finishes, even if the
            // caller times out or disconnects while the worker is running.
            let (_writer, _confirmation) = (writer, guard);
            #[cfg(test)]
            tokio::task::block_in_place(|| {});
            store.set_project_hire_cap(&request.project_id, request.cap)?;
            encode_result(json!({"project_id":request.project_id,"cap":request.cap}))
        });
        timeout_at(deadline, sql)
            .await
            .map_err(|_| unavailable())?
            .map_err(|error| HandlerError::new("storage_error", error.to_string()))?
    }
}

#[cfg(test)]
#[path = "permissions_tests.rs"]
mod tests;

//! Memory policy reads are open; accepted policy changes use the existing
//! operator-confirmation and shared-write coordinator.
use super::{encode_result, invalid_params, HandlerError, ProjectsHandler};
use serde::Deserialize;
use serde_json::Value;

pub(super) const READS: &[&str] = &["context.project", "context.session", "workspace.document"];
pub(super) const WRITES: &[&str] = &["project.set_domain", "workspace.set_document"];
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProjectRequest {
    folder: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRequest {
    agent_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DocumentRequest {
    workspace_id: String,
    revision: Option<i64>,
}
impl ProjectsHandler {
    pub(super) fn memory_read(&self, op: &str, params: Value) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        match op {
            "context.project" => {
                let req: ProjectRequest = serde_json::from_value(params).map_err(invalid_params)?;
                encode_result(store.context_project(&req.folder, &self.incarnation)?)
            }
            "context.session" => {
                let req: SessionRequest = serde_json::from_value(params).map_err(invalid_params)?;
                encode_result(store.context_session(&req.agent_id, &self.incarnation)?)
            }
            "workspace.document" => {
                let req: DocumentRequest =
                    serde_json::from_value(params).map_err(invalid_params)?;
                encode_result(store.workspace_document(&req.workspace_id, req.revision)?)
            }
            _ => unreachable!(),
        }
    }
    pub(super) fn memory_write(
        &self,
        op: &str,
        params: Value,
        principal: &str,
    ) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        let writer = store
            .with_principal(principal)
            .with_confirmation(super::shared_write::placement_confirmation(principal, None));
        let result = match op {
            "project.set_domain" => writer.set_project_domain_at(
                serde_json::from_value(params).map_err(invalid_params)?,
                (self.clock)(),
            ),
            "workspace.set_document" => writer.set_workspace_document_at(
                serde_json::from_value(params).map_err(invalid_params)?,
                (self.clock)(),
            ),
            _ => unreachable!(),
        };
        self.record_mutation(result.map_err(Into::into))
    }
}

#[cfg(test)]
#[path = "memory_context_tests.rs"]
mod tests;

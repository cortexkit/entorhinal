//! Operator-accepted memory policy is opaque shared text, not executable configuration.
use crate::{
    agent::{AgentPrecheck, PlacementConfirmation},
    mutations::{append, domain, Action},
    JournalWriter, RegistryError, RegistryStore,
};
use entorhinal_catalog::{names, sha256_hex};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const V13_MEMORY_CONTEXT: &str = r#"
ALTER TABLE project ADD COLUMN domain TEXT NULL;
CREATE TABLE workspace_document (
 workspace_id TEXT NOT NULL REFERENCES workspace(workspace_id) ON DELETE CASCADE,
 revision INTEGER NOT NULL CHECK(revision > 0),
 charter TEXT NOT NULL, document TEXT NOT NULL, sha256 TEXT NOT NULL,
 accepted_at_ms INTEGER NOT NULL, accepted_via TEXT NOT NULL,
 PRIMARY KEY(workspace_id,revision)
);
"#;
pub const CHARTER_LIMIT: usize = 4 * 1024;
pub const DOCUMENT_LIMIT: usize = 64 * 1024;
pub const SHARED_ENTRY_LIMIT: usize = 256 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SetProjectDomainRequest {
    pub project_id: String,
    #[serde(deserialize_with = "required_domain")]
    pub domain: Option<String>,
    pub request_key: String,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct SetWorkspaceDocumentRequest {
    pub workspace_id: String,
    pub expected_revision: i64,
    pub charter: String,
    pub document: String,
    pub request_key: String,
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WorkspaceDocument {
    pub workspace_id: String,
    pub revision: i64,
    pub charter: String,
    pub document: String,
    pub sha256: String,
    pub accepted_at_ms: i64,
    pub accepted_via: String,
}

fn required_domain<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    Option::<String>::deserialize(de)
}

pub(crate) fn validate_domain(value: Option<&str>) -> Result<(), RegistryError> {
    if value.is_some_and(|value| !names::map_name(value).is_ok_and(|name| name.canonical == value))
    {
        return Err(domain(
            "invalid_domain",
            "domain must be a canonical catalog package name (@scope/name)",
        ));
    }
    Ok(())
}
fn key(value: &str) -> Result<(), RegistryError> {
    if value.is_empty() {
        return Err(domain(
            "request_key_required",
            "request_key must not be empty",
        ));
    }
    Ok(())
}
pub(crate) fn validate_text(field: &str, text: &str, limit: usize) -> Result<(), RegistryError> {
    if text.len() > limit {
        return Err(RegistryError::Domain {
            code: "document_too_large".into(),
            message: format!("{field} exceeds {limit} UTF-8 bytes"),
            detail: Some(json!({"field":field,"limit":limit})),
        });
    }
    if let Some((offset, _)) = text
        .char_indices()
        .find(|(_, c)| c.is_control() && !matches!(c, '\t' | '\n' | '\r'))
    {
        return Err(RegistryError::Domain {
            code: "document_invalid_text".into(),
            message: format!("{field} contains a control character at byte {offset}"),
            detail: Some(json!({"field":field,"byte_offset":offset})),
        });
    }
    Ok(())
}
/// SHA-256 of `ck-workspace-document-v1\0`, followed by each UTF-8 field's
/// unsigned 64-bit big-endian byte length and bytes, charter before document.
/// No newline or Unicode normalization is performed.
pub fn document_sha256(charter: &str, document: &str) -> String {
    let mut bytes = b"ck-workspace-document-v1\0".to_vec();
    for text in [charter, document] {
        bytes.extend_from_slice(&(text.len() as u64).to_be_bytes());
        bytes.extend_from_slice(text.as_bytes());
    }
    sha256_hex(bytes)
}
pub(crate) fn load_document(
    conn: &Connection,
    workspace: &str,
    revision: Option<i64>,
) -> rusqlite::Result<Option<WorkspaceDocument>> {
    conn.query_row("SELECT workspace_id,revision,charter,document,sha256,accepted_at_ms,accepted_via FROM workspace_document WHERE workspace_id=?1 AND (?2 IS NULL OR revision=?2) ORDER BY revision DESC LIMIT 1", params![workspace, revision], |r| Ok(WorkspaceDocument {workspace_id:r.get(0)?,revision:r.get(1)?,charter:r.get(2)?,document:r.get(3)?,sha256:r.get(4)?,accepted_at_ms:r.get(5)?,accepted_via:r.get(6)?})).optional()
}
fn summary(text: String) -> Result<String, RegistryError> {
    if text.chars().count() > crate::agent::OPERATOR_SUMMARY_LIMIT {
        return Err(domain(
            "operator_summary_too_long",
            "shorten the project or workspace name to fit the operator prompt",
        ));
    }
    Ok(text)
}
pub(crate) fn domain_summary(name: &str, value: Option<&str>) -> Result<String, RegistryError> {
    summary(format!(
        "set domain of project {name} to {}",
        value.unwrap_or("unset")
    ))
}
fn plan_domain(conn: &Connection, req: &SetProjectDomainRequest) -> Result<String, RegistryError> {
    key(&req.request_key)?;
    validate_domain(req.domain.as_deref())?;
    let name: String = conn
        .query_row(
            "SELECT name FROM project WHERE project_id=?1",
            [&req.project_id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| domain("project_unknown", &req.project_id))?;
    domain_summary(&name, req.domain.as_deref())
}
fn document_entry(doc: &WorkspaceDocument) -> crate::shared_entry::ProjectEntry {
    crate::shared_entry::ProjectEntry {
        op: "project.shared".into(),
        tables: [(
            "workspace_document".into(),
            crate::shared_entry::TableChanges {
                upsert: vec![serde_json::from_value(json!(doc)).expect("document is an object")],
                delete: vec![],
            },
        )]
        .into(),
        agents: vec![],
        member_name: None,
        requesting_agent_id: None,
        authorization: None,
    }
}
fn plan_document(
    conn: &Connection,
    req: &SetWorkspaceDocumentRequest,
    now: i64,
) -> Result<(WorkspaceDocument, String), RegistryError> {
    key(&req.request_key)?;
    validate_text("charter", &req.charter, CHARTER_LIMIT)?;
    validate_text("document", &req.document, DOCUMENT_LIMIT)?;
    let name: String = conn
        .query_row(
            "SELECT name FROM workspace WHERE workspace_id=?1",
            [&req.workspace_id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| domain("workspace_unknown", &req.workspace_id))?;
    let current = load_document(conn, &req.workspace_id, None)?.map_or(0, |d| d.revision);
    if req.expected_revision != current {
        return Err(RegistryError::Domain {
            code: "revision_conflict".into(),
            message: format!("current revision is {current}"),
            detail: Some(json!({"current_revision":current})),
        });
    }
    let revision = current
        .checked_add(1)
        .ok_or_else(|| domain("revision_conflict", "revision exhausted"))?;
    let doc = WorkspaceDocument {
        workspace_id: req.workspace_id.clone(),
        revision,
        charter: req.charter.clone(),
        document: req.document.clone(),
        sha256: document_sha256(&req.charter, &req.document),
        accepted_at_ms: now,
        accepted_via: "direct".into(),
    };
    // Match the canonical JSON bytes submitted to the shared identity log,
    // whose coordinator refuses entries over 256 KiB. Check before prompting
    // and again under the writer lock, including unusually long workspace ids.
    let encoded_bytes = crate::shared_entry::canonical_bytes(&document_entry(&doc)).len();
    if encoded_bytes > SHARED_ENTRY_LIMIT {
        return Err(RegistryError::Domain {
            code: "document_too_large".into(),
            message: format!("encoded shared entry exceeds {SHARED_ENTRY_LIMIT} bytes"),
            detail: Some(
                json!({"field":"encoded_entry","limit":SHARED_ENTRY_LIMIT,"bytes":encoded_bytes}),
            ),
        });
    }
    let text = summary(format!("set document of workspace {name} revision {revision} (charter {} bytes, document {} bytes, sha256 {})",doc.charter.len(),doc.document.len(),&doc.sha256[..12]))?;
    Ok((doc, text))
}

impl RegistryStore {
    pub fn workspace_document(
        &self,
        workspace_id: &str,
        revision: Option<i64>,
    ) -> Result<WorkspaceDocument, RegistryError> {
        self.read(|conn| {
            Ok(load_document(conn, workspace_id, revision)?.ok_or_else(|| {
                domain(
                    "revision_unknown",
                    format!("no accepted revision {revision:?} for {workspace_id}"),
                )
            }))
        })?
    }
    pub(crate) fn precheck_memory_confirmation(
        &self,
        op: &str,
        params: Value,
        now: i64,
    ) -> Result<AgentPrecheck, RegistryError> {
        match op {
            "project.set_domain" => {
                let req: SetProjectDomainRequest = serde_json::from_value(params)
                    .map_err(|e| domain("invalid_request", e.to_string()))?;
                if let Some(bytes) = self.shared_request_cache(op, Some(&req.request_key))? {
                    return Ok(AgentPrecheck::Cached(bytes));
                }
                let text = self.read(|c| Ok(plan_domain(c, &req)))??;
                Ok(AgentPrecheck::Ready(text))
            }
            "workspace.set_document" => {
                let req: SetWorkspaceDocumentRequest = serde_json::from_value(params)
                    .map_err(|e| domain("invalid_request", e.to_string()))?;
                if let Some(bytes) = self.shared_request_cache(op, Some(&req.request_key))? {
                    return Ok(AgentPrecheck::Cached(bytes));
                }
                let (_, text) = self.read(|c| Ok(plan_document(c, &req, now)))??;
                Ok(AgentPrecheck::Ready(text))
            }
            _ => unreachable!(),
        }
    }
}
impl JournalWriter<'_> {
    fn approve_memory(&self, text: &str, direct_only: bool) -> Result<(), RegistryError> {
        if direct_only && self.principal != "direct" {
            return Err(domain(
                "direct_caller_required",
                "workspace documents require the operator",
            ));
        }
        if self.principal == "direct"
            && !matches!(&self.confirmation, PlacementConfirmation::Approved(a) if a.approves(text))
        {
            return Err(domain(
                "operator_approval_stale",
                "operator approval is missing or the approved summary changed",
            ));
        }
        Ok(())
    }
    pub fn set_project_domain_at(
        &self,
        req: SetProjectDomainRequest,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        self.mutation("project.set_domain", Some(&req.request_key), |tx| {
            let text = plan_domain(tx, &req)?;
            self.approve_memory(&text, false)?;
            let seq = append(
                tx,
                "project.set_domain",
                &json!(req),
                "operator",
                Some(&req.request_key),
                now,
                self.principal,
            )?;
            tx.execute(
                "UPDATE project SET domain=?1,updated_at=?2 WHERE project_id=?3",
                params![req.domain, now, req.project_id],
            )?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!({"project_id":req.project_id,"domain":req.domain}),
                payload: json!(req),
            })
        })
    }
    pub fn set_workspace_document_at(
        &self,
        req: SetWorkspaceDocumentRequest,
        now: i64,
    ) -> Result<Vec<u8>, RegistryError> {
        if self.principal != "direct" {
            return Err(domain(
                "direct_caller_required",
                "workspace documents require the operator",
            ));
        }
        self.mutation("workspace.set_document", Some(&req.request_key), |tx| {
            let (doc, text) = plan_document(tx, &req, now)?;
            self.approve_memory(&text, true)?;
            let seq = append(
                tx,
                "workspace.set_document",
                &json!(doc),
                "operator",
                Some(&req.request_key),
                now,
                self.principal,
            )?;
            restore_document(tx, &doc)?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value: json!(doc),
                payload: json!(doc),
            })
        })
    }
}
pub(crate) fn restore_document(
    tx: &Transaction<'_>,
    doc: &WorkspaceDocument,
) -> rusqlite::Result<()> {
    tx.execute("INSERT INTO workspace_document(workspace_id,revision,charter,document,sha256,accepted_at_ms,accepted_via) VALUES(?1,?2,?3,?4,?5,?6,?7)",params![doc.workspace_id,doc.revision,doc.charter,doc.document,doc.sha256,doc.accepted_at_ms,doc.accepted_via])?;
    Ok(())
}

#[cfg(test)]
#[path = "memory_tests.rs"]
mod tests;

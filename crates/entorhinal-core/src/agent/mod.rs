//! Agent identity schema and pure validators. Identity does not include runtime
//! residence, personas, wake policies or authority grants.

mod claims;
mod feed;
mod fleet;
pub mod import;
mod journal;
mod names;
mod reads;
pub mod schema;
mod store;
mod validate;

pub(crate) use claims::load_claims;
pub use claims::AgentNameClaim;
pub use feed::{AgentChangesReply, AgentSnapshotReply};
pub use fleet::avatar_fingerprint;
pub(crate) use journal::{change_ops_sql, IDENTITY_CHANGE_OPS, IDENTITY_MARKER_OP};
pub use journal::{replay_agent_entry, AgentChangeEntry};
pub use names::*;
pub(crate) use store::load_row;
pub use store::{valid_agent_id, AgentMutationError, AgentRow, StoredAgentAvatar};
pub use validate::*;

use std::fmt;

use rusqlite::{Connection, OptionalExtension};

use crate::{mutations::domain, RegistryError};

/// A live agent's project must stay present and in the same workspace so its
/// identity and name claim keep referring to the same namespace.
pub(crate) fn ensure_project_unbound(
    conn: &Connection,
    project_id: &str,
) -> Result<(), RegistryError> {
    let bound: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent WHERE project_id=?1 AND terminal_reason IS NULL)",
        [project_id],
        |row| row.get(0),
    )?;
    if bound {
        return Err(domain(
            "bound_by_live_agent",
            format!("project {project_id} is bound by a live agent"),
        ));
    }
    Ok(())
}

/// Workspace heads bind their own workspace; other agents bind through their
/// project's current placement, not their stored workspace or historical claim.
pub(crate) fn ensure_workspace_unbound(
    conn: &Connection,
    workspace_id: &str,
) -> Result<(), RegistryError> {
    let bound: bool = conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM agent a WHERE a.terminal_reason IS NULL AND (
            (a.role='workspace_head' AND a.workspace_id=?1)
            OR EXISTS(SELECT 1 FROM project_workspace pw
                      WHERE pw.project_id=a.project_id AND pw.workspace_id=?1)))",
        [workspace_id],
        |row| row.get(0),
    )?;
    if bound {
        return Err(domain(
            "bound_by_live_agent",
            format!("workspace {workspace_id} is bound by a live agent"),
        ));
    }
    Ok(())
}

/// Insert-only placement operations can change an unplaced project, but leave
/// an existing placement alone. Refuse only the attachment that would change it.
pub(crate) fn ensure_project_attachment_allowed(
    conn: &Connection,
    project_id: &str,
) -> Result<(), RegistryError> {
    if conn
        .query_row(
            "SELECT 1 FROM project_workspace WHERE project_id=?1",
            [project_id],
            |_| Ok(()),
        )
        .optional()?
        .is_none()
    {
        ensure_project_unbound(conn, project_id)?;
    }
    Ok(())
}

/// Explain whether a name is empty, too long after normalization, or contains a
/// disallowed code point, so callers can report the specific validation failure.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:734-740.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidNameReason {
    Empty,
    TooLong,
    DisallowedCharacter { codepoint: u32 },
}

/// Distinguish label count, emptiness, scalar-length and case-folded duplicate
/// failures while keeping the shared `invalid_labels` refusal code.
/// Source: prefrontal 873870be8,
/// crates/prefrontal-core-store/src/agent_registry.rs:768-773.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InvalidAgentLabelReason {
    TooMany,
    Empty,
    TooLong,
    Duplicate,
}

/// Identity-validator subset of core's errors; request decoding failures keep
/// the module's `invalid_request` code rather than becoming identity failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AgentRegistryError {
    InvalidName { reason: InvalidNameReason },
    InvalidTag,
    InvalidLabels { reason: InvalidAgentLabelReason },
    InvalidProjectId,
    InvalidWorkspaceId,
    InvalidGithubIdentity { field: &'static str },
    InvalidRequest { message: String },
}

impl AgentRegistryError {
    /// Return the stable wire refusal code for each validation failure.
    /// Decoding and request-shape failures use `invalid_request`, separately
    /// from failures of an already decoded identity field.
    /// Source: prefrontal 873870be8,
    /// crates/prefrontal-core-store/src/agent_registry.rs:843-873;
    /// `invalid_request` is core's module decoder/shape-check code.
    pub fn code(&self) -> &'static str {
        match self {
            Self::InvalidName { .. } => "invalid_name",
            Self::InvalidTag => "invalid_tag",
            Self::InvalidLabels { .. } => "invalid_labels",
            // Clients have always received `invalid_role_shape` for an invalid
            // project or workspace id on create: core's module translates its
            // store errors before replying. Core now relays entorhinal's reply
            // unchanged, so entorhinal returns the code clients already know.
            Self::InvalidProjectId | Self::InvalidWorkspaceId => "invalid_role_shape",
            Self::InvalidGithubIdentity { .. } => "invalid_github_identity",
            Self::InvalidRequest { .. } => "invalid_request",
        }
    }

    pub(super) fn invalid_request(message: impl Into<String>) -> Self {
        Self::InvalidRequest {
            message: message.into(),
        }
    }
}

impl fmt::Display for AgentRegistryError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidName {
                reason: InvalidNameReason::DisallowedCharacter { codepoint },
            } => write!(f, "invalid_name: disallowed character U+{codepoint:04X}"),
            Self::InvalidName { reason } => write!(f, "invalid_name: {reason:?}"),
            Self::InvalidLabels { reason } => write!(f, "invalid_labels: {reason:?}"),
            Self::InvalidGithubIdentity { field } => {
                write!(f, "invalid_github_identity: invalid {field}")
            }
            Self::InvalidRequest { message } => write!(f, "invalid_request: {message}"),
            Self::InvalidProjectId => f.write_str("invalid project_id"),
            Self::InvalidWorkspaceId => f.write_str("invalid workspace_id"),
            _ => f.write_str(self.code()),
        }
    }
}

impl std::error::Error for AgentRegistryError {}

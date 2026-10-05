//! Agent identity schema and pure validators. Identity does not include runtime
//! residence, personas, wake policies or authority grants.

mod names;
pub mod schema;
mod validate;

pub use names::*;
pub use validate::*;

use std::fmt;

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
            Self::InvalidProjectId => "invalid_project_id",
            Self::InvalidWorkspaceId => "invalid_workspace_id",
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
            _ => f.write_str(self.code()),
        }
    }
}

impl std::error::Error for AgentRegistryError {}

//! Shared field limits for agent identities and consented tool writes.
//!
//! Identity caps retain the existing validators' byte/scalar accounting. The
//! additional tool-only caps leave room for JSON escaping and registration's
//! duplicated alias warnings without restricting management reads or stored data.

pub const MAX_NAME_SCALARS: usize = 24;
pub const MAX_TAG_BYTES: usize = 256;
pub const MAX_AGENT_LABELS: usize = 16;
pub const MAX_AGENT_LABEL_SCALARS: usize = 32;
pub const MAX_SCOPE_BYTES: usize = 512;
pub const MAX_WORKSPACE_NAME_BYTES: usize = 1_024;
pub const MAX_PROJECT_NAME_BYTES: usize = 1_024;
pub const MAX_PROJECT_ROOT_BYTES: usize = 4_096;
pub const MAX_ALIAS_OWNER_BYTES: usize = MAX_SCOPE_BYTES;
pub const MAX_CATALOG_PLAN_ID_BYTES: usize = 128;

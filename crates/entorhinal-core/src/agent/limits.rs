//! Shared field limits for agent identities and consented tool writes.
//!
//! Identity caps retain the existing validators' byte/scalar accounting. The
//! additional tool-only caps leave room for JSON escaping and registration's
//! duplicated alias warnings without restricting management reads or stored data.

// Identity caps copied from core, which must agree with them so a relayed write
// is judged the same on both sides. Names count Unicode scalars of both the
// display and folded name; tags and scope ids count bytes; labels count scalars
// and have their own list limit.
// Source: prefrontal 873870be8, crates/prefrontal-core-store/src/agent_registry.rs:25-33.
pub const MAX_NAME_SCALARS: usize = 24;
pub const MAX_TAG_BYTES: usize = 256;
pub const MAX_AGENT_LABELS: usize = 16;
pub const MAX_AGENT_LABEL_SCALARS: usize = 32;
pub const MAX_SCOPE_BYTES: usize = 512;

// Tool-only caps, entorhinal's own. They bound what a consented tool write can
// echo in its reply, so a committed write never exceeds the tool-result limit.
// Operator commands and stored data are not capped by these.
pub const MAX_WORKSPACE_NAME_BYTES: usize = 1_024;
pub const MAX_PROJECT_NAME_BYTES: usize = 1_024;
pub const MAX_PROJECT_ROOT_BYTES: usize = 4_096;
pub const MAX_ALIAS_OWNER_BYTES: usize = MAX_SCOPE_BYTES;
pub const MAX_CATALOG_PLAN_ID_BYTES: usize = 128;

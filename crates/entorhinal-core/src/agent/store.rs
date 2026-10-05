use std::fmt;

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    claims, decode_github_identity, journal, normalize_agent_name, validate_agent_avatar,
    validate_agent_labels, validate_agent_tag, validate_github_identity, validate_project_id,
    validate_workspace_id, AgentAvatar, AgentChangeEntry, AgentNameClaim, AgentRegistryError,
    GithubIdentity,
};
use crate::{JournalWriter, RegistryError, RegistryStore};

/// A refusal ready for the module's ErrorWithDetail envelope. Store errors are
/// kept distinct from unknown identities and domain refusals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentMutationError {
    pub code: String,
    pub message: String,
    pub detail: Option<Value>,
}

impl AgentMutationError {
    pub(crate) fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            detail: None,
        }
    }

    fn with_detail(mut self, detail: Value) -> Self {
        self.detail = Some(detail);
        self
    }
}

impl fmt::Display for AgentMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for AgentMutationError {}
impl From<AgentRegistryError> for AgentMutationError {
    fn from(error: AgentRegistryError) -> Self {
        Self::new(error.code(), error.to_string())
    }
}
impl From<RegistryError> for AgentMutationError {
    fn from(error: RegistryError) -> Self {
        match error {
            RegistryError::Domain { code, message } => Self::new(&code, message),
            _ => Self::new("storage_error", error.to_string()),
        }
    }
}
impl From<rusqlite::Error> for AgentMutationError {
    fn from(error: rusqlite::Error) -> Self {
        Self::new("storage_error", error.to_string())
    }
}

/// Both deployed id lengths are syntactically valid. Lookup still requires a
/// stored row, so a never-imported legacy id does not acquire an identity.
pub fn valid_agent_id(id: &str) -> bool {
    id.strip_prefix("agent_").is_some_and(|hex| {
        matches!(hex.len(), 8 | 16)
            && hex
                .bytes()
                .all(|c| c.is_ascii_digit() || (b'a'..=b'f').contains(&c))
    })
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StoredAgentAvatar {
    pub genome: String,
    #[serde(rename = "type")]
    pub avatar_type: String,
    pub version: Option<i64>,
}

/// The identity snapshot/entry row is deliberately separate from core's digest:
/// it includes durable metadata and no runtime residence or persona state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AgentRow {
    pub agent_id: String,
    pub name: String,
    pub name_version: i64,
    pub name_normalization_version: i64,
    pub tag: String,
    pub labels: Vec<String>,
    pub role: String,
    pub project_id: Option<String>,
    pub workspace_id: Option<String>,
    pub avatar: Option<StoredAgentAvatar>,
    pub github_identity: Option<GithubIdentity>,
    pub status: String,
    pub merged_into: Option<String>,
    pub supervisor_agent_id: Option<String>,
    pub request_key: Option<String>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
    pub terminal_at_ms: Option<i64>,
    pub agent_generation: i64,
}

impl AgentRow {
    /// Stored rows use `retired`, but gone replies use core's `deleted` so
    /// existing clients see the terminal-state words they already decode.
    /// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:992-1001.
    pub fn gone(&self) -> Option<Value> {
        match self.status.as_str() {
            "retired" => Some(json!({"reason":"deleted", "at":self.terminal_at_ms})),
            "merged" => Some(
                json!({"reason":"merged", "at":self.terminal_at_ms, "into_agent_id":self.merged_into}),
            ),
            _ => None,
        }
    }

    /// Keep core's digest keys `agent_id`, `name`, `name_version`, `tag`,
    /// `labels`, `role`, `created_at`, and optional `project_id`/`workspace_id`,
    /// adding the independent `agent_generation`. Leave out runtime fields
    /// `persona_ref`, `sleep`, `wake_policy_version`, `residence` and
    /// `reachability`: the identity store does not own that state.
    /// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:950-989.
    pub fn digest(&self) -> Value {
        let mut value = json!({"agent_id":self.agent_id, "name":self.name,
            "name_version":self.name_version, "tag":self.tag, "labels":self.labels,
            "role":self.role, "created_at":self.created_at_ms, "agent_generation":self.agent_generation});
        if let Some(id) = &self.project_id {
            value["project_id"] = json!(id);
        }
        if let Some(id) = &self.workspace_id {
            value["workspace_id"] = json!(id);
        }
        value
    }
}

fn decode_json<T: DeserializeOwned>(value: String) -> rusqlite::Result<T> {
    serde_json::from_str(&value).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
    })
}

pub(crate) fn load_row(conn: &Connection, id: &str) -> rusqlite::Result<Option<AgentRow>> {
    if !valid_agent_id(id) {
        return Ok(None);
    }
    conn.query_row("SELECT agent_id,name,name_version,name_normalization_version,tag,labels_json,role,project_id,workspace_id,avatar_genome,avatar_type,avatar_version,github_identity_json,terminal_reason,merged_into,supervisor_agent_id,request_key,created_at_ms,updated_at_ms,terminal_at_ms,agent_generation FROM agent WHERE agent_id=?1", [id], |r| {
        let genome: Option<String> = r.get(9)?;
        let avatar = match genome {
            Some(genome) => Some(StoredAgentAvatar { genome, avatar_type:r.get(10)?, version:r.get(11)? }),
            None => None,
        };
        Ok(AgentRow {
            agent_id:r.get(0)?, name:r.get(1)?, name_version:r.get(2)?, name_normalization_version:r.get(3)?,
            tag:r.get(4)?, labels:decode_json(r.get(5)?)?, role:r.get(6)?, project_id:r.get(7)?, workspace_id:r.get(8)?,
            avatar, github_identity:r.get::<_, Option<String>>(12)?.map(decode_json).transpose()?,
            status:r.get::<_, Option<String>>(13)?.unwrap_or_else(|| "live".into()), merged_into:r.get(14)?,
            supervisor_agent_id:r.get(15)?, request_key:r.get(16)?, created_at_ms:r.get(17)?, updated_at_ms:r.get(18)?, terminal_at_ms:r.get(19)?, agent_generation:r.get(20)?,
        })
    }).optional()
}

fn encoded<T: Serialize>(value: &T) -> rusqlite::Result<String> {
    serde_json::to_string(value)
        .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))
}

pub(crate) fn write_row(tx: &Transaction<'_>, row: &AgentRow) -> rusqlite::Result<()> {
    tx.execute("INSERT INTO agent(agent_id,name,name_version,name_normalization_version,tag,labels_json,role,project_id,workspace_id,avatar_genome,avatar_type,avatar_version,github_identity_json,terminal_reason,merged_into,supervisor_agent_id,request_key,created_at_ms,updated_at_ms,terminal_at_ms,agent_generation) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21) ON CONFLICT(agent_id) DO UPDATE SET name=excluded.name,name_version=excluded.name_version,name_normalization_version=excluded.name_normalization_version,tag=excluded.tag,labels_json=excluded.labels_json,role=excluded.role,project_id=excluded.project_id,workspace_id=excluded.workspace_id,avatar_genome=excluded.avatar_genome,avatar_type=excluded.avatar_type,avatar_version=excluded.avatar_version,github_identity_json=excluded.github_identity_json,terminal_reason=excluded.terminal_reason,merged_into=excluded.merged_into,supervisor_agent_id=excluded.supervisor_agent_id,request_key=excluded.request_key,created_at_ms=excluded.created_at_ms,updated_at_ms=excluded.updated_at_ms,terminal_at_ms=excluded.terminal_at_ms,agent_generation=excluded.agent_generation",
        params![row.agent_id,row.name,row.name_version,row.name_normalization_version,row.tag,encoded(&row.labels)?,row.role,row.project_id,row.workspace_id,
            row.avatar.as_ref().map(|a| &a.genome),row.avatar.as_ref().map(|a| &a.avatar_type),row.avatar.as_ref().and_then(|a| a.version),
            row.github_identity.as_ref().map(encoded).transpose()?,if row.status == "live" {None} else {Some(&row.status)},
            row.merged_into,row.supervisor_agent_id,row.request_key,row.created_at_ms,row.updated_at_ms,row.terminal_at_ms,row.agent_generation])?;
    Ok(())
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Role {
    Assistant,
    WorkspaceHead,
    Head,
    Hiree,
}
impl Role {
    fn as_str(self) -> &'static str {
        match self {
            Self::Assistant => "assistant",
            Self::WorkspaceHead => "workspace_head",
            Self::Head => "head",
            Self::Hiree => "hiree",
        }
    }
}

// Refuse unknown request fields before a cached reply can be returned, so a
// reused request key cannot smuggle in fields that core would refuse.
// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:580-628,631-646,685-693,722-732.
macro_rules! request {
    ($name:ident { $($field:ident : $ty:ty),* $(,)? }) => {
        #[derive(Debug, Deserialize)]
        #[serde(deny_unknown_fields)]
        struct $name { $($field:$ty,)* request_key:Option<String>, actor:Option<String> }
    };
}
request!(Create { role:Role, name:String, tag:String, project_id:Option<String>, workspace_id:Option<String>, supervisor_agent_id:Option<String> });
request!(Rename {
    agent_id: String,
    name: String
});
request!(Tag {
    agent_id: String,
    tag: String
});
request!(Labels { agent:Option<String>, agent_id:Option<String>, labels:Vec<String> });
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Github {
    agent_id: String,
    // Value normally treats a missing field as null. Requiring deserialization
    // distinguishes an explicit clear from a malformed request with no edit.
    #[serde(deserialize_with = "required_value")]
    github_identity: Value,
    request_key: Option<String>,
    actor: Option<String>,
}
fn required_value<'de, D: serde::Deserializer<'de>>(deserializer: D) -> Result<Value, D::Error> {
    Value::deserialize(deserializer)
}
request!(Dispose { agent_id: String });
request!(Merge {
    agent_id: String,
    into_agent_id: String
});

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct Avatar {
    #[serde(rename = "agentId")]
    agent_id: String,
    genome: String,
    #[serde(rename = "type")]
    avatar_type: Option<String>,
    version: Option<i64>,
    #[serde(rename = "seedOnly", default)]
    seed_only: bool,
    request_key: Option<String>,
    actor: Option<String>,
}

enum Mutation {
    Create(Create),
    Rename(Rename),
    Tag(Tag),
    Labels(Labels),
    Avatar(Avatar),
    Github(Github, Option<GithubIdentity>),
    Dispose(Dispose),
    Merge(Merge),
}

fn decode<T: DeserializeOwned>(params: Value) -> Result<T, AgentMutationError> {
    serde_json::from_value(params)
        .map_err(|error| AgentMutationError::new("invalid_request", error.to_string()))
}

impl Mutation {
    fn decode(op: &str, params: Value) -> Result<Self, AgentMutationError> {
        Ok(match op {
            "agent.create" => Self::Create(decode(params)?),
            "agent.rename" => Self::Rename(decode(params)?),
            "agent.update_tag" => Self::Tag(decode(params)?),
            "agent.set_labels" => {
                let request: Labels = decode(params)?;
                match (request.agent.as_deref(), request.agent_id.as_deref()) {
                    (Some(id), None) | (None, Some(id)) if !id.is_empty() => (),
                    _ => {
                        return Err(AgentMutationError::new(
                            "invalid_request",
                            "agent.set_labels requires exactly one non-empty agent or agent_id",
                        ))
                    }
                }
                Self::Labels(request)
            }
            "agent.set_avatar" => Self::Avatar(decode(params)?),
            "agent.set_github_identity" => {
                let request: Github = decode(params)?;
                let identity = decode_github_identity(request.github_identity.clone())?;
                Self::Github(request, identity)
            }
            "agent.dispose" => Self::Dispose(decode(params)?),
            "agent.merge" => Self::Merge(decode(params)?),
            _ => return Err(AgentMutationError::new("unknown_method", op)),
        })
    }

    fn meta(&self) -> (&Option<String>, &Option<String>) {
        match self {
            Self::Create(r) => (&r.request_key, &r.actor),
            Self::Rename(r) => (&r.request_key, &r.actor),
            Self::Tag(r) => (&r.request_key, &r.actor),
            Self::Labels(r) => (&r.request_key, &r.actor),
            Self::Avatar(r) => (&r.request_key, &r.actor),
            Self::Github(r, _) => (&r.request_key, &r.actor),
            Self::Dispose(r) => (&r.request_key, &r.actor),
            Self::Merge(r) => (&r.request_key, &r.actor),
        }
    }
    fn target(&self) -> Option<&str> {
        match self {
            Self::Create(_) => None,
            Self::Rename(r) => Some(&r.agent_id),
            Self::Tag(r) => Some(&r.agent_id),
            Self::Labels(r) => r.agent.as_deref().or(r.agent_id.as_deref()),
            Self::Avatar(r) => Some(&r.agent_id),
            Self::Github(r, _) => Some(&r.agent_id),
            Self::Dispose(r) => Some(&r.agent_id),
            Self::Merge(r) => Some(&r.agent_id),
        }
    }
}

fn placement(conn: &Connection, project: &str) -> Result<String, AgentMutationError> {
    conn.query_row("SELECT w.workspace_id FROM project_workspace pw JOIN workspace w ON w.workspace_id=pw.workspace_id WHERE pw.project_id=?1", [project], |r| r.get::<_, String>(0))
        .optional().map_err(|error| AgentMutationError::new("activation_gap", error.to_string()))?
        .ok_or_else(|| AgentMutationError::new("unresolved_workspace", "project has no resolved workspace"))
}

fn project(conn: &Connection, id: &str) -> Result<String, AgentMutationError> {
    // A current project wins before the registry's single alias hop. The alias
    // target must itself be current before an agent may bind to it.
    if conn
        .query_row(
            "SELECT 1 FROM project WHERE project_id=?1",
            [id],
            |_| Ok(()),
        )
        .optional()?
        .is_some()
    {
        return Ok(id.into());
    }
    let resolved = conn
        .query_row(
            "SELECT project_id FROM project_alias WHERE old_id=?1",
            [id],
            |r| r.get::<_, String>(0),
        )
        .optional()?
        .unwrap_or_else(|| id.into());
    if conn
        .query_row(
            "SELECT 1 FROM project WHERE project_id=?1",
            [&resolved],
            |_| Ok(()),
        )
        .optional()?
        .is_none()
    {
        return Err(AgentMutationError::new("not_found", id));
    }
    Ok(resolved)
}

fn head_conflict(
    conn: &Connection,
    project: &str,
) -> Result<Option<AgentMutationError>, AgentMutationError> {
    let head = conn.query_row("SELECT agent_id,request_key FROM agent WHERE project_id=?1 AND role='head' AND terminal_reason IS NULL", [project], |r| Ok((r.get::<_, String>(0)?, r.get::<_, Option<String>>(1)?))).optional()?;
    Ok(head.map(|(id, key)| {
        AgentMutationError::new("agent_project_taken", "project already has a live head")
            .with_detail(json!({"agent_id":id, "request_key":key}))
    }))
}

fn create(
    tx: &Transaction<'_>,
    store: &RegistryStore,
    r: &Create,
    now: i64,
) -> Result<(AgentRow, Vec<AgentNameClaim>), AgentMutationError> {
    let mut project_id = r
        .project_id
        .as_deref()
        .map(validate_project_id)
        .transpose()?;
    let mut workspace_id = r
        .workspace_id
        .as_deref()
        .map(validate_workspace_id)
        .transpose()?;
    let shape = match r.role {
        Role::Assistant => project_id.is_none() && workspace_id.is_none(),
        Role::WorkspaceHead => project_id.is_none() && workspace_id.is_some(),
        Role::Head | Role::Hiree => project_id.is_some() && workspace_id.is_none(),
    };
    if !shape {
        return Err(AgentMutationError::new(
            "invalid_role_shape",
            "role does not match project_id and workspace_id",
        ));
    }
    let name = normalize_agent_name(&r.name)?;
    let tag = validate_agent_tag(&r.tag)?;
    if let Some(id) = &project_id {
        project_id = Some(project(tx, id)?);
    }
    if matches!(r.role, Role::WorkspaceHead)
        && tx
            .query_row(
                "SELECT 1 FROM workspace WHERE workspace_id=?1",
                [&workspace_id],
                |_| Ok(()),
            )
            .optional()?
            .is_none()
    {
        return Err(AgentMutationError::new(
            "not_found",
            workspace_id.as_deref().unwrap(),
        ));
    }
    if let Some(id) = &project_id {
        workspace_id = Some(placement(tx, id)?);
    }
    if let Some(id) = &r.supervisor_agent_id {
        let supervisor = if valid_agent_id(id) {
            load_row(tx, id)?
        } else {
            None
        };
        if !matches!(r.role, Role::Hiree)
            || !supervisor.is_some_and(|s| {
                s.role == "head" && s.status == "live" && s.project_id == project_id
            })
        {
            return Err(AgentMutationError::new(
                "invalid_supervisor",
                "supervisor must be this hiree's live project head",
            ));
        }
    }
    let (kind, key) = match r.role {
        Role::Assistant => ("assistant", "global"),
        _ => ("workspace", workspace_id.as_deref().unwrap()),
    };
    claims::check_name(tx, kind, key, &name, None)?;
    // The unique index on a project's live head is the last guard against
    // creating two heads. Its violation becomes `agent_project_taken`, naming
    // the existing head and its request key, which may be NULL for an imported head.
    let agent_id = loop {
        let id = format!("agent_{}", store.ids.hex(8)?);
        if load_row(tx, &id)?.is_none() {
            break id;
        }
    };
    let row = AgentRow {
        agent_id,
        name: name.stored_name.clone(),
        name_version: 1,
        name_normalization_version: 1,
        tag,
        labels: vec![],
        role: r.role.as_str().into(),
        project_id: project_id.clone(),
        workspace_id: workspace_id.clone(),
        avatar: None,
        github_identity: None,
        status: "live".into(),
        merged_into: None,
        supervisor_agent_id: r.supervisor_agent_id.clone(),
        request_key: r.request_key.clone(),
        created_at_ms: now,
        updated_at_ms: now,
        terminal_at_ms: None,
        agent_generation: 1,
    };
    if let Err(error) = write_row(tx, &row) {
        if error
            .to_string()
            .contains("UNIQUE constraint failed: agent.project_id")
        {
            if let Some(conflict) = head_conflict(tx, project_id.as_deref().unwrap())? {
                return Err(conflict);
            }
        }
        return Err(error.into());
    }
    let claim = claims::claim(tx, &row.agent_id, kind, key, &name, now)?;
    Ok((row, vec![claim]))
}

impl RegistryStore {
    /// Run an identity mutation after the module has admitted its route. Store
    /// the supplied timestamp; the serving layer attaches the process
    /// `incarnation` to replies instead of persisting it in the cache. Call
    /// through `with_principal` to record the route's verified caller on the
    /// journal row.
    pub fn agent_mutation(
        &self,
        op: &str,
        params: Value,
        now: i64,
    ) -> Result<Vec<u8>, AgentMutationError> {
        self.with_principal("entorhinal")
            .agent_mutation(op, params, now)
    }

    pub fn agent_row(&self, id: &str) -> Result<Option<AgentRow>, RegistryError> {
        self.read(|conn| load_row(conn, id))
    }

    pub fn agent_claims(&self, id: &str) -> Result<Vec<AgentNameClaim>, RegistryError> {
        self.read(|conn| claims::load_claims(conn, id))
    }

    /// Serve stored GitHub identity only for live agents. Read the journal's
    /// current generation in the same snapshot as the agent's generation, so
    /// the identity and both generation values describe the same committed
    /// state even when another request is writing concurrently.
    /// Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:3342-3354.
    pub fn agent_github_identity(&self, id: &str) -> Result<Vec<u8>, AgentMutationError> {
        let (row, generation) =
            self.read(|conn| Ok((load_row(conn, id)?, self.generation_from_connection(conn)?)))?;
        let row = row.ok_or_else(|| {
            AgentMutationError::new("unknown_agent", format!("unknown agent {id}"))
        })?;
        if let Some(gone) = row.gone() {
            return Err(AgentMutationError::new("gone", "agent is gone").with_detail(gone));
        }
        let mut value = json!({"github_identity":row.github_identity,"agent_generation":row.agent_generation,"generation":generation});
        if let Some(project_id) = row.project_id {
            value["projectId"] = json!(project_id);
        }
        crate::mutations::wire_value(value).map_err(Into::into)
    }
}

impl JournalWriter<'_> {
    pub fn agent_mutation(
        &self,
        op: &str,
        params: Value,
        now: i64,
    ) -> Result<Vec<u8>, AgentMutationError> {
        let request = Mutation::decode(op, params)?;
        let (key, actor) = request.meta();
        let key = key
            .as_deref()
            .filter(|key| !key.is_empty())
            .ok_or_else(|| {
                AgentMutationError::new("request_key_required", "request_key is required")
            })?;
        self.mutation_with_error(op, Some(key), |tx| {
            if tx.query_row(
                "SELECT 1 FROM registry_journal WHERE op='agent.cutover' LIMIT 1",
                [],
                |_| Ok(()),
            ).optional()?.is_none() {
                return Err(AgentMutationError::new(
                    "authority_not_cut_over",
                    "agent identity authority is not cut over",
                ));
            }
            let (row, claims, value, old_name) = match &request {
                Mutation::Create(r) => {
                    let (row, claims) = create(tx, self, r, now)?;
                    let value = json!({"agent": row.digest()});
                    (row, claims, value, None)
                }
                _ => {
                    let id = request.target().unwrap();
                    let mut row = load_row(tx, id)?.ok_or_else(|| {
                        AgentMutationError::new("unknown_agent", format!("unknown agent {id}"))
                    })?;
                    let previous = row.clone();
                    let idempotent = matches!(&request, Mutation::Dispose(_) if row.status == "retired")
                        || matches!(&request, Mutation::Merge(r) if row.status == "merged"
                            && row.merged_into.as_deref() == Some(r.into_agent_id.as_str()));
                    if row.status != "live" && !idempotent {
                        return Err(AgentMutationError::new("gone", "agent is gone")
                            .with_detail(row.gone().unwrap()));
                    }
                    let mut claims = vec![];
                    let mut old_name = None;
                    let mut applied = true;
                    if !idempotent {
                        match &request {
                            Mutation::Rename(r) => {
                                if matches!(row.role.as_str(), "head" | "hiree") {
                                    placement(tx, row.project_id.as_deref().unwrap())?;
                                }
                                let name = normalize_agent_name(&r.name)?;
                                // A rename keeps the namespace of the agent's active
                                // claim. Current project placement is only checked,
                                // not used to choose the namespace, because an imported
                                // claim may name a workspace that no longer exists.
                                // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_claims.rs:248-266.
                                let active = claims::active_claim(tx, id)?;
                                claims::check_name(tx, &active.namespace_kind, &active.namespace_key, &name, Some(id))?;
                                claims.push(claims::release(tx, id, now)?);
                                claims.push(claims::claim(tx, id, &active.namespace_kind, &active.namespace_key, &name, now)?);
                                old_name = Some(row.name.clone());
                                row.name = name.stored_name;
                                row.name_normalization_version = name.normalization_version;
                                row.name_version = row.name_version.checked_add(1).ok_or_else(|| {
                                    AgentMutationError::new("storage_error", "name version overflow")
                                })?;
                            }
                            Mutation::Tag(r) => row.tag = validate_agent_tag(&r.tag)?,
                            Mutation::Labels(r) => row.labels = validate_agent_labels(&r.labels)?,
                            Mutation::Avatar(r) => {
                                validate_agent_avatar(&r.genome, r.avatar_type.as_deref())?;
                                applied = !r.seed_only || row.avatar.is_none();
                                if applied {
                                    row.avatar = Some(StoredAgentAvatar {
                                        genome: r.genome.clone(),
                                        avatar_type: r.avatar_type.clone().unwrap(),
                                        version: r.version,
                                    });
                                }
                            }
                            Mutation::Github(_, identity) => {
                                if let Some(identity) = identity {
                                    validate_github_identity(identity)?;
                                }
                                row.github_identity = identity.clone();
                            }
                            Mutation::Dispose(_) | Mutation::Merge(_) => {
                                if let Mutation::Merge(r) = &request {
                                    if id == r.into_agent_id {
                                        return Err(AgentMutationError::new("merge_self", "an agent cannot merge into itself"));
                                    }
                                    if !load_row(tx, &r.into_agent_id)?.is_some_and(|target| target.status == "live") {
                                        return Err(AgentMutationError::new("merge_target_not_live", "merge target is not live"));
                                    }
                                    row.status = "merged".into();
                                    row.merged_into = Some(r.into_agent_id.clone());
                                } else {
                                    row.status = "retired".into();
                                }
                                row.terminal_at_ms = Some(now);
                                claims.push(claims::release(tx, id, now)?);
                            }
                            Mutation::Create(_) => unreachable!(),
                        }
                    }
                    // A no-change success is still journaled, but must leave the
                    // identity's own generation and modification timestamp alone.
                    if row != previous {
                        row.agent_generation = previous.agent_generation.checked_add(1).ok_or_else(|| {
                            AgentMutationError::new("storage_error", "agent generation overflow")
                        })?;
                        row.updated_at_ms = now;
                        write_row(tx, &row)?;
                    }
                    let value = match &request {
                        Mutation::Avatar(_) => json!({"avatar": row.avatar.as_ref().map(|a| AgentAvatar {
                            genome: a.genome.clone(), avatar_type: Some(a.avatar_type.clone()), version: a.version,
                        }), "applied": applied}),
                        Mutation::Github(_, _) => json!({"github_identity": row.github_identity}),
                        Mutation::Dispose(_) | Mutation::Merge(_) => json!({"gone": row.gone()}),
                        _ => json!({"agent": row.digest()}),
                    };
                    (row, claims, value, old_name)
                }
            };
            let mut entry = AgentChangeEntry::new(op, row, claims);
            if old_name.is_some() {
                entry.old_display_name = old_name;
                entry.new_display_name = Some(entry.row.name.clone());
            }
            journal::record(tx, entry, value, actor.as_deref().unwrap_or("module"), key, now, self.principal)
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mutations::tests::{result, Fixture};
    use crate::{RegisterRequest, RemoveRequest};

    fn fixture(label: &str) -> Fixture {
        let mut f = Fixture::new(label);
        f.store.use_sequential_ids();
        f.store
            .apply_entry("agent.cutover", "{}", "module", None, |_| Ok(()))
            .unwrap();
        f
    }

    fn call(f: &Fixture, op: &str, params: Value, now: i64) -> Value {
        let reply = result(
            &f.store
                .with_principal("reserved:prefrontal-core")
                .agent_mutation(op, params, now)
                .unwrap(),
        );
        assert!(reply.get("noop").is_none());
        assert!(reply["generation"].is_i64());
        reply
    }

    fn assistant(f: &Fixture, name: &str, key: &str) -> String {
        call(
            f,
            "agent.create",
            json!({"role":"assistant", "name":name,"tag":"assistant","request_key":key}),
            10,
        )["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .into()
    }

    fn row(f: &Fixture, id: &str) -> AgentRow {
        f.store.agent_row(id).unwrap().unwrap()
    }

    fn projection(f: &Fixture) -> Value {
        f.store
            .read(|conn| {
                let ids = conn
                    .prepare("SELECT agent_id FROM agent ORDER BY agent_id")?
                    .query_map([], |r| r.get::<_, String>(0))?
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let rows = ids
                    .iter()
                    .map(|id| load_row(conn, id).map(Option::unwrap))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                let claims = ids
                    .iter()
                    .map(|id| claims::load_claims(conn, id))
                    .collect::<rusqlite::Result<Vec<_>>>()?;
                Ok(json!({"rows":rows,"claims":claims}))
            })
            .unwrap()
    }

    fn refuse(f: &Fixture, op: &str, params: Value, expected: &str) -> AgentMutationError {
        let before = projection(f);
        let head = f.store.generation().unwrap();
        let error = f.store.agent_mutation(op, params, 99).unwrap_err();
        assert_eq!(error.code, expected, "{error}");
        assert_eq!(projection(f), before, "refusal changed rows or claims");
        assert_eq!(
            f.store.generation().unwrap(),
            head,
            "refusal appended a journal row"
        );
        error
    }

    fn placed(f: &Fixture, project: &str, workspace: &str) {
        f.store
            .register(RegisterRequest {
                project_id: Some(project.into()),
                name: project.into(),
                roots: vec![f.dir(project)],
                workspace_id: Some(workspace.into()),
                ..Default::default()
            })
            .unwrap();
    }

    fn entries(f: &Fixture) -> Vec<AgentChangeEntry> {
        f.store.read(|conn| conn.prepare("SELECT payload_json FROM registry_journal WHERE op!='agent.cutover' AND op LIKE 'agent.%' ORDER BY seq")?
            .query_map([],|r| {
                let value:Value = decode_json(r.get(0)?)?;
                serde_json::from_value(value["entry"].clone()).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))
            })?.collect()).unwrap()
    }

    #[test]
    fn creates_mint_from_store_source_and_claim_role_namespaces() {
        let f = fixture("agent-roles");
        placed(&f, "P", "W");
        let a = assistant(&f, "Shared", "a");
        let h = call(
            &f,
            "agent.create",
            json!({"role":"head","project_id":"P","name":"Shared","tag":"head","request_key":"h"}),
            20,
        )["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(a, "agent_0000000000000001");
        assert_eq!(h, "agent_0000000000000002");
        assert_ne!(a, h);
        assert!(valid_agent_id(&a) && a.len() == 22);
        for (role, name, key, project_id, workspace_id) in [
            ("hiree", "Hire", "hire", Some("P"), None),
            ("workspace_head", "Team", "team", None, Some("W")),
        ] {
            let id = call(&f,"agent.create",json!({"role":role,"name":name,"tag":role,"project_id":project_id,"workspace_id":workspace_id,"request_key":key}),30)["agent"]["agent_id"].as_str().unwrap().to_owned();
            assert_eq!(row(&f, &id).workspace_id.as_deref(), Some("W"));
            assert_eq!(row(&f, &id).agent_generation, 1);
            assert_eq!(row(&f, &id).name_version, 1);
            let claims = f.store.agent_claims(&id).unwrap();
            assert_eq!(
                (
                    claims[0].namespace_kind.as_str(),
                    claims[0].namespace_key.as_str()
                ),
                ("workspace", "W")
            );
        }
        let ac = f.store.agent_claims(&a).unwrap();
        assert_eq!(
            (ac[0].namespace_kind.as_str(), ac[0].namespace_key.as_str()),
            ("assistant", "global")
        );
        assert_eq!(row(&f, &h).workspace_id.as_deref(), Some("W"));
        let hc = f.store.agent_claims(&h).unwrap();
        assert_eq!(
            (hc[0].namespace_kind.as_str(), hc[0].namespace_key.as_str()),
            ("workspace", "W")
        );
        let before = projection(&f);
        let head = f.store.generation().unwrap();
        assert_eq!(f.store.rebuild().unwrap(), head);
        assert_eq!(projection(&f), before);
        assert_eq!(entries(&f).len(), 4);
    }

    // Creating each pair in one namespace must refuse the second name with
    // `name_conflict`: composed/decomposed Café, Straße/STRASSE, and Alice with
    // surrounding Unicode whitespace/alice. The expected collisions are fixed
    // inputs, not values computed by the normalizer under test.
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:7582-7616.
    #[test]
    fn nfc_case_fold_and_trim_each_create_claim_collisions() {
        for (namespace, first, contender) in [
            ("nfc", "Café", "Cafe\u{301}"),
            ("fold", "Straße", "STRASSE"),
            ("trim", "\u{3000}Alice\u{00A0}", "alice"),
        ] {
            let f = fixture(namespace);
            assistant(&f, first, "first");
            refuse(
                &f,
                "agent.create",
                json!({"role":"assistant","name":contender,"tag":"assistant","request_key":"second"}),
                "name_conflict",
            );
        }
    }

    // All-whitespace names and names containing 25 characters must be refused
    // with `invalid_name`, rather than shortened and stored as name claims.
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:7618-7634.
    #[test]
    fn invalid_names_are_typed_and_never_truncated() {
        let f = fixture("bad-names");
        for name in ["\u{3000}\u{00A0}".to_owned(), "x".repeat(25)] {
            refuse(
                &f,
                "agent.create",
                json!({"role":"assistant","name":name,"tag":"assistant","request_key":"bad"}),
                "invalid_name",
            );
        }
    }

    // Names containing U+200B (zero-width space), U+202E (right-to-left override)
    // or U+2060 (word joiner) must be refused before a claim is written, with
    // the offending codepoint in the error. The Cyrillic name Алиса is accepted
    // and stored unchanged.
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:7636-7661.
    #[test]
    fn invisible_and_bidi_characters_are_refused_at_claim_time_with_the_codepoint() {
        let f = fixture("invisible-names");
        for (name, codepoint) in [
            ("ali\u{200B}ce", 0x200B),
            ("alice\u{202E}", 0x202E),
            ("alice\u{2060}", 0x2060),
        ] {
            let error = refuse(
                &f,
                "agent.create",
                json!({"role":"assistant","name":name,"tag":"assistant","request_key":"bad"}),
                "invalid_name",
            );
            assert!(error.message.contains(&format!("U+{codepoint:04X}")));
        }
        let id = assistant(&f, "Алиса", "cyrillic");
        assert_eq!(row(&f, &id).name, "Алиса");
    }

    // Creating a name with surrounding spaces and a decomposed accent stores
    // Café, keeps the tag and empty labels, and preserves the row's name and
    // normalization versions and creation/update timestamps. Its claim keeps
    // the display name, normalized name and normalization version. Uppercase
    // agent ids remain unknown. Persona, wake, residence and
    // machine-binding assertions from core are dropped because those fields
    // describe runtime state, not agent identity.
    // Source: prefrontal 873870be8 crates/prefrontal-core-store/src/agent_registry.rs:7733-7763.
    #[test]
    fn create_and_read_agent_rows_validate_and_preserve_display_name() {
        let f = fixture("display-name");
        let id = assistant(&f, "  Cafe\u{301}  ", "create");
        let created = row(&f, &id);
        assert_eq!(created.name, "Café");
        assert_eq!(created.tag, "assistant");
        assert!(created.labels.is_empty());
        assert_eq!(created.name_normalization_version, 1);
        assert_eq!(created.name_version, 1);
        assert_eq!(created.created_at_ms, 10);
        assert_eq!(created.updated_at_ms, 10);
        assert_eq!(created.status, "live");
        let claim = &f.store.agent_claims(&id).unwrap()[0];
        assert_eq!(claim.display_name, "Café");
        assert_eq!(claim.normalized_name, "café");
        assert_eq!(claim.name_normalization_version, 1);
        assert_eq!(f.store.agent_row(&id).unwrap(), Some(created));
        assert_eq!(
            result(&f.store.agent_github_identity(&id).unwrap())["github_identity"],
            Value::Null
        );
        assert_eq!(f.store.agent_row(&id.to_uppercase()).unwrap(), None);
        assert_eq!(
            f.store
                .agent_github_identity(&id.to_uppercase())
                .unwrap_err()
                .code,
            "unknown_agent"
        );
    }

    #[test]
    fn create_check_order_and_every_refusal_leave_rows_claims_and_journal_unchanged() {
        let f = fixture("create-order");
        placed(&f, "P", "W");
        assistant(&f, "Taken", "taken");
        let base = json!({"role":"assistant","name":"Taken","tag":"ok","request_key":"bad"});
        let mut p = base.clone();
        p["project_id"] = json!("");
        p["name"] = json!("");
        refuse(&f, "agent.create", p, "invalid_project_id");
        let mut p = base.clone();
        p["workspace_id"] = json!("");
        refuse(&f, "agent.create", p, "invalid_workspace_id");
        let mut p = base.clone();
        p["project_id"] = json!("P");
        p["name"] = json!("");
        refuse(&f, "agent.create", p, "invalid_role_shape");
        let mut p = base.clone();
        p["name"] = json!("");
        p["tag"] = json!(" ");
        refuse(&f, "agent.create", p, "invalid_name");
        let mut p = base.clone();
        p["tag"] = json!(" ");
        refuse(&f, "agent.create", p, "invalid_tag");
        let mut p = base.clone();
        p["supervisor_agent_id"] = json!("bad");
        refuse(&f, "agent.create", p, "invalid_supervisor");
        refuse(&f, "agent.create", base, "name_conflict");
        for (role, project_id, workspace_id) in [
            ("assistant", Some("P"), None),
            ("assistant", None, Some("W")),
            ("workspace_head", Some("P"), Some("W")),
            ("workspace_head", None, None),
            ("head", None, None),
            ("head", Some("P"), Some("W")),
            ("hiree", None, None),
            ("hiree", Some("P"), Some("W")),
        ] {
            refuse(
                &f,
                "agent.create",
                json!({"role":role,"project_id":project_id,"workspace_id":workspace_id,"name":"Good","tag":"ok","request_key":"shape"}),
                "invalid_role_shape",
            );
        }
        for role in ["head", "hiree"] {
            refuse(
                &f,
                "agent.create",
                json!({"role":role,"project_id":"absent","name":"Good","tag":"ok","request_key":"missing"}),
                "not_found",
            );
        }
        refuse(
            &f,
            "agent.create",
            json!({"role":"workspace_head","workspace_id":"absent","name":"Good","tag":"ok","request_key":"missing"}),
            "not_found",
        );
        let id = call(
            &f,
            "agent.create",
            json!({"role":"head","project_id":"P","name":"Head","tag":"ok","request_key":"head"}),
            20,
        )["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        refuse(
            &f,
            "agent.create",
            json!({"role":"head","project_id":"P","name":"Head","tag":"ok","request_key":"bad"}),
            "name_conflict",
        );
        let error = refuse(
            &f,
            "agent.create",
            json!({"role":"head","project_id":"P","name":"Other","tag":"ok","request_key":"bad"}),
            "agent_project_taken",
        );
        assert_eq!(
            error.detail,
            Some(json!({"agent_id":id,"request_key":"head"}))
        );
    }

    #[test]
    fn alias_resolution_and_missing_or_failed_placement_precede_claiming() {
        let f = fixture("placement");
        placed(&f, "P", "W");
        f.store
            .apply_entry("fixture", "{}", "test", None, |tx| {
                tx.execute("INSERT INTO project_alias VALUES('old','P',0)", [])?;
                Ok(())
            })
            .unwrap();
        let id = call(&f,"agent.create",json!({"role":"head","project_id":" old ","name":"Head","tag":"ok","request_key":"head"}),20)["agent"]["agent_id"].as_str().unwrap().to_owned();
        assert_eq!(row(&f, &id).project_id.as_deref(), Some("P"));
        placed(&f, "Q", "V");
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute("INSERT INTO project_alias VALUES('P','Q',0)", []))
            .unwrap();
        let current = call(
            &f,
            "agent.create",
            json!({"role":"hiree","project_id":"P","name":"Hire","tag":"ok","request_key":"hire"}),
            25,
        )["agent"]["agent_id"]
            .as_str()
            .unwrap()
            .to_owned();
        assert_eq!(row(&f, &current).project_id.as_deref(), Some("P"));
        assert_eq!(row(&f, &current).workspace_id.as_deref(), Some("W"));
        f.store
            .register(RegisterRequest {
                project_id: Some("unplaced".into()),
                name: "unplaced".into(),
                roots: vec![f.dir("unplaced")],
                ..Default::default()
            })
            .unwrap();
        for role in ["head", "hiree"] {
            refuse(
                &f,
                "agent.create",
                json!({"role":role,"project_id":"unplaced","name":"Good","tag":"ok","supervisor_agent_id":"bad","request_key":"unplaced"}),
                "unresolved_workspace",
            );
        }
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute_batch("DROP TABLE project_workspace"))
            .unwrap();
        refuse(
            &f,
            "agent.create",
            json!({"role":"hiree","project_id":"P","name":"Good","tag":"ok","request_key":"gap"}),
            "activation_gap",
        );
        refuse(
            &f,
            "agent.rename",
            json!({"agent_id":id,"name":"New","request_key":"gap"}),
            "activation_gap",
        );
    }

    #[test]
    fn supervisors_are_optional_and_only_the_hirees_live_project_head_is_valid() {
        let f = fixture("supervisors");
        placed(&f, "P", "W");
        placed(&f, "Q", "V");
        let make_head = |project, name, key| {
            call(&f,"agent.create",json!({"role":"head","project_id":project,"name":name,"tag":"ok","request_key":key}),20)["agent"]["agent_id"].as_str().unwrap().to_owned()
        };
        let head = make_head("P", "Head", "head");
        let other = make_head("Q", "Other", "other");
        let a = assistant(&f, "Assistant", "assistant");
        for (role, project_id, workspace_id) in [
            ("assistant", None, None),
            ("workspace_head", None, Some("W")),
            ("head", Some("P"), None),
            ("hiree", Some("P"), None),
        ] {
            for supervisor in ["AGENT_0000000000000001", "agent_1", "0000000000000001"] {
                refuse(
                    &f,
                    "agent.create",
                    json!({"role":role,"project_id":project_id,"workspace_id":workspace_id,"name":"Good","tag":"ok","supervisor_agent_id":supervisor,"request_key":"bad"}),
                    "invalid_supervisor",
                );
            }
            if role != "hiree" {
                refuse(
                    &f,
                    "agent.create",
                    json!({"role":role,"project_id":project_id,"workspace_id":workspace_id,"name":"Good","tag":"ok","supervisor_agent_id":head,"request_key":"bad"}),
                    "invalid_supervisor",
                );
            }
        }
        for supervisor in [&other, &a, "agent_ffffffffffffffff"] {
            refuse(
                &f,
                "agent.create",
                json!({"role":"hiree","project_id":"P","name":"Good","tag":"ok","supervisor_agent_id":supervisor,"request_key":"bad"}),
                "invalid_supervisor",
            );
        }
        for (key, supervisor) in [("none", None), ("some", Some(head.as_str()))] {
            let id = call(&f,"agent.create",json!({"role":"hiree","project_id":"P","name":key,"tag":"ok","supervisor_agent_id":supervisor,"request_key":key}),30)["agent"]["agent_id"].as_str().unwrap().to_owned();
            assert_eq!(row(&f, &id).supervisor_agent_id.as_deref(), supervisor);
        }
        let id = call(&f,"agent.create",json!({"role":"hiree","project_id":"P","name":"omitted","tag":"ok","request_key":"omitted"}),30)["agent"]["agent_id"].as_str().unwrap().to_owned();
        assert_eq!(row(&f, &id).supervisor_agent_id, None);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":head,"request_key":"retire"}),
            40,
        );
        refuse(
            &f,
            "agent.create",
            json!({"role":"hiree","project_id":"P","name":"Good","tag":"ok","supervisor_agent_id":head,"request_key":"bad"}),
            "invalid_supervisor",
        );
    }

    #[test]
    fn rename_is_atomic_and_claims_in_the_existing_namespace() {
        let f = fixture("rename");
        let a = assistant(&f, "Alpha", "a");
        let b = assistant(&f, "Beta", "b");
        refuse(
            &f,
            "agent.rename",
            json!({"agent_id":a,"name":"Beta","request_key":"collision"}),
            "name_conflict",
        );
        let v = call(
            &f,
            "agent.rename",
            json!({"agent_id":a,"name":"New","request_key":"rename"}),
            50,
        );
        assert_eq!(v["agent"]["name_version"], 2);
        assert_eq!(row(&f, &a).agent_generation, 2);
        assert_eq!(row(&f, &b).agent_generation, 1);
        let claims = f.store.agent_claims(&a).unwrap();
        assert_eq!(claims.len(), 2);
        assert_eq!(claims[0].released_at_ms, Some(50));
        assert_eq!(claims[1].released_at_ms, None);
        assistant(&f, "Alpha", "reuse-old");
        let entry = entries(&f)
            .into_iter()
            .find(|e| e.op == "agent.rename")
            .unwrap();
        assert_eq!(entry.old_display_name.as_deref(), Some("Alpha"));
        assert_eq!(entry.new_display_name.as_deref(), Some("New"));
        assert_eq!(entry.claims, claims);
        let before = projection(&f);
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
    }

    fn import_fixture(f: &Fixture, mut row: AgentRow, workspace: &str) {
        row.request_key = None;
        let name = normalize_agent_name(&row.name).unwrap();
        f.store.apply_entry("agent.import", "{}", "test", None, |tx| {
            write_row(tx,&row)?;
            let claim = claims::claim(tx,&row.agent_id,"workspace",workspace,&name,10).unwrap();
            let mut entry = AgentChangeEntry::new("agent.import",row.clone(),vec![claim]);
            entry.seq = tx.query_row("SELECT MAX(seq) FROM registry_journal", [], |r| r.get(0))?;
            tx.execute("UPDATE registry_journal SET payload_json=?1 WHERE seq=(SELECT MAX(seq) FROM registry_journal)", [json!({"entry":entry}).to_string()])?;
            Ok(())
        }).unwrap();
    }

    #[test]
    fn imported_head_keeps_w1_claim_after_w1_removal_and_rebuild_and_uses_its_generation() {
        let f = fixture("imported-rename");
        placed(&f, "P", "W2");
        placed(&f, "spare", "W1");
        let template = assistant(&f, "Template", "template");
        let mut imported = row(&f, &template);
        imported.agent_id = "agent_16013c86".into();
        imported.name = "Imported".into();
        imported.role = "head".into();
        imported.project_id = Some("P".into());
        imported.workspace_id = Some("W1".into());
        imported.agent_generation = 1_000_000;
        import_fixture(&f, imported, "W1");
        f.store
            .remove(RemoveRequest {
                workspace_id: Some("W1".into()),
                ..Default::default()
            })
            .unwrap();
        let error = refuse(
            &f,
            "agent.create",
            json!({"role":"head","project_id":"P","name":"Other","tag":"ok","request_key":"second-head"}),
            "agent_project_taken",
        );
        assert_eq!(
            error.detail,
            Some(json!({"agent_id":"agent_16013c86","request_key":null}))
        );
        let rename = json!({"agent_id":"agent_16013c86","name":"Renamed","request_key":"rename"});
        call(&f, "agent.rename", rename, 60);
        let claims = f.store.agent_claims("agent_16013c86").unwrap();
        assert_eq!(claims.len(), 2);
        assert!(claims
            .iter()
            .all(|c| c.namespace_kind == "workspace" && c.namespace_key == "W1"));
        assert_eq!(claims[0].released_at_ms, Some(60));
        assert_eq!(claims[1].released_at_ms, None);
        assert_eq!(
            row(&f, "agent_16013c86").workspace_id.as_deref(),
            Some("W1")
        );
        assert_eq!(row(&f, "agent_16013c86").agent_generation, 1_000_001);
        assert_eq!(entries(&f).last().unwrap().agent_generation, 1_000_001);
        let before = projection(&f);
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
        call(
            &f,
            "agent.rename",
            json!({"agent_id":"agent_16013c86","name":"Again","request_key":"rename-again"}),
            70,
        );
        assert_eq!(row(&f, "agent_16013c86").agent_generation, 1_000_002);
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("DELETE FROM project_workspace WHERE project_id='P'", [])
            })
            .unwrap();
        refuse(
            &f,
            "agent.rename",
            json!({"agent_id":"agent_16013c86","name":"Blocked","request_key":"unplaced"}),
            "unresolved_workspace",
        );
    }

    #[test]
    fn updates_pin_validation_no_change_successes_and_name_version() {
        let f = fixture("updates");
        let id = assistant(&f, "Agent", "create");
        for (op, fields, code) in [
            ("agent.update_tag", json!({"tag":" "}), "invalid_tag"),
            (
                "agent.update_tag",
                json!({"tag":"x".repeat(257)}),
                "invalid_tag",
            ),
            (
                "agent.set_labels",
                json!({"labels":vec!["a";17]}),
                "invalid_labels",
            ),
            (
                "agent.set_labels",
                json!({"labels":["x".repeat(33)]}),
                "invalid_labels",
            ),
            (
                "agent.set_labels",
                json!({"labels":[" "]}),
                "invalid_labels",
            ),
            (
                "agent.set_labels",
                json!({"labels":["Straße","STRASSE"]}),
                "invalid_labels",
            ),
            (
                "agent.set_avatar",
                json!({"genome":"a".repeat(2047),"type":"creature.classic"}),
                "invalid_request",
            ),
            (
                "agent.set_avatar",
                json!({"genome":"a".repeat(2049),"type":"creature.classic"}),
                "invalid_request",
            ),
            (
                "agent.set_avatar",
                json!({"genome":"a".repeat(2048)}),
                "invalid_request",
            ),
            (
                "agent.set_avatar",
                json!({"genome":"a".repeat(2048),"type":"unknown"}),
                "invalid_request",
            ),
            (
                "agent.set_github_identity",
                json!({"github_identity":{"kind":"user_token","login":"","credential_ref":"ref"}}),
                "invalid_github_identity",
            ),
        ] {
            let mut params = fields;
            params[if op == "agent.set_avatar" {
                "agentId"
            } else {
                "agent_id"
            }] = json!(id);
            params["request_key"] = json!("bad");
            refuse(&f, op, params, code);
        }
        let labels = (0..16).map(|i| format!(" {i:032} ")).collect::<Vec<_>>();
        let cases = [
            (
                "agent.update_tag",
                json!({"agent_id":id,"tag":"x".repeat(256)}),
            ),
            ("agent.set_labels", json!({"agent":id,"labels":labels})),
            (
                "agent.set_avatar",
                json!({"agentId":id,"genome":format!("{}z","a".repeat(2047)),"type":"creature.classic","seedOnly":true}),
            ),
            (
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":{"kind":"user_token","login":"alice","credential_ref":"ref","coauthor_line":"line"}}),
            ),
        ];
        for (i, (op, mut params)) in cases.into_iter().enumerate() {
            let before = row(&f, &id);
            params["request_key"] = json!(format!("update-{i}"));
            let reply = call(&f, op, params.clone(), 30 + i as i64);
            let changed = row(&f, &id);
            assert_eq!(changed.agent_generation, before.agent_generation + 1);
            assert_eq!(changed.name_version, 1);
            assert_eq!(changed.updated_at_ms, 30 + i as i64);
            if op == "agent.set_avatar" {
                assert_eq!(reply["applied"], true);
                assert!(reply["avatar"].get("version").is_none());
            }
            params["request_key"] = json!(format!("same-{i}"));
            let head = f.store.generation().unwrap();
            let reply = call(&f, op, params, 80);
            assert_eq!(f.store.generation().unwrap(), head + 1);
            assert_eq!(row(&f, &id), changed);
            if op == "agent.set_avatar" {
                assert_eq!(reply["applied"], false);
            }
            assert_eq!(entries(&f).last().unwrap().row, changed);
        }
        assert_eq!(
            row(&f, &id).labels,
            (0..16).map(|i| format!("{i:032}")).collect::<Vec<_>>()
        );
        let before = projection(&f);
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
    }

    #[test]
    fn merge_and_dispose_release_claims_but_refusals_and_idempotent_calls_do_not() {
        let f = fixture("lifecycle");
        let a = assistant(&f, "Source", "a");
        let b = assistant(&f, "Target", "b");
        let c = assistant(&f, "Retire", "c");
        let d = assistant(&f, "Other target", "d");
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":c,"request_key":"retire"}),
            40,
        );
        for target in [
            &c,
            "agent_ffffffffffffffff",
            "AGENT_0000000000000001",
            "bad",
        ] {
            refuse(
                &f,
                "agent.merge",
                json!({"agent_id":a,"into_agent_id":target,"request_key":"bad"}),
                "merge_target_not_live",
            );
        }
        refuse(
            &f,
            "agent.merge",
            json!({"agent_id":a,"into_agent_id":a,"request_key":"bad"}),
            "merge_self",
        );
        assert_eq!(
            call(
                &f,
                "agent.merge",
                json!({"agent_id":a,"into_agent_id":b,"request_key":"merge"}),
                50
            )["gone"],
            json!({"reason":"merged","at":50,"into_agent_id":b})
        );
        let merged = row(&f, &a);
        let retired = row(&f, &c);
        assert_eq!(merged.agent_generation, 2);
        assert_eq!(merged.name_version, 1);
        assert_eq!(retired.agent_generation, 2);
        assert_eq!(retired.name_version, 1);
        assert_eq!(
            f.store.agent_claims(&a).unwrap()[0].released_at_ms,
            Some(50)
        );
        assert_eq!(
            f.store.agent_claims(&c).unwrap()[0].released_at_ms,
            Some(40)
        );
        let head = f.store.generation().unwrap();
        assert_eq!(
            call(
                &f,
                "agent.merge",
                json!({"agent_id":a,"into_agent_id":b,"request_key":"merge-again"}),
                60
            )["gone"],
            merged.gone().unwrap()
        );
        assert_eq!(
            call(
                &f,
                "agent.dispose",
                json!({"agent_id":c,"request_key":"retire-again"}),
                60
            )["gone"],
            json!({"reason":"deleted","at":40})
        );
        assert_eq!(f.store.generation().unwrap(), head + 2);
        assert_eq!(row(&f, &a), merged);
        assert_eq!(row(&f, &c), retired);
        assert!(entries(&f).last().unwrap().claims.is_empty());
        for (op, params) in [
            ("agent.dispose", json!({"agent_id":a})),
            ("agent.merge", json!({"agent_id":c,"into_agent_id":b})),
            ("agent.merge", json!({"agent_id":a,"into_agent_id":d})),
            ("agent.rename", json!({"agent_id":a,"name":"Target"})),
            ("agent.update_tag", json!({"agent_id":a,"tag":" "})),
        ] {
            let mut params = params;
            params["request_key"] = json!("gone");
            let err = refuse(&f, op, params, "gone");
            assert!(err.detail.is_some());
        }
        assistant(&f, "Source", "reuse-source");
        assistant(&f, "Retire", "reuse-retire");
        let before = projection(&f);
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
    }

    #[test]
    fn unknown_and_terminal_source_checks_precede_mutation_checks() {
        let f = fixture("source-order");
        let live = assistant(&f, "Taken", "taken");
        let gone = assistant(&f, "Gone", "gone");
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":gone,"request_key":"dispose"}),
            40,
        );
        for (id, code) in [
            ("bad", "unknown_agent"),
            ("agent_abcdef12", "unknown_agent"),
            ("agent_ffffffffffffffff", "unknown_agent"),
            (gone.as_str(), "gone"),
        ] {
            for (op, mut params) in [
                ("agent.rename", json!({"agent_id":id,"name":"Taken"})),
                ("agent.merge", json!({"agent_id":id,"into_agent_id":"bad"})),
                ("agent.update_tag", json!({"agent_id":id,"tag":" "})),
                ("agent.set_labels", json!({"agent_id":id,"labels":[" "]})),
                ("agent.set_avatar", json!({"agentId":id,"genome":"bad"})),
                (
                    "agent.set_github_identity",
                    json!({"agent_id":id,"github_identity":{"kind":"user_token","login":"","credential_ref":"ref"}}),
                ),
            ] {
                params["request_key"] = json!("bad");
                refuse(&f, op, params, code);
            }
        }
        refuse(
            &f,
            "agent.update_tag",
            json!({"agent_id":live.to_uppercase(),"tag":"ok","request_key":"upper"}),
            "unknown_agent",
        );
    }

    #[test]
    fn request_keys_cache_original_reply_across_restart_and_reject_cross_op_reuse() {
        let mut f = fixture("agent-cache");
        let create = json!({"role":"assistant","name":"Original","tag":"ok","request_key":"create","actor":"operator"});
        let original = call(&f, "agent.create", create, 10);
        let id = original["agent"]["agent_id"].as_str().unwrap().to_owned();
        let head = f.store.generation().unwrap();
        assert_eq!(
            call(
                &f,
                "agent.create",
                json!({"role":"assistant","name":"Changed","tag":"new","request_key":"create"}),
                20
            ),
            original
        );
        assert_eq!(f.store.generation().unwrap(), head);
        let avatar = json!({"agentId":id,"genome":"a".repeat(2048),"type":"creature.classic","request_key":"seed"});
        call(&f, "agent.set_avatar", avatar.clone(), 30);
        let mut seed = avatar.clone();
        seed["seedOnly"] = json!(true);
        seed["request_key"] = json!("skip");
        let skipped = call(&f, "agent.set_avatar", seed.clone(), 40);
        assert_eq!(skipped["applied"], false);
        let mut reroll = avatar;
        reroll["genome"] = json!("b".repeat(2048));
        reroll["request_key"] = json!("reroll");
        call(&f, "agent.set_avatar", reroll, 50);
        let db_path = f.root.join("store.db");
        let mut replacement = Fixture::new("temporary-replacement");
        std::mem::swap(&mut f.store, &mut replacement.store);
        drop(replacement);
        let descriptor = cortexkit_store::StorageDescriptor {
            module_id: "restart".into(),
            storage_namespace: "tests".into(),
            isolation: cortexkit_store::Isolation::Module,
            backend: cortexkit_store::StorageBackend::Sqlite {
                path: db_path.to_string_lossy().into_owned(),
            },
        };
        f.store = RegistryStore::open(&descriptor).unwrap();
        seed["genome"] = json!("c".repeat(2048));
        let head = f.store.generation().unwrap();
        let before = projection(&f);
        assert_eq!(call(&f, "agent.set_avatar", seed, 60), skipped);
        assert_eq!(
            call(
                &f,
                "agent.create",
                json!({"role":"assistant","name":"Another","tag":"ok","request_key":"create"}),
                60
            ),
            original
        );
        assert_eq!(f.store.generation().unwrap(), head);
        assert_eq!(projection(&f), before);
        refuse(
            &f,
            "agent.update_tag",
            json!({"agent_id":id,"tag":"ok","request_key":"skip"}),
            "request_key_reused_across_ops",
        );
        refuse(
            &f,
            "agent.create",
            json!({"role":"assistant","name":"Good","tag":"ok","request_key":"create","persona_ref":"removed"}),
            "invalid_request",
        );
        let attribution = f
            .store
            .read(|conn| {
                conn.query_row(
                    "SELECT principal,actor FROM registry_journal WHERE request_key='create'",
                    [],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
            })
            .unwrap();
        assert_eq!(
            attribution,
            ("reserved:prefrontal-core".into(), "operator".into())
        );
        assert_eq!(
            f.store
                .read(|conn| conn.query_row(
                    "SELECT actor FROM registry_journal WHERE request_key='skip'",
                    [],
                    |r| r.get::<_, String>(0)
                ))
                .unwrap(),
            "module"
        );
    }

    #[test]
    fn decode_and_key_checks_precede_marker_and_cached_reply() {
        let f = Fixture::new("agent-precutover");
        refuse(
            &f,
            "agent.create",
            json!({"role":"assistant","name":"Good","tag":"ok"}),
            "request_key_required",
        );
        refuse(
            &f,
            "agent.create",
            json!({"role":"assistant","name":"Good","tag":"ok","request_key":""}),
            "request_key_required",
        );
        refuse(
            &f,
            "agent.create",
            json!({"role":"assistant","name":"Good","tag":"ok","request_key":"new"}),
            "authority_not_cut_over",
        );
        let f = fixture("closed-bodies");
        let id = assistant(&f, "Agent", "create");
        for extra in [
            "persona_ref",
            "residence",
            "wake_policy",
            "sleep",
            "callerHarness",
            "callerSession",
        ] {
            let mut params =
                json!({"role":"assistant","name":"Good","tag":"ok","request_key":"create"});
            params[extra] = Value::Null;
            refuse(&f, "agent.create", params, "invalid_request");
        }
        for fields in [
            json!({"agent":id,"agent_id":id,"labels":[]}),
            json!({"labels":[]}),
            json!({"agent":"","labels":[]}),
        ] {
            let mut params = fields;
            params["request_key"] = json!("labels");
            refuse(&f, "agent.set_labels", params, "invalid_request");
        }
        refuse(
            &f,
            "agent.set_github_identity",
            json!({"agent_id":id,"request_key":"github"}),
            "invalid_request",
        );
        for identity in [
            json!({"kind":"unknown"}),
            json!({"kind":"user_token","login":"alice","credential_ref":"ref","secret":"no"}),
        ] {
            refuse(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":identity,"request_key":"github"}),
                "invalid_request",
            );
        }
        refuse(
            &f,
            "agent.set_avatar",
            json!({"agent_id":id,"genome":"a".repeat(2048),"type":"creature.classic","request_key":"avatar"}),
            "invalid_request",
        );
    }

    #[test]
    fn transaction_faults_after_claim_and_row_writes_roll_back_all_projections() {
        let f = fixture("agent-rollback");
        let a = assistant(&f, "Alpha", "a");
        f.store.db.with_conn_fenced(|tx| tx.execute_batch("CREATE TRIGGER fail_journal BEFORE INSERT ON registry_journal WHEN NEW.op LIKE 'agent.%' BEGIN SELECT RAISE(ABORT,'journal fault'); END;")).unwrap();
        refuse(
            &f,
            "agent.rename",
            json!({"agent_id":a,"name":"New","request_key":"rename"}),
            "storage_error",
        );
        refuse(
            &f,
            "agent.dispose",
            json!({"agent_id":a,"request_key":"dispose"}),
            "storage_error",
        );
        refuse(
            &f,
            "agent.create",
            json!({"role":"assistant","name":"Other","tag":"ok","request_key":"other"}),
            "storage_error",
        );
        f.store
            .db
            .with_conn_fenced(|tx| tx.execute_batch("DROP TRIGGER fail_journal"))
            .unwrap();
        call(
            &f,
            "agent.rename",
            json!({"agent_id":a,"name":"New","request_key":"rename"}),
            60,
        );
        assert_eq!(row(&f, &a).name, "New");
    }

    #[test]
    fn github_constraints_clearing_and_equal_avatar_values_use_live_agent_generations() {
        let f = fixture("github-bounds");
        let id = assistant(&f, "Agent", "create");
        let app = json!({"kind":"app","app_id":1,"app_slug":"app","installation_id":2,"credential_ref":"ref","coauthor_line":"line"});
        for (field, value) in [
            ("app_id", json!(0)),
            ("installation_id", json!(0)),
            ("app_slug", json!("")),
            ("credential_ref", json!("")),
            ("coauthor_line", json!("")),
            ("client_id", json!("")),
        ] {
            let mut invalid = app.clone();
            invalid[field] = value;
            refuse(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":invalid,"request_key":"bad"}),
                "invalid_github_identity",
            );
        }
        let user = json!({"kind":"user_token","login":"alice","credential_ref":"ref","coauthor_line":"line"});
        for field in ["login", "credential_ref", "coauthor_line"] {
            let mut invalid = user.clone();
            invalid[field] = json!("");
            refuse(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":invalid,"request_key":"bad"}),
                "invalid_github_identity",
            );
        }
        for (i, identity) in [app, user, Value::Null].into_iter().enumerate() {
            call(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":identity,"request_key":format!("github-{i}")}),
                30 + i as i64,
            );
            let changed = row(&f, &id);
            let reply = result(&f.store.agent_github_identity(&id).unwrap());
            assert_eq!(reply["github_identity"], identity);
            assert_eq!(reply["agent_generation"], changed.agent_generation);
            assert_ne!(reply["generation"], reply["agent_generation"]);
            call(
                &f,
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":identity,"request_key":format!("same-github-{i}")}),
                80,
            );
            assert_eq!(row(&f, &id), changed);
        }
        let avatar = json!({"agentId":id,"genome":"a".repeat(2048),"type":"creature.classic","version":2,"request_key":"avatar"});
        assert_eq!(
            call(&f, "agent.set_avatar", avatar.clone(), 50)["avatar"]["version"],
            2
        );
        let before = row(&f, &id);
        let mut same = avatar;
        same["request_key"] = json!("same-avatar");
        assert_eq!(call(&f, "agent.set_avatar", same, 80)["applied"], true);
        assert_eq!(row(&f, &id), before);
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":id,"request_key":"dispose"}),
            90,
        );
        assert_eq!(
            f.store.agent_github_identity(&id).unwrap_err().detail,
            Some(json!({"reason":"deleted","at":90}))
        );
    }

    #[test]
    fn journal_entries_pin_closed_wire_keys_and_rebuild_replaces_corrupt_projections() {
        let f = fixture("entry-shape");
        let id = assistant(&f, "Agent", "create");
        let keys = |value: Value| {
            value
                .as_object()
                .unwrap()
                .keys()
                .cloned()
                .collect::<std::collections::BTreeSet<_>>()
        };
        let expected = |names: &[&str]| {
            names
                .iter()
                .map(|s| s.to_string())
                .collect::<std::collections::BTreeSet<_>>()
        };
        let entry = serde_json::to_value(entries(&f).pop().unwrap()).unwrap();
        assert_eq!(
            keys(entry.clone()),
            expected(&[
                "seq",
                "op",
                "agent_id",
                "agent_generation",
                "row",
                "claims",
                "old_display_name",
                "new_display_name",
                "status",
                "merged_into"
            ])
        );
        assert_eq!(
            keys(entry["row"].clone()),
            expected(&[
                "agent_id",
                "name",
                "name_version",
                "name_normalization_version",
                "tag",
                "labels",
                "role",
                "project_id",
                "workspace_id",
                "avatar",
                "github_identity",
                "status",
                "merged_into",
                "supervisor_agent_id",
                "request_key",
                "created_at_ms",
                "updated_at_ms",
                "terminal_at_ms",
                "agent_generation"
            ])
        );
        assert_eq!(
            keys(entry["claims"][0].clone()),
            expected(&[
                "claim_id",
                "agent_id",
                "namespace_kind",
                "namespace_key",
                "normalized_name",
                "name_normalization_version",
                "display_name",
                "claimed_at_ms",
                "released_at_ms"
            ])
        );
        assert_eq!(entry["seq"], 2);
        assert_eq!(entry["agent_generation"], 1);
        for field in [
            "old_display_name",
            "new_display_name",
            "status",
            "merged_into",
        ] {
            assert!(entry[field].is_null());
        }
        let before = projection(&f);
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("UPDATE agent SET agent_generation=700,name='Corrupt' WHERE agent_id=?1",[&id])?;
            tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_ffffffffffffffff','Stray','tag','assistant',0,0)",[])?;
            tx.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_ffffffffffffffff','assistant','global','stray','Stray',0)",[])?;
            Ok(())
        }).unwrap();
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
        assert!(f
            .store
            .agent_row("agent_ffffffffffffffff")
            .unwrap()
            .is_none());
    }

    #[test]
    fn create_never_reuses_a_tombstone_when_the_id_source_restarts() {
        let mut f = fixture("agent-tombstone");
        let retired = assistant(&f, "Agent", "create");
        call(
            &f,
            "agent.dispose",
            json!({"agent_id":retired,"request_key":"dispose"}),
            30,
        );
        f.store.use_sequential_ids();
        let live = assistant(&f, "Agent", "create-again");
        assert_ne!(retired, live);
        assert_eq!(live, "agent_0000000000000002");
        assert_eq!(row(&f, &retired).status, "retired");
    }
}

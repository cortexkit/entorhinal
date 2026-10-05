use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use cortexkit_paths::{IdentityError, ProjectRootId};
use rusqlite::{params, types::ValueRef, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{implicit_project_id, now_unix_millis, RegistryError, RegistryStore};

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RegisterRequest {
    #[serde(default)]
    pub project_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub derived_root_parents: Vec<String>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct AssignWorkspaceRequest {
    pub project_id: String,
    pub workspace_id: String,
    #[serde(default)]
    pub workspace_name: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

/// Set or clear a workspace's root directory. `root: null` clears it.
///
/// The root is operator-set data, never derived from member paths: prefrontal
/// reads `<root>/.cortexkit/WORKSPACE.md` as operator text for every member
/// project, so a guessed root would put text the operator never chose in front
/// of their agents.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SetWorkspaceRootRequest {
    pub workspace_id: String,
    pub root: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct UpgradeImplicitRequest {
    #[serde(alias = "implicitProjectId", alias = "oldId")]
    pub implicit_id: String,
    #[serde(default)]
    pub project_id: Option<String>,
    pub name: String,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub root: Option<String>,
    #[serde(default)]
    pub roots: Vec<String>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct RemoveRequest {
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub successor_project_id: Option<String>,
    #[serde(default)]
    pub workspace_id: Option<String>,
    #[serde(default)]
    pub request_key: Option<String>,
    #[serde(default)]
    pub actor: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeedImportRequest {
    pub source: String,
    #[serde(default)]
    pub request_key: Option<String>,
    pub payload: SeedPayload,
    #[serde(default)]
    pub actor: Option<String>,
    /// Seed-time guard (room ruling, #workspace-projects-design): roots that
    /// are $HOME itself or an ancestor of the XDG config/data homes are
    /// excluded from the import by default. Such bindings are observation
    /// debris (a session whose cwd was $HOME), and a registered $HOME root
    /// would containment-swallow every unregistered path under it. Off-switch
    /// exists for stress runs only.
    #[serde(default = "default_true")]
    pub exclude_home_scoped: bool,
}

fn default_true() -> bool {
    true
}

/// True when `root` is the user's home directory, an ancestor of it, or an
/// ancestor-or-equal of an XDG config/data home. The XDG homes are consulted
/// explicitly: when XDG_CONFIG_HOME or XDG_DATA_HOME points outside $HOME
/// (containerized setups, custom layouts), a root containing them is still
/// observation debris, not a project.
fn is_home_scoped(root: &str) -> bool {
    is_home_scoped_against(
        root,
        std::env::var("HOME").ok().as_deref(),
        std::env::var("XDG_CONFIG_HOME").ok().as_deref(),
        std::env::var("XDG_DATA_HOME").ok().as_deref(),
    )
}

/// Env-injected core of the guard, so tests exercise relocated-XDG layouts
/// without process-global env mutation (which races parallel tests).
fn is_home_scoped_against(
    root: &str,
    home: Option<&str>,
    xdg_config: Option<&str>,
    xdg_data: Option<&str>,
) -> bool {
    let root_trimmed = root.trim_end_matches('/');
    if root_trimmed.is_empty() {
        // Filesystem root contains everything, including the homes.
        return true;
    }
    let ancestor_or_equal = |target: &str| -> bool {
        let target = target.trim_end_matches('/');
        if target.is_empty() {
            return false;
        }
        root_trimmed == target || target.starts_with(&format!("{root_trimmed}/"))
    };
    [home, xdg_config, xdg_data]
        .into_iter()
        .flatten()
        .any(ancestor_or_equal)
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeedPayload {
    #[serde(default)]
    pub pairs: Vec<SeedPair>,
    #[serde(default)]
    pub workspaces: Vec<SeedWorkspace>,
    #[serde(default)]
    pub members: Vec<SeedMember>,
    #[serde(default)]
    pub names: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeedPair {
    pub canonical_root: String,
    pub mc_identity: String,
    #[serde(default)]
    pub identity_class: Option<String>,
    #[serde(default)]
    pub resolved_through_cooldown: bool,
    #[serde(default)]
    pub name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeedWorkspace {
    pub workspace_id: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct SeedMember {
    pub mc_identity: String,
    pub workspace_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct VerifyReply {
    pub ok: bool,
    pub local_members: i64,
    pub project_workspaces: i64,
    pub mismatches: Vec<String>,
    pub generation: i64,
    pub replay: ReplayReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RebuildReply {
    pub generation: i64,
    pub replay: ReplayReport,
}

/// Differences are relative to the journal: missing rows are restored by
/// replay, while unexpected live rows are removed. A changed row appears on
/// both sides, even when its primary key is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayReport {
    pub ok: bool,
    pub tables: Vec<ReplayTableDifference>,
}

impl Default for ReplayReport {
    fn default() -> Self {
        Self {
            ok: true,
            tables: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ReplayTableDifference {
    pub table: String,
    pub missing: u64,
    pub unexpected: u64,
    /// At most five primary keys across both sides, including every component
    /// of a composite key. Column names are the database's names.
    pub missing_keys: Vec<BTreeMap<String, Value>>,
    pub unexpected_keys: Vec<BTreeMap<String, Value>>,
}

#[derive(Debug)]
pub(crate) struct Action {
    pub(crate) changed: bool,
    pub(crate) seq: Option<i64>,
    pub(crate) value: Value,
    pub(crate) payload: Value,
}

pub(crate) fn domain(code: &str, message: impl Into<String>) -> RegistryError {
    RegistryError::Domain {
        code: code.to_string(),
        message: message.into(),
    }
}

fn reserved(id: &str) -> bool {
    let rest = id.strip_prefix("pj-implicit");
    rest.is_some_and(|r| {
        r.split_once('-')
            .is_some_and(|(n, _)| !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()))
    })
}

pub(crate) fn canonical(raw: &str) -> Result<String, RegistryError> {
    match ProjectRootId::from_path(raw) {
        Ok(id) => {
            let value = id.to_string();
            if value != raw {
                return Err(domain(
                    "not_canonical",
                    format!("path must be canonical: {raw}"),
                ));
            }
            Ok(value)
        }
        Err(IdentityError::NonExistentPath { .. }) => Err(domain(
            "root_not_found",
            format!("path does not exist: {raw}"),
        )),
        Err(error) => Err(domain("not_canonical", error.to_string())),
    }
}

pub(crate) fn append(
    tx: &Transaction<'_>,
    op: &str,
    payload: &Value,
    actor: &str,
    key: Option<&str>,
    now: i64,
    principal: &str,
) -> rusqlite::Result<i64> {
    tx.execute("INSERT INTO registry_journal(op,payload_json,actor,request_key,created_at,principal) VALUES(?1,?2,?3,?4,?5,?6)", params![op, serde_json::to_string(payload).unwrap(), actor, key, now, principal])?;
    Ok(tx.last_insert_rowid())
}

fn cached(tx: &Transaction<'_>, op: &str, key: Option<&str>) -> rusqlite::Result<Option<Vec<u8>>> {
    // The op is part of the lookup: a request_key reused across DIFFERENT op
    // kinds must not serve the other op's cached reply as this op's result.
    key.and_then(|key| {
        tx.query_row(
            "SELECT response_json FROM registry_journal WHERE request_key=?1 AND op=?2",
            [key, op],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .transpose()
    })
    .transpose()
    .map(|v| v.flatten().map(String::into_bytes))
}

pub(crate) fn wire_value(value: Value) -> Result<Vec<u8>, RegistryError> {
    wire(value)
}

fn wire(value: Value) -> Result<Vec<u8>, RegistryError> {
    serde_json::to_vec(&json!({"result": value}))
        .map_err(|e| domain("encode_failed", e.to_string()))
}

impl RegistryStore {
    pub fn register(&self, req: RegisterRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").register(req)
    }

    pub fn assign_workspace(&self, req: AssignWorkspaceRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").assign_workspace(req)
    }

    pub fn set_workspace_root(
        &self,
        req: SetWorkspaceRootRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").set_workspace_root(req)
    }

    pub fn upgrade_implicit(&self, req: UpgradeImplicitRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").upgrade_implicit(req)
    }

    pub fn remove(&self, req: RemoveRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").remove(req)
    }

    pub fn seed_import(&self, req: SeedImportRequest) -> Result<Vec<u8>, RegistryError> {
        self.with_principal("entorhinal").seed_import(req)
    }
}

impl super::JournalWriter<'_> {
    pub(crate) fn mutation<F>(
        &self,
        op: &str,
        key: Option<&str>,
        action: F,
    ) -> Result<Vec<u8>, RegistryError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<Action, RegistryError>,
    {
        self.mutation_with_error(op, key, action)
    }

    // Agent refusals carry structured detail while project callers keep their
    // existing error type. Both paths must share the same cache and rollback.
    pub(crate) fn mutation_with_error<F, E>(
        &self,
        op: &str,
        key: Option<&str>,
        action: F,
    ) -> Result<Vec<u8>, E>
    where
        F: FnOnce(&Transaction<'_>) -> Result<Action, E>,
        E: From<RegistryError>,
    {
        // Domain errors must ABORT the transaction, never commit around it:
        // with_conn_fenced commits on any Ok, so returning a domain error as
        // Ok(Err(_)) would commit whatever the action closure wrote before
        // failing (a journal row without its projection, or a row with NULL
        // response_json that then poisons request_key idempotency). The error
        // is stashed outside the closure and a rusqlite error is returned as
        // a rollback sentinel; the stash wins on the way out.
        let domain_error = std::cell::RefCell::new(None::<E>);
        let out = self.db.with_conn_fenced(|tx| -> rusqlite::Result<Vec<u8>> {
            let fail = |e: E| -> rusqlite::Error {
                *domain_error.borrow_mut() = Some(e);
                rusqlite::Error::QueryReturnedNoRows
            };
            if let Some(blob) = cached(tx, op, key)? {
                return Ok(blob);
            }
            // request_key is globally UNIQUE in the journal. A key that
            // exists under a DIFFERENT op is a caller bug: serving the
            // other op's reply would be silent corruption, and letting the
            // append hit the constraint would surface a raw database
            // error. Refuse with a typed domain error instead.
            if let Some(key) = key {
                if let Some(other) = tx
                    .query_row(
                        "SELECT op FROM registry_journal WHERE request_key=?1",
                        [key],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?
                {
                    if other != op {
                        return Err(fail(
                            domain(
                                "request_key_reused_across_ops",
                                format!("request_key is already bound to op '{other}'"),
                            )
                            .into(),
                        ));
                    }
                }
            }
            let action = action(tx).map_err(fail)?;
            let generation = action.seq.unwrap_or(tx.query_row(
                "SELECT COALESCE(MAX(seq),0) FROM registry_journal",
                [],
                |r| r.get(0),
            )?);
            let mut value = action.value;
            if let Value::Object(ref mut map) = value {
                map.insert("generation".to_string(), json!(generation));
                // Agent replies keep core's reply keys, which have no `noop`.
                if !op.starts_with("agent.") {
                    map.insert("noop".to_string(), json!(!action.changed));
                }
            }
            let blob = wire(value).map_err(|error| fail(error.into()))?;
            if action.changed {
                tx.execute(
                    "UPDATE registry_journal SET payload_json=?1,response_json=?2 WHERE seq=?3",
                    params![
                        serde_json::to_string(&action.payload).unwrap(),
                        String::from_utf8_lossy(&blob).to_string(),
                        action.seq.unwrap()
                    ],
                )?;
            }
            Ok(blob)
        });
        match out {
            Ok(blob) => Ok(blob),
            Err(store_error) => Err(domain_error
                .into_inner()
                .unwrap_or_else(|| RegistryError::Store(store_error).into())),
        }
    }

    pub fn register(&self, mut req: RegisterRequest) -> Result<Vec<u8>, RegistryError> {
        if let Some(id) = req.project_id.as_ref() {
            if reserved(id) {
                return Err(domain("reserved_project_id_namespace", id));
            }
        }
        req.roots = req
            .roots
            .iter()
            .map(|p| canonical(p))
            .collect::<Result<_, _>>()?;
        req.derived_root_parents = req
            .derived_root_parents
            .iter()
            .map(|p| canonical(p))
            .collect::<Result<_, _>>()?;
        req.roots.sort();
        req.roots.dedup();
        req.derived_root_parents.sort();
        req.derived_root_parents.dedup();
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("register", key.clone().as_deref(), move |tx| {
            let now = now_unix_millis();
            let mut owners = BTreeSet::new();
            for root in &req.roots { if let Some(owner) = tx.query_row("SELECT project_id FROM project_root WHERE canonical_root=?1",[root],|r|r.get::<_,String>(0)).optional()? { owners.insert(owner); } }
            for parent in &req.derived_root_parents { if let Some(owner) = tx.query_row("SELECT project_id FROM derived_root_parent WHERE canonical_parent=?1",[parent],|r|r.get::<_,String>(0)).optional()? { owners.insert(owner); } }
            let project_id = match req.project_id.clone() { Some(id) => id, None => if owners.len()==1 { owners.iter().next().unwrap().clone() } else if owners.len()>1 { return Err(domain("project_conflict", format!("conflicting owners: {}", owners.into_iter().collect::<Vec<_>>().join(",")))) } else { mint("register1", &req.name, &req.roots) } };
            let existing = tx.query_row("SELECT name FROM project WHERE project_id=?1",[&project_id],|r|r.get::<_,String>(0)).optional()?;
            if req.project_id.is_some() && existing.is_none() && tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1",[&project_id],|r|r.get::<_,String>(0)).optional()?.is_some() { return Err(domain("project_id_occupied", &project_id)); }
            for root in &req.roots {
                if let Some(owner)=tx.query_row("SELECT project_id FROM project_root WHERE canonical_root=?1",[root],|r|r.get::<_,String>(0)).optional()? { if owner!=project_id { return Err(domain("root_conflict",format!("root {root} is owned by {owner}"))); } }
                if let Some(owner)=tx.query_row("SELECT project_id FROM derived_root_parent WHERE canonical_parent=?1",[root],|r|r.get::<_,String>(0)).optional()? { if owner!=project_id { return Err(domain("root_conflict",format!("root {root} is claimed by {owner}"))); } }
            }
            for parent in &req.derived_root_parents {
                let mut stmt=tx.prepare("SELECT project_id,canonical_root FROM project_root WHERE project_id<>?1")?;
                let rows=stmt.query_map([&project_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
                for row in rows { let (owner,root)=row?; if super::path_prefix_or_equal(Path::new(parent),Path::new(&root)) { return Err(domain("derived_parent_contains_foreign_root",format!("parent {parent} conflicts with owner {owner}"))); } }
            }
            for root in &req.roots {
                let mut stmt=tx.prepare("SELECT project_id,canonical_parent FROM derived_root_parent WHERE project_id<>?1")?;
                let rows=stmt.query_map([&project_id],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?)))?;
                for row in rows { let (owner,parent)=row?; if super::path_prefix_or_equal(Path::new(&parent),Path::new(root)) { return Err(domain("root_under_foreign_derived_parent",format!("root {root} is under {parent}, owned by {owner}"))); } }
            }
            if let Some(w) = &req.workspace_id { if let Some(old)=tx.query_row("SELECT workspace_id FROM project_workspace WHERE project_id=?1",[&project_id],|r|r.get::<_,String>(0)).optional()? { if old!=*w { return Err(domain("workspace_conflict",format!("project {project_id} belongs to workspace {old}"))); } } }
            if req.workspace_id.is_some() {
                super::agent::ensure_project_attachment_allowed(tx, &project_id)?;
            }
            let mut aliases=Vec::new();
            for root in &req.roots { let old=implicit_project_id(root); if let Some(target)=tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1",[&old],|r|r.get::<_,String>(0)).optional()? { if target!=project_id { aliases.push(json!({"oldId":old,"owner":target})); } } }
            let mut changed=existing.is_none();
            changed |= existing.as_ref().is_some_and(|n| n!=&req.name);
            changed |= req.roots.iter().any(|r| tx.query_row("SELECT 1 FROM project_root WHERE canonical_root=?1",[r],|x|x.get::<_,i64>(0)).optional().unwrap_or(None).is_none());
            changed |= req.derived_root_parents.iter().any(|r| tx.query_row("SELECT 1 FROM derived_root_parent WHERE canonical_parent=?1",[r],|x|x.get::<_,i64>(0)).optional().unwrap_or(None).is_none());
            if let Some(w)=&req.workspace_id { changed |= tx.query_row("SELECT 1 FROM project_workspace WHERE project_id=?1",[&project_id],|x|x.get::<_,i64>(0)).optional()?.is_none(); let _=w; }
            let alias_new= req.roots.iter().any(|r| tx.query_row("SELECT 1 FROM project_alias WHERE old_id=?1",[implicit_project_id(r)],|x|x.get::<_,i64>(0)).optional().unwrap_or(None).is_none()); changed |= alias_new;
            let mut response=json!({"projectId":project_id,"name":req.name,"roots":req.roots,"derivedRootParents":req.derived_root_parents,"warnings":aliases});
            if !changed { return Ok(Action{changed:false,seq:None,value:response,payload}); }
            let seq=append(tx,"register",&payload,&actor,key.as_deref(),now,self.principal)?;
            if existing.is_none() { tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3)",params![&project_id,&req.name,now])?; } else { tx.execute("UPDATE project SET name=?1,updated_at=?2 WHERE project_id=?3",params![&req.name,&now,&project_id])?; }
            for root in &req.roots { tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![root,&project_id,now])?; let old=implicit_project_id(root); let _=tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![old,&project_id,now])?; }
            for parent in &req.derived_root_parents { tx.execute("INSERT OR IGNORE INTO derived_root_parent(canonical_parent,project_id) VALUES(?1,?2)",[parent,&project_id])?; }
            if let Some(w)=&req.workspace_id { tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)",params![w,now])?; tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",[&project_id,w])?; tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",[w,&project_id])?; }
            response["implicitAliasCollisions"]=json!(aliases); Ok(Action{changed:true,seq:Some(seq),value:response,payload:json!({"request":payload,"implicitAliasCollisions":aliases})})
        })
    }

    pub fn assign_workspace(&self, req: AssignWorkspaceRequest) -> Result<Vec<u8>, RegistryError> {
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        // A PROJECT CAN BE MOVED BETWEEN WORKSPACES.
        //
        // This used to refuse a second assignment with `workspace_conflict`,
        // which left a user who filed a project in the wrong workspace with no
        // way to correct it.
        //
        // Whatever this does must be mirrored in the `"assign_workspace"` arm of
        // `replay()`: the tables are a projection of the journal, so a move that
        // the live path applies and replay does not would silently REVERT the
        // next time anything calls `rebuild()`. Nothing fails in that case --
        // the move simply disappears -- so the replay test is the one that
        // matters.
        self.mutation(
            "assign_workspace",
            key.clone().as_deref(),
            move |tx| {
                let now = now_unix_millis();
                if tx
                    .query_row(
                        "SELECT 1 FROM project WHERE project_id=?1",
                        [&req.project_id],
                        |r| r.get::<_, i64>(0),
                    )
                    .optional()?
                    .is_none()
                {
                    return Err(domain("not_found", &req.project_id));
                };
                let current = tx
                    .query_row(
                        "SELECT workspace_id FROM project_workspace WHERE project_id=?1",
                        [&req.project_id],
                        |r| r.get::<_, String>(0),
                    )
                    .optional()?;
                // Re-assigning to the same workspace stays a no-op and appends
                // NOTHING to the journal. Recording it would grow the journal on
                // repeated calls and make every rebuild replay work that changes
                // no state.
                if current.as_deref() == Some(req.workspace_id.as_str()) {
                    return Ok(Action {
                        changed: false,
                        seq: None,
                        value: json!({"projectId":req.project_id,"workspaceId":req.workspace_id}),
                        payload,
                    });
                }
                super::agent::ensure_project_unbound(tx, &req.project_id)?;
                let seq = append(
                    tx,
                    "assign_workspace",
                    &payload,
                    &actor,
                    key.as_deref(),
                    now,
                    self.principal,
                )?;
                tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3)",params![&req.workspace_id,req.workspace_name.clone().unwrap_or_else(||req.workspace_id.clone()),now])?;
                // The old membership row is REMOVED, not merely superseded.
                // workspace_member carries UNIQUE(ref_kind, device_fingerprint,
                // project_id), so for a local ref exactly one row per project can
                // exist -- leaving the old one would make the insert below fail
                // outright rather than merely leaving the project listed under
                // its former workspace.
                if let Some(old) = current.as_deref() {
                    tx.execute(
                        "DELETE FROM workspace_member WHERE workspace_id=?1 AND ref_kind='local' AND device_fingerprint='' AND project_id=?2",
                        [old, req.project_id.as_str()],
                    )?;
                }
                tx.execute(
                    "INSERT INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)
                     ON CONFLICT(project_id) DO UPDATE SET workspace_id=excluded.workspace_id",
                    [&req.project_id, &req.workspace_id],
                )?;
                tx.execute("INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",[&req.workspace_id,&req.project_id])?;
                Ok(Action {
                    changed: true,
                    seq: Some(seq),
                    value: json!({"projectId":req.project_id,"workspaceId":req.workspace_id}),
                    payload,
                })
            },
        )
    }

    pub fn set_workspace_root(
        &self,
        req: SetWorkspaceRootRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        // Validated before the transaction: the path checks read the
        // filesystem, and the journal must record exactly the value the
        // projection holds so `rebuild` reproduces it without re-reading disk.
        if let Some(root) = req.root.as_deref() {
            if !Path::new(root).is_absolute() {
                return Err(domain(
                    "not_absolute",
                    format!("workspace root must be an absolute path: {root}"),
                ));
            }
            canonical(root)?;
            if !Path::new(root).is_dir() {
                return Err(domain(
                    "not_a_directory",
                    format!("workspace root is not a directory: {root}"),
                ));
            }
        }
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("set_workspace_root", key.clone().as_deref(), move |tx| {
            let now = now_unix_millis();
            let current = tx
                .query_row(
                    "SELECT root FROM workspace WHERE workspace_id=?1",
                    [&req.workspace_id],
                    |r| r.get::<_, Option<String>>(0),
                )
                .optional()?;
            let Some(current) = current else {
                return Err(domain("not_found", &req.workspace_id));
            };
            let value = json!({"workspaceId": req.workspace_id, "root": req.root});
            // Setting the value it already holds appends nothing, the same rule
            // `assign_workspace` follows, so repeated calls don't grow the journal.
            if current == req.root {
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value,
                    payload,
                });
            }
            let seq = append(
                tx,
                "set_workspace_root",
                &payload,
                &actor,
                key.as_deref(),
                now,
                self.principal,
            )?;
            tx.execute(
                "UPDATE workspace SET root=?1, updated_at=?2 WHERE workspace_id=?3",
                params![req.root, now, req.workspace_id],
            )?;
            Ok(Action {
                changed: true,
                seq: Some(seq),
                value,
                payload,
            })
        })
    }

    pub fn upgrade_implicit(
        &self,
        mut req: UpgradeImplicitRequest,
    ) -> Result<Vec<u8>, RegistryError> {
        if let Some(id) = &req.project_id {
            if reserved(id) {
                return Err(domain("reserved_project_id_namespace", id));
            }
        }
        if let Some(r) = req.root.take() {
            req.roots.push(r)
        }
        req.roots = req
            .roots
            .iter()
            .map(|r| canonical(r))
            .collect::<Result<_, _>>()?;
        let id = req
            .project_id
            .clone()
            .unwrap_or_else(|| mint("upgrade1", &req.name, &req.roots));
        let _register = RegisterRequest {
            project_id: Some(id.clone()),
            name: req.name.clone(),
            workspace_id: req.workspace_id.clone(),
            roots: req.roots.clone(),
            derived_root_parents: vec![],
            request_key: None,
            actor: req.actor.clone(),
        };
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("upgrade_implicit", key.clone().as_deref(), move |tx| {
            let now = now_unix_millis();
            if let Some(target) = tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1", [&req.implicit_id], |r| r.get::<_, String>(0)).optional()? {
                if target != id { return Err(domain("alias_conflict", target)); }
            }
            let existing = tx.query_row("SELECT name FROM project WHERE project_id=?1", [&id], |r| r.get::<_, String>(0)).optional()?;
            let mut changed = existing.is_none() || existing.as_ref().is_some_and(|n| n != &req.name);
            for r in &req.roots {
                changed |= tx.query_row("SELECT 1 FROM project_root WHERE canonical_root=?1", [r], |r| r.get::<_, i64>(0)).optional()?.is_none();
            }
            changed |= tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1", [&req.implicit_id], |r| r.get::<_, String>(0)).optional()?.is_none();
            if !changed {
                return Ok(Action { changed: false, seq: None, value: json!({"projectId":id,"oldId":req.implicit_id}), payload });
            }
            if req.workspace_id.is_some() {
                super::agent::ensure_project_attachment_allowed(tx, &id)?;
            }
            let seq = append(tx, "upgrade_implicit", &payload, &actor, key.as_deref(), now, self.principal)?;
            if existing.is_none() {
                tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3)", params![&id,&req.name,now])?;
            } else {
                tx.execute("UPDATE project SET name=?1,updated_at=?2 WHERE project_id=?3", params![&req.name,now,&id])?;
            }
            for r in &req.roots {
                tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)", params![r,&id,now])?;
                tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![implicit_project_id(r),&id,now])?;
            }
            tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![&req.implicit_id,&id,now])?;
            if let Some(w) = &req.workspace_id {
                tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)", params![w,now])?;
                tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)", [&id,w])?;
                tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)", [w,&id])?;
            }
            Ok(Action { changed: true, seq: Some(seq), value: json!({"projectId":id,"oldId":req.implicit_id}), payload })
        })
    }

    pub fn remove(&self, req: RemoveRequest) -> Result<Vec<u8>, RegistryError> {
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("remove", key.clone().as_deref(), move |tx| {
            let now = now_unix_millis();
            if let Some(w) = &req.workspace_id {
                if tx.query_row("SELECT 1 FROM workspace WHERE workspace_id=?1", [w], |r| r.get::<_, i64>(0)).optional()?.is_none() {
                    return Err(domain("not_found", w));
                }
                super::agent::ensure_workspace_unbound(tx, w)?;
                let seq = append(tx, "remove", &payload, &actor, key.as_deref(), now, self.principal)?;
                tx.execute("DELETE FROM project_workspace WHERE workspace_id=?1", [w])?;
                tx.execute("DELETE FROM workspace_member WHERE workspace_id=?1", [w])?;
                tx.execute("DELETE FROM workspace WHERE workspace_id=?1", [w])?;
                return Ok(Action { changed: true, seq: Some(seq), value: json!({"workspaceId":w}), payload });
            }
            let id = req.project_id.clone().ok_or_else(|| domain("invalid_params", "projectId is required"))?;
            if tx.query_row("SELECT 1 FROM project WHERE project_id=?1", [&id], |r| r.get::<_, i64>(0)).optional()?.is_none() {
                return Err(domain("not_found", id));
            }
            if let Some(s) = &req.successor_project_id {
                if s == &id || tx.query_row("SELECT 1 FROM project WHERE project_id=?1", [s], |r| r.get::<_, i64>(0)).optional()?.is_none() {
                    return Err(domain("not_found", s));
                }
            }
            // Removing the source also merges its aliases when a successor is
            // supplied. Refusing every bound source prevents two live heads from
            // resolving to the successor and keeps existing agents on valid ids.
            super::agent::ensure_project_unbound(tx, &id)?;
            let dropped = tx.query_row("SELECT workspace_id FROM project_workspace WHERE project_id=?1", [&id], |r| r.get::<_, String>(0)).optional()?;
            let seq = append(tx, "remove", &payload, &actor, key.as_deref(), now, self.principal)?;
            super::binding::retire_project_bindings(tx, &id, seq)?;
            if let Some(s) = &req.successor_project_id {
                tx.execute("UPDATE project_root SET project_id=?1 WHERE project_id=?2", [s,&id])?;
                tx.execute("UPDATE derived_root_parent SET project_id=?1 WHERE project_id=?2", [s,&id])?;
                tx.execute("UPDATE project_alias SET project_id=?1 WHERE project_id=?2", [s,&id])?;
                tx.execute("DELETE FROM project_alias WHERE old_id=?1", [&id])?;
                tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![&id,s,now])?;
            } else {
                tx.execute("DELETE FROM project_root WHERE project_id=?1", [&id])?;
                tx.execute("DELETE FROM derived_root_parent WHERE project_id=?1", [&id])?;
                tx.execute("DELETE FROM project_alias WHERE project_id=?1 OR old_id=?1", [&id])?;
            }
            tx.execute("DELETE FROM project_workspace WHERE project_id=?1", [&id])?;
            tx.execute("DELETE FROM workspace_member WHERE ref_kind='local' AND project_id=?1", [&id])?;
            tx.execute("DELETE FROM project WHERE project_id=?1", [&id])?;
            Ok(Action { changed: true, seq: Some(seq), value: json!({"projectId":id,"successorProjectId":req.successor_project_id,"droppedWorkspaceId":dropped}), payload })
        })
    }
}

fn mint(domain: &str, name: &str, roots: &[String]) -> String {
    let mut h = blake3::Hasher::new();
    h.update(domain.as_bytes());
    h.update(b":");
    h.update(name.as_bytes());
    for r in roots {
        h.update(b"\0");
        h.update(r.as_bytes());
    }
    format!("pj-{}", &h.finalize().to_hex()[..16])
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SeedReportEntry {
    kind: String,
    mc_identity: Option<String>,
    canonical_root: Option<String>,
    project_id: Option<String>,
    owner_project_id: Option<String>,
    new_project_id: Option<String>,
    roots: Option<Vec<String>>,
    reason: Option<String>,
}

impl super::JournalWriter<'_> {
    pub fn seed_import(&self, mut req: SeedImportRequest) -> Result<Vec<u8>, RegistryError> {
        if req.source != "mc" {
            return Err(domain("invalid_source", &req.source));
        }
        // Seeds import HISTORICAL topology: roots that no longer exist on disk
        // are skipped with per-root provenance (spec §10 rule 1), unlike live
        // mutations where existence-at-registration rejects. The journal payload
        // records survivors only, so rebuild never depends on disk state.
        let mut missing: Vec<SeedPair> = Vec::new();
        let mut home_scoped: Vec<SeedPair> = Vec::new();
        let mut surviving = Vec::with_capacity(req.payload.pairs.len());
        for mut pair in std::mem::take(&mut req.payload.pairs) {
            if req.exclude_home_scoped && is_home_scoped(&pair.canonical_root) {
                home_scoped.push(pair);
                continue;
            }
            // Seeds are historically OBSERVED path strings, not caller input:
            // the module canonicalizes them itself (case drift on APFS, symlink
            // components). I6's strict canonical-input equality applies to live
            // mutations only.
            match ProjectRootId::from_path(&pair.canonical_root) {
                Ok(id) => {
                    pair.canonical_root = id.to_string();
                    surviving.push(pair);
                }
                Err(IdentityError::NonExistentPath { .. }) => {
                    missing.push(pair);
                }
                Err(error) => return Err(domain("not_canonical", error.to_string())),
            }
        }
        req.payload.pairs = surviving;
        missing.sort_by(|a, b| {
            a.mc_identity
                .cmp(&b.mc_identity)
                .then(a.canonical_root.cmp(&b.canonical_root))
        });
        home_scoped.sort_by(|a, b| {
            a.mc_identity
                .cmp(&b.mc_identity)
                .then(a.canonical_root.cmp(&b.canonical_root))
        });
        req.payload.pairs.sort_by(|a, b| {
            a.mc_identity
                .cmp(&b.mc_identity)
                .then(a.canonical_root.cmp(&b.canonical_root))
        });
        let mut dedup = Vec::new();
        let mut seen = BTreeSet::new();
        for pair in req.payload.pairs {
            if seen.insert((
                pair.canonical_root.clone(),
                pair.mc_identity.clone(),
                pair.identity_class.clone(),
            )) {
                dedup.push(pair);
            }
        }
        req.payload.pairs = dedup;
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "seed_import".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("seed_import",key.clone().as_deref(),move|tx|{
            let now=now_unix_millis(); let mut report=Vec::new(); let mut groups:BTreeMap<String,Vec<SeedPair>>=BTreeMap::new();
            for p in &missing {report.push(SeedReportEntry{kind:"skipped".into(),mc_identity:Some(p.mc_identity.clone()),canonical_root:Some(p.canonical_root.clone()),project_id:None,owner_project_id:None,new_project_id:None,roots:None,reason:Some("missing".into())});}
            for p in &home_scoped {report.push(SeedReportEntry{kind:"skipped".into(),mc_identity:Some(p.mc_identity.clone()),canonical_root:Some(p.canonical_root.clone()),project_id:None,owner_project_id:None,new_project_id:None,roots:None,reason:Some("home_scoped".into())});}
            let mut by_root:BTreeMap<String,Vec<SeedPair>>=BTreeMap::new(); for p in &req.payload.pairs {by_root.entry(p.canonical_root.clone()).or_default().push(p.clone());}
            for (root, rows) in &by_root { let has_git=rows.iter().any(|p|p.identity_class.as_deref()==Some("git")||p.mc_identity.starts_with("git:")); let mut identities=BTreeSet::new(); for p in rows {if has_git&&p.identity_class.as_deref()==Some("dir"){report.push(SeedReportEntry{kind:"identity_class_precedence".into(),mc_identity:Some(p.mc_identity.clone()),canonical_root:Some(root.clone()),project_id:None,owner_project_id:None,new_project_id:None,roots:None,reason:Some("git identity wins".into())});}else{identities.insert(p.mc_identity.clone());}} if identities.len()>1 {for id in identities {report.push(SeedReportEntry{kind:"conflicted".into(),mc_identity:Some(id),canonical_root:Some(root.clone()),project_id:None,owner_project_id:None,new_project_id:None,roots:None,reason:Some("one root observed under multiple identities".into())});}} else if let Some(id)=identities.into_iter().next(){groups.entry(id).or_default().extend(rows.iter().filter(|p|!has_git||p.identity_class.as_deref()!=Some("dir")).cloned());}}
            let mut changed=false;
            for workspace in &req.payload.workspaces {
                let old=tx.query_row("SELECT name FROM workspace WHERE workspace_id=?1",[&workspace.workspace_id],|r|r.get::<_,String>(0)).optional()?;
                if old.as_deref()!=Some(&workspace.name) {
                    if old.is_some() { tx.execute("UPDATE workspace SET name=?1,updated_at=?2 WHERE workspace_id=?3",params![workspace.name,now,workspace.workspace_id])?; }
                    else { tx.execute("INSERT INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3)",params![workspace.workspace_id,workspace.name,now])?; }
                    changed=true;
                }
            }
            for (identity, rows) in groups { let minted=seed_id(&identity); let occupied_alias=tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1",[&minted],|r|r.get::<_,String>(0)).optional()?; let live=tx.query_row("SELECT seed_identity FROM project WHERE project_id=?1",[&minted],|r|r.get::<_,Option<String>>(0)).optional()?; let target=if let Some(ref seed)=live {if seed.as_deref()==Some(&identity){report.push(SeedReportEntry{kind:"rejoined".into(),mc_identity:Some(identity.clone()),canonical_root:None,project_id:Some(minted.clone()),owner_project_id:None,new_project_id:None,roots:None,reason:None});Some(minted.clone())}else{report.push(SeedReportEntry{kind:"conflicted".into(),mc_identity:Some(identity.clone()),canonical_root:None,project_id:Some(minted.clone()),owner_project_id:None,new_project_id:None,roots:None,reason:Some("minted id occupied by another project".into())});None}} else if occupied_alias.is_some(){report.push(SeedReportEntry{kind:"alias_occupied".into(),mc_identity:Some(identity.clone()),canonical_root:None,project_id:None,owner_project_id:occupied_alias,new_project_id:None,roots:None,reason:Some("minted id is an alias key".into())});None} else {Some(minted.clone())};
                let mut available=Vec::new(); let mut owners=BTreeMap::new(); for p in &rows {if let Some(owner)=tx.query_row("SELECT project_id FROM project_root WHERE canonical_root=?1",[&p.canonical_root],|r|r.get::<_,String>(0)).optional()? {owners.insert(p.canonical_root.clone(),owner.clone());report.push(SeedReportEntry{kind:"skipped".into(),mc_identity:Some(identity.clone()),canonical_root:Some(p.canonical_root.clone()),project_id:None,owner_project_id:Some(owner),new_project_id:target.clone(),roots:None,reason:Some("root already registered".into())});}else if target.is_some(){available.push(p.clone());}}
                if owners.values().collect::<BTreeSet<_>>().len()>1 || (target.is_some() && !owners.is_empty() && !available.is_empty()) {report.push(SeedReportEntry{kind:"identity_split".into(),mc_identity:Some(identity.clone()),canonical_root:None,project_id:None,owner_project_id:None,new_project_id:target.clone(),roots:Some(available.iter().map(|p|p.canonical_root.clone()).collect()),reason:None});}
                let Some(project) = target else { continue };
                if !available.is_empty() || live.is_none() {
                    let claims = req.payload.members.iter().filter(|m| m.mc_identity == identity).map(|m| m.workspace_id.clone()).collect::<BTreeSet<_>>();
                    if claims.len() == 1 {
                        super::agent::ensure_project_attachment_allowed(tx, &project)?;
                    }
                    let name = req.payload.names.get(&identity).cloned().or_else(|| rows.first().and_then(|p| p.name.clone())).unwrap_or_else(|| Path::new(&rows[0].canonical_root).file_name().map(|x| x.to_string_lossy().into_owned()).unwrap_or_else(|| identity.clone()));
                    if live.is_none() {
                        tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,?3,?4,?4)", params![project,name,identity,now])?;
                        changed = true;
                    }
                    for p in &available {
                        tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)", params![p.canonical_root,project,now])?;
                        tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![implicit_project_id(&p.canonical_root),project,now])?;
                        changed = true;
                    }
                    if claims.len() == 1 {
                        let w = claims.iter().next().unwrap();
                        tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)", params![project,w])?;
                        tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)", params![w,project])?;
                        changed = true;
                    } else if claims.len() > 1 {
                        report.push(SeedReportEntry { kind: "multi_workspace".into(), mc_identity: Some(identity.clone()), canonical_root: None, project_id: Some(project.clone()), owner_project_id: None, new_project_id: None, roots: None, reason: Some("detached because export claimed multiple workspaces".into()) });
                    }
                }
            }
            let value=json!({"report":report}); if !changed {return Ok(Action{changed:false,seq:None,value,payload});} let seq=append(tx,"seed_import",&payload,&actor,key.as_deref(),now,self.principal)?; Ok(Action{changed:true,seq:Some(seq),value,payload})
        })
    }
}

impl RegistryStore {
    pub fn verify(&self) -> Result<VerifyReply, RegistryError> {
        self.read(|conn| {
            // Keep the live checks and both snapshots in one transaction. On
            // error Transaction's drop rolls back; success rolls back explicitly.
            // Verification never commits the temporary replay or its side effects.
            let tx = conn.unchecked_transaction()?;
            let mut mismatches = Vec::new();
            let local = tx.query_row(
                "SELECT COUNT(*) FROM workspace_member WHERE ref_kind='local'",
                [],
                |r| r.get(0),
            )?;
            let pairs = tx.query_row("SELECT COUNT(*) FROM project_workspace", [], |r| r.get(0))?;
            for query in [
                "SELECT wm.workspace_id,wm.project_id FROM workspace_member wm LEFT JOIN project_workspace pw ON pw.workspace_id=wm.workspace_id AND pw.project_id=wm.project_id WHERE wm.ref_kind='local' AND pw.project_id IS NULL",
                "SELECT pw.workspace_id,pw.project_id FROM project_workspace pw LEFT JOIN workspace_member wm ON wm.workspace_id=pw.workspace_id AND wm.project_id=pw.project_id AND wm.ref_kind='local' WHERE wm.project_id IS NULL",
            ] {
                let mut stmt = tx.prepare(query)?;
                for row in stmt.query_map([], |r| {
                    Ok(format!("{}:{}", r.get::<_, String>(0)?, r.get::<_, String>(1)?))
                })? {
                    mismatches.push(row?);
                }
            }
            let generation = self.generation_from_connection(&tx)?;
            let replay = replay_and_compare(&tx)?;
            tx.rollback()?;
            Ok(VerifyReply {
                ok: mismatches.is_empty() && replay.ok,
                local_members: local,
                project_workspaces: pairs,
                mismatches,
                generation,
                replay,
            })
        })
    }

    pub fn rebuild(&self) -> Result<RebuildReply, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let replay = replay_and_compare(tx)?;
                Ok(RebuildReply {
                    generation: self.generation_from_connection(tx)?,
                    replay,
                })
            })
            .map_err(RegistryError::Store)
    }
}

// Deletion order respects projection dependencies. This is also the sole list
// used for snapshots, so verification and repair always cover the same tables.
const DERIVED_TABLES: &[&str] = &[
    "agent_name_claim",
    "agent",
    "workspace_member",
    "project_workspace",
    "project_alias",
    "derived_root_parent",
    "project_root",
    "project",
    "workspace",
    "root_binding",
    "retired_binding",
    "root_approval",
];

fn delete_and_replay(tx: &Transaction<'_>) -> rusqlite::Result<()> {
    // Historical claims and tombstoned ids are projections too. Defer foreign
    // keys because an agent's merge target or supervisor may be replayed later.
    tx.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
    for table in DERIVED_TABLES {
        tx.execute(&format!("DELETE FROM {table}"), [])?;
    }
    let mut stmt =
        tx.prepare("SELECT seq,op,payload_json,created_at FROM registry_journal ORDER BY seq")?;
    let rows = stmt
        .query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, i64>(3)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    drop(stmt);
    for (seq, op, payload, now) in rows {
        let value: Value = serde_json::from_str(&payload)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let request = value.get("request").cloned().unwrap_or(value);
        replay(tx, seq, &op, request, now)?;
    }
    Ok(())
}

// Preserve SQLite storage classes and exact bytes, rather than converting rows
// to JSON (which could equate a blob with text, or lose integer precision).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum StoredCell {
    Null,
    Integer(i64),
    Real(u64),
    Text(Vec<u8>),
    Blob(Vec<u8>),
}

impl StoredCell {
    fn from_sql(value: ValueRef<'_>) -> Self {
        match value {
            ValueRef::Null => Self::Null,
            ValueRef::Integer(value) => Self::Integer(value),
            ValueRef::Real(value) => Self::Real(value.to_bits()),
            ValueRef::Text(value) => Self::Text(value.to_vec()),
            ValueRef::Blob(value) => Self::Blob(value.to_vec()),
        }
    }

    fn sample_value(&self) -> Value {
        match self {
            Self::Null => Value::Null,
            Self::Integer(value) => json!(value),
            Self::Real(bits) => json!(f64::from_bits(*bits)),
            Self::Text(bytes) => json!(String::from_utf8_lossy(bytes)),
            Self::Blob(bytes) => json!(bytes),
        }
    }
}

struct TableSnapshot {
    primary_columns: Vec<(usize, String)>,
    // A sorted multiset compares every column deterministically, including
    // duplicate rows allowed by SQLite's nullable non-integer primary keys.
    rows: BTreeMap<Vec<StoredCell>, u64>,
}

fn snapshot_table(conn: &Connection, table: &str) -> rusqlite::Result<TableSnapshot> {
    let primary_columns = conn
        .prepare(&format!("PRAGMA table_info({table})"))?
        .query_map([], |r| {
            Ok((
                r.get::<_, usize>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, i64>(5)?,
            ))
        })?
        .collect::<rusqlite::Result<Vec<_>>>()?
        .into_iter()
        .filter(|(_, _, key_position)| *key_position > 0)
        .map(|(index, name, _)| (index, name))
        .collect();
    let mut stmt = conn.prepare(&format!("SELECT * FROM {table}"))?;
    let columns = stmt.column_count();
    let mut rows = BTreeMap::new();
    for row in stmt.query_map([], |r| {
        (0..columns)
            .map(|index| r.get_ref(index).map(StoredCell::from_sql))
            .collect::<rusqlite::Result<Vec<_>>>()
    })? {
        *rows.entry(row?).or_insert(0) += 1;
    }
    Ok(TableSnapshot {
        primary_columns,
        rows,
    })
}

fn differing_rows(
    source: &TableSnapshot,
    other: &TableSnapshot,
    sample_limit: usize,
) -> (u64, Vec<BTreeMap<String, Value>>) {
    let mut count = 0;
    let mut keys = Vec::new();
    for (row, occurrences) in &source.rows {
        let difference = occurrences.saturating_sub(*other.rows.get(row).unwrap_or(&0));
        count += difference;
        if difference > 0 && keys.len() < sample_limit {
            keys.push(
                source
                    .primary_columns
                    .iter()
                    .map(|(index, name)| (name.clone(), row[*index].sample_value()))
                    .collect(),
            );
        }
    }
    (count, keys)
}

fn replay_and_compare(tx: &Transaction<'_>) -> rusqlite::Result<ReplayReport> {
    let live = DERIVED_TABLES
        .iter()
        .map(|table| snapshot_table(tx, table))
        .collect::<rusqlite::Result<Vec<_>>>()?;
    delete_and_replay(tx)?;
    let mut tables = Vec::new();
    for (table, before) in DERIVED_TABLES.iter().zip(live) {
        let after = snapshot_table(tx, table)?;
        let (missing, missing_keys) = differing_rows(&after, &before, 5);
        let (unexpected, unexpected_keys) = differing_rows(&before, &after, 5 - missing_keys.len());
        if missing > 0 || unexpected > 0 {
            tables.push(ReplayTableDifference {
                table: (*table).into(),
                missing,
                unexpected,
                missing_keys,
                unexpected_keys,
            });
        }
    }
    Ok(ReplayReport {
        ok: tables.is_empty(),
        tables,
    })
}

fn seed_id(identity: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"seed1:");
    h.update(identity.as_bytes());
    format!("pj-{}", &h.finalize().to_hex()[..16])
}

fn replay(tx: &Transaction<'_>, seq: i64, op: &str, v: Value, now: i64) -> rusqlite::Result<()> {
    if super::agent::replay_agent_entry(tx, op, &v)? {
        return Ok(());
    }
    if super::binding::replay_binding_op(tx, seq, op, &v)?
        || super::binding::replay_root_op(tx, seq, op, &v, now)?
    {
        return Ok(());
    }
    match op {
        "register" => {
            let r: RegisterRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            // Older journal rows record the request, not the resolved id. Infer
            // ownership from the state replayed so far, just as live register does.
            let id = if let Some(id) = r.project_id {
                id
            } else {
                let mut owners = BTreeSet::new();
                for root in &r.roots {
                    if let Some(owner) = tx
                        .query_row(
                            "SELECT project_id FROM project_root WHERE canonical_root=?1",
                            [root],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?
                    {
                        owners.insert(owner);
                    }
                }
                for parent in &r.derived_root_parents {
                    if let Some(owner) = tx
                        .query_row(
                            "SELECT project_id FROM derived_root_parent WHERE canonical_parent=?1",
                            [parent],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?
                    {
                        owners.insert(owner);
                    }
                }
                if owners.len() == 1 {
                    owners.into_iter().next().unwrap()
                } else if owners.len() > 1 {
                    return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(domain(
                        "project_conflict",
                        format!(
                            "conflicting owners: {}",
                            owners.into_iter().collect::<Vec<_>>().join(",")
                        ),
                    ))));
                } else {
                    mint("register1", &r.name, &r.roots)
                }
            };
            // Mirrors the live `register`, which renames an existing project and
            // places a new one in its workspace. Replay used to do neither, so a
            // `rebuild` reverted renames and silently unplaced every project that
            // was put in a workspace at registration.
            if r.workspace_id.is_some() {
                super::agent::ensure_project_attachment_allowed(tx, &id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            }
            tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3) ON CONFLICT(project_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at",params![id,r.name,now])?;
            if let Some(w) = &r.workspace_id {
                tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)",params![w,now])?;
                tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",params![id,w])?;
                tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",params![w,id])?;
            }
            for root in r.roots {
                tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![root,id,now])?;
                tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&root),id,now])?;
            }
            for p in r.derived_root_parents {
                tx.execute("INSERT OR IGNORE INTO derived_root_parent(canonical_parent,project_id) VALUES(?1,?2)",params![p,id])?;
            }
        }
        // Mirrors the live `set_workspace_root`. The journal holds the value
        // that was validated when it was set, so replay never re-reads disk: a
        // root directory removed since then still rebuilds to what was recorded.
        "set_workspace_root" => {
            let r: SetWorkspaceRootRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            tx.execute(
                "UPDATE workspace SET root=?1, updated_at=?2 WHERE workspace_id=?3",
                params![r.root, now, r.workspace_id],
            )?;
        }
        "assign_workspace" => {
            let r: AssignWorkspaceRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3)",params![r.workspace_id,r.workspace_name.as_ref().unwrap_or(&r.workspace_id),now])?;
            // REPLAY MUST CONVERGE ON THE LAST ENTRY, NOT THE FIRST.
            //
            // These were `INSERT OR IGNORE`, which was correct only while a
            // second assignment was refused. Now that a project can move,
            // ignoring a conflict would keep the EARLIEST workspace: replaying
            // assign(P,A) then assign(P,B) would leave P in A, so a rebuilt
            // store would silently disagree with the live tables and the move
            // would revert with nothing reporting a failure.
            //
            // Mirrors the live path in `assign_workspace`, including removing
            // the stale membership row -- UNIQUE(ref_kind, device_fingerprint,
            // project_id) means the insert below would otherwise fail on the
            // second assignment and abort the whole rebuild.
            let previous = tx
                .query_row(
                    "SELECT workspace_id FROM project_workspace WHERE project_id=?1",
                    [&r.project_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()?;
            if previous.as_deref() != Some(r.workspace_id.as_str()) {
                super::agent::ensure_project_unbound(tx, &r.project_id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            }
            if let Some(old) = previous.as_deref() {
                tx.execute(
                    "DELETE FROM workspace_member WHERE workspace_id=?1 AND ref_kind='local' AND device_fingerprint='' AND project_id=?2",
                    [old, r.project_id.as_str()],
                )?;
            }
            tx.execute(
                "INSERT INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)
                 ON CONFLICT(project_id) DO UPDATE SET workspace_id=excluded.workspace_id",
                params![r.project_id, r.workspace_id],
            )?;
            tx.execute(
                "INSERT OR IGNORE INTO workspace_member VALUES(?1,'local','',?2)",
                params![r.workspace_id, r.project_id],
            )?;
        }
        "upgrade_implicit" => {
            let r: UpgradeImplicitRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let id = r
                .project_id
                .unwrap_or_else(|| mint("upgrade1", &r.name, &r.roots));
            if r.workspace_id.is_some() {
                super::agent::ensure_project_attachment_allowed(tx, &id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            }
            tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3) ON CONFLICT(project_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at", params![id,r.name,now])?;
            for root in r.roots {
                tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![root,id,now])?;
                tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&root),id,now])?;
            }
            tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![r.implicit_id,id,now])?;
            if let Some(w) = &r.workspace_id {
                tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)",params![w,now])?;
                tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",params![id,w])?;
                tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",params![w,id])?;
            }
        }
        "seed_import" => {
            let r: SeedImportRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            // The payload contains surviving observations, including identities
            // that live seed_import declined. Reapply those decisions against
            // replayed state rather than treating every observation as a binding.
            let mut by_root: BTreeMap<String, Vec<SeedPair>> = BTreeMap::new();
            for p in r.payload.pairs {
                by_root.entry(p.canonical_root.clone()).or_default().push(p);
            }
            let mut groups: BTreeMap<String, Vec<SeedPair>> = BTreeMap::new();
            for rows in by_root.values() {
                let has_git = rows.iter().any(|p| {
                    p.identity_class.as_deref() == Some("git") || p.mc_identity.starts_with("git:")
                });
                let candidates = rows
                    .iter()
                    .filter(|p| !has_git || p.identity_class.as_deref() != Some("dir"));
                let identities = candidates
                    .clone()
                    .map(|p| p.mc_identity.clone())
                    .collect::<BTreeSet<_>>();
                if identities.len() == 1 {
                    groups
                        .entry(identities.into_iter().next().unwrap())
                        .or_default()
                        .extend(candidates.cloned());
                }
            }
            for w in r.payload.workspaces {
                let old = tx
                    .query_row(
                        "SELECT name FROM workspace WHERE workspace_id=?1",
                        [&w.workspace_id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?;
                if old.as_deref() != Some(w.name.as_str()) {
                    tx.execute("INSERT INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3) ON CONFLICT(workspace_id) DO UPDATE SET name=excluded.name,updated_at=excluded.updated_at",params![w.workspace_id,w.name,now])?;
                }
            }
            for (identity, rows) in groups {
                let id = seed_id(&identity);
                let live = tx
                    .query_row(
                        "SELECT seed_identity FROM project WHERE project_id=?1",
                        [&id],
                        |row| row.get::<_, Option<String>>(0),
                    )
                    .optional()?;
                if let Some(seed) = &live {
                    if seed.as_deref() != Some(identity.as_str()) {
                        continue;
                    }
                } else if tx
                    .query_row(
                        "SELECT project_id FROM project_alias WHERE old_id=?1",
                        [&id],
                        |row| row.get::<_, String>(0),
                    )
                    .optional()?
                    .is_some()
                {
                    continue;
                }
                let mut available = Vec::new();
                for p in &rows {
                    if tx
                        .query_row(
                            "SELECT project_id FROM project_root WHERE canonical_root=?1",
                            [&p.canonical_root],
                            |row| row.get::<_, String>(0),
                        )
                        .optional()?
                        .is_none()
                    {
                        available.push(p);
                    }
                }
                if available.is_empty() && live.is_some() {
                    continue;
                }
                if live.is_none() {
                    let name = r
                        .payload
                        .names
                        .get(&identity)
                        .cloned()
                        .or_else(|| rows.first().and_then(|p| p.name.clone()))
                        .unwrap_or_else(|| {
                            Path::new(&rows[0].canonical_root)
                                .file_name()
                                .map(|x| x.to_string_lossy().into_owned())
                                .unwrap_or_else(|| identity.clone())
                        });
                    tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,?3,?4,?4)",params![id,name,identity,now])?;
                }
                for p in available {
                    tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![p.canonical_root,id,now])?;
                    tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&p.canonical_root),id,now])?;
                }
                let claims = r
                    .payload
                    .members
                    .iter()
                    .filter(|m| m.mc_identity == identity)
                    .map(|m| &m.workspace_id)
                    .collect::<BTreeSet<_>>();
                if claims.len() == 1 {
                    let w = claims.into_iter().next().unwrap();
                    super::agent::ensure_project_attachment_allowed(tx, &id)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                    tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",params![id,w])?;
                    tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",params![w,id])?;
                }
            }
        }
        "remove" => {
            let r: RemoveRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            if let Some(workspace) = r.workspace_id {
                super::agent::ensure_workspace_unbound(tx, &workspace)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                tx.execute(
                    "DELETE FROM project_workspace WHERE workspace_id=?1",
                    [&workspace],
                )?;
                tx.execute(
                    "DELETE FROM workspace_member WHERE workspace_id=?1",
                    [&workspace],
                )?;
                tx.execute("DELETE FROM workspace WHERE workspace_id=?1", [&workspace])?;
            } else if let Some(id) = r.project_id {
                super::agent::ensure_project_unbound(tx, &id)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
                super::binding::retire_project_bindings(tx, &id, seq)?;
                if let Some(successor) = r.successor_project_id {
                    tx.execute(
                        "UPDATE project_root SET project_id=?1 WHERE project_id=?2",
                        params![successor, id],
                    )?;
                    tx.execute(
                        "UPDATE derived_root_parent SET project_id=?1 WHERE project_id=?2",
                        params![successor, id],
                    )?;
                    tx.execute(
                        "UPDATE project_alias SET project_id=?1 WHERE project_id=?2",
                        params![successor, id],
                    )?;
                    tx.execute("DELETE FROM project_alias WHERE old_id=?1", [&id])?;
                    tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![id,successor,now])?;
                } else {
                    tx.execute("DELETE FROM project_root WHERE project_id=?1", [&id])?;
                    tx.execute("DELETE FROM derived_root_parent WHERE project_id=?1", [&id])?;
                    tx.execute(
                        "DELETE FROM project_alias WHERE project_id=?1 OR old_id=?1",
                        [&id],
                    )?;
                }
                tx.execute("DELETE FROM project_workspace WHERE project_id=?1", [&id])?;
                tx.execute(
                    "DELETE FROM workspace_member WHERE ref_kind='local' AND project_id=?1",
                    [&id],
                )?;
                tx.execute("DELETE FROM project WHERE project_id=?1", [&id])?;
            }
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod agent_lifecycle_tests {
    use super::{tests::*, *};
    use crate::agent::{normalize_agent_name, AgentChangeEntry, AgentNameClaim};
    use rusqlite::types::Value as SqlValue;

    fn fixture(label: &str) -> Fixture {
        let mut f = Fixture::new(label);
        f.store.use_sequential_ids();
        f.store
            .apply_entry("agent.cutover", "{}", "module", None, |_| Ok(()))
            .unwrap();
        f
    }

    fn placed(f: &Fixture, project: &str, workspace: Option<&str>) {
        f.store
            .with_principal("direct")
            .register(RegisterRequest {
                project_id: Some(project.into()),
                name: project.into(),
                workspace_id: workspace.map(str::to_owned),
                ..Default::default()
            })
            .unwrap();
    }

    fn create(
        f: &Fixture,
        role: &str,
        project: Option<&str>,
        workspace: Option<&str>,
        name: &str,
    ) -> String {
        let value = result(&f.store.with_principal("reserved:prefrontal-core").agent_mutation(
            "agent.create",
            json!({"role":role,"project_id":project,"workspace_id":workspace,"name":name,"tag":"test","request_key":format!("create-{name}")}),
            10,
        ).unwrap());
        value["agent"]["agent_id"].as_str().unwrap().into()
    }

    fn dispose(f: &Fixture, id: &str) {
        f.store
            .agent_mutation(
                "agent.dispose",
                json!({"agent_id":id,"request_key":format!("dispose-{id}")}),
                20,
            )
            .unwrap();
    }

    // Journal a full imported row and claim to exercise bindings that normal
    // create cannot produce: an unplaced project or a different stored workspace.
    fn import_bound(f: &Fixture, role: &str, project: &str, stored_workspace: &str) -> String {
        let template = create(f, "assistant", None, None, "Template");
        let mut row = f.store.agent_row(&template).unwrap().unwrap();
        row.agent_id = "agent_16013c86".into();
        row.name = "Imported".into();
        row.role = role.into();
        row.project_id = Some(project.into());
        row.workspace_id = Some(stored_workspace.into());
        row.request_key = None;
        let id = row.agent_id.clone();
        f.store
            .apply_entry("agent.import", "{}", "test", None, |tx| {
                let claim_id = tx.query_row(
                    "SELECT COALESCE(MAX(claim_id),0)+1 FROM agent_name_claim",
                    [],
                    |r| r.get(0),
                )?;
                let claim = AgentNameClaim {
                    claim_id,
                    agent_id: id.clone(),
                    namespace_kind: "workspace".into(),
                    namespace_key: stored_workspace.into(),
                    normalized_name: normalize_agent_name(&row.name).unwrap().normalized_name,
                    name_normalization_version: 1,
                    display_name: row.name.clone(),
                    claimed_at_ms: 10,
                    released_at_ms: None,
                };
                let mut entry = AgentChangeEntry::new("agent.import", row.clone(), vec![claim]);
                entry.seq =
                    tx.query_row("SELECT MAX(seq) FROM registry_journal", [], |r| r.get(0))?;
                let payload = json!({"entry":entry});
                assert!(crate::agent::replay_agent_entry(
                    tx,
                    "agent.import",
                    &payload
                )?);
                tx.execute(
                    "UPDATE registry_journal SET payload_json=?1 WHERE seq=?2",
                    params![payload.to_string(), entry.seq],
                )?;
                Ok(())
            })
            .unwrap();
        id
    }

    fn projection(f: &Fixture) -> BTreeMap<&'static str, Vec<Vec<SqlValue>>> {
        f.store
            .read(|conn| {
                let mut tables = BTreeMap::new();
                for table in [
                    "project",
                    "project_root",
                    "project_alias",
                    "derived_root_parent",
                    "project_workspace",
                    "workspace_member",
                    "workspace",
                    "agent",
                    "agent_name_claim",
                    "registry_journal",
                    "root_binding",
                    "retired_binding",
                    "root_approval",
                ] {
                    let columns = conn
                        .prepare(&format!("SELECT * FROM {table}"))?
                        .column_count();
                    let order = (1..=columns)
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let rows = conn
                        .prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?
                        .query_map([], |row| {
                            (0..columns)
                                .map(|n| row.get(n))
                                .collect::<rusqlite::Result<Vec<SqlValue>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    tables.insert(table, rows);
                }
                Ok(tables)
            })
            .unwrap()
    }

    fn refused(f: &Fixture, call: impl FnOnce() -> Result<Vec<u8>, RegistryError>) {
        let before = projection(f);
        let error = call().unwrap_err();
        assert!(
            matches!(&error, RegistryError::Domain { code, .. } if code == "bound_by_live_agent"),
            "{error}"
        );
        assert_eq!(
            projection(f),
            before,
            "refusal changed stored state or journal"
        );
    }

    #[test]
    fn remove_bound_project_refuses_and_writes_nothing() {
        for role in ["head", "hiree"] {
            let f = fixture(role);
            placed(&f, "P", Some("W"));
            let id = create(&f, role, Some("P"), None, "Bound");
            refused(&f, || {
                f.store.remove(RemoveRequest {
                    project_id: Some("P".into()),
                    request_key: Some("remove".into()),
                    ..Default::default()
                })
            });
            dispose(&f, &id);
            f.store
                .remove(RemoveRequest {
                    project_id: Some("P".into()),
                    request_key: Some("remove".into()),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(f.store.agent_row(&id).unwrap().unwrap().status, "retired");
        }
    }

    #[test]
    fn project_merge_by_alias_refuses_two_live_heads() {
        let f = fixture("merge-heads");
        placed(&f, "P", Some("W"));
        placed(&f, "Q", Some("W"));
        let source = create(&f, "head", Some("P"), None, "Source");
        let target = create(&f, "head", Some("Q"), None, "Target");
        let req = RemoveRequest {
            project_id: Some("P".into()),
            successor_project_id: Some("Q".into()),
            request_key: Some("merge".into()),
            ..Default::default()
        };
        refused(&f, || f.store.remove(req.clone()));
        dispose(&f, &source);
        f.store.remove(req).unwrap();
        assert_eq!(
            f.store
                .agent_row(&target)
                .unwrap()
                .unwrap()
                .project_id
                .as_deref(),
            Some("Q")
        );
        assert_eq!(
            f.store
                .read(|conn| conn.query_row(
                    "SELECT project_id FROM project_alias WHERE old_id='P'",
                    [],
                    |r| r.get::<_, String>(0)
                ))
                .unwrap(),
            "Q"
        );
    }

    #[test]
    fn remove_bound_workspace_refuses_heads_and_live_hires() {
        for role in ["workspace_head", "head", "hiree"] {
            let f = fixture(role);
            placed(&f, "P", Some("W"));
            if role == "hiree" {
                let head = create(&f, "head", Some("P"), None, "RetiredHead");
                dispose(&f, &head);
            }
            let id = if role == "workspace_head" {
                create(&f, role, None, Some("W"), "Bound")
            } else {
                create(&f, role, Some("P"), None, "Bound")
            };
            let req = RemoveRequest {
                workspace_id: Some("W".into()),
                request_key: Some("remove-w".into()),
                ..Default::default()
            };
            refused(&f, || f.store.remove(req.clone()));
            dispose(&f, &id);
            f.store.remove(req).unwrap();
            assert!(f
                .store
                .read(|conn| conn
                    .query_row(
                        "SELECT workspace_id FROM project_workspace WHERE project_id='P'",
                        [],
                        |r| r.get::<_, String>(0)
                    )
                    .optional())
                .unwrap()
                .is_none());
        }
    }

    #[test]
    fn stored_workspace_alone_does_not_bind_but_project_placement_does() {
        for role in ["head", "hiree"] {
            let f = fixture(role);
            placed(&f, "P", Some("W2"));
            placed(&f, "Spare", Some("W1"));
            let id = import_bound(&f, role, "P", "W1");
            let row = f.store.agent_row(&id).unwrap();
            let claims = f.store.agent_claims(&id).unwrap();
            f.store
                .remove(RemoveRequest {
                    workspace_id: Some("W1".into()),
                    ..Default::default()
                })
                .unwrap();
            assert_eq!(f.store.agent_row(&id).unwrap(), row);
            assert_eq!(f.store.agent_claims(&id).unwrap(), claims);
            refused(&f, || {
                f.store.remove(RemoveRequest {
                    workspace_id: Some("W2".into()),
                    ..Default::default()
                })
            });
            let before = projection(&f);
            f.store.rebuild().unwrap();
            assert_eq!(projection(&f), before);
        }
    }

    #[test]
    fn assign_workspace_refuses_bound_placement_changes_but_allows_same_workspace() {
        for role in ["head", "hiree"] {
            for workspace in [Some("W1"), None] {
                let f = fixture(role);
                placed(&f, "P", workspace);
                if workspace.is_some() {
                    create(&f, role, Some("P"), None, "Bound");
                } else {
                    import_bound(&f, role, "P", "W1");
                }
                let req = AssignWorkspaceRequest {
                    project_id: "P".into(),
                    workspace_id: "W2".into(),
                    request_key: Some("move".into()),
                    ..Default::default()
                };
                refused(&f, || {
                    f.store.with_principal("direct").assign_workspace(req)
                });
                if workspace.is_some() {
                    let before = projection(&f);
                    let out = result(
                        &f.store
                            .assign_workspace(AssignWorkspaceRequest {
                                project_id: "P".into(),
                                workspace_id: "W1".into(),
                                ..Default::default()
                            })
                            .unwrap(),
                    );
                    assert_eq!(out["noop"], true);
                    assert_eq!(projection(&f), before);
                }
            }
        }
    }

    #[test]
    fn register_refuses_attaching_unplaced_bound_projects_but_allows_renames() {
        for role in ["head", "hiree"] {
            let f = fixture(role);
            placed(&f, "P", None);
            let id = import_bound(&f, role, "P", "W1");
            let claims = f.store.agent_claims(&id).unwrap();
            let mut req = RegisterRequest {
                project_id: Some("P".into()),
                name: "Renamed".into(),
                workspace_id: Some("W2".into()),
                roots: vec![f.dir("new-root")],
                request_key: Some("register".into()),
                ..Default::default()
            };
            refused(&f, || {
                f.store.with_principal("direct").register(req.clone())
            });
            req.workspace_id = None;
            f.store.register(req).unwrap();
            assert_eq!(f.store.agent_claims(&id).unwrap(), claims);
            let before = projection(&f);
            f.store.rebuild().unwrap();
            assert_eq!(projection(&f), before);
        }
    }

    #[test]
    fn upgrade_implicit_refuses_attaching_bound_projects_but_allows_renames() {
        for role in ["head", "hiree"] {
            let f = fixture(role);
            placed(&f, "P", None);
            let id = import_bound(&f, role, "P", "W1");
            let claims = f.store.agent_claims(&id).unwrap();
            let mut req = UpgradeImplicitRequest {
                implicit_id: "old-id".into(),
                project_id: Some("P".into()),
                name: "Renamed".into(),
                workspace_id: Some("W2".into()),
                roots: vec![f.dir("new-root")],
                request_key: Some("upgrade".into()),
                ..Default::default()
            };
            refused(&f, || {
                f.store
                    .with_principal("direct")
                    .upgrade_implicit(req.clone())
            });
            req.workspace_id = None;
            f.store.upgrade_implicit(req).unwrap();
            assert_eq!(f.store.agent_claims(&id).unwrap(), claims);
            let before = projection(&f);
            f.store.rebuild().unwrap();
            assert_eq!(projection(&f), before);
        }
    }

    #[test]
    fn seed_import_refuses_bound_attachment_and_rolls_back_the_whole_call() {
        for role in ["head", "hiree"] {
            let f = fixture(role);
            let identity = "git:bound";
            let project = seed_id(identity);
            let first = SeedPair {
                canonical_root: f.dir("first"),
                mc_identity: identity.into(),
                ..Default::default()
            };
            f.store
                .seed_import(SeedImportRequest {
                    source: "mc".into(),
                    payload: SeedPayload {
                        pairs: vec![first.clone()],
                        ..Default::default()
                    },
                    ..Default::default()
                })
                .unwrap();
            let id = import_bound(&f, role, &project, "W1");
            let req = SeedImportRequest {
                source: "mc".into(),
                request_key: Some("seed".into()),
                payload: SeedPayload {
                    pairs: vec![
                        first,
                        SeedPair {
                            canonical_root: f.dir("second"),
                            mc_identity: identity.into(),
                            ..Default::default()
                        },
                        SeedPair {
                            canonical_root: f.dir("unrelated"),
                            mc_identity: "aaa:earlier".into(),
                            ..Default::default()
                        },
                    ],
                    workspaces: vec![SeedWorkspace {
                        workspace_id: "W2".into(),
                        name: "Team".into(),
                    }],
                    members: vec![SeedMember {
                        mc_identity: identity.into(),
                        workspace_id: "W2".into(),
                    }],
                    ..Default::default()
                },
                ..Default::default()
            };
            refused(&f, || {
                f.store.with_principal("direct").seed_import(req.clone())
            });
            dispose(&f, &id);
            f.store.seed_import(req).unwrap();
            assert_eq!(
                f.store
                    .read(|conn| conn.query_row(
                        "SELECT workspace_id FROM project_workspace WHERE project_id=?1",
                        [&project],
                        |r| r.get::<_, String>(0)
                    ))
                    .unwrap(),
                "W2"
            );
            let before = projection(&f);
            f.store.rebuild().unwrap();
            assert_eq!(projection(&f), before);
        }
    }

    #[test]
    fn all_terminal_agents_allow_operator_moves_and_new_workspace_creation() {
        let f = fixture("terminal-move");
        placed(&f, "P", Some("W1"));
        let head = create(&f, "head", Some("P"), None, "Head");
        let hire = create(&f, "hiree", Some("P"), None, "Hire");
        dispose(&f, &head);
        let target = create(&f, "assistant", None, None, "Target");
        f.store
            .agent_mutation(
                "agent.merge",
                json!({"agent_id":hire,"into_agent_id":target,"request_key":"merge-hire"}),
                30,
            )
            .unwrap();
        let claims = f.store.agent_claims(&hire).unwrap();
        f.store
            .with_principal("direct")
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "P".into(),
                workspace_id: "New".into(),
                workspace_name: Some("Team".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(f.store.read(|conn| conn.query_row("SELECT w.workspace_id,w.name FROM workspace w JOIN project_workspace pw USING(workspace_id) WHERE pw.project_id='P'", [], |r| Ok((r.get::<_, String>(0)?,r.get::<_, String>(1)?)))).unwrap(), ("New".into(), "Team".into()));
        assert_eq!(f.store.agent_claims(&hire).unwrap(), claims);
        let before = projection(&f);
        f.store.rebuild().unwrap();
        assert_eq!(projection(&f), before);
    }

    #[test]
    fn rebuild_refuses_historical_ops_that_change_live_bindings_and_rolls_back() {
        for op in [
            "register",
            "assign_workspace",
            "upgrade_implicit",
            "seed_import",
            "remove-project",
            "remove-workspace",
        ] {
            let f = fixture(op);
            let project = seed_id("git:bound");
            let root = f.dir("root");
            let request = match op {
                "register" => json!({"projectId":project,"name":"Renamed","workspaceId":"W2"}),
                "assign_workspace" => json!({"projectId":project,"workspaceId":"W2"}),
                "upgrade_implicit" => {
                    json!({"implicitId":"old","projectId":project,"name":"Renamed","workspaceId":"W2"})
                }
                "seed_import" => {
                    json!({"source":"mc","payload":{"pairs":[{"canonicalRoot":root,"mcIdentity":"git:bound"}],"workspaces":[{"workspaceId":"W2","name":"Team"}],"members":[{"mcIdentity":"git:bound","workspaceId":"W2"}]}})
                }
                "remove-project" => json!({"projectId":project}),
                "remove-workspace" => json!({"workspaceId":"W1"}),
                _ => unreachable!(),
            };
            if op == "seed_import" {
                f.store
                    .seed_import(SeedImportRequest {
                        source: "mc".into(),
                        payload: SeedPayload {
                            pairs: vec![SeedPair {
                                canonical_root: f.dir("initial"),
                                mc_identity: "git:bound".into(),
                                ..Default::default()
                            }],
                            ..Default::default()
                        },
                        ..Default::default()
                    })
                    .unwrap();
            } else {
                placed(&f, &project, (op == "remove-workspace").then_some("W1"));
            }
            import_bound(&f, "head", &project, "W1");
            let journal_op = if op.starts_with("remove-") {
                "remove"
            } else {
                op
            };
            f.store
                .apply_entry(journal_op, &request.to_string(), "test", None, |_| Ok(()))
                .unwrap();
            let before = projection(&f);
            let error = f.store.rebuild().unwrap_err();
            assert!(
                error.to_string().contains("bound_by_live_agent"),
                "{op}: {error}"
            );
            assert_eq!(projection(&f), before, "failed rebuild changed {op} state");
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
    use std::{
        fs,
        path::PathBuf,
        sync::atomic::{AtomicU64, Ordering},
    };

    static NEXT: AtomicU64 = AtomicU64::new(0);
    pub(crate) struct Fixture {
        pub(crate) root: PathBuf,
        pub(crate) store: RegistryStore,
    }
    impl Fixture {
        pub(crate) fn new(label: &str) -> Self {
            let root = std::env::temp_dir().join(format!(
                "ck-entorhinal-mutations-{label}-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            fs::create_dir_all(&root).unwrap();
            let descriptor = StorageDescriptor {
                module_id: format!("test-{label}-{}", NEXT.load(Ordering::Relaxed)),
                storage_namespace: "tests".into(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            Self {
                root,
                store: RegistryStore::open(&descriptor).unwrap(),
            }
        }
        pub(crate) fn dir(&self, name: &str) -> String {
            let path = self.root.join(name);
            fs::create_dir_all(&path).unwrap();
            fs::canonicalize(path)
                .unwrap()
                .to_string_lossy()
                .into_owned()
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.root);
        }
    }
    pub(crate) fn result(blob: &[u8]) -> Value {
        serde_json::from_slice::<Value>(blob).unwrap()["result"].clone()
    }
    pub(crate) fn code(error: RegistryError, expected: &str) {
        assert!(error.to_string().starts_with(expected), "{error}");
    }
    pub(crate) fn register(f: &Fixture, id: &str, root: String) -> Vec<u8> {
        f.store
            .register(RegisterRequest {
                project_id: Some(id.into()),
                name: id.into(),
                roots: vec![root],
                ..Default::default()
            })
            .unwrap()
    }

    #[test]
    fn register_happy_path_universal_alias_and_closed_noop() {
        let f = Fixture::new("register");
        let root = f.dir("root");
        let first = register(&f, "p1", root.clone());
        assert_eq!(result(&first)["generation"], 1);
        assert_eq!(
            f.store
                .resolve_project_id(&implicit_project_id(&root))
                .unwrap()
                .via,
            "alias"
        );
        let second = f
            .store
            .register(RegisterRequest {
                project_id: Some("p1".into()),
                name: "p1".into(),
                roots: vec![root],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&second)["noop"], true);
        assert_eq!(result(&second)["generation"], 1);
    }

    #[test]
    fn request_key_replay_is_byte_identical_and_noop_is_not_frozen() {
        let f = Fixture::new("request-key");
        let root = f.dir("root");
        let request = RegisterRequest {
            project_id: Some("p".into()),
            name: "p".into(),
            roots: vec![root],
            request_key: Some("rk".into()),
            ..Default::default()
        };
        let first = f.store.register(request.clone()).unwrap();
        let replay = f.store.register(request).unwrap();
        assert_eq!(first, replay);
        assert_eq!(f.store.generation().unwrap(), 1);
    }

    #[test]
    fn register_rejections_are_typed_and_name_the_owner() {
        let f = Fixture::new("reject");
        let root = f.dir("root");
        code(
            f.store
                .register(RegisterRequest {
                    project_id: Some("pj-implicit1-bad".into()),
                    name: "x".into(),
                    roots: vec![],
                    ..Default::default()
                })
                .unwrap_err(),
            "reserved_project_id_namespace",
        );
        code(
            f.store
                .register(RegisterRequest {
                    project_id: Some("x".into()),
                    name: "x".into(),
                    roots: vec![format!("{root}/.")],
                    ..Default::default()
                })
                .unwrap_err(),
            "not_canonical",
        );
        register(&f, "owner", root.clone());
        let conflict = f
            .store
            .register(RegisterRequest {
                project_id: Some("other".into()),
                name: "other".into(),
                roots: vec![root.clone()],
                ..Default::default()
            })
            .unwrap_err();
        assert!(conflict.to_string().contains("owner"));
        code(
            f.store
                .assign_workspace(AssignWorkspaceRequest {
                    project_id: "missing".into(),
                    workspace_id: "w".into(),
                    ..Default::default()
                })
                .unwrap_err(),
            "not_found",
        );
        code(
            f.store
                .remove(RemoveRequest {
                    project_id: Some("missing".into()),
                    ..Default::default()
                })
                .unwrap_err(),
            "not_found",
        );
    }

    /// A project filed in the wrong workspace can be moved to another one.
    ///
    /// Asserts the move BOTH ways round: present under the new workspace AND
    /// absent from the old. Only the first half would pass if the old
    /// membership row were left behind, and a project listed under two
    /// workspaces is the shape that makes `enumerate` lie.
    #[test]
    fn assign_workspace_moves_between_workspaces() {
        let f = Fixture::new("assign_move");
        let root = f.dir("root");
        register(&f, "p", root);
        for ws in ["w1", "w2"] {
            f.store
                .assign_workspace(AssignWorkspaceRequest {
                    project_id: "p".into(),
                    workspace_id: ws.into(),
                    ..Default::default()
                })
                .unwrap();
        }
        assert_eq!(f.store.enumerate(Some("w2")).unwrap().projects.len(), 1);
        assert_eq!(
            f.store.enumerate(Some("w1")).unwrap().projects.len(),
            0,
            "project still listed under the workspace it was moved out of"
        );
        // The unscoped enumerate carries placement per row (the peer-roster
        // join field): placed projects say where, unplaced say nothing.
        let all = f.store.enumerate(None).unwrap();
        let placed = all.projects.iter().find(|p| p.project_id == "p").unwrap();
        assert_eq!(
            placed.workspace_id.as_deref(),
            Some("w2"),
            "enumerate must carry the project's current workspace"
        );
        let unplaced_root = f.dir("unplaced");
        register(&f, "q", unplaced_root);
        let all = f.store.enumerate(None).unwrap();
        let unplaced = all.projects.iter().find(|p| p.project_id == "q").unwrap();
        assert_eq!(
            unplaced.workspace_id, None,
            "an unplaced project must carry NO workspace, not an empty one"
        );
    }

    /// THE LOAD-BEARING ONE: a rebuild must land on the LAST assignment.
    ///
    /// The tables are a projection of the journal, so a move the live path
    /// applies and `replay()` does not would revert on the next `rebuild()`
    /// with nothing reporting a failure -- the move would simply be gone.
    ///
    /// It asserts through BOTH read surfaces because they read different
    /// tables that the replay arm writes separately: `resolve` reads
    /// `project_workspace`, `enumerate` reads `workspace_member`. Asserting
    /// only through `enumerate` would leave the `project_workspace` write
    /// unchecked, and the two can disagree.
    #[test]
    fn assign_workspace_replay_converges_on_last_assignment() {
        let f = Fixture::new("assign_replay");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        for ws in ["w1", "w2"] {
            f.store
                .assign_workspace(AssignWorkspaceRequest {
                    project_id: "p".into(),
                    workspace_id: ws.into(),
                    ..Default::default()
                })
                .unwrap();
        }
        f.store.rebuild().unwrap();
        assert_eq!(
            f.store.resolve(&root).unwrap().workspace_id.as_deref(),
            Some("w2"),
            "rebuild did not converge on the last assignment"
        );
        assert_eq!(
            f.store.enumerate(Some("w2")).unwrap().projects.len(),
            1,
            "rebuild left the project out of its current workspace"
        );
        assert_eq!(
            f.store.enumerate(Some("w1")).unwrap().projects.len(),
            0,
            "rebuild left the project under its former workspace"
        );
    }

    #[test]
    fn assign_workspace_dual_write_and_noop() {
        let f = Fixture::new("assign");
        let root = f.dir("root");
        register(&f, "p", root);
        let a = f
            .store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&a)["generation"], 2);
        let e = f.store.enumerate(Some("w")).unwrap();
        assert_eq!(e.projects.len(), 1);
        let n = f
            .store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&n)["noop"], true);
        assert_eq!(f.store.generation().unwrap(), 2);
    }

    #[test]
    fn upgrade_creates_explicit_and_preserves_old_id_alias() {
        let f = Fixture::new("upgrade");
        let root = f.dir("root");
        let old = implicit_project_id(&root);
        let out = f
            .store
            .upgrade_implicit(UpgradeImplicitRequest {
                implicit_id: old.clone(),
                project_id: Some("explicit".into()),
                name: "named".into(),
                root: Some(root),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&out)["projectId"], "explicit");
        let resolved = f.store.resolve_project_id(&old).unwrap();
        assert_eq!(resolved.project_id, "explicit");
        assert!(!resolved.gone);
    }

    #[test]
    fn remove_merge_disposition_keeps_remote_and_drops_local_membership() {
        let f = Fixture::new("remove");
        let r1 = f.dir("one");
        let r2 = f.dir("two");
        register(&f, "removed", r1.clone());
        register(&f, "successor", r2);
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "removed".into(),
                workspace_id: "old-w".into(),
                ..Default::default()
            })
            .unwrap();
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "successor".into(),
                workspace_id: "new-w".into(),
                ..Default::default()
            })
            .unwrap();
        f.store.apply_entry("remote", "{}", "test", None, |tx| { tx.execute("INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES('old-w','remote','device','removed')",[])?; Ok(()) }).unwrap();
        let out = f
            .store
            .remove(RemoveRequest {
                project_id: Some("removed".into()),
                successor_project_id: Some("successor".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&out)["droppedWorkspaceId"], "old-w");
        assert_eq!(f.store.resolve(&r1).unwrap().project_id, "successor");
        assert_eq!(
            f.store.resolve_project_id("removed").unwrap().project_id,
            "successor"
        );
        let remote: i64 = f
            .store
            .db
            .with_conn(|c| {
                c.query_row(
                    "SELECT COUNT(*) FROM workspace_member WHERE ref_kind='remote'",
                    [],
                    |r| r.get(0),
                )
            })
            .unwrap();
        assert_eq!(remote, 1);
        let local_old:i64=f.store.db.with_conn(|c|c.query_row("SELECT COUNT(*) FROM workspace_member WHERE ref_kind='local' AND project_id='removed'",[],|r|r.get(0))).unwrap();
        assert_eq!(local_old, 0);
    }

    #[test]
    fn seed_import_is_deterministic_rejoined_and_records_identity_split() {
        let f = Fixture::new("seed");
        let owned = f.dir("owned");
        let fresh = f.dir("fresh");
        register(&f, "existing", owned.clone());
        let request = SeedImportRequest {
            source: "mc".into(),
            request_key: None,
            payload: SeedPayload {
                pairs: vec![
                    SeedPair {
                        canonical_root: owned.clone(),
                        mc_identity: "git:one".into(),
                        identity_class: Some("git".into()),
                        ..Default::default()
                    },
                    SeedPair {
                        canonical_root: fresh.clone(),
                        mc_identity: "git:one".into(),
                        identity_class: Some("git".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let first = f.store.seed_import(request.clone()).unwrap();
        let second = f.store.seed_import(request).unwrap();
        assert!(result(&first)["report"].is_array());
        assert!(result(&second)["report"].is_array());
        assert_eq!(result(&second)["noop"], true);
        assert!(result(&first)["report"]
            .as_array()
            .unwrap()
            .iter()
            .any(|v| v["kind"] == "identity_split"));
        let id: Option<String> = f
            .store
            .db
            .with_conn(|c| {
                c.query_row(
                    "SELECT seed_identity FROM project WHERE project_id LIKE 'pj-%'",
                    [],
                    |r| r.get(0),
                )
                .optional()
            })
            .unwrap();
        assert_eq!(id.as_deref(), Some("git:one"));
    }

    #[test]
    fn rebuild_replays_journal_and_preserves_generation_and_seed_provenance() {
        let f = Fixture::new("rebuild");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        let before: Vec<(String, String)> = f
            .store
            .db
            .with_conn(|c| {
                let mut s = c.prepare(
                    "SELECT canonical_root,project_id FROM project_root ORDER BY canonical_root",
                )?;
                let rows = s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect();
                rows
            })
            .unwrap();
        let generation = f.store.generation().unwrap();
        assert_eq!(f.store.rebuild().unwrap().generation, generation);
        let after: Vec<(String, String)> = f
            .store
            .db
            .with_conn(|c| {
                let mut s = c.prepare(
                    "SELECT canonical_root,project_id FROM project_root ORDER BY canonical_root",
                )?;
                let rows = s.query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?.collect();
                rows
            })
            .unwrap();
        assert_eq!(before, after);
    }

    #[test]
    fn every_effectful_mutation_advances_generation_and_every_noop_does_not() {
        let f = Fixture::new("i8");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        let g = f.store.generation().unwrap();
        let noop = f
            .store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "p".into(),
                roots: vec![root],
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&noop)["generation"], g);
        let assigned = f
            .store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&assigned)["generation"], g + 1);
        let noassign = f
            .store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(result(&noassign)["generation"], g + 1);
    }

    /// The generation must never decrease, and must never hand the same value to
    /// two different states, across a journal replay.
    ///
    /// This is an invariant rather than an implementation detail because
    /// consumers outside this module use the generation to decide whether
    /// something minted against an earlier identity snapshot is still valid. A
    /// generation that repeated after a replay would re-validate a stale
    /// assertion silently: nothing would error, and the holder would speak with
    /// authority it no longer has. So replay has to preserve the value AND leave
    /// the next mutation issuing a seq above every value ever issued, which is
    /// the part a reader cannot verify from `MAX(seq)` alone.
    #[test]
    fn generation_never_decreases_or_repeats_across_a_journal_replay() {
        let f = Fixture::new("generation-replay");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                ..Default::default()
            })
            .unwrap();
        let before = f.store.generation().unwrap();
        assert!(before > 0, "the fixture must have advanced the generation");

        assert_eq!(
            f.store.rebuild().unwrap().generation,
            before,
            "replay must not move the generation"
        );
        assert_eq!(
            f.store.generation().unwrap(),
            before,
            "the generation after replay must be the one replay reported"
        );

        // The claim that matters: the next effectful mutation must issue a value
        // ABOVE everything already issued, so no post-replay state can wear a
        // generation an earlier state already wore.
        let after = f
            .store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w2".into(),
                ..Default::default()
            })
            .unwrap();
        let next = result(&after)["generation"].as_i64().unwrap();
        assert!(
            next > before,
            "a mutation after replay must advance past every earlier generation, got {next} after {before}"
        );

        // And a second replay from the longer journal is still stable.
        assert_eq!(
            f.store.rebuild().unwrap().generation,
            next,
            "replay must stay stable once the journal has grown"
        );
    }
}

#[cfg(test)]
mod adversarial_tests {
    use super::tests::*;
    use super::*;
    use std::{fs, path::PathBuf};

    #[test]
    fn ancestry_rejects_both_directions_and_workspace_i2() {
        let f = Fixture::new("ancestry");
        let tree = f.dir("tree");
        let child = PathBuf::from(&tree).join("child");
        fs::create_dir_all(&child).unwrap();
        let child = fs::canonicalize(child)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        f.store
            .register(RegisterRequest {
                project_id: Some("foreign".into()),
                name: "foreign".into(),
                derived_root_parents: vec![tree.clone()],
                ..Default::default()
            })
            .unwrap();
        code(
            f.store
                .register(RegisterRequest {
                    project_id: Some("late".into()),
                    name: "late".into(),
                    roots: vec![child],
                    ..Default::default()
                })
                .unwrap_err(),
            "root_under_foreign_derived_parent",
        );
        let root2 = f.dir("root2");
        f.store
            .register(RegisterRequest {
                project_id: Some("root-owner".into()),
                name: "root-owner".into(),
                roots: vec![root2.clone()],
                ..Default::default()
            })
            .unwrap();
        let parent2 = root2.clone();
        code(
            f.store
                .register(RegisterRequest {
                    project_id: Some("parent-owner".into()),
                    name: "parent-owner".into(),
                    derived_root_parents: vec![parent2],
                    ..Default::default()
                })
                .unwrap_err(),
            "derived_parent_contains_foreign_root",
        );
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "foreign".into(),
                workspace_id: "w1".into(),
                ..Default::default()
            })
            .unwrap();
        // A SECOND ASSIGNMENT MOVES THE PROJECT; it does not violate I2.
        //
        // This asserted `workspace_conflict` and was named for I2, but refusing
        // a move never enforced I2 -- the primary key on project_workspace
        // does, and it holds equally after a move because the project still
        // belongs to exactly one workspace. The refusal was a separate
        // immutability rule that I2 did not require, and it left a user who
        // filed a project in the wrong workspace with no way to correct it.
        //
        // The spec's "I2 is never auto-resolved by preference" is scoped to
        // seed_import, where one project is claimed by several workspaces at
        // once and the importer has no basis to choose. An explicit
        // assign_workspace is the operator stating the preference, which is the
        // case that clause exists to defer to.
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "foreign".into(),
                workspace_id: "w2".into(),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            f.store.enumerate(Some("w2")).unwrap().projects.len(),
            1,
            "explicit re-assignment must move the project"
        );
    }

    #[test]
    fn seed_import_home_guard_excludes_by_default_and_flag_disables() {
        let home = std::env::var("HOME").unwrap();
        let f = Fixture::new("seed-home");
        let live = f.dir("normal-project");
        let build = |exclude: bool| SeedImportRequest {
            source: "mc".into(),
            request_key: None,
            actor: None,
            exclude_home_scoped: exclude,
            payload: SeedPayload {
                pairs: vec![
                    SeedPair {
                        canonical_root: home.clone(),
                        mc_identity: "dir:home".into(),
                        identity_class: Some("dir".into()),
                        ..Default::default()
                    },
                    SeedPair {
                        canonical_root: live.clone(),
                        mc_identity: "git:cccc".into(),
                        identity_class: Some("git".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            },
        };
        // Default: $HOME row skips with home_scoped provenance, normal row mints.
        let out = f.store.seed_import(build(true)).unwrap();
        let v = result(&out);
        let entries = v["report"].as_array().unwrap();
        assert!(
            entries
                .iter()
                .any(|e| e["kind"] == "skipped" && e["reason"] == "home_scoped"),
            "$HOME root must skip with home_scoped provenance"
        );
        assert!(f.store.resolve(&live).is_ok());
        let gone = f.store.resolve_project_id(&seed_id("dir:home")).unwrap();
        assert!(gone.gone, "$HOME group must not mint under the guard");
        // Contrastive: guard off (stress runs) imports the row.
        let f2 = Fixture::new("seed-home-off");
        let out2 = f2.store.seed_import(build(false)).unwrap();
        let v2 = result(&out2);
        assert!(
            !v2["report"]
                .as_array()
                .unwrap()
                .iter()
                .any(|e| e["reason"] == "home_scoped"),
            "guard off must not skip"
        );
        let minted = f2.store.resolve_project_id(&seed_id("dir:home")).unwrap();
        assert!(!minted.gone, "guard off mints the $HOME project");
    }

    #[test]
    fn seed_import_skips_dead_roots_with_provenance() {
        let f = Fixture::new("seed-dead");
        let live = f.dir("alive");
        let dead = format!("{}/long-gone-worktree", f.root.display());
        let out = f
            .store
            .seed_import(SeedImportRequest {
                source: "mc".into(),
                request_key: None,
                actor: None,
                exclude_home_scoped: true,
                payload: SeedPayload {
                    pairs: vec![
                        SeedPair {
                            canonical_root: live.clone(),
                            mc_identity: "git:aaaa".into(),
                            identity_class: Some("git".into()),
                            ..Default::default()
                        },
                        SeedPair {
                            canonical_root: dead.clone(),
                            mc_identity: "git:aaaa".into(),
                            identity_class: Some("git".into()),
                            ..Default::default()
                        },
                        SeedPair {
                            canonical_root: format!("{dead}-2"),
                            mc_identity: "git:bbbb".into(),
                            identity_class: Some("git".into()),
                            ..Default::default()
                        },
                    ],
                    ..Default::default()
                },
            })
            .unwrap();
        let v = result(&out);
        let entries = v["report"].as_array().unwrap();
        let skipped: Vec<_> = entries
            .iter()
            .filter(|e| e["kind"] == "skipped" && e["reason"] == "missing")
            .collect();
        assert_eq!(skipped.len(), 2, "both dead roots skip with provenance");
        // The all-dead identity mints nothing; the survivor identity minted with its live root.
        assert!(f.store.resolve(&live).is_ok());
        let gone = f.store.resolve_project_id(&seed_id("git:bbbb")).unwrap();
        assert!(gone.gone, "all-dead group must not mint a project");
        // Journal payload records survivors only: rebuild works with dead paths still absent.
        f.store.rebuild().unwrap();
        assert!(f.store.resolve(&live).is_ok());
    }

    #[test]
    fn seed_import_records_alias_occupied_identity_class_and_multi_workspace() {
        let f = Fixture::new("seed-adversarial");
        let root = f.dir("root");
        let occupied = seed_id("git:occupied");
        register(&f, &occupied, root.clone());
        let successor_root = f.dir("successor");
        register(&f, "successor", successor_root);
        f.store
            .remove(RemoveRequest {
                project_id: Some(occupied),
                successor_project_id: Some("successor".into()),
                ..Default::default()
            })
            .unwrap();
        let a = f.dir("a");
        let b = f.dir("b");
        let request = SeedImportRequest {
            source: "mc".into(),
            payload: SeedPayload {
                pairs: vec![
                    SeedPair {
                        canonical_root: a.clone(),
                        mc_identity: "git:mix".into(),
                        identity_class: Some("git".into()),
                        ..Default::default()
                    },
                    SeedPair {
                        canonical_root: a.clone(),
                        mc_identity: "dir:mix".into(),
                        identity_class: Some("dir".into()),
                        ..Default::default()
                    },
                    SeedPair {
                        canonical_root: b.clone(),
                        mc_identity: "git:occupied".into(),
                        identity_class: Some("git".into()),
                        ..Default::default()
                    },
                ],
                members: vec![
                    SeedMember {
                        mc_identity: "git:mix".into(),
                        workspace_id: "w1".into(),
                    },
                    SeedMember {
                        mc_identity: "git:mix".into(),
                        workspace_id: "w2".into(),
                    },
                ],
                workspaces: vec![
                    SeedWorkspace {
                        workspace_id: "w1".into(),
                        name: "one".into(),
                    },
                    SeedWorkspace {
                        workspace_id: "w2".into(),
                        name: "two".into(),
                    },
                ],
                ..Default::default()
            },
            ..Default::default()
        };
        let out = result(&f.store.seed_import(request).unwrap());
        let kinds = out["report"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|v| v["kind"].as_str())
            .collect::<Vec<_>>();
        assert!(kinds.contains(&"identity_class_precedence"));
        assert!(kinds.contains(&"alias_occupied"));
        assert!(kinds.contains(&"multi_workspace"));
    }
}

#[cfg(test)]
mod audit_fix_tests {
    use super::tests::Fixture;
    use super::*;

    /// Audit finding (glm C1 / grok c3): a domain error surfaced by the action
    /// closure must ROLL BACK the whole transaction. with_conn_fenced commits
    /// on any Ok, so the old Ok(Err(_)) shape would have committed partial
    /// writes. Contrastive: the failing register (root conflict) must leave
    /// the journal length and generation untouched.
    #[test]
    fn domain_error_rolls_back_instead_of_committing_partial_state() {
        let f = Fixture::new("rollback");
        let store = &f.store;
        let root_a = f.dir("a");
        let root_b = f.dir("b");
        store
            .register(RegisterRequest {
                project_id: Some("p1".into()),
                name: "one".into(),
                roots: vec![root_a.clone()],
                derived_root_parents: vec![],
                workspace_id: None,
                request_key: None,
                actor: None,
            })
            .unwrap();
        let generation_before = store.generation().unwrap();
        // Second project claiming p1's root: domain error root_conflict.
        let err = store
            .register(RegisterRequest {
                project_id: Some("p2".into()),
                name: "two".into(),
                roots: vec![root_a.clone()],
                derived_root_parents: vec![],
                workspace_id: None,
                request_key: Some("rk-conflict".into()),
                actor: None,
            })
            .unwrap_err();
        assert!(matches!(err, RegistryError::Domain { ref code, .. } if code == "root_conflict"));
        // Nothing committed: generation unchanged, no journal row carries the
        // failed request_key, and p2 does not exist.
        assert_eq!(store.generation().unwrap(), generation_before);
        let resolved = store.resolve_project_id("p2");
        assert!(
            resolved.is_err() || resolved.unwrap().gone,
            "failed register must not mint the project"
        );
        // The failed request_key must not be poisoned: a corrected retry with
        // the same key succeeds rather than serving a cached NULL.
        store
            .register(RegisterRequest {
                project_id: Some("p2".into()),
                name: "two".into(),
                roots: vec![root_b],
                derived_root_parents: vec![],
                workspace_id: None,
                request_key: Some("rk-conflict".into()),
                actor: None,
            })
            .unwrap();
    }

    /// Audit finding (glm C15): request_key idempotency must be scoped to the
    /// op kind — the same key on a DIFFERENT op must not serve the first op's
    /// cached reply.
    #[test]
    fn request_key_cache_is_op_scoped() {
        let f = Fixture::new("opscope");
        let store = &f.store;
        let root_a = f.dir("a");
        let blob = store
            .register(RegisterRequest {
                project_id: Some("p1".into()),
                name: "one".into(),
                roots: vec![root_a],
                derived_root_parents: vec![],
                workspace_id: None,
                request_key: Some("shared-key".into()),
                actor: None,
            })
            .unwrap();
        let registered: serde_json::Value = serde_json::from_slice(&blob).unwrap();
        assert_eq!(registered["result"]["projectId"], "p1");
        // Same request_key, different op: typed refusal — never the other
        // op's cached reply (silent corruption) and never a raw UNIQUE
        // constraint error.
        let err = store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p1".into(),
                workspace_id: "w1".into(),
                workspace_name: None,
                request_key: Some("shared-key".into()),
                actor: None,
            })
            .unwrap_err();
        assert!(
            matches!(err, RegistryError::Domain { ref code, .. } if code == "request_key_reused_across_ops"),
            "cross-op key reuse must refuse typed, got: {err}"
        );
        // Contrastive: SAME op with the same key replays the cached reply.
        let replay = store
            .register(RegisterRequest {
                project_id: Some("ignored".into()),
                name: "ignored".into(),
                roots: vec![],
                derived_root_parents: vec![],
                workspace_id: None,
                request_key: Some("shared-key".into()),
                actor: None,
            })
            .unwrap();
        let replayed: serde_json::Value = serde_json::from_slice(&replay).unwrap();
        assert_eq!(
            replayed["result"], registered["result"],
            "same-op replay must serve the cached reply byte-for-byte"
        );
    }

    /// Audit finding (sol C4): the home guard's contract is XDG-aware. An
    /// explicitly relocated XDG home outside $HOME must still be guarded.
    #[test]
    fn home_guard_consults_relocated_xdg_homes() {
        let check = |root: &str| {
            is_home_scoped_against(root, Some("/Users/u"), Some("/srv/xdg/config"), None)
        };
        // Ancestor-or-equal of the relocated XDG config home: guarded.
        assert!(check("/srv/xdg"));
        assert!(check("/srv/xdg/config"));
        // Unrelated path outside both trees: admitted.
        assert!(!check("/srv/projects/app"));
        // $HOME and its ancestors remain guarded; filesystem root always.
        assert!(check("/Users/u"));
        assert!(check("/Users"));
        assert!(check("/"));
        // A CHILD of the XDG home is not an ancestor: admitted (matches the
        // $HOME semantics where only home itself and ancestors guard).
        assert!(!is_home_scoped_against(
            "/srv/xdg/config/app",
            Some("/Users/u"),
            Some("/srv/xdg/config"),
            None
        ));
    }
}

#[cfg(test)]
mod workspace_root_tests {
    use super::tests::*;
    use super::*;

    fn placed_project(f: &Fixture) -> String {
        let root = f.dir("member");
        f.store
            .register(RegisterRequest {
                project_id: Some("member".into()),
                name: "member".into(),
                workspace_id: Some("ws".into()),
                roots: vec![root.clone()],
                ..Default::default()
            })
            .unwrap();
        root
    }

    fn set(f: &Fixture, workspace: &str, root: Option<&str>) -> Result<Vec<u8>, RegistryError> {
        f.store.set_workspace_root(SetWorkspaceRootRequest {
            workspace_id: workspace.into(),
            root: root.map(str::to_string),
            ..Default::default()
        })
    }

    fn journal_len(f: &Fixture) -> i64 {
        f.store.generation().unwrap()
    }

    #[test]
    fn a_set_root_reaches_resolve_and_enumerate_and_clearing_returns_null() {
        let f = Fixture::new("ws-root-set");
        let member = placed_project(&f);
        let before = f.store.resolve(&member).unwrap();
        assert_eq!(before.workspace_id.as_deref(), Some("ws"));
        assert_eq!(
            before.workspace_root, None,
            "no root until the operator sets one"
        );

        let root = f.dir("workspace-root");
        set(&f, "ws", Some(&root)).unwrap();
        assert_eq!(
            f.store.resolve(&member).unwrap().workspace_root.as_deref(),
            Some(root.as_str())
        );
        let listed = f.store.enumerate(None).unwrap();
        assert_eq!(listed.workspaces[0].root.as_deref(), Some(root.as_str()));

        set(&f, "ws", None).unwrap();
        assert_eq!(f.store.resolve(&member).unwrap().workspace_root, None);
    }

    /// The key is always on the wire, as null when unset, so a reader can tell
    /// "this entorhinal has no root to report" from "this entorhinal predates
    /// the field".
    #[test]
    fn the_reply_carries_the_key_even_when_null() {
        let f = Fixture::new("ws-root-wire");
        let member = placed_project(&f);
        let wire = serde_json::to_value(f.store.resolve(&member).unwrap()).unwrap();
        assert!(
            wire.as_object().unwrap().contains_key("workspaceRoot"),
            "{wire}"
        );
        assert!(wire["workspaceRoot"].is_null());
        let listed = serde_json::to_value(f.store.enumerate(None).unwrap()).unwrap();
        assert!(
            listed["workspaces"][0]
                .as_object()
                .unwrap()
                .contains_key("root"),
            "{listed}"
        );
    }

    #[test]
    fn refusals_are_named_and_write_nothing() {
        let f = Fixture::new("ws-root-refuse");
        placed_project(&f);
        let head = journal_len(&f);
        code(
            set(&f, "ws", Some("relative/dir")).unwrap_err(),
            "not_absolute",
        );
        code(
            set(&f, "ws", Some("/definitely/not/here/xyz")).unwrap_err(),
            "root_not_found",
        );
        let file = format!("{}/a-file", f.dir("holder"));
        std::fs::write(&file, b"x").unwrap();
        code(set(&f, "ws", Some(&file)).unwrap_err(), "not_a_directory");
        let root = f.dir("workspace-root");
        code(
            set(&f, "no-such-workspace", Some(&root)).unwrap_err(),
            "not_found",
        );
        assert_eq!(
            journal_len(&f),
            head,
            "a refused call must not reach the journal"
        );
    }

    #[test]
    fn setting_the_same_value_appends_nothing() {
        let f = Fixture::new("ws-root-noop");
        placed_project(&f);
        let root = f.dir("workspace-root");
        set(&f, "ws", Some(&root)).unwrap();
        let head = journal_len(&f);
        set(&f, "ws", Some(&root)).unwrap();
        assert_eq!(journal_len(&f), head);
    }

    /// The tables are a projection of the journal: a root that the live path
    /// sets and replay drops would silently vanish on the next `rebuild`.
    /// Replay must also reproduce the LAST value, and must not re-read disk,
    /// so a root directory removed after it was set still rebuilds to it.
    /// Placement made at registration survives a rebuild. Replay of
    /// `register` used to skip the workspace, so every project placed that way
    /// came back unplaced, with nothing reporting a failure.
    #[test]
    fn rebuild_keeps_a_workspace_placement_made_at_registration() {
        let f = Fixture::new("ws-register-replay");
        let member = placed_project(&f);
        f.store.rebuild().unwrap();
        assert_eq!(
            f.store.resolve(&member).unwrap().workspace_id.as_deref(),
            Some("ws")
        );
    }

    #[test]
    fn rebuild_reproduces_the_last_root_without_rereading_disk() {
        let f = Fixture::new("ws-root-replay");
        let member = placed_project(&f);
        let first = f.dir("first-root");
        let last = f.dir("last-root");
        set(&f, "ws", Some(&first)).unwrap();
        set(&f, "ws", Some(&last)).unwrap();
        std::fs::remove_dir(&last).unwrap();
        f.store.rebuild().unwrap();
        assert_eq!(
            f.store.resolve(&member).unwrap().workspace_root.as_deref(),
            Some(last.as_str())
        );

        set(&f, "ws", None).unwrap();
        f.store.rebuild().unwrap();
        assert_eq!(f.store.resolve(&member).unwrap().workspace_root, None);
    }
}

#[cfg(test)]
mod project_replay_tests {
    use super::{tests::*, *};
    use rusqlite::types::Value as SqlValue;

    fn projection(f: &Fixture) -> BTreeMap<&'static str, Vec<Vec<SqlValue>>> {
        f.store
            .db
            .with_conn(|conn| {
                let mut tables = BTreeMap::new();
                for table in [
                    "project",
                    "project_root",
                    "project_alias",
                    "derived_root_parent",
                    "project_workspace",
                    "workspace_member",
                    "workspace",
                    "agent",
                    "agent_name_claim",
                ] {
                    let columns = conn
                        .prepare(&format!("SELECT * FROM {table}"))?
                        .column_count();
                    let order = (1..=columns)
                        .map(|n| n.to_string())
                        .collect::<Vec<_>>()
                        .join(",");
                    let mut stmt =
                        conn.prepare(&format!("SELECT * FROM {table} ORDER BY {order}"))?;
                    let rows = stmt
                        .query_map([], |row| {
                            (0..columns)
                                .map(|n| row.get(n))
                                .collect::<rusqlite::Result<Vec<SqlValue>>>()
                        })?
                        .collect::<rusqlite::Result<Vec<_>>>()?;
                    tables.insert(table, rows);
                }
                Ok(tables)
            })
            .unwrap()
    }

    fn assert_rebuild_preserves_projection(f: &Fixture) {
        // Read the actual tables, including provenance and timestamps, rather
        // than a resolve reply that could hide an extra project or lost alias.
        let before = projection(f);
        let fleet = f.store.agent_fleet_identity(None, "replay-test").unwrap();
        let generation = f.store.generation().unwrap();
        assert_eq!(f.store.rebuild().unwrap().generation, generation);
        let after = projection(f);
        for (table, rows) in before {
            assert_eq!(rows, after[table], "rebuild changed {table}");
        }
        assert_eq!(
            fleet,
            f.store.agent_fleet_identity(None, "replay-test").unwrap(),
            "rebuild changed fleet identity"
        );
        assert!(f.store.verify().unwrap().ok);
    }

    fn bind_live_head(f: &Fixture, project: &str) {
        let placed = f
            .store
            .db
            .with_conn(|conn| {
                conn.query_row(
                    "SELECT EXISTS(SELECT 1 FROM project_workspace WHERE project_id=?1)",
                    [project],
                    |r| r.get::<_, bool>(0),
                )
            })
            .unwrap();
        let path = f.root.join("core-head.db");
        let source = rusqlite::Connection::open(&path).unwrap();
        for migration in [
            include_str!("../tests/fixtures/core-agent-migrations/076_agent_registry.sql"),
            include_str!("../tests/fixtures/core-agent-migrations/080_agent_github_identity.sql"),
            include_str!("../tests/fixtures/core-agent-migrations/082_wake_delivery.sql"),
            include_str!("../tests/fixtures/core-agent-migrations/109_agent_generation.sql"),
            include_str!("../tests/fixtures/core-agent-migrations/112_agent_avatar.sql"),
            include_str!("../tests/fixtures/core-agent-migrations/126_agent_labels.sql"),
        ] {
            source.execute_batch(migration).unwrap();
        }
        // `agent.create` refuses a head whose project isn't in a workspace, so a
        // head bound to an unplaced project can only exist by being imported from
        // core, where it was created before its project lost its placement. Build
        // one through the real import so these cases cover that state too.
        if !placed {
            source.execute("INSERT INTO agent(agent_id,name,tag,role,project_id,created_at_ms,updated_at_ms) VALUES('agent_16013c86','Head','test','head',?1,10,10)", [project]).unwrap();
            source.execute_batch("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms) VALUES('agent_16013c86','workspace','historical','head','Head',10)").unwrap();
        }
        f.store
            .with_principal("reserved:prefrontal-core")
            .agent_import(
                json!({"snapshot_path":path,"request_key":"import-head"}),
                10,
            )
            .unwrap();
        if placed {
            f.store.with_principal("reserved:prefrontal-core").agent_mutation("agent.create", json!({"role":"head","project_id":project,"name":"Head","tag":"test","request_key":"create-head"}), 20).unwrap();
        }
        let snapshot = f.store.agent_snapshot().unwrap();
        assert_eq!(snapshot.agents.len(), 1);
        assert_eq!(snapshot.agents[0].role, "head");
        assert_eq!(snapshot.agents[0].status, "live");
        assert_eq!(snapshot.agents[0].project_id.as_deref(), Some(project));
        assert_eq!(snapshot.claims.len(), 1);
        let fleet = result(&f.store.agent_fleet_identity(None, "replay-test").unwrap());
        assert_eq!(fleet["agents"][0]["project"]["id"], project);
    }

    fn project_names(f: &Fixture) -> BTreeMap<String, String> {
        f.store
            .db
            .with_conn(|conn| {
                let mut stmt = conn.prepare("SELECT project_id,name FROM project")?;
                let rows = stmt
                    .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                    .collect();
                rows
            })
            .unwrap()
    }

    fn seed(f: &Fixture, payload: SeedPayload) -> Value {
        result(
            &f.store
                .seed_import(SeedImportRequest {
                    source: "mc".into(),
                    payload,
                    ..Default::default()
                })
                .unwrap(),
        )
    }

    fn workspace(id: &str, name: &str) -> SeedWorkspace {
        SeedWorkspace {
            workspace_id: id.into(),
            name: name.into(),
        }
    }

    fn pair(root: &str, identity: &str) -> SeedPair {
        SeedPair {
            canonical_root: root.into(),
            mc_identity: identity.into(),
            ..Default::default()
        }
    }

    fn has_report(out: &Value, kind: &str) -> bool {
        out["report"]
            .as_array()
            .unwrap()
            .iter()
            .any(|entry| entry["kind"] == kind)
    }

    #[test]
    fn rebuild_upgrade_creates_project_with_workspace_placement() {
        let f = Fixture::new("replay-upgrade-workspace");
        let root = f.dir("root");
        f.store
            .upgrade_implicit(UpgradeImplicitRequest {
                implicit_id: implicit_project_id(&root),
                name: "Upgraded".into(),
                workspace_id: Some("w".into()),
                root: Some(root.clone()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            f.store.resolve(&root).unwrap().workspace_id.as_deref(),
            Some("w")
        );
        bind_live_head(&f, &f.store.resolve(&root).unwrap().project_id);
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_upgrade_renames_existing_project_without_changing_placement() {
        let f = Fixture::new("replay-upgrade-rename");
        let root = f.dir("root");
        f.store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "Original".into(),
                roots: vec![root.clone()],
                workspace_id: Some("w".into()),
                ..Default::default()
            })
            .unwrap();
        f.store
            .upgrade_implicit(UpgradeImplicitRequest {
                implicit_id: implicit_project_id(&root),
                project_id: Some("p".into()),
                name: "Renamed".into(),
                roots: vec![root.clone()],
                workspace_id: Some("w".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            project_names(&f),
            BTreeMap::from([("p".into(), "Renamed".into())])
        );
        assert_eq!(
            f.store.resolve(&root).unwrap().workspace_id.as_deref(),
            Some("w")
        );
        bind_live_head(&f, "p");
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_upgrade_workspace_only_noop_does_not_create_workspace() {
        let f = Fixture::new("replay-upgrade-noop");
        let root = f.dir("root");
        let req = UpgradeImplicitRequest {
            implicit_id: implicit_project_id(&root),
            project_id: Some("p".into()),
            name: "Project".into(),
            roots: vec![root.clone()],
            ..Default::default()
        };
        f.store.upgrade_implicit(req.clone()).unwrap();
        let out = result(
            &f.store
                .upgrade_implicit(UpgradeImplicitRequest {
                    workspace_id: Some("w".into()),
                    ..req
                })
                .unwrap(),
        );
        assert_eq!(out["noop"], true);
        assert!(projection(&f)["workspace"].is_empty());
        assert!(projection(&f)["project_workspace"].is_empty());
        bind_live_head(&f, "p");
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_register_root_inferred_rename_keeps_project_id() {
        let f = Fixture::new("replay-register-owner");
        let root = f.dir("root");
        let fresh = f.dir("fresh");
        register(&f, "p", root.clone());
        let out = result(
            &f.store
                .register(RegisterRequest {
                    name: "Renamed".into(),
                    roots: vec![root, fresh],
                    ..Default::default()
                })
                .unwrap(),
        );
        assert_eq!(out["projectId"], "p");
        assert_eq!(
            project_names(&f),
            BTreeMap::from([("p".into(), "Renamed".into())])
        );
        bind_live_head(&f, "p");
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_register_derived_parent_inferred_rename_keeps_project_id() {
        let f = Fixture::new("replay-register-parent");
        let parent = f.dir("parent");
        f.store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "Original".into(),
                derived_root_parents: vec![parent.clone()],
                ..Default::default()
            })
            .unwrap();
        let out = result(
            &f.store
                .register(RegisterRequest {
                    name: "Renamed".into(),
                    derived_root_parents: vec![parent],
                    ..Default::default()
                })
                .unwrap(),
        );
        assert_eq!(out["projectId"], "p");
        assert_eq!(
            project_names(&f),
            BTreeMap::from([("p".into(), "Renamed".into())])
        );
        bind_live_head(&f, "p");
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_assign_workspace_uses_journaled_workspace_name() {
        let f = Fixture::new("replay-assign-name");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                workspace_name: Some("Team".into()),
                ..Default::default()
            })
            .unwrap();
        assert_eq!(
            projection(&f)["workspace"][0][1],
            SqlValue::Text("Team".into())
        );
        bind_live_head(&f, "p");
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_renames_workspace_and_preserves_unchanged_workspace_timestamp() {
        let f = Fixture::new("replay-seed-workspace-name");
        seed(
            &f,
            SeedPayload {
                workspaces: vec![workspace("w", "Original"), workspace("unchanged", "Same")],
                ..Default::default()
            },
        );
        // Distinct journal times make a spurious update observable even when
        // these two seed calls happen within the same clock millisecond.
        f.store
            .db
            .with_conn_fenced(|tx| {
                tx.execute("UPDATE registry_journal SET created_at=1", [])?;
                tx.execute("UPDATE workspace SET created_at=1,updated_at=1", [])?;
                Ok(())
            })
            .unwrap();
        seed(
            &f,
            SeedPayload {
                workspaces: vec![workspace("w", "Renamed"), workspace("unchanged", "Same")],
                pairs: vec![pair(&f.dir("seed-root"), "git:workspace-name")],
                members: vec![SeedMember {
                    mc_identity: "git:workspace-name".into(),
                    workspace_id: "w".into(),
                }],
                ..Default::default()
            },
        );
        assert_eq!(
            projection(&f)["workspace"][1][1],
            SqlValue::Text("Renamed".into())
        );
        bind_live_head(&f, &seed_id("git:workspace-name"));
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_project_name_prefers_payload_then_pair_then_root_basename() {
        let f = Fixture::new("replay-seed-project-names");
        let named = f.dir("payload-root");
        let paired = f.dir("pair-root");
        let unnamed = f.dir("basename");
        seed(
            &f,
            SeedPayload {
                pairs: vec![
                    SeedPair {
                        name: Some("Pair loses".into()),
                        ..pair(&named, "git:payload")
                    },
                    SeedPair {
                        name: Some("Pair wins".into()),
                        ..pair(&paired, "git:pair")
                    },
                    pair(&unnamed, "git:basename"),
                ],
                names: BTreeMap::from([("git:payload".into(), "Payload wins".into())]),
                ..Default::default()
            },
        );
        assert_eq!(
            project_names(&f),
            BTreeMap::from([
                (seed_id("git:payload"), "Payload wins".into()),
                (seed_id("git:pair"), "Pair wins".into()),
                (seed_id("git:basename"), "basename".into()),
            ])
        );
        bind_live_head(&f, &seed_id("git:payload"));
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_multi_workspace_preserves_an_imported_live_head() {
        let f = Fixture::new("replay-seed-multi-head");
        let root = f.dir("multi");
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![pair(&root, "git:multi")],
                workspaces: vec![workspace("w1", "One"), workspace("w2", "Two")],
                members: vec![
                    SeedMember {
                        mc_identity: "git:multi".into(),
                        workspace_id: "w1".into(),
                    },
                    SeedMember {
                        mc_identity: "git:multi".into(),
                        workspace_id: "w2".into(),
                    },
                ],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "multi_workspace"));
        assert_eq!(f.store.resolve(&root).unwrap().workspace_id, None);
        bind_live_head(&f, &seed_id("git:multi"));
        let fleet = result(&f.store.agent_fleet_identity(None, "replay-test").unwrap());
        assert!(fleet["agents"][0].get("workspace").is_none());
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_multi_workspace_stays_unplaced_and_duplicate_claim_is_single() {
        let f = Fixture::new("replay-seed-multi-workspace");
        let multi = f.dir("multi");
        let single = f.dir("single");
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![pair(&multi, "git:multi"), pair(&single, "git:single")],
                workspaces: vec![workspace("w1", "One"), workspace("w2", "Two")],
                members: vec![
                    SeedMember {
                        mc_identity: "git:multi".into(),
                        workspace_id: "w1".into(),
                    },
                    SeedMember {
                        mc_identity: "git:multi".into(),
                        workspace_id: "w2".into(),
                    },
                    SeedMember {
                        mc_identity: "git:single".into(),
                        workspace_id: "w1".into(),
                    },
                    SeedMember {
                        mc_identity: "git:single".into(),
                        workspace_id: "w1".into(),
                    },
                ],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "multi_workspace"));
        assert_eq!(f.store.resolve(&multi).unwrap().workspace_id, None);
        assert_eq!(
            f.store.resolve(&single).unwrap().workspace_id.as_deref(),
            Some("w1")
        );
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_alias_occupied_does_not_create_project() {
        let f = Fixture::new("replay-seed-alias-occupied");
        let id = seed_id("git:occupied");
        register(&f, &id, f.dir("old"));
        register(&f, "successor", f.dir("successor"));
        f.store
            .remove(RemoveRequest {
                project_id: Some(id),
                successor_project_id: Some("successor".into()),
                ..Default::default()
            })
            .unwrap();
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![pair(&f.dir("fresh"), "git:occupied")],
                // An effectful workspace write ensures the skipped observation is
                // journaled and reaches replay instead of only testing a live noop.
                workspaces: vec![workspace("w", "Team")],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "alias_occupied"));
        assert_eq!(out["noop"], false);
        assert_eq!(
            project_names(&f),
            BTreeMap::from([("successor".into(), "successor".into())])
        );
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_conflicted_root_is_skipped_per_root_not_per_identity() {
        let f = Fixture::new("replay-seed-conflicted-root");
        let conflict = f.dir("conflict");
        let fresh = f.dir("fresh");
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![
                    pair(&conflict, "git:a"),
                    pair(&conflict, "git:b"),
                    pair(&fresh, "git:a"),
                ],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "conflicted"));
        assert_eq!(
            project_names(&f),
            BTreeMap::from([(seed_id("git:a"), "fresh".into())])
        );
        assert_eq!(projection(&f)["project_root"].len(), 1);
        assert_eq!(projection(&f)["project_root"][0][0], SqlValue::Text(fresh));
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_minted_id_occupied_does_not_bind_observation() {
        let f = Fixture::new("replay-seed-id-occupied");
        let id = seed_id("git:occupied");
        register(&f, &id, f.dir("owned"));
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![pair(&f.dir("fresh"), "git:occupied")],
                workspaces: vec![workspace("w", "Team")],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "conflicted"));
        assert_eq!(project_names(&f), BTreeMap::from([(id.clone(), id)]));
        assert_eq!(projection(&f)["project_root"].len(), 1);
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_seed_identity_class_precedence_is_per_root() {
        for (identity, class) in [("git:winner", None), ("opaque-winner", Some("git"))] {
            let f = Fixture::new("replay-seed-precedence");
            let mixed = f.dir("mixed");
            let directory = f.dir("directory");
            let out = seed(
                &f,
                SeedPayload {
                    pairs: vec![
                        SeedPair {
                            identity_class: class.map(String::from),
                            ..pair(&mixed, identity)
                        },
                        SeedPair {
                            identity_class: Some("dir".into()),
                            ..pair(&mixed, "dir:loser")
                        },
                        SeedPair {
                            identity_class: Some("dir".into()),
                            ..pair(&directory, "dir:loser")
                        },
                    ],
                    ..Default::default()
                },
            );
            assert!(has_report(&out, "identity_class_precedence"));
            assert_eq!(
                project_names(&f),
                BTreeMap::from([
                    (seed_id(identity), "mixed".into()),
                    (seed_id("dir:loser"), "directory".into()),
                ])
            );
            assert_rebuild_preserves_projection(&f);
        }
    }

    #[test]
    fn rebuild_seed_rejoined_project_without_new_roots_does_not_place_or_rename() {
        let f = Fixture::new("replay-seed-rejoined");
        let root = f.dir("root");
        seed(
            &f,
            SeedPayload {
                pairs: vec![pair(&root, "git:one")],
                ..Default::default()
            },
        );
        let out = seed(
            &f,
            SeedPayload {
                pairs: vec![SeedPair {
                    name: Some("Not applied".into()),
                    ..pair(&root, "git:one")
                }],
                workspaces: vec![workspace("w", "Team")],
                members: vec![SeedMember {
                    mc_identity: "git:one".into(),
                    workspace_id: "w".into(),
                }],
                ..Default::default()
            },
        );
        assert!(has_report(&out, "rejoined"));
        assert_eq!(out["noop"], false);
        assert_eq!(
            project_names(&f),
            BTreeMap::from([(seed_id("git:one"), "root".into())])
        );
        assert_eq!(f.store.resolve(&root).unwrap().workspace_id, None);
        assert_rebuild_preserves_projection(&f);
    }

    #[test]
    fn rebuild_legacy_request_only_rows_apply_live_rules_without_disk_or_cached_replies() {
        let f = Fixture::new("replay-legacy-requests");
        let root = f.dir("root");
        register(&f, "p", root.clone());
        f.store
            .register(RegisterRequest {
                name: "Inferred rename".into(),
                roots: vec![root.clone()],
                ..Default::default()
            })
            .unwrap();
        f.store
            .upgrade_implicit(UpgradeImplicitRequest {
                implicit_id: implicit_project_id(&root),
                project_id: Some("p".into()),
                name: "Upgraded rename".into(),
                roots: vec![root.clone()],
                ..Default::default()
            })
            .unwrap();
        f.store
            .assign_workspace(AssignWorkspaceRequest {
                project_id: "p".into(),
                workspace_id: "w".into(),
                workspace_name: Some("Team".into()),
                ..Default::default()
            })
            .unwrap();
        let seeded_root = f.dir("seeded");
        seed(
            &f,
            SeedPayload {
                pairs: vec![SeedPair {
                    name: Some("Seeded name".into()),
                    ..pair(&seeded_root, "git:seeded")
                }],
                workspaces: vec![workspace("w", "Renamed team")],
                ..Default::default()
            },
        );
        // Legacy journals contain request bodies, not precomputed projection
        // decisions. Remove envelopes and cached replies to pin that contract.
        f.store.db.with_conn_fenced(|tx| {
            let mut stmt = tx.prepare("SELECT seq,payload_json FROM registry_journal")?;
            let rows = stmt.query_map([], |row| Ok((row.get::<_, i64>(0)?, row.get::<_, String>(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            drop(stmt);
            for (seq, payload) in rows {
                let value: Value = serde_json::from_str(&payload).unwrap();
                let request = value.get("request").unwrap_or(&value);
                tx.execute("UPDATE registry_journal SET payload_json=?1,response_json=NULL WHERE seq=?2",
                    params![serde_json::to_string(request).unwrap(), seq])?;
            }
            Ok(())
        }).unwrap();
        std::fs::remove_dir(&root).unwrap();
        std::fs::remove_dir(&seeded_root).unwrap();
        assert_eq!(
            project_names(&f),
            BTreeMap::from([
                ("p".into(), "Upgraded rename".into()),
                (seed_id("git:seeded"), "Seeded name".into()),
            ])
        );
        assert_rebuild_preserves_projection(&f);
    }
}

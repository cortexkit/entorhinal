use std::{
    collections::{BTreeMap, BTreeSet},
    path::Path,
};

use cortexkit_paths::{IdentityError, ProjectRootId};
use rusqlite::{params, OptionalExtension, Transaction};
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
}

#[derive(Debug)]
struct Action {
    changed: bool,
    seq: Option<i64>,
    value: Value,
    payload: Value,
}

fn domain(code: &str, message: impl Into<String>) -> RegistryError {
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

fn canonical(raw: &str) -> Result<String, RegistryError> {
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

fn append(
    tx: &Transaction<'_>,
    op: &str,
    payload: &Value,
    actor: &str,
    key: Option<&str>,
    now: i64,
) -> rusqlite::Result<i64> {
    tx.execute("INSERT INTO registry_journal(op,payload_json,actor,request_key,created_at) VALUES(?1,?2,?3,?4,?5)", params![op, serde_json::to_string(payload).unwrap(), actor, key, now])?;
    Ok(tx.last_insert_rowid())
}

fn cached(tx: &Transaction<'_>, key: Option<&str>) -> rusqlite::Result<Option<Vec<u8>>> {
    key.and_then(|key| {
        tx.query_row(
            "SELECT response_json FROM registry_journal WHERE request_key=?1",
            [key],
            |r| r.get::<_, Option<String>>(0),
        )
        .optional()
        .transpose()
    })
    .transpose()
    .map(|v| v.flatten().map(String::into_bytes))
}

fn wire(value: Value) -> Result<Vec<u8>, RegistryError> {
    serde_json::to_vec(&json!({"result": value}))
        .map_err(|e| domain("encode_failed", e.to_string()))
}

impl RegistryStore {
    fn mutation<F>(&self, _op: &str, key: Option<&str>, action: F) -> Result<Vec<u8>, RegistryError>
    where
        F: FnOnce(&Transaction<'_>) -> Result<Action, RegistryError>,
    {
        let out = self
            .db
            .with_conn_fenced(|tx| -> rusqlite::Result<Result<Vec<u8>, RegistryError>> {
                if let Some(blob) = cached(tx, key)? {
                    return Ok(Ok(blob));
                }
                let action = match action(tx) {
                    Ok(a) => a,
                    Err(e) => return Ok(Err(e)),
                };
                let generation = action.seq.unwrap_or(tx.query_row(
                    "SELECT COALESCE(MAX(seq),0) FROM registry_journal",
                    [],
                    |r| r.get(0),
                )?);
                let mut value = action.value;
                if let Value::Object(ref mut map) = value {
                    map.insert("generation".to_string(), json!(generation));
                    map.insert("noop".to_string(), json!(!action.changed));
                }
                let blob = match wire(value) {
                    Ok(v) => v,
                    Err(e) => return Ok(Err(e)),
                };
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
                Ok(Ok(blob))
            })
            .map_err(RegistryError::Store)?;
        out
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
            let seq=append(tx,"register",&payload,&actor,key.as_deref(),now)?;
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
        self.mutation("assign_workspace",key.clone().as_deref(),move|tx|{ let now=now_unix_millis(); if tx.query_row("SELECT 1 FROM project WHERE project_id=?1",[&req.project_id],|r|r.get::<_,i64>(0)).optional()?.is_none(){return Err(domain("not_found",&req.project_id)); }; if let Some(old)=tx.query_row("SELECT workspace_id FROM project_workspace WHERE project_id=?1",[&req.project_id],|r|r.get::<_,String>(0)).optional()? {if old==req.workspace_id{return Ok(Action{changed:false,seq:None,value:json!({"projectId":req.project_id,"workspaceId":old}),payload});} return Err(domain("workspace_conflict",format!("project is already in workspace {old}")));} let seq=append(tx,"assign_workspace",&payload,&actor,key.as_deref(),now)?; tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3)",params![&req.workspace_id,req.workspace_name.clone().unwrap_or_else(||req.workspace_id.clone()),now])?; tx.execute("INSERT INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",[&req.project_id,&req.workspace_id])?; tx.execute("INSERT INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",[&req.workspace_id,&req.project_id])?; Ok(Action{changed:true,seq:Some(seq),value:json!({"projectId":req.project_id,"workspaceId":req.workspace_id}),payload}) })
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
        self.mutation("upgrade_implicit",key.clone().as_deref(),move|tx|{ let now=now_unix_millis(); if let Some(target)=tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1",[&req.implicit_id],|r|r.get::<_,String>(0)).optional()? {if target!=id{return Err(domain("alias_conflict",target));} let _=target;} let existing=tx.query_row("SELECT name FROM project WHERE project_id=?1",[&id],|r|r.get::<_,String>(0)).optional()?; let mut changed=existing.is_none()||existing.as_ref().is_some_and(|n|n!=&req.name); for r in &req.roots {changed|=tx.query_row("SELECT 1 FROM project_root WHERE canonical_root=?1",[r],|r|r.get::<_,i64>(0)).optional()?.is_none();} changed|=tx.query_row("SELECT project_id FROM project_alias WHERE old_id=?1",[&req.implicit_id],|r|r.get::<_,String>(0)).optional()?.is_none(); if !changed{return Ok(Action{changed:false,seq:None,value:json!({"projectId":id,"oldId":req.implicit_id}),payload});} let seq=append(tx,"upgrade_implicit",&payload,&actor,key.as_deref(),now)?; if existing.is_none(){tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3)",params![&id,&req.name,now])?;} else {tx.execute("UPDATE project SET name=?1,updated_at=?2 WHERE project_id=?3",params![&req.name,now,&id])?;} for r in &req.roots{tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![r,&id,now])?;tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(r),&id,now])?;} tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![&req.implicit_id,&id,now])?; if let Some(w)=&req.workspace_id{tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)",params![w,now])?;tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",[&id,w])?;tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",[w,&id])?;} Ok(Action{changed:true,seq:Some(seq),value:json!({"projectId":id,"oldId":req.implicit_id}),payload}) })
    }

    pub fn remove(&self, req: RemoveRequest) -> Result<Vec<u8>, RegistryError> {
        let key = req.request_key.clone();
        let actor = req.actor.clone().unwrap_or_else(|| "module".into());
        let payload =
            serde_json::to_value(&req).map_err(|e| domain("encode_failed", e.to_string()))?;
        self.mutation("remove",key.clone().as_deref(),move|tx|{let now=now_unix_millis(); if let Some(w)=&req.workspace_id {if tx.query_row("SELECT 1 FROM workspace WHERE workspace_id=?1",[w],|r|r.get::<_,i64>(0)).optional()?.is_none(){return Err(domain("not_found",w));} let seq=append(tx,"remove",&payload,&actor,key.as_deref(),now)?; tx.execute("DELETE FROM project_workspace WHERE workspace_id=?1",[w])?; tx.execute("DELETE FROM workspace_member WHERE workspace_id=?1",[w])?; tx.execute("DELETE FROM workspace WHERE workspace_id=?1",[w])?; return Ok(Action{changed:true,seq:Some(seq),value:json!({"workspaceId":w}),payload});} let id=req.project_id.clone().ok_or_else(||domain("invalid_params","projectId is required"))?; if tx.query_row("SELECT 1 FROM project WHERE project_id=?1",[&id],|r|r.get::<_,i64>(0)).optional()?.is_none(){return Err(domain("not_found",id)); }; if let Some(s)=&req.successor_project_id {if s==&id||tx.query_row("SELECT 1 FROM project WHERE project_id=?1",[s],|r|r.get::<_,i64>(0)).optional()?.is_none(){return Err(domain("not_found",s));}} let dropped=tx.query_row("SELECT workspace_id FROM project_workspace WHERE project_id=?1",[&id],|r|r.get::<_,String>(0)).optional()?; let seq=append(tx,"remove",&payload,&actor,key.as_deref(),now)?; if let Some(s)=&req.successor_project_id {tx.execute("UPDATE project_root SET project_id=?1 WHERE project_id=?2",[s,&id])?;tx.execute("UPDATE derived_root_parent SET project_id=?1 WHERE project_id=?2",[s,&id])?;tx.execute("UPDATE project_alias SET project_id=?1 WHERE project_id=?2",[s,&id])?;tx.execute("DELETE FROM project_alias WHERE old_id=?1",[&id])?;tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![&id,s,now])?;} else {tx.execute("DELETE FROM project_root WHERE project_id=?1",[&id])?;tx.execute("DELETE FROM derived_root_parent WHERE project_id=?1",[&id])?;tx.execute("DELETE FROM project_alias WHERE project_id=?1 OR old_id=?1",[&id])?;} tx.execute("DELETE FROM project_workspace WHERE project_id=?1",[&id])?;tx.execute("DELETE FROM workspace_member WHERE ref_kind='local' AND project_id=?1",[&id])?;tx.execute("DELETE FROM project WHERE project_id=?1",[&id])?;Ok(Action{changed:true,seq:Some(seq),value:json!({"projectId":id,"successorProjectId":req.successor_project_id,"droppedWorkspaceId":dropped}),payload}) })
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

impl RegistryStore {
    pub fn seed_import(&self, mut req: SeedImportRequest) -> Result<Vec<u8>, RegistryError> {
        if req.source != "mc" {
            return Err(domain("invalid_source", &req.source));
        }
        for pair in &mut req.payload.pairs {
            pair.canonical_root = canonical(&pair.canonical_root)?;
        }
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
                let Some(project)=target else {continue}; if !available.is_empty() || live.is_none() {let name=req.payload.names.get(&identity).cloned().or_else(||rows.first().and_then(|p|p.name.clone())).unwrap_or_else(||Path::new(&rows[0].canonical_root).file_name().map(|x|x.to_string_lossy().into_owned()).unwrap_or_else(||identity.clone())); if live.is_none(){tx.execute("INSERT INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,?3,?4,?4)",params![project,name,identity,now])?;changed=true;} for p in &available {tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![p.canonical_root,project,now])?;tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&p.canonical_root),project,now])?;changed=true;} let claims=req.payload.members.iter().filter(|m|m.mc_identity==identity).map(|m|m.workspace_id.clone()).collect::<BTreeSet<_>>(); if claims.len()==1 {let w=claims.iter().next().unwrap();tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",params![project,w])?;tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",params![w,project])?;changed=true;} else if claims.len()>1 {report.push(SeedReportEntry{kind:"multi_workspace".into(),mc_identity:Some(identity.clone()),canonical_root:None,project_id:Some(project.clone()),owner_project_id:None,new_project_id:None,roots:None,reason:Some("detached because export claimed multiple workspaces".into())});}}
            }
            let value=json!({"report":report}); if !changed {return Ok(Action{changed:false,seq:None,value,payload});} let seq=append(tx,"seed_import",&payload,&actor,key.as_deref(),now)?; Ok(Action{changed:true,seq:Some(seq),value,payload})
        })
    }

    pub fn verify(&self) -> Result<VerifyReply, RegistryError> {
        self.read(|conn| { let mut mismatches=Vec::new(); let local: i64=conn.query_row("SELECT COUNT(*) FROM workspace_member WHERE ref_kind='local'",[],|r|r.get(0))?; let pairs:i64=conn.query_row("SELECT COUNT(*) FROM project_workspace",[],|r|r.get(0))?; let mut stmt=conn.prepare("SELECT wm.workspace_id,wm.project_id FROM workspace_member wm LEFT JOIN project_workspace pw ON pw.workspace_id=wm.workspace_id AND pw.project_id=wm.project_id WHERE wm.ref_kind='local' AND pw.project_id IS NULL")?; for row in stmt.query_map([],|r|Ok(format!("{}:{}",r.get::<_,String>(0)?,r.get::<_,String>(1)?)))? {mismatches.push(row?);} let mut stmt=conn.prepare("SELECT pw.workspace_id,pw.project_id FROM project_workspace pw LEFT JOIN workspace_member wm ON wm.workspace_id=pw.workspace_id AND wm.project_id=pw.project_id AND wm.ref_kind='local' WHERE wm.project_id IS NULL")?; for row in stmt.query_map([],|r|Ok(format!("{}:{}",r.get::<_,String>(0)?,r.get::<_,String>(1)?)))? {mismatches.push(row?);} Ok(VerifyReply{ok:mismatches.is_empty(),local_members:local,project_workspaces:pairs,mismatches,generation:self.generation_from_connection(conn)?}) })
    }

    pub fn rebuild(&self) -> Result<i64, RegistryError> {
        self.db.with_conn_fenced(|tx| { tx.execute_batch("DELETE FROM workspace_member; DELETE FROM project_workspace; DELETE FROM project_alias; DELETE FROM derived_root_parent; DELETE FROM project_root; DELETE FROM project; DELETE FROM workspace;")?; let mut stmt=tx.prepare("SELECT op,payload_json,created_at FROM registry_journal ORDER BY seq")?; let rows=stmt.query_map([],|r|Ok((r.get::<_,String>(0)?,r.get::<_,String>(1)?,r.get::<_,i64>(2)?)))?.collect::<rusqlite::Result<Vec<_>>>()?; drop(stmt); for (op,payload,now) in rows {let request:Value=serde_json::from_str::<Value>(&payload).ok().and_then(|v|v.get("request").cloned()).unwrap_or_else(||serde_json::from_str(&payload).unwrap()); replay(tx,&op,request,now)?;} tx.query_row("SELECT COALESCE(MAX(seq),0) FROM registry_journal",[],|r|r.get(0)) }).map_err(RegistryError::Store)
    }
}

fn seed_id(identity: &str) -> String {
    let mut h = blake3::Hasher::new();
    h.update(b"seed1:");
    h.update(identity.as_bytes());
    format!("pj-{}", &h.finalize().to_hex()[..16])
}

fn replay(tx: &Transaction<'_>, op: &str, v: Value, now: i64) -> rusqlite::Result<()> {
    match op {
        "register" => {
            let r: RegisterRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            let id = r
                .project_id
                .unwrap_or_else(|| mint("register1", &r.name, &r.roots));
            tx.execute("INSERT OR IGNORE INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3)",params![id,r.name,now])?;
            for root in r.roots {
                tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![root,id,now])?;
                tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&root),id,now])?;
            }
            for p in r.derived_root_parents {
                tx.execute("INSERT OR IGNORE INTO derived_root_parent(canonical_parent,project_id) VALUES(?1,?2)",params![p,id])?;
            }
        }
        "assign_workspace" => {
            let r: AssignWorkspaceRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?1,?2,?2)",params![r.workspace_id,now])?;
            tx.execute(
                "INSERT OR IGNORE INTO project_workspace VALUES(?1,?2)",
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
            tx.execute("INSERT OR IGNORE INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,NULL,?3,?3)", params![id,r.name,now])?;
            for root in r.roots {
                tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![root,id,now])?;
                tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&root),id,now])?;
            }
            tx.execute("INSERT OR REPLACE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![r.implicit_id,id,now])?;
        }
        "seed_import" => {
            let r: SeedImportRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            for w in r.payload.workspaces {
                tx.execute("INSERT OR IGNORE INTO workspace(workspace_id,name,created_at,updated_at) VALUES(?1,?2,?3,?3)",params![w.workspace_id,w.name,now])?;
            }
            let mut groups: BTreeMap<String, Vec<SeedPair>> = BTreeMap::new();
            for p in r.payload.pairs {
                groups.entry(p.mc_identity.clone()).or_default().push(p);
            }
            for (identity, rows) in groups {
                let id = seed_id(&identity);
                tx.execute("INSERT OR IGNORE INTO project(project_id,name,implicit,seed_identity,created_at,updated_at) VALUES(?1,?2,0,?3,?4,?4)",params![id,r.payload.names.get(&identity).cloned().unwrap_or_else(||identity.clone()),identity,now])?;
                for p in rows {
                    tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)",params![p.canonical_root,id,now])?;
                    tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)",params![implicit_project_id(&p.canonical_root),id,now])?;
                }
                for m in r
                    .payload
                    .members
                    .iter()
                    .filter(|m| m.mc_identity == identity)
                {
                    tx.execute("INSERT OR IGNORE INTO project_workspace(project_id,workspace_id) VALUES(?1,?2)",params![id,m.workspace_id])?;
                    tx.execute("INSERT OR IGNORE INTO workspace_member(workspace_id,ref_kind,device_fingerprint,project_id) VALUES(?1,'local','',?2)",params![m.workspace_id,id])?;
                }
            }
        }
        "remove" => {
            let r: RemoveRequest = serde_json::from_value(v)
                .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
            if let Some(workspace) = r.workspace_id {
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
                "ck-projects-mutations-{label}-{}-{}",
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
        assert_eq!(f.store.rebuild().unwrap(), generation);
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
        code(
            f.store
                .assign_workspace(AssignWorkspaceRequest {
                    project_id: "foreign".into(),
                    workspace_id: "w2".into(),
                    ..Default::default()
                })
                .unwrap_err(),
            "workspace_conflict",
        );
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

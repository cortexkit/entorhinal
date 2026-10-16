//! Machine-local consent state. These tables are not identity projections and
//! must never be replayed, exported, or used as authority by a receiving machine.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{load_row, AgentMutationError};
use crate::{JournalWriter, RegistryError, RegistryStore};

pub(crate) const V10_CONSENT: &str = r#"
CREATE TABLE consent_grant (
    id TEXT PRIMARY KEY,
    holder_agent_id TEXT NOT NULL,
    class TEXT NOT NULL CHECK(class IN ('register_project','create_head','hire')),
    scope_id TEXT NOT NULL,
    granted_at_ms INTEGER NOT NULL,
    card_ref TEXT NOT NULL,
    revoked_at_ms INTEGER NULL,
    UNIQUE(holder_agent_id,class,scope_id)
);
CREATE TABLE consent_request (
    agent_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    request_key TEXT NOT NULL,
    args_digest TEXT NOT NULL,
    scope_ref TEXT NOT NULL,
    scope_epoch INTEGER NOT NULL,
    created_at_ms INTEGER NOT NULL,
    elicitation_id TEXT NULL,
    settlement_json TEXT NULL,
    state TEXT NOT NULL CHECK(state IN ('awaiting','applied','refused')),
    outcome_json TEXT NULL,
    PRIMARY KEY(agent_id,tool,request_key),
    CHECK((state='awaiting' AND outcome_json IS NULL) OR
          (state<>'awaiting' AND outcome_json IS NOT NULL))
);
CREATE TABLE consent_feed_cursor (
    scope_ref TEXT NOT NULL,
    scope_epoch INTEGER NOT NULL,
    cursor INTEGER NOT NULL,
    PRIMARY KEY(scope_ref,scope_epoch)
);
CREATE TABLE consent_write_plan (
    agent_id TEXT NOT NULL,
    tool TEXT NOT NULL,
    request_key TEXT NOT NULL,
    plan_json TEXT NOT NULL,
    PRIMARY KEY(agent_id,tool,request_key),
    FOREIGN KEY(agent_id,tool,request_key) REFERENCES consent_request(agent_id,tool,request_key) ON DELETE CASCADE
);
CREATE TABLE agent_audit (
    row_id INTEGER PRIMARY KEY AUTOINCREMENT,
    time_ms INTEGER NOT NULL,
    agent_id TEXT NOT NULL,
    action TEXT NOT NULL,
    target_json TEXT NOT NULL,
    authorization_json TEXT NOT NULL,
    outcome_json TEXT NOT NULL
);
CREATE INDEX agent_audit_page ON agent_audit(time_ms DESC,row_id DESC);
CREATE TABLE project_hire_cap (
    project_id TEXT PRIMARY KEY,
    cap INTEGER NOT NULL CHECK(cap>=0)
);
"#;

/// Journal evidence, not a lookup instruction. Replay restores the recorded
/// identity state even when the authorizing card or grant no longer exists.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Authorization {
    Card { id: String, answer: String },
    Grant { id: String },
    PublisherGrants { ids: Vec<String> },
    OwnHire,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GrantClass {
    RegisterProject,
    CreateHead,
    Hire,
}
impl GrantClass {
    fn as_str(&self) -> &'static str {
        match self {
            Self::RegisterProject => "register_project",
            Self::CreateHead => "create_head",
            Self::Hire => "hire",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StandingGrant {
    pub id: String,
    pub holder_agent_id: String,
    pub class: GrantClass,
    pub scope_id: String,
    pub granted_at_ms: i64,
    pub card_ref: String,
    pub revoked_at_ms: Option<i64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RequestIdentity {
    pub agent_id: String,
    pub tool: String,
    pub request_key: String,
}
impl RequestIdentity {
    pub fn validate(&self) -> Result<(), AgentMutationError> {
        if !(1..=128).contains(&self.request_key.len())
            || !self.request_key.bytes().all(|b| (0x20..=0x7e).contains(&b))
        {
            return Err(AgentMutationError::new(
                "invalid_request_key",
                "request_key must be 1..=128 printable ASCII bytes",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestState {
    Awaiting,
    Applied,
    Refused,
}
impl RequestState {
    fn as_str(&self) -> &'static str {
        match self {
            Self::Awaiting => "awaiting",
            Self::Applied => "applied",
            Self::Refused => "refused",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecordedSettlement {
    pub state: String,
    pub choice: Option<String>,
    pub elicitation_id: String,
    pub settled_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsentRequest {
    pub identity: RequestIdentity,
    pub args_digest: String,
    pub scope_ref: String,
    pub scope_epoch: i64,
    pub created_at_ms: i64,
    pub elicitation_id: Option<String>,
    pub settlement: Option<RecordedSettlement>,
    pub state: RequestState,
    pub outcome: Option<Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditRow {
    pub row_id: i64,
    pub time_ms: i64,
    pub agent_id: String,
    pub action: String,
    pub target: Value,
    pub authorization: Authorization,
    pub outcome: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AuditPage {
    pub rows: Vec<AuditRow>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_cursor: Option<String>,
}

fn decode<T: serde::de::DeserializeOwned>(s: String) -> rusqlite::Result<T> {
    serde_json::from_str(&s).map_err(crate::shared_entry::decode_error)
}
fn encode<T: Serialize>(v: &T) -> rusqlite::Result<String> {
    serde_json::to_string(v).map_err(crate::shared_entry::decode_error)
}

pub fn load_request(
    conn: &Connection,
    key: &RequestIdentity,
) -> rusqlite::Result<Option<ConsentRequest>> {
    conn.query_row(
        "SELECT args_digest,scope_ref,scope_epoch,created_at_ms,elicitation_id,settlement_json,state,outcome_json
         FROM consent_request WHERE agent_id=?1 AND tool=?2 AND request_key=?3",
        params![key.agent_id,key.tool,key.request_key],
        |r| Ok(ConsentRequest {
            identity: key.clone(), args_digest: r.get(0)?, scope_ref: r.get(1)?, scope_epoch: r.get(2)?,
            created_at_ms: r.get(3)?, elicitation_id: r.get(4)?,
            settlement: r.get::<_,Option<String>>(5)?.map(decode).transpose()?,
            state: decode(format!("\"{}\"",r.get::<_,String>(6)?))?,
            outcome: r.get::<_,Option<String>>(7)?.map(decode).transpose()?,
        }),
    ).optional()
}

/// Save a page of Cingulate answers and its feed cursor in one local transaction.
/// Only after this transaction commits may the caller acknowledge that page to
/// Cingulate, so a crash cannot discard answers that were not saved locally.
pub struct ConsentTransaction<'a, 'conn> {
    tx: &'a Transaction<'conn>,
}
impl ConsentTransaction<'_, '_> {
    /// Preserve the target approved on the card across retries and restarts.
    pub fn save_write_plan(
        &self,
        key: &RequestIdentity,
        plan: &ConsentWrite,
    ) -> Result<(), AgentMutationError> {
        self.tx.execute("INSERT OR IGNORE INTO consent_write_plan(agent_id,tool,request_key,plan_json) VALUES(?1,?2,?3,?4)", params![key.agent_id,key.tool,key.request_key,encode(plan)?])?;
        Ok(())
    }
    pub fn request(
        &self,
        key: &RequestIdentity,
    ) -> Result<Option<ConsentRequest>, AgentMutationError> {
        Ok(load_request(self.tx, key)?)
    }

    /// Insert once, or return the existing row without changing its owner,
    /// settlement or outcome. The same agent, tool and key with a different
    /// arguments digest is refused, including when the saved request is terminal.
    pub fn begin_request(
        &self,
        request: &ConsentRequest,
    ) -> Result<ConsentRequest, AgentMutationError> {
        request.identity.validate()?;
        if let Some(existing) = self.request(&request.identity)? {
            if existing.args_digest != request.args_digest {
                return Err(AgentMutationError::new(
                    "request_key_conflict",
                    "request key has different arguments",
                ));
            }
            return Ok(existing);
        }
        if request.state != RequestState::Awaiting || request.outcome.is_some() {
            return Err(AgentMutationError::new(
                "invalid_request",
                "new requests must be awaiting",
            ));
        }
        self.tx.execute("INSERT INTO consent_request(agent_id,tool,request_key,args_digest,scope_ref,scope_epoch,created_at_ms,elicitation_id,settlement_json,state,outcome_json) VALUES(?1,?2,?3,?4,?5,?6,?7,?8,?9,'awaiting',NULL)",
            params![request.identity.agent_id,request.identity.tool,request.identity.request_key,request.args_digest,request.scope_ref,request.scope_epoch,request.created_at_ms,request.elicitation_id,request.settlement.as_ref().map(encode).transpose()?])?;
        Ok(request.clone())
    }

    pub fn set_elicitation(
        &self,
        key: &RequestIdentity,
        id: &str,
    ) -> Result<(), AgentMutationError> {
        self.tx.execute("UPDATE consent_request SET elicitation_id=?4 WHERE agent_id=?1 AND tool=?2 AND request_key=?3 AND state='awaiting' AND elicitation_id IS NULL", params![key.agent_id,key.tool,key.request_key,id])?;
        Ok(())
    }

    /// Save an answer only when its agent, tool, key, arguments digest and scope
    /// match a request still awaiting consent. Recording an answer never applies
    /// a write or turns the request terminal.
    pub fn record_settlement(
        &self,
        key: &RequestIdentity,
        digest: &str,
        scope_ref: &str,
        epoch: i64,
        answer: &RecordedSettlement,
    ) -> Result<bool, AgentMutationError> {
        Ok(self.tx.execute("UPDATE consent_request SET settlement_json=?6,elicitation_id=?7 WHERE agent_id=?1 AND tool=?2 AND request_key=?3 AND args_digest=?4 AND scope_ref=?5 AND scope_epoch=?8 AND state='awaiting' AND settlement_json IS NULL AND (elicitation_id IS NULL OR elicitation_id=?7)",
            params![key.agent_id,key.tool,key.request_key,digest,scope_ref,encode(answer)?,answer.elicitation_id,epoch])? == 1)
    }

    pub fn delete_awaiting(&self, key: &RequestIdentity) -> Result<(), AgentMutationError> {
        self.tx.execute("DELETE FROM consent_request WHERE agent_id=?1 AND tool=?2 AND request_key=?3 AND state='awaiting'", params![key.agent_id,key.tool,key.request_key])?;
        Ok(())
    }

    pub fn finish_request(
        &self,
        key: &RequestIdentity,
        digest: &str,
        state: RequestState,
        outcome: &Value,
    ) -> Result<(), AgentMutationError> {
        if state == RequestState::Awaiting {
            return Err(AgentMutationError::new(
                "invalid_request",
                "finish requires a terminal state",
            ));
        }
        let changed = self.tx.execute("UPDATE consent_request SET state=?5,outcome_json=?6 WHERE agent_id=?1 AND tool=?2 AND request_key=?3 AND args_digest=?4 AND state='awaiting'",
            params![key.agent_id,key.tool,key.request_key,digest,state.as_str(),encode(outcome)?])?;
        if changed != 1 {
            return Err(AgentMutationError::new(
                "request_key_conflict",
                "request is missing, terminal, or has different arguments",
            ));
        }
        Ok(())
    }

    /// A repeated approval to grant standing permission refreshes the row for
    /// the same holder, class and scope, preserving the original grant id.
    pub fn put_grant(&self, grant: &StandingGrant) -> Result<String, AgentMutationError> {
        self.tx.execute("INSERT INTO consent_grant(id,holder_agent_id,class,scope_id,granted_at_ms,card_ref,revoked_at_ms) VALUES(?1,?2,?3,?4,?5,?6,NULL) ON CONFLICT(holder_agent_id,class,scope_id) DO UPDATE SET granted_at_ms=excluded.granted_at_ms,card_ref=excluded.card_ref,revoked_at_ms=NULL",
            params![grant.id,grant.holder_agent_id,grant.class.as_str(),grant.scope_id,grant.granted_at_ms,grant.card_ref])?;
        Ok(self.tx.query_row(
            "SELECT id FROM consent_grant WHERE holder_agent_id=?1 AND class=?2 AND scope_id=?3",
            params![grant.holder_agent_id, grant.class.as_str(), grant.scope_id],
            |r| r.get(0),
        )?)
    }

    pub fn revoke_grant(&self, id: &str, now: i64) -> Result<bool, AgentMutationError> {
        Ok(self.tx.execute(
            "UPDATE consent_grant SET revoked_at_ms=?2 WHERE id=?1 AND revoked_at_ms IS NULL",
            params![id, now],
        )? == 1)
    }

    pub fn save_feed_cursor(
        &self,
        scope_ref: &str,
        epoch: i64,
        cursor: i64,
    ) -> Result<(), AgentMutationError> {
        self.tx.execute("INSERT INTO consent_feed_cursor(scope_ref,scope_epoch,cursor) VALUES(?1,?2,?3) ON CONFLICT(scope_ref,scope_epoch) DO UPDATE SET cursor=MAX(cursor,excluded.cursor)", params![scope_ref,epoch,cursor])?;
        Ok(())
    }

    pub fn append_audit(&self, row: &AuditRow) -> Result<i64, AgentMutationError> {
        self.tx.execute("INSERT INTO agent_audit(time_ms,agent_id,action,target_json,authorization_json,outcome_json) VALUES(?1,?2,?3,?4,?5,?6)", params![row.time_ms,row.agent_id,row.action,encode(&row.target)?,encode(&row.authorization)?,encode(&row.outcome)?])?;
        Ok(self.tx.last_insert_rowid())
    }
}

impl RegistryStore {
    pub fn consent_write_plan(
        &self,
        key: &RequestIdentity,
    ) -> Result<Option<ConsentWrite>, RegistryError> {
        self.read(|conn| conn.query_row("SELECT plan_json FROM consent_write_plan WHERE agent_id=?1 AND tool=?2 AND request_key=?3", params![key.agent_id,key.tool,key.request_key], |r| decode(r.get(0)?)).optional())
    }
    pub fn agent_audit_page(
        &self,
        since_ms: Option<i64>,
        limit: Option<i64>,
        cursor: Option<&str>,
    ) -> Result<AuditPage, AgentMutationError> {
        let limit = limit.unwrap_or(200);
        if !(1..=200).contains(&limit) {
            return Err(AgentMutationError::new(
                "invalid_request",
                "audit limit must be 1..=200",
            ));
        }
        let after = cursor
            .map(|c| {
                let (time, id) = c
                    .strip_prefix("a1:")
                    .and_then(|s| s.split_once(':'))
                    .ok_or_else(|| {
                        AgentMutationError::new("invalid_request", "invalid audit cursor")
                    })?;
                let parse = |v: &str| {
                    i64::from_str_radix(v, 16).map_err(|_| {
                        AgentMutationError::new("invalid_request", "invalid audit cursor")
                    })
                };
                Ok::<_, AgentMutationError>((parse(time)?, parse(id)?))
            })
            .transpose()?;
        self.read_agent(|conn| {
            let mut rows = conn.prepare("SELECT row_id,time_ms,agent_id,action,target_json,authorization_json,outcome_json FROM agent_audit WHERE (?1 IS NULL OR time_ms>=?1) AND (?2 IS NULL OR time_ms<?2 OR (time_ms=?2 AND row_id<?3)) ORDER BY time_ms DESC,row_id DESC LIMIT ?4")?.query_map(params![since_ms,after.map(|a| a.0),after.map(|a| a.1),limit+1], |r| Ok(AuditRow {
                row_id:r.get(0)?,time_ms:r.get(1)?,agent_id:r.get(2)?,action:r.get(3)?,target:decode(r.get(4)?)?,authorization:decode(r.get(5)?)?,outcome:decode(r.get(6)?)?,
            }))?.collect::<rusqlite::Result<Vec<_>>>()?;
            let more = rows.len() > limit as usize;
            rows.truncate(limit as usize);
            let next_cursor = if more { rows.last().map(|r| format!("a1:{:x}:{:x}",r.time_ms,r.row_id)) } else { None };
            Ok(AuditPage { rows, next_cursor })
        })
    }
    /// Domain errors roll the whole local page back, including its cursor.
    pub fn consent_transaction<T>(
        &self,
        action: impl FnOnce(&ConsentTransaction<'_, '_>) -> Result<T, AgentMutationError>,
    ) -> Result<T, AgentMutationError> {
        let mut failure = None;
        let result = self.db.with_conn_fenced(|tx| {
            action(&ConsentTransaction { tx }).map_err(|e| {
                failure = Some(e);
                rusqlite::Error::QueryReturnedNoRows
            })
        });
        result.map_err(|e| failure.unwrap_or_else(|| RegistryError::Store(e).into()))
    }

    pub fn consent_request(
        &self,
        key: &RequestIdentity,
    ) -> Result<Option<ConsentRequest>, AgentMutationError> {
        key.validate()?;
        Ok(self.read(|conn| load_request(conn, key))?)
    }

    pub fn consent_feed_cursor(&self, scope_ref: &str, epoch: i64) -> Result<i64, RegistryError> {
        self.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT cursor FROM consent_feed_cursor WHERE scope_ref=?1 AND scope_epoch=?2",
                    params![scope_ref, epoch],
                    |r| r.get(0),
                )
                .optional()?
                .unwrap_or(0))
        })
    }

    pub fn consent_grants(&self) -> Result<Vec<StandingGrant>, RegistryError> {
        self.read(|conn| conn.prepare("SELECT id,holder_agent_id,class,scope_id,granted_at_ms,card_ref,revoked_at_ms FROM consent_grant ORDER BY id")?.query_map([], |r| Ok(StandingGrant {
            id:r.get(0)?, holder_agent_id:r.get(1)?, class:decode(format!("\"{}\"",r.get::<_,String>(2)?))?, scope_id:r.get(3)?, granted_at_ms:r.get(4)?, card_ref:r.get(5)?, revoked_at_ms:r.get(6)?,
        }))?.collect())
    }

    /// Keep grants for audit and listing, but stop using them as authority when
    /// the holder agent is retired or merged, or the workspace or project is removed.
    pub fn covering_consent_grant(
        &self,
        holder: &str,
        class: GrantClass,
        scope: &str,
    ) -> Result<Option<StandingGrant>, RegistryError> {
        let grants = self.consent_grants()?;
        for grant in grants {
            if grant.holder_agent_id == holder
                && grant.class == class
                && grant.scope_id == scope
                && self.read(|conn| grant_live(conn, &grant.id, holder))?
            {
                return Ok(Some(grant));
            }
        }
        Ok(None)
    }
}

fn grant_live(conn: &Connection, id: &str, holder: &str) -> rusqlite::Result<bool> {
    conn.query_row("SELECT EXISTS(SELECT 1 FROM consent_grant g JOIN agent a ON a.agent_id=g.holder_agent_id WHERE g.id=?1 AND g.holder_agent_id=?2 AND g.revoked_at_ms IS NULL AND a.terminal_reason IS NULL AND ((g.class='hire' AND EXISTS(SELECT 1 FROM project WHERE project_id=g.scope_id)) OR (g.class IN ('register_project','create_head') AND EXISTS(SELECT 1 FROM workspace WHERE workspace_id=g.scope_id))))", params![id,holder], |r|r.get(0))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HireCap {
    pub cap: i64,
    pub live_hires: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HireRequest {
    pub name: String,
    pub tag: String,
    #[serde(default)]
    pub labels: Vec<String>,
}

pub(crate) fn hire_cap(conn: &Connection, project: &str) -> rusqlite::Result<HireCap> {
    conn.query_row("SELECT COALESCE((SELECT cap FROM project_hire_cap WHERE project_id=?1),10),(SELECT COUNT(*) FROM agent WHERE project_id=?1 AND role='hiree' AND terminal_reason IS NULL)", [project], |r| Ok(HireCap { cap:r.get(0)?, live_hires:r.get(1)? }))
}

pub(crate) fn check_hire_cap(conn: &Connection, project: &str) -> Result<(), AgentMutationError> {
    let status = hire_cap(conn, project)?;
    if status.live_hires >= status.cap {
        return Err(AgentMutationError::new(
            "hire_cap_reached",
            "project live hires have reached the hire cap",
        )
        .with_detail(json!({"cap":status.cap,"current_count":status.live_hires})));
    }
    Ok(())
}

impl RegistryStore {
    pub fn project_hire_cap(&self, project: &str) -> Result<HireCap, RegistryError> {
        self.read(|conn| hire_cap(conn, project))
    }

    /// Local operator configuration; lowering a cap never retires existing hires.
    pub fn set_project_hire_cap(&self, project: &str, cap: i64) -> Result<(), AgentMutationError> {
        if cap < 0 {
            return Err(AgentMutationError::new(
                "invalid_request",
                "hire cap must be non-negative",
            ));
        }
        self.consent_transaction(|local| {
            if !local.tx.query_row("SELECT EXISTS(SELECT 1 FROM project WHERE project_id=?1)", [project], |r| r.get::<_,bool>(0))? {
                return Err(AgentMutationError::new("not_found", "unknown project"));
            }
            local.tx.execute("INSERT INTO project_hire_cap(project_id,cap) VALUES(?1,?2) ON CONFLICT(project_id) DO UPDATE SET cap=excluded.cap", params![project,cap])?;
            Ok(())
        })
    }
}

/// Optional consent completion records the request outcome, its audit row and
/// any new standing grant in the same transaction as the agent's write and journal entry.
#[derive(Debug, Clone)]
pub struct ConsentCommit {
    pub request: ConsentRequest,
    pub authorization: Authorization,
    pub action: String,
    pub target: Value,
    pub granted: Option<StandingGrant>,
    pub now_ms: i64,
}

impl JournalWriter<'_> {
    pub fn with_consent_commit(mut self, commit: ConsentCommit) -> Self {
        self.consent = Some(commit);
        self
    }
}

pub(crate) fn complete(
    tx: &Transaction<'_>,
    commit: &ConsentCommit,
    outcome: &Value,
) -> Result<(), AgentMutationError> {
    let local = ConsentTransaction { tx };
    local.begin_request(&commit.request)?;
    if let Some(grant) = &commit.granted {
        local.put_grant(grant)?;
    }
    local.finish_request(
        &commit.request.identity,
        &commit.request.args_digest,
        RequestState::Applied,
        outcome,
    )?;
    local.append_audit(&AuditRow {
        row_id: 0,
        time_ms: commit.now_ms,
        agent_id: commit.request.identity.agent_id.clone(),
        action: commit.action.clone(),
        target: commit
            .target
            .get("target")
            .unwrap_or(&commit.target)
            .clone(),
        authorization: commit.authorization.clone(),
        outcome: outcome.clone(),
    })?;
    Ok(())
}

pub(crate) fn admit_commit(
    conn: &Connection,
    commit: &ConsentCommit,
) -> Result<Option<Value>, AgentMutationError> {
    commit.request.identity.validate()?;
    let caller = load_row(conn, &commit.request.identity.agent_id)?
        .ok_or_else(|| AgentMutationError::new("unknown_agent", "unknown requesting agent"))?;
    if caller.status != "live" {
        return Err(AgentMutationError::new("gone", "requesting agent is gone"));
    }
    if let Some(request) = load_request(conn, &commit.request.identity)? {
        if request.args_digest != commit.request.args_digest {
            return Err(AgentMutationError::new(
                "request_key_conflict",
                "request key has different arguments",
            ));
        }
        if request.state != RequestState::Awaiting {
            return Ok(request.outcome);
        }
    }
    if commit.target.get("tool").is_some() {
        let plan: ConsentWrite = serde_json::from_value(commit.target.clone())
            .map_err(|e| AgentMutationError::new("invalid_request", e.to_string()))?;
        let fresh = if plan.tool == "catalog_install" {
            plan.clone()
        } else {
            prepare_write(
                conn,
                &commit.request.identity.agent_id,
                &plan.tool,
                &plan.arguments,
            )
            .map_err(|e| match e.code.as_str() {
                "gone" | "hire_cap_reached" | "not_your_hire" => e,
                _ => AgentMutationError::new("consent_stale", e.message),
            })?
        };
        if fresh != plan {
            return Err(AgentMutationError::new(
                "consent_stale",
                "approved target or placement changed",
            ));
        }
        if let Authorization::Grant { id } = &commit.authorization {
            let grant = conn
                .query_row(
                    "SELECT class,scope_id FROM consent_grant WHERE id=?1",
                    [id],
                    |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
                )
                .optional()?;
            if grant
                .as_ref()
                .map(|(class, scope)| (class.as_str(), scope.as_str()))
                != plan
                    .grant_class
                    .as_ref()
                    .zip(plan.scope_id.as_deref())
                    .map(|(class, scope)| (class.as_str(), scope))
            {
                return Err(AgentMutationError::new(
                    "consent_stale",
                    "grant no longer covers the target",
                ));
            }
        }
    }
    if let Authorization::Grant { id } = &commit.authorization {
        let live = grant_live(conn, id, &commit.request.identity.agent_id)?;
        if !live {
            return Err(AgentMutationError::new(
                "consent_stale",
                "standing grant is no longer live",
            ));
        }
    }
    Ok(None)
}

/// A validated agent write. Save the project and workspace IDs and names shown
/// on the consent card separately from the arguments, so a retry after a project
/// move cannot apply approval for the old workspace to a different workspace.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConsentWrite {
    pub tool: String,
    pub arguments: Value,
    pub target: Value,
    pub grant_class: Option<GrantClass>,
    pub scope_id: Option<String>,
}

fn prepare_write(
    conn: &Connection,
    caller_id: &str,
    tool: &str,
    arguments: &Value,
) -> Result<ConsentWrite, AgentMutationError> {
    super::store::require_cutover(conn)?;
    let caller = load_row(conn, caller_id)?
        .ok_or_else(|| AgentMutationError::new("unknown_agent", "unknown requesting agent"))?;
    if caller.status != "live" {
        return Err(AgentMutationError::new("gone", "requesting agent is gone"));
    }
    let role = match tool {
        "projects_create_workspace" | "projects_register" | "agents_create_head" => "assistant",
        "agents_hire" | "agents_retire_hire" => "head",
        _ => return Err(AgentMutationError::new("unknown_tool", tool)),
    };
    if caller.role != role {
        return Err(AgentMutationError::new(
            "role_forbidden",
            "stored role cannot perform this write",
        ));
    }
    let text = |field: &str| {
        arguments[field].as_str().ok_or_else(|| {
            AgentMutationError::new("invalid_request", format!("missing string {field}"))
        })
    };
    let workspace = |id: &str| -> Result<String, AgentMutationError> {
        conn.query_row(
            "SELECT name FROM workspace WHERE workspace_id=?1",
            [id],
            |r| r.get(0),
        )
        .optional()?
        .ok_or_else(|| AgentMutationError::new("unknown_workspace", "workspace must already exist"))
    };
    let project = |id: &str| -> Result<Value, AgentMutationError> {
        conn.query_row("SELECT p.name,pw.workspace_id FROM project p LEFT JOIN project_workspace pw ON pw.project_id=p.project_id WHERE p.project_id=?1", [id], |r| Ok(json!({"project_id":id,"name":r.get::<_,String>(0)?,"workspace_id":r.get::<_,Option<String>>(1)?}))).optional()?.ok_or_else(|| AgentMutationError::new("unknown_project", "project must already exist"))
    };
    let (target, grant_class, scope_id) = match tool {
        "projects_create_workspace" => {
            let id = super::validate_workspace_id(text("workspace_id")?)
                .map_err(|e| AgentMutationError::new("invalid_workspace_id", e.to_string()))?;
            if conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM workspace WHERE workspace_id=?1)",
                [&id],
                |r| r.get::<_, bool>(0),
            )? {
                return Err(AgentMutationError::new(
                    "workspace_exists",
                    "workspace already exists",
                ));
            }
            (
                json!({"kind":"workspace","label":text("name")?.trim(),"workspace_id":id}),
                None,
                None,
            )
        }
        "projects_register" => {
            let id = super::validate_workspace_id(text("workspace_id")?)
                .map_err(|e| AgentMutationError::new("invalid_workspace_id", e.to_string()))?;
            let label = workspace(&id)?;
            let dir = crate::mutations::canonical(text("dir")?)?;
            if conn.query_row("SELECT EXISTS(SELECT 1 FROM project WHERE name=?1 UNION ALL SELECT 1 FROM project_root WHERE canonical_root=?2 UNION ALL SELECT 1 FROM derived_root_parent WHERE canonical_parent=?2)", params![text("name")?,dir], |r| r.get::<_,bool>(0))? {
                return Err(AgentMutationError::new("project_exists", "project name or directory already registered"));
            }
            (
                json!({"kind":"project","label":text("name")?,"dir":dir,"workspace_id":id,"workspace_label":label}),
                Some(GrantClass::RegisterProject),
                Some(id),
            )
        }
        "agents_create_head" => {
            let id = super::validate_project_id(text("project_id")?)
                .map_err(|e| AgentMutationError::new("invalid_project_id", e.to_string()))?;
            let placement = project(&id)?;
            let ws = placement["workspace_id"].as_str().ok_or_else(|| {
                AgentMutationError::new("invalid_role_shape", "head requires a placed project")
            })?;
            let label = workspace(ws)?;
            (
                json!({"kind":"agent","label":text("name")?,"placement":placement,"workspace_label":label}),
                Some(GrantClass::CreateHead),
                Some(ws.to_owned()),
            )
        }
        "agents_hire" => {
            let id = caller.project_id.as_deref().ok_or_else(|| {
                AgentMutationError::new("invalid_role_shape", "head requires a project")
            })?;
            check_hire_cap(conn, id)?;
            (
                json!({"kind":"agent","label":text("name")?,"placement":project(id)?}),
                Some(GrantClass::Hire),
                Some(id.to_owned()),
            )
        }
        "agents_retire_hire" => {
            let hire = load_row(conn, text("agent_id")?)?
                .filter(|hire| {
                    hire.status == "live"
                        && hire.role == "hiree"
                        && hire.supervisor_agent_id.as_deref() == Some(caller_id)
                })
                .ok_or_else(|| {
                    AgentMutationError::new(
                        "not_your_hire",
                        "target must be the caller's live hire",
                    )
                })?;
            (
                json!({"kind":"agent","label":hire.name,"agent_id":hire.agent_id,"project_id":hire.project_id,"supervisor_agent_id":hire.supervisor_agent_id}),
                None,
                None,
            )
        }
        _ => unreachable!(),
    };
    Ok(ConsentWrite {
        tool: tool.into(),
        arguments: arguments.clone(),
        target,
        grant_class,
        scope_id,
    })
}

impl RegistryStore {
    /// Run the same create, register or hire path the operator uses on a disposable
    /// snapshot, then roll it back. Checking before raising the consent card avoids
    /// holding a database writer lock while waiting for the user.
    pub fn precheck_consent_write(
        &self,
        caller: &str,
        tool: &str,
        arguments: Value,
        now: i64,
    ) -> Result<ConsentWrite, AgentMutationError> {
        let plan = self.read_agent(|conn| prepare_write(conn, caller, tool, &arguments))?;
        let request = ConsentRequest {
            identity: RequestIdentity {
                agent_id: caller.into(),
                tool: tool.into(),
                request_key: "precheck".into(),
            },
            args_digest: String::new(),
            scope_ref: String::new(),
            scope_epoch: 0,
            created_at_ms: now,
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        };
        let summary = std::cell::RefCell::new(None);
        let writer = JournalWriter {
            store: self,
            principal: "agent",
            confirmation: super::PlacementConfirmation::NotRequired,
            probe: Some(&summary),
            create_avatar: None,
            consent: Some(ConsentCommit {
                request,
                authorization: Authorization::OwnHire,
                action: tool.into(),
                target: json!(plan),
                granted: None,
                now_ms: now,
            }),
        };
        writer.apply_consent_write(&plan, now, None)?;
        Ok(plan)
    }
}

impl JournalWriter<'_> {
    pub fn apply_consent_write(
        &self,
        plan: &ConsentWrite,
        now: i64,
        agent_id: Option<&str>,
    ) -> Result<Vec<u8>, AgentMutationError> {
        let commit = self
            .consent
            .as_ref()
            .ok_or_else(|| AgentMutationError::new("invalid_request", "consent commit required"))?;
        if commit.action != plan.tool
            || commit.request.identity.tool != plan.tool
            || commit.target != json!(plan)
        {
            return Err(AgentMutationError::new(
                "invalid_request",
                "write does not match its consent commit",
            ));
        }
        let args = &plan.arguments;
        let text = |value: &Value, field: &str| {
            value[field].as_str().map(str::to_owned).ok_or_else(|| {
                AgentMutationError::new(
                    "consent_lost",
                    format!("saved consent plan has no string {field}"),
                )
            })
        };
        let result = match plan.tool.as_str() {
            "projects_create_workspace" => self.create_workspace_at(crate::CreateWorkspaceRequest { workspace_id:text(&plan.target,"workspace_id")?, name:text(args,"name")? }, now).map_err(Into::into),
            "projects_register" => self.register_at(crate::RegisterRequest { name:text(args,"name")?, roots:vec![text(&plan.target,"dir")?], workspace_id:plan.scope_id.clone(), ..Default::default() }, now).map_err(Into::into),
            "agents_create_head" => self.agent_mutation_with_id("agent.create", json!({"role":"head","name":args["name"],"tag":args["tag"],"project_id":plan.target["placement"]["project_id"],"request_key":commit.request.identity.request_key}), now, agent_id),
            "agents_hire" => self.agents_hire(&serde_json::from_value(args.clone()).map_err(|e| AgentMutationError::new("invalid_request", e.to_string()))?, now, agent_id),
            "agents_retire_hire" => self.agent_mutation("agent.dispose", json!({"agent_id":args["agent_id"],"request_key":commit.request.identity.request_key}), now),
            _ => Err(AgentMutationError::new("unknown_tool", &plan.tool)),
        };
        result.map_err(|e| {
            if self.probe.is_none()
                && !matches!(
                    e.code.as_str(),
                    "gone"
                        | "hire_cap_reached"
                        | "not_your_hire"
                        | "consent_stale"
                        | "request_key_conflict"
                        | "storage_error"
                )
            {
                AgentMutationError::new("consent_stale", e.message)
            } else {
                e
            }
        })
    }
}

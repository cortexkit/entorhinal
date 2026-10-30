//! Fleet-wide personal data. Only validated field changes travel in the identity log.
use crate::{
    agent::{AgentPrecheck, OperatorApproval},
    mutations::{append, domain, wire_value, Action},
    JournalWriter, RegistryError, RegistryStore,
};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const SET_OP: &str = "user.set";
// Join snapshots retain their original parts in the journal for audit. Project
// history may expose the other snapshot tables, but never personal profile rows.
pub(crate) fn public_journal_payload(payload: String) -> String {
    let Ok(mut value) = serde_json::from_str::<Value>(&payload) else {
        return payload;
    };
    let Some(tables) = value
        .pointer_mut("/snapshot_part/tables")
        .and_then(Value::as_object_mut)
    else {
        return payload;
    };
    let profile = tables.remove("user_profile").is_some();
    let expertise = tables.remove("user_expertise").is_some();
    if profile || expertise {
        String::from_utf8(crate::shared_entry::canonical_bytes(&value)).expect("JSON is UTF-8")
    } else {
        payload
    }
}
pub(crate) const V18_USER_PROFILE: &str = r#"
CREATE TABLE user_profile (
 id INTEGER PRIMARY KEY CHECK(id=1),
 name TEXT, address_as TEXT, pronouns TEXT, computer_savviness INTEGER, verbosity TEXT
);
CREATE TABLE user_expertise (
 scope_kind TEXT NOT NULL CHECK(scope_kind IN ('workspace','project')),
 scope_id TEXT NOT NULL, text TEXT NOT NULL, PRIMARY KEY(scope_kind,scope_id)
);
CREATE TABLE user_request (
 request_key TEXT PRIMARY KEY, body TEXT NOT NULL, response BLOB NOT NULL
);
CREATE TRIGGER user_project_removed AFTER DELETE ON project BEGIN
 DELETE FROM user_expertise WHERE scope_kind='project' AND scope_id=OLD.project_id;
END;
CREATE TRIGGER user_workspace_removed AFTER DELETE ON workspace BEGIN
 DELETE FROM user_expertise WHERE scope_kind='workspace' AND scope_id=OLD.workspace_id;
END;
"#;

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserScope {
    pub kind: String,
    pub id: String,
}
#[derive(Clone, Debug, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct UserGetRequest {
    pub project_id: Option<String>,
    pub token: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserSetRequest {
    pub field: String,
    #[serde(deserialize_with = "required_value")]
    pub value: Value,
    #[serde(
        default,
        deserialize_with = "present_scope",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<UserScope>,
    pub request_key: String,
    #[serde(
        default,
        deserialize_with = "present_actor",
        skip_serializing_if = "Option::is_none"
    )]
    pub actor: Option<String>,
}
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct UserEntry {
    pub op: String,
    pub field: String,
    #[serde(deserialize_with = "required_value")]
    pub value: Value,
    #[serde(
        default,
        deserialize_with = "present_scope",
        skip_serializing_if = "Option::is_none"
    )]
    pub scope: Option<UserScope>,
    pub principal: String,
    #[serde(
        default,
        deserialize_with = "present_actor",
        skip_serializing_if = "Option::is_none"
    )]
    pub actor: Option<String>,
}

fn required_value<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Value, D::Error> {
    Value::deserialize(de)
}
fn present_scope<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Option<UserScope>, D::Error> {
    UserScope::deserialize(de).map(Some)
}
fn present_actor<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Option<String>, D::Error> {
    String::deserialize(de).map(Some)
}
fn text(value: &str, limit: usize) -> Result<String, RegistryError> {
    // Check before trimming: a leading tab or trailing newline is still unsafe text.
    if value.chars().any(|c| c.is_control() || matches!(c, '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}' | '\u{2028}' | '\u{2029}')) {
        return Err(domain("invalid_value", "text contains forbidden characters"));
    }
    let value = value.trim();
    if value.is_empty() || value.chars().count() > limit {
        return Err(domain(
            "invalid_value",
            format!("text must contain 1–{limit} characters"),
        ));
    }
    Ok(value.into())
}
fn normalize(
    field: &str,
    value: &Value,
    scope: Option<&UserScope>,
) -> Result<Value, RegistryError> {
    let limit = match field {
        "name" | "address_as" => Some(64),
        "pronouns" => Some(32),
        "expertise" => Some(300),
        "computer_savviness" | "verbosity" => None,
        _ => return Err(domain("invalid_request", "unknown user field")),
    };
    if (field == "expertise") != scope.is_some() {
        return Err(domain(
            "invalid_request",
            "scope is required only for expertise",
        ));
    }
    if let Some(scope) = scope {
        if !matches!(scope.kind.as_str(), "workspace" | "project") {
            return Err(domain(
                "invalid_request",
                "scope kind must be workspace or project",
            ));
        }
    }
    if value.is_null() {
        return Ok(Value::Null);
    }
    if let Some(limit) = limit {
        return value
            .as_str()
            .ok_or_else(|| domain("invalid_value", "value must be text"))
            .and_then(|s| text(s, limit))
            .map(Value::String);
    }
    match field {
        "computer_savviness" if value.as_i64().is_some_and(|v| (1..=3).contains(&v)) => {
            Ok(value.clone())
        }
        "verbosity" => {
            let v = value
                .as_str()
                .ok_or_else(|| domain("invalid_value", "verbosity must be text"))?;
            let v = text(v, 8)?;
            if matches!(v.as_str(), "concise" | "balanced" | "verbose") {
                Ok(json!(v))
            } else {
                Err(domain(
                    "invalid_value",
                    "verbosity must be concise, balanced or verbose",
                ))
            }
        }
        _ => Err(domain(
            "invalid_value",
            "computer_savviness must be 1, 2 or 3",
        )),
    }
}
fn scope_exists(conn: &Connection, scope: &UserScope) -> Result<(), RegistryError> {
    let exists: bool = match scope.kind.as_str() {
        "workspace" => conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM workspace WHERE workspace_id=?1)",
            [&scope.id],
            |r| r.get(0),
        )?,
        "project" if !scope.id.starts_with("pj-implicit") => conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM project WHERE project_id=?1)",
            [&scope.id],
            |r| r.get(0),
        )?,
        _ => false,
    };
    if exists {
        Ok(())
    } else {
        Err(domain(
            "unknown_scope",
            "scope must name an existing workspace or registered project",
        ))
    }
}
impl UserSetRequest {
    pub fn normalized(mut self) -> Result<Self, RegistryError> {
        if self.request_key.is_empty()
            || self.request_key.len() > 128
            || !self.request_key.bytes().all(|b| (0x20..=0x7e).contains(&b))
        {
            return Err(domain(
                "invalid_request",
                "request_key must be 1–128 printable ASCII characters",
            ));
        }
        self.value = normalize(&self.field, &self.value, self.scope.as_ref())?;
        self.actor = self.actor.as_deref().map(|v| text(v, 128)).transpose()?;
        Ok(self)
    }
    fn entry(&self, principal: &str) -> UserEntry {
        UserEntry {
            op: SET_OP.into(),
            field: self.field.clone(),
            value: self.value.clone(),
            scope: self.scope.clone(),
            principal: principal.into(),
            actor: self.actor.clone(),
        }
    }
    fn summary(&self) -> Result<String, RegistryError> {
        let scope = self
            .scope
            .as_ref()
            .map(|s| format!(" of {} {}", s.kind, json!(s.id)))
            .unwrap_or_default();
        let value = if self.value.is_null() {
            "clear".into()
        } else {
            self.value.to_string()
        };
        let summary = format!("set user {}{scope} to {value}", self.field);
        if summary.chars().count() > crate::agent::OPERATOR_SUMMARY_LIMIT {
            return Err(domain(
                "operator_summary_too_long",
                "exact user change exceeds the operator summary limit",
            ));
        }
        Ok(summary)
    }
}
impl UserEntry {
    pub(crate) fn validate(&self, conn: &Connection) -> Result<(), RegistryError> {
        if self.op != SET_OP {
            return Err(domain("invalid_request", "unknown user entry op"));
        }
        if normalize(&self.field, &self.value, self.scope.as_ref())? != self.value
            || self.actor.as_deref().map(|v| text(v, 128)).transpose()? != self.actor
        {
            return Err(domain("invalid_value", "user entry text must be trimmed"));
        }
        if let Some(scope) = &self.scope {
            scope_exists(conn, scope)?;
        }
        Ok(())
    }
    pub(crate) fn restore(&self, tx: &Transaction<'_>) -> rusqlite::Result<()> {
        self.validate(tx).map_err(sql_error)?;
        if let Some(scope) = &self.scope {
            if self.value.is_null() {
                tx.execute(
                    "DELETE FROM user_expertise WHERE scope_kind=?1 AND scope_id=?2",
                    params![scope.kind, scope.id],
                )?;
            } else {
                tx.execute("INSERT INTO user_expertise VALUES(?1,?2,?3) ON CONFLICT(scope_kind,scope_id) DO UPDATE SET text=excluded.text", params![scope.kind,scope.id,self.value.as_str()])?;
            }
        } else {
            tx.execute("INSERT OR IGNORE INTO user_profile(id) VALUES(1)", [])?;
            // The field was checked against the fixed global-field list above.
            let value = match &self.value {
                Value::Null => rusqlite::types::Value::Null,
                Value::String(v) => rusqlite::types::Value::Text(v.clone()),
                _ => {
                    rusqlite::types::Value::Integer(self.value.as_i64().expect("validated integer"))
                }
            };
            tx.execute(
                &format!("UPDATE user_profile SET {}=?1 WHERE id=1", self.field),
                [value],
            )?;
            tx.execute("DELETE FROM user_profile WHERE name IS NULL AND address_as IS NULL AND pronouns IS NULL AND computer_savviness IS NULL AND verbosity IS NULL", [])?;
        }
        Ok(())
    }
}
pub(crate) fn sql_error(error: RegistryError) -> rusqlite::Error {
    rusqlite::Error::ToSqlConversionFailure(Box::new(error))
}
pub(crate) fn replay_user(tx: &Transaction<'_>, value: Value) -> rusqlite::Result<()> {
    let entry: UserEntry = serde_json::from_value(value.get("entry").cloned().unwrap_or(value))
        .map_err(crate::shared_entry::decode_error)?;
    entry.restore(tx)
}

fn record(conn: &Connection) -> rusqlite::Result<Value> {
    let user = conn.query_row("SELECT name,address_as,pronouns,computer_savviness,verbosity FROM user_profile WHERE id=1", [], |r| {
        let mut user = Map::new();
        for (i, field) in ["name","address_as","pronouns","computer_savviness","verbosity"].iter().enumerate() {
            let value = if i == 3 { r.get::<_,Option<i64>>(i)?.map(Value::from) } else { r.get::<_,Option<String>>(i)?.map(Value::from) };
            if let Some(value) = value { user.insert((*field).into(),value); }
        }
        Ok(Value::Object(user))
    }).optional()?.unwrap_or_else(|| json!({}));
    let expertise = conn.prepare("SELECT scope_kind,scope_id,text FROM user_expertise ORDER BY CASE scope_kind WHEN 'workspace' THEN 0 ELSE 1 END,scope_id")?.query_map([], |r| Ok(json!({"scope":{"kind":r.get::<_,String>(0)?,"id":r.get::<_,String>(1)?},"text":r.get::<_,String>(2)?})))?.collect::<rusqlite::Result<Vec<_>>>()?;
    Ok(json!({"user":user,"expertise":expertise}))
}
fn reply(conn: &Connection, req: &UserGetRequest) -> Result<Value, RegistryError> {
    let mut value = record(conn)?;
    if let Some(project) = &req.project_id {
        let exists: bool = conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM project WHERE project_id=?1)",
            [project],
            |r| r.get(0),
        )?;
        if project.starts_with("pj-implicit") || !exists {
            return Err(domain(
                "unknown_project",
                "project must name a registered project",
            ));
        }
        let effective: Option<Value> = conn.query_row("SELECT e.scope_kind,e.scope_id,e.text FROM user_expertise e WHERE (e.scope_kind='project' AND e.scope_id=?1) OR (e.scope_kind='workspace' AND e.scope_id=(SELECT workspace_id FROM project_workspace WHERE project_id=?1)) ORDER BY CASE e.scope_kind WHEN 'project' THEN 0 ELSE 1 END LIMIT 1",[project],|r| Ok(json!({"from":{"kind":r.get::<_,String>(0)?,"id":r.get::<_,String>(1)?},"text":r.get::<_,String>(2)?}))).optional()?;
        if let Some(effective) = effective {
            value["effective_expertise"] = effective;
        }
    }
    // Hash the returned state, not the journal head: unrelated writes and no-ops
    // must not invalidate a profile token. Project reads also cover placement.
    let token = format!(
        "u1:{}",
        blake3::hash(&crate::shared_entry::canonical_bytes(&value)).to_hex()
    );
    if req.token.as_deref() == Some(&token) {
        return Ok(json!({"token":token,"unchanged":true}));
    }
    value["token"] = json!(token);
    value["unchanged"] = json!(false);
    Ok(value)
}
fn cached(
    conn: &Connection,
    req: &UserSetRequest,
    original_body: &str,
) -> Result<Option<Vec<u8>>, RegistryError> {
    let row: Option<(String, Vec<u8>)> = conn
        .query_row(
            "SELECT body,response FROM user_request WHERE request_key=?1",
            [&req.request_key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    if let Some((body, response)) = row {
        if body != original_body {
            return Err(domain(
                "request_key_reused",
                "request key has a different body",
            ));
        }
        return Ok(Some(response));
    }
    Ok(None)
}
impl RegistryStore {
    pub fn user_get(&self, req: UserGetRequest) -> Result<Value, RegistryError> {
        // Preserve domain errors while using one committed read snapshot.
        let mut refusal = None;
        let out = self.read(|conn| {
            reply(conn, &req).map_err(|e| {
                refusal = Some(e);
                rusqlite::Error::QueryReturnedNoRows
            })
        });
        out.map_err(|e| refusal.unwrap_or(e))
    }
    pub fn probe_user_set(&self, req: &UserSetRequest) -> Result<Option<Vec<u8>>, RegistryError> {
        let body = serde_json::to_string(req).expect("request encodes");
        let req = req.clone().normalized()?;
        let mut refusal = None;
        let out = self.read(|conn| {
            cached(conn, &req, &body).map_err(|e| {
                refusal = Some(e);
                rusqlite::Error::QueryReturnedNoRows
            })
        });
        out.map_err(|e| refusal.unwrap_or(e))
    }
    pub fn precheck_user_confirmation(
        &self,
        req: &UserSetRequest,
    ) -> Result<AgentPrecheck, RegistryError> {
        if let Some(body) = self.probe_user_set(req)? {
            return Ok(AgentPrecheck::Cached(body));
        }
        let req = req.clone().normalized()?;
        let mut refusal = None;
        self.read(|conn| {
            req.entry("direct").validate(conn).map_err(|e| {
                refusal = Some(e);
                rusqlite::Error::QueryReturnedNoRows
            })
        })
        .map_err(|e| refusal.unwrap_or(e))?;
        Ok(AgentPrecheck::Ready(req.summary()?))
    }
}
impl JournalWriter<'_> {
    pub fn user_set(
        &self,
        req: UserSetRequest,
        now: i64,
        approval: Option<&OperatorApproval>,
    ) -> Result<Vec<u8>, RegistryError> {
        let original_body = serde_json::to_string(&req).expect("request encodes");
        let req = req.normalized()?;
        self.mutation(SET_OP, None, |tx| {
            if let Some(body) = cached(tx, &req, &original_body)? {
                let value: Value = serde_json::from_slice(&body)
                    .map_err(|e| domain("storage_error", e.to_string()))?;
                return Ok(Action {
                    changed: false,
                    seq: None,
                    value: value["result"].clone(),
                    payload: json!({}),
                });
            }
            let entry = req.entry(self.principal);
            entry.validate(tx)?;
            if self.principal == "direct"
                && !approval.is_some_and(|a| req.summary().is_ok_and(|s| a.approves(&s)))
            {
                return Err(domain(
                    "operator_approval_stale",
                    "user change needs approval of the exact summary",
                ));
            }
            let before = record(tx)?;
            entry.restore(tx)?;
            let changed = before != record(tx)?;
            let payload = json!({"entry":entry});
            let seq = if changed {
                Some(append(
                    tx,
                    SET_OP,
                    &payload,
                    req.actor.as_deref().unwrap_or("user"),
                    None,
                    now,
                    self.principal,
                )?)
            } else {
                None
            };
            let value = reply(tx, &UserGetRequest::default())?;
            let response = wire_value(value.clone())?;
            tx.execute(
                "INSERT INTO user_request VALUES(?1,?2,?3)",
                params![req.request_key, original_body, response],
            )?;
            Ok(Action {
                changed,
                seq,
                value,
                payload,
            })
        })
    }
}

/// Snapshots contain complete rows rather than individual field writes. Validate
/// the resulting state with the same value and scope rules as local changes.
pub(crate) fn validate_projection(conn: &Connection) -> rusqlite::Result<()> {
    // Local clears remove an all-absent row. Require the same representation
    // in snapshots too, so a later no-op cannot change the stored state without
    // a journal entry.
    let empty_row: bool = conn.query_row("SELECT EXISTS(SELECT 1 FROM user_profile WHERE name IS NULL AND address_as IS NULL AND pronouns IS NULL AND computer_savviness IS NULL AND verbosity IS NULL)",[],|r|r.get(0))?;
    if empty_row {
        return Err(sql_error(domain(
            "invalid_value",
            "empty profile must omit its row",
        )));
    }
    let state = record(conn)?;
    for (field, value) in state["user"].as_object().expect("user object") {
        if normalize(field, value, None).map_err(sql_error)? != *value {
            return Err(sql_error(domain("invalid_value", "untrimmed user field")));
        }
    }
    for item in state["expertise"].as_array().expect("expertise array") {
        let scope: UserScope = serde_json::from_value(item["scope"].clone())
            .map_err(crate::shared_entry::decode_error)?;
        scope_exists(conn, &scope).map_err(sql_error)?;
        if normalize("expertise", &item["text"], Some(&scope)).map_err(sql_error)? != item["text"] {
            return Err(sql_error(domain("invalid_value", "untrimmed expertise")));
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "user_profile_tests.rs"]
mod tests;

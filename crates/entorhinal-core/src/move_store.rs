//! Machine-local move progress, independent of the identity journal.

/// Migration 9 keeps move progress outside the journal-replayed tables. A
/// received session reference can bind to an agent before a local host opens it,
/// so its session_binding row permits NULL harness and session_id.
pub const V9_AGENT_MOVES: &str = r#"
CREATE TABLE agent_move (
    cut_id TEXT PRIMARY KEY NOT NULL,
    agent_id TEXT NOT NULL,
    side TEXT NOT NULL CHECK(side IN ('source','receiver')),
    peer TEXT NOT NULL,
    step TEXT NOT NULL CHECK(step IN (
        'preparing','exported','sealed','published','releasing','released','aborted',
        'picked_up','imported','staged','claimed','activating','activated','done','taken_over'
    )),
    plan TEXT NOT NULL,
    request_body TEXT NULL,
    record_version INTEGER NOT NULL,
    claim_version INTEGER NULL,
    claim_take_body TEXT NULL,
    claim_take_version INTEGER NULL,
    claim_landed_version INTEGER NULL,
    seal_sent INTEGER NOT NULL CHECK(seal_sent IN (0,1)),
    abort_requested TEXT NULL,
    hold TEXT NULL,
    cut_accept TEXT NULL,
    reason TEXT NULL,
    detail TEXT NULL,
    request_key TEXT UNIQUE NULL,
    attempts INTEGER NOT NULL,
    activated_at_ms INTEGER NULL,
    created_at_ms INTEGER NOT NULL,
    updated_at_ms INTEGER NOT NULL
);
CREATE UNIQUE INDEX agent_move_open_agent ON agent_move(agent_id)
    WHERE step NOT IN ('released','aborted','done','taken_over');

CREATE TABLE move_setting (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    lid_close_moves INTEGER NOT NULL DEFAULT 0 CHECK(lid_close_moves IN (0,1))
);
INSERT INTO move_setting(id, lid_close_moves) VALUES (1, 0);

CREATE TABLE session_binding_v9 (
    ref_kind TEXT NOT NULL,
    ref TEXT NOT NULL,
    agent_id TEXT NOT NULL,
    harness TEXT NULL,
    session_id TEXT NULL,
    project_root TEXT NOT NULL,
    registered_by TEXT NOT NULL CHECK(registered_by IN ('orchestrator','host','move')),
    registered_at_ms INTEGER NOT NULL,
    PRIMARY KEY (ref_kind, ref)
);
INSERT INTO session_binding_v9
    (ref_kind, ref, agent_id, harness, session_id, project_root, registered_by, registered_at_ms)
    SELECT ref_kind, ref, agent_id, harness, session_id, '', registered_by, registered_at_ms
    FROM session_binding;
DROP TABLE session_binding;
ALTER TABLE session_binding_v9 RENAME TO session_binding;
CREATE UNIQUE INDEX session_binding_session ON session_binding(harness, session_id);
"#;

use rusqlite::{params, OptionalExtension};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{agent::AgentMutationError, move_record::MoveRef, RegistryError, RegistryStore};

macro_rules! text_enum {
    ($name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
        pub enum $name {
            $(#[serde(rename = $text)] $variant),+
        }

        impl rusqlite::types::ToSql for $name {
            fn to_sql(&self) -> rusqlite::Result<rusqlite::types::ToSqlOutput<'_>> {
                let text = match self { $(Self::$variant => $text),+ };
                Ok(text.into())
            }
        }

        impl rusqlite::types::FromSql for $name {
            fn column_result(value: rusqlite::types::ValueRef<'_>) -> rusqlite::types::FromSqlResult<Self> {
                match value.as_str()? {
                    $($text => Ok(Self::$variant),)+
                    _ => Err(rusqlite::types::FromSqlError::InvalidType),
                }
            }
        }
    };
}

text_enum!(MoveSide { Source => "source", Receiver => "receiver" });
text_enum!(MoveStep {
    Preparing => "preparing",
    Exported => "exported",
    Sealed => "sealed",
    Published => "published",
    Releasing => "releasing",
    Released => "released",
    Aborted => "aborted",
    PickedUp => "picked_up",
    Imported => "imported",
    Staged => "staged",
    Claimed => "claimed",
    Activating => "activating",
    Activated => "activated",
    Done => "done",
    TakenOver => "taken_over",
});

impl MoveStep {
    pub fn before_boundary(self, side: MoveSide) -> bool {
        match side {
            MoveSide::Source => matches!(
                self,
                Self::Preparing | Self::Exported | Self::Sealed | Self::Published
            ),
            MoveSide::Receiver => matches!(
                self,
                Self::PickedUp | Self::Imported | Self::Staged | Self::Claimed
            ),
        }
    }

    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Released | Self::Aborted | Self::Done | Self::TakenOver
        )
    }
}

/// A durable move checkpoint. Storage preserves the JSON values without
/// interpreting them: the participant/ref plan and the claim body prepared for
/// a compare-and-swap must be available unchanged when the driver restarts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentMove {
    pub cut_id: String,
    pub agent_id: String,
    pub side: MoveSide,
    pub peer: String,
    pub step: MoveStep,
    pub plan: Value,
    pub request_body: Option<Value>,
    pub record_version: i64,
    pub claim_version: Option<i64>,
    pub claim_take_body: Option<Value>,
    pub claim_take_version: Option<i64>,
    pub claim_landed_version: Option<i64>,
    pub seal_sent: bool,
    pub abort_requested: Option<Value>,
    pub hold: Option<Value>,
    pub cut_accept: Option<Value>,
    pub reason: Option<String>,
    pub detail: Option<Value>,
    pub request_key: Option<String>,
    pub attempts: i64,
    pub activated_at_ms: Option<i64>,
    pub created_at_ms: i64,
    pub updated_at_ms: i64,
}

impl AgentMove {
    pub fn abort_stuck(&self) -> bool {
        self.abort_requested.is_some()
            && self
                .hold
                .as_ref()
                .is_some_and(|h| h["state"] == "stuck" && h["op"] == "agent_data.abort")
    }

    /// Operator-facing diagnostics are derived from the durable participant
    /// checkpoints, without exposing the private recovery plan or claim bodies.
    pub fn notices(&self) -> Value {
        let mut notices = serde_json::Map::new();
        if self.side == MoveSide::Source && self.step == MoveStep::Published {
            notices.insert("status".into(), "waiting_for_target".into());
            notices.insert(
                "message".into(),
                "waiting for the target Mac to pick up the move".into(),
            );
            notices.insert(
                "waiting_since_ms".into(),
                self.plan["_source"]["published_at_ms"]
                    .as_i64()
                    .unwrap_or(self.updated_at_ms)
                    .into(),
            );
        }
        let diagnostics: Vec<_> = ["_source", "_receiver"]
            .into_iter()
            .flat_map(|key| {
                self.plan[key]["errors"]
                    .as_array()
                    .into_iter()
                    .flatten()
                    .cloned()
            })
            .collect();
        if !diagnostics.is_empty() {
            notices.insert("diagnostics".into(), Value::Array(diagnostics));
        }
        let warnings: Vec<_> = self.plan["_source"]["enforced"].as_object().into_iter().flatten()
            .filter(|(_, enforced)| **enforced == false)
            .map(|(module, _)| serde_json::json!({"module_id":module,"code":"release_not_enforced","message":format!("The old Mac cannot block writes for {module}; anything typed there becomes a fork")}))
            .collect();
        if !warnings.is_empty() {
            notices.insert("warnings".into(), Value::Array(warnings));
        }
        let progress = if self.side == MoveSide::Source {
            &self.plan["_source"]
        } else {
            &self.plan["_receiver"]
        };
        let violation = &progress["contract_violation"];
        let fork = &progress["activation_fork"];
        let (status, detail, message) =
            if let Some(detail) = self.hold.as_ref().filter(|_| self.abort_stuck()) {
                let module = detail["module_id"].as_str().unwrap_or("the participant");
                let message = if self.side == MoveSide::Source {
                    format!("abort stuck, agent frozen on this Mac until {module} is repaired")
                } else {
                    format!("abort stuck, cleanup cannot finish until {module} is repaired")
                };
                ("abort_stuck", detail, message)
            } else if self.side == MoveSide::Receiver
                && self.step == MoveStep::Activating
                && (!fork.is_null() || !violation.is_null())
            {
                let detail = if !fork.is_null() { fork } else { violation };
                (
                    "held_as_fork",
                    detail,
                    "held as a fork, awaiting the operator".into(),
                )
            } else if !violation.is_null() {
                (
                    "contract_violation",
                    violation,
                    "participant contract violation; operator repair is required".into(),
                )
            } else {
                return Value::Object(notices);
            };
        notices.insert("status".into(), status.into());
        notices.insert("message".into(), message.into());
        notices.insert("diagnostic".into(), detail.clone());
        Value::Object(notices)
    }

    pub fn error_codes(&self) -> Vec<String> {
        let mut codes = Vec::new();
        for key in ["_source", "_receiver"] {
            if let Some(errors) = self.plan[key]["errors"].as_array() {
                codes.extend(
                    errors
                        .iter()
                        .filter_map(|e| e["code"].as_str().map(str::to_owned)),
                );
            }
        }
        if let Some(accept) = self
            .cut_accept
            .as_ref()
            .filter(|v| v["state"] != "accepted")
        {
            codes.push(
                accept["code"]
                    .as_str()
                    .unwrap_or("cut_accept_pending")
                    .into(),
            );
        }
        if let Some(hold) = self.hold.as_ref().filter(|v| v["move_error"] == true) {
            if let Some(code) = hold["code"].as_str() {
                codes.push(code.into());
            }
        }
        codes.sort();
        codes.dedup();
        codes
    }
}

fn public_move(conn: &rusqlite::Connection, row: &AgentMove) -> rusqlite::Result<Value> {
    let mut value = serde_json::to_value(row)
        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
    let object = value.as_object_mut().ok_or(rusqlite::Error::InvalidQuery)?;
    for key in [
        "plan",
        "request_body",
        "claim_version",
        "claim_take_body",
        "claim_take_version",
        "claim_landed_version",
    ] {
        object.remove(key);
    }
    object.insert(
        "peer_name".into(),
        serde_json::json!(crate::claim_cache::holder_name(conn, Some(&row.peer))?),
    );
    if let Value::Object(notices) = row.notices() {
        object.extend(notices);
    }
    Ok(value)
}

pub(crate) type WhereMoveView = (Option<Value>, Vec<Value>, Vec<Value>, Vec<Value>);

pub(crate) fn where_moves(
    conn: &rusqlite::Connection,
    agent: &str,
) -> rusqlite::Result<WhereMoveView> {
    let mut stmt = conn.prepare(&format!(
        "SELECT {MOVE_COLUMNS} FROM agent_move WHERE agent_id=?1 ORDER BY created_at_ms, cut_id"
    ))?;
    let rows = stmt
        .query_map([agent], AgentMove::from_row)?
        .collect::<Result<Vec<_>, _>>()?;
    let mut moving = None;
    let mut errors = Vec::new();
    let mut warnings = Vec::new();
    let mut diagnostics = Vec::new();
    for row in rows {
        let notices = row.notices();
        if !row.step.is_terminal() {
            let mut value = serde_json::json!({"cut_id":row.cut_id,"side":row.side,"step":row.step,"peer":row.peer,"peer_name":crate::claim_cache::holder_name(conn, Some(&row.peer))?});
            if row.reason.as_deref() == Some("diverged") {
                if let Some(detail) = &row.detail {
                    value["detail"] = detail.clone();
                    value["reason"] = serde_json::json!(row.reason);
                }
            }
            if let Some(hold) = &row.hold {
                value["hold"] = hold.clone();
            }
            value
                .as_object_mut()
                .expect("this function just constructed an object with json!")
                .extend(
                    notices
                        .as_object()
                        .into_iter()
                        .flatten()
                        .filter(|(key, _)| key.as_str() != "warnings")
                        .map(|(key, value)| (key.clone(), value.clone())),
                );
            moving = Some(value);
        }
        for code in row.error_codes() {
            errors.push(serde_json::json!({"cut_id":row.cut_id,"code":code}));
        }
        if row.reason.as_deref() == Some("diverged") {
            if let Some(detail) = &row.detail {
                let mut item = detail.clone();
                item["cut_id"] = serde_json::json!(row.cut_id);
                diagnostics.push(item);
            }
        }
        if let Some(items) = notices["warnings"].as_array() {
            for item in items {
                let mut item = item.clone();
                item["cut_id"] = serde_json::json!(row.cut_id);
                warnings.push(item);
            }
        }
        if let Some(items) = notices["diagnostics"].as_array() {
            for item in items {
                let mut item = item.clone();
                item["cut_id"] = serde_json::json!(row.cut_id);
                diagnostics.push(item);
            }
        }
    }
    Ok((moving, errors, warnings, diagnostics))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct MoveSetting {
    pub lid_close_moves: bool,
}

const MOVE_COLUMNS: &str = "cut_id, agent_id, side, peer, step, plan, request_body,
    record_version, claim_version, claim_take_body, claim_take_version, claim_landed_version,
    seal_sent, abort_requested, hold, cut_accept, reason, detail, request_key, attempts,
    activated_at_ms, created_at_ms, updated_at_ms";

fn json_column(row: &rusqlite::Row<'_>, index: usize) -> rusqlite::Result<Option<Value>> {
    let text: Option<String> = row.get(index)?;
    text.map(|text| {
        serde_json::from_str(&text).map_err(|error| {
            rusqlite::Error::FromSqlConversionFailure(
                index,
                rusqlite::types::Type::Text,
                Box::new(error),
            )
        })
    })
    .transpose()
}

impl AgentMove {
    fn from_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            cut_id: row.get(0)?,
            agent_id: row.get(1)?,
            side: row.get(2)?,
            peer: row.get(3)?,
            step: row.get(4)?,
            plan: json_column(row, 5)?.ok_or(rusqlite::Error::InvalidColumnType(
                5,
                "plan".into(),
                rusqlite::types::Type::Null,
            ))?,
            request_body: json_column(row, 6)?,
            record_version: row.get(7)?,
            claim_version: row.get(8)?,
            claim_take_body: json_column(row, 9)?,
            claim_take_version: row.get(10)?,
            claim_landed_version: row.get(11)?,
            seal_sent: row.get(12)?,
            abort_requested: json_column(row, 13)?,
            hold: json_column(row, 14)?,
            cut_accept: json_column(row, 15)?,
            reason: row.get(16)?,
            detail: json_column(row, 17)?,
            request_key: row.get(18)?,
            attempts: row.get(19)?,
            activated_at_ms: row.get(20)?,
            created_at_ms: row.get(21)?,
            updated_at_ms: row.get(22)?,
        })
    }

    fn write(&self, conn: &rusqlite::Connection, insert: bool) -> rusqlite::Result<usize> {
        let sql = if insert {
            format!(
                "INSERT INTO agent_move ({MOVE_COLUMNS}) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12,
                ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23)"
            )
        } else {
            "UPDATE agent_move SET agent_id=?2, side=?3, peer=?4, step=?5, plan=?6,
                request_body=?7, record_version=?8, claim_version=?9, claim_take_body=?10,
                claim_take_version=?11, claim_landed_version=?12, seal_sent=?13,
                abort_requested=?14, hold=?15, cut_accept=?16, reason=?17, detail=?18,
                request_key=?19, attempts=?20, activated_at_ms=?21, created_at_ms=?22,
                updated_at_ms=?23 WHERE cut_id=?1"
                .into()
        };
        conn.execute(
            &sql,
            params![
                self.cut_id,
                self.agent_id,
                self.side,
                self.peer,
                self.step,
                self.plan.to_string(),
                self.request_body.as_ref().map(Value::to_string),
                self.record_version,
                self.claim_version,
                self.claim_take_body.as_ref().map(Value::to_string),
                self.claim_take_version,
                self.claim_landed_version,
                self.seal_sent,
                self.abort_requested.as_ref().map(Value::to_string),
                self.hold.as_ref().map(Value::to_string),
                self.cut_accept.as_ref().map(Value::to_string),
                self.reason,
                self.detail.as_ref().map(Value::to_string),
                self.request_key,
                self.attempts,
                self.activated_at_ms,
                self.created_at_ms,
                self.updated_at_ms,
            ],
        )
    }
}

impl RegistryStore {
    pub fn move_authority_cut_over(&self) -> Result<bool, RegistryError> {
        self.read(|conn| {
            Ok(conn
                .query_row(
                    "SELECT 1 FROM registry_journal WHERE op=?1 LIMIT 1",
                    [crate::agent::IDENTITY_MARKER_OP],
                    |_| Ok(()),
                )
                .optional()?
                .is_some())
        })
    }

    /// Targets are resolved only against persisted member names, not against
    /// enrollment guesses or a display name saved in an earlier request.
    pub fn resolve_move_member(&self, to: &str) -> Result<(String, String), AgentMutationError> {
        let rows = self.read(|conn| {
            let mut stmt = conn.prepare("SELECT author,display_name FROM member_name WHERE author=?1 OR display_name=?1 ORDER BY author")?;
            let rows = stmt.query_map([to], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?.collect::<Result<Vec<_>, _>>()?;
            Ok(rows)
        }).map_err(|e| crate::session_refusal("storage_error", e.to_string(), None))?;
        match rows.as_slice() {
            [member] => Ok(member.clone()),
            [] => Err(crate::session_refusal(
                "unknown_member",
                "target has no member_name row",
                None,
            )),
            _ => Err(crate::session_refusal(
                "invalid_request",
                "target name is ambiguous",
                None,
            )),
        }
    }

    /// Read the agent's live bound (ref_kind, ref) pairs in stable order. Participants
    /// move data by these pairs, not by the source Mac's host-session identifiers.
    pub fn agent_bound_refs(&self, agent_id: &str) -> Result<Vec<MoveRef>, RegistryError> {
        self.read(|conn| bound_refs(conn, agent_id))
    }

    pub fn agent_has_bindings(&self, agent_id: &str) -> Result<bool, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM session_binding WHERE agent_id=?1)",
                [agent_id],
                |r| r.get(0),
            )
        })
    }

    /// Commit a fully checked source move. The key lookup and the registration
    /// fence share the insert's transaction, so retries and new sessions cannot
    /// slip between the final checks and the durable checkpoint.
    pub fn admit_agent_move(
        &self,
        row: &AgentMove,
        checked_refs: &[MoveRef],
    ) -> Result<AgentMove, AgentMutationError> {
        let outcome = self.with_where_move_changes(|tx| {
            let check = || -> Result<AgentMove, AgentMutationError> {
                if row.side != MoveSide::Source
                    || row.step != MoveStep::Preparing
                    || row.request_key.is_none()
                    || row.request_body.is_none()
                {
                    return Err(crate::session_refusal(
                        "invalid_request",
                        "invalid admission checkpoint",
                        None,
                    ));
                }
                let existing = tx
                    .query_row(
                        &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE request_key=?1"),
                        [&row.request_key],
                        AgentMove::from_row,
                    )
                    .optional()?;
                if let Some(existing) = existing {
                    return if existing.request_body == row.request_body {
                        Ok(existing)
                    } else {
                        Err(crate::session_refusal(
                            "request_key_reused",
                            "request key belongs to a different move request",
                            None,
                        ))
                    };
                }
                if bound_refs(tx, &row.agent_id)? != checked_refs {
                    return Err(crate::session_refusal(
                        "agent_busy",
                        "a session started during the check",
                        None,
                    ));
                }
                if checked_refs.len() > 256 {
                    return Err(crate::session_refusal(
                        "live_ref_cap",
                        "too many live session references",
                        Some(serde_json::json!({"live_refs":checked_refs.len(),"cap":256})),
                    ));
                }
                let pins = crate::catalog_agent::read_pins(tx, &row.agent_id)?;
                let digest = if pins.is_empty() {
                    None
                } else {
                    Some(crate::catalog_agent::pin_digest(&pins)?)
                };
                if row.plan["pin_digest"].as_str() != digest.as_deref() {
                    return Err(crate::session_refusal(
                        "agent_busy",
                        "catalog pins changed during the move check",
                        None,
                    ));
                }
                let open = open_moves(tx)?;
                if let Some(existing) = open.iter().find(|m| m.agent_id == row.agent_id) {
                    return Err(crate::session_refusal(
                        "move_in_progress",
                        "agent has an open move",
                        Some(move_detail(existing)),
                    ));
                }
                if open.len() >= 4 {
                    return Err(crate::session_refusal(
                        "move_cap",
                        "four moves are already open",
                        None,
                    ));
                }
                row.write(tx, true)?;
                Ok(row.clone())
            };
            Ok(check())
        });
        outcome.map_err(|e| crate::session_refusal("storage_error", e.to_string(), None))?
    }

    /// Acquire a receiver move slot atomically with source admission. If this
    /// cut_id already has a checkpoint, return it unchanged so an inbox poll
    /// after a crash cannot insert a second receiver row for the same move.
    pub fn admit_received_move(&self, row: &AgentMove) -> Result<AgentMove, AgentMutationError> {
        let outcome = self.with_where_move_changes(|tx| {
            let check = || -> Result<AgentMove, AgentMutationError> {
                if row.side != MoveSide::Receiver
                    || !matches!(row.step, MoveStep::PickedUp | MoveStep::Aborted)
                {
                    return Err(crate::session_refusal(
                        "invalid_request",
                        "invalid receiver checkpoint",
                        None,
                    ));
                }
                if let Some(existing) = tx
                    .query_row(
                        &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE cut_id=?1"),
                        [&row.cut_id],
                        AgentMove::from_row,
                    )
                    .optional()?
                {
                    return Ok(existing);
                }
                {
                    let open = open_moves(tx)?;
                    if let Some(existing) = open.iter().find(|m| m.agent_id == row.agent_id) {
                        return Err(crate::session_refusal(
                            "move_in_progress",
                            "agent has an open move",
                            Some(move_detail(existing)),
                        ));
                    }
                    if open.len() >= 4 {
                        return Err(crate::session_refusal(
                            "move_cap",
                            "four moves are already open",
                            None,
                        ));
                    }
                }
                row.write(tx, true)?;
                Ok(row.clone())
            };
            Ok(check())
        });
        outcome.map_err(|e| crate::session_refusal("storage_error", e.to_string(), None))?
    }

    /// Insert a checkpoint, refusing duplicate cuts or request keys and a second
    /// non-terminal move for the same agent, on either side.
    pub fn insert_agent_move(&self, row: &AgentMove) -> Result<(), RegistryError> {
        self.with_where_move_changes(|conn| {
            row.write(conn, true)?;
            Ok(())
        })
        .map_err(RegistryError::Store)
    }

    /// Replace an existing checkpoint; a missing cut returns false, never inserts.
    pub fn update_agent_move(&self, row: &AgentMove) -> Result<bool, RegistryError> {
        self.with_where_move_changes(|conn| Ok(row.write(conn, false)? != 0))
            .map_err(RegistryError::Store)
    }

    /// Checkpoint against operator edits in the same fenced transaction. An
    /// accepted abort wins the race with entry into the irreversible boundary.
    /// Background accept diagnostics belong to their own task and are preserved.
    pub fn checkpoint_agent_move(
        &self,
        row: &AgentMove,
        clear_abort: bool,
    ) -> Result<AgentMove, RegistryError> {
        self.with_where_move_changes(|tx| {
            let current = load_move(tx, &row.cut_id)?;
            if row.step == MoveStep::Activating
                && current.step == MoveStep::Claimed
                && current.abort_requested.is_some()
            {
                return Ok(current);
            }
            let mut next = row.clone();
            if clear_abort {
                next.abort_requested = None;
            } else if current.abort_requested.is_some() {
                next.abort_requested = current.abort_requested;
            }
            next.cut_accept = current.cut_accept;
            next.write(tx, false)?;
            Ok(next)
        })
        .map_err(RegistryError::Store)
    }

    /// Decide an operator abort without a read/write gap. Refusal leaves every
    /// column untouched, including timestamps, so recovery cannot turn it into cleanup.
    pub fn request_agent_move_abort(
        &self,
        cut_id: &str,
        now_ms: i64,
    ) -> Result<AgentMove, AgentMutationError> {
        self.with_where_move_changes(|tx| {
            let mut row = load_move(tx, cut_id)?;
            if row.step == MoveStep::Aborted {
                return Ok(Ok(row));
            }
            if !row.step.before_boundary(row.side) {
                return Ok(Err(crate::session_refusal(
                    "invalid_state",
                    "move has crossed the abort boundary",
                    Some(move_detail(&row)),
                )));
            }
            if row.abort_stuck() {
                let request = row
                    .abort_requested
                    .as_mut()
                    .and_then(Value::as_object_mut)
                    .ok_or(rusqlite::Error::InvalidQuery)?;
                request.insert("retry_pending".into(), Value::Bool(true));
            } else if row.abort_requested.is_some() {
                return Ok(Ok(row));
            } else {
                row.abort_requested = Some(serde_json::json!({"reason":"operator","detail":null}));
            }
            row.updated_at_ms = now_ms;
            row.write(tx, false)?;
            Ok(Ok(row))
        })
        .map_err(|e| crate::session_refusal("storage_error", e.to_string(), None))?
    }

    /// Record activation and ref ownership together. Existing bindings for this
    /// agent retain their host metadata; new ones have no local host session yet.
    pub fn activate_agent_move(
        &self,
        row: &AgentMove,
        refs: &[MoveRef],
    ) -> Result<AgentMove, AgentMutationError> {
        self.activate_agent_move_carrying(row, refs, &[])
            .map(|(row, _)| row)
    }

    /// [`Self::activate_agent_move`], installing the bindings the source
    /// carried in place of the placeholders it would otherwise write, in the
    /// same transaction as the activation checkpoint.
    pub fn activate_agent_move_carrying(
        &self,
        row: &AgentMove,
        refs: &[MoveRef],
        carried: &[crate::session_carry::CarriedBinding],
    ) -> Result<(AgentMove, crate::session_carry::CarriedInstall), AgentMutationError> {
        self.with_where_move_changes(|tx| {
            let current = load_move(tx, &row.cut_id)?;
            if current.side != MoveSide::Receiver
                || current.step != MoveStep::Activating
                || row.step != MoveStep::Activated
                || row.activated_at_ms.is_none()
            {
                return Ok(Err(crate::session_refusal(
                    "invalid_state",
                    "invalid activation checkpoint",
                    Some(move_detail(&current)),
                )));
            }
            // Check all owners before inserting anything: a refusal must not
            // leave a partial set of moved-in bindings.
            for r in refs {
                let owner: Option<String> = tx
                    .query_row(
                        "SELECT agent_id FROM session_binding WHERE ref_kind=?1 AND ref=?2",
                        params![r.ref_kind, r.reference],
                        |r| r.get(0),
                    )
                    .optional()?;
                if owner.is_some_and(|id| id != row.agent_id) {
                    return Ok(Err(crate::session_refusal(
                        "ref_bound_elsewhere",
                        "incoming reference has another owner",
                        Some(serde_json::json!({"ref_kind":r.ref_kind,"ref":r.reference})),
                    )));
                }
            }
            let installed = crate::session_carry::install_moved_bindings(
                tx,
                &row.agent_id,
                row.activated_at_ms,
                refs,
                carried,
            )?;
            let mut next = row.clone();
            next.abort_requested = current.abort_requested;
            next.cut_accept =
                Some(serde_json::json!({"state":"pending","attempts":0,"retry_at_ms":0}));
            next.write(tx, false)?;
            Ok(Ok((next, installed)))
        })
        .map_err(|e| crate::session_refusal("storage_error", e.to_string(), None))?
    }

    pub fn set_move_cut_accept(
        &self,
        cut_id: &str,
        accept: &Value,
        now_ms: i64,
    ) -> Result<(), RegistryError> {
        self.with_where_move_changes(|tx| {
            tx.execute(
                "UPDATE agent_move SET cut_accept=?2, updated_at_ms=?3 WHERE cut_id=?1",
                params![cut_id, accept.to_string(), now_ms],
            )?;
            Ok(())
        })
        .map_err(RegistryError::Store)
    }

    /// Pending accepts survive terminal move steps and are not limited to the
    /// recent-history window used by operator-facing move lists.
    pub fn list_pending_cut_accepts(&self) -> Result<Vec<AgentMove>, RegistryError> {
        Ok(self
            .list_agent_moves()?
            .into_iter()
            .filter(|r| {
                r.side == MoveSide::Receiver
                    && r.cut_accept
                        .as_ref()
                        .is_some_and(|a| a["state"] == "pending")
            })
            .collect())
    }

    pub fn agent_move(&self, cut_id: &str) -> Result<Option<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE cut_id=?1"),
                    [cut_id],
                    AgentMove::from_row,
                )
                .optional()
            })
            .map_err(RegistryError::Store)
    }

    /// Consume only this move's explicit operator request to retry a stuck abort.
    /// Persisting the flag lets a stopped move driver honor it when it starts again.
    pub fn consume_move_abort_retry(&self, cut_id: &str) -> Result<bool, RegistryError> {
        self.with_where_move_changes(|tx| {
            let mut row = load_move(tx, cut_id)?;
            if let Some(request) = row
                .abort_requested
                .as_mut()
                .and_then(Value::as_object_mut)
                .filter(|v| v.get("retry_pending") == Some(&Value::Bool(true)))
            {
                request.remove("retry_pending");
                row.write(tx, false)?;
                Ok(true)
            } else {
                Ok(false)
            }
        })
        .map_err(RegistryError::Store)
    }

    pub fn move_status_view(&self, cut_id: &str) -> Result<Option<Value>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE cut_id=?1"),
                    [cut_id],
                    AgentMove::from_row,
                )
                .optional()?
                .map(|row| public_move(conn, &row))
                .transpose()
            })
            .map_err(RegistryError::Store)
    }

    pub fn moves_view(&self) -> Result<Vec<Value>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move
                WHERE step NOT IN ('released','aborted','done','taken_over')
                ORDER BY created_at_ms, cut_id"
                ))?;
                let mut rows = stmt
                    .query_map([], AgentMove::from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move
                WHERE step IN ('released','aborted','done','taken_over')
                ORDER BY updated_at_ms DESC, cut_id DESC LIMIT 32"
                ))?;
                rows.extend(
                    stmt.query_map([], AgentMove::from_row)?
                        .collect::<Result<Vec<_>, _>>()?,
                );
                rows.iter().map(|row| public_move(conn, row)).collect()
            })
            .map_err(RegistryError::Store)
    }

    /// Look up agent_move.request_key independently of registry_journal.request_key;
    /// the same key may identify one move and one unrelated journal operation.
    pub fn agent_move_by_request_key(
        &self,
        request_key: &str,
    ) -> Result<Option<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE request_key=?1"),
                    [request_key],
                    AgentMove::from_row,
                )
                .optional()
            })
            .map_err(RegistryError::Store)
    }

    pub fn list_agent_moves(&self) -> Result<Vec<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move ORDER BY created_at_ms, cut_id"
                ))?;
                let rows = stmt
                    .query_map([], AgentMove::from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .map_err(RegistryError::Store)
    }

    pub fn list_open_agent_moves(&self) -> Result<Vec<AgentMove>, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {MOVE_COLUMNS} FROM agent_move
                 WHERE step NOT IN ('released','aborted','done','taken_over')
                 ORDER BY created_at_ms, cut_id"
                ))?;
                let rows = stmt
                    .query_map([], AgentMove::from_row)?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(rows)
            })
            .map_err(RegistryError::Store)
    }

    pub fn move_setting(&self) -> Result<MoveSetting, RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.query_row(
                    "SELECT lid_close_moves FROM move_setting WHERE id=1",
                    [],
                    |row| {
                        Ok(MoveSetting {
                            lid_close_moves: row.get(0)?,
                        })
                    },
                )
            })
            .map_err(RegistryError::Store)
    }

    pub fn set_move_setting(&self, setting: MoveSetting) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|conn| {
                conn.execute(
                    "UPDATE move_setting SET lid_close_moves=?1 WHERE id=1",
                    [setting.lid_close_moves],
                )?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }
}

fn bound_refs(conn: &rusqlite::Connection, agent_id: &str) -> rusqlite::Result<Vec<MoveRef>> {
    let mut stmt = conn.prepare(
        "SELECT ref_kind,ref FROM session_binding WHERE agent_id=?1 AND ended_at_ms IS NULL ORDER BY ref_kind,ref",
    )?;
    let rows = stmt
        .query_map([agent_id], |r| {
            Ok(MoveRef {
                ref_kind: r.get(0)?,
                reference: r.get(1)?,
            })
        })?
        .collect();
    rows
}

pub(crate) fn load_move(conn: &rusqlite::Connection, cut_id: &str) -> rusqlite::Result<AgentMove> {
    conn.query_row(
        &format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE cut_id=?1"),
        [cut_id],
        AgentMove::from_row,
    )
}

fn open_moves(conn: &rusqlite::Connection) -> rusqlite::Result<Vec<AgentMove>> {
    let mut stmt = conn.prepare(&format!("SELECT {MOVE_COLUMNS} FROM agent_move WHERE step NOT IN ('released','aborted','done','taken_over') ORDER BY cut_id"))?;
    let rows = stmt.query_map([], AgentMove::from_row)?.collect();
    rows
}

pub(crate) fn move_detail(row: &AgentMove) -> Value {
    serde_json::json!({"cut_id": row.cut_id, "side": row.side, "step": row.step})
}

/// Refuse session registration that could write past an open move's cut. Check
/// in the registration transaction, including incoming receiver-plan references
/// that have not yet been bound to their destination agent.
pub(crate) fn check_moving_registration(
    conn: &rusqlite::Connection,
    request: &crate::SessionRegistration<'_>,
) -> Result<(), AgentMutationError> {
    for row in open_moves(conn)? {
        let blocks_agent = row.agent_id == request.agent_id
            && (row.side == MoveSide::Source
                || matches!(
                    row.step,
                    MoveStep::PickedUp
                        | MoveStep::Imported
                        | MoveStep::Staged
                        | MoveStep::Claimed
                        | MoveStep::Activating
                ));
        let reserves_ref = row.side == MoveSide::Receiver
            && (row.step.before_boundary(MoveSide::Receiver) || row.step == MoveStep::Activating)
            && row.agent_id != request.agent_id
            && row
                .plan
                .get("participants")
                .and_then(Value::as_array)
                .is_some_and(|participants| {
                    participants.iter().any(|p| {
                        p.get("refs").and_then(Value::as_array).is_some_and(|refs| {
                            refs.iter().any(|r| {
                                r.get("ref_kind").and_then(Value::as_str) == Some(request.ref_kind)
                                    && r.get("ref").and_then(Value::as_str)
                                        == Some(request.session_ref)
                            })
                        })
                    })
                });
        if blocks_agent || reserves_ref {
            return Err(crate::session_refusal(
                "agent_moving",
                "a move fences this session registration",
                Some(move_detail(&row)),
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
    use serde_json::json;
    use std::sync::atomic::{AtomicU64, Ordering};

    static NEXT: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        store: RegistryStore,
        descriptor: StorageDescriptor,
        _cleanup: crate::scratch_cleanup::ScratchCleanup,
    }

    impl Fixture {
        fn new() -> Self {
            Self::with_migrations(&crate::MIGRATIONS)
        }

        fn with_migrations(migrations: &[cortexkit_store::Migration]) -> Self {
            let root = std::env::temp_dir().join(format!(
                "agent-move-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            let descriptor = StorageDescriptor {
                module_id: "entorhinal-test".into(),
                storage_namespace: "moves".into(),
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let store = RegistryStore::open_with_migrations(&descriptor, migrations).unwrap();
            Self {
                store,
                descriptor,
                _cleanup: crate::scratch_cleanup::ScratchCleanup(root),
            }
        }
    }

    fn checkpoint(cut_id: &str, side: MoveSide) -> AgentMove {
        AgentMove {
            cut_id: cut_id.into(),
            agent_id: "agent-1".into(),
            side,
            peer: "peer-author".into(),
            step: match side {
                MoveSide::Source => MoveStep::Preparing,
                MoveSide::Receiver => MoveStep::PickedUp,
            },
            plan: json!({"participants": [{"module_id": "a", "refs": [{"ref_kind": "session", "ref": "r"}]}]}),
            request_body: Some(json!({"agent_id": "agent-1", "to": "Display name as submitted"})),
            record_version: 3,
            claim_version: Some(4),
            claim_take_body: Some(
                json!({"holder": "self", "epoch": 2, "cut_id": cut_id, "reason": null}),
            ),
            claim_take_version: Some(4),
            claim_landed_version: Some(5),
            seal_sent: true,
            abort_requested: Some(json!({"reason": "operator", "detail": {"note": "full detail"}})),
            hold: Some(json!({"code": "external_not_present", "ref": "r", "note": "外部"})),
            cut_accept: Some(json!({"state": "refused", "code": "unavailable"})),
            reason: Some("operator".into()),
            detail: Some(json!({"nested": [null, true, 17, "verbatim"]})),
            request_key: Some(format!("key-{cut_id}")),
            attempts: 7,
            activated_at_ms: Some(99),
            created_at_ms: 1,
            updated_at_ms: 2,
        }
    }

    #[test]
    fn abort_boundary_refuses_without_writes_for_both_sides() {
        for (side, steps) in [
            (
                MoveSide::Source,
                vec![MoveStep::Releasing, MoveStep::Released],
            ),
            (
                MoveSide::Receiver,
                vec![
                    MoveStep::Activating,
                    MoveStep::Activated,
                    MoveStep::Done,
                    MoveStep::TakenOver,
                ],
            ),
        ] {
            for step in steps {
                let f = Fixture::new();
                let mut row = checkpoint("cut", side);
                row.step = step;
                row.abort_requested = None;
                f.store.insert_agent_move(&row).unwrap();
                let error = f.store.request_agent_move_abort("cut", 300).unwrap_err();
                assert_eq!(error.code, "invalid_state");
                assert_eq!(error.detail.unwrap()["step"], json!(step));
                assert_eq!(f.store.agent_move("cut").unwrap().unwrap(), row);
            }
        }
    }

    fn view_row() -> AgentMove {
        let mut row = checkpoint("cut", MoveSide::Receiver);
        row.agent_id = "agent_00000001".into();
        row.abort_requested = None;
        row.cut_accept = None;
        row.reason = None;
        row.detail = None;
        row
    }

    fn view_agent(f: &Fixture) {
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_00000001','Ada','test','assistant',1,1)", [])?;
            tx.execute("INSERT INTO member_name(author,display_name) VALUES('peer-author','Studio Mac')", [])?;
            // Moves happen only with sharing on, where an unread claim is unknown.
            tx.execute("UPDATE identity_log_state SET state='enabled' WHERE id=1", [])?;
            Ok(())
        }).unwrap();
    }

    #[test]
    fn move_views_pin_where_hold_moves_and_cut_accept_failure_literals() {
        let f = Fixture::new();
        view_agent(&f);
        let row = view_row();
        f.store.insert_agent_move(&row).unwrap();
        assert_eq!(
            serde_json::to_string(&f.store.agent_where("agent_00000001").unwrap()).unwrap(),
            r#"{"known":false,"status":"unclaimed","holder":null,"holder_name":null,"epoch":0,"cut_id":null,"read_at_ms":null,"stale":true,"moving":{"cut_id":"cut","hold":{"code":"external_not_present","note":"外部","ref":"r"},"peer":"peer-author","peer_name":"Studio Mac","side":"receiver","step":"picked_up"}}"#
        );
        assert_eq!(
            serde_json::to_string(&json!({"moves":f.store.moves_view().unwrap()})).unwrap(),
            r#"{"moves":[{"abort_requested":null,"activated_at_ms":99,"agent_id":"agent_00000001","attempts":7,"created_at_ms":1,"cut_accept":null,"cut_id":"cut","detail":null,"hold":{"code":"external_not_present","note":"外部","ref":"r"},"peer":"peer-author","peer_name":"Studio Mac","reason":null,"record_version":3,"request_key":"key-cut","seal_sent":true,"side":"receiver","step":"picked_up","updated_at_ms":2}]}"#
        );
        let mut row = row;
        row.step = MoveStep::Done;
        row.hold = None;
        row.cut_accept = Some(json!({"state":"refused","code":"cut_not_published"}));
        f.store.update_agent_move(&row).unwrap();
        assert_eq!(
            serde_json::to_string(&f.store.move_status_view("cut").unwrap().unwrap()).unwrap(),
            r#"{"abort_requested":null,"activated_at_ms":99,"agent_id":"agent_00000001","attempts":7,"created_at_ms":1,"cut_accept":{"code":"cut_not_published","state":"refused"},"cut_id":"cut","detail":null,"hold":null,"peer":"peer-author","peer_name":"Studio Mac","reason":null,"record_version":3,"request_key":"key-cut","seal_sent":true,"side":"receiver","step":"done","updated_at_ms":2}"#
        );
        assert_eq!(
            serde_json::to_string(&f.store.agent_where("agent_00000001").unwrap()).unwrap(),
            r#"{"known":false,"status":"unclaimed","holder":null,"holder_name":null,"epoch":0,"cut_id":null,"read_at_ms":null,"stale":true,"moveErrors":[{"code":"cut_not_published","cut_id":"cut"}]}"#
        );
        assert_eq!(
            f.store.moves_view().unwrap()[0],
            f.store.move_status_view("cut").unwrap().unwrap()
        );
        assert!(f.store.move_status_view("missing").unwrap().is_none());
    }

    #[test]
    fn moves_view_orders_open_first_and_limits_finished_to_latest_32() {
        let f = Fixture::new();
        for n in 0..36 {
            let mut row = view_row();
            row.cut_id = format!("cut-{n:02}");
            row.request_key = Some(format!("key-{n}"));
            row.step = MoveStep::Done;
            row.updated_at_ms = n;
            f.store.insert_agent_move(&row).unwrap();
        }
        let mut row = view_row();
        row.cut_id = "open".into();
        row.request_key = None;
        f.store.insert_agent_move(&row).unwrap();
        let rows = f.store.moves_view().unwrap();
        assert_eq!(rows.len(), 33);
        assert_eq!(rows[0]["cut_id"], "open");
        assert_eq!(rows[1]["cut_id"], "cut-35");
        assert_eq!(rows[32]["cut_id"], "cut-04");
        for key in [
            "plan",
            "request_body",
            "claim_version",
            "claim_take_body",
            "claim_take_version",
            "claim_landed_version",
        ] {
            assert!(rows.iter().all(|row| row.get(key).is_none()), "{key}");
        }
    }

    #[test]
    fn move_views_explain_forks_stuck_aborts_contract_violations_and_release_warnings() {
        let f = Fixture::new();
        view_agent(&f);
        let mut row = view_row();
        row.side = MoveSide::Source;
        row.step = MoveStep::Released;
        row.hold = None;
        row.plan["_source"] = json!({"enforced":{"data":false,"other":true}});
        f.store.insert_agent_move(&row).unwrap();
        let status = f.store.move_status_view("cut").unwrap().unwrap();
        assert_eq!(status["warnings"][0]["module_id"], "data");
        assert_eq!(
            status["warnings"][0]["message"],
            "The old Mac cannot block writes for data; anything typed there becomes a fork"
        );
        let where_ = serde_json::to_value(f.store.agent_where("agent_00000001").unwrap()).unwrap();
        assert!(where_.get("moveErrors").is_none());
        assert_eq!(where_["moveWarnings"][0]["code"], "release_not_enforced");
        assert_eq!(where_["moveWarnings"][0]["cut_id"], "cut");
        for marker in ["activation_fork", "contract_violation"] {
            row.side = MoveSide::Receiver;
            row.step = MoveStep::Activating;
            row.plan = json!({"_receiver":{marker:{"module_id":"data","code":"digest_mismatch","reason":"staging_digest_mismatch"}}});
            f.store.update_agent_move(&row).unwrap();
            let status = f.store.move_status_view("cut").unwrap().unwrap();
            assert_eq!(status["status"], "held_as_fork");
            assert_eq!(status["message"], "held as a fork, awaiting the operator");
            assert_eq!(status["diagnostic"]["module_id"], "data");
            assert_eq!(status["diagnostic"]["reason"], "staging_digest_mismatch");
            assert_eq!(
                serde_json::to_value(f.store.agent_where("agent_00000001").unwrap()).unwrap()
                    ["moving"]["status"],
                "held_as_fork"
            );
        }
        row.side = MoveSide::Source;
        row.step = MoveStep::Preparing;
        row.abort_requested = Some(json!({"reason":"operator","detail":null}));
        row.hold = Some(
            json!({"state":"stuck","op":"agent_data.abort","module_id":"data","code":"digest_mismatch","reason":"undo_digest_mismatch","move_error":true}),
        );
        f.store.update_agent_move(&row).unwrap();
        let status = f.store.move_status_view("cut").unwrap().unwrap();
        assert_eq!(status["status"], "abort_stuck");
        assert_eq!(
            status["message"],
            "abort stuck, agent frozen on this Mac until data is repaired"
        );
        assert_eq!(
            serde_json::to_value(f.store.agent_where("agent_00000001").unwrap()).unwrap()["moving"]
                ["message"],
            status["message"]
        );
        row.hold = None;
        row.plan = json!({"_source":{"contract_violation":{"module_id":"data","code":"unexpected_refusal"},"errors":[{"module_id":"data","code":"unexpected_refusal"}]}});
        f.store.update_agent_move(&row).unwrap();
        let status = f.store.move_status_view("cut").unwrap().unwrap();
        assert_eq!(
            status["diagnostic"],
            json!({"module_id":"data","code":"unexpected_refusal"})
        );
        assert_eq!(
            status["diagnostics"][0],
            json!({"module_id":"data","code":"unexpected_refusal"})
        );
        assert_eq!(
            serde_json::to_value(f.store.agent_where("agent_00000001").unwrap()).unwrap()
                ["moveDiagnostics"][0],
            json!({"cut_id":"cut","module_id":"data","code":"unexpected_refusal"})
        );
        assert_eq!(row.error_codes(), vec!["unexpected_refusal"]);
    }

    #[test]
    fn abort_retry_is_durable_cut_scoped_and_consumed_once() {
        let f = Fixture::new();
        for cut in ["a", "b"] {
            let mut row = view_row();
            row.cut_id = cut.into();
            row.agent_id = cut.into();
            row.request_key = None;
            row.abort_requested = Some(json!({"reason":"operator","detail":null}));
            row.hold =
                Some(json!({"state":"stuck","op":"agent_data.abort","code":"digest_mismatch"}));
            f.store.insert_agent_move(&row).unwrap();
        }
        let before_b = f.store.agent_move("b").unwrap().unwrap();
        f.store.request_agent_move_abort("a", 99).unwrap();
        drop(f.store);
        let reopened = RegistryStore::open(&f.descriptor).unwrap();
        assert!(!reopened.consume_move_abort_retry("b").unwrap());
        assert!(reopened.consume_move_abort_retry("a").unwrap());
        assert!(!reopened.consume_move_abort_retry("a").unwrap());
        assert_eq!(reopened.agent_move("b").unwrap().unwrap(), before_b);
    }

    #[test]
    fn abort_and_activation_boundary_race_has_one_fenced_winner() {
        for abort_first in [true, false] {
            let f = Fixture::new();
            let mut row = checkpoint("cut", MoveSide::Receiver);
            row.step = MoveStep::Claimed;
            row.abort_requested = None;
            f.store.insert_agent_move(&row).unwrap();
            let mut next = row.clone();
            next.step = MoveStep::Activating;
            if abort_first {
                let accepted = f.store.request_agent_move_abort("cut", 99).unwrap();
                assert!(accepted.abort_requested.is_some());
                assert_eq!(
                    f.store.checkpoint_agent_move(&next, false).unwrap(),
                    accepted
                );
            } else {
                assert_eq!(
                    f.store.checkpoint_agent_move(&next, false).unwrap().step,
                    MoveStep::Activating
                );
                assert_eq!(
                    f.store
                        .request_agent_move_abort("cut", 99)
                        .unwrap_err()
                        .code,
                    "invalid_state"
                );
            }
        }
        for _ in 0..8 {
            let f = Fixture::new();
            let mut row = checkpoint("cut", MoveSide::Receiver);
            row.step = MoveStep::Claimed;
            row.abort_requested = None;
            f.store.insert_agent_move(&row).unwrap();
            row.step = MoveStep::Activating;
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let store = f.store.clone();
            let start = barrier.clone();
            let thread = std::thread::spawn(move || {
                start.wait();
                store.request_agent_move_abort("cut", 99)
            });
            barrier.wait();
            let saved = f.store.checkpoint_agent_move(&row, false).unwrap();
            let abort = thread.join().unwrap();
            match abort {
                Ok(_) => {
                    assert_eq!(saved.step, MoveStep::Claimed);
                    assert!(saved.abort_requested.is_some());
                }
                Err(error) => {
                    assert_eq!(error.code, "invalid_state");
                    assert_eq!(saved.step, MoveStep::Activating);
                    assert!(saved.abort_requested.is_none());
                }
            }
        }
    }

    #[test]
    fn activation_binding_transaction_refuses_all_writes_on_owner_conflict_and_preserves_accept_diagnostics(
    ) {
        let f = Fixture::new();
        let mut row = checkpoint("cut", MoveSide::Receiver);
        row.step = MoveStep::Activating;
        row.abort_requested = None;
        row.cut_accept = None;
        f.store.insert_agent_move(&row).unwrap();
        f.store.db.with_conn_fenced(|tx| tx.execute_batch("INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES('session','z','other',NULL,NULL,'','move',1)")).unwrap();
        let mut next = row.clone();
        next.step = MoveStep::Activated;
        next.activated_at_ms = Some(123);
        let refs = vec![
            MoveRef {
                ref_kind: "session".into(),
                reference: "r".into(),
            },
            MoveRef {
                ref_kind: "session".into(),
                reference: "z".into(),
            },
        ];
        assert_eq!(
            f.store.activate_agent_move(&next, &refs).unwrap_err().code,
            "ref_bound_elsewhere"
        );
        assert_eq!(f.store.agent_move("cut").unwrap().unwrap(), row);
        assert!(f
            .store
            .session_bindings_for_refs(&[("session", "r")])
            .unwrap()[0]
            .is_none());
        let activated = f.store.activate_agent_move(&next, &refs[..1]).unwrap();
        assert_eq!(activated.step, MoveStep::Activated);
        assert_eq!(activated.activated_at_ms, Some(123));
        let accepted = json!({"state":"refused","code":"cut_not_published"});
        f.store.set_move_cut_accept("cut", &accepted, 200).unwrap();
        let mut stale = activated;
        stale.step = MoveStep::Done;
        assert_eq!(
            f.store
                .checkpoint_agent_move(&stale, false)
                .unwrap()
                .cut_accept,
            Some(accepted)
        );
    }

    #[test]
    fn received_move_acquires_slot_and_refuses_open_agent_and_cap() {
        let f = Fixture::new();
        let receiver = checkpoint("receiver", MoveSide::Receiver);
        assert_eq!(f.store.admit_received_move(&receiver).unwrap(), receiver);
        let mut resend = receiver.clone();
        resend.plan = json!({"changed":true});
        assert_eq!(f.store.admit_received_move(&resend).unwrap(), receiver);
        let other_cut = checkpoint("duplicate-agent", MoveSide::Receiver);
        assert_eq!(
            f.store.admit_received_move(&other_cut).unwrap_err().code,
            "move_in_progress"
        );
        for i in 2..=4 {
            let mut row = checkpoint(&format!("r{i}"), MoveSide::Receiver);
            row.agent_id = format!("agent-{i}");
            f.store.admit_received_move(&row).unwrap();
        }
        let mut fifth = checkpoint("fifth", MoveSide::Receiver);
        fifth.agent_id = "agent-5".into();
        assert_eq!(
            f.store.admit_received_move(&fifth).unwrap_err().code,
            "move_cap"
        );
        assert!(f.store.agent_move("fifth").unwrap().is_none());
        fifth.step = MoveStep::Aborted;
        assert_eq!(
            f.store.admit_received_move(&fifth).unwrap_err().code,
            "move_cap"
        );
        let mut invalid = checkpoint("invalid", MoveSide::Source);
        invalid.agent_id = "agent-6".into();
        assert_eq!(
            f.store.admit_received_move(&invalid).unwrap_err().code,
            "invalid_request"
        );
    }

    #[test]
    fn received_move_races_source_admission_in_one_fenced_transaction() {
        for same_agent in [true, false] {
            let f = Fixture::new();
            if !same_agent {
                for i in 1..=3 {
                    let mut row = checkpoint(&format!("existing-{i}"), MoveSide::Receiver);
                    row.agent_id = format!("existing-{i}");
                    f.store.admit_received_move(&row).unwrap();
                }
            }
            let mut source = checkpoint("source", MoveSide::Source);
            let receiver = checkpoint("receiver", MoveSide::Receiver);
            if !same_agent {
                source.agent_id = "other-agent".into();
            }
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            let store = f.store.clone();
            let start = barrier.clone();
            let thread = std::thread::spawn(move || {
                start.wait();
                store.admit_agent_move(&source, &[])
            });
            barrier.wait();
            let received = f.store.admit_received_move(&receiver);
            let admitted = thread.join().unwrap();
            assert_ne!(received.is_ok(), admitted.is_ok());
            let refusal = received.err().or_else(|| admitted.err()).unwrap();
            assert_eq!(
                refusal.code,
                if same_agent {
                    "move_in_progress"
                } else {
                    "move_cap"
                }
            );
            assert_eq!(
                f.store.list_open_agent_moves().unwrap().len(),
                if same_agent { 1 } else { 4 }
            );
        }
    }

    fn registration(agent: &str) -> crate::SessionRegistration<'_> {
        crate::SessionRegistration {
            agent_id: agent,
            ref_kind: "session",
            session_ref: "r",
            harness: "host",
            session_id: "s",
            project_root: Some("/project"),
            registered_by: "host",
            runner: None,
            network: None,
            now_ms: 1,
        }
    }

    #[test]
    fn admission_commit_registration_orders_and_restart_checkpoint() {
        let f = Fixture::new();
        let row = checkpoint("cut", MoveSide::Source);
        assert!(
            f.store.list_open_agent_moves().unwrap().is_empty(),
            "a crash before commit leaves nothing"
        );
        // Check directly that registering a session for an agent with an open
        // move is refused. Module tests exercise registration of a cut-over agent too.
        f.store.db.with_conn_fenced(|tx| {
            tx.execute("INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms) VALUES('session','r','agent-1','host','s','','host',1)", [])?;
            Ok(())
        }).unwrap();
        assert_eq!(
            f.store.admit_agent_move(&row, &[]).unwrap_err().code,
            "agent_busy"
        );
        assert!(f.store.list_open_agent_moves().unwrap().is_empty());
        let refs = f.store.agent_bound_refs("agent-1").unwrap();
        let admitted = f.store.admit_agent_move(&row, &refs).unwrap();
        f.store
            .db
            .with_conn_fenced(|tx| {
                assert_eq!(
                    check_moving_registration(tx, &registration("agent-1"))
                        .unwrap_err()
                        .code,
                    "agent_moving"
                );
                Ok(())
            })
            .unwrap();
        let Fixture {
            store,
            descriptor,
            _cleanup,
        } = f;
        drop(store);
        let store = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(
            store.list_open_agent_moves().unwrap(),
            vec![admitted.clone()]
        );
        assert_eq!(
            store.admit_agent_move(&row, &[]).unwrap(),
            admitted,
            "resend returns persisted plan before stale checks"
        );
        let mut reused = row;
        reused.request_body = Some(json!({"agent_id":"agent-1", "to":"renamed target"}));
        assert_eq!(
            store.admit_agent_move(&reused, &[]).unwrap_err().code,
            "request_key_reused"
        );
    }

    #[test]
    fn registration_fences_source_receiver_steps_and_other_agents_plan_refs() {
        let f = Fixture::new();
        for side in [MoveSide::Source, MoveSide::Receiver] {
            let steps = if side == MoveSide::Source {
                vec![
                    MoveStep::Preparing,
                    MoveStep::Exported,
                    MoveStep::Sealed,
                    MoveStep::Published,
                    MoveStep::Releasing,
                    MoveStep::Released,
                ]
            } else {
                vec![
                    MoveStep::PickedUp,
                    MoveStep::Imported,
                    MoveStep::Staged,
                    MoveStep::Claimed,
                    MoveStep::Activating,
                    MoveStep::Activated,
                    MoveStep::Done,
                ]
            };
            let mut row = checkpoint(
                if side == MoveSide::Source {
                    "source"
                } else {
                    "receiver"
                },
                side,
            );
            row.step = steps[0];
            f.store.insert_agent_move(&row).unwrap();
            for step in steps {
                row.step = step;
                f.store.update_agent_move(&row).unwrap();
                f.store
                    .db
                    .with_conn_fenced(|tx| {
                        let answer = check_moving_registration(tx, &registration("agent-1"));
                        let blocks = !step.is_terminal() && step != MoveStep::Activated;
                        assert_eq!(answer.is_err(), blocks, "{side:?} {step:?}");
                        if blocks {
                            assert_eq!(answer.unwrap_err().detail, Some(move_detail(&row)));
                        }
                        let other = check_moving_registration(tx, &registration("other"));
                        assert_eq!(other.is_err(), side == MoveSide::Receiver && blocks);
                        Ok(())
                    })
                    .unwrap();
            }
        }
    }

    #[test]
    fn admission_commit_rechecks_open_agent_and_cap_before_insert() {
        let f = Fixture::new();
        for n in 0..4 {
            let mut row = checkpoint(&format!("cut-{n}"), MoveSide::Receiver);
            row.agent_id = format!("other-{n}");
            f.store.insert_agent_move(&row).unwrap();
        }
        let row = checkpoint("new", MoveSide::Source);
        assert_eq!(
            f.store.admit_agent_move(&row, &[]).unwrap_err().code,
            "move_cap"
        );
        let mut same_agent = row.clone();
        same_agent.agent_id = "other-0".into();
        assert_eq!(
            f.store.admit_agent_move(&same_agent, &[]).unwrap_err().code,
            "move_in_progress"
        );
        assert_eq!(f.store.list_open_agent_moves().unwrap().len(), 4);
    }

    #[test]
    fn migration_9_preserves_part1_bindings_and_session_index() {
        type SchemaObject = (String, String, String, Option<String>);
        fn binding_objects(conn: &rusqlite::Connection) -> rusqlite::Result<Vec<SchemaObject>> {
            conn.prepare("SELECT type,name,tbl_name,sql FROM sqlite_master WHERE type IN ('index','trigger','view') AND (tbl_name='session_binding' OR (type='view' AND instr(sql,'session_binding')>0)) ORDER BY type,name")?
                .query_map([], |r| Ok((r.get(0)?,r.get(1)?,r.get(2)?,r.get(3)?)))?
                .collect()
        }
        let Fixture {
            store,
            descriptor,
            _cleanup,
        } = Fixture::with_migrations(&crate::MIGRATIONS[..8]);
        store
            .db
            .with_conn_fenced(|conn| {
                conn.execute_batch(
                    "INSERT INTO session_binding VALUES
                ('session','r1','a1','host-a','s1','host',11),
                ('session','r2','a2','host-b','s2','orchestrator',22);",
                )?;
                Ok(())
            })
            .unwrap();
        let before = store
            .db
            .with_conn_fenced(|conn| binding_objects(conn))
            .unwrap();
        assert!(
            !before.is_empty(),
            "schema 8 must define objects to preserve"
        );
        drop(store);
        // Stop at migration 9: later migrations add their own columns and
        // indexes to session_binding, which this test does not describe.
        let store =
            RegistryStore::open_with_migrations(&descriptor, &crate::MIGRATIONS[..9]).unwrap();
        store.db.with_conn_fenced(|conn| {
            assert_eq!(binding_objects(conn)?, before, "migration 9 lost or changed a schema-8 session_binding object");
            let rows = conn.prepare("SELECT ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms FROM session_binding ORDER BY ref")?
                .query_map([], |row| Ok((
                    row.get::<_, String>(0)?, row.get::<_, String>(1)?, row.get::<_, String>(2)?,
                    row.get::<_, String>(3)?, row.get::<_, String>(4)?, row.get::<_, String>(5)?,
                    row.get::<_, String>(6)?, row.get::<_, i64>(7)?,
                )))?.collect::<Result<Vec<_>, _>>()?;
            assert_eq!(rows, vec![
                ("session".into(), "r1".into(), "a1".into(), "host-a".into(), "s1".into(), "".into(), "host".into(), 11),
                ("session".into(), "r2".into(), "a2".into(), "host-b".into(), "s2".into(), "".into(), "orchestrator".into(), 22),
            ]);
            let index_sql: String = conn.query_row("SELECT sql FROM sqlite_master WHERE name='session_binding_session'", [], |row| row.get(0))?;
            assert!(index_sql.contains("UNIQUE INDEX"));
            assert!(index_sql.contains("session_binding(harness, session_id)"));
            assert!(conn.execute("INSERT INTO session_binding VALUES ('session','collision','a3','host-a','s1','','host',33)", []).is_err());
            conn.execute_batch("INSERT INTO session_binding VALUES
                ('session','m1','a1',NULL,NULL,'','move',44),
                ('session','m2','a1',NULL,NULL,'','move',55);")?;
            let moved: i64 = conn.query_row("SELECT COUNT(*) FROM session_binding WHERE registered_by='move' AND harness IS NULL AND session_id IS NULL", [], |row| row.get(0))?;
            assert_eq!(moved, 2);
            assert!(conn.execute("INSERT INTO session_binding VALUES ('session','bad','a1',NULL,NULL,'','other',66)", []).is_err());
            Ok(())
        }).unwrap();
        drop(store);
        // Current readers need the additive columns from later migrations.
        let store = RegistryStore::open(&descriptor).unwrap();
        let legacy = store.session_binding_for("host-a", "s1").unwrap().unwrap();
        assert_eq!(
            legacy.project_root, None,
            "an old binding has no known project root"
        );
        assert!(serde_json::to_value(&legacy).unwrap()["project_root"].is_null());
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: false
            }
        );
    }

    #[test]
    fn move_checkpoint_insert_update_and_restart_round_trip() {
        let Fixture {
            store,
            descriptor,
            _cleanup,
        } = Fixture::new();
        let mut row = checkpoint("cut-1", MoveSide::Receiver);
        store.insert_agent_move(&row).unwrap();
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert_eq!(
            store
                .agent_move_by_request_key(row.request_key.as_ref().unwrap())
                .unwrap(),
            Some(row.clone())
        );
        assert_eq!(store.list_agent_moves().unwrap(), vec![row.clone()]);
        assert_eq!(store.list_open_agent_moves().unwrap(), vec![row.clone()]);
        row.step = MoveStep::Activated;
        row.plan = json!({"pinned": [1, 2, 3]});
        row.request_body = Some(Value::Null);
        row.record_version = 10;
        row.claim_version = None;
        row.claim_take_body = Some(json!({"epoch": 100, "reason": null}));
        row.claim_take_version = Some(80);
        row.claim_landed_version = Some(81);
        row.abort_requested = None;
        row.hold = Some(json!({"code": "busy", "module_id": "b"}));
        row.cut_accept = Some(json!({"state": "accepted"}));
        row.seal_sent = false;
        row.reason = None;
        row.detail = None;
        row.attempts = 0;
        row.activated_at_ms = Some(101);
        row.updated_at_ms = 102;
        assert!(store.update_agent_move(&row).unwrap());
        store
            .set_move_setting(MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        drop(store);
        let store = RegistryStore::open(&descriptor).unwrap();
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: true
            }
        );
        row.step = MoveStep::Done;
        row.request_body = None;
        row.claim_take_body = None;
        row.claim_take_version = None;
        row.claim_landed_version = None;
        row.hold = None;
        row.cut_accept = None;
        row.request_key = None;
        row.activated_at_ms = None;
        assert!(store.update_agent_move(&row).unwrap());
        assert_eq!(store.agent_move(&row.cut_id).unwrap(), Some(row.clone()));
        assert!(store.list_open_agent_moves().unwrap().is_empty());
        row.cut_id = "missing".into();
        assert!(!store.update_agent_move(&row).unwrap());
        assert_eq!(store.agent_move("missing").unwrap(), None);
        assert_eq!(store.agent_move_by_request_key("missing").unwrap(), None);
    }

    #[test]
    fn one_open_move_per_agent_on_either_side() {
        for first_side in [MoveSide::Source, MoveSide::Receiver] {
            for second_side in [MoveSide::Source, MoveSide::Receiver] {
                let fixture = Fixture::new();
                let store = &fixture.store;
                let mut first = checkpoint("first", first_side);
                let mut second = checkpoint("second", second_side);
                store.insert_agent_move(&first).unwrap();
                for step in [
                    MoveStep::Preparing,
                    MoveStep::Exported,
                    MoveStep::Sealed,
                    MoveStep::Published,
                    MoveStep::Releasing,
                    MoveStep::PickedUp,
                    MoveStep::Imported,
                    MoveStep::Staged,
                    MoveStep::Claimed,
                    MoveStep::Activating,
                    MoveStep::Activated,
                ] {
                    assert!(!step.is_terminal());
                    first.step = step;
                    store.update_agent_move(&first).unwrap();
                    let error = store.insert_agent_move(&second).unwrap_err();
                    assert!(
                        error.to_string().contains("agent_move.agent_id"),
                        "{first_side:?}/{second_side:?}/{step:?}: {error}"
                    );
                }
                assert_eq!(store.list_agent_moves().unwrap(), vec![first.clone()]);
                for terminal in [
                    MoveStep::Released,
                    MoveStep::Aborted,
                    MoveStep::Done,
                    MoveStep::TakenOver,
                ] {
                    assert!(terminal.is_terminal());
                    first.step = terminal;
                    store.update_agent_move(&first).unwrap();
                    second.cut_id = format!("second-{terminal:?}");
                    second.request_key = Some(format!("key-{terminal:?}"));
                    store.insert_agent_move(&second).unwrap();
                    first.step = match first_side {
                        MoveSide::Source => MoveStep::Preparing,
                        MoveSide::Receiver => MoveStep::PickedUp,
                    };
                    assert!(store.update_agent_move(&first).is_err());
                    second.step = MoveStep::Aborted;
                    store.update_agent_move(&second).unwrap();
                    first.step = terminal;
                    second.step = match second_side {
                        MoveSide::Source => MoveStep::Preparing,
                        MoveSide::Receiver => MoveStep::PickedUp,
                    };
                }
                second.agent_id = "other-agent".into();
                second.cut_id = "other-cut".into();
                second.request_key = None;
                store.insert_agent_move(&second).unwrap();
            }
        }
    }

    #[test]
    fn move_request_keys_are_unique_and_separate_from_journal_keys() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let mut first = checkpoint("first", MoveSide::Source);
        first.request_key = Some("same-key".into());
        store
            .apply_entry("test", "{}", "test", Some("same-key"), |_| Ok(()))
            .unwrap();
        let generation = store.generation().unwrap();
        store.insert_agent_move(&first).unwrap();
        let mut second = checkpoint("second", MoveSide::Receiver);
        second.agent_id = "other-agent".into();
        second.request_key = first.request_key.clone();
        let error = store.insert_agent_move(&second).unwrap_err();
        assert!(
            error.to_string().contains("agent_move.request_key"),
            "{error}"
        );
        assert_eq!(
            store.agent_move_by_request_key("same-key").unwrap(),
            Some(first.clone())
        );
        second.request_key = None;
        store.insert_agent_move(&second).unwrap();
        second.request_key = first.request_key.clone();
        assert!(store.update_agent_move(&second).is_err());
        assert_eq!(store.generation().unwrap(), generation);
    }

    #[test]
    fn move_apis_do_not_append_journal_rows() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let generation = store.generation().unwrap();
        assert_eq!(
            store.move_setting().unwrap(),
            MoveSetting {
                lid_close_moves: false
            }
        );
        store
            .set_move_setting(MoveSetting {
                lid_close_moves: true,
            })
            .unwrap();
        let mut row = checkpoint("first", MoveSide::Source);
        store.insert_agent_move(&row).unwrap();
        store.agent_move("first").unwrap();
        store
            .agent_move_by_request_key(row.request_key.as_ref().unwrap())
            .unwrap();
        store.list_agent_moves().unwrap();
        store.list_open_agent_moves().unwrap();
        row.step = MoveStep::Published;
        store.update_agent_move(&row).unwrap();
        assert_eq!(store.generation().unwrap(), generation);
    }

    #[test]
    fn verify_and_rebuild_leave_move_tables_intact() {
        let fixture = Fixture::new();
        let store = &fixture.store;
        let row = checkpoint("first", MoveSide::Receiver);
        let setting = MoveSetting {
            lid_close_moves: true,
        };
        store.insert_agent_move(&row).unwrap();
        store.set_move_setting(setting).unwrap();
        let verify = store.verify().unwrap();
        assert!(verify.ok, "{verify:?}");
        assert_eq!(store.list_agent_moves().unwrap(), vec![row.clone()]);
        assert_eq!(store.move_setting().unwrap(), setting);
        let rebuilt = store.rebuild().unwrap();
        assert!(rebuilt.replay.ok, "{rebuilt:?}");
        assert_eq!(store.list_agent_moves().unwrap(), vec![row]);
        assert_eq!(store.move_setting().unwrap(), setting);
        assert!(store.verify().unwrap().ok);
    }
}

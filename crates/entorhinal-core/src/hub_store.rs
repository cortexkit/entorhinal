//! Machine-local state for the hub log and KV bucket.

use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde_json::Value;

use crate::{shared_entry::decode_error, RegistryError, RegistryStore};

// NULL hub_log_base leaves a legacy store on its current backend. Zero is a
// real base: a hub log started without engram history.
pub(crate) const V26_HUB_STORE: &str = r#"
ALTER TABLE identity_log_state ADD COLUMN hub_log_base INTEGER NULL CHECK(hub_log_base >= 0);
ALTER TABLE identity_log_state ADD COLUMN hub_ready INTEGER NOT NULL DEFAULT 0
    CHECK(hub_ready IN (0,1) AND (hub_ready = 0 OR hub_log_base IS NOT NULL));
-- The freeze records both the migration id and whether the marker is pending or applied.
ALTER TABLE identity_log_state ADD COLUMN engram_frozen TEXT NULL;
ALTER TABLE identity_log_state ADD COLUMN enable_id TEXT NULL;
ALTER TABLE identity_log_state ADD COLUMN enable_snapshot_digest TEXT NULL;

ALTER TABLE agent_move ADD COLUMN receiver_decision TEXT NULL
    CHECK(receiver_decision IS NULL OR (side = 'receiver' AND receiver_decision IN ('abort','activate')));

-- Refusing a revision marks the key without throwing away its last accepted
-- value. Watch liveness and lease timing are never restored from these rows.
CREATE TABLE hub_kv_cache (
    key TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0),
    accepted_revision INTEGER NULL CHECK(accepted_revision > 0 AND accepted_revision <= revision),
    value BLOB NULL CHECK(value IS NULL OR accepted_revision IS NOT NULL),
    integrity_failed INTEGER NOT NULL CHECK(integrity_failed IN (0,1))
);

CREATE TABLE hub_attempt (
    request_key TEXT PRIMARY KEY NOT NULL,
    params_digest TEXT NOT NULL,
    key TEXT NOT NULL,
    expected INTEGER NOT NULL CHECK(expected >= 0),
    value BLOB NOT NULL,
    step TEXT NOT NULL,
    outcome TEXT NULL,
    outcome_op_id TEXT NULL CHECK((outcome IS NULL) = (outcome_op_id IS NULL)),
    outcome_at_ms INTEGER NULL CHECK((outcome IS NULL) = (outcome_at_ms IS NULL))
);
CREATE INDEX hub_attempt_retention ON hub_attempt(outcome_at_ms) WHERE outcome_at_ms IS NOT NULL;

CREATE TABLE hub_attempt_send (
    request_key TEXT NOT NULL REFERENCES hub_attempt(request_key) ON DELETE CASCADE,
    op_id TEXT NOT NULL,
    sent_at_ms INTEGER NOT NULL CHECK(sent_at_ms >= 0),
    process TEXT NOT NULL,
    PRIMARY KEY(request_key, op_id)
);
CREATE INDEX hub_attempt_send_by_op ON hub_attempt_send(op_id);

CREATE TABLE hub_anchor (
    box TEXT NOT NULL,
    key_version TEXT NOT NULL,
    pub BLOB NOT NULL,
    episode INTEGER NOT NULL CHECK(episode >= 0),
    PRIMARY KEY(box, episode, key_version)
);

-- For a join wrap, the rows sharing its request id give exactly the epochs
-- carried by that wrap, even after a later import supplies another source.
CREATE TABLE hub_held_epoch (
    epoch INTEGER NOT NULL CHECK(epoch > 0),
    source_kind TEXT NOT NULL CHECK(source_kind IN ('mint','join')),
    source_id TEXT NOT NULL,
    PRIMARY KEY(epoch, source_kind, source_id)
);
CREATE INDEX hub_held_epoch_by_source ON hub_held_epoch(source_kind, source_id, epoch);

CREATE TABLE hub_epoch_status (
    id INTEGER PRIMARY KEY CHECK(id = 1),
    epoch INTEGER NOT NULL CHECK(epoch > 0),
    state TEXT NOT NULL CHECK(state IN ('pending','committed')),
    mint_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0)
);
"#;

const ATTEMPT_RETENTION_MS: i64 = 7 * 24 * 60 * 60 * 1_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubAttemptWrite {
    pub request_key: String,
    pub params_digest: String,
    pub key: String,
    pub expected: u64,
    pub value: Vec<u8>,
    pub step: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubAttemptSend {
    pub op_id: String,
    /// Monotonic milliseconds, usable only for a send from the current process.
    pub sent_at_ms: u64,
    /// An identifier unique to this process lifetime, not a reusable OS PID.
    pub process: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubAttemptOutcome {
    pub op_id: String,
    pub value: Value,
    /// Wall-clock milliseconds so retention is measured across restarts.
    pub recorded_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubAttempt {
    pub write: HubAttemptWrite,
    pub op_ids: Vec<HubAttemptSend>,
    pub outcome: Option<HubAttemptOutcome>,
}

fn load_attempt(conn: &Connection, request_key: &str) -> rusqlite::Result<Option<HubAttempt>> {
    let mut attempt = conn
        .query_row(
            "SELECT params_digest,key,expected,value,step,outcome,outcome_op_id,outcome_at_ms
             FROM hub_attempt WHERE request_key=?1",
            [request_key],
            |row| {
                let outcome = row
                    .get::<_, Option<String>>(5)?
                    .map(|value| {
                        Ok::<_, rusqlite::Error>(HubAttemptOutcome {
                            op_id: row.get(6)?,
                            value: serde_json::from_str(&value).map_err(decode_error)?,
                            recorded_at_ms: row.get(7)?,
                        })
                    })
                    .transpose()?;
                Ok(HubAttempt {
                    write: HubAttemptWrite {
                        request_key: request_key.into(),
                        params_digest: row.get(0)?,
                        key: row.get(1)?,
                        expected: row.get(2)?,
                        value: row.get(3)?,
                        step: row.get(4)?,
                    },
                    op_ids: Vec::new(),
                    outcome,
                })
            },
        )
        .optional()?;
    if let Some(attempt) = &mut attempt {
        let mut stmt = conn.prepare(
            "SELECT op_id,sent_at_ms,process FROM hub_attempt_send WHERE request_key=?1 ORDER BY op_id",
        )?;
        attempt.op_ids = stmt
            .query_map([request_key], |row| {
                Ok(HubAttemptSend {
                    op_id: row.get(0)?,
                    sent_at_ms: row.get(1)?,
                    process: row.get(2)?,
                })
            })?
            .collect::<rusqlite::Result<_>>()?;
    }
    Ok(attempt)
}

pub(crate) fn record_attempt_outcome(
    tx: &Transaction<'_>,
    write: &HubAttemptWrite,
    outcome: &HubAttemptOutcome,
) -> rusqlite::Result<bool> {
    // A publish can land after its replacement was saved with new encrypted
    // bytes. Match the target, step and sent op id, not the latest bytes.
    let changed = tx.execute(
        "UPDATE hub_attempt SET outcome=?6,outcome_op_id=?7,outcome_at_ms=?8
         WHERE request_key=?1 AND params_digest=?2 AND key=?3 AND expected=?4 AND step=?5
         AND outcome IS NULL
         AND EXISTS(SELECT 1 FROM hub_attempt_send WHERE request_key=?1 AND op_id=?7)",
        params![
            write.request_key,
            write.params_digest,
            write.key,
            write.expected,
            write.step,
            serde_json::to_string(&outcome.value).map_err(decode_error)?,
            outcome.op_id,
            outcome.recorded_at_ms,
        ],
    )?;
    Ok(changed == 1)
}

impl RegistryStore {
    pub fn hub_attempt(&self, request_key: &str) -> Result<Option<HubAttempt>, RegistryError> {
        self.read(|conn| load_attempt(conn, request_key))
    }

    /// Save the bytes and send identity together before publishing. To change
    /// the bytes or step, pass the exact open row previously read. On a false
    /// result do not send: reload the row to resolve the conflict or replay.
    pub fn persist_hub_attempt(
        &self,
        write: &HubAttemptWrite,
        send: &HubAttemptSend,
        previous: Option<&HubAttempt>,
    ) -> Result<bool, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let current = load_attempt(tx, &write.request_key)?;
                if current.as_ref() != previous
                    || current.as_ref().is_some_and(|row| {
                        row.outcome.is_some() || row.write.params_digest != write.params_digest
                    })
                {
                    return Ok(false);
                }
                tx.execute(
                    "INSERT INTO hub_attempt(request_key,params_digest,key,expected,value,step)
                     VALUES(?1,?2,?3,?4,?5,?6)
                     ON CONFLICT(request_key) DO UPDATE SET
                         key=excluded.key,expected=excluded.expected,value=excluded.value,step=excluded.step",
                    params![write.request_key,write.params_digest,write.key,write.expected,write.value,write.step],
                )?;
                // The hub may acknowledge the original send after a resend.
                // Keep its timestamp and process so a lease cannot start its
                // acting window at the later send or in a different process.
                tx.execute(
                    "INSERT INTO hub_attempt_send(request_key,op_id,sent_at_ms,process) VALUES(?1,?2,?3,?4)
                     ON CONFLICT(request_key,op_id) DO NOTHING",
                    params![write.request_key,send.op_id,send.sent_at_ms,send.process],
                )?;
                Ok(true)
            })
            .map_err(RegistryError::Store)
    }

    /// Close an open attempt without deleting it. Replaying a close does not
    /// replace its result or reset its retention deadline.
    pub fn record_hub_attempt_outcome(
        &self,
        write: &HubAttemptWrite,
        outcome: &HubAttemptOutcome,
    ) -> Result<bool, RegistryError> {
        self.db
            .with_conn_fenced(|tx| record_attempt_outcome(tx, write, outcome))
            .map_err(RegistryError::Store)
    }

    /// Prune concluded requests whose outcomes are at least seven days old.
    /// Open attempts never expire, regardless of when they were sent.
    pub fn prune_hub_attempts(&self, now_ms: i64) -> Result<usize, RegistryError> {
        let Some(cutoff) = now_ms.checked_sub(ATTEMPT_RETENTION_MS) else {
            return Ok(0);
        };
        self.db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "DELETE FROM hub_attempt WHERE outcome_at_ms <= ?1",
                    [cutoff],
                )
            })
            .map_err(RegistryError::Store)
    }
}

#[cfg(test)]
#[path = "hub_store_tests.rs"]
mod tests;

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
ALTER TABLE identity_log_state ADD COLUMN hub_quieting INTEGER NOT NULL DEFAULT 0 CHECK(hub_quieting IN (0,1));
ALTER TABLE identity_log_state ADD COLUMN enable_id TEXT NULL;
ALTER TABLE identity_log_state ADD COLUMN enable_snapshot_digest TEXT NULL;

ALTER TABLE agent_move ADD COLUMN receiver_decision TEXT NULL
    CHECK(receiver_decision IS NULL OR (side = 'receiver' AND receiver_decision IN ('abort','activate')));

CREATE TABLE role_gen_high_water (
    role TEXT PRIMARY KEY NOT NULL,
    gen INTEGER NOT NULL CHECK(gen > 0)
);

-- Refusing a revision marks the key without throwing away its last accepted
-- value. Watch liveness and lease timing are never restored from these rows.
CREATE TABLE hub_kv_cache (
    key TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL CHECK(revision > 0),
    accepted_revision INTEGER NULL CHECK(accepted_revision > 0 AND accepted_revision <= revision),
    value BLOB NULL CHECK(value IS NULL OR accepted_revision IS NOT NULL),
    body TEXT NULL CHECK(body IS NULL OR value IS NOT NULL),
    read_at_ms INTEGER NULL,
    integrity_failed INTEGER NOT NULL CHECK(integrity_failed IN (0,1))
);

CREATE TABLE hub_log_writer (
    position INTEGER PRIMARY KEY CHECK(position > 0),
    entry_id TEXT NOT NULL UNIQUE,
    box TEXT NOT NULL,
    episode INTEGER NOT NULL CHECK(episode >= 0)
);

CREATE TABLE hub_attempt (
    request_key TEXT PRIMARY KEY NOT NULL,
    params_digest TEXT NOT NULL,
    key TEXT NOT NULL,
    expected INTEGER NOT NULL CHECK(expected >= 0),
    value BLOB NOT NULL,
    step TEXT NOT NULL,
    continuation TEXT NULL,
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

CREATE TABLE hub_local_reply (
    request_key TEXT PRIMARY KEY NOT NULL,
    params_digest TEXT NOT NULL,
    reply TEXT NOT NULL
);

CREATE TABLE hub_anchor (
    box TEXT NOT NULL,
    key_version TEXT NOT NULL,
    pub BLOB NOT NULL,
    episode INTEGER NOT NULL CHECK(episode >= 0),
    PRIMARY KEY(box, episode, key_version)
);

-- Membership is replayed from the hub journal, not included in snapshots.
CREATE TABLE hub_membership (
    position INTEGER PRIMARY KEY CHECK(position > 0),
    kind TEXT NOT NULL,
    body TEXT NOT NULL
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
pub struct HubAnchor {
    pub box_id: String,
    pub episode: u64,
    pub version: String,
    pub public_key: Vec<u8>,
}

pub struct HubMembershipScan {
    pub through: u64,
    pub candidates: Vec<(u64, Vec<u8>)>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubOwnMint {
    pub epoch: u64,
    pub mint_id: String,
    pub committed: bool,
    pub revision: u64,
}

pub enum HubEpochSource<'a> {
    Mint(&'a str),
    Join(&'a str),
}

impl HubEpochSource<'_> {
    fn parts(&self) -> (&'static str, &str) {
        match self {
            Self::Mint(id) => ("mint", id),
            Self::Join(id) => ("join", id),
        }
    }
}

pub const MEMBERSHIP_OPS: &[&str] = &["member.key", "member.removed", "member.admitted"];

pub(crate) fn record_log_writer(
    tx: &Transaction<'_>,
    entry: &crate::remote_apply::RemoteEntry,
    episode: u64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO hub_log_writer(position,entry_id,box,episode) VALUES(?1,?2,?3,?4)",
        params![entry.position, entry.entry_id, entry.author, episode],
    )?;
    Ok(())
}

pub(crate) fn agent_creator(
    conn: &Connection,
    id: &str,
    through: i64,
) -> rusqlite::Result<Option<(String, u64)>> {
    conn.query_row(
        "SELECT w.box,w.episode FROM registry_journal j JOIN hub_log_writer w
           ON w.position=j.log_position AND w.entry_id=j.entry_id
          WHERE j.op='agent.create' AND json_extract(j.payload_json,'$.entry.agent_id')=?1 AND w.position<=?2
          ORDER BY w.position LIMIT 1",
        params![id, through],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .optional()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HubKvCached {
    pub revision: u64,
    pub accepted_revision: Option<u64>,
    pub value: Option<Vec<u8>>,
    pub body: Option<Value>,
    pub read_at_ms: Option<i64>,
    pub integrity_failed: bool,
}

pub(crate) fn kv_cached(conn: &Connection, key: &str) -> rusqlite::Result<Option<HubKvCached>> {
    conn.query_row(
        "SELECT revision,accepted_revision,value,body,read_at_ms,integrity_failed
           FROM hub_kv_cache WHERE key=?1",
        [key],
        |row| {
            let body = row.get::<_, Option<String>>(3)?;
            Ok(HubKvCached {
                revision: row.get(0)?,
                accepted_revision: row.get(1)?,
                value: row.get(2)?,
                body: body
                    .as_deref()
                    .map(serde_json::from_str)
                    .transpose()
                    .map_err(decode_error)?,
                read_at_ms: row.get(4)?,
                integrity_failed: row.get(5)?,
            })
        },
    )
    .optional()
}

pub(crate) fn replay_membership(
    tx: &Transaction<'_>,
    kind: &str,
    body: &Value,
    position: i64,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO hub_membership(position,kind,body) VALUES(?1,?2,?3)",
        params![position, kind, body.to_string()],
    )?;
    Ok(())
}

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

fn attempts_by_op(conn: &Connection, op_id: &str) -> rusqlite::Result<Vec<HubAttempt>> {
    let mut stmt = conn
        .prepare("SELECT request_key FROM hub_attempt_send WHERE op_id=?1 ORDER BY request_key")?;
    let keys = stmt
        .query_map([op_id], |row| row.get::<_, String>(0))?
        .collect::<rusqlite::Result<Vec<_>>>()?;
    keys.iter()
        .map(|key| load_attempt(conn, key)?.ok_or(rusqlite::Error::QueryReturnedNoRows))
        .collect()
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

fn save_attempt(
    tx: &Transaction<'_>,
    write: &HubAttemptWrite,
    send: &HubAttemptSend,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT INTO hub_attempt(request_key,params_digest,key,expected,value,step)
         VALUES(?1,?2,?3,?4,?5,?6)
         ON CONFLICT(request_key) DO UPDATE SET
             key=excluded.key,expected=excluded.expected,value=excluded.value,step=excluded.step,
             outcome=NULL,outcome_op_id=NULL,outcome_at_ms=NULL",
        params![
            write.request_key,
            write.params_digest,
            write.key,
            write.expected,
            write.value,
            write.step
        ],
    )?;
    // Keep the first send's clock and process. A later resend must not
    // extend the time that a lease can be held for the same op id.
    tx.execute(
        "INSERT INTO hub_attempt_send(request_key,op_id,sent_at_ms,process) VALUES(?1,?2,?3,?4)
         ON CONFLICT(request_key,op_id) DO NOTHING",
        params![write.request_key, send.op_id, send.sent_at_ms, send.process],
    )?;
    Ok(())
}

const OPEN_HUB_ATTEMPTS: &str = "SELECT request_key FROM hub_attempt WHERE outcome IS NULL
    AND substr(step,1,length(?1))=?1 ORDER BY request_key";

impl RegistryStore {
    pub fn hub_created_agent(
        &self,
        request_key: &str,
    ) -> Result<Option<(i64, crate::agent::AgentRow)>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT j.seq,json_extract(j.payload_json,'$.entry.row') FROM registry_journal j
                   JOIN hub_attempt_send s ON s.op_id=j.entry_id
                   JOIN hub_log_writer w ON w.position=j.log_position AND w.entry_id=j.entry_id
                  WHERE s.request_key=?1 AND j.op='agent.create' LIMIT 1",
                [request_key],
                |row| {
                    let body: String = row.get(1)?;
                    Ok((
                        row.get(0)?,
                        serde_json::from_str(&body).map_err(decode_error)?,
                    ))
                },
            )
            .optional()
        })
    }

    pub fn hub_agent_creator(&self, id: &str) -> Result<Option<(String, u64)>, RegistryError> {
        self.hub_agent_creator_through(id, i64::MAX)
    }

    pub fn hub_agent_creator_through(
        &self,
        id: &str,
        through: i64,
    ) -> Result<Option<(String, u64)>, RegistryError> {
        self.read(|conn| agent_creator(conn, id, through))
    }

    pub fn hub_kv_cached(&self, key: &str) -> Result<Option<HubKvCached>, RegistryError> {
        self.read(|conn| kv_cached(conn, key))
    }

    pub fn hub_own_mint(&self) -> Result<Option<HubOwnMint>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT epoch,mint_id,state,revision FROM hub_epoch_status WHERE id=1",
                [],
                |row| {
                    Ok(HubOwnMint {
                        epoch: row.get(0)?,
                        mint_id: row.get(1)?,
                        committed: row.get::<_, String>(2)? == "committed",
                        revision: row.get(3)?,
                    })
                },
            )
            .optional()
        })
    }

    pub fn record_hub_own_mint(&self, mint: &HubOwnMint) -> Result<(), RegistryError> {
        self.db.with_conn_fenced(|tx| {
            tx.execute(
                "INSERT INTO hub_epoch_status(id,epoch,state,mint_id,revision) VALUES(1,?1,?2,?3,?4)
                 ON CONFLICT(id) DO UPDATE SET epoch=?1,state=?2,mint_id=?3,revision=?4
                 WHERE hub_epoch_status.revision<=?4",
                params![mint.epoch, if mint.committed { "committed" } else { "pending" }, mint.mint_id, mint.revision],
            )?;
            Ok(())
        }).map_err(RegistryError::Store)
    }

    pub fn clear_hub_own_mint(&self, epoch: u64, mint_id: &str) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "DELETE FROM hub_epoch_status WHERE id=1 AND epoch=?1 AND mint_id=?2",
                    params![epoch, mint_id],
                )?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }

    pub fn record_hub_held_epochs(
        &self,
        source: HubEpochSource<'_>,
        epochs: &[u64],
    ) -> Result<(), RegistryError> {
        let (kind, id) = source.parts();
        self.db
            .with_conn_fenced(|tx| {
                for epoch in epochs {
                    tx.execute(
                        "INSERT INTO hub_held_epoch(epoch,source_kind,source_id) VALUES(?1,?2,?3)
                     ON CONFLICT(epoch,source_kind,source_id) DO NOTHING",
                        params![epoch, kind, id],
                    )?;
                }
                Ok(())
            })
            .map_err(RegistryError::Store)
    }

    /// Each join row names an epoch actually carried by that join wrap.
    pub fn hub_holds_epoch(&self, epoch: u64, mint_id: &str) -> Result<bool, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM hub_held_epoch WHERE epoch=?1 AND
                            (source_kind='join' OR (source_kind='mint' AND source_id=?2)))",
                params![epoch, mint_id],
                |row| row.get(0),
            )
        })
    }

    pub fn hub_has_epoch(&self, epoch: u64) -> Result<bool, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM hub_held_epoch WHERE epoch=?1)",
                [epoch],
                |row| row.get(0),
            )
        })
    }

    /// Install a verified value (or an allowed delete with no body). A refusal
    /// advances the observed revision but preserves the previously accepted value.
    pub fn record_hub_kv(
        &self,
        key: &str,
        revision: u64,
        accepted: Option<(&[u8], Option<&Value>)>,
        now: i64,
    ) -> Result<bool, RegistryError> {
        self.with_where_changes(|tx| {
            if let Some((value, body)) = accepted {
                let updated = tx.execute(
                    "INSERT INTO hub_kv_cache(key,revision,accepted_revision,value,body,read_at_ms,integrity_failed)
                     VALUES(?1,?2,?2,?3,?4,?5,0)
                     ON CONFLICT(key) DO UPDATE SET revision=?2,accepted_revision=?2,value=?3,body=?4,read_at_ms=?5,integrity_failed=0
                      WHERE revision<?2 OR (revision=?2 AND integrity_failed=0
                        AND (accepted_revision IS NOT revision OR value=?3))",
                    params![key, revision, value, body.map(Value::to_string), now],
                )? != 0;
                if updated {
                    if let (Some(role), Some(gen)) = (key.strip_prefix("role."), body.and_then(|v| v["gen"].as_i64())) {
                        crate::role_cache::raise_gen(tx, role, gen)?;
                    }
                }
                Ok(updated)
            } else {
                Ok(tx.execute(
                    "INSERT INTO hub_kv_cache(key,revision,integrity_failed,read_at_ms) VALUES(?1,?2,1,?3)
                     ON CONFLICT(key) DO UPDATE SET revision=?2,integrity_failed=1
                     WHERE revision<?2 OR (revision=?2 AND integrity_failed=0)",
                    params![key, revision, now],
                )? != 0)
            }
        })
        .map_err(RegistryError::Store)
    }

    /// A held revision is neither accepted nor refused. Keep the last accepted
    /// value for judgement when replay or a roster change makes this one verifiable.
    pub fn hold_hub_kv(&self, key: &str, revision: u64) -> Result<(), RegistryError> {
        self.with_where_changes(|tx| {
            tx.execute(
                "INSERT INTO hub_kv_cache(key,revision,integrity_failed) VALUES(?1,?2,0)
                 ON CONFLICT(key) DO UPDATE SET revision=?2,integrity_failed=0
                 WHERE revision<?2",
                params![key, revision],
            )?;
            Ok(())
        })
        .map_err(RegistryError::Store)
    }

    pub fn open_hub_attempts(&self, step_prefix: &str) -> Result<Vec<HubAttempt>, RegistryError> {
        self.read(|conn| {
            let mut stmt = conn.prepare(OPEN_HUB_ATTEMPTS)?;
            let keys = stmt.query_map([step_prefix], |row| row.get::<_, String>(0))?;
            let mut rows = Vec::new();
            for key in keys {
                if let Some(row) = load_attempt(conn, &key?)? {
                    rows.push(row);
                }
            }
            Ok(rows)
        })
    }

    pub fn hub_membership_scan(&self) -> Result<HubMembershipScan, RegistryError> {
        self.read(|conn| {
            let mut rows = conn
                .prepare("SELECT sequence,candidate FROM hub_membership_scan ORDER BY sequence")?;
            let mut scan = HubMembershipScan {
                through: 0,
                candidates: Vec::new(),
            };
            for row in rows.query_map([], |row| {
                Ok((row.get::<_, u64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?))
            })? {
                let (sequence, candidate) = row?;
                scan.through = sequence;
                if let Some(candidate) = candidate {
                    scan.candidates.push((sequence, candidate));
                }
            }
            Ok(scan)
        })
    }

    pub fn checkpoint_hub_membership_scan(
        &self,
        through: u64,
        candidates: &[(u64, Vec<u8>)],
    ) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "DELETE FROM hub_membership_scan WHERE candidate IS NULL AND sequence < ?1",
                    [through],
                )?;
                for (sequence, value) in candidates {
                    tx.execute(
                        "INSERT INTO hub_membership_scan(sequence,candidate) VALUES(?1,?2)
                     ON CONFLICT(sequence) DO NOTHING",
                        params![sequence, value],
                    )?;
                }
                tx.execute(
                    "INSERT INTO hub_membership_scan(sequence,candidate) VALUES(?1,NULL)
                 ON CONFLICT(sequence) DO NOTHING",
                    [through],
                )?;
                Ok(())
            })
            .map_err(RegistryError::Store)
    }

    pub fn hub_anchors(&self) -> Result<Vec<HubAnchor>, RegistryError> {
        self.read(|conn| {
            conn.prepare("SELECT box,episode,key_version,pub FROM hub_anchor")?
                .query_map([], |r| {
                    Ok(HubAnchor {
                        box_id: r.get(0)?,
                        episode: r.get(1)?,
                        version: r.get(2)?,
                        public_key: r.get(3)?,
                    })
                })?
                .collect()
        })
    }

    pub fn persist_hub_anchors(&self, anchors: &[HubAnchor]) -> Result<(), RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                for anchor in anchors {
                    let count = tx.execute(
                        "INSERT INTO hub_anchor(box,episode,key_version,pub) VALUES(?1,?2,?3,?4)
                     ON CONFLICT(box,episode,key_version) DO UPDATE SET pub=excluded.pub
                     WHERE hub_anchor.pub=excluded.pub",
                        params![
                            anchor.box_id,
                            anchor.episode,
                            anchor.version,
                            anchor.public_key
                        ],
                    )?;
                    if count != 1 {
                        return Err(rusqlite::Error::InvalidParameterName(
                            "conflicting public keys for one hub anchor".into(),
                        ));
                    }
                }
                Ok(())
            })
            .map_err(RegistryError::Store)
    }

    pub fn hub_snapshot_applied(&self, base: i64) -> Result<bool, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM registry_journal
                                WHERE op IN ('shared.snapshot', 'hub.snapshot') AND log_position>?1)",
                [base],
                |r| r.get(0),
            )
        })
    }

    pub fn hub_log_base(&self) -> Result<Option<i64>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT hub_log_base FROM identity_log_state WHERE id=1",
                [],
                |row| row.get(0),
            )
        })
    }

    pub fn journal_log_position(&self, entry_id: &str) -> Result<Option<i64>, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT log_position FROM registry_journal WHERE entry_id=?1 AND log_position IS NOT NULL",
                [entry_id],
                |row| row.get(0),
            )
            .optional()
        })
    }

    pub fn hub_attempt(&self, request_key: &str) -> Result<Option<HubAttempt>, RegistryError> {
        self.read(|conn| load_attempt(conn, request_key))
    }

    /// A derived snapshot id can appear in more than one local request.
    /// Reads on the read connection, so it never waits for the writer.
    pub fn hub_attempts_by_op(&self, op_id: &str) -> Result<Vec<HubAttempt>, RegistryError> {
        self.read(|conn| attempts_by_op(conn, op_id))
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
        self.persist_hub_attempt_with_local(write, send, previous, None)
    }

    pub fn persist_hub_attempt_with_local(
        &self,
        write: &HubAttemptWrite,
        send: &HubAttemptSend,
        previous: Option<&HubAttempt>,
        continuation: Option<&crate::HubContinuation>,
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
                save_attempt(tx, write, send)?;
                if let Some(continuation) = continuation {
                    tx.execute(
                        "UPDATE hub_attempt SET continuation=?1 WHERE request_key=?2",
                        params![
                            serde_json::to_string(continuation).map_err(decode_error)?,
                            write.request_key
                        ],
                    )?;
                }
                Ok(true)
            })
            .map_err(RegistryError::Store)
    }

    pub fn hub_local_reply(
        &self,
        key: &str,
        digest: &str,
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        let row = self.read(|conn| {
            conn.query_row(
                "SELECT params_digest,reply FROM hub_local_reply WHERE request_key=?1",
                [key],
                |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)),
            )
            .optional()
        })?;
        match row {
            Some((saved, _)) if saved != digest => Err(crate::mutations::domain(
                "request_key_conflict",
                "request key has different arguments",
            )),
            Some((_, reply)) => Ok(Some(reply.into_bytes())),
            None => Ok(None),
        }
    }

    pub fn hub_local_time(&self, key: &str) -> Result<Option<i64>, RegistryError> {
        self.read(|conn| conn.query_row("SELECT json_extract(continuation,'$.now') FROM hub_attempt WHERE request_key=?1 AND continuation IS NOT NULL", [key], |r| r.get(0)).optional())
    }

    /// Close requests matching an applied operation and its current target.
    /// An op id in the send history alone does not identify the current step.
    pub fn record_hub_attempt_outcome_by_op(
        &self,
        key: &str,
        position: u64,
        outcome: &HubAttemptOutcome,
        matches_current: impl Fn(&HubAttemptWrite, u64) -> bool,
    ) -> Result<usize, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let mut settled = 0;
                for row in attempts_by_op(tx, &outcome.op_id)? {
                    if row.outcome.is_none()
                        && row.write.key == key
                        && matches_current(&row.write, position)
                        && record_attempt_outcome(tx, &row.write, outcome)?
                    {
                        settled += 1;
                    }
                }
                Ok(settled)
            })
            .map_err(RegistryError::Store)
    }

    /// Replace a landed intermediate step and save the next send in one
    /// transaction. The targets must differ so a late delivery from the
    /// earlier step can never settle the new one.
    pub fn advance_hub_attempt(
        &self,
        previous: &HubAttempt,
        landed_op_id: &str,
        next: &HubAttemptWrite,
        send: &HubAttemptSend,
    ) -> Result<bool, RegistryError> {
        self.db
            .with_conn_fenced(|tx| {
                let prior = &previous.write;
                if prior.request_key != next.request_key
                    || prior.params_digest != next.params_digest
                    || prior.key == next.key
                    || prior.step == next.step
                    || previous.outcome.is_some()
                    || !previous.op_ids.iter().any(|op| op.op_id == landed_op_id)
                    || previous.op_ids.iter().any(|op| op.op_id == send.op_id)
                    || load_attempt(tx, &prior.request_key)?.as_ref() != Some(previous)
                {
                    return Ok(false);
                }
                save_attempt(tx, next, send)?;
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

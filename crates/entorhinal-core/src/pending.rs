//! Synchronous SQLite helpers for shared writes made by the asynchronous module.
//! A worker thread keeps the transaction open while the module asks the log to
//! append an entry. The module reports whether to commit or roll back; these
//! helpers record unfinished attempts without making network calls themselves.

use super::{cached, domain};
use crate::{RegistryError, RegistryStore};
use rusqlite::{params, Connection, OptionalExtension, Transaction};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

pub(crate) const HUB_LOCAL: &str = "hub.local_write";

/// Migration 27.
///
/// `pending_entry.continuation` keeps this machine's own half of a legacy
/// shared write whose append may have reached the log: the request key, the
/// digest of its arguments and the reply the rolled-back local transaction
/// would have stored. When catch-up later applies that same entry, the reply is
/// saved under the request key, so a retry with the same key gets the original
/// success instead of a conflict with its own earlier write. None of it is
/// ever part of the shared entry.
///
/// The partial indexes keep session retention, identity change feeds, agent
/// cutover checks, and open attempt recovery from scanning unrelated rows.
/// SQLite uses a partial index only when the query repeats its condition: the identity op
/// list must match `IDENTITY_CHANGE_OPS`, and a check for the one-time
/// `agent.cutover` journal entry (written when this store took over agent
/// identity) must test `op='agent.cutover'` literally, not through a bound
/// parameter.
/// The move history index serves an agent's moves in display order.
pub(crate) const V27_LEGACY_REPLY_AND_QUERY_INDEXES: &str = r#"
ALTER TABLE pending_entry ADD COLUMN continuation TEXT NULL;
CREATE INDEX session_binding_ended
    ON session_binding(ended_at_ms) WHERE ended_at_ms IS NOT NULL;
CREATE INDEX registry_journal_identity
    ON registry_journal(seq)
    WHERE op IN ('agent.create','agent.rename','agent.update_tag','agent.set_labels',
                 'agent.set_avatar','agent.set_github_identity','agent.dispose',
                 'agent.merge','agent.import','agent.set_namespace');

-- Keep signed membership candidates even when the local roster cannot verify
-- them yet. The highest sequence is the scan cursor; a NULL row marks a
-- cursor that ends on a non-membership entry.
CREATE TABLE hub_membership_scan (
    sequence INTEGER PRIMARY KEY CHECK(sequence > 0),
    candidate BLOB NULL
);
CREATE INDEX registry_journal_cutover
    ON registry_journal(op) WHERE op='agent.cutover';
CREATE INDEX agent_move_agent_history
    ON agent_move(agent_id, created_at_ms, cut_id);
CREATE INDEX hub_attempt_open ON hub_attempt(request_key, step) WHERE outcome IS NULL;
"#;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HubContinuation {
    pub entry_id: String,
    pub now: i64,
    pub reply: Value,
    pub roots: Option<HubRoots>,
    pub consent: Option<HubConsent>,
    bindings: Vec<crate::binding::PreparedBinding>,
    mappings: Vec<crate::shared_entry::RootKeyMapping>,
}

impl HubContinuation {
    pub fn prepare(
        store: &RegistryStore,
        entry_id: &str,
        now: i64,
        reply: Value,
        roots: Option<HubRoots>,
        label: Option<&str>,
        consent: Option<HubConsent>,
    ) -> Result<Self, RegistryError> {
        let mut bindings = Vec::new();
        let mut mappings = Vec::new();
        if let Some(roots) = &roots {
            for root in &roots.roots {
                let key = store.read(|conn| {
                    Ok(crate::root_keys::incoming(
                        conn,
                        &roots.project,
                        root,
                        label,
                    ))
                })??;
                if let Some(key) = key {
                    mappings.push(crate::shared_entry::RootKeyMapping {
                        canonical_root: root.clone(),
                        kind: key.kind,
                        root_key: key.root_key,
                    });
                }
                if store.root_records_enabled() {
                    match store.prepare_hub_binding(root) {
                        Ok(binding) => bindings.push(binding),
                        Err(RegistryError::Domain { code, .. })
                            if code == "root_identity_unverifiable" => {}
                        Err(error) => return Err(error),
                    }
                }
            }
        }
        Ok(Self {
            entry_id: entry_id.into(),
            now,
            reply,
            roots,
            consent,
            bindings,
            mappings,
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HubRoots {
    pub project: String,
    pub roots: Vec<String>,
    pub parents: Vec<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct HubConsent {
    request: crate::agent::consent::ConsentRequest,
    authorization: crate::agent::consent::Authorization,
    action: String,
    target: Value,
    granted: Option<crate::agent::consent::StandingGrant>,
    now_ms: i64,
}

impl From<crate::agent::consent::ConsentCommit> for HubConsent {
    fn from(commit: crate::agent::consent::ConsentCommit) -> Self {
        Self {
            request: commit.request,
            authorization: commit.authorization,
            action: commit.action,
            target: commit.target,
            granted: commit.granted,
            now_ms: commit.now_ms,
        }
    }
}

#[derive(Deserialize, Serialize)]
struct LocalImage {
    roots: Option<HubRoots>,
    mappings: Vec<crate::shared_entry::RootKeyMapping>,
    now: i64,
}

fn restore_roots(tx: &Transaction<'_>, image: &LocalImage) -> rusqlite::Result<()> {
    if let Some(intent) = &image.roots {
        for root in &intent.roots {
            tx.execute("INSERT OR IGNORE INTO project_root(canonical_root,project_id,added_at) VALUES(?1,?2,?3)", params![root, intent.project, image.now])?;
            tx.execute("INSERT OR IGNORE INTO project_alias(old_id,project_id,created_at) VALUES(?1,?2,?3)", params![crate::implicit_project_id(root), intent.project, image.now])?;
        }
        for parent in &intent.parents {
            tx.execute("INSERT OR IGNORE INTO derived_root_parent(canonical_parent,project_id) VALUES(?1,?2)", params![parent, intent.project])?;
        }
    }
    crate::shared_entry::replay_root_keys(tx, json!({"mappings":image.mappings}))
}

pub(crate) fn replay_hub_local(tx: &Transaction<'_>, value: Value) -> rusqlite::Result<()> {
    let image: Option<LocalImage> =
        serde_json::from_value(value).map_err(crate::shared_entry::decode_error)?;
    if let Some(image) = image {
        restore_roots(tx, &image)?;
    }
    Ok(())
}

fn local_image(
    tx: &Transaction<'_>,
    continuation: &HubContinuation,
) -> Result<LocalImage, RegistryError> {
    let mut mappings = Vec::new();
    if let Some(roots) = &continuation.roots {
        for (table, column, paths) in [
            ("project_root", "canonical_root", &roots.roots),
            ("derived_root_parent", "canonical_parent", &roots.parents),
        ] {
            for path in paths {
                let owner: Option<String> = tx
                    .query_row(
                        &format!("SELECT project_id FROM {table} WHERE {column}=?1"),
                        [path],
                        |r| r.get(0),
                    )
                    .optional()?;
                if owner.is_some_and(|owner| owner != roots.project) {
                    return Err(domain(
                        "root_conflict",
                        format!("{path} now belongs to another project"),
                    ));
                }
            }
        }
        for key in &continuation.mappings {
            let present: bool = tx.query_row("SELECT EXISTS(SELECT 1 FROM project_root_key WHERE project_id=?1 AND kind=?2 AND root_key=?3)",
                params![roots.project, key.kind, key.root_key], |r| r.get(0))?;
            if present {
                mappings.push(key.clone());
            }
        }
    }
    Ok(LocalImage {
        roots: continuation.roots.clone(),
        mappings,
        now: continuation.now,
    })
}

fn local_for_entry(
    conn: &Connection,
    entry: &crate::remote_apply::RemoteEntry,
) -> rusqlite::Result<Option<(String, HubContinuation, i64)>> {
    conn.query_row(
        "SELECT a.request_key,a.continuation,a.expected FROM hub_attempt a
          JOIN hub_attempt_send s ON s.request_key=a.request_key WHERE s.op_id=?1 AND a.key='ck.fleet.idlog'
          AND a.continuation IS NOT NULL AND a.outcome IS NULL AND a.expected+1=?2-(SELECT hub_log_base FROM identity_log_state WHERE id=1)",
        params![entry.entry_id, entry.position], |r| Ok((r.get(0)?,
            serde_json::from_str(&r.get::<_, String>(1)?).map_err(crate::shared_entry::decode_error)?, r.get(2)?)),
    ).optional()
}

impl RegistryStore {
    pub(crate) fn check_hub_bindings(
        &self,
        entry: &crate::remote_apply::RemoteEntry,
    ) -> Result<Vec<String>, RegistryError> {
        let local = self.read(|conn| local_for_entry(conn, entry))?;
        Ok(local
            .into_iter()
            .flat_map(|(_, continuation, _)| continuation.bindings)
            .filter(|binding| binding.still_present())
            .map(|binding| binding.root)
            .collect())
    }
}

pub(crate) fn apply_hub_local(
    tx: &Transaction<'_>,
    entry: &crate::remote_apply::RemoteEntry,
    present: &[String],
) -> rusqlite::Result<()> {
    let Some((request_key, continuation, expected)) = local_for_entry(tx, entry)? else {
        return Ok(());
    };
    tx.execute_batch("SAVEPOINT hub_local_write")?;
    let result = (|| {
        let image = local_image(tx, &continuation)?;
        restore_roots(tx, &image)?;
        let value = serde_json::to_value(image).map_err(crate::shared_entry::decode_error)?;
        let mut seq = super::append(
            tx,
            HUB_LOCAL,
            &value,
            "log",
            None,
            continuation.now,
            "entorhinal",
        )?;
        let mut reply = continuation.reply.clone();
        let mut replaced = Vec::new();
        if let Some(roots) = &continuation.roots {
            for binding in &continuation.bindings {
                if present.contains(&binding.root) {
                    if let Some((bound_seq, _)) =
                        binding.write(tx, &roots.project, "log", "entorhinal")?
                    {
                        seq = bound_seq;
                    }
                } else {
                    replaced.push(&binding.root);
                }
            }
        }
        if let Some(result) = reply.get_mut("result").and_then(Value::as_object_mut) {
            if let Some(generation) = result.get_mut("generation") {
                *generation = seq.into();
            }
            if !replaced.is_empty() {
                result.insert("unboundRoots".into(), json!(replaced));
            }
        }
        if let Some(consent) = continuation.consent.clone() {
            let consent = crate::agent::consent::ConsentCommit {
                request: consent.request,
                authorization: consent.authorization,
                action: consent.action,
                target: consent.target,
                granted: consent.granted,
                now_ms: consent.now_ms,
            };
            crate::agent::consent::complete(tx, &consent, &reply["result"])?;
        }
        Ok::<_, RegistryError>(reply)
    })();
    let reply = match result {
        Ok(reply) => reply,
        Err(RegistryError::Domain {
            code,
            message,
            detail,
        }) => {
            tx.execute_batch("ROLLBACK TO hub_local_write")?;
            json!({"error":{"code":code,"message":message,"detail":detail}})
        }
        Err(error) => return Err(rusqlite::Error::ToSqlConversionFailure(Box::new(error))),
    };
    tx.execute_batch("RELEASE hub_local_write")?;
    let outcome = json!({"status":"landed","position":expected + 1});
    tx.execute("UPDATE hub_attempt SET outcome=?1,outcome_op_id=?2,outcome_at_ms=?3 WHERE request_key=?4 AND outcome IS NULL",
        params![outcome.to_string(), entry.entry_id, crate::now_unix_millis(), request_key])?;
    tx.execute("INSERT INTO hub_local_reply(request_key,params_digest,reply) SELECT request_key,params_digest,?1 FROM hub_attempt WHERE request_key=?2",
        params![reply.to_string(), request_key])?;
    Ok(())
}
use std::cell::RefCell;

/// Save the reply of this machine's own legacy shared write when the entry it
/// sent is applied from the log after its local transaction failed. Runs in
/// the transaction that applies the entry, before that entry's pending row is
/// cleared. A reply already saved under the same key is kept.
pub(crate) fn install_legacy_reply(
    tx: &Transaction<'_>,
    entry: &crate::remote_apply::RemoteEntry,
) -> rusqlite::Result<()> {
    tx.execute(
        "INSERT OR IGNORE INTO hub_local_reply(request_key,params_digest,reply)
         SELECT json_extract(continuation,'$.request_key'),json_extract(continuation,'$.params_digest'),
                json_extract(continuation,'$.reply')
           FROM pending_entry WHERE entry_id=?1 AND expected_head+1=?2 AND continuation IS NOT NULL",
        params![entry.entry_id, entry.position],
    )?;
    Ok(())
}

pub enum SharedSettlement {
    Commit(i64),
    Rollback {
        sent: bool,
        code: String,
        message: String,
    },
}

struct Attempt {
    id: String,
    sent: bool,
    settle: Box<dyn FnMut(Vec<u8>) -> SharedSettlement>,
    preview: Option<Preview>,
    /// The request key and argument digest the caller caches this write's
    /// reply under, if any.
    reply_key: Option<(String, String)>,
    /// The key, digest and reply captured when the entry was handed to the
    /// log, saved on the pending row if the local transaction then fails.
    continuation: Option<String>,
}
type Preview = Box<dyn FnOnce(Vec<u8>, Value)>;
thread_local! {
    static ATTEMPT: RefCell<Option<Attempt>> = const { RefCell::new(None) };
}

pub(crate) fn entry_id() -> Option<String> {
    ATTEMPT.with(|slot| slot.borrow().as_ref().map(|attempt| attempt.id.clone()))
}

impl RegistryStore {
    /// Look up a reply by request key and operation name, and refuse a key that
    /// was already used for a different operation. Otherwise a request could
    /// receive another operation's reply. Use the read connection so this check
    /// does not wait for a database write or contact the log.
    pub fn shared_request_cache(
        &self,
        op: &str,
        key: Option<&str>,
    ) -> Result<Option<Vec<u8>>, RegistryError> {
        let result = self.read(|conn| {
            if let Some(blob) = cached(conn, op, key)? {
                return Ok(Ok(Some(blob)));
            }
            if let Some(key) = key {
                let other: Option<String> = conn
                    .query_row(
                        "SELECT op FROM registry_journal WHERE request_key=?1",
                        [key],
                        |r| r.get(0),
                    )
                    .optional()?;
                if let Some(other) = other.filter(|other| other != op) {
                    return Ok(Err(domain(
                        "request_key_reused_across_ops",
                        format!("request_key is already bound to op '{other}'"),
                    )));
                }
            }
            Ok(Ok(None))
        })?;
        result
    }

    /// Save the entry id and expected log head in a separate transaction before
    /// running the operation. If the operation fails, delete this record only
    /// when no append request for the entry could have been sent. Once a request
    /// might have reached the log, keep the record for later log reads to check,
    /// because the log may accept the entry even after the local write fails.
    pub fn shared_attempt<E: From<RegistryError>>(
        &self,
        id: String,
        head: i64,
        settle: impl FnMut(Vec<u8>) -> SharedSettlement + 'static,
        operation: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, E> {
        self.shared_attempt_inner(id, head, settle, None, None, operation)
    }

    /// [`Self::shared_attempt`] for a write whose reply the caller caches
    /// under `reply_key`, a request key and the digest of its arguments. If
    /// the entry may have reached the log but the local transaction fails, the
    /// pending row keeps the key, digest and reply. Applying that entry from
    /// the log later saves the reply in `hub_local_reply`, where the caller
    /// looks before running a retry.
    pub fn shared_attempt_with_reply<E: From<RegistryError>>(
        &self,
        id: String,
        head: i64,
        reply_key: Option<(String, String)>,
        settle: impl FnMut(Vec<u8>) -> SharedSettlement + 'static,
        operation: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, E> {
        self.shared_attempt_inner(id, head, settle, None, reply_key, operation)
    }

    pub fn preview_hub_write<E: From<RegistryError>>(
        &self,
        id: String,
        preview: impl FnOnce(Vec<u8>, Value) + 'static,
        operation: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, E> {
        self.shared_attempt_inner(
            id,
            0,
            |_| unreachable!("hub preview always rolls back a shared change"),
            Some(Box::new(preview)),
            None,
            operation,
        )
    }

    fn shared_attempt_inner<E: From<RegistryError>>(
        &self,
        id: String,
        head: i64,
        settle: impl FnMut(Vec<u8>) -> SharedSettlement + 'static,
        preview: Option<Preview>,
        reply_key: Option<(String, String)>,
        operation: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, E> {
        let legacy = preview.is_none();
        if legacy {
            self.db
                .with_conn_fenced(|tx| {
                    tx.execute(
                        "INSERT INTO pending_entry(entry_id,expected_head) VALUES(?1,?2)",
                        params![id, head],
                    )?;
                    Ok(())
                })
                .map_err(RegistryError::Store)?;
        }
        ATTEMPT.with(|slot| {
            assert!(slot.borrow().is_none(), "nested shared attempt");
            *slot.borrow_mut() = Some(Attempt {
                id: id.clone(),
                sent: false,
                settle: Box::new(settle),
                preview,
                reply_key,
                continuation: None,
            });
        });
        struct Reset;
        impl Drop for Reset {
            fn drop(&mut self) {
                ATTEMPT.with(|slot| {
                    slot.borrow_mut().take();
                });
            }
        }
        let reset = Reset;
        let result = operation();
        let (sent, continuation) = ATTEMPT.with(|slot| {
            let mut slot = slot.borrow_mut();
            let attempt = slot.as_mut().unwrap();
            (attempt.sent, attempt.continuation.take())
        });
        drop(reset);
        if legacy && result.is_err() && !sent {
            self.db
                .with_conn_fenced(|tx| {
                    tx.execute("DELETE FROM pending_entry WHERE entry_id=?1", [&id])?;
                    Ok(())
                })
                .map_err(RegistryError::Store)?;
        } else if legacy && result.is_err() {
            if let Some(continuation) = continuation {
                // The local transaction has rolled back, so its reply survives
                // only here, on the pending row committed before it began.
                self.db
                    .with_conn_fenced(|tx| {
                        tx.execute(
                            "UPDATE pending_entry SET continuation=?1 WHERE entry_id=?2",
                            params![continuation, id],
                        )?;
                        Ok(())
                    })
                    .map_err(RegistryError::Store)?;
            }
        }
        result
    }

    pub fn observe_log_head(&self, head: i64) -> Result<(), RegistryError> {
        self.db.with_conn_fenced(|tx| {
            tx.execute("UPDATE identity_log_state SET last_seen_head=MAX(last_seen_head,?1) WHERE id=1", [head])?;
            Ok(())
        }).map_err(RegistryError::Store)?;
        Ok(())
    }

    pub fn is_pending_entry(&self, id: &str) -> Result<bool, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT EXISTS(SELECT 1 FROM pending_entry WHERE entry_id=?1)",
                [id],
                |r| r.get(0),
            )
        })
    }

    pub fn root_count(&self, project_id: &str) -> Result<i64, RegistryError> {
        self.read(|conn| {
            conn.query_row(
                "SELECT COUNT(*) FROM project_root WHERE project_id=?1",
                [project_id],
                |r| r.get(0),
            )
        })
    }
}

pub(crate) fn finish(tx: &Transaction<'_>, seq: Option<i64>) -> Result<(), RegistryError> {
    ATTEMPT.with(|slot| {
        let mut slot = slot.borrow_mut();
        let Some(attempt) = slot.as_mut() else { return Ok(()); };
        let entry: Option<Vec<u8>> = match seq {
            Some(seq) => tx.query_row("SELECT entry FROM registry_journal WHERE seq=?1 AND stream='shared'", [seq], |r| r.get(0)).optional()?.flatten(),
            None => None,
        };
        if let Some(entry) = entry {
            if entry.len() > 256 * 1024 {
                return Err(domain("shared_entry_too_large", format!("shared entry is {} bytes; cap is 262144 bytes", entry.len())));
            }
            if let Some(preview) = attempt.preview.take() {
                let reply: Option<String> = tx.query_row("SELECT response_json FROM registry_journal WHERE seq=?1", [seq], |r| r.get(0))?;
                let reply = reply.as_deref().map(serde_json::from_str).transpose()
                    .map_err(crate::shared_entry::decode_error)?.unwrap_or_else(|| json!({"result":{}}));
                preview(entry, reply);
                return Err(domain("hub_write_preview", "shared preview rolled back"));
            }
            // Mark the entry as possibly sent before asking the module for an
            // answer. If its async request is cancelled or the worker exits
            // before getting that answer, the log may already have accepted it.
            // Keeping the pending row lets later log reads check what happened;
            // the row alone neither resends the entry nor commits local changes.
            attempt.sent = true;
            // Capture the reply this transaction stores under its request key,
            // in case the transaction rolls back after the log accepted the
            // entry. Agent tool writes made under a consent card pass no key:
            // their outcome is recorded on the consent request instead.
            if let Some((request_key, params_digest)) = &attempt.reply_key {
                let reply: Option<String> = tx.query_row(
                    "SELECT response_json FROM registry_journal WHERE seq=?1",
                    [seq],
                    |r| r.get(0),
                )?;
                attempt.continuation = reply.map(|reply| {
                    json!({"request_key":request_key,"params_digest":params_digest,"reply":reply})
                        .to_string()
                });
            }
            match (attempt.settle)(entry) {
                SharedSettlement::Commit(position) => {
                    tx.execute("UPDATE registry_journal SET entry_id=?1,log_position=?2 WHERE seq=?3", params![attempt.id,position,seq])?;
                    tx.execute("UPDATE identity_log_state SET last_applied_position=?1,last_seen_head=MAX(last_seen_head,?1) WHERE id=1", [position])?;
                    // Observing an occupied position proves every older attempt
                    // lost its only possible slot, even when its reply was lost.
                    tx.execute("DELETE FROM pending_entry WHERE expected_head < ?1", [position])?;
                }
                SharedSettlement::Rollback { sent, code, message } => {
                    attempt.sent = sent;
                    return Err(domain(&code, message));
                }
            }
        }
        tx.execute("DELETE FROM pending_entry WHERE entry_id=?1", [&attempt.id])?;
        Ok(())
    })
}

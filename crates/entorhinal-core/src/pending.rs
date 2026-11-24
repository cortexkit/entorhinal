//! Synchronous SQLite helpers for previewing hub writes and completing their
//! local follow-up writes when the hub entry lands.

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

struct Attempt {
    id: String,
    preview: Option<Preview>,
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

    pub fn preview_hub_write<E: From<RegistryError>>(
        &self,
        id: String,
        preview: impl FnOnce(Vec<u8>, Value) + 'static,
        operation: impl FnOnce() -> Result<Vec<u8>, E>,
    ) -> Result<Vec<u8>, E> {
        ATTEMPT.with(|slot| {
            assert!(slot.borrow().is_none(), "nested shared attempt");
            *slot.borrow_mut() = Some(Attempt {
                id,
                preview: Some(Box::new(preview)),
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
        let _reset = Reset;
        operation()
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
        let Some(seq) = seq else {
            return Ok(());
        };
        let mut slot = slot.borrow_mut();
        let Some(attempt) = slot.as_mut() else {
            return Ok(());
        };
        let entry: Option<Vec<u8>> = tx
            .query_row(
                "SELECT entry FROM registry_journal WHERE seq=?1 AND stream='shared'",
                [seq],
                |r| r.get(0),
            )
            .optional()?
            .flatten();
        let Some(entry) = entry else {
            return Ok(());
        };
        if entry.len() > 256 * 1024 {
            return Err(domain(
                "shared_entry_too_large",
                format!("shared entry is {} bytes; cap is 262144 bytes", entry.len()),
            ));
        }
        let reply: Option<String> = tx.query_row(
            "SELECT response_json FROM registry_journal WHERE seq=?1",
            [seq],
            |r| r.get(0),
        )?;
        let reply = reply
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map_err(crate::shared_entry::decode_error)?
            .unwrap_or_else(|| json!({"result":{}}));
        let preview = attempt
            .preview
            .take()
            .expect("hub preview already captured a shared write");
        preview(entry, reply);
        Err(domain("hub_write_preview", "shared preview rolled back"))
    })
}

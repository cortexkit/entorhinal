//! Synchronous SQLite helpers for shared writes made by the asynchronous module.
//! A worker thread keeps the transaction open while the module asks the log to
//! append an entry. The module reports whether to commit or roll back; these
//! helpers record unfinished attempts without making network calls themselves.

use super::{cached, domain};
use crate::{RegistryError, RegistryStore};
use rusqlite::{params, OptionalExtension, Transaction};
use std::cell::RefCell;

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
}
thread_local! {
    static ATTEMPT: RefCell<Option<Attempt>> = const { RefCell::new(None) };
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
        self.db
            .with_conn_fenced(|tx| {
                tx.execute(
                    "INSERT INTO pending_entry(entry_id,expected_head) VALUES(?1,?2)",
                    params![id, head],
                )?;
                Ok(())
            })
            .map_err(RegistryError::Store)?;
        ATTEMPT.with(|slot| {
            assert!(slot.borrow().is_none(), "nested shared attempt");
            *slot.borrow_mut() = Some(Attempt {
                id: id.clone(),
                sent: false,
                settle: Box::new(settle),
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
        let sent = ATTEMPT.with(|slot| slot.borrow().as_ref().unwrap().sent);
        drop(reset);
        if result.is_err() && !sent {
            self.db
                .with_conn_fenced(|tx| {
                    tx.execute("DELETE FROM pending_entry WHERE entry_id=?1", [&id])?;
                    Ok(())
                })
                .map_err(RegistryError::Store)?;
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

pub(super) fn finish(tx: &Transaction<'_>, seq: Option<i64>) -> Result<(), RegistryError> {
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
            // Mark the entry as possibly sent before asking the module for an
            // answer. If its async request is cancelled or the worker exits
            // before getting that answer, the log may already have accepted it.
            // Keeping the pending row lets later log reads check what happened;
            // the row alone neither resends the entry nor commits local changes.
            attempt.sent = true;
            match (attempt.settle)(entry) {
                SharedSettlement::Commit(position) => {
                    tx.execute("UPDATE registry_journal SET entry_id=?1,log_position=?2 WHERE seq=?3", params![attempt.id,position,seq])?;
                    tx.execute("UPDATE identity_log_state SET last_applied_position=?1,last_seen_head=MAX(last_seen_head,?1) WHERE id=1", [position])?;
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

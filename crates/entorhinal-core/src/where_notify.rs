//! Post-commit invalidation for whereabouts fields derived outside claim_cache.

use std::sync::{Arc, Mutex};

use cortexkit_store::StoreError;
use rusqlite::{Connection, OptionalExtension, Transaction};

use crate::RegistryStore;

type Listener = Arc<dyn Fn() + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct WhereNotifications(Arc<Mutex<Option<Listener>>>);

#[derive(PartialEq)]
struct AgentLiveness {
    id: String,
    terminal: Option<String>,
    terminal_at: Option<i64>,
    merged_into: Option<String>,
}

#[derive(PartialEq)]
struct Dependencies {
    self_author: Option<String>,
    names: Vec<(String, String)>,
    sharing_off: bool,
    agents: Vec<AgentLiveness>,
}

fn exists(conn: &Connection, table: &str) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1)",
        [table],
        |row| row.get(0),
    )
}

impl Dependencies {
    fn read(conn: &Connection) -> rusqlite::Result<Self> {
        // Older-schema recovery fixtures can legitimately omit these tables.
        let self_author = if exists(conn, "self_identity")? {
            crate::claim_cache::self_author(conn)?
        } else {
            None
        };
        let names = if exists(conn, "member_name")? {
            let mut statement =
                conn.prepare("SELECT author,display_name FROM member_name ORDER BY author")?;
            let rows = statement
                .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        } else {
            Vec::new()
        };
        let sharing_off = if exists(conn, "identity_log_state")? {
            conn.query_row(
                "SELECT state='disabled' FROM identity_log_state WHERE id=1",
                [],
                |row| row.get(0),
            )
            .optional()?
            .unwrap_or(true)
        } else {
            true
        };
        let agents = if exists(conn, "agent")? {
            let mut statement = conn.prepare("SELECT agent_id,terminal_reason,terminal_at_ms,merged_into FROM agent ORDER BY agent_id")?;
            let rows = statement
                .query_map([], |row| {
                    Ok(AgentLiveness {
                        id: row.get(0)?,
                        terminal: row.get(1)?,
                        terminal_at: row.get(2)?,
                        merged_into: row.get(3)?,
                    })
                })?
                .collect::<rusqlite::Result<Vec<_>>>()?;
            rows
        } else {
            Vec::new()
        };
        Ok(Self {
            self_author,
            names,
            sharing_off,
            agents,
        })
    }
}

impl RegistryStore {
    /// Install a process-local, nonblocking invalidator shared by store clones.
    /// It runs only after a successful commit and after the writer lock is
    /// released. The listener must not retain a strong reference to this store.
    pub fn set_where_change_listener(&self, listener: Arc<dyn Fn() + Send + Sync>) {
        *self.where_notifications.0.lock().unwrap() = Some(listener);
    }

    pub(crate) fn with_where_changes<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, StoreError> {
        self.where_write(false, action)
    }

    pub(crate) fn with_where_move_changes<T>(
        &self,
        action: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, StoreError> {
        self.where_write(true, action)
    }

    fn where_write<T>(
        &self,
        moves: bool,
        action: impl FnOnce(&Transaction<'_>) -> rusqlite::Result<T>,
    ) -> Result<T, StoreError> {
        let observing = self.where_notifications.0.lock().unwrap().is_some();
        let mut changed = false;
        let result = self.db.with_conn_fenced(|tx| {
            // Compare only the small dependency tables, not every whereabouts
            // entry. Reading before and after inside the transaction prevents a
            // rollback or another writer from inventing or hiding a change.
            let before = observing.then(|| Dependencies::read(tx)).transpose()?;
            let before_writes = tx.total_changes();
            let value = action(tx)?;
            changed = match before {
                Some(before) => {
                    (moves && tx.total_changes() != before_writes)
                        || before != Dependencies::read(tx)?
                }
                // A listener can attach while this write is in flight. Force
                // resync then, rather than losing the first observed commit.
                None => true,
            };
            Ok(value)
        });
        if result.is_ok() && changed {
            let listener = self.where_notifications.0.lock().unwrap().clone();
            if let Some(listener) = listener {
                listener();
            }
        }
        result
    }
}

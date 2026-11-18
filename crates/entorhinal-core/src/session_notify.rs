//! Keys of session bindings changed by a successful local commit.

use std::sync::{Arc, Mutex};

use rusqlite::{Connection, OptionalExtension};

use crate::RegistryStore;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct SessionChangeKey {
    pub agent_id: String,
    pub harness: String,
    pub session_id: String,
}

type Listener = Arc<dyn Fn(Vec<SessionChangeKey>) + Send + Sync>;

#[derive(Clone, Default)]
pub(crate) struct SessionNotifications(Arc<Mutex<Option<Listener>>>);

pub(crate) fn key_for_host(
    conn: &Connection,
    harness: &str,
    session_id: &str,
) -> rusqlite::Result<Option<SessionChangeKey>> {
    conn.query_row(
        "SELECT agent_id FROM session_binding WHERE harness=?1 AND session_id=?2",
        [harness, session_id],
        |row| {
            Ok(SessionChangeKey {
                agent_id: row.get(0)?,
                harness: harness.to_owned(),
                session_id: session_id.to_owned(),
            })
        },
    )
    .optional()
}

pub(crate) fn key_for_ref(
    conn: &Connection,
    kind: &str,
    reference: &str,
) -> rusqlite::Result<Option<SessionChangeKey>> {
    conn.query_row(
        "SELECT agent_id,harness,session_id FROM session_binding
         WHERE ref_kind=?1 AND ref=?2 AND harness IS NOT NULL AND session_id IS NOT NULL",
        [kind, reference],
        |row| {
            Ok(SessionChangeKey {
                agent_id: row.get(0)?,
                harness: row.get(1)?,
                session_id: row.get(2)?,
            })
        },
    )
    .optional()
}

impl RegistryStore {
    /// Read only the committed host keys a watch received, not the agent's
    /// whole session history. Missing and ended (for open-only reads) rows omit.
    pub fn changed_agent_sessions(
        &self,
        agent_id: &str,
        keys: &[(String, String)],
        open: bool,
    ) -> Result<Vec<crate::AgentSession>, crate::RegistryError> {
        self.read(|conn| {
            let mut statement = conn.prepare(
                "SELECT runner,session_id,harness,ref_kind,ref,NULLIF(project_root,''),
                        registered_at_ms,ended_at_ms,
                        COALESCE((SELECT network FROM session_network AS history
                                  WHERE history.ref_kind=session_binding.ref_kind
                                    AND history.ref=session_binding.ref
                                  ORDER BY since_ms DESC LIMIT 1), ?4),held_on,role
                 FROM session_binding WHERE agent_id=?1 AND harness=?2 AND session_id=?3
                   AND (NOT ?5 OR ended_at_ms IS NULL)",
            )?;
            let mut rows = Vec::new();
            for (harness, session) in keys {
                if let Some(row) = statement
                    .query_row(
                        rusqlite::params![agent_id, harness, session, crate::NETWORK_ABSENT, open],
                        crate::AgentSession::from_row,
                    )
                    .optional()?
                {
                    rows.push(row);
                }
            }
            rows.sort_by(|a, b| {
                b.registered_at_ms
                    .cmp(&a.registered_at_ms)
                    .then_with(|| a.ref_kind.cmp(&b.ref_kind))
                    .then_with(|| a.session_ref.cmp(&b.session_ref))
            });
            Ok(rows)
        })
    }

    /// Install a nonblocking listener shared by store clones. Calls happen only
    /// after a successful commit, outside the writer lock. Do not retain the store.
    pub fn set_session_change_listener(&self, listener: Listener) {
        *self.session_notifications.0.lock().unwrap() = Some(listener);
    }

    pub(crate) fn notify_session_changes(&self, mut keys: Vec<SessionChangeKey>) {
        keys.sort();
        keys.dedup();
        if !keys.is_empty() {
            let listener = self.session_notifications.0.lock().unwrap().clone();
            if let Some(listener) = listener {
                listener(keys);
            }
        }
    }
}

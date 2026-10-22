//! Store provenance is machine-local. Adoption that clears state records that
//! reset in the journal, so replay cannot resurrect the previous Mac's approvals.
use crate::{RegistryError, RegistryStore};
use rusqlite::{params, OptionalExtension, Transaction};
use std::sync::atomic::Ordering;

pub(crate) const V14_STORE_ORIGIN: &str =
    "CREATE TABLE store_origin(machine_id TEXT NOT NULL, recorded_at_ms INTEGER NOT NULL);";

/// Clear journal-derived local authority. A previously shared registry must be
/// empty to rejoin the log, so that form also resets the shared projections.
pub(crate) fn clear_local_projections(
    tx: &Transaction<'_>,
    reset_shared: bool,
) -> rusqlite::Result<()> {
    if reset_shared {
        tx.execute_batch("PRAGMA defer_foreign_keys=ON;")?;
        for table in crate::mutations::DERIVED_TABLES {
            tx.execute(&format!("DELETE FROM {table}"), [])?;
        }
        return Ok(());
    }
    tx.execute_batch(
        "DELETE FROM root_approval;
        DELETE FROM root_binding;
        DELETE FROM retired_binding;
        DELETE FROM workspace_root;
        DELETE FROM root_owned_remotes;
        UPDATE derived_root_parent SET source_root=NULL, registration_epoch=NULL;",
    )
}

/// Non-journaled observations, authority and execution checkpoints. Child rows
/// precede their parents. Shared catalog pins are deliberately not included.
pub(crate) const LOCAL_TABLES: &[&str] = &[
    "self_identity",
    "claim_cache",
    "role_cache",
    "role_take_request",
    "agent_move",
    "move_setting",
    "session_network",
    "session_binding",
    "pending_entry",
    "consent_write_plan",
    "consent_request",
    "consent_feed_cursor",
    "consent_grant",
    "agent_audit",
    "project_hire_cap",
    "catalog_generation",
    "catalog_prepared",
    "catalog_plan",
    "catalog_transition",
    "catalog_starter_proof",
    "catalog_status",
    "catalog_feed",
    "catalog_scope_map",
    "catalog_registry_key",
    "catalog_registry",
    "catalog_publisher_grant",
    "catalog_audit",
];

impl RegistryStore {
    pub fn is_foreign(&self) -> bool {
        self.foreign.load(Ordering::Acquire)
    }

    /// Compare before any startup work. An absent daemon id never invents an
    /// origin or changes an existing one. Returns the recorded store id.
    pub fn check_store_origin(
        &self,
        daemon: Option<&str>,
        now: i64,
    ) -> Result<Option<String>, RegistryError> {
        let origin = self
            .db
            .with_conn_fenced(|tx| {
                let origin: Option<String> = tx
                    .query_row("SELECT machine_id FROM store_origin", [], |r| r.get(0))
                    .optional()?;
                if origin.is_none() {
                    if let Some(id) = daemon {
                        tx.execute("INSERT INTO store_origin VALUES(?1,?2)", params![id, now])?;
                        return Ok(Some(id.to_owned()));
                    }
                }
                Ok(origin)
            })
            .map_err(RegistryError::Store)?;
        self.foreign.store(
            daemon.zip(origin.as_deref()).is_some_and(|(a, b)| a != b),
            Ordering::Release,
        );
        Ok(origin)
    }

    /// Called only after module admission and operator confirmation. Unless
    /// `same_machine` is set, the adoption journal entry and state clearing
    /// commit atomically with the new provenance. The journal itself is kept.
    pub fn adopt_store(
        &self,
        daemon: &str,
        same_machine: bool,
        now: i64,
    ) -> Result<(), RegistryError> {
        let adopted = self.db.with_conn_fenced(|tx| {
            let origin: Option<String> = tx.query_row("SELECT machine_id FROM store_origin", [], |r| r.get(0)).optional()?;
            let Some(origin) = origin.filter(|id| id != daemon) else { return Ok(false); };
            if !same_machine {
                let reset_shared: bool = tx.query_row("SELECT state<>'disabled' FROM identity_log_state WHERE id=1", [], |r|r.get(0))?;
                for table in LOCAL_TABLES {
                    tx.execute(&format!("DELETE FROM {table}"), [])?;
                }
                clear_local_projections(tx, reset_shared)?;
                tx.execute("INSERT INTO move_setting(id,lid_close_moves) VALUES(1,0)", [])?;
                // This table is created lazily by the catalog poller.
                let poll_clock = tx.query_row("SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type='table' AND name='catalog_poll_clock')", [], |r|r.get::<_,bool>(0))?;
                if poll_clock { tx.execute("DELETE FROM catalog_poll_clock", [])?; }
                tx.execute("UPDATE identity_log_state SET state='disabled',last_applied_position=0,last_seen_head=0,enable_parts=NULL,enable_backfill=NULL WHERE id=1", [])?;
                tx.execute("INSERT INTO registry_journal(op,payload_json,actor,created_at,principal,stream,origin) VALUES('store.adopt',?1,'operator',?2,'direct','local','here')", params![serde_json::json!({"storeMachineId":origin,"daemonMachineId":daemon,"resetShared":reset_shared}).to_string(),now])?;
            }
            tx.execute("DELETE FROM store_origin", [])?;
            tx.execute("INSERT INTO store_origin VALUES(?1,?2)", params![daemon,now])?;
            Ok(true)
        }).map_err(RegistryError::Store)?;
        if !adopted {
            return Err(RegistryError::Domain {
                code: "store_not_foreign".into(),
                message: "store does not belong to another machine".into(),
                detail: None,
            });
        }
        self.foreign.store(false, Ordering::Release);
        Ok(())
    }
}

//! Background upkeep of session bindings: writing the end times the liveness
//! feed reports, and pruning bindings whose session ended long ago.
//!
//! The liveness feed arrives several times a second and must never wait on
//! SQLite, so a `gone` report only appends to an in-memory queue and wakes
//! this task. The task drains everything queued so far and writes it in one
//! transaction on the blocking pool. A failed write is counted and logged once
//! per kind; it never reaches the feed's reply.
//!
//! Like claim refresh, the task holds the handler's store slot weakly and
//! takes a store handle only for one blocking job, so dropping the handler or
//! taking its store out closes the database. It never runs on a store from
//! another Mac, which the central write gate keeps read-only.

use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex, Weak,
};

use entorhinal_core::{GoneSession, RegistryStore};
use tokio::time::{Duration, Instant};

use super::{move_driver::SqlOperations, ProjectsHandler};

/// The first retention pass waits this long after the task starts, so it
/// never competes with startup work.
pub(super) const RETENTION_FIRST_DELAY: Duration = Duration::from_secs(60);
/// Later retention passes run this often.
pub(super) const RETENTION_INTERVAL: Duration = Duration::from_secs(60 * 60);
/// Most end stamps held while the writer is behind. Beyond it a stamp is
/// dropped and counted; the session then keeps no end time, which only delays
/// its pruning and never makes a live session look ended.
const PENDING_CAP: usize = 8192;

/// Why a stamp or a retention pass did not land. Each kind is logged the
/// first time it happens; every stamp that fails to land is also counted.
#[derive(Clone, Copy, Debug)]
enum Failure {
    QueueFull,
    StoreUnavailable,
    ForeignStore,
    StampWrite,
    Prune,
}

impl Failure {
    const COUNT: usize = 5;

    fn label(self) -> &'static str {
        match self {
            Self::QueueFull => "queue_full",
            Self::StoreUnavailable => "store_unavailable",
            Self::ForeignStore => "store_from_another_machine",
            Self::StampWrite => "stamp_write_failed",
            Self::Prune => "prune_failed",
        }
    }
}

/// The queue between the liveness feed and the writer task, and the task's
/// counters. Every counter is an atomic so the health report reads them
/// without a lock.
#[derive(Default)]
pub(super) struct SessionUpkeep {
    pending: Mutex<Vec<GoneSession>>,
    wake: tokio::sync::Notify,
    pub(super) stamped: AtomicU64,
    pub(super) stamp_failures: AtomicU64,
    pub(super) pruned: AtomicU64,
    logged: [AtomicBool; Failure::COUNT],
    #[cfg(test)]
    pub(super) stamps_done: tokio::sync::Notify,
    #[cfg(test)]
    pub(super) prune_done: tokio::sync::Notify,
}

impl SessionUpkeep {
    /// Queue end stamps without touching SQLite. The lock is held only to
    /// append, never while a write runs.
    pub(super) fn push(&self, stamps: Vec<GoneSession>) {
        if stamps.is_empty() {
            return;
        }
        let mut pending = self.pending.lock().unwrap_or_else(|e| e.into_inner());
        let room = PENDING_CAP.saturating_sub(pending.len());
        let dropped = stamps.len().saturating_sub(room);
        pending.extend(stamps.into_iter().take(room));
        drop(pending);
        if dropped > 0 {
            self.fail(
                Failure::QueueFull,
                dropped as u64,
                "the end-stamp queue is full",
            );
        }
        // A wake sent while the writer is busy is kept as one permit, so the
        // writer drains this push on its next pass.
        self.wake.notify_one();
    }

    /// Add this task's counters to the health metrics. Atomics only, so the
    /// health report never waits for the store or the queue.
    pub(super) fn metrics(&self, mut metrics: serde_json::Value) -> serde_json::Value {
        metrics["sessionEndStamps"] = self.stamped.load(Ordering::Relaxed).into();
        metrics["sessionEndStampFailures"] = self.stamp_failures.load(Ordering::Relaxed).into();
        metrics["sessionsPruned"] = self.pruned.load(Ordering::Relaxed).into();
        metrics
    }

    fn take(&self) -> Vec<GoneSession> {
        std::mem::take(&mut *self.pending.lock().unwrap_or_else(|e| e.into_inner()))
    }

    fn fail(&self, kind: Failure, stamps: u64, detail: &str) {
        if !matches!(kind, Failure::Prune) {
            self.stamp_failures.fetch_add(stamps, Ordering::Relaxed);
        }
        if !self.logged[kind as usize].swap(true, Ordering::Relaxed) {
            tracing::warn!(
                target: "sessions",
                kind = kind.label(),
                detail,
                "session binding upkeep failed; later failures of this kind are counted, not logged"
            );
        }
    }
}

struct Worker {
    store: Weak<Mutex<Option<RegistryStore>>>,
    upkeep: Arc<SessionUpkeep>,
    sql: Arc<SqlOperations>,
    clock: fn() -> i64,
}

impl Worker {
    /// The handler's current store, or `None` once the handler or its store
    /// is gone.
    fn store(&self) -> Option<RegistryStore> {
        self.store
            .upgrade()?
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    async fn run(self) {
        let mut next_prune = Instant::now() + RETENTION_FIRST_DELAY;
        loop {
            tokio::select! {
                () = self.upkeep.wake.notified() => self.write_stamps().await,
                () = tokio::time::sleep_until(next_prune) => {
                    self.prune().await;
                    next_prune = Instant::now() + RETENTION_INTERVAL;
                }
            }
        }
    }

    /// Write every stamp queued so far in one transaction.
    async fn write_stamps(&self) {
        let stamps = self.upkeep.take();
        if !stamps.is_empty() {
            let count = stamps.len() as u64;
            match self.store() {
                None => self.upkeep.fail(
                    Failure::StoreUnavailable,
                    count,
                    "projects storage is not ready",
                ),
                Some(store) if store.is_foreign() => self.upkeep.fail(
                    Failure::ForeignStore,
                    count,
                    "the store belongs to another Mac",
                ),
                Some(store) => {
                    let guard = self.sql.begin();
                    let written = tokio::task::spawn_blocking(move || {
                        let _guard = guard;
                        store.stamp_gone_sessions(&stamps)
                    })
                    .await;
                    match written {
                        Ok(Ok(stamped)) => {
                            self.upkeep
                                .stamped
                                .fetch_add(stamped as u64, Ordering::Relaxed);
                        }
                        Ok(Err(error)) => {
                            self.upkeep
                                .fail(Failure::StampWrite, count, &error.to_string())
                        }
                        Err(error) => {
                            self.upkeep
                                .fail(Failure::StampWrite, count, &error.to_string())
                        }
                    }
                }
            }
        }
        #[cfg(test)]
        self.upkeep.stamps_done.notify_waiters();
    }

    /// Delete bindings that ended past the retention window.
    async fn prune(&self) {
        if let Some(store) = self.store().filter(|store| !store.is_foreign()) {
            let now = (self.clock)();
            let guard = self.sql.begin();
            let pruned = tokio::task::spawn_blocking(move || {
                let _guard = guard;
                store.prune_ended_sessions(now)
            })
            .await;
            match pruned {
                Ok(Ok(pruned)) => {
                    self.upkeep
                        .pruned
                        .fetch_add(pruned as u64, Ordering::Relaxed);
                }
                Ok(Err(error)) => self.upkeep.fail(Failure::Prune, 0, &error.to_string()),
                Err(error) => self.upkeep.fail(Failure::Prune, 0, &error.to_string()),
            }
        }
        #[cfg(test)]
        self.upkeep.prune_done.notify_waiters();
    }
}

impl ProjectsHandler {
    /// Start the upkeep task once. A store from another Mac gets none; store
    /// adoption starts it.
    pub(super) fn start_session_upkeep(&self) {
        if self.health.origin.is_foreign() {
            return;
        }
        let mut task = self
            .session_upkeep_task
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if task.is_some() {
            return;
        }
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            upkeep: self.session_upkeep.clone(),
            sql: self.session_upkeep_sql.clone(),
            clock: self.clock,
        };
        *task = Some(tokio::spawn(worker.run()));
    }

    /// Stop the task and wait for any blocking job still holding a store
    /// handle. Tests call this before reopening the store or deleting its
    /// folder, which Windows refuses while a file is open.
    #[cfg(test)]
    pub(super) async fn stop_session_upkeep(&self) {
        let task = self
            .session_upkeep_task
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.session_upkeep_sql.wait().await;
    }
}

#[cfg(test)]
#[path = "session_upkeep_tests.rs"]
mod tests;

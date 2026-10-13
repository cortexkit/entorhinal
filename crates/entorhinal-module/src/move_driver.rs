//! Machine-local move tasks. Remote calls never own a store handle or the writer.

use crate::{
    door_client::DoorClient,
    move_receiver::{poll_inbox, CutAccept, Receiver},
    move_source::Source,
    participant_discovery::ParticipantDiscovery,
    record_client::RecordClient,
    HandlerError, HealthGauges, ProjectsHandler,
};
use cortexkit_role_agent_data::{
    errors::{driver_action_for, DriverAction, ErrorBody, RefusalCode},
    Op,
};
use entorhinal_core::{move_store::AgentMove, RegistryError, RegistryStore};
use std::{
    collections::HashSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, Weak,
    },
    time::Duration,
};

pub(crate) const POLL: Duration = Duration::from_secs(30);

pub(crate) fn participant_action(op: &str, code: &str) -> Option<DriverAction> {
    Op::ALL
        .into_iter()
        .find(|candidate| candidate.method() == op)
        .map(|op| driver_action_for(op, &RefusalCode::from_code(code)))
}

pub(crate) fn digest_reason(code: &str, detail: Option<&serde_json::Value>) -> Option<String> {
    let mut body = ErrorBody::new(RefusalCode::from_code(code), "");
    body.detail = detail.cloned();
    body.digest_mismatch_detail().ok().flatten()?.reason
}

#[cfg(test)]
pub(crate) fn digest_refusal(op: &str) -> crate::participant_client::Refusal {
    let vectors: serde_json::Value = serde_json::from_str(
        cortexkit_role_agent_data::vectors::file("digest-mismatch.json").unwrap(),
    )
    .unwrap();
    let vector = vectors["refusals"]
        .as_array()
        .unwrap()
        .iter()
        .find(|v| v["op"] == op)
        .unwrap();
    let body: ErrorBody = serde_json::from_value(vector["body"].clone()).unwrap();
    crate::participant_client::Refusal {
        code: body.code.as_str().into(),
        detail: body.detail,
    }
}

#[derive(Default)]
pub(crate) struct SqlOperations {
    count: AtomicUsize,
    idle: tokio::sync::Notify,
    #[cfg(test)]
    waiting: tokio::sync::Notify,
}

impl SqlOperations {
    pub(crate) fn begin(self: &Arc<Self>) -> SqlGuard {
        self.count.fetch_add(1, Ordering::AcqRel);
        SqlGuard(self.clone())
    }

    pub(crate) async fn wait(&self) {
        #[cfg(test)]
        self.waiting.notify_one();
        loop {
            let idle = self.idle.notified();
            tokio::pin!(idle);
            idle.as_mut().enable();
            if self.count.load(Ordering::Acquire) == 0 {
                return;
            }
            idle.await;
        }
    }
}

pub(crate) struct SqlGuard(Arc<SqlOperations>);

impl Drop for SqlGuard {
    fn drop(&mut self) {
        if self.0.count.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.idle.notify_waiters();
        }
    }
}

pub(crate) struct MoveDriver {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
    sql: Arc<SqlOperations>,
}

pub(crate) struct ReconnectConnector {
    pub(crate) inner: Arc<dyn crate::log_client::LogConnector>,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

#[async_trait::async_trait]
impl crate::log_client::LogConnector for ReconnectConnector {
    async fn connect(
        &self,
    ) -> Result<Arc<dyn crate::log_client::LogTransport>, crate::log_client::TransportError> {
        let transport = self.inner.connect().await?;
        self.wake.notify_waiters();
        self.wake.notify_one();
        Ok(transport)
    }
}

#[derive(Clone)]
pub(crate) struct Worker {
    pub(crate) store: Weak<Mutex<Option<RegistryStore>>>,
    pub(crate) sql_operations: Arc<SqlOperations>,
    pub(crate) records: RecordClient,
    pub(crate) door: DoorClient,
    pub(crate) discovery: Arc<dyn ParticipantDiscovery>,
    pub(crate) health: Arc<HealthGauges>,
    #[cfg(test)]
    pub(crate) writer: Arc<tokio::sync::Mutex<()>>,
    pub(crate) clock: fn() -> i64,
    pub(crate) wake: Arc<tokio::sync::Notify>,
}

impl Worker {
    pub(crate) async fn sql<T: Send + 'static>(
        &self,
        operation: impl FnOnce(&RegistryStore) -> Result<T, RegistryError> + Send + 'static,
    ) -> Result<T, HandlerError> {
        let slot = self.store.clone();
        // Count queued work too: aborting its async caller cannot stop a blocking closure.
        let guard = self.sql_operations.begin();
        tokio::task::spawn_blocking(move || {
            // The store is dropped before this guard, including when the operation panics.
            let _guard = guard;
            let store = slot
                .upgrade()
                .and_then(|slot| slot.lock().unwrap().clone())
                .ok_or_else(|| HandlerError::new("storage_unavailable", "move store is gone"))?;
            operation(&store).map_err(HandlerError::from)
        })
        .await
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?
    }

    pub(crate) async fn row(&self, cut: &str) -> Result<AgentMove, HandlerError> {
        let cut = cut.to_owned();
        self.sql(move |s| s.agent_move(&cut))
            .await?
            .ok_or_else(|| HandlerError::new("invalid_state", "move row is gone"))
    }

    pub(crate) async fn consume_abort_retry(&self, cut: &str) -> Result<bool, HandlerError> {
        let cut = cut.to_owned();
        self.sql(move |store| store.consume_move_abort_retry(&cut))
            .await
    }

    /// Serialize checkpoints with local operator edits in the fenced writer,
    /// without waiting for journal catch-up's shared writer lock.
    pub(crate) async fn save(
        &self,
        row: &mut AgentMove,
        clear_abort: bool,
    ) -> Result<(), HandlerError> {
        let mut next = row.clone();
        next.updated_at_ms = (self.clock)();
        if clear_abort {
            next.abort_requested = None;
        }
        let saved = self
            .sql(move |s| s.checkpoint_agent_move(&next, clear_abort))
            .await?;
        *row = saved;
        self.gauges().await
    }

    pub(crate) async fn gauges(&self) -> Result<(), HandlerError> {
        let rows = self.sql(RegistryStore::list_agent_moves).await?;
        self.health.moves_open.store(
            rows.iter().filter(|r| !r.step.is_terminal()).count() as u64,
            Ordering::Relaxed,
        );
        let errors = rows
            .iter()
            .map(|r| {
                let diagnostics = ["_source", "_receiver"]
                    .iter()
                    .map(|key| {
                        r.plan
                            .get(key)
                            .and_then(|s| s.get("errors"))
                            .and_then(serde_json::Value::as_array)
                            .map_or(0, Vec::len)
                    })
                    .sum::<usize>();
                diagnostics
                    + usize::from(
                        r.cut_accept
                            .as_ref()
                            .is_some_and(|v| v["state"] != "accepted"),
                    )
                    + usize::from(r.hold.as_ref().is_some_and(|v| v["move_error"] == true))
            })
            .sum::<usize>();
        self.health
            .move_errors
            .store(errors as u64, Ordering::Relaxed);
        Ok(())
    }
}

impl ProjectsHandler {
    pub(crate) fn start_move_driver(&self) {
        let mut task = self.move_driver_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let sql = Arc::new(SqlOperations::default());
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            sql_operations: sql.clone(),
            records: self.record_client.clone(),
            door: self.door_client.clone(),
            discovery: self.participant_discovery.clone(),
            health: self.health.clone(),
            #[cfg(test)]
            writer: self.writer.clone(),
            clock: self.clock,
            wake: self.move_wake.clone(),
        };
        let liveness = self.liveness.clone();
        let wake = self.move_wake.clone();
        let (stop, stopped) = tokio::sync::oneshot::channel();
        let driver = tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut owners = HashSet::new();
            let mut tasks = tokio::task::JoinSet::new();
            let poll = async {
                loop {
                    tokio::select! {
                        _ = interval.tick() => {},
                        _ = wake.notified() => {},
                        Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                            if let Ok(owner) = result { owners.remove(&owner); }
                        }
                    }
                    if worker.store.upgrade().is_none() {
                        break;
                    }
                    let _ = poll_inbox(&worker).await;
                    let Ok(rows) = worker.sql(RegistryStore::list_open_agent_moves).await else {
                        continue;
                    };
                    let _ = worker.gauges().await;
                    for row in rows {
                        if owners.insert((row.cut_id.clone(), false)) {
                            let worker = worker.clone();
                            let liveness = liveness.clone();
                            tasks.spawn(async move {
                                let cut = row.cut_id;
                                match row.side {
                                    entorhinal_core::move_store::MoveSide::Source => {
                                        Source::new(worker, cut.clone()).run().await
                                    }
                                    entorhinal_core::move_store::MoveSide::Receiver => {
                                        Receiver::new(worker, cut.clone(), liveness).run().await
                                    }
                                }
                                (cut, false)
                            });
                        }
                    }
                    if let Ok(rows) = worker.sql(RegistryStore::list_pending_cut_accepts).await {
                        for row in rows {
                            if owners.insert((row.cut_id.clone(), true)) {
                                let worker = worker.clone();
                                tasks.spawn(async move {
                                    let cut = row.cut_id;
                                    CutAccept::new(worker, cut.clone()).run().await;
                                    (cut, true)
                                });
                            }
                        }
                    }
                }
            };
            tokio::select! {
                _ = stopped => {},
                _ = poll => {},
            }
            tasks.shutdown().await;
        });
        *task = Some(MoveDriver {
            stop,
            task: driver,
            sql,
        });
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) async fn stop_move_driver(&self) {
        let driver = self.move_driver_task.lock().unwrap().take();
        if let Some(driver) = driver {
            let _ = driver.stop.send(());
            let _ = driver.task.await;
            // Draining async tasks does not release stores held by spawn_blocking work.
            driver.sql.wait().await;
        }
    }
}

pub(crate) fn backoff(attempts: i64) -> Duration {
    Duration::from_secs(
        1u64.checked_shl(attempts.saturating_sub(1).clamp(0, 9) as u32)
            .unwrap_or(300)
            .min(300),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{future::Future, task::Poll};

    #[tokio::test]
    async fn stop_move_driver_waits_for_blocking_sql_before_store_reopen() {
        let (dir, descriptor) = crate::tests::scratch_descriptor("move-driver-stop");
        let mut handler = ProjectsHandler::with_runtime("sql-drain".into(), || 0);
        handler.record_client = RecordClient::new(Arc::new(
            crate::fake_record::FakeRecordConnector(crate::fake_record::FakeRecord::new([1; 16])),
        ));
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        handler.start_move_driver();
        let sql = handler
            .move_driver_task
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .sql
            .clone();
        let worker = Worker {
            store: Arc::downgrade(&handler.store),
            sql_operations: sql.clone(),
            records: handler.record_client.clone(),
            door: handler.door_client.clone(),
            discovery: handler.participant_discovery.clone(),
            health: handler.health.clone(),
            writer: handler.writer.clone(),
            clock: handler.clock,
            wake: handler.move_wake.clone(),
        };
        let (entered, running) = tokio::sync::oneshot::channel();
        let (release, gate) = std::sync::mpsc::channel();
        let blocked_worker = worker.clone();
        let operation = tokio::spawn(async move {
            blocked_worker
                .sql(move |store| {
                    entered.send(()).unwrap();
                    gate.recv().unwrap();
                    store.list_open_agent_moves()
                })
                .await
        });
        running.await.unwrap();
        operation.abort();
        assert!(operation.await.unwrap_err().is_cancelled());

        let mut stop = Box::pin(handler.stop_move_driver());
        let stopped_early = tokio::select! {
            _ = &mut stop => true,
            _ = sql.waiting.notified() => {
                // Poll after the driver and its move tasks have drained, not merely after stop begins.
                std::future::poll_fn(|cx| Poll::Ready(stop.as_mut().poll(cx).is_ready())).await
            }
        };
        // Release the blocking thread even if shutdown returned early, so a red test cannot hang.
        release.send(()).unwrap();
        if stopped_early {
            sql.wait().await;
        } else {
            stop.await;
        }
        assert!(
            !stopped_early,
            "stop_move_driver returned while blocking SQL still held the writer lease"
        );

        assert_eq!(
            worker
                .sql::<()>(|_| panic!("SQL panic exercises the drop guard"))
                .await
                .unwrap_err()
                .code,
            "storage_error"
        );
        assert_eq!(sql.count.load(Ordering::Acquire), 0);

        *handler.store.lock().unwrap() = None;
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        handler.start_move_driver();
        handler.stop_move_driver().await;
        handler.stop_move_driver().await;
        *handler.store.lock().unwrap() = None;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

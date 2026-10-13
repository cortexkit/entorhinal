//! Machine-local move tasks. Remote calls never own a store handle or the writer.

use crate::{
    door_client::DoorClient,
    move_receiver::{poll_inbox, Receiver},
    move_source::Source,
    participant_discovery::ParticipantDiscovery,
    record_client::RecordClient,
    HandlerError, HealthGauges, ProjectsHandler,
};
use entorhinal_core::{move_store::AgentMove, RegistryError, RegistryStore};
use std::{
    collections::HashSet,
    sync::{atomic::Ordering, Arc, Mutex, Weak},
    time::Duration,
};

pub(crate) const POLL: Duration = Duration::from_secs(30);

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
    pub(crate) records: RecordClient,
    pub(crate) door: DoorClient,
    pub(crate) discovery: Arc<dyn ParticipantDiscovery>,
    pub(crate) health: Arc<HealthGauges>,
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
        tokio::task::spawn_blocking(move || {
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

    /// Serialize checkpoints with local operator edits. Preserve an abort that
    /// arrived while the remote call was in flight unless the source is already
    /// releasing or the receiver has advanced beyond published.
    pub(crate) async fn save(
        &self,
        row: &mut AgentMove,
        clear_abort: bool,
    ) -> Result<(), HandlerError> {
        let _writer = self.writer.lock().await;
        let mut next = row.clone();
        next.updated_at_ms = (self.clock)();
        if clear_abort {
            next.abort_requested = None;
        }
        let saved = self
            .sql(move |s| {
                if let Some(current) = s.agent_move(&next.cut_id)? {
                    if !clear_abort && current.abort_requested.is_some() {
                        next.abort_requested = current.abort_requested;
                    }
                }
                s.update_agent_move(&next)?;
                Ok(next)
            })
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
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            records: self.record_client.clone(),
            door: self.door_client.clone(),
            discovery: self.participant_discovery.clone(),
            health: self.health.clone(),
            writer: self.writer.clone(),
            clock: self.clock,
            wake: self.move_wake.clone(),
        };
        let liveness = self.liveness.clone();
        let wake = self.move_wake.clone();
        *task = Some(tokio::spawn(async move {
            let mut interval = tokio::time::interval(POLL);
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            let mut owners = HashSet::new();
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    _ = interval.tick() => {},
                    _ = wake.notified() => {},
                    Some(result) = tasks.join_next(), if !tasks.is_empty() => {
                        if let Ok(cut) = result { owners.remove(&cut); }
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
                    if owners.insert(row.cut_id.clone()) {
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
                            cut
                        });
                    }
                }
            }
        }));
    }
}

pub(crate) fn backoff(attempts: i64) -> Duration {
    Duration::from_secs(
        1u64.checked_shl(attempts.saturating_sub(1).clamp(0, 9) as u32)
            .unwrap_or(300)
            .min(300),
    )
}

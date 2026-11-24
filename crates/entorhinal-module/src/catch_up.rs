//! Starts the reader that applies new hub log entries to an installed store,
//! or finishes enabling or joining the hub before that reader can start.

use super::{HandlerError, HealthGauges, ProjectsHandler};
use std::sync::{atomic::Ordering, Arc};
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

const INTERVAL: Duration = Duration::from_secs(30);
// The probe asks ck-bus for a hub credential, and every probe is refused when ck-bus is disabled.
const PROBE_BACKOFF_START: Duration = Duration::from_secs(30);
const PROBE_BACKOFF_MAX: Duration = Duration::from_secs(15 * 60);
pub(super) const READ_DEADLINE: Duration = Duration::from_secs(25);

struct Worker {
    health: Arc<HealthGauges>,
    hub_switch: super::hub_switch::Switch,
    hub_runtime: super::hub_switch::Runtime,
    wake: Arc<tokio::sync::Notify>,
    writer: Arc<tokio::sync::Mutex<()>>,
}

impl ProjectsHandler {
    pub(super) fn start_catch_up(&self) {
        if self.health.origin.is_foreign() {
            return;
        }
        let Some(switch) = &self.hub_switch else {
            return;
        };
        let mut task = self.catch_up_task.lock().unwrap();
        if task.is_none() {
            let worker = Worker {
                health: self.health.clone(),
                hub_switch: switch.clone(),
                hub_runtime: super::hub_switch::Runtime::new(self),
                wake: self.catch_up_wake.clone(),
                writer: self.writer.clone(),
            };
            *task = Some(tokio::spawn(async move { worker.background().await }));
        }
    }
}

impl Drop for ProjectsHandler {
    fn drop(&mut self) {
        if let Some(task) = self.liveness_bus_task.get_mut().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.bus_changes_task.get_mut().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.catch_up_task.get_mut().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.hub_tail_task.lock().unwrap().take() {
            task.abort();
        }
        if let Some(task) = self.session_upkeep_task.get_mut().unwrap().take() {
            task.abort();
        }
    }
}

impl Worker {
    async fn background(self) {
        let mut switch_error = None;
        let mut gate_error = None;
        let mut ticks = tokio::time::interval(INTERVAL);
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut hub_retry = Some(Instant::now());
        let mut backoff = Duration::from_secs(1);
        let mut next_probe = None;
        let mut probe_backoff = PROBE_BACKOFF_START;
        loop {
            let installed = self.health.hub_membership.load(Ordering::Acquire) != 0;
            tokio::select! {
                _ = ticks.tick(), if !installed => {},
                _ = self.wake.notified() => {},
                _ = async { tokio::time::sleep_until(hub_retry.expect("armed retry")).await },
                    if installed && hub_retry.is_some() => {},
            }
            let deadline = Instant::now() + READ_DEADLINE;
            let Ok(_writer) = timeout_at(deadline, self.writer.lock()).await else {
                hub_retry = Some(Instant::now() + backoff);
                continue;
            };
            let sql = &self.hub_runtime.sql;
            if !sql.available() {
                break;
            }
            let backend = sql
                .run(deadline, "hub_startup_state", |store| {
                    Ok((store.is_foreign(), store.hub_log_base()?))
                })
                .await;
            let Ok((false, base)) = backend else {
                continue;
            };
            let switch = &self.hub_switch;
            if let Some(base) = base {
                let result = switch
                    .start_installed(&self.hub_runtime, base, deadline)
                    .await;
                log_switch_result(&mut switch_error, &result);
                if result.is_ok() {
                    self.health.hub_switch_state.store(0, Ordering::Relaxed);
                    break;
                }
                hub_retry = Some(Instant::now() + backoff);
                backoff = (backoff * 2).min(Duration::from_secs(60));
                continue;
            }
            let (planned, state) = match sql
                .run(deadline, "hub_catch_up_state", |store| {
                    Ok((
                        store.hub_enable_plan()?.is_some(),
                        store.identity_log_status()?.state,
                    ))
                })
                .await
            {
                Ok(state) => state,
                Err(error) => {
                    log_switch_result(&mut switch_error, &Err::<(), _>(error));
                    continue;
                }
            };
            if planned {
                let result = switch.finish(&self.hub_runtime, deadline).await;
                self.observe_switch(&result);
                if result.is_ok() {
                    self.wake.notify_one();
                }
                log_switch_result(&mut switch_error, &result);
                continue;
            }
            if next_probe.is_some_and(|due| Instant::now() < due) {
                continue;
            }
            let discovery_deadline = deadline.min(Instant::now() + Duration::from_secs(5));
            let head = switch.head(discovery_deadline).await;
            log_switch_result(&mut switch_error, &head);
            let head = match head {
                Ok(head) => {
                    next_probe = None;
                    probe_backoff = PROBE_BACKOFF_START;
                    head
                }
                Err(_) => {
                    next_probe = Some(Instant::now() + probe_backoff);
                    probe_backoff = (probe_backoff * 2).min(PROBE_BACKOFF_MAX);
                    continue;
                }
            };
            if state == "disabled" {
                let result = if head > 0 {
                    switch.finish(&self.hub_runtime, deadline).await
                } else {
                    Err(super::hub_switch::waiting())
                };
                self.observe_switch(&result);
                if result.is_ok() {
                    self.wake.notify_one();
                }
                log_switch_result(&mut gate_error, &result);
            }
        }
    }

    fn observe_switch<T>(&self, result: &Result<T, HandlerError>) {
        let state = match result.as_ref().err().map(|error| error.code.as_str()) {
            Some("hub_switch_waiting") => 1,
            Some("hub_migration_mismatch") => 2,
            _ => 0,
        };
        self.health.hub_switch_state.store(state, Ordering::Relaxed);
    }
}

fn log_switch_result<T>(last: &mut Option<(String, String)>, result: &Result<T, HandlerError>) {
    let current = result
        .as_ref()
        .err()
        .map(|error| (error.code.clone(), error.message.clone()));
    if current != *last {
        if let Some((code, message)) = &current {
            tracing::warn!(target: "identity_log", "switch gate: {code}: {message}");
        }
        *last = current;
    }
}

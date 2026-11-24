//! Enable the native hub log, or join an already bootstrapped hub.

use super::*;
use tokio::time::{timeout_at, Instant};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EnableParams {
    #[serde(default)]
    without_agents: bool,
}

pub(super) fn refresh(health: &HealthGauges, store: &RegistryStore) {
    if let Ok(status) = store.identity_log_status() {
        refresh_status(health, &status);
    }
}

pub(super) fn refresh_status(health: &HealthGauges, status: &entorhinal_core::IdentityLogStatus) {
    health.log_mode.store(
        match status.state.as_str() {
            "enabling" => 1,
            "enabled" => 3,
            _ => 0,
        },
        Ordering::Relaxed,
    );
    health
        .log_applied
        .store(status.last_applied_position, Ordering::Relaxed);
    health
        .log_head
        .store(status.last_seen_head, Ordering::Relaxed);
    health
        .log_pending
        .store(status.pending_write_count, Ordering::Relaxed);
}

pub(super) fn status_state(health: &HealthGauges) -> &'static str {
    match health.hub_switch_state.load(Ordering::Relaxed) {
        1 => return "hub_switch_waiting",
        2 => return "hub_migration_mismatch",
        _ => (),
    }
    let mode = health.log_mode.load(Ordering::Relaxed);
    if mode == 1 {
        return "enabling";
    }
    let error = health.log_state.load(Ordering::Relaxed);
    if error != 0 {
        return shared_write::health_state(error);
    }
    if mode == 3 {
        "enabled"
    } else {
        "disabled"
    }
}

fn error_name(code: u64) -> Option<&'static str> {
    match code {
        0 => None,
        9 => Some("engram_outcome_unknown"),
        _ => Some(shared_write::health_state(code)),
    }
}
pub(super) fn last_error(health: &HealthGauges) -> Option<&'static str> {
    error_name(health.log_error.load(Ordering::Relaxed))
}
pub(super) fn last_error_at(health: &HealthGauges) -> Option<i64> {
    let time = health.log_error_at.load(Ordering::Relaxed);
    (time != 0).then_some(time)
}
fn record_error(health: &HealthGauges, error: &HandlerError) {
    let code = match error.code.as_str() {
        "identity_log_invariant" => 3,
        "identity_log_stalled" => health.log_state.load(Ordering::Relaxed),
        _ => return,
    };
    health.log_error.store(code, Ordering::Relaxed);
    health.log_error_at.store(unix_millis(), Ordering::Relaxed);
}

impl ProjectsHandler {
    pub(super) async fn enable_log(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit("identity_log.enable", key)?;
        let params: EnableParams = serde_json::from_value(params)
            .map_err(|error| HandlerError::new("invalid_request", error.to_string()))?;
        let switch = self.hub_switch.as_ref().ok_or_else(|| {
            HandlerError::new("hub_not_ready", "hub identity log is not available")
        })?;
        let deadline = received + super::catch_up::READ_DEADLINE;
        let _writer = timeout_at(deadline, self.writer.lock())
            .await
            .map_err(|_| {
                HandlerError::new("identity_log_contended", "writer lock deadline expired")
            })?;
        // Hub snapshot recovery is large; other requests share the RPC dispatcher.
        let result =
            Box::pin(self.enable_hub(switch, params.without_agents, key, received, deadline)).await;
        if let Err(error) = &result {
            record_error(&self.health, error);
        }
        let health = self.health.clone();
        let (status, installed) = self
            .served_hub_sql()
            .run(
                Instant::now() + Duration::from_secs(3),
                "identity_log_status",
                move |store| {
                    let status = store.identity_log_status()?;
                    refresh_status(&health, &status);
                    health
                        .generation
                        .fetch_max(store.generation()?, Ordering::Relaxed);
                    Ok((status, store.hub_log_base()?.is_some()))
                },
            )
            .await?;
        self.commits.notify_waiters();
        result?;
        if installed || self.health.origin.daemon_id().is_some() {
            self.catch_up_wake.notify_one();
        }
        self.encode_identity_log_status(status)
    }

    async fn enable_hub(
        &self,
        switch: &hub_switch::Switch,
        without_agents: bool,
        key: RouteKey,
        received: Instant,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let runtime = hub_switch::Runtime::new(self);
        let sql = &runtime.sql;
        let (installed, planned) = sql
            .run(deadline, "hub_enable_state", |store| {
                Ok((
                    store.hub_log_base()?.is_some(),
                    store.hub_enable_plan()?.is_some(),
                ))
            })
            .await?;
        if installed {
            return Ok(());
        }
        if planned {
            return switch.finish(&runtime, None, deadline).await;
        }
        if switch.head(deadline).await? > 0 {
            return switch.finish(&runtime, None, deadline).await;
        }
        sql.run(deadline, "check_enable_preconditions", move |store| {
            store.check_enable_preconditions(0)?;
            Ok(store.check_agent_import_preconditions(without_agents)?)
        })
        .await?;
        let mut confirmation = agent_ops::ConfirmationGuard::take(&self.confirmation_slot, key)?;
        if !self.route_admissions().contains_key(&key) {
            return Err(agent_ops::OperatorConfirmError::Declined.into());
        }
        let summary = "Start a new shared registry for this account?";
        tokio::select! {
            biased;
            _ = confirmation.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
            result = timeout_at(received + shared_write::OPERATOR_BOUND, self.operator_confirmer.confirm_operator(summary, key)) => {
                result.map_err(|_| HandlerError::from(agent_ops::OperatorConfirmError::Declined))??;
            }
        }
        drop(confirmation);
        let deadline = Instant::now() + super::catch_up::READ_DEADLINE;
        if switch.head(deadline).await? > 0 {
            return switch.finish(&runtime, None, deadline).await;
        }
        switch.prepare_enable(sql, deadline).await?;
        self.catch_up_wake.notify_one();
        switch.finish(&runtime, None, deadline).await
    }
}

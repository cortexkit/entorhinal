//! Startup provenance and the operator-only recovery path. Health reads only
//! immutable ids and an atomic flag, never the store or its mutex.
use super::*;
use std::sync::OnceLock;

pub(super) const ADOPT: &str = "store.adopt";

#[derive(Default)]
pub(super) struct Origin {
    daemon: OnceLock<Option<String>>,
    store: OnceLock<Option<String>>,
    foreign: AtomicBool,
    resume_pending: AtomicBool,
}
impl Origin {
    pub(super) fn metrics(&self, mut metrics: Value) -> Value {
        metrics["storeOrigin"] = json!(self.label());
        metrics["storeMachineId"] = json!(self.store_id());
        metrics["daemonMachineId"] = json!(self.daemon_id());
        metrics
    }
    pub(super) fn set_daemon(&self, id: Option<String>) {
        let _ = self.daemon.set(id);
    }
    pub(super) fn daemon_id(&self) -> Option<&str> {
        self.daemon.get().and_then(|id| id.as_deref())
    }
    pub(super) fn store_id(&self) -> Option<&str> {
        if self.store.get().is_some() && !self.is_foreign() && self.daemon_id().is_some() {
            self.daemon_id()
        } else {
            self.store.get().and_then(|id| id.as_deref())
        }
    }
    pub(super) fn is_foreign(&self) -> bool {
        self.foreign.load(Ordering::Acquire)
    }
    pub(super) fn label(&self) -> &'static str {
        if self.daemon_id().is_none() {
            "unchecked"
        } else if self.is_foreign() {
            "foreign"
        } else {
            "this_machine"
        }
    }
    pub(super) fn check(&self, store: &RegistryStore) -> Result<(), String> {
        let id = store
            .check_store_origin(self.daemon_id(), unix_millis())
            .map_err(|e| format!("checking store origin: {e}"))?;
        let _ = self.store.set(id);
        self.foreign.store(store.is_foreign(), Ordering::Release);
        if self.daemon_id().is_none() {
            tracing::warn!(target: "store", "daemon supplied no machine id; store origin unchecked");
        }
        Ok(())
    }
}

impl ProjectsHandler {
    pub(super) fn resume_after_store_adoption(&self) {
        if !self
            .health
            .origin
            .resume_pending
            .swap(false, Ordering::AcqRel)
        {
            return;
        }
        self.watches.resync_all();
        self.commits.notify_waiters();
        self.start_catch_up();
        self.start_catalog_poll();
        self.start_claim_refresh();
        self.claim_refresh.signal();
        self.start_role_refresh();
        self.role_refresh.notify_one();
        self.start_move_driver();
        self.move_wake.notify_one();
        self.catalog_poll_wake.notify_one();
        if let Ok(store) = self.with_store(|s| Ok(s.clone())) {
            catalog_seed::start(store, &self.catalog_sql);
        }
    }
    /// One refusal for every persistent mutation, including consent-backed tool
    /// writes that intentionally bypass principal-based write authorization.
    pub(super) fn admit_store_write(&self) -> Result<(), HandlerError> {
        if self.health.origin.is_foreign() {
            return Err(HandlerError {
                code: "store_from_another_machine".into(),
                message: "store belongs to another Mac; use ck projects adopt-store".into(),
                detail: Some(
                    json!({"storeMachineId":self.health.origin.store_id(),"daemonMachineId":self.health.origin.daemon_id()}),
                ),
            });
        }
        Ok(())
    }
    pub(super) fn admit_store_method(&self, method: &str) -> Result<(), HandlerError> {
        if !matches!(method, "projects.session_liveness" | ADOPT)
            && (MUTATING_METHODS.contains(&method)
                || (cortexkit_role_agent_data::REQUIRED_OPS.contains(&method)
                    && !matches!(
                        method,
                        "agent_data.describe" | "agent_data.busy" | "agent_data.status"
                    )))
        {
            self.admit_store_write()?;
        }
        Ok(())
    }

    pub(super) async fn adopt_store(
        &self,
        params: Value,
        key: RouteKey,
        received: tokio::time::Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admission_for(key);
        if !matches!(admission.principal, Some(Principal::Direct))
            || admission.flow_id.is_some()
            || admission.scope.is_some()
        {
            return Err(HandlerError::new(
                "caller_forbidden",
                "store adoption requires an unscoped Direct operator",
            ));
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            #[serde(default)]
            same_machine: bool,
        }
        let request: Request = serde_json::from_value(params).map_err(invalid_params)?;
        if !self.health.origin.is_foreign() {
            return Err(HandlerError::new(
                "store_not_foreign",
                "store does not belong to another machine",
            ));
        }
        let daemon = self
            .health
            .origin
            .daemon_id()
            .expect("foreign requires daemon id");
        let old = self
            .health
            .origin
            .store_id()
            .expect("foreign requires store id");
        let store = self.with_store(|s| Ok(s.clone()))?;
        let sharing = store
            .identity_log_status()
            .map_err(|e| HandlerError::new("storage_error", e.to_string()))?
            .state
            != "disabled";
        let action = if request.same_machine {
            "nothing is cleared"
        } else if sharing {
            "reset registry; rejoin the shared log afterwards"
        } else {
            "keep projects; clear local authority and execution state"
        };
        let summary = format!("Adopt store {old} on {daemon}: {action}.");
        let mut guard = agent_ops::ConfirmationGuard::take(&self.confirmation_slot, key)?;
        let deadline = received + Duration::from_secs(270);
        tokio::select! {
            biased;
            _ = guard.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
            result = tokio::time::timeout_at(deadline, self.operator_confirmer.confirm_operator(&summary, key)) => {
                result.map_err(|_| HandlerError::from(agent_ops::OperatorConfirmError::Declined))??;
            }
        }
        let writer = tokio::time::timeout_at(deadline, self.writer.clone().lock_owned())
            .await
            .map_err(|_| HandlerError::from(agent_ops::OperatorConfirmError::Declined))?;
        if *guard.cancelled.borrow()
            || !self.route_admissions().contains_key(&key)
            || tokio::time::Instant::now() >= deadline
        {
            return Err(agent_ops::OperatorConfirmError::Declined.into());
        }
        if !self.health.origin.is_foreign() {
            return Err(HandlerError::new(
                "store_not_foreign",
                "store was already adopted",
            ));
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let same_machine = request.same_machine;
        let id = daemon.to_owned();
        let now = (self.clock)();
        let worker_store = store.clone();
        let health = self.health.clone();
        tokio::task::spawn_blocking(move || {
            // A cancelled response must not release write exclusion or leave
            // health foreign after the transaction has committed adoption.
            let _writer = writer;
            worker_store.adopt_store(&id, same_machine, now)?;
            enable::refresh(&health, &worker_store);
            health
                .generation
                .store(worker_store.generation().unwrap_or(0), Ordering::Relaxed);
            health.origin.foreign.store(false, Ordering::Release);
            health.origin.resume_pending.store(true, Ordering::Release);
            Ok::<_, RegistryError>(())
        })
        .await
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?
        .map_err(HandlerError::from)?;
        self.resume_after_store_adoption();
        encode_result(json!({"storeOrigin":"this_machine","sameMachine":same_machine}))
    }
}

#[cfg(test)]
#[path = "store_origin_tests.rs"]
mod tests;

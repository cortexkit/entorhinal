//! Role claims refresh independently of the shared identity log. Only operator
//! routes stamped Direct can take roles, after confirmation; role operations
//! never acquire the mutex that serializes shared identity-log writes.

use super::{
    agent_ops::{ConfirmationGuard, OperatorConfirmError},
    claim_codec::RoleClaim,
    encode_result,
    record_client::{CasRequest, CasSettlement, RecordError, RecordReply, MANY_CAP},
    HandlerError, ProjectsHandler, RouteKey,
};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    role_cache::{valid_role, RoleTakeAttempt, RoleTakeInput},
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    sync::{atomic::Ordering, Arc},
    time::Duration,
};
use tokio::time::{timeout_at, Instant};

pub(crate) const READ: &str = "role.read";
pub(crate) const TAKE: &str = "role.take";

fn record_error(error: RecordError) -> HandlerError {
    match error {
        RecordError::Refused { code, detail } => HandlerError {
            message: code.clone(),
            code,
            detail,
        },
        _ => HandlerError::new("engram_unavailable", error.code()),
    }
}

fn decoded(role: &str, reply: &RecordReply) -> Result<Option<RoleClaim>, HandlerError> {
    if reply.absent {
        return Ok(None);
    }
    RoleClaim::decode(role, reply.body.as_deref().unwrap_or_default())
        .map(Some)
        .map_err(|code| HandlerError::new("claim_integrity_failed", code))
}

fn epoch_moved(claim: Option<RoleClaim>) -> HandlerError {
    HandlerError {
        code: "epoch_moved".into(),
        message: "the role epoch moved".into(),
        detail: Some(
            json!({"current_epoch":claim.as_ref().map_or(0, |c| c.epoch),"holder":claim.and_then(|c| c.holder)}),
        ),
    }
}

async fn local<T: Send + 'static>(
    operation: impl FnOnce() -> Result<T, HandlerError> + Send + 'static,
) -> Result<T, HandlerError> {
    tokio::task::spawn_blocking(operation)
        .await
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?
}

impl ProjectsHandler {
    pub(super) fn admit_role_take(&self, key: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(TAKE, admission.flow_id.as_deref())?;
        if admission.principal != Some(subc_protocol::Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "role.take requires Direct",
            ));
        }
        Ok(())
    }

    pub(super) fn role_read(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !valid_role(&request.role) {
            return Err(HandlerError::new("invalid_request", "invalid role"));
        }
        encode_result(
            self.with_store(|s| Ok(s.clone()))?
                .role_read(&request.role)?,
        )
    }

    pub(super) async fn take_role(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_role_take(key)?;
        let input: RoleTakeInput = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !valid_role(&input.role)
            || input.expected_epoch < 0
            || input.expected_epoch == i64::MAX
            || input.request_id.len() != 32
            || !input
                .request_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(HandlerError::new(
                "invalid_request",
                "invalid role, epoch or request_id",
            ));
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let attempt = store.role_take_attempt(&input)?;
        let mut confirmation = None;
        let attempt = match attempt {
            Some(attempt) => attempt,
            None => {
                #[cfg(test)]
                if let Some(barrier) = &self.confirmation_admission_barrier {
                    barrier.wait().await;
                }
                let mut guard = ConfirmationGuard::take(&self.confirmation_slot, key)?;
                if !self.route_admissions().contains_key(&key) {
                    return Err(OperatorConfirmError::Declined.into());
                }
                let summary = format!(
                    "Take role '{}' at epoch {}: {}",
                    input.role, input.expected_epoch, input.reason
                );
                tokio::select! {
                    biased;
                    _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
                    result = timeout_at(received + super::shared_write::OPERATOR_BOUND, self.operator_confirmer.confirm_operator(&summary, key)) => {
                        result.map_err(|_| HandlerError::from(OperatorConfirmError::Declined))??;
                    }
                }
                if Instant::now() >= received + super::shared_write::OPERATOR_BOUND {
                    return Err(OperatorConfirmError::Declined.into());
                }
                confirmation = Some(guard);
                let subject = format!("role:{}", input.role);
                let read = self.record_client.read(&subject);
                let reply = tokio::select! {
                    biased;
                    _ = confirmation.as_mut().expect("approved take has a guard").cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
                    result = read => result.map_err(record_error)?,
                };
                let claim = decoded(&input.role, &reply)?;
                if claim.as_ref().map_or(0, |c| c.epoch) != input.expected_epoch {
                    return Err(epoch_moved(claim));
                }
                let own = store.persisted_self_author()?;
                if own
                    .as_ref()
                    .is_some_and(|known| *known != reply.self_author)
                {
                    self.health
                        .self_author_conflicts
                        .fetch_add(1, Ordering::Relaxed);
                    return Err(HandlerError::new(
                        "self_author_conflict",
                        "record identity differs from persisted identity",
                    ));
                }
                let holder = own.unwrap_or_else(|| reply.self_author.clone());
                let body = serde_json::to_vec(&RoleClaim {
                    v: 1,
                    role: input.role.clone(),
                    holder: Some(holder),
                    epoch: input.expected_epoch + 1,
                    reason: input.reason.clone(),
                })
                .expect("role body encodes");
                let attempt = RoleTakeAttempt {
                    input,
                    expected_version: i64::try_from(reply.version).map_err(|_| {
                        HandlerError::new(
                            "claim_integrity_failed",
                            "record version exceeds local range",
                        )
                    })?,
                    body,
                    epoch: None,
                };
                let cache = store.clone();
                let now = (self.clock)();
                let attempt = local(move || {
                    cache
                        .prepare_role_take(&attempt, &reply.self_author, now)
                        .map_err(Into::into)
                })
                .await?;
                attempt
            }
        };
        if let Some(epoch) = attempt.epoch {
            return encode_result(json!({"epoch":epoch,"held_here":true}));
        }
        let claim = RoleClaim::decode(&attempt.input.role, &attempt.body)
            .map_err(|code| HandlerError::new("storage_error", code))?;
        let holder = claim
            .holder
            .clone()
            .ok_or_else(|| HandlerError::new("storage_error", "approved role body lacks holder"))?;
        let mut write_id = [0; 16];
        write_id.copy_from_slice(
            &super::log_client::decode_hex(&attempt.input.request_id)
                .map_err(|e| HandlerError::new("invalid_request", format!("{e:?}")))?,
        );
        let request = CasRequest {
            subject: format!("role:{}", attempt.input.role),
            expected_version: attempt.expected_version as u64,
            body: attempt.body.clone(),
            write_id,
        };
        let settle = self.record_client.cas_settled(&request, &holder, |bytes| {
            RoleClaim::decode(&attempt.input.role, bytes)
        });
        let result = if let Some(guard) = &mut confirmation {
            tokio::select! {
                biased;
                _ = guard.cancelled.wait_for(|closed| *closed) => return Err(OperatorConfirmError::Declined.into()),
                result = settle => result,
            }
        } else { settle.await }.map_err(record_error)?;
        match result {
            CasSettlement::Moved(reply) => Err(epoch_moved(decoded(&attempt.input.role, &reply)?)),
            CasSettlement::Landed(version) => {
                let version = i64::try_from(version).map_err(|_| {
                    HandlerError::new("storage_error", "record version exceeds local range")
                })?;
                let epoch = claim.epoch;
                let now = (self.clock)();
                local(move || {
                    store
                        .finish_role_take(&attempt, &holder, epoch, version, now)
                        .map_err(Into::into)
                })
                .await?;
                encode_result(json!({"epoch":epoch,"held_here":true}))
            }
        }
    }

    pub(super) fn start_role_refresh(&self) {
        let mut task = self.role_refresh_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let store = Arc::downgrade(&self.store);
        let records = self.record_client.clone();
        let signal = self.role_refresh.clone();
        let health = self.health.clone();
        let clock = self.clock;
        let sql_operations = self.role_refresh_sql.clone();
        *task = Some(tokio::spawn(async move {
            let mut ticks = tokio::time::interval_at(
                Instant::now() + Duration::from_secs(30),
                Duration::from_secs(30),
            );
            ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! { _ = ticks.tick() => {}, _ = signal.notified() => {} }
                let roles = {
                    let Some(slot) = store.upgrade() else {
                        return;
                    };
                    let Some(cache) = slot.lock().unwrap().clone() else {
                        continue;
                    };
                    match cache.role_subjects() {
                        Ok(roles) => roles,
                        Err(_) => {
                            health.role_refresh_failed.store(true, Ordering::Relaxed);
                            continue;
                        }
                    }
                };
                let mut failed = false;
                for chunk in roles.chunks(MANY_CAP) {
                    let mut offset = 0;
                    while offset < chunk.len() {
                        let subjects: Vec<_> = chunk[offset..]
                            .iter()
                            .map(|role| format!("role:{role}"))
                            .collect();
                        let (entries, next) = match records.read_many(&subjects).await {
                            Ok(page) => page,
                            Err(error) => (vec![Err(error); subjects.len()], None),
                        };
                        let mut observations = Vec::new();
                        for (role, entry) in chunk[offset..].iter().zip(entries) {
                            let observation = match entry {
                                Ok(reply) if reply.absent => ClaimObservation::Absent {
                                    stale: reply.stale,
                                    self_author: reply.self_author,
                                },
                                Ok(reply) => match RoleClaim::decode(
                                    role,
                                    reply.body.as_deref().unwrap_or_default(),
                                ) {
                                    Ok(claim) if i64::try_from(reply.version).is_ok() => {
                                        ClaimObservation::Present {
                                            claim: CachedClaim {
                                                holder: claim.holder,
                                                epoch: claim.epoch,
                                                cut_id: None,
                                                version: reply.version as i64,
                                                read_at_ms: 0,
                                                stale: reply.stale,
                                                integrity_failed: false,
                                            },
                                            self_author: reply.self_author,
                                        }
                                    }
                                    result => {
                                        failed = true;
                                        let code = result.err().unwrap_or("claim_malformed");
                                        match code {
                                            "claim_version_unknown" => {
                                                &health.claim_version_unknown
                                            }
                                            "claim_subject_mismatch" => {
                                                &health.claim_subject_mismatch
                                            }
                                            _ => &health.claim_malformed,
                                        }
                                        .fetch_add(1, Ordering::Relaxed);
                                        ClaimObservation::CodecFailed {
                                            self_author: reply.self_author,
                                        }
                                    }
                                },
                                Err(error) => {
                                    failed = true;
                                    if ["verify_failed", "decrypt_failed"].contains(&error.code()) {
                                        ClaimObservation::IntegrityFailed
                                    } else {
                                        ClaimObservation::Unavailable
                                    }
                                }
                            };
                            observations.push((role.clone(), observation));
                        }
                        let cache = store
                            .upgrade()
                            .and_then(|slot| slot.lock().unwrap().clone());
                        let Some(cache) = cache else {
                            return;
                        };
                        let now = clock();
                        let guard = sql_operations.begin();
                        match tokio::task::spawn_blocking(move || {
                            let _guard = guard;
                            let store = cache;
                            store.cache_roles(&observations, now)
                        })
                        .await
                        {
                            Ok(Ok(conflicts)) => {
                                health
                                    .self_author_conflicts
                                    .fetch_add(conflicts as u64, Ordering::Relaxed);
                            }
                            _ => failed = true,
                        }
                        offset += next.unwrap_or(subjects.len());
                    }
                }
                health.role_refresh_failed.store(failed, Ordering::Relaxed);
                #[cfg(test)]
                health.role_refresh_done.notify_one();
            }
        }));
    }

    #[cfg(test)]
    pub(super) async fn stop_role_refresh(&self) {
        let task = self.role_refresh_task.lock().unwrap().take();
        if let Some(task) = task {
            task.abort();
            let _ = task.await;
        }
        self.role_refresh_sql.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent_ops::OperatorConfirmer,
        fake_log::{Action, FakeLog},
        fake_record::{FakeRecord, FakeRecordConnector},
        log_client::LogConnector,
        record_client::RecordClient,
        tests::{reserved, scratch_descriptor},
        RouteAdmission,
    };
    use entorhinal_core::RegistryStore;
    use std::sync::{atomic::AtomicBool, Mutex};
    use subc_client_rs::{HandlerOutcome, ModuleHandler, RouteHandle};
    use subc_protocol::Principal;
    const DIRECT: RouteKey = (80, 1);
    const MODULE: RouteKey = (81, 1);
    #[derive(Default)]
    struct Prompt {
        calls: Mutex<Vec<(String, RouteKey)>>,
        error: Mutex<Option<OperatorConfirmError>>,
        hung: AtomicBool,
        entered: tokio::sync::Notify,
    }
    #[async_trait::async_trait]
    impl OperatorConfirmer for Prompt {
        async fn confirm_operator(
            &self,
            summary: &str,
            route: RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.calls.lock().unwrap().push((summary.into(), route));
            self.entered.notify_one();
            if self.hung.load(Ordering::Relaxed) {
                std::future::pending::<()>().await;
            }
            self.error.lock().unwrap().map_or(Ok(()), Err)
        }
    }
    struct Fixture {
        handler: Arc<ProjectsHandler>,
        record: Arc<FakeRecord>,
        prompt: Arc<Prompt>,
        log: FakeLog,
        dir: std::path::PathBuf,
        descriptor: cortexkit_store_types::StorageDescriptor,
    }
    impl Fixture {
        fn new() -> Self {
            let (dir, descriptor) = scratch_descriptor("roles");
            let log = FakeLog::default().with_author([1; 16]);
            let record = FakeRecord::new([1; 16]);
            let prompt = Arc::new(Prompt::default());
            let mut h = ProjectsHandler::with_log_connector(
                "0123456789abcdef".into(),
                || 700,
                Arc::new(log.clone()) as Arc<dyn LogConnector>,
            )
            .with_operator_confirmer(prompt.clone());
            h.record_client = RecordClient::new(Arc::new(FakeRecordConnector(record.clone())));
            super::super::install_store(
                &h.store,
                &h.health,
                &h.catalog_sql,
                Ok(RegistryStore::open(&descriptor).unwrap()),
            );
            h.route_admissions().insert(
                DIRECT,
                RouteAdmission::from_bind(Some(Principal::Direct), None),
            );
            h.route_admissions().insert(
                MODULE,
                RouteAdmission::from_bind(Some(reserved("some-module")), None),
            );
            Self {
                handler: Arc::new(h),
                record,
                prompt,
                log,
                dir,
                descriptor,
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        async fn call(&self, key: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(
                    &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                    key,
                )
                .await
        }
        fn read(&self) -> Value {
            serde_json::from_slice::<Value>(&self.handler.role_read(json!({"role":"x"})).unwrap())
                .unwrap()["result"]
                .clone()
        }
        async fn refresh(&self) {
            self.handler.start_role_refresh();
            self.handler.role_refresh.notify_one();
            self.handler.health.role_refresh_done.notified().await;
        }

        async fn close(&self) {
            crate::tests::stop_store_tasks(&self.handler).await;
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler
                .role_refresh_task
                .lock()
                .unwrap()
                .take()
                .inspect(|task| task.abort());
            self.handler
                .catch_up_task
                .lock()
                .unwrap()
                .take()
                .inspect(|task| task.abort());
            self.handler.store.lock().unwrap().take();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }
    fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(bytes) => {
                serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
            }
            other => panic!("expected response: {other:?}"),
        }
    }
    fn code(outcome: HandlerOutcome) -> String {
        match outcome {
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                code
            }
            other => panic!("expected refusal: {other:?}"),
        }
    }
    fn input() -> Value {
        json!({"role":"x","expected_epoch":0,"request_id":"11".repeat(16),"reason":"run coordinator"})
    }
    fn claim(epoch: i64, holder: &str) -> Vec<u8> {
        serde_json::to_vec(&RoleClaim {
            v: 1,
            role: "x".into(),
            holder: Some(holder.into()),
            epoch,
            reason: "cloud".into(),
        })
        .unwrap()
    }

    #[tokio::test(start_paused = true)]
    async fn role_timer_runs_with_log_disabled_and_refreshes_integrity_and_sources() {
        let f = Fixture::new();
        assert_eq!(f.store().identity_log_status().unwrap().state, "disabled");
        assert_eq!(f.read()["known"], false);
        f.record
            .put("role:x", 1, claim(1, &f.record.author), &f.record.author);
        f.handler.start_role_refresh();
        tokio::task::yield_now().await;
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 0);
        tokio::time::advance(Duration::from_secs(30)).await;
        tokio::task::yield_now().await;
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 1);
        f.handler.health.role_refresh_done.notified().await;
        assert_eq!(f.read()["known"], true);
        assert_eq!(f.read()["held_here"], true);
        assert_eq!(f.read()["source"], "cloud");
        f.record.error("role:x", Some("decrypt_failed"));
        f.refresh().await;
        assert_eq!(
            f.handler.role_read(json!({"role":"x"})).unwrap_err().code,
            "claim_integrity_failed"
        );
        f.record.error("role:x", None);
        f.refresh().await;
        assert_eq!(f.read()["stale"], false);
        assert_eq!(f.log.calls(), 0);
        f.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn role_store_install_and_reconnect_signal_the_role_pass() {
        let f = Fixture::new();
        let role_batches = || {
            f.record
                .batches
                .lock()
                .unwrap()
                .iter()
                .filter(|subjects| {
                    !subjects.is_empty()
                        && subjects.iter().all(|subject| subject.starts_with("role:"))
                })
                .count()
        };
        f.read();
        f.record
            .put("role:x", 1, claim(1, &f.record.author), &f.record.author);
        crate::tests::stop_store_tasks(&f.handler).await;
        f.handler.store.lock().unwrap().take();
        let installed = tokio::time::Instant::now();
        f.handler
            .on_hello_ack(&crate::tests::hello_ack(&f.descriptor))
            .await;
        // Count only after the role-refresh pass reports done: it runs its SQL
        // on the blocking pool, which a single yield doesn't wait for. The paused
        // clock must not have reached the 30 s periodic role-refresh timer, which
        // proves this pass came from the store install itself, not the timer.
        f.handler.health.role_refresh_done.notified().await;
        assert!(installed.elapsed() < Duration::from_secs(30));
        assert_eq!(role_batches(), 1);
        assert_eq!(f.read()["known"], true);
        assert_eq!(f.log.calls(), 0);
        f.handler
            .catch_up_task
            .lock()
            .unwrap()
            .take()
            .unwrap()
            .abort();
        result(
            f.call(
                DIRECT,
                "identity_log.enable",
                json!({"without_agents":true}),
            )
            .await,
        );
        f.record.error("role:x", Some("unavailable"));
        f.refresh().await;
        assert_eq!(f.read()["stale"], true);
        f.record.error("role:x", None);
        f.handler.health.log_state.store(2, Ordering::Relaxed);
        let before = role_batches();
        f.handler.start_catch_up();
        tokio::task::yield_now().await;
        assert_eq!(role_batches(), before + 1);
        f.handler.health.role_refresh_done.notified().await;
        assert_eq!(role_batches(), before + 1);
        assert_eq!(f.read()["stale"], false);
        f.close().await;
    }

    #[tokio::test]
    async fn role_take_is_confirmed_direct_only_and_ledger_precedes_prompts() {
        let f = Fixture::new();
        assert_eq!(
            code(f.call(MODULE, TAKE, input()).await),
            "caller_forbidden"
        );
        let response = result(f.call(DIRECT, TAKE, input()).await);
        assert_eq!(response, json!({"epoch":1,"held_here":true}));
        assert_eq!(
            f.prompt.calls.lock().unwrap().as_slice(),
            [("Take role 'x' at epoch 0: run coordinator".into(), DIRECT)]
        );
        assert_eq!(f.read()["source"], "local");
        let reply = f.handler.record_client.read("role:x").await.unwrap();
        assert_eq!(
            RoleClaim::decode("x", reply.body.as_deref().unwrap()).unwrap(),
            RoleClaim {
                v: 1,
                role: "x".into(),
                holder: Some(f.record.author.clone()),
                epoch: 1,
                reason: "run coordinator".into()
            }
        );
        let calls = f.record.calls.load(Ordering::Relaxed);
        assert_eq!(result(f.call(DIRECT, TAKE, input()).await), response);
        let mut reused = input();
        reused["reason"] = json!("other");
        assert_eq!(code(f.call(DIRECT, TAKE, reused).await), "id_reused");
        assert_eq!(f.record.calls.load(Ordering::Relaxed), calls);
        assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
        f.refresh().await;
        assert_eq!(f.read()["source"], "cloud");
        assert_eq!(f.read()["stale"], false);
        assert_eq!(f.store().generation().unwrap(), 0);
        f.close().await;
    }

    #[tokio::test]
    async fn role_take_refuses_missing_presence_epoch_conflict_and_invalid_requests() {
        let f = Fixture::new();
        *f.prompt.error.lock().unwrap() = Some(OperatorConfirmError::PresenceUnavailable);
        assert_eq!(
            code(f.call(DIRECT, TAKE, input()).await),
            "operator_presence_unavailable"
        );
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 0);
        *f.prompt.error.lock().unwrap() = None;
        f.record
            .put("role:x", 5, claim(5, &"02".repeat(16)), &"02".repeat(16));
        assert_eq!(code(f.call(DIRECT, TAKE, input()).await), "epoch_moved");
        assert!(f
            .store()
            .role_take_attempt(&serde_json::from_value(input()).unwrap())
            .unwrap()
            .is_none());
        f.store()
            .cache_roles(
                &[(
                    "x".into(),
                    ClaimObservation::Absent {
                        stale: false,
                        self_author: "03".repeat(16),
                    },
                )],
                1,
            )
            .unwrap();
        let mut moved = input();
        moved["expected_epoch"] = json!(5);
        assert_eq!(
            code(f.call(DIRECT, TAKE, moved).await),
            "self_author_conflict"
        );
        assert_eq!(
            f.handler
                .health
                .self_author_conflicts
                .load(Ordering::Relaxed),
            1
        );
        for bad in [
            json!({}),
            json!({"role":"X","expected_epoch":0,"request_id":"11".repeat(16),"reason":"x"}),
            json!({"role":"x","expected_epoch":-1,"request_id":"11".repeat(16),"reason":"x"}),
            json!({"role":"x","expected_epoch":0,"request_id":"AA".repeat(16),"reason":"x"}),
            json!({"role":"x","expected_epoch":0,"request_id":"1","reason":"x"}),
            json!({"role":"x","expected_epoch":0,"request_id":"11".repeat(16),"reason":"x","extra":true}),
            json!({"role":"x","expected_epoch":i64::MAX,"request_id":"11".repeat(16),"reason":"x"}),
        ] {
            assert_eq!(code(f.call(DIRECT, TAKE, bad).await), "invalid_request");
        }
        let bytes = serde_json::to_vec(&json!({"method":TAKE,"params":input()})).unwrap();
        assert_eq!(
            code(f.handler.handle_request(&bytes, DIRECT)),
            "served_path_required"
        );
        f.handler
            .route_admissions()
            .get_mut(&DIRECT)
            .unwrap()
            .flow_id = Some("flow".into());
        assert_eq!(
            code(f.call(DIRECT, TAKE, input()).await),
            "flow_scope_not_admitted"
        );
        f.close().await;
    }

    #[tokio::test]
    async fn role_lost_reply_retries_exact_attempt_without_another_prompt() {
        let f = Fixture::new();
        f.record.drop_reply.store(true, Ordering::Relaxed);
        let answer = result(f.call(DIRECT, TAKE, input()).await);
        assert_eq!(answer, json!({"epoch":1,"held_here":true}));
        assert_eq!(
            f.handler
                .record_client
                .read("role:x")
                .await
                .unwrap()
                .version,
            1
        );
        let calls = f.record.calls.load(Ordering::Relaxed);
        assert_eq!(result(f.call(DIRECT, TAKE, input()).await), answer);
        assert_eq!(f.record.calls.load(Ordering::Relaxed), calls);
        assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
        f.close().await;
    }

    #[tokio::test]
    async fn role_read_bytes_keep_unknown_null_fields_without_record_calls() {
        let f = Fixture::new();
        assert_eq!(f.handler.role_read(json!({"role":"x"})).unwrap(), br#"{"result":{"epoch":0,"held_here":false,"holder":null,"holder_name":null,"known":false,"role":"x","source":null,"stale":true}}"#);
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 0);
        assert_eq!(
            f.handler
                .role_read(json!({"role":"x","extra":true}))
                .unwrap_err()
                .code,
            "invalid_request"
        );
        assert_eq!(
            result(f.call(MODULE, READ, json!({"role":"x"})).await)["known"],
            false
        );
        f.close().await;
    }

    #[tokio::test(start_paused = true)]
    async fn role_unsettled_lost_reply_retry_reuses_approved_bytes_without_presence() {
        let f = Fixture::new();
        f.record.drop_reply.store(true, Ordering::Relaxed);
        *f.record.after_drop.lock().unwrap() = Some(Box::new(|record| {
            record.hung.store(true, Ordering::Relaxed);
        }));
        assert_eq!(code(f.call(DIRECT, TAKE, input()).await), "unavailable");
        let ledger = f
            .store()
            .role_take_attempt(&serde_json::from_value(input()).unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(ledger.epoch, None);
        f.record.hung.store(false, Ordering::Relaxed);
        *f.prompt.error.lock().unwrap() = Some(OperatorConfirmError::PresenceUnavailable);
        assert_eq!(
            result(f.call(DIRECT, TAKE, input()).await),
            json!({"epoch":1,"held_here":true})
        );
        assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
        assert_eq!(
            f.handler
                .record_client
                .read("role:x")
                .await
                .unwrap()
                .version,
            1
        );
        f.close().await;
    }

    #[tokio::test]
    async fn role_past_window_settlement_compares_decoded_body_and_author() {
        let f = Fixture::new();
        f.record.drop_reply.store(true, Ordering::Relaxed);
        *f.record.after_drop.lock().unwrap() = Some(Box::new(|record| {
            let body = serde_json::to_vec_pretty(&RoleClaim {
                v: 1,
                role: "x".into(),
                holder: Some(record.author.clone()),
                epoch: 1,
                reason: "run coordinator".into(),
            })
            .unwrap();
            record.advance_versions("role:x", 1, 64, &body);
        }));
        assert_eq!(
            result(f.call(DIRECT, TAKE, input()).await),
            json!({"epoch":1,"held_here":true})
        );
        assert_eq!(
            f.handler
                .record_client
                .read("role:x")
                .await
                .unwrap()
                .version,
            65
        );
        assert_eq!(
            f.store()
                .role_take_attempt(&serde_json::from_value(input()).unwrap())
                .unwrap()
                .unwrap()
                .epoch,
            Some(1)
        );
        f.close().await;
    }

    #[tokio::test]
    async fn role_past_window_superseded_keeps_ledger_unsettled_and_retries_without_prompt() {
        for change in ["body", "author"] {
            let f = Fixture::new();
            f.record.drop_reply.store(true, Ordering::Relaxed);
            *f.record.after_drop.lock().unwrap() = Some(Box::new(move |record| {
                let body = serde_json::to_vec(&RoleClaim {
                    v: 1,
                    role: "x".into(),
                    holder: Some(record.author.clone()),
                    epoch: if change == "body" { 2 } else { 1 },
                    reason: "run coordinator".into(),
                })
                .unwrap();
                record.advance_versions("role:x", 1, 64, &body);
                if change == "author" {
                    record.put("role:x", 65, body, &"02".repeat(16));
                }
            }));
            assert_eq!(code(f.call(DIRECT, TAKE, input()).await), "epoch_moved");
            assert_eq!(
                f.store()
                    .role_take_attempt(&serde_json::from_value(input()).unwrap())
                    .unwrap()
                    .unwrap()
                    .epoch,
                None
            );
            assert_eq!(code(f.call(DIRECT, TAKE, input()).await), "epoch_moved");
            assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
            assert_eq!(
                f.handler
                    .record_client
                    .read("role:x")
                    .await
                    .unwrap()
                    .version,
                65
            );
            f.close().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn role_prompt_cancels_on_route_close_and_operator_bound() {
        for route_close in [true, false] {
            let f = Fixture::new();
            f.prompt.hung.store(true, Ordering::Relaxed);
            let handler = f.handler.clone();
            let take =
                tokio::spawn(
                    async move { handler.take_role(input(), DIRECT, Instant::now()).await },
                );
            f.prompt.entered.notified().await;
            assert_eq!(
                code(f.call(DIRECT, TAKE, input()).await),
                "operator_confirmation_busy"
            );
            if route_close {
                f.handler
                    .on_route_gone(&RouteHandle::detached(DIRECT.0, DIRECT.1))
                    .await;
            } else {
                tokio::time::advance(super::super::shared_write::OPERATOR_BOUND).await;
            }
            assert_eq!(take.await.unwrap().unwrap_err().code, "operator_declined");
            assert!(f.handler.confirmation_slot.lock().unwrap().is_none());
            assert_eq!(f.record.calls.load(Ordering::Relaxed), 0);
            f.close().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn role_take_does_not_queue_for_hung_catchup_writer() {
        let f = Fixture::new();
        result(
            f.call(
                DIRECT,
                "identity_log.enable",
                json!({"without_agents":true}),
            )
            .await,
        );
        let before = f.log.calls();
        f.log.on_read(Action::Hang);
        f.handler.start_catch_up();
        f.log.wait_for_calls(before + 1).await;
        assert!(f.handler.writer.try_lock().is_err());
        let answer = tokio::time::timeout(Duration::from_secs(1), f.call(DIRECT, TAKE, input()))
            .await
            .expect("role take must not wait for shared writer");
        assert_eq!(result(answer), json!({"epoch":1,"held_here":true}));
        f.close().await;
    }

    #[tokio::test]
    async fn role_store_install_refreshes_persisted_set_and_restart_stales_until_verified_read() {
        let f = Fixture::new();
        result(f.call(DIRECT, TAKE, input()).await);
        f.record.error("role:x", Some("verify_failed"));
        f.refresh().await;
        crate::tests::stop_store_tasks(&f.handler).await;
        f.handler.store.lock().unwrap().take();
        super::super::install_store(
            &f.handler.store,
            &f.handler.health,
            &f.handler.catalog_sql,
            Ok(RegistryStore::open(&f.descriptor).unwrap()),
        );
        assert_eq!(
            f.handler.role_read(json!({"role":"x"})).unwrap_err().code,
            "claim_integrity_failed"
        );
        f.record.error("role:x", None);
        f.refresh().await;
        assert_eq!(f.read()["source"], "cloud");
        assert_eq!(f.read()["stale"], false);
        crate::tests::stop_store_tasks(&f.handler).await;
        f.handler.store.lock().unwrap().take();
        super::super::install_store(
            &f.handler.store,
            &f.handler.health,
            &f.handler.catalog_sql,
            Ok(RegistryStore::open(&f.descriptor).unwrap()),
        );
        assert_eq!(f.read()["stale"], true);
        assert_eq!(f.read()["source"], "cloud");
        f.close().await;
    }
}

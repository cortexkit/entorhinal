//! Agent claim refresh runs independently of catch-up. Remote calls finish
//! before a short cache transaction, so neither readers nor session admission
//! queue behind the shared-log writer while engram is unavailable.

use super::{
    claim_codec::AgentClaim,
    encode_result,
    record_client::{RecordClient, MANY_CAP},
    HandlerError, HealthGauges, ProjectsHandler, RouteKey,
};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc, Mutex, Weak};
use tokio::sync::{mpsc, oneshot};

pub(crate) const REFRESH: &str = "agent.refresh_claims";
pub(crate) const WHERE: &str = "agent.where";

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub(crate) struct RefreshCounts {
    pub refreshed: usize,
    pub failed: usize,
}
type Waiter = oneshot::Sender<Result<RefreshCounts, HandlerError>>;

pub(crate) struct RefreshQueue {
    sender: mpsc::UnboundedSender<Option<Waiter>>,
    receiver: Mutex<Option<mpsc::UnboundedReceiver<Option<Waiter>>>>,
}

impl Default for RefreshQueue {
    fn default() -> Self {
        let (sender, receiver) = mpsc::unbounded_channel();
        Self {
            sender,
            receiver: Mutex::new(Some(receiver)),
        }
    }
}

impl RefreshQueue {
    pub(crate) fn signal(&self) {
        let _ = self.sender.send(None);
    }
}

/// The refresh task outlives no handler: it holds the handler's store slot
/// weakly, and takes a store handle only for the moment it reads or writes
/// SQLite, never across a call to engram. So dropping the handler, or taking
/// its store out, closes the database even while a refresh waits on a
/// slow or hung engram reply.
struct Worker {
    store: Weak<Mutex<Option<RegistryStore>>>,
    records: RecordClient,
    health: Arc<HealthGauges>,
    clock: fn() -> i64,
}

impl Worker {
    /// The handler's current store, or `None` once the handler or its store
    /// is gone.
    fn store(&self) -> Option<RegistryStore> {
        self.store.upgrade()?.lock().unwrap().clone()
    }

    async fn pass(&self) -> Result<RefreshCounts, HandlerError> {
        let agents = {
            let Some(store) = self.store() else {
                return Ok(RefreshCounts::default());
            };
            if store.identity_log_status()?.state != "enabled" {
                return Ok(RefreshCounts::default());
            }
            store.live_claim_agents()?
        };
        let mut counts = RefreshCounts::default();
        let mut reads_succeeded = 0;
        for chunk in agents.chunks(MANY_CAP) {
            let mut offset = 0;
            while offset < chunk.len() {
                let subjects: Vec<String> = chunk[offset..]
                    .iter()
                    .map(|id| format!("claim:{id}"))
                    .collect();
                let (entries, next) = match self.records.read_many(&subjects).await {
                    Ok(page) => page,
                    Err(error) => (vec![Err(error); subjects.len()], None),
                };
                let mut observations = Vec::new();
                for (id, reply) in chunk[offset..].iter().zip(entries) {
                    if reply.is_ok() {
                        reads_succeeded += 1;
                    }
                    let observation = match reply {
                        Ok(reply) if reply.absent => {
                            counts.refreshed += 1;
                            ClaimObservation::Absent {
                                stale: reply.stale,
                                self_author: reply.self_author,
                            }
                        }
                        Ok(reply) => {
                            match AgentClaim::decode(id, reply.body.as_deref().unwrap_or_default())
                            {
                                Ok(claim) if i64::try_from(reply.version).is_ok() => {
                                    counts.refreshed += 1;
                                    ClaimObservation::Present {
                                        claim: CachedClaim {
                                            holder: claim.holder,
                                            epoch: claim.epoch,
                                            cut_id: claim.cut_id,
                                            version: reply.version as i64,
                                            read_at_ms: 0,
                                            stale: reply.stale,
                                            integrity_failed: false,
                                        },
                                        self_author: reply.self_author,
                                    }
                                }
                                result => {
                                    counts.failed += 1;
                                    let code = result.err().unwrap_or("claim_malformed");
                                    match code {
                                        "claim_version_unknown" => {
                                            &self.health.claim_version_unknown
                                        }
                                        "claim_subject_mismatch" => {
                                            &self.health.claim_subject_mismatch
                                        }
                                        _ => &self.health.claim_malformed,
                                    }
                                    .fetch_add(1, Ordering::Relaxed);
                                    ClaimObservation::CodecFailed {
                                        self_author: reply.self_author,
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            counts.failed += 1;
                            if ["verify_failed", "decrypt_failed"].contains(&error.code()) {
                                ClaimObservation::IntegrityFailed
                            } else {
                                ClaimObservation::Unavailable
                            }
                        }
                    };
                    observations.push((id.clone(), observation));
                }
                // A shared write can own SQLite's writer connection while its
                // engram reply is pending. Waiting for that connection must not
                // block the executor that delivers the reply.
                let Some(cache) = self.store() else {
                    return Ok(counts);
                };
                let now = (self.clock)();
                let conflicts = tokio::task::spawn_blocking(move || {
                    cache.cache_agent_claims(&observations, now)
                })
                .await
                .map_err(|e| HandlerError::new("storage_error", e.to_string()))??;
                self.health
                    .self_author_conflicts
                    .fetch_add(conflicts as u64, Ordering::Relaxed);
                offset += next.unwrap_or(subjects.len());
            }
        }
        self.health
            .claim_refreshed
            .fetch_add(counts.refreshed as u64, Ordering::Relaxed);
        self.health
            .claim_failed
            .fetch_add(counts.failed as u64, Ordering::Relaxed);
        self.health
            .claim_refresh_failed
            .store(counts.failed > 0, Ordering::Relaxed);
        #[cfg(test)]
        self.health.claim_refresh_done.notify_waiters();
        if reads_succeeded == 0 && counts.failed > 0 {
            return Err(HandlerError::new(
                "engram_unavailable",
                "no claim record read succeeded",
            ));
        }
        Ok(counts)
    }
}

impl ProjectsHandler {
    pub(super) fn start_claim_refresh(&self) {
        let mut task = self.claim_refresh_task.lock().unwrap();
        if task.is_some() {
            return;
        }
        let mut receiver = self.claim_refresh.receiver.lock().unwrap().take().unwrap();
        let worker = Worker {
            store: Arc::downgrade(&self.store),
            records: self.record_client.clone(),
            health: self.health.clone(),
            clock: self.clock,
        };
        *task = Some(tokio::spawn(async move {
            while let Some(first) = receiver.recv().await {
                let mut waiters = first.into_iter().collect::<Vec<_>>();
                // Coalesce queued requests before beginning a pass. Requests
                // received during a pass remain queued for the following pass.
                while let Ok(request) = receiver.try_recv() {
                    waiters.extend(request);
                }
                let result = worker.pass().await;
                if result.is_err() {
                    worker
                        .health
                        .claim_refresh_failed
                        .store(true, Ordering::Relaxed);
                }
                for waiter in waiters {
                    let _ = waiter.send(match &result {
                        Ok(counts) => Ok(*counts),
                        Err(error) => Err(HandlerError {
                            code: error.code.clone(),
                            message: error.message.clone(),
                            detail: error.detail.clone(),
                        }),
                    });
                }
            }
        }));
    }

    pub(super) fn admit_claim_refresh(&self, key: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(REFRESH, admission.flow_id.as_deref())?;
        if admission.principal != Some(subc_protocol::Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "claim refresh requires Direct",
            ));
        }
        Ok(())
    }

    pub(super) async fn refresh_agent_claims(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_claim_refresh(key)?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {}
        let _: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if self.with_store(RegistryStore::identity_log_status)?.state != "enabled" {
            return Err(HandlerError::new(
                "identity_log_disabled",
                "claim refresh requires the identity log enabled",
            ));
        }
        self.start_claim_refresh();
        let (sender, receiver) = oneshot::channel();
        self.claim_refresh
            .sender
            .send(Some(sender))
            .map_err(|_| HandlerError::new("storage_unavailable", "claim refresh stopped"))?;
        let result = receiver
            .await
            .map_err(|_| HandlerError::new("storage_unavailable", "claim refresh stopped"))??;
        encode_result(result)
    }

    pub(super) fn agent_where(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            agent_id: String,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !entorhinal_core::agent::valid_agent_id(&request.agent_id) {
            return Err(HandlerError::new("invalid_request", "invalid agent_id"));
        }
        encode_result(self.with_agent_store(|store| store.agent_where(&request.agent_id))?)
    }
}

#[cfg(test)]
mod tests {
    use super::super::{
        fake_log::{Action, FakeLog},
        fake_record::{FakeRecord, FakeRecordConnector},
        record_client::RecordClient,
        sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
        tests::{reserved, scratch_descriptor},
        RouteAdmission,
    };
    use super::*;
    use serde_json::json;
    use subc_client_rs::{HandlerOutcome, ModuleHandler};
    use subc_protocol::Principal;
    const DIRECT: RouteKey = (60, 1);
    const HOST: RouteKey = (61, 1);
    const OTHER: RouteKey = (62, 1);
    struct Fixture {
        handler: Arc<ProjectsHandler>,
        record: Arc<FakeRecord>,
        log: FakeLog,
        descriptor: cortexkit_store_types::StorageDescriptor,
        dir: std::path::PathBuf,
    }
    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
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
            HandlerOutcome::Error { code, .. } => code,
            other => panic!("expected error: {other:?}"),
        }
    }
    impl Fixture {
        async fn new(enabled: bool) -> Self {
            let (dir, descriptor) = scratch_descriptor("claims");
            let log = FakeLog::default().with_author([1; 16]);
            let record = FakeRecord::new([1; 16]);
            let mut handler = ProjectsHandler::with_log_connector(
                "0123456789abcdef".into(),
                || 700,
                Arc::new(log.clone()),
            );
            handler.record_client =
                RecordClient::new(Arc::new(FakeRecordConnector(record.clone())));
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider("prefrontal-core".into()),
            );
            handler.provider_resolver = resolver;
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            handler.health.store_ready.store(true, Ordering::Relaxed);
            handler.route_admissions().insert(
                DIRECT,
                RouteAdmission::from_bind(Some(Principal::Direct), None),
            );
            handler.route_admissions().insert(
                HOST,
                RouteAdmission::from_bind(Some(reserved("prefrontal-core")), None),
            );
            handler.route_admissions().insert(
                OTHER,
                RouteAdmission::from_bind(Some(reserved("other")), None),
            );
            let f = Self {
                handler: Arc::new(handler),
                record,
                log,
                descriptor,
                dir,
            };
            if enabled {
                result(
                    f.call(
                        DIRECT,
                        "identity_log.enable",
                        json!({"without_agents":true}),
                    )
                    .await,
                );
                assert_eq!(
                    result(f.call(DIRECT, REFRESH, json!({})).await),
                    json!({"refreshed":0,"failed":0})
                );
            }
            f
        }
        async fn call(&self, key: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(&body(method, params), key)
                .await
        }
        async fn create(&self, n: usize) -> String {
            result(self.call(HOST,"agent.create",json!({"name":format!("Agent {n}"),"role":"assistant","tag":"test","request_key":format!("create-{n}")})).await)["agent"]["agent_id"].as_str().unwrap().into()
        }
        fn put(&self, id: &str, holder: &str, epoch: i64) {
            self.record.put(&format!("claim:{id}"),1,serde_json::to_vec(&json!({"v":1,"agent_id":id,"holder":holder,"epoch":epoch,"cut_id":null,"reason":null})).unwrap(),holder);
        }
        async fn refresh(&self) -> Value {
            result(self.call(DIRECT, REFRESH, json!({})).await)
        }
        fn where_(&self, id: &str) -> Value {
            result(
                self.handler
                    .handle_request(&body(WHERE, json!({"agent_id":id})), OTHER),
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn refresh_admission_empty_disabled_and_sync_seam_are_closed() {
        let f = Fixture::new(false).await;
        assert_eq!(
            code(f.call(OTHER, REFRESH, json!({})).await),
            "caller_forbidden"
        );
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({"extra":1})).await),
            "invalid_request"
        );
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({})).await),
            "identity_log_disabled"
        );
        assert_eq!(
            code(f.handler.handle_request(&body(REFRESH, json!({})), OTHER)),
            "caller_forbidden"
        );
        assert_eq!(
            code(f.handler.handle_request(&body(REFRESH, json!({})), DIRECT)),
            "served_path_required"
        );
        let mut admission = RouteAdmission::from_bind(Some(Principal::Direct), None);
        admission.flow_id = Some("flow".into());
        f.handler.route_admissions().insert(DIRECT, admission);
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({})).await),
            "flow_scope_not_admitted"
        );
        let f = Fixture::new(true).await;
        assert_eq!(f.refresh().await, json!({"refreshed":0,"failed":0}));
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 0);
    }
    #[tokio::test]
    async fn refresh_batches_live_agents_at_64_and_where_never_calls_record() {
        let f = Fixture::new(true).await;
        let mut agents = Vec::new();
        for n in 0..65 {
            agents.push(f.create(n).await);
        }
        let own = f.record.author.clone();
        f.put(&agents[0], &own, 1);
        f.put(&agents[1], "02020202020202020202020202020202", 3);
        assert_eq!(f.where_(&agents[0])["known"], false);
        assert_eq!(f.refresh().await, json!({"refreshed":65,"failed":0}));
        assert_eq!(
            f.record
                .batches
                .lock()
                .unwrap()
                .iter()
                .map(Vec::len)
                .collect::<Vec<_>>(),
            vec![64, 1]
        );
        let calls = f.record.calls.load(Ordering::Relaxed);
        assert_eq!(f.where_(&agents[0])["status"], "held_here");
        assert_eq!(f.where_(&agents[1])["status"], "held_elsewhere");
        let unclaimed = f.where_(&agents[2]);
        assert_eq!(unclaimed["known"], true);
        assert_eq!(unclaimed["holder"], Value::Null);
        assert_eq!(unclaimed["cut_id"], Value::Null);
        assert_eq!(f.record.calls.load(Ordering::Relaxed), calls);
        assert_eq!(
            f.handler
                .with_store(|s| s.cached_agent_claim(&agents[2]))
                .unwrap()
                .unwrap()
                .version,
            0
        );
        result(
            f.call(
                HOST,
                "agent.dispose",
                json!({"agent_id":agents[2],"request_key":"retire"}),
            )
            .await,
        );
        f.record.batches.lock().unwrap().clear();
        assert_eq!(f.refresh().await["refreshed"], 64);
        assert!(!f.record.batches.lock().unwrap()[0].contains(&format!("claim:{}", agents[2])));
    }
    #[tokio::test]
    async fn refresh_integrity_and_all_codec_errors_are_persisted_and_cleared() {
        let f = Fixture::new(true).await;
        let mut ids = Vec::new();
        for n in 0..5 {
            ids.push(f.create(n).await);
        }
        for id in &ids {
            f.put(id, &f.record.author, 1);
        }
        f.refresh().await;
        f.record
            .error(&format!("claim:{}", ids[0]), Some("verify_failed"));
        f.record
            .error(&format!("claim:{}", ids[1]), Some("decrypt_failed"));
        for (id, bytes) in [
            (ids[2].clone(), b"{\"v\":2}".to_vec()),
            (
                ids[3].clone(),
                serde_json::to_vec(&json!({"v":1,"agent_id":"other","epoch":0,"holder":null}))
                    .unwrap(),
            ),
            (ids[4].clone(), b"bad".to_vec()),
        ] {
            f.record
                .put(&format!("claim:{id}"), 2, bytes, &f.record.author);
        }
        assert_eq!(f.refresh().await, json!({"refreshed":0,"failed":5}));
        for id in &ids {
            assert_eq!(
                code(
                    f.handler
                        .handle_request(&body(WHERE, json!({"agent_id":id})), OTHER)
                ),
                "claim_integrity_failed"
            );
        }
        assert_eq!(
            f.handler
                .health
                .claim_version_unknown
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            f.handler
                .health
                .claim_subject_mismatch
                .load(Ordering::Relaxed),
            1
        );
        assert_eq!(f.handler.health.claim_malformed.load(Ordering::Relaxed), 1);
        for id in &ids {
            f.record.error(&format!("claim:{id}"), None);
            f.record.put(&format!("claim:{id}"),3,serde_json::to_vec(&json!({"v":1,"agent_id":id,"holder":f.record.author,"epoch":1,"cut_id":null,"reason":null})).unwrap(),&f.record.author);
        }
        assert_eq!(f.refresh().await["refreshed"], 5);
        for id in &ids {
            assert_eq!(f.where_(id)["status"], "held_here");
        }
    }
    #[tokio::test]
    async fn refresh_conflict_is_health_error_and_does_not_replace_self_author() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.put(&id, &f.record.author, 1);
        f.refresh().await;
        let other = FakeRecord::new([2; 16]);
        let worker = Worker {
            store: Arc::downgrade(&f.handler.store),
            records: RecordClient::new(Arc::new(FakeRecordConnector(other))),
            health: f.handler.health.clone(),
            clock: f.handler.clock,
        };
        assert_eq!(worker.pass().await.unwrap().refreshed, 1);
        assert_eq!(
            f.handler
                .with_store(RegistryStore::persisted_self_author)
                .unwrap()
                .as_deref(),
            Some(f.record.author.as_str())
        );
        let health = f.handler.health().await;
        assert_eq!(
            health.metrics.unwrap()["claimError"],
            "self_author_conflict"
        );
        assert_eq!(f.handler.health.log_state.load(Ordering::Relaxed), 0);
    }
    #[tokio::test(start_paused = true)]
    async fn refresh_failure_reconnect_signals_exactly_one_pass_without_private_timer() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.record.error(&format!("claim:{id}"), Some("unavailable"));
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({})).await),
            "engram_unavailable"
        );
        assert_eq!(f.handler.health.log_state.load(Ordering::Relaxed), 0);
        let before = f.record.calls.load(Ordering::Relaxed);
        f.record.error(&format!("claim:{id}"), None);
        // The fake call counter advances before the blocking cache transaction
        // commits. Wait for that commit before asserting the cache answer.
        let committed = f.handler.health.claim_refresh_done.notified();
        tokio::pin!(committed);
        committed.as_mut().enable();
        f.handler.start_catch_up();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            f.record.wait_for_calls(before + 1),
        )
        .await
        .expect("the reconnect must schedule a refresh pass");
        committed.await;
        assert_eq!(f.record.calls.load(Ordering::Relaxed), before + 1);
        let log_calls = f.log.calls();
        tokio::time::advance(std::time::Duration::from_secs(30)).await;
        f.log.wait_for_calls(log_calls + 1).await;
        tokio::task::yield_now().await;
        assert_eq!(f.record.calls.load(Ordering::Relaxed), before + 1);
        assert_eq!(f.where_(&id)["known"], true);
    }
    #[tokio::test(start_paused = true)]
    async fn refresh_received_during_inflight_waits_for_next_pass_without_duplication() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.record.hung.store(true, Ordering::Relaxed);
        let handler = f.handler.clone();
        let first =
            tokio::spawn(async move { handler.refresh_agent_claims(json!({}), DIRECT).await });
        f.record.wait_for_calls(1).await;
        let handler = f.handler.clone();
        let (sent, received) = oneshot::channel();
        let second = tokio::spawn(async move {
            sent.send(()).unwrap();
            handler.refresh_agent_claims(json!({}), DIRECT).await
        });
        received.await.unwrap();
        tokio::task::yield_now().await;
        f.record.hung.store(false, Ordering::Relaxed);
        tokio::time::advance(super::super::catch_up::READ_DEADLINE).await;
        assert_eq!(first.await.unwrap().unwrap_err().code, "engram_unavailable");
        let reply = second.await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&reply).unwrap()["result"],
            json!({"refreshed":1,"failed":0})
        );
        assert_eq!(f.record.calls.load(Ordering::Relaxed), 2);
        assert_eq!(f.where_(&id)["known"], true);
    }
    #[tokio::test(start_paused = true)]
    async fn hung_catchup_and_record_do_not_block_registration_or_cached_reads() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.record.hung.store(true, Ordering::Relaxed);
        let h = f.handler.clone();
        let refresh = tokio::spawn(async move { h.refresh_agent_claims(json!({}), DIRECT).await });
        f.record.wait_for_calls(1).await;
        let calls = f.log.calls();
        f.log.on_read(Action::Hang);
        f.handler.start_catch_up();
        f.log.wait_for_calls(calls + 1).await;
        assert!(f.handler.writer.try_lock().is_err());
        let params =
            json!({"agent_id":id,"ref_kind":"test","ref":"r","harness":"pi","session_id":"s"});
        let outcome = tokio::time::timeout(
            std::time::Duration::from_secs(1),
            f.call(HOST, "agent.register_session", params),
        )
        .await
        .expect("registration must not wait on the writer");
        assert_eq!(result(outcome)["agent_id"], id);
        for (method, params) in [
            (WHERE, json!({"agent_id":id})),
            ("agent.resolve", json!({"agent_id":id})),
            (
                "agent.for_session",
                json!({"harness":"pi","session_id":"s"}),
            ),
            (
                "agent.session_liveness",
                json!({"refs":[{"ref_kind":"test","ref":"r"}]}),
            ),
            ("enumerate", json!({})),
            ("identity_log.status", json!({})),
        ] {
            result(
                tokio::time::timeout(
                    std::time::Duration::from_secs(1),
                    f.call(OTHER, method, params),
                )
                .await
                .expect("read must not wait on the writer"),
            );
        }
        assert!(f.handler.writer.try_lock().is_err());
        f.handler.health().await;
        tokio::time::advance(super::super::catch_up::READ_DEADLINE).await;
        assert_eq!(
            refresh.await.unwrap().unwrap_err().code,
            "engram_unavailable"
        );
    }
    #[tokio::test]
    async fn cached_here_survives_restart_with_failed_records_and_verify_rebuild() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.put(&id, &f.record.author, 1);
        f.refresh().await;
        assert!(f.handler.with_store(RegistryStore::verify).unwrap().ok);
        f.handler.with_store(RegistryStore::rebuild).unwrap();
        assert_eq!(f.where_(&id)["status"], "held_here");
        let descriptor = f.descriptor.clone();
        let record = f.record.clone();
        let log = f.log.clone();
        if let Some(task) = f.handler.claim_refresh_task.lock().unwrap().take() {
            task.abort();
        }
        f.handler.store.lock().unwrap().take();
        tokio::task::yield_now().await;
        let mut handler =
            ProjectsHandler::with_log_connector("abcdef0123456789".into(), || 701, Arc::new(log));
        record.offline.store(true, Ordering::Relaxed);
        handler.record_client = RecordClient::new(Arc::new(FakeRecordConnector(record.clone())));
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        let resolver = FakeResolver::new();
        resolver.set(
            ORCHESTRATOR_CAPABILITY,
            ProviderResolution::Provider("prefrontal-core".into()),
        );
        handler.provider_resolver = resolver;
        handler.route_admissions().insert(
            HOST,
            RouteAdmission::from_bind(Some(reserved("prefrontal-core")), None),
        );
        assert_eq!(
            result(handler.handle_request(&body(WHERE, json!({"agent_id":id})), OTHER))["status"],
            "held_here"
        );
        let calls = record.calls.load(Ordering::Relaxed);
        assert_eq!(result(handler.handle_served_request(&body("agent.register_session",json!({"agent_id":id,"ref_kind":"test","ref":"ref","harness":"pi","session_id":"s"})),HOST).await)["agent_id"],id);
        assert_eq!(record.calls.load(Ordering::Relaxed), calls);
        drop(handler);
    }

    #[tokio::test(start_paused = true)]
    async fn hello_ack_startup_refreshes_persisted_live_agents() {
        let f = Fixture::new(true).await;
        let id = f.create(1).await;
        f.put(&id, &f.record.author, 1);
        f.refresh().await;
        if let Some(task) = f.handler.claim_refresh_task.lock().unwrap().take() {
            task.abort();
        }
        f.handler.store.lock().unwrap().take();
        tokio::task::yield_now().await;
        let peer = FakeRecord::new([1; 16]);
        let holder = "02020202020202020202020202020202";
        peer.put(
            &format!("claim:{id}"),
            2,
            serde_json::to_vec(
                &json!({"v":1,"agent_id":id,"holder":holder,"epoch":2,"cut_id":null,"reason":null}),
            )
            .unwrap(),
            holder,
        );
        let mut handler = ProjectsHandler::with_log_connector(
            "abcdef0123456789".into(),
            || 701,
            Arc::new(f.log.clone()),
        );
        handler.record_client = RecordClient::new(Arc::new(FakeRecordConnector(peer.clone())));
        handler.route_admissions().insert(
            DIRECT,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        handler
            .on_hello_ack(&super::super::tests::hello_ack(&f.descriptor))
            .await;
        tokio::time::timeout(std::time::Duration::from_secs(1), peer.wait_for_calls(1))
            .await
            .expect("startup must schedule a refresh");
        handler
            .refresh_agent_claims(json!({}), DIRECT)
            .await
            .unwrap();
        assert_eq!(peer.calls.load(Ordering::Relaxed), 2);
        assert_eq!(
            result(handler.handle_request(&body(WHERE, json!({"agent_id":id})), OTHER))["holder"],
            holder
        );
        drop(handler);
    }
}

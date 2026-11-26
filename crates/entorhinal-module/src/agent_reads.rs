//! Serves identity reads and the bounded, notification-driven change feed.
//! Core owns the wire bodies and consistent store snapshots; waiting belongs to
//! the process so a held poll never blocks a resolve or a committing writer.

use std::time::Duration;

use async_trait::async_trait;
use entorhinal_core::{agent::AgentChangesReply, RegistryStore};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;

use super::{encode_result, HandlerError, ProjectsHandler, RouteKey, WireRequest};

pub(super) const READ_METHODS: &[&str] = &[
    "node.self",
    "agent.preview_repair_workspaces",
    "agent.preview_avatar_manifest",
    "agent.resolve",
    "agent.resolve_name",
    "agent.list",
    "agent.peer_roster",
    "agent.avatar_read",
    "agent.github_identity",
    "agent.fleet_identity",
    "agent.snapshot",
    "agent.changes",
];

const WAIT_TIMEOUT: Duration = Duration::from_secs(25);

/// How long a served read may run on the blocking pool before its caller is
/// refused. Under the daemon's 30 s call timeout, so the caller hears a named,
/// retryable refusal instead of timing out itself.
pub(super) const SERVED_READ_DEADLINE: Duration = Duration::from_secs(20);

/// Warns once if a served handler lacks its self-handle.
static MISSING_SELF_HANDLE: std::sync::Once = std::sync::Once::new();

#[cfg(test)]
thread_local! {
    /// Blocking-pool reads started on this thread whose results have not been
    /// taken yet. A watch test polls a watch until this is zero, since the
    /// watch's snapshot now finishes on another thread.
    pub(super) static READS_IN_FLIGHT: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Counts one read in `READS_IN_FLIGHT` while it lives.
struct InFlight;

impl InFlight {
    fn start() -> Self {
        #[cfg(test)]
        READS_IN_FLIGHT.with(|count| count.set(count.get() + 1));
        Self
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        #[cfg(test)]
        READS_IN_FLIGHT.with(|count| count.set(count.get().saturating_sub(1)));
    }
}

/// Separating the wait clock from row timestamps permits deterministic timeout
/// tests without delaying the suite or making wall-clock jumps affect a poll.
#[async_trait]
pub(super) trait FeedClock: Send + Sync {
    async fn sleep(&self, duration: Duration);
}

pub(super) struct TokioFeedClock;

#[async_trait]
impl FeedClock for TokioFeedClock {
    async fn sleep(&self, duration: Duration) {
        tokio::time::sleep(duration).await;
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyRequest {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FleetRequest {
    // Tokens are opaque. Even a non-string token is a cache miss, not a refusal.
    token: Option<Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ChangesRequest {
    incarnation: String,
    cursor: i64,
    limit: Option<i64>,
    wait: Option<bool>,
}

fn decode<T: DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))
}

impl ProjectsHandler {
    /// The transport and asynchronous test seam both use this path. Ordinary
    /// requests retain the synchronous path; only changes may suspend a task.
    pub(super) async fn handle_request_wait(
        &self,
        body: &[u8],
        key: RouteKey,
    ) -> subc_client_rs::HandlerOutcome {
        let request = match serde_json::from_slice::<WireRequest>(body) {
            Ok(request) => request,
            Err(_) => return self.handle_request(body, key),
        };
        let result = if request.method == "agent.changes" {
            match self.admit(&request.method, key) {
                Ok(_) => self.agent_changes_wait(request.params).await,
                Err(error) => Err(error),
            }
        } else {
            self.execute_off_runtime(request, key).await
        };
        match result {
            Ok(body) => subc_client_rs::HandlerOutcome::Response(body),
            Err(error) => error.into_outcome(),
        }
    }

    /// Run one served request's synchronous handling, store reads included, on
    /// the blocking pool, so a slow query or a stalled disk occupies a
    /// blocking thread and one reader, never a runtime worker.
    async fn execute_off_runtime(
        &self,
        request: WireRequest,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        let Some(handler) = self.this.get().and_then(std::sync::Weak::upgrade) else {
            // Only a handler a unit test built by value, without
            // `into_served`, gets here; `serve_module` always installs the
            // handle. Such tests keep the inline path.
            MISSING_SELF_HANDLE.call_once(|| {
                tracing::warn!(
                    target: "store",
                    "served handler has no self-handle; serving reads inline"
                );
            });
            return self.execute(request, key);
        };
        let method = request.method.clone();
        let job = tokio::task::spawn_blocking(
            self.served_reads
                .tracked(move || handler.execute(request, key)),
        );
        self.finish_read(&method, job).await
    }

    /// Run one store job on the blocking pool under the served-read deadline,
    /// for reads that finish their reply on the async side.
    pub(super) async fn read_blocking<T: Send + 'static>(
        &self,
        method: &str,
        job: impl FnOnce(&RegistryStore) -> Result<T, HandlerError> + Send + 'static,
    ) -> Result<T, HandlerError> {
        let store = self.with_agent_store(|store| Ok(store.clone()))?;
        let _in_flight = InFlight::start();
        let job = tokio::task::spawn_blocking(self.served_reads.tracked(move || job(&store)));
        self.finish_read(method, job).await
    }

    async fn finish_read<T>(
        &self,
        method: &str,
        job: tokio::task::JoinHandle<Result<T, HandlerError>>,
    ) -> Result<T, HandlerError> {
        match tokio::time::timeout(SERVED_READ_DEADLINE, job).await {
            Ok(Ok(result)) => result,
            Ok(Err(error)) => Err(HandlerError::new(
                "storage_error",
                format!("read did not finish: {error}"),
            )),
            Err(_) => {
                // The read is abandoned, not cancelled: a blocking job cannot
                // be stopped, so it runs to completion, still counted in
                // `served_reads`, and draining waits for it.
                tracing::warn!(target: "store", method, "served read passed its deadline");
                Err(HandlerError::new(
                    "read_timeout",
                    format!(
                        "'{method}' did not finish within {} s; retry",
                        SERVED_READ_DEADLINE.as_secs()
                    ),
                ))
            }
        }
    }

    /// Insert the volatile nonce only after core serializes its result. Keeping
    /// it out of core structs preserves project goldens and persistent caches.
    pub(super) fn attach_incarnation(&self, body: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
        let mut body: Value = serde_json::from_slice(&body)
            .map_err(|error| HandlerError::new("encode_failed", error.to_string()))?;
        let result = body
            .get_mut("result")
            .and_then(Value::as_object_mut)
            .ok_or_else(|| {
                HandlerError::new("encode_failed", "read reply must have a result object")
            })?;
        result.insert(
            "incarnation".into(),
            Value::String(self.incarnation.clone()),
        );
        serde_json::to_vec(&body)
            .map_err(|error| HandlerError::new("encode_failed", error.to_string()))
    }

    pub(super) fn encode_read_result<T: serde::Serialize>(
        &self,
        reply: T,
    ) -> Result<Vec<u8>, HandlerError> {
        self.attach_incarnation(encode_result(reply)?)
    }

    pub(super) fn with_agent_store<T>(
        &self,
        read: impl FnOnce(&RegistryStore) -> Result<T, entorhinal_core::agent::AgentMutationError>,
    ) -> Result<T, HandlerError> {
        let store = self.health.store_refusal.lookup(&self.store, || {
            HandlerError::new("storage_unavailable", "agent identity storage is not ready")
        })?;
        read(&store).map_err(HandlerError::from)
    }

    pub(super) fn agent_read(&self, method: &str, params: Value) -> Result<Vec<u8>, HandlerError> {
        match method {
            "node.self" => {
                let _: EmptyRequest = decode(params)?;
                #[derive(serde::Serialize)]
                struct SelfReply<'a> {
                    #[serde(flatten)]
                    node: entorhinal_core::claim_cache::NodeSelf,
                    #[serde(skip_serializing_if = "Option::is_none")]
                    box_id: Option<&'a str>,
                }
                encode_result(SelfReply {
                    node: self.with_store(RegistryStore::node_self)?,
                    box_id: self.health.origin.daemon_id(),
                })
            }
            "agent.snapshot" => {
                let _: EmptyRequest = decode(params)?;
                self.encode_read_result(self.with_agent_store(RegistryStore::agent_snapshot)?)
            }
            "agent.fleet_identity" => {
                let request: FleetRequest = decode(params)?;
                self.with_agent_store(|store| {
                    store.agent_fleet_identity(
                        request.token.as_ref().and_then(Value::as_str),
                        &self.incarnation,
                    )
                })
            }
            // Synchronous callers can scan, but only the async serving path holds
            // a wait. The transport always enters handle_request_wait above.
            "agent.changes" => {
                let request: ChangesRequest = decode(params)?;
                self.encode_read_result(self.scan_changes(&request)?)
            }
            _ => self.attach_incarnation(
                self.with_agent_store(|store| store.agent_read(method, params))?,
            ),
        }
    }

    fn scan_changes(&self, request: &ChangesRequest) -> Result<AgentChangesReply, HandlerError> {
        if request.incarnation != self.incarnation {
            return Err(HandlerError::new(
                "snapshot_required",
                "incarnation changed; take a new snapshot",
            ));
        }
        self.with_agent_store(|store| store.agent_changes(request.cursor, request.limit))
    }

    /// `scan_changes` with its store read on the blocking pool, for the
    /// long-poll, which runs on the async side between scans.
    async fn scan_changes_off_runtime(
        &self,
        request: &ChangesRequest,
    ) -> Result<AgentChangesReply, HandlerError> {
        if request.incarnation != self.incarnation {
            return Err(HandlerError::new(
                "snapshot_required",
                "incarnation changed; take a new snapshot",
            ));
        }
        let (cursor, limit) = (request.cursor, request.limit);
        self.read_blocking("agent.changes", move |store| {
            store
                .agent_changes(cursor, limit)
                .map_err(HandlerError::from)
        })
        .await
    }

    async fn agent_changes_wait(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: ChangesRequest = decode(params)?;
        // Register before scanning: a commit between the scan and suspension
        // must not be lost. notify_waiters wakes every consumer, not just one.
        let notified = self.commits.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let mut reply = self.scan_changes_off_runtime(&request).await?;
        if !request.wait.unwrap_or(true)
            || reply.cursor != request.cursor
            || !reply.entries.is_empty()
        {
            return self.encode_read_result(reply);
        }
        let Ok(_permit) = self.feed_waits.try_acquire() else {
            return self.encode_read_result(reply);
        };
        let timeout = self.feed_clock.sleep(WAIT_TIMEOUT);
        tokio::pin!(timeout);
        loop {
            tokio::select! {
                // Re-read even at the deadline: a concurrently committed entry
                // wins over an empty timeout response.
                _ = &mut timeout => return self.encode_read_result(self.scan_changes_off_runtime(&request).await?),
                _ = &mut notified => {}
            }
            notified.set(self.commits.notified());
            notified.as_mut().enable();
            reply = self.scan_changes_off_runtime(&request).await?;
            if reply.cursor != request.cursor || !reply.entries.is_empty() {
                return self.encode_read_result(reply);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        collections::BTreeSet,
        future::{poll_fn, Future},
        path::PathBuf,
        pin::Pin,
        sync::{
            atomic::{AtomicU64, Ordering},
            Arc, Mutex,
        },
        task::Poll,
    };

    use cortexkit_store_types::{Isolation, StorageBackend, StorageDescriptor};
    use serde_json::json;
    use subc_client_rs::HandlerOutcome;
    use subc_protocol::{ErrorBody, Principal};

    use super::*;
    use crate::{
        tests::{flow_stamp, reserved},
        RouteAdmission, WRITER_MODULE,
    };

    const INCARNATION: &str = "0123456789abcdef";
    const OPERATOR: RouteKey = (40, 1);
    const READER: RouteKey = (41, 1);
    const UNKNOWN: &str = "agent_0000000000000000";
    static COUNTER: AtomicU64 = AtomicU64::new(0);

    struct Fixture {
        root: PathBuf,
        descriptor: StorageDescriptor,
        handler: ProjectsHandler,
    }

    impl Fixture {
        fn new(activated: bool) -> Self {
            let id = format!(
                "{}-{}",
                std::process::id(),
                COUNTER.fetch_add(1, Ordering::Relaxed)
            );
            let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../target/module-agent-reads")
                .join(&id);
            std::fs::create_dir_all(&root).unwrap();
            let descriptor = StorageDescriptor {
                module_id: "entorhinal".into(),
                storage_namespace: id,
                isolation: Isolation::Module,
                backend: StorageBackend::Sqlite {
                    path: root.join("store.db").to_string_lossy().into_owned(),
                },
            };
            let handler = ProjectsHandler::with_runtime(INCARNATION.into(), || 700);
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            handler.route_admissions().insert(
                OPERATOR,
                RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), None),
            );
            let f = Self {
                root,
                descriptor,
                handler,
            };
            if activated {
                // A source with no `agent` table is a valid import of zero agents: an
                // install that never created agents still has to record the cutover.
                let source_path = f.root.join("snapshot.db");
                let mut source_descriptor = f.descriptor.clone();
                source_descriptor.storage_namespace.push_str("-source");
                source_descriptor.backend = StorageBackend::Sqlite {
                    path: source_path.to_string_lossy().into_owned(),
                };
                let source = RegistryStore::open(&source_descriptor).unwrap();
                source
                    .apply_entry("fixture", "{}", "fixture", None, |tx| {
                        tx.execute_batch("DROP TABLE agent_name_claim; DROP TABLE agent;")
                    })
                    .unwrap();
                drop(source);
                let imported = f.ok(
                    "agent.import",
                    json!({"snapshot_path":source_path,"request_key":"cutover"}),
                );
                assert_eq!(
                    imported,
                    json!({"agents_imported":0,"claims_imported":0,"generation":1,"incarnation":INCARNATION})
                );
            }
            f
        }

        fn outcome(&self, route: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler.handle_request(&body(method, params), route)
        }

        fn ok(&self, method: &str, params: Value) -> Value {
            success(self.outcome(OPERATOR, method, params))
        }

        fn fail(&self, method: &str, params: Value, code: &str) -> ErrorBody {
            let head = self.head();
            let error = wire_error(self.outcome(OPERATOR, method, params));
            assert_eq!(error.code, code, "{method}: {}", error.message);
            assert_eq!(self.head(), head, "a refusal must not append");
            error
        }

        fn create(&self, name: &str) -> String {
            self.ok("agent.create", json!({"name":name,"role":"assistant","tag":"test","request_key":format!("create-{name}")}))["agent"]["agent_id"].as_str().unwrap().into()
        }

        fn project(&self, id: &str, workspace: Option<&str>) -> String {
            let path = self.root.join(id);
            std::fs::create_dir_all(&path).unwrap();
            let root = RegistryStore::canonical_mutation_root(path.to_str().unwrap()).unwrap();
            self.handler
                .seed_project(
                    json!({"projectId":id,"name":id,"roots":[root],"workspaceId":workspace}),
                    "reserved:prefrontal",
                )
                .unwrap();
            root
        }

        fn head(&self) -> i64 {
            self.handler.with_store(RegistryStore::generation).unwrap()
        }

        fn sql(&self, sql: &str) {
            self.handler
                .with_store(|store| {
                    store.apply_entry("fixture", "{}", "fixture", None, |tx| tx.execute_batch(sql))
                })
                .unwrap();
        }

        fn restart(&mut self) {
            drop(self.handler.store.lock().unwrap().take());
            self.handler = ProjectsHandler::with_runtime("fedcba9876543210".into(), || 701);
            *self.handler.store.lock().unwrap() =
                Some(RegistryStore::open(&self.descriptor).unwrap());
            self.handler.route_admissions().insert(
                OPERATOR,
                RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), None),
            );
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            drop(self.handler.store.lock().unwrap().take());
            std::fs::remove_dir_all(&self.root).unwrap();
        }
    }

    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
    }

    async fn request(handler: &ProjectsHandler, method: &str, params: Value) -> HandlerOutcome {
        handler
            .handle_request_wait(&body(method, params), READER)
            .await
    }

    fn success(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(body) => {
                serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
            }
            HandlerOutcome::Error { code, message } => panic!("{code}: {message}"),
            HandlerOutcome::ErrorWithDetail {
                code,
                message,
                detail,
            } => panic!("{code}: {message}: {detail}"),
            HandlerOutcome::Streamed => panic!("identity replies do not stream"),
        }
    }

    fn wire_error(outcome: HandlerOutcome) -> ErrorBody {
        let error = match outcome {
            HandlerOutcome::Error { code, message } => ErrorBody::new(code, message),
            HandlerOutcome::ErrorWithDetail {
                code,
                message,
                detail,
            } => ErrorBody::new(code, message).with_detail(detail),
            _ => panic!("expected an error, never a status or success"),
        };
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Decoder {
            code: String,
            message: String,
            detail: Option<Value>,
        }
        let decoded: Decoder =
            serde_json::from_slice(&serde_json::to_vec(&error).unwrap()).unwrap();
        ErrorBody {
            code: decoded.code,
            message: decoded.message,
            detail: decoded.detail,
        }
    }

    fn keys(value: &Value, expected: &[&str]) {
        assert_eq!(
            value
                .as_object()
                .unwrap()
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            expected.iter().copied().collect(),
            "{value}"
        );
    }

    fn fleet_keys(value: &Value, expected: &[&str]) {
        let mut expected = expected.to_vec();
        expected.extend(["incarnation", "generation"]);
        keys(value, &expected);
        assert_eq!(value["incarnation"], INCARNATION);
        assert!(value["generation"].is_i64());
        absent_runtime_fields(value);
    }

    fn absent_runtime_fields(value: &Value) {
        match value {
            Value::Object(object) => {
                for (key, child) in object {
                    assert!(
                        ![
                            "noop",
                            "reachability",
                            "persona_ref",
                            "residence",
                            "sleep",
                            "wake_policy",
                            "wake_policy_version",
                            "bounced_deliveries"
                        ]
                        .contains(&key.as_str()),
                        "unexpected key {key}"
                    );
                    absent_runtime_fields(child);
                }
            }
            Value::Array(array) => array.iter().for_each(absent_runtime_fields),
            _ => (),
        }
    }

    fn changes(cursor: i64, wait: Option<bool>) -> Value {
        let mut params = json!({"incarnation":INCARNATION,"cursor":cursor});
        if let Some(wait) = wait {
            params["wait"] = json!(wait);
        }
        params
    }

    #[test]
    fn node_self_sharing_off_has_no_author_or_name_and_does_not_write() {
        let f = Fixture::new(false);
        f.sql("INSERT INTO member_name(author,display_name) VALUES('1111111111111111','Other machine');");
        let before = f.ok("journal_tail", json!({"afterSeq":0}));
        assert_eq!(
            f.ok("node.self", json!({})),
            json!({"author":null,"display_name":null,"sharing":"disabled"})
        );
        assert_eq!(f.ok("journal_tail", json!({"afterSeq":0})), before);
    }

    #[test]
    fn node_self_sharing_on_uses_own_member_name() {
        let f = Fixture::new(true);
        f.sql("UPDATE identity_log_state SET state='enabled' WHERE id=1;
            INSERT INTO member_name(author,display_name) VALUES('1111111111111111','Other machine');
            INSERT INTO member_name(author,display_name) VALUES('2222222222222222','Workshop machine');
            INSERT INTO self_identity(id,self_author,learned_at_ms) VALUES(1,'2222222222222222',700);");
        let reply = f.ok("node.self", json!({}));
        assert_eq!(
            reply,
            json!({"author":"2222222222222222","display_name":"Workshop machine","sharing":"enabled"})
        );
    }

    #[test]
    fn node_self_sharing_on_without_own_name_ignores_other_member_name() {
        let f = Fixture::new(false);
        f.sql("UPDATE identity_log_state SET state='enabled' WHERE id=1;
            INSERT INTO self_identity(id,self_author,learned_at_ms) VALUES(1,'2222222222222222',700);
            INSERT INTO member_name(author,display_name) VALUES('1111111111111111','Other machine');");
        assert_eq!(
            f.ok("node.self", json!({})),
            json!({"author":"2222222222222222","display_name":null,"sharing":"enabled"})
        );
    }

    #[test]
    fn node_self_reports_bootstrap_states_without_an_author() {
        let f = Fixture::new(false);
        for state in ["enabling", "joining", "enabled"] {
            f.sql(&format!(
                "UPDATE identity_log_state SET state='{state}' WHERE id=1"
            ));
            assert_eq!(
                f.ok("node.self", json!({})),
                json!({"author":null,"display_name":null,"sharing":state})
            );
        }
    }

    #[test]
    fn node_self_refuses_unknown_params_and_unavailable_storage() {
        let f = Fixture::new(false);
        for params in [json!({"unknown":true}), Value::Null, json!("invalid")] {
            f.fail("node.self", params, "invalid_request");
        }
        let unavailable = ProjectsHandler::with_runtime(INCARNATION.into(), || 700);
        assert_eq!(
            wire_error(unavailable.handle_request(&body("node.self", json!({})), READER)).code,
            "storage_unavailable"
        );
        f.sql("DROP TABLE member_name");
        f.fail("node.self", json!({}), "storage_error");
    }

    #[tokio::test]
    async fn all_reads_are_open_to_every_principal_before_and_after_cutover() {
        for activated in [false, true] {
            let f = Fixture::new(activated);
            for principal in [
                Some(reserved(WRITER_MODULE)),
                Some(Principal::Direct),
                Some(reserved("callosum")),
                Some(reserved("other")),
                Some(Principal::Unverified),
                None,
            ] {
                for flow in [false, true] {
                    f.handler.route_admissions().insert(
                        READER,
                        RouteAdmission::from_bind(
                            principal.clone(),
                            flow.then(|| flow_stamp(Some("fl_read"))).as_ref(),
                        ),
                    );
                    for (method, params) in read_requests(f.head(), &f.root) {
                        let outcome = request(&f.handler, method, params).await;
                        match method {
                            "agent.peer_roster" if !activated => {
                                assert_eq!(wire_error(outcome).code, "registry_not_activated")
                            }
                            "agent.github_identity" => {
                                assert_eq!(wire_error(outcome).code, "unknown_agent")
                            }
                            "node.self" => {
                                assert_eq!(
                                    success(outcome),
                                    json!({"author":null,"display_name":null,"sharing":"disabled"})
                                );
                            }
                            _ => {
                                let result = success(outcome);
                                assert_eq!(result["incarnation"], INCARNATION, "{method}");
                                assert_eq!(result["generation"], f.head(), "{method}");
                            }
                        }
                    }
                }
            }
            assert_eq!(
                f.ok("agent.list", json!({"activated_only":true})),
                json!({"agents":[],"incarnation":INCARNATION,"generation":f.head()})
            );
            assert_eq!(
                f.ok("agent.resolve", json!({"agent_id":UNKNOWN})),
                json!({"agent_id":UNKNOWN,"status":"unknown","merged_into":null,"gone":null,"incarnation":INCARNATION,"generation":f.head()})
            );
        }
    }

    fn read_requests(cursor: i64, root: &std::path::Path) -> Vec<(&'static str, Value)> {
        vec![
            ("node.self", json!({})),
            ("agent.resolve", json!({"agent_id":UNKNOWN})),
            ("agent.resolve_name", json!({"name":"Missing"})),
            ("agent.list", json!({})),
            ("agent.peer_roster", json!({"workspace_id":"W"})),
            ("agent.avatar_read", json!({"agentIds":[UNKNOWN]})),
            ("agent.github_identity", json!({"agent_id":UNKNOWN})),
            ("agent.snapshot", json!({})),
            ("agent.fleet_identity", json!({})),
            ("agent.changes", changes(cursor, Some(false))),
            ("resolve", json!({"canonicalRoot":root})),
            ("resolve_project_id", json!({"projectId":"absent"})),
            ("enumerate", json!({})),
            ("journal_tail", json!({"afterSeq":0})),
            ("trust", json!({"canonicalRoot":root})),
            ("verify", json!({})),
        ]
    }

    #[test]
    fn wal_reads_and_liveness_answer_prewrite_state_before_a_parked_writer_is_released() {
        use std::{sync::mpsc, thread, time::Instant};

        let f = Fixture::new(true);
        let root = f.project("P", Some("W"));
        let agent = f.create("Alice");
        let mut requests = read_requests(f.head(), std::path::Path::new(&root));
        requests.push(("resolve_remote", json!({"owner":"owner","repo":"repo"})));
        requests.push((
            "agent.session_liveness",
            json!({"refs":[{"ref_kind":"pi","ref":"missing"}]}),
        ));
        for (method, params) in &mut requests {
            if matches!(*method, "agent.resolve" | "agent.github_identity") {
                *params = json!({"agent_id":agent});
            }
        }
        let expected = requests
            .iter()
            .map(|(method, params)| f.ok(method, params.clone()))
            .collect::<Vec<_>>();
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (reply_tx, reply_rx) = mpsc::channel();
        let (received, prompt) = thread::scope(|scope| {
            let handler = &f.handler;
            let writer = scope.spawn(move || {
                handler.with_store(|store| {
                    store.apply_entry("fixture", "{}", "test", None, |tx| {
                        tx.execute_batch("UPDATE project SET name='uncommitted'; UPDATE agent SET terminal_reason='retired',terminal_at_ms=900; DELETE FROM project_root;")?;
                        parked_tx.send(()).unwrap();
                        // Park until the reads are done. The bound only keeps a
                        // regression from hanging the suite; it is longer than the
                        // whole read guard below, so a blocked read can't slip
                        // through by outlasting the park.
                        let _ = release_rx.recv_timeout(Duration::from_secs(60));
                        tx.execute_batch("SELECT * FROM missing_rollback_sentinel")
                    })
                }).unwrap_err();
            });
            parked_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            scope.spawn(move || {
                for (method, params) in requests {
                    let start = Instant::now();
                    let outcome = handler.handle_request(&body(method, params), OPERATOR);
                    let reply = success(outcome);
                    let _ = reply_tx.send((method, reply, start.elapsed()));
                }
            });
            // Every reply must arrive while the writer is still parked: a read
            // queued behind the writer can't finish until `release_tx` below,
            // so receiving them all first proves no read waited for it. One
            // deadline bounds the whole batch only so that regression fails
            // instead of hanging; it is generous enough that a loaded machine
            // slows the replies without making them look blocked.
            let deadline = Instant::now() + Duration::from_secs(30);
            let mut received = Vec::new();
            let mut all_before_release = true;
            for _ in &expected {
                match reply_rx.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
                    Ok(reply) => received.push(reply),
                    Err(_) => {
                        all_before_release = false;
                        break;
                    }
                }
            }
            release_tx.send(()).unwrap();
            writer.join().unwrap();
            (received, all_before_release)
        });
        let timings = received
            .iter()
            .map(|(method, _, elapsed)| format!("{method} {elapsed:?}"))
            .collect::<Vec<_>>();
        assert!(
            prompt,
            "a WAL read waited for the parked writer; replies before release: {timings:?}"
        );
        for ((method, reply, _), expected) in received.into_iter().zip(expected) {
            assert_eq!(reply, expected, "{method} must see only committed state");
        }
        assert!(!f.handler.liveness.lock().unwrap().readable());
    }

    #[tokio::test]
    async fn health_reads_only_atomics_while_writer_and_store_slot_are_parked() {
        use std::{
            future::Future,
            sync::mpsc,
            task::{Context, Poll, Waker},
            thread,
        };
        use subc_client_rs::ModuleHandler;

        let f = Fixture::new(true);
        f.project("P", None);
        let generation = f.head();
        let (parked_tx, parked_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (health_tx, health_rx) = mpsc::channel();
        thread::scope(|scope| {
            let handler = &f.handler;
            let writer = scope.spawn(move || {
                handler
                    .with_store(|store| {
                        store.apply_entry("fixture", "{}", "test", None, |tx| {
                            tx.execute("UPDATE project SET name='uncommitted'", [])?;
                            // Even consulting the read handle would require this
                            // installation slot; health must use atomics only.
                            let _slot = handler.store.lock().unwrap();
                            parked_tx.send(()).unwrap();
                            // Park until health has answered. The bound only
                            // keeps a regression from hanging the suite; it is
                            // longer than the health guard below, so a blocked
                            // health read can't slip through by outlasting it.
                            let _ = release_rx.recv_timeout(Duration::from_secs(60));
                            tx.execute_batch("SELECT * FROM missing_rollback_sentinel")
                        })
                    })
                    .unwrap_err();
            });
            parked_rx.recv_timeout(Duration::from_secs(30)).unwrap();
            // Health has no awaits, store reads or locks, so its first poll must
            // be ready while the writer is known to be inside its uncommitted
            // transaction and holding the store slot. The poll runs on its own
            // thread so that a health read that blocks on the slot fails the
            // guard below instead of deadlocking this one.
            let runtime = tokio::runtime::Handle::current();
            scope.spawn(move || {
                let _context = runtime.enter();
                let first_poll = std::pin::pin!(handler.health())
                    .as_mut()
                    .poll(&mut Context::from_waker(Waker::noop()));
                let _ = health_tx.send(match first_poll {
                    Poll::Ready(report) => Some(report),
                    Poll::Pending => None,
                });
            });
            // The writer cannot release before `release_tx` below, so receiving
            // the report first proves health never waited for storage. The
            // bound only turns that regression into a failure, not a hang.
            let health = health_rx.recv_timeout(Duration::from_secs(30));
            let _ = release_tx.send(());
            writer.join().unwrap();
            let health = health
                .expect("health waited for the parked writer or store slot")
                .expect("health must answer on its first poll");
            assert_eq!(health.metrics.unwrap()["generation"], generation);
        });
    }

    #[tokio::test]
    async fn identity_storage_failures_are_errors_never_statuses() {
        let f = Fixture::new(false);
        let unavailable = ProjectsHandler::with_runtime(INCARNATION.into(), || 700);
        for (method, params) in read_requests(0, &f.root)
            .into_iter()
            .filter(|(method, _)| method.starts_with("agent.") || *method == "node.self")
        {
            assert_eq!(
                wire_error(request(&unavailable, method, params.clone()).await).code,
                "storage_unavailable",
                "{method}"
            );
        }
        f.sql("DROP TABLE agent_name_claim; DROP TABLE agent;");
        for (method, params) in [
            ("agent.resolve", json!({"agent_id":UNKNOWN})),
            ("agent.snapshot", json!({})),
            ("agent.fleet_identity", json!({})),
            ("agent.github_identity", json!({"agent_id":UNKNOWN})),
            ("agent.list", json!({})),
            ("agent.resolve_name", json!({"name":"Missing"})),
            ("agent.avatar_read", json!({"agentIds":[UNKNOWN]})),
        ] {
            assert_eq!(
                wire_error(request(&f.handler, method, params).await).code,
                "storage_error",
                "{method}"
            );
        }
    }

    #[test]
    fn project_reads_add_only_incarnation_and_legacy_resolve_matches_the_unchanged_golden() {
        let mut f = Fixture::new(false);
        let root = f.project("pj-openai-auth", None);
        f.handler
            .seed_project(
                json!({"projectId":"pj-openai-auth","name":"openai-auth","roots":[root]}),
                "reserved:prefrontal",
            )
            .unwrap();
        f.project("other", None);
        let mut actual = json!({"result":f.ok("resolve", json!({"canonicalRoot":root}))});
        assert_eq!(
            actual["result"]
                .as_object_mut()
                .unwrap()
                .remove("incarnation"),
            Some(json!(INCARNATION))
        );
        actual["result"]["canonicalRoot"] = json!("/fixture/openai-auth");
        let golden: Value = serde_json::from_str(include_str!(
            "../../entorhinal-core/tests/golden/resolve/legacy-resolve-root.json"
        ))
        .unwrap();
        assert_eq!(actual, golden);
        for (method, params) in [
            ("resolve", json!({"canonicalRoot":root})),
            ("resolve_project_id", json!({"projectId":"pj-openai-auth"})),
            ("enumerate", json!({})),
            ("journal_tail", json!({"afterSeq":0})),
            ("trust", json!({"canonicalRoot":root})),
            ("verify", json!({})),
        ] {
            let core = f
                .handler
                .with_store(|store| match method {
                    "resolve" => Ok(serde_json::to_value(store.resolve(&root)?).unwrap()),
                    "resolve_project_id" => Ok(serde_json::to_value(
                        store.resolve_project_id("pj-openai-auth")?,
                    )
                    .unwrap()),
                    "enumerate" => Ok(serde_json::to_value(store.enumerate(None)?).unwrap()),
                    "journal_tail" => {
                        Ok(serde_json::to_value(store.journal_tail(0, 100)?).unwrap())
                    }
                    "trust" => Ok(serde_json::to_value(store.trust(&root)?).unwrap()),
                    "verify" => Ok(serde_json::to_value(store.verify()?).unwrap()),
                    _ => unreachable!(),
                })
                .unwrap();
            let mut wire = f.ok(method, params.clone());
            assert_eq!(
                wire.as_object_mut().unwrap().remove("incarnation"),
                Some(json!(INCARNATION))
            );
            assert_eq!(wire, core, "{method} must differ by exactly one key");
        }
        let before = f.ok("enumerate", json!({}));
        f.restart();
        let after = f.ok("enumerate", json!({}));
        assert_eq!(after["generation"], before["generation"]);
        assert_ne!(after["incarnation"], before["incarnation"]);
    }

    #[test]
    fn inherited_wire_bodies_pin_core_keys_casing_and_removed_fields() {
        let f = Fixture::new(true);
        f.project("P", Some("W"));
        // Expected keys come from core encoders, not entorhinal's own output.
        // Replies omit runtime keys such as `residence` and `wake_policy`.
        // Entorhinal adds generation and incarnation, plus agent_generation
        // or an avatar fingerprint where applicable.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:548-732,950-1001,3085-3422.
        let created = f.ok("agent.create", json!({"role":"hiree","name":"Alice","tag":"test","project_id":"P","request_key":"create","actor":null,"supervisor_agent_id":null}));
        fleet_keys(&created, &["agent"]);
        let id = created["agent"]["agent_id"].as_str().unwrap();
        keys(
            &created["agent"],
            &[
                "agent_id",
                "name",
                "name_version",
                "tag",
                "labels",
                "role",
                "created_at",
                "agent_generation",
                "project_id",
                "workspace_id",
            ],
        );
        assert_eq!(created["agent"]["role"], "hiree");
        let head = f.ok(
            "agent.create",
            json!({"role":"head","name":"Head","tag":"test","project_id":"P","request_key":"head"}),
        );
        let head_id = head["agent"]["agent_id"].as_str().unwrap();
        for (method, params, top) in [
            (
                "agent.rename",
                json!({"agent_id":id,"name":"Renamed","request_key":"rename"}),
                vec!["agent"],
            ),
            (
                "agent.update_tag",
                json!({"agent_id":id,"tag":"changed","request_key":"tag"}),
                vec!["agent"],
            ),
            (
                "agent.set_labels",
                json!({"agent":id,"labels":["One"],"request_key":"labels"}),
                vec!["agent"],
            ),
            (
                "agent.set_avatar",
                json!({"agentId":id,"genome":"a".repeat(2048),"type":"creature.classic","seedOnly":false,"request_key":"avatar"}),
                vec!["avatar", "applied"],
            ),
            (
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":null,"request_key":"github"}),
                vec!["github_identity"],
            ),
            (
                "agent.resolve_name",
                json!({"name":"Renamed","workspace_id":"W"}),
                vec!["agent_id", "digest"],
            ),
            (
                "agent.list",
                json!({"role":"hiree","project_id":"P","workspace_id":"W","include_gone":false,"activated_only":true,"cursor":null,"limit":50}),
                vec!["agents"],
            ),
            (
                "agent.peer_roster",
                json!({"workspace_id":"W"}),
                vec!["peers"],
            ),
            (
                "agent.avatar_read",
                json!({"agentIds":[id]}),
                vec!["avatars"],
            ),
            (
                "agent.github_identity",
                json!({"agent_id":id}),
                vec!["github_identity", "projectId", "agent_generation"],
            ),
        ] {
            let reply = f.ok(method, params.clone());
            fleet_keys(&reply, &top);
            if method == "agent.peer_roster" {
                assert_eq!(
                    reply["peers"],
                    json!([{"agent_id":head_id,"name":"Head","tag":"test","role":"head","project_id":"P"}])
                );
            }
            // Every inherited decoder refuses unknown, removed, and caller
            // metadata; mutations may not bypass this via a committed key.
            for field in [
                "unknown",
                "callerHarness",
                "callerSession",
                "persona_ref",
                "residence",
                "wake_policy",
                "sleep",
            ] {
                let mut bad = params.clone();
                bad[field] = json!(null);
                f.fail(method, bad, "invalid_request");
            }
            if method == "agent.avatar_read" {
                keys(
                    &reply["avatars"][0],
                    &["agentId", "fingerprint", "genome", "type"],
                );
                for field in [
                    "harness",
                    "session_id",
                    "session",
                    "caller_directory",
                    "caller_session",
                    "agent_ids",
                ] {
                    let mut bad = params.clone();
                    bad[field] = json!("ignored");
                    f.fail(method, bad, "invalid_request");
                }
            }
            if method == "agent.set_avatar" {
                keys(&reply["avatar"], &["genome", "type"]);
                for field in ["agent_id", "avatar_type", "seed_only"] {
                    let mut bad = params.clone();
                    bad[field] = json!(null);
                    f.fail(method, bad, "invalid_request");
                }
            }
        }
        for identity in [
            json!({"kind":"app","app_id":1,"app_slug":"app","installation_id":2,"credential_ref":"cred","coauthor_line":"Bot <bot@example.com>"}),
            json!({"kind":"user_token","login":"alice","credential_ref":"cred","coauthor_line":null}),
            Value::Null,
        ] {
            let mut params = json!({"agent_id":id,"github_identity":identity,"request_key":format!("github-{}",f.head())});
            let changed = f.ok("agent.set_github_identity", params.clone());
            fleet_keys(&changed, &["github_identity"]);
            assert_eq!(changed["github_identity"], identity);
            let read = f.ok("agent.github_identity", json!({"agent_id":id}));
            fleet_keys(&read, &["github_identity", "projectId", "agent_generation"]);
            assert_eq!(read["github_identity"], identity);
            assert_eq!(read["projectId"], "P");
            assert_ne!(read["agent_generation"], read["generation"]);
            params["github_identity"] =
                json!({"kind":"user_token","login":"alice","credential_ref":"cred","secret":"no"});
            f.fail("agent.set_github_identity", params, "invalid_request");
        }
        let other = f.create("Other");
        let merge = f.ok(
            "agent.merge",
            json!({"agent_id":other,"into_agent_id":id,"request_key":"merge"}),
        );
        fleet_keys(&merge, &["gone"]);
        keys(&merge["gone"], &["reason", "at", "into_agent_id"]);
        let dispose = f.ok(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"dispose"}),
        );
        fleet_keys(&dispose, &["gone"]);
        assert_eq!(dispose["gone"], json!({"reason":"deleted","at":700}));
        let resolve = f.ok("agent.resolve", json!({"agent_id":id}));
        fleet_keys(&resolve, &["agent_id", "status", "merged_into", "gone"]);
        assert_eq!(resolve["status"], "retired");
        assert_eq!(resolve["gone"], dispose["gone"]);
        let list = f.ok("agent.list", json!({"include_gone":true}));
        fleet_keys(&list, &["agents", "gone"]);
        assert_eq!(list["gone"].as_array().unwrap().len(), 2);
        for item in list["gone"].as_array().unwrap() {
            keys(item, &["agent_id", "gone"]);
        }
        let unknown = f.ok("agent.resolve_name", json!({"name":"Absent"}));
        fleet_keys(&unknown, &["refused"]);
        keys(&unknown["refused"], &["code", "details"]);
        assert_eq!(unknown["refused"]["code"], "name_unknown");
        f.project("P2", Some("W2"));
        f.ok("agent.create", json!({"role":"head","name":"Head","tag":"test","project_id":"P2","request_key":"head2"}));
        let ambiguous = f.ok("agent.resolve_name", json!({"name":"Head"}));
        fleet_keys(&ambiguous, &["refused"]);
        assert_eq!(
            ambiguous["refused"],
            json!({"code":"name_ambiguous","details":{"candidates":["W/Head","W2/Head"]}})
        );
    }

    #[test]
    fn inherited_error_codes_decode_core_conditions_and_structured_detail() {
        let f = Fixture::new(true);
        let id = f.create("Alice");
        f.project("P", Some("W"));
        f.ok(
            "agent.create",
            json!({"role":"head","name":"Head","tag":"test","project_id":"P","request_key":"head"}),
        );
        // Conditions and codes are core's validator/error contract.
        // Source: prefrontal 873870be8 crates/prefrontal-core-module/src/agent_registry_ops.rs:774-808,3038-3049,3107-3119;
        // crates/prefrontal-core-store/src/agent_registry.rs:1379-1395,1468-1552.
        for (method, params, code) in [
            (
                "agent.rename",
                json!({"agent_id":id,"name":"","request_key":"e-name"}),
                "invalid_name",
            ),
            (
                "agent.create",
                json!({"role":"assistant","name":"Alice","tag":"test","request_key":"e-conflict"}),
                "name_conflict",
            ),
            (
                "agent.create",
                json!({"role":"head","name":"Other","tag":"test","project_id":"P","request_key":"e-head"}),
                "agent_project_taken",
            ),
            (
                "agent.github_identity",
                json!({"agent_id":UNKNOWN}),
                "unknown_agent",
            ),
            (
                "agent.github_identity",
                json!({"agent_id":"AGENT_0000000000000000"}),
                "unknown_agent",
            ),
            (
                "agent.merge",
                json!({"agent_id":id,"into_agent_id":id,"request_key":"e-self"}),
                "merge_self",
            ),
            (
                "agent.merge",
                json!({"agent_id":id,"into_agent_id":UNKNOWN,"request_key":"e-target"}),
                "merge_target_not_live",
            ),
            (
                "agent.update_tag",
                json!({"agent_id":id,"tag":" ","request_key":"e-tag"}),
                "invalid_tag",
            ),
            (
                "agent.set_labels",
                json!({"agent_id":id,"labels":["One","one"],"request_key":"e-labels"}),
                "invalid_labels",
            ),
            (
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":{"kind":"user_token","login":"","credential_ref":"cred"},"request_key":"e-github"}),
                "invalid_github_identity",
            ),
            (
                "agent.create",
                json!({"role":"head","name":"New","tag":"test","project_id":" ","request_key":"e-project"}),
                "invalid_role_shape",
            ),
            (
                "agent.create",
                json!({"role":"workspace_head","name":"New","tag":"test","workspace_id":" ","request_key":"e-workspace"}),
                "invalid_role_shape",
            ),
            (
                "agent.create",
                json!({"role":"assistant","name":"New","tag":"test","project_id":"P","request_key":"e-shape"}),
                "invalid_role_shape",
            ),
            ("agent.list", json!({"limit":0}), "invalid_cursor"),
            ("agent.list", json!({"limit":201}), "invalid_cursor"),
            ("agent.list", json!({"cursor":""}), "invalid_cursor"),
        ] {
            let field = if params.get("project_id") == Some(&json!(" ")) {
                Some("project_id")
            } else if params.get("workspace_id") == Some(&json!(" ")) {
                Some("workspace_id")
            } else {
                None
            };
            let error = f.fail(method, params, code);
            if let Some(field) = field {
                assert_eq!(error.message, format!("invalid {field}"));
            }
        }
        f.ok(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"retire"}),
        );
        let gone = f.fail("agent.github_identity", json!({"agent_id":id}), "gone");
        assert_eq!(gone.detail, Some(json!({"reason":"deleted","at":700})));
        f.project("Unplaced", None);
        f.fail("agent.create", json!({"role":"head","name":"Unplaced head","tag":"test","project_id":"Unplaced","request_key":"e-unplaced"}), "unresolved_workspace");
        f.sql("DROP TABLE project_workspace;");
        f.fail("agent.create", json!({"role":"hiree","name":"Gap","tag":"test","project_id":"P","request_key":"e-gap"}), "activation_gap");
        let before = Fixture::new(false);
        before.fail(
            "agent.peer_roster",
            json!({"workspace_id":"W"}),
            "registry_not_activated",
        );
    }

    #[derive(Default)]
    struct ManualClock {
        millis: AtomicU64,
        advanced: tokio::sync::Notify,
        durations: Mutex<Vec<Duration>>,
    }

    #[test]
    fn snapshot_and_changes_wire_keys_keep_terminal_rows_claim_history_and_full_page_cursor() {
        let f = Fixture::new(true);
        let id = f.create("Alice");
        f.ok(
            "agent.rename",
            json!({"agent_id":id,"name":"Renamed","request_key":"rename"}),
        );
        f.ok(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"dispose"}),
        );
        let snapshot = f.ok("agent.snapshot", json!({}));
        fleet_keys(&snapshot, &["agents", "claims"]);
        let row_keys = [
            "agent_id",
            "name",
            "name_version",
            "name_normalization_version",
            "tag",
            "labels",
            "role",
            "project_id",
            "workspace_id",
            "avatar",
            "github_identity",
            "status",
            "merged_into",
            "supervisor_agent_id",
            "request_key",
            "created_at_ms",
            "updated_at_ms",
            "terminal_at_ms",
            "agent_generation",
        ];
        let claim_keys = [
            "claim_id",
            "agent_id",
            "namespace_kind",
            "namespace_key",
            "normalized_name",
            "name_normalization_version",
            "display_name",
            "claimed_at_ms",
            "released_at_ms",
        ];
        keys(&snapshot["agents"][0], &row_keys);
        assert_eq!(snapshot["agents"][0]["status"], "retired");
        assert_eq!(snapshot["claims"].as_array().unwrap().len(), 2);
        for claim in snapshot["claims"].as_array().unwrap() {
            keys(claim, &claim_keys);
        }
        let mut cursor = 0;
        for op in ["agent.create", "agent.rename", "agent.dispose"] {
            let page = f.ok(
                "agent.changes",
                json!({"incarnation":INCARNATION,"cursor":cursor,"limit":1,"wait":false}),
            );
            fleet_keys(&page, &["cursor", "entries"]);
            assert_eq!(page["entries"].as_array().unwrap().len(), 1);
            let entry = &page["entries"][0];
            keys(
                entry,
                &[
                    "seq",
                    "op",
                    "agent_id",
                    "agent_generation",
                    "row",
                    "claims",
                    "old_display_name",
                    "new_display_name",
                    "status",
                    "merged_into",
                ],
            );
            assert_eq!(entry["op"], op);
            keys(&entry["row"], &row_keys);
            for claim in entry["claims"].as_array().unwrap() {
                keys(claim, &claim_keys);
            }
            assert_eq!(page["cursor"], entry["seq"]);
            assert!(entry["seq"].as_i64().unwrap() > cursor);
            cursor = page["cursor"].as_i64().unwrap();
        }
        assert_eq!(cursor, f.head());
        assert_eq!(
            f.ok("agent.changes", changes(cursor, Some(false)))["entries"],
            json!([])
        );
        assert_eq!(f.ok("agent.fleet_identity", json!({}))["agents"], json!([]));
        for method in ["agent.snapshot", "agent.fleet_identity"] {
            f.fail(method, json!({"unknown":true}), "invalid_request");
        }
    }

    impl ManualClock {
        fn advance(&self, duration: Duration) {
            self.millis
                .fetch_add(duration.as_millis() as u64, Ordering::SeqCst);
            self.advanced.notify_waiters();
        }
    }

    #[async_trait]
    impl FeedClock for ManualClock {
        async fn sleep(&self, duration: Duration) {
            self.durations.lock().unwrap().push(duration);
            let deadline = self.millis.load(Ordering::SeqCst) + duration.as_millis() as u64;
            loop {
                let notified = self.advanced.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                if self.millis.load(Ordering::SeqCst) >= deadline {
                    return;
                }
                notified.await;
            }
        }
    }

    /// Poll until the call waits on something other than its own store reads,
    /// which finish on the blocking pool, and check it is still waiting.
    async fn pending<F: Future>(future: &mut Pin<Box<F>>) {
        loop {
            assert!(
                poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await,
                "wait should remain held"
            );
            if READS_IN_FLIGHT.with(std::cell::Cell::get) == 0 {
                return;
            }
            tokio::task::yield_now().await;
        }
    }

    /// Poll until the call returns. It may stay pending only while its own
    /// store reads finish on the blocking pool, never on the clock or a
    /// notification.
    async fn ready<F: Future>(future: &mut Pin<Box<F>>) -> F::Output {
        loop {
            if let Poll::Ready(value) = poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx))).await {
                return value;
            }
            assert!(
                READS_IN_FLIGHT.with(std::cell::Cell::get) > 0,
                "call must return without clock advancing or another wake-up"
            );
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn changes_wait_holds_no_store_lock_and_commit_notification_returns_the_entry() {
        let mut f = Fixture::new(true);
        let clock = Arc::new(ManualClock::default());
        f.handler.feed_clock = clock.clone();
        let cursor = f.head();
        let mut wait = Box::pin(request(&f.handler, "agent.changes", changes(cursor, None)));
        pending(&mut wait).await;
        assert!(
            f.handler.store.try_lock().is_ok(),
            "a pending wait must not hold storage"
        );
        let resolved = f.ok("agent.resolve", json!({"agent_id":UNKNOWN}));
        assert_eq!(resolved["status"], "unknown");
        let created = f.create("Alice");
        let reply = success(ready(&mut wait).await);
        fleet_keys(&reply, &["cursor", "entries"]);
        assert_eq!(reply["entries"].as_array().unwrap().len(), 1);
        assert_eq!(reply["entries"][0]["op"], "agent.create");
        assert_eq!(reply["entries"][0]["row"]["agent_id"], created);
        assert_eq!(reply["cursor"], f.head());
        assert_eq!(clock.millis.load(Ordering::SeqCst), 0);
        assert_eq!(f.handler.feed_waits.available_permits(), 8);
    }

    #[tokio::test]
    async fn changes_timeout_is_empty_success_at_exactly_twenty_five_injected_seconds() {
        let mut f = Fixture::new(true);
        let clock = Arc::new(ManualClock::default());
        f.handler.feed_clock = clock.clone();
        let cursor = f.head();
        let mut wait = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(cursor, Some(true)),
        ));
        pending(&mut wait).await;
        clock.advance(Duration::from_secs(24));
        pending(&mut wait).await;
        clock.advance(Duration::from_millis(999));
        pending(&mut wait).await;
        clock.advance(Duration::from_millis(1));
        assert_eq!(
            success(ready(&mut wait).await),
            json!({"incarnation":INCARNATION,"generation":cursor,"cursor":cursor,"entries":[]})
        );
        assert_eq!(
            *clock.durations.lock().unwrap(),
            vec![Duration::from_secs(25)]
        );
        assert_eq!(f.handler.feed_waits.available_permits(), 8);
    }

    #[tokio::test]
    async fn eight_waits_leave_ninth_and_resolve_immediate_and_cancellation_releases_capacity() {
        let mut f = Fixture::new(true);
        let clock = Arc::new(ManualClock::default());
        f.handler.feed_clock = clock.clone();
        let cursor = f.head();
        let mut waits = Vec::new();
        for _ in 0..8 {
            let mut wait = Box::pin(request(
                &f.handler,
                "agent.changes",
                changes(cursor, Some(true)),
            ));
            pending(&mut wait).await;
            waits.push(wait);
        }
        assert_eq!(f.handler.feed_waits.available_permits(), 0);
        let mut ninth = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(cursor, Some(true)),
        ));
        assert_eq!(
            success(ready(&mut ninth).await),
            json!({"incarnation":INCARNATION,"generation":cursor,"cursor":cursor,"entries":[]})
        );
        assert!(f.handler.store.try_lock().is_ok());
        let mut resolve = Box::pin(request(
            &f.handler,
            "resolve",
            json!({"canonicalRoot":f.root}),
        ));
        assert_eq!(
            success(ready(&mut resolve).await)["incarnation"],
            INCARNATION
        );
        drop(waits.pop());
        assert_eq!(f.handler.feed_waits.available_permits(), 1);
        let mut replacement = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(cursor, Some(true)),
        ));
        pending(&mut replacement).await;
        f.create("Alice");
        waits.push(replacement);
        for mut wait in waits {
            let reply = success(ready(&mut wait).await);
            assert_eq!(reply["entries"][0]["op"], "agent.create");
        }
        assert_eq!(f.handler.feed_waits.available_permits(), 8);
        assert_eq!(clock.millis.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn changes_skip_nonidentity_commits_immediately_and_refuse_stale_cursors() {
        let mut f = Fixture::new(true);
        f.handler.feed_clock = Arc::new(ManualClock::default());
        let cursor = f.head();
        let mut wait = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(cursor, Some(true)),
        ));
        pending(&mut wait).await;
        f.project("P", None);
        let expected =
            json!({"incarnation":INCARNATION,"generation":f.head(),"cursor":f.head(),"entries":[]});
        assert_eq!(success(ready(&mut wait).await), expected);
        let mut trailing = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(cursor, Some(true)),
        ));
        assert_eq!(success(ready(&mut trailing).await), expected);
        let mut no_wait = Box::pin(request(
            &f.handler,
            "agent.changes",
            changes(f.head(), Some(false)),
        ));
        assert_eq!(success(ready(&mut no_wait).await), expected);
        for params in [
            json!({"incarnation":"fedcba9876543210","cursor":0}),
            changes(f.head() + 1, Some(true)),
        ] {
            assert_eq!(
                wire_error(request(&f.handler, "agent.changes", params).await).code,
                "snapshot_required"
            );
        }
        for params in [
            json!({}),
            json!({"incarnation":INCARNATION,"cursor":0,"limit":0}),
            json!({"incarnation":INCARNATION,"cursor":0,"limit":1001}),
            json!({"incarnation":INCARNATION,"cursor":0,"extra":null}),
        ] {
            assert_eq!(
                wire_error(request(&f.handler, "agent.changes", params).await).code,
                "invalid_request"
            );
        }
    }

    /// Park the first store read inside its snapshot, holding its reader,
    /// until released. The wait is bounded so a regression that parks a
    /// runtime worker fails the test instead of hanging it.
    fn park_first_read(
        store: &RegistryStore,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let armed = std::sync::atomic::AtomicBool::new(true);
        let (parked_tx, parked_rx) = std::sync::mpsc::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
        let release_rx = Mutex::new(release_rx);
        store.set_read_hook(Some(Arc::new(move || {
            if armed.swap(false, Ordering::SeqCst) {
                parked_tx.send(()).unwrap();
                let _ = release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(10));
            }
        })));
        (parked_rx, release_tx)
    }

    /// The handler as the module serves it, with its self-handle installed.
    fn served(f: &mut Fixture) -> Arc<ProjectsHandler> {
        std::mem::replace(
            &mut f.handler,
            ProjectsHandler::with_runtime(INCARNATION.into(), || 700),
        )
        .into_served()
        .0
    }

    async fn wait_parked(parked: std::sync::mpsc::Receiver<()>) {
        tokio::task::spawn_blocking(move || parked.recv_timeout(Duration::from_secs(10)))
            .await
            .unwrap()
            .expect("the read parked inside its snapshot");
    }

    #[tokio::test]
    async fn a_parked_served_read_leaves_other_reads_and_health_answering_on_one_worker() {
        assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
        let mut f = Fixture::new(false);
        let handler = served(&mut f);
        let store = handler.store.lock().unwrap().clone().unwrap();
        let (parked, release) = park_first_read(&store);
        let request = body("enumerate", json!({}));
        let mut enumerate = Box::pin(handler.handle_served_request(&request, READER));
        assert!(
            poll_fn(|cx| Poll::Ready(enumerate.as_mut().poll(cx).is_pending())).await,
            "a served read must not run on the runtime worker"
        );
        wait_parked(parked).await;
        let health = <ProjectsHandler as crate::ModuleHandler>::health(&handler).await;
        assert_eq!(health.metrics.unwrap()["enumerateCount"], 0);
        let resolved = success(
            handler
                .handle_served_request(&body("resolve", json!({"canonicalRoot":f.root})), READER)
                .await,
        );
        assert_eq!(resolved["incarnation"], INCARNATION);
        assert!(
            poll_fn(|cx| Poll::Ready(enumerate.as_mut().poll(cx).is_pending())).await,
            "the parked read is still parked"
        );
        release.send(()).unwrap();
        assert_eq!(success(enumerate.await)["projects"], json!([]));
        store.set_read_hook(None);
        handler.served_reads.wait().await;
        handler.store.lock().unwrap().take();
    }

    #[tokio::test(start_paused = true)]
    async fn a_served_read_past_its_deadline_is_refused_and_still_awaited() {
        let mut f = Fixture::new(false);
        let handler = served(&mut f);
        let store = handler.store.lock().unwrap().clone().unwrap();
        let (parked, release) = park_first_read(&store);
        let request = body("enumerate", json!({}));
        let mut enumerate = Box::pin(handler.handle_served_request(&request, READER));
        assert!(poll_fn(|cx| Poll::Ready(enumerate.as_mut().poll(cx).is_pending())).await);
        wait_parked(parked).await;
        tokio::time::advance(SERVED_READ_DEADLINE).await;
        assert_eq!(wire_error(enumerate.await).code, "read_timeout");
        // The abandoned read still holds its reader and store handle; draining
        // waits for it to finish.
        let mut drained = Box::pin(handler.served_reads.wait());
        assert!(poll_fn(|cx| Poll::Ready(drained.as_mut().poll(cx).is_pending())).await);
        release.send(()).unwrap();
        drained.await;
        store.set_read_hook(None);
        handler.store.lock().unwrap().take();
    }

    #[test]
    fn fleet_tokens_use_pair_invalidate_on_every_listed_append_and_restart() {
        let mut f = Fixture::new(true);
        let root = f.project("P", Some("W"));
        assert!(std::process::Command::new("git")
            .args(["init", "--quiet", &root])
            .status()
            .unwrap()
            .success());
        f.handler
            .with_store(|store| store.bind_unbound_roots("test"))
            .unwrap();
        let token = f.ok("agent.fleet_identity", json!({}))["token"].clone();
        let id = f.create("Alice");
        assert_ne!(
            f.ok("agent.fleet_identity", json!({"token":token}))["token"],
            token
        );
        for (method, params) in [
            (
                "agent.rename",
                json!({"agent_id":id,"name":"Renamed","request_key":"rename"}),
            ),
            (
                "agent.update_tag",
                json!({"agent_id":id,"tag":"changed","request_key":"tag"}),
            ),
            (
                "agent.set_labels",
                json!({"agent_id":id,"labels":["One"],"request_key":"labels"}),
            ),
            (
                "agent.set_avatar",
                json!({"agentId":id,"genome":"a".repeat(2048),"type":"creature.classic","seedOnly":false,"request_key":"avatar"}),
            ),
            (
                "agent.set_github_identity",
                json!({"agent_id":id,"github_identity":{"kind":"user_token","login":"login","credential_ref":"cred"},"request_key":"github"}),
            ),
            (
                "register",
                json!({"projectId":"P","name":"Renamed project","roots":[root]}),
            ),
            ("approve_root", json!({"root":root})),
            (
                "agent.dispose",
                json!({"agent_id":id,"request_key":"dispose"}),
            ),
            (
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W2"}),
            ),
        ] {
            let full = f.ok("agent.fleet_identity", json!({}));
            fleet_keys(&full, &["logPosition", "token", "unchanged", "agents"]);
            assert_eq!(full["token"], format!("{}:{}", INCARNATION, f.head()));
            let unchanged = f.ok("agent.fleet_identity", json!({"token":full["token"]}));
            fleet_keys(&unchanged, &["logPosition", "token", "unchanged"]);
            assert_eq!(unchanged["unchanged"], true);
            match method {
                "register" => f
                    .handler
                    .seed_project(params, "reserved:prefrontal")
                    .unwrap(),
                "approve_root" => {
                    f.handler
                        .with_store(|store| store.approve_root(&root, "fixture"))
                        .unwrap();
                }
                _ => {
                    f.ok(method, params);
                }
            }
            assert_eq!(
                f.ok("agent.fleet_identity", json!({"token":full["token"]}))["unchanged"],
                false,
                "{method}"
            );
        }
        let target = f.create("Target");
        let source = f.create("Source");
        let token = f.ok("agent.fleet_identity", json!({}))["token"].clone();
        f.ok(
            "agent.merge",
            json!({"agent_id":source,"into_agent_id":target,"request_key":"merge"}),
        );
        assert_eq!(
            f.ok("agent.fleet_identity", json!({"token":token}))["unchanged"],
            false
        );
        for token in [
            Value::Null,
            json!("bad"),
            json!(format!("fedcba9876543210:{}", f.head())),
            json!(format!("{INCARNATION}:{}", f.head() - 1)),
            json!(42),
            json!({"bad":true}),
        ] {
            let reply = f.ok("agent.fleet_identity", json!({"token":token}));
            fleet_keys(&reply, &["logPosition", "token", "unchanged", "agents"]);
            assert_eq!(reply["unchanged"], false);
            assert_eq!(reply["agents"].as_array().unwrap().len(), 1);
        }
        let old = f.ok("agent.fleet_identity", json!({}));
        f.restart();
        let new = f.ok("agent.fleet_identity", json!({"token":old["token"]}));
        assert_eq!(new["unchanged"], false);
        assert_eq!(new["generation"], old["generation"]);
        assert_ne!(new["incarnation"], old["incarnation"]);
        assert_eq!(new["agents"], old["agents"]);
        assert_eq!(
            f.fail(
                "agent.changes",
                changes(f.head(), Some(false)),
                "snapshot_required"
            )
            .detail,
            None
        );
        f.handler
            .with_store(|store| {
                store.apply_entry("fixture", "{}", "fixture", None, |tx| {
                    let mut query = tx.prepare(
                        "SELECT payload_json,coalesce(response_json,'') FROM registry_journal",
                    )?;
                    let journal = query
                        .query_map([], |row| {
                            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
                        })?
                        .collect::<Result<Vec<_>, _>>()?;
                    for (payload, cached) in journal {
                        assert!(!payload.contains(INCARNATION) && !cached.contains(INCARNATION));
                        assert!(!cached.contains("incarnation"));
                    }
                    Ok(())
                })
            })
            .unwrap();
    }

    #[test]
    fn manifest_pins_exact_authority_surface_capabilities_and_protocol_dependencies() {
        let manifest = serde_json::to_value(crate::manifest()).unwrap();
        let surfaces = manifest["provides"].as_array().unwrap();
        assert_eq!(
            surfaces.len(),
            3,
            "management, agent tools and package move participant"
        );
        assert_eq!(surfaces[0]["role"], "management_surface");
        assert_eq!(surfaces[1]["role"], "tool_provider");
        assert_eq!(
            surfaces[2],
            serde_json::to_value(crate::participant_discovery::provider_role()).unwrap()
        );
        let operations = surfaces[0]["operations"].as_array().unwrap();
        let expected_queries = [
            "placement.read",
            "user.get",
            "context.project",
            "context.session",
            "workspace.document",
            "catalog.registry_list",
            "catalog.read_blob",
            "catalog.agent_generation",
            "permissions.list",
            "agent.audit_list",
            "agent.preview_repair_workspaces",
            "agent.preview_avatar_manifest",
            "resolve",
            "resolve_project_id",
            "resolve_remote",
            "resolve_root_key",
            "preview_attach_root",
            "identity_log.status",
            "fleet.epoch_current",
            "enumerate",
            "journal_tail",
            "trust",
            "verify",
            "agent.resolve",
            "agent.resolve_name",
            "agent.list",
            "agent.peer_roster",
            "agent.avatar_read",
            "agent.github_identity",
            "agent.fleet_identity",
            "agent.fleet_identity_watch",
            "agent.snapshot",
            "agent.changes",
            "agent.for_session",
            "agent.session_for_ref",
            "agent.session_liveness",
            "agent.sessions",
            "agent.sessions_watch",
            "agent.session_for_runner",
            "agent.session_residence",
            "agent.session_for_harness",
            "agent.where",
            "agent.where_many",
            "node.self",
            "agent.where_watch",
            "catalog.status_watch",
            "agent.move_status",
            "agent.move_refs",
            "agent.moves",
            "role.read",
            "role.fence",
            "role.lease",
        ];
        let expected_mutations = [
            "placement.backfill",
            "user.set",
            "store.adopt",
            "project.set_domain",
            "workspace.set_document",
            crate::catalog_seed::SEED,
            "catalog.registry_add",
            "catalog.publish",
            "catalog.publisher_trust",
            "catalog.registry_repin",
            "catalog.registry_remove",
            "catalog.ack_activation",
            "permissions.revoke",
            "agent.set_hire_cap",
            "agent.repair_workspaces",
            "agent.apply_avatar_manifest",
            "register",
            "assign_workspace",
            "set_workspace_root",
            "approve_workspace_root",
            "unapprove_workspace_root",
            "set_owned_remotes",
            "upgrade_implicit",
            "remove",
            "seed_import",
            "rebuild",
            "add_root",
            "refresh_root_key",
            "rebind_root",
            "attach_root",
            "identity_log.enable",
            "remove_root",
            "move_root_to_new_project",
            "attach_derived_parent",
            "approve_root",
            "unapprove_root",
            "approve_project",
            "unapprove_project",
            "agent.create",
            "agent.rename",
            "agent.update_tag",
            "agent.set_labels",
            "agent.set_avatar",
            "agent.set_github_identity",
            "agent.dispose",
            "agent.merge",
            "agent.import",
            "agent.register_session",
            "agent.end_session",
            "agent.set_session_network",
            "agent.refresh_claims",
            "agent.move",
            "agent.move_abort",
            "agent.set_lid_close_moves",
            "member.set_name",
            "role.take",
            "role.lease.ack",
            "role.lease.release",
            "role.lease.yield",
            "agent.take",
        ];
        for (kind, names) in [
            ("query", expected_queries.as_slice()),
            ("mutate", expected_mutations.as_slice()),
        ] {
            assert_eq!(
                operations
                    .iter()
                    .filter(|op| op["kind"] == kind)
                    .map(|op| op["name"].as_str().unwrap())
                    .collect::<BTreeSet<_>>(),
                names.iter().copied().collect()
            );
        }
        assert_eq!(
            operations.len(),
            expected_queries.len() + expected_mutations.len()
        );
        assert_eq!(
            manifest["capabilities"]["provides"],
            json!([
                "project-identity/v1",
                "agent-identity/v1",
                "bind-no-project/v1",
                "permissions/v1",
                "tool-provider/v1",
                "agent-data/v1",
                "call-modes/v1",
                "agent-session-role/v1"
            ])
        );
        assert_eq!(manifest["capabilities"]["requires"], json!([]));
        // Only the streaming queries are declared as watches; every
        // other declared operation stays unary.
        let typed = crate::manifest();
        let watched = operations
            .iter()
            .map(|op| op["name"].as_str().unwrap())
            .filter(|name| typed.is_watch_operation(name))
            .collect::<BTreeSet<_>>();
        assert_eq!(
            watched,
            BTreeSet::from([
                "agent.fleet_identity_watch",
                "agent.where_watch",
                "agent.sessions_watch",
                "catalog.status_watch",
                "role.lease",
            ])
        );
    }
}

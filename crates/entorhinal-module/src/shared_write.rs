//! Routes identity writes to the installed hub or commits them locally when
//! sharing is off. During native enable, bootstrap fences protect the snapshot.

use super::*;
use entorhinal_core::agent::{AgentPrecheck, OperatorApproval};
use entorhinal_core::RegistryStore;
use serde_json::Value;
use std::time::Duration;
use tokio::time::{timeout_at, Instant};

const DEADLINE: Duration = Duration::from_secs(25);
pub(super) const OPERATOR_BOUND: Duration = Duration::from_secs(270);

pub(super) fn health_state(state: u64) -> &'static str {
    match state {
        3 => "identity_log_invariant",
        4 => "log_gap",
        5 => "log_verify_failed",
        6 => "apply_failed",
        _ => "ok",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_log::FailConnector;
    use crate::log_client::LogConnector;
    use serde_json::json;
    use std::sync::{atomic::Ordering, Arc, Mutex};
    use std::{
        future::Future,
        pin::Pin,
        task::{Context, Poll, Waker},
    };

    const ROUTE: RouteKey = (71, 1);
    // Tokio schedules timers at millisecond precision. Advancing simulated time
    // by an extra millisecond lets a due timer fire even if its deadline was
    // rounded up; this does not measure how long SQLite takes in real time.
    const TIMER_TICK: Duration = Duration::from_millis(1);
    fn fixed_avatar() -> Result<entorhinal_core::agent::StoredAgentAvatar, HandlerError> {
        Ok(entorhinal_core::agent::StoredAgentAvatar {
            genome: "ab".repeat(16_384),
            avatar_type: "creature.wild".into(),
            version: Some(1),
        })
    }

    /// Require a result on the first poll, without waiting for another async task.
    /// Tests use this while a write holds the lock or waits for the log's reply,
    /// to prove that cached replies, liveness and reads do not queue behind it.
    /// This checks whether an operation waits, not how fast SQLite runs.
    fn ready<F: Future>(future: F) -> F::Output {
        let mut future = std::pin::pin!(future);
        match future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        {
            Poll::Ready(result) => result,
            Poll::Pending => panic!("operation waited instead of answering from committed state"),
        }
    }

    fn pending<F: Future>(future: Pin<&mut F>) {
        assert!(
            future
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending(),
            "write completed before its deadline"
        );
    }

    async fn finish<F: Future>(mut write: Pin<&mut F>) -> F::Output {
        let frozen = Instant::now();
        let result = loop {
            tokio::select! {
                result = &mut write => break result,
                () = tokio::task::yield_now() => {},
            }
        };
        assert_eq!(
            Instant::now(),
            frozen,
            "SQLite completion advanced the paused clock"
        );
        result
    }

    struct Fixture {
        dir: PathBuf,
        descriptor: StorageDescriptor,
        handler: ProjectsHandler,
    }
    impl Fixture {
        fn new(label: &str, connector: Arc<dyn LogConnector>) -> Self {
            let (dir, descriptor) = crate::tests::scratch_descriptor(label);
            let handler = Self::handler(&descriptor, connector);
            handler
                .with_store(|s| s.apply_entry("agent.cutover", "{}", "test", None, |_| Ok(())))
                .unwrap();
            Self {
                dir,
                descriptor,
                handler,
            }
        }
        fn handler(
            descriptor: &StorageDescriptor,
            connector: Arc<dyn LogConnector>,
        ) -> ProjectsHandler {
            let mut handler =
                ProjectsHandler::with_log_connector("0123456789abcdef".into(), || 700, connector);
            handler.agent_avatar_draw = fixed_avatar;
            *handler.store.lock().unwrap() = Some(RegistryStore::open(descriptor).unwrap());
            handler.health.store_ready.store(true, Ordering::Relaxed);
            handler.route_admissions().insert(
                ROUTE,
                RouteAdmission {
                    principal: Some(Principal::Reserved {
                        module_id: WRITER_MODULE.into(),
                    }),
                    flow_id: None,
                    handle: None,
                    scope: None,
                    call_mode: None,
                    providers: Default::default(),
                },
            );
            handler
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        fn image(&self) -> Value {
            let store = self.store();
            json!({"projects":store.enumerate(None).unwrap(), "journal":store.journal_tail(0, 1000).unwrap()})
        }
        fn pending(&self) -> i64 {
            self.store()
                .identity_log_status()
                .unwrap()
                .pending_write_count
        }
        async fn call(&self, op: &str, params: Value) -> Result<Vec<u8>, HandlerError> {
            let request = serde_json::to_vec(&json!({"method":op,"params":params})).unwrap();
            match self.handler.handle_served_request(&request, ROUTE).await {
                HandlerOutcome::Response(body) => Ok(body),
                HandlerOutcome::Error { code, message } => Err(HandlerError {
                    code,
                    message,
                    detail: None,
                }),
                HandlerOutcome::ErrorWithDetail {
                    code,
                    message,
                    detail,
                } => Err(HandlerError {
                    code,
                    message,
                    detail: Some(detail),
                }),
                _ => panic!("unexpected handler outcome"),
            }
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            std::fs::remove_dir_all(&self.dir).unwrap();
        }
    }
    fn register(id: &str, key: &str) -> Value {
        json!({"projectId":id, "name":id, "roots":[], "requestKey":key})
    }
    fn agent(key: &str) -> Value {
        json!({"role":"assistant", "name":"Ada", "tag":"helper", "request_key":key})
    }

    #[tokio::test]
    async fn create_defaults_are_atomic_explicit_avatars_win_and_entropy_failure_refuses() {
        fn failed_entropy() -> Result<entorhinal_core::agent::StoredAgentAvatar, HandlerError> {
            Err(HandlerError::new("storage_error", "entropy unavailable"))
        }
        fn malformed_genome() -> Result<entorhinal_core::agent::StoredAgentAvatar, HandlerError> {
            Ok(entorhinal_core::agent::StoredAgentAvatar {
                genome: "ab".into(),
                avatar_type: "creature.wild".into(),
                version: Some(1),
            })
        }
        {
            let mut f = Fixture::new("create-avatars", Arc::new(FailConnector::default()));
            f.handler.agent_avatar_draw = draw_agent_avatar;
            let created: Value =
                serde_json::from_slice(&f.call("agent.create", agent("wild")).await.unwrap())
                    .unwrap();
            let id = created["result"]["agent"]["agent_id"].as_str().unwrap();
            let row = f.store().agent_row(id).unwrap().unwrap();
            assert_wild_avatar(&serde_json::to_value(&row.avatar).unwrap());
            let feed = f.store().agent_changes(0, None).unwrap();
            assert_eq!(feed.entries.len(), 1);
            assert_eq!(feed.entries[0].op, "agent.create");
            assert_eq!(feed.entries[0].row.avatar, row.avatar);
            assert_eq!(row.agent_generation, 1);
            f.handler.agent_avatar_draw = failed_entropy;
            let mut explicit = agent("explicit");
            explicit["name"] = json!("Explicit");
            explicit["avatar"] =
                json!({"type":"creature.classic","version":2,"genome":"cd".repeat(1024)});
            let reply: Value =
                serde_json::from_slice(&f.call("agent.create", explicit.clone()).await.unwrap())
                    .unwrap();
            let id = reply["result"]["agent"]["agent_id"].as_str().unwrap();
            assert_eq!(
                serde_json::to_value(f.store().agent_row(id).unwrap().unwrap().avatar).unwrap(),
                explicit["avatar"]
            );
            let before = serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap();
            let mut no_entropy = agent("no-entropy");
            no_entropy["name"] = json!("No entropy");
            assert_eq!(
                f.call("agent.create", no_entropy).await.unwrap_err().code,
                "storage_error"
            );
            assert_eq!(
                serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                before
            );
            let mut malformed = agent("malformed");
            malformed["name"] = json!("Malformed");
            malformed["avatar"] = json!({"type":"creature.wild","version":1,"genome":"ab"});
            assert_eq!(
                f.call("agent.create", malformed).await.unwrap_err().code,
                "invalid_request"
            );
            assert_eq!(
                serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                before
            );
            f.handler.agent_avatar_draw = malformed_genome;
            let mut malformed_default = agent("malformed-default");
            malformed_default["name"] = json!("Malformed default");
            assert_eq!(
                f.call("agent.create", malformed_default)
                    .await
                    .unwrap_err()
                    .code,
                "invalid_request"
            );
            assert_eq!(
                serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                before
            );
        }
    }

    #[tokio::test]
    async fn malformed_agent_cache_retry_keeps_strict_decode_and_enabling_fence() {
        let fail = Arc::new(FailConnector::default());
        let f = Fixture::new("write-agent-cache", fail.clone());
        f.call("agent.create", agent("cache")).await.unwrap();
        let mut malformed = agent("cache");
        malformed["residence"] = json!("removed-field");
        assert_eq!(
            f.call("agent.create", malformed).await.unwrap_err().code,
            "invalid_request"
        );
        let plan = "7b".repeat(16);
        f.store().begin_hub_enable(&plan, &"ab".repeat(16)).unwrap();
        assert_eq!(
            f.call("agent.create", agent("cache"))
                .await
                .unwrap_err()
                .code,
            "identity_log_enabling"
        );
        assert_eq!(fail.calls(), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn local_writes_and_liveness_never_touch_engram_on_enabled_store() {
        let fail = Arc::new(FailConnector::default());
        let mut f = Fixture::new("write-local", fail.clone());
        let resolver = crate::sessions::FakeResolver::new();
        resolver.set(
            crate::sessions::HOST_CAPABILITY,
            crate::sessions::ProviderResolution::Provider("prefrontal".into()),
        );
        f.handler.provider_resolver = resolver;
        let root = f.dir.join("repo");
        let container = f.dir.join("workers");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&container).unwrap();
        let root = crate::tests::canonical_test_path(root);
        let container = crate::tests::canonical_test_path(container);
        assert!(std::process::Command::new("git")
            .args(["init", "--quiet"])
            .arg(&root)
            .status()
            .unwrap()
            .success());
        // Register and bind the checkout before installing a hub base.
        f.store()
            .register(
                serde_json::from_value(
                    json!({"projectId":"P","name":"P","roots":[root],"workspaceId":"W"}),
                )
                .unwrap(),
            )
            .unwrap();
        {
            let mut store = f.handler.store.lock().unwrap();
            let store = store.as_mut().unwrap();
            store.set_root_records(true);
            store.bind_unbound_roots("test").unwrap();
        }
        f.store()
            .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                tx.execute(
                    "UPDATE identity_log_state SET state='enabled', hub_log_base=0, hub_ready=1",
                    [],
                )?;
                Ok(())
            })
            .unwrap();
        f.call("approve_root", json!({"root":root})).await.unwrap();
        let resolved: Value =
            serde_json::from_slice(&f.handler.resolve(json!({"canonicalRoot":root})).unwrap())
                .unwrap();
        let record = &resolved["result"]["rootRecords"][0];
        f.call("attach_derived_parent", json!({"projectId":"P","root":root,"incarnation":record["incarnation"]["value"],"registrationEpoch":record["registrationEpoch"],"container":container})).await.unwrap();
        f.call("unapprove_root", json!({"root":root}))
            .await
            .unwrap();
        f.call("set_owned_remotes", json!({"root":root,"remotes":[]}))
            .await
            .unwrap();
        f.call(
            "set_workspace_root",
            json!({"workspaceId":"W","root":container}),
        )
        .await
        .unwrap();
        f.call("set_workspace_root", json!({"workspaceId":"W","root":null}))
            .await
            .unwrap();
        let lock = f.handler.writer.lock().await;
        ready(f.call(
            "agent.session_liveness",
            json!({"refs":[{"ref_kind":"pi","ref":"missing"}]}),
        ))
        .unwrap();
        drop(lock);
        f.call("remove_root", json!({"projectId":"P","root":root}))
            .await
            .unwrap();
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
        assert_eq!(f.pending(), 0);
        assert_eq!(fail.calls(), 0);
    }

    mod confirmation {
        use super::*;
        use crate::agent_ops::{OperatorConfirmError, OperatorConfirmer};
        const DIRECT: RouteKey = (72, 1);
        const ID: &str = "agent_0123456789abcdef";

        type PromptAnswer = tokio::sync::oneshot::Sender<Result<(), OperatorConfirmError>>;
        struct Prompt {
            calls: Mutex<Vec<(String, RouteKey)>>,
            answers: Mutex<Vec<Option<PromptAnswer>>>,
            immediate: Option<Result<(), OperatorConfirmError>>,
        }
        impl Prompt {
            fn new(immediate: Option<Result<(), OperatorConfirmError>>) -> Arc<Self> {
                Arc::new(Self {
                    calls: Mutex::new(vec![]),
                    answers: Mutex::new(vec![]),
                    immediate,
                })
            }
            fn count(&self) -> usize {
                self.calls.lock().unwrap().len()
            }
            fn answer(&self, index: usize) {
                let _ = self.answers.lock().unwrap()[index]
                    .take()
                    .unwrap()
                    .send(Ok(()));
            }
        }
        #[async_trait]
        impl OperatorConfirmer for Prompt {
            async fn confirm_operator(
                &self,
                summary: &str,
                route: RouteKey,
            ) -> Result<(), OperatorConfirmError> {
                self.calls.lock().unwrap().push((summary.into(), route));
                if let Some(result) = self.immediate {
                    return result;
                }
                let (tx, rx) = tokio::sync::oneshot::channel();
                self.answers.lock().unwrap().push(Some(tx));
                rx.await.unwrap_or(Err(OperatorConfirmError::Declined))
            }
        }
        fn direct(f: &mut Fixture, prompt: Arc<Prompt>) {
            f.handler.operator_confirmer = prompt;
            bind(f);
        }
        fn bind(f: &Fixture) {
            f.handler.route_admissions().insert(
                DIRECT,
                RouteAdmission {
                    principal: Some(Principal::Direct),
                    flow_id: None,
                    handle: None,
                    scope: None,
                    call_mode: None,
                    providers: Default::default(),
                },
            );
        }
        fn state(f: &Fixture, state: &str) {
            f.store()
                .apply_entry("fixture.state", "{}", "test", None, |tx| {
                    tx.execute("UPDATE identity_log_state SET state=?1", [state])?;
                    Ok(())
                })
                .unwrap();
        }
        fn write<'a>(
            f: &'a Fixture,
            op: &str,
            params: Value,
        ) -> impl Future<Output = Result<Vec<u8>, HandlerError>> + 'a {
            f.handler.write_wait(
                WireRequest {
                    method: op.into(),
                    params,
                },
                DIRECT,
                Instant::now(),
            )
        }
        fn image(f: &Fixture) -> Value {
            json!({"journal":f.image(), "snapshot":f.store().agent_snapshot().unwrap(), "pending":f.pending(), "generation":f.store().generation().unwrap()})
        }
        fn rows(f: &Fixture) -> Vec<entorhinal_core::JournalEntry> {
            // The project feed intentionally excludes identities. Inspect the
            // real identity journal in a transaction that always rolls back;
            // neither its observation marker nor a generation is committed.
            let mut observed = None;
            let result = f.store().apply_entry("fixture.inspect", "{}", "test", None, |tx| {
                observed = Some(tx.prepare("SELECT seq,op,payload_json,actor,request_key,created_at,principal FROM registry_journal WHERE op IN ('agent.create','agent.rename','agent.dispose','agent.update_tag','agent.set_labels') ORDER BY seq")?
                    .query_map([], |r| Ok(entorhinal_core::JournalEntry { seq:r.get(0)?,op:r.get(1)?,payload_json:r.get(2)?,actor:r.get(3)?,request_key:r.get(4)?,created_at:r.get(5)?,principal:r.get(6)? }))?
                    .collect::<Result<Vec<_>,_>>()?);
                tx.execute_batch("SELECT fixture_observation_rollback;")
            });
            assert!(result.is_err());
            observed.unwrap()
        }
        fn stable_draw() -> Result<String, HandlerError> {
            Ok(ID.into())
        }
        fn new_request(key: &str) -> Value {
            json!({"role":"assistant","name":"Ada","tag":"helper","request_key":key,"actor":"ck-agents"})
        }
        fn id(body: &[u8]) -> String {
            serde_json::from_slice::<Value>(body).unwrap()["result"]["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }
        fn methods(id: &str) -> Vec<(&'static str, Value)> {
            vec![
                (
                    "agent.rename",
                    json!({"agent_id":id,"name":"Grace","request_key":"rename"}),
                ),
                (
                    "agent.update_tag",
                    json!({"agent_id":id,"tag":"q\"\\\né","request_key":"tag"}),
                ),
                (
                    "agent.set_labels",
                    json!({"agent_id":id,"labels":["one","é"],"request_key":"labels"}),
                ),
                (
                    "agent.set_labels",
                    json!({"agent_id":id,"labels":[],"request_key":"none"}),
                ),
                (
                    "agent.dispose",
                    json!({"agent_id":id,"request_key":"dispose"}),
                ),
            ]
        }

        #[tokio::test]
        async fn confirmed_methods_pin_summaries_and_local_attribution() {
            {
                let mut f = Fixture::new("confirmed-summaries", Arc::new(FailConnector::default()));
                let prompt = Prompt::new(Some(Ok(())));
                direct(&mut f, prompt.clone());
                f.handler.agent_id_draw = stable_draw;
                let mut params = new_request("create");
                params["tag"] = json!("q\"\\\né");
                let notified = f.handler.commits.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let target = id(&write(&f, "agent.create", params).await.unwrap());
                ready(notified.as_mut());
                for (op, params) in methods(&target) {
                    write(&f, op, params).await.unwrap();
                }
                let calls = prompt.calls.lock().unwrap().clone();
                let expected = [
                    "create assistant agent \"Ada\" with tag \"q\\\"\\\\\\né\"".into(),
                    format!("rename agent \"Ada\" ({target:?}) to \"Grace\""),
                    format!("set tag of agent \"Grace\" ({target:?}) to \"q\\\"\\\\\\né\""),
                    format!("set labels of agent \"Grace\" ({target:?}) to \"one\", \"é\""),
                    format!("set labels of agent \"Grace\" ({target:?}) to none"),
                    format!("retire agent \"Grace\" ({target:?})"),
                ];
                assert_eq!(
                    calls,
                    expected
                        .into_iter()
                        .map(|s| (s, DIRECT))
                        .collect::<Vec<_>>()
                );
                let journal = rows(&f);
                assert_eq!(journal.len(), 6);
                for row in &journal {
                    assert_eq!(row.principal.as_deref(), Some("direct"));
                    let payload: Value = serde_json::from_str(&row.payload_json).unwrap();
                    assert_eq!(payload["operator_confirmed"], true);
                    assert!(payload["entry"].get("operator_confirmed").is_none());
                    let _: entorhinal_core::agent::AgentChangeEntry =
                        serde_json::from_value(payload["entry"].clone()).unwrap();
                }
                let snapshot = f.store().agent_snapshot().unwrap();
                let changes =
                    serde_json::to_value(f.store().agent_changes(0, None).unwrap()).unwrap();
                assert!(!changes.to_string().contains("operator_confirmed"));
                f.store().rebuild().unwrap();
                assert_eq!(
                    serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                    serde_json::to_value(snapshot).unwrap()
                );
            }
        }

        async fn placed_head(f: &Fixture) -> String {
            f.call(
                "register",
                json!({"projectId":"P","name":"P","workspaceId":"W1","roots":[],"requestKey":"P"}),
            )
            .await
            .unwrap();
            id(&f.call("agent.create", json!({"role":"head","name":"Head","tag":"ok","project_id":"P","request_key":"head"})).await.unwrap())
        }

        #[tokio::test]
        async fn carrying_placement_prompts_exactly_and_cached_reply_is_unchanged() {
            {
                let fail = Arc::new(FailConnector::default());
                let mut f = Fixture::new("carry-confirmed", fail.clone());
                let prompt = Prompt::new(Some(Ok(())));
                direct(&mut f, prompt.clone());
                write(&f, "register", json!({"projectId":"empty","name":"Empty","roots":[],"workspaceId":"W1","requestKey":"empty"})).await.unwrap();
                assert_eq!(prompt.count(), 0);
                placed_head(&f).await;
                let params = json!({"projectId":"P","workspaceId":"W2","requestKey":"carry"});
                let committed = write(&f, "assign_workspace", params.clone()).await.unwrap();
                assert_eq!(
                    *prompt.calls.lock().unwrap(),
                    vec![("move \"P\" to \"W2\": \"Head\"".into(), DIRECT)]
                );
                assert!(
                    serde_json::from_slice::<Value>(&committed).unwrap()["result"]
                        .get("incarnation")
                        .is_none()
                );
                assert_eq!(
                    committed,
                    f.store()
                        .shared_request_cache("assign_workspace", Some("carry"))
                        .unwrap()
                        .unwrap()
                );
                prompt.calls.lock().unwrap().clear();
                let before = image(&f);
                assert_eq!(
                    write(&f, "assign_workspace", params).await.unwrap(),
                    committed
                );
                assert_eq!(prompt.count(), 0);
                assert_eq!(image(&f), before);
                assert!(f.store().verify().unwrap().ok);
                assert!(f.store().rebuild().unwrap().replay.ok);
            }
        }

        #[tokio::test]
        async fn carrying_refusals_precede_prompt_and_sync_direct_requires_approval() {
            let fail = Arc::new(FailConnector::default());
            let mut f = Fixture::new("carry-refusals", fail.clone());
            let prompt = Prompt::new(Some(Ok(())));
            direct(&mut f, prompt.clone());
            let moving = placed_head(&f).await;
            f.call(
                "register",
                json!({"projectId":"Q","name":"Q","workspaceId":"W2","roots":[],"requestKey":"Q"}),
            )
            .await
            .unwrap();
            let holder = id(&f.call("agent.create", json!({"role":"head","name":"Head","tag":"ok","project_id":"Q","request_key":"holder"})).await.unwrap());
            let before = image(&f);
            let error = write(
                &f,
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W2","requestKey":"taken"}),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "name_conflict");
            assert_eq!(
                error.detail.unwrap(),
                json!({"agent_id":moving,"holder_agent_id":holder,"workspace_id":"W2"})
            );
            assert_eq!(prompt.count(), 0);
            assert_eq!(image(&f), before);
            assert_eq!(
                write(
                    &f,
                    "assign_workspace",
                    json!({"projectId":"missing","workspaceId":"W3"})
                )
                .await
                .unwrap_err()
                .code,
                "not_found"
            );
            let params = json!({"projectId":"P","workspaceId":"W3","requestKey":"sync"});
            let body =
                serde_json::to_vec(&json!({"method":"assign_workspace","params":params})).unwrap();
            assert!(
                matches!(f.handler.handle_request(&body, DIRECT), HandlerOutcome::Error {code, ..} if code == "operator_approval_stale")
            );
            assert_eq!(image(&f), before);
            assert!(matches!(
                f.handler.handle_request(&body, ROUTE),
                HandlerOutcome::Response(_)
            ));
            f.call(
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W1","requestKey":"executive"}),
            )
            .await
            .unwrap();
            assert_eq!(prompt.count(), 0);
            assert!(f.store().verify().unwrap().ok);
            assert!(f.store().rebuild().unwrap().replay.ok);
        }

        #[tokio::test]
        async fn carrying_approval_rechecks_summary_under_writer_lock() {
            let fail = Arc::new(FailConnector::default());
            let mut f = Fixture::new("carry-stale", fail.clone());
            let prompt = Prompt::new(None);
            direct(&mut f, prompt.clone());
            let moving = placed_head(&f).await;
            let mut waiting = Box::pin(write(
                &f,
                "assign_workspace",
                json!({"projectId":"P","workspaceId":"W2","requestKey":"stale"}),
            ));
            pending(waiting.as_mut());
            assert_eq!(prompt.count(), 1);
            f.call(
                "agent.rename",
                json!({"agent_id":moving,"name":"Changed","request_key":"rename"}),
            )
            .await
            .unwrap();
            let before = image(&f);
            prompt.answer(0);
            assert_eq!(waiting.await.unwrap_err().code, "operator_approval_stale");
            assert_eq!(image(&f), before);
        }

        #[tokio::test]
        async fn refusals_are_unpersisted_and_retries_prompt_again() {
            for error in [
                OperatorConfirmError::Declined,
                OperatorConfirmError::PresenceUnavailable,
                OperatorConfirmError::Unsupported,
            ] {
                let mut f =
                    Fixture::new("confirmation-refused", Arc::new(FailConnector::default()));
                let prompt = Prompt::new(Some(Err(error)));
                direct(&mut f, prompt.clone());
                let before = image(&f);
                for _ in 0..2 {
                    let result = write(&f, "agent.create", new_request("refused"))
                        .await
                        .unwrap_err();
                    assert_eq!(
                        result.code,
                        if matches!(error, OperatorConfirmError::Declined) {
                            "operator_declined"
                        } else {
                            "operator_presence_unavailable"
                        }
                    );
                    assert_eq!(image(&f), before);
                }
                assert_eq!(prompt.count(), 2);
            }
        }

        #[tokio::test]
        async fn prechecks_replay_and_domain_refusals_do_not_wait_for_writer_or_prompt() {
            {
                let mut f =
                    Fixture::new("confirmation-precheck", Arc::new(FailConnector::default()));
                let prompt = Prompt::new(Some(Ok(())));
                direct(&mut f, prompt.clone());
                let landed = write(&f, "agent.create", new_request("landed"))
                    .await
                    .unwrap();
                let target = id(&landed);
                f.call("register", json!({"projectId":"P","name":"P","workspaceId":"W1","roots":[],"requestKey":"P"})).await.unwrap();
                f.call("register", register("unplaced", "unplaced"))
                    .await
                    .unwrap();
                f.call("agent.create", json!({"role":"head","name":"Head","tag":"ok","project_id":"P","request_key":"head"})).await.unwrap();
                let lock = f.handler.writer.lock().await;
                let before = image(&f);
                let body = ready(write(&f, "agent.create", json!({"role":"assistant","name":"different","tag":"changed","request_key":"landed"}))).unwrap();
                assert_eq!(id(&body), target);
                assert_eq!(body, landed);
                for (op, params, code) in [
                    (
                        "agent.rename",
                        json!({"agent_id":target,"name":"Other","request_key":"landed"}),
                        "request_key_reused_across_ops",
                    ),
                    (
                        "agent.create",
                        json!({"role":"assistant","name":"X","tag":"ok"}),
                        "request_key_required",
                    ),
                    (
                        "agent.create",
                        json!({"role":"assistant","name":"","tag":"ok","request_key":"bad"}),
                        "invalid_name",
                    ),
                    (
                        "agent.create",
                        json!({"role":"head","name":"Another","tag":"ok","project_id":"P","request_key":"taken"}),
                        "agent_project_taken",
                    ),
                    (
                        "agent.create",
                        json!({"role":"head","name":"Head","tag":"ok","project_id":"unplaced","request_key":"workspace"}),
                        "unresolved_workspace",
                    ),
                    (
                        "agent.rename",
                        json!({"agent_id":"agent_00000000","name":"X","request_key":"unknown"}),
                        "unknown_agent",
                    ),
                    (
                        "agent.create",
                        json!({"role":"head","name":"H","tag":"ok","request_key":"shape"}),
                        "invalid_role_shape",
                    ),
                    (
                        "agent.create",
                        json!({"role":"hiree","name":"Hire","tag":"ok","project_id":"P","supervisor_agent_id":target,"request_key":"supervisor"}),
                        "invalid_supervisor",
                    ),
                    (
                        "agent.create",
                        json!({"role":"assistant","name":"X","tag":"ok","request_key":"fields","bad":true}),
                        "invalid_request",
                    ),
                ] {
                    assert_eq!(ready(write(&f, op, params)).unwrap_err().code, code);
                }
                assert_eq!(image(&f), before);
                assert_eq!(prompt.count(), 1);
                drop(lock);
                for state_name in ["enabling", "joining"] {
                    state(&f, state_name);
                    for key in ["landed", "fresh"] {
                        assert_eq!(
                            ready(write(&f, "agent.create", new_request(key)))
                                .unwrap_err()
                                .code,
                            "identity_log_enabling"
                        );
                    }
                }
                state(&f, "disabled");
                f.store()
                    .apply_entry("fixture.uncut", "{}", "test", None, |tx| {
                        tx.execute("DELETE FROM registry_journal WHERE op='agent.cutover'", [])?;
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(
                    ready(write(&f, "agent.create", new_request("uncut")))
                        .unwrap_err()
                        .code,
                    "authority_not_cut_over"
                );
                assert_eq!(
                    id(&ready(write(&f, "agent.create", new_request("landed"))).unwrap()),
                    target
                );
                f.handler
                    .route_admissions()
                    .get_mut(&DIRECT)
                    .unwrap()
                    .flow_id = Some("flow".into());
                assert_eq!(
                    ready(write(&f, "agent.create", new_request("flow")))
                        .unwrap_err()
                        .code,
                    "flow_scope_not_admitted"
                );
                assert_eq!(prompt.count(), 1);
            }
            let f = Fixture::new(
                "confirmation-default-unsupported",
                Arc::new(FailConnector::default()),
            );
            bind(&f);
            state(&f, "disabled");
            let before = image(&f);
            let error = write(&f, "agent.create", new_request("unsupported"))
                .await
                .unwrap_err();
            assert_eq!(error.code, "operator_presence_unavailable");
            assert_eq!(
                error.message,
                "this daemon does not support operator confirmation"
            );
            assert_eq!(image(&f), before);
        }

        #[tokio::test]
        async fn escaped_summary_cap_is_scalar_based_and_placement_is_pinned() {
            let mut f = Fixture::new(
                "confirmation-summary-cap",
                Arc::new(FailConnector::default()),
            );
            let prompt = Prompt::new(Some(Ok(())));
            direct(&mut f, prompt.clone());
            // Literal escaped prefix, followed by a multibyte tag and an
            // interior newline (trailing whitespace would be trimmed).
            let prefix = "create assistant agent \"Ada\" with tag \"";
            let n = 200 - prefix.chars().count() - 4;
            let tag = format!("{}\nx", "é".repeat(50) + &"x".repeat(n - 50));
            let mut params = new_request("cap");
            params["tag"] = json!(tag);
            write(&f, "agent.create", params.clone()).await.unwrap();
            assert_eq!(prompt.calls.lock().unwrap()[0].0.chars().count(), 200);
            params["request_key"] = json!("over");
            params["name"] = json!("Bob");
            params["tag"] = json!(format!("{tag}x"));
            assert_eq!(
                ready(write(&f, "agent.create", params)).unwrap_err().code,
                "operator_summary_too_long"
            );
            assert_eq!(prompt.count(), 1);
            f.call("register", json!({"projectId":"P","name":"P","workspaceId":"W","roots":[],"requestKey":"project"})).await.unwrap();
            f.call(
                "agent.create",
                json!({"role":"head","name":"H","tag":"ok","project_id":"P","request_key":"H"}),
            )
            .await
            .unwrap();
            let head = f
                .store()
                .agent_snapshot()
                .unwrap()
                .agents
                .iter()
                .find(|a| a.role == "head")
                .unwrap()
                .agent_id
                .clone();
            write(&f, "agent.create", json!({"role":"hiree","name":"Hire","tag":"ok","project_id":"P","supervisor_agent_id":head,"request_key":"hire"})).await.unwrap();
            assert_eq!(prompt.calls.lock().unwrap()[1].0, format!("create hiree agent \"Hire\" for project \"P\" in workspace \"W\" supervised by \"H\" ({head:?}) with tag \"ok\""));
            write(&f, "agent.create", json!({"role":"workspace_head","name":"WH","tag":"ok","workspace_id":"W","request_key":"WH"})).await.unwrap();
            assert_eq!(
                prompt.calls.lock().unwrap()[2].0,
                "create workspace head agent \"WH\" for workspace \"W\" with tag \"ok\""
            );
        }

        #[tokio::test]
        async fn relay_bytes_are_unchanged_and_unconfirmed_seams_stay_closed() {
            let mut f = Fixture::new("confirmation-relay", Arc::new(FailConnector::default()));
            let mut baseline = Fixture::new(
                "confirmation-relay-baseline",
                Arc::new(FailConnector::default()),
            );
            let prompt = Prompt::new(Some(Err(OperatorConfirmError::Declined)));
            direct(&mut f, prompt.clone());
            f.handler.agent_id_draw = stable_draw;
            baseline.handler.agent_id_draw = stable_draw;
            for (op, params) in
                std::iter::once(("agent.create", new_request("create"))).chain(methods(ID))
            {
                assert_eq!(
                    f.call(op, params.clone()).await.unwrap(),
                    baseline.call(op, params).await.unwrap()
                );
            }
            let actual = rows(&f);
            let expected = rows(&baseline);
            assert_eq!(
                serde_json::to_vec(&actual).unwrap(),
                serde_json::to_vec(&expected).unwrap()
            );
            assert!(actual
                .iter()
                .all(|r| r.principal.as_deref() == Some("reserved:prefrontal")
                    && !r.payload_json.contains("operator_confirmed")));
            let before = image(&f);
            for op in [
                "agent.set_avatar",
                "agent.set_github_identity",
                "agent.merge",
                "agent.import",
            ] {
                assert_eq!(
                    ready(write(&f, op, Value::Null)).unwrap_err().code,
                    "direct_identity_write_not_admitted"
                );
            }
            for cut in [true, false] {
                if !cut {
                    f.store()
                        .apply_entry("fixture.uncut", "{}", "test", None, |tx| {
                            tx.execute(
                                "DELETE FROM registry_journal WHERE op='agent.cutover'",
                                [],
                            )?;
                            Ok(())
                        })
                        .unwrap();
                }
                let snapshot = image(&f);
                for op in agent_ops::CONFIRMABLE_METHODS
                    .iter()
                    .filter(|op| op.starts_with("agent."))
                {
                    let body = serde_json::to_vec(&json!({"method":op,"params":null})).unwrap();
                    let outcome = f.handler.handle_request(&body, DIRECT);
                    assert!(
                        matches!(outcome, HandlerOutcome::Error {code, ..} | HandlerOutcome::ErrorWithDetail {code, ..} if code == "direct_identity_write_not_admitted")
                    );
                }
                assert_eq!(image(&f), snapshot);
            }
            assert_eq!(prompt.count(), 0);
            assert_eq!(
                before["snapshot"]["agents"],
                image(&f)["snapshot"]["agents"]
            );
            assert_eq!(
                before["snapshot"]["claims"],
                image(&f)["snapshot"]["claims"]
            );
        }

        #[tokio::test]
        async fn prompt_holds_no_locks_and_revalidation_catches_name_and_placement_changes() {
            for change in ["name", "placement"] {
                let fail = Arc::new(FailConnector::default());
                let mut f = Fixture::new("confirmation-unlocked", fail.clone());
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone());
                let params = if change == "placement" {
                    f.call("register", json!({"projectId":"P","name":"P","workspaceId":"W1","roots":[],"requestKey":"P"})).await.unwrap();
                    json!({"role":"head","name":"Ada","tag":"ok","project_id":"P","request_key":"direct"})
                } else {
                    new_request("direct")
                };
                let mut waiting = Box::pin(write(&f, "agent.create", params));
                pending(waiting.as_mut());
                assert_eq!(prompt.count(), 1);
                // Assert completion order while the prompt is still unanswered.
                f.call("register", register("other", "other"))
                    .await
                    .unwrap();
                if change == "name" {
                    f.call("agent.create", new_request("core")).await.unwrap();
                } else {
                    f.call(
                        "assign_workspace",
                        json!({"projectId":"P","workspaceId":"W2","requestKey":"move"}),
                    )
                    .await
                    .unwrap();
                }
                let before = image(&f);
                prompt.answer(0);
                assert_eq!(
                    waiting.await.unwrap_err().code,
                    if change == "name" {
                        "name_conflict"
                    } else {
                        "operator_approval_stale"
                    }
                );
                assert_eq!(image(&f), before);
                assert_eq!(prompt.count(), 1);
            }
        }

        #[tokio::test(start_paused = true)]
        async fn busy_slot_survives_approval_and_releases_on_finish_drop_and_deadline() {
            let mut f = Fixture::new("confirmation-busy", Arc::new(FailConnector::default()));
            let prompt = Prompt::new(None);
            direct(&mut f, prompt.clone());
            let mut first = Box::pin(write(&f, "agent.create", new_request("first")));
            pending(first.as_mut());
            assert_eq!(
                ready(write(&f, "agent.create", new_request("second")))
                    .unwrap_err()
                    .code,
                "operator_confirmation_busy"
            );
            let lock = f.handler.writer.lock().await;
            prompt.answer(0);
            pending(first.as_mut());
            assert_eq!(
                ready(write(&f, "agent.create", new_request("second")))
                    .unwrap_err()
                    .code,
                "operator_confirmation_busy"
            );
            drop(lock);
            first.await.unwrap();
            let mut next = new_request("next");
            next["name"] = json!("Bob");
            let mut abandoned = Box::pin(write(&f, "agent.create", next.clone()));
            pending(abandoned.as_mut());
            drop(abandoned);
            prompt.answer(1);
            assert_eq!(rows(&f).len(), 1);
            let received = Instant::now();
            let mut expired = Box::pin(f.handler.write_wait(
                WireRequest {
                    method: "agent.create".into(),
                    params: next.clone(),
                },
                DIRECT,
                received,
            ));
            pending(expired.as_mut());
            tokio::time::advance(Duration::from_secs(270) + TIMER_TICK).await;
            assert_eq!(
                ready(expired.as_mut()).unwrap_err().code,
                "operator_declined"
            );
            drop(expired);
            let mut last = Box::pin(write(&f, "agent.create", next));
            pending(last.as_mut());
            prompt.answer(3);
            last.await.unwrap();
            assert_eq!(prompt.count(), 4);
            assert_eq!(rows(&f).len(), 2);
        }

        #[tokio::test]
        async fn route_closure_wins_ready_prompt_and_lock_and_admission_slot_gap() {
            for stage in ["prompt", "lock", "admission"] {
                let fail = Arc::new(FailConnector::default());
                let mut f = Fixture::new("confirmation-route-close", fail.clone());
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone());
                let barrier = Arc::new(tokio::sync::Barrier::new(2));
                if stage == "admission" {
                    f.handler.confirmation_admission_barrier = Some(barrier.clone());
                }
                let lock = f.handler.writer.clone().lock_owned().await;
                let before = image(&f);
                let mut first = Box::pin(write(&f, "agent.create", new_request("closed")));
                pending(first.as_mut());
                if stage == "lock" {
                    prompt.answer(0);
                    pending(first.as_mut());
                }
                f.handler
                    .on_route_gone(&RouteHandle::detached(DIRECT.0, DIRECT.1))
                    .await;
                if stage == "prompt" {
                    prompt.answer(0);
                }
                if stage == "admission" {
                    barrier.wait().await;
                }
                drop(lock);
                assert_eq!(first.await.unwrap_err().code, "operator_declined");
                assert_eq!(prompt.count(), if stage == "admission" { 0 } else { 1 });
                assert_eq!(image(&f), before);
                assert_eq!(fail.calls(), 0);
                f.handler.confirmation_admission_barrier = None;
                bind(&f);
                let count = prompt.count();
                let mut later = Box::pin(write(&f, "agent.create", new_request("live")));
                pending(later.as_mut());
                prompt.answer(count);
                later.await.unwrap();
                assert_eq!(rows(&f).len(), 1);
            }
        }

        #[tokio::test(start_paused = true)]
        async fn approval_starts_stage_deadlines_and_late_approval_is_clamped() {
            for late in [false, true] {
                let mut f = Fixture::new(
                    "confirmation-stage-deadline",
                    Arc::new(FailConnector::default()),
                );
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone());
                let received = Instant::now();
                let mut first = Box::pin(write(&f, "agent.create", new_request("deadline")));
                pending(first.as_mut());
                tokio::time::advance(Duration::from_secs(if late { 260 } else { 30 })).await;
                let approval = Instant::now();
                let lock = f.handler.writer.clone().lock_owned().await;
                let before = image(&f);
                prompt.answer(0);
                pending(first.as_mut());
                let bound =
                    (approval + Duration::from_secs(25)).min(received + Duration::from_secs(270));
                tokio::time::advance(
                    bound.saturating_duration_since(Instant::now()) - Duration::from_secs(1),
                )
                .await;
                pending(first.as_mut());
                tokio::time::advance(Duration::from_secs(1) + TIMER_TICK).await;
                assert_eq!(
                    ready(first.as_mut()).unwrap_err().code,
                    "identity_log_contended"
                );
                drop(lock);
                assert_eq!(prompt.count(), 1);
                assert_eq!(image(&f), before);
            }
            // A late approval still gets a fresh write budget.
            let mut f = Fixture::new(
                "confirmation-delayed-success",
                Arc::new(FailConnector::default()),
            );
            let prompt = Prompt::new(None);
            direct(&mut f, prompt.clone());
            let mut first = Box::pin(write(&f, "agent.create", new_request("delayed")));
            pending(first.as_mut());
            tokio::time::advance(Duration::from_secs(30)).await;
            prompt.answer(0);
            finish(first.as_mut()).await.unwrap();
            assert_eq!(rows(&f).len(), 1);
        }
        /// The tests above answer the prompt through a stub confirmer. These
        /// serve the real handler, with the confirmer production builds, over
        /// the SDK's in-process stand-in daemon: the `operator.confirm` request,
        /// the scripted answer and any withdrawal cross the SDK's real
        /// connection and its real error mapping.
        mod served {
            use super::*;
            use crate::agent_ops::DaemonConfirmer;
            use subc_client_rs::test_support::{
                BindIdentity, Frame, FrameType, ModuleHarness, OperatorAnswer, OperatorRefusal,
                RouteTarget,
            };

            /// The stand-in's HELLO_ACK carries no storage descriptor, and the
            /// real `on_hello_ack` refuses to open a store without one.
            /// This wrapper gives it the fixture's scratch descriptor and
            /// forwards every hook `ProjectsHandler` implements, unchanged.
            struct Scratch {
                handler: Arc<ProjectsHandler>,
                descriptor: StorageDescriptor,
            }
            #[async_trait]
            impl ModuleHandler for Scratch {
                async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
                    self.handler.handle(ctx, body).await
                }
                async fn on_hello_ack(&self, ack: &ModuleHelloAckBody) {
                    let ack = ModuleHelloAckBody {
                        storage: Some(serde_json::to_value(&self.descriptor).unwrap()),
                        ..ack.clone()
                    };
                    self.handler.on_hello_ack(&ack).await;
                }
                async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
                    self.handler.on_bind(request).await
                }
                async fn on_route_gone(&self, handle: &RouteHandle) {
                    self.handler.on_route_gone(handle).await;
                }
                async fn health(&self) -> HealthReport {
                    self.handler.health().await
                }
            }

            /// Serve a fresh handler over the fixture's store, which already
            /// holds the `agent.cutover` marker, and bind one `Direct` route.
            /// Also returns a clone of the served handler: its background
            /// tasks keep the store files open, so a test must stop them
            /// (`finish`) before the fixture deletes its folder, or Windows
            /// refuses the deletion.
            async fn start(
                f: &mut Fixture,
                answers: impl IntoIterator<Item = OperatorAnswer>,
            ) -> (ModuleHarness, RouteHandle, Arc<ProjectsHandler>) {
                // Disabled keeps the write local, so no log stand-in is needed.
                state(f, "disabled");
                // Release the fixture's store and its lease. The served
                // handler's own `on_hello_ack` reopens the same file into the
                // same shared slot, so the fixture's helpers read its writes.
                f.handler.store.lock().unwrap().take();
                let mut handler = ProjectsHandler::with_log_connector(
                    "0123456789abcdef".into(),
                    || 700,
                    Arc::new(FailConnector::default()),
                );
                handler.store = f.handler.store.clone();
                // Built as `serve_module` builds it: the confirmer shares the
                // handler's route admissions and receives the live module
                // handle before any request is served.
                let confirmer = Arc::new(DaemonConfirmer::new(handler.route_admissions.clone()));
                let handler = Arc::new(handler.with_operator_confirmer(confirmer.clone()));
                let served = handler.clone();
                let descriptor = f.descriptor.clone();
                let mut module = ModuleHarness::start(
                    move |handle| {
                        confirmer.set_module_handle(handle);
                        Scratch {
                            handler,
                            descriptor,
                        }
                    },
                    answers,
                )
                .await;
                assert!(
                    f.handler.store.lock().unwrap().is_some(),
                    "the served handler must open the scratch store"
                );
                let route = module
                    .bind_route(
                        RouteBindRequest::new(
                            RouteHandle::detached(DIRECT.0, DIRECT.1),
                            RouteTarget::ToolProvider {
                                module_id: MODULE_ID.into(),
                            },
                            BindIdentity::new("test", "ck-agents"),
                        )
                        .with_principal(Principal::Direct),
                    )
                    .await;
                (module, route, served)
            }

            /// Shut the harness down and stop the served handler's background
            /// tasks, so no task still holds the store when the fixture drops.
            async fn finish(module: &mut ModuleHarness, served: &ProjectsHandler) {
                module.shutdown().await;
                crate::tests::stop_store_tasks(served).await;
            }

            async fn create(module: &mut ModuleHarness, route: RouteHandle) -> Frame {
                let body = json!({"method":"agent.create","params":new_request("served")});
                let corr = module
                    .send_request(route, serde_json::to_vec(&body).unwrap())
                    .await;
                tokio::time::timeout(Duration::from_secs(30), module.read_reply(route, corr))
                    .await
                    .expect("the served handler never replied")
            }

            /// The one confirm request the module sent, checked against the
            /// exact summary and the caller's route; returns its correlation.
            fn one_confirm(module: &ModuleHarness) -> u64 {
                let requests: Vec<_> = module.confirm_requests().collect();
                assert_eq!(requests.len(), 1, "{requests:?}");
                let (corr, request) = &requests[0];
                assert_eq!(
                    request.summary,
                    "create assistant agent \"Ada\" with tag \"helper\""
                );
                assert_eq!((request.route_channel, request.route_epoch), DIRECT);
                *corr
            }

            fn cancels(module: &ModuleHarness) -> Vec<(u16, u32, u64, usize)> {
                module
                    .observed_frames()
                    .iter()
                    .filter(|frame| frame.header.ty == FrameType::Cancel)
                    .map(|frame| {
                        let h = &frame.header;
                        (h.channel, h.epoch, h.corr, frame.body.len())
                    })
                    .collect()
            }

            fn error(reply: &Frame) -> Value {
                assert_eq!(
                    reply.header.ty,
                    FrameType::Error,
                    "{}",
                    String::from_utf8_lossy(&reply.body)
                );
                serde_json::from_slice(&reply.body).unwrap()
            }

            #[tokio::test]
            async fn confirmed_direct_create_commits_with_the_exact_summary_and_route() {
                let mut f = Fixture::new("served-confirmed", Arc::new(FailConnector::default()));
                let (mut module, route, served) = start(&mut f, [OperatorAnswer::Confirmed]).await;
                let generation = f.store().generation().unwrap();
                let reply = create(&mut module, route).await;
                assert_eq!(
                    reply.header.ty,
                    FrameType::Response,
                    "{}",
                    String::from_utf8_lossy(&reply.body)
                );
                let agent = id(&reply.body);
                one_confirm(&module);
                finish(&mut module, &served).await;
                assert!(
                    cancels(&module).is_empty(),
                    "a confirmed prompt is not withdrawn"
                );
                let journal = rows(&f);
                assert_eq!(journal.len(), 1);
                assert_eq!(journal[0].op, "agent.create");
                assert_eq!(journal[0].principal.as_deref(), Some("direct"));
                let payload: Value = serde_json::from_str(&journal[0].payload_json).unwrap();
                assert_eq!(payload["operator_confirmed"], true);
                assert_wild_avatar(&payload["entry"]["row"]["avatar"]);
                assert_eq!(
                    serde_json::to_value(f.store().agent_row(&agent).unwrap().unwrap().avatar)
                        .unwrap(),
                    payload["entry"]["row"]["avatar"]
                );
                assert!(f.store().generation().unwrap() > generation);
                assert!(serde_json::to_string(&f.store().agent_snapshot().unwrap())
                    .unwrap()
                    .contains(&agent));
            }

            #[tokio::test]
            async fn every_scripted_refusal_maps_to_its_code_and_writes_nothing() {
                let declined = (
                    "operator_declined",
                    "the operator declined this write at the prompt",
                );
                let unavailable = (
                    "operator_presence_unavailable",
                    "operator confirmation is unavailable on this machine",
                );
                let refused = |refusal| OperatorAnswer::Refused {
                    refusal,
                    reason: "scripted refusal".into(),
                };
                for (answer, (code, message)) in [
                    (refused(OperatorRefusal::Declined), declined),
                    (refused(OperatorRefusal::PresenceUnavailable), unavailable),
                    (refused(OperatorRefusal::SummaryInvalid), unavailable),
                    (refused(OperatorRefusal::NotPermitted), unavailable),
                    (
                        OperatorAnswer::UnknownOp,
                        (
                            "operator_presence_unavailable",
                            "this daemon does not support operator confirmation",
                        ),
                    ),
                ] {
                    let label = format!("{answer:?}");
                    let mut f = Fixture::new("served-refused", Arc::new(FailConnector::default()));
                    let (mut module, route, served) = start(&mut f, [answer]).await;
                    let before = image(&f);
                    let body = error(&create(&mut module, route).await);
                    assert_eq!(
                        (body["code"].as_str(), body["message"].as_str()),
                        (Some(code), Some(message)),
                        "{label}"
                    );
                    one_confirm(&module);
                    finish(&mut module, &served).await;
                    assert!(
                        cancels(&module).is_empty(),
                        "{label}: a refused prompt is not withdrawn"
                    );
                    assert_eq!(image(&f), before, "{label}");
                    assert!(rows(&f).is_empty(), "{label}");
                }
            }

            /// The harness cannot close a route, so this drives the other
            /// withdrawal: the stand-in never answers and entorhinal's own
            /// receipt-relative bound expires before the SDK's 290-second one.
            #[tokio::test(start_paused = true)]
            async fn unanswered_prompt_is_withdrawn_at_the_operator_bound() {
                let mut f = Fixture::new("served-unanswered", Arc::new(FailConnector::default()));
                let (mut module, route, served) = start(&mut f, [OperatorAnswer::NoAnswer]).await;
                let before = image(&f);
                let sent = Instant::now();
                let body = json!({"method":"agent.create","params":new_request("served")});
                let corr = module
                    .send_request(route, serde_json::to_vec(&body).unwrap())
                    .await;
                let frame = module.next_frame().await.unwrap();
                assert_eq!(
                    (frame.header.channel, frame.header.ty),
                    (0, FrameType::Request)
                );
                let confirm = one_confirm(&module);
                tokio::time::advance(OPERATOR_BOUND + TIMER_TICK).await;
                let body = error(&module.read_reply(route, corr).await);
                assert_eq!(body["code"], "operator_declined");
                // The SDK's own 290-second deadline would have answered
                // operator_presence_unavailable; this proves entorhinal's bound won.
                let waited = Instant::now() - sent;
                assert!(
                    waited >= OPERATOR_BOUND && waited < Duration::from_secs(290),
                    "{waited:?}"
                );
                finish(&mut module, &served).await;
                assert_eq!(cancels(&module), [(0, 0, confirm, 0)]);
                assert_eq!(image(&f), before);
                assert!(rows(&f).is_empty());
            }
        }
    }
}

#[derive(Clone)]
enum Write {
    PlacementBackfill(entorhinal_core::placement::BackfillRequest),
    ManifestAvatar(Arc<agent_ops::AvatarBatchApproval>, usize),
    CatalogInstall(String, Box<entorhinal_core::agent::consent::ConsentCommit>),
    Consent(
        entorhinal_core::agent::consent::ConsentWrite,
        Box<entorhinal_core::agent::consent::ConsentCommit>,
    ),
    Register(RegisterRequest),
    Domain(entorhinal_core::SetProjectDomainRequest),
    Document(entorhinal_core::SetWorkspaceDocumentRequest),
    Assign(AssignWorkspaceRequest),
    Upgrade(UpgradeImplicitRequest),
    Remove(RemoveRequest),
    Seed(SeedImportRequest),
    Add(entorhinal_core::AddRootRequest),
    RefreshKey(entorhinal_core::RefreshRootKeyRequest),
    Attach(entorhinal_core::AttachRootRequest),
    MoveRoot(entorhinal_core::MoveRootToNewProjectRequest),
    Agent(String, Value),
    Member(entorhinal_core::shared_entry::MemberName),
    Pins(entorhinal_core::catalog_agent::PinWriteRequest),
    User(entorhinal_core::user_profile::UserSetRequest),
}

impl Write {
    fn decode(method: &str, params: Value) -> Result<Self, HandlerError> {
        Ok(match method {
            "placement.backfill" => {
                Self::PlacementBackfill(serde_json::from_value(params).map_err(invalid_params)?)
            }
            super::user_profile::SET => Self::User(super::user_profile::decode_set(params)?),
            "project.set_domain" => {
                Self::Domain(serde_json::from_value(params).map_err(invalid_params)?)
            }
            "workspace.set_document" => {
                Self::Document(serde_json::from_value(params).map_err(invalid_params)?)
            }
            "register" => Self::Register(serde_json::from_value(params).map_err(invalid_params)?),
            "assign_workspace" => {
                Self::Assign(serde_json::from_value(params).map_err(invalid_params)?)
            }
            "upgrade_implicit" => {
                Self::Upgrade(serde_json::from_value(params).map_err(invalid_params)?)
            }
            "remove" => Self::Remove(serde_json::from_value(params).map_err(invalid_params)?),
            "seed_import" => Self::Seed(serde_json::from_value(params).map_err(invalid_params)?),
            "add_root" => Self::Add(serde_json::from_value(params).map_err(invalid_params)?),
            "refresh_root_key" => {
                Self::RefreshKey(serde_json::from_value(params).map_err(invalid_params)?)
            }
            "attach_root" => Self::Attach(serde_json::from_value(params).map_err(invalid_params)?),
            "move_root_to_new_project" => {
                Self::MoveRoot(serde_json::from_value(params).map_err(invalid_params)?)
            }
            super::member::SET_NAME => {
                Self::Member(serde_json::from_value(params).map_err(invalid_params)?)
            }
            op => Self::Agent(op.into(), params),
        })
    }
    fn probe(
        &self,
        store: &RegistryStore,
        op: &str,
        principal: &str,
    ) -> Result<Option<Vec<u8>>, HandlerError> {
        let key = match self {
            Self::PlacementBackfill(r) => r.request_key.as_deref(),
            Self::User(req) => return store.probe_user_set(req).map_err(Into::into),
            Self::ManifestAvatar(approval, index) => return approval.probe(store, *index),
            Self::Consent(_, commit) | Self::CatalogInstall(_, commit) => {
                let row = store.consent_request(&commit.request.identity)?;
                if let Some(row) = row {
                    if row.args_digest != commit.request.args_digest {
                        return Err(HandlerError::new(
                            "request_key_conflict",
                            "request key has different arguments",
                        ));
                    }
                    if row.state != entorhinal_core::agent::consent::RequestState::Awaiting {
                        return serde_json::to_vec(&json!({"result":row.outcome}))
                            .map(Some)
                            .map_err(invalid_params);
                    }
                }
                return Ok(None);
            }
            Self::Domain(r) => Some(r.request_key.as_str()),
            Self::Document(r) => Some(r.request_key.as_str()),
            Self::Register(r) => r.request_key.as_deref(),
            Self::Assign(r) => r.request_key.as_deref(),
            Self::Upgrade(r) => r.request_key.as_deref(),
            Self::Remove(r) => r.request_key.as_deref(),
            Self::Seed(r) => r.request_key.as_deref(),
            Self::Add(_) => None,
            Self::RefreshKey(_) => None,
            Self::MoveRoot(_) => None,
            Self::Attach(r) => r.request_key.as_deref(),
            Self::Member(_) => None,
            Self::Pins(r) => r.request_key.as_deref(),
            Self::Agent(op, params) => {
                return store
                    .with_principal(principal)
                    .probe_agent_mutation(op, params.clone())
                    .map_err(Into::into)
            }
        };
        store.shared_request_cache(op, key).map_err(storage_error)
    }
    fn run(
        self,
        store: &RegistryStore,
        principal: &str,
        now: i64,
        agent_id: &str,
        avatar: Option<entorhinal_core::agent::StoredAgentAvatar>,
        approval: Option<&OperatorApproval>,
    ) -> Result<Vec<u8>, HandlerError> {
        let writer = store
            .with_principal(principal)
            .with_create_avatar(avatar)
            .with_confirmation(placement_confirmation(principal, approval));
        match self {
            Self::PlacementBackfill(r) => writer.placement_backfill(r, now),
            Self::User(req) => writer.user_set(req, now, approval),
            Self::ManifestAvatar(approval, index) => return approval.apply(store, index, now),
            Self::CatalogInstall(id, commit) => {
                return writer
                    .with_consent_commit(*commit)
                    .catalog_install(&id, now)
                    .map_err(storage_error)
            }
            Self::Consent(plan, commit) => {
                return writer
                    .with_consent_commit(*commit)
                    .apply_consent_write(&plan, now, (!agent_id.is_empty()).then_some(agent_id))
                    .map_err(Into::into)
            }
            Self::Domain(r) => writer.set_project_domain_at(r, now),
            Self::Document(r) => writer.set_workspace_document_at(r, now),
            Self::Register(r) => writer.register_at(r, now),
            Self::Pins(r) => writer.catalog_set_pins(r, now),
            Self::Assign(r) => writer.assign_workspace_at(r, now),
            Self::Upgrade(r) => writer.upgrade_implicit_at(r, now),
            Self::Remove(r) => writer.remove_at(r, now),
            Self::Seed(r) => writer.seed_import_at(r, now),
            Self::Add(r) => writer.add_root_at(r, now),
            Self::RefreshKey(r) => writer.refresh_root_key_at(r, now),
            Self::Attach(r) => writer.attach_root_at(r, now),
            Self::MoveRoot(r) => writer.move_root_to_new_project_at(r, now),
            Self::Member(name) => {
                return store.set_member_name(name, principal).map_err(Into::into)
            }
            Self::Agent(op, params) => {
                return writer
                    .agent_mutation_with_approval(
                        &op,
                        params,
                        now,
                        (!agent_id.is_empty()).then_some(agent_id),
                        approval,
                    )
                    .map_err(Into::into)
            }
        }
        .map_err(storage_error)
    }
}

fn storage_error(error: entorhinal_core::RegistryError) -> HandlerError {
    match error {
        RegistryError::Domain {
            code,
            message,
            detail,
        } => HandlerError {
            code,
            message,
            detail,
        },
        error => HandlerError::new("storage_error", error.to_string()),
    }
}

pub(super) fn placement_confirmation(
    principal: &str,
    approval: Option<&OperatorApproval>,
) -> entorhinal_core::agent::PlacementConfirmation {
    use entorhinal_core::agent::PlacementConfirmation;
    if principal != "direct" {
        PlacementConfirmation::NotRequired
    } else if let Some(approval) = approval {
        PlacementConfirmation::Approved(approval.clone())
    } else {
        PlacementConfirmation::NoPrompt
    }
}

pub(super) fn draw_agent_id() -> Result<String, HandlerError> {
    let mut random = [0u8; 8];
    getrandom::getrandom(&mut random)
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
    Ok(format!("agent_{}", log_client::encode_hex(&random)))
}

pub(super) fn draw_agent_avatar() -> Result<entorhinal_core::agent::StoredAgentAvatar, HandlerError>
{
    let mut random = [0u8; 16_384];
    getrandom::getrandom(&mut random)
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
    Ok(entorhinal_core::agent::StoredAgentAvatar {
        genome: log_client::encode_hex(&random),
        avatar_type: "creature.wild".into(),
        version: Some(1),
    })
}

#[cfg(test)]
pub(super) fn assert_wild_avatar(avatar: &Value) {
    assert_eq!(avatar["type"], "creature.wild");
    assert_eq!(avatar["version"], 1);
    let genome = avatar["genome"]
        .as_str()
        .expect("a create must carry an avatar genome");
    assert_eq!(genome.len(), 32_768);
    assert!(genome
        .bytes()
        .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)));
}

impl From<RegistryError> for HandlerError {
    fn from(error: RegistryError) -> Self {
        storage_error(error)
    }
}

fn shared_candidate(op: &str) -> bool {
    matches!(
        op,
        "project.set_domain"
            | "placement.backfill"
            | "user.set"
            | "workspace.set_document"
            | "register"
            | "assign_workspace"
            | "upgrade_implicit"
            | "remove"
            | "seed_import"
            | "add_root"
            | "refresh_root_key"
            | "attach_root"
            | "move_root_to_new_project"
            | "member.set_name"
    ) || (agent_ops::MUTATING_METHODS.contains(&op) && op != "agent.import")
}

fn bootstrap_fence(
    store: &RegistryStore,
    state: &str,
    request: &WireRequest,
) -> Result<(), HandlerError> {
    if !matches!(state, "enabling" | "joining") {
        return Ok(());
    }
    if shared_candidate(&request.method)
        || matches!(
            request.method.as_str(),
            "agent.import" | "set_workspace_root"
        )
    {
        return Err(HandlerError::new(
            "identity_log_enabling",
            "identity log bootstrap is in progress",
        ));
    }
    if request.method == "remove_root" {
        let root: entorhinal_core::RemoveRootRequest =
            serde_json::from_value(request.params.clone()).map_err(invalid_params)?;
        if store.root_count(&root.project_id).map_err(storage_error)? <= 1 {
            return Err(HandlerError::new(
                "identity_log_enabling",
                "last-root removal is fenced during bootstrap",
            ));
        }
    }
    Ok(())
}

async fn hub_or_local_sql<R: Send + 'static>(
    store: Option<&RegistryStore>,
    hub: &crate::hub_write::HubSql,
    deadline: Instant,
    operation: &'static str,
    work: impl FnOnce(&RegistryStore) -> Result<R, HandlerError> + Send + 'static,
) -> Result<R, HandlerError> {
    if let Some(store) = store {
        work(store)
    } else {
        hub.run(deadline, operation, work).await
    }
}

impl ProjectsHandler {
    pub(super) async fn avatar_manifest_entry_wait(
        &self,
        approval: Arc<agent_ops::AvatarBatchApproval>,
        index: usize,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.write_wait_inner(
            WireRequest {
                method: "agent.set_avatar".into(),
                params: Value::Null,
            },
            key,
            Instant::now(),
            Some(Write::ManifestAvatar(approval, index)),
            None,
        )
        .await
    }

    pub(super) async fn catalog_install_write_wait(
        &self,
        id: String,
        commit: entorhinal_core::agent::consent::ConsentCommit,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.write_wait_inner(
            WireRequest {
                method: "catalog.pin".into(),
                params: Value::Null,
            },
            (0, 0),
            Instant::now(),
            Some(Write::CatalogInstall(id, Box::new(commit))),
            Some(deadline),
        )
        .await
    }
    // Append approved pin changes to the shared identity log and commit their
    // projection together. This is not a public RPC: catalog tools must first
    // check caller identity, ownership and approval before invoking it.
    #[allow(dead_code)]
    pub(super) async fn catalog_pins_write_wait(
        &self,
        pins: entorhinal_core::catalog_agent::PinWriteRequest,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.write_wait_inner(
            WireRequest {
                method: "catalog.pin".into(),
                params: serde_json::to_value(&pins).map_err(invalid_params)?,
            },
            key,
            received,
            Some(Write::Pins(pins)),
            None,
        )
        .await
    }
    pub(super) async fn consent_write_wait(
        &self,
        plan: entorhinal_core::agent::consent::ConsentWrite,
        commit: entorhinal_core::agent::consent::ConsentCommit,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.write_wait_inner(
            WireRequest {
                method: "agent.consent_write".into(),
                params: Value::Null,
            },
            (0, 0),
            Instant::now(),
            Some(Write::Consent(plan, Box::new(commit))),
            Some(deadline),
        )
        .await
    }

    pub(super) async fn write_wait(
        &self,
        request: WireRequest,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.write_wait_inner(request, key, received, None, None)
            .await
    }

    async fn write_wait_inner(
        &self,
        mut request: WireRequest,
        key: RouteKey,
        received: Instant,
        consent: Option<Write>,
        consent_deadline: Option<Instant>,
    ) -> Result<Vec<u8>, HandlerError> {
        let agent_write = consent.is_some();
        self.admit_store_method(&request.method)?;
        // Only the served async path can confirm Direct. admit/execute remain
        // unconfirmed, including synchronous tests and non-confirmable methods.
        let admission = self.admission_for(key);
        let direct = !agent_write
            && matches!(admission.principal, Some(Principal::Direct))
            && (agent_ops::CONFIRMABLE_METHODS.contains(&request.method.as_str())
                || request.method == super::user_profile::SET
                || request.method == "placement.backfill");
        if request.method == "placement.backfill" {
            authorize_write(&request.method, admission.principal.as_ref())?;
        }
        if request.method == super::user_profile::SET {
            super::user_profile::admit(admission.principal.as_ref(), admission.flow_id.as_deref())?;
        }
        refuse_flow_write(&request.method, admission.flow_id.as_deref())?;
        if request.method == super::member::SET_NAME {
            super::member::admit(admission.principal.as_ref())?;
        }
        if !direct && !agent_write {
            authorize_write(&request.method, admission.principal.as_ref())?;
        }
        let principal = if agent_write {
            "agent".into()
        } else {
            principal_label(admission.principal.as_ref())
        };
        // Session registration is machine-local, but uses the same SQLite
        // writer connection as shared writes. Wait for its synchronous mutex
        // off the runtime. Do not join the shared-log writer queue: background
        // network reads hold that queue even without an open SQL transaction.
        // Resolve which modules may register sessions only after rejecting
        // writes on read-only flow routes, so refused callers do not query the
        // daemon. Pass those answers to admission; synchronous callers cannot
        // reuse the serving path's capability cache.
        if request.method == super::sessions::REGISTER_SESSION {
            let providers = self
                .route_session_providers(&request.method, &admission)
                .await;
            let (_, role) = self.admit_with(&request.method, key, Some(&providers))?;
            let role = role.ok_or_else(|| sessions::orchestrator_unbound(Some(&providers)))?;
            let store = self.with_store(|s| Ok(s.clone()))?;
            let now = (self.clock)();
            let clock = self.clock;
            let residence = self.residence();
            let hub_selected = self.hub_backend_selected();
            let worker = tokio::task::spawn_blocking(move || {
                // On a single-worker test runtime this panics before SQLite if
                // registration is ever moved from this blocking thread back onto
                // an async task: test builds call `block_in_place`, which panics
                // when called from an async task on that runtime but not from a
                // blocking thread.
                #[cfg(test)]
                tokio::task::block_in_place(|| {});
                crate::hub_residence::with_claim_context(
                    residence.as_deref(),
                    hub_selected,
                    |context| {
                        sessions::register_session_on_store(
                            &store,
                            request.params,
                            role,
                            now,
                            clock,
                            context,
                        )
                    },
                )
            });
            return timeout_at(received + DEADLINE, worker)
                .await
                .map_err(|_| {
                    HandlerError::new(
                        "identity_log_contended",
                        "session registration deadline expired",
                    )
                })?
                .map_err(|error| HandlerError::new("storage_error", error.to_string()))?;
        }
        // Ending a session is machine-local like registration and takes the
        // same path: admission against the orchestrator's provider, then the
        // SQL on the blocking pool through the store's fenced writer.
        if request.method == super::sessions::END_SESSION {
            let providers = self
                .route_session_providers(&request.method, &admission)
                .await;
            self.admit_with(&request.method, key, Some(&providers))?;
            let store = self.with_store(|s| Ok(s.clone()))?;
            let now = (self.clock)();
            let worker = tokio::task::spawn_blocking(move || {
                sessions::end_session_on_store(&store, request.params, now)
            });
            let deadline = received + DEADLINE;
            return timeout_at(deadline, worker)
                .await
                .map_err(|_| {
                    HandlerError::new("identity_log_contended", "session end deadline expired")
                })?
                .map_err(|error| HandlerError::new("storage_error", error.to_string()))?;
        }
        // Recording a session's network access is machine-local and admitted
        // like ending one. The handler's clock goes to the blocking thread
        // and is read inside the store's fenced transaction, so the entry is
        // stamped when it commits rather than when the request arrived.
        if request.method == super::sessions::SET_SESSION_NETWORK {
            let providers = self
                .route_session_providers(&request.method, &admission)
                .await;
            self.admit_with(&request.method, key, Some(&providers))?;
            let store = self.with_store(|s| Ok(s.clone()))?;
            let clock = self.clock;
            let worker = tokio::task::spawn_blocking(move || {
                sessions::set_session_network_on_store(&store, request.params, clock)
            });
            let deadline = received + DEADLINE;
            return timeout_at(deadline, worker)
                .await
                .map_err(|_| {
                    HandlerError::new("identity_log_contended", "session network deadline expired")
                })?
                .map_err(|error| HandlerError::new("storage_error", error.to_string()))?;
        }
        let candidate = agent_write || shared_candidate(&request.method);
        let hub_sql = self.served_hub_sql();
        let early_deadline = received + DEADLINE;
        let read_backend = |store: &RegistryStore| Ok(store.hub_log_base()?.is_some());
        let hub = if !candidate {
            false
        } else if self.hub_switch.is_none() && !self.hub_backend_selected() {
            let store = self.with_store(|s| Ok(s.clone()))?;
            read_backend(&store)?
        } else {
            hub_sql
                .run(early_deadline, "shared_backend", read_backend)
                .await?
        };
        let store = if hub {
            None
        } else {
            Some(self.with_store(|s| Ok(s.clone()))?)
        };
        if hub && !self.residence().is_some_and(|residence| residence.ready()) {
            return Err(HandlerError::new(
                "hub_not_ready",
                "hub snapshot and residence have not caught up",
            ));
        }
        if request.method == super::member::SET_NAME {
            let params = request.params.clone();
            let name = hub_or_local_sql(
                store.as_ref(),
                &hub_sql,
                early_deadline,
                "prepare_member_name",
                move |store| super::member::prepare(store, params),
            )
            .await?;
            request.params = serde_json::to_value(name).expect("member name encodes");
        }
        let state = hub_or_local_sql(
            store.as_ref(),
            &hub_sql,
            early_deadline,
            "identity_log_status",
            |store| Ok(store.identity_log_status().map_err(storage_error)?.state),
        )
        .await?;
        let prepared = if agent_write {
            consent
        } else {
            candidate
                .then(|| Write::decode(&request.method, request.params.clone()))
                .transpose()?
        };
        if agent_write && matches!(state.as_str(), "enabling" | "joining") {
            return Err(HandlerError::new(
                "identity_log_enabling",
                "identity log bootstrap is in progress",
            ));
        }
        if matches!(state.as_str(), "enabling" | "joining") {
            let req = WireRequest {
                method: request.method.clone(),
                params: request.params.clone(),
            };
            let state = state.clone();
            hub_or_local_sql(
                store.as_ref(),
                &hub_sql,
                early_deadline,
                "bootstrap_fence",
                move |store| bootstrap_fence(store, &state, &req),
            )
            .await?;
        }
        if hub && request.method != "agent.create" {
            if let Some(write) = &prepared {
                let (key, digest) = hub_request(&request, write)?;
                if let Some(key) = key {
                    if let Some(reply) = hub_sql
                        .run(early_deadline, "hub_local_reply", move |store| {
                            Ok(store.hub_local_reply(&key, &digest)?)
                        })
                        .await?
                    {
                        let reply = hub_reply(reply)?;
                        return if request.method.starts_with("agent.") {
                            self.attach_incarnation(reply)
                        } else {
                            Ok(reply)
                        };
                    }
                }
            }
        }
        let hub_create = if request.method == "agent.create" && hub {
            let residence = self.residence().ok_or_else(|| {
                HandlerError::new("hub_not_ready", "hub residence watch is not installed")
            })?;
            let write = prepared.clone().expect("create is a shared write");
            let caller = principal.clone();
            hub_sql
                .run(early_deadline, "probe_agent_create", move |store| {
                    write.probe(store, "agent.create", &caller)
                })
                .await?;
            let key = request.params["request_key"]
                .as_str()
                .filter(|key| !key.is_empty())
                .ok_or_else(|| {
                    HandlerError::new("request_key_required", "request_key is required")
                })?;
            let digest = crate::hub_write::params_digest(&request.method, &request.params)?;
            if hub_sql.attempt(key, early_deadline).await?.is_some() {
                return self.attach_incarnation(
                    Box::pin(residence.create(key, &digest, None, received + DEADLINE)).await?,
                );
            }
            Some((residence, key.to_owned(), digest))
        } else {
            None
        };
        let mut confirmation = None;
        let mut approval = None;
        let mut budget_start = received;
        if direct {
            let check = prepared.clone();
            let method = request.method.clone();
            let params = request.params.clone();
            let clock = self.clock;
            let precheck = hub_or_local_sql(
                store.as_ref(),
                &hub_sql,
                early_deadline,
                "precheck_operator_confirmation",
                move |store| {
                    Ok(if let Some(Write::PlacementBackfill(req)) = &check {
                        if let Some(body) = store.shared_request_cache(
                            "placement.backfill",
                            req.request_key.as_deref(),
                        )? {
                            AgentPrecheck::Cached(body)
                        } else {
                            AgentPrecheck::Ready(store.placement_backfill_precheck()?)
                        }
                    } else if let Some(Write::User(req)) = &check {
                        store.precheck_user_confirmation(req)?
                    } else {
                        store.precheck_agent_confirmation(&method, params, clock())?
                    })
                },
            )
            .await?;
            let summary = match precheck {
                AgentPrecheck::Cached(body) => {
                    return if request.method.starts_with("agent.") {
                        self.attach_incarnation(body)
                    } else {
                        Ok(body)
                    }
                }
                AgentPrecheck::Ready(summary) => Some(summary),
                // A confirmable method whose precheck found nothing needing approval.
                AgentPrecheck::NoPrompt => None,
            };
            if let Some(summary) = summary {
                #[cfg(test)]
                if let Some(barrier) = &self.confirmation_admission_barrier {
                    barrier.wait().await;
                }
                let mut guard = agent_ops::ConfirmationGuard::take(&self.confirmation_slot, key)?;
                // Removal precedes signalling in on_route_gone. A closure between
                // admission and slot installation is therefore never lost.
                if !self.route_admissions().contains_key(&key) {
                    return Err(agent_ops::OperatorConfirmError::Declined.into());
                }
                // The SDK waits up to 290 seconds. Our receipt-relative 270-second
                // bound expires first and drops its future, withdrawing the prompt.
                tokio::select! {
                    biased;
                    _ = guard.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
                    result = timeout_at(received + OPERATOR_BOUND, self.operator_confirmer.confirm_operator(&summary, key)) => {
                        result.map_err(|_| HandlerError::from(agent_ops::OperatorConfirmError::Declined))??;
                    }
                }
                if Instant::now() >= received + OPERATOR_BOUND {
                    return Err(agent_ops::OperatorConfirmError::Declined.into());
                }
                budget_start = Instant::now();
                approval = Some(OperatorApproval::new(summary));
                confirmation = Some(guard);
            }
        } else if state == "enabled" {
            if let Some(write) = &prepared {
                let write = write.clone();
                let method = request.method.clone();
                let caller = principal.clone();
                if let Some(body) = hub_or_local_sql(
                    store.as_ref(),
                    &hub_sql,
                    early_deadline,
                    "probe_shared_write",
                    move |store| write.probe(store, &method, &caller),
                )
                .await?
                {
                    return if request.method.starts_with("agent.") {
                        self.attach_incarnation(body)
                    } else {
                        Ok(body)
                    };
                }
            }
        }
        let deadline = if let Some(deadline) = consent_deadline {
            deadline
        } else if direct {
            (budget_start + DEADLINE).min(received + OPERATOR_BOUND)
        } else {
            received + DEADLINE
        };
        if let Some((residence, request_key, digest)) = hub_create {
            let agent_id = (self.agent_id_draw)()?;
            let avatar = if request.params.get("avatar").is_none_or(Value::is_null) {
                Some((self.agent_avatar_draw)()?)
            } else {
                None
            };
            let now = (self.clock)();
            let write = prepared.expect("create is a shared write");
            let preview = move |store: &RegistryStore, saved: Option<&Value>| {
                let id = saved
                    .and_then(|body| body["agent_id"].as_str())
                    .unwrap_or(&agent_id);
                let time = saved
                    .and_then(|body| body["row"]["updated_at_ms"].as_i64())
                    .unwrap_or(now);
                preview_hub_create(store, || {
                    write.clone().run(
                        store,
                        &principal,
                        time,
                        id,
                        avatar.clone(),
                        approval.as_ref(),
                    )
                })
            };
            let create = Box::pin(residence.create(
                &request_key,
                &digest,
                Some(Arc::new(preview)),
                deadline,
            ));
            let reply = if let Some(guard) = &mut confirmation {
                tokio::select! {
                    biased;
                    _ = guard.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
                    result = create => result,
                }
            } else {
                create.await
            }?;
            return self.record_mutation(self.attach_incarnation(reply));
        }
        let lock = timeout_at(deadline, self.writer.clone().lock_owned());
        let lock_result = if let Some(guard) = &mut confirmation {
            tokio::select! {
                biased;
                _ = guard.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
                result = lock => result,
            }
        } else {
            lock.await
        };
        let writer = lock_result.map_err(|_| {
            HandlerError::new("identity_log_contended", "writer lock deadline expired")
        })?;
        // Re-read after the lock: enable/join can change state while we queue.
        let (state, hub) = hub_or_local_sql(
            store.as_ref(),
            &hub_sql,
            deadline,
            "shared_backend",
            move |store| {
                Ok((
                    store.identity_log_status().map_err(storage_error)?.state,
                    candidate && store.hub_log_base()?.is_some(),
                ))
            },
        )
        .await?;
        if hub {
            drop(store);
            drop(writer);
            if !self.residence().is_some_and(|residence| residence.ready()) {
                return Err(HandlerError::new(
                    "hub_not_ready",
                    "hub snapshot and residence have not caught up",
                ));
            }
            let write = prepared.expect("shared write was decoded");
            // Keep the hub-only future out of the local write state machine.
            let result =
                Box::pin(self.hub_shared_write(&request, write, &principal, approval, deadline));
            let body = if let Some(guard) = &mut confirmation {
                tokio::select! {
                    biased;
                    _ = guard.cancelled.wait_for(|closed| *closed) => return Err(agent_ops::OperatorConfirmError::Declined.into()),
                    result = result => result,
                }
            } else {
                result.await
            }?;
            return self.record_mutation(if request.method.starts_with("agent.") {
                self.attach_incarnation(body)
            } else {
                Ok(body)
            });
        }
        let store = store.ok_or_else(|| {
            self.health
                .store_refusal
                .get()
                .unwrap_or_else(crate::hub_write::store_unavailable)
        })?;
        if agent_write && matches!(state.as_str(), "enabling" | "joining") {
            return Err(HandlerError::new(
                "identity_log_enabling",
                "identity log bootstrap is in progress",
            ));
        }
        bootstrap_fence(&store, &state, &request)?;
        let creating = request.method == "agent.create"
            || matches!(&prepared, Some(Write::Consent(plan,_)) if matches!(plan.tool.as_str(), "agents_hire" | "agents_create_head"));
        if creating {
            if let Some(body) =
                prepared
                    .as_ref()
                    .unwrap()
                    .probe(&store, &request.method, &principal)?
            {
                return if request.method.starts_with("agent.") {
                    self.attach_incarnation(body)
                } else {
                    Ok(body)
                };
            }
        }
        let agent_id = if creating {
            (self.agent_id_draw)()?
        } else {
            String::new()
        };
        let avatar = if creating && request.params.get("avatar").is_none_or(Value::is_null) {
            Some((self.agent_avatar_draw)()?)
        } else {
            None
        };
        if agent_write {
            let body = prepared.unwrap().run(
                &store,
                &principal,
                (self.clock)(),
                &agent_id,
                avatar,
                None,
            )?;
            self.commits.notify_waiters();
            return self.record_mutation(Ok(body));
        }
        if approval.is_some() || creating {
            let before = store.generation().map_err(storage_error)?;
            let body = prepared.unwrap().run(
                &store,
                &principal,
                (self.clock)(),
                &agent_id,
                avatar,
                approval.as_ref(),
            )?;
            // Confirmed local commits wake the identity feed just like
            // execute does; a cached reply must not wake idle readers.
            let committed = store.generation().map_err(storage_error)? != before;
            if committed {
                self.commits.notify_waiters();
            }
            if request.method == "register" && store.root_records_enabled() {
                if let Err(error) = store
                    .with_principal(&principal)
                    .bind_unbound_roots("entorhinal")
                {
                    tracing::warn!(target:"binding", "binding after confirmed register failed: {error}");
                }
            }
            return self.record_mutation(if request.method.starts_with("agent.") {
                self.attach_incarnation(body)
            } else {
                Ok(body)
            });
        }
        self.execute(request, key)
    }

    async fn hub_shared_write(
        &self,
        request: &WireRequest,
        write: Write,
        principal: &str,
        approval: Option<OperatorApproval>,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let log = self.hub_shared_log.lock().unwrap().clone().ok_or_else(|| {
            HandlerError::new("hub_not_ready", "hub identity writer is not installed")
        })?;
        let (key, digest) = hub_request(request, &write)?;
        let key = key.map_or_else(crate::hub_write::new_op_id, Ok)?;
        let sql = self.served_hub_sql();
        let lookup = key.clone();
        let proof = digest.clone();
        let (reply, saved_time) = sql
            .run(deadline, "hub_local_reply", move |store| {
                let reply = store.hub_local_reply(&lookup, &proof)?;
                let time = if reply.is_none() {
                    store.hub_local_time(&lookup)?
                } else {
                    None
                };
                Ok((reply, time))
            })
            .await?;
        if let Some(reply) = reply {
            return hub_reply(reply);
        }
        let now = saved_time.unwrap_or_else(self.clock);
        let draws_agent = matches!(&write, Write::Consent(plan, _) if matches!(plan.tool.as_str(), "agents_hire" | "agents_create_head"));
        let id = if draws_agent {
            (self.agent_id_draw)()?
        } else {
            String::new()
        };
        let avatar = if draws_agent {
            Some((self.agent_avatar_draw)()?)
        } else {
            None
        };
        let principal = principal.to_owned();
        let preview = Arc::new(
            move |store: &RegistryStore, saved: Option<&Value>, entry_id: &str| {
                let consent = match &write {
                    Write::Consent(_, consent) | Write::CatalogInstall(_, consent) => {
                        Some(consent.as_ref())
                    }
                    _ => None,
                };
                let id = saved
                    .and_then(|body| body["agent_id"].as_str())
                    .unwrap_or(&id);
                let avatar = saved
                    .and_then(|body| serde_json::from_value(body["row"]["avatar"].clone()).ok())
                    .or_else(|| avatar.clone());
                let label = if let Write::Register(req) = &write {
                    req.label.as_deref()
                } else {
                    None
                };
                preview_local(store, entry_id, consent, label, now, || {
                    write
                        .clone()
                        .run(store, &principal, now, id, avatar, approval.as_ref())
                })
            },
        );
        let attempt = sql.attempt(&key, deadline).await?;
        let (expected, body, continuation) = if let Some(attempt) = attempt {
            let record = crate::fleet_record::UnverifiedRecord::parse(
                &attempt.write.key,
                &attempt.write.value,
            )?;
            (
                attempt.write.expected,
                json!({"op":record.header().kind}),
                None,
            )
        } else {
            log.catch_up(deadline).await?;
            let expected = log.applied(deadline).await?;
            let preview = preview.clone();
            match sql
                .run(deadline, "preview_hub_write", move |store| {
                    preview(store, None, &crate::hub_write::new_op_id()?)
                })
                .await?
            {
                HubPreview::Local(reply) => return Ok(reply),
                HubPreview::Shared(body, local) => (expected, body, Some(local)),
            }
        };
        let replan = move |store: &RegistryStore, saved: &Value, entry_id: &str| match preview(
            store,
            Some(saved),
            entry_id,
        ) {
            Ok(HubPreview::Shared(body, local)) => {
                Ok(crate::hub_write::Replanned::WithLocal(body, local))
            }
            Ok(HubPreview::Local(_)) => Ok(crate::hub_write::Replanned::Refused(
                HandlerError::new("head_moved", "operation no longer needs a shared entry"),
            )),
            Err(error) => Ok(crate::hub_write::Replanned::Refused(error)),
        };
        hub_reply(
            log.append_local(
                &key,
                &digest,
                expected,
                body,
                continuation.as_deref(),
                Arc::new(replan),
                deadline,
            )
            .await?,
        )
    }
}

fn hub_request(
    request: &WireRequest,
    write: &Write,
) -> Result<(Option<String>, String), HandlerError> {
    let consent = match write {
        Write::Consent(_, commit) | Write::CatalogInstall(_, commit) => Some(commit.as_ref()),
        _ => None,
    };
    let key = if let Some(commit) = consent {
        let identity = &commit.request.identity;
        Some(format!(
            "consent:{}:{}:{}",
            identity.agent_id, identity.tool, identity.request_key
        ))
    } else {
        request.params["request_key"]
            .as_str()
            .or_else(|| request.params["requestKey"].as_str())
            .map(str::to_owned)
    };
    let digest = crate::hub_write::params_digest(
        &request.method,
        &json!([request.params, consent.map(|c| &c.request.args_digest)]),
    )?;
    Ok((key, digest))
}

enum HubPreview {
    Local(Vec<u8>),
    Shared(Value, Box<entorhinal_core::HubContinuation>),
}

fn preview_local(
    store: &RegistryStore,
    entry_id: &str,
    consent: Option<&entorhinal_core::agent::consent::ConsentCommit>,
    label: Option<&str>,
    now: i64,
    apply: impl FnOnce() -> Result<Vec<u8>, HandlerError>,
) -> Result<HubPreview, HandlerError> {
    let captured = Arc::new(std::sync::Mutex::new(None));
    let slot = captured.clone();
    let result = store.preview_hub_write(
        entry_id.to_owned(),
        move |body, reply| {
            *slot.lock().unwrap() = Some((body, reply));
        },
        apply,
    );
    let preview = captured.lock().unwrap().take();
    match (preview, result) {
        (Some((bytes, reply)), Err(_)) => {
            let body = serde_json::from_slice(&bytes).map_err(invalid_params)?;
            let result = &reply["result"];
            let roots = result["projectId"].as_str().and_then(|id| {
                let paths = |field: &str| {
                    result[field]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .map(str::to_owned)
                        .collect::<Vec<_>>()
                };
                let mut roots = paths("roots");
                if let Some(root) = result["root"].as_str() {
                    roots.push(root.to_owned());
                }
                let parents = paths("derivedRootParents");
                (!roots.is_empty() || !parents.is_empty()).then(|| entorhinal_core::HubRoots {
                    project: id.into(),
                    roots,
                    parents,
                })
            });
            Ok(HubPreview::Shared(
                body,
                Box::new(entorhinal_core::HubContinuation::prepare(
                    store,
                    entry_id,
                    now,
                    reply,
                    roots,
                    label,
                    consent.cloned().map(Into::into),
                )?),
            ))
        }
        (None, Err(error)) => Err(error),
        (None, Ok(reply)) => Ok(HubPreview::Local(reply)),
        (Some(_), Ok(_)) => Err(HandlerError::new(
            "storage_error",
            "hub preview unexpectedly committed",
        )),
    }
}

fn hub_reply(bytes: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
    let value: Value = serde_json::from_slice(&bytes).map_err(invalid_params)?;
    if let Some(error) = value.get("error") {
        return Err(HandlerError::new(
            error["code"].as_str().unwrap_or("storage_error"),
            error["message"]
                .as_str()
                .unwrap_or("local continuation refused"),
        ));
    }
    Ok(bytes)
}

fn preview_hub_create(
    store: &RegistryStore,
    apply: impl FnOnce() -> Result<Vec<u8>, HandlerError>,
) -> Result<Value, HandlerError> {
    let captured = Arc::new(std::sync::Mutex::new(None));
    let slot = captured.clone();
    let status = store.identity_log_status()?;
    if status.state != "enabled" {
        return Err(HandlerError::new(
            "hub_not_ready",
            "hub identity log is not enabled",
        ));
    }
    let result = store.preview_hub_write(
        crate::hub_write::new_op_id()?,
        move |bytes, _| *slot.lock().unwrap() = Some(bytes),
        apply,
    );
    match result {
        Err(error) if error.code == "hub_write_preview" => {
            let bytes = captured.lock().unwrap().take().ok_or_else(|| {
                HandlerError::new("storage_error", "create preview has no after-image")
            })?;
            serde_json::from_slice(&bytes).map_err(invalid_params)
        }
        Err(error) => Err(error),
        Ok(_) => Err(HandlerError::new(
            "hub_not_ready",
            "agent create did not produce a shared after-image",
        )),
    }
}

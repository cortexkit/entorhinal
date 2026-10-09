//! Runs one shared write at a time while readers use already committed data.
//! A worker thread keeps the SQLite transaction open while the async task asks
//! the log to append the entry, resending it if the result is uncertain. Local
//! changes commit only after the log confirms the append. If confirmation does
//! not arrive before the time limit, or the log rejects the attempt as outdated
//! or invalid, the local changes roll back.

use super::{
    log_client::{self, AppendRequest, EntryKind, LogError},
    *,
};
use entorhinal_core::agent::{AgentPrecheck, OperatorApproval};
use entorhinal_core::{RegistryStore, SharedSettlement};
use serde_json::Value;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::{timeout_at, Instant};

const DEADLINE: Duration = Duration::from_secs(25);
const SETTLEMENT: Duration = Duration::from_secs(20);
const SEND_WAIT: Duration = Duration::from_secs(1);
const RESEND_DELAY: Duration = Duration::from_millis(100);
const RETRIES: usize = 5;
const OPERATOR_BOUND: Duration = Duration::from_secs(270);

pub(super) fn health_state(state: u64) -> &'static str {
    match state {
        1 => "engram_key_unavailable",
        2 => "not_member",
        3 => "identity_log_invariant",
        4 => "log_gap",
        5 => "log_verify_failed",
        6 => "apply_failed",
        7 => "log_head_regressed",
        _ => "ok",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_log::{Action, FailConnector, FakeLog};
    use crate::log_client::{LogConnector, LogTransport, TransportError};
    use serde_json::json;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    };
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
    static DRAWS: AtomicUsize = AtomicUsize::new(0);
    fn fixed_draw() -> Result<String, HandlerError> {
        DRAWS.fetch_add(1, Ordering::SeqCst);
        Ok("agent_0123456789abcdef".into())
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

    /// Keep the test's async task runnable until the specified log call occurs.
    /// When all async tasks are waiting, Tokio can advance its paused clock to
    /// the next timer even though SQLite is still running on a worker thread.
    /// Yielding in this loop keeps database preparation from using simulated
    /// time that the test reserves for waiting on the log.
    async fn reach<F: Future, B: Future<Output = ()>>(mut write: Pin<&mut F>, barrier: B) {
        let frozen = Instant::now();
        tokio::pin!(barrier);
        loop {
            tokio::select! {
                biased;
                () = &mut barrier => break,
                _ = &mut write => panic!("write finished before the expected log call"),
                () = tokio::task::yield_now() => {},
            }
        }
        assert_eq!(
            Instant::now(),
            frozen,
            "SQLite preparation advanced the paused clock"
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

    /// Advance past the request's deadline and poll the write so it can discard
    /// its temporary SQLite changes. If it is not ready, advance by one resend
    /// interval and poll again. Code that incorrectly gave the request more time
    /// would then send another append, which the timestamp check rejects. Finally
    /// let any database rollback finish while the simulated clock stays still.
    async fn expire<F: Future>(mut write: Pin<&mut F>, tap: &Tap, bound: Instant) -> F::Output {
        tokio::time::advance(bound.saturating_duration_since(Instant::now()) + TIMER_TICK).await;
        let mut result = match write.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            Poll::Ready(result) => Some(result),
            Poll::Pending => None,
        };
        if result.is_none() {
            tokio::time::advance(RESEND_DELAY + TIMER_TICK).await;
            result = match write.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
                Poll::Ready(result) => Some(result),
                Poll::Pending => None,
            };
        }
        assert!(
            tap.sent_at.lock().unwrap().iter().all(|time| *time < bound),
            "a resend started at or after settlement expiry"
        );
        match result {
            Some(result) => result,
            None => finish(write).await,
        }
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
                .with_store(|s| {
                    s.apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                        tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
                        Ok(())
                    })
                })
                .unwrap();
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
            let handler =
                ProjectsHandler::with_log_connector("0123456789abcdef".into(), || 700, connector);
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
                },
            );
            handler
        }
        fn restart(&mut self, connector: Arc<dyn LogConnector>) {
            self.handler.store.lock().unwrap().take();
            self.handler = Self::handler(&self.descriptor, connector);
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
    fn refusal(code: &str) -> Action {
        Action::Refuse {
            code: code.into(),
            detail: None,
        }
    }

    // Record append requests when they reach the log transport, so assertions
    // inspect what was actually sent. To simulate another writer acting first,
    // add a separate entry before handling the tested request. Its expected log
    // head is then outdated, so head_moved reflects a real new log position.
    struct Tap {
        fake: FakeLog,
        requests: Mutex<Vec<Value>>,
        sent_at: Mutex<Vec<Instant>>,
        races: AtomicUsize,
        hang_after_race: AtomicBool,
        race_entry: Mutex<Option<Vec<u8>>>,
    }
    impl Tap {
        fn new(races: usize) -> Arc<Self> {
            Arc::new(Self {
                fake: FakeLog::default(),
                requests: Mutex::new(vec![]),
                sent_at: Mutex::new(vec![]),
                races: AtomicUsize::new(races),
                hang_after_race: AtomicBool::new(false),
                race_entry: Mutex::new(None),
            })
        }
        fn sends(&self) -> Vec<Value> {
            self.requests.lock().unwrap().clone()
        }
    }
    struct TapConnector(Arc<Tap>);
    #[async_trait]
    impl LogConnector for TapConnector {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            Ok(self.0.clone())
        }
    }
    fn connector(tap: &Arc<Tap>) -> Arc<dyn LogConnector> {
        Arc::new(TapConnector(tap.clone()))
    }
    #[async_trait]
    impl LogTransport for Tap {
        async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
            let mut raced = false;
            if method == log_client::APPEND {
                self.requests.lock().unwrap().push(params.clone());
                self.sent_at.lock().unwrap().push(Instant::now());
                if self
                    .races
                    .try_update(Ordering::SeqCst, Ordering::SeqCst, |n| n.checked_sub(1))
                    .is_ok()
                {
                    raced = true;
                    let head = self.fake.head();
                    let data = self
                        .race_entry
                        .lock()
                        .unwrap()
                        .take()
                        .unwrap_or_else(|| br#"{"op":"project.shared","tables":{}}"#.to_vec());
                    self.fake
                        .client()
                        .append(&AppendRequest {
                            expected_head: head,
                            entry_id: [(head + 1) as u8; 16],
                            kind: EntryKind::Change,
                            data,
                        })
                        .await
                        .unwrap();
                }
            }
            let result = self.fake.call(method, params).await;
            if raced && self.hang_after_race.load(Ordering::SeqCst) {
                for _ in 0..30 {
                    self.fake.on_append(Action::Hang);
                }
            }
            result
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cached_register_restart_precedes_lock_connector_and_pending() {
        let tap = Tap::new(0);
        let mut f = Fixture::new("write-cache", connector(&tap));
        let root = f.dir.join("checkout");
        let parent = f.dir.join("workers");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&parent).unwrap();
        let request = json!({"projectId":"P","name":"P","roots":[std::fs::canonicalize(root).unwrap()],"derivedRootParents":[std::fs::canonicalize(parent).unwrap()],"requestKey":"key"});
        let body = f.call("register", request.clone()).await.unwrap();
        let image = f.image();
        let fail = Arc::new(FailConnector::default());
        f.restart(fail.clone());
        let lock = f.handler.writer.lock().await;
        let retry = ready(f.call("register", request)).unwrap();
        assert_eq!(retry, body);
        assert_eq!(f.pending(), 0);
        assert_eq!(f.image(), image);
        assert_eq!(fail.calls(), 0);
        assert_eq!(
            f.call("remove", json!({"projectId":"P","requestKey":"key"}))
                .await
                .unwrap_err()
                .code,
            "request_key_reused_across_ops"
        );
        drop(lock);
        assert_eq!(tap.fake.head(), 1);
    }

    #[tokio::test(start_paused = true)]
    async fn local_commit_clears_losing_attempts_but_keeps_current_and_future_slots() {
        let tap = Tap::new(0);
        let mut f = Fixture::new("shared-pending-cleanup", connector(&tap));
        tap.fake.on_append(Action::Hold);
        let received = Instant::now();
        let mut x = Box::pin(f.call("register", register("X", "X")));
        reach(x.as_mut(), tap.fake.wait_for_calls(2)).await;
        assert_eq!(
            expire(x.as_mut(), &tap, received + SETTLEMENT)
                .await
                .unwrap_err()
                .code,
            "engram_outcome_unknown"
        );
        assert_eq!(f.pending(), 1);
        finish(std::pin::pin!(f.call("register", register("Y", "Y"))))
            .await
            .unwrap();
        assert_eq!(tap.fake.head(), 1);
        assert_eq!(f.pending(), 0);
        assert_eq!(
            ready(f.handler.health()).metrics.unwrap()["pendingWriteCount"],
            0
        );
        let status: Value =
            serde_json::from_slice(&ready(f.call("identity_log.status", json!({}))).unwrap())
                .unwrap();
        assert_eq!(status["result"]["pendingWriteCount"], 0);
        // The earlier append, still held by the fake log, expected the head
        // that Y's entry has since moved past, so the log refuses it.
        assert!(tap.fake.release_next().is_err());
        drop(x);
        f.restart(connector(&tap));
        assert_eq!(f.pending(), 0);
        for head in [2, 3] {
            let store = f.store();
            let result = store.shared_attempt(
                format!("future-{head}"),
                head,
                |_| SharedSettlement::Rollback {
                    sent: true,
                    code: "engram_outcome_unknown".into(),
                    message: "unobserved attempt".into(),
                },
                || {
                    store.with_principal("test").register_at(
                        RegisterRequest {
                            project_id: Some(format!("future-{head}")),
                            name: "Future".into(),
                            ..Default::default()
                        },
                        700,
                    )
                },
            );
            assert!(result.is_err());
        }
        finish(std::pin::pin!(f.call("register", register("Z", "Z"))))
            .await
            .unwrap();
        assert_eq!(tap.fake.head(), 2);
        assert_eq!(
            f.pending(),
            2,
            "attempts at or beyond the committed position must survive"
        );
        assert_eq!(
            ready(f.handler.health()).metrics.unwrap()["pendingWriteCount"],
            2
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_write_keeps_writer_exclusion_until_blocking_commit_finishes() {
        let tap = Tap::new(0);
        let mut f = Fixture::new("shared-dropped-commit", connector(&tap));
        let entered = Arc::new(tokio::sync::Notify::new());
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let release_rx = Mutex::new(release_rx);
        let signal = entered.clone();
        // Park only after the append's successful receipt has reached the
        // blocking worker, but before that worker commits its transaction.
        f.handler.shared_commit_hook = Some(Arc::new(move || {
            signal.notify_one();
            release_rx.lock().unwrap().recv().unwrap();
        }));
        let mut write = Box::pin(f.handler.write_wait(
            WireRequest {
                method: "register".into(),
                params: register("once", "once"),
            },
            ROUTE,
            Instant::now(),
        ));
        reach(write.as_mut(), entered.notified()).await;
        assert_eq!(tap.fake.head(), 1);
        drop(write);
        let mut queued = Box::pin(f.handler.writer.lock());
        let excluded = queued
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending();
        drop(queued);
        // Always release the worker before asserting, including on a broken
        // exclusion implementation, so a failed test cannot strand a thread.
        release_tx.send(()).unwrap();
        let store = f.store();
        let catch = async {
            let _writer = f.handler.writer.lock().await;
            f.handler.catch_up(&store, Instant::now() + DEADLINE).await
        };
        finish(std::pin::pin!(catch)).await.unwrap();
        assert!(
            excluded,
            "dropping the request released exclusion before commit"
        );
        assert_eq!(
            f.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            1
        );
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
        assert_eq!(f.store().generation().unwrap(), 3);
        assert_eq!(f.handler.health.log_state.load(Ordering::Relaxed), 0);
        assert_eq!(f.pending(), 0);
        assert_eq!(tap.sends().len(), 1);
    }

    #[tokio::test]
    async fn domain_and_oversize_refusals_restore_pending_journal_and_projection_on_restart() {
        let tap = Tap::new(0);
        let mut f = Fixture::new("write-refused", connector(&tap));
        let image = f.image();
        assert_eq!(
            f.call("remove", json!({"projectId":"absent"}))
                .await
                .unwrap_err()
                .code,
            "not_found"
        );
        let error = f
            .call(
                "register",
                json!({"projectId":"big","name":"x".repeat(300_000),"roots":[]}),
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "shared_entry_too_large");
        assert!(
            error.message.contains("bytes; cap is 262144 bytes"),
            "{}",
            error.message
        );
        assert_eq!(f.pending(), 0);
        assert_eq!(f.image(), image);
        assert!(tap.sends().is_empty());
        f.restart(connector(&tap));
        assert_eq!(f.pending(), 0);
        assert_eq!(f.image(), image);
        assert_eq!(tap.fake.head(), 0);
    }

    #[tokio::test]
    async fn empty_shared_comparison_commits_local_roots_without_append_or_pending() {
        let tap = Tap::new(0);
        let f = Fixture::new("write-empty", connector(&tap));
        f.call("register", register("P", "first")).await.unwrap();
        let root = f.dir.join("root");
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let before = f.store().generation().unwrap();
        f.call(
            "register",
            json!({"projectId":"P","name":"P","roots":[root],"requestKey":"local"}),
        )
        .await
        .unwrap();
        assert!(f.store().generation().unwrap() > before);
        assert_eq!(f.pending(), 0);
        assert_eq!(tap.sends().len(), 1);
        assert_eq!(tap.fake.head(), 1);
        assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
        assert!(f.store().verify().unwrap().ok);
    }

    #[tokio::test]
    async fn lost_reply_resends_identical_bytes_id_and_stale_head_and_commits_once() {
        let tap = Tap::new(0);
        tap.fake.on_append(Action::DropReply);
        let f = Fixture::new("write-lost", connector(&tap));
        f.call("register", register("P", "lost")).await.unwrap();
        let sends = tap.sends();
        assert_eq!(sends.len(), 2);
        assert_eq!(sends[0], sends[1]);
        assert_eq!(sends[1]["expected_head"], 0);
        assert_eq!(tap.fake.head(), 1);
        assert_eq!(f.pending(), 0);
        assert_eq!(
            f.store()
                .journal_tail(0, 100)
                .unwrap()
                .entries
                .iter()
                .filter(|e| e.op == "register")
                .count(),
            1
        );
        assert!(f.store().verify().unwrap().ok);
    }

    #[tokio::test]
    async fn head_moved_uses_new_entry_id_but_agent_id_drawn_once_before_attempts() {
        DRAWS.store(0, Ordering::SeqCst);
        let tap = Tap::new(1);
        let mut f = Fixture::new("write-race", connector(&tap));
        f.handler.agent_id_draw = fixed_draw;
        let body: Value =
            serde_json::from_slice(&f.call("agent.create", agent("new-agent")).await.unwrap())
                .unwrap();
        assert_eq!(
            body["result"]["agent"]["agent_id"],
            "agent_0123456789abcdef"
        );
        assert_eq!(DRAWS.load(Ordering::SeqCst), 1);
        let sends = tap.sends();
        assert_eq!(sends.len(), 2);
        assert_ne!(sends[0]["entry_id"], sends[1]["entry_id"]);
        for send in sends {
            let entry: Value = serde_json::from_slice(
                &log_client::decode_hex(send["entry"]["data"].as_str().unwrap()).unwrap(),
            )
            .unwrap();
            assert_eq!(entry["agent_id"], "agent_0123456789abcdef");
            assert_eq!(entry["row"]["created_at_ms"], 700);
        }
        assert_eq!(f.pending(), 0);
        assert_eq!(f.store().agent_snapshot().unwrap().agents.len(), 1);
        assert!(f.store().verify().unwrap().ok);
    }

    #[tokio::test]
    async fn five_retries_then_head_moved_refuses_identity_log_contended() {
        let tap = Tap::new(6);
        let f = Fixture::new("write-contended", connector(&tap));
        assert_eq!(
            f.call("agent.create", agent("contended"))
                .await
                .unwrap_err()
                .code,
            "identity_log_contended"
        );
        let sends = tap.sends();
        assert_eq!(sends.len(), 6);
        assert_eq!(
            sends
                .iter()
                .map(|v| v["entry_id"].as_str().unwrap())
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            6
        );
        assert!(f.store().agent_snapshot().unwrap().agents.is_empty());
        assert_eq!(tap.fake.head(), 6);
    }

    #[tokio::test(start_paused = true)]
    async fn ambiguous_resend_refusals_keep_pending_until_receipt_relative_bound_and_recovery() {
        async fn case(code: Option<&str>, state: &str) {
            let tap = Tap::new(0);
            tap.fake.on_append(Action::Hold);
            for _ in 0..30 {
                tap.fake
                    .on_append(code.map(refusal).unwrap_or(Action::Hang));
            }
            let mut f = Fixture::new(&format!("write-unknown-{state}"), connector(&tap));
            // Pretend the module received this request 18 seconds ago. Only two
            // seconds remain in its 20-second limit for confirming the append;
            // starting that limit at the first send would give it extra time.
            let received = Instant::now() - Duration::from_secs(18);
            let bound = received + SETTLEMENT;
            let result = {
                let write = f.handler.write_wait(
                    WireRequest {
                        method: "register".into(),
                        params: register("P", "unknown"),
                    },
                    ROUTE,
                    received,
                );
                tokio::pin!(write);
                reach(write.as_mut(), tap.fake.wait_for_calls(2)).await;
                assert_eq!(tap.fake.held_count(), 1);
                assert_eq!(tap.sends().len(), 1);
                // Time out the held send, then permit the resend under the same id.
                tokio::time::advance(SEND_WAIT + TIMER_TICK).await;
                pending(write.as_mut());
                tokio::time::advance(RESEND_DELAY + TIMER_TICK).await;
                reach(write.as_mut(), tap.fake.wait_for_calls(3)).await;
                assert_eq!(tap.sends().len(), 2);
                if code.is_some() {
                    // Each refusal must allow another resend before expiry,
                    // even if SQLite has not finished an erroneous early rollback.
                    tokio::time::advance(RESEND_DELAY + TIMER_TICK).await;
                    pending(write.as_mut());
                    assert_eq!(
                        tap.sends().len(),
                        3,
                        "a resend refusal ended settlement early"
                    );
                }
                expire(write.as_mut(), &tap, bound).await
            };
            assert_eq!(result.unwrap_err().code, "engram_outcome_unknown");
            assert_eq!(f.pending(), 1);
            assert_eq!(tap.fake.head(), 0);
            assert!(f.store().enumerate(None).unwrap().projects.is_empty());
            assert_eq!(
                health_state(f.handler.health.log_state.load(Ordering::Relaxed)),
                state
            );
            assert!(tap.sends().len() >= 2);
            let sends = tap.sends();
            assert!(sends.iter().all(|v| v == &sends[0]));
            f.restart(connector(&tap));
            f.handler
                .catch_up(&f.store(), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(
                f.pending(),
                1,
                "an unchanged head cannot settle an in-flight send"
            );
            assert_eq!(tap.fake.release_next().unwrap(), 1);
            f.handler
                .catch_up(&f.store(), Instant::now() + Duration::from_secs(1))
                .await
                .unwrap();
            assert_eq!(f.pending(), 0);
            assert_eq!(f.store().enumerate(None).unwrap().projects.len(), 1);
            assert_eq!(
                f.handler
                    .health
                    .log_own_entries_applied
                    .load(Ordering::Relaxed),
                1
            );
            assert!(f.store().verify().unwrap().ok);
        }
        // Run the cases one at a time because they share Tokio's paused clock.
        // One case advancing time could otherwise expire another case's request
        // while its worker thread is still doing database work.
        case(Some("unavailable"), "ok").await;
        case(Some("not_member"), "not_member").await;
        case(Some("key_unavailable"), "engram_key_unavailable").await;
        case(None, "ok").await;
    }

    struct Warning(Arc<Mutex<String>>);
    impl tracing::Subscriber for Warning {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            struct Fields<'a>(&'a mut String);
            impl tracing::field::Visit for Fields<'_> {
                fn record_debug(
                    &mut self,
                    field: &tracing::field::Field,
                    value: &dyn std::fmt::Debug,
                ) {
                    self.0.push_str(&format!("{}={value:?};", field.name()));
                }
            }
            if *event.metadata().level() == tracing::Level::WARN {
                event.record(&mut Fields(&mut self.0.lock().unwrap()));
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    #[tokio::test]
    async fn id_reused_rolls_back_warns_entry_id_sets_health_and_never_retries() {
        let tap = Tap::new(0);
        tap.fake.on_append(refusal("id_reused"));
        let f = Fixture::new("write-invariant", connector(&tap));
        let image = f.image();
        let output = Arc::new(Mutex::new(String::new()));
        let _subscriber = tracing::subscriber::set_default(Warning(output.clone()));
        assert_eq!(
            f.call("register", register("P", "bug"))
                .await
                .unwrap_err()
                .code,
            "identity_log_invariant"
        );
        assert_eq!(tap.sends().len(), 1);
        assert_eq!(f.pending(), 1);
        assert_eq!(f.image(), image);
        assert_eq!(
            health_state(f.handler.health.log_state.load(Ordering::Relaxed)),
            "identity_log_invariant"
        );
        let sends = tap.sends();
        let output = output.lock().unwrap();
        assert!(
            output.contains(sends[0]["entry_id"].as_str().unwrap()),
            "{output}"
        );
        assert!(output.contains("identity log invariant"), "{output}");
        assert_eq!(tap.fake.head(), 0);
    }

    struct HangConnect;
    #[async_trait]
    impl LogConnector for HangConnect {
        async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
            std::future::pending().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn queued_connecting_and_catchup_deadlines_are_receipt_relative_and_never_send() {
        assert_eq!(DEADLINE, Duration::from_secs(25));
        assert_eq!(SETTLEMENT, Duration::from_secs(20));
        async fn case(label: &str, connector: Arc<dyn LogConnector>, lock: bool, code: &str) {
            let f = Fixture::new(label, connector);
            let _guard = if lock {
                Some(f.handler.writer.lock().await)
            } else {
                None
            };
            let before = f.image();
            let received = Instant::now() - Duration::from_secs(18);
            let write = f.handler.write_wait(
                WireRequest {
                    method: "register".into(),
                    params: register("P", "deadline"),
                },
                ROUTE,
                received,
            );
            tokio::pin!(write);
            pending(write.as_mut());
            tokio::time::advance(Duration::from_secs(6)).await;
            pending(write.as_mut());
            assert_eq!(f.pending(), 0);
            assert_eq!(f.image(), before);
            tokio::time::advance(Duration::from_secs(1)).await;
            tokio::time::sleep_until(received + DEADLINE).await;
            let error = ready(write.as_mut()).unwrap_err();
            assert_eq!(error.code, code);
            assert_eq!(f.pending(), 0);
            assert_eq!(f.image(), before);
        }
        let tap = Tap::new(0);
        tap.fake.on_read(Action::Hang);
        case(
            "write-queued",
            Arc::new(FailConnector::default()),
            true,
            "identity_log_contended",
        )
        .await;
        case(
            "write-connect",
            Arc::new(HangConnect),
            false,
            "engram_unavailable",
        )
        .await;
        case(
            "write-catchup",
            connector(&tap),
            false,
            "engram_unavailable",
        )
        .await;
        assert_eq!(tap.fake.calls(), 1);
        assert!(tap.sends().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn settlement_expired_before_first_send_deletes_the_committed_pending_row() {
        let tap = Tap::new(0);
        let f = Fixture::new("write-never-sent", connector(&tap));
        let before = f.image();
        let error = f
            .handler
            .write_wait(
                WireRequest {
                    method: "register".into(),
                    params: register("P", "never"),
                },
                ROUTE,
                Instant::now() - SETTLEMENT,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, "engram_unavailable");
        assert_eq!(f.pending(), 0);
        assert_eq!(f.image(), before);
        assert!(tap.sends().is_empty());
    }

    #[tokio::test]
    async fn malformed_agent_cache_retry_keeps_strict_decode_and_enabling_fence() {
        let tap = Tap::new(0);
        let f = Fixture::new("write-agent-cache", connector(&tap));
        f.call("agent.create", agent("cache")).await.unwrap();
        let calls = tap.fake.calls();
        let mut malformed = agent("cache");
        malformed["residence"] = json!("removed-field");
        assert_eq!(
            f.call("agent.create", malformed).await.unwrap_err().code,
            "invalid_request"
        );
        assert_eq!(tap.fake.calls(), calls);
        f.store()
            .apply_entry("test.enabling", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='enabling'", [])?;
                Ok(())
            })
            .unwrap();
        assert_eq!(
            f.call("agent.create", agent("cache"))
                .await
                .unwrap_err()
                .code,
            "identity_log_enabling"
        );
        assert_eq!(tap.fake.calls(), calls);
    }

    #[tokio::test(start_paused = true)]
    async fn local_writes_and_liveness_never_touch_engram_on_enabled_store() {
        let fail = Arc::new(FailConnector::default());
        let f = Fixture::new("write-local", fail.clone());
        let root = f.dir.join("repo");
        let container = f.dir.join("workers");
        std::fs::create_dir(&root).unwrap();
        std::fs::create_dir(&container).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        let container = std::fs::canonicalize(container).unwrap();
        assert!(std::process::Command::new("git")
            .args(["init", "--quiet"])
            .arg(&root)
            .status()
            .unwrap()
            .success());
        // Register the checkout and its execution binding (the checkout identity
        // used for approvals) with shared logging disabled. Turn logging back on
        // before testing local writes, so their success cannot depend on a log
        // connection. The fake connector fails if any operation tries to use it.
        f.store()
            .apply_entry("test.disabled", "{}", "test", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state='disabled'", [])?;
                Ok(())
            })
            .unwrap();
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
                tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
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
            "projects.session_liveness",
            json!({"seq":1,"snapshot":true,"sessions":[]}),
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

    #[tokio::test(start_paused = true)]
    async fn pending_commits_before_parked_transaction_and_reads_show_prewrite_state() {
        let tap = Tap::new(0);
        let f = Fixture::new("write-parked", connector(&tap));
        let body: Value =
            serde_json::from_slice(&f.call("agent.create", agent("first")).await.unwrap()).unwrap();
        let id = body["result"]["agent"]["agent_id"].as_str().unwrap();
        let generation = f.store().generation().unwrap();
        tap.fake.on_append(Action::Hold);
        let observe = async {
            tap.fake.wait_for_calls(4).await;
            assert_eq!(
                f.pending(),
                1,
                "pending row must be visible outside the open write transaction"
            );
            for (op, params) in [
                (
                    "resolve",
                    json!({"canonicalRoot":std::fs::canonicalize(&f.dir).unwrap()}),
                ),
                ("enumerate", json!({})),
                ("verify", json!({})),
                ("agent.resolve", json!({"agent_id":id})),
            ] {
                let bytes = serde_json::to_vec(&json!({"method":op,"params":params})).unwrap();
                let reply = ready(f.handler.handle_served_request(&bytes, ROUTE));
                assert!(matches!(reply, HandlerOutcome::Response(_)), "{op}");
            }
            let health = ready(f.handler.health());
            assert_eq!(health.metrics.unwrap()["generation"], generation);
            assert_eq!(f.store().generation().unwrap(), generation);
            assert_eq!(f.store().agent_row(id).unwrap().unwrap().name, "Ada");
            tap.fake.release_next().unwrap();
        };
        let (result, ()) = tokio::join!(
            f.call(
                "agent.rename",
                json!({"agent_id":id,"name":"Grace","request_key":"rename"})
            ),
            observe
        );
        result.unwrap();
        assert_eq!(f.pending(), 0);
        assert_eq!(f.store().agent_row(id).unwrap().unwrap().name, "Grace");
        assert_eq!(tap.fake.head(), 2);
        assert!(f.store().verify().unwrap().ok);
    }

    #[tokio::test(start_paused = true)]
    async fn head_moved_then_ambiguous_append_does_not_restart_settlement_clock() {
        let tap = Tap::new(1);
        // Add another writer's entry first, so the initial append is refused with
        // head_moved. Leave later append requests unanswered to verify that
        // reading the new entry and retrying do not extend the original time limit.
        tap.hang_after_race.store(true, Ordering::SeqCst);
        let f = Fixture::new("write-race-deadline", connector(&tap));
        let received = Instant::now() - Duration::from_secs(18);
        let bound = received + SETTLEMENT;
        let write = f.handler.write_wait(
            WireRequest {
                method: "register".into(),
                params: register("P", "race"),
            },
            ROUTE,
            received,
        );
        tokio::pin!(write);
        reach(write.as_mut(), tap.fake.wait_for_calls(5)).await;
        assert_eq!(tap.sends().len(), 2);
        let result = expire(write.as_mut(), &tap, bound).await;
        assert_eq!(result.unwrap_err().code, "engram_outcome_unknown");
        assert_eq!(f.pending(), 1);
        assert!(f.store().enumerate(None).unwrap().projects.is_empty());
    }

    #[tokio::test]
    async fn add_root_head_moved_reuses_first_attempt_timestamps() {
        let tap = Tap::new(0);
        let f = Fixture::new("write-add-clock", connector(&tap));
        f.call("register", register("P", "base")).await.unwrap();
        tap.races.store(1, Ordering::SeqCst);
        let root = f.dir.join("root");
        std::fs::create_dir(&root).unwrap();
        let root = std::fs::canonicalize(root).unwrap();
        f.call(
            "add_root",
            json!({"projectId":"P","root":root,"label":"stable"}),
        )
        .await
        .unwrap();
        let sends = tap.sends();
        assert_eq!(sends.len(), 3);
        let entries: Vec<Value> = sends[1..]
            .iter()
            .map(|send| {
                serde_json::from_slice(
                    &log_client::decode_hex(send["entry"]["data"].as_str().unwrap()).unwrap(),
                )
                .unwrap()
            })
            .collect();
        assert_eq!(entries[0], entries[1]);
        assert_eq!(
            entries[0]["tables"]["project_root_key"]["upsert"][0]["created_at"],
            700
        );
        for row in f
            .store()
            .journal_tail(0, 100)
            .unwrap()
            .entries
            .iter()
            .filter(|e| matches!(e.op.as_str(), "add_root" | "root_key.assign"))
        {
            assert_eq!(row.created_at, 700);
        }
        assert_eq!(f.pending(), 0);
        assert!(f.store().verify().unwrap().ok);
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
        fn direct(f: &mut Fixture, prompt: Arc<Prompt>, enabled: bool) {
            f.handler.operator_confirmer = prompt;
            bind(f);
            if !enabled {
                state(f, "disabled");
            }
        }
        fn bind(f: &Fixture) {
            f.handler.route_admissions().insert(
                DIRECT,
                RouteAdmission {
                    principal: Some(Principal::Direct),
                    flow_id: None,
                    handle: None,
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
        async fn confirmed_methods_pin_summaries_local_attribution_and_shared_bytes() {
            for enabled in [false, true] {
                let tap = Tap::new(0);
                let mut f = Fixture::new("confirmed-summaries", connector(&tap));
                let prompt = Prompt::new(Some(Ok(())));
                direct(&mut f, prompt.clone(), enabled);
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
                for send in tap.sends() {
                    let bytes =
                        log_client::decode_hex(send["entry"]["data"].as_str().unwrap()).unwrap();
                    assert!(!String::from_utf8(bytes)
                        .unwrap()
                        .contains("operator_confirmed"));
                }
            }
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
                direct(&mut f, prompt.clone(), false);
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
            for enabled in [false, true] {
                let tap = Tap::new(0);
                let mut f = Fixture::new("confirmation-precheck", connector(&tap));
                let prompt = Prompt::new(Some(Ok(())));
                direct(&mut f, prompt.clone(), enabled);
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
            direct(&mut f, prompt.clone(), false);
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
            let tap = Tap::new(0);
            let other = Tap::new(0);
            let mut f = Fixture::new("confirmation-relay", connector(&tap));
            let mut baseline = Fixture::new("confirmation-relay-baseline", connector(&other));
            let prompt = Prompt::new(Some(Err(OperatorConfirmError::Declined)));
            direct(&mut f, prompt.clone(), true);
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
            assert!(actual.iter().all(|r| r.principal.as_deref()
                == Some("reserved:prefrontal-core")
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
                for op in agent_ops::CONFIRMABLE_METHODS {
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
            for (enabled, change) in [(false, "name"), (true, "name"), (false, "placement")] {
                let tap = Tap::new(0);
                let mut f = Fixture::new("confirmation-unlocked", connector(&tap));
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone(), enabled);
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
            direct(&mut f, prompt.clone(), false);
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
                let tap = Tap::new(0);
                let mut f = Fixture::new("confirmation-route-close", connector(&tap));
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone(), true);
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
                assert!(tap.sends().is_empty());
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
            for (stage, late, enabled) in [
                ("lock", false, true),
                ("connect", false, true),
                ("catchup", false, true),
                ("unsent", false, true),
                ("lock", true, true),
                ("lock", true, false),
            ] {
                let tap = Tap::new(0);
                if stage == "catchup" {
                    tap.fake.on_read(Action::Hang);
                }
                let connector = if stage == "connect" {
                    Arc::new(HangConnect) as Arc<dyn LogConnector>
                } else {
                    connector(&tap)
                };
                let mut f = Fixture::new("confirmation-stage-deadline", connector);
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone(), enabled);
                let received = Instant::now();
                let mut first = Box::pin(write(&f, "agent.create", new_request("deadline")));
                pending(first.as_mut());
                tokio::time::advance(Duration::from_secs(if late { 260 } else { 30 })).await;
                let approval = Instant::now();
                let lock = if matches!(stage, "lock" | "unsent") {
                    Some(f.handler.writer.clone().lock_owned().await)
                } else {
                    None
                };
                let before = image(&f);
                prompt.answer(0);
                pending(first.as_mut());
                let bound =
                    (approval + Duration::from_secs(25)).min(received + Duration::from_secs(270));
                if stage == "unsent" {
                    tokio::time::advance(Duration::from_secs(20) + TIMER_TICK).await;
                    drop(lock);
                    assert_eq!(
                        finish(first.as_mut()).await.unwrap_err().code,
                        "engram_unavailable"
                    );
                } else {
                    tokio::time::advance(
                        bound.saturating_duration_since(Instant::now()) - Duration::from_secs(1),
                    )
                    .await;
                    pending(first.as_mut());
                    tokio::time::advance(Duration::from_secs(1) + TIMER_TICK).await;
                    assert_eq!(
                        ready(first.as_mut()).unwrap_err().code,
                        if stage == "lock" {
                            "identity_log_contended"
                        } else {
                            "engram_unavailable"
                        }
                    );
                }
                assert_eq!(prompt.count(), 1);
                assert_eq!(image(&f), before);
                assert!(tap.sends().is_empty());
            }
            // An approval later than the old receipt-relative catch-up budget
            // must still have a fresh budget and be able to land.
            let tap = Tap::new(0);
            let mut f = Fixture::new("confirmation-delayed-success", connector(&tap));
            let prompt = Prompt::new(None);
            direct(&mut f, prompt.clone(), true);
            let mut first = Box::pin(write(&f, "agent.create", new_request("delayed")));
            pending(first.as_mut());
            tokio::time::advance(Duration::from_secs(30)).await;
            prompt.answer(0);
            finish(first.as_mut()).await.unwrap();
            assert_eq!(rows(&f).len(), 1);
        }

        #[tokio::test(start_paused = true)]
        async fn recovered_append_loses_local_approval_but_lost_reply_resend_retains_it() {
            for late in [false, true] {
                let tap = Tap::new(0);
                tap.fake.on_append(Action::DropReply);
                for _ in 0..300 {
                    tap.fake.on_append(Action::Hang);
                }
                let mut f = Fixture::new("confirmation-recovered", connector(&tap));
                let prompt = Prompt::new(None);
                direct(&mut f, prompt.clone(), true);
                let received = Instant::now();
                let mut first = Box::pin(write(&f, "agent.create", new_request("unknown")));
                pending(first.as_mut());
                tokio::time::advance(Duration::from_secs(if late { 260 } else { 30 })).await;
                let approval = Instant::now();
                prompt.answer(0);
                reach(first.as_mut(), tap.fake.wait_for_calls(2)).await;
                assert_eq!(tap.fake.head(), 1);
                let bound =
                    (approval + Duration::from_secs(20)).min(received + Duration::from_secs(270));
                assert_eq!(
                    expire(first.as_mut(), &tap, bound).await.unwrap_err().code,
                    "engram_outcome_unknown"
                );
                drop(first);
                assert!(rows(&f).is_empty());
                assert_eq!(f.pending(), 1);
                assert!(f.store().agent_snapshot().unwrap().agents.is_empty());
                finish(std::pin::pin!(f
                    .handler
                    .catch_up(&f.store(), Instant::now() + DEADLINE)))
                .await
                .unwrap();
                let row = rows(&f).pop().unwrap();
                assert_eq!(row.actor, "log");
                assert_eq!(row.principal.as_deref(), Some("entorhinal"));
                assert!(!row.payload_json.contains("operator_confirmed"));
                assert_eq!(f.pending(), 0);
                assert_eq!(prompt.count(), 1);
                let snapshot = f.store().agent_snapshot().unwrap();
                finish(std::pin::pin!(f
                    .handler
                    .catch_up(&f.store(), Instant::now() + DEADLINE)))
                .await
                .unwrap();
                assert_eq!(
                    serde_json::to_value(f.store().agent_snapshot().unwrap()).unwrap(),
                    serde_json::to_value(snapshot).unwrap()
                );
            }
            let tap = Tap::new(0);
            tap.fake.on_append(Action::DropReply);
            let mut f = Fixture::new("confirmation-resend", connector(&tap));
            let prompt = Prompt::new(Some(Ok(())));
            direct(&mut f, prompt.clone(), true);
            let mut first = Box::pin(write(&f, "agent.create", new_request("lost")));
            reach(first.as_mut(), tap.fake.wait_for_calls(2)).await;
            tokio::time::advance(RESEND_DELAY + TIMER_TICK).await;
            finish(first.as_mut()).await.unwrap();
            assert_eq!(tap.sends().len(), 2);
            assert_eq!(tap.sends()[0], tap.sends()[1]);
            let row = rows(&f).pop().unwrap();
            assert_eq!(row.principal.as_deref(), Some("direct"));
            assert_eq!(
                serde_json::from_str::<Value>(&row.payload_json).unwrap()["operator_confirmed"],
                true
            );
            assert_eq!(prompt.count(), 1);
            assert_eq!(f.pending(), 0);
        }

        #[tokio::test]
        async fn retry_catchup_renaming_target_invalidates_approval_without_reprompting() {
            let tap = Tap::new(0);
            let mut f = Fixture::new("confirmation-retry-stale", connector(&tap));
            let prompt = Prompt::new(Some(Ok(())));
            direct(&mut f, prompt.clone(), true);
            f.handler.agent_id_draw = stable_draw;
            f.call("agent.create", new_request("seed")).await.unwrap();
            // Build the competing entry through the real core mutation and
            // shared encoder, not by computing an expected summary ourselves.
            let remote = Tap::new(0);
            let mut peer = Fixture::new("confirmation-retry-peer", connector(&remote));
            peer.handler.agent_id_draw = stable_draw;
            peer.call("agent.create", new_request("seed"))
                .await
                .unwrap();
            peer.call(
                "agent.rename",
                json!({"agent_id":ID,"name":"Grace","request_key":"remote-rename"}),
            )
            .await
            .unwrap();
            *tap.race_entry.lock().unwrap() = Some(
                log_client::decode_hex(remote.sends()[1]["entry"]["data"].as_str().unwrap())
                    .unwrap(),
            );
            tap.races.store(1, Ordering::SeqCst);
            let error = write(
                &f,
                "agent.update_tag",
                json!({"agent_id":ID,"tag":"new","request_key":"stale"}),
            )
            .await
            .unwrap_err();
            assert_eq!(error.code, "operator_approval_stale");
            assert_eq!(prompt.count(), 1);
            let target = f.store().agent_row(ID).unwrap().unwrap();
            assert_eq!(target.name, "Grace");
            assert_eq!(target.tag, "helper");
            assert_eq!(tap.sends().len(), 2);
            assert_eq!(
                rows(&f).iter().map(|r| r.op.as_str()).collect::<Vec<_>>(),
                ["agent.create", "agent.rename"]
            );
            assert_eq!(f.pending(), 0);
        }

        #[tokio::test]
        async fn head_moved_reuses_one_confirmation_and_one_agent_identity() {
            let tap = Tap::new(1);
            let mut f = Fixture::new("confirmation-head-moved", connector(&tap));
            let prompt = Prompt::new(Some(Ok(())));
            direct(&mut f, prompt.clone(), true);
            static COUNT: AtomicUsize = AtomicUsize::new(0);
            fn draw() -> Result<String, HandlerError> {
                COUNT.fetch_add(1, Ordering::SeqCst);
                Ok(ID.into())
            }
            f.handler.agent_id_draw = draw;
            let target = id(&write(&f, "agent.create", new_request("retry"))
                .await
                .unwrap());
            assert_eq!(target, ID);
            assert_eq!(COUNT.load(Ordering::SeqCst), 1);
            assert_eq!(prompt.count(), 1);
            assert_eq!(tap.sends().len(), 2);
            for send in tap.sends() {
                let entry: Value = serde_json::from_slice(
                    &log_client::decode_hex(send["entry"]["data"].as_str().unwrap()).unwrap(),
                )
                .unwrap();
                assert_eq!(entry["agent_id"], ID);
            }
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
                handler: ProjectsHandler,
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
            async fn start(
                f: &mut Fixture,
                answers: impl IntoIterator<Item = OperatorAnswer>,
            ) -> (ModuleHarness, RouteHandle) {
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
                let handler = handler.with_operator_confirmer(confirmer.clone());
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
                            BindIdentity::new("/tmp/project", "test", "ck-agents"),
                        )
                        .with_principal(Principal::Direct),
                    )
                    .await;
                (module, route)
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
                let (mut module, route) = start(&mut f, [OperatorAnswer::Confirmed]).await;
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
                module.shutdown().await;
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
                    let (mut module, route) = start(&mut f, [answer]).await;
                    let before = image(&f);
                    let body = error(&create(&mut module, route).await);
                    assert_eq!(
                        (body["code"].as_str(), body["message"].as_str()),
                        (Some(code), Some(message)),
                        "{label}"
                    );
                    one_confirm(&module);
                    module.shutdown().await;
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
                let (mut module, route) = start(&mut f, [OperatorAnswer::NoAnswer]).await;
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
                module.shutdown().await;
                assert_eq!(cancels(&module), [(0, 0, confirm, 0)]);
                assert_eq!(image(&f), before);
                assert!(rows(&f).is_empty());
            }
        }
    }
}

#[derive(Clone)]
enum Write {
    Register(RegisterRequest),
    Assign(AssignWorkspaceRequest),
    Upgrade(UpgradeImplicitRequest),
    Remove(RemoveRequest),
    Seed(SeedImportRequest),
    Add(entorhinal_core::AddRootRequest),
    Attach(entorhinal_core::AttachRootRequest),
    Agent(String, Value),
}

impl Write {
    fn decode(method: &str, params: Value) -> Result<Self, HandlerError> {
        Ok(match method {
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
            "attach_root" => Self::Attach(serde_json::from_value(params).map_err(invalid_params)?),
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
            Self::Register(r) => r.request_key.as_deref(),
            Self::Assign(r) => r.request_key.as_deref(),
            Self::Upgrade(r) => r.request_key.as_deref(),
            Self::Remove(r) => r.request_key.as_deref(),
            Self::Seed(r) => r.request_key.as_deref(),
            Self::Add(_) => None,
            Self::Attach(r) => r.request_key.as_deref(),
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
        approval: Option<&OperatorApproval>,
    ) -> Result<Vec<u8>, HandlerError> {
        let writer = store.with_principal(principal);
        match self {
            Self::Register(r) => writer.register_at(r, now),
            Self::Assign(r) => writer.assign_workspace_at(r, now),
            Self::Upgrade(r) => writer.upgrade_implicit_at(r, now),
            Self::Remove(r) => writer.remove_at(r, now),
            Self::Seed(r) => writer.seed_import_at(r, now),
            Self::Add(r) => writer.add_root_at(r, now),
            Self::Attach(r) => writer.attach_root_at(r, now),
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
    HandlerError::new(
        match &error {
            RegistryError::Domain { code, .. } => code.as_str(),
            _ => "storage_error",
        },
        error.to_string(),
    )
}

pub(super) fn draw_agent_id() -> Result<String, HandlerError> {
    let mut random = [0u8; 8];
    getrandom::getrandom(&mut random)
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
    Ok(format!("agent_{}", log_client::encode_hex(&random)))
}

impl From<RegistryError> for HandlerError {
    fn from(error: RegistryError) -> Self {
        storage_error(error)
    }
}

fn shared_candidate(op: &str) -> bool {
    matches!(
        op,
        "register"
            | "assign_workspace"
            | "upgrade_implicit"
            | "remove"
            | "seed_import"
            | "add_root"
            | "attach_root"
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

impl ProjectsHandler {
    pub(super) async fn write_wait(
        &self,
        request: WireRequest,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        // Only the served async path can confirm Direct. admit/execute remain
        // unconfirmed, including synchronous tests and non-confirmable methods.
        let admission = self.admission_for(key);
        let direct = matches!(admission.principal, Some(Principal::Direct))
            && agent_ops::CONFIRMABLE_METHODS.contains(&request.method.as_str());
        refuse_flow_write(&request.method, admission.flow_id.as_deref())?;
        if !direct {
            authorize_write(&request.method, admission.principal.as_ref())?;
        }
        let principal = principal_label(admission.principal.as_ref());
        // Liveness is volatile, not a SQLite mutation. It must not queue behind
        // a transaction waiting for the log.
        if request.method == "projects.session_liveness" {
            return self.execute(request, key);
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        let state = store.identity_log_status().map_err(storage_error)?.state;
        let candidate = shared_candidate(&request.method);
        let prepared = candidate
            .then(|| Write::decode(&request.method, request.params.clone()))
            .transpose()?;
        bootstrap_fence(&store, &state, &request)?;
        let mut confirmation = None;
        let mut approval = None;
        let mut budget_start = received;
        if direct {
            let summary = match store.precheck_agent_confirmation(
                &request.method,
                request.params.clone(),
                (self.clock)(),
            )? {
                AgentPrecheck::Cached(body) => return self.attach_incarnation(body),
                AgentPrecheck::Ready(summary) => summary,
            };
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
        } else if state == "enabled" {
            if let Some(write) = &prepared {
                if let Some(body) = write.probe(&store, &request.method, &principal)? {
                    return if request.method.starts_with("agent.") {
                        self.attach_incarnation(body)
                    } else {
                        Ok(body)
                    };
                }
            }
        }
        let deadline = if direct {
            (budget_start + DEADLINE).min(received + OPERATOR_BOUND)
        } else {
            received + DEADLINE
        };
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
        let mut writer = lock_result.map_err(|_| {
            HandlerError::new("identity_log_contended", "writer lock deadline expired")
        })?;
        // Re-read after the lock: enable/join can change state while we queue.
        let state = store.identity_log_status().map_err(storage_error)?.state;
        bootstrap_fence(&store, &state, &request)?;
        if state != "enabled" || !candidate {
            if let Some(approval) = &approval {
                let before = store.generation().map_err(storage_error)?;
                let body = prepared.unwrap().run(
                    &store,
                    &principal,
                    (self.clock)(),
                    "",
                    Some(approval),
                )?;
                // Confirmed local commits wake the identity feed just like
                // execute does; a cached reply must not wake idle readers.
                if store.generation().map_err(storage_error)? != before {
                    self.commits.notify_waiters();
                }
                return self.record_mutation(self.attach_incarnation(body));
            }
            return self.execute(request, key);
        }
        let write = prepared.unwrap();
        // A preceding writer may have populated this key while we queued.
        if let Some(body) = write.probe(&store, &request.method, &principal)? {
            return if request.method.starts_with("agent.") {
                self.attach_incarnation(body)
            } else {
                Ok(body)
            };
        }
        let now = (self.clock)();
        let agent_id = if request.method == "agent.create" {
            (self.agent_id_draw)()?
        } else {
            String::new()
        };
        let settlement = if direct {
            (budget_start + SETTLEMENT).min(received + OPERATOR_BOUND)
        } else {
            received + SETTLEMENT
        };
        for retry in 0..=RETRIES {
            self.catch_up(&store, deadline).await?;
            let status = store.identity_log_status().map_err(storage_error)?;
            let head = status.last_applied_position;
            let pending_count = status.pending_write_count;
            let mut id = [0; 16];
            getrandom::getrandom(&mut id)
                .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
            let (bytes_tx, bytes_rx) = tokio::sync::oneshot::channel();
            let (decision_tx, decision_rx) = std::sync::mpsc::channel();
            let worker_store = store.clone();
            let operation = write.clone();
            let principal = principal.clone();
            let stable_id = agent_id.clone();
            let approval = approval.clone();
            let health = self.health.clone();
            #[cfg(test)]
            let commit_hook = self.shared_commit_hook.clone();
            let worker = tokio::task::spawn_blocking(move || {
                // Dropping the request must not release exclusion before this
                // worker has committed or rolled back its SQLite transaction.
                let mut bytes_tx = Some(bytes_tx);
                let result = worker_store.shared_attempt(
                    log_client::encode_hex(&id),
                    head,
                    move |bytes| {
                        // Preparation reaches this callback only after the pending
                        // row committed. Publish before the append can park.
                        health
                            .log_pending
                            .store(pending_count + 1, Ordering::Relaxed);
                        let _ = bytes_tx.take().unwrap().send(bytes);
                        let decision =
                            decision_rx
                                .recv()
                                .unwrap_or_else(|_| SharedSettlement::Rollback {
                                    sent: true,
                                    code: "engram_outcome_unknown".into(),
                                    message: "settlement waiter was cancelled".into(),
                                });
                        #[cfg(test)]
                        if matches!(decision, SharedSettlement::Commit(_)) {
                            if let Some(hook) = &commit_hook {
                                hook();
                            }
                        }
                        decision
                    },
                    || {
                        operation.run(
                            &worker_store,
                            &principal,
                            now,
                            &stable_id,
                            approval.as_ref(),
                        )
                    },
                );
                (writer, result)
            });
            let mut committed_position = None;
            let mut proven_unsent = false;
            if let Ok(bytes) = bytes_rx.await {
                let append = AppendRequest {
                    expected_head: head as u64,
                    entry_id: id,
                    kind: EntryKind::Change,
                    data: bytes,
                };
                let decision = self.settle(&append, settlement).await;
                match &decision {
                    SharedSettlement::Commit(position) => committed_position = Some(*position),
                    SharedSettlement::Rollback { sent, .. } => proven_unsent = !sent,
                }
                let _ = decision_tx.send(decision);
            }
            let (returned_writer, result) = worker
                .await
                .map_err(|e| HandlerError::new("storage_error", e.to_string()))?;
            writer = returned_writer;
            if result.is_ok() || proven_unsent {
                super::enable::refresh(&self.health, &store);
                if let Some(position) = committed_position.filter(|_| result.is_ok()) {
                    self.health.log_applied.store(position, Ordering::Relaxed);
                    self.health.log_head.fetch_max(position, Ordering::Relaxed);
                }
            }
            if let Err(error) = &result {
                super::enable::record_error(&self.health, error);
            }
            if result.as_ref().is_err_and(|e| e.code == "head_moved") {
                if retry == RETRIES {
                    return Err(HandlerError::new(
                        "identity_log_contended",
                        "log head moved after five retries",
                    ));
                }
                continue;
            }
            let mut body = result?;
            if matches!(request.method.as_str(), "register" | "add_root")
                && store.root_records_enabled()
            {
                if let Err(error) = store
                    .with_principal(&principal_label(admission.principal.as_ref()))
                    .bind_unbound_roots("entorhinal")
                {
                    tracing::warn!(target:"binding", "binding after shared write failed: {error}");
                }
            }
            if request.method.starts_with("agent.") {
                body = self.attach_incarnation(body)?;
            }
            self.commits.notify_waiters();
            return self.record_mutation(Ok(body));
        }
        unreachable!()
    }

    async fn settle(&self, request: &AppendRequest, bound: Instant) -> SharedSettlement {
        let mut sent = false;
        loop {
            if Instant::now() >= bound {
                return SharedSettlement::Rollback {
                    sent,
                    code: if sent {
                        "engram_outcome_unknown"
                    } else {
                        "engram_unavailable"
                    }
                    .into(),
                    message: "shared write settlement deadline expired".into(),
                };
            }
            sent = true;
            match timeout_at(
                bound.min(Instant::now() + SEND_WAIT),
                self.log_client.append(request),
            )
            .await
            {
                Ok(Ok(reply)) if reply.position == request.expected_head + 1 => {
                    return SharedSettlement::Commit(reply.position as i64)
                }
                Ok(Err(LogError::HeadMoved { .. })) => {
                    return SharedSettlement::Rollback {
                        sent,
                        code: "head_moved".into(),
                        message: "log head moved".into(),
                    }
                }
                Ok(Err(LogError::IdReused)) | Ok(Ok(_)) => {
                    self.health.log_state.store(3, Ordering::Relaxed);
                    tracing::warn!(entry_id = %log_client::encode_hex(&request.entry_id), "identity log invariant: entry id reused or invalid receipt position");
                    return SharedSettlement::Rollback {
                        sent,
                        code: "identity_log_invariant".into(),
                        message: format!(
                            "identity log invariant for entry_id {}",
                            log_client::encode_hex(&request.entry_id)
                        ),
                    };
                }
                Ok(Err(LogError::KeyUnavailable)) => {
                    self.health.log_state.store(1, Ordering::Relaxed)
                }
                Ok(Err(LogError::NotMember)) => self.health.log_state.store(2, Ordering::Relaxed),
                _ => {}
            }
            tokio::time::sleep_until(bound.min(Instant::now() + RESEND_DELAY)).await;
        }
    }
}

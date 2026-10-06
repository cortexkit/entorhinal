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
use entorhinal_core::{RegistryStore, SharedSettlement};
use serde_json::Value;
use std::{sync::atomic::Ordering, time::Duration};
use tokio::time::{timeout_at, Instant};

const DEADLINE: Duration = Duration::from_secs(25);
const SETTLEMENT: Duration = Duration::from_secs(20);
const SEND_WAIT: Duration = Duration::from_secs(1);
const RESEND_DELAY: Duration = Duration::from_millis(100);
const RETRIES: usize = 5;

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
    }
    impl Tap {
        fn new(races: usize) -> Arc<Self> {
            Arc::new(Self {
                fake: FakeLog::default(),
                requests: Mutex::new(vec![]),
                sent_at: Mutex::new(vec![]),
                races: AtomicUsize::new(races),
                hang_after_race: AtomicBool::new(false),
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
                    self.fake
                        .client()
                        .append(&AppendRequest {
                            expected_head: head,
                            entry_id: [(head + 1) as u8; 16],
                            kind: EntryKind::Change,
                            data: br#"{"op":"project.shared","tables":{}}"#.to_vec(),
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
                    .agent_mutation_with_id(
                        &op,
                        params,
                        now,
                        (!agent_id.is_empty()).then_some(agent_id),
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
        let admission = self.admit(&request.method, key)?;
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
        if state == "enabled" {
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
        let deadline = received + DEADLINE;
        let mut writer = timeout_at(deadline, self.writer.clone().lock_owned())
            .await
            .map_err(|_| {
                HandlerError::new("identity_log_contended", "writer lock deadline expired")
            })?;
        // Re-read after the lock: enable/join can change state while we queue.
        let state = store.identity_log_status().map_err(storage_error)?.state;
        bootstrap_fence(&store, &state, &request)?;
        if state != "enabled" || !candidate {
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
        let settlement = received + SETTLEMENT;
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
                    || operation.run(&worker_store, &principal, now, &stable_id),
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

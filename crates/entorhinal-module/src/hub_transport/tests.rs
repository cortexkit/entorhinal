use std::{future::Future, task::Poll};

use futures_util::{poll, stream};
use tracing_subscriber::fmt::{format::FmtSpan, MakeWriter};

use super::*;
use crate::tests::log_surface_handler;
use subc_client_rs::ModuleHandler;

mod live;

#[tokio::test]
async fn hub_health_is_absent_for_unswitched_store_and_reads_stay_local() {
    let (dir, handler, _) = log_surface_handler("hub-transport-read");
    let report = handler.health().await.metrics.unwrap();
    for key in [
        "hubConnected",
        "hubLogTailLive",
        "hubKvWatchLive",
        "hubFailures",
        "kvRecordsRefused",
        "epochHeld",
    ] {
        assert!(report.get(key).is_none(), "dormant metric {key}");
    }

    let _hub = Hub::new(SubcConnector::default(), handler.health.clone());
    let state = &handler.health.hub;
    let report = handler.health().await.metrics.unwrap();
    assert_eq!(report["hubConnected"], false);
    assert_eq!(report["hubLogTailLive"], false);
    assert_eq!(report["hubKvWatchLive"], false);
    assert_eq!(report["hubFailures"], 0);
    assert_eq!(report["kvRecordsRefused"], 0);
    assert_eq!(report["epochHeld"], Value::Null);
    state.failed();
    state.kv_records_refused.fetch_add(1, Ordering::Relaxed);
    state.epoch_held.store(17, Ordering::Relaxed);
    let report = handler.health().await.metrics.unwrap();
    assert_eq!(report["hubConnected"], false);
    assert_eq!(report["hubFailures"], 1);
    assert_eq!(report["kvRecordsRefused"], 1);
    assert_eq!(report["epochHeld"], 17);
    let local: Value = serde_json::from_slice(
        &handler
            .resolve_project_id(json!({"project_id": "p"}))
            .unwrap(),
    )
    .unwrap();
    assert_eq!(local["result"]["projectId"], "p", "{local}");
    drop(handler);
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test(start_paused = true)]
async fn hub_watch_and_tail_keep_their_stream_on_transient_errors() {
    let health = Arc::new(HealthGauges::default());
    for log in [false, true] {
        let opens = Arc::new(AtomicU64::new(0));
        let count = opens.clone();
        let open = move |cursor| {
            assert_eq!(cursor, log.then_some(15));
            assert_eq!(
                count.fetch_add(1, Ordering::Relaxed),
                0,
                "reopened after transient error"
            );
            async {
                Ok(Subscription::new(
                    None,
                    stream::iter([
                        Err(Failure::code("MissingHeartbeat")),
                        Err(Failure::code("Recreate")),
                        Ok((16, "restored")),
                    ]),
                ))
            }
        };
        let mut feed = if log {
            Feed::log(health.clone(), 15, open)
        } else {
            Feed::kv(health.clone(), open)
        };
        assert_eq!(feed.next().await, "restored");
        assert_eq!(opens.load(Ordering::Relaxed), 1);
        assert!(health.hub.connected.load(Ordering::Relaxed));
        assert_eq!(health.hub.log_tail_live.load(Ordering::Relaxed), log);
        assert_ne!(health.hub.kv_watch_live.load(Ordering::Relaxed), log);
    }
    assert_eq!(health.hub.failures.load(Ordering::Relaxed), 4);
    assert!(!health.hub.log_tail_live.load(Ordering::Relaxed));
}

#[derive(Clone, Default)]
struct Log(std::sync::Arc<std::sync::Mutex<Vec<u8>>>);

struct LogWriter(Log);

impl std::io::Write for LogWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 .0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for Log {
    type Writer = LogWriter;
    fn make_writer(&'a self) -> Self::Writer {
        LogWriter(self.clone())
    }
}

#[tokio::test(start_paused = true)]
async fn hub_subscription_backoff_caps_and_logs_only_changes_and_recovery() {
    let health = Arc::new(HealthGauges::default());
    let output = Log::default();
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_span_events(FmtSpan::NONE)
        .with_writer(output.clone())
        .finish();
    let _guard = tracing::subscriber::set_default(subscriber);
    let start = Instant::now();
    let mut attempts = 0;
    let mut feed = Feed::log(health.clone(), 9, move |cursor| {
        assert_eq!(cursor, Some(9));
        let index = attempts;
        attempts += 1;
        // Check the complete retry schedule against elapsed time since startup.
        assert_eq!(
            start.elapsed().as_secs(),
            [0, 1, 3, 7, 15, 31, 63, 123, 183][index]
        );
        async move {
            match index {
                0..=2 => Err(Failure::code("NoResponders")),
                3..=7 => Err(Failure::code("TimedOut")),
                8 => Ok(Subscription::new(
                    None,
                    stream::iter([Ok((10, "recovered")), Ok((11, "live"))]),
                )),
                _ => unreachable!(),
            }
        }
    });
    assert_eq!(feed.next().await, "recovered");
    assert_eq!(feed.next().await, "live");
    assert_eq!(health.hub.failures.load(Ordering::Relaxed), 8);
    let logged = String::from_utf8(output.0.lock().unwrap().clone()).unwrap();
    assert_eq!(logged.lines().count(), 3, "{logged}");
    assert!(
        logged
            .lines()
            .next()
            .unwrap()
            .contains("code=\"NoResponders\""),
        "{logged}"
    );
    assert!(
        logged.lines().nth(1).unwrap().contains("code=\"TimedOut\""),
        "{logged}"
    );
    assert!(
        logged
            .lines()
            .nth(2)
            .unwrap()
            .contains("hub subscription recovered"),
        "{logged}"
    );
}

#[tokio::test(start_paused = true)]
async fn hub_tail_reopens_from_last_delivery_after_credential_replacement() {
    let health = Arc::new(HealthGauges::default());
    let (signal, changes) = watch::channel(0);
    let mut opens = 0;
    let mut feed = Feed::log(health, 7, move |cursor| {
        opens += 1;
        let changes = changes.clone();
        async move {
            match (opens, cursor) {
                (1, Some(7)) => Ok(Subscription::new(
                    Some(changes),
                    stream::iter([Ok((8, 8))]).chain(stream::pending()),
                )),
                (2, Some(8)) => Ok(Subscription::new(
                    None,
                    stream::iter([Ok((8, 8)), Ok((9, 9))]),
                )),
                _ => panic!("unexpected open {opens} at {cursor:?}"),
            }
        }
    });
    assert_eq!(feed.next().await, 8);
    let mut next = std::pin::pin!(feed.next());
    assert_eq!(poll!(next.as_mut()), Poll::Pending);
    signal.send_replace(1);
    assert_eq!(next.await, 9);
}

#[tokio::test(start_paused = true)]
async fn hub_identity_log_feed_reopens_from_commit_and_delivers_snapshot_prefix_again() {
    let health = Arc::new(HealthGauges::default());
    let (signal, changes) = watch::channel(0);
    let mut opens = 0;
    let mut feed = Feed::log(health, 7, move |cursor| {
        opens += 1;
        let mut changes = changes.clone();
        changes.borrow_and_update();
        async move {
            let (sequence, label) = match (opens, cursor) {
                (1, Some(7)) => (8, "part one"),
                (2, Some(7)) => (8, "part one again"),
                (3, Some(8)) => (9, "next entry"),
                _ => panic!("unexpected open {opens} at {cursor:?}"),
            };
            Ok(Subscription::new(
                Some(changes),
                stream::iter([Ok((sequence, label))]).chain(stream::pending()),
            ))
        }
    })
    .with_durable_cursor();

    assert_eq!(feed.next().await, "part one");
    signal.send_replace(1);
    assert_eq!(feed.next().await, "part one again");
    feed.applied(8);
    signal.send_replace(2);
    assert_eq!(feed.next().await, "next entry");
}

async fn within<F: Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(12), future)
        .await
        .expect("NATS rig timed out")
}

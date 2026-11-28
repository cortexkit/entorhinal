use std::{
    future::Future,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use tokio::time::Instant;

const DEGRADE_AFTER: Duration = Duration::from_secs(60);

/// Tracks explicit connect outcomes from all local account-bus clients. The
/// bus SDK does not expose failures inside its automatic reconnect loop.
pub(super) struct Tracker {
    origin: Instant,
    origin_unix_ms: i64,
    // Zero means no failed attempt; otherwise this is nanoseconds since
    // origin plus one. The grace period uses monotonic time even if the
    // system clock changes.
    first_failure: AtomicU64,
}

impl Default for Tracker {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
            origin_unix_ms: super::unix_millis(),
            first_failure: AtomicU64::new(0),
        }
    }
}

pub(super) struct Outage {
    pub(super) since_ms: i64,
    pub(super) degraded: bool,
}

impl Tracker {
    pub(super) async fn connect<T, E>(
        &self,
        attempt: impl Future<Output = Result<T, E>>,
    ) -> Result<T, E> {
        let result = attempt.await;
        self.record(result.is_ok());
        result
    }

    fn record(&self, connected: bool) {
        if connected {
            self.first_failure.store(0, Ordering::Relaxed);
        } else {
            let elapsed = u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let _ = self.first_failure.compare_exchange(
                0,
                elapsed.saturating_add(1),
                Ordering::Relaxed,
                Ordering::Relaxed,
            );
        }
    }

    pub(super) fn outage(&self) -> Option<Outage> {
        let nanos = self.first_failure.load(Ordering::Relaxed).checked_sub(1)?;
        let since = Duration::from_nanos(nanos);
        Some(Outage {
            since_ms: self.origin_unix_ms + since.as_millis() as i64,
            degraded: self.origin.elapsed().saturating_sub(since) >= DEGRADE_AFTER,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::future::ready;

    use subc_client_rs::{HealthStatus, ModuleHandler};
    use tokio::time::advance;

    use super::*;
    use crate::ProjectsHandler;

    #[tokio::test(start_paused = true)]
    async fn local_bus_health_waits_60s_and_recovers_on_the_next_success() {
        let handler = ProjectsHandler::with_runtime("local-bus-health".into(), || 7);
        let health = &handler.health;
        let bus = &health.local_bus;
        let metric = "busUnreachableSinceMs";
        health.store_ready.store(true, Ordering::Relaxed);

        let (connection, _peer) = tokio::sync::oneshot::channel::<()>();
        let connection = bus.connect(ready(Ok::<_, ()>(connection))).await.unwrap();
        drop(connection);
        advance(Duration::from_secs(61)).await;
        let report = handler.health().await;
        assert_eq!(report.status, HealthStatus::Ok, "deliberate close");
        assert!(report.metrics.unwrap().get(metric).is_none());

        let failure = Err::<(), _>("connect refused");
        assert_eq!(bus.connect(ready(failure)).await, failure);
        let since = bus.origin_unix_ms + 61_000;
        advance(Duration::from_secs(59)).await;
        assert_eq!(bus.connect(ready(failure)).await, failure);
        let report = handler.health().await;
        assert_eq!(report.status, HealthStatus::Ok, "59s of failed connects");
        assert_eq!(report.detail, None);
        assert_eq!(report.metrics.unwrap()[metric], since);

        advance(Duration::from_secs(1)).await;
        let report = handler.health().await;
        assert_eq!(
            report.status,
            HealthStatus::Degraded,
            "60s of failed connects"
        );
        assert_eq!(report.detail.as_deref(), Some("bus_unreachable"));
        assert_eq!(report.metrics.unwrap()[metric], since);

        health.store_opening.store(true, Ordering::Relaxed);
        let report = handler.health().await;
        assert_eq!(report.status, HealthStatus::Degraded);
        assert_eq!(
            report.detail.as_deref(),
            Some("projects storage lease is held by an exiting predecessor; retrying")
        );
        assert_eq!(report.metrics.unwrap()[metric], since);
        health.store_opening.store(false, Ordering::Relaxed);
        health.store_ready.store(false, Ordering::Relaxed);
        let report = handler.health().await;
        assert_eq!(report.status, HealthStatus::Failing);
        assert_eq!(
            report.detail.as_deref(),
            Some("projects storage is not ready")
        );
        assert_eq!(report.metrics.unwrap()[metric], since);
        health.store_ready.store(true, Ordering::Relaxed);

        bus.connect(ready(Ok::<_, ()>(()))).await.unwrap();
        let report = handler.health().await;
        assert_eq!(report.status, HealthStatus::Ok, "one successful connect");
        assert_eq!(report.detail, None);
        assert!(report.metrics.unwrap().get(metric).is_none());

        for gauge in [&health.log_state, &health.move_errors] {
            gauge.store(1, Ordering::Relaxed);
            let report = handler.health().await;
            assert_eq!(report.status, HealthStatus::Degraded);
            assert_eq!(report.detail, None);
            assert!(report.metrics.unwrap().get(metric).is_none());
            gauge.store(0, Ordering::Relaxed);
        }

        assert_eq!(bus.connect(ready(failure)).await, failure);
        let report = handler.health().await;
        assert_eq!(
            report.status,
            HealthStatus::Ok,
            "a new streak starts at zero"
        );
        assert_eq!(report.metrics.unwrap()[metric], since + 60_000);
    }
}

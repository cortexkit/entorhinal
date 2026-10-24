//! Durable, machine-local cleanup after the move has crossed its final boundary.

use std::sync::atomic::Ordering;
use std::time::Duration;

use entorhinal_core::RegistryStore;
use serde_json::json;

use crate::door_client::{CutRequest, DoorError};
use crate::move_driver::{backoff, Worker};
use crate::HandlerError;

const SLOW_RETRY: Duration = Duration::from_secs(6 * 60 * 60);

pub(crate) async fn run(worker: Worker) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        interval.tick().await;
        if worker.store.upgrade().is_none() {
            break;
        }
        let _ = tick(&worker).await;
    }
}

pub(crate) async fn tick(worker: &Worker) -> Result<(), HandlerError> {
    let rows = worker
        .sql(|store| {
            if store.is_foreign() {
                Ok(Vec::new())
            } else {
                store.list_agent_moves()
            }
        })
        .await?;
    worker.health.cut_releases_pending.store(
        rows.iter()
            .filter(|row| row.plan["_cut_release"]["state"] == "pending")
            .count() as u64,
        Ordering::Relaxed,
    );
    for row in rows {
        let mut release = row.plan["_cut_release"].clone();
        if release["state"] != "pending"
            || release["retry_at_ms"].as_i64().unwrap_or(0) > (worker.clock)()
        {
            continue;
        }
        // Persist the next deadline before sending: a crash or lost reply must
        // not turn an unsupported plane into a rapid retry loop after restart.
        // Use the slow deadline until a reply tells us a shorter one is safe.
        let attempts = release["attempts"].as_i64().unwrap_or(0).saturating_add(1);
        release["attempts"] = json!(attempts);
        release["retry_at_ms"] =
            json!((worker.clock)().saturating_add(SLOW_RETRY.as_millis() as i64));
        save(worker, &row.cut_id, release.clone()).await?;
        let result = tokio::time::timeout(
            Duration::from_secs(30),
            worker.door.release(&CutRequest {
                cut_id: row.cut_id.clone(),
            }),
        )
        .await;
        let mut stopped = None;
        match result {
            Ok(Ok(reply)) => {
                release = json!({"state":"released","outcome":reply.outcome,
                    "reservationDropped":reply.reservation_dropped});
            }
            Ok(Err(DoorError::Refused { code, .. }))
                if matches!(
                    code.as_str(),
                    "invalid_request" | "caller_forbidden" | "cut_release_unknown"
                ) =>
            {
                release = json!({"state":"stopped","code":code});
                stopped = Some(code);
            }
            Ok(Err(DoorError::Refused { code, .. })) if code == "plane_version_unsupported" => {}
            _ => {
                release["retry_at_ms"] =
                    json!((worker.clock)().saturating_add(backoff(attempts).as_millis() as i64));
            }
        }
        save(worker, &row.cut_id, release.clone()).await?;
        if release["state"] == "released" {
            worker
                .health
                .cut_releases_released
                .fetch_add(1, Ordering::Relaxed);
        } else if let Some(code) = stopped {
            worker
                .health
                .cut_releases_stopped
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!(cut_id = row.cut_id, code, "cut release stopped");
        }
    }
    worker.gauges().await
}

async fn save(worker: &Worker, cut: &str, release: serde_json::Value) -> Result<(), HandlerError> {
    let cut = cut.to_owned();
    worker
        .sql(move |store: &RegistryStore| store.set_move_cut_release(&cut, &release))
        .await
}

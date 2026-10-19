//! Call-bound Cingulate transport. No card operation uses an unscoped route.

use std::{sync::Arc, time::Duration};

use async_trait::async_trait;
use serde_json::{json, Value};
use subc_client_rs::{CallError, CallOptions, CloseRouteOptions, RouteHandle, SubcConsumer};
use subc_protocol::{
    scope::{ScopeSelector, ScopeStamp},
    BindIdentity, RouteTarget,
};
use tokio::time::{timeout_at, Instant};

use crate::log_client::SubcConnector;

pub(super) const CALL_BUDGET: Duration = Duration::from_secs(115);
pub(super) const AWAIT_MS: i64 = 100_000;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(super) struct Owner {
    pub scope_ref: String,
    pub epoch: i64,
}
pub(super) fn request_bytes(method: &str, params: &Value) -> Result<Vec<u8>, ConsentError> {
    serde_json::to_vec(&json!({"method":method,"params":params}))
        .map_err(|_| ConsentError::InvalidReply)
}

impl Owner {
    pub fn from_stamp(stamp: &ScopeStamp) -> Result<Self, ConsentError> {
        Ok(Self {
            scope_ref: stamp.scope_ref.clone(),
            epoch: i64::try_from(stamp.scope_epoch).map_err(|_| ConsentError::InvalidReply)?,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ConsentError {
    Refused(String),
    Ambiguous,
    InvalidReply,
}

#[async_trait]
pub(super) trait ConsentRoute: Send + Sync {
    async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Value, ConsentError>;
    async fn close(&self);
}

#[async_trait]
pub(super) trait ConsentConnector: Send + Sync {
    async fn open(
        &self,
        stamp: &ScopeStamp,
        deadline: Instant,
    ) -> Result<Box<dyn ConsentRoute>, ConsentError>;
}

pub(super) struct SubcCingulate {
    pub connector: Arc<SubcConnector>,
}

fn call_error(error: CallError) -> ConsentError {
    if let Some(refusal) = error.route_open_refusal() {
        return ConsentError::Refused(refusal.code.clone());
    }
    match error {
        CallError::Module(body) if body.code != "cancelled" => ConsentError::Refused(body.code),
        _ => ConsentError::Ambiguous,
    }
}

#[async_trait]
impl ConsentConnector for SubcCingulate {
    async fn open(
        &self,
        stamp: &ScopeStamp,
        deadline: Instant,
    ) -> Result<Box<dyn ConsentRoute>, ConsentError> {
        static NEXT_ROUTE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
        let consumer = timeout_at(deadline, self.connector.consumer())
            .await
            .map_err(|_| ConsentError::Ambiguous)?
            .map_err(|_| ConsentError::Refused("consent_unavailable".into()))?;
        let cwd = crate::current_directory::read_current_directory(std::env::current_dir)
            .map_err(|_| ConsentError::Refused("consent_unavailable".into()))?;
        // The SDK caches routes by identity. A unique activity prevents one held
        // call's close from tearing down another call under the same owner.
        let activity = format!(
            "agent consent {}",
            NEXT_ROUTE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        );
        let identity = BindIdentity::new(&cwd, "ck-entorhinal", activity);
        let selector = ScopeSelector::new(
            stamp.owner.clone(),
            stamp.scope_ref.clone(),
            Some(stamp.scope_epoch),
        );
        let handle = timeout_at(
            deadline,
            consumer.open_route_scoped(
                RouteTarget::ManagementSurface {
                    module_id: "cingulate".into(),
                },
                identity,
                selector,
                CallOptions {
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    ..Default::default()
                },
            ),
        )
        .await
        .map_err(|_| ConsentError::Ambiguous)?
        .map_err(call_error)?;
        Ok(Box::new(SubcConsentRoute {
            consumer,
            handle,
            closed: std::sync::atomic::AtomicBool::new(false),
        }))
    }
}

struct SubcConsentRoute {
    consumer: Arc<SubcConsumer>,
    handle: RouteHandle,
    closed: std::sync::atomic::AtomicBool,
}

#[async_trait]
impl ConsentRoute for SubcConsentRoute {
    async fn call(
        &self,
        method: &str,
        params: Value,
        deadline: Instant,
    ) -> Result<Value, ConsentError> {
        let bytes = request_bytes(method, &params)?;
        let reply = self
            .consumer
            .request(
                &self.handle,
                bytes,
                CallOptions {
                    timeout: deadline.saturating_duration_since(Instant::now()),
                    ..Default::default()
                },
            )
            .await
            .map_err(call_error)?;
        serde_json::from_slice::<Value>(&reply)
            .ok()
            .and_then(|envelope| envelope.get("result").cloned())
            .ok_or(ConsentError::InvalidReply)
    }

    async fn close(&self) {
        let _ = self
            .consumer
            .close_handle(&self.handle, CloseRouteOptions::default())
            .await;
        self.closed
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }
}

impl Drop for SubcConsentRoute {
    fn drop(&mut self) {
        if self.closed.load(std::sync::atomic::Ordering::Relaxed) {
            return;
        }
        // Cancellation of the held tool call must also release its carrier.
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            let consumer = self.consumer.clone();
            let handle = self.handle;
            runtime.spawn(async move {
                let _ = consumer
                    .close_handle(&handle, CloseRouteOptions::default())
                    .await;
            });
        }
    }
}

pub(super) async fn bounded_call(
    route: &dyn ConsentRoute,
    method: &str,
    params: Value,
    deadline: Instant,
) -> Result<Value, ConsentError> {
    if Instant::now() >= deadline {
        return Err(ConsentError::Ambiguous);
    }
    timeout_at(deadline, route.call(method, params, deadline))
        .await
        .map_err(|_| ConsentError::Ambiguous)?
}

#[cfg(test)]
#[path = "consent_wire_tests.rs"]
mod tests;

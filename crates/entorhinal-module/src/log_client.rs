//! Shared transport for calling internal services through the Subconscious daemon.
//! Connections open on first use.

use std::{path::PathBuf, sync::Arc};

use async_trait::async_trait;
use serde_json::{json, Value};
use subc_client_rs::{CallError, CallOptions, ConsumerOptions, SubcConsumer};
use subc_protocol::{BindIdentity, RouteTarget};

#[derive(Clone, Debug)]
pub(crate) enum TransportError {
    /// `message` is the peer's human-readable reason from the wire error body.
    /// It is kept for operator logs and move diagnostics only; no decision
    /// reads it, so callers that build refusals locally may leave it empty.
    Refusal {
        code: String,
        detail: Option<Value>,
        message: Option<String>,
    },
    Unavailable,
    NoReply,
}

#[async_trait]
pub(crate) trait LogTransport: Send + Sync {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError>;
}

#[async_trait]
pub(crate) trait LogConnector: Send + Sync {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError>;
}

pub(crate) fn encode_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = String::with_capacity(bytes.len() * 2);
    for &byte in bytes {
        text.push(HEX[(byte >> 4) as usize] as char);
        text.push(HEX[(byte & 15) as usize] as char);
    }
    text
}

pub(crate) fn decode_hex(text: &str) -> Result<Vec<u8>, &'static str> {
    fn nibble(byte: u8) -> Result<u8, &'static str> {
        match byte {
            b'0'..=b'9' => Ok(byte - b'0'),
            b'a'..=b'f' => Ok(byte - b'a' + 10),
            _ => Err("expected lowercase hex"),
        }
    }
    if !text.len().is_multiple_of(2) {
        return Err("hex has odd length");
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|pair| Ok(nibble(pair[0])? * 16 + nibble(pair[1])?))
        .collect()
}

/// SUBC caches the consumer connection and routes, but opens neither at startup.
pub(crate) struct SubcConnector {
    connection_file: Option<PathBuf>,
    consumer: tokio::sync::Mutex<Option<Arc<SubcTransport>>>,
    read_cwd: super::current_directory::CwdReader,
}

impl Default for SubcConnector {
    fn default() -> Self {
        Self {
            connection_file: None,
            consumer: tokio::sync::Mutex::new(None),
            read_cwd: std::env::current_dir,
        }
    }
}

impl SubcConnector {
    /// Use the supervisor's daemon, not discovery of a different live daemon.
    /// Parsing argv is local only; the connection file is not read until connect.
    pub(crate) fn from_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Self {
        Self {
            connection_file: subc_connection_file(args),
            ..Self::default()
        }
    }
}

/// The daemon connection file the supervisor passed as `--subc <path>` or
/// `--subc=<path>`. Without one, a consumer falls back to the client
/// library's default daemon connection file. A supervised entorhinal always
/// receives `--subc`; the subc client library refuses to serve without a
/// valid one.
pub(crate) fn subc_connection_file(
    args: impl IntoIterator<Item = std::ffi::OsString>,
) -> Option<PathBuf> {
    let mut args = args.into_iter();
    while let Some(arg) = args.next() {
        let path = if arg == "--subc" {
            args.next().map(PathBuf::from)
        } else {
            arg.to_str()
                .and_then(|s| s.strip_prefix("--subc="))
                .map(PathBuf::from)
        };
        if path.is_some() {
            return path;
        }
    }
    None
}

impl SubcConnector {
    async fn transport(&self) -> Result<Arc<SubcTransport>, TransportError> {
        super::current_directory::read_current_directory(self.read_cwd).map_err(|error| {
            TransportError::Refusal {
                code: error.code.into(),
                detail: Some(json!({"message": error.message})),
                message: None,
            }
        })?;
        let mut slot = self.consumer.lock().await;
        if let Some(transport) = slot.as_ref() {
            return Ok(transport.clone());
        }
        let consumer = match &self.connection_file {
            Some(path) => SubcConsumer::connect(path, ConsumerOptions::default()).await,
            None => SubcConsumer::connect_default(ConsumerOptions::default()).await,
        }
        .map_err(|_| TransportError::Unavailable)?;
        let transport = Arc::new(SubcTransport {
            consumer: Arc::new(consumer),
        });
        *slot = Some(transport.clone());
        Ok(transport)
    }

    pub(crate) async fn consumer(&self) -> Result<Arc<SubcConsumer>, TransportError> {
        Ok(self.transport().await?.consumer.clone())
    }

    pub(crate) async fn open_internal_service(
        &self,
        target: RouteTarget,
        activity: &'static str,
    ) -> Result<Arc<dyn LogTransport>, TransportError> {
        let transport = self.transport().await?;
        let identity = BindIdentity::new("ck-entorhinal", activity);
        // With the default CallOptions the SDK fills the omitted consumer identity
        // from SUBC_MODULE_ID and the supervisor's cached launch secret. The daemon
        // therefore authenticates reserved:entorhinal, the principal participants
        // admit, without an operator identity or a session/flow scope.
        transport
            .consumer
            .open_route(target.clone(), identity.clone(), CallOptions::default())
            .await
            .map_err(|error| {
                warn_route_call_error(&target, "route.open", &error);
                service_call_error(error)
            })?;
        Ok(Arc::new(ServiceTransport {
            consumer: transport.consumer.clone(),
            target,
            identity,
        }))
    }
}

struct SubcTransport {
    consumer: Arc<SubcConsumer>,
}

struct ServiceTransport {
    consumer: Arc<SubcConsumer>,
    target: RouteTarget,
    identity: BindIdentity,
}

pub(crate) fn warn_call_error(module: &str, service: &str, method: &str, error: &CallError) {
    // The SDK wraps route-open refusals in NotSent, but they are answers, not
    // transport failures. Leave their logging to the refusal handlers.
    if error.route_open_refusal().is_some() {
        return;
    }
    let variant = match error {
        CallError::Module(_) => return,
        CallError::NotSent(_) => "NotSent",
        CallError::DaemonTooOld { .. } => "DaemonTooOld",
        CallError::OutcomeUnknown(_) => "OutcomeUnknown",
        CallError::SubscriptionBackpressure(_) => "SubscriptionBackpressure",
        CallError::StaleRouteHandle(_) => "StaleRouteHandle",
        CallError::CapabilityUnprovided { .. } => "CapabilityUnprovided",
        CallError::CapabilityAmbiguous { .. } => "CapabilityAmbiguous",
        CallError::InvalidCapabilityIdentifier { .. } => "InvalidCapabilityIdentifier",
    };
    tracing::warn!(module, service, method, variant, error = %error, "outbound subc call failed");
}

fn warn_route_call_error(target: &RouteTarget, method: &str, error: &CallError) {
    let (module, service) = match target {
        RouteTarget::InternalService {
            module_id,
            service_id,
        } => (module_id.as_str(), service_id.as_str()),
        RouteTarget::ManagementSurface { module_id } => (module_id.as_str(), "management_surface"),
        RouteTarget::ToolProvider { module_id } => (module_id.as_str(), "tool_provider"),
    };
    warn_call_error(module, service, method, error);
}

pub(crate) fn service_call_error(error: CallError) -> TransportError {
    if let Some(body) = error.route_open_refusal() {
        return TransportError::Refusal {
            code: body.code.clone(),
            detail: body.detail.clone(),
            message: Some(body.message.clone()),
        };
    }
    match error {
        CallError::Module(body) => TransportError::Refusal {
            code: body.code,
            detail: body.detail,
            message: Some(body.message),
        },
        CallError::OutcomeUnknown(_) => TransportError::NoReply,
        _ => TransportError::Unavailable,
    }
}

#[async_trait]
impl LogTransport for ServiceTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        let body = serde_json::to_vec(&json!({"method": method, "params": params}))
            .map_err(|_| TransportError::Unavailable)?;
        self.consumer
            .call(
                self.target.clone(),
                self.identity.clone(),
                body,
                CallOptions::default(),
            )
            .await
            .map_err(|error| {
                warn_route_call_error(&self.target, method, &error);
                service_call_error(error)
            })
    }
}

#[cfg(test)]
#[path = "outbound_call_tests.rs"]
mod outbound_call_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subc_args_configure_the_connector_without_opening_it() {
        for args in [
            ["--subc", "target/nonexistent-daemon.json"].as_slice(),
            ["--subc=target/nonexistent-daemon.json"].as_slice(),
        ] {
            let connector = SubcConnector::from_args(args.iter().map(std::ffi::OsString::from));
            assert_eq!(
                connector.connection_file,
                Some(PathBuf::from("target/nonexistent-daemon.json"))
            );
            assert!(connector.consumer.try_lock().unwrap().is_none());
        }
    }

    #[test]
    fn hex_codec_rejects_noncanonical_input() {
        assert_eq!(encode_hex(&[0, 0xa1, 0xff]), "00a1ff");
        assert_eq!(decode_hex("00a1ff").unwrap(), [0, 0xa1, 0xff]);
        for bad in ["AA", "a", "gg"] {
            assert!(decode_hex(bad).is_err(), "{bad}");
        }
    }
    #[tokio::test]
    async fn startup_and_every_local_read_work_without_engram() {
        use super::super::*;
        let handler = ProjectsHandler::with_runtime("offline".into(), unix_millis);
        let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/log-client-offline")
            .join(incarnation::new_incarnation().unwrap());
        std::fs::create_dir_all(&root).unwrap();
        let descriptor = StorageDescriptor {
            module_id: MODULE_ID.into(),
            storage_namespace: "default".into(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: root.join("store.db").to_string_lossy().into_owned(),
            },
        };
        handler
            .on_hello_ack(&ModuleHelloAckBody {
                negotiated_ver: PROTOCOL_VERSION,
                subc_ops: vec![],
                subc_capabilities: vec![],
                storage: Some(serde_json::to_value(descriptor).unwrap()),
                machine_id: None,
            })
            .await;
        assert_eq!(handler.health().await.status, HealthStatus::Ok);
        let requests = [
            ("agent.preview_repair_workspaces", json!({})),
            ("resolve", json!({"canonicalRoot":root})),
            ("resolve_project_id", json!({"projectId":"missing"})),
            (
                "resolve_remote",
                json!({"owner":"missing","repo":"missing"}),
            ),
            ("enumerate", json!({})),
            ("journal_tail", json!({"afterSeq":0})),
            ("trust", json!({"canonicalRoot":root})),
            ("verify", json!({})),
            ("identity_log.status", json!({})),
            (
                "resolve_root_key",
                json!({"projectId":"missing","kind":"remote","rootKey":"a/b"}),
            ),
            ("preview_attach_root", json!({"path":root})),
            (
                "agent.resolve",
                json!({"agent_id":"agent_0000000000000000"}),
            ),
            ("agent.resolve_name", json!({"name":"Missing"})),
            ("agent.list", json!({})),
            ("agent.peer_roster", json!({"workspace_id":"missing"})),
            (
                "agent.avatar_read",
                json!({"agentIds":["agent_0000000000000000"]}),
            ),
            (
                "agent.github_identity",
                json!({"agent_id":"agent_0000000000000000"}),
            ),
            ("agent.snapshot", json!({})),
            ("agent.fleet_identity", json!({})),
            (
                "agent.changes",
                json!({"incarnation":"offline","cursor":0,"wait":false}),
            ),
        ];
        for (method, params) in requests {
            let body = serde_json::to_vec(&json!({"method":method,"params":params})).unwrap();
            let outcome = handler.handle_request_wait(&body, (9, 1)).await;
            let expected = match method {
                "resolve_root_key" | "preview_attach_root" => Some("identity_log_disabled"),
                "agent.peer_roster" => Some("registry_not_activated"),
                "agent.github_identity" => Some("unknown_agent"),
                _ => None,
            };
            match (outcome, expected) {
                (HandlerOutcome::Response(bytes), None) => {
                    assert!(
                        serde_json::from_slice::<Value>(&bytes).unwrap()["result"].is_object(),
                        "{method}"
                    );
                }
                (HandlerOutcome::Error { code, .. }, Some(expected)) => {
                    assert_eq!(code, expected, "{method}")
                }
                (other, _) => panic!("{method}: {other:?}"),
            }
        }
        assert!(manifest().capabilities.unwrap().requires.is_empty());
        // Every background task opened by the store must stop before its folder
        // is deleted; Windows refuses to delete a file a task still holds open.
        crate::tests::stop_store_tasks(&handler).await;
        drop(handler);
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(test)]
mod cwd_tests {
    use super::*;

    #[tokio::test]
    async fn cwd_unreadable_is_terminal_and_sends_no_frame() {
        let connector = SubcConnector {
            connection_file: Some(std::env::temp_dir().join("absent-cwd-refusal.json")),
            read_cwd: || Err(std::io::Error::other("injected cwd failure")),
            ..SubcConnector::default()
        };
        let error = match connector.consumer().await {
            Ok(_) => panic!("cwd failure must refuse"),
            Err(error) => error,
        };
        assert!(matches!(error, TransportError::Refusal { code, .. } if code == "cwd_unreadable"));
        assert!(
            connector.consumer.lock().await.is_none(),
            "no consumer or route may be opened"
        );
    }

    #[test]
    fn module_refusal_keeps_the_wire_message_beside_code_and_detail() {
        let mut body = subc_protocol::ErrorBody::new("invalid_request", "a cut needs items");
        body.detail = Some(json!({"field":"items"}));
        match service_call_error(CallError::Module(body)) {
            TransportError::Refusal {
                code,
                detail,
                message,
            } => {
                assert_eq!(code, "invalid_request");
                assert_eq!(detail, Some(json!({"field":"items"})));
                assert_eq!(message.as_deref(), Some("a cut needs items"));
            }
            other => panic!("a module error is a refusal, got {other:?}"),
        }
    }
}

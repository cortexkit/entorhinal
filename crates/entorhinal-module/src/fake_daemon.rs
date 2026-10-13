//! Scripted daemon discovery and service admission for tests only.

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use serde_json::{json, Value};
use subc_client_rs::{CatalogList, SupervisorEntry, SupervisorListResponse};
use subc_protocol::{manifest::ProviderRole, RouteTarget};

use super::log_client::{LogTransport, TransportError};
use super::participant_discovery::{participant_target, ParticipantDiscovery};

#[derive(Clone)]
pub(crate) struct DaemonSnapshot {
    pub(crate) supervisor: SupervisorListResponse,
    pub(crate) catalog: CatalogList,
}

impl DaemonSnapshot {
    pub(crate) fn new(generation: u64, configured: Vec<Value>, catalog: Vec<Value>) -> Self {
        Self {
            supervisor: SupervisorListResponse {
                generation,
                modules: configured
                    .into_iter()
                    .map(|value| serde_json::from_value::<SupervisorEntry>(value).unwrap())
                    .collect(),
            },
            catalog: serde_json::from_value(json!({
                "generation": generation, "modules": catalog, "subc_ops": [],
            }))
            .unwrap(),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum DaemonCall {
    SupervisorList,
    CatalogList,
    Open {
        target: RouteTarget,
        principal: String,
    },
    Request {
        target: RouteTarget,
        principal: String,
        body: Value,
    },
}

#[derive(Clone)]
pub(crate) struct FakeDaemon {
    snapshot: Arc<Mutex<DaemonSnapshot>>,
    calls: Arc<Mutex<Vec<DaemonCall>>>,
}

impl FakeDaemon {
    pub(crate) fn new(snapshot: DaemonSnapshot) -> Self {
        Self {
            snapshot: Arc::new(Mutex::new(snapshot)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub(crate) fn restart(&self, snapshot: DaemonSnapshot) {
        *self.snapshot.lock().unwrap() = snapshot;
    }

    pub(crate) fn calls(&self) -> Vec<DaemonCall> {
        self.calls.lock().unwrap().clone()
    }

    fn admit(&self, target: &RouteTarget, principal: &str) -> Result<(), TransportError> {
        self.calls.lock().unwrap().push(DaemonCall::Open {
            target: target.clone(),
            principal: principal.into(),
        });
        let RouteTarget::InternalService {
            module_id,
            service_id,
        } = target
        else {
            return Err(TransportError::Refusal {
                code: "invalid_target".into(),
                detail: None,
            });
        };
        let snapshot = self.snapshot.lock().unwrap();
        let entry = snapshot
            .catalog
            .modules
            .iter()
            .find(|entry| &entry.module_id == module_id)
            .ok_or_else(|| TransportError::Refusal {
                code: "module_not_found".into(),
                detail: None,
            })?;
        if !entry.roles.iter().any(|role| {
            matches!(role,
                ProviderRole::InternalService { service_id: declared, .. } if declared == service_id
            )
        }) {
            return Err(TransportError::Refusal {
                code: "service_not_provided".into(),
                detail: Some(json!({"module_id": module_id, "service_id": service_id})),
            });
        }
        if !entry.ready {
            return Err(TransportError::Refusal {
                code: "module_warming".into(),
                detail: serde_json::to_value(&entry.not_ready).ok(),
            });
        }
        if principal != "reserved:entorhinal" {
            return Err(TransportError::Refusal {
                code: "forbidden".into(),
                detail: None,
            });
        }
        Ok(())
    }

    fn request(&self, target: RouteTarget, principal: String, body: Value) -> Vec<u8> {
        self.calls.lock().unwrap().push(DaemonCall::Request {
            target,
            principal,
            body,
        });
        serde_json::to_vec(&json!({"result": {}})).unwrap()
    }
}

#[async_trait]
impl ParticipantDiscovery for FakeDaemon {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        self.calls.lock().unwrap().push(DaemonCall::SupervisorList);
        Ok(self.snapshot.lock().unwrap().supervisor.clone())
    }

    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        self.calls.lock().unwrap().push(DaemonCall::CatalogList);
        Ok(self.snapshot.lock().unwrap().catalog.clone())
    }

    async fn open_participant(
        &self,
        module_id: &str,
    ) -> Result<Arc<dyn LogTransport>, TransportError> {
        let target = participant_target(module_id);
        self.admit(&target, "reserved:entorhinal")?;
        Ok(Arc::new(FakeTransport {
            daemon: self.clone(),
            target,
        }))
    }
}

struct FakeTransport {
    daemon: FakeDaemon,
    target: RouteTarget,
}

#[async_trait]
impl LogTransport for FakeTransport {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        Ok(self.daemon.request(
            self.target.clone(),
            "reserved:entorhinal".into(),
            json!({"method": method, "params": params}),
        ))
    }
}

/// A loopback peer for exercising the real consumer adapter and its wire identity.
pub(crate) struct WireDaemon {
    task: tokio::task::JoinHandle<()>,
}

impl WireDaemon {
    pub(crate) async fn start(daemon: FakeDaemon, connection_file: &std::path::Path) -> Self {
        use subc_transport::{
            generate_daemon_id, generate_key, write_atomic, ConnectionInfo, Endpoint,
            SCHEMA_VERSION,
        };
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let info = ConnectionInfo {
            schema: SCHEMA_VERSION,
            wire_version: None,
            endpoints: vec![Endpoint {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
            }],
            key: generate_key().unwrap(),
            daemon_id: generate_daemon_id().unwrap(),
            pid: std::process::id(),
            daemon_ver: "participant-test-double".into(),
        };
        write_atomic(connection_file, &info).unwrap();
        let task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            serve_wire(daemon, info, stream).await;
        });
        Self { task }
    }
}

impl Drop for WireDaemon {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn serve_wire(
    daemon: FakeDaemon,
    info: subc_transport::ConnectionInfo,
    mut stream: tokio::net::TcpStream,
) {
    use std::collections::BTreeMap;
    use subc_protocol::{Flags, Frame, FrameType, Priority};
    use subc_transport::{authenticate_server, read_frame, write_frame};
    use tokio::io::AsyncWriteExt;

    authenticate_server(
        &mut stream,
        &info.key,
        &info.daemon_id,
        &info.daemon_ver,
        std::time::Duration::from_secs(2),
    )
    .await
    .unwrap();
    let mut routes = BTreeMap::new();
    while let Ok(Some(frame)) = read_frame(&mut stream).await {
        let (ty, body) = if frame.header.ty == FrameType::Ping {
            (FrameType::Pong, vec![])
        } else if frame.header.ty != FrameType::Request {
            continue;
        } else if frame.header.channel == 0 {
            let request: Value = serde_json::from_slice(&frame.body).unwrap();
            let result = match request["op"].as_str().unwrap() {
                "supervisor.list" => {
                    let list = daemon.supervisor_list().await.unwrap();
                    Ok(
                        json!({"op": "supervisor.list", "generation": list.generation, "modules": list.modules}),
                    )
                }
                "catalog.list" => {
                    let list = daemon.catalog_list().await.unwrap();
                    Ok(
                        json!({"op": "catalog.list", "generation": list.generation, "modules": list.modules, "subc_ops": []}),
                    )
                }
                "route.open" => {
                    let target: RouteTarget =
                        serde_json::from_value(request["target"].clone()).unwrap();
                    // The fake authenticates the launch identity just as the daemon
                    // does; a missing or substituted secret is never a module caller.
                    let principal = if request["consumer_identity"]
                        == json!({
                            "module_id": "entorhinal", "launch_nonce": "fake-launch-secret",
                        }) {
                        "reserved:entorhinal"
                    } else {
                        "anonymous"
                    };
                    assert!(
                        request.get("scope").is_none_or(Value::is_null),
                        "participant route must be unscoped"
                    );
                    daemon.admit(&target, principal).map(|()| {
                        let channel = 41 + u16::try_from(routes.len()).unwrap();
                        routes.insert(channel, (target, principal.to_string()));
                        json!({"op": "route.open", "route_channel": channel, "route_epoch": 1})
                    })
                }
                "server.describe" => Ok(json!({
                    "op": "server.describe", "protocol_ver": subc_protocol::PROTOCOL_VERSION,
                    "subc_ops": [], "capabilities": [], "connected_clients": 1,
                })),
                op => panic!("unexpected consumer op: {op}"),
            };
            match result {
                Ok(body) => (FrameType::Response, serde_json::to_vec(&body).unwrap()),
                Err(TransportError::Refusal { code, detail }) => (
                    FrameType::Error,
                    serde_json::to_vec(
                        &json!({"code": code, "message": "fake route refusal", "detail": detail}),
                    )
                    .unwrap(),
                ),
                Err(error) => panic!("unexpected fake transport error: {error:?}"),
            }
        } else {
            let (target, principal) = routes.get(&frame.header.channel).unwrap().clone();
            let body = serde_json::from_slice(&frame.body).unwrap();
            (FrameType::Response, daemon.request(target, principal, body))
        };
        let reply = Frame::build_with_version(
            frame.header.ver,
            ty,
            Flags::new(false, Priority::Interactive, false),
            frame.header.channel,
            frame.header.epoch,
            frame.header.corr,
            body,
        )
        .unwrap();
        if write_frame(&mut stream, &reply).await.is_err() || stream.flush().await.is_err() {
            break;
        }
    }
}

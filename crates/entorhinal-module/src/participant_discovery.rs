//! Daemon reads and route availability for agent-data participants.
//!
//! Missing, unready or unroutable required modules refuse a move. Finding them
//! available does not admit it: the move driver must also check ref kinds,
//! liveness and busy state before recording the move.

use std::collections::BTreeMap;
use std::sync::Arc;

use async_trait::async_trait;
use serde_json::{json, Value};
use subc_client_rs::{CatalogList, SupervisorListResponse};
use subc_protocol::{manifest::ProviderRole, RouteTarget};

use super::log_client::{service_call_error, LogTransport, SubcConnector, TransportError};

// The discovery capability, service id, provider role and ten method names are
// copied from agent-data/v1 because cortexkit-role-agent-data is not yet published.
pub(crate) const CAPABILITY: &str = "agent-data/v1";
pub(crate) const SERVICE_ID: &str = "agent-data";
pub(crate) const OPERATIONS: [&str; 10] = [
    "agent_data.describe",
    "agent_data.busy",
    "agent_data.prepare",
    "agent_data.export",
    "agent_data.diverged",
    "agent_data.import",
    "agent_data.activate",
    "agent_data.release",
    "agent_data.abort",
    "agent_data.status",
];

pub(crate) fn provider_role() -> ProviderRole {
    serde_json::from_value(json!({
        "role": "internal_service",
        "service_id": SERVICE_ID,
        "transport": "bulk",
        "agent_facing": false,
        "operations": OPERATIONS,
    }))
    .expect("the copied agent-data provider role is valid")
}

pub(crate) fn participant_target(module_id: &str) -> RouteTarget {
    RouteTarget::InternalService {
        module_id: module_id.into(),
        service_id: SERVICE_ID.into(),
    }
}

#[async_trait]
pub(crate) trait ParticipantDiscovery: Send + Sync {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError>;
    async fn catalog_list(&self) -> Result<CatalogList, TransportError>;
    async fn open_participant(
        &self,
        module_id: &str,
    ) -> Result<Arc<dyn LogTransport>, TransportError>;
}

/// Shares the identity-log connector's lazy, authenticated consumer connection.
/// A failed initial connection is not retained, so later discovery can retry.
pub(crate) struct SubcParticipantDiscovery {
    connector: Arc<SubcConnector>,
}

impl SubcParticipantDiscovery {
    pub(crate) fn new(connector: Arc<SubcConnector>) -> Self {
        Self { connector }
    }
}

#[async_trait]
impl ParticipantDiscovery for SubcParticipantDiscovery {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        self.connector
            .consumer()
            .await?
            .supervisor_list()
            .await
            .map_err(service_call_error)
    }

    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        self.connector
            .consumer()
            .await?
            .catalog_list()
            .await
            .map_err(service_call_error)
    }

    async fn open_participant(
        &self,
        module_id: &str,
    ) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.connector
            .open_internal_service(participant_target(module_id), "agent-data")
            .await
    }
}

pub(crate) struct Participant {
    pub(crate) module_id: String,
    pub(crate) transport: Arc<dyn LogTransport>,
}

#[derive(Debug, PartialEq)]
pub(crate) struct ParticipantUnavailable {
    pub(crate) detail: Value,
}

impl ParticipantUnavailable {
    pub(crate) fn code(&self) -> &'static str {
        "participant_unavailable"
    }
}

fn refusal(error: TransportError) -> Value {
    match error {
        TransportError::Refusal { code, detail } => json!({"code": code, "detail": detail}),
        TransportError::Unavailable => json!({"code": "unavailable"}),
        TransportError::NoReply => json!({"code": "outcome_unknown"}),
    }
}

pub(crate) async fn discover_participants(
    daemon: &dyn ParticipantDiscovery,
) -> Result<Vec<Participant>, ParticipantUnavailable> {
    let configured = daemon
        .supervisor_list()
        .await
        .map_err(|error| ParticipantUnavailable {
            detail: json!({"op": "supervisor.list", "refusal": refusal(error)}),
        })?;
    let catalog = daemon
        .catalog_list()
        .await
        .map_err(|error| ParticipantUnavailable {
            detail: json!({"op": "catalog.list", "refusal": refusal(error)}),
        })?;
    let registered: BTreeMap<_, _> = catalog
        .modules
        .iter()
        .map(|entry| (entry.module_id.as_str(), entry))
        .collect();
    let enabled: BTreeMap<_, _> = configured
        .modules
        .iter()
        .filter(|entry| entry.enabled)
        .map(|entry| (entry.module_id.as_str(), entry))
        .collect();

    let mut required = Vec::new();
    for (module_id, supervisor) in enabled {
        let Some(entry) = registered.get(module_id) else {
            // A configured module missing from the catalog may be a participant
            // that is down. Its missing capability declaration cannot exclude it.
            return Err(ParticipantUnavailable {
                detail: json!({"module_id": module_id, "state": supervisor.state}),
            });
        };
        if !entry
            .capabilities
            .as_ref()
            .is_some_and(|capabilities| capabilities.provides.iter().any(|name| name == CAPABILITY))
        {
            continue;
        }
        if !entry.ready {
            return Err(ParticipantUnavailable {
                detail: json!({
                    "module_id": module_id,
                    "state": supervisor.state,
                    "not_ready": entry.not_ready,
                }),
            });
        }
        required.push((module_id, supervisor));
    }

    let mut participants = Vec::new();
    for (module_id, supervisor) in required {
        let transport =
            daemon
                .open_participant(module_id)
                .await
                .map_err(|error| ParticipantUnavailable {
                    detail: json!({
                        "module_id": module_id,
                        "state": supervisor.state,
                        "route_refusal": refusal(error),
                    }),
                })?;
        participants.push(Participant {
            module_id: module_id.into(),
            transport,
        });
    }
    Ok(participants)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fake_daemon::{DaemonCall, DaemonSnapshot, FakeDaemon};

    fn configured(module_id: &str, enabled: bool, state: &str) -> Value {
        json!({
            "module_id": module_id, "enabled": enabled, "state": state,
            "live": state == "running", "health": "ok",
        })
    }

    fn participant(module_id: &str) -> Value {
        json!({
            "module_id": module_id, "ready": true, "roles": [provider_role()],
            "control_ops": [], "capabilities": {"provides": [CAPABILITY], "requires": []},
        })
    }

    fn daemon(configured: Vec<Value>, catalog: Vec<Value>) -> FakeDaemon {
        FakeDaemon::new(DaemonSnapshot::new(1, configured, catalog))
    }

    async fn unavailable(daemon: &dyn ParticipantDiscovery) -> Value {
        let error = discover_participants(daemon)
            .await
            .err()
            .expect("must refuse");
        assert_eq!(error.code(), "participant_unavailable");
        error.detail
    }

    #[test]
    fn copied_provider_role_pins_service_transport_and_all_ten_operations() {
        let role = serde_json::to_value(provider_role()).unwrap();
        assert_eq!(
            role,
            json!({
                "role": "internal_service", "service_id": "agent-data", "transport": "bulk",
                "agent_facing": false, "operations": [
                    "agent_data.describe", "agent_data.busy", "agent_data.prepare", "agent_data.export",
                    "agent_data.diverged", "agent_data.import", "agent_data.activate", "agent_data.release",
                    "agent_data.abort", "agent_data.status",
                ],
            })
        );
    }

    #[tokio::test]
    async fn available_participants_are_sorted_and_calls_use_internal_service_module_identity() {
        let daemon = daemon(
            vec![
                configured("zeta", true, "running"),
                configured("alpha", true, "running"),
            ],
            vec![participant("zeta"), participant("alpha")],
        );
        let participants = discover_participants(&daemon).await.unwrap();
        assert_eq!(
            participants
                .iter()
                .map(|p| p.module_id.as_str())
                .collect::<Vec<_>>(),
            vec!["alpha", "zeta"]
        );
        participants[0]
            .transport
            .call("agent_data.describe", json!({}))
            .await
            .unwrap();
        assert_eq!(
            daemon.calls(),
            vec![
                DaemonCall::SupervisorList,
                DaemonCall::CatalogList,
                DaemonCall::Open {
                    target: RouteTarget::InternalService {
                        module_id: "alpha".into(),
                        service_id: "agent-data".into(),
                    },
                    principal: "reserved:entorhinal".into()
                },
                DaemonCall::Open {
                    target: RouteTarget::InternalService {
                        module_id: "zeta".into(),
                        service_id: "agent-data".into(),
                    },
                    principal: "reserved:entorhinal".into()
                },
                DaemonCall::Request {
                    target: RouteTarget::InternalService {
                        module_id: "alpha".into(),
                        service_id: "agent-data".into(),
                    },
                    principal: "reserved:entorhinal".into(),
                    body: json!({
                        "method": "agent_data.describe", "params": {},
                    })
                },
            ]
        );
    }

    #[tokio::test]
    async fn stopped_and_unregistered_enabled_modules_refuse_with_supervisor_state() {
        for state in ["stopped", "starting"] {
            let daemon = daemon(vec![configured("missing", true, state)], vec![]);
            assert_eq!(
                unavailable(&daemon).await,
                json!({"module_id": "missing", "state": state})
            );
            assert_eq!(
                daemon.calls(),
                vec![DaemonCall::SupervisorList, DaemonCall::CatalogList]
            );
        }
    }

    #[tokio::test]
    async fn disabled_modules_are_ignored_even_when_absent_or_not_ready() {
        let mut disabled = participant("disabled");
        disabled["ready"] = json!(false);
        let daemon = daemon(
            vec![
                configured("disabled", false, "stopped"),
                configured("absent", false, "stopped"),
            ],
            vec![disabled],
        );
        assert!(discover_participants(&daemon).await.unwrap().is_empty());
        assert_eq!(
            daemon.calls(),
            vec![DaemonCall::SupervisorList, DaemonCall::CatalogList]
        );
    }

    #[tokio::test]
    async fn registered_nonparticipants_and_modules_not_enabled_in_supervisor_are_ignored() {
        let mut unrelated = participant("unrelated");
        unrelated["capabilities"]["provides"] = json!(["agent-data/v2"]);
        unrelated["ready"] = json!(false);
        let mut no_capabilities = participant("no-capabilities");
        no_capabilities["capabilities"] = Value::Null;
        let daemon = daemon(
            vec![
                configured("unrelated", true, "running"),
                configured("no-capabilities", true, "running"),
            ],
            vec![unrelated, no_capabilities, participant("unconfigured")],
        );
        assert!(discover_participants(&daemon).await.unwrap().is_empty());
        assert_eq!(
            daemon.calls(),
            vec![DaemonCall::SupervisorList, DaemonCall::CatalogList]
        );
    }

    #[tokio::test]
    async fn not_ready_participant_preserves_reason_and_opens_no_routes() {
        let mut warming = participant("warming");
        warming["ready"] = json!(false);
        warming["not_ready"] = json!({
            "reason": "required_capability_unprovided", "capability": "session-store/v1",
        });
        let daemon = daemon(
            vec![
                configured("available", true, "running"),
                configured("warming", true, "running"),
            ],
            vec![participant("available"), warming],
        );
        assert_eq!(
            unavailable(&daemon).await,
            json!({
                "module_id": "warming", "state": "running", "not_ready": {
                    "reason": "required_capability_unprovided", "capability": "session-store/v1",
                },
            })
        );
        assert_eq!(
            daemon.calls(),
            vec![DaemonCall::SupervisorList, DaemonCall::CatalogList]
        );
    }

    #[tokio::test]
    async fn missing_agent_data_provider_role_surfaces_route_refusal_as_participant_unavailable() {
        for roles in [
            json!([]),
            json!([{
                "role": "internal_service", "service_id": "another-service", "transport": "bulk",
                "agent_facing": false, "operations": ["agent_data.describe"],
            }]),
        ] {
            let mut entry = participant("wrong-role");
            entry["roles"] = roles;
            let daemon = daemon(vec![configured("wrong-role", true, "running")], vec![entry]);
            assert_eq!(
                unavailable(&daemon).await,
                json!({
                    "module_id": "wrong-role", "state": "running", "route_refusal": {
                        "code": "service_not_provided", "detail": {
                            "module_id": "wrong-role", "service_id": "agent-data",
                        },
                    },
                })
            );
        }
    }

    #[tokio::test]
    async fn restart_reads_current_catalog_and_does_not_reuse_old_participant_set() {
        let configured = vec![configured("module", true, "running")];
        let daemon = daemon(configured.clone(), vec![participant("module")]);
        assert_eq!(discover_participants(&daemon).await.unwrap().len(), 1);
        daemon.restart(DaemonSnapshot::new(2, configured.clone(), vec![]));
        assert_eq!(unavailable(&daemon).await["module_id"], "module");
        let mut entry = participant("module");
        entry["capabilities"]["provides"] = json!([]);
        daemon.restart(DaemonSnapshot::new(3, configured.clone(), vec![entry]));
        assert!(discover_participants(&daemon).await.unwrap().is_empty());
        daemon.restart(DaemonSnapshot::new(
            4,
            configured,
            vec![participant("module")],
        ));
        assert_eq!(discover_participants(&daemon).await.unwrap().len(), 1);
    }

    #[test]
    fn production_consumer_discovery_and_routes_use_supervised_identity() {
        // A fresh process isolates the SDK's cached launch secret from other tests
        // and from the developer's daemon environment.
        let mut child = std::process::Command::new(std::env::current_exe().unwrap());
        child
            .env_clear()
            .env("SUBC_MODULE_ID", "entorhinal")
            .env("SUBC_LAUNCH_NONCE", "fake-launch-secret")
            .args([
                "--exact",
                "participant_discovery::tests::authenticated_consumer_probe",
                "--ignored",
                "--nocapture",
            ]);
        // Windows needs a few variables the cleared environment drops. Without
        // TEMP and TMP, the temp directory falls back to C:\Windows, where the
        // probe's scratch folder can't be made owner-only.
        #[cfg(windows)]
        for name in ["SYSTEMROOT", "PATH", "TEMP", "TMP"] {
            if let Some(value) = std::env::var_os(name) {
                child.env(name, value);
            }
        }
        let output = child.output().unwrap();
        assert!(
            output.status.success(),
            "child stdout: {}\nchild stderr: {}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
    }

    #[tokio::test]
    #[ignore = "run in an isolated process by production_consumer_discovery_and_routes_use_supervised_identity"]
    async fn authenticated_consumer_probe() {
        tokio::time::timeout(std::time::Duration::from_secs(12), async {
            let scratch = cortexkit_test_support::ScratchDir::new("participant-discovery");
            let connection_file = scratch.path().join("subc.json");
            let connector = Arc::new(SubcConnector::from_args([
                std::ffi::OsString::from("--subc"), connection_file.clone().into_os_string(),
            ]));
            let production = SubcParticipantDiscovery::new(connector);
            let missing = discover_participants(&production).await.err().unwrap();
            assert_eq!(missing.code(), "participant_unavailable");
            assert_eq!(missing.detail["op"], "supervisor.list");

            let fake = daemon(vec![configured("participant", true, "running")], vec![participant("participant")]);
            let _wire = crate::fake_daemon::WireDaemon::start(fake.clone(), &connection_file).await;
            let participants = discover_participants(&production).await.unwrap();
            assert_eq!(participants.len(), 1, "initial connect failure must not be cached");
            for method in OPERATIONS {
                participants[0].transport.call(method, json!({})).await.unwrap();
            }
            let calls = fake.calls();
            assert_eq!(&calls[..3], &[
                DaemonCall::SupervisorList, DaemonCall::CatalogList,
                DaemonCall::Open { target: RouteTarget::InternalService {
                    module_id: "participant".into(), service_id: "agent-data".into(),
                }, principal: "reserved:entorhinal".into() },
            ]);
            assert_eq!(calls.len(), 13, "one route shared by ten calls");
            for (call, method) in calls[3..].iter().zip(OPERATIONS) {
                assert_eq!(call, &DaemonCall::Request {
                    target: RouteTarget::InternalService {
                        module_id: "participant".into(), service_id: "agent-data".into(),
                    }, principal: "reserved:entorhinal".into(),
                    body: json!({"method": method, "params": {}}),
                });
            }

            let mut bad_role = participant("bad-role");
            bad_role["roles"] = json!([]);
            fake.restart(DaemonSnapshot::new(2, vec![configured("bad-role", true, "running")], vec![bad_role]));
            assert_eq!(unavailable(&production).await, json!({
                "module_id": "bad-role", "state": "running", "route_refusal": {
                    "code": "service_not_provided", "detail": {"module_id": "bad-role", "service_id": "agent-data"},
                },
            }));
        }).await.expect("production adapter probe exceeded its deadline");
    }
}

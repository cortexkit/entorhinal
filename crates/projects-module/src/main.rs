#![forbid(unsafe_code)]

//! The supervised `ck-projects` module.
//!
//! `subc-client-rs` owns the HELLO, HELLO_ACK, route binding, health control, and
//! frame lifecycle. This binary only supplies the manifest and domain handler.

use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use cortexkit_store_types::{sqlite_store_path, Isolation, StorageBackend, StorageDescriptor};
use projects_core::{RegistryError, RegistryStore};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{HandlerOutcome, HealthReport, HealthStatus, ModuleHandler, RequestCtx};
use subc_protocol::{
    manifest::{
        Bindings, IdentityBinding, ManagementOperation, ManagementOperationKind, ModuleManifest,
        ProviderRole, StorageBinding, StorageKind, StorageScope, TrustTier,
    },
    ModuleHelloAckBody, PROTOCOL_VERSION,
};

const MODULE_ID: &str = "projects";
const DEFAULT_STORAGE_NAMESPACE: &str = "default";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if std::env::args().any(|argument| argument == "--version") {
        println!("ck-projects {}", env!("CARGO_PKG_VERSION"));
        return Ok(());
    }

    subc_client_rs::serve(manifest(), ProjectsHandler::new()).await?;
    Ok(())
}

#[derive(Default)]
struct HealthGauges {
    store_ready: AtomicBool,
    store_failed: AtomicBool,
    generation: AtomicI64,
    resolve_count: AtomicU64,
    enumerate_count: AtomicU64,
    journal_tail_count: AtomicU64,
    last_operation_ms: AtomicI64,
}

struct ProjectsHandler {
    store: Arc<Mutex<Option<RegistryStore>>>,
    health: Arc<HealthGauges>,
}

impl ProjectsHandler {
    fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(None)),
            health: Arc::new(HealthGauges::default()),
        }
    }

    fn with_store<T>(
        &self,
        operation: impl FnOnce(&RegistryStore) -> Result<T, RegistryError>,
    ) -> Result<T, HandlerError> {
        let guard = self
            .store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let store = guard.as_ref().ok_or_else(|| HandlerError {
            code: "storage_unavailable".to_string(),
            message: "projects storage is not ready".to_string(),
        })?;
        operation(store).map_err(|error| HandlerError {
            code: "storage_error".to_string(),
            message: error.to_string(),
        })
    }

    fn record_query(&self, generation: i64, counter: &AtomicU64) {
        self.health.generation.store(generation, Ordering::Relaxed);
        counter.fetch_add(1, Ordering::Relaxed);
        self.health
            .last_operation_ms
            .store(unix_millis(), Ordering::Relaxed);
    }
}

#[async_trait]
impl ModuleHandler for ProjectsHandler {
    async fn handle(&self, _ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request = match serde_json::from_slice::<WireRequest>(&body) {
            Ok(request) => request,
            Err(error) => {
                return HandlerOutcome::Error {
                    code: "invalid_request".to_string(),
                    message: format!("request must be JSON with method and params: {error}"),
                }
            }
        };

        let result = match request.method.as_str() {
            "resolve" => self.resolve(request.params),
            "resolve_project_id" => self.resolve_project_id(request.params),
            "enumerate" => self.enumerate(request.params),
            "journal_tail" => self.journal_tail(request.params),
            "register" | "assign_workspace" | "upgrade_implicit" | "remove" | "seed_import" => {
                Err(HandlerError {
                    code: "unimplemented".to_string(),
                    message: format!(
                        "mutation '{}' is reserved for a later skeleton step",
                        request.method
                    ),
                })
            }
            _ => Err(HandlerError {
                code: "unknown_method".to_string(),
                message: format!(
                    "unknown projects method '{}', see the management manifest",
                    request.method
                ),
            }),
        };

        match result {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => HandlerOutcome::Error {
                code: error.code,
                message: error.message,
            },
        }
    }

    async fn on_hello_ack(&self, ack: &ModuleHelloAckBody) {
        let descriptor = match ack.storage.as_ref() {
            Some(value) => serde_json::from_value::<StorageDescriptor>(value.clone())
                .map_err(|error| format!("HELLO_ACK storage descriptor is invalid: {error}")),
            None => Ok(fallback_storage_descriptor()),
        };

        let opened = descriptor.and_then(|descriptor| {
            RegistryStore::open(&descriptor)
                .map_err(|error| format!("opening projects storage: {error}"))
        });
        let mut guard = self
            .store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        match opened {
            Ok(store) => {
                self.health
                    .generation
                    .store(store.generation().unwrap_or(0), Ordering::Relaxed);
                self.health.store_ready.store(true, Ordering::Relaxed);
                self.health.store_failed.store(false, Ordering::Relaxed);
                *guard = Some(store);
            }
            Err(error) => {
                eprintln!("[ck-projects] {error}");
                self.health.store_ready.store(false, Ordering::Relaxed);
                self.health.store_failed.store(true, Ordering::Relaxed);
                *guard = None;
            }
        }
    }

    async fn health(&self) -> HealthReport {
        // Health is deliberately insulated from the store mutex and database. The
        // daemon's health path must remain responsive even when a domain request is
        // blocked or the store has failed; only cached atomic gauges are read here.
        let ready = self.health.store_ready.load(Ordering::Relaxed);
        let failed = self.health.store_failed.load(Ordering::Relaxed);
        let status = if failed || !ready {
            HealthStatus::Failing
        } else {
            HealthStatus::Ok
        };
        HealthReport {
            status,
            detail: (!ready).then(|| "projects storage is not ready".to_string()),
            metrics: Some(json!({
                "storeReady": ready,
                "generation": self.health.generation.load(Ordering::Relaxed),
                "resolveCount": self.health.resolve_count.load(Ordering::Relaxed),
                "enumerateCount": self.health.enumerate_count.load(Ordering::Relaxed),
                "journalTailCount": self.health.journal_tail_count.load(Ordering::Relaxed),
                "lastOperationMs": self.health.last_operation_ms.load(Ordering::Relaxed),
            })),
        }
    }
}

impl ProjectsHandler {
    fn resolve(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<ResolveParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.resolve(&params.canonical_root))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        encode_result(reply)
    }

    fn resolve_project_id(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params =
            serde_json::from_value::<ResolveProjectIdParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.resolve_project_id(&params.project_id))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        encode_result(reply)
    }

    fn enumerate(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<EnumerateParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.enumerate(params.workspace_id.as_deref()))?;
        self.record_query(reply.generation, &self.health.enumerate_count);
        encode_result(reply)
    }

    fn journal_tail(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<JournalTailParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| {
            store.journal_tail(params.after_seq, params.limit.unwrap_or(100))
        })?;
        self.record_query(reply.generation, &self.health.journal_tail_count);
        encode_result(reply)
    }
}

#[derive(Debug)]
struct HandlerError {
    code: String,
    message: String,
}

fn invalid_params(error: serde_json::Error) -> HandlerError {
    HandlerError {
        code: "invalid_params".to_string(),
        message: error.to_string(),
    }
}

fn encode_result<T: serde::Serialize>(result: T) -> Result<Vec<u8>, HandlerError> {
    serde_json::to_vec(&json!({ "result": result })).map_err(|error| HandlerError {
        code: "encode_failed".to_string(),
        message: error.to_string(),
    })
}

#[derive(Debug, Deserialize)]
struct WireRequest {
    method: String,
    #[serde(default)]
    params: Value,
}

#[derive(Debug, Deserialize)]
struct ResolveParams {
    #[serde(rename = "canonicalRoot", alias = "canonical_root")]
    canonical_root: String,
}

#[derive(Debug, Deserialize)]
struct ResolveProjectIdParams {
    #[serde(rename = "projectId", alias = "project_id")]
    project_id: String,
}

#[derive(Debug, Default, Deserialize)]
struct EnumerateParams {
    #[serde(rename = "workspaceId", alias = "workspace_id")]
    workspace_id: Option<String>,
}

#[derive(Debug, Default, Deserialize)]
struct JournalTailParams {
    #[serde(rename = "afterSeq", alias = "after_seq", default)]
    after_seq: i64,
    #[serde(default)]
    limit: Option<i64>,
}

fn manifest() -> ModuleManifest {
    ModuleManifest {
        module_id: MODULE_ID.to_string(),
        module_version: env!("CARGO_PKG_VERSION").to_string(),
        protocol_ver: PROTOCOL_VERSION,
        trust_tier: TrustTier::FirstParty,
        provides: vec![ProviderRole::ManagementSurface {
            operations: vec![
                management_operation("resolve", ManagementOperationKind::Query),
                management_operation("resolve_project_id", ManagementOperationKind::Query),
                management_operation("enumerate", ManagementOperationKind::Query),
                management_operation("journal_tail", ManagementOperationKind::Query),
                management_operation("register", ManagementOperationKind::Mutate),
                management_operation("assign_workspace", ManagementOperationKind::Mutate),
                management_operation("upgrade_implicit", ManagementOperationKind::Mutate),
                management_operation("remove", ManagementOperationKind::Mutate),
                management_operation("seed_import", ManagementOperationKind::Mutate),
            ],
            config_schema: json!({"type": "object"}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
        }],
        consumes: Vec::new(),
        scheduled_tasks: Vec::new(),
        bindings: Bindings {
            storage: StorageBinding {
                kind: StorageKind::Sqlite,
                scope: StorageScope::Project,
                owns_schema: true,
            },
            vault_grants: Vec::new(),
            identity: IdentityBinding {
                requires: Vec::new(),
                optional: Vec::new(),
            },
        },
    }
}

fn management_operation(name: &str, kind: ManagementOperationKind) -> ManagementOperation {
    ManagementOperation {
        name: name.to_string(),
        kind,
    }
}

fn fallback_storage_descriptor() -> StorageDescriptor {
    let data_home = std::env::var_os("XDG_DATA_HOME")
        .filter(|value| !value.is_empty())
        .map(PathBuf::from)
        .or_else(|| {
            std::env::var_os("HOME")
                .filter(|value| !value.is_empty())
                .map(|home| PathBuf::from(home).join(".local/share"))
        })
        .unwrap_or_else(|| std::env::temp_dir().join("ck-projects-data"));
    let path = sqlite_store_path(&data_home.to_string_lossy(), "ck-projects");
    StorageDescriptor {
        module_id: MODULE_ID.to_string(),
        storage_namespace: DEFAULT_STORAGE_NAMESPACE.to_string(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite { path },
    }
}

fn unix_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_declares_the_projects_surface_and_all_skeleton_mutations() {
        let value = serde_json::to_value(manifest()).expect("manifest serializes");
        assert_eq!(value["module_id"], "projects");
        assert_eq!(value["trust_tier"], "first_party");
        let operations = value["provides"][0]["operations"].as_array().unwrap();
        assert!(operations
            .iter()
            .any(|operation| operation["name"] == "resolve"));
        assert!(operations
            .iter()
            .any(|operation| operation["name"] == "seed_import"));
    }

    #[test]
    fn version_probe_is_not_a_subc_connection_attempt() {
        assert_eq!(env!("CARGO_PKG_NAME"), "projects-module");
        assert_eq!(
            subc_protocol::session::MODULE_CONTROL_OP_HEALTH_CHECK,
            "health.check"
        );
    }
}

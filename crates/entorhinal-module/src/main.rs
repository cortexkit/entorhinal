#![forbid(unsafe_code)]

//! The supervised `ck-entorhinal` module.
//!
//! `subc-client-rs` owns the HELLO, HELLO_ACK, route binding, health control, and
//! frame lifecycle. This binary only supplies the manifest and domain handler.

use std::collections::BTreeMap;
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use async_trait::async_trait;
use cortexkit_store_types::{sqlite_store_path, Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    AssignWorkspaceRequest, RegisterRequest, RegistryError, RegistryStore, RemoveRequest,
    SeedImportRequest, UpgradeImplicitRequest,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{HandlerOutcome, HealthReport, HealthStatus, ModuleHandler, RequestCtx};
use subc_protocol::{
    manifest::{
        Bindings, Concurrency, IdentityBinding, ManagementOperation, ManagementOperationKind,
        ModuleManifest, ProviderRole, StorageBinding, StorageKind, StorageScope, TrustTier,
    },
    ModuleHelloAckBody, PROTOCOL_VERSION,
};

// The id the daemon serves this module under, and the LAST of three names to be
// reconciled: the repo and binary became `entorhinal` in the fleet rename wave
// while this constant stayed `projects`, so a consumer that guessed either of
// the other two dialled a module that does not exist.
//
// Changed before first run, deliberately. The daemon derives the module's store
// path from this id, so the first launch mints a directory and the rename stops
// being free from then on -- it becomes a data migration. This module has never
// been deployed, which is the only reason a one-line change is sufficient.
const MODULE_ID: &str = "entorhinal";
const DEFAULT_STORAGE_NAMESPACE: &str = "default";

mod cli;

// PARSE ARGV BEFORE ACTING ON IT.
//
// `ck` dispatches an unknown domain to `ck-<domain>` on PATH, so `ck entorhinal
// list` arrives here as argv. Serving is therefore reachable ONLY from empty
// argv, which is how the supervisor spawns it; anything else is answered and
// exits. Without the split, a mistyped operator command would fall through to
// `serve`, claim the module's identity against the daemon, and sit there
// looking healthy while doing nothing the operator asked for.
fn main() -> std::process::ExitCode {
    // The user-facing command surface is `ck projects` and `ck workspaces`,
    // dispatched by `ck` to `ck-projects` / `ck-workspaces` -- both symlinks to
    // this binary. argv[0] selects the face; the module identity (ck-entorhinal)
    // never appears in an operator's vocabulary. One binary rather than three
    // because the faces share every line of transport, rendering, and parse
    // machinery, and a shared binary cannot drift from itself.
    let face = cli::face_from_argv0(std::env::args().next().as_deref());
    let arguments: Vec<String> = std::env::args().skip(1).collect();
    match cli::parse(face, &arguments) {
        cli::Invocation::Report(text) => {
            println!("{text}");
            std::process::ExitCode::SUCCESS
        }
        // Refusals go to stderr and exit nonzero. A mistyped flag that exits 0
        // reports success for a command that never ran, which any wrapper
        // checking the status code would believe.
        cli::Invocation::Refuse(text) => {
            eprintln!("{text}");
            std::process::ExitCode::from(64)
        }
        cli::Invocation::Command(command) => cli::run(command),
        cli::Invocation::Module => match serve_module() {
            Ok(()) => std::process::ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("ck-entorhinal: {error}");
                std::process::ExitCode::FAILURE
            }
        },
    }
}

#[tokio::main]
async fn serve_module() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
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
    liveness_batches: AtomicU64,
    liveness_gaps: AtomicU64,
    liveness_sessions: AtomicU64,
}

/// Volatile liveness state fed by `projects.session_liveness` batches. Never
/// journaled (I8 forbids liveness in the topology journal); it exists to
/// annotate enumerate answers and to feed future GC-candidacy signals, so a
/// wrong, late, or absent feed can never corrupt topology.
#[derive(Default)]
struct LivenessState {
    /// Last accepted per-emitter sequence number; None until first contact.
    last_seq: Option<u64>,
    /// True when a dropped batch was detected and no snapshot has rebased yet.
    stale: bool,
    /// session_id -> (state, canonical_root, last_activity_ms).
    sessions: BTreeMap<String, (String, String, i64)>,
}

struct ProjectsHandler {
    store: Arc<Mutex<Option<RegistryStore>>>,
    health: Arc<HealthGauges>,
    liveness: Arc<Mutex<LivenessState>>,
}

impl ProjectsHandler {
    fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(None)),
            health: Arc::new(HealthGauges::default()),
            liveness: Arc::new(Mutex::new(LivenessState::default())),
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
            code: match &error {
                RegistryError::Domain { code, .. } => code.clone(),
                _ => "storage_error".to_string(),
            },
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
            "register" => self.register(request.params),
            "assign_workspace" => self.assign_workspace(request.params),
            "upgrade_implicit" => self.upgrade_implicit(request.params),
            "remove" => self.remove(request.params),
            "seed_import" => self.seed_import(request.params),
            "projects.session_liveness" => self.session_liveness(request.params),
            "verify" => self.verify(),
            "rebuild" => self.rebuild(),
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
                eprintln!("[ck-entorhinal] {error}");
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
                "livenessBatches": self.health.liveness_batches.load(Ordering::Relaxed),
                "livenessGaps": self.health.liveness_gaps.load(Ordering::Relaxed),
                "livenessSessions": self.health.liveness_sessions.load(Ordering::Relaxed),
            })),
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LivenessBatch {
    seq: u64,
    #[serde(default)]
    snapshot: bool,
    #[serde(default)]
    sessions: Vec<LivenessSession>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct LivenessSession {
    session_id: String,
    canonical_root: String,
    state: String,
    #[serde(default)]
    last_activity_ms: i64,
    // project_id_hint intentionally ignored for state: the registry re-resolves
    // canonical_root at read time; the hint is a producer fast-path only.
}

impl ProjectsHandler {
    /// Ingest a `projects.session_liveness` batch (push-on-transition +
    /// snapshot-on-reconnect, pinned in #workspace-projects-design). A seq gap
    /// marks the volatile state stale until the next snapshot rebases it; the
    /// reply carries `snapshotRequested` so the emitter can resend cheaply.
    fn session_liveness(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let batch = serde_json::from_value::<LivenessBatch>(params).map_err(invalid_params)?;
        let mut state = self
            .liveness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if batch.snapshot {
            state.sessions.clear();
            state.stale = false;
        } else {
            let expected = state.last_seq.map(|s| s.wrapping_add(1));
            if expected.is_some_and(|e| batch.seq != e) {
                state.stale = true;
                self.health.liveness_gaps.fetch_add(1, Ordering::Relaxed);
            }
        }
        // Request a snapshot until one actually rebases us: staleness is a
        // LATCH, not a per-batch flag. If only the freshly-detected gap
        // answered true, a lost snapshot re-emit would leave the state stale
        // forever while every later contiguous delta reported clean.
        let snapshot_requested = state.stale;
        state.last_seq = Some(batch.seq);

        for session in batch.sessions {
            if session.state == "gone" {
                state.sessions.remove(&session.session_id);
            } else {
                state.sessions.insert(
                    session.session_id,
                    (
                        session.state,
                        session.canonical_root,
                        session.last_activity_ms,
                    ),
                );
            }
        }
        let tracked = state.sessions.len() as u64;
        drop(state);

        self.health.liveness_batches.fetch_add(1, Ordering::Relaxed);
        self.health
            .liveness_sessions
            .store(tracked, Ordering::Relaxed);
        serde_json::to_vec(&json!({
            "result": { "accepted": true, "snapshotRequested": snapshot_requested, "tracked": tracked }
        }))
        .map_err(|error| HandlerError {
            code: "encode_failed".to_string(),
            message: error.to_string(),
        })
    }

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
        let mut reply = self.with_store(|store| store.enumerate(params.workspace_id.as_deref()))?;
        self.annotate_liveness(&mut reply);
        self.record_query(reply.generation, &self.health.enumerate_count);
        encode_result(reply)
    }

    /// Join the volatile liveness map onto enumerate results: a project's
    /// last_route_activity_ms is the newest activity among live sessions whose
    /// canonical_root falls inside one of the project's roots. This is the
    /// consumer the liveness feed exists for; without it the accepted batches
    /// would annotate nothing.
    fn annotate_liveness(&self, reply: &mut entorhinal_core::EnumerateReply) {
        let state = self
            .liveness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        if state.sessions.is_empty() {
            return;
        }
        for project in &mut reply.projects {
            let newest = state
                .sessions
                .values()
                .filter(|(_, root, _)| {
                    project.roots.iter().any(|project_root| {
                        root == project_root
                            || root.starts_with(&format!("{}/", project_root.trim_end_matches('/')))
                    })
                })
                .map(|(_, _, activity)| *activity)
                .max();
            project.last_route_activity_ms = newest;
        }
    }

    fn journal_tail(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<JournalTailParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| {
            store.journal_tail(params.after_seq, params.limit.unwrap_or(100))
        })?;
        self.record_query(reply.generation, &self.health.journal_tail_count);
        encode_result(reply)
    }

    fn register(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RegisterRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.register(req)))
    }
    fn assign_workspace(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<AssignWorkspaceRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.assign_workspace(req)))
    }
    fn upgrade_implicit(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<UpgradeImplicitRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.upgrade_implicit(req)))
    }
    fn remove(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RemoveRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.remove(req)))
    }
    fn seed_import(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<SeedImportRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.seed_import(req)))
    }

    /// Refresh the health generation gauge from a mutation's wire reply, which
    /// carries the post-mutation generation. Without this the gauge reports
    /// the pre-mutation generation until the next query-side op runs.
    fn record_mutation(
        &self,
        result: Result<Vec<u8>, HandlerError>,
    ) -> Result<Vec<u8>, HandlerError> {
        if let Ok(blob) = &result {
            if let Ok(value) = serde_json::from_slice::<Value>(blob) {
                if let Some(generation) = value
                    .get("result")
                    .and_then(|r| r.get("generation"))
                    .and_then(Value::as_i64)
                {
                    self.health.generation.store(generation, Ordering::Relaxed);
                }
            }
            self.health
                .last_operation_ms
                .store(unix_millis(), Ordering::Relaxed);
        }
        result
    }
    fn verify(&self) -> Result<Vec<u8>, HandlerError> {
        let reply = self.with_store(|s| s.verify())?;
        encode_result(reply)
    }
    fn rebuild(&self) -> Result<Vec<u8>, HandlerError> {
        let generation = self.with_store(|s| s.rebuild())?;
        encode_result(json!({"generation":generation}))
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
                management_operation(
                    "resolve",
                    ManagementOperationKind::Query,
                    "Resolve a filesystem path to its registered or implicit project identity.",
                ),
                management_operation(
                    "resolve_project_id",
                    ManagementOperationKind::Query,
                    "Look up a project by its durable project id and return its registration record.",
                ),
                management_operation(
                    "enumerate",
                    ManagementOperationKind::Query,
                    "List all registered projects with workspace assignments and tags.",
                ),
                management_operation(
                    "journal_tail",
                    ManagementOperationKind::Query,
                    "Read the newest registry journal entries for operator inspection.",
                ),
                management_operation(
                    "register",
                    ManagementOperationKind::Mutate,
                    "Register a project root under a workspace, minting its durable project id.",
                ),
                management_operation(
                    "assign_workspace",
                    ManagementOperationKind::Mutate,
                    "Move a registered project to a different workspace.",
                ),
                management_operation(
                    "upgrade_implicit",
                    ManagementOperationKind::Mutate,
                    "Promote an implicit (path-derived) project to a registered one, keeping its id.",
                ),
                management_operation(
                    "remove",
                    ManagementOperationKind::Mutate,
                    "Remove a project registration; its id stops resolving.",
                ),
                management_operation(
                    "seed_import",
                    ManagementOperationKind::Mutate,
                    "Bulk-import registrations from a seed file (operator recovery path).",
                ),
                management_operation(
                    "verify",
                    ManagementOperationKind::Query,
                    "Check registry invariants and report inconsistencies without mutating.",
                ),
                management_operation(
                    "rebuild",
                    ManagementOperationKind::Mutate,
                    "Rebuild registry projections by replaying the journal (operator recovery path).",
                ),
                // Volatile liveness ingestion — never journaled, feeds enumerate
                // annotations only (ALF's producer per #workspace-projects-design).
                management_operation(
                    "projects.session_liveness",
                    ManagementOperationKind::Mutate,
                    "Ingest session liveness snapshots and deltas from the executive for dead-folder detection.",
                ),
            ],
            config_schema: json!({"type": "object"}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
            // Registry ops are short SQLite reads/writes serialized behind the
            // module's own store lock; ModuleManaged matches the pre-field
            // default the daemon applied while `concurrency` was implicit.
            concurrency: Concurrency::ModuleManaged,
        }],
        consumes: Vec::new(),
        // Capability grammar declarations wait for the fleet owner round: the
        // cuts draft records entorhinal -> project-registry/v1, but a provides
        // claim belongs with the reviewed registry entry and corpus, not ahead
        // of them. None = grammar inactive for this module, deliberately.
        capabilities: None,
        // Build provenance is declared by CK_BUILD_* env at build time once the
        // release script injects it; None is the honest value until then — the
        // daemon serves declared_absent rather than a fabricated rev.
        provenance: None,
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

fn management_operation(
    name: &str,
    kind: ManagementOperationKind,
    description: &str,
) -> ManagementOperation {
    ManagementOperation {
        name: name.to_string(),
        kind,
        description: Some(description.to_string()),
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
        .unwrap_or_else(|| std::env::temp_dir().join("ck-entorhinal-data"));
    let path = sqlite_store_path(&data_home.to_string_lossy(), "ck-entorhinal");
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

    #[tokio::test]
    async fn session_liveness_gap_detection_and_snapshot_rebase() {
        let handler = ProjectsHandler::new();
        let call = |h: &ProjectsHandler, v: serde_json::Value| h.session_liveness(v);
        // Snapshot establishes the baseline at seq 10.
        let r = call(
            &handler,
            json!({"seq": 10, "snapshot": true, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"}]}),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(v["result"]["snapshotRequested"], false);
        assert_eq!(v["result"]["tracked"], 1);
        // Contiguous transition batch: accepted, no gap.
        let r = call(
            &handler,
            json!({"seq": 11, "sessions": [
            {"sessionId": "s2", "canonicalRoot": "/tmp/b", "state": "idle"}]}),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(v["result"]["snapshotRequested"], false);
        assert_eq!(v["result"]["tracked"], 2);
        // Dropped batch (seq 12 lost): gap detected, snapshot requested.
        let r = call(
            &handler,
            json!({"seq": 13, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "gone"}]}),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(
            v["result"]["snapshotRequested"], true,
            "gap must request a snapshot"
        );
        assert_eq!(v["result"]["tracked"], 1, "gone still applies");
        assert_eq!(handler.health.liveness_gaps.load(Ordering::Relaxed), 1);
        // THE LATCH (audit C1): a contiguous delta arriving while state is
        // still stale must KEEP requesting the snapshot. Before the fix this
        // read false (gap was per-batch), so one lost snapshot re-emit left
        // the state stale forever with every later reply reading clean.
        let r = call(
            &handler,
            json!({"seq": 14, "sessions": [
            {"sessionId": "s3", "canonicalRoot": "/tmp/c", "state": "active"}]}),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(
            v["result"]["snapshotRequested"], true,
            "stale state must keep requesting a snapshot on contiguous deltas"
        );
        // Snapshot rebases: stale clears, seq resets legally.
        let r = call(
            &handler,
            json!({"seq": 1, "snapshot": true, "sessions": []}),
        )
        .unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(v["result"]["snapshotRequested"], false);
        assert_eq!(v["result"]["tracked"], 0);
        // And post-rebase deltas read clean again.
        let r = call(&handler, json!({"seq": 2, "sessions": []})).unwrap();
        let v: Value = serde_json::from_slice(&r).unwrap();
        assert_eq!(v["result"]["snapshotRequested"], false);
    }

    #[test]
    fn enumerate_annotation_joins_liveness_by_root_containment() {
        let handler = ProjectsHandler::new();
        handler
            .session_liveness(json!({"seq": 1, "snapshot": true, "sessions": [
                {"sessionId": "s1", "canonicalRoot": "/w/proj/sub", "state": "active", "lastActivityMs": 500},
                {"sessionId": "s2", "canonicalRoot": "/w/proj", "state": "idle", "lastActivityMs": 900},
                {"sessionId": "s3", "canonicalRoot": "/elsewhere", "state": "active", "lastActivityMs": 999}]}))
            .unwrap();
        let mut reply = entorhinal_core::EnumerateReply {
            generation: 1,
            workspaces: vec![],
            projects: vec![
                entorhinal_core::ProjectView {
                    project_id: "p1".into(),
                    name: "p1".into(),
                    roots: vec!["/w/proj".into()],
                    implicit: false,
                    ref_kind: "local".into(),
                    device_fingerprint: None,
                    workspace_id: None,
                    last_route_activity_ms: None,
                },
                entorhinal_core::ProjectView {
                    project_id: "p2".into(),
                    name: "p2".into(),
                    roots: vec!["/w/other".into()],
                    implicit: false,
                    ref_kind: "local".into(),
                    device_fingerprint: None,
                    workspace_id: None,
                    last_route_activity_ms: None,
                },
            ],
        };
        handler.annotate_liveness(&mut reply);
        // p1 gets the NEWEST activity among sessions inside its root (s1
        // strictly under, s2 exactly at) and never /elsewhere's.
        assert_eq!(reply.projects[0].last_route_activity_ms, Some(900));
        // No live session touches p2: annotation stays None, not zero.
        assert_eq!(reply.projects[1].last_route_activity_ms, None);
    }

    #[test]
    fn manifest_declares_the_projects_surface_and_all_skeleton_mutations() {
        let value = serde_json::to_value(manifest()).expect("manifest serializes");
        assert_eq!(value["module_id"], "entorhinal");
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
        assert_eq!(env!("CARGO_PKG_NAME"), "entorhinal-module");
        assert_eq!(
            subc_protocol::session::MODULE_CONTROL_OP_HEALTH_CHECK,
            "health.check"
        );
    }
}

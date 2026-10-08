#![forbid(unsafe_code)]

//! The supervised `ck-entorhinal` module.
//!
//! `subc-client-rs` owns the HELLO, HELLO_ACK, route binding, health control, and
//! frame lifecycle. This binary only supplies the manifest and domain handler.

use std::collections::{BTreeMap, HashMap};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use async_trait::async_trait;
use cortexkit_store_types::{sqlite_store_path, Isolation, StorageBackend, StorageDescriptor};
use entorhinal_core::{
    AssignWorkspaceRequest, RegisterRequest, RegistryError, RegistryStore, RemoveRequest,
    SeedImportRequest, SetWorkspaceRootRequest, UpgradeImplicitRequest,
};
use serde::Deserialize;
use serde_json::{json, Value};
use subc_client_rs::{
    BindDecision, HandlerOutcome, HealthReport, HealthStatus, ModuleHandler, RequestCtx,
    RouteBindRequest, RouteHandle,
};
use subc_protocol::{
    manifest::{
        build_provenance_from_source, BuildGitShaAbsenceReason, BuildGitShaSource,
        CapabilityDeclarations, Concurrency, GitTreeState, ManagementOperation,
        ManagementOperationKind, ManifestProvenance, ModuleManifest, ProviderRole,
    },
    scope::ScopeStamp,
    ModuleHelloAckBody, Principal, PROTOCOL_VERSION,
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

mod agent_ops;
mod agent_reads;
mod catch_up;
mod cli;
mod enable;
mod incarnation;
// The client for engram's identity log. Only writes to shared state and
// catch-up call it, and it opens its route on first use, so startup and every
// read work with engram absent. It has no callers until the shared write path
// lands, hence the dead-code allowances.
#[cfg(test)]
#[allow(dead_code)]
mod fake_log;
#[allow(dead_code)]
mod log_client;
mod shared_write;
#[cfg(test)]
mod two_machine_acceptance;

// PARSE ARGV BEFORE ACTING ON IT.
//
// `ck` dispatches an unknown domain to `ck-<domain>` on PATH, so `ck entorhinal
// list` arrives here as argv. Serving is therefore reachable ONLY from empty
// argv, which is how the supervisor spawns it; anything else is answered and
// exits. Without the split, a mistyped operator command would fall through to
// `serve`, claim the module's identity against the daemon, and sit there
// looking healthy while doing nothing the operator asked for.
fn main() -> std::process::ExitCode {
    // The user-facing command surface is `ck projects`, `ck workspaces` and `ck agents`,
    // dispatched by `ck` to `ck-projects` / `ck-workspaces` / `ck-agents` -- symlinks to
    // this binary. argv[0] selects the face; the module identity (ck-entorhinal)
    // never appears in an operator's vocabulary. One binary rather than four
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
        cli::Invocation::Manifest => match serde_json::to_string(&manifest()) {
            Ok(json) => {
                println!("{json}");
                std::process::ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("ck-entorhinal: cannot serialize the manifest: {error}");
                std::process::ExitCode::FAILURE
            }
        },
        cli::Invocation::Module => {
            // Module mode logs through the fleet logger (dated segments under
            // the module's data dir, read by `ck module logs entorhinal`). The
            // CLI faces above print to stdout/stderr because that IS their
            // output. The logger needs SUBC_MODULE_ID from the supervisor's
            // spawn environment; a hand-launched module has none, and this
            // stderr line is the only place a refusal can go before a logger
            // exists.
            let _logger = match cortexkit_log::init_from_env() {
                Ok(handle) => handle,
                Err(error) => {
                    eprintln!("ck-entorhinal: cannot start the fleet logger: {error}");
                    return std::process::ExitCode::FAILURE;
                }
            };
            tracing::info!("entorhinal module starting");
            match serve_module() {
                Ok(()) => std::process::ExitCode::SUCCESS,
                Err(error) => {
                    tracing::error!("module exited: {error}");
                    std::process::ExitCode::FAILURE
                }
            }
        }
    }
}

#[tokio::main]
async fn serve_module() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let handler = ProjectsHandler::new()?;
    let confirmer = Arc::new(agent_ops::DaemonConfirmer::new(
        handler.route_admissions.clone(),
    ));
    let handler = handler.with_operator_confirmer(confirmer.clone());
    // The SDK reads `--subc` and `SUBC_MODULE_ID` exactly as `serve` does, but
    // returns the live handle. Install it before polling the serve future so no
    // request can need a confirmation before the confirmer has a connection.
    let (handle, serving) = subc_client_rs::serve_from_env_with_handle(manifest(), handler).await?;
    confirmer.set_module_handle(handle);
    serving.await?;
    Ok(())
}

/// How long a fresh instance keeps retrying a lease that a predecessor still
/// holds. The lease is a kernel lock released at process exit, so a hold
/// that outlasts this budget is a second live writer, not an exit in
/// progress, and is reported as the refusal it is.
const LEASE_HELD_RETRY_BUDGET: Duration = Duration::from_secs(10);
const LEASE_HELD_RETRY_FIRST_DELAY: Duration = Duration::from_millis(50);
const LEASE_HELD_RETRY_MAX_DELAY: Duration = Duration::from_secs(1);

#[derive(Default)]
struct HealthGauges {
    store_ready: AtomicBool,
    store_failed: AtomicBool,
    /// True while the open is being retried against a lease a predecessor
    /// still holds; health reports degraded rather than failing, so the
    /// daemon serves `module_warming` to callers instead of a fault.
    store_opening: AtomicBool,
    generation: AtomicI64,
    resolve_count: AtomicU64,
    enumerate_count: AtomicU64,
    journal_tail_count: AtomicU64,
    last_operation_ms: AtomicI64,
    liveness_batches: AtomicU64,
    liveness_gaps: AtomicU64,
    liveness_sessions: AtomicU64,
    /// Batches refused for carrying a seq at or below the last applied one.
    /// Separate from `liveness_gaps` on purpose: a gap means "a batch I never
    /// saw may exist", while this means "a batch arrived out of order". After
    /// the emitter delivers in seq order this must stay 0 forever, so any
    /// nonzero value is a producer regression rather than a tolerated event.
    liveness_dropped_older: AtomicU64,
    /// Whether the first out-of-order refusal of this incarnation has been
    /// logged. Refusing quietly would let that regression hide behind a
    /// counter nobody reads.
    dropped_older_logged: AtomicBool,
    log_state: AtomicU64,
    log_own_entries_applied: AtomicU64,
    log_mode: AtomicU64,
    log_applied: AtomicI64,
    log_head: AtomicI64,
    log_pending: AtomicI64,
    log_error: AtomicU64,
    log_error_at: AtomicI64,
}

/// Volatile liveness state fed by `projects.session_liveness` batches. Never
/// journaled (I8 forbids liveness in the topology journal); it exists to
/// annotate enumerate answers and to feed future GC-candidacy signals, so a
/// wrong, late, or absent feed can never corrupt topology.
struct LivenessState {
    /// Last accepted per-emitter sequence number; None until first contact.
    last_seq: Option<u64>,
    /// True until a snapshot has rebased the mirror: from startup, and again
    /// after a dropped or refused batch.
    stale: bool,
    /// session_id -> (state, canonical_root, last_activity_ms).
    sessions: BTreeMap<String, (String, String, i64)>,
}

impl Default for LivenessState {
    /// A mirror that has never received a snapshot is incomplete, so it starts
    /// stale. The emitter's session set outlives this process: when only this
    /// module restarts, the emitter keeps sending deltas from its own seq, and
    /// a mirror that took the first delta as complete would hold only the
    /// sessions that happened to change since, until every live session
    /// changed again. Starting stale makes the first reply ask for a snapshot.
    fn default() -> Self {
        Self {
            last_seq: None,
            stale: true,
            sessions: BTreeMap::new(),
        }
    }
}

struct ProjectsHandler {
    store: Arc<Mutex<Option<RegistryStore>>>,
    health: Arc<HealthGauges>,
    liveness: Arc<Mutex<LivenessState>>,
    /// What the daemon stamped on each route when it was bound. The daemon
    /// stamps it once, at `route.bind`, never per request, so it is recorded
    /// here and looked up for every request on that route.
    route_admissions: Arc<Mutex<HashMap<RouteKey, RouteAdmission>>>,
    /// Volatile reply token: never persisted in the request-key cache.
    incarnation: String,
    clock: fn() -> i64,
    commits: Arc<tokio::sync::Notify>,
    feed_waits: tokio::sync::Semaphore,
    feed_clock: Arc<dyn agent_reads::FeedClock>,
    /// Unit tests shorten this policy; production uses the ten-second default.
    lease_retry_budget: Duration,
    #[allow(dead_code)]
    log_client: log_client::LogClient,
    writer: Arc<tokio::sync::Mutex<()>>,
    catch_up_task: Mutex<Option<tokio::task::JoinHandle<()>>>,
    agent_id_draw: fn() -> Result<String, HandlerError>,
    operator_confirmer: Arc<dyn agent_ops::OperatorConfirmer>,
    confirmation_slot: Arc<Mutex<Option<agent_ops::ConfirmationSlot>>>,
    #[cfg(test)]
    confirmation_admission_barrier: Option<Arc<tokio::sync::Barrier>>,
    #[cfg(test)]
    shared_commit_hook: Option<Arc<dyn Fn() + Send + Sync>>,
}

/// The parts of a route's bind stamp that decide what the route may do here.
#[derive(Clone, Debug, Default)]
struct RouteAdmission {
    principal: Option<Principal>,
    /// The flow whose scope the route was opened under, if any.
    flow_id: Option<String>,
    /// Retain the SDK's connection identity until this admission is removed.
    handle: Option<RouteHandle>,
}

impl RouteAdmission {
    fn from_bind(principal: Option<Principal>, scope: Option<&ScopeStamp>) -> Self {
        Self {
            principal,
            flow_id: scope.and_then(|stamp| stamp.attributes.flow_id.clone()),
            handle: None,
        }
    }
}

type RouteKey = (u16, u32);

fn route_key(handle: &RouteHandle) -> RouteKey {
    (handle.channel, handle.epoch)
}

/// The one module besides the operator that may change the registry: the
/// executive registers repositories, records session liveness, and writes the
/// user's decisions.
const WRITER_MODULE: &str = "prefrontal-core";

/// Methods that change the registry or the liveness state derived from it.
const MUTATING_METHODS: &[&str] = &[
    "register",
    "assign_workspace",
    "set_workspace_root",
    "set_owned_remotes",
    "upgrade_implicit",
    "remove",
    "seed_import",
    "rebuild",
    "projects.session_liveness",
    "add_root",
    "attach_root",
    "identity_log.enable",
    "remove_root",
    "attach_derived_parent",
    "approve_root",
    "unapprove_root",
    "approve_project",
    "unapprove_project",
    "agent.create",
    "agent.rename",
    "agent.update_tag",
    "agent.set_labels",
    "agent.set_avatar",
    "agent.set_github_identity",
    "agent.dispose",
    "agent.merge",
    "agent.import",
];

/// Admit project/workspace mutations from the operator (`Direct`: the `ck`
/// faces) or executive, but identity mutations only from the executive relay.
/// An agent's shell can connect as Direct, so Direct cannot rewrite identities.
/// Enabling the shared log is an operator decision, never an executive write.
///
/// This stops a module that has no business writing here from changing the
/// registry by accident or through a bug. Project ids key other modules' state,
/// so a stray `remove` would cut a project off from it. It is not a security
/// boundary: any process running as the user can connect as `Direct` or open
/// the store file itself. A route with no recorded principal is refused rather
/// than assumed to be the operator.
fn authorize_write(method: &str, principal: Option<&Principal>) -> Result<(), HandlerError> {
    if method == "identity_log.enable" {
        return match principal {
            Some(Principal::Direct) => Ok(()),
            other => Err(HandlerError::new(
                "write_not_permitted",
                format!("'{method}' is accepted only from the operator (ck); this route was opened by {}", principal_label(other)),
            )),
        };
    }
    if agent_ops::MUTATING_METHODS.contains(&method) {
        return match principal {
            Some(Principal::Reserved { module_id }) if module_id == WRITER_MODULE => Ok(()),
            Some(Principal::Direct) => Err(HandlerError::new(
                "direct_identity_write_not_admitted",
                format!("'{method}' must be called through {WRITER_MODULE}'s relay; Direct agent identity writes are not admitted"),
            )),
            other => Err(HandlerError::new(
                "write_not_permitted",
                format!("'{method}' changes agent identity and is accepted only from reserved:{WRITER_MODULE}; this route was opened by {}", principal_label(other)),
            )),
        };
    }
    if !MUTATING_METHODS.contains(&method) {
        return Ok(());
    }
    match principal {
        Some(Principal::Direct) => Ok(()),
        Some(Principal::Reserved { module_id }) if module_id == WRITER_MODULE => Ok(()),
        other => Err(HandlerError {
            code: "write_not_permitted".to_string(),
            detail: None,
            message: format!(
                "'{method}' changes the registry and is accepted only from the operator (ck) or {WRITER_MODULE}; this route was opened by {}",
                principal_label(other)
            ),
        }),
    }
}

/// Refuse a mutating method on a route opened under a flow's scope, whoever
/// opened it.
///
/// A flow acts as its owner agent, and the executive can open a route under a
/// flow's scope, so that route carries the executive's principal. Admitting it
/// on the principal alone would let a flow change the registry with authority
/// granted only to the operator and the executive. No flow needs to write
/// here, so every mutation on a flow-scoped route refuses. Reads stay open
/// because they answer every caller alike.
fn refuse_flow_write(method: &str, flow_id: Option<&str>) -> Result<(), HandlerError> {
    match flow_id {
        Some(flow_id) if MUTATING_METHODS.contains(&method) => Err(HandlerError {
            code: "flow_scope_not_admitted".to_string(),
            detail: None,
            message: format!(
                "'{method}' changes the registry and is not accepted on a route opened under flow '{flow_id}'"
            ),
        }),
        _ => Ok(()),
    }
}

fn principal_label(principal: Option<&Principal>) -> String {
    match principal {
        Some(Principal::Direct) => "direct".to_string(),
        Some(Principal::Reserved { module_id }) => format!("reserved:{module_id}"),
        Some(Principal::Unverified) => "unverified".to_string(),
        None => "an unknown route".to_string(),
    }
}

impl ProjectsHandler {
    fn new() -> Result<Self, std::io::Error> {
        let incarnation = incarnation::new_incarnation().map_err(|error| {
            std::io::Error::other(format!("cannot mint process incarnation: {error}"))
        })?;
        Ok(Self::with_runtime(incarnation, unix_millis))
    }

    fn with_runtime(incarnation: String, clock: fn() -> i64) -> Self {
        Self::with_log_connector(
            incarnation,
            clock,
            Arc::new(log_client::SubcConnector::from_args(
                std::env::args_os().skip(1),
            )),
        )
    }

    fn with_log_connector(
        incarnation: String,
        clock: fn() -> i64,
        connector: Arc<dyn log_client::LogConnector>,
    ) -> Self {
        let route_admissions = Arc::new(Mutex::new(HashMap::new()));
        let operator_confirmer =
            Arc::new(agent_ops::DaemonConfirmer::new(route_admissions.clone()));
        Self {
            store: Arc::new(Mutex::new(None)),
            health: Arc::new(HealthGauges::default()),
            liveness: Arc::new(Mutex::new(LivenessState::default())),
            route_admissions,
            incarnation,
            clock,
            commits: Arc::new(tokio::sync::Notify::new()),
            feed_waits: tokio::sync::Semaphore::new(8),
            feed_clock: Arc::new(agent_reads::TokioFeedClock),
            lease_retry_budget: LEASE_HELD_RETRY_BUDGET,
            log_client: log_client::LogClient::new(connector),
            writer: Arc::new(tokio::sync::Mutex::new(())),
            catch_up_task: Mutex::new(None),
            agent_id_draw: shared_write::draw_agent_id,
            operator_confirmer,
            confirmation_slot: Arc::new(Mutex::new(None)),
            #[cfg(test)]
            confirmation_admission_barrier: None,
            #[cfg(test)]
            shared_commit_hook: None,
        }
    }

    fn route_admissions(&self) -> std::sync::MutexGuard<'_, HashMap<RouteKey, RouteAdmission>> {
        self.route_admissions
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// An unknown route gets the empty admission: no principal, which
    /// `authorize_write` refuses rather than treating as the operator.
    fn admission_for(&self, key: RouteKey) -> RouteAdmission {
        self.route_admissions()
            .get(&key)
            .cloned()
            .unwrap_or_default()
    }

    /// Whether a request on this route may run `method`. A flow-scoped route may
    /// not write at all. Project writes are admitted from `Direct` (the `ck`
    /// faces) or the executive, as before. Agent identity writes are admitted
    /// here only from the executive (`reserved:prefrontal-core`): every local
    /// process, including an agent's shell, reaches entorhinal as `Direct`.
    /// The one way a `Direct` caller writes agent identity is `write_wait`,
    /// which, for five methods only, asks the operator to approve the exact
    /// write at a Touch ID prompt first. Nothing else skips this refusal.
    /// Enabling the identity log is reserved to an unscoped operator route.
    fn admit(&self, method: &str, key: RouteKey) -> Result<RouteAdmission, HandlerError> {
        let admission = self.admission_for(key);
        refuse_flow_write(method, admission.flow_id.as_deref())?;
        authorize_write(method, admission.principal.as_ref())?;
        Ok(admission)
    }

    fn with_store<T>(
        &self,
        operation: impl FnOnce(&RegistryStore) -> Result<T, RegistryError>,
    ) -> Result<T, HandlerError> {
        let guard = self
            .store
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let store = guard.as_ref().cloned().ok_or_else(|| HandlerError {
            code: "storage_unavailable".to_string(),
            message: "projects storage is not ready".to_string(),
            detail: None,
        })?;
        // The slot protects installation only. SQLite's writer connection lock
        // still serializes whole mutation transactions; reads have a separate
        // connection and must not queue behind a writer holding that lock.
        drop(guard);
        operation(&store).map_err(|error| HandlerError {
            code: match &error {
                RegistryError::Domain { code, .. } => code.clone(),
                _ => "storage_error".to_string(),
            },
            message: error.to_string(),
            detail: None,
        })
    }

    fn record_query(&self, generation: i64, counter: &AtomicU64) {
        self.health
            .generation
            .fetch_max(generation, Ordering::Relaxed);
        counter.fetch_add(1, Ordering::Relaxed);
        self.health
            .last_operation_ms
            .store(unix_millis(), Ordering::Relaxed);
    }
}

#[async_trait]
impl ModuleHandler for ProjectsHandler {
    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        self.handle_served_request(&body, route_key(&ctx.route_handle()))
            .await
    }

    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        self.route_admissions().insert(
            route_key(&request.handle),
            RouteAdmission {
                handle: Some(request.handle),
                ..RouteAdmission::from_bind(request.principal.clone(), request.scope.as_ref())
            },
        );
        BindDecision::accept()
    }

    async fn on_route_gone(&self, handle: &RouteHandle) {
        let key = route_key(handle);
        self.route_admissions().remove(&key);
        if let Some(slot) = self
            .confirmation_slot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
        {
            if slot.route == key {
                slot.cancelled.send_replace(true);
            }
        }
    }

    async fn on_hello_ack(&self, ack: &ModuleHelloAckBody) {
        let descriptor = match ack.storage.as_ref() {
            Some(value) => serde_json::from_value::<StorageDescriptor>(value.clone())
                .map_err(|error| format!("HELLO_ACK storage descriptor is invalid: {error}")),
            None => Ok(fallback_storage_descriptor()),
        };
        let descriptor = match descriptor {
            Ok(descriptor) => descriptor,
            Err(error) => {
                install_store(&self.store, &self.health, Err(error));
                return;
            }
        };

        match RegistryStore::open(&descriptor) {
            Err(error) if error.is_lease_held() => {
                // The predecessor is still exiting. Retry off this path so the
                // HELLO_ACK handler and the health reply stay prompt.
                tracing::warn!(
                    target: "store",
                    "opening projects storage: {error}; retrying for up to {:.1}s",
                    self.lease_retry_budget.as_secs_f64()
                );
                self.health.store_opening.store(true, Ordering::Relaxed);
                let store = Arc::clone(&self.store);
                let health = Arc::clone(&self.health);
                let retry_budget = self.lease_retry_budget;
                tokio::spawn(async move {
                    let outcome = retry_open_while_lease_held(&descriptor, retry_budget).await;
                    health.store_opening.store(false, Ordering::Relaxed);
                    install_store(&store, &health, outcome);
                });
            }
            outcome => install_store(
                &self.store,
                &self.health,
                outcome.map_err(|error| format!("opening projects storage: {error}")),
            ),
        }
        self.start_catch_up();
    }

    async fn health(&self) -> HealthReport {
        // Health is deliberately insulated from the store mutex and database. The
        // daemon's health path must remain responsive even when a domain request is
        // blocked or the store has failed; only cached atomic gauges are read here.
        let ready = self.health.store_ready.load(Ordering::Relaxed);
        let failed = self.health.store_failed.load(Ordering::Relaxed);
        let opening = self.health.store_opening.load(Ordering::Relaxed);
        let status = if opening {
            HealthStatus::Degraded
        } else if failed || !ready {
            HealthStatus::Failing
        } else if self.health.log_state.load(Ordering::Relaxed) != 0 {
            HealthStatus::Degraded
        } else {
            HealthStatus::Ok
        };
        let detail = if opening {
            Some("projects storage lease is held by an exiting predecessor; retrying".to_string())
        } else {
            (!ready).then(|| "projects storage is not ready".to_string())
        };
        HealthReport {
            status,
            detail,
            metrics: Some(json!({
                "storeReady": ready,
                "generation": self.health.generation.load(Ordering::Relaxed),
                "resolveCount": self.health.resolve_count.load(Ordering::Relaxed),
                "enumerateCount": self.health.enumerate_count.load(Ordering::Relaxed),
                "journalTailCount": self.health.journal_tail_count.load(Ordering::Relaxed),
                "lastOperationMs": self.health.last_operation_ms.load(Ordering::Relaxed),
                "livenessBatches": self.health.liveness_batches.load(Ordering::Relaxed),
                "livenessGaps": self.health.liveness_gaps.load(Ordering::Relaxed),
                "livenessDroppedOlder": self.health.liveness_dropped_older.load(Ordering::Relaxed),
                "livenessSessions": self.health.liveness_sessions.load(Ordering::Relaxed),
                "identityLogState": enable::status_state(&self.health),
                "lastAppliedPosition": self.health.log_applied.load(Ordering::Relaxed),
                "lastSeenHead": self.health.log_head.load(Ordering::Relaxed),
                "pendingWriteCount": self.health.log_pending.load(Ordering::Relaxed),
                "lastError": enable::last_error(&self.health),
                "lastErrorAt": enable::last_error_at(&self.health),
                "logOwnEntriesAppliedFromLog": self.health.log_own_entries_applied.load(Ordering::Relaxed),
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
    /// Decoded requests that change state use `write_wait`, which handles cached
    /// replies, serializes database writes, and sends shared changes to the log.
    /// It calls synchronous `execute` for operations that only change local state.
    /// Queries use `handle_request_wait` instead, without joining the write queue.
    async fn handle_served_request(&self, body: &[u8], key: RouteKey) -> HandlerOutcome {
        let received = tokio::time::Instant::now();
        let request = match serde_json::from_slice::<WireRequest>(body) {
            Ok(request) => request,
            Err(_) => return self.handle_request(body, key),
        };
        if !MUTATING_METHODS.contains(&request.method.as_str()) {
            return self.handle_request_wait(body, key).await;
        }
        let result = if request.method == "identity_log.enable" {
            self.enable_log(request.params, key, received).await
        } else {
            self.write_wait(request, key, received).await
        };
        match result {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => error.into_outcome(),
        }
    }

    /// The same envelope/admission/response path serves SUBC and the route seam
    /// used by tests, without constructing a transport or inventing a principal.
    fn handle_request(&self, body: &[u8], key: RouteKey) -> HandlerOutcome {
        let request = match serde_json::from_slice::<WireRequest>(body) {
            Ok(request) => request,
            Err(error) => {
                return HandlerError::new(
                    "invalid_request",
                    format!("request must be JSON with method and params: {error}"),
                )
                .into_outcome();
            }
        };
        match self.execute(request, key) {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => error.into_outcome(),
        }
    }

    /// The principal saved when this route was bound decides whether the request
    /// is admitted, and the same value is recorded on every journal row the
    /// request writes, including the `bind_root` rows a `register` or `add_root`
    /// appends after its own row. Reading it once means the two can't disagree.
    fn execute(&self, request: WireRequest, key: RouteKey) -> Result<Vec<u8>, HandlerError> {
        let admission = self.admit(&request.method, key)?;
        let principal = principal_label(admission.principal.as_ref());
        // Notify after the commit, not from a poll timer. Comparing the store's
        // head also catches secondary bind_root commits and partial successes;
        // cached retries and volatile liveness updates don't wake idle readers.
        let mutating = MUTATING_METHODS.contains(&request.method.as_str());
        let before = mutating.then(|| self.with_store(RegistryStore::generation).ok());
        let result = match request.method.as_str() {
            "resolve" => self.resolve(request.params),
            "resolve_project_id" => self.resolve_project_id(request.params),
            "resolve_remote" => self.resolve_remote(request.params),
            "resolve_root_key" => self.resolve_root_key(request.params),
            "preview_attach_root" => self.preview_attach_root(request.params),
            "identity_log.status" => self.identity_log_status(),
            "identity_log.enable" => Err(HandlerError::new(
                "engram_unavailable",
                "identity log enabling isn't available in this build yet",
            )),
            "enumerate" => self.enumerate(request.params),
            "journal_tail" => self.journal_tail(request.params),
            "register" => self.register(request.params, &principal),
            "assign_workspace" => self.assign_workspace(request.params, &principal),
            "set_workspace_root" => self.set_workspace_root(request.params, &principal),
            "set_owned_remotes" => self.set_owned_remotes(request.params, &principal),
            "upgrade_implicit" => self.upgrade_implicit(request.params, &principal),
            "remove" => self.remove(request.params, &principal),
            "seed_import" => self.seed_import(request.params, &principal),
            "projects.session_liveness" => self.session_liveness(request.params),
            "add_root" => self.add_root(request.params, &principal),
            "attach_root" => self.attach_root(request.params, &principal),
            "remove_root" => self.remove_root(request.params, &principal),
            "attach_derived_parent" => self.attach_derived_parent(request.params, &principal),
            "approve_root" => self.set_root_approval(request.params, true, &principal),
            "unapprove_root" => self.set_root_approval(request.params, false, &principal),
            "trust" => self.trust(request.params),
            "approve_project" => self.set_project_approval(request.params, true, &principal),
            "unapprove_project" => self.set_project_approval(request.params, false, &principal),
            "verify" => self.verify(),
            "rebuild" => self.rebuild(),
            method if agent_ops::MUTATING_METHODS.contains(&method) => {
                self.agent_mutation(method, request.params, &principal)
            }
            method if agent_reads::READ_METHODS.contains(&method) => {
                self.agent_read(method, request.params)
            }
            _ => Err(HandlerError {
                code: "unknown_method".to_string(),
                detail: None,
                message: format!(
                    "unknown projects method '{}', see the management manifest",
                    request.method
                ),
            }),
        };
        if mutating && before.flatten() != self.with_store(RegistryStore::generation).ok() {
            self.commits.notify_waiters();
        }
        result
    }

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

        // Set when the batch is refused for arriving out of order, carrying the
        // last seq that was applied, for the log line below.
        let mut dropped_after: Option<u64> = None;

        if batch.snapshot {
            // A snapshot is authoritative and replaces the whole mirror, so it
            // is accepted whatever its seq says: a restarted emitter legally
            // rebases to a lower seq. An EMPTY snapshot is information, not a
            // no-op -- it states the emitter tracks nothing, which is the only
            // way a mirror still holding sessions whose `gone` was lost can be
            // emptied again.
            state.sessions.clear();
            state.stale = false;
            state.last_seq = Some(batch.seq);
        } else if let Some(last) = state.last_seq {
            if batch.seq <= last {
                // Refuse it instead of applying it. Liveness is latest-wins per
                // session, so a batch older than one already applied installs
                // state that was already superseded. The case that bites is a
                // stale non-`gone` entry landing after the `gone` that removed
                // the session: it reinserts an entry whose terminal signal is
                // already spent, and nothing would ever prune it again. That
                // entry then keeps its project's last_route_activity_ms alive
                // forever, which is the GC-candidacy signal this feed exists to
                // produce.
                //
                // Ask for a rebase too. Refusing alone would be fail-deaf: an
                // emitter that restarts its process-lifetime counter and loses
                // its reconnect snapshot in transit would have every later
                // delta look older than the pre-restart high-water mark, and
                // this handler would ignore the feed indefinitely. Requesting a
                // snapshot makes the refusal recoverable in one round trip.
                state.stale = true;
                dropped_after = Some(last);
            } else {
                if batch.seq != last.wrapping_add(1) {
                    state.stale = true;
                    self.health.liveness_gaps.fetch_add(1, Ordering::Relaxed);
                }
                state.last_seq = Some(batch.seq);
            }
        } else {
            // First contact: any seq is the baseline, since there is nothing to
            // be out of order with. The mirror stays stale until a snapshot
            // arrives, because a delta says nothing about the sessions that
            // did not change.
            state.last_seq = Some(batch.seq);
        }

        // Request a snapshot until one actually rebases us: staleness is a
        // LATCH, not a per-batch flag. If only the freshly-detected gap
        // answered true, a lost snapshot re-emit would leave the state stale
        // forever while every later contiguous delta reported clean.
        let snapshot_requested = state.stale;

        if dropped_after.is_none() {
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
        }
        let tracked = state.sessions.len() as u64;
        drop(state);

        if let Some(last) = dropped_after {
            self.health
                .liveness_dropped_older
                .fetch_add(1, Ordering::Relaxed);
            if !self
                .health
                .dropped_older_logged
                .swap(true, Ordering::Relaxed)
            {
                tracing::warn!(
                    target: "liveness",
                    seq = batch.seq,
                    last_applied_seq = last,
                    "refused a session_liveness batch at or below the last applied seq and asked \
                     for a snapshot; either the emitter restarted its counter without sending a \
                     snapshot first, or it is delivering out of seq order"
                );
            }
        }

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
            detail: None,
        })
    }

    fn resolve(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<ResolveParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| {
            store.resolve_with_binding(&params.canonical_root, params.execution_binding.as_ref())
        })?;
        self.record_query(reply.generation, &self.health.resolve_count);
        self.encode_read_result(reply)
    }

    fn resolve_project_id(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params =
            serde_json::from_value::<ResolveProjectIdParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.resolve_project_id(&params.project_id))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        self.encode_read_result(reply)
    }

    fn enumerate(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<EnumerateParams>(params).map_err(invalid_params)?;
        let mut reply = self.with_store(|store| store.enumerate(params.workspace_id.as_deref()))?;
        self.annotate_liveness(&mut reply);
        self.record_query(reply.generation, &self.health.enumerate_count);
        self.encode_read_result(reply)
    }

    fn resolve_remote(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params =
            serde_json::from_value::<ResolveRemoteParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.resolve_remote(&params.owner, &params.repo))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        self.encode_read_result(reply)
    }

    fn set_owned_remotes(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let request = serde_json::from_value::<entorhinal_core::SetOwnedRemotesRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(
            self.with_store(|s| s.with_principal(principal).set_owned_remotes(request)),
        )
    }

    fn resolve_root_key(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request = serde_json::from_value::<entorhinal_core::ResolveRootKeyRequest>(params)
            .map_err(invalid_params)?;
        let reply = self.with_store(|s| s.resolve_root_key(request))?;
        self.encode_read_result(reply)
    }

    fn preview_attach_root(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request = serde_json::from_value::<entorhinal_core::AttachRootRequest>(params)
            .map_err(invalid_params)?;
        let reply = self.with_store(|s| s.preview_attach_root(request))?;
        self.encode_read_result(reply)
    }

    fn identity_log_status(&self) -> Result<Vec<u8>, HandlerError> {
        let reply = self.with_store(RegistryStore::identity_log_status)?;
        enable::refresh_status(&self.health, &reply);
        let mut reply = serde_json::to_value(reply).map_err(invalid_params)?;
        reply["state"] = json!(enable::status_state(&self.health));
        reply["lastError"] = json!(enable::last_error(&self.health));
        reply["lastErrorAt"] = json!(enable::last_error_at(&self.health));
        self.encode_read_result(reply)
    }

    fn attach_root(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let request = serde_json::from_value::<entorhinal_core::AttachRootRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).attach_root(request)))
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
        self.encode_read_result(reply)
    }

    fn register(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RegisterRequest>(params).map_err(invalid_params)?;
        let reply =
            self.record_mutation(self.with_store(|s| s.with_principal(principal).register(req)))?;
        // With root records on, a newly registered root is bound at once, so
        // it never answers as unbound. Unreadable identity leaves it unbound
        // and authorizing nothing; the registration itself still stands.
        self.with_store(|store| {
            if store.root_records_enabled() {
                if let Err(error) = store
                    .with_principal(principal)
                    .bind_unbound_roots("entorhinal")
                {
                    tracing::warn!(target: "binding", "binding after register failed: {error}");
                }
            }
            Ok(())
        })?;
        Ok(reply)
    }
    fn set_workspace_root(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<SetWorkspaceRootRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| {
            s.with_principal(principal)
                .set_workspace_root_at(req, (self.clock)())
        }))
    }

    fn assign_workspace(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<AssignWorkspaceRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).assign_workspace(req)))
    }
    fn upgrade_implicit(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<UpgradeImplicitRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).upgrade_implicit(req)))
    }
    fn remove(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RemoveRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).remove(req)))
    }
    fn add_root(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::AddRootRequest>(params)
            .map_err(invalid_params)?;
        let reply =
            self.record_mutation(self.with_store(|s| s.with_principal(principal).add_root(req)))?;
        // As after register: bind at once, so the new root never answers as
        // unbound while root records are on. Binding never approves.
        self.with_store(|store| {
            if store.root_records_enabled() {
                if let Err(error) = store
                    .with_principal(principal)
                    .bind_unbound_roots("entorhinal")
                {
                    tracing::warn!(target: "binding", "binding after add_root failed: {error}");
                }
            }
            Ok(())
        })?;
        Ok(reply)
    }
    fn remove_root(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::RemoveRootRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).remove_root(req)))
    }
    fn attach_derived_parent(
        &self,
        params: Value,
        principal: &str,
    ) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::AttachDerivedParentRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(
            self.with_store(|s| s.with_principal(principal).attach_derived_parent(req)),
        )
    }
    fn set_root_approval(
        &self,
        params: Value,
        approve: bool,
        principal: &str,
    ) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RootApprovalParams>(params).map_err(invalid_params)?;
        let actor = req.actor.unwrap_or_else(|| "operator".to_string());
        self.record_mutation(self.with_store(|s| {
            if approve {
                s.with_principal(principal).approve_root(&req.root, &actor)
            } else {
                s.with_principal(principal)
                    .unapprove_root(&req.root, &actor)
            }
        }))
    }
    fn set_project_approval(
        &self,
        params: Value,
        approve: bool,
        principal: &str,
    ) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<ProjectApprovalParams>(params).map_err(invalid_params)?;
        let actor = req.actor.unwrap_or_else(|| "operator".to_string());
        self.record_mutation(self.with_store(|s| {
            s.with_principal(principal)
                .set_project_approval(&req.canonical_root, &actor, approve)
        }))
    }
    fn trust(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<ResolveParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.trust(&params.canonical_root))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        self.encode_read_result(reply)
    }
    fn seed_import(&self, params: Value, principal: &str) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<SeedImportRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.with_principal(principal).seed_import(req)))
    }

    /// Refresh the health generation gauge from a mutation's wire reply, which
    /// carries the post-mutation generation. Without this the gauge reports
    /// the pre-mutation generation until the next query-side op runs.
    /// Cached retries carry an older generation; they must not lower the gauge.
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
                    self.health
                        .generation
                        .fetch_max(generation, Ordering::Relaxed);
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
        self.encode_read_result(reply)
    }
    fn rebuild(&self) -> Result<Vec<u8>, HandlerError> {
        let reply = self.with_store(|s| s.rebuild())?;
        encode_result(reply)
    }
}

#[derive(Debug)]
struct HandlerError {
    code: String,
    message: String,
    detail: Option<Value>,
}

impl HandlerError {
    fn new(code: &str, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            detail: None,
        }
    }

    fn into_outcome(self) -> HandlerOutcome {
        match self.detail {
            Some(detail) => HandlerOutcome::ErrorWithDetail {
                code: self.code,
                message: self.message,
                detail,
            },
            None => HandlerOutcome::Error {
                code: self.code,
                message: self.message,
            },
        }
    }
}

fn invalid_params(error: serde_json::Error) -> HandlerError {
    HandlerError {
        code: "invalid_params".to_string(),
        message: error.to_string(),
        detail: None,
    }
}

fn encode_result<T: serde::Serialize>(result: T) -> Result<Vec<u8>, HandlerError> {
    serde_json::to_vec(&json!({ "result": result })).map_err(|error| HandlerError {
        code: "encode_failed".to_string(),
        message: error.to_string(),
        detail: None,
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
    /// What a caller captured about the root an execution was admitted under.
    /// Answered in `bindingStatus` when root records are enabled.
    #[serde(default, rename = "executionBinding")]
    execution_binding: Option<entorhinal_core::ExecutionBinding>,
}

#[derive(Debug, Deserialize)]
struct ProjectApprovalParams {
    #[serde(rename = "canonicalRoot")]
    canonical_root: String,
    #[serde(default)]
    actor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RootApprovalParams {
    root: String,
    #[serde(default)]
    actor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ResolveProjectIdParams {
    #[serde(rename = "projectId", alias = "project_id")]
    project_id: String,
}

#[derive(Deserialize)]
struct ResolveRemoteParams {
    owner: String,
    repo: String,
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

/// The capability identifier entorhinal provides; consumers require it by this name.
const PROJECT_IDENTITY_CAPABILITY: &str = "project-identity/v1";

fn manifest() -> ModuleManifest {
    ModuleManifest::builder(MODULE_ID, env!("CARGO_PKG_VERSION"))
    .protocol_ver(PROTOCOL_VERSION)
    .provides(vec![ProviderRole::ManagementSurface {
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
                    "resolve_remote",
                    ManagementOperationKind::Query,
                    "Find the project owning a GitHub repository through its live owned remotes.",
                ),
                management_operation(
                    "resolve_root_key",
                    ManagementOperationKind::Query,
                    "List local roots mapped to a project's machine-neutral root key.",
                ),
                management_operation(
                    "preview_attach_root",
                    ManagementOperationKind::Query,
                    "Match a checkout to an existing project and root key without attaching it.",
                ),
                management_operation(
                    "identity_log.status",
                    ManagementOperationKind::Query,
                    "Read local identity log state, progress and pending-write count without contacting engram.",
                ),
                management_operation(
                    "identity_log.enable",
                    ManagementOperationKind::Mutate,
                    "Enable the shared identity log or join it (operator only).",
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
                    "set_workspace_root",
                    ManagementOperationKind::Mutate,
                    "Set or clear a workspace's root directory (operator-set, never derived).",
                ),
                management_operation(
                    "set_owned_remotes",
                    ManagementOperationKind::Mutate,
                    "Replace a root's owned remote names, or reset to origin with null.",
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
                management_operation(
                    "add_root",
                    ManagementOperationKind::Mutate,
                    "Add a root to a registered project; it starts unapproved.",
                ),
                management_operation(
                    "attach_root",
                    ManagementOperationKind::Mutate,
                    "Attach a local checkout to an existing project's root key; it starts unapproved.",
                ),
                management_operation(
                    "remove_root",
                    ManagementOperationKind::Mutate,
                    "Remove one root from a project, retiring its binding and worker containers.",
                ),
                management_operation(
                    "attach_derived_parent",
                    ManagementOperationKind::Mutate,
                    "Attach a worker container to a root's current approved binding.",
                ),
                management_operation(
                    "approve_root",
                    ManagementOperationKind::Mutate,
                    "Approve a root's current binding for autonomous work.",
                ),
                management_operation(
                    "unapprove_root",
                    ManagementOperationKind::Mutate,
                    "Withdraw a root's approval.",
                ),
                management_operation(
                    "approve_project",
                    ManagementOperationKind::Mutate,
                    "Approve every root of the project a path belongs to; refused unless all are identified.",
                ),
                management_operation(
                    "unapprove_project",
                    ManagementOperationKind::Mutate,
                    "Withdraw approval from every root of the project a path belongs to.",
                ),
                management_operation(
                    "trust",
                    ManagementOperationKind::Query,
                    "Show the project a path belongs to and each root's identity and approval.",
                ),
                // Volatile liveness ingestion: never journaled, it only feeds the
                // dead-folder annotations on enumerate. The executive produces it.
                management_operation(
                    "projects.session_liveness",
                    ManagementOperationKind::Mutate,
                    "Ingest session liveness snapshots and deltas from the executive for dead-folder detection.",
                ),
            ].into_iter().chain(agent_reads::READ_METHODS.iter().map(|method| {
                management_operation(method, ManagementOperationKind::Query, agent_operation_description(method))
            })).chain(agent_ops::MUTATING_METHODS.iter().map(|method| {
                management_operation(method, ManagementOperationKind::Mutate, agent_operation_description(method))
            })).collect(),
            config_schema: json!({"type": "object"}),
            observability: Vec::new(),
            identity_scope: Vec::new(),
            // Registry ops use short SQLite locks. Change-feed waits suspend
            // their task outside that lock and are capped by the module.
            concurrency: Concurrency::ModuleManaged,
        }])
    // entorhinal declares no self-signals yet: its ops are operator-driven
    // registry reads/writes, not autonomous signals.
    .self_signals(None)
    // Which source commit and wire version this binary is, so `ck provenance
    // entorhinal` can answer. At HELLO the SDK adds where this process got its
    // launch nonce: from the pipe the daemon hands over on a file descriptor,
    // or from the SUBC_LAUNCH_NONCE environment variable. The daemon will stop
    // setting that variable only once every module reports the pipe, so a
    // module that declares no provenance holds that step up.
    .provenance(declared_provenance())
    // The capabilities other modules declare `required` when they cannot work
    // without project or agent identity. The daemon holds a module not-ready while a
    // capability it requires has no registered provider, so this name is what
    // makes that dependency enforceable. Breaking changes to either identity
    // surface require a new capability version.
    .capabilities(Some(CapabilityDeclarations {
        provides: vec![PROJECT_IDENTITY_CAPABILITY.to_owned(), "agent-identity/v1".to_owned()],
        // Entorhinal calls engram (the module hosting the shared identity log)
        // only to write shared state, and serves every read without it. So
        // nothing is declared here: a `requires` entry makes the daemon refuse
        // every route to entorhinal, reads included, while engram is down. The
        // daemon admits a route that entorhinal opens to engram without one.
        requires: Vec::new(),
        must_never_reach: Vec::new(),
    }))
    .build()
}

/// Build facts embedded by `build.rs`. Debug builds intentionally omit a git
/// stamp and explain that provenance is stamped only for release builds. A
/// revision from a dirty release tree is declined with a reason, while a source
/// tarball reports that Git is unavailable.
fn declared_provenance() -> Option<ManifestProvenance> {
    if option_env!("ENTORHINAL_BUILD_PROFILE").is_none()
        && option_env!("ENTORHINAL_BUILD_REV").is_none()
    {
        return Some(
            ManifestProvenance::new()
                .with_build_git_sha_absence_reason(Some(
                    BuildGitShaAbsenceReason::ForwardCompatibleUnknown(
                        "provenance_stamped_only_in_release_builds".to_string(),
                    ),
                ))
                .with_wire_crate_version(Some(
                    subc_protocol::SUBC_PROTOCOL_CRATE_VERSION.to_string(),
                )),
        );
    }

    let source = match (
        option_env!("ENTORHINAL_BUILD_REV"),
        option_env!("ENTORHINAL_BUILD_TREE"),
    ) {
        (Some(revision), Some(tree)) => BuildGitShaSource::Git {
            revision,
            tree_state: if tree == "clean" {
                GitTreeState::Clean
            } else {
                GitTreeState::Dirty
            },
        },
        _ => BuildGitShaSource::NoGitDir,
    };
    // `build.rs` only emits a full 40-character hex revision, so a form error
    // here would be a build-script bug. Declaring nothing is then better than
    // refusing to start the registry every other module depends on.
    build_provenance_from_source(source, None, None).ok()
}

/// What each agent operation does, as the daemon's catalog shows it to callers.
/// The agent methods are listed in `agent_reads` and `agent_ops`; a method
/// missing here fails the manifest test rather than shipping undescribed.
fn agent_operation_description(method: &str) -> &'static str {
    match method {
        "agent.resolve" => "Look up one agent by id, including retired and merged agents.",
        "agent.resolve_name" => "Find an agent by name within a workspace or the global namespace.",
        "agent.list" => "List agents, filtered by role, project or workspace and paged by agent id.",
        "agent.peer_roster" => "List a workspace's live heads: its workspace head and the heads of its projects.",
        "agent.avatar_read" => "Read the avatars of up to 64 agents.",
        "agent.github_identity" => "Read the GitHub identity bound to an agent; it names credentials and holds none.",
        "agent.fleet_identity" => "List live agents with their project and workspace placement for the fleet view.",
        "agent.snapshot" => "Read every agent and name claim at one generation, to seed a copy.",
        "agent.changes" => "Read identity changes after a cursor, optionally waiting up to 25 s for the next one.",
        "agent.create" => "Create an agent through the executive, or Direct with operator confirmation.",
        "agent.rename" => "Rename an agent through the executive, or Direct with operator confirmation.",
        "agent.update_tag" => "Change an agent's tag through the executive, or Direct with operator confirmation.",
        "agent.set_labels" => "Replace an agent's labels through the executive, or Direct with operator confirmation.",
        "agent.set_avatar" => "Set an agent's avatar. Accepted only from the executive.",
        "agent.set_github_identity" => "Bind or clear an agent's GitHub identity. Accepted only from the executive.",
        "agent.dispose" => "Retire an agent; its id stays reserved. Executive or Direct with operator confirmation.",
        "agent.merge" => "Merge one agent into another and retire the source. Accepted only from the executive.",
        "agent.import" => "Import the executive's agent registry once, at cutover. Accepted only from the executive.",
        _ => "",
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

/// Publishes an open outcome into the shared store slot and the health gauges.
fn install_store(
    store: &Mutex<Option<RegistryStore>>,
    health: &HealthGauges,
    outcome: Result<RegistryStore, String>,
) {
    let mut guard = store
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    match outcome {
        Ok(mut opened) => {
            enable_root_records(&mut opened);
            enable::refresh(health, &opened);
            health
                .generation
                .store(opened.generation().unwrap_or(0), Ordering::Relaxed);
            health.store_ready.store(true, Ordering::Relaxed);
            health.store_failed.store(false, Ordering::Relaxed);
            *guard = Some(opened);
        }
        Err(error) => {
            tracing::error!(target: "store", "{error}");
            health.store_ready.store(false, Ordering::Relaxed);
            health.store_failed.store(true, Ordering::Relaxed);
            *guard = None;
        }
    }
}

/// Environment switch for root records (per-root identity, epoch and approval
/// on resolve and enumerate replies). Off, every reply keeps the legacy shape.
/// Turn it on only after the consumers that read the new fields are running:
/// a consumer built before them would otherwise first see them in production.
const ROOT_RECORDS_ENV: &str = "ENTORHINAL_ROOT_RECORDS";

/// Turn root records on when configured, and bind every registered root that
/// has no binding yet, so each root's reply carries its checkout identity from
/// the first request. Binding never approves anything.
fn enable_root_records(store: &mut RegistryStore) {
    if std::env::var(ROOT_RECORDS_ENV).as_deref() != Ok("1") {
        return;
    }
    store.set_root_records(true);
    match store.bind_unbound_roots("entorhinal") {
        Ok(report) => {
            let bound = report
                .iter()
                .filter(|entry| entry.outcome == "bound")
                .count();
            for entry in report.iter().filter(|entry| entry.outcome != "bound") {
                tracing::warn!(target: "binding", root = %entry.root, outcome = %entry.outcome, "root left unbound");
            }
            tracing::info!(target: "binding", bound, unbound = report.len() - bound, "root records enabled");
        }
        Err(error) => tracing::error!(target: "binding", "binding unbound roots failed: {error}"),
    }
}

/// Retries the open with doubling delays while the only obstacle is a lease
/// a predecessor still holds. Any other error, and a hold that outlasts the
/// budget, end the retry with that error. The budget is supplied by the handler
/// so tests can exercise expiry without waiting for the production interval.
async fn retry_open_while_lease_held(
    descriptor: &StorageDescriptor,
    retry_budget: Duration,
) -> Result<RegistryStore, String> {
    let started = Instant::now();
    let mut delay = LEASE_HELD_RETRY_FIRST_DELAY;
    loop {
        tokio::time::sleep(delay).await;
        match RegistryStore::open(descriptor) {
            Err(error) if error.is_lease_held() && started.elapsed() < retry_budget => {
                delay = (delay * 2).min(LEASE_HELD_RETRY_MAX_DELAY);
            }
            outcome => {
                return outcome.map_err(|error| {
                    format!(
                        "opening projects storage after {:.1}s: {error}",
                        started.elapsed().as_secs_f64()
                    )
                })
            }
        }
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
    async fn route_admission_keeps_bind_handle_until_route_gone() {
        let handler = ProjectsHandler::new().unwrap();
        let handle = RouteHandle::detached(7, 3);
        let request = RouteBindRequest::new(
            handle,
            subc_protocol::RouteTarget::ToolProvider {
                module_id: MODULE_ID.into(),
            },
            subc_protocol::BindIdentity::new(PathBuf::from("/tmp/project"), "test", "bind"),
        )
        .with_principal(Principal::Direct);
        handler.on_bind(&request).await;
        let key = route_key(&handle);
        let admission = handler.admission_for(key);
        assert_eq!(admission.handle, Some(handle));
        assert!(matches!(admission.principal, Some(Principal::Direct)));
        handler.on_route_gone(&handle).await;
        assert!(!handler.route_admissions().contains_key(&key));
        assert!(handler.admission_for(key).handle.is_none());
    }

    pub(super) fn reserved(module_id: &str) -> Principal {
        Principal::Reserved {
            module_id: module_id.to_string(),
        }
    }

    #[test]
    fn every_agent_operation_has_its_own_catalog_description() {
        let mut seen = std::collections::BTreeSet::new();
        for method in agent_reads::READ_METHODS
            .iter()
            .chain(agent_ops::MUTATING_METHODS.iter())
        {
            let description = agent_operation_description(method);
            assert!(!description.is_empty(), "{method} has no description");
            assert!(
                seen.insert(description),
                "{method} repeats another agent operation's description"
            );
        }
    }

    #[test]
    fn writes_are_refused_unless_the_operator_or_the_executive_opened_the_route() {
        for method in MUTATING_METHODS {
            let direct = authorize_write(method, Some(&Principal::Direct));
            if agent_ops::MUTATING_METHODS.contains(method) {
                assert_eq!(
                    direct.unwrap_err().code,
                    "direct_identity_write_not_admitted",
                    "{method} from direct"
                );
            } else {
                assert!(direct.is_ok(), "{method} from direct");
            }
            let executive = authorize_write(method, Some(&reserved(WRITER_MODULE)));
            if *method == "identity_log.enable" {
                assert_eq!(executive.unwrap_err().code, "write_not_permitted");
            } else {
                assert!(executive.is_ok(), "{method} from {WRITER_MODULE}");
            }
            for refused in [Some(reserved("aft")), Some(Principal::Unverified), None] {
                let error = authorize_write(method, refused.as_ref())
                    .expect_err("a write from any other principal must be refused");
                assert_eq!(
                    error.code, "write_not_permitted",
                    "{method} from {refused:?}"
                );
            }
        }
    }

    #[test]
    fn queries_are_answered_for_every_principal() {
        for method in [
            "resolve",
            "resolve_project_id",
            "resolve_remote",
            "enumerate",
            "journal_tail",
            "verify",
        ] {
            for principal in [Some(reserved("aft")), Some(Principal::Unverified), None] {
                assert!(
                    authorize_write(method, principal.as_ref()).is_ok(),
                    "{method}"
                );
            }
        }
    }

    /// A stamp as the daemon would send it for a route opened under a flow's
    /// scope, parsed from JSON so the test does not depend on how the protocol
    /// crate builds one.
    pub(super) fn flow_stamp(flow_id: Option<&str>) -> ScopeStamp {
        let mut attributes = serde_json::Map::new();
        attributes.insert("agent_id".into(), json!("agent_0123456789abcdef"));
        if let Some(flow_id) = flow_id {
            attributes.insert("flow_id".into(), json!(flow_id));
        }
        serde_json::from_value(json!({
            "owner": {"kind": "reserved", "module_id": WRITER_MODULE},
            "ref": "head:agent_0123456789abcdef",
            "scope_epoch": 1,
            "kind": "head",
            "attributes": attributes,
            "owner_authorized": true,
        }))
        .expect("a stamp the daemon could send")
    }

    #[test]
    fn a_bind_records_the_flow_its_scope_belongs_to() {
        let flow = RouteAdmission::from_bind(
            Some(reserved(WRITER_MODULE)),
            Some(&flow_stamp(Some("fl_nightly"))),
        );
        assert_eq!(flow.flow_id.as_deref(), Some("fl_nightly"));

        let head =
            RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), Some(&flow_stamp(None)));
        assert_eq!(head.flow_id, None, "a scope without a flow records none");

        let unscoped = RouteAdmission::from_bind(Some(Principal::Direct), None);
        assert_eq!(unscoped.flow_id, None, "an unscoped route records none");
    }

    /// The executive can open a route under a flow's scope, so the principal on
    /// such a route is the executive's own. That must not let the flow write.
    #[test]
    fn a_flow_scoped_route_cannot_write_even_with_the_executives_principal() {
        let admission = RouteAdmission::from_bind(
            Some(reserved(WRITER_MODULE)),
            Some(&flow_stamp(Some("fl_nightly"))),
        );
        assert!(
            authorize_write("register", admission.principal.as_ref()).is_ok(),
            "on the principal alone this write would be admitted, which is the gap"
        );
        for method in MUTATING_METHODS {
            let error = refuse_flow_write(method, admission.flow_id.as_deref())
                .expect_err("a flow must not change the registry");
            assert_eq!(error.code, "flow_scope_not_admitted", "{method}");
        }
        for method in [
            "resolve",
            "resolve_project_id",
            "resolve_remote",
            "enumerate",
            "journal_tail",
            "verify",
        ] {
            assert!(
                refuse_flow_write(method, admission.flow_id.as_deref()).is_ok(),
                "{method} is a read and answers every caller alike"
            );
        }
        for method in MUTATING_METHODS {
            assert!(
                refuse_flow_write(method, None).is_ok(),
                "{method} on a route with no flow is left to authorize_write"
            );
        }
    }

    /// The handler admits through `admit`, so this pins that the flow check is
    /// actually applied to a bound route, not only that the check exists.
    #[test]
    fn the_handler_refuses_a_write_on_a_bound_flow_route() {
        let handler = ProjectsHandler::new().unwrap();
        let flow_route: RouteKey = (7, 1);
        let head_route: RouteKey = (8, 1);
        handler.route_admissions().insert(
            flow_route,
            RouteAdmission::from_bind(
                Some(reserved(WRITER_MODULE)),
                Some(&flow_stamp(Some("fl_nightly"))),
            ),
        );
        handler.route_admissions().insert(
            head_route,
            RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), Some(&flow_stamp(None))),
        );

        let error = handler
            .admit("register", flow_route)
            .expect_err("a flow route must not write");
        assert_eq!(error.code, "flow_scope_not_admitted");
        assert!(
            handler.admit("enumerate", flow_route).is_ok(),
            "reads stay open"
        );
        assert!(
            handler.admit("register", head_route).is_ok(),
            "the executive's own scoped route still writes"
        );
        assert_eq!(
            handler.admit("register", (9, 1)).unwrap_err().code,
            "write_not_permitted",
            "an unknown route has no principal and is refused"
        );
    }

    /// A mutation added to the manifest but missing from the write list would
    /// be callable by anyone. Derive the list's completeness from the manifest
    /// the module declares, so the two cannot drift apart.
    #[test]
    fn every_declared_mutation_is_write_checked() {
        let manifest = manifest();
        let mut declared = Vec::new();
        for role in &manifest.provides {
            if let ProviderRole::ManagementSurface { operations, .. } = role {
                declared.extend(
                    operations
                        .iter()
                        .filter(|operation| operation.kind == ManagementOperationKind::Mutate)
                        .map(|operation| operation.name.clone()),
                );
            }
        }
        assert!(
            !declared.is_empty(),
            "the manifest declares no mutations; the test would pass vacuously"
        );
        for name in &declared {
            assert!(
                MUTATING_METHODS.contains(&name.as_str()),
                "declared mutation '{name}' is not write-checked"
            );
        }
    }

    /// A fresh store directory per call. Parallel tests often share a label and
    /// start in the same millisecond, so the name also carries a per-process
    /// counter; without it two tests open one store and the second fails on
    /// the first's writer lease.
    pub(super) fn scratch_descriptor(name: &str) -> (PathBuf, StorageDescriptor) {
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "entorhinal-{name}-{}-{}-{}",
            std::process::id(),
            unix_millis(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let descriptor = StorageDescriptor {
            module_id: MODULE_ID.to_string(),
            storage_namespace: DEFAULT_STORAGE_NAMESPACE.to_string(),
            isolation: Isolation::Module,
            backend: StorageBackend::Sqlite {
                path: dir.join("store.db").to_string_lossy().into_owned(),
            },
        };
        (dir, descriptor)
    }

    fn hello_ack(descriptor: &StorageDescriptor) -> ModuleHelloAckBody {
        ModuleHelloAckBody {
            negotiated_ver: 1,
            subc_ops: Vec::new(),
            subc_capabilities: Vec::new(),
            storage: Some(serde_json::to_value(descriptor).unwrap()),
            machine_id: None,
        }
    }

    fn execute_on(
        handler: &ProjectsHandler,
        key: RouteKey,
        method: &str,
        params: Value,
    ) -> Vec<u8> {
        handler
            .execute(
                WireRequest {
                    method: method.into(),
                    params,
                },
                key,
            )
            .unwrap()
    }

    fn ownership_handler(label: &str) -> (PathBuf, ProjectsHandler, String) {
        let (dir, descriptor) = scratch_descriptor(label);
        let root = dir.join("repo");
        std::fs::create_dir_all(&root).unwrap();
        for args in [
            vec!["init", "--quiet"],
            vec![
                "remote",
                "add",
                "origin",
                "https://github.com/ualtinok/opencode.git",
            ],
        ] {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&root)
                .args(&args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "git {args:?}: {}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        let root = std::fs::canonicalize(root)
            .unwrap()
            .to_string_lossy()
            .into_owned();
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        store
            .register(RegisterRequest {
                project_id: Some("p".into()),
                name: "P".into(),
                roots: vec![root.clone()],
                ..Default::default()
            })
            .unwrap();
        let handler = ProjectsHandler::with_runtime("ownership-incarnation".into(), unix_millis);
        *handler.store.lock().unwrap() = Some(store);
        (dir, handler, root)
    }

    // Install a shared key image directly: admission and query tests do not
    // depend on a transport or pretend to enable a real shared log.
    pub(super) fn log_surface_handler(label: &str) -> (PathBuf, ProjectsHandler, String) {
        let (dir, handler, root) = ownership_handler(label);
        handler.with_store(|s| s.apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
            tx.execute("INSERT INTO project_root_key(project_id,kind,root_key,created_at) VALUES('p','remote','ualtinok/opencode',1),('p','label','local',1)", [])?;
            tx.execute("UPDATE project_root SET root_key_kind='remote',root_key='ualtinok/opencode' WHERE project_id='p'", [])?;
            Ok(())
        })).unwrap();
        (dir, handler, root)
    }

    fn attach_checkout(dir: &std::path::Path, name: &str) -> String {
        let path = dir.join(name);
        std::fs::create_dir_all(&path).unwrap();
        for args in [
            vec!["init", "--quiet"],
            vec![
                "remote",
                "add",
                "origin",
                "https://github.com/ualtinok/opencode.git",
            ],
        ] {
            let out = std::process::Command::new("git")
                .arg("-C")
                .arg(&path)
                .args(args)
                .output()
                .unwrap();
            assert!(
                out.status.success(),
                "{}",
                String::from_utf8_lossy(&out.stderr)
            );
        }
        std::fs::canonicalize(path)
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    #[test]
    fn attach_surface_writes_are_attributed_only_to_direct_and_core() {
        let (dir, handler, _) = log_surface_handler("attach-admission");
        for (index, principal, label) in [
            (0, Principal::Direct, "direct"),
            (1, reserved(WRITER_MODULE), "reserved:prefrontal-core"),
        ] {
            let key = (70 + index, 1);
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
            let root = attach_checkout(&dir, &format!("clone-{index}"));
            let reply: Value = serde_json::from_slice(&execute_on(
                &handler,
                key,
                "attach_root",
                json!({"path":root,"actor":"operator"}),
            ))
            .unwrap();
            assert_eq!(reply["result"]["projectId"], "p");
            assert_eq!(
                reply["result"]["rootKey"],
                json!({"kind":"remote","rootKey":"ualtinok/opencode"})
            );
            let trust = handler.with_store(|s| s.trust(&root)).unwrap();
            let trust = serde_json::to_value(trust).unwrap();
            let record = trust["rootRecords"]
                .as_array()
                .unwrap()
                .iter()
                .find(|r| r["root"] == root)
                .unwrap();
            assert_eq!(record["identity"], "bound");
            assert_eq!(record["approval"]["state"], "unapproved");
            let tail = handler.with_store(|s| s.journal_tail(0, 100)).unwrap();
            let rows: Vec<_> = tail
                .entries
                .iter()
                .filter(|row| row.op == "attach_root" || row.op == "root_key.assign")
                .rev()
                .take(2)
                .collect();
            assert_eq!(
                rows.len(),
                2,
                "attachment must journal its write and key mapping"
            );
            for row in rows {
                assert_eq!(row.principal.as_deref(), Some(label));
            }
        }
        let before = handler.with_store(RegistryStore::generation).unwrap();
        for (index, principal) in [Some(reserved("plexus")), Some(Principal::Unverified), None]
            .into_iter()
            .enumerate()
        {
            let key = (80 + index as u16, 1);
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(principal, None));
            let error = handler
                .execute(
                    WireRequest {
                        method: "attach_root".into(),
                        params: Value::Null,
                    },
                    key,
                )
                .unwrap_err();
            assert_eq!(error.code, "write_not_permitted");
        }
        assert_eq!(
            handler.with_store(RegistryStore::generation).unwrap(),
            before
        );
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn attach_surface_flow_routes_refuse_mutations_before_decoding() {
        let handler = ProjectsHandler::new().unwrap();
        for (index, principal) in [Principal::Direct, reserved(WRITER_MODULE)]
            .into_iter()
            .enumerate()
        {
            let key = (90 + index as u16, 1);
            handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(Some(principal), Some(&flow_stamp(Some("fl_attach")))),
            );
            for method in ["attach_root", "identity_log.enable"] {
                let error = handler
                    .execute(
                        WireRequest {
                            method: method.into(),
                            params: Value::Null,
                        },
                        key,
                    )
                    .unwrap_err();
                assert_eq!(error.code, "flow_scope_not_admitted", "{method}");
            }
        }
    }

    #[test]
    fn attach_surface_queries_answer_every_principal_on_flow_routes() {
        let (dir, handler, original) = log_surface_handler("attach-queries");
        let path = attach_checkout(&dir, "preview");
        let before = handler.with_store(RegistryStore::generation).unwrap();
        for (index, principal) in [
            Some(Principal::Direct),
            Some(reserved(WRITER_MODULE)),
            Some(reserved("plexus")),
            Some(Principal::Unverified),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            let key = (100 + index as u16, 1);
            handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal, Some(&flow_stamp(Some("fl_queries")))),
            );
            for (method, params, expected) in [
                (
                    "resolve_root_key",
                    json!({"projectId":"p","kind":"remote","rootKey":"ualtinok/opencode"}),
                    json!({"roots":[original]}),
                ),
                (
                    "preview_attach_root",
                    json!({"path":path}),
                    json!({"projectId":"p","root":path,"rootKey":{"kind":"remote","rootKey":"ualtinok/opencode"}}),
                ),
                (
                    "identity_log.status",
                    json!({}),
                    json!({"state":"enabled","lastAppliedPosition":0,"lastSeenHead":0,"pendingWriteCount":0,"lastError":null,"lastErrorAt":null}),
                ),
            ] {
                let reply: Value =
                    serde_json::from_slice(&execute_on(&handler, key, method, params)).unwrap();
                let mut expected = expected;
                expected["incarnation"] = json!("ownership-incarnation");
                assert_eq!(reply["result"], expected, "{method}");
            }
        }
        assert_eq!(
            handler.with_store(RegistryStore::generation).unwrap(),
            before
        );
        assert!(!std::path::Path::new(&path)
            .join(".git")
            .join(entorhinal_core::INCARNATION_FILE)
            .exists());
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn attach_surface_enable_is_direct_only_and_fails_closed() {
        let (dir, descriptor) = scratch_descriptor("enable-admission");
        let handler = ProjectsHandler::new().unwrap();
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        let before = handler
            .with_store(RegistryStore::identity_log_status)
            .unwrap();
        let head = handler.with_store(RegistryStore::generation).unwrap();
        for (index, principal) in [
            Some(Principal::Direct),
            Some(reserved(WRITER_MODULE)),
            Some(reserved("plexus")),
            Some(Principal::Unverified),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            let key = (110 + index as u16, 1);
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(principal.clone(), None));
            let error = handler
                .execute(
                    WireRequest {
                        method: "identity_log.enable".into(),
                        params: json!({}),
                    },
                    key,
                )
                .unwrap_err();
            if principal == Some(Principal::Direct) {
                assert_eq!(error.code, "engram_unavailable");
                assert!(error.message.contains("isn't available in this build yet"));
            } else {
                assert_eq!(error.code, "write_not_permitted");
            }
        }
        assert_eq!(
            handler
                .with_store(RegistryStore::identity_log_status)
                .unwrap(),
            before
        );
        assert_eq!(handler.with_store(RegistryStore::generation).unwrap(), head);
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ownership_write_admission_and_principal_attribution() {
        let (dir, handler, root) = ownership_handler("ownership-admission");
        for (key, principal, names, label) in [
            ((31, 1), Principal::Direct, json!([]), "direct"),
            (
                (32, 1),
                reserved(WRITER_MODULE),
                json!(["origin"]),
                "reserved:prefrontal-core",
            ),
        ] {
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
            let out = execute_on(
                &handler,
                key,
                "set_owned_remotes",
                json!({"root":root,"remotes":names,"actor":"operator"}),
            );
            assert_eq!(
                serde_json::from_slice::<Value>(&out).unwrap()["result"]["remotes"],
                names
            );
            let tail = handler.with_store(|s| s.journal_tail(0, 100)).unwrap();
            let row = tail.entries.last().unwrap();
            assert_eq!(row.op, "set_owned_remotes");
            assert_eq!(row.principal.as_deref(), Some(label));
            assert_eq!(row.actor, "operator");
        }
        let before = handler.with_store(RegistryStore::generation).unwrap();
        handler.route_admissions().insert(
            (33, 1),
            RouteAdmission::from_bind(Some(reserved("plexus")), None),
        );
        let err = handler
            .execute(
                WireRequest {
                    method: "set_owned_remotes".into(),
                    params: json!({"root":root,"remotes":[]}),
                },
                (33, 1),
            )
            .unwrap_err();
        assert_eq!(err.code, "write_not_permitted");
        assert_eq!(
            handler.with_store(RegistryStore::generation).unwrap(),
            before
        );
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ownership_flow_route_refuses_write() {
        let (dir, handler, root) = ownership_handler("ownership-flow");
        handler.route_admissions().insert(
            (34, 1),
            RouteAdmission::from_bind(
                Some(reserved(WRITER_MODULE)),
                Some(&flow_stamp(Some("fl_ownership"))),
            ),
        );
        let before = handler.with_store(RegistryStore::generation).unwrap();
        let err = handler
            .execute(
                WireRequest {
                    method: "set_owned_remotes".into(),
                    params: json!({"root":root,"remotes":[]}),
                },
                (34, 1),
            )
            .unwrap_err();
        assert_eq!(err.code, "flow_scope_not_admitted");
        assert_eq!(
            handler.with_store(RegistryStore::generation).unwrap(),
            before
        );
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn ownership_query_is_open_and_carries_read_metadata_and_manifest() {
        let (dir, handler, root) = ownership_handler("ownership-query");
        for (index, principal) in [
            Some(Principal::Direct),
            Some(reserved(WRITER_MODULE)),
            Some(reserved("plexus")),
            Some(Principal::Unverified),
            None,
        ]
        .into_iter()
        .enumerate()
        {
            let key = (40 + index as u16, 1);
            handler.route_admissions().insert(
                key,
                RouteAdmission::from_bind(principal, Some(&flow_stamp(Some("fl_read")))),
            );
            let reply: Value = serde_json::from_slice(&execute_on(
                &handler,
                key,
                "resolve_remote",
                json!({"owner":"UALTINOK","repo":"OpenCode"}),
            ))
            .unwrap();
            assert_eq!(
                reply,
                json!({"result":{"status":"found","projectId":"p","root":root,"generation":1,"incarnation":"ownership-incarnation"}})
            );
        }
        let manifest = serde_json::to_value(manifest()).unwrap();
        let operations = manifest["provides"][0]["operations"].as_array().unwrap();
        for (name, kind) in [("resolve_remote", "query"), ("set_owned_remotes", "mutate")] {
            let operation = operations
                .iter()
                .find(|op| op["name"] == name)
                .expect("ownership operation declared");
            assert_eq!(operation["kind"], kind);
        }
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn journal_repo(dir: &std::path::Path, name: &str) -> String {
        let root = dir.join(name);
        std::fs::create_dir_all(root.join(".git")).unwrap();
        std::fs::canonicalize(root)
            .unwrap()
            .to_string_lossy()
            .into_owned()
    }

    #[tokio::test]
    // Intentionally hold the store lock while checking atomics-only health.
    #[allow(clippy::await_holding_lock)]
    async fn cached_mutation_retry_cannot_lower_the_health_generation() {
        let (dir, descriptor) = scratch_descriptor("health-generation");
        let handler = ProjectsHandler::new().unwrap();
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        let key = (72, 1);
        handler.route_admissions().insert(
            key,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        let first_params = json!({"projectId":"first", "name":"First", "requestKey":"first-key"});
        let first = execute_on(&handler, key, "register", first_params.clone());
        let second = execute_on(
            &handler,
            key,
            "register",
            json!({"projectId":"second", "name":"Second", "requestKey":"second-key"}),
        );
        let generation = |bytes: &[u8]| {
            serde_json::from_slice::<Value>(bytes).unwrap()["result"]["generation"]
                .as_i64()
                .unwrap()
        };
        assert!(generation(&second) > generation(&first));
        let retried = execute_on(&handler, key, "register", first_params);
        assert_eq!(retried, first);
        // Holding the store mutex cannot block the atomics-only health path.
        let guard = handler.store.lock().unwrap();
        let health = handler.health().await.metrics.unwrap();
        assert_eq!(health["generation"], generation(&second));
        drop(guard);
        assert_eq!(
            handler.with_store(|s| s.generation()).unwrap(),
            generation(&second)
        );
        drop(handler);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[test]
    fn requests_attribute_all_project_and_secondary_binding_rows_to_the_bound_principal() {
        for (principal, expected) in [
            (Principal::Direct, "direct"),
            (reserved(WRITER_MODULE), "reserved:prefrontal-core"),
        ] {
            let (dir, descriptor) = scratch_descriptor(expected);
            let dir = std::fs::canonicalize(dir).unwrap();
            let handler = ProjectsHandler::new().unwrap();
            let mut store = RegistryStore::open(&descriptor).unwrap();
            store.set_root_records(true);
            *handler.store.lock().unwrap() = Some(store);
            let key = (71, 1);
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(Some(principal), None));
            let root = journal_repo(&dir, "first");
            let second = journal_repo(&dir, "second");
            let third = journal_repo(&dir, "third");
            execute_on(
                &handler,
                key,
                "register",
                json!({"projectId":"project", "name":"Project", "roots":[root,second], "actor":"request actor", "requestKey":"register-key"}),
            );
            execute_on(
                &handler,
                key,
                "add_root",
                json!({"projectId":"project", "root":third, "actor":null}),
            );
            execute_on(
                &handler,
                key,
                "approve_project",
                json!({"canonicalRoot":root}),
            );
            let record = handler
                .with_store(|s| Ok(s.trust(&root)?.root_fields.unwrap().root_records.remove(0)))
                .unwrap();
            let container = dir.join("workers");
            std::fs::create_dir_all(&container).unwrap();
            execute_on(
                &handler,
                key,
                "attach_derived_parent",
                json!({"projectId":"project", "root":root, "incarnation":record.incarnation.unwrap().value, "registrationEpoch":record.registration_epoch.unwrap(), "container":container}),
            );
            execute_on(&handler, key, "unapprove_root", json!({"root":root}));
            execute_on(&handler, key, "approve_root", json!({"root":root}));
            execute_on(
                &handler,
                key,
                "unapprove_project",
                json!({"canonicalRoot":root}),
            );
            execute_on(
                &handler,
                key,
                "assign_workspace",
                json!({"projectId":"project", "workspaceId":"team", "workspaceName":"Team"}),
            );
            execute_on(
                &handler,
                key,
                "set_workspace_root",
                json!({"workspaceId":"team", "root":dir}),
            );
            execute_on(
                &handler,
                key,
                "upgrade_implicit",
                json!({"projectId":"project", "implicitId":entorhinal_core::implicit_project_id(&root), "name":"Renamed", "roots":[root]}),
            );
            execute_on(
                &handler,
                key,
                "remove_root",
                json!({"projectId":"project", "root":third}),
            );
            let seed = journal_repo(&dir, "seed");
            execute_on(
                &handler,
                key,
                "seed_import",
                json!({"source":"mc", "excludeHomeScoped":false, "payload":{"pairs":[{"mcIdentity":"git:seed", "canonicalRoot":seed}]}}),
            );

            let before = handler.with_store(|s| s.enumerate(None)).unwrap();
            let rows = handler
                .with_store(|s| s.journal_tail(0, 100))
                .unwrap()
                .entries;
            assert_eq!(
                rows.iter().map(|row| row.op.as_str()).collect::<Vec<_>>(),
                [
                    "register",
                    "bind_root",
                    "bind_root",
                    "add_root",
                    "bind_root",
                    "approve_root",
                    "approve_root",
                    "approve_root",
                    "attach_derived_parent",
                    "unapprove_root",
                    "approve_root",
                    "unapprove_root",
                    "unapprove_root",
                    "unapprove_root",
                    "assign_workspace",
                    "set_workspace_root",
                    "upgrade_implicit",
                    "remove_root",
                    "seed_import",
                ]
            );
            assert_eq!(rows[0].actor, "request actor");
            assert_eq!(rows[3].actor, "module");
            assert_eq!(
                rows[1].actor, "entorhinal",
                "secondary binding keeps its existing actor"
            );
            for row in &rows {
                assert_eq!(
                    row.principal.as_deref(),
                    Some(expected),
                    "{} at seq {}",
                    row.op,
                    row.seq
                );
            }
            execute_on(&handler, key, "rebuild", json!({}));
            let after = handler.with_store(|s| s.enumerate(None)).unwrap();
            assert_eq!(
                serde_json::to_value(before).unwrap(),
                serde_json::to_value(after).unwrap()
            );
            let rebuilt_rows = handler
                .with_store(|s| s.journal_tail(0, 100))
                .unwrap()
                .entries;
            assert_eq!(
                serde_json::to_value(&rows).unwrap(),
                serde_json::to_value(&rebuilt_rows).unwrap()
            );
            assert_eq!(
                rows.iter().map(|row| &row.principal).collect::<Vec<_>>(),
                rebuilt_rows
                    .iter()
                    .map(|row| &row.principal)
                    .collect::<Vec<_>>()
            );

            execute_on(&handler, key, "remove", json!({"workspaceId":"team"}));
            execute_on(&handler, key, "remove", json!({"projectId":"project"}));
            let removals = handler
                .with_store(|s| s.journal_tail(19, 100))
                .unwrap()
                .entries;
            assert_eq!(
                removals
                    .iter()
                    .map(|row| row.op.as_str())
                    .collect::<Vec<_>>(),
                ["remove", "remove"]
            );
            for row in removals {
                assert_eq!(row.principal.as_deref(), Some(expected));
            }
            drop(handler);
            std::fs::remove_dir_all(dir).unwrap();
        }
    }

    #[test]
    fn startup_binding_uses_entorhinal_and_later_request_binding_uses_direct() {
        let (dir, descriptor) = scratch_descriptor("startup-principal");
        let handler = ProjectsHandler::new().unwrap();
        let store = RegistryStore::open(&descriptor).unwrap();
        let root = journal_repo(&dir, "before-restart");
        store
            .with_principal("reserved:prefrontal-core")
            .register(RegisterRequest {
                project_id: Some("project".into()),
                name: "Project".into(),
                roots: vec![root],
                ..Default::default()
            })
            .unwrap();
        drop(store);
        let mut store = RegistryStore::open(&descriptor).unwrap();
        store.set_root_records(true);
        let reports = store.bind_unbound_roots("entorhinal").unwrap();
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].outcome, "bound");
        *handler.store.lock().unwrap() = Some(store);
        let key = (72, 2);
        handler.route_admissions().insert(
            key,
            RouteAdmission::from_bind(Some(Principal::Direct), None),
        );
        let added = journal_repo(&dir, "after-restart");
        execute_on(
            &handler,
            key,
            "add_root",
            json!({"projectId":"project", "root":added}),
        );
        let rows = handler
            .with_store(|s| s.journal_tail(0, 10))
            .unwrap()
            .entries;
        assert_eq!(
            rows.iter().map(|row| row.op.as_str()).collect::<Vec<_>>(),
            ["register", "bind_root", "add_root", "bind_root"]
        );
        assert_eq!(
            rows.iter()
                .map(|row| row.principal.as_deref())
                .collect::<Vec<_>>(),
            [
                Some("reserved:prefrontal-core"),
                Some("entorhinal"),
                Some("direct"),
                Some("direct")
            ]
        );
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn journal_principal_labels_are_pinned_and_refused_routes_append_nothing() {
        assert_eq!(principal_label(Some(&Principal::Direct)), "direct");
        assert_eq!(
            principal_label(Some(&reserved("another-module"))),
            "reserved:another-module"
        );
        assert_eq!(principal_label(Some(&Principal::Unverified)), "unverified");
        let (dir, descriptor) = scratch_descriptor("refused-principal");
        let handler = ProjectsHandler::new().unwrap();
        *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
        for (index, principal) in [
            None,
            Some(Principal::Unverified),
            Some(reserved("another-module")),
        ]
        .into_iter()
        .enumerate()
        {
            let key = (73, index as u32);
            handler
                .route_admissions()
                .insert(key, RouteAdmission::from_bind(principal, None));
            let error = handler
                .execute(
                    WireRequest {
                        method: "register".into(),
                        params: json!({"name":"Not admitted"}),
                    },
                    key,
                )
                .unwrap_err();
            assert_eq!(error.code, "write_not_permitted");
            assert_eq!(handler.with_store(|s| s.generation()).unwrap(), 0);
        }
        drop(handler);
        std::fs::remove_dir_all(dir).unwrap();
    }

    async fn wait_until_settled(handler: &ProjectsHandler, budget: Duration) -> Duration {
        let started = Instant::now();
        while handler.health.store_opening.load(Ordering::Relaxed) {
            assert!(
                started.elapsed() < budget,
                "open did not settle within {budget:?}"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        started.elapsed()
    }

    // The predecessor shape from the E2E rig: the previous instance still
    // holds the store's kernel lease while the new one registers. The lease
    // releases the instant the holder drops, and the open must then succeed
    // without a restart; health reads degraded, never failing, in between.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_retries_while_a_predecessor_still_holds_the_lease() {
        let (dir, descriptor) = scratch_descriptor("lease-held-then-released");
        let predecessor = RegistryStore::open(&descriptor).expect("predecessor opens");
        let mut handler = ProjectsHandler::new().unwrap();
        handler.lease_retry_budget = Duration::from_secs(2);

        handler.on_hello_ack(&hello_ack(&descriptor)).await;
        let during = handler.health().await;
        assert_eq!(during.status, HealthStatus::Degraded, "{during:?}");
        assert!(
            during
                .detail
                .as_deref()
                .unwrap_or("")
                .contains("predecessor"),
            "{during:?}"
        );
        assert!(
            handler.store.lock().unwrap().is_none(),
            "no store until the lease is free"
        );

        tokio::time::sleep(Duration::from_millis(300)).await;
        drop(predecessor);

        let settled_after = wait_until_settled(&handler, Duration::from_secs(5)).await;
        let after = handler.health().await;
        assert_eq!(after.status, HealthStatus::Ok, "{after:?}");
        assert!(handler.store.lock().unwrap().is_some());
        assert!(
            settled_after < Duration::from_secs(3),
            "the retry must pick the lease up promptly after release, not at the budget (took {settled_after:?})"
        );
        let _ = std::fs::remove_dir_all(dir);
    }

    // A hold that outlasts the budget is a second live writer: the refusal
    // is reported as today, after the budget and not much later.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn open_gives_up_on_a_lease_held_past_the_budget() {
        let (dir, descriptor) = scratch_descriptor("lease-held-past-budget");
        let _other_writer = RegistryStore::open(&descriptor).expect("other writer opens");
        let mut handler = ProjectsHandler::new().unwrap();
        handler.lease_retry_budget = Duration::from_millis(300);

        handler.on_hello_ack(&hello_ack(&descriptor)).await;
        wait_until_settled(&handler, Duration::from_secs(3)).await;
        let after = handler.health().await;
        assert_eq!(after.status, HealthStatus::Failing, "{after:?}");
        assert!(handler.store.lock().unwrap().is_none());
        let _ = std::fs::remove_dir_all(dir);
    }

    // Only the lease is retried. Any other open error is the same fail-once
    // it always was: no retry task, failing at once.
    #[tokio::test]
    async fn a_non_lease_open_error_fails_once_without_retrying() {
        let (dir, mut descriptor) = scratch_descriptor("unopenable");
        let blocker = dir.join("not-a-directory");
        std::fs::write(&blocker, b"file").unwrap();
        descriptor.backend = StorageBackend::Sqlite {
            path: blocker.join("store.db").to_string_lossy().into_owned(),
        };
        let handler = ProjectsHandler::new().unwrap();

        let started = Instant::now();
        handler.on_hello_ack(&hello_ack(&descriptor)).await;
        assert!(
            !handler.health.store_opening.load(Ordering::Relaxed),
            "a non-lease error must not start the retry task"
        );
        assert!(started.elapsed() < Duration::from_secs(1));
        let after = handler.health().await;
        assert_eq!(after.status, HealthStatus::Failing, "{after:?}");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn session_liveness_gap_detection_and_snapshot_rebase() {
        let handler = ProjectsHandler::new().unwrap();
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

    /// Only the `snapshot` marker rebases: a batch that merely happens to list
    /// every session is still a delta, and staleness must survive it.
    ///
    /// Read this test for what it does NOT prove. `snapshot` is optional on the
    /// wire and defaults to false, so a test writing the key by hand passes
    /// whether or not any producer sets it. The rebase path has been
    /// unreachable in production for exactly that reason, with a producer whose
    /// batch type had no such field, while tests on both sides stayed green.
    /// Proving a producer marks its snapshots needs a test against that
    /// producer's serialized output, not against JSON written here.
    #[test]
    fn a_batch_that_does_not_say_it_is_a_snapshot_cannot_clear_staleness() {
        let handler = ProjectsHandler::new().unwrap();
        let call = |v: serde_json::Value| handler.session_liveness(v);
        let requested = |bytes: &[u8]| -> bool {
            serde_json::from_slice::<Value>(bytes).unwrap()["result"]["snapshotRequested"]
                .as_bool()
                .unwrap()
        };

        call(json!({"seq": 1, "snapshot": true, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"}]}))
        .unwrap();
        // Skip seq 2, so a batch is genuinely missing and staleness latches.
        assert!(requested(&call(json!({"seq": 3, "sessions": []})).unwrap()));

        // A full-state resend carrying every tracked session but no `snapshot`
        // key. However complete it looks, it must not count as a rebase.
        let r = call(json!({"seq": 4, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"}]}))
        .unwrap();
        assert!(
            requested(&r),
            "a resend without the snapshot marker is a delta, so staleness must persist"
        );

        // Only the marked batch rebases.
        assert!(!requested(
            &call(json!({"seq": 5, "snapshot": true, "sessions": []})).unwrap()
        ));
    }

    /// A receiver that restarts while the emitter keeps running must ask for a
    /// snapshot, because the first batch it sees is a delta and its mirror is
    /// empty. Taking that delta as the whole state leaves every session that
    /// did not change missing until it changes again.
    #[test]
    fn a_fresh_receiver_asks_for_a_snapshot_until_one_arrives() {
        let handler = ProjectsHandler::new().unwrap();
        let call = |v: serde_json::Value| handler.session_liveness(v);
        let requested = |bytes: &[u8]| -> bool {
            serde_json::from_slice::<Value>(bytes).unwrap()["result"]["snapshotRequested"]
                .as_bool()
                .unwrap()
        };

        // The emitter is mid-stream: its first batch to this process is a delta.
        let r = call(json!({"seq": 4200, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"}]}))
        .unwrap();
        assert!(
            requested(&r),
            "a mirror that has never seen a snapshot is incomplete and must ask for one"
        );
        assert_eq!(
            handler.health.liveness_gaps.load(Ordering::Relaxed),
            0,
            "first contact is not a gap"
        );

        // A contiguous delta does not complete the mirror either.
        let r = call(json!({"seq": 4201, "sessions": []})).unwrap();
        assert!(requested(&r), "only a snapshot completes the mirror");

        // The snapshot does.
        let r = call(json!({"seq": 4202, "snapshot": true, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"},
            {"sessionId": "s2", "canonicalRoot": "/tmp/b", "state": "idle"}]}))
        .unwrap();
        assert!(!requested(&r));
    }

    /// A repeated sequence number is refused like an older one. A replay of
    /// seq N need not carry the same payload as the batch already applied at
    /// N, so applying it could undo that batch's `gone`.
    #[test]
    fn an_equal_sequence_batch_cannot_resurrect_a_gone_session() {
        let handler = ProjectsHandler::new().unwrap();
        handler
            .session_liveness(json!({"seq":10,"snapshot":true,"sessions":[
                {"sessionId":"s1","canonicalRoot":"/tmp/a","state":"active","lastActivityMs":100}
            ]}))
            .unwrap();
        handler
            .session_liveness(json!({"seq":11,"sessions":[
                {"sessionId":"s1","canonicalRoot":"/tmp/a","state":"gone"}
            ]}))
            .unwrap();
        let before = {
            let state = handler.liveness.lock().unwrap();
            (state.sessions.clone(), state.last_seq)
        };
        assert!(before.0.is_empty());
        // A duplicate sequence need not carry the same payload. It must not
        // undo an already applied terminal transition.
        let reply = handler
            .session_liveness(json!({"seq":11,"sessions":[
                {"sessionId":"s1","canonicalRoot":"/tmp/a","state":"active","lastActivityMs":999}
            ]}))
            .unwrap();
        let reply: Value = serde_json::from_slice(&reply).unwrap();
        let state = handler.liveness.lock().unwrap();
        assert_eq!(
            (state.sessions.clone(), state.last_seq),
            before,
            "an equal-sequence delta must not change the mirror or its applied sequence"
        );
        assert!(
            state.stale,
            "a refused duplicate requests an authoritative rebase"
        );
        assert_eq!(reply["result"]["tracked"], 0);
        assert_eq!(reply["result"]["snapshotRequested"], true);
        assert_eq!(
            handler
                .health
                .liveness_dropped_older
                .load(Ordering::Relaxed),
            1,
            "the equal-sequence refusal must be counted"
        );
        assert_eq!(handler.health.liveness_gaps.load(Ordering::Relaxed), 0);
    }

    /// Out-of-order delivery must not resurrect a removed session.
    ///
    /// Nothing in the protocol guarantees a producer delivers in seq order: a
    /// producer that assigns seq and then sends concurrently can have a `gone`
    /// overtaken by an older batch for the same session. Applying that older
    /// batch reinserts an entry whose terminal signal is already spent, so
    /// nothing prunes it again, and the entry keeps its project's
    /// `last_route_activity_ms` alive forever -- the GC-candidacy signal this
    /// feed exists to produce. Hence the refusal is the receiver's own
    /// invariant, not a workaround for one producer's scheduling.
    #[test]
    fn an_older_batch_cannot_resurrect_a_session_that_is_already_gone() {
        let handler = ProjectsHandler::new().unwrap();
        let call = |v: serde_json::Value| handler.session_liveness(v);
        let tracked = |bytes: &[u8]| -> u64 {
            serde_json::from_slice::<Value>(bytes).unwrap()["result"]["tracked"]
                .as_u64()
                .unwrap()
        };

        call(json!({"seq": 10, "snapshot": true, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active", "lastActivityMs": 100}]}))
        .unwrap();
        let r = call(json!({"seq": 11, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "gone"}]}))
        .unwrap();
        assert_eq!(tracked(&r), 0, "gone prunes the entry");

        // The overtaken batch: seq 9, arriving after seq 11 was applied.
        let r = call(json!({"seq": 9, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active", "lastActivityMs": 90}]}))
        .unwrap();
        assert_eq!(
            tracked(&r),
            0,
            "a batch older than the last applied one must not be applied at all"
        );
        assert_eq!(
            handler
                .health
                .liveness_dropped_older
                .load(Ordering::Relaxed),
            1,
            "the refusal must be counted, not silent"
        );
        assert_eq!(
            handler.health.liveness_gaps.load(Ordering::Relaxed),
            0,
            "a reorder is not a gap: nothing was missed, so they must not share a counter"
        );

        // Refusing alone would be fail-deaf, so the refusal asks for a rebase:
        // otherwise an emitter that restarted its counter and lost its snapshot
        // would have every later delta look older than the old high-water mark.
        let r = call(json!({"seq": 12, "sessions": []})).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&r).unwrap()["result"]["snapshotRequested"],
            true,
            "a refused batch must leave the state asking to be rebased"
        );

        // And the rebase is honoured even though its seq goes backwards, which
        // is what a restarted emitter sends.
        let r = call(json!({"seq": 1, "snapshot": true, "sessions": [
            {"sessionId": "s2", "canonicalRoot": "/tmp/b", "state": "active"}]}))
        .unwrap();
        assert_eq!(tracked(&r), 1);
        assert_eq!(
            serde_json::from_slice::<Value>(&r).unwrap()["result"]["snapshotRequested"],
            false
        );
    }

    /// A snapshot replaces the mirror rather than merging into it, so a session
    /// the snapshot does not name is gone afterwards.
    ///
    /// This is the backstop for a `gone` that is never delivered, which no gap
    /// detector can recover: if a producer drops that batch and then restarts
    /// with nothing tracked, its reconnect snapshot is the only remaining
    /// statement that the session is over. An empty snapshot therefore has to
    /// be honoured as well, since "I track nothing" is the whole message in
    /// that case. A producer that suppresses an empty snapshot as a no-op
    /// disables recovery precisely where it is needed.
    #[test]
    fn a_snapshot_replaces_the_mirror_and_an_empty_one_still_counts() {
        let handler = ProjectsHandler::new().unwrap();
        let call = |v: serde_json::Value| handler.session_liveness(v);
        let tracked = |bytes: &[u8]| -> u64 {
            serde_json::from_slice::<Value>(bytes).unwrap()["result"]["tracked"]
                .as_u64()
                .unwrap()
        };

        let r = call(json!({"seq": 1, "snapshot": true, "sessions": [
            {"sessionId": "s1", "canonicalRoot": "/tmp/a", "state": "active"},
            {"sessionId": "s2", "canonicalRoot": "/tmp/b", "state": "idle"}]}))
        .unwrap();
        assert_eq!(tracked(&r), 2);

        // s1 is absent from this snapshot and was never sent as `gone`.
        let r = call(json!({"seq": 2, "snapshot": true, "sessions": [
            {"sessionId": "s2", "canonicalRoot": "/tmp/b", "state": "idle"}]}))
        .unwrap();
        assert_eq!(
            tracked(&r),
            1,
            "a session the snapshot omits must be dropped, not kept from the previous state"
        );

        // The empty snapshot: the emitter states it tracks nothing.
        let r = call(json!({"seq": 3, "snapshot": true, "sessions": []})).unwrap();
        assert_eq!(
            tracked(&r),
            0,
            "an empty snapshot empties the mirror; it is a statement, not a no-op"
        );
    }

    #[test]
    fn enumerate_annotation_joins_liveness_by_root_containment() {
        let handler = ProjectsHandler::new().unwrap();
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
                    root_records: None,
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
                    root_records: None,
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
        // The daemon reads no trust tier or storage binding, so the manifest
        // declares neither; a fabricated value here would be the wire lying.
        assert!(value.get("trust_tier").is_none());
        assert!(value.get("bindings").is_none());
        let operations = value["provides"][0]["operations"].as_array().unwrap();
        assert!(operations
            .iter()
            .any(|operation| operation["name"] == "resolve"));
        assert!(operations
            .iter()
            .any(|operation| operation["name"] == "seed_import"));
        // Consumers declare this `required`, so its spelling is a contract.
        assert_eq!(
            value["capabilities"]["provides"],
            serde_json::json!(["project-identity/v1", "agent-identity/v1"])
        );
        assert_eq!(value["capabilities"]["requires"], json!([]));
        for (name, kind) in [
            ("attach_root", "mutate"),
            ("identity_log.enable", "mutate"),
            ("resolve_root_key", "query"),
            ("preview_attach_root", "query"),
            ("identity_log.status", "query"),
        ] {
            let matches: Vec<_> = operations.iter().filter(|op| op["name"] == name).collect();
            assert_eq!(matches.len(), 1, "{name} must be declared exactly once");
            assert_eq!(matches[0]["kind"], kind, "{name}");
        }
        assert!(subc_protocol::manifest::is_valid_capability_identifier(
            PROJECT_IDENTITY_CAPABILITY
        ));
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

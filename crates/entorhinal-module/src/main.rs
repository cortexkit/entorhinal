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
        build_provenance_from_source, BuildGitShaSource, CapabilityDeclarations, Concurrency,
        GitTreeState, ManagementOperation, ManagementOperationKind, ManifestProvenance,
        ModuleManifest, ProviderRole,
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
    subc_client_rs::serve(manifest(), ProjectsHandler::new()).await?;
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
    route_admissions: Mutex<HashMap<RouteKey, RouteAdmission>>,
}

/// The parts of a route's bind stamp that decide what the route may do here.
#[derive(Clone, Default)]
struct RouteAdmission {
    principal: Option<Principal>,
    /// The flow whose scope the route was opened under, if any.
    flow_id: Option<String>,
}

impl RouteAdmission {
    fn from_bind(principal: Option<Principal>, scope: Option<&ScopeStamp>) -> Self {
        Self {
            principal,
            flow_id: scope.and_then(|stamp| stamp.attributes.flow_id.clone()),
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
    "upgrade_implicit",
    "remove",
    "seed_import",
    "rebuild",
    "projects.session_liveness",
    "add_root",
    "remove_root",
    "attach_derived_parent",
    "approve_root",
    "unapprove_root",
    "approve_project",
    "unapprove_project",
];

/// Refuse a mutating method unless the route was opened by the operator
/// (`Direct`: the `ck` faces) or by the executive.
///
/// This stops a module that has no business writing here from changing the
/// registry by accident or through a bug. Project ids key other modules' state,
/// so a stray `remove` would cut a project off from it. It is not a security
/// boundary: any process running as the user can connect as `Direct` or open
/// the store file itself. A route with no recorded principal is refused rather
/// than assumed to be the operator.
fn authorize_write(method: &str, principal: Option<&Principal>) -> Result<(), HandlerError> {
    if !MUTATING_METHODS.contains(&method) {
        return Ok(());
    }
    match principal {
        Some(Principal::Direct) => Ok(()),
        Some(Principal::Reserved { module_id }) if module_id == WRITER_MODULE => Ok(()),
        other => Err(HandlerError {
            code: "write_not_permitted".to_string(),
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
    fn new() -> Self {
        Self {
            store: Arc::new(Mutex::new(None)),
            health: Arc::new(HealthGauges::default()),
            liveness: Arc::new(Mutex::new(LivenessState::default())),
            route_admissions: Mutex::new(HashMap::new()),
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

    /// Whether a request on this route may run `method`: a flow-scoped route
    /// may not write at all, and otherwise only the operator and the executive
    /// may write.
    fn admit(&self, method: &str, key: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        refuse_flow_write(method, admission.flow_id.as_deref())?;
        authorize_write(method, admission.principal.as_ref())
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
    async fn handle(&self, ctx: RequestCtx, body: Vec<u8>) -> HandlerOutcome {
        let request = match serde_json::from_slice::<WireRequest>(&body) {
            Ok(request) => request,
            Err(error) => {
                return HandlerOutcome::Error {
                    code: "invalid_request".to_string(),
                    message: format!("request must be JSON with method and params: {error}"),
                }
            }
        };

        if let Err(error) = self.admit(&request.method, route_key(&ctx.route_handle())) {
            return HandlerOutcome::Error {
                code: error.code,
                message: error.message,
            };
        }

        let result = match request.method.as_str() {
            "resolve" => self.resolve(request.params),
            "resolve_project_id" => self.resolve_project_id(request.params),
            "enumerate" => self.enumerate(request.params),
            "journal_tail" => self.journal_tail(request.params),
            "register" => self.register(request.params),
            "assign_workspace" => self.assign_workspace(request.params),
            "set_workspace_root" => self.set_workspace_root(request.params),
            "upgrade_implicit" => self.upgrade_implicit(request.params),
            "remove" => self.remove(request.params),
            "seed_import" => self.seed_import(request.params),
            "projects.session_liveness" => self.session_liveness(request.params),
            "add_root" => self.add_root(request.params),
            "remove_root" => self.remove_root(request.params),
            "attach_derived_parent" => self.attach_derived_parent(request.params),
            "approve_root" => self.set_root_approval(request.params, true),
            "unapprove_root" => self.set_root_approval(request.params, false),
            "trust" => self.trust(request.params),
            "approve_project" => self.set_project_approval(request.params, true),
            "unapprove_project" => self.set_project_approval(request.params, false),
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

    async fn on_bind(&self, request: &RouteBindRequest) -> BindDecision {
        self.route_admissions().insert(
            route_key(&request.handle),
            RouteAdmission::from_bind(request.principal.clone(), request.scope.as_ref()),
        );
        BindDecision::accept()
    }

    async fn on_route_gone(&self, handle: &RouteHandle) {
        self.route_admissions().remove(&route_key(handle));
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
                    "opening projects storage: {error}; retrying for up to {}s",
                    LEASE_HELD_RETRY_BUDGET.as_secs()
                );
                self.health.store_opening.store(true, Ordering::Relaxed);
                let store = Arc::clone(&self.store);
                let health = Arc::clone(&self.health);
                tokio::spawn(async move {
                    let outcome = retry_open_while_lease_held(&descriptor).await;
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
                    "refused an out-of-order session_liveness batch and asked for a snapshot; \
                     the emitter is delivering out of seq order"
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
        })
    }

    fn resolve(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<ResolveParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| {
            store.resolve_with_binding(&params.canonical_root, params.execution_binding.as_ref())
        })?;
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
        let reply = self.record_mutation(self.with_store(|s| s.register(req)))?;
        // With root records on, a newly registered root is bound at once, so
        // it never answers as unbound. Unreadable identity leaves it unbound
        // and authorizing nothing; the registration itself still stands.
        self.with_store(|store| {
            if store.root_records_enabled() {
                if let Err(error) = store.bind_unbound_roots("entorhinal") {
                    tracing::warn!(target: "binding", "binding after register failed: {error}");
                }
            }
            Ok(())
        })?;
        Ok(reply)
    }
    fn set_workspace_root(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<SetWorkspaceRootRequest>(params).map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.set_workspace_root(req)))
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
    fn add_root(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::AddRootRequest>(params)
            .map_err(invalid_params)?;
        let reply = self.record_mutation(self.with_store(|s| s.add_root(req)))?;
        // As after register: bind at once, so the new root never answers as
        // unbound while root records are on. Binding never approves.
        self.with_store(|store| {
            if store.root_records_enabled() {
                if let Err(error) = store.bind_unbound_roots("entorhinal") {
                    tracing::warn!(target: "binding", "binding after add_root failed: {error}");
                }
            }
            Ok(())
        })?;
        Ok(reply)
    }
    fn remove_root(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::RemoveRootRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.remove_root(req)))
    }
    fn attach_derived_parent(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<entorhinal_core::AttachDerivedParentRequest>(params)
            .map_err(invalid_params)?;
        self.record_mutation(self.with_store(|s| s.attach_derived_parent(req)))
    }
    fn set_root_approval(&self, params: Value, approve: bool) -> Result<Vec<u8>, HandlerError> {
        let req = serde_json::from_value::<RootApprovalParams>(params).map_err(invalid_params)?;
        let actor = req.actor.unwrap_or_else(|| "operator".to_string());
        self.record_mutation(self.with_store(|s| {
            if approve {
                s.approve_root(&req.root, &actor)
            } else {
                s.unapprove_root(&req.root, &actor)
            }
        }))
    }
    fn set_project_approval(&self, params: Value, approve: bool) -> Result<Vec<u8>, HandlerError> {
        let req =
            serde_json::from_value::<ProjectApprovalParams>(params).map_err(invalid_params)?;
        let actor = req.actor.unwrap_or_else(|| "operator".to_string());
        self.record_mutation(
            self.with_store(|s| s.set_project_approval(&req.canonical_root, &actor, approve)),
        )
    }
    fn trust(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let params = serde_json::from_value::<ResolveParams>(params).map_err(invalid_params)?;
        let reply = self.with_store(|store| store.trust(&params.canonical_root))?;
        self.record_query(reply.generation, &self.health.resolve_count);
        encode_result(reply)
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
        }])
    // Capability grammar declarations wait for the fleet owner round: the
    // cuts draft records entorhinal -> project-registry/v1, but a provides
    // claim belongs with the reviewed registry entry and corpus, not ahead
    // of them. None = grammar inactive for this module, deliberately.
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
    // The capability other modules declare `required` when they cannot work
    // without project identity. The daemon holds a module not-ready while a
    // capability it requires has no registered provider, so this name is what
    // makes that dependency enforceable. It covers the whole surface above
    // (resolve, resolve_project_id, enumerate, register and the workspace
    // operations); a breaking change to that surface moves it to v2.
    .capabilities(Some(CapabilityDeclarations {
        provides: vec![PROJECT_IDENTITY_CAPABILITY.to_owned()],
        requires: Vec::new(),
        must_never_reach: Vec::new(),
    }))
    .build()
}

/// Build facts embedded by `build.rs`. A revision from a dirty tree is declined
/// with a reason, and a build with no git (a source tarball) says so, rather
/// than declaring a revision that does not describe the running code.
fn declared_provenance() -> Option<ManifestProvenance> {
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
/// budget, end the retry with that error.
async fn retry_open_while_lease_held(
    descriptor: &StorageDescriptor,
) -> Result<RegistryStore, String> {
    let started = Instant::now();
    let mut delay = LEASE_HELD_RETRY_FIRST_DELAY;
    loop {
        tokio::time::sleep(delay).await;
        match RegistryStore::open(descriptor) {
            Err(error) if error.is_lease_held() && started.elapsed() < LEASE_HELD_RETRY_BUDGET => {
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

    fn reserved(module_id: &str) -> Principal {
        Principal::Reserved {
            module_id: module_id.to_string(),
        }
    }

    #[test]
    fn writes_are_refused_unless_the_operator_or_the_executive_opened_the_route() {
        for method in MUTATING_METHODS {
            assert!(
                authorize_write(method, Some(&Principal::Direct)).is_ok(),
                "{method} from direct"
            );
            assert!(
                authorize_write(method, Some(&reserved(WRITER_MODULE))).is_ok(),
                "{method} from {WRITER_MODULE}"
            );
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
    fn flow_stamp(flow_id: Option<&str>) -> ScopeStamp {
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
        let handler = ProjectsHandler::new();
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

    fn scratch_descriptor(name: &str) -> (PathBuf, StorageDescriptor) {
        let dir = std::env::temp_dir().join(format!(
            "entorhinal-{name}-{}-{}",
            std::process::id(),
            unix_millis()
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
        let handler = ProjectsHandler::new();

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
        let handler = ProjectsHandler::new();

        let started = Instant::now();
        handler.on_hello_ack(&hello_ack(&descriptor)).await;
        wait_until_settled(&handler, LEASE_HELD_RETRY_BUDGET + Duration::from_secs(3)).await;
        let elapsed = started.elapsed();
        assert!(
            elapsed >= LEASE_HELD_RETRY_BUDGET,
            "gave up before the budget: {elapsed:?}"
        );
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
        let handler = ProjectsHandler::new();

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
        let handler = ProjectsHandler::new();
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
        let handler = ProjectsHandler::new();
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
        let handler = ProjectsHandler::new();
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
        let handler = ProjectsHandler::new();
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
            serde_json::json!(["project-identity/v1"])
        );
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

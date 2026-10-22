//! Agent sessions: `agent.register_session`, `agent.end_session`,
//! `agent.set_session_network`, `agent.sessions`, `agent.for_session` and
//! `agent.session_liveness`.
//!
//! A session is registered by the module that starts it for an agent. Two
//! reserved capabilities may do that: `agent-orchestrator/v1`, held by the
//! module that owns the agent's work, and `agent-host/v1`, held by the module
//! that hosts sessions and feeds their liveness. The subc daemon binds each
//! reserved capability to exactly one module id, so the caller is admitted
//! when its daemon-stamped principal is `reserved:<that module id>`.
//!
//! Lookup answers identify the daemon's sole provider module id, or the lack
//! of a sole provider. Each bound route caches its own answers. Replacing a
//! reserved provider requires the previous provider to leave the daemon, which
//! closes that provider's routes. A cached answer that would refuse a caller
//! is refreshed once, never on a timer. Closing a route or changing its
//! principal invalidates its cache. No fixed module name decides admission.

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use entorhinal_core::{RegistryStore, SessionRegistration};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_client_rs::{CallError, ConsumerOptions, SubcConsumer};
use subc_protocol::Principal;

use super::{encode_result, HandlerError, ProjectsHandler, SessionKey};

pub(super) const REGISTER_SESSION: &str = "agent.register_session";
/// Marks a session's binding ended. Only the orchestrator's provider may.
pub(super) const END_SESSION: &str = "agent.end_session";
/// Records whether a session's agent can reach the network. Only the
/// orchestrator's provider may, because it is the one that knows from the
/// session's plan.
pub(super) const SET_SESSION_NETWORK: &str = "agent.set_session_network";
pub(super) const FOR_SESSION: &str = "agent.for_session";
pub(super) const SESSION_FOR_REF: &str = "agent.session_for_ref";
pub(super) const SESSION_LIVENESS: &str = "agent.session_liveness";
/// Lists one agent's session bindings, newest first.
pub(super) const SESSIONS: &str = "agent.sessions";
/// Resolves a runner's session id to its binding.
pub(super) const SESSION_FOR_RUNNER: &str = "agent.session_for_runner";
/// The session reads, open to every caller like the other reads.
pub(super) const READ_METHODS: &[&str] = &[
    FOR_SESSION,
    SESSION_FOR_REF,
    SESSION_LIVENESS,
    SESSIONS,
    SESSION_FOR_RUNNER,
];

/// The module that owns an agent's work and registers the sessions it starts.
pub(super) const ORCHESTRATOR_CAPABILITY: &str = "agent-orchestrator/v1";
/// The module that hosts sessions and is the source of their liveness.
pub(super) const HOST_CAPABILITY: &str = "agent-host/v1";

/// How long one capability lookup may take. The lookup is a local round trip
/// to the daemon; one that has not answered by then counts as a transport
/// failure, which admits no one.
const RESOLVE_DEADLINE: Duration = Duration::from_secs(5);

const REF_KIND_MAX: usize = 64;
const REF_MAX: usize = 512;
const HARNESS_MAX: usize = 32;
const SESSION_ID_MAX: usize = 256;
const PROJECT_ROOT_MAX: usize = 4096;
const LIVENESS_REFS_MAX: usize = 256;
/// A runner is a module id, bounded like the other module ids callers send.
const RUNNER_MAX: usize = 64;
const SESSIONS_LIMIT_DEFAULT: usize = 100;
const SESSIONS_LIMIT_MAX: usize = 500;

/// What the daemon answered when asked which module provides a capability.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum ProviderResolution {
    /// Exactly one module provides it.
    Provider(String),
    /// No module provides it (`capability_unprovided`).
    Unprovided,
    /// More than one module claims it (`capability_ambiguous`). This is a
    /// refusal, never a pick.
    Ambiguous(Vec<String>),
    /// The daemon refused the identifier (`invalid_capability_identifier`).
    InvalidIdentifier,
    /// The daemon could not be asked or did not answer.
    Transport(String),
}

impl ProviderResolution {
    fn sole(&self) -> Option<&str> {
        match self {
            Self::Provider(module_id) => Some(module_id),
            _ => None,
        }
    }

    fn outcome(&self) -> &'static str {
        match self {
            Self::Provider(_) => "resolved",
            Self::Unprovided => "capability_unprovided",
            Self::Ambiguous(_) => "capability_ambiguous",
            Self::InvalidIdentifier => "invalid_capability_identifier",
            Self::Transport(_) => "transport_failure",
        }
    }
}

/// Asks which module provides a capability. Production asks the subc daemon;
/// tests script each capability's answer.
#[async_trait]
pub(super) trait ProviderResolver: Send + Sync {
    async fn resolve_provider(&self, capability: &str) -> ProviderResolution;
}

/// Resolves through the daemon named by `--subc`. The consumer connection is
/// opened on the first session write, never at startup, and dropped after a
/// transport failure so the next request reconnects.
pub(super) struct SubcProviderResolver {
    connection_file: Option<PathBuf>,
    consumer: tokio::sync::Mutex<Option<Arc<SubcConsumer>>>,
}

impl SubcProviderResolver {
    pub(super) fn from_args(args: impl IntoIterator<Item = std::ffi::OsString>) -> Self {
        Self {
            connection_file: super::log_client::subc_connection_file(args),
            consumer: tokio::sync::Mutex::new(None),
        }
    }

    async fn consumer(&self) -> Result<Arc<SubcConsumer>, String> {
        let mut slot = self.consumer.lock().await;
        if let Some(consumer) = slot.as_ref() {
            return Ok(consumer.clone());
        }
        let consumer = match &self.connection_file {
            Some(path) => SubcConsumer::connect(path, ConsumerOptions::default()).await,
            None => SubcConsumer::connect_default(ConsumerOptions::default()).await,
        }
        .map_err(|error| error.to_string())?;
        let consumer = Arc::new(consumer);
        *slot = Some(consumer.clone());
        Ok(consumer)
    }
}

#[async_trait]
impl ProviderResolver for SubcProviderResolver {
    async fn resolve_provider(&self, capability: &str) -> ProviderResolution {
        let consumer = match self.consumer().await {
            Ok(consumer) => consumer,
            Err(error) => return ProviderResolution::Transport(error),
        };
        match consumer.resolve_provider(capability).await {
            Ok(module_id) => ProviderResolution::Provider(module_id),
            Err(CallError::CapabilityUnprovided { .. }) => ProviderResolution::Unprovided,
            Err(CallError::CapabilityAmbiguous { claimants, .. }) => {
                ProviderResolution::Ambiguous(claimants)
            }
            Err(CallError::InvalidCapabilityIdentifier { .. }) => {
                ProviderResolution::InvalidIdentifier
            }
            Err(error) => {
                self.consumer.lock().await.take();
                ProviderResolution::Transport(error.to_string())
            }
        }
    }
}

/// Session capabilities for one request. Feed batches resolve only the host;
/// their unused orchestrator slot admits no one.
#[derive(Clone, Debug)]
pub(super) struct ResolvedProviders {
    pub(super) orchestrator: ProviderResolution,
    pub(super) host: ProviderResolution,
}

#[derive(Clone, Debug, Default)]
struct CachedProviders {
    host: Option<ProviderResolution>,
    orchestrator: Option<ProviderResolution>,
    invalidated: bool,
}

/// A route owns both its answers and its lookup lock. The lock coalesces first
/// use by concurrent requests without blocking unrelated routes.
#[derive(Debug, Default)]
pub(super) struct RouteProviders {
    cached: std::sync::Mutex<CachedProviders>,
    resolving: tokio::sync::Mutex<()>,
}

impl RouteProviders {
    /// Retire this admission, including answers held by an in-flight lookup.
    /// A replacement route gets a new cache rather than reviving this one.
    pub(super) fn invalidate(&self) {
        let mut cached = self.cached.lock().unwrap_or_else(|e| e.into_inner());
        cached.host = None;
        cached.orchestrator = None;
        cached.invalidated = true;
    }
}

/// Which capability a registration was admitted under, stored on the binding.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SessionRole {
    Orchestrator,
    Host,
}

impl SessionRole {
    fn as_str(self) -> &'static str {
        match self {
            Self::Orchestrator => "orchestrator",
            Self::Host => "host",
        }
    }
}

/// The refusal when neither session capability has a sole provider, or when
/// no resolution was performed (`providers` is `None`): only the served
/// request path can await the daemon, so the synchronous `handle_request`
/// path passes `None` and always refuses. When providers were resolved, the
/// detail names what the daemon answered for each capability.
pub(super) fn orchestrator_unbound(providers: Option<&ResolvedProviders>) -> HandlerError {
    let detail = providers.map(|providers| {
        json!({
            ORCHESTRATOR_CAPABILITY: providers.orchestrator.outcome(),
            HOST_CAPABILITY: providers.host.outcome(),
        })
    });
    HandlerError {
        code: "orchestrator_unbound".into(),
        message: match providers {
            Some(_) => format!(
                "no module is the sole provider of {ORCHESTRATOR_CAPABILITY} or {HOST_CAPABILITY}, so no one may register sessions"
            ),
            None => format!(
                "'{REGISTER_SESSION}' resolves its providers only on the served request path; this request had none"
            ),
        },
        detail,
    }
}

/// Admit `agent.register_session`. First the providers: with neither
/// capability resolved to a sole provider, refuse `orchestrator_unbound`.
/// Then the caller: it must be `reserved:<provider>` of one of them, else
/// `caller_forbidden`. A module providing both is admitted as the
/// orchestrator.
pub(super) fn admit_session_registration(
    providers: Option<&ResolvedProviders>,
    principal: Option<&Principal>,
) -> Result<SessionRole, HandlerError> {
    let (orchestrator, host) = match providers {
        Some(providers) => (providers.orchestrator.sole(), providers.host.sole()),
        None => (None, None),
    };
    if orchestrator.is_none() && host.is_none() {
        return Err(orchestrator_unbound(providers));
    }
    let caller = match principal {
        Some(Principal::Reserved { module_id }) => Some(module_id.as_str()),
        _ => None,
    };
    if caller.is_some() && caller == orchestrator {
        return Ok(SessionRole::Orchestrator);
    }
    if caller.is_some() && caller == host {
        return Ok(SessionRole::Host);
    }
    Err(HandlerError::new(
        "caller_forbidden",
        format!(
            "'{REGISTER_SESSION}' is accepted only from the provider of {ORCHESTRATOR_CAPABILITY} or {HOST_CAPABILITY}; this route was opened by {}",
            super::principal_label(principal)
        ),
    ))
}

/// Admit `agent.end_session` or `agent.set_session_network` (`method`):
/// only the sole provider of `agent-orchestrator/v1`, matched against the
/// route's daemon-stamped principal. The host provider registers sessions but
/// does not end them; its clean ends arrive through the liveness feed
/// instead. Without a resolved sole orchestrator, refuse
/// `orchestrator_unbound`; any other caller gets `caller_forbidden`.
pub(super) fn admit_orchestrator_write(
    method: &str,
    providers: Option<&ResolvedProviders>,
    principal: Option<&Principal>,
) -> Result<(), HandlerError> {
    let Some(orchestrator) = providers.and_then(|providers| providers.orchestrator.sole()) else {
        return Err(HandlerError {
            code: "orchestrator_unbound".into(),
            message: match providers {
                // The wording predates the network write and is kept for
                // `agent.end_session`, whose refusal callers may match.
                Some(_) if method == END_SESSION => format!(
                    "no module is the sole provider of {ORCHESTRATOR_CAPABILITY}, so no one may end sessions"
                ),
                Some(_) => format!(
                    "no module is the sole provider of {ORCHESTRATOR_CAPABILITY}, so no one may call '{method}'"
                ),
                None => format!(
                    "'{method}' resolves its provider only on the served request path; this request had none"
                ),
            },
            detail: providers.map(|providers| {
                json!({ORCHESTRATOR_CAPABILITY: providers.orchestrator.outcome()})
            }),
        });
    };
    if matches!(principal, Some(Principal::Reserved { module_id }) if module_id == orchestrator) {
        return Ok(());
    }
    Err(HandlerError::new(
        "caller_forbidden",
        format!(
            "'{method}' is accepted only from the provider of {ORCHESTRATOR_CAPABILITY}; this route was opened by {}",
            super::principal_label(principal)
        ),
    ))
}

/// Only the sole host claimant may feed liveness. Missing or failed resolution
/// takes precedence over caller admission, including on the synchronous seam.
pub(super) fn admit_liveness_feed(
    providers: Option<&ResolvedProviders>,
    principal: Option<&Principal>,
) -> Result<(), HandlerError> {
    let host = providers.and_then(|providers| providers.host.sole());
    let Some(host) = host else {
        return Err(HandlerError {
            code: "host_unbound".into(),
            message: format!("no sole provider of {HOST_CAPABILITY} was resolved for this batch"),
            detail: providers.map(|providers| json!({HOST_CAPABILITY: providers.host.outcome()})),
        });
    };
    if matches!(principal, Some(Principal::Reserved { module_id }) if module_id == host) {
        return Ok(());
    }
    Err(HandlerError::new(
        "caller_forbidden",
        format!(
            "'projects.session_liveness' is accepted only from the provider of {HOST_CAPABILITY}; this route was opened by {}",
            super::principal_label(principal)
        ),
    ))
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterSessionRequest {
    agent_id: String,
    ref_kind: String,
    #[serde(rename = "ref")]
    session_ref: String,
    harness: String,
    session_id: String,
    project_root: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EndSessionRequest {
    harness: String,
    session: String,
    #[serde(default)]
    runner: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SetSessionNetworkRequest {
    harness: String,
    session: String,
    network: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSessionsRequest {
    agent_id: String,
    #[serde(default)]
    cursor: Option<String>,
    #[serde(default)]
    limit: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionForRunnerRequest {
    runner: String,
    session: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForSessionRequest {
    harness: String,
    session_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionForRefRequest {
    ref_kind: String,
    #[serde(rename = "ref")]
    session_ref: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionLivenessRequest {
    refs: Vec<SessionRef>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SessionRef {
    ref_kind: String,
    #[serde(rename = "ref")]
    session_ref: String,
}

/// One answer of `agent.session_liveness`. `cause` is present only on an
/// `unknown` answer whose mirrored session carried one.
#[derive(Debug, PartialEq, Eq, Serialize)]
pub(super) struct LivenessAnswer {
    pub(super) state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(super) cause: Option<String>,
}

impl LivenessAnswer {
    fn unknown(cause: Option<String>) -> Self {
        Self {
            state: "unknown",
            cause,
        }
    }
}

fn decode<T: DeserializeOwned>(params: Value) -> Result<T, HandlerError> {
    serde_json::from_value(params)
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))
}

/// Byte-length bounds share the decoder's refusal code: a value outside its
/// bound is a malformed request like an unknown or missing field.
fn bounded(field: &str, value: &str, max: usize) -> Result<(), HandlerError> {
    if value.is_empty() || value.len() > max {
        return Err(HandlerError::new(
            "invalid_request",
            format!("{field} must be 1 to {max} bytes"),
        ));
    }
    Ok(())
}

fn session_key_bounds(harness: &str, session_id: &str) -> Result<(), HandlerError> {
    bounded("harness", harness, HARNESS_MAX)?;
    bounded("session_id", session_id, SESSION_ID_MAX)
}

fn ref_bounds(ref_kind: &str, session_ref: &str) -> Result<(), HandlerError> {
    bounded("ref_kind", ref_kind, REF_KIND_MAX)?;
    bounded("ref", session_ref, REF_MAX)
}

/// A runner names a module, so beyond its length bound it may hold no
/// control character.
fn runner_bounds(runner: &str) -> Result<(), HandlerError> {
    bounded("runner", runner, RUNNER_MAX)?;
    if runner.chars().any(char::is_control) {
        return Err(HandlerError::new(
            "invalid_request",
            "runner must not contain control characters",
        ));
    }
    Ok(())
}

/// Take the optional `runner` out of a registration before the strict decode
/// of the rest. Registration's field set and its `unknown field` refusal are a
/// published contract that callers decode byte for byte, and listing `runner`
/// among the expected fields would change that refusal's text.
fn take_runner(params: &mut Value) -> Result<Option<String>, HandlerError> {
    let Some(object) = params.as_object_mut() else {
        return Ok(None);
    };
    match object.remove("runner") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(runner)) => Ok(Some(runner)),
        Some(_) => Err(HandlerError::new(
            "invalid_request",
            "runner must be a string or null",
        )),
    }
}

/// Refuse a network value other than `yes`, `no` or `unknown`.
fn network_bounds(network: &str) -> Result<(), HandlerError> {
    entorhinal_core::session_network::check_network(network).map_err(HandlerError::from)
}

/// Take the optional `network` out of a registration before the strict
/// decode of the rest, for the same reason as `runner`: listing it among the
/// expected fields would change the published unknown-field refusal. Null
/// reads as absent, like a null runner.
fn take_network(params: &mut Value) -> Result<Option<String>, HandlerError> {
    let Some(object) = params.as_object_mut() else {
        return Ok(None);
    };
    match object.remove("network") {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(network)) => {
            network_bounds(&network)?;
            Ok(Some(network))
        }
        Some(_) => Err(HandlerError::new(
            "invalid_request",
            "network must be yes, no or unknown",
        )),
    }
}

/// The opaque `agent.sessions` cursor: base64url of the last listed row's
/// `[registered_at_ms, ref_kind, ref]`.
fn encode_cursor(cursor: &entorhinal_core::SessionCursor) -> String {
    use base64::Engine;
    let bytes = serde_json::to_vec(&json!([
        cursor.registered_at_ms,
        cursor.ref_kind,
        cursor.session_ref
    ]))
    .expect("a cursor encodes");
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes)
}

fn decode_cursor(cursor: &str) -> Result<entorhinal_core::SessionCursor, HandlerError> {
    use base64::Engine;
    let refused =
        || HandlerError::new("invalid_request", "cursor was not issued by agent.sessions");
    let bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(cursor)
        .map_err(|_| refused())?;
    let (registered_at_ms, ref_kind, session_ref): (i64, String, String) =
        serde_json::from_slice(&bytes).map_err(|_| refused())?;
    Ok(entorhinal_core::SessionCursor {
        registered_at_ms,
        ref_kind,
        session_ref,
    })
}

impl super::LivenessState {
    /// Map a bound session's mirrored state to the answer callers act on.
    /// `active` and `idle` both mean the host process is open. A mirror still
    /// waiting for a snapshot cannot vouch for anything, so every session
    /// reads `unknown` until one arrives.
    fn answer(&self, key: &SessionKey) -> LivenessAnswer {
        let entry = self.sessions.get(key);
        if self.stale {
            return LivenessAnswer::unknown(entry.and_then(|entry| entry.cause.clone()));
        }
        match entry {
            Some(entry) => match entry.state.as_str() {
                "active" | "idle" => LivenessAnswer {
                    state: "active",
                    cause: None,
                },
                _ => LivenessAnswer::unknown(entry.cause.clone()),
            },
            None if self.gone.contains(key) => LivenessAnswer {
                state: "gone",
                cause: None,
            },
            None => LivenessAnswer::unknown(None),
        }
    }
}

impl ProjectsHandler {
    /// The agent-orchestrator/v1 role relays moves confirmed by the operator's
    /// phone; agent-host/v1 only registers host sessions and has no move authority.
    /// Refresh a cached provider mismatch once because the orchestrator may have
    /// changed since this route opened, without granting the host move authority.
    pub(super) async fn move_orchestrator(&self, admission: &super::RouteAdmission) -> bool {
        let move_route = &admission.providers;
        let _move_lookup = move_route.resolving.lock().await;
        let move_cached = move_route
            .cached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        if move_cached.invalidated {
            return false;
        }
        let mut answer = match move_cached.orchestrator.clone() {
            Some(answer) => answer,
            None => self.resolve_session_provider(ORCHESTRATOR_CAPABILITY).await,
        };
        let matches = |answer: &ProviderResolution| match &admission.principal {
            Some(Principal::Reserved { module_id }) => answer.sole() == Some(module_id.as_str()),
            _ => false,
        };
        if !matches(&answer)
            && move_cached.orchestrator.is_some()
            && !matches!(answer, ProviderResolution::Transport(_))
        {
            answer = self.resolve_session_provider(ORCHESTRATOR_CAPABILITY).await;
        }
        let mut move_cache = move_route.cached.lock().unwrap_or_else(|e| e.into_inner());
        if move_cache.invalidated {
            return false;
        }
        move_cache.orchestrator = if matches!(answer, ProviderResolution::Transport(_)) {
            None
        } else {
            Some(answer.clone())
        };
        matches(&answer)
    }

    /// A stalled catalog lookup admits no one, just like a transport failure.
    pub(super) async fn resolve_session_provider(
        &self,
        capability: &'static str,
    ) -> ProviderResolution {
        tokio::time::timeout(
            RESOLVE_DEADLINE,
            self.provider_resolver.resolve_provider(capability),
        )
        .await
        .unwrap_or_else(|_| {
            ProviderResolution::Transport(format!(
                "resolving {capability} took longer than {RESOLVE_DEADLINE:?}"
            ))
        })
    }

    async fn resolve_needed_providers(
        &self,
        providers: &mut ResolvedProviders,
        host: bool,
        orchestrator: bool,
    ) {
        let (host, orchestrator) = tokio::join!(
            async {
                if host {
                    Some(self.resolve_session_provider(HOST_CAPABILITY).await)
                } else {
                    None
                }
            },
            async {
                if orchestrator {
                    Some(self.resolve_session_provider(ORCHESTRATOR_CAPABILITY).await)
                } else {
                    None
                }
            }
        );
        if let Some(host) = host {
            providers.host = host;
        }
        if let Some(orchestrator) = orchestrator {
            providers.orchestrator = orchestrator;
        }
    }

    /// Use route-local claimant answers, resolving missing capabilities and
    /// refreshing cached answers once before a refusal. Transport failures are
    /// never remembered, even when the other capability resolved successfully.
    pub(super) async fn route_session_providers(
        &self,
        method: &str,
        admission: &super::RouteAdmission,
    ) -> ResolvedProviders {
        let route = &admission.providers;
        let _lookup = route.resolving.lock().await;
        let cached = route
            .cached
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone();
        let registration = method == REGISTER_SESSION;
        // Ending a session and recording its network need only the
        // orchestrator's provider; the feed needs only the host's;
        // registration needs both.
        let ending = method == END_SESSION || method == SET_SESSION_NETWORK;
        let wants_host = !ending;
        let wants_orchestrator = registration || ending;
        let mut providers = ResolvedProviders {
            host: cached
                .host
                .clone()
                .unwrap_or(ProviderResolution::Unprovided),
            orchestrator: cached
                .orchestrator
                .clone()
                .unwrap_or(ProviderResolution::Unprovided),
        };
        if cached.invalidated {
            return providers;
        }
        self.resolve_needed_providers(
            &mut providers,
            wants_host && cached.host.is_none(),
            wants_orchestrator && cached.orchestrator.is_none(),
        )
        .await;
        let transport_failed = |providers: &ResolvedProviders| {
            (wants_host && matches!(providers.host, ProviderResolution::Transport(_)))
                || (wants_orchestrator
                    && matches!(providers.orchestrator, ProviderResolution::Transport(_)))
        };
        let would_refuse = if registration {
            admit_session_registration(Some(&providers), admission.principal.as_ref()).is_err()
        } else if ending {
            admit_orchestrator_write(method, Some(&providers), admission.principal.as_ref())
                .is_err()
        } else {
            admit_liveness_feed(Some(&providers), admission.principal.as_ref()).is_err()
        };
        if would_refuse && !transport_failed(&providers) {
            // Fresh answers already read above need no second lookup. Only
            // cached answers get the one refresh before the final decision.
            self.resolve_needed_providers(
                &mut providers,
                wants_host && cached.host.is_some(),
                wants_orchestrator && cached.orchestrator.is_some(),
            )
            .await;
        }
        let mut cache = route.cached.lock().unwrap_or_else(|e| e.into_inner());
        if cache.invalidated {
            return ResolvedProviders {
                host: ProviderResolution::Unprovided,
                orchestrator: ProviderResolution::Unprovided,
            };
        }
        if transport_failed(&providers) {
            cache.host = None;
            cache.orchestrator = None;
        } else {
            if wants_host {
                cache.host = Some(providers.host.clone());
            }
            if wants_orchestrator {
                cache.orchestrator = Some(providers.orchestrator.clone());
            }
        }
        providers
    }

    /// Called only after admission decided `role`. Decoding comes after
    /// admission, as for every other write, so a caller that may not register
    /// learns nothing about the request's shape.
    pub(super) fn register_session(
        &self,
        params: Value,
        role: SessionRole,
    ) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        register_session_on_store(&store, params, role, (self.clock)(), self.clock)
    }
}

/// `now` stamps the registration; `clock` is read again when a network
/// value commits, so a network entry carries its commit time.
pub(super) fn register_session_on_store(
    store: &RegistryStore,
    params: Value,
    role: SessionRole,
    now: i64,
    clock: fn() -> i64,
) -> Result<Vec<u8>, HandlerError> {
    let mut params = params;
    let runner = take_runner(&mut params)?;
    let network = take_network(&mut params)?;
    let request: RegisterSessionRequest = decode(params)?;
    ref_bounds(&request.ref_kind, &request.session_ref)?;
    session_key_bounds(&request.harness, &request.session_id)?;
    bounded("project_root", &request.project_root, PROJECT_ROOT_MAX)?;
    if let Some(runner) = &runner {
        runner_bounds(runner)?;
    }
    let binding = store
        .register_session(&SessionRegistration {
            agent_id: &request.agent_id,
            ref_kind: &request.ref_kind,
            session_ref: &request.session_ref,
            harness: &request.harness,
            session_id: &request.session_id,
            project_root: &request.project_root,
            registered_by: role.as_str(),
            runner: runner.as_deref(),
            network: network
                .as_deref()
                .map(|network| entorhinal_core::NetworkStamp { network, clock }),
            now_ms: now,
        })
        .map_err(HandlerError::from)?;
    // The reply is the binding as before; neither the runner nor the network
    // value is echoed, so the reply bytes callers decode strictly are the
    // same with or without them.
    encode_result(binding)
}

impl ProjectsHandler {
    /// The synchronous seam. Admission has already passed, which only the
    /// served path can do, so this runs only from that path's tests.
    pub(super) fn end_session(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        end_session_on_store(&store, params, (self.clock)())
    }
}

/// End the binding of `{harness, session}` at `now`, the handler's clock. An
/// unknown session answers `{found: false}` rather than refusing, so a caller
/// can end sessions it never registered here without checking first.
pub(super) fn end_session_on_store(
    store: &RegistryStore,
    params: Value,
    now: i64,
) -> Result<Vec<u8>, HandlerError> {
    let request: EndSessionRequest = decode(params)?;
    bounded("harness", &request.harness, HARNESS_MAX)?;
    bounded("session", &request.session, SESSION_ID_MAX)?;
    if let Some(runner) = &request.runner {
        runner_bounds(runner)?;
    }
    let ended = store
        .end_session(
            &request.harness,
            &request.session,
            request.runner.as_deref(),
            now,
        )
        .map_err(HandlerError::from)?;
    encode_result(match ended {
        entorhinal_core::SessionEnd::NotFound => json!({"found": false}),
        entorhinal_core::SessionEnd::Ended { ended_at_ms }
        | entorhinal_core::SessionEnd::AlreadyEnded { ended_at_ms } => {
            json!({"found": true, "ended_at_ms": ended_at_ms})
        }
    })
}

impl ProjectsHandler {
    /// The synchronous seam, like `end_session`'s: only the served path can
    /// admit the caller, so this runs only from that path's tests.
    pub(super) fn set_session_network(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        set_session_network_on_store(&store, params, self.clock)
    }
}

/// Record the network value of the binding of `{harness, session}`. The
/// stamp is `clock` read when the entry commits. Replies `{network,
/// since_ms}`: the new entry, or the current one when the value is already
/// current and nothing was written.
pub(super) fn set_session_network_on_store(
    store: &RegistryStore,
    params: Value,
    clock: fn() -> i64,
) -> Result<Vec<u8>, HandlerError> {
    let request: SetSessionNetworkRequest = decode(params)?;
    bounded("harness", &request.harness, HARNESS_MAX)?;
    bounded("session", &request.session, SESSION_ID_MAX)?;
    network_bounds(&request.network)?;
    let change = store
        .set_session_network(&request.harness, &request.session, &request.network, clock)
        .map_err(HandlerError::from)?;
    encode_result(change.entry())
}

impl ProjectsHandler {
    pub(super) fn for_session(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: ForSessionRequest = decode(params)?;
        session_key_bounds(&request.harness, &request.session_id)?;
        let binding = self
            .with_store(|store| store.session_binding_for(&request.harness, &request.session_id))?
            .ok_or_else(|| {
                HandlerError::new(
                    "not_found",
                    "no agent session is registered under this harness and session id",
                )
            })?;
        self.encode_read_result(json!({
            "agent_id": binding.agent_id,
            "ref_kind": binding.ref_kind,
            "ref": binding.session_ref,
        }))
    }

    /// The binding a runner's session id names, so a caller holding only
    /// `{runner, session}` learns the harness and project root behind it. A
    /// binding registered without a runner is `not_found` here; its callers
    /// look it up by harness and session id with `agent.for_session`.
    pub(super) fn session_for_runner(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: SessionForRunnerRequest = decode(params)?;
        runner_bounds(&request.runner)?;
        bounded("session", &request.session, SESSION_ID_MAX)?;
        let binding = self
            .with_store(|store| {
                store.session_binding_for_runner(&request.runner, &request.session)
            })?
            .ok_or_else(|| {
                HandlerError::new(
                    "not_found",
                    "no agent session is registered under this runner and session",
                )
            })?;
        self.encode_read_result(binding)
    }

    pub(super) fn session_for_ref(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: SessionForRefRequest = decode(params)?;
        ref_bounds(&request.ref_kind, &request.session_ref)?;
        let refs = [(request.ref_kind.as_str(), request.session_ref.as_str())];
        let binding = self
            .with_store(|store| store.session_bindings_for_refs(&refs))?
            .into_iter()
            .next()
            .flatten()
            .ok_or_else(|| {
                HandlerError::new(
                    "not_found",
                    "no agent session is registered under this reference",
                )
            })?;
        self.encode_read_result(json!({
            "agent_id": binding.agent_id,
            "ref_kind": binding.ref_kind,
            "ref": binding.session_ref,
            "harness": binding.harness,
            "session_id": binding.session_id,
            "project_root": binding.project_root,
        }))
    }

    /// One page of an agent's sessions with each one's liveness. Liveness
    /// comes from the in-memory mirror exactly as `agent.session_liveness`
    /// answers it; a session the mirror cannot vouch for whose binding has an
    /// end time reads `gone`. A reference moved in from another Mac, with no
    /// host session here yet, reads `unknown` until it ends.
    pub(super) fn agent_sessions(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: AgentSessionsRequest = decode(params)?;
        let limit = match request.limit {
            None => SESSIONS_LIMIT_DEFAULT,
            Some(limit) if (1..=SESSIONS_LIMIT_MAX as u64).contains(&limit) => limit as usize,
            Some(_) => {
                return Err(HandlerError::new(
                    "invalid_request",
                    format!("limit must be 1 to {SESSIONS_LIMIT_MAX}"),
                ))
            }
        };
        let cursor = request.cursor.as_deref().map(decode_cursor).transpose()?;
        let page = self.with_agent_store(|store| {
            store.agent_sessions(&request.agent_id, cursor.as_ref(), limit)
        })?;
        let state = self
            .liveness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let sessions: Vec<Value> = page
            .sessions
            .into_iter()
            .map(|row| {
                let mirrored = match (&row.harness, &row.session) {
                    (Some(harness), Some(session)) => {
                        state.answer(&SessionKey::new(harness, session)).state
                    }
                    _ => "unknown",
                };
                let liveness = if mirrored == "unknown" && row.ended_at_ms.is_some() {
                    "gone"
                } else {
                    mirrored
                };
                json!({
                    "runner": row.runner,
                    "session": row.session,
                    "harness": row.harness,
                    "ref_kind": row.ref_kind,
                    "ref": row.session_ref,
                    "project_root": row.project_root,
                    "registered_at_ms": row.registered_at_ms,
                    "ended_at_ms": row.ended_at_ms,
                    "liveness": liveness,
                    "network": row.network,
                })
            })
            .collect();
        drop(state);
        self.encode_read_result(json!({
            "sessions": sessions,
            "next_cursor": page.next.as_ref().map(encode_cursor),
        }))
    }

    pub(super) fn agent_session_liveness(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let request: SessionLivenessRequest = decode(params)?;
        if request.refs.is_empty() || request.refs.len() > LIVENESS_REFS_MAX {
            return Err(HandlerError::new(
                "invalid_request",
                format!("refs must hold 1 to {LIVENESS_REFS_MAX} entries"),
            ));
        }
        for item in &request.refs {
            ref_bounds(&item.ref_kind, &item.session_ref)?;
        }
        let refs: Vec<(&str, &str)> = request
            .refs
            .iter()
            .map(|item| (item.ref_kind.as_str(), item.session_ref.as_str()))
            .collect();
        let answers = self.read_session_liveness(&refs)?;
        self.encode_read_result(json!({ "refs": answers }))
    }

    pub(super) fn read_session_liveness(
        &self,
        refs: &[(&str, &str)],
    ) -> Result<Vec<LivenessAnswer>, HandlerError> {
        let bindings = self.with_store(|store| store.session_bindings_for_refs(refs))?;
        let state = self
            .liveness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let answers: Vec<LivenessAnswer> = bindings
            .iter()
            .map(|binding| match binding {
                Some(binding) => match (&binding.harness, &binding.session_id) {
                    (Some(harness), Some(session_id)) => {
                        state.answer(&SessionKey::new(harness, session_id))
                    }
                    _ => LivenessAnswer {
                        state: "gone",
                        cause: None,
                    },
                },
                None => LivenessAnswer::unknown(None),
            })
            .collect();
        drop(state);
        Ok(answers)
    }
}

/// A resolver whose answer for each capability a test sets and changes. An
/// unscripted capability answers `capability_unprovided`.
#[cfg(test)]
pub(super) struct FakeResolver {
    answers: std::sync::Mutex<std::collections::HashMap<String, ProviderResolution>>,
    calls: std::sync::atomic::AtomicUsize,
}

#[cfg(test)]
impl FakeResolver {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            answers: std::sync::Mutex::new(std::collections::HashMap::new()),
            calls: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    pub(super) fn set(&self, capability: &str, answer: ProviderResolution) {
        self.answers
            .lock()
            .unwrap()
            .insert(capability.into(), answer);
    }

    pub(super) fn calls(&self) -> usize {
        self.calls.load(std::sync::atomic::Ordering::Relaxed)
    }
}

#[cfg(test)]
#[async_trait]
impl ProviderResolver for FakeResolver {
    async fn resolve_provider(&self, capability: &str) -> ProviderResolution {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.answers
            .lock()
            .unwrap()
            .get(capability)
            .cloned()
            .unwrap_or(ProviderResolution::Unprovided)
    }
}

#[cfg(test)]
pub(super) mod tests {
    use std::path::PathBuf;

    use cortexkit_store_types::StorageDescriptor;
    use entorhinal_core::RegistryStore;
    use subc_client_rs::{HandlerOutcome, ModuleHandler, RouteHandle};

    use super::*;
    use crate::{
        tests::{flow_stamp, reserved, scratch_descriptor},
        RouteAdmission, RouteKey, WRITER_MODULE,
    };

    pub(crate) const NOW: i64 = 700;
    pub(crate) const EXECUTIVE: RouteKey = (50, 1);
    pub(crate) const CALLER: RouteKey = (51, 1);
    const UNKNOWN_AGENT: &str = "agent_0000000000000000";

    pub(crate) struct Fixture {
        dir: PathBuf,
        pub(crate) handler: ProjectsHandler,
        pub(crate) resolver: Arc<FakeResolver>,
    }

    impl Fixture {
        /// A store, with the agent-identity cutover recorded when `cut_over`,
        /// and a resolver naming no provider for either capability.
        pub(crate) fn new(label: &str, cut_over: bool) -> Self {
            Self::with_clock(label, cut_over, || NOW)
        }

        /// The same fixture with the handler reading `clock` for the time.
        pub(crate) fn with_clock(label: &str, cut_over: bool, clock: fn() -> i64) -> Self {
            let (dir, descriptor) = scratch_descriptor(label);
            let resolver = FakeResolver::new();
            let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), clock);
            handler.provider_resolver = resolver.clone() as Arc<dyn ProviderResolver>;
            *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
            handler.route_admissions().insert(
                EXECUTIVE,
                RouteAdmission::from_bind(Some(reserved(WRITER_MODULE)), None),
            );
            let f = Self {
                dir,
                handler,
                resolver,
            };
            if cut_over {
                f.cut_over(&descriptor);
            }
            f
        }

        // An import of a source with no agent tables records the cutover with
        // zero agents, which is all registration needs.
        fn cut_over(&self, descriptor: &StorageDescriptor) {
            let mut source_descriptor = descriptor.clone();
            source_descriptor.storage_namespace.push_str("-source");
            let source_path = self.dir.join("snapshot.db");
            source_descriptor.backend = cortexkit_store_types::StorageBackend::Sqlite {
                path: source_path.to_string_lossy().into_owned(),
            };
            let source = RegistryStore::open(&source_descriptor).unwrap();
            source
                .apply_entry("fixture", "{}", "fixture", None, |tx| {
                    tx.execute_batch("DROP TABLE agent_name_claim; DROP TABLE agent;")
                })
                .unwrap();
            drop(source);
            self.sync_ok(
                EXECUTIVE,
                "agent.import",
                json!({"snapshot_path":source_path,"request_key":"cutover"}),
            );
        }

        pub(crate) fn create(&self, name: &str) -> String {
            self.sync_ok(
                EXECUTIVE,
                "agent.create",
                json!({"name":name,"role":"assistant","tag":"test","request_key":format!("create-{name}")}),
            )["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }

        pub(crate) fn bind(&self, route: RouteKey, principal: Option<Principal>, flow: bool) {
            self.handler.route_admissions().insert(
                route,
                RouteAdmission::from_bind(
                    principal,
                    flow.then(|| flow_stamp(Some("fl_sessions"))).as_ref(),
                ),
            );
        }

        pub(crate) fn providers(&self, orchestrator: ProviderResolution, host: ProviderResolution) {
            self.resolver.set(ORCHESTRATOR_CAPABILITY, orchestrator);
            self.resolver.set(HOST_CAPABILITY, host);
        }

        pub(crate) fn sync(&self, route: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler.handle_request(&body(method, params), route)
        }

        pub(crate) fn sync_ok(&self, route: RouteKey, method: &str, params: Value) -> Value {
            result(self.sync(route, method, params))
        }

        pub(crate) async fn served(
            &self,
            route: RouteKey,
            method: &str,
            params: Value,
        ) -> HandlerOutcome {
            self.handler
                .handle_served_request(&body(method, params), route)
                .await
        }

        pub(crate) async fn register(&self, route: RouteKey, params: Value) -> HandlerOutcome {
            self.served(route, REGISTER_SESSION, params).await
        }

        fn binding(
            &self,
            harness: &str,
            session_id: &str,
        ) -> Option<entorhinal_core::SessionBinding> {
            self.handler
                .with_store(|store| store.session_binding_for(harness, session_id))
                .unwrap()
        }

        fn bindings(&self) -> usize {
            let refs = [("pi", "r1"), ("pi", "r2"), ("pi", "r3")];
            self.handler
                .with_store(|store| store.session_bindings_for_refs(&refs))
                .unwrap()
                .iter()
                .filter(|binding| binding.is_some())
                .count()
        }

        pub(crate) async fn feed(&self, batch: Value) -> Value {
            result(
                self.served(EXECUTIVE, "projects.session_liveness", batch)
                    .await,
            )
        }

        fn liveness(&self, refs: Value) -> Vec<u8> {
            match self.sync(CALLER, SESSION_LIVENESS, json!({ "refs": refs })) {
                HandlerOutcome::Response(body) => body,
                other => panic!("liveness refused: {other:?}"),
            }
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            drop(self.handler.store.lock().unwrap().take());
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    pub(crate) fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
    }

    pub(crate) fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(body) => {
                serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
            }
            other => panic!("expected success, got {other:?}"),
        }
    }

    pub(crate) fn refusal(outcome: HandlerOutcome) -> (String, Option<Value>) {
        match outcome {
            HandlerOutcome::Error { code, .. } => (code, None),
            HandlerOutcome::ErrorWithDetail { code, detail, .. } => (code, Some(detail)),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    pub(crate) fn code(outcome: HandlerOutcome) -> String {
        refusal(outcome).0
    }

    pub(crate) fn provider(module_id: &str) -> ProviderResolution {
        ProviderResolution::Provider(module_id.into())
    }

    fn registration_with_root(
        agent_id: &str,
        session_ref: &str,
        harness: &str,
        session_id: &str,
        project_root: &str,
    ) -> Value {
        json!({
            "agent_id":agent_id,
            "ref_kind":"pi",
            "ref":session_ref,
            "harness":harness,
            "session_id":session_id,
            "project_root":project_root,
        })
    }

    pub(crate) fn registration(
        agent_id: &str,
        session_ref: &str,
        harness: &str,
        session_id: &str,
    ) -> Value {
        registration_with_root(agent_id, session_ref, harness, session_id, "/project")
    }

    /// A host provider that is not the executive can register through
    /// `handle_served_request`, and its binding is recorded as `host`. This
    /// requires `authorize_write` to exempt the method from its usual
    /// operator-or-executive check, which would otherwise refuse this caller.
    #[tokio::test]
    async fn a_host_provider_other_than_the_executive_registers_as_host() {
        let f = Fixture::new("register-host", true);
        let agent = f.create("Hosted");
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        f.bind(CALLER, Some(reserved("thalamus-harness")), false);
        let reply = result(
            f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(
            reply,
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1","project_root":"/project","registered_by":"host","registered_at_ms":NOW})
        );
        assert_eq!(f.binding("pi", "s1").unwrap().registered_by, "host");
        assert_eq!(
            f.resolver.calls(),
            2,
            "both capabilities are resolved on first registration"
        );
    }

    /// The orchestrator's provider registers as `orchestrator`, and so does a
    /// module that provides both capabilities. Each new route resolves both
    /// capabilities once; later registrations reuse those answers.
    #[tokio::test]
    async fn the_orchestrator_and_a_provider_of_both_register_as_orchestrator() {
        let f = Fixture::new("register-orchestrator", true);
        let agent = f.create("Worker");
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(reply["registered_by"], "orchestrator");
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        f.handler
            .on_route_gone(&RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1))
            .await;
        f.bind(EXECUTIVE, Some(reserved("prefrontal-core")), false);
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
                .await,
        );
        assert_eq!(reply["registered_by"], "orchestrator");
        // Being the sole provider of either capability alone is enough to
        // register: here the executive provides only the host capability and
        // is recorded as `host`.
        f.providers(ProviderResolution::Unprovided, provider("prefrontal-core"));
        f.handler
            .on_route_gone(&RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1))
            .await;
        f.bind(EXECUTIVE, Some(reserved("prefrontal-core")), false);
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r3", "pi", "s3"))
                .await,
        );
        assert_eq!(reply["registered_by"], "host");
        assert_eq!(f.resolver.calls(), 6);
    }

    /// With neither capability resolved to a sole provider, whatever the
    /// reason, nobody registers: not the executive, not the operator.
    #[tokio::test]
    async fn no_sole_provider_refuses_every_caller_orchestrator_unbound() {
        let f = Fixture::new("register-unbound", true);
        let agent = f.create("Unbound");
        let failures = [
            ProviderResolution::Unprovided,
            ProviderResolution::Ambiguous(vec!["prefrontal-core".into(), "rogue".into()]),
            ProviderResolution::InvalidIdentifier,
            ProviderResolution::Transport("daemon gone".into()),
        ];
        for orchestrator in &failures {
            for host in &failures {
                f.providers(orchestrator.clone(), host.clone());
                for principal in [
                    Some(reserved(WRITER_MODULE)),
                    Some(reserved("rogue")),
                    Some(Principal::Direct),
                    None,
                ] {
                    f.bind(CALLER, principal.clone(), false);
                    let (code, detail) = refusal(
                        f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                            .await,
                    );
                    assert_eq!(
                        code, "orchestrator_unbound",
                        "{orchestrator:?}/{host:?}/{principal:?}"
                    );
                    assert_eq!(
                        detail.unwrap(),
                        json!({ORCHESTRATOR_CAPABILITY: orchestrator.outcome(), HOST_CAPABILITY: host.outcome()})
                    );
                }
            }
        }
        assert_eq!(f.bindings(), 0);
    }

    /// An ambiguous or failed capability admits no one, even a module the
    /// daemon listed among its claimants, while the other capability's sole
    /// provider is still admitted.
    #[tokio::test]
    async fn a_failed_capability_admits_no_one_while_the_other_still_admits() {
        let f = Fixture::new("register-partial", true);
        let agent = f.create("Partial");
        f.providers(
            ProviderResolution::Ambiguous(vec!["prefrontal-core".into(), "rogue".into()]),
            provider("thalamus-harness"),
        );
        f.bind(CALLER, Some(reserved("rogue")), false);
        assert_eq!(
            code(
                f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "caller_forbidden"
        );
        f.bind(CALLER, Some(reserved("thalamus-harness")), false);
        let reply = result(
            f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(reply["registered_by"], "host");
    }

    /// Admission happens before anything about the agent or the bindings is
    /// read, in the stated order: flow route, resolution, principal.
    #[tokio::test]
    async fn admission_refuses_flow_then_resolution_then_principal() {
        let f = Fixture::new("register-admission-order", true);
        let agent = f.create("Ordered");
        // A flow route is refused before the daemon is asked anything.
        f.bind(CALLER, Some(reserved(WRITER_MODULE)), true);
        assert_eq!(
            code(
                f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "flow_scope_not_admitted"
        );
        assert_eq!(f.resolver.calls(), 0, "a flow route resolves nothing");
        // Provider resolution is checked before the caller's principal, so a
        // `Direct` caller gets `orchestrator_unbound`, not `caller_forbidden`.
        f.bind(CALLER, Some(Principal::Direct), false);
        assert_eq!(
            code(
                f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "orchestrator_unbound"
        );
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        for principal in [
            Some(Principal::Direct),
            Some(reserved("aft")),
            Some(Principal::Unverified),
            None,
        ] {
            f.bind(CALLER, principal.clone(), false);
            let (code, _) = refusal(
                f.register(CALLER, json!({"not":"even a registration"}))
                    .await,
            );
            assert_eq!(
                code, "caller_forbidden",
                "{principal:?}: admission precedes decoding"
            );
        }
        assert_eq!(f.bindings(), 0);
    }

    /// Once a caller is admitted, registration checks in order: that agent
    /// identity is cut over to this store, then the agent, then the
    /// reference, then the session key. Each refusal leaves the bindings
    /// unchanged.
    #[tokio::test]
    async fn registration_refuses_cutover_agent_ref_and_session_in_order() {
        let pre = Fixture::new("register-precutover", false);
        pre.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
        assert_eq!(
            code(
                pre.register(EXECUTIVE, registration(UNKNOWN_AGENT, "r1", "pi", "s1"))
                    .await
            ),
            "authority_not_cut_over"
        );

        let f = Fixture::new("register-order", true);
        f.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
        let first = f.create("First");
        let second = f.create("Second");
        let retired = f.create("Retired");
        f.sync_ok(
            EXECUTIVE,
            "agent.dispose",
            json!({"agent_id":retired,"request_key":"retire"}),
        );
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(UNKNOWN_AGENT, "r1", "pi", "s1"))
                    .await
            ),
            "unknown_agent"
        );
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration("not-an-id", "r1", "pi", "s1"))
                    .await
            ),
            "unknown_agent"
        );
        let (code_gone, detail) = refusal(
            f.register(EXECUTIVE, registration(&retired, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(code_gone, "gone");
        assert_eq!(detail, Some(json!({"reason":"deleted","at":NOW})));
        assert_eq!(f.bindings(), 0, "refusals write nothing");

        result(
            f.register(EXECUTIVE, registration(&first, "r1", "pi", "s1"))
                .await,
        );
        // The reference already belongs to another agent, whatever session
        // key is offered, and even once that session has ended.
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"s1","canonicalRoot":"/w","state":"gone"}]}))
            .await;
        for session_id in ["s1", "s9"] {
            let (code, detail) = refusal(
                f.register(EXECUTIVE, registration(&second, "r1", "pi", session_id))
                    .await,
            );
            assert_eq!(code, "ref_bound_elsewhere");
            assert_eq!(detail, Some(json!({"agent_id":first})));
        }
        // The session key already binds another reference, even of the same
        // agent.
        for agent in [&first, &second] {
            let (code, detail) = refusal(
                f.register(EXECUTIVE, registration(agent, "r2", "pi", "s1"))
                    .await,
            );
            assert_eq!(code, "session_bound_elsewhere");
            assert_eq!(
                detail,
                Some(json!({"ref_kind":"pi","ref":"r1","agent_id":first}))
            );
        }
        // Agent checks come before reference checks: an unknown agent asking
        // for a bound reference hears about the agent.
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(UNKNOWN_AGENT, "r1", "pi", "s1"))
                    .await
            ),
            "unknown_agent"
        );
        assert_eq!(f.bindings(), 1);
        assert_eq!(f.binding("pi", "s1").unwrap().agent_id, first);
    }

    /// Re-registering a reference for its own agent moves it to the new
    /// session key and records the new caller role and time.
    #[tokio::test]
    async fn re_registering_a_reference_updates_its_session_and_role() {
        let f = Fixture::new("register-again", true);
        let agent = f.create("Again");
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        result(
            f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        f.bind(CALLER, Some(reserved("thalamus-harness")), false);
        let reply = result(
            f.register(CALLER, registration(&agent, "r1", "opencode", "s2"))
                .await,
        );
        assert_eq!(reply["registered_by"], "host");
        assert!(
            f.binding("pi", "s1").is_none(),
            "the old session key is freed"
        );
        let moved = f.binding("opencode", "s2").unwrap();
        assert_eq!(
            (moved.session_ref.as_str(), moved.registered_by.as_str()),
            ("r1", "host")
        );
        // The same registration again is accepted unchanged.
        result(
            f.register(CALLER, registration(&agent, "r1", "opencode", "s2"))
                .await,
        );
        assert_eq!(f.bindings(), 1);
    }

    /// The synchronous seam resolves no providers, so even a route that was
    /// just admitted on the served path is refused there.
    #[tokio::test]
    async fn the_sync_seam_refuses_registration_orchestrator_unbound() {
        let f = Fixture::new("register-sync", true);
        let agent = f.create("Seam");
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        let params = registration(&agent, "r1", "pi", "s1");
        result(f.register(EXECUTIVE, params.clone()).await);
        let calls = f.resolver.calls();
        let (code, detail) = refusal(f.sync(EXECUTIVE, REGISTER_SESSION, params));
        assert_eq!(code, "orchestrator_unbound");
        assert_eq!(detail, None, "nothing was resolved on this seam");
        assert_eq!(f.resolver.calls(), calls, "the seam never asks the daemon");
    }

    /// A cached mismatch gets exactly one fresh answer before refusing, so a
    /// transiently outdated claimant answer does not decide admission alone.
    #[tokio::test]
    async fn a_cached_registration_mismatch_refreshes_once_before_refusal() {
        let f = Fixture::new("register-switch", true);
        let agent = f.create("Switched");
        f.providers(ProviderResolution::Unprovided, provider("thalamus-harness"));
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "caller_forbidden"
        );
        let calls = f.resolver.calls();
        assert_eq!(calls, 2);
        f.providers(ProviderResolution::Unprovided, provider("another-host"));
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
                    .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            f.resolver.calls(),
            calls + 2,
            "each capability is refreshed once"
        );
        let cache = f
            .handler
            .admission_for(EXECUTIVE)
            .providers
            .cached
            .lock()
            .unwrap()
            .clone();
        assert_eq!(cache.host, Some(provider("another-host")));
        assert!(f.binding("pi", "s2").is_none());
    }

    /// Every malformed request is `invalid_request`, never a domain refusal.
    #[tokio::test]
    async fn malformed_session_requests_are_invalid_request() {
        let f = Fixture::new("sessions-invalid", true);
        let agent = f.create("Shape");
        f.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
        let long = |n: usize| "x".repeat(n);
        let mut bad_registrations = vec![
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi"}),
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1"}),
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1","project_root":"/project","extra":1}),
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":7,"project_root":"/project"}),
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1","project_root":7}),
        ];
        for (field, max) in [
            ("ref_kind", 64),
            ("ref", 512),
            ("harness", 32),
            ("session_id", 256),
            ("project_root", PROJECT_ROOT_MAX),
        ] {
            for size in [0, max + 1] {
                let mut params = registration(&agent, "r1", "pi", "s1");
                params[field] = json!(long(size));
                bad_registrations.push(params);
            }
            let mut params = registration(
                &agent,
                &format!("max-{field}"),
                "pi",
                &format!("max-{field}"),
            );
            params[field] = json!(long(max));
            assert!(
                matches!(
                    f.register(EXECUTIVE, params).await,
                    HandlerOutcome::Response(_)
                ),
                "{field} at {max} bytes is accepted"
            );
        }
        for params in bad_registrations {
            assert_eq!(
                code(f.register(EXECUTIVE, params.clone()).await),
                "invalid_request",
                "{params}"
            );
        }
        let mut multibyte_at_limit =
            registration(&agent, "multibyte-limit", "pi", "multibyte-limit");
        multibyte_at_limit["project_root"] = json!("é".repeat(PROJECT_ROOT_MAX / 2));
        assert!(matches!(
            f.register(EXECUTIVE, multibyte_at_limit).await,
            HandlerOutcome::Response(_)
        ));
        let mut multibyte_over_limit =
            registration(&agent, "multibyte-over", "pi", "multibyte-over");
        multibyte_over_limit["project_root"] = json!("é".repeat(PROJECT_ROOT_MAX / 2 + 1));
        assert_eq!(
            code(f.register(EXECUTIVE, multibyte_over_limit).await),
            "invalid_request"
        );
        for params in [
            json!({"harness":"pi"}),
            json!({"harness":"pi","session_id":"s1","agent_id":agent}),
            json!({"harness":"","session_id":"s1"}),
            json!({"harness":long(33),"session_id":"s1"}),
            json!({"harness":"pi","session_id":long(257)}),
        ] {
            assert_eq!(
                code(f.sync(CALLER, FOR_SESSION, params.clone())),
                "invalid_request",
                "{params}"
            );
        }
        for params in [
            json!({}),
            json!({"ref_kind":"pi"}),
            json!({"ref":"r1"}),
            json!({"ref_kind":"pi","ref":"r1","extra":true}),
            json!({"ref_kind":"","ref":"r1"}),
            json!({"ref_kind":long(65),"ref":"r1"}),
            json!({"ref_kind":"pi","ref":""}),
            json!({"ref_kind":"pi","ref":long(513)}),
        ] {
            assert_eq!(
                code(f.sync(CALLER, SESSION_FOR_REF, params.clone())),
                "invalid_request",
                "{params}"
            );
        }
        assert_eq!(
            code(f.sync(
                CALLER,
                SESSION_FOR_REF,
                json!({"ref_kind":long(64),"ref":long(512)})
            )),
            "not_found"
        );
        let one = json!({"ref_kind":"pi","ref":"r1"});
        for params in [
            json!({"refs":[]}),
            json!({"refs":vec![one.clone(); 257]}),
            json!({"refs":[{"ref_kind":"pi"}]}),
            json!({"refs":[{"ref_kind":"pi","ref":"r1","harness":"pi"}]}),
            json!({"refs":[{"ref_kind":"","ref":"r1"}]}),
            json!({"refs":[{"ref_kind":"pi","ref":long(513)}]}),
            json!({"refs":[one], "extra":true}),
        ] {
            assert_eq!(
                code(f.sync(CALLER, SESSION_LIVENESS, params.clone())),
                "invalid_request",
                "{params}"
            );
        }
        assert!(matches!(
            f.sync(
                CALLER,
                SESSION_LIVENESS,
                json!({"refs":vec![json!({"ref_kind":"pi","ref":"r1"}); 256]})
            ),
            HandlerOutcome::Response(_)
        ));
    }

    /// `agent.for_session` answers the reference a session key binds, for any
    /// harness string, and `not_found` otherwise; a refused second reference
    /// does not disturb the first.
    #[tokio::test]
    async fn for_session_returns_the_binding_or_not_found() {
        let f = Fixture::new("for-session", true);
        let agent = f.create("Lookup");
        f.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
        f.bind(CALLER, Some(reserved("anyone")), true);
        assert_eq!(
            code(f.sync(
                CALLER,
                FOR_SESSION,
                json!({"harness":"claude-code","session_id":"c1"})
            )),
            "not_found"
        );
        result(
            f.register(EXECUTIVE, registration(&agent, "r1", "claude-code", "c1"))
                .await,
        );
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(&agent, "r2", "claude-code", "c1"))
                    .await
            ),
            "session_bound_elsewhere"
        );
        let expected =
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","incarnation":"0123456789abcdef"});
        for principal in [None, Some(Principal::Direct), Some(reserved("anyone"))] {
            for flow in [false, true] {
                f.bind(CALLER, principal.clone(), flow);
                assert_eq!(
                    f.sync_ok(
                        CALLER,
                        FOR_SESSION,
                        json!({"harness":"claude-code","session_id":"c1"})
                    ),
                    expected
                );
                assert_eq!(
                    result(
                        f.served(
                            CALLER,
                            FOR_SESSION,
                            json!({"harness":"claude-code","session_id":"c1"})
                        )
                        .await
                    ),
                    expected
                );
            }
        }
        // The harness is part of the key: the same session id elsewhere is
        // another session.
        assert_eq!(
            code(f.sync(
                CALLER,
                FOR_SESSION,
                json!({"harness":"opencode","session_id":"c1"})
            )),
            "not_found"
        );
    }

    /// A reference selects one registration even when other sessions share its
    /// reference kind; re-registering it replaces its host session and root.
    #[tokio::test]
    async fn session_for_ref_returns_the_matching_binding_and_tracks_reregistration() {
        let f = Fixture::new("session-for-ref", true);
        let agent = f.create("ReverseLookup");
        f.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
        assert_eq!(
            code(f.sync(
                CALLER,
                SESSION_FOR_REF,
                json!({"ref_kind":"pi","ref":"missing"})
            )),
            "not_found"
        );
        result(
            f.register(
                EXECUTIVE,
                registration_with_root(&agent, "r1", "pi", "s1", "/first"),
            )
            .await,
        );
        let root = "  /workspace/../project  ";
        result(
            f.register(
                EXECUTIVE,
                registration_with_root(&agent, "r2", "pi", "s2", root),
            )
            .await,
        );
        let params = json!({"ref_kind":"pi","ref":"r2"});
        let expected = json!({
            "agent_id":agent,
            "ref_kind":"pi",
            "ref":"r2",
            "harness":"pi",
            "session_id":"s2",
            "project_root":root,
            "incarnation":"0123456789abcdef",
        });
        for principal in [
            None,
            Some(Principal::Direct),
            Some(reserved("another-module")),
        ] {
            for flow in [false, true] {
                f.bind(CALLER, principal.clone(), flow);
                assert_eq!(f.sync_ok(CALLER, SESSION_FOR_REF, params.clone()), expected);
                assert_eq!(
                    result(f.served(CALLER, SESSION_FOR_REF, params.clone()).await),
                    expected
                );
            }
        }

        result(
            f.register(
                EXECUTIVE,
                registration_with_root(&agent, "r2", "pi", "s3", " /new-root/ "),
            )
            .await,
        );
        assert_eq!(
            f.sync_ok(CALLER, SESSION_FOR_REF, params),
            json!({
                "agent_id":agent,
                "ref_kind":"pi",
                "ref":"r2",
                "harness":"pi",
                "session_id":"s3",
                "project_root":" /new-root/ ",
                "incarnation":"0123456789abcdef",
            })
        );
    }

    /// Every mapping, in request order, with the exact reply bytes: `cause`
    /// appears only on an `unknown` answer whose entry carried one.
    #[tokio::test]
    async fn session_liveness_maps_every_state_in_request_order() {
        let f = Fixture::new("liveness-map", true);
        let agent = f.create("Live");
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        for (session_ref, harness, session_id) in [
            ("active", "pi", "s-active"),
            ("idle", "pi", "s-idle"),
            ("gone", "pi", "s-gone"),
            ("lapsed", "pi", "s-lapsed"),
            ("odd", "pi", "s-odd"),
            ("missing", "pi", "s-missing"),
            ("caused", "pi", "s-caused"),
            ("twin", "opencode", "s-active"),
        ] {
            result(
                f.register(
                    EXECUTIVE,
                    registration(&agent, session_ref, harness, session_id),
                )
                .await,
            );
        }
        let refs: Vec<Value> = [
            "unbound", "twin", "active", "idle", "gone", "lapsed", "odd", "missing", "caused",
        ]
        .iter()
        .map(|session_ref| json!({"ref_kind":"pi","ref":session_ref}))
        .collect();
        let refs = json!(refs);
        // Before the first snapshot the mirror trusts no reported state:
        // every ref answers `unknown`, but an entry's `cause` (here
        // `host_not_seen`) is still passed through on its answer.
        f.feed(json!({"seq":1,"sessions":[
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"active"},
            {"harness":"pi","sessionId":"s-caused","canonicalRoot":"/w","state":"lapsed","cause":"host_not_seen"}]})).await;
        assert_eq!(
            String::from_utf8(f.liveness(refs.clone())).unwrap(),
            r#"{"result":{"incarnation":"0123456789abcdef","refs":[{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"state":"unknown"},{"cause":"host_not_seen","state":"unknown"}]}}"#
        );
        f.feed(json!({"seq":2,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"active","cause":"ignored"},
            {"harness":"pi","sessionId":"s-idle","canonicalRoot":"/w","state":"idle"},
            {"harness":"pi","sessionId":"s-gone","canonicalRoot":"/w","state":"active"},
            {"harness":"pi","sessionId":"s-lapsed","canonicalRoot":"/w","state":"lapsed"},
            {"harness":"pi","sessionId":"s-odd","canonicalRoot":"/w","state":"hibernating","cause":"something new"},
            {"harness":"pi","sessionId":"s-caused","canonicalRoot":"/w","state":"lapsed","cause":"lease_expired"}]})).await;
        f.feed(json!({"seq":3,"sessions":[
            {"harness":"pi","sessionId":"s-gone","canonicalRoot":"/w","state":"gone","cause":"ignored"}]})).await;
        assert_eq!(
            String::from_utf8(f.liveness(refs.clone())).unwrap(),
            r#"{"result":{"incarnation":"0123456789abcdef","refs":[{"state":"unknown"},{"state":"unknown"},{"state":"active"},{"state":"active"},{"state":"gone"},{"state":"unknown"},{"cause":"something new","state":"unknown"},{"state":"unknown"},{"cause":"lease_expired","state":"unknown"}]}}"#
        );
        // The same session id under another harness is another session.
        f.feed(json!({"seq":4,"sessions":[
            {"harness":"opencode","sessionId":"s-active","canonicalRoot":"/w","state":"idle"},
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"gone"}]}))
            .await;
        let answers: Value = serde_json::from_slice(&f.liveness(json!([
            {"ref_kind":"pi","ref":"twin"},{"ref_kind":"pi","ref":"active"}])))
        .unwrap();
        assert_eq!(
            answers["result"]["refs"],
            json!([{"state":"active"},{"state":"gone"}])
        );
        // A later report revives a session whose `gone` was a lapse in
        // reporting, and a snapshot forgets every `gone`.
        f.feed(json!({"seq":5,"sessions":[
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"active"}]}))
            .await;
        f.feed(json!({"seq":6,"snapshot":true,"sessions":[]})).await;
        let answers: Value = serde_json::from_slice(&f.liveness(json!([
            {"ref_kind":"pi","ref":"active"},{"ref_kind":"pi","ref":"gone"}])))
        .unwrap();
        assert_eq!(
            answers["result"]["refs"],
            json!([{"state":"unknown"},{"state":"unknown"}])
        );
        // Reads are open to every caller, on any route.
        for principal in [None, Some(Principal::Direct), Some(reserved("anyone"))] {
            f.bind(CALLER, principal, true);
            assert!(matches!(
                f.served(
                    CALLER,
                    SESSION_LIVENESS,
                    json!({"refs":[{"ref_kind":"pi","ref":"x"}]})
                )
                .await,
                HandlerOutcome::Response(_)
            ));
        }
    }

    #[tokio::test]
    async fn registration_reuses_both_claimants_on_one_route() {
        let f = Fixture::new("register-cache", true);
        let agent = f.create("Cached");
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        for (session_ref, session_id) in [("r1", "s1"), ("r2", "s2"), ("r3", "s3")] {
            let reply = result(
                f.register(
                    EXECUTIVE,
                    registration(&agent, session_ref, "pi", session_id),
                )
                .await,
            );
            assert_eq!(reply["registered_by"], "orchestrator");
        }
        assert_eq!(
            f.resolver.calls(),
            2,
            "one lookup for each capability, not each registration"
        );
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        assert_eq!(
            f.resolver.calls(),
            2,
            "the feed reuses this route's cached host"
        );
    }

    #[tokio::test]
    async fn registration_after_feed_resolves_only_the_missing_orchestrator() {
        let f = Fixture::new("register-cache-after-feed", true);
        let agent = f.create("Cached");
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 1);
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(reply["registered_by"], "orchestrator");
        assert_eq!(
            f.resolver.calls(),
            2,
            "registration reuses the host already resolved by the feed"
        );
    }

    #[tokio::test]
    async fn registration_transport_failure_caches_neither_capability() {
        let f = Fixture::new("register-cache-transport", true);
        let agent = f.create("Cached");
        f.providers(
            ProviderResolution::Transport("catalog unavailable".into()),
            provider("prefrontal-core"),
        );
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(
            reply["registered_by"], "host",
            "a failed capability still admits no one while the other can admit"
        );
        let cache = f
            .handler
            .admission_for(EXECUTIVE)
            .providers
            .cached
            .lock()
            .unwrap()
            .clone();
        assert!(cache.host.is_none());
        assert!(cache.orchestrator.is_none());
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
                .await,
        );
        assert_eq!(reply["registered_by"], "orchestrator");
        assert_eq!(f.resolver.calls(), 4);
    }

    #[tokio::test]
    async fn registration_cached_unbound_refreshes_and_admits_rebound_host() {
        let f = Fixture::new("register-cache-unbound", true);
        let agent = f.create("Rebound");
        f.bind(CALLER, Some(reserved("thalamus-harness")), false);
        assert_eq!(
            code(
                f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                    .await
            ),
            "orchestrator_unbound"
        );
        assert_eq!(f.resolver.calls(), 2);
        f.providers(provider("prefrontal-core"), provider("thalamus-harness"));
        let reply = result(
            f.register(CALLER, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        assert_eq!(reply["registered_by"], "host");
        assert_eq!(
            f.resolver.calls(),
            4,
            "one fresh lookup per capability before deciding"
        );
        result(
            f.register(CALLER, registration(&agent, "r2", "pi", "s2"))
                .await,
        );
        assert_eq!(f.resolver.calls(), 4);
    }

    #[tokio::test]
    async fn feed_batches_resolve_host_once_per_route_and_invalidate_on_close() {
        let f = Fixture::new("feed-cache", false);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        for seq in 1..=4 {
            f.feed(json!({"seq":seq,"snapshot":seq == 1,"sessions":[]}))
                .await;
        }
        assert_eq!(f.resolver.calls(), 1);
        let old = f.handler.admission_for(EXECUTIVE).providers;
        assert_eq!(
            old.cached.lock().unwrap().host,
            Some(provider("prefrontal-core"))
        );
        f.handler
            .on_route_gone(&RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1))
            .await;
        let cache = old.cached.lock().unwrap().clone();
        assert!(
            cache.invalidated,
            "closing the route retires even retained admissions"
        );
        assert!(cache.host.is_none());
        let next = (EXECUTIVE.0, EXECUTIVE.1 + 1);
        f.bind(next, Some(reserved("prefrontal-core")), false);
        result(
            f.served(
                next,
                "projects.session_liveness",
                json!({"seq":1,"snapshot":true,"sessions":[]}),
            )
            .await,
        );
        assert_eq!(
            f.resolver.calls(),
            2,
            "a new route cannot inherit the closed route's answer"
        );
    }

    #[tokio::test]
    async fn feed_cached_mismatch_refreshes_once_before_refusal() {
        let f = Fixture::new("feed-cache-mismatch", false);
        f.bind(CALLER, Some(reserved("non-host")), false);
        f.resolver.set(HOST_CAPABILITY, provider("first-host"));
        let before = mirror(&f);
        assert_eq!(
            code(
                f.served(
                    CALLER,
                    "projects.session_liveness",
                    json!({"seq":1,"snapshot":true,"sessions":[]})
                )
                .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            f.resolver.calls(),
            1,
            "a fresh refusal does not trigger another lookup"
        );
        f.resolver.set(HOST_CAPABILITY, provider("second-host"));
        assert_eq!(
            code(
                f.served(
                    CALLER,
                    "projects.session_liveness",
                    json!({"seq":1,"snapshot":true,"sessions":[]})
                )
                .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            f.resolver.calls(),
            2,
            "a cached mismatch gets exactly one refresh"
        );
        assert_eq!(
            f.handler
                .admission_for(CALLER)
                .providers
                .cached
                .lock()
                .unwrap()
                .host,
            Some(provider("second-host"))
        );
        assert_eq!(mirror(&f), before);
    }

    #[tokio::test]
    async fn feed_cached_unbound_refreshes_before_deciding() {
        let f = Fixture::new("feed-cache-unbound", false);
        assert_eq!(
            code(
                f.served(
                    EXECUTIVE,
                    "projects.session_liveness",
                    json!({"seq":1,"snapshot":true,"sessions":[]})
                )
                .await
            ),
            "host_unbound"
        );
        assert_eq!(f.resolver.calls(), 1);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        assert_eq!(
            f.resolver.calls(),
            2,
            "unbound cache is refreshed once before admission"
        );
        f.feed(json!({"seq":2,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 2);
    }

    #[tokio::test]
    async fn feed_principal_change_invalidates_the_old_admission() {
        let f = Fixture::new("feed-cache-principal", false);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        let old = f.handler.admission_for(EXECUTIVE).providers;
        f.resolver
            .set(HOST_CAPABILITY, provider("thalamus-harness"));
        let request = subc_client_rs::RouteBindRequest::new(
            RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1),
            subc_protocol::RouteTarget::ToolProvider {
                module_id: "entorhinal".into(),
            },
            subc_protocol::BindIdentity::new(f.dir.clone(), "test", "bind"),
        )
        .with_principal(reserved("thalamus-harness"));
        f.handler.on_bind(&request).await;
        assert!(old.cached.lock().unwrap().invalidated);
        assert!(old.cached.lock().unwrap().host.is_none());
        assert!(mirror(&f).stale);
        f.feed(json!({"seq":2,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 2);
        assert_eq!(
            mirror(&f).sender,
            Some((EXECUTIVE, reserved("thalamus-harness")))
        );
    }

    #[tokio::test]
    async fn feed_transport_failure_is_not_cached() {
        let f = Fixture::new("feed-cache-transport", false);
        f.resolver.set(
            HOST_CAPABILITY,
            ProviderResolution::Transport("catalog unavailable".into()),
        );
        for _ in 0..2 {
            assert_eq!(
                code(
                    f.served(
                        EXECUTIVE,
                        "projects.session_liveness",
                        json!({"seq":1,"snapshot":true,"sessions":[]})
                    )
                    .await
                ),
                "host_unbound"
            );
            assert!(f
                .handler
                .admission_for(EXECUTIVE)
                .providers
                .cached
                .lock()
                .unwrap()
                .host
                .is_none());
        }
        assert_eq!(f.resolver.calls(), 2);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        f.feed(json!({"seq":2,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 3);
    }

    struct GateResolver {
        fake: Arc<FakeResolver>,
        first: std::sync::atomic::AtomicBool,
        entered: tokio::sync::Notify,
        release: tokio::sync::Notify,
    }

    impl GateResolver {
        fn new(fake: Arc<FakeResolver>) -> Arc<Self> {
            Arc::new(Self {
                fake,
                first: std::sync::atomic::AtomicBool::new(true),
                entered: tokio::sync::Notify::new(),
                release: tokio::sync::Notify::new(),
            })
        }
    }

    #[async_trait]
    impl ProviderResolver for GateResolver {
        async fn resolve_provider(&self, capability: &str) -> ProviderResolution {
            let answer = self.fake.resolve_provider(capability).await;
            if self.first.swap(false, std::sync::atomic::Ordering::Relaxed) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            answer
        }
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_feed_first_use_shares_one_resolution() {
        let mut f = Fixture::new("feed-cache-concurrent", false);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        let gate = GateResolver::new(f.resolver.clone());
        f.handler.provider_resolver = gate.clone();
        let both = async {
            tokio::join!(
                f.served(
                    EXECUTIVE,
                    "projects.session_liveness",
                    json!({"seq":1,"snapshot":true,"sessions":[]})
                ),
                f.served(
                    EXECUTIVE,
                    "projects.session_liveness",
                    json!({"seq":2,"sessions":[]})
                )
            )
        };
        tokio::pin!(both);
        tokio::select! { _ = gate.entered.notified() => {}, _ = &mut both => panic!("requests completed before the catalog answered") }
        assert_eq!(f.resolver.calls(), 1);
        gate.release.notify_one();
        let (first, second) = tokio::time::timeout(Duration::from_secs(1), both)
            .await
            .unwrap();
        result(first);
        result(second);
        assert_eq!(
            f.resolver.calls(),
            1,
            "concurrent batches share the first lookup"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn closing_a_route_during_resolution_cannot_repopulate_its_cache() {
        let mut f = Fixture::new("feed-cache-inflight", false);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        let gate = GateResolver::new(f.resolver.clone());
        f.handler.provider_resolver = gate.clone();
        let old = f.handler.admission_for(EXECUTIVE).providers;
        let first = f.served(
            EXECUTIVE,
            "projects.session_liveness",
            json!({"seq":1,"snapshot":true,"sessions":[]}),
        );
        tokio::pin!(first);
        tokio::select! { _ = gate.entered.notified() => {}, _ = &mut first => panic!("request completed before the catalog answered") }
        f.handler
            .on_route_gone(&RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1))
            .await;
        f.bind(EXECUTIVE, Some(reserved("prefrontal-core")), false);
        gate.release.notify_one();
        assert_eq!(code(first.await), "host_unbound");
        assert!(old.cached.lock().unwrap().host.is_none());
        assert!(f
            .handler
            .admission_for(EXECUTIVE)
            .providers
            .cached
            .lock()
            .unwrap()
            .host
            .is_none());
        assert_eq!(mirror(&f).last_seq, None);
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn feed_resolution_timeout_is_not_cached() {
        let mut f = Fixture::new("feed-cache-timeout", false);
        f.resolver.set(HOST_CAPABILITY, provider("prefrontal-core"));
        let gate = GateResolver::new(f.resolver.clone());
        f.handler.provider_resolver = gate;
        assert_eq!(
            code(
                f.served(
                    EXECUTIVE,
                    "projects.session_liveness",
                    json!({"seq":1,"snapshot":true,"sessions":[]})
                )
                .await
            ),
            "host_unbound"
        );
        assert!(f
            .handler
            .admission_for(EXECUTIVE)
            .providers
            .cached
            .lock()
            .unwrap()
            .host
            .is_none());
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
        assert_eq!(f.resolver.calls(), 2);
    }

    #[derive(Debug, PartialEq, Eq)]
    struct MirrorSnapshot {
        last_seq: Option<u64>,
        stale: bool,
        sessions: super::super::MirrorSessions,
        gone: std::collections::BTreeSet<super::super::SessionKey>,
        sender: Option<(RouteKey, Principal)>,
    }

    fn mirror(f: &Fixture) -> MirrorSnapshot {
        let state = f.handler.liveness.lock().unwrap();
        MirrorSnapshot {
            last_seq: state.last_seq,
            stale: state.stale,
            sessions: state.sessions.clone(),
            gone: state.gone.clone(),
            sender: state.sender.clone(),
        }
    }

    fn feed_refs() -> Value {
        json!([
            {"ref_kind":"pi","ref":"r1"},
            {"ref_kind":"pi","ref":"r2"},
            {"ref_kind":"pi","ref":"r3"},
            {"ref_kind":"pi","ref":"unbound"}
        ])
    }

    async fn live_feed_fixture(label: &str) -> Fixture {
        let f = Fixture::new(label, true);
        f.providers(provider("prefrontal-core"), provider("prefrontal-core"));
        let agent = f.create("Live");
        for session_ref in ["r1", "r2", "r3"] {
            result(
                f.register(
                    EXECUTIVE,
                    registration(&agent, session_ref, "pi", session_ref),
                )
                .await,
            );
        }
        std::fs::create_dir(f.dir.join("repo")).unwrap();
        let root = crate::tests::canonical_test_path(f.dir.join("repo"));
        f.sync_ok(
            EXECUTIVE,
            "register",
            json!({"projectId":"P","name":"P","roots":[root]}),
        );
        f.feed(json!({"seq":10,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"r1","canonicalRoot":root,"state":"active","lastActivityMs":100},
            {"harness":"pi","sessionId":"r2","canonicalRoot":root,"state":"gone"},
            {"harness":"pi","sessionId":"r3","canonicalRoot":root,"state":"lapsed","cause":"lease_expired","lastActivityMs":90}
        ]})).await;
        assert_eq!(
            f.sync_ok(CALLER, "enumerate", json!({}))["projects"][0]["lastRouteActivityMs"],
            100
        );
        f
    }

    fn assert_feed_unknown(f: &Fixture) {
        let answers: Value = serde_json::from_slice(&f.liveness(feed_refs())).unwrap();
        assert_eq!(
            answers["result"]["refs"],
            json!([
                {"state":"unknown"}, {"state":"unknown"},
                {"state":"unknown","cause":"lease_expired"}, {"state":"unknown"}
            ])
        );
        assert_eq!(
            f.sync_ok(CALLER, "enumerate", json!({}))["projects"][0]["lastRouteActivityMs"],
            Value::Null
        );
    }

    #[tokio::test]
    async fn liveness_feed_unbound_preserves_mirror_sequence_and_latch() {
        let f = live_feed_fixture("feed-unbound").await;
        for stale in [false, true] {
            f.handler.liveness.lock().unwrap().stale = stale;
            let before = mirror(&f);
            let answers = f.liveness(feed_refs());
            let enumerated = f.sync_ok(CALLER, "enumerate", json!({}));
            for unavailable in [
                ProviderResolution::Unprovided,
                ProviderResolution::Ambiguous(vec![
                    "prefrontal-core".into(),
                    "thalamus-harness".into(),
                ]),
                ProviderResolution::Transport("catalog unavailable".into()),
                ProviderResolution::InvalidIdentifier,
            ] {
                f.resolver.set(HOST_CAPABILITY, unavailable.clone());
                f.bind(EXECUTIVE, Some(reserved("prefrontal-core")), false);
                let calls = f.resolver.calls();
                let batches = f
                    .handler
                    .health
                    .liveness_batches
                    .load(std::sync::atomic::Ordering::Relaxed);
                let refusals = f
                    .handler
                    .health
                    .liveness_refusals
                    .load(std::sync::atomic::Ordering::Relaxed);
                let (code, detail) = refusal(
                    f.served(
                        EXECUTIVE,
                        "projects.session_liveness",
                        json!({"seq":1,"snapshot":true,"sessions":[]}),
                    )
                    .await,
                );
                assert_eq!(code, "host_unbound");
                assert_eq!(
                    detail,
                    Some(json!({HOST_CAPABILITY: unavailable.outcome()}))
                );
                assert_eq!(
                    f.resolver.calls(),
                    calls + 1,
                    "a fresh feed route resolves only the host"
                );
                assert_eq!(mirror(&f), before);
                assert_eq!(f.liveness(feed_refs()), answers);
                assert_eq!(f.sync_ok(CALLER, "enumerate", json!({})), enumerated);
                assert_eq!(
                    f.handler
                        .health
                        .liveness_batches
                        .load(std::sync::atomic::Ordering::Relaxed),
                    batches
                );
                assert_eq!(
                    f.handler
                        .health
                        .liveness_refusals
                        .load(std::sync::atomic::Ordering::Relaxed),
                    refusals + 1
                );
            }
        }
    }

    #[tokio::test]
    async fn liveness_feed_non_host_callers_preserve_every_answer() {
        let f = live_feed_fixture("feed-forbidden").await;
        let before = mirror(&f);
        let answers = f.liveness(feed_refs());
        for principal in [
            Some(Principal::Direct),
            Some(reserved("another-module")),
            None,
        ] {
            f.bind(CALLER, principal, false);
            assert_eq!(
                code(
                    f.served(
                        CALLER,
                        "projects.session_liveness",
                        json!({"seq":11,"snapshot":true,"sessions":[]})
                    )
                    .await
                ),
                "caller_forbidden"
            );
            assert_eq!(mirror(&f), before);
            assert_eq!(f.liveness(feed_refs()), answers);
        }
    }

    #[tokio::test]
    async fn liveness_feed_sync_seam_has_no_cached_provider() {
        let f = live_feed_fixture("feed-sync").await;
        let before = mirror(&f);
        let calls = f.resolver.calls();
        assert_eq!(
            code(f.sync(
                EXECUTIVE,
                "projects.session_liveness",
                json!({"seq":11,"snapshot":true,"sessions":[]})
            )),
            "host_unbound"
        );
        assert_eq!(mirror(&f), before);
        assert_eq!(f.resolver.calls(), calls);
    }

    #[tokio::test]
    async fn liveness_feed_flow_refuses_before_resolution() {
        let f = live_feed_fixture("feed-flow").await;
        f.bind(CALLER, Some(reserved("prefrontal-core")), true);
        let before = mirror(&f);
        let calls = f.resolver.calls();
        assert_eq!(
            code(
                f.served(CALLER, "projects.session_liveness", Value::Null)
                    .await
            ),
            "flow_scope_not_admitted"
        );
        assert_eq!(mirror(&f), before);
        assert_eq!(f.resolver.calls(), calls);
    }

    #[tokio::test]
    async fn liveness_feed_rebinding_to_thalamus_latches_until_snapshot() {
        let f = live_feed_fixture("feed-rebinding").await;
        let previous_cache = f.handler.admission_for(EXECUTIVE).providers;
        f.bind(CALLER, Some(reserved("thalamus-harness")), false);
        assert_eq!(
            code(
                f.served(
                    CALLER,
                    "projects.session_liveness",
                    json!({"seq":11,"sessions":[]})
                )
                .await
            ),
            "caller_forbidden"
        );
        f.resolver
            .set(HOST_CAPABILITY, provider("thalamus-harness"));
        let orchestrator_route = (53, 1);
        f.bind(orchestrator_route, Some(reserved("prefrontal-core")), false);
        let before = mirror(&f);
        assert_eq!(
            code(
                f.served(
                    orchestrator_route,
                    "projects.session_liveness",
                    json!({"seq":11,"snapshot":true,"sessions":[]})
                )
                .await
            ),
            "caller_forbidden"
        );
        assert_eq!(
            mirror(&f),
            before,
            "the orchestrator is not the rebound host"
        );

        let calls = f.resolver.calls();
        let reply = result(
            f.served(
                CALLER,
                "projects.session_liveness",
                json!({"seq":11,"sessions":[]}),
            )
            .await,
        );
        assert_eq!(
            f.resolver.calls(),
            calls + 1,
            "the new module's cached mismatch refreshes once"
        );
        assert_eq!(reply["accepted"], true);
        assert!(
            previous_cache.cached.lock().unwrap().invalidated,
            "a different host principal retires the previous sender's claimant cache"
        );
        assert_eq!(
            reply["snapshotRequested"], true,
            "a new principal invalidates the old complete mirror even at a contiguous sequence"
        );
        assert_eq!(
            mirror(&f).sender,
            Some((CALLER, reserved("thalamus-harness")))
        );
        assert_eq!(
            f.handler
                .health
                .liveness_gaps
                .load(std::sync::atomic::Ordering::Relaxed),
            0
        );
        assert_feed_unknown(&f);

        let reply = result(
            f.served(
                CALLER,
                "projects.session_liveness",
                json!({"seq":12,"sessions":[]}),
            )
            .await,
        );
        assert_eq!(reply["snapshotRequested"], true);
        assert_feed_unknown(&f);
        let reply = result(f.served(CALLER, "projects.session_liveness", json!({"seq":1,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"r1","canonicalRoot":crate::tests::canonical_test_path(f.dir.join("repo")),"state":"idle","lastActivityMs":200}
        ]})).await);
        assert_eq!(reply["snapshotRequested"], false);
        let answers: Value = serde_json::from_slice(&f.liveness(feed_refs())).unwrap();
        assert_eq!(answers["result"]["refs"][0], json!({"state":"active"}));
        assert_eq!(
            f.sync_ok(CALLER, "enumerate", json!({}))["projects"][0]["lastRouteActivityMs"],
            200
        );
    }

    #[tokio::test]
    async fn liveness_feed_recorded_route_closing_latches_until_snapshot() {
        let f = live_feed_fixture("feed-route-close").await;
        f.bind(CALLER, Some(reserved("prefrontal-core")), false);
        let reply = result(
            f.served(
                CALLER,
                "projects.session_liveness",
                json!({"seq":11,"sessions":[]}),
            )
            .await,
        );
        assert_eq!(
            reply["snapshotRequested"], false,
            "a new route for the same principal is not a principal change"
        );
        assert_eq!(
            mirror(&f).sender,
            Some((CALLER, reserved("prefrontal-core")))
        );
        let before = mirror(&f);
        f.handler
            .on_route_gone(&RouteHandle::detached(EXECUTIVE.0, EXECUTIVE.1))
            .await;
        assert_eq!(
            mirror(&f),
            before,
            "closing an earlier sender route does not invalidate the current mirror"
        );
        f.handler
            .on_route_gone(&RouteHandle::detached(CALLER.0, CALLER.1))
            .await;
        let after = mirror(&f);
        assert!(
            after.stale,
            "closing the recorded sender route invalidates the mirror"
        );
        assert_eq!(after.last_seq, before.last_seq);
        assert_eq!(after.sessions, before.sessions);
        assert_feed_unknown(&f);

        f.bind(EXECUTIVE, Some(reserved("prefrontal-core")), false);
        let reply = f.feed(json!({"seq":12,"sessions":[]})).await;
        assert_eq!(reply["snapshotRequested"], true);
        assert_feed_unknown(&f);
        let reply = f.feed(json!({"seq":1,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"r1","canonicalRoot":crate::tests::canonical_test_path(f.dir.join("repo")),"state":"active","lastActivityMs":200}
        ]})).await;
        assert_eq!(reply["snapshotRequested"], false);
        let answers: Value = serde_json::from_slice(&f.liveness(feed_refs())).unwrap();
        assert_eq!(answers["result"]["refs"][0], json!({"state":"active"}));
        assert_eq!(
            f.sync_ok(CALLER, "enumerate", json!({}))["projects"][0]["lastRouteActivityMs"],
            200
        );
    }

    /// Batches carrying `lapsed` or a state this build does not know are
    /// applied, not refused, and enumerate's annotation counts those sessions
    /// exactly as it counts any other: it depends on the set of sessions and
    /// their roots, never on their state or harness.
    #[tokio::test]
    async fn unfamiliar_states_are_accepted_and_leave_enumerate_annotation_unchanged() {
        let annotate = |sessions: Value| async move {
            let handler = super::super::tests::liveness_handler();
            let reply: Value = serde_json::from_slice(
                &super::super::tests::served_liveness(
                    &handler,
                    json!({"seq":1,"snapshot":true,"sessions":sessions}),
                )
                .await
                .unwrap(),
            )
            .unwrap();
            assert_eq!(reply["result"]["accepted"], true);
            let mut enumerated = entorhinal_core::EnumerateReply {
                generation: 1,
                workspaces: vec![],
                projects: vec![entorhinal_core::ProjectView {
                    project_id: "p".into(),
                    name: "p".into(),
                    roots: vec!["/w/proj".into()],
                    implicit: false,
                    ref_kind: "local".into(),
                    device_fingerprint: None,
                    workspace_id: None,
                    last_route_activity_ms: None,
                    root_records: None,
                }],
            };
            handler.annotate_liveness(&mut enumerated);
            (
                reply["result"]["tracked"].clone(),
                enumerated.projects[0].last_route_activity_ms,
            )
        };
        let nested = std::path::Path::new("/w/proj").join("sub");
        let nested = nested.to_str().unwrap();
        let familiar = annotate(json!([
            {"sessionId":"s1","canonicalRoot":"/w/proj","state":"active","lastActivityMs":500},
            {"sessionId":"s2","canonicalRoot":nested,"state":"idle","lastActivityMs":900}]))
        .await;
        assert_eq!(familiar, (json!(2), Some(900)));
        for (state, harness) in [
            ("lapsed", "pi"),
            ("hibernating", "opencode"),
            ("lapsed", ""),
        ] {
            assert_eq!(
                annotate(json!([
                    {"sessionId":"s1","canonicalRoot":"/w/proj","state":"active","lastActivityMs":500},
                    {"harness":harness,"sessionId":"s2","canonicalRoot":nested,"state":state,"lastActivityMs":900,"cause":"lease_expired"}])).await,
                familiar,
                "{state} under '{harness}'"
            );
        }
    }
}

#[cfg(test)]
#[path = "session_network_tests.rs"]
mod network_tests;

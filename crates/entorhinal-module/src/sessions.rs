//! Agent sessions: `agent.register_session`, `agent.for_session` and
//! `agent.session_liveness`.
//!
//! A session is registered by the module that starts it for an agent. Two
//! reserved capabilities may do that: `agent-orchestrator/v1`, held by the
//! module that owns the agent's work, and `agent-host/v1`, held by the module
//! that hosts sessions and feeds their liveness. The subc daemon binds each
//! reserved capability to exactly one module id, so the caller is admitted
//! when its daemon-stamped principal is `reserved:<that module id>`.
//!
//! The provider of each capability is asked of the daemon for every request
//! and never remembered on the route: a capability can be rebound to another
//! module between two requests. No module name is compared anywhere here.

use std::{path::PathBuf, sync::Arc, time::Duration};

use async_trait::async_trait;
use entorhinal_core::SessionRegistration;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_client_rs::{CallError, ConsumerOptions, SubcConsumer};
use subc_protocol::Principal;

use super::{encode_result, HandlerError, ProjectsHandler, SessionKey};

pub(super) const REGISTER_SESSION: &str = "agent.register_session";
pub(super) const FOR_SESSION: &str = "agent.for_session";
pub(super) const SESSION_LIVENESS: &str = "agent.session_liveness";
/// The session reads, open to every caller like the other reads.
pub(super) const READ_METHODS: &[&str] = &[FOR_SESSION, SESSION_LIVENESS];

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
const LIVENESS_REFS_MAX: usize = 256;

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
/// opened on the first registration, never at startup, and dropped after a
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

/// Both session capabilities as resolved for one request.
#[derive(Clone, Debug)]
pub(super) struct ResolvedProviders {
    pub(super) orchestrator: ProviderResolution,
    pub(super) host: ProviderResolution,
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RegisterSessionRequest {
    agent_id: String,
    ref_kind: String,
    #[serde(rename = "ref")]
    session_ref: String,
    harness: String,
    session_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ForSessionRequest {
    harness: String,
    session_id: String,
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
    state: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    cause: Option<String>,
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
    /// Ask the daemon for both session capabilities at once. A lookup that
    /// does not finish within its deadline counts as a transport failure.
    pub(super) async fn resolve_session_providers(&self) -> ResolvedProviders {
        let resolve = |capability: &'static str| async move {
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
        };
        let (orchestrator, host) =
            tokio::join!(resolve(ORCHESTRATOR_CAPABILITY), resolve(HOST_CAPABILITY));
        ResolvedProviders { orchestrator, host }
    }

    /// Called only after admission decided `role`. Decoding comes after
    /// admission, as for every other write, so a caller that may not register
    /// learns nothing about the request's shape.
    pub(super) fn register_session(
        &self,
        params: Value,
        role: SessionRole,
    ) -> Result<Vec<u8>, HandlerError> {
        let request: RegisterSessionRequest = decode(params)?;
        ref_bounds(&request.ref_kind, &request.session_ref)?;
        session_key_bounds(&request.harness, &request.session_id)?;
        let binding = self.with_agent_store(|store| {
            store.register_session(&SessionRegistration {
                agent_id: &request.agent_id,
                ref_kind: &request.ref_kind,
                session_ref: &request.session_ref,
                harness: &request.harness,
                session_id: &request.session_id,
                registered_by: role.as_str(),
                now_ms: (self.clock)(),
            })
        })?;
        encode_result(binding)
    }

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
        let bindings = self.with_store(|store| store.session_bindings_for_refs(&refs))?;
        let state = self
            .liveness
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let answers: Vec<LivenessAnswer> = bindings
            .iter()
            .map(|binding| match binding {
                Some(binding) => {
                    state.answer(&SessionKey::new(&binding.harness, &binding.session_id))
                }
                None => LivenessAnswer::unknown(None),
            })
            .collect();
        drop(state);
        self.encode_read_result(json!({ "refs": answers }))
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
mod tests {
    use std::path::PathBuf;

    use cortexkit_store_types::StorageDescriptor;
    use entorhinal_core::RegistryStore;
    use subc_client_rs::HandlerOutcome;

    use super::*;
    use crate::{
        tests::{flow_stamp, reserved, scratch_descriptor},
        RouteAdmission, RouteKey, WRITER_MODULE,
    };

    const NOW: i64 = 700;
    const EXECUTIVE: RouteKey = (50, 1);
    const CALLER: RouteKey = (51, 1);
    const UNKNOWN_AGENT: &str = "agent_0000000000000000";

    struct Fixture {
        dir: PathBuf,
        handler: ProjectsHandler,
        resolver: Arc<FakeResolver>,
    }

    impl Fixture {
        /// A store, with the agent-identity cutover recorded when `cut_over`,
        /// and a resolver naming no provider for either capability.
        fn new(label: &str, cut_over: bool) -> Self {
            let (dir, descriptor) = scratch_descriptor(label);
            let resolver = FakeResolver::new();
            let mut handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || NOW);
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

        fn create(&self, name: &str) -> String {
            self.sync_ok(
                EXECUTIVE,
                "agent.create",
                json!({"name":name,"role":"assistant","tag":"test","request_key":format!("create-{name}")}),
            )["agent"]["agent_id"]
                .as_str()
                .unwrap()
                .into()
        }

        fn bind(&self, route: RouteKey, principal: Option<Principal>, flow: bool) {
            self.handler.route_admissions().insert(
                route,
                RouteAdmission::from_bind(
                    principal,
                    flow.then(|| flow_stamp(Some("fl_sessions"))).as_ref(),
                ),
            );
        }

        fn providers(&self, orchestrator: ProviderResolution, host: ProviderResolution) {
            self.resolver.set(ORCHESTRATOR_CAPABILITY, orchestrator);
            self.resolver.set(HOST_CAPABILITY, host);
        }

        fn sync(&self, route: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler.handle_request(&body(method, params), route)
        }

        fn sync_ok(&self, route: RouteKey, method: &str, params: Value) -> Value {
            result(self.sync(route, method, params))
        }

        async fn served(&self, route: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(&body(method, params), route)
                .await
        }

        async fn register(&self, route: RouteKey, params: Value) -> HandlerOutcome {
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

        fn feed(&self, batch: Value) {
            self.handler.session_liveness(batch).unwrap();
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

    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
    }

    fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(body) => {
                serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
            }
            other => panic!("expected success, got {other:?}"),
        }
    }

    fn refusal(outcome: HandlerOutcome) -> (String, Option<Value>) {
        match outcome {
            HandlerOutcome::Error { code, .. } => (code, None),
            HandlerOutcome::ErrorWithDetail { code, detail, .. } => (code, Some(detail)),
            other => panic!("expected a refusal, got {other:?}"),
        }
    }

    fn code(outcome: HandlerOutcome) -> String {
        refusal(outcome).0
    }

    fn provider(module_id: &str) -> ProviderResolution {
        ProviderResolution::Provider(module_id.into())
    }

    fn registration(agent_id: &str, session_ref: &str, harness: &str, session_id: &str) -> Value {
        json!({"agent_id":agent_id,"ref_kind":"pi","ref":session_ref,"harness":harness,"session_id":session_id})
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
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1","registered_by":"host","registered_at_ms":NOW})
        );
        assert_eq!(f.binding("pi", "s1").unwrap().registered_by, "host");
        assert_eq!(
            f.resolver.calls(),
            2,
            "both capabilities are resolved per request"
        );
    }

    /// The orchestrator's provider registers as `orchestrator`, and so does a
    /// module that provides both capabilities. Provider resolution is never
    /// cached: each registration asks the resolver for both capabilities
    /// again, which the call count checks.
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
        let reply = result(
            f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
                .await,
        );
        assert_eq!(reply["registered_by"], "orchestrator");
        // Being the sole provider of either capability alone is enough to
        // register: here the executive provides only the host capability and
        // is recorded as `host`.
        f.providers(ProviderResolution::Unprovided, provider("prefrontal-core"));
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
        f.feed(json!({"seq":1,"snapshot":true,"sessions":[
            {"harness":"pi","sessionId":"s1","canonicalRoot":"/w","state":"gone"}]}));
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

    /// Providers are resolved per request and never remembered on the route,
    /// so after a rebinding the previous provider is refused at once.
    #[tokio::test]
    async fn after_a_provider_switch_the_old_principal_is_caller_forbidden() {
        let f = Fixture::new("register-switch", true);
        let agent = f.create("Switched");
        f.providers(ProviderResolution::Unprovided, provider("prefrontal-core"));
        result(
            f.register(EXECUTIVE, registration(&agent, "r1", "pi", "s1"))
                .await,
        );
        f.providers(ProviderResolution::Unprovided, provider("thalamus-harness"));
        assert_eq!(
            code(
                f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
                    .await
            ),
            "caller_forbidden"
        );
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
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":"s1","extra":1}),
            json!({"agent_id":agent,"ref_kind":"pi","ref":"r1","harness":"pi","session_id":7}),
        ];
        for (field, max) in [
            ("ref_kind", 64),
            ("ref", 512),
            ("harness", 32),
            ("session_id", 256),
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

    /// Every mapping, in request order, with the exact reply bytes: `cause`
    /// appears only on an `unknown` answer whose entry carried one.
    #[tokio::test]
    async fn session_liveness_maps_every_state_in_request_order() {
        let f = Fixture::new("liveness-map", true);
        let agent = f.create("Live");
        f.providers(provider("prefrontal-core"), ProviderResolution::Unprovided);
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
            {"harness":"pi","sessionId":"s-caused","canonicalRoot":"/w","state":"lapsed","cause":"host_not_seen"}]}));
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
            {"harness":"pi","sessionId":"s-caused","canonicalRoot":"/w","state":"lapsed","cause":"lease_expired"}]}));
        f.feed(json!({"seq":3,"sessions":[
            {"harness":"pi","sessionId":"s-gone","canonicalRoot":"/w","state":"gone","cause":"ignored"}]}));
        assert_eq!(
            String::from_utf8(f.liveness(refs.clone())).unwrap(),
            r#"{"result":{"incarnation":"0123456789abcdef","refs":[{"state":"unknown"},{"state":"unknown"},{"state":"active"},{"state":"active"},{"state":"gone"},{"state":"unknown"},{"cause":"something new","state":"unknown"},{"state":"unknown"},{"cause":"lease_expired","state":"unknown"}]}}"#
        );
        // The same session id under another harness is another session.
        f.feed(json!({"seq":4,"sessions":[
            {"harness":"opencode","sessionId":"s-active","canonicalRoot":"/w","state":"idle"},
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"gone"}]}));
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
            {"harness":"pi","sessionId":"s-active","canonicalRoot":"/w","state":"active"}]}));
        f.feed(json!({"seq":6,"snapshot":true,"sessions":[]}));
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

    /// Batches carrying `lapsed` or a state this build does not know are
    /// applied, not refused, and enumerate's annotation counts those sessions
    /// exactly as it counts any other: it depends on the set of sessions and
    /// their roots, never on their state or harness.
    #[test]
    fn unfamiliar_states_are_accepted_and_leave_enumerate_annotation_unchanged() {
        let annotate = |sessions: Value| {
            let handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || NOW);
            let reply: Value = serde_json::from_slice(
                &handler
                    .session_liveness(json!({"seq":1,"snapshot":true,"sessions":sessions}))
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
            {"sessionId":"s2","canonicalRoot":nested,"state":"idle","lastActivityMs":900}]));
        assert_eq!(familiar, (json!(2), Some(900)));
        for (state, harness) in [
            ("lapsed", "pi"),
            ("hibernating", "opencode"),
            ("lapsed", ""),
        ] {
            assert_eq!(
                annotate(json!([
                    {"sessionId":"s1","canonicalRoot":"/w/proj","state":"active","lastActivityMs":500},
                    {"harness":harness,"sessionId":"s2","canonicalRoot":nested,"state":state,"lastActivityMs":900,"cause":"lease_expired"}])),
                familiar,
                "{state} under '{harness}'"
            );
        }
    }
}

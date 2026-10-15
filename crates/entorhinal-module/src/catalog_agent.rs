//! Package verification and consumer checks build a persistent plan after
//! catalog_prepare returns. Consent and shared pin writes run only inside the
//! subsequent inbound catalog_install call.
use crate::catalog_blob::CatalogError;
use crate::catalog_consumer::{self, ConsumerDiscovery};
use crate::catalog_trust::CatalogTrust;
use crate::{encode_result, HandlerError, ProjectsHandler, RouteAdmission};
use entorhinal_catalog::{names, record::VerifiedPackage};
use entorhinal_core::catalog_agent::{CatalogSpec, CatalogTarget, PreparedPackage, PreparedPlan};
use entorhinal_core::catalog_resolution::CatalogSubject;
use entorhinal_core::RegistryStore;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    sync::{Arc, Mutex},
};
use tokio::time::Instant;

pub(crate) type TrustFactory =
    dyn Fn(RegistryStore, &str) -> Result<CatalogTrust, CatalogError> + Send + Sync;
pub const AGENT_GENERATION: &str = "catalog.agent_generation";
pub const ACK_ACTIVATION: &str = "catalog.ack_activation";
pub trait LocalCapabilities: Send + Sync {
    fn tokens(&self, catalog: &subc_client_rs::CatalogList) -> Vec<String>;
}
/// Convert only ready modules' declared cap-* capabilities through the shared
/// catalog adapter. Malformed declarations confer nothing and warn once per name.
#[derive(Default)]
pub struct DaemonCapabilities {
    bad_names: Mutex<BTreeSet<String>>,
}
impl LocalCapabilities for DaemonCapabilities {
    fn tokens(&self, catalog: &subc_client_rs::CatalogList) -> Vec<String> {
        let mut tokens = BTreeSet::new();
        for module in catalog.modules.iter().filter(|module| module.ready) {
            for name in module
                .capabilities
                .iter()
                .flat_map(|caps| &caps.provides)
                .filter(|name| name.starts_with("cap-"))
            {
                match entorhinal_catalog::resolver::manifest_to_capability(name) {
                    Ok(token) => {
                        tokens.insert(token);
                    }
                    Err(_) => {
                        if self.bad_names.lock().unwrap().insert(name.clone()) {
                            tracing::warn!(capability = %name, "ignoring malformed catalog capability declaration");
                        }
                    }
                }
            }
        }
        tokens.into_iter().collect()
    }
}
pub struct CatalogAgent {
    pub consumers: Arc<dyn ConsumerDiscovery>,
    pub sources: Arc<TrustFactory>,
    pub capabilities: Arc<dyn LocalCapabilities>,
    running: Arc<Mutex<BTreeSet<String>>>,
    #[cfg(test)]
    finished: Arc<tokio::sync::Notify>,
}
impl Default for CatalogAgent {
    fn default() -> Self {
        Self {
            consumers: Arc::new(catalog_consumer::SubcConsumers(Arc::new(
                crate::log_client::SubcConnector::from_args(std::env::args_os().skip(1)),
            ))),
            sources: Arc::new(CatalogTrust::for_scope),
            capabilities: Arc::new(DaemonCapabilities::default()),
            running: Arc::default(),
            #[cfg(test)]
            finished: Arc::default(),
        }
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    operation: String,
    target: Value,
    package: String,
    range: String,
}
fn failure(error: &HandlerError) -> Value {
    json!({"code":error.code,"message":error.message,"detail":error.detail})
}
impl From<crate::catalog_network::NetworkError> for HandlerError {
    fn from(e: crate::catalog_network::NetworkError) -> Self {
        Self {
            code: e.code().into(),
            message: format!("{e:?}"),
            detail: e.detail().cloned(),
        }
    }
}
impl From<entorhinal_core::catalog_resolution::CatalogResolveError> for HandlerError {
    fn from(e: entorhinal_core::catalog_resolution::CatalogResolveError) -> Self {
        match e {
            entorhinal_core::catalog_resolution::CatalogResolveError::Store(e) => e.into(),
            entorhinal_core::catalog_resolution::CatalogResolveError::Refusal(v) => Self {
                code: v["code"].as_str().unwrap_or("resolution_failed").into(),
                message: "catalog resolution refused".into(),
                detail: Some(v),
            },
        }
    }
}
impl CatalogAgent {
    fn start(
        &self,
        store: RegistryStore,
        plan: entorhinal_core::catalog_agent::CatalogPlan,
        clock: fn() -> i64,
    ) {
        if !self.running.lock().unwrap().insert(plan.plan_id.clone()) {
            return;
        }
        let consumers = self.consumers.clone();
        let sources = self.sources.clone();
        let capabilities = self.capabilities.clone();
        let running = self.running.clone();
        #[cfg(test)]
        let finished = self.finished.clone();
        tokio::spawn(async move {
            let result = prepare(
                &store,
                &plan,
                consumers.as_ref(),
                sources.as_ref(),
                capabilities.as_ref(),
            )
            .await;
            let result = match result {
                Ok(detail) => store
                    .catalog_save_prepared(&plan.plan_id, &plan.agent_id, &detail)
                    .and_then(|()| {
                        store.catalog_plan_ready(
                            &plan.plan_id,
                            &plan.agent_id,
                            detail.packages.into_iter().map(|p| p.pin).collect(),
                            clock(),
                        )
                    })
                    .map(|_| ())
                    .map_err(HandlerError::from),
                Err(e) => Err(e),
            };
            if let Err(error) = result {
                let _ = store.catalog_plan_failed(&plan.plan_id, &plan.agent_id, failure(&error));
            }
            running.lock().unwrap().remove(&plan.plan_id);
            #[cfg(test)]
            finished.notify_one();
        });
    }
}

#[cfg(test)]
#[path = "catalog_agent_tests.rs"]
mod tests;
fn generation_reply(generation: &entorhinal_core::catalog_agent::CatalogGeneration) -> Value {
    let mut reply = serde_json::to_value(generation).unwrap();
    let statuses = &generation.pinned_status;
    if !statuses.is_empty() {
        reply["pinned_status"] = json!(statuses.iter().map(|s| json!({"name":s["name"],"version":s["version"],"state":s["state"],"reason":s["reason"]})).collect::<Vec<_>>());
    }
    reply
}
async fn prepare(
    store: &RegistryStore,
    plan: &entorhinal_core::catalog_agent::CatalogPlan,
    daemon: &dyn ConsumerDiscovery,
    factory: &TrustFactory,
    capabilities: &dyn LocalCapabilities,
) -> Result<PreparedPlan, HandlerError> {
    let pins = store.catalog_pins(&plan.agent_id)?;
    let mut queue: VecDeque<_> = pins
        .iter()
        .map(|p| p.name.clone())
        .chain([plan.spec.package.clone()])
        .collect();
    let mut scopes = BTreeMap::<String, CatalogTrust>::new();
    // Refresh status for the requested package and every currently pinned
    // publisher scope before selecting any candidate version.
    for name in queue.iter() {
        let scope = format!(
            "@{}",
            names::map_name(name)
                .map_err(|_| HandlerError::new("name_invalid", "invalid package name"))?
                .scope
        );
        if let std::collections::btree_map::Entry::Vacant(entry) = scopes.entry(scope.clone()) {
            let mut trust = factory(store.clone(), &scope)?;
            refresh(&mut trust).await?;
            entry.insert(trust);
        }
    }
    if let Some(status) = store.catalog_blocked_pin(&pins)? {
        return Err(status_refusal(&status));
    }
    let mut seen = BTreeSet::new();
    let mut candidates = Vec::<VerifiedPackage>::new();
    while let Some(name) = queue.pop_front() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if seen.len() > 256 {
            return Err(HandlerError::new("resolution_limit", "too many packages"));
        }
        let scope = format!(
            "@{}",
            names::map_name(&name)
                .map_err(|_| HandlerError::new("name_invalid", "invalid package name"))?
                .scope
        );
        if !scopes.contains_key(&scope) {
            let mut trust = factory(store.clone(), &scope)?;
            refresh(&mut trust).await?;
            scopes.insert(scope.clone(), trust);
        }
        let trust = scopes.get_mut(&scope).expect("scope was opened above");
        for version in trust.versions(&name).await? {
            if candidates.len() >= 1024 {
                return Err(HandlerError::new(
                    "resolution_limit",
                    "too many candidate versions",
                ));
            }
            let verified = trust.verify_version(&name, &version).await?;
            queue.extend(verified.manifest.requires.packages.keys().cloned());
            candidates.push(verified);
        }
    }
    let catalog = daemon.catalog_list().await.map_err(|e| HandlerError {
        code: "consumer_unavailable".into(),
        message: format!("daemon catalog: {e:?}"),
        detail: Some(json!({"module_id":"daemon"})),
    })?;
    let caps = capabilities.tokens(&catalog);
    let subject = if plan.operation == "install" {
        CatalogSubject::Install(plan.spec.clone())
    } else {
        CatalogSubject::Upgrade(plan.spec.clone())
    };
    let resolved = store
        .catalog_resolve(&plan.agent_id, subject, &candidates, &caps)
        .map_err(|e| {
            let mut error = HandlerError::from(e);
            let missing: Vec<_> = candidates
                .iter()
                .flat_map(|p| {
                    p.manifest
                        .requires
                        .capabilities
                        .iter()
                        .filter(|t| !caps.contains(t))
                        .map(|t| json!({"package":p.record.name,"capability":t}))
                })
                .collect();
            if !missing.is_empty() {
                error.detail.get_or_insert_with(|| json!({}))["missing_capabilities"] =
                    json!(missing);
            }
            error
        })?;
    let grants = store.publisher_grants()?;
    let mut packages = Vec::new();
    for pin in resolved.closure {
        let verified = candidates
            .iter()
            .find(|v| v.record.name == pin.name && v.record.version == pin.version)
            .expect("resolver returns only verified candidates");
        let root_certified = pin.name.starts_with("@cortexkit/");
        let trusted = root_certified
            || grants.iter().any(|g| {
                g.publisher_fingerprint == pin.publisher_fingerprint && g.revoked_at_ms.is_none()
            });
        let scripts = &verified.manifest.body["scripts"];
        let has_scripts = scripts.as_array().is_some_and(|a| !a.is_empty())
            || scripts.as_object().is_some_and(|a| !a.is_empty());
        packages.push(PreparedPackage {
            pin,
            trust: if trusted { "trusted" } else { "first_contact" }.into(),
            capabilities: verified.manifest.requires.capabilities.clone(),
            has_scripts,
            root_certified,
        });
    }
    let consumers = catalog_consumer::discover(daemon, &packages).await?;
    let checks = catalog_consumer::check(&consumers, &plan.plan_id, &packages).await?;
    let detail = PreparedPlan {
        packages,
        moves: serde_json::to_value(resolved.resolution.moves).unwrap(),
        checks,
    };
    preview_card(store, plan, &detail)?;
    Ok(detail)
}
fn status_refusal(status: &Value) -> HandlerError {
    HandlerError {
        code: if status["state"] == "tombstoned" {
            "version_tombstoned"
        } else {
            "version_revoked"
        }
        .into(),
        message: "pinned package status blocks preparation".into(),
        detail: Some(
            json!({"name":status["name"],"version":status["version"],"reason":status["reason"]}),
        ),
    }
}
fn preview_card(
    store: &RegistryStore,
    plan: &entorhinal_core::catalog_agent::CatalogPlan,
    detail: &PreparedPlan,
) -> Result<Value, HandlerError> {
    use entorhinal_core::agent::consent::{ConsentRequest, RequestIdentity, RequestState};
    let identity = RequestIdentity {
        agent_id: plan.agent_id.clone(),
        tool: "catalog_install".into(),
        request_key: plan.plan_id.clone(),
    };
    let row = ConsentRequest {
        identity,
        args_digest: cortexkit_role_tool_provider::catalog::composition_digest(
            &json!({"plan_id":plan.plan_id}),
        )
        .unwrap(),
        scope_ref: String::new(),
        scope_epoch: 0,
        created_at_ms: 0,
        elicitation_id: None,
        settlement: None,
        state: RequestState::Awaiting,
        outcome: None,
    };
    let context = crate::consent::CardContext {
        caller_label: store
            .agent_row(&plan.agent_id)?
            .ok_or_else(|| HandlerError::new("gone", "agent disappeared"))?
            .name,
        target_kind: "catalog_plan".into(),
        target_label: plan.plan_id.clone(),
        target_scope: json!({"operation":plan.operation,"target":{"plan_id":plan.plan_id,"base_pin_digest":plan.base_pin_digest,"prepared":detail}}),
    };
    crate::consent::render_card(&row,&context).map_err(|e| {
        let mut e = HandlerError::from(e);
        e.detail = Some(json!({"module_id":detail.checks.iter().max_by_key(|c|c.card_section.len()).map(|c|&c.module_id),"modules":detail.checks.iter().map(|c|&c.module_id).collect::<Vec<_>>() }));
        e
    })
}
async fn refresh(trust: &mut CatalogTrust) -> Result<(), HandlerError> {
    let applied = trust.apply_scope_feed().await?;
    if let Some(e) = applied.refusal {
        return Err(e.into());
    }
    for (_, result) in applied.results {
        result?;
    }
    Ok(())
}

impl ProjectsHandler {
    pub(super) fn admit_catalog_activation(
        &self,
        key: crate::RouteKey,
    ) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        if admission.flow_id.is_some()
            || !matches!(admission.principal,Some(subc_protocol::Principal::Reserved {ref module_id}) if module_id == crate::WRITER_MODULE)
        {
            return Err(HandlerError::new(
                "caller_forbidden",
                "catalog activation is reserved to core",
            ));
        }
        Ok(())
    }
    pub fn catalog_activation(
        &self,
        method: &str,
        params: Value,
        key: crate::RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_catalog_activation(key)?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Read {
            agent_id: String,
        }
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Ack {
            agent_id: String,
            generation_id: String,
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        if method == ACK_ACTIVATION {
            let args: Ack = serde_json::from_value(params).map_err(crate::invalid_params)?;
            let generation = store.catalog_activate_generation(
                &args.generation_id,
                &args.agent_id,
                (self.clock)(),
            ).map_err(|e| {
                let mut error = HandlerError::from(e);
                if error.code == "activation_blocked" {
                    if let Ok(g) = store.catalog_generation(&args.generation_id, &args.agent_id) {
                        if let Ok(Some(status)) = store.catalog_blocked_pin(&g.closure) {
                            error.detail = Some(json!({"name":status["name"],"version":status["version"],"reason":status["reason"]}));
                        }
                    }
                }
                error
            })?;
            let generation = store.catalog_generation(&generation.generation_id, &args.agent_id)?;
            return encode_result(json!({"generation":generation_reply(&generation)}));
        }
        let args: Read = serde_json::from_value(params).map_err(crate::invalid_params)?;
        let generations = store.catalog_agent_generations(&args.agent_id)?;
        let active = generations
            .iter()
            .find(|g| matches!(g.state.as_str(), "active" | "degraded"));
        let ready = generations
            .iter()
            .filter(|g| {
                g.state == "ready"
                    && active.is_none_or(|a| {
                        (g.created_at_ms, &g.generation_id) > (a.created_at_ms, &a.generation_id)
                    })
            })
            .collect::<Vec<_>>();
        encode_result(
            json!({"active":active.map(generation_reply),"ready":ready.into_iter().map(generation_reply).collect::<Vec<_>>()}),
        )
    }
    pub async fn execute_catalog_tool(
        &self,
        name: &str,
        mut arguments: Map<String, Value>,
        admission: RouteAdmission,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        let stamp = admission
            .scope
            .as_ref()
            .expect("tool admission requires a daemon stamp");
        let caller = stamp
            .attributes
            .agent_id
            .as_deref()
            .expect("tool admission requires an agent");
        let store = self.with_store(|s| Ok(s.clone()))?;
        match name {
            "catalog_prepare" => {
                let spec: Spec =
                    serde_json::from_value(arguments.remove("spec").unwrap_or(Value::Null))
                        .map_err(crate::invalid_params)?;
                if !matches!(spec.operation.as_str(), "install" | "upgrade")
                    || spec.target.get("agent_id").is_none()
                {
                    return Err(HandlerError::new(
                        "operation_not_yet_supported",
                        "only agent installs and upgrades are supported",
                    ));
                }
                if spec.target != json!({"agent_id":caller}) {
                    return Err(HandlerError::new(
                        "caller_forbidden",
                        "agents can prepare only for themselves",
                    ));
                }
                let key = arguments["request_key"]
                    .as_str()
                    .expect("request key validated");
                let plan_id = format!(
                    "plan:{}",
                    entorhinal_catalog::sha256_hex(
                        serde_json::to_vec(&json!([caller, key])).unwrap()
                    )
                );
                store.catalog_create_plan(
                    &plan_id,
                    caller,
                    CatalogTarget::Agent(caller.into()),
                    &spec.operation,
                    CatalogSpec {
                        package: spec.package,
                        range: spec.range,
                    },
                    (self.clock)(),
                )?;
                let plan = store.catalog_plan(&plan_id, caller)?;
                if plan.state == "pending" {
                    self.catalog_agent.start(store, plan, self.clock);
                }
                encode_result(json!({"plan_id":plan_id,"state":"pending"}))
            }
            "catalog_plan" => {
                let id = arguments["plan_id"].as_str().expect("validated plan id");
                let plan = store.catalog_plan(id, caller)?;
                let mut value = serde_json::to_value(&plan).unwrap();
                if plan.state == "ready" {
                    let detail = store.catalog_prepared(id, caller)?;
                    value["closure"] = json!(detail.packages);
                    value["moves"] = detail.moves.clone();
                    value["checks"] = json!(detail.checks);
                    value["card"] = preview_card(&store, &plan, &detail)?;
                }
                encode_result(value)
            }
            "catalog_generation" => encode_result(
                json!({"generations":store.catalog_agent_generations(caller)?.iter().map(generation_reply).collect::<Vec<_>>()}),
            ),
            "catalog_install" => {
                self.install_catalog_plan(
                    &store,
                    caller,
                    stamp,
                    arguments["plan_id"].as_str().expect("validated plan id"),
                    deadline,
                )
                .await
            }
            _ => Err(HandlerError::new("unknown_tool", "unknown catalog tool")),
        }
    }
    async fn prefetch_catalog(
        &self,
        store: &RegistryStore,
        caller: &str,
        generation_id: &str,
        deadline: Instant,
    ) -> Result<(), HandlerError> {
        let generation = store.catalog_generation(generation_id, caller)?;
        if !matches!(generation.state.as_str(), "approved" | "installed") {
            return Ok(());
        }
        let detail = store.catalog_prepared(&generation.plan_id, caller)?;
        match catalog_consumer::prefetch(
            self.catalog_agent.consumers.as_ref(),
            generation_id,
            &detail.packages,
            &detail.checks,
            deadline,
        )
        .await
        {
            Ok(()) => {
                store.catalog_generation_ready(generation_id, caller)?;
            }
            Err(e) => store.catalog_prefetch_error(generation_id, caller, failure(&e))?,
        }
        Ok(())
    }
    async fn install_catalog_plan(
        &self,
        store: &RegistryStore,
        caller: &str,
        stamp: &subc_protocol::scope::ScopeStamp,
        id: &str,
        deadline: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        use crate::consent::{CardContext, ConsentCall, ConsentDecision};
        use crate::tool_provider::writes::{pending, replay};
        use entorhinal_core::agent::consent::{
            Authorization, ConsentCommit, ConsentRequest, ConsentWrite, RequestIdentity,
            RequestState,
        };
        let identity = RequestIdentity {
            agent_id: caller.into(),
            tool: "catalog_install".into(),
            request_key: id.into(),
        };
        let arguments = json!({"plan_id":id});
        let digest = cortexkit_role_tool_provider::catalog::composition_digest(&arguments)
            .expect("JSON arguments canonicalize");
        let saved = store.consent_request(&identity)?;
        if let Some(row) = &saved {
            if row.args_digest != digest {
                return Err(HandlerError::new(
                    "request_key_conflict",
                    "install arguments changed",
                ));
            }
            if row.state != RequestState::Awaiting {
                let outcome = row.outcome.clone().ok_or_else(|| {
                    HandlerError::new("consent_lost", "terminal request has no outcome")
                })?;
                if let Some(generation) = outcome["generation_id"].as_str() {
                    self.prefetch_catalog(store, caller, generation, deadline)
                        .await?;
                }
                return replay(outcome);
            }
        }
        let lease = self
            .consent_coordinator
            .try_lease(&identity)
            .ok_or_else(|| pending(saved.as_ref().and_then(|s| s.elicitation_id.clone())))?;
        let row = saved.clone().unwrap_or(ConsentRequest {
            identity,
            args_digest: digest,
            scope_ref: stamp.scope_ref.clone(),
            scope_epoch: i64::try_from(stamp.scope_epoch)
                .map_err(|_| HandlerError::new("caller_forbidden", "scope epoch out of range"))?,
            created_at_ms: (self.clock)(),
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        });
        let fresh = store.catalog_install_target(id, caller);
        let target = match fresh {
            Ok(target) => target,
            Err(e) if saved.is_some() => {
                return self.refuse_agent_write(
                    store,
                    &row,
                    "catalog_install",
                    &Value::Null,
                    Authorization::Card {
                        id: row.elicitation_id.clone().unwrap_or_default(),
                        answer: "deny".into(),
                    },
                    e.into(),
                )
            }
            Err(e) => return Err(e.into()),
        };
        let plan = if saved.is_some() {
            let plan = store
                .consent_write_plan(&row.identity)?
                .ok_or_else(|| HandlerError::new("consent_lost", "missing saved catalog target"))?;
            if plan.tool != "catalog_install"
                || plan.arguments != arguments
                || plan.target != target
            {
                return self.refuse_agent_write(
                    store,
                    &row,
                    "catalog_install",
                    &target,
                    Authorization::Card {
                        id: row.elicitation_id.clone().unwrap_or_default(),
                        answer: "deny".into(),
                    },
                    HandlerError::new("plan_stale", "approved catalog target changed"),
                );
            }
            plan
        } else {
            ConsentWrite {
                tool: "catalog_install".into(),
                arguments,
                target,
                grant_class: None,
                scope_id: None,
            }
        };
        let detail: PreparedPlan = serde_json::from_value(plan.target["prepared"].clone())
            .map_err(crate::invalid_params)?;
        let operation = store.catalog_plan(id, caller)?.operation;
        let op = if operation == "install" {
            entorhinal_core::catalog_grants::Operation::Install
        } else {
            entorhinal_core::catalog_grants::Operation::Upgrade
        };
        let grants = store.publisher_grants()?;
        let covered = saved.is_none()
            && entorhinal_core::catalog_grants::coverage(
                &detail
                    .packages
                    .iter()
                    .map(PreparedPackage::coverage)
                    .collect::<Vec<_>>(),
                &grants,
                op,
                caller,
                true,
            ) == entorhinal_core::catalog_grants::Coverage::Covered;
        let mut attempt = None;
        let authorization = if covered {
            Authorization::PublisherGrants {
                ids: grants
                    .iter()
                    .filter(|g| {
                        g.placement_agent_id == caller
                            && g.operation == op
                            && g.revoked_at_ms.is_none()
                    })
                    .map(|g| g.id.clone())
                    .collect(),
            }
        } else {
            let context = CardContext {
                caller_label: store
                    .agent_row(caller)?
                    .ok_or_else(|| HandlerError::new("gone", "agent disappeared"))?
                    .name,
                target_kind: "catalog_plan".into(),
                target_label: id.into(),
                target_scope: json!({"operation":operation,"target":plan.target}),
            };
            attempt = Some(
                self.consent_coordinator
                    .obtain_leased(
                        ConsentCall {
                            store,
                            connector: self.consent_connector.as_ref(),
                            stamp,
                            deadline,
                            now_ms: &self.clock,
                        },
                        &row,
                        &context,
                        Some(&plan),
                        lease,
                    )
                    .await?,
            );
            match &attempt.as_ref().expect("attempt assigned above").decision {
                ConsentDecision::Pending(id) => return Err(pending(id.clone())),
                ConsentDecision::Replay(outcome) => return replay(outcome.clone()),
                ConsentDecision::Refused("consent_unavailable") => {
                    return Err(HandlerError::new(
                        "consent_unavailable",
                        "Cingulate is unavailable",
                    ))
                }
                ConsentDecision::Refused(code) => {
                    return self.refuse_agent_write(
                        store,
                        &row,
                        "catalog_install",
                        &plan.target,
                        Authorization::Card {
                            id: row.elicitation_id.clone().unwrap_or_default(),
                            answer: "deny".into(),
                        },
                        HandlerError::new(code, "consent did not authorize installation"),
                    )
                }
                ConsentDecision::Approved(answer) => Authorization::Card {
                    id: answer.elicitation_id.clone(),
                    answer: answer.choice.clone().unwrap_or_default(),
                },
            }
        };
        let commit = ConsentCommit {
            request: row.clone(),
            authorization: authorization.clone(),
            action: "catalog_install".into(),
            target: json!(plan),
            granted: None,
            now_ms: (self.clock)(),
        };
        let result = self
            .catalog_install_write_wait(id.into(), commit, deadline)
            .await;
        let outcome = match result {
            Ok(body) => {
                let envelope: Value =
                    serde_json::from_slice(&body).map_err(crate::invalid_params)?;
                let outcome = envelope["result"].clone();
                self.prefetch_catalog(
                    store,
                    caller,
                    outcome["generation_id"].as_str().ok_or_else(|| {
                        HandlerError::new("invalid_reply", "missing generation id")
                    })?,
                    deadline,
                )
                .await?;
                replay(outcome)
            }
            Err(e)
                if matches!(
                    e.code.as_str(),
                    "plan_stale" | "gone" | "invalid_reply" | "consent_lost"
                ) =>
            {
                self.refuse_agent_write(
                    store,
                    &row,
                    "catalog_install",
                    &plan.target,
                    authorization,
                    e,
                )
            }
            Err(e) => Err(e),
        };
        drop(attempt);
        outcome
    }
}

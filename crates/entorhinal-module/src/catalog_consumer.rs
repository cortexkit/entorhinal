//! Each consumer capability declares one package kind; none is a wildcard.
use crate::log_client::{service_call_error, LogTransport, SubcConnector, TransportError};
use crate::{HandlerError, WRITER_MODULE};
use async_trait::async_trait;
use entorhinal_catalog::Kind;
use entorhinal_core::catalog_agent::{ConsumerCheck, PreparedPackage};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc};
use subc_client_rs::{CatalogList, SupervisorListResponse};
use subc_protocol::RouteTarget;

pub const SKILL: &str = "ck-catalog-consumer-skill/v1";
pub const PERSONA: &str = "ck-catalog-consumer-persona/v1";
pub const FLOW: &str = "ck-catalog-consumer-flow/v1";
pub const MEMORY_PROFILE: &str = "ck-catalog-consumer-memory-profile/v1";
#[cfg(test)]
pub const CAPABILITIES: &[&str] = &[SKILL, PERSONA, FLOW, MEMORY_PROFILE];
pub const SERVICE: &str = "catalog-consumer";
pub fn parse_kind(capability: &str) -> Option<Kind> {
    match capability {
        SKILL => Some(Kind::Skill),
        PERSONA => Some(Kind::Persona),
        FLOW => Some(Kind::Flow),
        MEMORY_PROFILE => Some(Kind::MemoryProfile),
        _ => None,
    }
}

#[async_trait]
pub(crate) trait ConsumerDiscovery: Send + Sync {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError>;
    async fn catalog_list(&self) -> Result<CatalogList, TransportError>;
    async fn open(&self, module: &str) -> Result<Arc<dyn LogTransport>, TransportError>;
}
pub struct SubcConsumers(pub Arc<SubcConnector>);
#[async_trait]
impl ConsumerDiscovery for SubcConsumers {
    async fn supervisor_list(&self) -> Result<SupervisorListResponse, TransportError> {
        self.0
            .consumer()
            .await?
            .supervisor_list()
            .await
            .map_err(service_call_error)
    }
    async fn catalog_list(&self) -> Result<CatalogList, TransportError> {
        self.0
            .consumer()
            .await?
            .catalog_list()
            .await
            .map_err(service_call_error)
    }
    async fn open(&self, module: &str) -> Result<Arc<dyn LogTransport>, TransportError> {
        self.0
            .open_internal_service(
                RouteTarget::InternalService {
                    module_id: module.into(),
                    service_id: SERVICE.into(),
                },
                SERVICE,
            )
            .await
    }
}
fn unavailable(module: &str, reason: impl std::fmt::Debug) -> HandlerError {
    HandlerError {
        code: "consumer_unavailable".into(),
        message: format!("catalog consumer {module} is unavailable"),
        detail: Some(json!({"module_id":module,"reason":format!("{reason:?}")})),
    }
}
fn route_unavailable(module: &str, error: TransportError) -> HandlerError {
    let mut refusal = unavailable(module, &error);
    refusal
        .detail
        .as_mut()
        .expect("unavailable carries module detail")["route_refusal"] = match error {
        TransportError::Refusal { code, detail } => json!({"code":code,"detail":detail}),
        TransportError::Unavailable => json!({"code":"unavailable"}),
        TransportError::NoReply => json!({"code":"outcome_unknown"}),
    };
    refusal
}
pub struct Consumer {
    module: String,
    kinds: Vec<Kind>,
    route: Arc<dyn LogTransport>,
}
pub async fn discover(
    daemon: &dyn ConsumerDiscovery,
    packages: &[PreparedPackage],
) -> Result<Vec<Consumer>, HandlerError> {
    let configured = daemon
        .supervisor_list()
        .await
        .map_err(|e| unavailable("daemon", e))?;
    let catalog = daemon
        .catalog_list()
        .await
        .map_err(|e| unavailable("daemon", e))?;
    let entries: BTreeMap<_, _> = catalog
        .modules
        .iter()
        .map(|e| (e.module_id.as_str(), e))
        .collect();
    let mut consumers = Vec::new();
    for module in configured.modules.iter().filter(|m| m.enabled) {
        let Some(entry) = entries.get(module.module_id.as_str()) else {
            return Err(unavailable(&module.module_id, &module.state));
        };
        let kinds: Vec<_> = entry
            .capabilities
            .as_ref()
            .map(|caps| {
                caps.provides
                    .iter()
                    .filter_map(|s| parse_kind(s))
                    .filter(|k| packages.iter().any(|p| p.pin.kind == *k))
                    .collect()
            })
            .unwrap_or_default();
        if kinds.is_empty() {
            continue;
        }
        if !entry.ready {
            return Err(unavailable(&module.module_id, &entry.not_ready));
        }
        let route = daemon
            .open(&module.module_id)
            .await
            .map_err(|e| route_unavailable(&module.module_id, e))?;
        consumers.push(Consumer {
            module: module.module_id.clone(),
            kinds,
            route,
        });
    }
    for package in packages {
        if !consumers
            .iter()
            .any(|c| c.kinds.contains(&package.pin.kind))
        {
            // Only skills are currently installable; prefrontal-core owns their
            // durable execution cache and must be present before preparation succeeds.
            return Err(unavailable(
                WRITER_MODULE,
                format!("no consumer for {:?}", package.pin.kind),
            ));
        }
    }
    consumers.sort_by(|a, b| a.module.cmp(&b.module));
    Ok(consumers)
}
fn subset<'a>(consumer: &Consumer, packages: &'a [PreparedPackage]) -> Vec<&'a PreparedPackage> {
    packages
        .iter()
        .filter(|p| consumer.kinds.contains(&p.pin.kind))
        .collect()
}
async fn call(
    consumer: &Consumer,
    method: &str,
    args: Value,
    deadline: tokio::time::Instant,
) -> Result<Value, HandlerError> {
    let value = tokio::time::timeout_at(
        deadline.min(tokio::time::Instant::now() + std::time::Duration::from_secs(20)),
        consumer.route.call(method, args),
    )
    .await
    .map_err(|e| unavailable(&consumer.module, e))?
    .map_err(|e| unavailable(&consumer.module, e))?;
    let value: Value =
        serde_json::from_slice(&value).map_err(|e| unavailable(&consumer.module, e))?;
    value
        .get("result")
        .cloned()
        .ok_or_else(|| unavailable(&consumer.module, "invalid reply"))
}
pub async fn check(
    consumers: &[Consumer],
    plan_id: &str,
    packages: &[PreparedPackage],
) -> Result<Vec<ConsumerCheck>, HandlerError> {
    let mut checks = Vec::new();
    for consumer in consumers {
        let reply = call(
            consumer,
            "catalog_consumer.check",
            json!({"plan_id":plan_id,"packages":subset(consumer,packages)}),
            tokio::time::Instant::now() + std::time::Duration::from_secs(20),
        )
        .await?;
        if reply["ok"] != true {
            return Err(HandlerError {
                code: "consumer_refused".into(),
                message: format!("{} refused the plan", consumer.module),
                detail: Some(json!({"module_id":consumer.module,"refusals":reply["refusals"]})),
            });
        }
        let section = reply["card_section"]
            .as_str()
            .filter(|v| !v.trim().is_empty())
            .ok_or_else(|| HandlerError {
                code: "consumer_contract_violation".into(),
                message: format!("{} returned no human card text", consumer.module),
                detail: Some(json!({"module_id":consumer.module,"field":"card_section"})),
            })?
            .to_owned();
        checks.push(ConsumerCheck {
            module_id: consumer.module.clone(),
            kinds: consumer.kinds.clone(),
            card_section: section,
        });
    }
    Ok(checks)
}
pub async fn prefetch(
    daemon: &dyn ConsumerDiscovery,
    generation: &str,
    packages: &[PreparedPackage],
    checks: &[ConsumerCheck],
    deadline: tokio::time::Instant,
) -> Result<(), HandlerError> {
    let consumers = tokio::time::timeout_at(deadline, discover(daemon, packages))
        .await
        .map_err(|e| unavailable("daemon", e))??;
    for checked in checks {
        if !consumers.iter().any(|c| {
            c.module == checked.module_id && checked.kinds.iter().all(|k| c.kinds.contains(k))
        }) {
            return Err(unavailable(
                &checked.module_id,
                "checked consumer disappeared or changed kinds",
            ));
        }
    }
    for consumer in &consumers {
        let reply = call(
            consumer,
            "catalog_consumer.prefetch",
            json!({"generation_id":generation,"packages":subset(consumer,packages)}),
            deadline,
        )
        .await?;
        if reply["ready"] != true {
            return Err(unavailable(&consumer.module, "prefetch not ready"));
        }
    }
    Ok(())
}

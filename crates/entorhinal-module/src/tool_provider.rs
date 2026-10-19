//! Agent tools are a separate surface from the operator and executive's
//! management methods. When a route opens, the daemon supplies a scope stamp
//! that attests the agent's identity and authority. Save that stamp and use it
//! to identify the caller; tool arguments and BindIdentity are caller-supplied
//! and cannot establish authority for a different agent.

use crate::{HandlerError, ProjectsHandler, RouteAdmission, RouteKey};
use cortexkit_role_tool_provider::{
    call::{check_call, ToolCallRequest},
    catalog::{
        composition_digest, schema_digest, CatalogAnswer, CatalogRequest, CatalogTool, Reply,
    },
    describe::{Major, RoleDescribe},
    errors, ops, CallMode, PROVIDES, REQUIRED_OPS,
};
use entorhinal_core::agent::AgentRow;
use serde_json::{json, Map, Value};
use subc_client_rs::HandlerOutcome;
use subc_protocol::{
    manifest::{Concurrency, ExecutionMode, ProviderRole, Tool},
    scope::ScopeKind,
};

const REPLY_MAX_MS: u64 = 120_000;
const ASSISTANT_CAPABILITY_TAG: &str = "entorhinal:agents.create/v1";
const HEAD_CAPABILITY_TAG: &str = "entorhinal:agents.hire/v1";
const CATALOG_INSTALL_CAPABILITY_TAG: &str = "entorhinal:catalog.install/v1";

#[derive(Clone, Copy, Debug)]
pub(super) struct AgentTool {
    pub name: &'static str,
    pub role: &'static str,
    pub writes: bool,
    description: &'static str,
    fields: &'static [&'static str],
}

const TOOLS: &[AgentTool] = &[
    AgentTool { name: "projects_create_workspace", role: "assistant", writes: true,
        description: "Ask the user to create a workspace. Workspace creation always requires consent.",
        fields: &["workspace_id", "name"] },
    AgentTool { name: "projects_register", role: "assistant", writes: true,
        description: "Register a new project in an existing workspace, with user consent or a workspace grant.",
        fields: &["name", "dir", "workspace_id"] },
    AgentTool { name: "agents_create_head", role: "assistant", writes: true,
        description: "Create a head for a project placed in a workspace, with user consent or a workspace grant.",
        fields: &["project_id", "name", "tag"] },
    AgentTool { name: "agents_hire", role: "head", writes: true,
        description: "Hire an agent in your project under your supervision, with user consent or a project grant.",
        fields: &["name", "tag"] },
    AgentTool { name: "agents_retire_hire", role: "head", writes: true,
        description: "Retire one of your own live hires immediately, without a consent card.",
        fields: &["agent_id"] },
    AgentTool { name: "agents_list_hires", role: "head", writes: false,
        description: "List your live hires.", fields: &[] },
    AgentTool {name:"catalog_prepare",role:"*",writes:true,description:"Prepare a verified package closure for your own agent in the background; returns a local plan immediately.",fields:&["spec"]},
    AgentTool {name:"catalog_plan",role:"*",writes:false,description:"Read your catalog plan's state, exact package digests, consumer checks, moves and named refusal.",fields:&["plan_id"]},
    AgentTool {name:"catalog_install",role:"*",writes:true,description:"Approve and install your ready catalog plan; retrying the plan returns the same generation.",fields:&["plan_id"]},
    AgentTool {name:"catalog_generation",role:"*",writes:false,description:"Read your current and pending local package generations and consumer readiness errors.",fields:&[]},
];

impl AgentTool {
    fn schema(self) -> Value {
        let mut properties = Map::new();
        let mut required = Vec::new();
        for field in self.fields {
            properties.insert((*field).into(), json!({"type":"string"}));
            required.push(*field);
        }
        if self.name == "projects_register" {
            properties.insert("domain".into(),json!({"type":"string","description":"Optional canonical memory domain (@scope/name), included in consent."}));
        }
        if self.name == "catalog_prepare" {
            properties.insert("spec".into(),json!({"type":"object","properties":{"operation":{"type":"string","enum":["install","upgrade"]},"target":{"type":"object","properties":{"agent_id":{"type":"string"}},"required":["agent_id"],"additionalProperties":false},"package":{"type":"string"},"range":{"type":"string"}},"required":["operation","target","package","range"],"additionalProperties":false}));
        }
        if self.name == "agents_hire" {
            properties.insert(
                "labels".into(),
                json!({"type":"array", "items":{"type":"string"}}),
            );
        }
        if self.writes && self.name != "catalog_install" {
            // Some regex engines let `$` precede a final newline. The negative
            // character check rejects controls there as well as inside the key.
            properties.insert("request_key".into(), json!({"type":"string", "minLength":1,
                "maxLength":128, "pattern":"^[ -~]+$", "not":{"pattern":"[^ -~]"},
                "description":"A caller-chosen key for this write. Reuse it only with the same arguments."}));
            required.push("request_key");
        }
        json!({"type":"object", "properties":properties, "required":required, "additionalProperties":false})
    }

    fn catalog(self) -> CatalogTool {
        let schema = self.schema();
        let mut tool = CatalogTool::new(
            self.name,
            schema_digest(&schema).expect("tool schema canonicalizes"),
            1,
            schema,
        )
        .with_reply(Reply::new(REPLY_MAX_MS));
        tool.description = Some(self.description.into());
        tool.modes = Some(vec![CallMode::Classic, CallMode::Codemode]);
        // Catalog tags control which tools core exposes; route admission still
        // checks the caller's role from the daemon's scope stamp.
        let capability = match self.name {
            "projects_create_workspace" | "projects_register" | "agents_create_head" => {
                Some(ASSISTANT_CAPABILITY_TAG)
            }
            "agents_hire" | "agents_retire_hire" | "agents_list_hires" => Some(HEAD_CAPABILITY_TAG),
            "catalog_prepare" | "catalog_plan" | "catalog_install" | "catalog_generation" => {
                Some(CATALOG_INSTALL_CAPABILITY_TAG)
            }
            _ => None,
        };
        if let Some(capability) = capability {
            tool = tool.with_capabilities(vec![capability.into()]);
        }
        tool
    }
}

pub(super) fn provider_role() -> ProviderRole {
    ProviderRole::ToolProvider {
        tools: TOOLS
            .iter()
            .map(|tool| Tool {
                name: tool.name.into(),
                description: Some(tool.description.into()),
                execution_mode: if tool.writes {
                    ExecutionMode::Mutating
                } else {
                    ExecutionMode::Pure
                },
                schema: tool.schema(),
            })
            .collect(),
        // The scope stamp saved when the daemon opened this route attests project
        // and caller identity. BindIdentity is supplied by the caller instead,
        // so trusting it would let callers impersonate a different agent.
        identity_scope: Vec::new(),
        concurrency: Concurrency::ModuleManaged,
        emits_push: false,
        sub_supervises: false,
    }
}

fn role_describe(arguments: &Map<String, Value>) -> Result<Value, HandlerError> {
    if !arguments.is_empty() {
        return Err(HandlerError::new(
            "invalid_request",
            "role.describe takes no arguments",
        ));
    }
    Ok(serde_json::to_value(RoleDescribe::new(
        vec![Major {
            version: PROVIDES.into(),
            ops: REQUIRED_OPS.iter().map(|op| (*op).into()).collect(),
            stability: "alpha".into(),
        }],
        env!("CARGO_PKG_VERSION"),
    ))
    .expect("role description serializes"))
}

fn catalog(arguments: Map<String, Value>) -> Result<Value, HandlerError> {
    for (field, value) in &arguments {
        let valid = match field.as_str() {
            "params" | "composition" => value.is_object(),
            "digest_only" => value.is_boolean(),
            _ => false,
        };
        if !valid {
            return Err(HandlerError::new(
                "invalid_request",
                format!("unsupported catalog member or type: {field}"),
            ));
        }
    }
    let request: CatalogRequest = serde_json::from_value(Value::Object(arguments))
        .map_err(|error| HandlerError::new("invalid_request", error.to_string()))?;
    if !request.params.is_empty() {
        return Err(HandlerError::new(
            "invalid_request",
            "no catalog params are defined",
        ));
    }
    let mut answer =
        CatalogAnswer::new("", "").with_tools(TOOLS.iter().map(|tool| tool.catalog()).collect());
    if let Some(composition) = request.composition {
        answer.composition_digest = Some(
            composition_digest(&Value::Object(composition))
                .map_err(|error| HandlerError::new("invalid_request", error.to_string()))?,
        );
    }
    // Hash canonical content before filling the digest fields, so the digest
    // never includes itself. The same inputs yield the same generation.
    let digest = composition_digest(&serde_json::to_value(&answer).expect("catalog serializes"))
        .map_err(|error| HandlerError::new("encode_failed", error.to_string()))?;
    answer.generation = digest.clone();
    answer.catalog_digest = digest;
    if request.digest_only == Some(true) {
        Ok(json!({"generation":answer.generation, "catalog_digest":answer.catalog_digest}))
    } else {
        Ok(serde_json::to_value(answer).expect("catalog serializes"))
    }
}

pub(crate) fn encode_tool_reply(value: Value) -> Result<Vec<u8>, HandlerError> {
    serde_json::to_vec(&value)
        .map_err(|error| HandlerError::new("encode_failed", error.to_string()))
}

fn validate_request_key(arguments: &Value) -> Result<(), HandlerError> {
    let valid = arguments
        .get("request_key")
        .and_then(Value::as_str)
        .is_some_and(|key| {
            (1..=128).contains(&key.len()) && key.bytes().all(|byte| (0x20..=0x7e).contains(&byte))
        });
    if !valid {
        return Err(HandlerError::new(
            "invalid_request_key",
            "request_key must be 1–128 printable ASCII bytes",
        ));
    }
    Ok(())
}

fn validate_arguments(tool: AgentTool, arguments: &Map<String, Value>) -> Result<(), HandlerError> {
    let schema = tool.schema();
    let properties = schema["properties"].as_object().expect("object properties");
    for field in schema["required"].as_array().expect("required fields") {
        let field = field.as_str().expect("field name");
        if !arguments.contains_key(field) {
            return Err(HandlerError::new(
                "invalid_request",
                format!("missing argument: {field}"),
            ));
        }
    }
    for (field, value) in arguments {
        let valid = properties
            .get(field)
            .is_some_and(|property| match property["type"].as_str() {
                Some("string") => value.is_string(),
                Some("object") => value.is_object(),
                Some("array") => value
                    .as_array()
                    .is_some_and(|values| values.iter().all(Value::is_string)),
                _ => false,
            });
        if !valid {
            return Err(HandlerError::new(
                "invalid_request",
                format!("unknown argument or wrong type: {field}"),
            ));
        }
    }
    Ok(())
}

impl ProjectsHandler {
    /// Re-read liveness and role on every call, including retries. A route bound
    /// before an agent's retirement must not preserve that agent's authority.
    pub(super) fn admit_agent_tool(
        &self,
        tool: AgentTool,
        key: RouteKey,
    ) -> Result<(RouteAdmission, AgentRow), HandlerError> {
        let admission = self.admission_for(key);
        let scope = admission.scope.as_ref().ok_or_else(|| {
            HandlerError::new("caller_forbidden", "an agent scope stamp is required")
        })?;
        if !scope.owner_authorized
            || scope.kind == ScopeKind::Worker
            || scope.attributes.flow_id.is_some()
            || (scope.attributes.run_id.is_some() && !scope.attributes.delegates)
        {
            return Err(HandlerError::new(
                "caller_forbidden",
                "this scope cannot act as an agent",
            ));
        }
        let id = scope
            .attributes
            .agent_id
            .as_deref()
            .filter(|id| !id.is_empty())
            .ok_or_else(|| {
                HandlerError::new("caller_forbidden", "the daemon stamp has no agent")
            })?;
        let caller = self
            .with_store(|store| store.agent_row(id))?
            .ok_or_else(|| HandlerError::new("unknown_agent", "the stamped agent is unknown"))?;
        if caller.status != "live" {
            return Err(HandlerError::new(
                "gone",
                "the stamped agent is no longer live",
            ));
        }
        if tool.role != "*" && caller.role != tool.role {
            return Err(HandlerError::new(
                "role_forbidden",
                "the stored agent role cannot use this tool",
            ));
        }
        let (claim, own) = self.with_store(|store| {
            Ok((
                store.cached_agent_claim(id)?,
                store.persisted_self_author()?,
            ))
        })?;
        if let Some(claim) = claim {
            if claim.integrity_failed {
                return Err(HandlerError::new(
                    "claim_integrity_failed",
                    "cached agent claim failed verification",
                ));
            }
            if claim.epoch >= 1 && (claim.holder.is_none() || claim.holder != own) {
                return Err(HandlerError::new(
                    "held_elsewhere",
                    "the stamped agent is held by another Mac",
                ));
            }
        }
        Ok((admission, caller))
    }

    async fn execute_tool_request(
        &self,
        value: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        let name = value
            .get("name")
            .and_then(Value::as_str)
            .ok_or_else(|| HandlerError::new("invalid_request", "tool name must be a string"))?;
        let deadline = tokio::time::Instant::now()
            + if name.starts_with("catalog_") {
                std::time::Duration::from_secs(110)
            } else {
                crate::consent_client::CALL_BUDGET
            };
        let tool = TOOLS.iter().find(|tool| tool.name == name).copied();
        let admission = if let Some(tool) = tool {
            let (admission, _caller) = self.admit_agent_tool(tool, key)?;
            if tool.writes && tool.name != "catalog_install" {
                validate_request_key(&value["arguments"])?;
            }
            Some(admission)
        } else {
            None
        };
        let request: ToolCallRequest = serde_json::from_value(value)
            .map_err(|error| HandlerError::new("invalid_request", error.to_string()))?;
        let pin = check_call(&request).map_err(provider_error)?;
        let arguments = request
            .arguments
            .as_object()
            .ok_or_else(|| HandlerError::new("invalid_request", "arguments must be an object"))?;
        if request.name == ops::ROLE_DESCRIBE {
            return encode_tool_reply(role_describe(arguments)?);
        }
        if request.name == ops::TOOL_CATALOG {
            return encode_tool_reply(catalog(arguments.clone())?);
        }
        let tool = tool.ok_or_else(|| provider_error(errors::unknown_tool(&request.name)))?;
        let admission = admission.expect("model tools passed caller admission");
        let definition = tool.catalog();
        let mode = admission.call_mode.as_ref().unwrap_or(&CallMode::Classic);
        if !cortexkit_role_tool_provider::catalog::serves_call_mode(&definition, mode) {
            return Err(HandlerError::new(
                errors::CALL_MODE_UNSUPPORTED,
                "this tool does not serve the route's call mode",
            ));
        }
        if let Some(pin) = pin {
            if pin.schema_digest != definition.schema_digest {
                return Err(HandlerError::new(
                    "tool_schema_changed",
                    "the tool schema no longer matches the pin",
                ));
            }
            if pin.semantics != definition.semantics {
                return Err(HandlerError::new(
                    "tool_semantics_changed",
                    "the tool semantics no longer match the pin",
                ));
            }
        }
        if !tool.writes || tool.name.starts_with("catalog_") {
            validate_arguments(tool, arguments)?;
        }
        if tool.name.starts_with("catalog_") {
            return self
                .execute_catalog_tool(tool.name, arguments.clone(), admission, deadline)
                .await;
        }
        self.execute_agent_tool(tool, arguments.clone(), admission, deadline)
            .await
    }

    pub(super) async fn handle_tool_request(&self, value: Value, key: RouteKey) -> HandlerOutcome {
        match self.execute_tool_request(value, key).await {
            Ok(body) => HandlerOutcome::Response(body),
            Err(error) => error.into_outcome(),
        }
    }
}

fn provider_error(error: subc_protocol::ErrorBody) -> HandlerError {
    HandlerError {
        code: error.code,
        message: error.message,
        detail: error.detail,
    }
}

#[cfg(test)]
#[path = "tool_provider_tests.rs"]
pub(crate) mod tests;

#[path = "tool_writes.rs"]
pub(crate) mod writes;

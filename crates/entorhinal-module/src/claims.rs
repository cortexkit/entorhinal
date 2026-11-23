//! Served agent whereabouts and the Direct-only `agent.refresh_claims` operation.

use super::{encode_result, HandlerError, ProjectsHandler, RouteKey};
use entorhinal_core::RegistryStore;
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const REFRESH: &str = "agent.refresh_claims";
pub(crate) const WHERE: &str = "agent.where";
pub(crate) const WHERE_MANY: &str = "agent.where_many";
/// Most agent ids `agent.where_many` answers in one call. This is the phone's
/// contract, independent of engram's own `read_many` batch size.
const WHERE_MANY_MAX: usize = 64;

pub(super) fn validate_where_ids(ids: &[String]) -> Result<(), HandlerError> {
    if ids.is_empty() || ids.len() > WHERE_MANY_MAX {
        return Err(HandlerError::new(
            "invalid_request",
            format!("agentIds must contain 1 to {WHERE_MANY_MAX} agent ids"),
        ));
    }
    let mut seen = std::collections::HashSet::with_capacity(ids.len());
    for id in ids {
        if !entorhinal_core::agent::valid_agent_id(id) {
            return Err(HandlerError::new("invalid_request", "invalid agent id"));
        }
        if !seen.insert(id.clone()) {
            return Err(HandlerError::new("invalid_request", "duplicate agent id"));
        }
    }
    Ok(())
}

#[derive(Clone, Copy, Debug, Default, Serialize, PartialEq, Eq)]
pub(crate) struct RefreshCounts {
    pub refreshed: usize,
    pub failed: usize,
}

impl ProjectsHandler {
    pub(super) fn admit_claim_refresh(&self, key: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(REFRESH, admission.flow_id.as_deref())?;
        if admission.principal != Some(subc_protocol::Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "claim refresh requires Direct",
            ));
        }
        Ok(())
    }

    pub(super) fn refresh_agent_claims(
        &self,
        params: Value,
        key: RouteKey,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_claim_refresh(key)?;
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {}
        let _: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if self.with_store(RegistryStore::identity_log_status)?.state != "enabled" {
            return Err(HandlerError::new(
                "identity_log_disabled",
                "claim refresh requires the identity log enabled",
            ));
        }
        if self.hub_backend_selected() {
            return Err(HandlerError::new(
                "hub_claims_watched",
                "the hub residence watch refreshes claims",
            ));
        }
        encode_result(RefreshCounts::default())
    }

    pub(super) fn agent_where(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            agent_id: String,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !entorhinal_core::agent::valid_agent_id(&request.agent_id) {
            return Err(HandlerError::new("invalid_request", "invalid agent_id"));
        }
        encode_result(self.agent_where_value(&request.agent_id)?)
    }

    pub(super) fn agent_where_many(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            #[serde(rename = "agentIds")]
            agent_ids: Vec<String>,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        validate_where_ids(&request.agent_ids)?;

        let mut agents = Vec::with_capacity(request.agent_ids.len());
        for id in request.agent_ids {
            match self.agent_where_value(&id) {
                Ok(mut agent) => {
                    agent["agent_id"] = Value::String(id);
                    agents.push(agent);
                }
                Err(error) if error.code == "unknown_agent" => {}
                Err(error) => return Err(error),
            }
        }
        encode_result(serde_json::json!({"agents":agents}))
    }

    fn agent_where_value(&self, agent_id: &str) -> Result<Value, HandlerError> {
        if let Some(residence) = self.residence() {
            let location = self.with_agent_store(|store| {
                residence.with_context(|context| store.hub_agent_where(agent_id, context))
            })?;
            let mut value = serde_json::to_value(location).map_err(crate::invalid_params)?;
            if let Some(moving) = value.get_mut("moving") {
                crate::move_ops::elapsed(moving, (self.clock)());
            }
            return Ok(value);
        }
        let location = self.with_agent_store(|store| store.agent_where(agent_id))?;
        // The log can be installed before the residence watch starts.
        if self.hub_backend_selected() {
            return Ok(
                serde_json::json!({"known":false,"status":"unclaimed","holder":null,
                "holder_name":null,"epoch":0,"cut_id":null,"read_at_ms":null,"stale":true}),
            );
        }
        let mut value = serde_json::to_value(location).map_err(crate::invalid_params)?;
        if let Some(moving) = value.get_mut("moving") {
            crate::move_ops::elapsed(moving, (self.clock)());
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{atomic::Ordering, Arc};

    use super::super::{
        tests::{reserved, scratch_descriptor},
        RouteAdmission,
    };
    use super::*;
    use serde_json::json;
    use subc_client_rs::HandlerOutcome;
    use subc_protocol::Principal;
    const DIRECT: RouteKey = (60, 1);
    const HOST: RouteKey = (61, 1);
    const OTHER: RouteKey = (62, 1);
    struct Fixture {
        handler: Arc<ProjectsHandler>,
        dir: std::path::PathBuf,
    }
    fn body(method: &str, params: Value) -> Vec<u8> {
        serde_json::to_vec(&json!({"method":method,"params":params})).unwrap()
    }
    fn result(outcome: HandlerOutcome) -> Value {
        match outcome {
            HandlerOutcome::Response(bytes) => {
                serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone()
            }
            other => panic!("expected response: {other:?}"),
        }
    }
    fn code(outcome: HandlerOutcome) -> String {
        match outcome {
            HandlerOutcome::Error { code, .. } => code,
            other => panic!("expected error: {other:?}"),
        }
    }
    impl Fixture {
        fn new() -> Self {
            let (dir, descriptor) = scratch_descriptor("claims");
            let handler = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700);
            let store = RegistryStore::open(&descriptor).unwrap();
            store
                .apply_entry("agent.cutover", "{}", "fixture", None, |_| Ok(()))
                .unwrap();
            *handler.store.lock().unwrap() = Some(store);
            handler.health.store_ready.store(true, Ordering::Relaxed);
            handler.route_admissions().insert(
                DIRECT,
                RouteAdmission::from_bind(Some(Principal::Direct), None),
            );
            handler.route_admissions().insert(
                HOST,
                RouteAdmission::from_bind(Some(reserved("prefrontal")), None),
            );
            handler.route_admissions().insert(
                OTHER,
                RouteAdmission::from_bind(Some(reserved("other")), None),
            );
            Self {
                handler: Arc::new(handler),
                dir,
            }
        }
        async fn call(&self, key: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(&body(method, params), key)
                .await
        }
        async fn create_only(&self, n: usize) -> String {
            result(self.call(HOST,"agent.create",json!({"name":format!("Agent {n}"),"role":"assistant","tag":"test","request_key":format!("create-{n}")})).await)["agent"]["agent_id"].as_str().unwrap().into()
        }
        fn where_(&self, id: &str) -> Value {
            result(
                self.handler
                    .handle_request(&body(WHERE, json!({"agent_id":id})), OTHER),
            )
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.dir);
        }
    }

    #[tokio::test]
    async fn refresh_admission_empty_disabled_and_sync_seam_are_closed() {
        let f = Fixture::new();
        assert_eq!(
            code(f.call(OTHER, REFRESH, json!({})).await),
            "caller_forbidden"
        );
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({"extra":1})).await),
            "invalid_request"
        );
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({})).await),
            "identity_log_disabled"
        );
        assert_eq!(
            code(f.handler.handle_request(&body(REFRESH, json!({})), OTHER)),
            "caller_forbidden"
        );
        assert_eq!(
            code(f.handler.handle_request(&body(REFRESH, json!({})), DIRECT)),
            "served_path_required"
        );
        let mut admission = RouteAdmission::from_bind(Some(Principal::Direct), None);
        admission.flow_id = Some("flow".into());
        f.handler.route_admissions().insert(DIRECT, admission);
        assert_eq!(
            code(f.call(DIRECT, REFRESH, json!({})).await),
            "flow_scope_not_admitted"
        );
    }

    #[tokio::test]
    async fn where_many_preserves_order_omits_unknown_and_matches_single_reply() {
        let f = Fixture::new();
        let first = f.create_only(1).await;
        let second = f.create_only(2).await;
        let expected = f.where_(&first);

        let reply = result(
            f.call(
                OTHER,
                WHERE_MANY,
                json!({"agentIds":[second,"agent_ffffffffffffffff",first]}),
            )
            .await,
        );
        let agents = reply["agents"].as_array().unwrap();
        assert_eq!(
            agents
                .iter()
                .map(|agent| agent["agent_id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            [second.as_str(), first.as_str()]
        );
        assert_eq!(agents[0]["status"], "unclaimed");
        let mut first_without_id = agents[1].clone();
        first_without_id.as_object_mut().unwrap().remove("agent_id");
        assert_eq!(first_without_id, expected);
    }

    #[tokio::test]
    async fn where_many_refuses_duplicate_ids() {
        let f = Fixture::new();
        assert_eq!(
            code(
                f.call(
                    OTHER,
                    WHERE_MANY,
                    json!({"agentIds":["agent_0000000000000001","agent_0000000000000001"]}),
                )
                .await
            ),
            "invalid_request"
        );
    }

    #[tokio::test]
    async fn where_many_accepts_64_and_refuses_invalid_shapes_and_65() {
        let f = Fixture::new();
        let ids = (0..64)
            .map(|n| format!("agent_{:016x}", 0x1000 + n))
            .collect::<Vec<_>>();
        let accepted = result(f.call(OTHER, WHERE_MANY, json!({"agentIds":ids})).await);
        assert_eq!(accepted, json!({"agents":[]}));

        let too_many = (0..65)
            .map(|n| format!("agent_{:016x}", 0x2000 + n))
            .collect::<Vec<_>>();
        for params in [
            json!({"agentIds":[]}),
            json!({"agentIds":too_many}),
            json!({"agentIds":["bad-id"]}),
            json!({"agentIds":["agent_0000000000000001"],"extra":true}),
        ] {
            assert_eq!(
                code(f.call(OTHER, WHERE_MANY, params).await),
                "invalid_request"
            );
        }
    }
}

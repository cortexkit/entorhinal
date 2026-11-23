//! Served agent whereabouts and the legacy move admission claim check.

use super::{
    claim_codec::AgentClaim,
    encode_result,
    record_client::{CasRequest, CasSettlement, RecordClient, RecordError, RecordReply, MANY_CAP},
    HandlerError, HealthGauges, ProjectsHandler, RouteKey,
};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    RegistryStore,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{atomic::Ordering, Arc, Mutex, Weak};

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

/// Only legacy move admission runs a pass. Do not keep the store open across a
/// call to engram; an interrupted move must not prevent the store reopening.
struct Worker {
    store: Weak<Mutex<Option<RegistryStore>>>,
    sql_operations: Arc<crate::move_driver::SqlOperations>,
    records: RecordClient,
    health: Arc<HealthGauges>,
    clock: fn() -> i64,
}

impl Worker {
    /// The handler's current store, or `None` once the handler or its store
    /// is gone.
    fn store(&self) -> Option<RegistryStore> {
        self.store.upgrade()?.lock().unwrap().clone()
    }

    async fn pass(&self) -> Result<RefreshCounts, HandlerError> {
        let Some(store) = self.store() else {
            return Ok(RefreshCounts::default());
        };
        let agents = tokio::task::spawn_blocking(self.sql_operations.tracked(move || {
            if store.is_foreign()
                || store.hub_log_base()?.is_some()
                || store.identity_log_status()?.state != "enabled"
            {
                return Ok(Vec::new());
            }
            store.live_claim_agents()
        }))
        .await
        .map_err(|e| HandlerError::new("storage_error", e.to_string()))??;
        let mut counts = RefreshCounts::default();
        let mut reads_succeeded = 0;
        for chunk in agents.chunks(MANY_CAP) {
            let mut offset = 0;
            while offset < chunk.len() {
                if self.health.hub_membership.load(Ordering::Acquire) != 0 {
                    return Ok(counts);
                }
                let subjects: Vec<String> = chunk[offset..]
                    .iter()
                    .map(|id| format!("claim:{id}"))
                    .collect();
                let (entries, next) = match self.records.read_many(&subjects).await {
                    Ok(page) => page,
                    Err(error) => (vec![Err(error); subjects.len()], None),
                };
                let Some(store) = self.store() else {
                    return Ok(counts);
                };
                let (bindings, known_author) =
                    tokio::task::spawn_blocking(self.sql_operations.tracked(move || {
                        let bindings = if store.is_foreign()
                            || store.identity_log_status()?.state != "enabled"
                        {
                            Vec::new()
                        } else {
                            store.local_claim_bindings()?
                        };
                        Ok::<_, entorhinal_core::RegistryError>((
                            bindings,
                            store.persisted_self_author()?,
                        ))
                    }))
                    .await
                    .map_err(|e| HandlerError::new("storage_error", e.to_string()))??;
                let mut observations = Vec::new();
                for (id, reply) in chunk[offset..].iter().zip(entries) {
                    if reply.is_ok() {
                        reads_succeeded += 1;
                    }
                    let local = bindings.iter().find(|(agent, _)| agent == id);
                    let reply = match reply {
                        Ok(reply)
                            if local.is_some()
                                && known_author
                                    .as_deref()
                                    .is_none_or(|author| author == reply.self_author) =>
                        {
                            self.claim_if_missing(id, reply).await
                        }
                        other => other,
                    };
                    if local.is_some_and(|(_, open)| *open) {
                        if let Ok(reply) = &reply {
                            self.report_conflict(id, reply);
                        }
                    }
                    let observation = match reply {
                        Ok(reply) if reply.absent => {
                            counts.refreshed += 1;
                            ClaimObservation::Absent {
                                stale: reply.stale,
                                self_author: reply.self_author,
                            }
                        }
                        Ok(reply) => {
                            match AgentClaim::decode(id, reply.body.as_deref().unwrap_or_default())
                            {
                                Ok(claim) if i64::try_from(reply.version).is_ok() => {
                                    counts.refreshed += 1;
                                    ClaimObservation::Present {
                                        claim: CachedClaim {
                                            holder: claim.holder,
                                            epoch: claim.epoch,
                                            cut_id: claim.cut_id,
                                            version: reply.version as i64,
                                            read_at_ms: 0,
                                            stale: reply.stale,
                                            integrity_failed: false,
                                        },
                                        self_author: reply.self_author,
                                    }
                                }
                                result => {
                                    counts.failed += 1;
                                    let code = result.err().unwrap_or("claim_malformed");
                                    match code {
                                        "claim_version_unknown" => {
                                            &self.health.claim_version_unknown
                                        }
                                        "claim_subject_mismatch" => {
                                            &self.health.claim_subject_mismatch
                                        }
                                        _ => &self.health.claim_malformed,
                                    }
                                    .fetch_add(1, Ordering::Relaxed);
                                    ClaimObservation::CodecFailed {
                                        self_author: reply.self_author,
                                    }
                                }
                            }
                        }
                        Err(error) => {
                            counts.failed += 1;
                            if ["verify_failed", "decrypt_failed"].contains(&error.code()) {
                                ClaimObservation::IntegrityFailed
                            } else {
                                ClaimObservation::Unavailable
                            }
                        }
                    };
                    observations.push((id.clone(), observation));
                }
                // A shared write can own SQLite's writer connection while its
                // engram reply is pending. Waiting for that connection must not
                // block the executor that delivers the reply.
                let Some(cache) = self.store() else {
                    return Ok(counts);
                };
                let now = (self.clock)();
                let conflicts =
                    tokio::task::spawn_blocking(self.sql_operations.tracked(move || {
                        let store = cache;
                        if store.hub_log_base()?.is_some() {
                            return Ok(0);
                        }
                        store.cache_agent_claims(&observations, now)
                    }))
                    .await
                    .map_err(|e| HandlerError::new("storage_error", e.to_string()))??;
                self.health
                    .self_author_conflicts
                    .fetch_add(conflicts as u64, Ordering::Relaxed);
                offset += next.unwrap_or(subjects.len());
            }
        }
        self.health
            .claim_refreshed
            .fetch_add(counts.refreshed as u64, Ordering::Relaxed);
        self.health
            .claim_failed
            .fetch_add(counts.failed as u64, Ordering::Relaxed);
        if reads_succeeded == 0 && counts.failed > 0 {
            return Err(HandlerError::new(
                "engram_unavailable",
                "no claim record read succeeded",
            ));
        }
        Ok(counts)
    }

    async fn claim_if_missing(
        &self,
        id: &str,
        reply: RecordReply,
    ) -> Result<RecordReply, RecordError> {
        if reply.stale
            || (!reply.absent
                && !AgentClaim::decode(id, reply.body.as_deref().unwrap_or_default())
                    .is_ok_and(|claim| claim.epoch == 0 && claim.holder.is_none()))
        {
            return Ok(reply);
        }
        // The cache collapses absent and epoch-zero records. Only a fresh
        // record read supplies the version that may be replaced safely.
        let current = self.records.read(&format!("claim:{id}")).await?;
        if current.stale
            || current.self_author != reply.self_author
            || (!current.absent
                && !AgentClaim::decode(id, current.body.as_deref().unwrap_or_default())
                    .is_ok_and(|claim| claim.epoch == 0 && claim.holder.is_none()))
        {
            return Ok(current);
        }
        let claim = AgentClaim {
            v: 1,
            agent_id: id.to_owned(),
            holder: Some(reply.self_author.clone()),
            epoch: 1,
            cut_id: None,
            reason: None,
        };
        let mut write_id = [0; 16];
        getrandom::getrandom(&mut write_id)
            .map_err(|_| RecordError::refusal("unavailable", None))?;
        let request = CasRequest {
            subject: format!("claim:{id}"),
            expected_version: current.version,
            body: serde_json::to_vec(&claim)
                .map_err(|_| RecordError::refusal("claim_malformed", None))?,
            write_id,
        };
        match self
            .records
            .cas_settled(&request, &reply.self_author, |body| {
                AgentClaim::decode(id, body)
            })
            .await?
        {
            CasSettlement::Landed(version) => Ok(RecordReply {
                absent: false,
                version,
                body: Some(request.body),
                author: Some(reply.self_author.clone()),
                self_author: reply.self_author,
                stale: false,
            }),
            CasSettlement::Moved(current) => Ok(current),
        }
    }

    fn report_conflict(&self, id: &str, reply: &RecordReply) {
        if !reply.stale
            && reply
                .body
                .as_deref()
                .and_then(|body| AgentClaim::decode(id, body).ok())
                .is_some_and(|claim| {
                    claim
                        .holder
                        .as_deref()
                        .is_some_and(|holder| holder != reply.self_author)
                })
            && self
                .health
                .claim_conflict_agents
                .lock()
                .unwrap()
                .insert(id.to_owned())
        {
            self.health.claim_conflicts.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                agent_id = id,
                "agent claim belongs to another computer while a local session is open"
            );
        }
    }
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

    /// Legacy moves refresh ownership before inspecting the cached claim. This
    /// check is deleted with legacy move admission; served reads never run it.
    pub(super) async fn wait_claim_refresh(&self) -> Result<RefreshCounts, HandlerError> {
        if self.hub_backend_selected() {
            return Err(HandlerError::new(
                "hub_claims_watched",
                "the hub residence watch refreshes claims",
            ));
        }
        let _pass = self.legacy_claim_pass.lock().await;
        Worker {
            store: Arc::downgrade(&self.store),
            sql_operations: self.claim_refresh_sql.clone(),
            records: self.record_client.clone(),
            health: self.health.clone(),
            clock: self.clock,
        }
        .pass()
        .await
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

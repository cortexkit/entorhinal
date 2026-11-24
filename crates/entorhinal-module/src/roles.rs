//! Served role reads, fences and operator takes. Only the hub handles takes.

use super::{encode_result, role_leases::Backend as _, HandlerError, ProjectsHandler, RouteKey};
use entorhinal_core::role_cache::{valid_role, RoleTakeInput};
use serde::Deserialize;
use serde_json::Value;
use tokio::time::Instant;

pub(crate) const READ: &str = "role.read";
pub(crate) const FENCE: &str = "role.fence";
pub(crate) const TAKE: &str = "role.take";

impl ProjectsHandler {
    pub(super) fn admit_role_take(&self, key: RouteKey) -> Result<(), HandlerError> {
        let admission = self.admission_for(key);
        super::refuse_flow_write(TAKE, admission.flow_id.as_deref())?;
        if admission.principal != Some(subc_protocol::Principal::Direct) {
            return Err(HandlerError::new(
                "caller_forbidden",
                "role.take requires Direct",
            ));
        }
        Ok(())
    }

    pub(super) fn role_read(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !super::role_leases::valid_role(&request.role) {
            return Err(HandlerError::new("invalid_request", "invalid role"));
        }
        if self.hub_backend_selected() {
            let (result, _) = self.hub_role_read(&request.role)?;
            return encode_result(result);
        }
        let store = self.with_store(|s| Ok(s.clone()))?;
        encode_result(store.role_read(&request.role)?)
    }

    pub(super) fn role_fence(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Request {
            role: String,
        }
        let request: Request = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        if !super::role_leases::valid_role(&request.role) {
            return Err(HandlerError::new("invalid_request", "invalid role"));
        }
        if self.hub_backend_selected() {
            let (read, version) = self.hub_role_read(&request.role)?;
            return encode_result(entorhinal_core::role_cache::RoleFence {
                subject: format!("role.{}", read.role),
                version,
                epoch: read.epoch,
                held_here: read.held_here,
                holder: read.holder,
                stale: read.stale,
                role: read.role,
            });
        }
        encode_result(
            self.with_store(|s| Ok(s.clone()))?
                .role_fence(&request.role)?,
        )
    }

    fn hub_role_read(
        &self,
        role: &str,
    ) -> Result<(entorhinal_core::role_cache::RoleRead, i64), HandlerError> {
        let mut read = entorhinal_core::role_cache::RoleRead {
            role: role.into(),
            stale: true,
            known: false,
            held_here: false,
            epoch: 0,
            holder: None,
            holder_name: None,
            source: None,
        };
        let Some(service) = self.residence() else {
            return Ok((read, 0));
        };
        let store = self.with_store(|s| Ok(s.clone()))?;
        let observed = service.role_on_blocking(&store, role)?;
        read.stale = !observed.ready;
        read.known = observed.ready || observed.record.is_some();
        if let Some(record) = observed.record {
            read.epoch = record.gen;
            read.held_here = observed.ready
                && observed.holder_current
                && record.holder.as_deref() == Some(service.local_box())
                && self.role_leases.holding(role, record.gen);
            read.holder = record.holder;
            read.source = Some("hub".into());
            if let Some(holder) = read.holder.as_deref() {
                read.holder_name = store
                    .resolve_move_member(holder)
                    .ok()
                    .filter(|(id, _)| id == holder)
                    .map(|(_, name)| name);
            }
        }
        let revision = i64::try_from(observed.revision)
            .map_err(|_| HandlerError::new("record_invalid", "role revision is out of range"))?;
        Ok((read, revision))
    }

    pub(super) async fn take_role(
        &self,
        params: Value,
        key: RouteKey,
        received: Instant,
    ) -> Result<Vec<u8>, HandlerError> {
        self.admit_role_take(key)?;
        let input: RoleTakeInput = serde_json::from_value(params)
            .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
        let hub = self.hub_backend_selected();
        let valid = if hub {
            super::role_leases::valid_role(&input.role)
        } else {
            valid_role(&input.role)
        };
        if !valid
            || input.expected_epoch < 0
            || input.expected_epoch == i64::MAX
            || input.request_id.len() != 32
            || !input
                .request_id
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(HandlerError::new(
                "invalid_request",
                "invalid role, epoch or request_id",
            ));
        }
        if hub {
            return self.take_hub_role(input, key, received).await;
        }
        let status = self.with_store(entorhinal_core::RegistryStore::identity_log_status)?;
        if status.state != "enabled" {
            return Err(HandlerError::new(
                "identity_log_disabled",
                "role.take requires an enabled identity log",
            ));
        }
        Err(HandlerError::new(
            "hub_not_ready",
            "hub role backend is not selected",
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        agent_ops::{OperatorConfirmError, OperatorConfirmer},
        tests::{reserved, scratch_descriptor},
        RouteAdmission,
    };
    use entorhinal_core::RegistryStore;
    use serde_json::json;
    use std::sync::{Arc, Mutex};
    use subc_client_rs::HandlerOutcome;
    use subc_protocol::Principal;
    const DIRECT: RouteKey = (80, 1);
    const MODULE: RouteKey = (81, 1);
    #[derive(Default)]
    struct Prompt {
        calls: Mutex<Vec<(String, RouteKey)>>,
    }
    #[async_trait::async_trait]
    impl OperatorConfirmer for Prompt {
        async fn confirm_operator(
            &self,
            summary: &str,
            route: RouteKey,
        ) -> Result<(), OperatorConfirmError> {
            self.calls.lock().unwrap().push((summary.into(), route));
            Err(OperatorConfirmError::Declined)
        }
    }
    struct Fixture {
        handler: Arc<ProjectsHandler>,
        prompt: Arc<Prompt>,
        dir: std::path::PathBuf,
        descriptor: cortexkit_store_types::StorageDescriptor,
    }
    impl Fixture {
        fn new() -> Self {
            let (dir, descriptor) = scratch_descriptor("roles");
            let prompt = Arc::new(Prompt::default());
            let h = ProjectsHandler::with_runtime("0123456789abcdef".into(), || 700)
                .with_operator_confirmer(prompt.clone());
            super::super::install_store(
                &h.store,
                &h.health,
                &h.catalog_sql,
                Ok(RegistryStore::open(&descriptor).unwrap()),
            );
            h.route_admissions().insert(
                DIRECT,
                RouteAdmission::from_bind(Some(Principal::Direct), None),
            );
            h.route_admissions().insert(
                MODULE,
                RouteAdmission::from_bind(Some(reserved("some-module")), None),
            );
            Self {
                handler: Arc::new(h),
                prompt,
                dir,
                descriptor,
            }
        }
        fn store(&self) -> RegistryStore {
            self.handler.with_store(|s| Ok(s.clone())).unwrap()
        }
        async fn call(&self, key: RouteKey, method: &str, params: Value) -> HandlerOutcome {
            self.handler
                .handle_served_request(
                    &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                    key,
                )
                .await
        }
        async fn close(&self) {
            crate::tests::stop_store_tasks(&self.handler).await;
        }
    }
    impl Drop for Fixture {
        fn drop(&mut self) {
            self.handler.store.lock().unwrap().take();
            let _ = std::fs::remove_dir_all(&self.dir);
        }
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
            HandlerOutcome::Error { code, .. } | HandlerOutcome::ErrorWithDetail { code, .. } => {
                code
            }
            other => panic!("expected refusal: {other:?}"),
        }
    }
    fn input() -> Value {
        json!({"role":"x","expected_epoch":0,"request_id":"11".repeat(16),"reason":"run coordinator"})
    }

    #[tokio::test]
    async fn role_take_refuses_before_hub_selection_without_prompting_or_writing() {
        let f = Fixture::new();
        // Startup catalog jobs can commit on another connection. Let them
        // finish before comparing the database's change counter.
        f.handler.catalog_sql.wait().await;
        let store = f.store();
        let cortexkit_store_types::StorageBackend::Sqlite { path } = &f.descriptor.backend else {
            unreachable!();
        };
        let reader =
            rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY)
                .unwrap();
        let data_version = || {
            reader
                .query_row("PRAGMA data_version", [], |row| row.get::<_, i64>(0))
                .unwrap()
        };
        for (state, expected) in [
            ("disabled", "identity_log_disabled"),
            ("enabling", "identity_log_disabled"),
            ("enabled", "hub_not_ready"),
        ] {
            match state {
                "enabling" => store
                    .begin_hub_enable(&"01".repeat(16), &"02".repeat(16))
                    .unwrap(),
                "enabled" => {
                    store
                        .apply_entry("fixture.role", "{}", "fixture", None, |tx| {
                            tx.execute(
                                "UPDATE identity_log_state SET state='enabled',hub_log_base=0 WHERE id=1",
                                [],
                            )?;
                            Ok(())
                        })
                        .unwrap();
                }
                _ => (),
            }
            assert_eq!(store.identity_log_status().unwrap().state, state);
            assert!(!f.handler.hub_backend_selected());
            assert!(f.handler.residence().is_none());
            let before = data_version();
            assert_eq!(
                code(f.call(MODULE, TAKE, json!({})).await),
                "caller_forbidden"
            );
            for bad in [
                json!({}),
                json!({"role":"X","expected_epoch":0,"request_id":"11".repeat(16),"reason":"x"}),
                json!({"role":"x","expected_epoch":-1,"request_id":"11".repeat(16),"reason":"x"}),
                json!({"role":"x","expected_epoch":i64::MAX,"request_id":"11".repeat(16),"reason":"x"}),
                json!({"role":"x","expected_epoch":0,"request_id":"AA".repeat(16),"reason":"x"}),
                json!({"role":"x","expected_epoch":0,"request_id":"1","reason":"x"}),
                json!({"role":"x","expected_epoch":0,"request_id":"11".repeat(16),"reason":"x","extra":true}),
            ] {
                assert_eq!(code(f.call(DIRECT, TAKE, bad).await), "invalid_request");
            }
            assert_eq!(code(f.call(DIRECT, TAKE, input()).await), expected);
            assert!(f.prompt.calls.lock().unwrap().is_empty());
            assert_eq!(data_version(), before);
        }
        let bytes = serde_json::to_vec(&json!({"method":TAKE,"params":input()})).unwrap();
        assert_eq!(
            code(f.handler.handle_request(&bytes, DIRECT)),
            "served_path_required"
        );
        f.handler
            .route_admissions()
            .get_mut(&DIRECT)
            .unwrap()
            .flow_id = Some("flow".into());
        assert_eq!(
            code(f.call(DIRECT, TAKE, input()).await),
            "flow_scope_not_admitted"
        );
        f.close().await;
    }

    #[tokio::test]
    async fn role_read_bytes_keep_unknown_null_fields_without_record_calls() {
        let f = Fixture::new();
        assert_eq!(f.handler.role_read(json!({"role":"x"})).unwrap(), br#"{"result":{"epoch":0,"held_here":false,"holder":null,"holder_name":null,"known":false,"role":"x","source":null,"stale":true}}"#);
        assert_eq!(
            f.handler
                .role_read(json!({"role":"x","extra":true}))
                .unwrap_err()
                .code,
            "invalid_request"
        );
        assert_eq!(
            result(f.call(MODULE, READ, json!({"role":"x"})).await)["known"],
            false
        );
        f.close().await;
    }

    #[tokio::test]
    async fn role_fence_of_an_unread_role_is_version_zero_and_stale() {
        let f = Fixture::new();
        assert_eq!(
            result(f.call(DIRECT, FENCE, json!({"role":"x"})).await),
            json!({"role":"x","subject":"role:x","version":0,"epoch":0,"held_here":false,"holder":null,"stale":true})
        );
        for bad in [json!({"role":"X"}), json!({"role":"x","extra":true})] {
            assert_eq!(
                f.handler.role_fence(bad).unwrap_err().code,
                "invalid_request"
            );
        }
        f.close().await;
    }
}

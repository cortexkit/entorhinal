use entorhinal_core::{shared_entry::MemberName, RegistryStore};
use serde::Deserialize;
use serde_json::Value;
use subc_protocol::Principal;

use crate::HandlerError;

pub(crate) const SET_NAME: &str = "member.set_name";

pub(crate) fn admit(principal: Option<&Principal>) -> Result<(), HandlerError> {
    if !matches!(principal, Some(Principal::Direct)) {
        return Err(HandlerError::new(
            "caller_forbidden",
            "member.set_name requires Direct",
        ));
    }
    Ok(())
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Params {
    display_name: String,
}

pub(crate) fn prepare(store: &RegistryStore, params: Value) -> Result<MemberName, HandlerError> {
    let params: Params = serde_json::from_value(params)
        .map_err(|e| HandlerError::new("invalid_request", e.to_string()))?;
    let display_name = params.display_name.trim().to_string();
    if !(1..=64).contains(&display_name.chars().count()) {
        return Err(HandlerError::new(
            "display_name_invalid",
            "display name must have 1 to 64 characters after trimming",
        ));
    }
    match store.identity_log_status()?.state.as_str() {
        "disabled" => {
            return Err(HandlerError::new(
                "identity_log_disabled",
                "enable the identity log before naming this member",
            ))
        }
        "enabling" | "joining" => {
            return Err(HandlerError::new(
                "identity_log_enabling",
                "identity log bootstrap is in progress",
            ))
        }
        _ => {}
    }
    let author = store.persisted_self_author()?.ok_or_else(|| {
        HandlerError::new(
            "self_author_unknown",
            "this member has not learned its own author",
        )
    })?;
    Ok(MemberName {
        author,
        display_name,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        fake_log::FakeLog, tests::scratch_descriptor, ProjectsHandler, RouteAdmission, WireRequest,
    };
    use serde_json::json;
    use std::sync::Arc;
    use tokio::time::{timeout, Duration, Instant};

    #[tokio::test(start_paused = true)]
    async fn s3_name_refusals_precede_decode_writer_and_engram_in_order() {
        let (dir, descriptor) = scratch_descriptor("name-refusals");
        let log = Arc::new(FakeLog::default());
        let handler = ProjectsHandler::with_log_connector("names".into(), || 700, log.clone());
        let store = RegistryStore::open(&descriptor).unwrap();
        *handler.store.lock().unwrap() = Some(store.clone());
        let route = (30, 1);
        let _writer = handler.writer.lock().await;
        for (flow, principal, state, params, expected) in [
            (
                Some("flow"),
                None,
                "disabled",
                json!({}),
                "flow_scope_not_admitted",
            ),
            (
                None,
                Some(Principal::Reserved {
                    module_id: "prefrontal-core".into(),
                }),
                "disabled",
                json!({}),
                "caller_forbidden",
            ),
            (None, None, "disabled", json!({}), "caller_forbidden"),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({}),
                "invalid_request",
            ),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({"display_name":"Studio","author":"01".repeat(16)}),
                "invalid_request",
            ),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({"display_name":3}),
                "invalid_request",
            ),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({"display_name":"  "}),
                "display_name_invalid",
            ),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({"display_name":"x".repeat(65)}),
                "display_name_invalid",
            ),
            (
                None,
                Some(Principal::Direct),
                "disabled",
                json!({"display_name":"Studio"}),
                "identity_log_disabled",
            ),
            (
                None,
                Some(Principal::Direct),
                "enabling",
                json!({"display_name":"Studio"}),
                "identity_log_enabling",
            ),
            (
                None,
                Some(Principal::Direct),
                "joining",
                json!({"display_name":"Studio"}),
                "identity_log_enabling",
            ),
            (
                None,
                Some(Principal::Direct),
                "enabled",
                json!({"display_name":"Studio"}),
                "self_author_unknown",
            ),
        ] {
            handler.route_admissions().insert(
                route,
                RouteAdmission {
                    principal,
                    flow_id: flow.map(str::to_string),
                    handle: None,
                    providers: Default::default(),
                },
            );
            store
                .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
                    tx.execute("UPDATE identity_log_state SET state=?1", [state])?;
                    Ok(())
                })
                .unwrap();
            let result = timeout(
                Duration::from_secs(1),
                handler.write_wait(
                    WireRequest {
                        method: SET_NAME.into(),
                        params,
                    },
                    route,
                    Instant::now(),
                ),
            )
            .await;
            assert_eq!(
                result
                    .expect("refusal must not queue for writer")
                    .unwrap_err()
                    .code,
                expected
            );
            assert_eq!(log.calls(), 0);
            assert_eq!(store.identity_log_status().unwrap().pending_write_count, 0);
        }
        *handler.store.lock().unwrap() = None;
        drop(store);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

//! Profile admission uses the principal recorded for the route, never request text.
use super::{encode_result, HandlerError, Principal, ProjectsHandler};
use entorhinal_core::user_profile::{UserGetRequest, UserSetRequest};
use serde_json::Value;

pub(super) const GET: &str = "user.get";
pub(super) const SET: &str = "user.set";

pub(super) fn admit(principal: Option<&Principal>, flow: Option<&str>) -> Result<(), HandlerError> {
    let allowed = match principal {
        Some(Principal::Direct) => true,
        Some(Principal::Reserved { module_id }) => {
            matches!(module_id.as_str(), "prefrontal-core" | "callosum")
        }
        _ => false,
    };
    if flow.is_none() && allowed {
        return Ok(());
    }
    Err(HandlerError::new(
        "caller_forbidden",
        "user profile is available only to its renderers and the operator",
    ))
}
pub(super) fn decode_set(params: Value) -> Result<UserSetRequest, HandlerError> {
    let req: UserSetRequest = serde_json::from_value(params).map_err(invalid_request)?;
    // Preserve the original body for request-key comparison; validation and
    // normalization are repeated inside the write transaction.
    req.clone().normalized()?;
    Ok(req)
}
pub(super) fn invalid_request(error: serde_json::Error) -> HandlerError {
    HandlerError::new("invalid_request", error.to_string())
}
impl ProjectsHandler {
    pub(super) fn user_get(&self, params: Value) -> Result<Vec<u8>, HandlerError> {
        let req: UserGetRequest = serde_json::from_value(params).map_err(invalid_request)?;
        encode_result(self.with_store(|s| s.user_get(req))?)
    }
}

#[cfg(test)]
#[path = "user_profile_tests.rs"]
mod tests;

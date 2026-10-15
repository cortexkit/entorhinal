//! Publish uses the read client's admission, TLS, redirect and response limits.
use super::*;
use crate::catalog_publish::{PublishReply, Signed};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};

impl RegistryClient {
    pub(crate) async fn publish(
        &self,
        signed: &Signed,
        archive: &[u8],
    ) -> Result<PublishReply, NetworkError> {
        tokio::time::timeout(self.limits.archive_timeout, self.upload(signed, archive))
            .await
            .map_err(|_| {
                NetworkError::new(
                    "registry_timeout",
                    "publish outcome unknown: request timed out",
                )
            })?
    }

    async fn upload(&self, signed: &Signed, archive: &[u8]) -> Result<PublishReply, NetworkError> {
        let mapped = entorhinal_catalog::names::map_name(&signed.record.name)
            .map_err(|e| NetworkError::new(e.code.as_str(), "invalid publish name"))?;
        let mut base = self.registry.clone();
        base.set_path(&format!("{}/", base.path().trim_end_matches('/')));
        let mut url = base
            .join(&format!(
                "v1/packages/{}/{}/{}",
                mapped.scope, mapped.name, signed.record.version
            ))
            .map_err(|e| NetworkError::new("registry_url_invalid", e.to_string()))?;
        self.admit(&url)?;
        for _ in 0..=10 {
            let response = self
                .client
                .put(url.clone())
                .header(reqwest::header::CONTENT_TYPE, "application/x-tar")
                .header(
                    "CK-Record",
                    URL_SAFE_NO_PAD.encode(signed.canonical.as_bytes()),
                )
                .header("CK-Signature", &signed.signature)
                .body(archive.to_vec())
                .send()
                .await
                .map_err(transport)?;
            let status = response.status();
            if status.is_redirection() {
                // Only method-preserving redirects may resend an authorized package.
                if !matches!(status.as_u16(), 307 | 308) {
                    return Err(NetworkError::new(
                        "registry_redirect_invalid",
                        "publish requires a method-preserving redirect",
                    ));
                }
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| {
                        NetworkError::new("registry_redirect_invalid", "redirect lacks Location")
                    })?;
                let next = url
                    .join(location)
                    .map_err(|e| NetworkError::new("registry_redirect_invalid", e.to_string()))?;
                if next.origin() != self.registry.origin() {
                    return Err(NetworkError::new(
                        "registry_not_allowlisted",
                        "publish redirect leaves registry origin",
                    ));
                }
                self.admit(&next)?;
                url = next;
                continue;
            }
            let retry = retry_after(
                response.headers().get(reqwest::header::RETRY_AFTER),
                (self.clock)(),
            );
            let bytes =
                Self::read_reply(response, self.limits.json_bytes, "json_too_large").await?;
            if !matches!(status.as_u16(), 200 | 201) {
                let value = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
                let code = value.as_ref().and_then(|v| v["error"]["code"].as_str());
                if status.as_u16() == 503 && code == Some("publish_unavailable") {
                    return Err(NetworkError::Retryable {
                        unavailable: Unavailable::Publish,
                        retry_after: retry,
                    });
                }
                let code = match code {
                    Some("version_exists") => "version_exists",
                    Some("scope_unknown") => "scope_unknown",
                    Some("signature_invalid") => "signature_invalid",
                    Some("key_unknown") => "key_unknown",
                    Some("key_revoked") => "key_revoked",
                    Some("key_not_valid") => "key_not_valid",
                    Some("archive_too_large") => "archive_too_large",
                    Some("manifest_too_large") => "manifest_too_large",
                    Some("record_invalid") => "record_invalid",
                    Some("name_invalid") => "name_invalid",
                    Some("name_not_canonical") => "name_not_canonical",
                    Some("record_mismatch") => "record_mismatch",
                    Some("archive_invalid") => "archive_invalid",
                    Some("manifest_invalid") => "manifest_invalid",
                    Some("files_mismatch") => "files_mismatch",
                    Some("flow_id_mismatch") => "flow_id_mismatch",
                    Some("flow_version_not_increasing") => "flow_version_not_increasing",
                    Some("id_bound") => "id_bound",
                    Some("storage_limit") => "storage_limit",
                    Some("rate_limited") => "rate_limited",
                    _ => "registry_http_error",
                };
                return Err(NetworkError::new(code, format!("HTTP {status}")));
            }
            let reply: PublishReply = serde_json::from_slice(&bytes).map_err(|_| {
                NetworkError::new(
                    "invalid_reply",
                    "publish outcome unknown: malformed registry reply",
                )
            })?;
            if !reply.matches(signed) {
                return Err(NetworkError::new(
                    "invalid_reply",
                    "publish outcome unknown: registry reply does not bind to request",
                ));
            }
            return Ok(reply);
        }
        Err(NetworkError::new(
            "registry_redirect_limit",
            "too many publish redirects",
        ))
    }
}

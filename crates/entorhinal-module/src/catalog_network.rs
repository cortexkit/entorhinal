//! A bounded registry transport. Trust verification stays in entorhinal-catalog.
use super::catalog_blob::CatalogError;
use reqwest::{redirect::Policy, Client, Url};
use serde::{Deserialize, Serialize};

pub const DEFAULT_RETRY_AFTER_SECONDS: u64 = 60;
pub const MAX_RETRY_AFTER_SECONDS: u64 = 3600;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unavailable {
    Publish,
    StatusKey,
    Status,
}
#[cfg_attr(not(test), allow(dead_code))]
impl Unavailable {
    pub fn code(self) -> &'static str {
        match self {
            Self::Publish => "publish_unavailable",
            Self::StatusKey => "status_key_unavailable",
            Self::Status => "status_unavailable",
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryAfterSource {
    DeltaSeconds,
    HttpDate,
    DefaultMissing,
    DefaultInvalid,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RetryAfter {
    pub delay: Duration,
    pub source: RetryAfterSource,
}
#[derive(Debug)]
#[cfg_attr(not(test), allow(dead_code))]
pub enum NetworkError {
    Failed(CatalogError),
    Retryable {
        unavailable: Unavailable,
        retry_after: RetryAfter,
    },
    FeedAfterUnavailable,
}
#[cfg_attr(not(test), allow(dead_code))]
impl NetworkError {
    fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self::Failed(CatalogError::new(code, message))
    }
    pub fn code(&self) -> &'static str {
        match self {
            Self::Failed(error) => error.code,
            Self::Retryable { unavailable, .. } => unavailable.code(),
            Self::FeedAfterUnavailable => "feed_after_unavailable",
        }
    }
    #[cfg_attr(not(test), allow(dead_code))]
    pub fn detail(&self) -> Option<&serde_json::Value> {
        match self {
            Self::Failed(error) => error.detail.as_ref(),
            _ => None,
        }
    }
}
impl From<CatalogError> for NetworkError {
    fn from(error: CatalogError) -> Self {
        Self::Failed(error)
    }
}
impl std::fmt::Display for NetworkError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}
impl std::error::Error for NetworkError {}
fn retry_after(header: Option<&reqwest::header::HeaderValue>, now_ms: i64) -> RetryAfter {
    let fallback = |source| RetryAfter {
        delay: Duration::from_secs(DEFAULT_RETRY_AFTER_SECONDS),
        source,
    };
    let Some(header) = header else {
        return fallback(RetryAfterSource::DefaultMissing);
    };
    let Ok(text) = header.to_str() else {
        return fallback(RetryAfterSource::DefaultInvalid);
    };
    let text = text.trim();
    if !text.is_empty() && text.bytes().all(|b| b.is_ascii_digit()) {
        if let Ok(seconds) = text.parse::<u64>() {
            return RetryAfter {
                delay: Duration::from_secs(seconds.min(MAX_RETRY_AFTER_SECONDS)),
                source: RetryAfterSource::DeltaSeconds,
            };
        }
    }
    if let Ok(date) = chrono::DateTime::parse_from_rfc2822(text) {
        let millis = date.timestamp_millis().saturating_sub(now_ms).max(0) as u64;
        return RetryAfter {
            delay: Duration::from_millis(millis.min(MAX_RETRY_AFTER_SECONDS * 1000)),
            source: RetryAfterSource::HttpDate,
        };
    }
    fallback(RetryAfterSource::DefaultInvalid)
}

use std::time::Duration;

#[derive(Clone, Copy, Debug)]
pub struct Limits {
    pub connect: Duration,
    pub archive_timeout: Duration,
    pub json_timeout: Duration,
    pub archive_bytes: usize,
    pub manifest_bytes: usize,
    pub json_bytes: usize,
}
#[allow(dead_code)]
impl Limits {
    pub const MODULE: Self = Self {
        connect: Duration::from_secs(10),
        archive_timeout: Duration::from_secs(120),
        json_timeout: Duration::from_secs(30),
        archive_bytes: 10_485_760,
        manifest_bytes: 65_536,
        json_bytes: 8_388_608,
    };
}

pub fn registry_url(text: &str) -> Result<Url, CatalogError> {
    let url =
        Url::parse(text).map_err(|e| CatalogError::new("registry_url_invalid", e.to_string()))?;
    if url.scheme() != "https"
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(CatalogError::new(
            "registry_url_invalid",
            "registry must be HTTPS without credentials, query or fragment",
        ));
    }
    Ok(url)
}

// Callers select a registry from the package's @scope mapping before downloading.
// Signature and status verification are separate from this byte transport.
#[allow(dead_code)]
pub struct RegistryClient {
    client: Client,
    registry: Url,
    allowlist: Vec<Url>,
    limits: Limits,
    clock: fn() -> i64,
}
pub(crate) struct RegistryReply {
    pub bytes: Vec<u8>,
    pub etag: Option<String>,
    pub not_modified: bool,
}
#[allow(dead_code)]
impl RegistryClient {
    pub fn new(registry: &str, allowlist: &[String], limits: Limits) -> Result<Self, CatalogError> {
        let registry = registry_url(registry)?;
        let allowlist = allowlist
            .iter()
            .map(|s| registry_url(s))
            .collect::<Result<Vec<_>, _>>()?;
        let client = Client::builder()
            .https_only(true)
            .redirect(Policy::none())
            .connect_timeout(limits.connect)
            .no_proxy()
            .build()
            .map_err(transport)?;
        let instance = Self {
            client,
            registry,
            allowlist,
            limits,
            clock: crate::unix_millis,
        };
        instance.admit(&instance.registry)?;
        Ok(instance)
    }
    pub fn for_scope(
        store: &entorhinal_core::RegistryStore,
        scope: &str,
        limits: Limits,
    ) -> Result<Self, CatalogError> {
        let (selected, allowlist) = Self::scope_configuration(store, scope)?;
        Self::new(&selected, &allowlist, limits)
    }
    fn scope_configuration(
        store: &entorhinal_core::RegistryStore,
        scope: &str,
    ) -> Result<(String, Vec<String>), CatalogError> {
        let selected = store
            .catalog_registry_for_scope(scope)
            .map_err(|e| CatalogError::new("store_error", e.to_string()))?
            .ok_or_else(|| CatalogError::new("scope_unmapped", "scope has no registry"))?;
        let allowlist = store
            .catalog_registries()
            .map_err(|e| CatalogError::new("store_error", e.to_string()))?
            .into_iter()
            .map(|r| r.url)
            .collect::<Vec<_>>();
        Ok((selected, allowlist))
    }
    #[cfg(test)]
    fn for_scope_loopback(
        store: &entorhinal_core::RegistryStore,
        scope: &str,
        limits: Limits,
    ) -> Self {
        let (selected, allowlist) = Self::scope_configuration(store, scope).unwrap();
        Self::loopback(&selected, &allowlist, limits)
    }
    fn admit(&self, target: &Url) -> Result<(), CatalogError> {
        if !self.allowlist.iter().any(|allowed| {
            let prefix = allowed.path().trim_end_matches('/');
            target.origin() == allowed.origin()
                && (prefix.is_empty()
                    || target.path() == prefix
                    || target
                        .path()
                        .strip_prefix(prefix)
                        .is_some_and(|suffix| suffix.starts_with('/')))
                && target.username().is_empty()
                && target.password().is_none()
        }) {
            return Err(CatalogError::new(
                "registry_not_allowlisted",
                "redirect or request is outside the registry allowlist",
            ));
        }
        Ok(())
    }
    pub async fn fetch_json(&self, path: &str) -> Result<Vec<u8>, NetworkError> {
        self.fetch(
            path,
            self.limits.json_bytes,
            self.limits.json_timeout,
            "json_too_large",
        )
        .await
    }
    pub(crate) async fn fetch_feed(
        &self,
        path: &str,
        etag: Option<&str>,
    ) -> Result<RegistryReply, NetworkError> {
        tokio::time::timeout(
            self.limits.json_timeout,
            self.download_reply(path, self.limits.json_bytes, "json_too_large", etag),
        )
        .await
        .map_err(|_| CatalogError::new("registry_timeout", "registry request timed out"))?
    }
    pub async fn fetch_manifest(&self, path: &str) -> Result<Vec<u8>, NetworkError> {
        self.fetch(
            path,
            self.limits.manifest_bytes,
            self.limits.json_timeout,
            "manifest_too_large",
        )
        .await
    }
    // Record verification checks signed manifest/archive digests before tar
    // structure, so these callers need bounded bytes without early tar checks.
    pub(crate) async fn fetch_archive_bytes(&self, digest: &str) -> Result<Vec<u8>, NetworkError> {
        if !entorhinal_catalog::is_hex(digest, 64) {
            return Err(NetworkError::new(
                "invalid_params",
                "invalid archive digest",
            ));
        }
        let bytes = self
            .fetch(
                &format!("v1/archives/{digest}"),
                self.limits.archive_bytes,
                self.limits.archive_timeout,
                "archive_too_large",
            )
            .await?;
        Ok(bytes)
    }
    pub async fn fetch_archive(&self, digest: &str) -> Result<Vec<u8>, NetworkError> {
        let bytes = self.fetch_archive_bytes(digest).await?;
        entorhinal_catalog::archive::read(&bytes).map_err(|e| CatalogError {
            code: e.code.as_str(),
            message: e.to_string(),
            detail: serde_json::to_value(&e).ok(),
        })?;
        if entorhinal_catalog::sha256_hex(&bytes) != digest {
            return Err(NetworkError::new(
                "digest_mismatch",
                "archive digest differs",
            ));
        }
        Ok(bytes)
    }
    async fn fetch(
        &self,
        path: &str,
        cap: usize,
        timeout: Duration,
        cap_code: &'static str,
    ) -> Result<Vec<u8>, NetworkError> {
        // One budget covers all redirects and the entire streamed response.
        tokio::time::timeout(timeout, self.download(path, cap, cap_code))
            .await
            .map_err(|_| CatalogError::new("registry_timeout", "registry request timed out"))?
    }
    async fn download(
        &self,
        path: &str,
        cap: usize,
        cap_code: &'static str,
    ) -> Result<Vec<u8>, NetworkError> {
        Ok(self.download_reply(path, cap, cap_code, None).await?.bytes)
    }
    async fn download_reply(
        &self,
        path: &str,
        cap: usize,
        cap_code: &'static str,
        etag: Option<&str>,
    ) -> Result<RegistryReply, NetworkError> {
        let mut base = self.registry.clone();
        base.set_path(&format!("{}/", base.path().trim_end_matches('/')));
        let mut url = base
            .join(path)
            .map_err(|e| CatalogError::new("registry_url_invalid", e.to_string()))?;
        self.admit(&url)?;
        for _ in 0..=10 {
            let request = self.client.get(url.clone());
            let request = if let Some(tag) = etag {
                request.header(reqwest::header::IF_NONE_MATCH, tag)
            } else {
                request
            };
            let response = request.send().await.map_err(transport)?;
            if response.status() == reqwest::StatusCode::NOT_MODIFIED {
                if etag.is_none() {
                    return Err(NetworkError::new(
                        "invalid_reply",
                        "304 without a held ETag",
                    ));
                }
                return Ok(RegistryReply {
                    bytes: vec![],
                    etag: etag.map(str::to_string),
                    not_modified: true,
                });
            }
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get(reqwest::header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| {
                        CatalogError::new(
                            "registry_redirect_invalid",
                            "redirect lacks a valid Location",
                        )
                    })?;
                url = url
                    .join(location)
                    .map_err(|e| CatalogError::new("registry_redirect_invalid", e.to_string()))?;
                self.admit(&url)?;
                continue;
            }
            let status = response.status();
            if !status.is_success() {
                let retry = retry_after(
                    response.headers().get(reqwest::header::RETRY_AFTER),
                    (self.clock)(),
                );
                let bytes =
                    Self::read_reply(response, self.limits.json_bytes, "json_too_large").await?;
                let envelope = serde_json::from_slice::<serde_json::Value>(&bytes).ok();
                let code = envelope.as_ref().and_then(|v| v["error"]["code"].as_str());
                if status == reqwest::StatusCode::SERVICE_UNAVAILABLE {
                    let unavailable = match code {
                        Some("publish_unavailable") => Some(Unavailable::Publish),
                        Some("status_key_unavailable") => Some(Unavailable::StatusKey),
                        Some("status_unavailable") => Some(Unavailable::Status),
                        _ => None,
                    };
                    if let Some(unavailable) = unavailable {
                        return Err(NetworkError::Retryable {
                            unavailable,
                            retry_after: retry,
                        });
                    }
                }
                if status == reqwest::StatusCode::CONFLICT
                    && code == Some("status_sequence_conflict")
                {
                    return Err(NetworkError::new(
                        "status_sequence_conflict",
                        "the status compare-and-set lost its sequence race",
                    ));
                }
                if status == reqwest::StatusCode::GONE && code == Some("feed_after_unavailable") {
                    return Err(NetworkError::FeedAfterUnavailable);
                }
                return Err(NetworkError::new(
                    "registry_http_error",
                    format!("HTTP {status}"),
                ));
            }
            let etag = response
                .headers()
                .get(reqwest::header::ETAG)
                .and_then(|tag| tag.to_str().ok())
                .filter(|tag| tag.starts_with('"') && tag.ends_with('"') && tag.len() <= 4096)
                .map(str::to_string);
            return Ok(RegistryReply {
                bytes: Self::read_reply(response, cap, cap_code).await?,
                etag,
                not_modified: false,
            });
        }
        Err(NetworkError::new(
            "registry_redirect_limit",
            "too many redirects",
        ))
    }

    async fn read_reply(
        mut response: reqwest::Response,
        cap: usize,
        cap_code: &'static str,
    ) -> Result<Vec<u8>, NetworkError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(transport)? {
            if chunk.len() > cap.saturating_sub(bytes.len()) {
                return Err(NetworkError::new(cap_code, "response exceeds cap"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }

    #[cfg(test)]
    pub(crate) fn loopback(registry: &str, allowlist: &[String], limits: Limits) -> Self {
        let allowlist: Vec<Url> = allowlist.iter().map(|s| Url::parse(s).unwrap()).collect();
        assert!(allowlist
            .iter()
            .all(|u| u.scheme() == "http" && u.host_str() == Some("127.0.0.1")));
        Self {
            client: Client::builder()
                .redirect(Policy::none())
                .no_proxy()
                .connect_timeout(limits.connect)
                .build()
                .unwrap(),
            registry: Url::parse(registry).unwrap(),
            allowlist,
            limits,
            clock: crate::unix_millis,
        }
    }
}
impl crate::ProjectsHandler {
    #[allow(dead_code)]
    pub(super) fn catalog_registry_client(
        &self,
        scope: &str,
    ) -> Result<RegistryClient, crate::HandlerError> {
        let store = self.with_store(|s| Ok(s.clone()))?;
        let mut client = RegistryClient::for_scope(&store, scope, Limits::MODULE)?;
        client.clock = self.clock;
        Ok(client)
    }
}

fn transport(e: reqwest::Error) -> CatalogError {
    CatalogError::new(
        if e.is_timeout() {
            "registry_timeout"
        } else {
            "registry_transport"
        },
        e.to_string(),
    )
}

#[cfg(test)]
#[path = "catalog_network_tests.rs"]
mod tests;

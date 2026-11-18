use std::sync::Arc;

use async_trait::async_trait;
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use entorhinal_catalog::MAX_SAFE_INTEGER;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use subc_client_rs::{CallOptions, SubcConsumer};
use subc_protocol::{BindIdentity, RouteTarget};

use super::{HandlerError, SealedBody, VAULT_LIMIT};

#[async_trait]
pub(crate) trait EpochVault: Send + Sync {
    fn control(&self) -> Option<&dyn EpochControl> {
        None
    }

    /// Seal with the epoch currently installed in the vault. Before persisting
    /// or sending the record, the hub writer must ensure the returned epoch
    /// matches the one authorized by the hub's epoch state.
    async fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<SealedBody, HandlerError>;
    async fn open(&self, aad: &[u8], body: &SealedBody) -> Result<Vec<u8>, HandlerError>;
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PendingMint {
    pub epoch: u64,
    pub mint_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct EpochStatus {
    pub current: Option<u64>,
    pub held: Vec<u64>,
    pub pending: Option<PendingMint>,
    pub sealing_blocked_by: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Minted {
    pub epoch: u64,
    pub mint_id: String,
    pub wraps: Vec<MintedWrap>,
}

impl Minted {
    pub(crate) fn mint(&self) -> PendingMint {
        PendingMint {
            epoch: self.epoch,
            mint_id: self.mint_id.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MintedWrap {
    pub member_id: String,
    pub wrap: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Imported {
    pub epochs: Vec<u64>,
    pub already_held: bool,
}

#[async_trait]
pub(crate) trait EpochControl: Send + Sync {
    async fn status(&self) -> Result<EpochStatus, HandlerError>;
    async fn mint(&self, members: &[String], excluded: &[String]) -> Result<Minted, HandlerError>;
    async fn commit(&self, mint: &PendingMint) -> Result<(), HandlerError>;
    async fn discard(&self, mint: &PendingMint) -> Result<(), HandlerError>;
    async fn import(&self, wrap: &Value) -> Result<Imported, HandlerError>;
    async fn wrap_for(&self, member_id: &str) -> Result<Value, HandlerError>;
}

pub(crate) struct SubcEpochVault {
    call: Arc<dyn VaultCall>,
}

impl SubcEpochVault {
    pub(crate) fn new(consumer: Arc<SubcConsumer>) -> Self {
        Self { call: consumer }
    }

    #[cfg(test)]
    pub(super) fn with_call(call: Arc<dyn VaultCall>) -> Self {
        Self { call }
    }

    async fn request<T: DeserializeOwned>(
        &self,
        method: &'static str,
        params: impl Serialize,
    ) -> Result<T, HandlerError> {
        let request = serde_json::to_vec(&Request { method, params })
            .map_err(|_| HandlerError::new("encode_failed", "could not encode vault request"))?;
        let reply = self.call.send(method, request).await?;
        let reply: Reply<T> = serde_json::from_slice(&reply)
            .map_err(|_| invalid_reply("invalid fleet epoch reply"))?;
        match reply.result {
            ResultBody::Refusal(Refusal { error }) => Err(refused(&error.code)),
            ResultBody::Success(value) => Ok(value),
        }
    }

    async fn finish(
        &self,
        method: &'static str,
        state: &str,
        mint: &PendingMint,
    ) -> Result<(), HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Finished {
            epoch: u64,
            state: String,
        }
        let done: Finished = self
            .request(
                method,
                json!({"epoch": mint.epoch, "mint_id": mint.mint_id}),
            )
            .await?;
        if done.epoch != mint.epoch || done.state != state {
            return Err(invalid_reply("vault finished a different fleet epoch"));
        }
        Ok(())
    }
}

#[async_trait]
impl EpochControl for SubcEpochVault {
    async fn status(&self) -> Result<EpochStatus, HandlerError> {
        self.request("fleet.epoch_status", json!({})).await
    }

    async fn mint(&self, members: &[String], excluded: &[String]) -> Result<Minted, HandlerError> {
        let params = if excluded.is_empty() {
            json!({"members": members})
        } else {
            json!({"members": members, "excluded": excluded})
        };
        self.request("fleet.epoch_mint", params).await
    }

    async fn commit(&self, mint: &PendingMint) -> Result<(), HandlerError> {
        self.finish("fleet.epoch_commit", "current", mint).await
    }

    async fn discard(&self, mint: &PendingMint) -> Result<(), HandlerError> {
        self.finish("fleet.epoch_discard", "discarded", mint).await
    }

    async fn import(&self, wrap: &Value) -> Result<Imported, HandlerError> {
        self.request("fleet.epoch_import", json!({"wrap": wrap}))
            .await
    }

    async fn wrap_for(&self, member_id: &str) -> Result<Value, HandlerError> {
        #[derive(Deserialize)]
        #[serde(deny_unknown_fields)]
        struct Wrapped {
            wrap: Value,
        }
        let Wrapped { wrap } = self
            .request("fleet.epoch_wrap_for", json!({"member_id": member_id}))
            .await?;
        if wrap["v"] != 1
            || ["ct", "enc", "from", "to"]
                .iter()
                .any(|field| !wrap[*field].as_str().is_some_and(|s| !s.is_empty()))
        {
            return Err(invalid_reply("invalid fleet join wrap"));
        }
        Ok(wrap)
    }
}

#[async_trait]
impl EpochVault for SubcEpochVault {
    fn control(&self) -> Option<&dyn EpochControl> {
        Some(self)
    }

    async fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<SealedBody, HandlerError> {
        let body: SealedBody = self
            .request(
                "fleet.epoch_seal",
                SealParams {
                    aad: URL_SAFE_NO_PAD.encode(aad),
                    plaintext: URL_SAFE_NO_PAD.encode(plaintext),
                },
            )
            .await?;
        body.validate().map_err(invalid_reply)?;
        Ok(body)
    }

    async fn open(&self, aad: &[u8], body: &SealedBody) -> Result<Vec<u8>, HandlerError> {
        let opened: Opened = self
            .request(
                "fleet.epoch_open",
                OpenParams {
                    aad: URL_SAFE_NO_PAD.encode(aad),
                    ciphertext: &body.ciphertext,
                    epoch: body.epoch,
                    nonce: &body.nonce,
                },
            )
            .await?;
        let plaintext = URL_SAFE_NO_PAD
            .decode(opened.plaintext)
            .map_err(|_| invalid_reply("invalid fleet epoch plaintext encoding"))?;
        if plaintext.len() > VAULT_LIMIT {
            return Err(invalid_reply(
                "fleet epoch plaintext exceeds the size limit",
            ));
        }
        Ok(plaintext)
    }
}

#[async_trait]
pub(super) trait VaultCall: Send + Sync {
    async fn send(&self, method: &'static str, request: Vec<u8>) -> Result<Vec<u8>, HandlerError>;
}

#[async_trait]
impl VaultCall for SubcConsumer {
    async fn send(&self, method: &'static str, request: Vec<u8>) -> Result<Vec<u8>, HandlerError> {
        const MODULE: &str = "claustrum";
        let target = RouteTarget::ManagementSurface {
            module_id: MODULE.into(),
        };
        // Default call options use the supervised launch identity, so the vault
        // sees reserved:entorhinal rather than an operator or session principal.
        self.call(
            target,
            BindIdentity::new(crate::MODULE_ID, "fleet-record"),
            request,
            CallOptions::default(),
        )
        .await
        .map_err(|error| {
            use crate::log_client::{service_call_error, warn_call_error, TransportError};
            warn_call_error(MODULE, "management_surface", method, &error);
            match service_call_error(error) {
                TransportError::Refusal { code, .. } => refused(&code),
                _ => HandlerError::new("vault_unavailable", "fleet epoch route unavailable"),
            }
        })
    }
}

impl SealedBody {
    pub(super) fn validate(&self) -> Result<(), &'static str> {
        if self.epoch == 0 || self.epoch > MAX_SAFE_INTEGER {
            return Err("invalid fleet epoch number");
        }
        let mut nonce = [0u8; 24];
        if self.nonce.len() != 32 || URL_SAFE_NO_PAD.decode_slice(&self.nonce, &mut nonce) != Ok(24)
        {
            return Err("invalid fleet epoch nonce");
        }
        let ciphertext = URL_SAFE_NO_PAD
            .decode(&self.ciphertext)
            .map_err(|_| "invalid fleet epoch ciphertext encoding")?;
        if !(16..=VAULT_LIMIT + 16).contains(&ciphertext.len()) {
            return Err("invalid fleet epoch ciphertext length");
        }
        Ok(())
    }
}

fn invalid_reply(message: &str) -> HandlerError {
    HandlerError::new("invalid_reply", message)
}

fn refused(code: &str) -> HandlerError {
    let mapped = match code {
        "fleet_epoch_pending" | "fleet_epoch_unavailable" => "epoch_key_unavailable",
        code => code,
    };
    HandlerError::new(
        mapped,
        format!("vault refused fleet epoch operation: {code}"),
    )
}

#[derive(Serialize)]
struct Request<T> {
    method: &'static str,
    params: T,
}

#[derive(Serialize)]
struct SealParams {
    aad: String,
    plaintext: String,
}

#[derive(Serialize)]
struct OpenParams<'a> {
    aad: String,
    ciphertext: &'a str,
    epoch: u64,
    nonce: &'a str,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Reply<T> {
    result: ResultBody<T>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum ResultBody<T> {
    Refusal(Refusal),
    Success(T),
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Refusal {
    error: VaultError,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct VaultError {
    code: String,
    #[serde(rename = "class")]
    _class: RefusalClass,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum RefusalClass {
    Transient,
    Permanent,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Opened {
    plaintext: String,
}

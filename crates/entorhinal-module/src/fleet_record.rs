//! Signed, canonical log entries and KV values for the fleet hub.
//!
//! Callers supply the destination and entry ID on write. On read they select a
//! trusted key using the writer's box ID, membership episode (`writer_ep`), and
//! key version. The log and KV consumers enforce membership and epoch policy.

// The hub consumers do not call the codec until their transport is wired.
#![allow(dead_code)]

pub(super) mod vault;

use cortexkit_claustrum_client::{CredentialResolver, PublicKeyAlgorithm};
use ed25519_dalek::{Signature, VerifyingKey};
use entorhinal_catalog::{decode_hex, is_hex, jcs, MAX_SAFE_INTEGER};
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::{Map, Value};

use crate::HandlerError;
use vault::EpochVault;

pub(super) const LOG_KEY: &str = "ck.fleet.idlog";
pub(super) const CREDENTIAL_ID: &str = "signing:fleet-record:1";
const VERSION: u64 = 1;
const SIGNING_PREFIX: &str = "ck-fleet-record/v1\n";
const VAULT_LIMIT: usize = 1024 * 1024;
const MAX_ENVELOPE_BYTES: usize = VAULT_LIMIT - SIGNING_PREFIX.len() + 128 + ",\"sig\":\"\"".len();

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct Header {
    pub kind: String,
    pub key: String,
    pub writer: String,
    pub writer_ep: u64,
    pub entry_id: String,
}

impl Header {
    pub(super) fn sealed(&self) -> Result<bool, HandlerError> {
        if !is_hex(&self.writer, 32)
            || !is_hex(&self.entry_id, 32)
            || self.writer_ep > MAX_SAFE_INTEGER
            || self.kind.is_empty()
        {
            return Err(invalid("invalid fleet record header"));
        }
        if self.key == LOG_KEY {
            return Ok(!matches!(
                self.kind.as_str(),
                "member.key" | "member.removed" | "member.admitted"
            ));
        }
        if self.key.split('.').next() != Some(self.kind.as_str()) {
            return Err(invalid("fleet KV kind does not match its key"));
        }
        match self.kind.as_str() {
            "epoch" | "wrap" | "wrapreq" | "wrapjoin" => Ok(false),
            "residence" | "role" | "roleprefer" | "rolehandback" | "move" | "node" => Ok(true),
            _ => Err(invalid("unknown fleet KV kind")),
        }
    }

    fn aad_fields(&self) -> Map<String, Value> {
        Map::from_iter([
            ("v".into(), VERSION.into()),
            ("kind".into(), self.kind.clone().into()),
            ("key".into(), self.key.clone().into()),
            ("writer".into(), self.writer.clone().into()),
            ("writer_ep".into(), self.writer_ep.into()),
            ("entry_id".into(), self.entry_id.clone().into()),
        ])
    }

    fn aad(&self) -> Result<String, HandlerError> {
        canonical(&Value::Object(self.aad_fields()))
    }

    fn take(fields: &mut Map<String, Value>) -> Result<Self, HandlerError> {
        if take::<u64>(fields, "v")? != VERSION {
            return Err(invalid("unsupported fleet record version"));
        }
        Ok(Self {
            kind: take(fields, "kind")?,
            key: take(fields, "key")?,
            writer: take(fields, "writer")?,
            writer_ep: take(fields, "writer_ep")?,
            entry_id: take(fields, "entry_id")?,
        })
    }
}

#[derive(Debug, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct SealedBody {
    epoch: u64,
    nonce: String,
    ciphertext: String,
}

#[derive(Debug)]
enum Body {
    Sealed(SealedBody),
    Unsealed(Value),
}

fn take<T: DeserializeOwned>(
    fields: &mut Map<String, Value>,
    name: &str,
) -> Result<T, HandlerError> {
    fields
        .remove(name)
        .and_then(|value| serde_json::from_value(value).ok())
        .ok_or_else(|| invalid(format!("missing or invalid fleet record field {name}")))
}

fn invalid(message: impl Into<String>) -> HandlerError {
    HandlerError::new("record_invalid", message)
}

fn signature_invalid() -> HandlerError {
    HandlerError::new(
        "signature_invalid",
        "fleet record signature or key does not match",
    )
}

fn canonical(value: &Value) -> Result<String, HandlerError> {
    jcs::to_string(value).map_err(|error| invalid(error.to_string()))
}

fn signing_input(unsigned: &Value) -> Result<String, HandlerError> {
    let mut input = SIGNING_PREFIX.to_owned();
    input.push_str(&canonical(unsigned)?);
    if input.len() > VAULT_LIMIT {
        return Err(invalid(
            "fleet record signing input exceeds the vault size limit",
        ));
    }
    Ok(input)
}

fn verifies(public_key_hex: &str, input: &str, signature_hex: &str) -> bool {
    let Some(key) =
        decode_hex::<32>(public_key_hex).and_then(|bytes| VerifyingKey::from_bytes(&bytes).ok())
    else {
        return false;
    };
    let Some(signature) = decode_hex::<64>(signature_hex) else {
        return false;
    };
    key.verify_strict(input.as_bytes(), &Signature::from_bytes(&signature))
        .is_ok()
}

/// Only a checked sign reply can produce bytes for an attempt row or publish.
#[derive(Debug)]
pub(super) struct PreparedRecord {
    bytes: Vec<u8>,
    epoch: Option<u64>,
}

impl PreparedRecord {
    pub(super) fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }

    pub(super) fn epoch(&self) -> Option<u64> {
        self.epoch
    }
}

pub(super) async fn prepare(
    signer: &dyn CredentialResolver,
    vault: &dyn EpochVault,
    header: &Header,
    body: &Value,
) -> Result<PreparedRecord, HandlerError> {
    let sealed = header.sealed()?;
    let plaintext = canonical(body)?;
    if plaintext.len() > VAULT_LIMIT
        || (header.key == LOG_KEY && plaintext.len() > crate::log_client::ENTRY_DATA_CAP)
    {
        return Err(invalid("fleet record body exceeds the plaintext limit"));
    }
    for _ in 0..2 {
        let public = signer.public_key(CREDENTIAL_ID).await.map_err(|error| {
            HandlerError::new(error.code(), "fleet record public key unavailable")
        })?;
        if public.algorithm != PublicKeyAlgorithm::Ed25519
            || !is_hex(&public.key_id, 16)
            || !is_hex(&public.public_key_hex, 64)
        {
            return Err(signature_invalid());
        }
        let mut fields = header.aad_fields();
        fields.insert("key_version".into(), public.key_id.clone().into());
        let epoch = if sealed {
            let body = vault
                .seal(header.aad()?.as_bytes(), plaintext.as_bytes())
                .await?;
            fields.insert("epoch".into(), body.epoch.into());
            fields.insert("nonce".into(), body.nonce.into());
            fields.insert("ciphertext".into(), body.ciphertext.into());
            Some(body.epoch)
        } else {
            fields.insert("body".into(), body.clone());
            None
        };
        let mut envelope = Value::Object(fields);
        let input = signing_input(&envelope)?;
        let signature = signer
            .sign(CREDENTIAL_ID, input.as_bytes())
            .await
            .map_err(|error| HandlerError::new(error.code(), "fleet record signing failed"))?;
        // A rotation between public_key and sign invalidates the whole candidate.
        if signature.key_id == public.key_id
            && verifies(&public.public_key_hex, &input, &signature.signature_hex)
        {
            envelope["sig"] = signature.signature_hex.into();
            return Ok(PreparedRecord {
                bytes: canonical(&envelope)?.into_bytes(),
                epoch,
            });
        }
    }
    Err(signature_invalid())
}

/// Untrusted metadata for selecting a trusted key. The body is not exposed.
#[derive(Debug)]
pub(super) struct UnverifiedRecord {
    header: Header,
    key_version: String,
    input: String,
    body: Body,
    sig: String,
}

impl UnverifiedRecord {
    pub(super) fn parse(expected_key: &str, bytes: &[u8]) -> Result<Self, HandlerError> {
        // A full record can be larger than its signing input only by the sig field.
        if bytes.len() > MAX_ENVELOPE_BYTES {
            return Err(invalid("fleet record exceeds the envelope size limit"));
        }
        let text = std::str::from_utf8(bytes).map_err(|_| invalid("fleet record is not UTF-8"))?;
        let mut value = jcs::parse(text).map_err(|error| invalid(error.to_string()))?;
        if canonical(&value)? != text {
            return Err(invalid("fleet record is not canonical JSON"));
        }
        let Value::Object(fields) = &mut value else {
            return Err(invalid("expected a fleet record object"));
        };
        let sig = take::<String>(fields, "sig")?;
        let input = signing_input(&value)?;
        let fields = value.as_object_mut().expect("fleet record object");
        let header = Header::take(fields)?;
        if header.key != expected_key {
            return Err(invalid("fleet record key does not match its destination"));
        }
        let key_version = take::<String>(fields, "key_version")?;
        if !is_hex(&key_version, 16) {
            return Err(invalid("invalid fleet record key_version"));
        }
        let body = if header.sealed()? {
            let body: SealedBody = serde_json::from_value(Value::Object(std::mem::take(fields)))
                .map_err(|_| invalid("invalid sealed fleet record body"))?;
            body.validate().map_err(invalid)?;
            Body::Sealed(body)
        } else {
            let body = take(fields, "body")?;
            if !fields.is_empty() {
                return Err(invalid("unexpected fields in unsealed fleet record"));
            }
            Body::Unsealed(body)
        };
        Ok(Self {
            header,
            key_version,
            input,
            body,
            sig,
        })
    }

    pub(super) fn header(&self) -> &Header {
        &self.header
    }

    pub(super) fn key_version(&self) -> &str {
        &self.key_version
    }

    pub(super) fn epoch(&self) -> Option<u64> {
        match &self.body {
            Body::Sealed(body) => Some(body.epoch),
            Body::Unsealed(_) => None,
        }
    }

    pub(super) fn verify(self, trusted_key_hex: &str) -> Result<VerifiedRecord, HandlerError> {
        if !verifies(trusted_key_hex, &self.input, &self.sig) {
            return Err(signature_invalid());
        }
        Ok(VerifiedRecord {
            header: self.header,
            body: self.body,
        })
    }
}

#[derive(Debug)]
pub(super) struct VerifiedRecord {
    header: Header,
    body: Body,
}

impl VerifiedRecord {
    pub(super) fn header(&self) -> &Header {
        &self.header
    }

    pub(super) fn epoch(&self) -> Option<u64> {
        match &self.body {
            Body::Sealed(body) => Some(body.epoch),
            Body::Unsealed(_) => None,
        }
    }

    pub(super) async fn open(&self, vault: &dyn EpochVault) -> Result<Vec<u8>, HandlerError> {
        let plaintext = match &self.body {
            Body::Sealed(body) => vault.open(self.header.aad()?.as_bytes(), body).await,
            Body::Unsealed(body) => Ok(canonical(body)?.into_bytes()),
        }?;
        if self.header.key == LOG_KEY && plaintext.len() > crate::log_client::ENTRY_DATA_CAP {
            return Err(invalid("fleet log body exceeds the plaintext limit"));
        }
        Ok(plaintext)
    }
}

#[cfg(test)]
mod tests;

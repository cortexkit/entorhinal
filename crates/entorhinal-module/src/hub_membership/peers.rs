use super::{box_id, fleet_record, Key, PeerAnchor, PeerKeysRead};
use crate::{
    log_client::{service_call_error, SubcConnector, TransportError},
    HandlerError,
};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use subc_client_rs::{CallOptions, RouteHandle};
use subc_protocol::{BindIdentity, RouteTarget};

const METHOD: &str = "callosum.peer_keys_read";
const IDENTITY: &str = "callosum.identity_read";

pub(in crate::hub_membership) fn key_id(bytes: &[u8]) -> String {
    entorhinal_catalog::hex(&Sha256::digest(bytes)[..8])
}

pub(crate) struct Callosum {
    pub connector: SubcConnector,
    pub local: Arc<dyn LocalComputer>,
}

#[async_trait]
pub(crate) trait LocalComputer: Send + Sync {
    async fn member_id(&self, connector: &SubcConnector) -> Result<String, HandlerError>;
}

#[derive(Default)]
pub(crate) struct LocalIdentity {
    saved: tokio::sync::Mutex<Option<(RouteHandle, Result<String, HandlerError>)>>,
}

#[async_trait]
impl LocalComputer for LocalIdentity {
    async fn member_id(&self, connector: &SubcConnector) -> Result<String, HandlerError> {
        let on_error = |error| {
            crate::log_client::warn_call_error("callosum", "management_surface", IDENTITY, &error);
            identity_error(service_call_error(error))
        };
        let consumer = connector.consumer().await.map_err(identity_error)?;
        let mut saved = self.saved.lock().await;
        let route = consumer
            .open_route(target(), bind_identity(), CallOptions::default())
            .await
            .map_err(on_error)?;
        if let Some((previous, result)) = saved.as_ref() {
            if *previous == route {
                return result.clone();
            }
        }
        let result = consumer
            .request(&route, request(IDENTITY), CallOptions::default())
            .await
            .map_err(on_error)
            .and_then(|bytes| decode_reply(&bytes))
            .and_then(identity);
        if !result
            .as_ref()
            .is_err_and(|error| error.code == "local_identity_unavailable")
        {
            *saved = Some((route, result.clone()));
        }
        result
    }
}

fn identity(reply: Value) -> Result<String, HandlerError> {
    let fingerprint = reply["fingerprint_hex"]
        .as_str()
        .filter(|id| box_id(id))
        .ok_or_else(invalid_reply)?;
    Ok(format!("fed-computer:{fingerprint}"))
}

fn identity_error(error: TransportError) -> HandlerError {
    match error {
        TransportError::Refusal { code, .. } if code == "fed_internal" => {
            HandlerError::new("fed_internal", "Callosum could not read its local identity")
        }
        TransportError::Refusal { code, .. } if code != "fed_not_ready" => {
            HandlerError::new(&code, "Callosum refused its local identity read")
        }
        _ => HandlerError::new(
            "local_identity_unavailable",
            "Callosum local identity is not available yet",
        ),
    }
}

#[cfg(test)]
mod identity_tests {
    use super::*;

    struct TestComputer(Value);

    #[async_trait]
    impl LocalComputer for TestComputer {
        async fn member_id(&self, _: &SubcConnector) -> Result<String, HandlerError> {
            identity(self.0.clone())
        }
    }

    #[tokio::test]
    async fn hub_callosum_identity_reply_accepts_new_fields_but_not_bad_fingerprints() {
        let reply = json!({"fingerprint_hex":"ab".repeat(16), "x25519_pubkey_hex":"23".repeat(32),
            "ed25519_pubkey_hex":"45".repeat(32), "future":true});
        let callosum = Callosum {
            connector: SubcConnector::default(),
            local: Arc::new(TestComputer(reply)),
        };
        assert_eq!(callosum.own_fingerprint().await.unwrap(), "ab".repeat(16));
        for fingerprint in ["AB".repeat(16), "ab".repeat(15), "g0".repeat(16)] {
            assert_eq!(
                identity(json!({"fingerprint_hex":fingerprint}))
                    .unwrap_err()
                    .code,
                "invalid_reply"
            );
        }
    }

    fn refusal(code: &str) -> TransportError {
        TransportError::Refusal {
            code: code.into(),
            detail: None,
            message: None,
        }
    }

    #[test]
    fn hub_callosum_identity_not_ready_can_retry() {
        assert_eq!(
            identity_error(refusal("fed_not_ready")).code,
            "local_identity_unavailable"
        );
    }

    #[test]
    fn hub_callosum_identity_internal_is_not_retried() {
        assert_eq!(identity_error(refusal("fed_internal")).code, "fed_internal");
    }
}

#[async_trait]
impl PeerKeysRead for Callosum {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError> {
        decode(&self.read().await?)
    }

    async fn own_fingerprint(&self) -> Result<String, HandlerError> {
        self.local
            .member_id(&self.connector)
            .await?
            .strip_prefix("fed-computer:")
            .filter(|id| box_id(id))
            .map(str::to_owned)
            .ok_or_else(invalid_reply)
    }

    async fn fleet_peers(&self) -> Result<Vec<(String, String)>, HandlerError> {
        let reply = self.read().await?;
        let peers = reply["peers"].as_array().ok_or_else(invalid_reply)?;
        Ok(peers
            .iter()
            .filter_map(|peer| {
                if peer["class"]
                    .as_str()
                    .is_some_and(|class| class != "computer")
                {
                    return None;
                }
                let id = peer["machine_id"].as_str().filter(|id| box_id(id))?;
                let fingerprint = peer["fingerprint"].as_str().filter(|id| box_id(id))?;
                Some((id.to_owned(), fingerprint.to_owned()))
            })
            .collect())
    }
}

impl Callosum {
    pub(crate) async fn lists_box(&self, box_id: &str) -> Result<bool, HandlerError> {
        let reply = self.read().await?;
        Ok(reply["peers"]
            .as_array()
            .ok_or_else(invalid_reply)?
            .iter()
            .any(|peer| peer["machine_id"].as_str() == Some(box_id)))
    }

    async fn read(&self) -> Result<Value, HandlerError> {
        call(&self.connector, METHOD).await
    }
}

async fn call(connector: &SubcConnector, method: &str) -> Result<Value, HandlerError> {
    let consumer = connector.consumer().await.map_err(|_| unavailable())?;
    let bytes = consumer
        .call(
            target(),
            bind_identity(),
            request(method),
            CallOptions::default(),
        )
        .await
        .map_err(|error| {
            crate::log_client::warn_call_error("callosum", "management_surface", method, &error);
            unavailable()
        })?;
    decode_reply(&bytes)
}

fn target() -> RouteTarget {
    RouteTarget::ManagementSurface {
        module_id: "callosum".into(),
    }
}

fn bind_identity() -> BindIdentity {
    BindIdentity::new(crate::MODULE_ID, "fleet-membership")
}

fn request(method: &str) -> Vec<u8> {
    json!({"method":method,"params":{}})
        .to_string()
        .into_bytes()
}

fn decode_reply(bytes: &[u8]) -> Result<Value, HandlerError> {
    let reply: Value = serde_json::from_slice(bytes).map_err(|_| invalid_reply())?;
    Ok(reply.get("result").cloned().unwrap_or(reply))
}

fn unavailable() -> HandlerError {
    HandlerError::new("hub_unavailable", "Callosum peer keys unavailable")
}

fn invalid_reply() -> HandlerError {
    HandlerError::new("invalid_reply", "invalid Callosum peer keys reply")
}

fn decode(reply: &Value) -> Result<Vec<PeerAnchor>, HandlerError> {
    #[derive(Deserialize)]
    struct Reply<'a> {
        #[serde(borrow)]
        peers: Vec<Peer<'a>>,
    }
    #[derive(Deserialize)]
    struct Peer<'a> {
        class: Option<&'a str>,
        machine_id: Option<&'a str>,
        fingerprint: &'a str,
        key_record: Option<Record>,
    }
    #[derive(Deserialize)]
    struct Record {
        keys: std::collections::BTreeMap<String, Value>,
    }
    let reply = Reply::deserialize(reply).map_err(|_| invalid_reply())?;
    Ok(reply
        .peers
        .into_iter()
        .filter_map(|peer| {
            if peer.class.is_some_and(|class| class != "computer") {
                return None;
            }
            let box_id = peer.machine_id.filter(|value| box_id(value))?;
            let record = peer.key_record?;
            let key = record.keys.get("fleet_record_ed25519")?;
            let public = key["pub"]
                .as_str()
                .filter(|pubkey| fleet_record::is_hex(pubkey, 64))?;
            // Callosum's version is the decimal generation, not the envelope key id.
            let generation = key["version"].as_str()?.parse::<u64>().ok()?;
            Some(PeerAnchor {
                box_id: box_id.into(),
                fingerprint: peer.fingerprint.into(),
                generation: Some(generation),
                fleet_record_ed25519: Key {
                    version: key_id(&entorhinal_catalog::decode_hex::<32>(public)?),
                    public: public.into(),
                },
            })
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hub_membership_peer_keys_read_decodes_the_callosum_management_shape() {
        // A non-vault caller's reply: only verification keys are visible. Copied
        // from callosum's test-vectors/peer-keys-v3/cases.json (tag
        // peer-keys-vectors-v3.0), case non_vault_sees_only_verification_keys.
        let mut reply = json!({"peers":[{
            "fingerprint":"72cd6e8422c407fb6d098690f1130b7d",
            "key_record":{"keys":{"fleet_record_ed25519":{"pub":"ee05","version":"13"},"msgsig_ed25519":{"pub":"bb02","version":"5"}}},
            "machine_id":"853ecdb6e1681c3dc8c1c578d14c23a2","registry_platform":"ios"
        }]});
        assert!(decode(&reply).unwrap().is_empty());
        reply["peers"][0]["key_record"]["keys"]["fleet_record_ed25519"]["pub"] =
            "01".repeat(32).into();
        let peers = decode(&reply).unwrap();
        assert_eq!(peers.len(), 1);
        assert_eq!(peers[0].box_id, "853ecdb6e1681c3dc8c1c578d14c23a2");
        assert_eq!(peers[0].generation, Some(13));
        assert_eq!(peers[0].fleet_record_ed25519.version, "72cd6e8422c407fb");
        reply["peers"][0]
            .as_object_mut()
            .unwrap()
            .remove("key_record");
        assert!(decode(&reply).unwrap().is_empty());

        if let Ok(path) = std::env::var("ENTORHINAL_CALLOSUM_PEER_KEYS_VECTORS") {
            let cases: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
            let mut success_cases = 0;
            for case in cases["cases"].as_array().unwrap() {
                assert_eq!(case["request"], json!({"method": METHOD, "params": {}}));
                if case["reply"].get("peers").is_some() {
                    assert!(
                        decode(&case["reply"]).unwrap().is_empty(),
                        "{}",
                        case["name"]
                    );
                    success_cases += 1;
                }
            }
            assert_eq!(success_cases, 11);
        }
    }
}

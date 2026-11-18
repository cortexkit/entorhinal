use super::{box_id, fleet_record, Key, PeerAnchor, PeerKeysRead};
use crate::{log_client::SubcConnector, HandlerError};
use async_trait::async_trait;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use subc_client_rs::CallOptions;
use subc_protocol::{BindIdentity, RouteTarget};

const METHOD: &str = "callosum.peer_keys_read";

pub(in crate::hub_membership) fn key_id(bytes: &[u8]) -> String {
    entorhinal_catalog::hex(&Sha256::digest(bytes)[..8])
}

pub(crate) struct Callosum {
    pub connector: SubcConnector,
    // The local transport identity is supplied separately from the peer roster.
    pub fingerprint: std::sync::Arc<dyn Fn() -> Result<String, HandlerError> + Send + Sync>,
}

#[async_trait]
impl PeerKeysRead for Callosum {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError> {
        let consumer = self.connector.consumer().await.map_err(|_| unavailable())?;
        let bytes = consumer
            .call(
                RouteTarget::ManagementSurface {
                    module_id: "callosum".into(),
                },
                BindIdentity::new(crate::MODULE_ID, "fleet-membership"),
                json!({"method":METHOD,"params":{}})
                    .to_string()
                    .into_bytes(),
                CallOptions::default(),
            )
            .await
            .map_err(|error| {
                crate::log_client::warn_call_error(
                    "callosum",
                    "management_surface",
                    METHOD,
                    &error,
                );
                unavailable()
            })?;
        let reply: Value = serde_json::from_slice(&bytes).map_err(|_| invalid_reply())?;
        decode(reply.get("result").unwrap_or(&reply))
    }

    async fn own_fingerprint(&self) -> Result<String, HandlerError> {
        (self.fingerprint)()
    }
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

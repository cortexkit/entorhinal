//! Entorhinal owns the meaning of claim bodies; engram only carries bytes.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct AgentClaim {
    pub v: u64,
    pub agent_id: String,
    pub holder: Option<String>,
    pub epoch: i64,
    pub cut_id: Option<String>,
    pub reason: Option<String>,
}

impl AgentClaim {
    pub(crate) fn decode(id: &str, bytes: &[u8]) -> Result<Self, &'static str> {
        let value: serde_json::Value =
            serde_json::from_slice(bytes).map_err(|_| "claim_malformed")?;
        let v = value["v"].as_u64().ok_or("claim_malformed")?;
        if v != 1 {
            return Err("claim_version_unknown");
        }
        let claim: Self = serde_json::from_value(value).map_err(|_| "claim_malformed")?;
        if claim.agent_id != id {
            return Err("claim_subject_mismatch");
        }
        if claim.epoch < 0
            || (claim.epoch >= 1 && claim.holder.is_none())
            || claim
                .holder
                .as_deref()
                .is_some_and(|holder| !super::record_client::valid_author(holder))
        {
            return Err("claim_malformed");
        }
        Ok(claim)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn claim_codec_refuses_each_named_error_and_roundtrips() {
        let good = json!({"v":1,"agent_id":"agent_0123456789abcdef","holder":"01010101010101010101010101010101","epoch":1,"cut_id":null,"reason":null});
        let bytes = serde_json::to_vec(&good).unwrap();
        let claim = AgentClaim::decode("agent_0123456789abcdef", &bytes).unwrap();
        assert_eq!(serde_json::to_value(claim).unwrap(), good);
        for (body, code) in [
            (json!({"v":2}), "claim_version_unknown"),
            (
                json!({"v":1,"agent_id":"other","holder":null,"epoch":0}),
                "claim_subject_mismatch",
            ),
            (
                json!({"v":1,"agent_id":"agent_0123456789abcdef","holder":null,"epoch":1}),
                "claim_malformed",
            ),
            (
                json!({"v":1,"agent_id":"agent_0123456789abcdef","holder":null,"epoch":-1}),
                "claim_malformed",
            ),
            (
                json!({"v":1,"agent_id":"agent_0123456789abcdef","holder":"BAD","epoch":1}),
                "claim_malformed",
            ),
            (
                json!({"v":1,"agent_id":"agent_0123456789abcdef","epoch":0,"extra":true}),
                "claim_malformed",
            ),
            (json!({"v":"1"}), "claim_malformed"),
        ] {
            assert_eq!(
                AgentClaim::decode(
                    "agent_0123456789abcdef",
                    &serde_json::to_vec(&body).unwrap()
                ),
                Err(code)
            );
        }
        assert_eq!(AgentClaim::decode("x", b"{"), Err("claim_malformed"));
    }
}

use super::*;
use crate::hub_write::tests::TestKeys;

pub(crate) fn anchor(box_id: &str, keys: &TestKeys) -> PeerAnchor {
    PeerAnchor {
        box_id: box_id.into(),
        fingerprint: format!("fingerprint-{box_id}"),
        generation: Some(1),
        fleet_record_ed25519: Key {
            version: keys.key_version(),
            public: keys.public_hex(),
        },
    }
}

pub(crate) struct Roster(pub Vec<PeerAnchor>);

#[async_trait]
impl PeerKeysRead for Roster {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError> {
        Ok(self.0.clone())
    }
    async fn own_fingerprint(&self) -> Result<String, HandlerError> {
        Ok("self-fingerprint".into())
    }
}

pub(crate) struct ChangingRoster(pub watch::Sender<Vec<PeerAnchor>>);

#[async_trait]
impl PeerKeysRead for ChangingRoster {
    async fn peer_keys_read(&self) -> Result<Vec<PeerAnchor>, HandlerError> {
        Ok(self.0.borrow().clone())
    }

    async fn own_fingerprint(&self) -> Result<String, HandlerError> {
        Ok("self-fingerprint".into())
    }

    fn peer_key_updates(
        self: Arc<Self>,
    ) -> BoxStream<'static, Result<Vec<PeerAnchor>, HandlerError>> {
        stream::unfold(self.0.subscribe(), |mut roster| async {
            roster.changed().await.ok()?;
            let peers = roster.borrow_and_update().clone();
            Some((Ok(peers), roster))
        })
        .boxed()
    }
}

use crate::fleet_record::Header;
use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use serde_json::json;

const A: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const C: &str = "cccccccccccccccccccccccccccccccc";
const D: &str = "dddddddddddddddddddddddddddddddd";

async fn candidate(
    sequence: u64,
    writer: (&str, u64),
    keys: &Arc<TestKeys>,
    kind: &str,
    body: Value,
) -> (u64, Candidate) {
    let signer = ClaustrumCredentialResolver::with_target(keys.clone(), 0);
    let prepared = fleet_record::prepare(
        &signer,
        keys.as_ref(),
        &Header {
            kind: kind.into(),
            key: LOG_KEY.into(),
            writer: writer.0.into(),
            writer_ep: writer.1,
            entry_id: format!("{sequence:032x}"),
        },
        &body,
    )
    .await
    .unwrap();
    let record = UnverifiedRecord::parse(LOG_KEY, &prepared.into_bytes()).unwrap();
    let event = Event::decode(&record).unwrap();
    (sequence, Candidate { record, event })
}

fn removal(target: &str, keys: &TestKeys) -> Value {
    json!({"box_id":target,"removal_id":format!("remove-{target}"),"epoch":2,
           "key_history":[{"version":keys.key_version(),"pub":keys.public_hex()}]})
}

#[tokio::test]
async fn hub_membership_transitive_removal_proofs_are_order_independent_and_revoke_outside_episode()
{
    let a = Arc::new(TestKeys::with_seed(10));
    let b = Arc::new(TestKeys::with_seed(11));
    let c = TestKeys::with_seed(12);
    let d = TestKeys::with_seed(13);
    let current = [anchor(A, &a)];
    let mut states = Vec::new();
    for order in [[1, 2, 3], [3, 2, 1], [2, 1, 3]] {
        let mut candidates = BTreeMap::new();
        for sequence in order {
            let (writer, keys, target, removed) = match sequence {
                1 => (B, &b, C, &c),
                2 => (A, &a, B, b.as_ref()),
                _ => (B, &b, D, &d),
            };
            let (sequence, candidate) = candidate(
                sequence,
                (writer, 0),
                keys,
                "member.removed",
                removal(target, removed),
            )
            .await;
            candidates.insert(sequence, candidate);
        }
        let (proof, _) = resolve(&candidates, &[], &current, 3).unwrap();
        assert_eq!(proof.verified.keys().copied().collect::<Vec<_>>(), [1, 2]);
        assert!(proof.history.contains_key(&(C.into(), 0)));
        assert!(!proof.history.contains_key(&(D.into(), 0)));
        states.push(proof);
    }
    assert_eq!(states[0], states[1]);
    assert_eq!(states[0], states[2]);
}

#[tokio::test]
async fn hub_membership_current_anchor_and_reused_key_are_scoped_to_the_readmitted_episode() {
    let a = Arc::new(TestKeys::with_seed(17));
    let b = Arc::new(TestKeys::with_seed(18));
    let mut candidates = BTreeMap::new();
    candidates.extend([
        candidate(1, (B, 0), &b, "member.removed", removal(A, &a)).await,
        candidate(2, (B, 0), &b, "member.admitted", json!({"box_id":A,"after_removal_id":format!("remove-{A}"),"request_id":"rejoin"})).await,
        candidate(4, (A, 1), &a, "member.key", json!({"box_id":A,"fingerprint":"new-fingerprint","version":a.key_version(),"pub":a.public_hex(),"history":[{"version":a.key_version(),"pub":a.public_hex()}]})).await,
    ]);
    let (proof, persisted) = resolve(&candidates, &[], &[anchor(A, &a), anchor(B, &b)], 4).unwrap();
    assert_eq!(
        persisted
            .iter()
            .find(|anchor| anchor.box_id == A)
            .unwrap()
            .episode,
        1
    );
    assert!(!proof.anchors.contains_key(&(A.into(), 0)));
    assert!(proof.history.contains_key(&(A.into(), 0)));
    assert!(proof.history.contains_key(&(A.into(), 1)));
    let sign = ClaustrumCredentialResolver::with_target(a.clone(), 0);
    let ordinary = fleet_record::prepare(
        &sign,
        a.as_ref(),
        &Header {
            kind: "member.set_name".into(),
            key: LOG_KEY.into(),
            writer: A.into(),
            writer_ep: 1,
            entry_id: "00112233445566778899aabbccddeeff".into(),
        },
        &json!({"op":"member.set_name","author":A,"display_name":"A"}),
    )
    .await
    .unwrap();
    let record = UnverifiedRecord::parse(LOG_KEY, &ordinary.into_bytes()).unwrap();
    assert_eq!(
        proof.log_key(&record, 3).unwrap_err().code,
        "signature_invalid"
    );
    assert_eq!(proof.log_key(&record, 5).unwrap(), a.public_hex());
}

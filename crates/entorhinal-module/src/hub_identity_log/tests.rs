use std::{collections::BTreeMap, path::PathBuf, sync::Mutex};

use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use serde_json::json;

use super::*;
use crate::{
    fleet_record::{self, Header},
    hub_membership::{
        tests::{anchor, Roster},
        PeerAnchor,
    },
    hub_write::{tests::TestKeys, KvEntry},
};
use entorhinal_core::hub_store::HubAttemptWrite;

const A: &str = "0123456789abcdef0123456789abcdef";
const B: &str = "fedcba9876543210fedcba9876543210";
const C: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

#[derive(Default)]
struct Positions {
    entries: Mutex<BTreeMap<u64, Vec<u8>>>,
    reads: Mutex<Vec<u64>>,
    hold: Mutex<Option<Arc<Notify>>>,
}

impl LogTransport for Positions {
    fn tail(self: &Arc<Self>, _: u64) -> Feed<LogDelivery> {
        unreachable!("test drives individual tail deliveries")
    }
}

impl Transport for Positions {
    async fn log_entry(&self, sequence: u64, _: Instant) -> Result<Option<Vec<u8>>, HandlerError> {
        self.reads.lock().unwrap().push(sequence);
        let hold = self.hold.lock().unwrap().take();
        if let Some(hold) = hold {
            hold.notified().await;
        }
        Ok(self.entries.lock().unwrap().get(&sequence).cloned())
    }
    async fn log_head(&self, _: Instant) -> Result<u64, HandlerError> {
        Ok(self
            .entries
            .lock()
            .unwrap()
            .last_key_value()
            .map_or(0, |(head, _)| *head))
    }
    async fn kv_entry(&self, _: &str, _: Instant) -> Result<Option<KvEntry>, HandlerError> {
        unreachable!()
    }
    async fn send(
        &self,
        _: &HubAttemptWrite,
        _: &str,
        _: Instant,
    ) -> Result<Option<u64>, HandlerError> {
        unreachable!()
    }
}

struct Scratch(PathBuf);
impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

async fn tail(base: i64) -> (Scratch, Tail<Positions, TestKeys>) {
    let fixture = tail_with(base, A, vec![anchor(B, &TestKeys::new())]);
    fixture
        .1
        .membership
        .discover(0, Instant::now() + READ_BUDGET)
        .await
        .unwrap();
    fixture
}

fn tail_with(
    base: i64,
    own_box: &str,
    current_peers: Vec<PeerAnchor>,
) -> (Scratch, Tail<Positions, TestKeys>) {
    let (path, descriptor) = crate::tests::scratch_descriptor("hub-identity-log-tail");
    let store = RegistryStore::open(&descriptor).unwrap();
    store.apply_entry("agent.cutover", "{}", "test", None, |tx| {
        tx.execute("UPDATE identity_log_state SET state='enabled',hub_log_base=?1,last_applied_position=?1,last_seen_head=?1", [base])?;
        Ok(())
    }).unwrap();
    let keys = Arc::new(TestKeys::new());
    let peers = Arc::new(Roster(current_peers));
    let signer = Arc::new(ClaustrumCredentialResolver::with_target(keys.clone(), 0));
    let transport = Arc::new(Positions::default());
    let membership = Arc::new(Membership::new(
        transport.clone(),
        store.clone(),
        AnchorSource {
            own_box: own_box.into(),
            peers: peers.clone(),
            signer: signer.clone(),
        },
    ));
    (
        Scratch(path),
        Tail {
            link: Arc::new(Link {
                transport,
                trust: keys.clone(),
                signer,
                vault: keys,
                peers,
            }),
            store,
            health: Arc::new(HealthGauges::default()),
            commits: Arc::new(Notify::new()),
            base,
            applied: base,
            snapshot: Snapshot::default(),
            membership,
            writer: Arc::default(),
            connected_head: None,
        },
    )
}

async fn signed(
    tail: &Tail<Positions, TestKeys>,
    sequence: u64,
    id: u64,
    writer: &str,
    body: Value,
) -> LogDelivery {
    let kind = op(&body).unwrap().to_owned();
    signed_with(
        tail.link.signer.as_ref(),
        tail.link.vault.as_ref(),
        sequence,
        (writer, 0),
        &kind,
        body,
        id,
    )
    .await
}

async fn signed_with(
    signer: &dyn CredentialResolver,
    vault: &dyn EpochVault,
    sequence: u64,
    writer: (&str, u64),
    kind: &str,
    body: Value,
    id: u64,
) -> LogDelivery {
    let bytes = fleet_record::prepare(
        signer,
        vault,
        &Header {
            kind: kind.into(),
            key: LOG_KEY.into(),
            writer: writer.0.into(),
            writer_ep: writer.1,
            entry_id: format!("{id:032x}"),
        },
        &body,
    )
    .await
    .unwrap()
    .into_bytes();
    LogDelivery { sequence, bytes }
}

fn part(index: u64, count: u64) -> Value {
    json!({"op":SNAPSHOT,"snapshot_id":"snapshot","part":index,"parts":count,"tables":{}})
}

#[tokio::test]
async fn hub_identity_log_multipart_apply_is_atomic_and_redelivery_is_inert() {
    let (_dir, mut tail) = tail(17).await;
    let first = signed(&tail, 1, 1, A, part(1, 2)).await;
    let generation = tail.store.generation().unwrap();
    assert!(!tail
        .apply(LogDelivery {
            sequence: 1,
            bytes: first.bytes.clone()
        })
        .await
        .unwrap());
    assert!(!tail
        .apply(LogDelivery {
            sequence: 1,
            bytes: first.bytes.clone()
        })
        .await
        .unwrap());
    assert_eq!(
        tail.store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        17
    );
    assert_eq!(tail.store.generation().unwrap(), generation);
    assert_eq!(
        tail.store
            .journal_log_position(&format!("{:032x}", 1))
            .unwrap(),
        None
    );

    let second = signed(&tail, 2, 2, B, part(2, 2)).await;
    assert!(tail.apply(second).await.unwrap());
    assert_eq!(
        tail.store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        19
    );
    assert_eq!(
        tail.store
            .journal_log_position(&format!("{:032x}", 1))
            .unwrap(),
        Some(18)
    );
    assert_eq!(
        tail.store
            .journal_log_position(&format!("{:032x}", 2))
            .unwrap(),
        Some(19)
    );
    let generation = tail.store.generation().unwrap();
    let commits = tail.commits.clone();
    let mut notified = std::pin::pin!(commits.notified());
    notified.as_mut().enable();
    assert!(!tail.apply(first).await.unwrap());
    assert!(futures_util::poll!(notified.as_mut()).is_pending());
    assert_eq!(tail.store.generation().unwrap(), generation);

    let conflict = signed(&tail, 1, 3, A, part(1, 2)).await;
    assert_eq!(
        tail.apply(conflict).await.unwrap_err().code,
        "hub_log_position_conflict"
    );
    let gap = signed(&tail, 4, 4, A, part(1, 1)).await;
    assert_eq!(tail.apply(gap).await.unwrap_err().code, "hub_log_gap");
    let invalid = signed(&tail, 3, 3, A, member(B)).await;
    assert_eq!(tail.apply(invalid).await.unwrap_err().code, "apply_failed");
    assert_eq!(tail.store.generation().unwrap(), generation);
    assert_eq!(
        tail.store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        19
    );
    assert!(futures_util::poll!(notified.as_mut()).is_pending());
}

fn member(author: &str) -> Value {
    json!({"op":"member.set_name","author":author,"display_name":"Fleet box"})
}

#[tokio::test]
async fn hub_identity_log_supersede_checks_the_entire_prefix_and_refuses_migrated_prefix() {
    let (_dir, mut migrated) = tail(17).await;
    let mut supersede = part(1, 1);
    supersede["supersedes_through"] = 17.into();
    let delivery = signed(&migrated, 1, 21, A, supersede).await;
    assert_eq!(
        migrated.apply(delivery).await.unwrap_err().code,
        "hub_supersede_migrated_prefix"
    );
    assert_eq!(
        migrated
            .store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        17
    );
    assert!(migrated.link.transport.reads.lock().unwrap().is_empty());

    for (writer, allowed) in [(A, true), (B, false)] {
        let (_dir, mut fresh) = tail(0).await;
        let before = signed(&fresh, 1, 31, A, member(A)).await;
        fresh
            .link
            .transport
            .entries
            .lock()
            .unwrap()
            .insert(1, before.bytes.clone());
        assert!(fresh.apply(before).await.unwrap());
        let mut supersede = part(1, 1);
        supersede["supersedes_through"] = 1.into();
        supersede["member_name"] = json!({"author":writer,"display_name":"Fleet box"});
        let delivery = signed(&fresh, 2, 32, writer, supersede).await;
        let result = fresh.apply(delivery).await;
        if allowed {
            assert!(result.unwrap());
        } else {
            assert_eq!(
                result.unwrap_err().message,
                "supersede replaces another box's entry at sequence 1"
            );
        }
        assert_eq!(*fresh.link.transport.reads.lock().unwrap(), [1]);
        assert_eq!(
            fresh
                .store
                .identity_log_status()
                .unwrap()
                .last_applied_position,
            if allowed { 2 } else { 1 }
        );
    }
}

fn key_body(box_id: &str, current: &TestKeys, history: &[&TestKeys]) -> Value {
    let key = |keys: &TestKeys| json!({"version":keys.key_version(),"pub":keys.public_hex()});
    json!({"box_id":box_id,"fingerprint":"tested-fingerprint","version":current.key_version(),
           "pub":current.public_hex(),"history":history.iter().map(|keys| key(keys)).collect::<Vec<_>>()})
}

fn removed(box_id: &str, history: &[&TestKeys]) -> Value {
    json!({"box_id":box_id,"removal_id":format!("remove-{box_id}"),"epoch":2,
           "key_history":history.iter().map(|k| json!({"version":k.key_version(),"pub":k.public_hex()})).collect::<Vec<_>>()})
}

fn put(tail: &Tail<Positions, TestKeys>, delivery: &LogDelivery) {
    tail.link
        .transport
        .entries
        .lock()
        .unwrap()
        .insert(delivery.sequence, delivery.bytes.clone());
}

#[tokio::test]
async fn hub_membership_cold_join_verifies_rotated_removed_snapshot_signer_and_stops_after_removal()
{
    let old = Arc::new(TestKeys::with_seed(11));
    let rotated = Arc::new(TestKeys::with_seed(12));
    let b = Arc::new(TestKeys::with_seed(13));
    let old_signer = ClaustrumCredentialResolver::with_target(old.clone(), 0);
    let rotated_signer = ClaustrumCredentialResolver::with_target(rotated.clone(), 0);
    let b_signer = ClaustrumCredentialResolver::with_target(b.clone(), 0);
    let (_scratch, mut tail) = tail_with(19, C, vec![anchor(B, &b)]);
    let vault = tail.link.vault.as_ref();
    let first = signed_with(&old_signer, vault, 1, (A, 0), SNAPSHOT, part(1, 1), 1).await;
    let keys = signed_with(
        &rotated_signer,
        vault,
        2,
        (A, 0),
        "member.key",
        key_body(A, &rotated, &[&old, &rotated]),
        2,
    )
    .await;
    let remove = signed_with(
        &b_signer,
        vault,
        3,
        (B, 0),
        "member.removed",
        removed(A, &[&old, &rotated]),
        3,
    )
    .await;
    let forged = signed_with(
        &old_signer,
        vault,
        4,
        (A, 0),
        "member.set_name",
        member(A),
        4,
    )
    .await;
    for entry in [&first, &keys, &remove, &forged] {
        put(&tail, entry);
    }

    let genesis = tail.store.generation().unwrap();
    assert!(tail.store.hub_anchors().unwrap().is_empty());
    tail.reconnect().await.unwrap();
    tail.update_barrier();
    assert_eq!(tail.health.hub_membership.load(Ordering::Acquire), 1);
    assert_eq!(tail.sequence(), 0);
    assert_eq!(tail.store.generation().unwrap(), genesis);
    assert_eq!(
        tail.store
            .journal_log_position(&format!("{:032x}", 1))
            .unwrap(),
        None
    );
    assert!(tail
        .store
        .hub_anchors()
        .unwrap()
        .iter()
        .all(|anchor| anchor.box_id != A));

    for entry in [first, keys, remove] {
        assert!(tail.apply(entry).await.unwrap());
    }
    let verification = tail.store.verify().unwrap();
    assert!(verification.ok, "{verification:?}");
    tail.store.rebuild().unwrap();
    assert!(tail.store.verify().unwrap().ok);
    assert!(tail
        .store
        .agent_changes(genesis, None)
        .unwrap()
        .entries
        .is_empty());
    assert_eq!(tail.membership.open_episode(A, 3), None);
    let error = tail.apply(forged).await.unwrap_err();
    assert_eq!(error.code, "member_episode_invalid");
    tail.stop(4, error);
    assert_eq!(tail.health.log_state.load(Ordering::Relaxed), 5);
    assert_eq!(
        tail.store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        22
    );
}

#[tokio::test]
async fn hub_membership_unanchored_removal_chain_stops_at_its_first_entry() {
    let a = Arc::new(TestKeys::with_seed(21));
    let b = Arc::new(TestKeys::with_seed(22));
    let (_scratch, mut tail) = tail_with(0, C, vec![]);
    let vault = tail.link.vault.as_ref();
    let a_signer = ClaustrumCredentialResolver::with_target(a.clone(), 0);
    let b_signer = ClaustrumCredentialResolver::with_target(b.clone(), 0);
    let first = signed_with(
        &a_signer,
        vault,
        1,
        (A, 0),
        "member.removed",
        removed(B, &[&b]),
        1,
    )
    .await;
    let second = signed_with(
        &b_signer,
        vault,
        2,
        (B, 0),
        "member.removed",
        removed(A, &[&a]),
        2,
    )
    .await;
    for entry in [&first, &second] {
        put(&tail, entry);
    }
    tail.reconnect().await.unwrap();
    let error = tail.apply(first).await.unwrap_err();
    assert_eq!(error.code, "signature_invalid");
    tail.stop(1, error);
    assert_eq!(
        tail.store
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        0
    );
    assert_eq!(tail.health.log_state.load(Ordering::Relaxed), 5);
}

#[tokio::test]
async fn hub_membership_holds_unknown_kv_until_its_arrival_head_is_scanned() {
    let (_scratch, tail) = tail(0).await;
    let unrelated = signed(&tail, 1, 1, A, member(A)).await;
    put(&tail, &unrelated);
    let gate = Arc::new(Notify::new());
    *tail.link.transport.hold.lock().unwrap() = Some(gate.clone());
    let impostor = Arc::new(TestKeys::with_seed(67));
    let signer = ClaustrumCredentialResolver::with_target(impostor, 0);
    let key = format!("wrapreq.{A}");
    let prepared = fleet_record::prepare(
        &signer,
        tail.link.vault.as_ref(),
        &Header {
            kind: "wrapreq".into(),
            key: key.clone(),
            writer: A.into(),
            writer_ep: 0,
            entry_id: "10000000000000000000000000000000".into(),
        },
        &json!({"v":1,"request_id":"request"}),
    )
    .await
    .unwrap();
    let record = UnverifiedRecord::parse(&key, &prepared.into_bytes()).unwrap();
    let mut intake = std::pin::pin!(tail.membership.verify_kv(
        record,
        0,
        Sight::FirstOrAfterGap,
        Instant::now() + READ_BUDGET
    ));
    assert!(futures_util::poll!(intake.as_mut()).is_pending());
    assert_eq!(*tail.link.transport.reads.lock().unwrap(), [1]);
    assert_eq!(tail.sequence(), 0);
    gate.notify_one();
    assert_eq!(intake.await.unwrap_err().code, "signature_invalid");
    assert_eq!(tail.sequence(), 0);
}

#[tokio::test]
async fn hub_membership_reconnect_resets_the_barrier_until_the_observed_head_is_applied() {
    let (_scratch, mut tail) = tail(0).await;
    tail.reconnect().await.unwrap();
    tail.update_barrier();
    assert_eq!(tail.health.hub_membership.load(Ordering::Acquire), 2);
    let delivery = signed(&tail, 1, 1, A, member(A)).await;
    put(&tail, &delivery);
    tail.connected_head = None;
    tail.update_barrier();
    assert_eq!(tail.health.hub_membership.load(Ordering::Acquire), 1);
    tail.reconnect().await.unwrap();
    tail.update_barrier();
    assert_eq!(tail.health.hub_membership.load(Ordering::Acquire), 1);
    assert!(tail.apply(delivery).await.unwrap());
    tail.update_barrier();
    assert_eq!(tail.health.hub_membership.load(Ordering::Acquire), 2);
}

#[tokio::test]
async fn hub_membership_never_serves_legacy_holder_after_activation() {
    let (_scratch, tail) = tail(0).await;
    let handler = ProjectsHandler::with_runtime("membership-read".into(), || 700);
    *handler.store.lock().unwrap() = Some(tail.store.clone());
    tail.store.apply_entry("agent.cutover", "{}", "test", None, |tx| {
        tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_16013c86','Test','','assistant',1,1)", [])?;
        tx.execute("INSERT INTO claim_cache(agent_id,holder,epoch,version,stale,integrity_failed,read_at_ms) VALUES('agent_16013c86','legacy-author',7,8,0,0,1)", [])?;
        tx.execute("INSERT INTO role_cache(role,holder,epoch,source,version,stale,integrity_failed,read_at_ms) VALUES('test','legacy-author',7,'cloud',8,0,0,1)", [])?;
        Ok(())
    }).unwrap();
    let read = || {
        let location = handler
            .agent_where(json!({"agent_id":"agent_16013c86"}))
            .unwrap();
        let role = handler.role_read(json!({"role":"test"})).unwrap();
        [location, role]
            .map(|bytes| serde_json::from_slice::<Value>(&bytes).unwrap()["result"].clone())
    };
    for value in read() {
        assert_eq!(value["holder"], "legacy-author");
    }
    for state in [1, 2] {
        handler
            .health
            .hub_membership
            .store(state, Ordering::Release);
        for value in read() {
            assert_eq!(value["stale"], true);
            assert!(value["holder"].is_null());
        }
    }
}

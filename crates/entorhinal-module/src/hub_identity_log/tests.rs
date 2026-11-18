use std::{collections::BTreeMap, path::PathBuf, sync::Mutex};

use cortexkit_claustrum_client::ClaustrumCredentialResolver;
use serde_json::json;

use super::*;
use crate::{
    fleet_record::{self, Header},
    hub_write::{tests::TestKeys, KvEntry},
};
use entorhinal_core::hub_store::HubAttemptWrite;

const A: &str = "0123456789abcdef0123456789abcdef";
const B: &str = "fedcba9876543210fedcba9876543210";

#[derive(Default)]
struct Positions {
    entries: Mutex<BTreeMap<u64, Vec<u8>>>,
    reads: Mutex<Vec<u64>>,
}

impl LogTransport for Positions {
    fn tail(self: &Arc<Self>, _: u64) -> Feed<LogDelivery> {
        unreachable!("test drives individual tail deliveries")
    }
}

impl Transport for Positions {
    async fn log_entry(&self, sequence: u64, _: Instant) -> Result<Option<Vec<u8>>, HandlerError> {
        self.reads.lock().unwrap().push(sequence);
        Ok(self.entries.lock().unwrap().get(&sequence).cloned())
    }
    async fn log_head(&self, _: Instant) -> Result<u64, HandlerError> {
        unreachable!()
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

fn tail(base: i64) -> (Scratch, Tail<Positions, TestKeys>) {
    let (path, descriptor) = crate::tests::scratch_descriptor("hub-identity-log-tail");
    let store = RegistryStore::open(&descriptor).unwrap();
    store.apply_entry("fixture", "{}", "test", None, |tx| {
        tx.execute("UPDATE identity_log_state SET state='enabled',hub_log_base=?1,last_applied_position=?1,last_seen_head=?1", [base])?;
        Ok(())
    }).unwrap();
    let keys = Arc::new(TestKeys::new());
    (
        Scratch(path),
        Tail {
            link: Arc::new(Link {
                transport: Arc::new(Positions::default()),
                trust: keys.clone(),
                signer: Arc::new(ClaustrumCredentialResolver::with_target(keys.clone(), 0)),
                vault: keys,
            }),
            store,
            health: Arc::new(HealthGauges::default()),
            commits: Arc::new(Notify::new()),
            base,
            applied: base,
            snapshot: Snapshot::default(),
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
    let bytes = fleet_record::prepare(
        tail.link.signer.as_ref(),
        tail.link.vault.as_ref(),
        &Header {
            kind: op(&body).unwrap().into(),
            key: LOG_KEY.into(),
            writer: writer.into(),
            writer_ep: 0,
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
    let (_dir, mut tail) = tail(17);
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
    let (_dir, mut migrated) = tail(17);
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
        let (_dir, mut fresh) = tail(0);
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

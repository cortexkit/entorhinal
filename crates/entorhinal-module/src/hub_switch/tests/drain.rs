use super::*;
use crate::{
    log_client::{LogConnector, LogTransport, TransportError},
    record_client::{CAS, READ_MANY},
};
use entorhinal_core::claim_cache::{CachedClaim, ClaimObservation};
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::Notify;

const CLAIM: &str = "claim:agent_0000000000000000";

fn agent(index: usize) -> String {
    format!("agent_{index:016x}")
}

fn add_agents(f: &Fixture, count: usize) {
    f.store()
        .apply_entry("test.agents", "{}", "test", None, |tx| {
            for i in 0..count {
                tx.execute(
                    "INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms)
                        VALUES(?1,?1,'tag','assistant',0,0)",
                    [agent(i)],
                )?;
            }
            Ok(())
        })
        .unwrap();
}

fn claim(id: &str, holder: &str, epoch: i64) -> Value {
    json!({"v":1,"agent_id":id,"holder":holder,"epoch":epoch,"cut_id":null,"reason":null})
}

fn error(code: &str) -> TransportError {
    TransportError::Refusal {
        code: code.into(),
        detail: None,
        message: None,
    }
}

type Hook = dyn Fn(&FakeRecord, &str, &Value) -> Result<Page, TransportError> + Send + Sync;

#[derive(Default)]
enum Page {
    #[default]
    Full,
    Prefix(usize),
    MissingLast,
}

#[derive(Clone)]
struct RecordPeer {
    inner: Arc<FakeRecord>,
    hook: Arc<Hook>,
    hold: Option<Arc<Hold>>,
}

#[derive(Default)]
struct Hold {
    entered: Notify,
    release: Notify,
    count: AtomicUsize,
}

impl RecordPeer {
    fn new(
        f: &Fixture,
        hook: impl Fn(&FakeRecord, &str, &Value) -> Result<Page, TransportError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            inner: f.records.clone(),
            hook: Arc::new(hook),
            hold: None,
        }
    }

    fn client(self, f: &Fixture) -> RecordClient {
        RecordClient::new(Arc::new(self)).fenced(LegacyFence::new(&f.slot))
    }
}

#[async_trait::async_trait]
impl LogConnector for RecordPeer {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(self.clone()))
    }
}

#[async_trait::async_trait]
impl LogTransport for RecordPeer {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        if let Some(hold) = &self.hold {
            if method == CAS
                && params["subject"] == CLAIM
                && hold.count.fetch_add(1, Ordering::Relaxed) == 0
            {
                hold.entered.notify_one();
                hold.release.notified().await;
            }
        }
        let page = (self.hook)(&self.inner, method, &params)?;
        let bytes = self.inner.call(method, params).await?;
        let mut reply: Value = match page {
            Page::Full => return Ok(bytes),
            _ => serde_json::from_slice(&bytes).unwrap(),
        };
        let entries = reply["result"]["entries"].as_array_mut().unwrap();
        match page {
            Page::Full => unreachable!(),
            Page::Prefix(n) => {
                entries.truncate(n);
                reply["result"]["next"] = json!(n);
            }
            Page::MissingLast => {
                entries.pop();
            }
        }
        Ok(serde_json::to_vec(&reply).unwrap())
    }
}

#[tokio::test]
async fn hub_switch_drain_lost_matching_fence_is_resent_and_not_counted() {
    let f = Fixture::new(false);
    add_agents(&f, 1);
    f.put(CLAIM, 4, claim(&agent(0), A, 7), A);
    f.freeze_at_marker(A, &[], &[]);
    let seen = Arc::new(Mutex::new(Vec::new()));
    let seen_by_peer = seen.clone();
    let client = RecordPeer::new(&f, move |peer, method, params| {
        if method == CAS && params["subject"] == CLAIM {
            let mut attempts = seen_by_peer.lock().unwrap();
            attempts.push(params.clone());
            if attempts.len() < 3 {
                return Err(TransportError::NoReply);
            }
            if attempts.len() == 3 {
                peer.drop_reply.store(true, Ordering::Relaxed);
            }
        }
        Ok(Page::Full)
    })
    .client(&f);

    assert_eq!(f.tick_with(&client).await.unwrap_err().code, "unavailable");
    assert_eq!(f.record_client().read(CLAIM).await.unwrap().version, 4);
    assert!(
        f.record_client()
            .read(&format!("hubdrained:{A}"))
            .await
            .unwrap()
            .absent
    );
    assert!(f.records.batches.lock().unwrap().is_empty());

    let image = f.tick_with(&client).await.unwrap().unwrap();
    assert_eq!(f.record_client().read(CLAIM).await.unwrap().version, 5);
    let attempts = seen.lock().unwrap();
    assert_eq!(attempts.len(), 4);
    assert!(attempts.iter().all(|attempt| attempt == &attempts[0]));
    assert_eq!(attempts[0]["expected_version"], 4);
    assert_eq!(
        attempts[0]["body"],
        encode_hex(&serde_json::to_vec(&claim(&agent(0), A, 7)).unwrap())
    );
    assert_eq!(image.image.claims[0].generation, 7);
}

#[tokio::test]
async fn hub_switch_drain_verify_failed_stops_before_ack() {
    let f = Fixture::new(false);
    add_agents(&f, 1);
    f.freeze_at_marker(A, &["connector_home"], &[]);
    let client = RecordPeer::new(&f, |_, method, params| {
        if method == CAS && params["subject"] == CLAIM {
            Err(error("verify_failed"))
        } else {
            Ok(Page::Full)
        }
    })
    .client(&f);
    assert_eq!(
        f.tick_with(&client).await.unwrap_err().code,
        "verify_failed"
    );
    for subject in [CLAIM, "role:connector_home", &format!("hubdrained:{A}")] {
        assert!(f.record_client().read(subject).await.unwrap().absent);
    }
    assert!(f.records.batches.lock().unwrap().is_empty());
}

#[tokio::test]
async fn hub_switch_image_restarts_every_batch_after_cloud_loss_or_bad_entry() {
    let f = Fixture::new(false);
    add_agents(&f, 130);
    for i in 0..130 {
        f.put(&format!("claim:{}", agent(i)), 1, claim(&agent(i), A, 5), A);
    }
    f.freeze_at_marker(A, &[], &[]);
    let fault = Arc::new(AtomicUsize::new(0));
    let fault_by_peer = fault.clone();
    let last = format!("claim:{}", agent(128));
    let last_by_peer = last.clone();
    let client = RecordPeer::new(&f, move |peer, method, params| {
        if method == READ_MANY {
            if params["subjects"][0] == CLAIM {
                return Ok(Page::Prefix(40));
            }
            if params["subjects"][0] == last_by_peer {
                match fault_by_peer.load(Ordering::Relaxed) {
                    0 => peer.offline.store(true, Ordering::Relaxed),
                    1 => peer.error(&last_by_peer, Some("decrypt_failed")),
                    2 => return Ok(Page::MissingLast),
                    _ => {}
                }
            }
        }
        Ok(Page::Full)
    })
    .client(&f);

    let batches = || {
        let batches = std::mem::take(&mut *f.records.batches.lock().unwrap());
        assert_eq!(
            batches.iter().map(Vec::len).collect::<Vec<_>>(),
            [64, 24, 64, 2]
        );
        for (batch, first) in batches.iter().zip([0, 40, 64, 128]) {
            assert_eq!(batch[0], format!("claim:{}", agent(first)));
        }
    };
    for (mode, code) in ["engram_unavailable", "decrypt_failed", "record_protocol"]
        .into_iter()
        .enumerate()
    {
        fault.store(mode, Ordering::Relaxed);
        assert_eq!(f.tick_with(&client).await.unwrap_err().code, code);
        batches();
        f.records.offline.store(false, Ordering::Relaxed);
        f.records.error(&last, None);
        assert!(f.store().hub_log_base().unwrap().is_none());
        assert!(f.store().cached_agent_claim(&agent(129)).unwrap().is_none());
    }
    fault.store(3, Ordering::Relaxed);
    let image = f.tick_with(&client).await.unwrap().unwrap();
    batches();
    assert_eq!(image.image.claims.len(), 130);
    assert!(image.image.claims.iter().all(|claim| claim.generation == 5));
    assert_eq!(
        f.records.cas_attempts.lock().unwrap().len(),
        131,
        "image retries must not repeat a completed drain"
    );
}

#[tokio::test]
async fn hub_switch_image_uses_post_fence_record_not_stale_cache_or_late_legacy_cas() {
    let f = Fixture::new(false);
    add_agents(&f, 1);
    f.put(CLAIM, 4, claim(&agent(0), B, 4), B);
    f.store()
        .cache_agent_claims(
            &[(
                agent(0),
                ClaimObservation::Present {
                    claim: CachedClaim {
                        holder: Some(B.into()),
                        epoch: 4,
                        cut_id: None,
                        version: 4,
                        read_at_ms: 1,
                        stale: true,
                        integrity_failed: false,
                    },
                    self_author: A.into(),
                },
            )],
            1,
        )
        .unwrap();
    let mut peer = Fixture::for_author(false, [0x22; 16]);
    peer.records = f.records.for_author([0x22; 16]);
    let held = |f: &Fixture| {
        let hold = Arc::new(Hold::default());
        let mut transport = RecordPeer::new(f, |_, _, _| Ok(Page::Full));
        transport.hold = Some(hold.clone());
        (transport.client(f), hold)
    };
    let (first, first_hold) = held(&peer);
    let (late, late_hold) = held(&peer);
    let (fencing, fence_hold) = held(&f);
    let request = |id, epoch| CasRequest {
        subject: CLAIM.into(),
        expected_version: 4,
        body: serde_json::to_vec(&claim(&agent(0), A, epoch)).unwrap(),
        write_id: [id; 16],
    };
    let early_request = request(6, 6);
    let late_request = request(9, 9);
    let start_drain = Notify::new();
    let early_landed = Notify::new();
    let ((), late_result, image, ()) = tokio::join!(
        async {
            assert_eq!(first.cas(&early_request).await.unwrap(), 5);
            early_landed.notify_one();
        },
        late.cas(&late_request),
        async {
            start_drain.notified().await;
            let image = f.tick_with(&fencing).await.unwrap().unwrap();
            late_hold.release.notify_one();
            image
        },
        async {
            first_hold.entered.notified().await;
            late_hold.entered.notified().await;
            f.freeze_at_marker(A, &[], &[]);
            start_drain.notify_one();
            fence_hold.entered.notified().await;
            first_hold.release.notify_one();
            early_landed.notified().await;
            fence_hold.release.notify_one();
        },
    );
    assert_eq!(late_result.unwrap_err().code(), "version_moved");
    let attempts = f.records.cas_attempts.lock().unwrap();
    assert_eq!(attempts[0]["expected_version"], 4);
    assert_eq!(attempts[1]["expected_version"], 5);
    assert_ne!(attempts[0]["write_id"], attempts[1]["write_id"]);
    assert_eq!(image.image.claims[0].generation, 6);
    assert_eq!(image.image.claims[0].holder.as_deref(), Some(BOX_A));
    assert_eq!(
        f.store()
            .cached_agent_claim(&agent(0))
            .unwrap()
            .unwrap()
            .epoch,
        4
    );
}

#[tokio::test]
async fn hub_switch_image_maps_holders_and_shared_authors_using_latest_member_box() {
    const C: &str = "33333333333333333333333333333333";
    const D: &str = "44444444444444444444444444444444";
    let f = Fixture::new(false);
    for (author, box_id) in [(B, BOX_A), (A, BOX_A), (C, BOX_B)] {
        f.apply_verified(
            author,
            json!({"op":"member.set_box","author":author,"box_id":box_id}),
        );
    }
    // Write B's name later than A's: the tie is ordered by member_box, not
    // by the name's own log position or by author bytes.
    for (author, name) in [
        (A, "Current"),
        (B, "Previous"),
        (C, "Excluded"),
        (D, "Unmapped"),
    ] {
        f.apply_verified(
            author,
            json!({"op":"member.set_name","author":author,"display_name":name}),
        );
    }
    f.store()
        .apply_entry("test.placement", "{}", "test", None, |tx| {
            tx.execute("INSERT INTO placement_state VALUES(1,42,?1)", [B])?;
            Ok(())
        })
        .unwrap();
    add_agents(&f, 4);
    f.store()
        .apply_entry("test.retire", "{}", "test", None, |tx| {
            tx.execute(
                "UPDATE agent SET terminal_reason='retired', terminal_at_ms=1 WHERE agent_id=?1",
                [agent(2)],
            )?;
            Ok(())
        })
        .unwrap();
    for (i, holder) in [B, C, D].into_iter().enumerate() {
        f.put(
            &format!("claim:{}", agent(i)),
            1,
            claim(&agent(i), holder, 12),
            holder,
        );
    }
    f.put(
        "role:connector_home",
        7,
        json!({"v":1,"role":"connector_home","holder":C,"epoch":9,"reason":"take"}),
        C,
    );
    f.freeze_at_marker(B, &["connector_home"], &[BOX_B]);
    let image = f.tick().await.unwrap().unwrap();
    assert_eq!(
        serde_json::to_value(&image.image).unwrap(),
        json!({
            "claims":[
                {"agent_id":agent(0),"generation":12,"holder":BOX_A},
                {"agent_id":agent(1),"generation":12,"holder":format!("engram:{C}")},
                {"agent_id":agent(2),"generation":12,"holder":format!("engram:{D}")},
                {"agent_id":agent(3),"generation":0,"holder":null}
            ],
            "roles":[{"role":"connector_home","generation":9,"holder":format!("engram:{C}")}]
        })
    );
    assert_eq!(image.tables["placement_state"][0]["author"], BOX_A);
    assert_eq!(
        serde_json::to_value(&image.tables["member_name"]).unwrap(),
        json!([
            {"author":BOX_A,"display_name":"Current"},
            {"author":format!("engram:{C}"),"display_name":"Excluded"},
            {"author":format!("engram:{D}"),"display_name":"Unmapped"}
        ])
    );
    assert!(!image.tables.contains_key("member_box"));
    assert_eq!(f.store().switch_members().unwrap().len(), 3);
    let terminal = f
        .record_client()
        .read(&format!("claim:{}", agent(2)))
        .await
        .unwrap();
    assert_eq!(terminal.version, 2);
    let absent = f
        .record_client()
        .read(&format!("claim:{}", agent(3)))
        .await
        .unwrap();
    assert_eq!(absent.version, 1);
    assert_eq!(
        crate::claim_codec::AgentClaim::decode(&agent(3), &absent.body.unwrap())
            .unwrap()
            .epoch,
        0
    );
}

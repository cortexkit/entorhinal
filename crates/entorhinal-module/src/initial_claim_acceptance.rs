//! Initial ownership is established through real refresh and registration paths,
//! with independent SQLite stores sharing the record peer's cloud.

use super::*;
use crate::{
    fake_record::{FakeRecord, FakeRecordConnector},
    record_client::{RecordClient, CAS},
    sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
};
use entorhinal_core::SessionRegistration;

const AGENT: &str = "agent_0123456789abcdef";

struct Pair {
    a: Machine,
    b: Machine,
    ar: Arc<FakeRecord>,
    br: Arc<FakeRecord>,
}

impl Pair {
    fn new(state: &str) -> Self {
        let log = FakeLog::default();
        let ar = FakeRecord::new([1; 16]);
        let br = ar.for_author([2; 16]);
        let mut machines = Vec::new();
        for (author, records) in [(1, ar.clone()), (2, br.clone())] {
            let mut machine = Machine::new_with_record(
                "initial-claim",
                Arc::new(log.with_author([author; 16])),
                RecordClient::new(Arc::new(FakeRecordConnector(records))),
            );
            let resolver = FakeResolver::new();
            resolver.set(
                ORCHESTRATOR_CAPABILITY,
                ProviderResolution::Provider(WRITER_MODULE.into()),
            );
            machine.handler.provider_resolver = resolver;
            machine.store().apply_entry("agent.cutover", "{}", "fixture", None, |tx| {
                tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES(?1,'Ada','A','assistant',1,1)", [AGENT])?;
                tx.execute("UPDATE identity_log_state SET state=?1", [state])?;
                Ok(())
            }).unwrap();
            machines.push(machine);
        }
        let b = machines.pop().unwrap();
        let a = machines.pop().unwrap();
        Self { a, b, ar, br }
    }

    fn bind(machine: &Machine, agent: &str, session: &str) {
        machine
            .store()
            .register_session(&SessionRegistration {
                role: None,
                agent_id: agent,
                ref_kind: "pi-session",
                session_ref: session,
                harness: "pi",
                session_id: session,
                project_root: Some("/project"),
                registered_by: "orchestrator",
                runner: Some("pi"),
                network: None,
                now_ms: 1,
            })
            .unwrap();
    }

    async fn refresh(machine: &Machine) {
        drive(machine.handler.wait_claim_refresh()).await.unwrap();
    }

    async fn claim(&self) -> crate::record_client::RecordReply {
        RecordClient::new(Arc::new(FakeRecordConnector(self.ar.clone())))
            .read(&format!("claim:{AGENT}"))
            .await
            .unwrap()
    }
}

#[tokio::test(start_paused = true)]
async fn initial_claim_registration_signals_refresh_and_replicates_location() {
    let p = Pair::new("enabled");
    Pair::refresh(&p.a).await;
    assert_eq!(p.a.store().agent_where(AGENT).unwrap().status, "unclaimed");
    let (sender, mut notices) = tokio::sync::mpsc::unbounded_channel();
    let sink = ClaimWatch {
        sender,
        cancel: tokio::sync::watch::channel(false).0,
    };
    let body =
        serde_json::to_vec(&json!({"method":"agent.where_watch","params":{"agentIds":[AGENT]}}))
            .unwrap();
    let watch = p.a.handler.serve_watch(&sink, &body, CORE);
    tokio::pin!(watch);
    poll_watch(watch.as_mut()).await;
    let initial: Value = serde_json::from_slice(&notices.try_recv().unwrap()).unwrap();
    assert_eq!(initial["kind"], "resync");
    let completed = p.a.handler.health.claim_refresh_done.notified();
    tokio::pin!(completed);
    completed.as_mut().enable();
    p.a.call(
        "agent.register_session",
        json!({
            "agent_id": AGENT, "ref_kind":"pi-session", "ref":"session", "harness":"pi",
            "session_id":"session", "runner":"pi", "project_root":"/project"
        }),
    )
    .await
    .unwrap();
    drive(completed).await;
    poll_watch(watch.as_mut()).await;
    tokio::time::advance(Duration::from_millis(200)).await;
    poll_watch(watch.as_mut()).await;
    let updated: Value = serde_json::from_slice(&notices.try_recv().unwrap()).unwrap();
    assert_eq!(updated["where"][0]["status"], "held_here");
    assert_eq!(updated["where"][0]["epoch"], 1);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
    let here =
        p.a.call("agent.where", json!({"agent_id":AGENT}))
            .await
            .unwrap();
    assert_eq!(here["status"], "held_here");
    assert_eq!(here["epoch"], 1);
    assert_eq!(here["holder"], "01".repeat(16));
    assert_eq!(here["cut_id"], Value::Null);
    let many =
        p.a.call("agent.where_many", json!({"agentIds":[AGENT]}))
            .await
            .unwrap();
    assert_eq!(many["agents"][0]["status"], "held_here");
    let residence =
        p.a.call(
            "agent.session_residence",
            json!({"runner":"pi","session":"session"}),
        )
        .await
        .unwrap();
    assert_eq!(residence["state"], "here");
    Pair::refresh(&p.b).await;
    let away =
        p.b.call("agent.where", json!({"agent_id":AGENT}))
            .await
            .unwrap();
    assert_eq!(away["status"], "held_elsewhere");
    assert_eq!(away["epoch"], 1);
    assert_eq!(away["holder"], "01".repeat(16));
    let record = p.claim().await;
    assert_eq!(record.version, 1);
    let claim: Value = serde_json::from_slice(record.body.as_deref().unwrap()).unwrap();
    assert_eq!(
        claim,
        json!({"v":1,"agent_id":AGENT,"holder":"01".repeat(16),"epoch":1,"cut_id":null,"reason":null})
    );
    assert!(p.br.cas_attempts.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn initial_claim_replaces_present_unheld_epoch_zero_using_fresh_version() {
    let p = Pair::new("enabled");
    Pair::bind(&p.a, AGENT, "ended");
    p.a.store().end_session("pi", "ended", None, 2).unwrap();
    p.ar.put(
        &format!("claim:{AGENT}"),
        7,
        serde_json::to_vec(
            &json!({"v":1,"agent_id":AGENT,"holder":null,"epoch":0,"cut_id":null,"reason":null}),
        )
        .unwrap(),
        &p.br.author,
    );
    Pair::refresh(&p.a).await;
    assert_eq!(p.a.store().agent_where(AGENT).unwrap().status, "held_here");
    let claim = p.claim().await;
    assert_eq!(claim.version, 8);
    assert_eq!(p.ar.cas_attempts.lock().unwrap()[0]["expected_version"], 7);
}

#[tokio::test(start_paused = true)]
async fn initial_claim_skips_unbound_terminal_remote_and_already_held_agents() {
    for situation in [
        "unbound",
        "retired",
        "merged",
        "remote",
        "held",
        "zero_with_holder",
        "zero_with_other_holder",
    ] {
        let p = Pair::new("enabled");
        if situation != "unbound" {
            Pair::bind(&p.a, AGENT, "session");
        }
        p.a.store()
            .apply_entry("fixture", "{}", "fixture", None, |tx| {
                match situation {
                    "retired" | "merged" => {
                        if situation == "merged" {
                        tx.execute("INSERT INTO agent(agent_id,name,tag,role,created_at_ms,updated_at_ms) VALUES('agent_0123456789abcdee','Grace','G','assistant',1,1)", [])?;
                    }
                    tx.execute("UPDATE agent SET terminal_reason=?1,terminal_at_ms=2,merged_into=?2 WHERE agent_id=?3", (situation, (situation == "merged").then_some("agent_0123456789abcdee"), AGENT))?;
                    }
                    "remote" => {
                        tx.execute("UPDATE session_binding SET held_on=?1", [&p.br.author])?;
                    }
                    _ => {}
                }
                Ok(())
            })
            .unwrap();
        if ["held", "zero_with_holder", "zero_with_other_holder"].contains(&situation) {
            p.ar.put(&format!("claim:{AGENT}"), 4, serde_json::to_vec(&json!({
                "v":1,"agent_id":AGENT,"holder":if situation=="zero_with_other_holder" { &p.br.author } else { &p.ar.author },"epoch":if situation=="held" { 3 } else { 0 },"cut_id":null,"reason":null
            })).unwrap(), &p.ar.author);
        }
        Pair::refresh(&p.a).await;
        assert!(p.ar.cas_attempts.lock().unwrap().is_empty(), "{situation}");
        assert_eq!(
            p.a.handler.health().await.metrics.unwrap()["claimConflicts"],
            u64::from(situation == "zero_with_other_holder"),
            "{situation}"
        );
    }
}

struct RacingPeer {
    peer: Arc<FakeRecord>,
    barrier: Arc<tokio::sync::Barrier>,
}

#[async_trait::async_trait]
impl LogConnector for RacingPeer {
    async fn connect(&self) -> Result<Arc<dyn LogTransport>, TransportError> {
        Ok(Arc::new(Self {
            peer: self.peer.clone(),
            barrier: self.barrier.clone(),
        }))
    }
}

#[async_trait::async_trait]
impl LogTransport for RacingPeer {
    async fn call(&self, method: &str, params: Value) -> Result<Vec<u8>, TransportError> {
        if method == CAS {
            self.barrier.wait().await;
        }
        self.peer.call(method, params).await
    }
}

#[tokio::test(start_paused = true)]
async fn initial_claim_race_lands_once_and_counts_loser_once() {
    let mut p = Pair::new("enabled");
    Pair::bind(&p.a, AGENT, "a-session");
    Pair::bind(&p.b, AGENT, "b-session");
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    p.a.handler.record_client = RecordClient::new(Arc::new(RacingPeer {
        peer: p.ar.clone(),
        barrier: barrier.clone(),
    }));
    p.b.handler.record_client = RecordClient::new(Arc::new(RacingPeer {
        peer: p.br.clone(),
        barrier,
    }));
    tokio::join!(Pair::refresh(&p.a), Pair::refresh(&p.b));
    let record = p.claim().await;
    assert_eq!(record.version, 1);
    let holder = record.author.unwrap();
    let (winner, loser) = if holder == p.ar.author {
        (&p.a, &p.b)
    } else {
        (&p.b, &p.a)
    };
    assert_eq!(
        winner.store().agent_where(AGENT).unwrap().status,
        "held_here"
    );
    assert_eq!(
        loser.store().agent_where(AGENT).unwrap().status,
        "held_elsewhere"
    );
    assert_eq!(
        winner.handler.health().await.metrics.unwrap()["claimConflicts"],
        0
    );
    assert_eq!(
        loser.handler.health().await.metrics.unwrap()["claimConflicts"],
        1
    );
    Pair::refresh(loser).await;
    assert_eq!(
        loser.handler.health().await.metrics.unwrap()["claimConflicts"],
        1
    );
    assert_eq!(p.claim().await.version, 1);
    assert_eq!(p.ar.cas_attempts.lock().unwrap().len(), 1);
    assert_eq!(p.br.cas_attempts.lock().unwrap().len(), 1);
}

#[tokio::test(start_paused = true)]
async fn initial_claim_disabled_and_transitional_sharing_write_nothing() {
    for state in ["disabled", "enabling", "joining"] {
        let p = Pair::new(state);
        Pair::bind(&p.a, AGENT, "session");
        Pair::refresh(&p.a).await;
        assert_eq!(p.ar.calls.load(Ordering::Relaxed), 0, "{state}");
        assert!(p.ar.cas_attempts.lock().unwrap().is_empty());
        assert_eq!(p.a.store().agent_where(AGENT).unwrap().epoch, 0);
    }
}

#[tokio::test(start_paused = true)]
async fn initial_claim_lost_cas_reply_settles_the_same_write() {
    let p = Pair::new("enabled");
    Pair::bind(&p.a, AGENT, "session");
    p.ar.drop_reply.store(true, Ordering::Relaxed);
    *p.ar.after_drop.lock().unwrap() = Some(Box::new(|peer| {
        // Lose both the first reply and its receipt replay, forcing read-back
        // settlement instead of relying on a successful CAS retry response.
        peer.drop_reply.store(true, Ordering::Relaxed);
    }));
    Pair::refresh(&p.a).await;
    assert_eq!(p.a.store().agent_where(AGENT).unwrap().status, "held_here");
    assert_eq!(p.claim().await.version, 1);
    let attempts = p.ar.cas_attempts.lock().unwrap();
    assert_eq!(attempts.len(), 2);
    assert_eq!(
        attempts[0], attempts[1],
        "lost replies must not create a second write attempt"
    );
}

#[tokio::test(start_paused = true)]
async fn initial_claim_ended_local_session_is_evidence_but_not_an_open_conflict() {
    let p = Pair::new("enabled");
    Pair::bind(&p.a, AGENT, "ended");
    p.a.store().end_session("pi", "ended", None, 2).unwrap();
    Pair::refresh(&p.a).await;
    assert_eq!(p.a.store().agent_where(AGENT).unwrap().status, "held_here");
    Pair::bind(&p.b, AGENT, "ended-elsewhere");
    p.b.store()
        .end_session("pi", "ended-elsewhere", None, 2)
        .unwrap();
    Pair::refresh(&p.b).await;
    assert_eq!(
        p.b.store().agent_where(AGENT).unwrap().status,
        "held_elsewhere"
    );
    assert_eq!(
        p.b.handler.health().await.metrics.unwrap()["claimConflicts"],
        0
    );
    assert_eq!(p.claim().await.version, 1);
}

struct ClaimWatch {
    sender: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
    cancel: tokio::sync::watch::Sender<bool>,
}

#[async_trait::async_trait]
impl crate::watches::WatchTransport for ClaimWatch {
    async fn emit(&self, body: Vec<u8>) -> Result<(), HandlerError> {
        self.sender.send(body).unwrap();
        Ok(())
    }
    async fn ended(&self) {
        let _ = self.cancel.subscribe().wait_for(|cancel| *cancel).await;
    }
}

async fn poll_watch(mut watch: std::pin::Pin<&mut impl Future>) {
    std::future::poll_fn(|cx| {
        assert!(watch.as_mut().poll(cx).is_pending());
        std::task::Poll::Ready(())
    })
    .await;
}

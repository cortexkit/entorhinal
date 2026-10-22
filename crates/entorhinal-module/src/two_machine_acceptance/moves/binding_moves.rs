//! Moves that carry an agent's session bindings and their network history
//! through entorhinal's own session-bindings participant.
use super::*;
use crate::session_move::SourceFault;
use entorhinal_core::{NetworkStamp, SESSION_RETENTION_MS};

const KIND: &str = "broca-session";
const RUNNER: &str = "broca";
const HARNESS: &str = "broca";
/// The sessions the moving agent has: two live, one ended and one whose
/// network history is `yes` then `no`.
const SESSIONS: [&str; 4] = ["live-1", "live-2", "ended", "history"];

/// Route this Mac's entorhinal participant through the production
/// dispatcher, which serves the package closure and the session bindings,
/// and list it in discovery beside the fake Broca participant.
fn carry_bindings(mac: &Mac, fault: SourceFault) {
    mac.discovery.daemon.restart(snapshot(
        &[("broca", KIND), ("entorhinal", "session-bindings")],
        true,
        true,
    ));
    *mac.discovery.catalog.lock().unwrap() =
        Some(Arc::new(mac.machine.handler.catalog_move_context()));
    *mac.discovery.bindings.lock().unwrap() = Some(Arc::new(
        mac.machine.handler.session_move_context().with_fault(fault),
    ));
}

/// Tell the liveness mirror these sessions are gone, so a move may take them.
fn sessions_gone(mac: &Mac) {
    let mut state = mac.machine.handler.liveness.lock().unwrap();
    state.stale = false;
    for session in SESSIONS {
        state.gone.insert(crate::SessionKey::new(HARNESS, session));
    }
}

fn session_ref(session: &str) -> String {
    format!("ref-{session}")
}

/// Register the four sessions on the source as a Broca host would, and let
/// the fake Broca participant hold them.
fn register_sessions(pair: &Pair) {
    for session in SESSIONS {
        let reference = session_ref(session);
        pair.a
            .store()
            .register_session(&SessionRegistration {
                agent_id: AGENT,
                ref_kind: KIND,
                session_ref: &reference,
                harness: HARNESS,
                session_id: session,
                project_root: Some("/work/project"),
                registered_by: "host",
                runner: Some(RUNNER),
                network: (session == "history").then_some(NetworkStamp {
                    network: "yes",
                    clock,
                }),
                now_ms: clock(),
            })
            .unwrap();
        pair.a.discovery.peers["broca"].hold(AgentRef::new(KIND, reference));
        elapse(1000);
    }
    pair.a
        .store()
        .set_session_network(HARNESS, "history", "no", clock)
        .unwrap();
    elapse(1000);
    pair.a
        .store()
        .end_session(HARNESS, "ended", Some(RUNNER), clock())
        .unwrap();
}

async fn bindings_pair(fault: SourceFault) -> Pair {
    let pair = Pair::new(&[("broca", KIND)]).await;
    carry_bindings(&pair.a, fault);
    carry_bindings(&pair.b, SourceFault::default());
    register_sessions(&pair);
    sessions_gone(&pair.a);
    sessions_gone(&pair.b);
    pair
}

/// What the apps and Thalamus read for the agent on one Mac.
async fn session_reads(mac: &Mac) -> (Value, Vec<Value>) {
    let sessions = mac
        .machine
        .call("agent.sessions", json!({"agent_id":AGENT}))
        .await
        .unwrap();
    // A binding the receiver installed as a placeholder has no runner, so
    // its lookup by runner reads as the refusal.
    let mut by_runner = Vec::new();
    for session in SESSIONS {
        by_runner.push(
            mac.machine
                .call(
                    "agent.session_for_runner",
                    json!({"runner":RUNNER,"session":session}),
                )
                .await
                .unwrap_or_else(|error| json!({"error":error})),
        );
    }
    (sessions, by_runner)
}

fn entorhinal_refs(record: &MoveRecord) -> Option<Vec<(String, String)>> {
    record
        .participants
        .iter()
        .find(|p| p.module_id == "entorhinal")
        .map(|p| {
            p.refs
                .iter()
                .map(|r| (r.ref_kind.clone(), r.reference.clone()))
                .collect()
        })
}

fn claim_taken(mac: &Mac) -> bool {
    mac.records
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(method, p)| method == "record.cas" && p["subject"] == format!("claim:{AGENT}"))
}

#[tokio::test(start_paused = true)]
async fn moved_bindings_and_network_history_read_the_same_on_both_macs_and_back() {
    let pair = bindings_pair(SourceFault::default()).await;
    let before = session_reads(&pair.a).await;
    let history = &before.1[3]["network_history"];
    assert_eq!(history.as_array().unwrap().len(), 2, "{history}");
    assert_eq!(history[0]["network"], "yes");
    assert_eq!(history[1]["network"], "no");
    assert!(before.1[2]["ended_at_ms"].is_i64());

    let cut = pair.published("bindings-out").await;
    assert_eq!(
        entorhinal_refs(&pair.a.record(&cut).await),
        Some(vec![("session-bindings".into(), AGENT.into())])
    );
    pair.finish(&cut).await;
    assert_eq!(session_reads(&pair.b).await, before);
    // The fixture's placeholder binding travels too, so the receiver holds
    // all five of the source's rows, every one installed as it was.
    let health = &pair.b.machine.handler.health;
    assert_eq!(
        health
            .session_bindings_installed
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );
    assert_eq!(
        pair.a
            .machine
            .handler
            .health
            .session_bindings_carried
            .load(std::sync::atomic::Ordering::Relaxed),
        5
    );
    assert_eq!(
        pair.b.machine.rows("session_network"),
        pair.a.machine.rows("session_network")
    );

    // Back to the first Mac in a new cut and stream. Its own rows are kept
    // as they are, so every read is still the same.
    pair.b
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    let back = pair.b.admit_to(A, "bindings-back").await.unwrap();
    pair.b.source_until(&back, MoveStep::Published).await;
    let item = |cut: &str, mac: &Mac| -> Vec<String> {
        mac.door
            .session_calls()
            .into_iter()
            .filter(|(m, p)| m == "session.admit" && p["sessionRef"].is_string())
            .map(|(_, p)| p["sessionRef"].as_str().unwrap().to_owned())
            .filter(|r| {
                r.as_str()
                    == &entorhinal_catalog::sha256_hex(
                        format!("session-bindings:{AGENT}:{cut}").as_bytes(),
                    )[..32]
            })
            .collect()
    };
    assert!(!item(&back, &pair.b).is_empty());
    assert_ne!(item(&back, &pair.b), item(&cut, &pair.a));
    pair.a.pickup().await;
    pair.a.receiver_until(&back, MoveStep::Activated).await;
    drive(CutAccept::new(pair.a.worker(), back.clone()).tick())
        .await
        .unwrap();
    pair.b.source_until(&back, MoveStep::Released).await;
    pair.a.receiver_until(&back, MoveStep::Done).await;
    assert_eq!(session_reads(&pair.a).await, before);
    assert_eq!(session_reads(&pair.b).await, before);
    // Ended bindings are kept for the retention window like any other.
    assert_eq!(
        pair.b
            .store()
            .prune_ended_sessions(clock() + SESSION_RETENTION_MS - 10_000)
            .unwrap(),
        0
    );
}

/// An agent with no bindings and no catalog pins leaves entorhinal out of
/// the move, as before bindings were carried, and opens no stream of its
/// own, even though entorhinal is listed as a participant.
#[tokio::test(start_paused = true)]
async fn plain_agent_moves_without_entorhinal_or_a_new_stream() {
    let pair = Pair::new(&[]).await;
    for mac in [&pair.a, &pair.b] {
        carry_bindings(mac, SourceFault::default());
        mac.discovery
            .daemon
            .restart(snapshot(&[("entorhinal", "session-bindings")], true, true));
    }
    let cut = pair.a.admit_to(B, "plain").await.unwrap();
    pair.a.source_until(&cut, MoveStep::Exported).await;
    let row = pair.a.row(&cut);
    assert_eq!(row.plan["participants"], json!([]));
    assert!(row.plan.get("pin_digest").is_none());
    assert!(pair.a.door.session_calls().is_empty());
}

#[tokio::test(start_paused = true)]
async fn colliding_binding_lands_as_a_placeholder_and_the_move_completes() {
    let pair = bindings_pair(SourceFault::default()).await;
    // Another agent's binding on the receiver already holds Broca session
    // live-1.
    pair.b
        .store()
        .apply_entry("fixture", "{}", "fixture", None, |tx| {
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
                 VALUES(?1,'other-ref','agent_1111111111111111',?2,'live-1','','host',5)",
                (KIND, HARNESS),
            )?;
            Ok(())
        })
        .unwrap();
    let before = session_reads(&pair.a).await;
    let cut = pair.published("collide").await;
    pair.finish(&cut).await;
    assert_eq!(pair.b.row(&cut).step, MoveStep::Done);
    let placeholder = pair
        .b
        .store()
        .session_bindings_for_refs(&[(KIND, "ref-live-1")])
        .unwrap()
        .remove(0)
        .unwrap();
    assert_eq!(placeholder.agent_id, AGENT);
    assert_eq!((placeholder.harness, placeholder.session_id), (None, None));
    let health = &pair.b.machine.handler.health;
    assert_eq!(
        health
            .session_bindings_placeholders
            .load(std::sync::atomic::Ordering::Relaxed),
        1
    );
    // Every other binding arrived whole.
    let after = session_reads(&pair.b).await;
    assert_eq!(after.1[1..], before.1[1..]);
}

#[tokio::test(start_paused = true)]
async fn unknown_bindings_format_is_refused_before_the_claim() {
    let pair = bindings_pair(SourceFault {
        magic: Some(b"ck-session-bindings/v2\n"),
        extra: None,
    })
    .await;
    let cut = pair.published("future-format").await;
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    let row = pair.b.row(&cut);
    assert_eq!(row.reason.as_deref(), Some("incompatible_version"));
    assert!(!claim_taken(&pair.b), "receiver took the claim");
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
}

fn stranger() -> entorhinal_core::session_carry::CarriedBinding {
    entorhinal_core::session_carry::CarriedBinding {
        ref_kind: KIND.into(),
        session_ref: "ref-stranger".into(),
        runner: None,
        harness: None,
        session_id: None,
        project_root: String::new(),
        registered_by: "move".into(),
        registered_at_ms: 1,
        ended_at_ms: None,
        network_history: vec![],
    }
}

#[tokio::test(start_paused = true)]
async fn carried_ref_outside_the_move_holds_as_a_contract_violation_before_the_claim() {
    let pair = bindings_pair(SourceFault {
        magic: None,
        extra: Some(stranger),
    })
    .await;
    let cut = pair.published("stranger").await;
    pair.b.pickup().await;
    let mut receiver = pair.b.receiver(&cut);
    for _ in 0..6 {
        drive(receiver.tick()).await.unwrap();
        elapse(300_000);
    }
    let row = pair.b.row(&cut);
    assert_eq!(row.step, MoveStep::PickedUp);
    let violation = &row.plan["_receiver"]["contract_violation"];
    assert_eq!(violation["ref"], "ref-stranger", "{violation}");
    assert!(!claim_taken(&pair.b), "receiver took the claim");
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
}

/// A receiving Mac whose entorhinal predates carried bindings describes only
/// the package closure. It refuses the move in its preconditions, before
/// any import or claim, and writes no binding.
#[tokio::test(start_paused = true)]
async fn older_receiver_refuses_the_bindings_stream_before_writing_anything() {
    let pair = bindings_pair(SourceFault::default()).await;
    *pair.b.discovery.bindings.lock().unwrap() = None;
    let cut = pair.published("older").await;
    pair.b.pickup().await;
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    let row = pair.b.row(&cut);
    assert_eq!(row.reason.as_deref(), Some("receiver_precondition"));
    assert_eq!(
        row.detail.as_ref().unwrap()["code"],
        "participant_unavailable"
    );
    assert_eq!(row.detail.as_ref().unwrap()["ref_kind"], "session-bindings");
    assert!(!claim_taken(&pair.b), "receiver took the claim");
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
    assert!(!pair.b.discovery.peers["broca"]
        .calls()
        .iter()
        .any(|r| r.op().method() == "agent_data.import"));
}

/// A receiver on an older build than this one keeps retrying the abort its
/// own entorhinal refuses. Once it runs this build, the stuck cut finishes
/// aborting on the next pass.
#[tokio::test(start_paused = true)]
async fn receiver_stuck_aborting_on_an_older_build_finishes_after_upgrading() {
    let pair = bindings_pair(SourceFault::default()).await;
    let cut = pair.published("stuck").await;
    pair.b.pickup().await;
    // What the older build leaves: its preconditions refused the move, and
    // its cleanup then sent its own entorhinal an abort it refused, which it
    // keeps resending.
    let mut row = pair.b.row(&cut);
    let refused = json!({"module_id":"entorhinal","op":"agent_data.abort","code":"invalid_request","detail":null});
    row.reason = Some("participant_refused".into());
    row.detail = Some(refused.clone());
    row.abort_requested = Some(json!({"reason":"participant_refused","detail":refused}));
    row.plan["_receiver"] =
        json!({"skipped_absent":[],"pending":{"module":"entorhinal","op":"agent_data.abort"}});
    assert!(pair.b.store().update_agent_move(&row).unwrap());
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    let row = pair.b.row(&cut);
    assert!(row.hold.is_none(), "{:?}", row.hold);
    assert_eq!(pair.a.record(&cut).await.state, MoveState::Aborted);
    assert!(!claim_taken(&pair.b), "receiver took the claim");
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
}

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

/// Route this machine's entorhinal participant through the production
/// dispatcher, which serves the package closure and the session bindings,
/// and list it in discovery beside the fake Broca participant.
fn carry_bindings(node: &MoveMachine, fault: SourceFault) {
    node.discovery.daemon.restart(snapshot(
        &[("broca", KIND), ("entorhinal", "session-bindings")],
        true,
        true,
    ));
    *node.discovery.catalog.lock().unwrap() =
        Some(Arc::new(node.machine.handler.catalog_move_context()));
    *node.discovery.bindings.lock().unwrap() = Some(Arc::new(
        node.machine
            .handler
            .session_move_context()
            .with_fault(fault),
    ));
}

/// Tell the liveness mirror these sessions are gone, so a move may take them.
fn sessions_gone(node: &MoveMachine) {
    let mut state = node.machine.handler.liveness.lock().unwrap();
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
    pair.a
        .store()
        .check_store_origin(Some("machine-a"), clock())
        .unwrap();
    pair.b
        .store()
        .check_store_origin(Some("machine-b"), clock())
        .unwrap();
    carry_bindings(&pair.a, fault);
    carry_bindings(&pair.b, SourceFault::default());
    register_sessions(&pair);
    sessions_gone(&pair.a);
    sessions_gone(&pair.b);
    pair
}

/// What the apps and Thalamus read for the agent on one machine.
async fn session_reads(node: &MoveMachine) -> (Value, Vec<Value>) {
    let sessions = node
        .machine
        .call("agent.sessions", json!({"agent_id":AGENT}))
        .await
        .unwrap();
    // A binding the receiver installed as a placeholder has no runner, so
    // its lookup by runner reads as the refusal.
    let mut by_runner = Vec::new();
    for session in SESSIONS {
        by_runner.push(
            node.machine
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

fn claim_taken(node: &MoveMachine) -> bool {
    node.records
        .calls
        .lock()
        .unwrap()
        .iter()
        .any(|(method, p)| method == "record.cas" && p["subject"] == format!("claim:{AGENT}"))
}

#[tokio::test(start_paused = true)]
async fn moved_bindings_and_network_history_read_the_same_on_both_machines_and_back() {
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
    for node in [&pair.a, &pair.b] {
        for request in node.discovery.peers["broca"].calls() {
            if let Some(refs) = request.refs() {
                assert_eq!(refs.as_slice().len(), 4);
                assert!(refs.as_slice().iter().all(|r| r.r#ref != "ref-ended"));
            }
        }
    }
    let mut remote = before.clone();
    for row in remote.0["sessions"].as_array_mut().unwrap() {
        if row["ref"] == "ref-ended" {
            row["heldOn"] = json!("machine-a");
        }
    }
    remote.1[2]["heldOn"] = json!("machine-a");
    assert_eq!(session_reads(&pair.b).await, remote);
    assert_eq!(
        pair.b
            .machine
            .call(
                "agent.session_for_ref",
                json!({"ref_kind":KIND,"ref":"ref-ended"})
            )
            .await
            .unwrap()["heldOn"],
        "machine-a"
    );
    for participant in pair.a.record(&cut).await.participants {
        assert!(participant.refs.iter().all(|r| r.reference != "ref-ended"));
    }
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

    // Back to the first machine in a new cut and stream. Its own rows are kept
    // as they are, so every read is still the same.
    pair.b
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    let back = pair.b.admit_to(A, "bindings-back").await.unwrap();
    pair.b.source_until(&back, MoveStep::Published).await;
    let item = |cut: &str, node: &MoveMachine| -> Vec<String> {
        node.door
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
    assert_eq!(session_reads(&pair.b).await, remote);
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
    for node in [&pair.a, &pair.b] {
        carry_bindings(node, SourceFault::default());
        node.discovery
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
    let mut before = session_reads(&pair.a).await;
    before.1[2]["heldOn"] = json!("machine-a");
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
        magic: Some(b"ck-session-bindings/v3\n"),
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
        held_on: None,
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

/// A receiving machine whose entorhinal predates carried bindings describes only
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

#[tokio::test(start_paused = true)]
async fn held_elsewhere_registration_and_rekey_refuse_without_writes() {
    let pair = bindings_pair(SourceFault::default()).await;
    let cut = pair.published("held-register").await;
    pair.finish(&cut).await;
    let before = (
        pair.b.machine.rows("session_binding"),
        pair.b.machine.rows("session_network"),
    );
    for (reference, session) in [("ref-ended", "different"), ("new-ref", "ended")] {
        let error = pair
            .b
            .store()
            .register_session(&SessionRegistration {
                agent_id: AGENT,
                ref_kind: KIND,
                session_ref: reference,
                harness: HARNESS,
                session_id: session,
                project_root: Some("/different"),
                registered_by: "host",
                runner: Some(RUNNER),
                network: Some(NetworkStamp {
                    network: "yes",
                    clock,
                }),
                now_ms: clock(),
            })
            .unwrap_err();
        assert_eq!(error.code, "session_on_other_machine");
        assert_eq!(error.detail, Some(json!({"machine_id":"machine-a"})));
        assert_eq!(
            (
                pair.b.machine.rows("session_binding"),
                pair.b.machine.rows("session_network")
            ),
            before
        );
    }
}

#[tokio::test(start_paused = true)]
async fn session_ending_after_admission_stays_in_every_participant_ref_set() {
    let pair = bindings_pair(SourceFault::default()).await;
    let cut = pair.a.admit_to(B, "ends-during-move").await.unwrap();
    pair.a
        .store()
        .end_session(HARNESS, "live-1", Some(RUNNER), clock())
        .unwrap();
    pair.a
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    pair.a.source_until(&cut, MoveStep::Published).await;
    let record = pair.a.record(&cut).await;
    assert!(record
        .participants
        .iter()
        .filter(|p| p.module_id == "broca")
        .all(|p| p.refs.iter().any(|r| r.reference == "ref-live-1")));
    pair.finish(&cut).await;
    let row = pair
        .b
        .store()
        .session_binding_for_runner(RUNNER, "live-1")
        .unwrap()
        .unwrap();
    assert!(row.ended_at_ms.is_some());
    assert_eq!(row.held_on, None);
    for node in [&pair.a, &pair.b] {
        for request in node.discovery.peers["broca"].calls() {
            if let Some(refs) = request.refs() {
                assert_eq!(refs.as_slice().len(), 4);
                assert!(refs.as_slice().iter().any(|r| r.r#ref == "ref-live-1"));
                assert!(refs.as_slice().iter().all(|r| r.r#ref != "ref-ended"));
            }
        }
    }
}

#[tokio::test(start_paused = true)]
async fn live_ref_cap_refuses_257_before_recording_but_thousands_finished_move() {
    let pair = bindings_pair(SourceFault::default()).await;
    pair.a.store().apply_entry("fixture.many_sessions", "{}", "fixture", None, |tx| {
        for i in 0..253 {
            tx.execute("INSERT INTO session_binding(ref_kind,ref,agent_id,project_root,registered_by,registered_at_ms) VALUES(?1,?2,?3,'','move',1)", rusqlite::params![KIND,format!("bulk-{i:04}"),AGENT])?;
        }
        Ok(())
    }).unwrap();
    // The fixture has four live refs, including its initial placeholder.
    let before = pair.a.machine.rows("agent_move");
    let error = drive(pair.a.machine.handler.admit_move(
        json!({"agent_id":AGENT,"to":B,"request_key":"too-many-live"}),
        CORE,
        tokio::time::Instant::now(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code, "live_ref_cap");
    assert_eq!(error.detail, Some(json!({"live_refs":257,"cap":256})));
    assert_eq!(pair.a.machine.rows("agent_move"), before);
    pair.a.store().apply_entry("fixture.finish_sessions", "{}", "fixture", None, |tx| {
        tx.execute("UPDATE session_binding SET ended_at_ms=10 WHERE ref LIKE 'bulk-%'", [])?;
        for i in 253..4000 {
            tx.execute("INSERT INTO session_binding(ref_kind,ref,agent_id,project_root,registered_by,registered_at_ms,ended_at_ms) VALUES(?1,?2,?3,'','move',1,10)", rusqlite::params![KIND,format!("bulk-{i:04}"),AGENT])?;
        }
        Ok(())
    }).unwrap();
    let cut = pair.published("many-finished").await;
    let record = pair.a.record(&cut).await;
    let refs = &record
        .participants
        .iter()
        .find(|p| p.module_id == "broca")
        .unwrap()
        .refs;
    assert_eq!(refs.len(), 4);
    assert!(refs
        .iter()
        .all(|r| !r.reference.starts_with("bulk-") && r.reference != "ref-ended"));
    pair.finish(&cut).await;
    assert_eq!(pair.b.machine.rows("session_binding").len(), 4005);
    assert_eq!(
        pair.b
            .store()
            .session_bindings_for_refs(&[(KIND, "bulk-3999")])
            .unwrap()[0]
            .as_ref()
            .unwrap()
            .held_on
            .as_deref(),
        Some("machine-a")
    );
}

#[tokio::test(start_paused = true)]
async fn abort_never_sends_finished_binding_refs_to_participants() {
    let pair = bindings_pair(SourceFault::default()).await;
    let cut = pair.a.admit_to(B, "abort-finished").await.unwrap();
    pair.a.source_until(&cut, MoveStep::Exported).await;
    pair.a.abort(&cut);
    pair.a.source_until(&cut, MoveStep::Aborted).await;
    let calls = pair.a.discovery.peers["broca"].calls();
    assert!(calls.iter().any(|r| r.op().method() == "agent_data.abort"));
    for request in calls {
        if let Some(refs) = request.refs() {
            assert_eq!(refs.as_slice().len(), 4);
            assert!(refs.as_slice().iter().all(|r| r.r#ref != "ref-ended"));
        }
    }
}

#[tokio::test(start_paused = true)]
async fn finished_only_agent_moves_metadata_without_participant_placeholders() {
    let pair = bindings_pair(SourceFault::default()).await;
    pair.a
        .store()
        .apply_entry("fixture.finish_all", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE session_binding SET ended_at_ms=COALESCE(ended_at_ms,?1) WHERE agent_id=?2",
                rusqlite::params![clock(), AGENT],
            )?;
            Ok(())
        })
        .unwrap();
    let cut = pair.published("only-finished").await;
    let record = pair.a.record(&cut).await;
    assert_eq!(
        entorhinal_refs(&record),
        Some(vec![("session-bindings".into(), AGENT.into())])
    );
    assert!(record
        .participants
        .iter()
        .filter(|p| p.module_id != "entorhinal")
        .all(|p| p.refs.is_empty()));
    pair.finish(&cut).await;
    let page = pair.b.store().agent_sessions(AGENT, None, 100).unwrap();
    assert_eq!(page.sessions.len(), 5);
    assert!(page
        .sessions
        .iter()
        .all(|r| r.ended_at_ms.is_some() && r.held_on.as_deref() == Some("machine-a")));
    assert_eq!(
        pair.b
            .machine
            .handler
            .health
            .session_bindings_placeholders
            .load(std::sync::atomic::Ordering::Relaxed),
        0
    );
    for node in [&pair.a, &pair.b] {
        assert!(node.discovery.peers["broca"]
            .calls()
            .iter()
            .all(|r| r.refs().is_none_or(|refs| refs.as_slice().is_empty())));
    }
}

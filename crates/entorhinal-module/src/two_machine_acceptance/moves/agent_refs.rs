use super::*;

const SESSION_KIND: &str = "session";

fn agent_ref() -> AgentRef {
    AgentRef::new("agent", AGENT)
}

fn session_ref() -> AgentRef {
    AgentRef::new(SESSION_KIND, "ref-session")
}

#[tokio::test(start_paused = true)]
async fn no_agent_declarer_preserves_legacy_plan_record_and_every_wire_request() {
    let pair = Pair::new(&[("broca", SESSION_KIND)]).await;
    let cut = pair.a.admit_to(B, "legacy").await.unwrap();
    assert_admission_described_once(&pair.a);
    let plan = serde_json::to_string(&pair.a.row(&cut).plan).unwrap();
    pair.a
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    pair.a.source_until(&cut, MoveStep::Published).await;
    let subject = format!("move:{cut}");
    let record = pair
        .a
        .machine
        .handler
        .record_client
        .read(&subject)
        .await
        .unwrap();
    pair.finish(&cut).await;
    let source = serde_json::to_string(&pair.a.discovery.peers["broca"].wire_calls())
        .unwrap()
        .replace(&cut, "<cut>");
    let receiver = serde_json::to_string(&pair.b.discovery.peers["broca"].wire_calls())
        .unwrap()
        .replace(&cut, "<cut>");
    let record = String::from_utf8(record.body.unwrap())
        .unwrap()
        .replace(&cut, "<cut>");
    // These literal bytes were captured with the admission and planner from
    // before agent refs were supported. Only the random cut ID is normalized;
    // decoding and re-encoding the record would hide wire-format changes.
    assert_eq!(plan.as_bytes(), br#"{"agent_id":"agent_0123456789abcdef","claim_version":0,"from":"01010101010101010101010101010101","participants":[{"external":[],"module_id":"broca","refs":[{"ref":"ref-session","ref_kind":"session"}]}],"to":"02020202020202020202020202020202"}"#);
    assert_eq!(record.as_bytes(), br#"{"v":1,"cut_id":"<cut>","agent_id":"agent_0123456789abcdef","from":"01010101010101010101010101010101","to":"02020202020202020202020202020202","state":"published","claim_version":0,"participants":[{"module_id":"broca","refs":[{"ref_kind":"session","ref":"ref-session"}],"external":[]}],"reason":null,"detail":null}"#);
    assert_eq!(source.as_bytes(), br#"[["agent_data.describe",{}],["agent_data.busy",{"refs":[{"ref":"ref-session","ref_kind":"session"}]}],["agent_data.prepare",{"cutId":"<cut>","refs":[{"ref":"ref-session","ref_kind":"session"}]}],["agent_data.export",{"cutId":"<cut>","refs":[{"ref":"ref-session","ref_kind":"session"}]}],["agent_data.release",{"cutId":"<cut>","holder":"02020202020202020202020202020202","refs":[{"ref":"ref-session","ref_kind":"session"}]}]]"#);
    assert_eq!(receiver.as_bytes(), br#"[["agent_data.describe",{}],["agent_data.diverged",{"cutId":"<cut>","refs":[{"ref":"ref-session","ref_kind":"session"}]}],["agent_data.import",{"cutId":"<cut>","refs":[{"ref":"ref-session","ref_kind":"session"}]}],["agent_data.activate",{"cutId":"<cut>","refs":[{"ref":"ref-session","ref_kind":"session"}]}]]"#);
}

// Declarations and session bindings are independent: agent-wide data must move
// even when the agent has never had a session.
async fn pair_with_refs(declarations: &[(&str, &[&str])], sessions: &[AgentRef]) -> Pair {
    let mut pair = Pair::new(&[]).await;
    let modules: Vec<_> = declarations.iter().map(|(m, _)| (*m, "")).collect();
    for (source, node) in [(true, &mut pair.a), (false, &mut pair.b)] {
        let mut peers = BTreeMap::new();
        for (module_index, (module, kinds)) in declarations.iter().enumerate() {
            let mut refs: Vec<_> = sessions
                .iter()
                .filter(|r| kinds.contains(&r.ref_kind.as_str()))
                .cloned()
                .collect();
            if kinds.contains(&"agent") {
                refs.push(agent_ref());
            }
            refs.sort();
            let peer = Arc::new(FakeParticipant::new(
                kinds,
                if source { refs.clone() } else { vec![] },
            ));
            peer.unknown_busy_for_unheld(true);
            let items: Vec<_> = refs.iter().enumerate().map(|(ref_index, r)| {
                let session = format!("{:032x}", 16 + module_index * 512 + ref_index);
                let item = CutItem { session_ref: session.clone(), stream: format!("reserved:{module}"), end_offset: 42, digest: "a".repeat(64) };
                node.door.set_stream(item.clone());
                json!({"ref_kind":r.ref_kind,"ref":r.r#ref,"sessionRef":session,"stream":item.stream,"endOffset":42,"digest":item.digest,"format":"v1","frozen":true})
            }).collect();
            peer.set_prepare(serde_json::from_value(json!({"items":items,"external":[]})).unwrap());
            peers.insert((*module).into(), peer);
        }
        node.discovery = Arc::new(Discovery {
            daemon: FakeDaemon::new(snapshot(&modules, true, true)),
            peers,
            gate: node.discovery.gate.clone(),
            catalog: Mutex::new(None),
            bindings: Mutex::new(None),
        });
        node.machine.handler.participant_discovery = node.discovery.clone();
    }
    for r in sessions {
        pair.a.store().apply_entry("fixture", "{}", "fixture", None, |tx| {
            tx.execute("INSERT INTO session_binding(ref_kind,ref,agent_id,project_root,registered_by,registered_at_ms) VALUES(?1,?2,?3,'','move',1)", (&r.ref_kind, &r.r#ref, AGENT))?;
            Ok(())
        }).unwrap();
    }
    pair
}

fn assert_admission_described_once(node: &MoveMachine) {
    for peer in node.discovery.peers.values() {
        assert_eq!(
            peer.calls()
                .iter()
                .filter(|r| matches!(r, Request::Describe(_)))
                .count(),
            1,
            "admission must reuse each participant's description"
        );
    }
}

fn assert_steps(node: &MoveMachine, module: &str, cut: &str, refs: &[AgentRef], ops: &[&str]) {
    let calls = node.discovery.peers[module].calls();
    let calls: Vec<_> = calls
        .iter()
        .filter(|r| r.cut_id().is_some_and(|id| id.as_str() == cut))
        .collect();
    for op in ops {
        assert!(
            calls.iter().any(|r| r.op().method() == *op),
            "missing {module} {op}"
        );
    }
    for call in calls {
        assert_eq!(
            call.refs().unwrap().as_slice(),
            refs,
            "{}",
            call.op().method()
        );
    }
}

#[tokio::test(start_paused = true)]
async fn agent_only_participants_receive_one_agent_ref_through_completion_and_back() {
    let pair = pair_with_refs(&[("alpha", &["agent"]), ("beta", &["agent"])], &[]).await;
    let cut = pair.a.admit_to(B, "agent-out").await.unwrap();
    assert_admission_described_once(&pair.a);
    pair.a
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    pair.a.source_until(&cut, MoveStep::Published).await;
    pair.finish(&cut).await;
    for module in ["alpha", "beta"] {
        assert_steps(
            &pair.a,
            module,
            &cut,
            &[agent_ref()],
            &[
                "agent_data.prepare",
                "agent_data.export",
                "agent_data.release",
            ],
        );
        assert_steps(
            &pair.b,
            module,
            &cut,
            &[agent_ref()],
            &[
                "agent_data.diverged",
                "agent_data.import",
                "agent_data.activate",
            ],
        );
        assert_eq!(
            pair.b.discovery.peers[module].held(),
            BTreeSet::from([agent_ref()])
        );
    }
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
    assert_eq!(pair.a.row(&cut).step, MoveStep::Released);
    assert_eq!(pair.b.row(&cut).step, MoveStep::Done);

    pair.b
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    let back = pair.b.admit_to(A, "agent-back").await.unwrap();
    pair.b.source_until(&back, MoveStep::Published).await;
    pair.a.pickup().await;
    pair.a.receiver_until(&back, MoveStep::Activated).await;
    drive(CutAccept::new(pair.a.worker(), back.clone()).tick())
        .await
        .unwrap();
    pair.b.source_until(&back, MoveStep::Released).await;
    pair.a.receiver_until(&back, MoveStep::Done).await;
    for module in ["alpha", "beta"] {
        assert_steps(
            &pair.b,
            module,
            &back,
            &[agent_ref()],
            &[
                "agent_data.prepare",
                "agent_data.export",
                "agent_data.release",
            ],
        );
        assert_steps(
            &pair.a,
            module,
            &back,
            &[agent_ref()],
            &[
                "agent_data.diverged",
                "agent_data.import",
                "agent_data.activate",
            ],
        );
        assert_eq!(
            pair.a.discovery.peers[module].held(),
            BTreeSet::from([agent_ref()])
        );
    }
    for node in [&pair.a, &pair.b] {
        assert!(node.store().agent_bound_refs(AGENT).unwrap().is_empty());
    }
    pair.clean();
}

#[tokio::test(start_paused = true)]
async fn agent_and_session_declarer_receives_both_but_session_only_receives_no_agent() {
    let pair = pair_with_refs(
        &[
            ("both", &["agent", SESSION_KIND]),
            ("session-only", &[SESSION_KIND]),
        ],
        &[session_ref()],
    )
    .await;
    let cut = pair.a.admit_to(B, "both").await.unwrap();
    assert_admission_described_once(&pair.a);
    pair.a
        .door
        .script("cut.seal", DoorEffect::Seal(SealState::Published, None));
    pair.a.source_until(&cut, MoveStep::Published).await;
    pair.finish(&cut).await;
    for (module, refs) in [
        ("both", vec![agent_ref(), session_ref()]),
        ("session-only", vec![session_ref()]),
    ] {
        assert_steps(
            &pair.a,
            module,
            &cut,
            &refs,
            &[
                "agent_data.prepare",
                "agent_data.export",
                "agent_data.release",
            ],
        );
        assert_steps(
            &pair.b,
            module,
            &cut,
            &refs,
            &["agent_data.import", "agent_data.activate"],
        );
    }
    pair.clean();
}

#[tokio::test(start_paused = true)]
async fn absent_agent_declarer_on_receiver_stops_before_import_or_claim() {
    let pair = pair_with_refs(&[("agent-owner", &["agent"])], &[]).await;
    let cut = pair.published("missing-agent-owner").await;
    pair.b.discovery.daemon.restart(snapshot(&[], true, true));
    let claim = pair.a.claim().await;
    pair.b.pickup().await;
    drive(pair.b.receiver(&cut).tick()).await.unwrap();
    let row = pair.b.row(&cut);
    assert_eq!(row.reason.as_deref(), Some("receiver_precondition"));
    assert_eq!(
        row.detail,
        Some(json!({"code":"participant_unavailable","module_id":"agent-owner","absent":true}))
    );
    assert!(crate::move_receiver::skipped_participants(&row).is_empty());
    pair.b.receiver_until(&cut, MoveStep::Aborted).await;
    assert_eq!(pair.a.claim().await, claim);
    assert!(pair.b.discovery.peers["agent-owner"].calls().is_empty());
    assert!(pair
        .b
        .door
        .calls()
        .iter()
        .all(|c| !matches!(c.request, DoorRequest::Accept(_))));
}

#[tokio::test(start_paused = true)]
async fn agent_ref_does_not_consume_any_of_256_live_session_slots() {
    let sessions: Vec<_> = (0..256)
        .map(|i| AgentRef::new(SESSION_KIND, format!("session-{i:03}")))
        .collect();
    let pair = pair_with_refs(
        &[
            ("agent-owner", &["agent"]),
            ("session-owner", &[SESSION_KIND]),
        ],
        &sessions,
    )
    .await;
    let cut = pair.a.admit_to(B, "full-session-cap").await.unwrap();
    assert_admission_described_once(&pair.a);
    assert_eq!(
        pair.a.row(&cut).plan["participants"][1]["refs"]
            .as_array()
            .unwrap()
            .len(),
        256
    );
    assert_eq!(
        pair.a.row(&cut).plan["participants"][0]["refs"],
        json!([{"ref_kind":"agent","ref":AGENT}])
    );
}

#[tokio::test(start_paused = true)]
async fn combined_declarer_refuses_257_routed_refs_before_busy_or_recording() {
    let sessions: Vec<_> = (0..256)
        .map(|i| AgentRef::new(SESSION_KIND, format!("session-{i:03}")))
        .collect();
    let pair = pair_with_refs(&[("owner", &["agent", SESSION_KIND])], &sessions).await;
    let before = pair.a.machine.rows("agent_move");
    let error = drive(pair.a.machine.handler.admit_move(
        json!({"agent_id":AGENT,"to":B,"request_key":"participant-cap"}),
        CORE,
        tokio::time::Instant::now(),
    ))
    .await
    .unwrap_err();
    assert_eq!(error.code, "live_ref_cap");
    assert_eq!(
        error.detail,
        Some(json!({"live_refs":256,"cap":256,"module_id":"owner","participant_refs":257}))
    );
    assert_eq!(pair.a.machine.rows("agent_move"), before);
    let calls = pair.a.discovery.peers["owner"].calls();
    assert_eq!(calls.len(), 1);
    assert!(matches!(calls[0], Request::Describe(_)));
}

#[tokio::test(start_paused = true)]
async fn agent_and_session_bindings_export_without_treating_agent_as_a_session() {
    let pair = pair_with_refs(&[("owner", &["agent", SESSION_KIND])], &[session_ref()]).await;
    for (node, origin) in [(&pair.a, "machine-a"), (&pair.b, "machine-b")] {
        node.store()
            .check_store_origin(Some(origin), clock())
            .unwrap();
        *node.discovery.catalog.lock().unwrap() =
            Some(Arc::new(node.machine.handler.catalog_move_context()));
        *node.discovery.bindings.lock().unwrap() =
            Some(Arc::new(node.machine.handler.session_move_context()));
        node.discovery.daemon.restart(snapshot(
            &[("owner", SESSION_KIND), ("entorhinal", "session-bindings")],
            true,
            true,
        ));
    }
    let bindings = pair.a.machine.rows("session_binding");
    let cut = pair.published("bindings-and-agent").await;
    pair.finish(&cut).await;
    assert_eq!(pair.b.machine.rows("session_binding"), bindings);
    assert_eq!(pair.b.store().agent_bound_refs(AGENT).unwrap().len(), 1);
    assert_steps(
        &pair.a,
        "owner",
        &cut,
        &[agent_ref(), session_ref()],
        &["agent_data.prepare", "agent_data.export"],
    );
    assert_steps(
        &pair.b,
        "owner",
        &cut,
        &[agent_ref(), session_ref()],
        &["agent_data.import", "agent_data.activate"],
    );
    pair.clean();
}

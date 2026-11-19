use super::*;
use cortexkit_role_agent_data::import::{ImportReply, ImportedItem};

/// The stream handle `Pair::new` gives the first module's cut item.
const AFT_STREAM: &str = "00000000000000000000000000000010";

async fn status(node: &MoveMachine, cut: &str) -> Value {
    node.machine
        .call("agent.move_status", json!({"cut_id":cut}))
        .await
        .unwrap()
}

async fn listed(node: &MoveMachine, cut: &str) -> Value {
    let moves = node.machine.call("agent.moves", json!({})).await.unwrap();
    moves["moves"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["cut_id"] == cut)
        .cloned()
        .unwrap()
}

#[tokio::test(start_paused = true)]
async fn import_drops_show_as_completed_with_losses_and_survive_a_restart() {
    let mut f = Pair::new(&[("aft", "aft-session"), ("pi", "pi-session")]).await;
    let dropped = BTreeMap::from([("root_missing".into(), 12), ("content_mismatch".into(), 3)]);
    f.b.discovery.peers["aft"].set_import(ImportReply::new(vec![ImportedItem::new(
        AFT_STREAM,
        42,
        "a".repeat(64),
    )
    .with_dropped(dropped)
    .unwrap()]));
    let cut = f.published("losses").await;
    f.b.pickup().await;
    // Lose the import reply twice, so the client gives up with the outcome
    // unknown and the receiver keeps the import pending across a restart. The
    // restarted receiver recovers the reply and then imports again, so the
    // same drops arrive more than once and must still be counted once.
    for _ in 0..2 {
        f.b.discovery.peers["aft"].script(
            "agent_data.import",
            Effect::LoseReply(crate::log_client::TransportError::NoReply),
        );
    }
    drive(f.b.receiver(&cut).tick()).await.unwrap();
    assert_eq!(f.b.row(&cut).step, MoveStep::PickedUp);
    assert_eq!(
        f.b.row(&cut).plan["_receiver"]["pending"]["op"],
        "agent_data.import"
    );
    f.b.restart().await;
    elapse(300_000);
    f.b.receiver_until(&cut, MoveStep::Activated).await;
    let imports = f.b.discovery.peers["aft"]
        .calls()
        .iter()
        .filter(|call| call.op().method() == "agent_data.import")
        .count();
    assert!(imports >= 3, "the import was resent after the restart");

    let losses = json!([
        {"participant":"aft","session_ref":AFT_STREAM,"reason":"content_mismatch","count":3},
        {"participant":"aft","session_ref":AFT_STREAM,"reason":"root_missing","count":12}
    ]);
    assert_eq!(status(&f.b, &cut).await["losses"], losses);
    assert_eq!(listed(&f.b, &cut).await["losses"], losses);

    drive(CutAccept::new(f.b.worker(), cut.clone()).tick())
        .await
        .unwrap();
    f.a.source_until(&cut, MoveStep::Released).await;
    f.b.receiver_until(&cut, MoveStep::Done).await;
    assert_eq!(f.b.where_().await["status"], "held_here");
    f.b.restart().await;
    let after = status(&f.b, &cut).await;
    assert_eq!(after["step"], "done");
    assert_eq!(after["losses"], losses);
    assert_eq!(listed(&f.b, &cut).await["losses"], losses);
    // Losses are recorded on the machine that ran the import only; the shared
    // move record and the source's view carry none.
    assert!(status(&f.a, &cut).await.get("losses").is_none());
    assert!(listed(&f.a, &cut).await.get("losses").is_none());
    let record = serde_json::to_value(f.a.record(&cut).await).unwrap();
    assert!(!record.to_string().contains("root_missing"));
    f.clean();
}

#[tokio::test(start_paused = true)]
async fn a_move_without_drops_keeps_its_status_replies_unchanged() {
    let f = Pair::new(&[("aft", "aft-session"), ("pi", "pi-session")]).await;
    f.b.discovery.peers["aft"].set_import(ImportReply::new(vec![ImportedItem::new(
        AFT_STREAM,
        42,
        "a".repeat(64),
    )]));
    let cut = f.published("clean").await;
    f.finish(&cut).await;
    for node in [&f.a, &f.b] {
        let row = node.row(&cut);
        for key in ["_source", "_receiver"] {
            assert!(row.plan[key].get("dropped").is_none(), "{key}");
        }
        // The handler replies are exactly the stored views: nothing is added
        // for a move whose imports dropped nothing.
        let view = node.store().move_status_view(&cut).unwrap().unwrap();
        assert_eq!(status(node, &cut).await, view);
        assert_eq!(listed(node, &cut).await, view);
        assert!(view.get("losses").is_none());
    }
    f.clean();
}

async fn optional_move_pair() -> Pair {
    let f = Pair::new(&[
        ("aft", "aft-session"),
        ("broca", "broca-session"),
        ("pi", "pi-session"),
    ])
    .await;
    f.a.discovery.peers["aft"].set_optional();
    // Broca is required and has a planned ref, but sealed no items.
    f.a.discovery.peers["broca"].set_prepare(PrepareReply::new(vec![], vec![]));
    f.b.discovery
        .daemon
        .restart(snapshot(&[("pi", "pi-session")], true, true));
    f
}

fn optional_move_losses() -> Value {
    json!([{"participant":"aft","session_ref":AFT_STREAM,"reason":"optional_participant_absent",
        "count":1,"source":A,"source_name":"Desk"}])
}

#[tokio::test(start_paused = true)]
async fn move_session_regression_skipped_participant_with_local_binding_is_not_opened() {
    let f = optional_move_pair().await;
    f.b.store().apply_entry("fixture", "{}", "fixture", None, |tx| {
        tx.execute(
            "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
             VALUES('aft-session','ref-aft-session',?1,NULL,NULL,'','move',1)",
            [AGENT],
        )?;
        Ok(())
    }).unwrap();
    let cut = f.published("optional-with-local-binding").await;
    f.b.pickup().await;
    drive(f.b.receiver(&cut).tick()).await.unwrap();
    let row = f.b.row(&cut);
    assert_eq!(
        row.step,
        MoveStep::Imported,
        "skipped participant must not be opened: {:?}",
        row.hold
    );
}

#[tokio::test(start_paused = true)]
async fn optional_move_receiver_skips_sealed_items_and_keeps_the_decision_across_restart() {
    let mut f = optional_move_pair().await;
    let cut = f.published("optional-receiver").await;
    let published = f.a.record(&cut).await;
    assert!(published.participants[0].optional);
    assert!(published.skipped.is_empty());
    assert!(f
        .a
        .door
        .cut(&cut)
        .unwrap()
        .items
        .iter()
        .any(|i| i.session_ref == AFT_STREAM));

    f.b.pickup().await;
    f.b.receiver_until(&cut, MoveStep::Imported).await;
    assert_eq!(
        status(&f.b, &cut).await["skipped_participants"],
        json!(["aft", "broca"])
    );
    f.b.restart().await;
    // Installing AFT after the decision must not make it active for this cut.
    f.b.discovery.daemon.restart(snapshot(
        &[("aft", "aft-session"), ("pi", "pi-session")],
        true,
        true,
    ));
    for (step, state) in [
        (MoveStep::Staged, MoveState::Staged),
        (MoveStep::Claimed, MoveState::Claimed),
        (MoveStep::Activated, MoveState::Activated),
    ] {
        f.b.receiver_until(&cut, step).await;
        let record = f.b.record(&cut).await;
        assert_eq!(record.state, state);
        assert_eq!(record.skipped, ["aft"]);
    }
    f.finish(&cut).await;
    f.b.restart().await;
    let row = status(&f.b, &cut).await;
    assert_eq!(row["step"], "done");
    assert_eq!(row["losses"], optional_move_losses());
    assert_eq!(listed(&f.b, &cut).await["losses"], row["losses"]);
    for module in ["aft", "broca"] {
        assert!(f.b.discovery.peers[module].calls().is_empty());
    }
}

#[tokio::test(start_paused = true)]
async fn optional_move_source_keeps_skipped_data_but_releases_required_no_items() {
    let mut f = optional_move_pair().await;
    f.a.discovery.peers["pi"].set_optional();
    let cut = f.published("optional-source").await;
    f.b.pickup().await;
    f.b.receiver_until(&cut, MoveStep::Staged).await;
    drive(f.a.source(&cut).tick()).await.unwrap();
    f.a.restart().await;
    assert_eq!(status(&f.a, &cut).await["losses"], optional_move_losses());
    f.finish(&cut).await;
    assert_eq!(f.a.record(&cut).await.skipped, ["aft"]);
    let aft = &f.a.discovery.peers["aft"];
    assert!(aft
        .calls()
        .iter()
        .any(|call| matches!(call, Request::Export(_))));
    assert!(!aft
        .calls()
        .iter()
        .any(|call| matches!(call, Request::Release(_))));
    assert!(aft.tombstones().is_empty());
    for (module, kind) in [("broca", "broca-session"), ("pi", "pi-session")] {
        assert_eq!(
            f.a.discovery.peers[module].tombstones()[&AgentRef::new(kind, format!("ref-{kind}"))],
            B
        );
    }
    let row = status(&f.a, &cut).await;
    assert_eq!(row["step"], "released");
    assert_eq!(row["losses"], optional_move_losses());
    assert_eq!(listed(&f.a, &cut).await["losses"], row["losses"]);
}

#[tokio::test(start_paused = true)]
async fn optional_move_absent_required_participant_with_items_still_aborts() {
    let f = Pair::new(&[("aft", "aft-session"), ("pi", "pi-session")]).await;
    f.b.discovery
        .daemon
        .restart(snapshot(&[("pi", "pi-session")], true, true));
    let cut = f.published("required-with-items").await;
    f.b.pickup().await;
    f.b.receiver_until(&cut, MoveStep::Aborted).await;
    f.a.source_until(&cut, MoveStep::Aborted).await;
    let record = f.a.record(&cut).await;
    assert_eq!(record.state, MoveState::Aborted);
    assert_eq!(
        record.detail,
        Some(json!({"code":"participant_unavailable","module_id":"aft","absent":true}))
    );
    assert!(record.skipped.is_empty());
    assert!(status(&f.b, &cut).await.get("losses").is_none());
    assert!(f.b.discovery.peers["aft"].calls().is_empty());
    f.clean();
}

#[tokio::test(start_paused = true)]
async fn optional_move_abort_cleanup_skips_absent_modules_before_and_after_staging() {
    for step in [MoveStep::PickedUp, MoveStep::Staged] {
        let mut f = optional_move_pair().await;
        let cut = f.published("optional-abort").await;
        f.b.pickup().await;
        f.b.receiver_until(&cut, step).await;
        f.b.abort(&cut);
        f.b.restart().await;
        f.b.receiver_until(&cut, MoveStep::Aborted).await;
        f.a.source_until(&cut, MoveStep::Aborted).await;
        let record = f.a.record(&cut).await;
        assert_eq!(record.state, MoveState::Aborted);
        assert_eq!(
            record.skipped,
            if step == MoveStep::Staged {
                vec!["aft"]
            } else {
                vec![]
            }
        );
        for node in [&f.a, &f.b] {
            assert!(status(node, &cut).await.get("losses").is_none());
        }
        assert!(f.b.discovery.peers["aft"].calls().is_empty());
        f.clean();
    }
}

#[tokio::test(start_paused = true)]
async fn optional_move_skipped_without_items_has_no_loss_but_is_not_released() {
    let f = optional_move_pair().await;
    f.a.discovery.peers["aft"].set_prepare(PrepareReply::new(vec![], vec![]));
    let cut = f.published("optional-no-items").await;
    f.finish(&cut).await;
    assert_eq!(f.a.record(&cut).await.skipped, ["aft"]);
    assert!(f.b.discovery.peers["aft"].calls().is_empty());
    assert!(!f.a.discovery.peers["aft"]
        .calls()
        .iter()
        .any(|call| matches!(call, Request::Release(_))));
    for node in [&f.a, &f.b] {
        assert!(status(node, &cut).await.get("losses").is_none());
        assert!(listed(node, &cut).await.get("losses").is_none());
    }
}

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

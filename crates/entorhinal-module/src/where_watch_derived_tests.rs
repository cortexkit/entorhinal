use super::*;
use entorhinal_core::move_store::{AgentMove, MoveStep};

const OTHER: &str = "02020202020202020202020202020202";
fn resync(receiver: &mut mpsc::UnboundedReceiver<Vec<u8>>, seq: u64) {
    assert_eq!(
        notice(receiver),
        json!({"kind":"resync","cursor":{"incarnation":INCARNATION,"seq":seq}})
    );
    quiet(receiver);
}

#[tokio::test(start_paused = true)]
async fn where_derived_sharing_and_liveness_resync() {
    let f = seeded("where-derived-sharing").await;
    let store = store(&f);
    let body = request(WHERE, json!({"cursor":{"incarnation":INCARNATION,"seq":0}}));
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert!(store.agent_where(HEAD).unwrap().known);
    f.success(
        CORE,
        "agent.rename",
        json!({"agent_id":HEAD,"name":"Identity only","request_key":"identity-only"}),
    )
    .await;
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);
    f.success(
        CORE,
        "agent.dispose",
        json!({"agent_id":ASSISTANT,"request_key":"gone"}),
    )
    .await;
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 1);
    assert_eq!(store.agent_where(ASSISTANT).unwrap_err().code, "gone");
    store.begin_hub_enable(&"ab".repeat(16), OTHER).unwrap();
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 2);
    assert!(!store.agent_where(HEAD).unwrap().known);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn where_derived_move_progress_resyncs() {
    let f = seeded("where-derived-move").await;
    let store = store(&f);
    let cached = store.cached_agent_claim(HEAD).unwrap();
    let body = request(
        WHERE,
        json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}}),
    );
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    let mut row: AgentMove = serde_json::from_value(json!({
        "cut_id":"derived-cut","agent_id":HEAD,"side":"source","peer":OTHER,"step":"preparing","plan":{},
        "record_version":0,"seal_sent":false,"attempts":0,"created_at_ms":700,"updated_at_ms":700,
    })).unwrap();
    store.insert_agent_move(&row).unwrap();
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 1);
    assert_eq!(
        store.agent_where(HEAD).unwrap().moving.unwrap()["step"],
        "preparing"
    );
    row.step = MoveStep::Aborted;
    row.reason = Some("diverged".into());
    row.detail = Some(json!({"code":"different-history"}));
    row.plan =
        json!({"_source":{"errors":[{"code":"participant_failed"}],"enforced":{"data":false}}});
    row.updated_at_ms = 701;
    assert!(store.update_agent_move(&row).unwrap());
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 2);
    let after = store.agent_where(HEAD).unwrap();
    assert!(after.moving.is_none());
    assert!(!after.move_diagnostics.is_empty());
    assert!(!after.move_errors.is_empty());
    assert!(!after.move_warnings.is_empty());
    assert_eq!(store.cached_agent_claim(HEAD).unwrap(), cached);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

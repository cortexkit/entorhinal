use super::*;
use entorhinal_core::{
    move_store::{AgentMove, MoveStep},
    remote_apply::{RemoteEntry, ENVELOPE_VERSION},
    role_cache::{RoleTakeAttempt, RoleTakeInput},
    shared_entry::{canonical_bytes, MemberName},
};

const OWN: &str = "01010101010101010101010101010101";
const OTHER: &str = "02020202020202020202020202020202";

fn held(holder: &str) -> ClaimObservation {
    ClaimObservation::Present {
        claim: CachedClaim {
            holder: Some(holder.into()),
            epoch: 1,
            cut_id: None,
            version: 1,
            read_at_ms: 0,
            stale: false,
            integrity_failed: false,
        },
        self_author: OWN.into(),
    }
}

fn remote(position: i64, body: Value) -> RemoteEntry {
    RemoteEntry {
        position,
        entry_id: format!("derived-{position}"),
        author: OTHER.into(),
        signer: "verified-signer".into(),
        key_id: "key".into(),
        envelope_version: ENVELOPE_VERSION,
        kind: "change".into(),
        entry: canonical_bytes(&body),
    }
}

fn sharing_on(store: &RegistryStore) {
    store
        .apply_entry("fixture", "{}", "test", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                [],
            )
        })
        .unwrap();
}

fn name(store: &RegistryStore, author: &str, display: &str) {
    store
        .set_member_name(
            MemberName {
                author: author.into(),
                display_name: display.into(),
            },
            "test",
        )
        .unwrap();
}

fn resync(receiver: &mut mpsc::UnboundedReceiver<Vec<u8>>, seq: u64) {
    assert_eq!(
        notice(receiver),
        json!({"kind":"resync","cursor":{"incarnation":INCARNATION,"seq":seq}})
    );
    quiet(receiver);
}

#[tokio::test(start_paused = true)]
async fn where_derived_member_rename_resyncs() {
    let f = seeded("where-derived-member").await;
    let store = store(&f);
    sharing_on(&store);
    store
        .cache_agent_claims(
            &[(HEAD.into(), held(OWN)), (ASSISTANT.into(), held(OTHER))],
            700,
        )
        .unwrap();
    name(&store, OWN, "Before");
    name(&store, OTHER, "Other before");
    let cached = store.cached_agent_claim(HEAD).unwrap();
    let body = request(
        WHERE,
        json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}}),
    );
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    quiet(&mut receiver);

    assert!(store
        .apply_entry("fixture", "{}", "test", None, |tx| {
            tx.execute(
                "UPDATE member_name SET display_name='Rolled back' WHERE author=?1",
                [OWN],
            )?;
            Err::<(), _>(rusqlite::Error::InvalidQuery)
        })
        .is_err());
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);

    name(&store, OWN, "Renamed locally");
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 1);
    assert_eq!(
        store.agent_where(HEAD).unwrap().holder_name.as_deref(),
        Some("Renamed locally")
    );
    assert_eq!(store.cached_agent_claim(HEAD).unwrap(), cached);

    let entry = remote(
        1,
        json!({"op":"member.set_name","author":OTHER,"display_name":"Renamed remotely"}),
    );
    store.apply_remote_entry(&entry, None).unwrap();
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 2);
    assert_eq!(
        store.agent_where(ASSISTANT).unwrap().holder_name.as_deref(),
        Some("Renamed remotely")
    );
    assert_eq!(store.cached_agent_claim(HEAD).unwrap(), cached);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));

    let body = request(
        WHERE,
        json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}}),
    );
    let (sink, mut receiver) = Sink::new();
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    resync(&mut receiver, 2);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn where_derived_self_author_learning_resyncs() {
    for source in ["role_cache", "role_take", "remote_apply"] {
        let f = seeded(&format!("where-derived-self-{source}")).await;
        let store = store(&f);
        sharing_on(&store);
        store
            .cache_agent_claims(&[(HEAD.into(), held(OWN))], 700)
            .unwrap();
        name(&store, OWN, "This Mac");
        store
            .apply_entry("fixture", "{}", "test", None, |tx| {
                tx.execute("DELETE FROM self_identity", [])
            })
            .unwrap();
        let before = store.cached_agent_claim(HEAD).unwrap();
        assert_eq!(store.agent_where(HEAD).unwrap().status, "held_elsewhere");
        assert_eq!(store.agent_where(HEAD).unwrap().holder_name, None);
        let body = request(
            WHERE,
            json!({"agentIds":[HEAD],"cursor":{"incarnation":INCARNATION,"seq":0}}),
        );
        let (sink, mut receiver) = Sink::new();
        let watch = f.handler.serve_watch(&sink, &body, READER);
        tokio::pin!(watch);
        pending(watch.as_mut()).await;
        quiet(&mut receiver);
        match source {
            "role_cache" => {
                store
                    .cache_roles(&[("watch_fixture".into(), absent(false))], 701)
                    .unwrap();
            }
            "role_take" => {
                store
                    .prepare_role_take(
                        &RoleTakeAttempt {
                            input: RoleTakeInput {
                                role: "watch_fixture".into(),
                                expected_epoch: 0,
                                request_id: "learn".into(),
                                reason: "test".into(),
                            },
                            expected_version: 0,
                            body: b"{}".to_vec(),
                            epoch: None,
                        },
                        OWN,
                        701,
                    )
                    .unwrap();
            }
            "remote_apply" => {
                store
                    .apply_remote_entry(
                        &remote(1, json!({"op":"project.shared","tables":{}})),
                        Some(OWN),
                    )
                    .unwrap();
            }
            _ => unreachable!(),
        }
        debounce(watch.as_mut()).await;
        resync(&mut receiver, 1);
        assert_eq!(store.cached_agent_claim(HEAD).unwrap(), before);
        let after = store.agent_where(HEAD).unwrap();
        assert_eq!(after.status, "held_here");
        assert_eq!(after.holder_name.as_deref(), Some("This Mac"));
        store
            .cache_roles(&[("watch_fixture".into(), absent(false))], 702)
            .unwrap();
        debounce(watch.as_mut()).await;
        quiet(&mut receiver);
        sink.cancel.send_replace(true);
        assert!(matches!(watch.await, HandlerOutcome::Streamed));
    }
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
    let plan = store.prepare_enable("derived-sharing", 700).unwrap();
    store.begin_enable(&[], &plan.backfill).unwrap();
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 1);
    assert!(!store.agent_where(HEAD).unwrap().known);
    store.abandon_enable().unwrap();
    debounce(watch.as_mut()).await;
    resync(&mut receiver, 2);
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
    resync(&mut receiver, 3);
    assert_eq!(store.agent_where(ASSISTANT).unwrap_err().code, "gone");
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

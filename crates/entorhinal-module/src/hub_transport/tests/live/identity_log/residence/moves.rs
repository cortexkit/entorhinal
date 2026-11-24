use super::*;
use crate::{
    door_client::Request as DoorRequest,
    fake_daemon::{DaemonSnapshot, FakeDaemon},
    fake_door::{Effect as DoorEffect, FakeDoor},
    fake_participant::{Effect as ParticipantEffect, FakeParticipant},
    hub_moves::{move_key, State},
    hub_residence::MovePosition,
    log_client::TransportError,
    move_driver::{SqlOperations, Worker},
    move_receiver::{poll_hub, Receiver},
    move_source::Source,
    participant_client::{AgentRef, Refusal},
    participant_discovery::{provider_role, ParticipantDiscovery},
    sessions::{FakeResolver, ProviderResolution, ORCHESTRATOR_CAPABILITY},
    RouteKey,
};
use entorhinal_core::move_store::{AgentMove, MoveSide, MoveStep};

const CUT: &str = "aabb0123456789abcdef0123456789ab";
const ADMIT: RouteKey = (930, 1);
thread_local! { static CONTROLLED_NOW: std::cell::Cell<i64> = const { std::cell::Cell::new(0) }; }

fn controlled_clock() -> i64 {
    CONTROLLED_NOW.with(|now| now.get())
}

fn set_controlled_now(value: i64) {
    CONTROLLED_NOW.with(|now| now.set(value));
}

fn advance_controlled(ms: i64) {
    CONTROLLED_NOW.with(|now| now.set(now.get() + ms));
}

struct Moving {
    rig: IdentityRig,
    source: Worker,
    receiver: Worker,
    a: Arc<hub_residence::Residence>,
    b: Arc<hub_residence::Residence>,
    door: FakeDoor,
    from: Arc<FakeParticipant>,
    to: Arc<FakeParticipant>,
}

struct Discovery(FakeDaemon, Arc<FakeParticipant>);

#[async_trait::async_trait]
impl ParticipantDiscovery for Discovery {
    async fn supervisor_list(
        &self,
    ) -> Result<subc_client_rs::SupervisorListResponse, crate::log_client::TransportError> {
        self.0.supervisor_list().await
    }
    async fn catalog_list(
        &self,
    ) -> Result<subc_client_rs::CatalogList, crate::log_client::TransportError> {
        self.0.catalog_list().await
    }
    async fn open_participant(
        &self,
        module: &str,
    ) -> Result<Arc<dyn crate::log_client::LogTransport>, crate::log_client::TransportError> {
        assert_eq!(module, "data");
        Ok(self.1.clone())
    }
}

fn worker(node: &Node, door: &FakeDoor, peer: Arc<FakeParticipant>) -> Worker {
    let handler = &node.handler;
    let daemon = FakeDaemon::new(DaemonSnapshot::new(
        1,
        vec![
            json!({"module_id":"data","state":"running","enabled":true,"live":true,"health":"ok"}),
        ],
        vec![
            json!({"module_id":"data","ready":true,"roles":[provider_role()],"control_ops":[],
            "capabilities":{"provides":["agent-data/v1"],"requires":[]}}),
        ],
    ));
    Worker {
        store: Arc::downgrade(&handler.store),
        sql_operations: Arc::new(SqlOperations::default()),
        door: door.client(),
        discovery: Arc::new(Discovery(daemon, peer)),
        catalog: None,
        health: handler.health.clone(),
        clock: crate::unix_millis,
        wake: handler.move_wake.clone(),
    }
}

impl Moving {
    async fn new() -> Option<Self> {
        Self::new_with_kinds(&["agent"]).await
    }

    async fn new_with_kinds(kinds: &[&str]) -> Option<Self> {
        let rig = IdentityRig::new().await?;
        for node in [&rig.a, &rig.b] {
            migrate_identity(node, MIGRATED, "move agent");
        }
        let image = Arc::new(Image(HashMap::from([(
            MIGRATED.into(),
            (Some(A.into()), 1),
        )])));
        residence(&rig.a, image.clone());
        residence(&rig.b, image);
        let a = rig.a.handler.residence().unwrap();
        let b = rig.b.handler.residence().unwrap();
        put(
            &rig,
            MIGRATED,
            0,
            A,
            json!({"v":1,"writer":A,"holder":A,"gen":1,"forced":false,"seeded":true}),
        )
        .await;
        until_async(|| async {
            a.move_origin(MIGRATED).await.is_ok() && b.move_member(B).await.ok().flatten().is_some()
        })
        .await;
        let author = |id: &str| {
            std::array::from_fn(|i| u8::from_str_radix(&id[2 * i..2 * i + 2], 16).unwrap())
        };
        let door = FakeDoor::new(author(A));
        let from = Arc::new(FakeParticipant::new(
            kinds,
            vec![AgentRef::new("agent", MIGRATED)],
        ));
        let item = crate::door_client::CutItem {
            session_ref: "11111111111111111111111111111111".into(),
            stream: "reserved:data".into(),
            end_offset: 42,
            digest: "a".repeat(64),
        };
        door.set_stream(item.clone());
        from.set_prepare(
            serde_json::from_value(json!({"items":[{
            "ref_kind":"agent", "ref":MIGRATED, "sessionRef":item.session_ref,
            "stream":item.stream, "endOffset":42, "digest":item.digest, "format":"v1", "frozen":true
        }],"external":[]}))
            .unwrap(),
        );
        let to = Arc::new(FakeParticipant::new(kinds, vec![]));
        let source = worker(&rig.a, &door, from.clone());
        let receiver = worker(&rig.b, &door.for_author(author(B)), to.clone());
        let now = crate::unix_millis();
        store_of(&rig.a.handler).insert_agent_move(&AgentMove {
            cut_id: CUT.into(), agent_id: MIGRATED.into(), side: MoveSide::Source, peer: B.into(),
            step: MoveStep::Preparing,
            plan: json!({"agent_id":MIGRATED,"from":A,"to":B,"claim_version":1,"hub_gen":1,
                "participants":[{"module_id":"data","external":[],"refs":[{"ref_kind":"agent","ref":MIGRATED}]}]}),
            request_body: None, record_version: 0, claim_version: Some(1), claim_take_body: None,
            claim_take_version: None, claim_landed_version: None, seal_sent: false, abort_requested: None,
            hold: None, cut_accept: None, reason: None, detail: None, request_key: None,
            attempts: 0, activated_at_ms: None, created_at_ms: now, updated_at_ms: now,
        }).unwrap();
        Some(Self {
            rig,
            source,
            receiver,
            a,
            b,
            door,
            from,
            to,
        })
    }

    async fn source_tick(&self) {
        self.fresh_source().tick().await.unwrap();
    }
    async fn receiver_tick(&self) {
        self.fresh_receiver().tick().await.unwrap();
    }
    fn fresh_source(&self) -> Source {
        Source::new(self.source.clone(), CUT.into())
    }
    fn fresh_receiver(&self) -> Receiver {
        Receiver::new(
            self.receiver.clone(),
            CUT.into(),
            self.rig.b.handler.liveness.clone(),
        )
    }
    fn receiver_door(&self) -> FakeDoor {
        self.door.for_author(std::array::from_fn(|i| {
            u8::from_str_radix(&B[2 * i..2 * i + 2], 16).unwrap()
        }))
    }
    fn door_count(&self, author: &str, op: &str) -> usize {
        self.door
            .calls()
            .iter()
            .filter(|call| {
                call.author == author && call.request.op() == op && call.request.cut_id() == CUT
            })
            .count()
    }
    async fn finish_receiver_abort(&self) {
        self.receiver_tick().await;
        self.watch(State::Aborted).await;
        self.receiver_tick().await;
        assert_eq!(
            self.receiver.row(CUT).await.unwrap().step,
            MoveStep::Aborted
        );
        assert_eq!(self.a.move_origin(MIGRATED).await.unwrap(), (1, A.into()));
    }
    async fn watch(&self, state: State) {
        until_async(|| async {
            for hub in [&self.a, &self.b] {
                if !hub
                    .watched_move(CUT)
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.body.state == state)
                {
                    return false;
                }
            }
            true
        })
        .await;
    }
    async fn publish(&self) {
        for step in [MoveStep::Exported, MoveStep::Sealed, MoveStep::Published] {
            self.source_tick().await;
            let row = self.source.row(CUT).await.unwrap();
            assert_eq!(
                row.step, step,
                "hold={:?}, detail={:?}",
                row.hold, row.detail
            );
        }
        self.watch(State::Published).await;
    }
    async fn stage(&self) {
        self.publish().await;
        poll_hub(&self.receiver, &self.b).await.unwrap();
        self.receiver_tick().await;
        assert_eq!(
            self.receiver.row(CUT).await.unwrap().step,
            MoveStep::Imported
        );
        self.receiver_tick().await;
        self.watch(State::Staged).await;
    }
    fn abort(&self) {
        store_of(&self.rig.b.handler)
            .request_agent_move_abort(CUT, crate::unix_millis())
            .unwrap();
    }
    fn arm_ack_loss(node: &Node, operation: &str) -> (oneshot::Receiver<()>, oneshot::Sender<()>) {
        let (committed, landed) = oneshot::channel();
        let (release, blocked) = oneshot::channel();
        *node.leaf.epoch_gate.lock().unwrap() =
            Some((operation.into(), Gate::LoseAck(committed, blocked)));
        (landed, release)
    }
}

#[tokio::test]
async fn hub_residence_moving_samples() {
    let Some(mut f) = Moving::new().await else {
        return;
    };
    f.source.clock = || 650;
    f.stage().await;
    for (node, side, author, peer, name) in [
        (&f.rig.a, "source", "b5".repeat(16), B, "Studio"),
        (&f.rig.b, "receiver", "a5".repeat(16), A, "Workshop"),
    ] {
        bind(&node.handler, subc_protocol::Principal::Direct);
        record_display_name(node, &author, peer, name);
        let sample =
            cutover_sample(&node.handler, "agent.where", json!({"agent_id":MIGRATED})).await;
        let reply: Value = serde_json::from_str(&sample).unwrap();
        let moving = &reply["reply"]["result"]["moving"];
        assert_eq!(moving["side"], side);
        assert_eq!(moving["peer"], peer);
        assert_eq!(moving["peer_name"], name);
        assert_eq!(moving["cut_id"], CUT);
        crate::served_vectors::published_file(
            "agent-sessions-v1",
            &format!("where-moving-hub-{side}.json"),
            vec![sample],
        );
    }
}

#[tokio::test]
async fn hub_residence_moving_hold_message_sample() {
    let Some(mut f) = Moving::new().await else {
        return;
    };
    let node = &f.rig.a;
    bind(&node.handler, subc_protocol::Principal::Direct);
    record_display_name(node, &"b5".repeat(16), B, "Studio");
    f.source.clock = controlled_clock;
    set_controlled_now(650);
    let refuse_export = |message| {
        f.from.script(
            "agent_data.export",
            ParticipantEffect::Refuse(Refusal {
                code: "unavailable".into(),
                detail: None,
                message,
            }),
        );
    };
    let message = "Session archive is still being indexed";
    refuse_export(Some(message.into()));
    let mut source = f.fresh_source();
    source.tick().await.unwrap();

    let sample = cutover_sample(&node.handler, "agent.where", json!({"agent_id":MIGRATED})).await;
    let reply: Value = serde_json::from_str(&sample).unwrap();
    let moving = &reply["reply"]["result"]["moving"];
    assert_eq!(moving["step"], "preparing");
    assert_eq!(
        moving["hold"],
        json!({"code":"unavailable","module_id":"data","op":"agent_data.export",
            "move_error":false,"message":message})
    );
    let batch = cutover_sample(
        &node.handler,
        "agent.where_many",
        json!({"agentIds":[MIGRATED]}),
    )
    .await;
    let batch: Value = serde_json::from_str(&batch).unwrap();
    assert_eq!(
        batch["reply"]["result"]["agents"][0]["moving"]["hold"],
        moving["hold"]
    );
    let listing = cutover_sample(&node.handler, "agent.moves", json!({})).await;
    let listing: Value = serde_json::from_str(&listing).unwrap();
    assert_eq!(
        listing["reply"]["result"]["moves"][0]["hold"],
        moving["hold"]
    );

    let long = format!("a\n\u{0085}{}🙂trailing", "é".repeat(254));
    let bounded = format!("a  {}", "é".repeat(254));
    for (wait, message, expected) in [
        (1_000, Some(long.clone()), Some(bounded.as_str())),
        (2_000, Some(long), Some(bounded.as_str())),
        (4_000, None, None),
    ] {
        advance_controlled(wait);
        refuse_export(message);
        source.tick().await.unwrap();
        let row = f.source.row(CUT).await.unwrap();
        let hold = row.hold.unwrap();
        assert_eq!(hold.get("message").and_then(Value::as_str), expected);
        if expected.is_none() {
            assert_eq!(
                hold,
                json!({"code":"unavailable","module_id":"data","op":"agent_data.export",
                    "move_error":false})
            );
        }
    }
    published_file(
        "agent-sessions-v1",
        "where-moving-hold-message.json",
        vec![sample],
    );
}

#[tokio::test]
async fn hub_move_restarts_through_the_full_handoff() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    f.source_tick().await;
    f.watch(State::Handed).await;
    f.receiver_tick().await;
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Claimed);
    f.receiver_tick().await;
    f.watch(State::Activated).await;
    // The receiver can clean up while the source is offline. Once the source
    // comes back, its own terminal checkpoint must queue a separate release.
    crate::cut_release::tick(&f.receiver).await.unwrap();
    assert_eq!(f.door_count(B, "cut.release"), 1);
    assert_eq!(f.door_count(A, "cut.release"), 0);
    f.source_tick().await;
    f.watch(State::Released).await;
    crate::cut_release::tick(&f.source).await.unwrap();
    assert_eq!(f.door_count(A, "cut.release"), 1);
    assert_eq!(
        f.source.row(CUT).await.unwrap().plan["_cut_release"]["state"],
        "released"
    );
    f.receiver_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Released);
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Done);
    assert!(f.from.frozen().is_empty());
    assert_eq!(
        f.from
            .tombstones()
            .get(&AgentRef::new("agent", MIGRATED))
            .map(String::as_str),
        Some(B)
    );
    assert!(f.to.held().contains(&AgentRef::new("agent", MIGRATED)));
    assert!(f.from.wire_calls().iter().any(|(method, params)| {
        method == "agent_data.release" && params["holder"] == B && params["holderBox"] == B
    }));
    let calls = f.door.calls();
    assert!(calls.iter().any(
        |call| matches!(&call.request, DoorRequest::Seal(s) if s.receiver.as_deref() == Some(B))
    ));
    assert!(calls.iter().any(|call| matches!(&call.request, DoorRequest::Takeover(t)
        if t.author.as_deref() == Some(A) && t.manifest_sha256.as_deref() == Some("opaque-manifest") && t.sealed_key.as_deref() == Some("opaque-key"))));
    assert_eq!(
        calls
            .iter()
            .filter(|call| matches!(call.request, DoorRequest::Accept(_)))
            .count(),
        1
    );
    let status = f.rig.b.handler.move_status(json!({"cut_id":CUT})).unwrap();
    let status: Value = serde_json::from_slice(&status).unwrap();
    assert!(status["result"].get("losses").is_none());
    let listed = f.rig.b.handler.moves(json!({})).unwrap();
    let listed: Value = serde_json::from_slice(&listed).unwrap();
    assert!(listed["result"]["moves"][0].get("losses").is_none());
}

#[tokio::test]
async fn hub_move_handed_abort_returns_residence_even_with_a_lost_reply() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    f.source_tick().await;
    f.watch(State::Handed).await;
    f.abort();
    let (landed, _release) = Moving::arm_ack_loss(&f.rig.b, "move.return:");
    let mut receiver = f.fresh_receiver();
    let crashed = tokio::spawn(async move { receiver.tick().await });
    within(async { landed.await.unwrap() }).await;
    crashed.abort();
    assert!(crashed.await.unwrap_err().is_cancelled());
    assert_eq!(
        f.receiver.row(CUT).await.unwrap().plan["_receiver"]["hub_decision"],
        "abort"
    );
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.receiver_tick().await;
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Aborted);
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
    assert_eq!(f.a.move_origin(MIGRATED).await.unwrap(), (3, A.into()));
    assert!(f.to.held().is_empty());
}

#[tokio::test]
async fn hub_move_receiver_completes_while_source_is_dead_after_transfer() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    let (landed, _release) = Moving::arm_ack_loss(&f.rig.a, "move.transfer:");
    let mut source = f.fresh_source();
    let crashed = tokio::spawn(async move { source.tick().await });
    within(async { landed.await.unwrap() }).await;
    crashed.abort();
    assert!(crashed.await.unwrap_err().is_cancelled());
    f.watch(State::Handing).await;
    until_async(|| async {
        let seen = f.b.watched_move(CUT).await.unwrap().unwrap();
        matches!(
            f.b.move_position(&seen.body).await,
            Ok(MovePosition::Receiver)
        )
    })
    .await;
    f.receiver_tick().await;
    f.receiver_tick().await;
    f.watch(State::Activated).await;
    f.receiver_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Published);
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Done);
}

#[tokio::test]
async fn hub_move_receiver_abort_fences_a_held_handing_cas_after_source_restart() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    let (held, ready) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let (settled, outcome) = oneshot::channel();
    *f.rig.a.leaf.epoch_gate.lock().unwrap() = Some((
        "move.handing:".into(),
        Gate::HoldKv {
            held,
            release: wait,
            settled,
        },
    ));
    let mut source = f.fresh_source();
    let crashed = tokio::spawn(async move { source.tick().await });
    within(async { ready.await.unwrap() }).await;
    crashed.abort();
    assert!(crashed.await.unwrap_err().is_cancelled());
    assert!(f.source.row(CUT).await.unwrap().plan["_source"]["handing"].is_array());
    f.abort();
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Aborted);
    release.send(()).unwrap();
    within(async {
        assert!(outcome.await.unwrap().is_err());
    })
    .await;
    f.receiver_tick().await;
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
    assert_eq!(f.a.move_origin(MIGRATED).await.unwrap(), (1, A.into()));
}

async fn put_move(
    rig: &IdentityRig,
    revision: u64,
    writer: &str,
    body: &crate::hub_moves::Move,
) -> u64 {
    let key = move_key(&body.cut_id);
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0),
        rig.keys.as_ref(),
        &Header {
            kind: "move".into(),
            key: key.clone(),
            writer: writer.into(),
            writer_ep: 0,
            entry_id: hub_write::new_op_id().unwrap(),
        },
        &json!(body),
    )
    .await
    .unwrap();
    rig.kv
        .update(key, signed.into_bytes().into(), revision)
        .await
        .unwrap()
}

#[tokio::test]
async fn hub_move_reader_refuses_wrong_side_and_changed_agent_without_advancing() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    let published = f.a.watched_move(CUT).await.unwrap().unwrap();
    let mut next = published.body.clone();
    next.state = State::Staged;
    let revision = put_move(&f.rig, published.revision, A, &next).await;
    let store = store_of(&f.rig.b.handler);
    let key = move_key(CUT);
    until(|| {
        store
            .hub_kv_cached(&key)
            .unwrap()
            .is_some_and(|row| row.revision == revision)
    })
    .await;
    let refused = store.hub_kv_cached(&key).unwrap().unwrap();
    assert!(refused.integrity_failed);
    assert_eq!(refused.body.unwrap()["state"], "published");
    next.agent_id = "agent_0000000000000041".into();
    let revision = put_move(&f.rig, revision, B, &next).await;
    until(|| {
        store
            .hub_kv_cached(&key)
            .unwrap()
            .is_some_and(|row| row.revision == revision)
    })
    .await;
    let refused = store.hub_kv_cached(&key).unwrap().unwrap();
    assert!(refused.integrity_failed);
    assert_eq!(refused.body.unwrap()["agent_id"], MIGRATED);
    assert!(f.b.move_member(A).await.unwrap().is_some());
}

#[tokio::test]
async fn hub_move_does_not_seal_when_a_required_export_has_no_target_provider() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.b.advertise_node(Vec::new(), true).await.unwrap();
    until_async(|| async { f.a.watched_node(B).await.ok().flatten().is_some() }).await;
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Exported);
    assert!(!f.from.frozen().is_empty());
    f.source_tick().await;
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(
        row.reason.as_deref(),
        Some("participant_unavailable_on_target")
    );
    assert_eq!(row.detail.unwrap()["detail"]["module"], "data");
    assert!(f.door.calls().is_empty());
    assert!(f.a.watched_move(CUT).await.unwrap().is_none());
    assert!(f.from.frozen().is_empty());
}

#[tokio::test]
async fn hub_move_stop_after_claim_holds_the_fork_without_activating() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    f.source_tick().await;
    f.watch(State::Handed).await;
    let door = f.door.for_author(std::array::from_fn(|i| {
        u8::from_str_radix(&B[2 * i..2 * i + 2], 16).unwrap()
    }));
    door.script(
        "cut.accept",
        crate::fake_door::Effect::Refuse("cut_object_missing".into()),
    );
    f.receiver_tick().await;
    f.receiver_tick().await;
    let row = f.receiver.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Activating);
    assert_eq!(row.plan["_receiver"]["hub_decision"], "activate");
    assert_eq!(row.hold.unwrap()["code"], "cut_object_missing");
    f.receiver_tick().await;
    assert_eq!(
        f.receiver.row(CUT).await.unwrap().step,
        MoveStep::Activating
    );
    assert!(f.to.held().is_empty());
    assert_eq!(f.b.move_origin(MIGRATED).await.unwrap(), (2, B.into()));
}

#[tokio::test]
async fn hub_move_removed_source_in_handing_waits_for_a_forced_take() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    let staged = f.a.watched_move(CUT).await.unwrap().unwrap();
    let mut handing = staged.body.clone();
    handing.state = State::Handing;
    f.a.write_move(&handing, Some(&staged)).await.unwrap();
    f.watch(State::Handing).await;

    let anchor = hub_membership::tests::anchor(A, &f.rig.keys);
    let removal = json!({"box_id":A,"epoch":2,"removal_id":"remove-handing-source","key_history":[anchor.fleet_record_ed25519]});
    let id = hub_write::new_op_id().unwrap();
    let signed = fleet_record::prepare(
        &ClaustrumCredentialResolver::with_target(f.rig.keys.clone(), 0),
        f.rig.keys.as_ref(),
        &Header {
            kind: "member.removed".into(),
            key: LOG_SUBJECT.into(),
            writer: B.into(),
            writer_ep: 0,
            entry_id: id.clone(),
        },
        &removal,
    )
    .await
    .unwrap();
    f.rig
        .b
        .leaf
        .session()
        .append(
            signed.into_bytes(),
            0,
            &id,
            Instant::now() + Duration::from_secs(5),
        )
        .await
        .unwrap();
    until_async(|| async { matches!(f.b.move_member(A).await, Ok(None)) }).await;
    f.receiver_tick().await;
    assert_eq!(
        f.b.watched_move(CUT).await.unwrap().unwrap().body.state,
        State::Handing
    );
    let revision = f
        .rig
        .kv
        .entry(format!("residence.{MIGRATED}"))
        .await
        .unwrap()
        .unwrap()
        .revision;
    put(&f.rig, MIGRATED, revision, B, record(B, B, 2, true)).await;
    until_async(|| async { matches!(f.b.move_position(&handing).await, Ok(MovePosition::Forced)) })
        .await;
    f.receiver_tick().await;
    until_async(|| async {
        f.b.watched_move(CUT)
            .await
            .ok()
            .flatten()
            .is_some_and(|seen| seen.body.state == State::Aborted)
    })
    .await;
    f.receiver_tick().await;
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
    assert!(f.to.staged().is_empty());
    assert!(f.to.held().is_empty());
}

#[tokio::test]
async fn hub_move_reaps_terminal_revisions_at_the_source_and_fleet_deadlines() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    let staged = f.b.watched_move(CUT).await.unwrap().unwrap();
    let key = move_key(CUT);
    let created_ms = |entry: kv::Entry| (entry.created.unix_timestamp_nanos() / 1_000_000) as i64;
    let started = created_ms(f.rig.kv.entry(&key).await.unwrap().unwrap());
    f.b.reap_move(&staged, false, started + crate::hub_moves::WEEK)
        .await
        .unwrap();
    assert_eq!(
        f.rig.kv.entry(&key).await.unwrap().unwrap().revision,
        staged.revision
    );
    f.abort();
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.receiver_tick().await;
    f.source_tick().await;
    let ended = created_ms(f.rig.kv.entry(&key).await.unwrap().unwrap());
    let aborted = f.a.watched_move(CUT).await.unwrap().unwrap();
    f.a.reap_move(&aborted, true, ended + crate::hub_moves::HOUR - 1)
        .await
        .unwrap();
    f.b.reap_move(&aborted, false, ended + crate::hub_moves::WEEK - 1)
        .await
        .unwrap();
    assert_eq!(
        f.rig.kv.entry(&key).await.unwrap().unwrap().revision,
        aborted.revision
    );
    f.a.reap_move(&aborted, true, ended + crate::hub_moves::HOUR)
        .await
        .unwrap();
    assert_eq!(
        f.rig.kv.entry(&key).await.unwrap().unwrap().operation,
        kv::Operation::Delete
    );
}

#[tokio::test]
async fn hub_move_missing_hub_object_aborts_before_claim() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    let door = f.door.for_author(std::array::from_fn(|i| {
        u8::from_str_radix(&B[2 * i..2 * i + 2], 16).unwrap()
    }));
    door.script(
        "cut.takeover",
        crate::fake_door::Effect::Refuse("cut_object_missing".into()),
    );
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    assert_eq!(
        f.receiver.row(CUT).await.unwrap().detail.unwrap()["code"],
        "cut_object_missing"
    );
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.receiver_tick().await;
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
    assert!(f.to.calls().is_empty());
    assert_eq!(f.a.move_origin(MIGRATED).await.unwrap(), (1, A.into()));
}

#[tokio::test]
async fn hub_move_receiver_aborts_its_door_on_preclaim_stop() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    f.to.script(
        "agent_data.import",
        ParticipantEffect::Refuse(Refusal {
            code: "local_advanced".into(),
            detail: None,
            message: None,
        }),
    );
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    // A lost response forces the receiver to repeat the successful abort.
    f.receiver_door()
        .script("cut.abort", DoorEffect::LoseReply(TransportError::NoReply));
    f.finish_receiver_abort().await;
    assert_eq!(f.door_count(B, "cut.abort"), 2);
    assert_eq!(f.door_count(B, "cut.accept"), 0);
    assert!(f.to.staged().is_empty());
}

#[tokio::test]
async fn hub_move_upload_failure_aborts_without_resealing() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.door.script(
        "cut.status",
        DoorEffect::UploadFailed("unavailable".into(), Some(json!({"vaultCode":"closed"}))),
    );
    f.source_tick().await;
    assert!(!f.from.frozen().is_empty());
    f.source_tick().await;
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.reason.as_deref(), Some("unavailable"));
    assert_eq!(row.detail.unwrap()["detail"]["vaultCode"], "closed");
    assert_eq!(f.door_count(A, "cut.seal"), 1);
    assert_eq!(f.door_count(A, "cut.abort"), 1);
    assert!(f.from.frozen().is_empty());
    assert_eq!(row.record_version, 0);
    assert!(f.a.watched_move(CUT).await.unwrap().is_none());
}

#[tokio::test]
async fn hub_move_cut_store_full_aborts_at_seal() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.door
        .script("cut.seal", DoorEffect::Refuse("cut_store_full".into()));
    f.source_tick().await;
    assert!(!f.from.frozen().is_empty());
    f.source_tick().await;
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.reason.as_deref(), Some("cut_store_full"));
    assert_eq!(row.detail.unwrap()["op"], "cut.seal");
    assert_eq!(f.door_count(A, "cut.seal"), 1);
    assert_eq!(f.door_count(A, "cut.abort"), 1);
    assert!(f.from.frozen().is_empty());
    assert_eq!(row.record_version, 0);
    assert!(f.a.watched_move(CUT).await.unwrap().is_none());
}

#[tokio::test]
async fn hub_move_key_unavailable_aborts_at_takeover() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    f.receiver_door()
        .script("cut.takeover", DoorEffect::Refuse("key_unavailable".into()));
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    f.finish_receiver_abort().await;
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.reason.as_deref(), Some("key_unavailable"));
    assert_eq!(f.door_count(B, "cut.takeover"), 1);
    assert_eq!(f.door_count(B, "cut.abort"), 1);
    assert_eq!(f.door_count(A, "cut.abort"), 1);
    assert!(f.from.frozen().is_empty());
}

#[tokio::test]
async fn hub_move_failed_takeover_status_aborts_with_door_detail() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    let door = f.receiver_door();
    door.script(
        "cut.takeover",
        DoorEffect::Takeover(crate::door_client::TakeoverProgress {
            state: crate::door_client::TakeoverState::InProgress,
            session_ref: None,
            code: None,
        }),
    );
    door.script(
        "cut.status",
        DoorEffect::HubTakeoverFailed("unavailable".into(), Some(json!({"vaultCode":"closed"}))),
    );
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    f.receiver_tick().await;
    f.finish_receiver_abort().await;
    let row = f.receiver.row(CUT).await.unwrap();
    assert_eq!(row.reason.as_deref(), Some("unavailable"));
    assert_eq!(row.detail.unwrap()["detail"]["vaultCode"], "closed");
    assert_eq!(f.door_count(B, "cut.takeover"), 1);
    assert_eq!(f.door_count(B, "cut.abort"), 1);
}

#[tokio::test]
async fn hub_move_import_pending_retries_inside_the_step() {
    let Some(mut f) = Moving::new().await else {
        return;
    };
    f.publish().await;
    f.to.script(
        "agent_data.import",
        ParticipantEffect::Refuse(Refusal {
            code: "cut_import_pending".into(),
            detail: None,
            message: None,
        }),
    );
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    let row = f.receiver.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::PickedUp);
    assert!(row.abort_requested.is_none());
    assert_eq!(row.hold.unwrap()["code"], "cut_import_pending");
    f.receiver.clock = || crate::unix_millis() + 1_000;
    f.receiver_tick().await;
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Imported);
    assert_eq!(f.door_count(B, "cut.takeover"), 1);
}

#[tokio::test]
async fn hub_move_cut_accepted_during_abort_holds_the_fork() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    f.abort();
    f.receiver_door()
        .script("cut.abort", DoorEffect::Refuse("cut_accepted".into()));
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.receiver_tick().await;
    let row = f.receiver.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Staged);
    assert_eq!(row.hold.unwrap()["code"], "cut_accepted");
    assert_eq!(
        row.plan["_receiver"]["activation_fork"]["code"],
        "cut_accepted"
    );
    assert!(f.to.staged().is_empty());
    f.receiver_tick().await;
    assert_eq!(f.door_count(B, "cut.abort"), 1);
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Staged);
}

#[tokio::test]
async fn hub_move_basal_import_drops_reach_move_status() {
    let Some(f) = Moving::new().await else {
        return;
    };
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/basal-agent-data-v1/receiver-flows.json");
    let samples: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let imported = samples
        .as_array()
        .unwrap()
        .iter()
        .find(|sample| sample["name"] == "import-with-drops")
        .unwrap()["reply"]["result"]
        .clone();
    f.to.set_import(serde_json::from_value(imported).unwrap());

    f.stage().await;
    f.source_tick().await;
    f.watch(State::Handed).await;
    f.receiver_tick().await;
    f.receiver_tick().await;
    f.watch(State::Activated).await;
    f.source_tick().await;
    f.watch(State::Released).await;
    f.receiver_tick().await;

    let expected = json!([{
        "participant":"data",
        "session_ref":"d5acbf637b6119cbd2a2b3f32cc6f140",
        "reason":"queued_event",
        "count":1
    }]);
    assert_eq!(
        json!(crate::move_receiver::import_losses(
            &f.receiver.row(CUT).await.unwrap()
        )),
        expected
    );
    let bytes = f.rig.b.handler.move_status(json!({"cut_id":CUT})).unwrap();
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(reply["result"]["losses"], expected);
}

#[tokio::test]
async fn hub_move_basal_local_advanced_aborts_as_diverged() {
    let Some(f) = Moving::new().await else {
        return;
    };
    let path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests/fixtures/basal-agent-data-v1/receiver-refusals.json");
    let samples: Value = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    let error = samples
        .as_array()
        .unwrap()
        .iter()
        .find(|sample| sample["name"] == "import-flow-id-clash")
        .unwrap()["error"]
        .clone();
    let body: subc_protocol::ErrorBody = serde_json::from_value(error).unwrap();
    f.to.script(
        "agent_data.import",
        ParticipantEffect::Refuse(Refusal {
            code: body.code.clone(),
            detail: body.detail.clone(),
            message: Some(body.message.clone()),
        }),
    );

    f.publish().await;
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    f.receiver_tick().await;
    f.source_tick().await;

    let receiver = f.receiver.row(CUT).await.unwrap();
    assert_eq!(receiver.step, MoveStep::Aborted);
    assert_eq!(receiver.reason.as_deref(), Some("diverged"));
    let source = f.source.row(CUT).await.unwrap();
    assert_eq!(source.step, MoveStep::Aborted);
    assert_eq!(source.reason.as_deref(), Some("diverged"));
    assert_eq!(source.detail.as_ref().unwrap()["module_id"], "data");
    assert_eq!(source.detail.as_ref().unwrap()["op"], "agent_data.import");
    assert_eq!(source.detail.as_ref().unwrap()["code"], "local_advanced");
}

#[tokio::test]
async fn refused_cleanup_backs_off_doubling_to_the_cap_then_aborts() {
    let Some(mut f) = Moving::new().await else {
        return;
    };
    set_controlled_now(0);
    f.receiver.clock = controlled_clock;
    f.publish().await;
    poll_hub(&f.receiver, &f.b).await.unwrap();
    f.receiver_tick().await;
    f.abort();
    f.receiver_tick().await;
    f.watch(State::Aborted).await;
    for _ in 0..11 {
        f.to.script(
            "agent_data.abort",
            ParticipantEffect::Refuse(Refusal {
                code: "invalid_request".into(),
                detail: None,
                message: None,
            }),
        );
    }

    let abort_calls = || {
        f.to.calls()
            .iter()
            .filter(|request| request.op().method() == "agent_data.abort")
            .count()
    };
    let mut calls = Vec::new();
    let mut driver = f.fresh_receiver();
    for second in 0..1300 {
        if f.receiver.row(CUT).await.unwrap().step == MoveStep::Aborted {
            break;
        }
        let before = abort_calls();
        driver.tick().await.unwrap();
        for _ in before..abort_calls() {
            calls.push(controlled_clock());
        }
        if second == 20 {
            // A restarted driver reads the saved wait instead of starting over.
            driver = f.fresh_receiver();
        }
        advance_controlled(1000);
    }
    let gaps: Vec<i64> = calls.windows(2).map(|w| w[1] - w[0]).collect();
    assert_eq!(
        gaps,
        vec![
            1_000, 2_000, 4_000, 8_000, 16_000, 32_000, 64_000, 128_000, 256_000, 300_000, 300_000
        ],
        "abort calls at {calls:?}"
    );
    assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Aborted);
}

#[tokio::test]
async fn hub_move_prepare_refusals_retry_or_abort() {
    // Eight transient busy answers recover without exporting before the retry.
    let Some(mut f) = Moving::new().await else {
        return;
    };
    set_controlled_now(0);
    f.source.clock = controlled_clock;
    for _ in 0..8 {
        f.from.script(
            "agent_data.prepare",
            ParticipantEffect::Refuse(Refusal {
                code: "busy".into(),
                detail: None,
                message: Some("still running".into()),
            }),
        );
    }
    for attempt in 0..8 {
        f.source_tick().await;
        assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Preparing);
        assert!(!f
            .from
            .calls()
            .iter()
            .any(|call| call.op().method() == "agent_data.export"));
        let delay = crate::move_driver::backoff(attempt + 1);
        advance_controlled(delay.as_millis() as i64);
    }
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Exported);
    assert_eq!(
        f.from
            .calls()
            .iter()
            .filter(|call| call.op().method() == "agent_data.prepare")
            .count(),
        9
    );

    // A terminal participant failure names the participant and never seals.
    let Some(f) = Moving::new().await else {
        return;
    };
    f.from.script(
        "agent_data.prepare",
        ParticipantEffect::Refuse(Refusal {
            code: "participant_failed".into(),
            detail: Some(json!({"reason":"fixture failed"})),
            message: Some("keep this message".into()),
        }),
    );
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.detail.as_ref().unwrap()["module_id"], "data");
    assert_eq!(row.detail.as_ref().unwrap()["op"], "agent_data.prepare");
    assert_eq!(row.detail.as_ref().unwrap()["message"], "keep this message");
    assert!(!f
        .door
        .calls()
        .iter()
        .any(|call| call.request.op() == "cut.seal"));

    // An out-of-contract refusal aborts instead of retrying after restart.
    let Some(f) = Moving::new().await else {
        return;
    };
    f.from.script(
        "agent_data.prepare",
        ParticipantEffect::Refuse(Refusal {
            code: "future_unlisted_code".into(),
            detail: None,
            message: None,
        }),
    );
    f.source_tick().await;
    let before = f
        .from
        .calls()
        .iter()
        .filter(|call| call.op().method() == "agent_data.prepare")
        .count();
    f.fresh_source().tick().await.unwrap();
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(
        f.from
            .calls()
            .iter()
            .filter(|call| call.op().method() == "agent_data.prepare")
            .count(),
        before
    );
}

#[tokio::test]
async fn hub_move_activation_failures_hold_the_fork_across_restart() {
    for code in [
        "participant_failed",
        "digest_mismatch",
        "local_advanced",
        "future_unlisted_code",
    ] {
        let Some(f) = Moving::new().await else {
            return;
        };
        f.stage().await;
        f.source_tick().await;
        f.watch(State::Handed).await;
        f.receiver_tick().await;
        f.to.script(
            "agent_data.activate",
            ParticipantEffect::Refuse(Refusal {
                code: code.into(),
                detail: None,
                message: Some("activation failed".into()),
            }),
        );
        f.receiver_tick().await;
        let before = f.to.calls().len();
        f.fresh_receiver().tick().await.unwrap();
        let row = f.receiver.row(CUT).await.unwrap();
        assert_eq!(row.step, MoveStep::Activating, "{code}");
        assert!(
            row.plan["_receiver"]["activation_fork"].is_object()
                || row.plan["_receiver"]["contract_violation"].is_object(),
            "{code}: {:?}",
            row.plan
        );
        assert_eq!(f.to.calls().len(), before, "{code} retried activation");
        assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Published);
    }
}

fn configure_admission(f: &mut Moving) {
    store_of(&f.rig.a.handler)
        .apply_entry("fixture.clear_move", "{}", "fixture", None, |tx| {
            tx.execute("DELETE FROM agent_move WHERE cut_id=?1", [CUT])?;
            Ok(())
        })
        .unwrap();
    let resolver = FakeResolver::new();
    resolver.set(
        ORCHESTRATOR_CAPABILITY,
        ProviderResolution::Provider("prefrontal".into()),
    );
    f.rig.a.handler.provider_resolver = resolver;
    f.rig.a.handler.participant_discovery = f.source.discovery.clone();
    f.rig.a.handler.route_admissions().insert(
        ADMIT,
        crate::RouteAdmission::from_bind(Some(crate::tests::reserved("prefrontal")), None),
    );
    let mut liveness = f.rig.a.handler.liveness.lock().unwrap();
    liveness.bus.watch_started();
    liveness.bus.finish_replay();
    crate::liveness_bus::tests::reconcile(&mut liveness);
}

async fn admit(f: &Moving, key: &str) -> Result<String, HandlerError> {
    let bytes = f
        .rig
        .a
        .handler
        .admit_move(
            json!({"agent_id":MIGRATED,"to":B,"request_key":key}),
            ADMIT,
            Instant::now(),
        )
        .await?;
    let reply: Value = serde_json::from_slice(&bytes).unwrap();
    Ok(reply["result"]["cut_id"].as_str().unwrap().into())
}

#[tokio::test]
async fn hub_move_admission_routes_agent_ref_and_replays_key() {
    let Some(mut f) = Moving::new().await else {
        return;
    };
    f.from.set_optional();
    configure_admission(&mut f);
    let cut = admit(&f, "admitted").await.unwrap();
    assert_eq!(admit(&f, "admitted").await.unwrap(), cut);
    let row = store_of(&f.rig.a.handler)
        .agent_move(&cut)
        .unwrap()
        .unwrap();
    assert_eq!(row.request_key.as_deref(), Some("admitted"));
    assert_eq!(row.plan["hub_gen"], 1);
    assert_eq!(row.plan["participants"][0]["optional"], true);
    assert_eq!(
        row.plan["participants"][0]["refs"],
        json!([{"ref_kind":"agent","ref":MIGRATED}])
    );
}

#[tokio::test]
async fn hub_move_live_session_ref_cap_is_256() {
    for count in [256, 257] {
        let Some(mut f) = Moving::new_with_kinds(&["session"]).await else {
            return;
        };
        configure_admission(&mut f);
        f.from = Arc::new(FakeParticipant::new(
            &["session"],
            (0..count)
                .map(|index| AgentRef::new("session", format!("session-{index}")))
                .collect(),
        ));
        f.source = worker(&f.rig.a, &f.door, f.from.clone());
        f.rig.a.handler.participant_discovery = f.source.discovery.clone();
        for index in 0..count {
            let reference = format!("session-{index}");
            store_of(&f.rig.a.handler)
                .register_session(&entorhinal_core::SessionRegistration {
                    role: None,
                    agent_id: MIGRATED,
                    ref_kind: "session",
                    session_ref: &reference,
                    harness: "host",
                    session_id: &reference,
                    project_root: None,
                    registered_by: "host",
                    runner: Some("data"),
                    network: None,
                    now_ms: 1000,
                })
                .unwrap();
        }

        let outcome = admit(&f, "ref-cap").await;
        if count == 256 {
            let cut = outcome.unwrap();
            let row = store_of(&f.rig.a.handler)
                .agent_move(&cut)
                .unwrap()
                .unwrap();
            assert_eq!(
                row.plan["participants"][0]["refs"]
                    .as_array()
                    .unwrap()
                    .len(),
                256
            );
        } else {
            assert_eq!(outcome.unwrap_err().code, "live_ref_cap");
            assert!(store_of(&f.rig.a.handler)
                .list_agent_moves()
                .unwrap()
                .is_empty());
            assert!(!f
                .from
                .calls()
                .iter()
                .any(|call| call.op().method() == "agent_data.busy"));
        }
    }
}

#[tokio::test]
async fn hub_move_oversized_prepare_aborts_before_export_or_seal() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.from.set_prepare(
        serde_json::from_value(json!({"items":[],"external":[{
            "ref_kind":"agent","ref":MIGRATED,
            "note":"x".repeat(entorhinal_core::move_record::MOVE_BODY_CAP),
            "check":{}
        }]}))
        .unwrap(),
    );
    f.source_tick().await;
    f.source_tick().await;
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert!(!f
        .from
        .calls()
        .iter()
        .any(|call| call.op().method() == "agent_data.export"));
    assert!(!f
        .door
        .calls()
        .iter()
        .any(|call| call.request.op() == "cut.seal"));
}

#[tokio::test]
async fn hub_move_diverged_advanced_and_unknown_abort_before_import() {
    for diverged in [
        cortexkit_role_agent_data::diverged::DivergedState::Advanced,
        cortexkit_role_agent_data::diverged::DivergedState::Unknown,
    ] {
        let Some(f) = Moving::new().await else {
            return;
        };
        f.to.hold(AgentRef::new("agent", MIGRATED));
        f.to.set_diverged(AgentRef::new("agent", MIGRATED), diverged);
        f.publish().await;
        poll_hub(&f.receiver, &f.b).await.unwrap();
        f.receiver_tick().await;
        f.receiver_tick().await;
        f.watch(State::Aborted).await;
        f.receiver_tick().await;
        assert_eq!(f.receiver.row(CUT).await.unwrap().step, MoveStep::Aborted);
        assert!(!f
            .to
            .calls()
            .iter()
            .any(|call| call.op().method() == "agent_data.import"));
        f.source_tick().await;
        assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Aborted);
    }
}

#[tokio::test]
async fn hub_move_import_refusal_retry_survives_restart() {
    for code in ["external_not_present", "busy", "cut_not_ready"] {
        let Some(mut f) = Moving::new().await else {
            return;
        };
        set_controlled_now(0);
        f.receiver.clock = controlled_clock;
        let refused_op = if code == "external_not_present" {
            "agent_data.diverged"
        } else {
            "agent_data.import"
        };
        f.to.script(
            refused_op,
            ParticipantEffect::Refuse(Refusal {
                code: code.into(),
                detail: None,
                message: (code == "busy").then(|| format!("\t{}", "a".repeat(512))),
            }),
        );
        f.publish().await;
        poll_hub(&f.receiver, &f.b).await.unwrap();
        f.receiver_tick().await;
        let row = f.receiver.row(CUT).await.unwrap();
        assert_eq!(row.step, MoveStep::PickedUp);
        let hold = row.hold.unwrap();
        if code == "busy" {
            assert_eq!(hold["message"], format!(" {}", "a".repeat(511)));
            assert_eq!(hold["detail"]["message"], hold["message"]);
        } else {
            assert!(hold.get("message").is_none());
            assert!(hold["detail"].get("message").is_none());
        }
        advance_controlled(1000);
        f.fresh_receiver().tick().await.unwrap();
        let row = f.receiver.row(CUT).await.unwrap();
        assert_eq!(
            row.step,
            MoveStep::Imported,
            "{code}: receiver={:?} hold={:?}",
            row.plan["_receiver"],
            row.hold
        );
        let imports =
            f.to.calls()
                .iter()
                .filter(|call| call.op().method() == "agent_data.import")
                .count();
        assert_eq!(
            imports,
            if code == "external_not_present" { 1 } else { 2 },
            "{code}"
        );
        if code == "external_not_present" {
            assert_eq!(
                f.to.calls()
                    .iter()
                    .filter(|call| call.op().method() == "agent_data.diverged")
                    .count(),
                2
            );
        }
    }
}

#[tokio::test]
async fn hub_move_admission_idle_session_requires_its_freezing_runner() {
    for (runner, state, admitted) in [
        (Some("data"), "idle", true),
        (Some("data"), "active", false),
        (None, "idle", false),
        (Some("elsewhere"), "idle", false),
    ] {
        let Some(mut f) = Moving::new_with_kinds(&["session"]).await else {
            return;
        };
        configure_admission(&mut f);
        f.from = Arc::new(FakeParticipant::new(
            &["session"],
            vec![AgentRef::new("session", "r")],
        ));
        f.source = worker(&f.rig.a, &f.door, f.from.clone());
        f.rig.a.handler.participant_discovery = f.source.discovery.clone();
        store_of(&f.rig.a.handler)
            .apply_entry("fixture.binding", "{}", "fixture", None, |tx| {
                tx.execute(
                    "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,runner) VALUES('session','r',?1,'host','r','/project','host',1000,?2)",
                    rusqlite::params![MIGRATED, runner],
                )?;
                Ok(())
            })
            .unwrap();
        {
            let mut liveness = f.rig.a.handler.liveness.lock().unwrap();
            liveness
                .bus
                .apply(crate::liveness_bus::tests::put(
                    "host.r", "host", "r", state,
                ))
                .unwrap();
            crate::liveness_bus::tests::reconcile(&mut liveness);
        }
        let outcome = admit(&f, "idle-session").await;
        assert_eq!(outcome.is_ok(), admitted, "{runner:?} {state}: {outcome:?}");
        assert_eq!(
            store_of(&f.rig.a.handler).list_agent_moves().unwrap().len(),
            usize::from(admitted)
        );
    }
}

#[tokio::test]
async fn hub_move_activation_boundary_retries_without_abort() {
    for code in [
        "busy",
        "external_not_present",
        "unavailable",
        "invalid_state",
    ] {
        let Some(mut f) = Moving::new().await else {
            return;
        };
        set_controlled_now(0);
        f.receiver.clock = controlled_clock;
        f.stage().await;
        f.source_tick().await;
        f.watch(State::Handed).await;
        f.receiver_tick().await;
        f.to.script(
            "agent_data.activate",
            ParticipantEffect::Refuse(Refusal {
                code: code.into(),
                detail: None,
                message: None,
            }),
        );
        f.receiver_tick().await;
        let row = f.receiver.row(CUT).await.unwrap();
        assert_eq!(row.step, MoveStep::Activating, "{code}");
        assert!(row.abort_requested.is_none(), "{code}");
        assert_eq!(row.hold.as_ref().unwrap()["code"], code);
        assert_eq!(row.plan["_receiver"]["retry_at_ms"], 1000);
        let calls = f.to.calls().len();
        f.fresh_receiver().tick().await.unwrap();
        assert_eq!(
            f.to.calls().len(),
            calls,
            "{code} retried before its deadline"
        );
        advance_controlled(1000);
        f.fresh_receiver().tick().await.unwrap();
        assert_eq!(
            f.receiver.row(CUT).await.unwrap().step,
            MoveStep::Activated,
            "{code}"
        );
        assert!(!f
            .to
            .calls()
            .iter()
            .any(|call| call.op().method() == "agent_data.abort"));
    }
}

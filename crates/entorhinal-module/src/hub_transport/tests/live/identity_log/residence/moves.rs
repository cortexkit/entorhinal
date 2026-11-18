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
};
use entorhinal_core::move_store::{AgentMove, MoveSide, MoveStep};

const CUT: &str = "aabb0123456789abcdef0123456789ab";

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
        records: handler.record_client.clone(),
        door: door.client(),
        discovery: Arc::new(Discovery(daemon, peer)),
        catalog: None,
        health: handler.health.clone(),
        watches: handler.watches.clone(),
        writer: handler.writer.clone(),
        clock: crate::unix_millis,
        wake: handler.move_wake.clone(),
    }
}

impl Moving {
    async fn new() -> Option<Self> {
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
        until(|| a.move_origin(MIGRATED).is_ok() && b.move_member(B).ok().flatten().is_some())
            .await;
        let author = |id: &str| {
            std::array::from_fn(|i| u8::from_str_radix(&id[2 * i..2 * i + 2], 16).unwrap())
        };
        let door = FakeDoor::new(author(A));
        let from = Arc::new(FakeParticipant::new(
            &["agent"],
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
        let to = Arc::new(FakeParticipant::new(&["agent"], vec![]));
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
        assert_eq!(self.a.move_origin(MIGRATED).unwrap(), (1, A.into()));
    }
    async fn watch(&self, state: State) {
        until(|| {
            [&self.a, &self.b].iter().all(|hub| {
                hub.watched_move(CUT)
                    .ok()
                    .flatten()
                    .is_some_and(|record| record.body.state == state)
            })
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
    assert_eq!(f.a.move_origin(MIGRATED).unwrap(), (3, A.into()));
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
    until(|| {
        let seen = f.b.watched_move(CUT).unwrap().unwrap();
        matches!(f.b.move_position(&seen.body), Ok(MovePosition::Receiver))
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
    assert_eq!(f.a.move_origin(MIGRATED).unwrap(), (1, A.into()));
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
    let published = f.a.watched_move(CUT).unwrap().unwrap();
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
    assert!(f.b.move_member(A).unwrap().is_some());
}

#[tokio::test]
async fn hub_move_does_not_seal_when_a_required_export_has_no_target_provider() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.b.advertise_node(Vec::new(), true).await.unwrap();
    until(|| f.a.watched_node(B).ok().flatten().is_some()).await;
    f.source_tick().await;
    assert_eq!(f.source.row(CUT).await.unwrap().step, MoveStep::Exported);
    assert!(!f.from.frozen().is_empty());
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
    assert!(f.a.watched_move(CUT).unwrap().is_none());
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
    assert_eq!(f.b.move_origin(MIGRATED).unwrap(), (2, B.into()));
}

#[tokio::test]
async fn hub_move_removed_source_in_handing_waits_for_a_forced_take() {
    let Some(f) = Moving::new().await else {
        return;
    };
    f.stage().await;
    let staged = f.a.watched_move(CUT).unwrap().unwrap();
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
    until(|| matches!(f.b.move_member(A), Ok(None))).await;
    f.receiver_tick().await;
    assert_eq!(
        f.b.watched_move(CUT).unwrap().unwrap().body.state,
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
    until(|| matches!(f.b.move_position(&handing), Ok(MovePosition::Forced))).await;
    f.receiver_tick().await;
    until(|| {
        f.b.watched_move(CUT)
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
    let staged = f.b.watched_move(CUT).unwrap().unwrap();
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
    let aborted = f.a.watched_move(CUT).unwrap().unwrap();
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
    assert_eq!(f.a.move_origin(MIGRATED).unwrap(), (1, A.into()));
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
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.reason.as_deref(), Some("unavailable"));
    assert_eq!(row.detail.unwrap()["detail"]["vaultCode"], "closed");
    assert_eq!(f.door_count(A, "cut.seal"), 1);
    assert_eq!(f.door_count(A, "cut.abort"), 1);
    assert!(f.from.frozen().is_empty());
    assert_eq!(row.record_version, 0);
    assert!(f.a.watched_move(CUT).unwrap().is_none());
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
    let row = f.source.row(CUT).await.unwrap();
    assert_eq!(row.step, MoveStep::Aborted);
    assert_eq!(row.reason.as_deref(), Some("cut_store_full"));
    assert_eq!(row.detail.unwrap()["op"], "cut.seal");
    assert_eq!(f.door_count(A, "cut.seal"), 1);
    assert_eq!(f.door_count(A, "cut.abort"), 1);
    assert!(f.from.frozen().is_empty());
    assert_eq!(row.record_version, 0);
    assert!(f.a.watched_move(CUT).unwrap().is_none());
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

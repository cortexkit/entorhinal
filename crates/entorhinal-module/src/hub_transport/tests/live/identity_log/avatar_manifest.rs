use super::*;
use crate::{served_vectors::Approve, RouteAdmission, RouteKey};

const ROUTE: RouteKey = (126, 1);
const IDS: [&str; 3] = [
    "agent_0000000000000001",
    "agent_0000000000000002",
    "agent_0000000000000003",
];

async fn setup(rig: &mut IdentityRig) -> Arc<Approve> {
    let confirmer = Arc::new(Approve {
        summaries: StdMutex::default(),
    });
    rig.a.handler.operator_confirmer = confirmer.clone();
    *rig.a.handler.hub_shared_log.lock().unwrap() = Some(rig.a.log.clone());
    rig.a.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission {
            principal: Some(subc_protocol::Principal::Direct),
            ..Default::default()
        },
    );
    residence::residence(&rig.a, Arc::new(residence::Image::default()));
    within(async {
        while !rig.a.handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    for (index, id) in IDS.into_iter().enumerate() {
        let mut create = AgentOperation::create(index as u64 + 1, format!("Agent {index}"));
        assert_eq!(create.new_id.as_deref(), Some(id));
        create.params["avatar"] =
            json!({"genome":"a".repeat(32_768),"type":"creature.wild","version":1});
        create.plan(&rig.a).append().await.unwrap();
        within(through(&rig.a.handler, index as u64 + 1)).await;
    }
    confirmer
}

fn manifest() -> Value {
    let entries: Vec<_> = IDS
        .into_iter()
        .map(|id| {
            json!({"agent_id":id,"avatar_genome":"b".repeat(32_768),"avatar_type":"creature.wild","avatar_version":2})
        })
        .collect();
    json!({"request_key":"avatar-batch","replace":true,"entries":entries})
}

async fn apply(node: &Node, params: Value) -> HandlerOutcome {
    node.handler
        .handle_served_request(&body("agent.apply_avatar_manifest", params), ROUTE)
        .await
}

fn agent(node: &Node, id: &str) -> entorhinal_core::agent::AgentRow {
    node.log.sql.test_store().agent_row(id).unwrap().unwrap()
}

fn assert_avatars(node: &Node, versions: [i64; 3]) {
    for (id, version) in IDS.into_iter().zip(versions) {
        let avatar = agent(node, id).avatar.unwrap();
        assert_eq!(avatar.version, Some(version), "avatar version for {id}");
        assert_eq!(avatar.avatar_type, "creature.wild");
        let genome = if version == 1 { "a" } else { "b" };
        assert_eq!(
            avatar.genome,
            genome.repeat(32_768),
            "avatar genome for {id}"
        );
    }
}

#[tokio::test]
async fn hub_avatar_manifest_writes_all_entries_and_replays_without_writing() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let confirmer = setup(&mut rig).await;
    assert_avatars(&rig.a, [1, 1, 1]);

    let first = result(apply(&rig.a, manifest()).await);
    assert_eq!(first["applied"], 3);
    assert_eq!(first["remaining"], json!([]));
    assert_eq!(first["skipped"], json!([]));
    let written = first["written"].as_array().unwrap();
    assert_eq!(written.len(), IDS.len());
    for (row, id) in written.iter().zip(IDS) {
        assert_eq!(row["agent_id"], id);
    }
    within(through(&rig.b.handler, 6)).await;
    assert_avatars(&rig.a, [2, 2, 2]);
    assert_avatars(&rig.b, [2, 2, 2]);

    let replay = result(apply(&rig.a, manifest()).await);
    assert_eq!(replay, first);

    let mut changed = manifest();
    changed["entries"][1]["avatar_version"] = json!(3);
    let conflict = apply(&rig.a, changed).await;
    assert!(
        matches!(&conflict, HandlerOutcome::Error { code, .. } if code == "request_key_conflict"),
        "{conflict:?}"
    );
    assert_eq!(rig.stream.info().await.unwrap().state.last_sequence, 6);
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn hub_avatar_manifest_stops_after_another_computer_changes_a_planned_agent() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let confirmer = setup(&mut rig).await;
    let (committed, first_written) = oneshot::channel();
    let (release, released) = oneshot::channel();
    *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(committed, released));

    // Hold the first append's reply until the other computer has committed
    // its rename, so the rename lands before the batch tries its second write.
    let (outcome, ()) = within(async {
        tokio::join!(apply(&rig.a, manifest()), async {
            first_written.await.unwrap();
            through(&rig.b.handler, 4).await;
            AgentOperation {
                method: "agent.rename",
                params: json!({"agent_id":IDS[1],"name":"Renamed elsewhere","request_key":"rename-between-writes"}),
                time: 800,
                new_id: None,
            }
            .plan(&rig.b)
            .append()
            .await
            .unwrap();
            release.send(()).unwrap();
        })
    })
    .await;
    let HandlerOutcome::ErrorWithDetail { code, detail, .. } = outcome else {
        panic!("expected a stale approval with partial progress, got {outcome:?}");
    };
    assert_eq!(code, "operator_approval_stale");
    let batch = &detail["batch"];
    assert_eq!(batch["applied"], 1);
    assert_eq!(batch["written"][0]["agent_id"], IDS[0]);
    assert_eq!(batch["remaining"].as_array().unwrap().len(), 2);
    assert_eq!(batch["remaining"][0]["agent_id"], IDS[1]);
    assert_eq!(batch["remaining"][1]["agent_id"], IDS[2]);
    assert_eq!(rig.stream.info().await.unwrap().state.last_sequence, 5);
    assert_eq!(agent(&rig.a, IDS[1]).name, "Renamed elsewhere");
    assert_avatars(&rig.a, [2, 1, 1]);

    let resumed = result(apply(&rig.a, manifest()).await);
    assert_eq!(resumed["applied"], 3);
    assert_eq!(resumed["written"][0], batch["written"][0]);
    assert_eq!(resumed["remaining"], json!([]));
    assert_eq!(rig.stream.info().await.unwrap().state.last_sequence, 7);
    assert_avatars(&rig.a, [2, 2, 2]);
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 2);
}

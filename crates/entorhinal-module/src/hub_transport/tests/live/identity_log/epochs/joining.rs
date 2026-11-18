use super::*;

async fn stop(node: &EpochNode) {
    let tail = node
        .node
        .handler
        .catch_up_task
        .lock()
        .unwrap()
        .take()
        .unwrap();
    tail.abort();
    assert!(tail.await.unwrap_err().is_cancelled());
}

async fn snapshot(node: &EpochNode) {
    let landed = node.node.log.write_epoch_step(
        "join-test-snapshot", "join-test-snapshot", &hub_write::Step {
            name: "shared.snapshot".into(), key: LOG_SUBJECT.into(), expected: 0,
            payload: hub_write::Payload::Record {
                kind: "shared.snapshot".into(),
                body: json!({"op":"shared.snapshot","snapshot_id":"initial","part":1,"parts":1,"tables":{}}),
                snapshot_id: None,
            },
        }, deadline(),
    ).await.unwrap();
    within(through(&node.node.handler, landed.position.unwrap())).await;
}

async fn read_body(rig: &IdentityRig, key: &str) -> Value {
    let bytes = rig.kv.get(key).await.unwrap().unwrap();
    UnverifiedRecord::parse(key, &bytes)
        .unwrap()
        .unsealed()
        .unwrap()
        .clone()
}

async fn membership_records(rig: &IdentityRig, kind: &str, box_id: &str) -> Vec<(u64, u64, Value)> {
    let leaf = &rig.a.leaf;
    let mut matches = Vec::new();
    for position in 1..=leaf.log_head(deadline()).await.unwrap() {
        let bytes = leaf.log_entry(position, deadline()).await.unwrap().unwrap();
        let record = UnverifiedRecord::parse(LOG_SUBJECT, &bytes).unwrap();
        if record.header().kind == kind && record.unsealed().unwrap()["box_id"] == box_id {
            matches.push((
                position,
                record.header().writer_ep,
                record.unsealed().unwrap().clone(),
            ));
        }
    }
    matches
}

async fn install_request(rig: &IdentityRig, node: &EpochNode, body: Value) -> Vec<u8> {
    let box_id = node.node.log.box_id.to_string();
    let key = format!("wrapreq.{box_id}");
    let bytes = signed_kv(rig, node, &key, 0, &body).await;
    rig.kv.put(&key, bytes.clone().into()).await.unwrap();
    bytes
}

async fn signed_kv(
    rig: &IdentityRig,
    node: &EpochNode,
    key: &str,
    episode: u64,
    body: &Value,
) -> Vec<u8> {
    let signer = ClaustrumCredentialResolver::with_target(rig.keys.clone(), 0);
    let record = crate::fleet_record::prepare(
        &signer,
        node.vault.as_ref(),
        &crate::fleet_record::Header {
            key: key.into(),
            kind: key.split('.').next().unwrap().into(),
            writer: node.node.log.box_id.to_string(),
            writer_ep: episode,
            entry_id: hub_write::new_op_id().unwrap(),
        },
        body,
    )
    .await
    .unwrap();
    record.into_bytes()
}

async fn rotate(node: &EpochNode, id: &str, removal: &str) {
    node.controller
        .deliver(&event(id, removal), deadline())
        .await
        .unwrap();
}

#[tokio::test]
async fn hub_join_wiped_removed_box_discovers_closure_and_publishes_its_new_episode_key() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    snapshot(&a).await;
    within(async {
        while membership_records(&rig, "member.key", B).await.is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    stop(&b).await;
    let stale = "0".repeat(32);
    install_request(&rig, &b, json!({"v":1,"request_id":stale})).await;
    roster(&fleet, &rig, &[A, C, D]);
    // Lose the minter after KV commit but before member.removed reaches the log.
    kill_minter_at(&a, "epoch.commit:", event(B, "600:permanent:12")).await;
    assert_eq!(a.fence().await, 2);
    assert!(history(&rig).await.is_empty());
    roster(&fleet, &rig, &[A, B, C, D]);
    a.controller.joins(deadline()).await.unwrap();
    assert!(!register(&rig).await["recipients"]
        .as_array()
        .unwrap()
        .contains(&json!(B)));
    assert!(a.vault.state.lock().unwrap().joins_served.is_empty());
    let watch = a.watch();
    within(async {
        while history(&rig).await.is_empty() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    drop(watch);
    a.controller.joins(deadline()).await.unwrap();
    assert!(!register(&rig).await["recipients"]
        .as_array()
        .unwrap()
        .contains(&json!(B)));
    assert!(membership_records(&rig, "member.admitted", B)
        .await
        .is_empty());
    AgentOperation::create(50, "Before admission".into())
        .plan(&a.node)
        .append()
        .await
        .unwrap();

    let wiped = EpochNode::start(&rig, "removed-and-wiped", b.node.leaf.clone(), fleet, None);
    let request = format!("wrapreq.{B}");
    wiped.controller.joins(deadline()).await.unwrap();
    let body = read_body(&rig, &request).await;
    assert_ne!(body["request_id"], stale);
    assert_eq!(body["after_removal_id"], "600:permanent:12");
    assert_eq!(wiped.node.log.applied_sequence().unwrap(), 0);
    a.controller.joins(deadline()).await.unwrap();
    assert!(register(&rig).await["recipients"]
        .as_array()
        .unwrap()
        .contains(&json!(B)));
    assert_eq!(membership_records(&rig, "member.key", B).await.len(), 1);
    assert!(wiped.node.log.writer_episode().is_err());
    let admitted = membership_records(&rig, "member.admitted", B).await;
    assert_eq!(admitted.len(), 1);
    assert_eq!(admitted[0].2["request_id"], body["request_id"]);
    within(through(&a.node.handler, admitted[0].0)).await;
    let probe_key = format!("wrapjoin.{A}.verification-probe");
    let probe = signed_kv(&rig, &wiped, &probe_key, 1, &json!({"v":1})).await;
    let sight = crate::hub_membership::Sight::ReplacesCached;
    assert_eq!(
        a.node
            .log
            .verify_kv(&probe_key, &probe, sight, deadline())
            .await
            .err()
            .unwrap()
            .code,
        "signature_invalid"
    );
    let _watch = wiped.watch();
    within(async {
        while wiped.node.log.writer_episode().ok() != Some(1) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let keys = membership_records(&rig, "member.key", B).await;
    assert_eq!(keys.len(), 2);
    assert!(keys[1].0 > admitted[0].0);
    assert_eq!(keys[1].1, 1);
    within(through(&a.node.handler, keys[1].0)).await;
    assert!(
        a.node
            .log
            .verify_kv(&probe_key, &probe, sight, deadline())
            .await
            .unwrap()
            .writer_current
    );
    let old_episode = signed_kv(&rig, &wiped, &probe_key, 0, &json!({"v":1})).await;
    assert_eq!(
        a.node
            .log
            .verify_kv(&probe_key, &old_episode, sight, deadline())
            .await
            .err()
            .unwrap()
            .code,
        "member_episode_invalid"
    );
    assert_eq!(
        wiped.vault.state.lock().unwrap().joins_imported,
        [vec![1, 2]]
    );
    a.controller.joins(deadline()).await.unwrap();
    assert_eq!(
        membership_records(&rig, "member.admitted", B).await.len(),
        1
    );
    wiped.controller.joins(deadline()).await.unwrap();
    assert!(rig.kv.get(&request).await.unwrap().is_none());
    assert!(wiped
        .node
        .leaf
        .join_keys(&format!("wrapjoin.{B}."), deadline())
        .await
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn hub_join_wiped_paired_box_imports_256_epochs_from_one_manifest() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    stop(&b).await;
    let epochs: Vec<_> = (1..=256).collect();
    a.vault.state.lock().unwrap().held.extend(&epochs);
    a.node
        .log
        .store
        .record_hub_held_epochs(
            entorhinal_core::hub_store::HubEpochSource::Join("all-epochs"),
            &epochs,
        )
        .unwrap();
    let revision = rig.kv.entry("epoch").await.unwrap().unwrap().revision;
    let recipients = json!([A, B, C, D]);
    a.node
        .log
        .write_epoch_step(
            "epoch-capacity",
            "epoch-capacity",
            &hub_write::Step {
                name: "epoch".into(),
                key: "epoch".into(),
                expected: revision,
                payload: hub_write::Payload::Record {
                    kind: "epoch".into(),
                    snapshot_id: None,
                    body: json!({"e":256,"state":"committed","minter":A,
                "mint_id":URL_SAFE_NO_PAD.encode([9; 16]),"base_e":255,
                "base_mint_id":URL_SAFE_NO_PAD.encode([8; 16]),
                "base_recipients":recipients,"recipients":recipients,"removed":[]}),
                },
            },
            deadline(),
        )
        .await
        .unwrap();
    let old_id = "0".repeat(32);
    install_request(&rig, &b, json!({"v":1,"request_id":old_id})).await;
    let wiped = EpochNode::start(&rig, "capacity-wipe", b.node.leaf.clone(), fleet, None);
    wiped.controller.joins(deadline()).await.unwrap();
    let key = format!("wrapreq.{B}");
    let request = read_body(&rig, &key).await;
    let id = request["request_id"].as_str().unwrap();
    assert_ne!(id, old_id);
    assert!(request.get("after_removal_id").is_none());
    a.controller.joins(deadline()).await.unwrap();
    let manifest_key = format!("wrapjoin.{B}.{id}");
    let manifest = read_body(&rig, &manifest_key).await;
    assert_eq!(manifest["through_epoch"], 256);
    let count = manifest["parts"].as_u64().unwrap();
    assert!(count > 1);
    for i in 0..count {
        let part = rig
            .kv
            .get(format!("{manifest_key}.{A}.{i}"))
            .await
            .unwrap()
            .unwrap();
        assert!(part.len() <= 12 * 1024);
    }
    let first = format!("{manifest_key}.{A}.0");
    let intact = rig.kv.get(&first).await.unwrap().unwrap();
    let mut corrupt = read_body(&rig, &first).await;
    corrupt["data"] = URL_SAFE_NO_PAD
        .encode(b"not the signed digest's data")
        .into();
    rig.kv
        .put(
            &first,
            signed_kv(&rig, &a, &first, 0, &corrupt).await.into(),
        )
        .await
        .unwrap();
    assert_eq!(
        wiped.controller.joins(deadline()).await.unwrap_err().code,
        "record_invalid"
    );
    assert!(wiped.vault.state.lock().unwrap().joins_imported.is_empty());
    rig.kv.put(&first, intact).await.unwrap();
    wiped.controller.joins(deadline()).await.unwrap();
    assert_eq!(a.vault.state.lock().unwrap().joins_served.len(), 1);
    assert_eq!(wiped.vault.state.lock().unwrap().joins_imported, [epochs]);
    assert_eq!(wiped.fence().await, 256);
    assert!(rig.kv.get(&key).await.unwrap().is_none());
    assert!(membership_records(&rig, "member.admitted", B)
        .await
        .is_empty());
}

#[tokio::test]
async fn hub_join_two_rotations_before_import_trigger_a_fresh_request() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    stop(&b).await;
    let mut wiped = EpochNode::new(
        &rig,
        "two-rotations-wipe",
        b.node.leaf.clone(),
        fleet.clone(),
        None,
    )
    .await;
    let key = format!("wrapreq.{B}");
    wiped.controller.joins(deadline()).await.unwrap();
    let first = read_body(&rig, &key).await;
    a.controller.joins(deadline()).await.unwrap();
    let old_manifest = format!("wrapjoin.{B}.{}", first["request_id"].as_str().unwrap());
    assert_eq!(read_body(&rig, &old_manifest).await["through_epoch"], 1);
    stop(&wiped).await;

    roster(&fleet, &rig, &[A, B, D]);
    rotate(&a, C, "700:permanent:13").await;
    let middle = AgentOperation::create(51, "Middle epoch".into())
        .plan(&a.node)
        .append()
        .await
        .unwrap()
        .position
        .unwrap();
    roster(&fleet, &rig, &[A, B]);
    rotate(&a, D, "701:permanent:13").await;
    assert_eq!(register(&rig).await["e"], 3);
    wiped.controller.joins(deadline()).await.unwrap();
    assert_eq!(wiped.vault.state.lock().unwrap().joins_imported, [vec![1]]);
    assert!(!wiped.vault.state.lock().unwrap().held.contains(&2));
    assert!(wiped.node.log.applied_sequence().unwrap() < middle);
    assert_ne!(
        first["request_id"],
        read_body(&rig, &key).await["request_id"]
    );
    assert!(rig.kv.get(&old_manifest).await.unwrap().is_some());
    a.controller.joins(deadline()).await.unwrap();
    let restarted = EpochNode::activate(&rig, &wiped.node.handler, &wiped.node.leaf, &wiped.vault);
    wiped.controller = Controller::new(restarted.clone()).unwrap();
    wiped.node.log = restarted;
    wiped.controller.joins(deadline()).await.unwrap();
    within(through(&wiped.node.handler, middle)).await;
    within(async {
        while !wiped.node.log.membership_ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    wiped.controller.joins(deadline()).await.unwrap();
    assert_eq!(
        wiped.vault.state.lock().unwrap().joins_imported,
        [vec![1], vec![1, 2, 3]]
    );
    assert!(rig.kv.get(&key).await.unwrap().is_none());
    assert!(rig.kv.get(&old_manifest).await.unwrap().is_none());
    assert!(membership_records(&rig, "member.admitted", B)
        .await
        .is_empty());
}

#[tokio::test]
async fn hub_join_new_vault_rotates_when_a_removed_peer_is_already_repaired() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    let (fleet, a, b) = devices(&rig).await;
    fleet.supports_excluded.store(true, Ordering::Release);
    // Pairing races ahead of this removal delivery: C must not get a new wrap.
    rotate(&a, C, "800:permanent:14").await;
    assert_eq!(
        a.vault.state.lock().unwrap().mint_exclusions,
        [vec![], vec![format!("fed-computer:{C}")]]
    );
    let committed = register(&rig).await;
    assert_eq!(committed["e"], 2);
    assert!(!committed["recipients"]
        .as_array()
        .unwrap()
        .contains(&json!(C)));
    assert!(rig
        .kv
        .get(format!(
            "wrap.2.{}.{C}",
            committed["mint_id"].as_str().unwrap()
        ))
        .await
        .unwrap()
        .is_none());
    assert_eq!(b.fence().await, 2);
}

#[tokio::test]
async fn hub_join_competing_responders_keep_only_the_winning_parts() {
    let Some(mut rig) = IdentityRig::new().await else {
        return;
    };
    let c_leaf = leaf(
        &mut rig._processes,
        rig.leaf_port,
        "leaf-c",
        C,
        Arc::default(),
    )
    .await;
    let (fleet, a, b) = devices(&rig).await;
    let c = EpochNode::new(&rig, "joiner-c", c_leaf, fleet, None).await;
    c.controller.joins(deadline()).await.unwrap();
    let request = read_body(&rig, &format!("wrapreq.{C}")).await;
    let id = request["request_id"].as_str().unwrap();
    let key = format!("wrapjoin.{C}.{id}");
    let race = Arc::new(Barrier::new(2));
    for responder in [&a, &b] {
        *responder.node.leaf.epoch_gate.lock().unwrap() =
            Some(("join.manifest:".into(), Gate::Race(race.clone())));
    }
    let (one, two) = within(async {
        tokio::join!(
            a.controller.joins(deadline()),
            b.controller.joins(deadline())
        )
    })
    .await;
    one.unwrap();
    two.unwrap();
    assert_eq!(a.vault.state.lock().unwrap().joins_served.len(), 1);
    assert_eq!(b.vault.state.lock().unwrap().joins_served.len(), 1);
    let manifest = read_body(&rig, &key).await;
    let winner = manifest["responder"].as_str().unwrap();
    assert_eq!(
        c.node
            .leaf
            .join_keys(&format!("{key}."), deadline())
            .await
            .unwrap(),
        [format!("{key}.{winner}.0")]
    );
    c.controller.joins(deadline()).await.unwrap();
    assert_eq!(c.vault.state.lock().unwrap().joins_imported, [vec![1]]);
    assert!(c
        .node
        .leaf
        .join_keys(&format!("wrapjoin.{C}."), deadline())
        .await
        .unwrap()
        .is_empty());
}

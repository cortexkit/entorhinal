//! Two computers sharing through the signed hub log, with independent stores.
use super::*;

async fn pair(rig: &IdentityRig, populated: bool) -> (Machine, Machine) {
    let fleet = pair_fleet(rig);
    let mut a = Machine::new(
        rig,
        store(&rig._processes, "converge-a"),
        &fleet,
        true,
        true,
        false,
    );
    let mut b = Machine::new(
        rig,
        store(&rig._processes, "converge-b"),
        &fleet,
        false,
        false,
        false,
    );
    if populated {
        add_agents(&a.store(), 4);
        a.store()
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
    }
    assert_eq!(
        result(a.enable(json!({"without_agents":!populated})).await)["state"],
        "enabled"
    );
    assert_eq!(result(b.enable(json!({})).await)["state"], "enabled");
    for member in [&mut a, &mut b] {
        Arc::get_mut(&mut member.handler)
            .unwrap()
            .operator_confirmer = Arc::new(crate::served_vectors::Approve {
            summaries: StdMutex::default(),
        });
    }
    activate(&a.handler, a.log.clone(), rig.a.leaf.clone());
    activate(&b.handler, b.log.clone(), rig.b.leaf.clone());
    within(async {
        while !a.handler.residence().unwrap().ready() || !b.handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    (a, b)
}

async fn apply_through(source: &Machine, follower: &Machine) {
    let head = source.log.log_head(deadline()).await.unwrap();
    crate::hub_identity_log::wait_for_applied(&follower.log, head, deadline())
        .await
        .unwrap();
}

async fn call(machine: &Machine, method: &str, params: Value, route: RouteKey) -> Value {
    result(
        machine
            .handler
            .handle_served_request(&body(method, params), route)
            .await,
    )
}

fn repo(label: &str, remote: &str) -> String {
    let (path, _) = crate::tests::scratch_descriptor(label);
    assert!(Command::new("git")
        .args(["init", "--quiet"])
        .current_dir(&path)
        .status()
        .unwrap()
        .success());
    assert!(Command::new("git")
        .args(["remote", "add", "origin", remote])
        .current_dir(&path)
        .status()
        .unwrap()
        .success());
    crate::tests::canonical_test_path(path)
        .to_string_lossy()
        .into_owned()
}

#[tokio::test]
async fn hub_memory_policy_round_trips_and_rebuilds() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let (a, b) = pair(&rig, false).await;
    let root = repo("policy-project", "https://github.com/example/policy.git");
    call(&a, "register", json!({"projectId":"P","name":"Project","workspaceId":"W","roots":[root],"domain":"@example/software"}), ROUTE).await;
    apply_through(&a, &b).await;
    assert_eq!(b.store().root_count("P").unwrap(), 0);
    for revision in 0..3 {
        let (writer, other) = if revision == 0 { (&a, &b) } else { (&b, &a) };
        call(writer, "workspace.set_document", json!({"workspace_id":"W","expected_revision":revision,"charter":if revision==2 {"\t".repeat(4096)}else{"Purpose".into()},"document":if revision==2 {"\"".repeat(65_536)}else{format!("Policy {revision}")},"request_key":format!("document-{revision}")}), ROUTE).await;
        apply_through(writer, other).await;
        let project = a.store().workspace_document("W", None).unwrap();
        assert_eq!(project.revision, revision + 1);
        let expected = serde_json::to_value(&project).unwrap();
        for member in [&a, &b] {
            assert!(member.store().verify().unwrap().ok);
            member.store().rebuild().unwrap();
            assert_eq!(
                serde_json::to_value(member.store().workspace_document("W", None).unwrap())
                    .unwrap(),
                expected
            );
        }
    }
    for (key, domain) in [
        ("domain", json!("@example/research")),
        ("clear", Value::Null),
    ] {
        call(
            &b,
            "project.set_domain",
            json!({"project_id":"P","domain":domain,"request_key":key}),
            ROUTE,
        )
        .await;
        apply_through(&b, &a).await;
        a.store().rebuild().unwrap();
        b.store().rebuild().unwrap();
        let report = a.store().context_project(&root, "fixed").unwrap();
        assert_eq!(report["domain"], domain);
        assert_eq!(
            report["members_digest"],
            entorhinal_catalog::sha256_hex("P")
        );
        assert_eq!(
            report["stamp"]["logPosition"],
            a.store()
                .identity_log_status()
                .unwrap()
                .last_applied_position
        );
    }
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn hub_remote_project_removal_cascades_and_retires_bindings() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let (a, b) = pair(&rig, false).await;
    let remote = "https://github.com/example/cascade.git";
    let source = repo("source-checkout", remote);
    let target = repo("follower-checkout", remote);
    call(
        &a,
        "register",
        json!({"projectId":"P","name":"Project","workspaceId":"W","roots":[source]}),
        ROUTE,
    )
    .await;
    call(
        &a,
        "register",
        json!({"projectId":"S","name":"Successor","roots":[]}),
        ROUTE,
    )
    .await;
    apply_through(&a, &b).await;
    assert_eq!(b.store().root_count("P").unwrap(), 0);
    call(&b, "attach_root", json!({"path":target}), ROUTE).await;
    let before = call(&b, "resolve", json!({"canonicalRoot":target}), ROUTE).await;
    let record = &before["rootRecords"][0];
    assert_eq!(record["approval"]["state"], "approved");
    let binding = json!({"root":target,"incarnation":record["incarnation"]["value"],"registrationEpoch":record["registrationEpoch"]});
    call(
        &a,
        "remove",
        json!({"projectId":"P","successorProjectId":"S"}),
        ROUTE,
    )
    .await;
    apply_through(&a, &b).await;
    let resolved = call(
        &b,
        "resolve",
        json!({"canonicalRoot":target,"executionBinding":binding}),
        ROUTE,
    )
    .await;
    assert_eq!(resolved["projectId"], "S");
    assert_eq!(resolved["bindingStatus"], "retired");
    assert_eq!(
        resolved["rootRecords"][0]["approval"]["state"],
        "unapproved"
    );
    assert_eq!(b.store().root_count("S").unwrap(), 1);
    b.store().rebuild().unwrap();
    assert_eq!(b.store().root_count("S").unwrap(), 1);
    call(&a, "remove", json!({"projectId":"S"}), ROUTE).await;
    apply_through(&a, &b).await;
    assert_eq!(b.store().root_count("S").unwrap(), 0);
    assert!(b.store().verify().unwrap().ok);
    fs::remove_dir_all(source).unwrap();
    fs::remove_dir_all(target).unwrap();
}

#[tokio::test]
async fn hub_avatar_manifest_resumes_after_second_write_refusal_without_duplicates() {
    let Some(rig) = empty_rig().await else {
        return;
    };
    let (mut a, b) = pair(&rig, true).await;
    let confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: StdMutex::default(),
    });
    Arc::get_mut(&mut a.handler).unwrap().operator_confirmer = confirmer.clone();
    let cursor = b.store().generation().unwrap();
    let params = json!({"request_key":"avatars","replace":true,"entries":[
        {"agent_id":agent(1),"avatar_genome":"a".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1},
        {"agent_id":agent(2),"avatar_genome":"b".repeat(32_768),"avatar_type":"creature.wild","avatar_version":1}
    ]});
    let wire = body("agent.apply_avatar_manifest", params);
    *rig.a.leaf.refuse_log_kind_after.lock().unwrap() = Some(("agent.set_avatar", 1));
    let outcome = a.handler.handle_served_request(&wire, ROUTE).await;
    let HandlerOutcome::ErrorWithDetail {
        code,
        message,
        detail,
    } = outcome
    else {
        panic!("expected second write to refuse: {outcome:?}")
    };
    assert_eq!(
        detail["batch"]["written"].as_array().unwrap().len(),
        1,
        "{code}: {message}\n{detail}"
    );
    assert_eq!(detail["batch"]["remaining"].as_array().unwrap().len(), 1);
    apply_through(&a, &b).await;
    assert_eq!(
        b.store().agent_changes(cursor, None).unwrap().entries.len(),
        1
    );
    rig.a.leaf.refuse_log_kind_after.lock().unwrap().take();
    let resumed = result(a.handler.handle_served_request(&wire, ROUTE).await);
    assert_eq!(resumed["written"].as_array().unwrap().len(), 1);
    assert!(resumed["remaining"].as_array().unwrap().is_empty());
    let repeated = result(a.handler.handle_served_request(&wire, ROUTE).await);
    assert!(repeated["written"].as_array().unwrap().is_empty());
    assert!(repeated["remaining"].as_array().unwrap().is_empty());
    assert_eq!(confirmer.summaries.lock().unwrap().len(), 2);
    apply_through(&a, &b).await;
    let changes = b.store().agent_changes(cursor, None).unwrap().entries;
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|e| e.op == "agent.set_avatar"));
    assert_ne!(changes[0].agent_id, changes[1].agent_id);
    for (index, hex) in [(1, "a"), (2, "b")] {
        let row = b.store().agent_row(&agent(index)).unwrap().unwrap();
        assert_eq!(row.avatar.unwrap().genome, hex.repeat(32_768));
    }
    for member in [&a, &b] {
        let old = serde_json::to_value(member.store().agent_snapshot().unwrap()).unwrap();
        assert!(member.store().verify().unwrap().ok);
        member.store().rebuild().unwrap();
        assert_eq!(
            serde_json::to_value(member.store().agent_snapshot().unwrap()).unwrap(),
            old
        );
    }
}

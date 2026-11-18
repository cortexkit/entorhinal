use super::*;
use crate::{
    hub_switch::snapshot::{transfer, Seeds},
    RouteAdmission, RouteKey,
};
use entorhinal_core::enable_state::HubSnapshot;

const ROUTE: RouteKey = (125, 1);
const CORE: RouteKey = (125, 2);

fn bootstrap(
    rig: &IdentityRig,
    mut store: RegistryStore,
    leaf: Arc<Leaf>,
    fleet: Arc<Fleet>,
    vault: Option<Arc<Vault>>,
    base: i64,
) -> (
    ProjectsHandler,
    Arc<IdentityLog<Leaf, TestKeys>>,
    Arc<Vault>,
) {
    let own = leaf.box_id.to_string();
    store.set_root_records(true);
    let mut handler = ProjectsHandler::with_runtime(own.clone(), || 700);
    handler.operator_confirmer = Arc::new(crate::served_vectors::Approve {
        summaries: StdMutex::default(),
    });
    *handler.store.lock().unwrap() = Some(store.clone());
    handler.route_admissions().insert(
        ROUTE,
        RouteAdmission {
            principal: Some(subc_protocol::Principal::Direct),
            ..Default::default()
        },
    );
    handler.route_admissions().insert(
        CORE,
        RouteAdmission::from_bind(
            Some(subc_protocol::Principal::Reserved {
                module_id: "prefrontal-core".into(),
            }),
            None,
        ),
    );
    let vault = vault.unwrap_or_else(|| {
        Arc::new(Vault {
            own: own.clone(),
            fleet: fleet.clone(),
            state: StdMutex::default(),
        })
    });
    let log = Arc::new(IdentityLog::new(
        store,
        handler.health.clone(),
        handler.writer.clone(),
        handler.commits.clone(),
        leaf.box_id.clone(),
        base,
        Link {
            transport: leaf,
            trust: rig.keys.clone(),
            signer: Arc::new(ClaustrumCredentialResolver::with_target(
                rig.keys.clone(),
                0,
            )),
            vault: vault.clone(),
            peers: Arc::new(Peers {
                own,
                current: fleet.current.clone(),
            }),
        },
    ));
    (handler, log, vault)
}

fn activate(handler: &ProjectsHandler, log: Arc<IdentityLog<Leaf, TestKeys>>, leaf: Arc<Leaf>) {
    *handler.hub_tail_task.lock().unwrap() = Some(log.start_installed_tail().unwrap());
    *handler.hub_shared_log.lock().unwrap() = Some(log.clone());
    let image = log.store.installed_hub_snapshot().unwrap().image;
    let feed = Feed::kv(handler.health.clone(), move |_| {
        Binding {
            session: leaf.session(),
            changes: None,
        }
        .residence_watch()
    })
    .with_durable_cursor();
    handler
        .activate_hub_residence(log, feed, Arc::new(Seeds::new(image)))
        .unwrap();
}

fn agent(index: usize) -> String {
    format!("agent_{index:016x}")
}

fn add_agents(store: &RegistryStore, labels: usize) {
    store.apply_entry("fixture.agents", "{}", "test", None, |tx| {
        for index in 1..=2 {
            tx.execute("INSERT INTO agent(agent_id,name,tag,role,labels_json,created_at_ms,updated_at_ms) VALUES(?1,?1,'tag','assistant',?2,0,0)",
                rusqlite::params![agent(index), json!(["x".repeat(labels)]).to_string()])?;
            tx.execute("INSERT INTO agent_name_claim(agent_id,namespace_kind,namespace_key,normalized_name,display_name,claimed_at_ms)
                VALUES(?1,'assistant','global',?1,?1,0)", [agent(index)])?;
        }
        Ok(())
    }).unwrap();
}

fn plan(store: &RegistryStore) -> HubSnapshot {
    serde_json::from_str(&store.hub_enable_plan().unwrap().unwrap().preimage).unwrap()
}

#[tokio::test]
async fn hub_snapshot_enable_resumes_after_part_and_seed_and_empty_member_joins() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.catch_up_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, &rig, &[A, B]);
    let (a, log, vault) = bootstrap(
        &rig,
        store(&rig._processes, "snapshot-a"),
        rig.a.leaf.clone(),
        fleet.clone(),
        None,
        0,
    );
    add_agents(&log.store, 100_000);
    log.store.begin_hub_enable(&"6a".repeat(16), A).unwrap();
    let image = plan(&log.store);

    let (committed, arrived) = oneshot::channel();
    let (release, held) = oneshot::channel();
    *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(committed, held));
    let partial = tokio::spawn({
        let log = log.clone();
        async move { transfer(&log, Some(&image), deadline()).await }
    });
    within(async {
        arrived.await.unwrap();
    })
    .await;
    partial.abort();
    let _ = partial.await;
    drop(release);
    assert_eq!(log.log_head(deadline()).await.unwrap(), 1);
    assert_eq!(log.store.hub_log_base().unwrap(), None);
    drop(log);
    drop(a);

    let (a, log, vault) = bootstrap(
        &rig,
        store(&rig._processes, "snapshot-a"),
        rig.a.leaf.clone(),
        fleet.clone(),
        Some(vault),
        0,
    );
    let (committed, arrived) = oneshot::channel();
    let (release, held) = oneshot::channel();
    *rig.a.leaf.epoch_gate.lock().unwrap() = Some((
        format!("residence.{}", agent(1)),
        Gate::LoseAck(committed, held),
    ));
    let image = plan(&log.store);
    let partial = tokio::spawn({
        let log = log.clone();
        async move { transfer(&log, Some(&image), deadline()).await }
    });
    within(async {
        arrived.await.unwrap();
    })
    .await;
    partial.abort();
    let _ = partial.await;
    drop(release);
    assert_eq!(log.log_head(deadline()).await.unwrap(), 2);
    assert!(rig
        .kv
        .get(format!("residence.{}", agent(1)))
        .await
        .unwrap()
        .is_some());
    assert!(rig
        .kv
        .get(format!("residence.{}", agent(2)))
        .await
        .unwrap()
        .is_none());
    assert_eq!(log.store.hub_log_base().unwrap(), None);
    drop(log);
    drop(a);

    let (a, log, _) = bootstrap(
        &rig,
        store(&rig._processes, "snapshot-a"),
        rig.a.leaf.clone(),
        fleet.clone(),
        Some(vault),
        0,
    );
    let image = plan(&log.store);
    assert!(transfer(&log, Some(&image), deadline()).await.unwrap());
    assert_eq!(log.log_head(deadline()).await.unwrap(), 2);
    assert!(log.store.hub_enable_plan().unwrap().is_none());
    let (b, follower, _) = bootstrap(
        &rig,
        store(&rig._processes, "snapshot-b"),
        rig.b.leaf.clone(),
        fleet,
        None,
        0,
    );
    assert!(transfer(&follower, None, deadline()).await.unwrap());
    assert_eq!(follower.store.installed_hub_snapshot().unwrap(), image);

    let root = rig._processes.dir.join("new-checkout");
    fs::create_dir(&root).unwrap();
    let root = root.canonicalize().unwrap();
    let req = body(
        "register",
        json!({"projectId":"new-project","name":"New project","roots":[root],"requestKey":"hub-register"}),
    );
    let rename = body(
        "agent.rename",
        json!({"agent_id":agent(1),"name":"Ada","request_key":"hub-rename"}),
    );
    let profile = body(
        "user.set",
        json!({"field":"name","value":"Mira","request_key":"hub-profile"}),
    );
    let placement = body("placement.backfill", json!({"requestKey":"hub-placement"}));
    let pins = entorhinal_core::catalog_agent::PinWriteRequest {
        agent_id: agent(1),
        request_key: Some("hub-pin".into()),
        pins: vec![entorhinal_core::catalog_agent::CatalogPin {
            name: "@acme/a".into(),
            version: "1.0.0".into(),
            kind: entorhinal_catalog::Kind::Skill,
            manifest_sha256: "11".repeat(32),
            archive_sha256: "22".repeat(32),
            publisher_fingerprint: "33".repeat(32),
            root: true,
            range: Some("^1".into()),
        }],
    };
    assert!(
        matches!(a.handle_active_request(&req, ROUTE).await, HandlerOutcome::Error { code, .. } if code == "hub_not_ready")
    );
    for (wire, route) in [(&rename, CORE), (&profile, CORE), (&placement, ROUTE)] {
        assert!(
            matches!(a.handle_active_request(wire, route).await, HandlerOutcome::Error { code, .. } if code == "hub_not_ready")
        );
    }
    assert_eq!(
        a.catalog_pins_write_wait(pins.clone(), CORE, Instant::now())
            .await
            .unwrap_err()
            .code,
        "hub_not_ready"
    );
    assert_eq!(log.log_head(deadline()).await.unwrap(), 2);
    activate(&a, log.clone(), rig.a.leaf.clone());
    activate(&b, follower.clone(), rig.b.leaf.clone());
    within(async {
        while !a.residence().unwrap().ready() || !b.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    result(a.handle_active_request(&placement, ROUTE).await);
    let reply = result(a.handle_active_request(&req, ROUTE).await);
    assert_eq!(reply["projectId"], "new-project");
    within(async {
        while !follower
            .store
            .enumerate(None)
            .unwrap()
            .projects
            .iter()
            .any(|project| project.project_id == "new-project")
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(log.store.root_count("new-project").unwrap(), 1);
    assert_eq!(follower.store.root_count("new-project").unwrap(), 0);
    assert_eq!(result(a.handle_active_request(&req, ROUTE).await), reply);
    for (wire, route) in [(&rename, CORE), (&profile, CORE)] {
        let outcome = a.handle_active_request(wire, route).await;
        assert!(
            matches!(outcome, HandlerOutcome::Response(_)),
            "{}: {outcome:?}",
            String::from_utf8_lossy(wire)
        );
    }
    a.catalog_pins_write_wait(pins.clone(), CORE, Instant::now())
        .await
        .unwrap();
    let head = log.log_head(deadline()).await.unwrap();
    crate::hub_identity_log::wait_for_applied(&follower, head, deadline())
        .await
        .unwrap();
    assert!(follower
        .store
        .agent_snapshot()
        .unwrap()
        .agents
        .iter()
        .any(|row| row.agent_id == agent(1) && row.name == "Ada"));
    assert_eq!(follower.store.catalog_pins(&agent(1)).unwrap(), pins.pins);
    assert_eq!(
        result(b.handle_request(&body("user.get", json!({})), CORE))["user"]["name"],
        "Mira"
    );
    assert!(follower
        .store
        .placement_read(Default::default(), "test")
        .unwrap()
        .facts
        .iter()
        .any(|fact| fact.project_id == "new-project"));
    assert!(log.store.verify().unwrap().ok);
}

fn migrated(store: &RegistryStore) -> HubSnapshot {
    use entorhinal_core::{
        enable_state::{HubClaim, HubImage, HubRole},
        remote_apply::RemoteEntry,
    };
    add_agents(store, 100_000);
    store
        .apply_entry("identity_log.enable", "{}", "test", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state='enabled'", [])?;
            Ok(())
        })
        .unwrap();
    let id = "6b".repeat(16);
    let mut excluded = vec!["7a".repeat(16)];
    store
        .apply_remote_entry(
            &RemoteEntry {
                position: 1,
                entry_id: "66".repeat(16),
                author: A.into(),
                signer: String::new(),
                key_id: String::new(),
                envelope_version: 1,
                kind: "change".into(),
                entry: serde_json::to_vec(
                    &json!({"op":"log.migrated","hub":true,"migration_id":id,"excluded":excluded,
            "roles":["connector_home"],"tables_digest":store.migration_tables_digest().unwrap()}),
                )
                .unwrap(),
            },
            Some(A),
        )
        .unwrap();
    let tables = store
        .capture_migration_source(1, excluded.clone())
        .unwrap()
        .into_mapped_tables()
        .unwrap();
    HubSnapshot {
        engram_through: 1,
        migration_id: id,
        excluded: std::mem::take(&mut excluded),
        tables,
        image: HubImage {
            claims: vec![
                HubClaim {
                    agent_id: agent(1),
                    generation: 0,
                    holder: None,
                },
                HubClaim {
                    agent_id: agent(2),
                    generation: 9,
                    holder: Some(A.into()),
                },
            ],
            roles: vec![HubRole {
                role: "connector_home".into(),
                generation: 3,
                holder: Some(A.into()),
            }],
        },
    }
}

#[tokio::test]
async fn hub_snapshot_migration_resumer_refuses_divergence_and_claims_gen_zero_only_after_ready() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.catch_up_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, &rig, &[A, B]);
    let (a, leader, _) = bootstrap(
        &rig,
        store(&rig._processes, "migration-a"),
        rig.a.leaf.clone(),
        fleet.clone(),
        None,
        1,
    );
    let image = migrated(&leader.store);
    let (committed, arrived) = oneshot::channel();
    let (release, held) = oneshot::channel();
    *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(committed, held));
    let first = tokio::spawn(async move { transfer(&leader, Some(&image), deadline()).await });
    within(async {
        arrived.await.unwrap();
    })
    .await;
    first.abort();
    let _ = first.await;
    drop(release);
    drop(a);

    let (b, log, _) = bootstrap(
        &rig,
        store(&rig._processes, "migration-b"),
        rig.b.leaf.clone(),
        fleet,
        None,
        1,
    );
    let mut image = migrated(&log.store);
    image.image.roles[0].generation += 1;
    assert_eq!(
        transfer(&log, Some(&image), deadline())
            .await
            .unwrap_err()
            .code,
        "hub_migration_mismatch"
    );
    assert_eq!(log.log_head(deadline()).await.unwrap(), 1);
    assert!(rig.b.leaf.sends.lock().unwrap().is_empty());
    assert!(log.store.hub_log_base().unwrap().is_none());

    image.image.roles[0].generation -= 1;
    assert!(transfer(&log, Some(&image), deadline()).await.unwrap());
    assert_eq!(log.store.hub_log_base().unwrap(), Some(1));
    assert_eq!(log.log_head(deadline()).await.unwrap(), 2);
    assert_eq!(rig.b.leaf.sends.lock().unwrap()[0].0, 1);
    activate(&b, log.clone(), rig.b.leaf.clone());
    let service = b.residence().unwrap();
    assert!(
        !service.ready(),
        "snapshot presence alone must not pass readiness"
    );
    let claim_key = format!("residence.{}", agent(1));
    let take = body(
        "agent.take",
        json!({"agent_id":agent(1),"request_key":"after-seed-take"}),
    );
    assert!(
        matches!(b.handle_active_request(&take, ROUTE).await, HandlerOutcome::Error { code, .. } if code == "hub_not_ready")
    );
    within(async {
        while !log.membership_ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    Seeds::new(image.image)
        .write(&log, deadline())
        .await
        .unwrap();
    within(async {
        while !service.ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert!(rig.kv.get(&claim_key).await.unwrap().is_none());
    assert!(rig.kv.get("role.connector_home").await.unwrap().is_some());
    result(b.handle_active_request(&take, ROUTE).await);
    within(async {
        loop {
            let row = log.store.hub_kv_cached(&claim_key).unwrap();
            if row
                .and_then(|row| row.body)
                .is_some_and(|body| body["gen"] == 1 && body["holder"] == B)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
}

fn git_checkout(root: &std::path::Path) {
    fs::create_dir(root).unwrap();
    assert!(Command::new("git")
        .args(["init", "-q"])
        .arg(root)
        .status()
        .unwrap()
        .success());
}

#[tokio::test]
async fn hub_snapshot_registration_binding_recovers_after_publish_and_reports_replaced_checkout() {
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.catch_up_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, &rig, &[A, B]);
    let (mut handler, mut log, vault) = bootstrap(
        &rig,
        store(&rig._processes, "binding-crash"),
        rig.a.leaf.clone(),
        fleet.clone(),
        None,
        0,
    );
    log.store.begin_hub_enable(&"7b".repeat(16), A).unwrap();
    assert!(transfer(&log, Some(&plan(&log.store)), deadline())
        .await
        .unwrap());
    activate(&handler, log.clone(), rig.a.leaf.clone());
    crate::hub_identity_log::wait_for_applied(&log, 2, deadline())
        .await
        .unwrap();
    for (project, replaced, conflict) in [
        ("recover", false, false),
        ("replaced", true, false),
        ("conflict", false, true),
    ] {
        within(async {
            while !handler.residence().unwrap().ready() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let root = rig._processes.dir.canonicalize().unwrap().join(project);
        git_checkout(&root);
        let req = body(
            "register",
            json!({"projectId":project,"name":project,"roots":[root],"requestKey":project}),
        );
        // Keep the verified watch state but stop applying until the process is
        // recreated; the log write can still land and its reply can be lost.
        let tail = handler.hub_tail_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
        let (committed, arrived) = oneshot::channel();
        let (release, held) = oneshot::channel();
        *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(committed, held));
        let mut request = Box::pin(handler.handle_active_request(&req, ROUTE));
        tokio::select! {
            result = &mut request => panic!("register returned before append landed: {result:?}"),
            result = arrived => result.unwrap(),
        }
        drop(request);
        drop(release);
        assert_eq!(log.store.root_count(project).unwrap(), 0);
        if replaced {
            fs::rename(&root, root.with_extension("old")).unwrap();
            git_checkout(&root);
        }
        if conflict {
            let request = serde_json::from_value(
                json!({"projectId":"recover","name":"recover","roots":[root]}),
            )
            .unwrap();
            log.store
                .with_principal("test")
                .register_at(request, 700)
                .unwrap();
        }
        handler.stop_hub_residence().await;
        drop(log);
        drop(handler);
        (handler, log, _) = bootstrap(
            &rig,
            store(&rig._processes, "binding-crash"),
            rig.a.leaf.clone(),
            fleet.clone(),
            Some(vault.clone()),
            0,
        );
        activate(&handler, log.clone(), rig.a.leaf.clone());
        let head = log.log_head(deadline()).await.unwrap();
        crate::hub_identity_log::wait_for_applied(&log, head, deadline())
            .await
            .unwrap();
        within(async {
            while !handler.residence().unwrap().ready() {
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
        let outcome = handler.handle_active_request(&req, ROUTE).await;
        if conflict {
            assert!(
                matches!(outcome, HandlerOutcome::Error { code, .. } if code == "root_conflict")
            );
            assert!(
                matches!(handler.handle_active_request(&req, ROUTE).await, HandlerOutcome::Error { code, .. } if code == "root_conflict")
            );
            assert_eq!(log.store.root_count(project).unwrap(), 0);
            assert!(log
                .store
                .enumerate(None)
                .unwrap()
                .projects
                .iter()
                .any(|row| row.project_id == project));
            continue;
        }
        let reply = result(outcome);
        assert_eq!(reply["projectId"], project);
        if replaced {
            assert_eq!(reply["unboundRoots"], json!([root]));
        } else {
            let resolved = log.store.resolve(root.to_str().unwrap()).unwrap();
            assert_eq!(resolved.project_id, project);
            assert_eq!(
                resolved.root_fields.unwrap().root_records[0].identity,
                "bound"
            );
        }
        assert_eq!(
            result(handler.handle_active_request(&req, ROUTE).await),
            reply
        );
        assert!(log.store.verify().unwrap().ok);
    }
}

#[tokio::test]
async fn hub_snapshot_tool_consent_settles_after_publish_and_restart() {
    use entorhinal_core::agent::consent::{
        Authorization, ConsentCommit, ConsentRequest, RequestIdentity, RequestState,
    };
    let Some(rig) = IdentityRig::new().await else {
        return;
    };
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.catch_up_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, &rig, &[A]);
    let (handler, log, vault) = bootstrap(
        &rig,
        store(&rig._processes, "consent-crash"),
        rig.a.leaf.clone(),
        fleet.clone(),
        None,
        0,
    );
    add_agents(&log.store, 4);
    log.store.begin_hub_enable(&"a1".repeat(16), A).unwrap();
    assert!(transfer(&log, Some(&plan(&log.store)), deadline())
        .await
        .unwrap());
    activate(&handler, log.clone(), rig.a.leaf.clone());
    within(async {
        while !handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let tail = handler.hub_tail_task.lock().unwrap().take().unwrap();
    tail.abort();
    let _ = tail.await;

    let caller = agent(1);
    let tool = "projects_create_workspace";
    let plan = log
        .store
        .precheck_consent_write(
            &caller,
            tool,
            json!({"workspace_id":"team","name":"Team"}),
            700,
        )
        .unwrap();
    let identity = RequestIdentity {
        agent_id: caller,
        tool: tool.into(),
        request_key: "create-team".into(),
    };
    let consent = ConsentCommit {
        request: ConsentRequest {
            identity: identity.clone(),
            args_digest: "approved args".into(),
            scope_ref: "workspace".into(),
            scope_epoch: 1,
            created_at_ms: 700,
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        },
        authorization: Authorization::Card {
            id: "fixture-card".into(),
            answer: "approve_once".into(),
        },
        action: tool.into(),
        target: json!(plan),
        granted: None,
        now_ms: 700,
    };
    let (committed, arrived) = oneshot::channel();
    let (release, held) = oneshot::channel();
    *rig.a.leaf.gate.lock().unwrap() = Some(Gate::LoseAck(committed, held));
    let mut request =
        Box::pin(handler.consent_write_wait(plan.clone(), consent.clone(), deadline()));
    tokio::select! {
        result = &mut request => panic!("tool write returned before append landed: {result:?}"),
        result = arrived => result.unwrap(),
    }
    drop(request);
    drop(release);
    assert!(log.store.consent_request(&identity).unwrap().is_none());
    handler.stop_hub_residence().await;
    drop(log);
    drop(handler);
    let (handler, log, _) = bootstrap(
        &rig,
        store(&rig._processes, "consent-crash"),
        rig.a.leaf.clone(),
        fleet,
        Some(vault),
        0,
    );
    activate(&handler, log.clone(), rig.a.leaf.clone());
    let head = log.log_head(deadline()).await.unwrap();
    crate::hub_identity_log::wait_for_applied(&log, head, deadline())
        .await
        .unwrap();
    within(async {
        while !handler.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let recorded = log.store.consent_request(&identity).unwrap().unwrap();
    assert_eq!(recorded.state, RequestState::Applied);
    let mut reply: Value = serde_json::from_slice(
        &handler
            .consent_write_wait(plan, consent, deadline())
            .await
            .unwrap(),
    )
    .unwrap();
    reply["result"]
        .as_object_mut()
        .unwrap()
        .remove("incarnation");
    assert_eq!(Some(reply["result"].clone()), recorded.outcome);
    assert_eq!(reply["result"]["workspaceId"], "team");
}

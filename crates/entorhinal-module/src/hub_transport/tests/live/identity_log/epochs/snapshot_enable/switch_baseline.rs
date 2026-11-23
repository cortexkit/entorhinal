use super::*;
use crate::sharing_baselines::{self as baseline, record, StoreFile};
use entorhinal_core::{enable_state::HubRole, remote_apply::RemoteEntry, shared_entry::MemberName};
use std::path::Path;

const BASELINE_BASE: i64 = 17;
const LEGACY: &str = "engram:11111111111111111111111111111111";
const LEGACY_ROLE: &str = "dreamer.legacy";
const HUB_ROLE: &str = "connector_home";

fn switch_snapshot() -> HubSnapshot {
    let source = StoreFile::new("switch-image-source");
    let store = source.open();
    record::cutover(&store);
    record::add_agent(&store, &agent(1), "Legacy agent");
    record::add_agent(&store, &agent(2), "Hub agent");
    store
        .set_member_name(
            MemberName {
                author: A.into(),
                display_name: "Home".into(),
            },
            "direct",
        )
        .unwrap();
    store.begin_hub_enable(&"c1".repeat(16), A).unwrap();
    let mut image = plan(&store);
    image.engram_through = BASELINE_BASE;
    image.excluded = vec![C.into()];
    image.image.claims[0].holder = Some(LEGACY.into());
    image.image.claims[0].generation = 7;
    image.image.claims[1].generation = 8;
    image.tables.get_mut("member_name").unwrap().push(
        serde_json::from_value(json!({"author":LEGACY,"display_name":"Previous member"})).unwrap(),
    );
    image.image.roles = vec![
        HubRole {
            role: HUB_ROLE.into(),
            generation: 13,
            holder: Some(A.into()),
        },
        HubRole {
            role: LEGACY_ROLE.into(),
            generation: 17,
            holder: Some(LEGACY.into()),
        },
    ];
    image
}

fn prepare_handler(handler: &mut ProjectsHandler) {
    baseline::admit(handler);
    Arc::get_mut(&mut handler.role_leases)
        .unwrap()
        .sample_clock(|| baseline::NOW as u64);
    handler
        .health
        .hub_membership
        .store(1, std::sync::atomic::Ordering::Release);
}

fn freeze_leader(store: &RegistryStore, snapshot: &HubSnapshot) {
    let author = LEGACY.strip_prefix("engram:").unwrap();
    let entry = |position, kind: &str, body: Value| RemoteEntry {
        position,
        entry_id: format!("{:032x}", 100 + position),
        author: author.into(),
        signer: author.into(),
        key_id: "legacy-key".into(),
        envelope_version: 1,
        kind: kind.into(),
        entry: serde_json::to_vec(&body).unwrap(),
    };
    store.apply_join_snapshot(&[entry(1,"snapshot",json!({"op":"shared.snapshot","snapshot_id":"baseline","part":1,"parts":1,"tables":{}}))],1,Some(author)).unwrap();
    for position in 2..BASELINE_BASE {
        store
            .apply_remote_entry(
                &entry(
                    position,
                    "change",
                    json!({"op":"project.shared","tables":{}}),
                ),
                Some(author),
            )
            .unwrap();
    }
    store.apply_remote_entry(&entry(BASELINE_BASE,"change",json!({"op":"log.migrated","hub":true,"migration_id":snapshot.migration_id,"excluded":snapshot.excluded,"roles":[HUB_ROLE],"tables_digest":store.migration_tables_digest().unwrap()})),Some(author)).unwrap();
}

async fn stop(handler: &ProjectsHandler) {
    handler.stop_hub_residence().await;
    let tail = handler.hub_tail_task.lock().unwrap().take().unwrap();
    tail.abort();
    assert!(tail.await.unwrap_err().is_cancelled());
    handler.hub_sql.wait().await;
}

async fn observe(
    handler: &ProjectsHandler,
    log: &IdentityLog<Leaf, TestKeys>,
    file: &StoreFile,
    head: u64,
    own: bool,
) -> String {
    within(async {
        while !handler.residence().unwrap().ready()
            || log.applied_sequence(deadline()).await.unwrap() < head
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    assert_eq!(log.applied_sequence(deadline()).await.unwrap(), head);
    let _lease = own.then(|| handler.role_leases.baseline_lease(HUB_ROLE, 13));
    let rows = baseline::served_reads(handler, file, &[HUB_ROLE.into(), LEGACY_ROLE.into()]).await;
    let entries: Vec<Value> = rows
        .lines()
        .map(|row| serde_json::from_str(row).unwrap())
        .collect();
    let result = |i: usize| &entries[i]["reply"]["result"];
    assert_eq!(entries.len(), 7, "{rows}");
    let agents = result(0)["agents"].as_array().unwrap();
    assert_eq!(agents.len(), 2);
    for (index, holder, epoch, status) in [
        (0, LEGACY, 7, "held_elsewhere"),
        (1, A, 8, if own { "held_here" } else { "held_elsewhere" }),
    ] {
        assert_eq!(agents[index]["holder"], holder);
        assert_eq!(agents[index]["epoch"], epoch);
        assert_eq!(agents[index]["known"], true);
        assert_eq!(agents[index]["status"], status);
        assert_eq!(agents[index]["stale"], false);
    }
    assert_eq!(result(1)["held_here"], own);
    assert_eq!(result(2)["held_here"], own);
    assert_eq!(result(3)["holder"], LEGACY);
    assert_eq!(result(4)["holder"], LEGACY);
    assert_eq!(result(3)["known"], true);
    assert_eq!(result(5)["state"], "unknown");
    assert_eq!(result(5)["holder"], LEGACY);
    assert_eq!(result(5)["epoch"], 7);
    assert_eq!(result(6)["state"], if own { "here" } else { "elsewhere" });
    rows
}

fn assert_position(file: &StoreFile, name: &str, position: u64, snapshot_end: i64) {
    let conn = file.read();
    assert_eq!(
        baseline::rows(
            &conn,
            "SELECT hub_log_base,last_seen_head,last_applied_position FROM identity_log_state"
        ),
        vec![vec![
            json!(BASELINE_BASE),
            json!(BASELINE_BASE as u64 + position),
            json!(BASELINE_BASE as u64 + position)
        ]],
        "{name}"
    );
    assert_eq!(
        baseline::rows(
            &conn,
            "SELECT MAX(log_position) FROM registry_journal WHERE op='hub.snapshot'"
        ),
        vec![vec![json!(snapshot_end)]]
    );
    assert_eq!(baseline::rows(&conn,&format!("SELECT COUNT(*) FROM registry_journal WHERE op='project.shared' AND log_position={}",BASELINE_BASE as u64 + position)), vec![vec![json!(1)]]);
}

async fn fixture(record_to: Option<&Path>) {
    let Some(rig) = IdentityRig::new().await else {
        assert!(
            record_to.is_none(),
            "nats-server is required to record the Switch baseline"
        );
        return;
    };
    for node in [&rig.a, &rig.b] {
        let tail = node.handler.hub_tail_task.lock().unwrap().take().unwrap();
        tail.abort();
        let _ = tail.await;
    }
    let fleet = Arc::new(Fleet::default());
    roster(&fleet, &rig, &[A, B]);
    let leader_file = StoreFile::new("switch-existing");
    let image = switch_snapshot();
    freeze_leader(&leader_file.open(), &image);
    entorhinal_core::legacy_sharing_test_allow(leader_file.path());
    let (mut a, leader, _) = bootstrap(
        &rig,
        leader_file.open(),
        rig.a.leaf.clone(),
        fleet.clone(),
        None,
        BASELINE_BASE,
    );
    prepare_handler(&mut a);
    assert!(transfer(&leader, Some(&image), deadline()).await.unwrap());
    let parts = leader.log_head(deadline()).await.unwrap();
    assert!(parts > 0);
    let snapshot_end = BASELINE_BASE + parts as i64;
    assert_eq!(
        leader
            .sql
            .test_store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        snapshot_end
    );
    let join_file = StoreFile::new("switch-join");
    let store = join_file.open();
    // A paired computer can already hold the epoch key before joining the
    // registry. Record that provenance so installing the snapshot does not
    // cache signed KV bytes belonging to this particular NATS process.
    let vault = Arc::new(Vault {
        own: B.into(),
        fleet: fleet.clone(),
        state: StdMutex::new(KeyState {
            held: [1].into(),
            ..Default::default()
        }),
    });
    store
        .record_hub_held_epochs(
            entorhinal_core::hub_store::HubEpochSource::Join("paired-before-registry-join"),
            &[1],
        )
        .unwrap();
    let (joining, follower, vault) = bootstrap(
        &rig,
        store,
        rig.b.leaf.clone(),
        fleet.clone(),
        Some(vault),
        0,
    );
    assert!(follower
        .sql
        .test_store()
        .enable_registry_is_empty()
        .unwrap());
    assert!(transfer(&follower, None, deadline()).await.unwrap());
    assert_eq!(
        follower.sql.test_store().hub_log_base().unwrap(),
        Some(BASELINE_BASE)
    );
    assert_eq!(
        follower
            .sql
            .test_store()
            .identity_log_status()
            .unwrap()
            .last_applied_position,
        snapshot_end
    );
    let store = follower.sql.test_store();
    for id in [agent(1), agent(2)] {
        record::session(&store, &id);
    }
    let source = if let Some(dir) = record_to {
        let path = dir.join("switch-observer.db");
        record::cache(
            &store,
            &agent(1),
            B,
            LEGACY.strip_prefix("engram:").unwrap(),
        );
        record::pending(&store);
        join_file.backup(&path);
        path
    } else {
        baseline::artifacts().join("switch-observer.db")
    };
    drop(store);
    drop(follower);
    drop(joining);
    activate(&a, leader.clone(), rig.a.leaf.clone());
    within(async {
        while !leader.membership_ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    Seeds::new(image.image)
        .write(&leader, deadline())
        .await
        .unwrap();
    let (mut b, follower, _) = bootstrap(
        &rig,
        join_file.open(),
        rig.b.leaf.clone(),
        fleet.clone(),
        Some(vault.clone()),
        BASELINE_BASE,
    );
    prepare_handler(&mut b);
    activate(&b, follower.clone(), rig.b.leaf.clone());
    for id in [agent(1), agent(2)] {
        record::session(&leader.sql.test_store(), &id);
    }
    within(async {
        while !a.residence().unwrap().ready() || !b.residence().unwrap().ready() {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let landed = leader.write_epoch_step("baseline-tail", "baseline-tail", &hub_write::Step {
        name:"project.shared".into(), key:LOG_SUBJECT.into(), expected:leader.log_head(deadline()).await.unwrap(),
        payload:hub_write::Payload::Record { kind:"project.shared".into(), body:json!({"op":"project.shared","tables":{"project":{"upsert":[{"project_id":"after_switch","name":"After switch","implicit":0,"seed_identity":null,"created_at":700,"updated_at":700}],"delete":[]}}}), snapshot_id:None },
    }, deadline()).await.unwrap();
    let position = landed.position.unwrap();
    let member = observe(&a, &leader, &leader_file, position, true).await;
    let joined = observe(&b, &follower, &join_file, position, false).await;
    assert_position(&leader_file, "existing member", position, snapshot_end);
    assert_position(&join_file, "joined observer", position, snapshot_end);
    stop(&b).await;
    drop(follower);
    drop(b);
    let restarted_file = StoreFile::copy(&source);
    check_observer_store(&restarted_file, snapshot_end);
    let (mut b, follower, _) = bootstrap(
        &rig,
        restarted_file.open(),
        rig.b.leaf.clone(),
        fleet.clone(),
        Some(vault),
        BASELINE_BASE,
    );
    prepare_handler(&mut b);
    activate(&b, follower.clone(), rig.b.leaf.clone());
    let restarted = observe(&b, &follower, &restarted_file, position, false).await;
    assert_position(
        &restarted_file,
        "restarted observer",
        position,
        snapshot_end,
    );
    assert_eq!(joined, restarted);
    within(async {
        while follower.writer_episode(deadline()).await.ok() != Some(0) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    stop(&b).await;
    drop(follower);
    drop(b);

    let controller = Controller::new(leader.clone()).unwrap();
    roster(&fleet, &rig, &[A]);
    controller
        .deliver(&event(B, "700:permanent:12"), deadline())
        .await
        .unwrap();
    within(async {
        while baseline::rows(
            &leader_file.read(),
            "SELECT seq FROM registry_journal WHERE op='member.removed'",
        )
        .is_empty()
        {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    roster(&fleet, &rig, &[A, B]);
    let readmitted_file = StoreFile::new("switch-readmission");
    let (joining, follower, vault) = bootstrap(
        &rig,
        readmitted_file.open(),
        rig.b.leaf.clone(),
        fleet.clone(),
        None,
        0,
    );
    let joining_controller = Controller::new(follower.clone()).unwrap();
    let end = deadline();
    loop {
        joining_controller.joins(end).await.unwrap();
        controller.joins(end).await.unwrap();
        if transfer(&follower, None, end).await.unwrap() {
            break;
        }
        assert!(tokio::time::Instant::now() < end);
        sleep(Duration::from_millis(10)).await;
    }
    drop(joining_controller);
    drop(follower);
    drop(joining);
    let (mut b, follower, _) = bootstrap(
        &rig,
        readmitted_file.open(),
        rig.b.leaf.clone(),
        fleet,
        Some(vault),
        BASELINE_BASE,
    );
    prepare_handler(&mut b);
    activate(&b, follower.clone(), rig.b.leaf.clone());
    for id in [agent(1), agent(2)] {
        record::session(&follower.sql.test_store(), &id);
    }
    within(async {
        while follower.writer_episode(deadline()).await.ok() != Some(1) {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let readmitted = observe(
        &b,
        &follower,
        &readmitted_file,
        leader.log_head(deadline()).await.unwrap(),
        false,
    )
    .await;
    assert_eq!(joined, readmitted);
    for (name, bytes) in [
        ("member", member),
        ("joined", joined),
        ("restarted", restarted),
        ("readmitted", readmitted),
    ] {
        let name = format!("switch-observer.{name}.jsonl");
        if let Some(dir) = record_to {
            fs::write(dir.join(name), bytes).unwrap();
        } else {
            let path = baseline::artifacts().join(name);
            assert_eq!(
                fs::read_to_string(&path).unwrap(),
                bytes,
                "{}",
                path.display()
            );
        }
    }
    stop(&b).await;
    stop(&a).await;
}

fn check_observer_store(file: &StoreFile, snapshot_end: i64) {
    let store = file.open();
    baseline::check_store(file, &store, "enabled", Some(BASELINE_BASE));
    baseline::check_pending(file, &store);
    assert_eq!(store.installed_hub_snapshot().unwrap().excluded, [C]);
    assert_eq!(
        store.identity_log_status().unwrap().last_applied_position,
        snapshot_end
    );
    assert!(baseline::rows(&file.read(), "SELECT key FROM hub_kv_cache").is_empty());
    assert!(baseline::rows(
        &file.read(),
        "SELECT sequence FROM hub_membership_scan WHERE candidate IS NOT NULL"
    )
    .is_empty());
}

#[tokio::test]
async fn sharing_baseline_switch_observers() {
    fixture(None).await;
}

#[tokio::test]
#[ignore = "one-shot recording of the original Switch observer"]
async fn record_switch_observers() {
    let dir = std::path::PathBuf::from(std::env::var_os("ENTORHINAL_RECORD_SWITCH").unwrap());
    fs::create_dir(&dir).unwrap();
    fixture(Some(&dir)).await;
}

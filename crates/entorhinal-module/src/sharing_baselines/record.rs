//! One-shot recorder for the original writers. It writes only to an explicitly
//! supplied empty directory, never to the checked-in fixture directory.

use super::*;
use crate::{fake_log::FakeLog, fake_record::FakeRecord};
use entorhinal_core::{
    claim_cache::{CachedClaim, ClaimObservation},
    enable_state::HubSnapshot,
    hub_store::{HubAttemptSend, HubAttemptWrite, HubEpochSource},
    remote_apply::RemoteEntry,
    role_cache::{RoleTakeAttempt, RoleTakeInput},
    shared_entry::MemberName,
    RegisterRequest, SessionRegistration, SharedSettlement,
};

const AUTHOR: &str = "11111111111111111111111111111111";
pub(crate) const AGENT: &str = "agent_0000000000000001";
const BOX_B: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const ROLE: &str = "connector_home";

pub(crate) fn add_agent(store: &RegistryStore, id: &str, name: &str) {
    store
        .with_principal("reserved:prefrontal")
        .agent_mutation_with_id(
            "agent.create",
            json!({"role":"assistant","name":name,"tag":"baseline","request_key":name}),
            NOW,
            Some(id),
        )
        .unwrap();
}

pub(crate) fn cutover(store: &RegistryStore) {
    // Import an empty snapshot to record the real local cutover marker.
    let path = store.store_folder().join("import.db");
    Connection::open(&path)
        .unwrap()
        .execute_batch("CREATE TABLE empty_snapshot(id INTEGER)")
        .unwrap();
    store
        .agent_import(
            json!({"snapshot_path":path,"request_key":"baseline-import"}),
            NOW,
        )
        .unwrap();
}

fn local(store: &RegistryStore) {
    cutover(store);
    add_agent(store, AGENT, "Aster");
    local_project(store);
}

fn local_project(store: &RegistryStore) {
    store
        .with_principal("direct")
        .register_at(
            RegisterRequest {
                project_id: Some("baseline_project".into()),
                name: "Baseline project".into(),
                roots: vec!["/".into()],
                label: Some("baseline".into()),
                ..Default::default()
            },
            NOW,
        )
        .unwrap();
}

fn local_role(store: &RegistryStore, own: &str) {
    let attempt = RoleTakeAttempt {
        input: RoleTakeInput {
            role: "local_role".into(),
            expected_epoch: 0,
            request_id: "67".repeat(16),
            reason: "fixture operator approval".into(),
        },
        expected_version: 0,
        body: serde_json::to_vec(&json!({"v":1,"holder":own,"epoch":1})).unwrap(),
        epoch: None,
    };
    let attempt = store.prepare_role_take(&attempt, own, NOW).unwrap();
    store.finish_role_take(&attempt, own, 1, 1, NOW).unwrap();
}

pub(crate) fn session(store: &RegistryStore, agent: &str) {
    store
        .register_session(&SessionRegistration {
            agent_id: agent,
            ref_kind: "broca-session",
            session_ref: agent,
            harness: "pi",
            session_id: agent,
            project_root: None,
            registered_by: "host",
            runner: Some("broca"),
            role: None,
            network: None,
            now_ms: NOW,
        })
        .unwrap();
}

pub(crate) fn cache(store: &RegistryStore, agent: &str, own: &str, holder: &str) {
    let observation = ClaimObservation::Present {
        claim: CachedClaim {
            holder: Some(holder.into()),
            epoch: 3,
            cut_id: None,
            version: 5,
            read_at_ms: NOW,
            stale: false,
            integrity_failed: false,
        },
        self_author: own.into(),
    };
    store
        .cache_roles(&[(ROLE.into(), observation.clone())], NOW)
        .unwrap();
    store
        .cache_agent_claims(&[(agent.into(), observation)], NOW)
        .unwrap();
    store.role_read("enrolled").unwrap();
    store.observe_role_gen(ROLE, 13).unwrap();
}

pub(crate) fn pending(store: &RegistryStore) {
    let expected = store.identity_log_status().unwrap().last_applied_position;
    let error = store
        .shared_attempt_with_reply(
            "51".repeat(16),
            expected,
            Some(("unanswered".into(), "d0".repeat(32))),
            |_| SharedSettlement::Rollback {
                sent: true,
                code: "engram_unavailable".into(),
                message: "lost reply".into(),
            },
            || {
                store.with_principal("direct").register_at(
                    RegisterRequest {
                        project_id: Some("unanswered".into()),
                        name: "Unanswered".into(),
                        request_key: Some("unanswered".into()),
                        ..Default::default()
                    },
                    NOW,
                )
            },
        )
        .unwrap_err();
    assert!(
        matches!(error, RegistryError::Domain { ref code, .. } if code == "engram_unavailable"),
        "{error:?}"
    );
    assert_eq!(store.identity_log_status().unwrap().pending_write_count, 1);
}

fn ledger(store: &RegistryStore) {
    let mut write = HubAttemptWrite {
        request_key: "outstanding-hub-attempt".into(),
        params_digest: "d1".repeat(32),
        key: format!("residence.{AGENT}"),
        expected: 83,
        value: vec![0, 255, 6],
        step: "residence".into(),
    };
    let mut send = HubAttemptSend {
        op_id: "31".repeat(16),
        sent_at_ms: 600,
        process: "before-restart".into(),
    };
    assert!(store.persist_hub_attempt(&write, &send, None).unwrap());
    let previous = store.hub_attempt(&write.request_key).unwrap().unwrap();
    write.value = vec![0, 255, 7];
    send = HubAttemptSend {
        op_id: "32".repeat(16),
        sent_at_ms: 650,
        process: "after-restart".into(),
    };
    assert!(store
        .persist_hub_attempt(&write, &send, Some(&previous))
        .unwrap());
    store
        .record_hub_held_epochs(HubEpochSource::Join("baseline-join"), &[1, 2])
        .unwrap();
}

fn legacy_handler(file: &StoreFile, log: &FakeLog) -> ProjectsHandler {
    let handler = ProjectsHandler::with_log_connector(BOX_A.into(), || NOW, Arc::new(log.clone()));
    admit(&handler);
    *handler.store.lock().unwrap() = Some(file.open());
    handler.health.store_ready.store(true, Ordering::Release);
    handler
}

async fn legacy_enable(handler: &ProjectsHandler, without_agents: bool) {
    let reply = call(
        handler,
        "identity_log.enable",
        json!({"without_agents":without_agents}),
    )
    .await;
    assert_eq!(
        serde_json::from_str::<Value>(&reply).unwrap()["result"]["state"],
        "enabled",
        "{reply}"
    );
}

fn legacy_local(file: &StoreFile, populate_agents: bool) {
    let store = file.open();
    if populate_agents {
        local(&store);
    } else {
        local_project(&store);
    }
    store.learn_verified_self_author(AUTHOR, NOW).unwrap();
    store.set_own_member_box(BOX_A.into()).unwrap();
    store
        .set_member_name(
            MemberName {
                author: AUTHOR.into(),
                display_name: "Aster's box".into(),
            },
            "direct",
        )
        .unwrap();
}

async fn legacy_create(store: &RegistryStore, log: &FakeLog) {
    use crate::log_client::{AppendRequest, EntryKind};
    let id = [0x43; 16];
    let head = log.head();
    let (send, receive) = std::sync::mpsc::channel();
    store.shared_attempt(log_client::encode_hex(&id), head as i64, move |entry| {
        send.send(entry).unwrap();
        SharedSettlement::Commit(head as i64 + 1)
    }, || store.with_principal("reserved:prefrontal").agent_mutation_with_id(
        "agent.create", json!({"role":"assistant","name":"Aster","tag":"baseline","request_key":"Aster"}), NOW, Some(AGENT),
    )).unwrap();
    let entry = receive.try_recv().unwrap();
    log.client()
        .append(&AppendRequest {
            expected_head: head,
            entry_id: id,
            kind: EntryKind::Change,
            data: entry,
        })
        .await
        .unwrap();
}

fn install(store: &RegistryStore, snapshot: &HubSnapshot, part_count: i64) {
    // The installer receives verified parts; the journal writer itself uses
    // only their IDs, positions, writer identity and episode, not wire bodies.
    let parts = (1..=part_count)
        .map(|part| {
            (
                RemoteEntry {
                    position: snapshot.engram_through + part,
                    entry_id: format!("{part:032x}"),
                    author: BOX_A.into(),
                    signer: BOX_A.into(),
                    key_id: "baseline-key".into(),
                    envelope_version: 1,
                    kind: "snapshot".into(),
                    entry: vec![],
                },
                1,
            )
        })
        .collect::<Vec<_>>();
    store.install_hub_snapshot(snapshot, &parts, BOX_A).unwrap();
}

async fn switched(handler: &ProjectsHandler, log: &FakeLog, excluded: &[&str], parts: i64) {
    let records = FakeRecord::new([0x11; 16]);
    let image = crate::hub_switch::baseline_migration_image(handler, log, records, excluded).await;
    assert_eq!(image.excluded, excluded);
    handler
        .with_store(|store| {
            install(store, &image, parts);
            Ok(())
        })
        .unwrap();
}

async fn remote_snapshot(log: &FakeLog) {
    use crate::log_client::{AppendRequest, EntryKind};
    // Both parts are independently audited, even though only the first
    // journal row carries the merged replay image.
    let head = log.head();
    for part in [1, 2] {
        log.client().append(&AppendRequest {
            expected_head: head + part - 1,
            entry_id: [20 + part as u8; 16], kind: EntryKind::Snapshot,
            data: serde_json::to_vec(&json!({"op":"shared.snapshot","snapshot_id":"multipart","part":part,"parts":2,"tables":{}})).unwrap(),
        }).await.unwrap();
    }
}

async fn remote_changes(log: &FakeLog) {
    use crate::log_client::{AppendRequest, EntryKind};
    let peer = StoreFile::new("legacy-peer");
    let store = peer.open();
    let author = "22".repeat(16);
    store.learn_verified_self_author(&author, NOW).unwrap();
    store.set_own_member_box(BOX_B.into()).unwrap();
    store
        .set_member_name(
            MemberName {
                author,
                display_name: "Excluded peer".into(),
            },
            "direct",
        )
        .unwrap();
    let conn = peer.read();
    let name_and_box: Vec<Vec<u8>> = conn
        .prepare("SELECT entry FROM registry_journal ORDER BY seq")
        .unwrap()
        .query_map([], |row| row.get(0))
        .unwrap()
        .collect::<rusqlite::Result<_>>()
        .unwrap();
    drop(conn);
    let peer_log = log.with_author([0x22; 16]);
    let body = json!({"op":"project.shared","tables":{"project":{"upsert":[{"project_id":"second_project","name":"Before switch","implicit":0,"seed_identity":null,"created_at":700,"updated_at":700}],"delete":[]}}});
    for (i, data) in name_and_box
        .into_iter()
        .chain([serde_json::to_vec(&body).unwrap()])
        .enumerate()
    {
        peer_log
            .client()
            .append(&AppendRequest {
                expected_head: log.head(),
                entry_id: [30 + i as u8; 16],
                kind: EntryKind::Change,
                data,
            })
            .await
            .unwrap();
    }
}

async fn make_history(dir: &Path) {
    let zero = StoreFile::new("legacy-zero");
    let supersede = StoreFile::new("legacy-supersede");
    let catchup = StoreFile::new("legacy-catch-up");
    let enabling = StoreFile::new("native-enabling");
    let installed = StoreFile::new("native-installed");
    let disabled = StoreFile::new("disabled");

    legacy_local(&zero, false);
    let zero_log = FakeLog::default().with_author([0x11; 16]);
    let handler = legacy_handler(&zero, &zero_log);
    legacy_enable(&handler, true).await;
    legacy_create(
        &handler.with_store(|store| Ok(store.clone())).unwrap(),
        &zero_log,
    )
    .await;
    switched(&handler, &zero_log, &[], 2).await;
    drop(handler);

    // A backup restored from before enable still has the same author. It
    // publishes a superseding image on the same log; an empty peer catches up
    // through both versions, including the multipart audit rows.
    let producer = StoreFile::new("legacy-producer");
    legacy_local(&producer, true);
    producer.backup(Path::new(supersede.path()));
    let log = FakeLog::default().with_author([0x11; 16]);
    let handler = legacy_handler(&producer, &log);
    legacy_enable(&handler, false).await;
    drop(handler);
    let receiver = legacy_handler(&catchup, &log);
    let receiver_store = receiver.with_store(|store| Ok(store.clone())).unwrap();
    receiver
        .catch_up(
            &receiver_store,
            tokio::time::Instant::now() + Duration::from_secs(4),
        )
        .await
        .unwrap();
    let handler = legacy_handler(&supersede, &log);
    add_agent(
        &handler.with_store(|store| Ok(store.clone())).unwrap(),
        "agent_0000000000000002",
        "Birch",
    );
    legacy_enable(&handler, false).await;
    receiver
        .catch_up(
            &receiver_store,
            tokio::time::Instant::now() + Duration::from_secs(4),
        )
        .await
        .unwrap();
    remote_changes(&log).await;
    remote_snapshot(&log).await;
    let store = handler.with_store(|store| Ok(store.clone())).unwrap();
    handler
        .catch_up(&store, tokio::time::Instant::now() + Duration::from_secs(4))
        .await
        .unwrap();
    receiver
        .catch_up(
            &receiver_store,
            tokio::time::Instant::now() + Duration::from_secs(4),
        )
        .await
        .unwrap();
    drop(store);
    switched(&handler, &log, &[BOX_B], 1).await;
    drop(handler);
    let image = crate::hub_switch::baseline_migration_image(
        &receiver,
        &log,
        FakeRecord::new([0x11; 16]),
        &[BOX_B],
    )
    .await;
    install(&receiver_store, &image, 1);
    drop(receiver_store);
    drop(receiver);

    let store = enabling.open();
    local(&store);
    store.begin_hub_enable(&"a1".repeat(16), BOX_A).unwrap();
    drop(store);
    enabling.backup(Path::new(installed.path()));
    let store = installed.open();
    let plan = store.hub_enable_plan().unwrap().unwrap();
    install(&store, &serde_json::from_str(&plan.preimage).unwrap(), 1);
    ledger(&store);
    pending(&store);
    drop(store);
    local(&disabled.open());

    for (name, file) in [
        ("legacy-zero", zero),
        ("legacy-supersede", supersede),
        ("legacy-catch-up", catchup),
        ("native-enabling", enabling),
        ("native-installed", installed),
        ("disabled", disabled),
    ] {
        let store = file.open();
        let own = store
            .persisted_self_author()
            .unwrap()
            .unwrap_or_else(|| BOX_A.into());
        for id in store.live_claim_agents().unwrap() {
            session(&store, &id);
        }
        cache(&store, AGENT, &own, AUTHOR);
        if name == "disabled" {
            local_role(&store, &own);
        }
        drop(store);
        file.backup(&dir.join(format!("{name}.db")));
    }
}

#[tokio::test]
#[ignore = "one-shot recording of the original history writers"]
async fn record_history() {
    let dir = PathBuf::from(std::env::var_os("ENTORHINAL_RECORD_HISTORY").unwrap());
    fs::create_dir(&dir).unwrap();
    make_history(&dir).await;
    for (name, state, base) in STORES {
        let file = StoreFile::copy(&dir.join(format!("{name}.db")));
        check_history(&file, name, state, *base);
        for (kind, bytes) in history_outputs(&dir, name, *base).await {
            fs::write(dir.join(format!("{name}.{kind}")), bytes).unwrap();
        }
    }
}

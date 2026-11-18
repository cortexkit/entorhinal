use std::fs;

use cortexkit_store::{Isolation, StorageBackend, StorageDescriptor};
use serde_json::json;

use super::*;
use crate::{mutations::tests::Fixture, MIGRATIONS};

fn descriptor(f: &Fixture, file: &str) -> StorageDescriptor {
    StorageDescriptor {
        module_id: "hub-store-test".into(),
        storage_namespace: f.root.file_name().unwrap().to_str().unwrap().into(),
        isolation: Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: f.root.join(file).to_string_lossy().into_owned(),
        },
    }
}

#[test]
fn hub_store_migration_preserves_legacy_progress_and_requires_snapshot_for_downgrade() {
    let f = Fixture::new("hub-migration");
    let live = descriptor(&f, "legacy.db");
    // The chain as shipped before the hub migration, found by its statements
    // so later migrations neither change this test's meaning nor edit it.
    let hub = MIGRATIONS
        .iter()
        .position(|m| m.statements == V26_HUB_STORE)
        .unwrap();
    let legacy_chain = &MIGRATIONS[..hub];
    let (new, old) = (MIGRATIONS.len(), hub);
    let legacy = RegistryStore::open_with_migrations(&live, legacy_chain).unwrap();
    legacy
        .db
        .with_conn_fenced(|tx| {
            tx.execute_batch(
                "UPDATE identity_log_state SET state='enabled',last_applied_position=17,last_seen_head=19;
                 INSERT INTO agent_move(cut_id,agent_id,side,peer,step,plan,record_version,
                    seal_sent,attempts,created_at_ms,updated_at_ms)
                 VALUES('old-cut','agent','receiver','source','done','{}',7,0,1,10,20);",
            )
        })
        .unwrap();
    let legacy_status = legacy.identity_log_status().unwrap();
    drop(legacy);

    // The release tool must save this SQLite image before the upgrade opens it.
    let snapshot = f.root.join("pre-upgrade.db");
    Connection::open(f.root.join("legacy.db"))
        .unwrap()
        .execute("VACUUM INTO ?1", [snapshot.to_str().unwrap()])
        .unwrap();

    let store = RegistryStore::open(&live).unwrap();
    assert_eq!(store.identity_log_status().unwrap(), legacy_status);
    store
        .read(|conn| {
            let flags = conn.query_row(
                "SELECT hub_log_base,hub_ready,engram_frozen,enable_id,enable_snapshot_digest
                 FROM identity_log_state WHERE id=1",
                [],
                |r| {
                    Ok((
                        r.get::<_, Option<i64>>(0)?,
                        r.get::<_, bool>(1)?,
                        r.get::<_, Option<String>>(2)?,
                        r.get::<_, Option<String>>(3)?,
                        r.get::<_, Option<String>>(4)?,
                    ))
                },
            )?;
            assert_eq!(flags, (None, false, None, None, None));
            let move_state = conn.query_row(
                "SELECT record_version,receiver_decision FROM agent_move WHERE cut_id='old-cut'",
                [],
                |r| Ok((r.get::<_, i64>(0)?, r.get::<_, Option<String>>(1)?)),
            )?;
            assert_eq!(move_state, (7, None));
            for table in [
                "hub_kv_cache",
                "hub_attempt",
                "hub_attempt_send",
                "hub_anchor",
                "hub_held_epoch",
                "hub_epoch_status",
            ] {
                let count = conn.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| {
                    r.get::<_, usize>(0)
                })?;
                assert_eq!(count, 0, "{table} must start empty");
            }
            Ok(())
        })
        .unwrap();
    drop(store);

    let error = RegistryStore::open_with_migrations(&live, legacy_chain).unwrap_err();
    let expected = format!("schema version {new}, ahead of this binary's highest migration {old}");
    assert!(matches!(error, RegistryError::Database(ref msg) if msg.contains(&expected)));
    let reopened = RegistryStore::open(&live).unwrap();
    assert_eq!(reopened.identity_log_status().unwrap(), legacy_status);
    drop(reopened);

    let restored = descriptor(&f, "restored.db");
    fs::copy(snapshot, f.root.join("restored.db")).unwrap();
    let restored = RegistryStore::open_with_migrations(&restored, legacy_chain).unwrap();
    assert_eq!(restored.identity_log_status().unwrap(), legacy_status);
    assert!(restored
        .read(|conn| conn.query_row(
            "SELECT hub_log_base FROM identity_log_state",
            [],
            |_| Ok(())
        ))
        .is_err());
}

fn write(request_key: &str) -> HubAttemptWrite {
    HubAttemptWrite {
        request_key: request_key.into(),
        params_digest: "digest".into(),
        key: "residence.agent".into(),
        expected: 83,
        value: vec![0, 255, 6],
        step: "residence".into(),
    }
}

fn send(op_id: &str, sent_at_ms: u64, process: &str) -> HubAttemptSend {
    HubAttemptSend {
        op_id: op_id.into(),
        sent_at_ms,
        process: process.into(),
    }
}

#[test]
fn hub_store_attempt_reseal_keeps_the_original_send_across_restarts() {
    let f = Fixture::new("hub-reseal");
    let path = descriptor(&f, "attempt.db");
    let store = RegistryStore::open(&path).unwrap();
    let original = write("req");
    let first = send("a", 52, "before-restart");
    assert!(store.persist_hub_attempt(&original, &first, None).unwrap());
    drop(store);

    let store = RegistryStore::open(&path).unwrap();
    let open = store.hub_attempt("req").unwrap().unwrap();
    assert_eq!(open.write, original);
    assert_eq!(open.op_ids, std::slice::from_ref(&first));
    let resend = send("a", 12, "after-restart");
    assert!(store
        .persist_hub_attempt(&original, &resend, Some(&open))
        .unwrap());
    assert_eq!(store.hub_attempt("req").unwrap().unwrap(), open);

    let mut resealed = original.clone();
    resealed.value = vec![255, 0, 7];
    let second = send("b", 13, "after-restart");
    assert!(store
        .persist_hub_attempt(&resealed, &second, Some(&open))
        .unwrap());
    assert!(!store
        .persist_hub_attempt(&original, &first, Some(&open))
        .unwrap());
    let open = store.hub_attempt("req").unwrap().unwrap();
    assert_eq!(open.write, resealed);
    assert_eq!(open.op_ids, [first, second]);

    let different_params = HubAttemptWrite {
        params_digest: "unrelated digest".into(),
        ..resealed
    };
    assert!(!store
        .persist_hub_attempt(&different_params, &resend, Some(&open))
        .unwrap());

    let outcome = HubAttemptOutcome {
        op_id: "a".into(),
        value: json!({"status": "landed"}),
        recorded_at_ms: 1_000,
    };
    assert!(store
        .record_hub_attempt_outcome(&original, &outcome)
        .unwrap());
    let closed = store.hub_attempt("req").unwrap().unwrap();
    assert_eq!(closed.outcome, Some(outcome));
    assert!(!store
        .persist_hub_attempt(&original, &resend, Some(&closed))
        .unwrap());
}

#[test]
fn hub_store_attempt_retention_is_seven_days_after_the_first_outcome() {
    let f = Fixture::new("hub-retention");
    let path = descriptor(&f, "attempt.db");
    let store = RegistryStore::open(&path).unwrap();
    let closed = write("closed");
    let unresolved = write("unresolved");
    let first = send("op", 10, "first-process");
    for write in [&closed, &unresolved] {
        assert!(store.persist_hub_attempt(write, &first, None).unwrap());
    }
    let outcome = HubAttemptOutcome {
        op_id: first.op_id.clone(),
        value: json!({"status": "superseded", "revision": 84}),
        recorded_at_ms: 1_700_000_000_000,
    };
    assert!(store.record_hub_attempt_outcome(&closed, &outcome).unwrap());
    drop(store);

    let store = RegistryStore::open(&path).unwrap();
    let deadline = outcome.recorded_at_ms + 604_800_000;
    assert!(!store
        .record_hub_attempt_outcome(
            &closed,
            &HubAttemptOutcome {
                op_id: first.op_id.clone(),
                value: json!({"status": "landed"}),
                recorded_at_ms: deadline - 1,
            },
        )
        .unwrap());
    assert_eq!(store.prune_hub_attempts(deadline - 1).unwrap(), 0);
    let attempt = store.hub_attempt("closed").unwrap().unwrap();
    assert_eq!(attempt.write, closed);
    assert_eq!(attempt.op_ids, std::slice::from_ref(&first));
    assert_eq!(attempt.outcome, Some(outcome));

    assert_eq!(store.prune_hub_attempts(deadline).unwrap(), 1);
    assert!(store.hub_attempt("closed").unwrap().is_none());
    assert_eq!(store.prune_hub_attempts(deadline + 604_800_000).unwrap(), 0);
    let attempt = store.hub_attempt("unresolved").unwrap().unwrap();
    assert_eq!(attempt.write, unresolved);
    assert_eq!(attempt.outcome, None);
    let new_send = send("op", 40, "different-process");
    assert!(store.persist_hub_attempt(&closed, &new_send, None).unwrap());
    assert_eq!(
        store.hub_attempt("closed").unwrap().unwrap().op_ids,
        [new_send]
    );
}

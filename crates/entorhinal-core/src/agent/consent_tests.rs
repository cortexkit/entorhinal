use super::consent::*;
use super::reads::tests::{create, fixture, placed, result, Fixture};
use crate::{RegistryStore, MIGRATIONS};
use cortexkit_store::StorageDescriptor;
use cortexkit_store_types::StorageBackend;
use serde_json::{json, Value};

fn request(agent: &str, tool: &str, key: &str) -> ConsentRequest {
    ConsentRequest {
        identity: RequestIdentity {
            agent_id: agent.into(),
            tool: tool.into(),
            request_key: key.into(),
        },
        args_digest: "digest".into(),
        scope_ref: "scope".into(),
        scope_epoch: 1,
        created_at_ms: 100,
        elicitation_id: None,
        settlement: None,
        state: RequestState::Awaiting,
        outcome: None,
    }
}
fn commit(agent: &str, key: &str, authorization: Authorization) -> ConsentCommit {
    ConsentCommit {
        request: request(agent, "agents_hire", key),
        authorization,
        action: "agents_hire".into(),
        target: json!({"project_id":"P"}),
        granted: None,
        now_ms: 100,
    }
}
fn hire(name: &str) -> HireRequest {
    HireRequest {
        name: name.into(),
        tag: "worker".into(),
        labels: vec!["build".into()],
    }
}
fn project_head() -> (Fixture, String) {
    let f = fixture(true);
    placed(&f, "P", "W");
    let head = create(&f, "Head", "head", Some("P"), None);
    (f, head)
}
fn count(f: &Fixture, table: &str) -> i64 {
    f.store
        .read(|conn| conn.query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0)))
        .unwrap()
}
fn grant(agent: &str) -> StandingGrant {
    StandingGrant {
        id: "grant1".into(),
        holder_agent_id: agent.into(),
        class: GrantClass::Hire,
        scope_id: "P".into(),
        granted_at_ms: 100,
        card_ref: "card1".into(),
        revoked_at_ms: None,
    }
}
fn enable(f: &Fixture) {
    f.store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='enabled' WHERE id=1",
                [],
            )?;
            crate::mutations::append(
                tx,
                "identity_log.enable",
                &json!({}),
                "test",
                None,
                100,
                "test",
            )?;
            Ok(())
        })
        .unwrap();
}

#[test]
fn request_keys_are_ascii_namespaced_and_conflicts_survive_terminal_state() {
    let f = fixture(true);
    for key in ["", "\n", "é", "\u{7f}", &"a".repeat(129)] {
        assert_eq!(
            f.store
                .consent_transaction(|tx| tx.begin_request(&request("a", "tool", key)))
                .unwrap_err()
                .code,
            "invalid_request_key"
        );
    }
    for key in [" ", "~", &"a".repeat(128)] {
        f.store
            .consent_transaction(|tx| tx.begin_request(&request("a", "tool", key)))
            .unwrap();
    }
    for (agent, tool) in [("a", "one"), ("a", "two"), ("b", "one")] {
        let r = request(agent, tool, "same");
        f.store
            .consent_transaction(|tx| {
                tx.begin_request(&r)?;
                tx.finish_request(
                    &r.identity,
                    &r.args_digest,
                    RequestState::Refused,
                    &json!({"code":"consent_declined"}),
                )
            })
            .unwrap();
        let mut changed = r.clone();
        changed.args_digest = "different".into();
        assert_eq!(
            f.store
                .consent_transaction(|tx| tx.begin_request(&changed))
                .unwrap_err()
                .code,
            "request_key_conflict"
        );
        assert_eq!(
            f.store
                .consent_transaction(|tx| tx.begin_request(&r))
                .unwrap()
                .state,
            RequestState::Refused
        );
    }
    assert_eq!(count(&f, "consent_request"), 6);
}

#[test]
fn settlement_capture_matches_binding_and_page_failure_rolls_back_cursor() {
    let f = fixture(true);
    let r = request("a", "tool", "key");
    f.store
        .consent_transaction(|tx| tx.begin_request(&r))
        .unwrap();
    let answer = RecordedSettlement {
        state: "answered".into(),
        choice: Some("approve_once".into()),
        elicitation_id: "card".into(),
        settled_at_ms: 200,
    };
    for (digest, scope, epoch) in [
        ("wrong", "scope", 1),
        ("digest", "wrong", 1),
        ("digest", "scope", 2),
    ] {
        assert!(!f
            .store
            .consent_transaction(|tx| tx.record_settlement(
                &r.identity,
                digest,
                scope,
                epoch,
                &answer
            ))
            .unwrap());
    }
    let failed: Result<(), _> = f.store.consent_transaction(|tx| {
        assert!(tx.record_settlement(&r.identity, "digest", "scope", 1, &answer)?);
        tx.save_feed_cursor("scope", 1, 12)?;
        tx.finish_request(&r.identity, "wrong", RequestState::Refused, &json!({}))
    });
    assert!(failed.is_err());
    assert_eq!(f.store.consent_feed_cursor("scope", 1).unwrap(), 0);
    assert_eq!(
        f.store
            .consent_request(&r.identity)
            .unwrap()
            .unwrap()
            .settlement,
        None
    );
    f.store
        .consent_transaction(|tx| {
            assert!(tx.record_settlement(&r.identity, "digest", "scope", 1, &answer)?);
            tx.save_feed_cursor("scope", 1, 12)
        })
        .unwrap();
    let saved = f.store.consent_request(&r.identity).unwrap().unwrap();
    assert_eq!(saved.state, RequestState::Awaiting);
    assert_eq!(saved.elicitation_id.as_deref(), Some("card"));
    assert_eq!(saved.settlement, Some(answer));
    assert_eq!(f.store.consent_feed_cursor("scope", 1).unwrap(), 12);
}

#[test]
fn consent_state_is_local_persistent_and_not_rebuilt_or_exported() {
    let (f, head) = project_head();
    enable(&f);
    let before = f.store.generation().unwrap();
    let r = request(&head, "agents_hire", "key");
    f.store
        .consent_transaction(|tx| {
            tx.begin_request(&r)?;
            tx.put_grant(&grant(&head))?;
            tx.save_feed_cursor("scope", 1, 9)?;
            tx.append_audit(&AuditRow {
                row_id: 0,
                time_ms: 100,
                agent_id: head.clone(),
                action: "refused".into(),
                target: json!("P"),
                authorization: Authorization::OwnHire,
                outcome: json!({"code":"consent_lost"}),
            })?;
            Ok(())
        })
        .unwrap();
    f.store.set_project_hire_cap("P", 3).unwrap();
    assert_eq!(f.store.generation().unwrap(), before);
    assert!(f
        .store
        .agent_changes(before, None)
        .unwrap()
        .entries
        .is_empty());
    let shared = f
        .store
        .read(crate::shared_entry::SharedState::capture)
        .unwrap();
    let image = serde_json::to_value(shared.snapshot(vec![])).unwrap();
    assert!(!image.to_string().contains("consent_"));
    assert!(!image.to_string().contains("agent_audit"));
    assert!(!image.to_string().contains("project_hire_cap"));
    assert!(f.store.verify().unwrap().ok);
    f.store.rebuild().unwrap();
    assert_eq!(
        f.store.consent_request(&r.identity).unwrap(),
        Some(r.clone())
    );
    for table in [
        "consent_request",
        "consent_grant",
        "consent_feed_cursor",
        "agent_audit",
        "project_hire_cap",
    ] {
        assert_eq!(
            count(&f, table),
            1,
            "local table {table} must survive rebuild"
        );
    }
    let descriptor = StorageDescriptor {
        module_id: "agent-read-test".into(),
        storage_namespace: "test".into(),
        isolation: cortexkit_store_types::Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: f.root.join("store.db").to_string_lossy().into_owned(),
        },
    };
    drop(f.store);
    let restarted = RegistryStore::open(&descriptor).unwrap();
    assert_eq!(restarted.consent_request(&r.identity).unwrap(), Some(r));
    assert_eq!(restarted.consent_feed_cursor("scope", 1).unwrap(), 9);
    assert_eq!(restarted.project_hire_cap("P").unwrap().cap, 3);
    assert_eq!(
        restarted
            .agent_audit_page(None, None, None)
            .unwrap()
            .rows
            .len(),
        1
    );
    assert!(restarted.verify().unwrap().ok);
}

#[test]
fn hire_cap_defaults_to_ten_and_operator_creation_is_not_gated() {
    let (f, head) = project_head();
    assert_eq!(
        f.store.project_hire_cap("P").unwrap(),
        HireCap {
            cap: 10,
            live_hires: 0
        }
    );
    for n in 0..11 {
        create(&f, &format!("Operator {n}"), "hiree", Some("P"), None);
    }
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 11);
    let error = f
        .store
        .with_principal("head")
        .precheck_agent_hire(&head, &hire("Hire"), 100)
        .unwrap_err();
    assert_eq!(error.code, "hire_cap_reached");
    assert_eq!(error.detail, Some(json!({"cap":10,"current_count":11})));
    assert_eq!(count(&f, "consent_request"), 0);
    assert_eq!(count(&f, "agent_audit"), 0);
    f.store.set_project_hire_cap("P", 0).unwrap();
    assert_eq!(
        f.store
            .with_principal("head")
            .precheck_agent_hire(&head, &hire("Hire"), 100)
            .unwrap_err()
            .code,
        "hire_cap_reached"
    );
    assert_eq!(
        f.store.set_project_hire_cap("P", -1).unwrap_err().code,
        "invalid_request"
    );
    assert!(f.store.verify().unwrap().ok);
    f.store.rebuild().unwrap();
    assert_eq!(
        f.store.project_hire_cap("P").unwrap(),
        HireCap {
            cap: 0,
            live_hires: 11
        }
    );
}

#[test]
fn hire_cap_is_rechecked_in_transaction_even_with_a_standing_grant() {
    let (f, head) = project_head();
    f.store
        .consent_transaction(|tx| tx.put_grant(&grant(&head)))
        .unwrap();
    let writer = f.store.with_principal("head").with_consent_commit(commit(
        &head,
        "key",
        Authorization::Grant {
            id: "grant1".into(),
        },
    ));
    writer
        .precheck_agent_hire(&head, &hire("Hire"), 100)
        .unwrap();
    f.store.set_project_hire_cap("P", 0).unwrap();
    let before = f.store.generation().unwrap();
    assert_eq!(
        writer
            .agents_hire(&hire("Hire"), 100, None)
            .unwrap_err()
            .code,
        "hire_cap_reached"
    );
    assert_eq!(f.store.generation().unwrap(), before);
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 0);
    assert_eq!(count(&f, "consent_request"), 0);
    assert_eq!(count(&f, "agent_audit"), 0);
    f.store.set_project_hire_cap("P", 1).unwrap();
    writer
        .precheck_agent_hire(&head, &hire("Hire"), 100)
        .unwrap();
    create(&f, "Concurrent operator hire", "hiree", Some("P"), None);
    assert_eq!(
        writer
            .agents_hire(&hire("Hire"), 100, None)
            .unwrap_err()
            .code,
        "hire_cap_reached"
    );
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 1);
}

#[test]
fn journal_authorization_variants_survive_export_and_replay_without_local_authority() {
    let (f, head) = project_head();
    enable(&f);
    for (index, authorization) in [
        Authorization::Card {
            id: "card1".into(),
            answer: "approve_once".into(),
        },
        Authorization::Grant {
            id: "grant1".into(),
        },
    ]
    .into_iter()
    .enumerate()
    {
        if matches!(authorization, Authorization::Grant { .. }) {
            f.store
                .consent_transaction(|tx| tx.put_grant(&grant(&head)))
                .unwrap();
        }
        let key = format!("key{index}");
        let writer = f.store.with_principal("head").with_consent_commit(commit(
            &head,
            &key,
            authorization.clone(),
        ));
        let body = result(
            &writer
                .agents_hire(&hire(&format!("Hire {index}")), 100, None)
                .unwrap(),
        );
        let row = f
            .store
            .agent_row(body["agent"]["agent_id"].as_str().unwrap())
            .unwrap()
            .unwrap();
        assert_eq!(row.labels, vec!["build"]);
        assert_eq!(row.tag, "worker");
        assert_eq!(row.supervisor_agent_id.as_deref(), Some(head.as_str()));
        let entry = f
            .store
            .agent_changes(0, None)
            .unwrap()
            .entries
            .pop()
            .unwrap();
        assert_eq!(entry.requesting_agent_id, Some(head.clone()));
        assert_eq!(entry.authorization, Some(authorization.clone()));
        let exported: Value = f
            .store
            .read(|conn| {
                conn.query_row(
                    "SELECT entry FROM registry_journal WHERE seq=?1",
                    [entry.seq],
                    |r| {
                        serde_json::from_slice(&r.get::<_, Vec<u8>>(0)?)
                            .map_err(crate::shared_entry::decode_error)
                    },
                )
            })
            .unwrap();
        assert_eq!(exported["authorization"], json!(authorization));
        assert_eq!(exported["requesting_agent_id"], json!(head));
        assert_eq!(exported["row"]["request_key"], Value::Null);
    }
    f.store.db.with_conn_fenced(|tx|tx.execute_batch("DELETE FROM consent_grant; DELETE FROM consent_request; DELETE FROM agent_audit; DELETE FROM consent_feed_cursor;")).unwrap();
    assert!(f.store.verify().unwrap().ok);
    f.store.rebuild().unwrap();
    let changes = f.store.agent_changes(0, None).unwrap().entries;
    assert_eq!(
        changes.iter().filter(|e| e.authorization.is_some()).count(),
        2
    );
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 2);
    assert_eq!(count(&f, "consent_request"), 0);
}

#[test]
fn own_hire_retirement_journal_carries_the_authorization() {
    let (f, head) = project_head();
    enable(&f);
    let target = result(&f.store.agent_mutation("agent.create",json!({"role":"hiree","name":"Worker","tag":"worker","project_id":"P","supervisor_agent_id":head,"request_key":"own-worker"}),100).unwrap())["agent"]["agent_id"].as_str().unwrap().to_owned();
    let mut done = commit(&head, "retire", Authorization::OwnHire);
    done.action = "agents_retire_hire".into();
    done.request.identity.tool = done.action.clone();
    done.target = json!({"agent_id":target});
    let writer = f.store.with_principal("head").with_consent_commit(done);
    writer
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":target,"request_key":"same-as-operator"}),
            100,
        )
        .unwrap();
    let entry = f
        .store
        .agent_changes(0, None)
        .unwrap()
        .entries
        .pop()
        .unwrap();
    assert_eq!(entry.authorization, Some(Authorization::OwnHire));
    f.store.rebuild().unwrap();
    assert_eq!(
        f.store.agent_row(&target).unwrap().unwrap().status,
        "retired"
    );
    assert_eq!(
        f.store.agent_audit_page(None, None, None).unwrap().rows[0].authorization,
        Authorization::OwnHire
    );
}

#[test]
fn terminal_retry_is_atomic_does_not_remint_and_rejects_changed_digest() {
    let (f, head) = project_head();
    let mut done = commit(
        &head,
        "key",
        Authorization::Card {
            id: "card1".into(),
            answer: "approve_always_project".into(),
        },
    );
    done.granted = Some(grant(&head));
    let writer = f
        .store
        .with_principal("head")
        .with_consent_commit(done.clone());
    let first = writer.agents_hire(&hire("Worker"), 100, None).unwrap();
    f.store
        .consent_transaction(|tx| tx.revoke_grant("grant1", 200))
        .unwrap();
    f.store.set_project_hire_cap("P", 0).unwrap();
    done.request.scope_epoch = 9;
    assert_eq!(
        f.store
            .with_principal("head")
            .with_consent_commit(done.clone())
            .agents_hire(&hire("Worker"), 100, None)
            .unwrap(),
        first
    );
    assert_eq!(count(&f, "agent_audit"), 1);
    assert_eq!(count(&f, "consent_request"), 1);
    assert_eq!(
        f.store.consent_grants().unwrap()[0].revoked_at_ms,
        Some(200)
    );
    done.request.args_digest = "changed".into();
    assert_eq!(
        f.store
            .with_principal("head")
            .with_consent_commit(done)
            .agents_hire(&hire("Worker"), 100, None)
            .unwrap_err()
            .code,
        "request_key_conflict"
    );
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 1);
}

#[test]
fn grants_refresh_in_place_and_revocation_blocks_a_new_write() {
    let (f, head) = project_head();
    f.store
        .consent_transaction(|tx| tx.put_grant(&grant(&head)))
        .unwrap();
    let mut replacement = grant(&head);
    replacement.id = "ignored-new-id".into();
    replacement.card_ref = "card2".into();
    replacement.granted_at_ms = 200;
    assert_eq!(
        f.store
            .consent_transaction(|tx| tx.put_grant(&replacement))
            .unwrap(),
        "grant1"
    );
    assert_eq!(f.store.consent_grants().unwrap()[0].card_ref, "card2");
    f.store
        .consent_transaction(|tx| tx.revoke_grant("grant1", 300))
        .unwrap();
    let writer = f.store.with_principal("head").with_consent_commit(commit(
        &head,
        "key",
        Authorization::Grant {
            id: "grant1".into(),
        },
    ));
    assert_eq!(
        writer
            .agents_hire(&hire("Worker"), 100, None)
            .unwrap_err()
            .code,
        "consent_stale"
    );
    assert_eq!(count(&f, "agent_audit"), 0);
    assert_eq!(count(&f, "consent_request"), 0);
    assert_eq!(f.store.project_hire_cap("P").unwrap().live_hires, 0);
}

#[test]
fn audit_pages_visit_tied_timestamps_once_and_since_is_inclusive() {
    let f = fixture(true);
    f.store
        .consent_transaction(|tx| {
            for time in [9, 10, 10, 10, 11] {
                tx.append_audit(&AuditRow {
                    row_id: 0,
                    time_ms: time,
                    agent_id: "head".into(),
                    action: "hire".into(),
                    target: json!("P"),
                    authorization: Authorization::OwnHire,
                    outcome: json!({}),
                })?;
            }
            Ok(())
        })
        .unwrap();
    let mut cursor = None;
    let mut ids = vec![];
    loop {
        let page = f
            .store
            .agent_audit_page(Some(10), Some(2), cursor.as_deref())
            .unwrap();
        ids.extend(page.rows.into_iter().map(|r| r.row_id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(ids, vec![5, 4, 3, 2]);
    assert_eq!(
        f.store
            .agent_audit_page(None, Some(201), None)
            .unwrap_err()
            .code,
        "invalid_request"
    );
    assert_eq!(
        f.store
            .agent_audit_page(None, None, Some("bad"))
            .unwrap_err()
            .code,
        "invalid_request"
    );
}

#[test]
fn consent_schema_upgrades_existing_store_without_changing_the_journal() {
    let f = fixture(true);
    let descriptor = StorageDescriptor {
        module_id: "agent-read-test".into(),
        storage_namespace: "test".into(),
        isolation: cortexkit_store_types::Isolation::Module,
        backend: StorageBackend::Sqlite {
            path: f.root.join("old.db").to_string_lossy().into_owned(),
        },
    };
    drop(f.store);
    let old = RegistryStore::open_with_migrations(&descriptor, &MIGRATIONS[..9]).unwrap();
    old.apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
        .unwrap();
    let before = old.generation().unwrap();
    drop(old);
    let upgraded = RegistryStore::open(&descriptor).unwrap();
    assert_eq!(upgraded.generation().unwrap(), before);
    assert_eq!(upgraded.consent_feed_cursor("scope", 1).unwrap(), 0);
    assert!(upgraded.verify().unwrap().ok);
    upgraded.rebuild().unwrap();
    assert_eq!(upgraded.consent_grants().unwrap(), vec![]);
}

#[test]
fn project_writes_export_authorization_and_rebuild_without_grants() {
    let f = fixture(true);
    let assistant = create(&f, "Assistant", "assistant", None, None);
    enable(&f);
    let mut done = commit(
        &assistant,
        "project",
        Authorization::Card {
            id: "project-card".into(),
            answer: "approve_once".into(),
        },
    );
    done.request.identity.tool = "projects_register".into();
    done.action = "projects_register".into();
    let writer = f
        .store
        .with_principal("assistant")
        .with_consent_commit(done);
    writer
        .register_at(
            crate::RegisterRequest {
                project_id: Some("P".into()),
                request_key: Some("create-Assistant-None-None".into()),
                name: "P".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            },
            100,
        )
        .unwrap();
    let exported: Value = f
        .store
        .read(|conn| {
            conn.query_row(
                "SELECT entry FROM registry_journal WHERE op='register'",
                [],
                |r| {
                    serde_json::from_slice(&r.get::<_, Vec<u8>>(0)?)
                        .map_err(crate::shared_entry::decode_error)
                },
            )
        })
        .unwrap();
    assert_eq!(exported["requesting_agent_id"], json!(assistant));
    assert_eq!(
        exported["authorization"],
        json!({"kind":"card","id":"project-card","answer":"approve_once"})
    );
    assert!(exported["tables"].get("consent_request").is_none());
    f.store.rebuild().unwrap();
    assert!(f.store.verify().unwrap().ok);
    assert_eq!(
        f.store
            .agent_audit_page(None, None, None)
            .unwrap()
            .rows
            .len(),
        1
    );
}

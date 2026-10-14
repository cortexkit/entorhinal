use super::*;
use crate::mutations::tests::Fixture;
use crate::remote_apply::{RemoteEntry, ENVELOPE_VERSION};

pub(crate) fn agent(store: &RegistryStore) -> String {
    store
        .apply_entry("agent.cutover", "{}", "module", None, |_| Ok(()))
        .unwrap();
    let reply: Value = serde_json::from_slice(
        &store
            .agent_mutation(
                "agent.create",
                json!({"role":"assistant","name":"Ada","tag":"catalog","request_key":"agent"}),
                1,
            )
            .unwrap(),
    )
    .unwrap();
    reply["result"]["agent"]["agent_id"]
        .as_str()
        .unwrap()
        .into()
}
pub(crate) fn pin(name: &str, version: &str, root: bool) -> CatalogPin {
    CatalogPin {
        name: name.into(),
        version: version.into(),
        kind: Kind::Skill,
        manifest_sha256: "11".repeat(32),
        archive_sha256: "22".repeat(32),
        publisher_fingerprint: "33".repeat(32),
        root,
        range: root.then(|| "^1".into()),
    }
}
fn write(store: &RegistryStore, agent: &str, pins: Vec<CatalogPin>) {
    store
        .with_principal("test")
        .catalog_set_pins(
            PinWriteRequest {
                agent_id: agent.into(),
                pins,
                request_key: None,
            },
            2,
        )
        .unwrap();
}
fn spec() -> CatalogSpec {
    CatalogSpec {
        package: "@acme/a".into(),
        range: "^1".into(),
    }
}
fn plan(store: &RegistryStore, id: &str, agent: &str) -> CatalogPlan {
    store
        .catalog_create_plan(
            id,
            agent,
            CatalogTarget::Agent(agent.into()),
            "install",
            spec(),
            3,
        )
        .unwrap()
}
fn ready(store: &RegistryStore, id: &str, agent: &str) -> CatalogPlan {
    plan(store, id, agent);
    store
        .catalog_plan_ready(id, agent, vec![pin("@acme/a", "1.0.0", true)], 4)
        .unwrap()
}
fn generation(store: &RegistryStore, id: &str, agent: &str) -> CatalogGeneration {
    ready(store, id, agent);
    store.catalog_approve_plan(id, agent, id, 5).unwrap()
}
fn refused<T: std::fmt::Debug>(result: Result<T, CatalogWriteError>, code: &str) {
    assert!(
        matches!(result,Err(CatalogWriteError::Refused(c)) if c==code),
        "expected {code}"
    );
}

#[test]
fn digest_golden_is_order_independent_and_root_sensitive() {
    let pins = vec![
        pin("@acme/b", "1.2.3", false),
        pin("@acme/a", "1.0.0", true),
    ];
    assert_eq!(
        pin_digest(&pins).unwrap(),
        "3e7243da8aea2df9130a8ad8ce76397c781278c0a90b21a6581d73ff3806583b"
    );
    let mut reversed = pins.clone();
    reversed.reverse();
    assert_eq!(pin_digest(&pins).unwrap(), pin_digest(&reversed).unwrap());
    reversed[0].range = Some("~1.0".into());
    assert_ne!(pin_digest(&pins).unwrap(), pin_digest(&reversed).unwrap());
}
#[test]
fn malformed_pins_are_refused() {
    let good = pin("@acme/a", "1.0.0", true);
    let mut cases = vec![vec![good.clone(), good.clone()]];
    for field in [
        "name",
        "version",
        "manifest",
        "archive",
        "publisher",
        "root",
        "range",
    ] {
        let mut bad = good.clone();
        match field {
            "name" => bad.name = "@ACME/a".into(),
            "version" => bad.version = "1".into(),
            "manifest" => bad.manifest_sha256 = "ff".into(),
            "archive" => bad.archive_sha256 = "GG".repeat(32),
            "publisher" => bad.publisher_fingerprint = "00".into(),
            "root" => bad.root = false,
            _ => bad.range = Some("???".into()),
        }
        cases.push(vec![bad]);
    }
    for bad in cases {
        assert!(
            validate_pins(&bad).is_err(),
            "malformed pins accepted: {bad:?}"
        );
    }
}
#[test]
fn pins_are_replayed_but_never_agent_changes() {
    let f = Fixture::new("catalog-feed");
    let id = agent(&f.store);
    let cursor = f.store.agent_snapshot().unwrap().generation;
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    let feed = f.store.agent_changes(cursor, Some(1)).unwrap();
    assert!(
        feed.entries.is_empty(),
        "catalog pin escaped into identity feed"
    );
    assert!(feed.cursor > cursor);
    assert!(f.store.verify().unwrap().replay.ok);
    f.store.rebuild().unwrap();
    assert_eq!(
        f.store.catalog_pins(&id).unwrap(),
        vec![pin("@acme/a", "1.0.0", true)]
    );
    assert!(f.store.verify().unwrap().replay.ok);
}
#[test]
fn remote_pin_apply_refuses_unknown_terminal_digest_and_shape_atomically() {
    let f = Fixture::new("catalog-remote");
    let id = agent(&f.store);
    let set = CatalogPinSet {
        op: "catalog.pin".into(),
        agent_id: id.clone(),
        pins: vec![pin("@acme/a", "1.0.0", true)],
        pin_digest: pin_digest(&[pin("@acme/a", "1.0.0", true)]).unwrap(),
    };
    let entry = |set: &CatalogPinSet| RemoteEntry {
        position: 1,
        entry_id: "pin-1".into(),
        author: "aa".repeat(16),
        signer: "test".into(),
        key_id: "key".into(),
        envelope_version: ENVELOPE_VERSION,
        kind: "change".into(),
        entry: serde_json::to_vec(set).unwrap(),
    };
    for bad in [
        CatalogPinSet {
            agent_id: "missing".into(),
            ..set.clone()
        },
        CatalogPinSet {
            pin_digest: "00".repeat(32),
            ..set.clone()
        },
        CatalogPinSet {
            pins: vec![set.pins[0].clone(), set.pins[0].clone()],
            ..set.clone()
        },
    ] {
        assert!(f.store.apply_remote_entry(&entry(&bad), None).is_err());
        assert!(f.store.catalog_pins(&id).unwrap().is_empty());
        assert_eq!(
            f.store.identity_log_status().unwrap().last_applied_position,
            0
        );
    }
    f.store.apply_remote_entry(&entry(&set), None).unwrap();
    assert_eq!(f.store.catalog_pins(&id).unwrap(), set.pins);
    f.store
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"retire"}),
            3,
        )
        .unwrap();
    let mut terminal = entry(&set);
    terminal.position = 2;
    terminal.entry_id = "pin-2".into();
    let error = f
        .store
        .apply_remote_entry(&terminal, None)
        .unwrap_err()
        .to_string();
    assert!(error.contains("not live"), "wrong remote refusal: {error}");
}
#[test]
fn project_delta_cannot_bypass_pin_entry_validation() {
    let f = Fixture::new("catalog-project-bypass");
    let id = agent(&f.store);
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    let mut snapshot = f
        .store
        .read(|c| Ok(crate::shared_entry::SharedState::capture(c)?.snapshot(vec![])))
        .unwrap();
    snapshot.op = "project.shared".into();
    assert!(f.store.read(|c| snapshot.validate(c)).is_err());
}
#[test]
fn plan_and_generation_transitions_replay_the_original_results() {
    let f = Fixture::new("catalog-transitions");
    let id = agent(&f.store);
    let head = f.store.agent_snapshot().unwrap().generation;
    let p = plan(&f.store, "p", &id);
    assert_eq!(p, plan(&f.store, "p", &id));
    let r = f
        .store
        .catalog_plan_ready("p", &id, vec![pin("@acme/a", "1.0.0", true)], 4)
        .unwrap();
    assert_eq!(
        r,
        f.store
            .catalog_plan_ready("p", &id, vec![pin("@acme/a", "1.0.0", true)], 999)
            .unwrap()
    );
    assert_eq!(p, plan(&f.store, "p", &id));
    let g = f.store.catalog_approve_plan("p", &id, "g", 5).unwrap();
    assert_eq!(g.state, "approved");
    let ready = f.store.catalog_generation_ready("g", &id).unwrap();
    let active = f.store.catalog_activate_generation("g", &id, 6).unwrap();
    let degraded = f
        .store
        .catalog_degrade_generation("g", &id, json!({"code":"revoked"}))
        .unwrap();
    assert_eq!(degraded.state, "degraded");
    assert_eq!(g, f.store.catalog_approve_plan("p", &id, "g", 99).unwrap());
    assert_eq!(ready, f.store.catalog_generation_ready("g", &id).unwrap());
    assert_eq!(
        active,
        f.store.catalog_activate_generation("g", &id, 99).unwrap()
    );
    assert_eq!(
        degraded,
        f.store
            .catalog_degrade_generation("g", &id, json!({"code":"revoked"}))
            .unwrap()
    );
    let failed = plan(&f.store, "failed", &id);
    assert_eq!(failed.state, "pending");
    let failed = f
        .store
        .catalog_plan_failed("failed", &id, json!({"code":"interrupted"}))
        .unwrap();
    assert_eq!(
        failed,
        f.store
            .catalog_plan_failed("failed", &id, json!({"code":"interrupted"}))
            .unwrap()
    );
    assert_eq!(
        f.store.agent_snapshot().unwrap().generation,
        head,
        "local transitions journaled"
    );
    assert!(f.store.verify().unwrap().replay.ok);
    f.store.rebuild().unwrap();
    assert_eq!(f.store.catalog_generation("g", &id).unwrap(), degraded);
    assert_eq!(f.store.catalog_plan("failed", &id).unwrap(), failed);
}
#[test]
fn transition_guards_reject_cross_owner_wrong_state_and_reused_ids() {
    let f = Fixture::new("catalog-transition-guards");
    let id = agent(&f.store);
    plan(&f.store, "p", &id);
    refused(
        f.store.catalog_plan_ready("p", "other", vec![], 4),
        "caller_forbidden",
    );
    refused(
        f.store.catalog_create_plan(
            "p",
            &id,
            CatalogTarget::Agent(id.clone()),
            "install",
            CatalogSpec {
                range: "^2".into(),
                ..spec()
            },
            3,
        ),
        "request_key_conflict",
    );
    refused(
        f.store.catalog_approve_plan("p", &id, "g", 5),
        "invalid_transition",
    );
    f.store
        .catalog_plan_failed("p", &id, json!({"code":"failed"}))
        .unwrap();
    refused(
        f.store.catalog_plan_ready("p", &id, vec![], 4),
        "invalid_transition",
    );
    generation(&f.store, "g", &id);
    refused(
        f.store.catalog_activate_generation("g", &id, 6),
        "invalid_transition",
    );
    refused(
        f.store.catalog_degrade_generation("g", &id, json!({})),
        "invalid_transition",
    );
    refused(
        f.store.catalog_approve_plan("g", &id, "another", 5),
        "request_key_conflict",
    );
}
#[test]
fn single_active_swap_keeps_superseded_and_schema_refuses_second_active() {
    let f = Fixture::new("catalog-active");
    let id = agent(&f.store);
    generation(&f.store, "g1", &id);
    f.store.catalog_generation_ready("g1", &id).unwrap();
    f.store.catalog_activate_generation("g1", &id, 6).unwrap();
    generation(&f.store, "g2", &id);
    f.store.catalog_generation_ready("g2", &id).unwrap();
    assert!(f
        .store
        .db
        .with_conn_fenced(|tx| tx.execute(
            "UPDATE catalog_generation SET state='active' WHERE generation_id='g2'",
            []
        ))
        .is_err());
    let active = f.store.catalog_activate_generation("g2", &id, 7).unwrap();
    assert_eq!(
        f.store.catalog_generation("g1", &id).unwrap().state,
        "superseded"
    );
    assert_eq!(
        f.store.catalog_generation("g1", &id).unwrap().closure,
        active.closure
    );
    assert_eq!(
        active,
        f.store.catalog_activate_generation("g2", &id, 99).unwrap()
    );
}
#[test]
fn kind_operation_scope_and_liveness_gates() {
    let f = Fixture::new("catalog-gates");
    let id = agent(&f.store);
    refused(
        f.store.catalog_create_plan(
            "hire",
            &id,
            CatalogTarget::Agent(id.clone()),
            "hire",
            spec(),
            1,
        ),
        "operation_not_yet_supported",
    );
    refused(
        f.store.catalog_create_plan(
            "tier",
            &id,
            CatalogTarget::Tier("workspace".into()),
            "install",
            spec(),
            1,
        ),
        "operation_not_yet_supported",
    );
    refused(
        f.store.catalog_create_plan(
            "other",
            &id,
            CatalogTarget::Agent("other".into()),
            "install",
            spec(),
            1,
        ),
        "caller_forbidden",
    );
    plan(&f.store, "kind", &id);
    let mut bad = pin("@acme/a", "1.0.0", true);
    bad.kind = Kind::Flow;
    let failed = f
        .store
        .catalog_plan_ready("kind", &id, vec![bad], 4)
        .unwrap();
    assert_eq!(failed.state, "failed");
    assert_eq!(
        failed.refusal.unwrap(),
        json!({"code":"kind_not_yet_supported","package":"@acme/a"})
    );
    refused(
        f.store.catalog_approve_plan("kind", &id, "g", 5),
        "invalid_transition",
    );
    plan(&f.store, "live", &id);
    f.store
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"retire"}),
            6,
        )
        .unwrap();
    refused(f.store.catalog_plan_ready("live", &id, vec![], 7), "gone");
}
#[test]
fn stale_pin_and_status_changes_refuse_approval_and_block_activation() {
    let f = Fixture::new("catalog-stale");
    let id = agent(&f.store);
    ready(&f.store, "p", &id);
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    refused(f.store.catalog_approve_plan("p", &id, "g", 5), "plan_stale");
    generation(&f.store, "g2", &id);
    f.store.catalog_generation_ready("g2", &id).unwrap();
    f.store.db.with_conn_fenced(|tx|tx.execute_batch("INSERT INTO catalog_registry VALUES('r',0,1,'inc'); INSERT INTO catalog_status VALUES('@acme/a','1.0.0','r',1,'t','t',0,'{\"state\":\"tombstoned\"}','s');")).unwrap();
    refused(
        f.store.catalog_activate_generation("g2", &id, 6),
        "activation_blocked",
    );
    assert_eq!(
        f.store.catalog_generation("g2", &id).unwrap().state,
        "ready"
    );
    ready(&f.store, "status", &id);
    f.store
        .db
        .with_conn_fenced(|tx| tx.execute("UPDATE catalog_status SET sequence=2", []))
        .unwrap();
    refused(
        f.store.catalog_approve_plan("status", &id, "g3", 7),
        "plan_stale",
    );
}

#[test]
fn corrupted_pin_projection_is_detected_and_rebuild_repairs_it() {
    let f = Fixture::new("catalog-corruption");
    let id = agent(&f.store);
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    f.store
        .db
        .with_conn_fenced(|tx| tx.execute("UPDATE catalog_pin_set SET pin_digest='bad'", []))
        .unwrap();
    assert!(
        !f.store.verify().unwrap().replay.ok,
        "verify missed corrupted pins"
    );
    f.store.rebuild().unwrap();
    assert!(f.store.verify().unwrap().replay.ok);
}
#[test]
fn snapshot_pins_use_the_same_validator_and_digest() {
    let f = Fixture::new("catalog-snapshot-validation");
    let id = agent(&f.store);
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    let mut snapshot = f
        .store
        .read(|c| Ok(crate::shared_entry::SharedState::capture(c)?.snapshot(vec![])))
        .unwrap();
    snapshot.tables.get_mut("catalog_pin_set").unwrap().upsert[0]
        .insert("pin_digest".into(), json!("bad"));
    assert!(
        f.store
            .db
            .with_conn_fenced(|tx| snapshot.apply(tx, 1))
            .is_err(),
        "bad snapshot digest accepted"
    );
    assert_ne!(f.store.catalog_pin_digest(&id).unwrap(), "bad");
}
#[test]
fn terminal_agent_pin_write_and_generation_activation_are_refused() {
    let f = Fixture::new("catalog-terminal");
    let id = agent(&f.store);
    generation(&f.store, "g", &id);
    f.store.catalog_generation_ready("g", &id).unwrap();
    f.store
        .agent_mutation(
            "agent.dispose",
            json!({"agent_id":id,"request_key":"retire"}),
            6,
        )
        .unwrap();
    assert!(f
        .store
        .with_principal("test")
        .catalog_set_pins(
            PinWriteRequest {
                agent_id: id.clone(),
                pins: vec![pin("@acme/a", "1.0.0", true)],
                request_key: None
            },
            7
        )
        .is_err());
    refused(f.store.catalog_activate_generation("g", &id, 7), "gone");
}

#[test]
fn shared_snapshot_transfers_pins_without_local_plans_or_generations() {
    let a = Fixture::new("catalog-snapshot-a");
    let b = Fixture::new("catalog-snapshot-b");
    let id = agent(&a.store);
    write(&a.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    generation(&a.store, "g", &id);
    let agents = a.store.agent_snapshot().unwrap();
    let image = a
        .store
        .read(|conn| {
            Ok(crate::shared_entry::SharedState::capture(conn)?.snapshot(
                agents
                    .agents
                    .iter()
                    .map(|row| {
                        crate::agent::AgentChangeEntry::new(
                            "agent.import",
                            row.clone(),
                            agents
                                .claims
                                .iter()
                                .filter(|c| c.agent_id == row.agent_id)
                                .cloned()
                                .collect(),
                        )
                    })
                    .collect(),
            ))
        })
        .unwrap();
    let entry = RemoteEntry {
        position: 1,
        entry_id: "snapshot".into(),
        author: "aa".repeat(16),
        signer: "test".into(),
        key_id: "key".into(),
        envelope_version: ENVELOPE_VERSION,
        kind: "snapshot".into(),
        entry: serde_json::to_vec(&image).unwrap(),
    };
    b.store.apply_remote_entry(&entry, None).unwrap();
    assert_eq!(
        a.store.catalog_pins(&id).unwrap(),
        b.store.catalog_pins(&id).unwrap()
    );
    assert!(b.store.catalog_plan("g", &id).is_err());
    assert!(b.store.catalog_generation("g", &id).is_err());
    assert!(b.store.verify().unwrap().replay.ok);
    b.store.rebuild().unwrap();
    assert!(b.store.verify().unwrap().replay.ok);
}

#[test]
fn ready_plan_refuses_a_changed_base_and_transition_state_is_closed() {
    let f = Fixture::new("catalog-ready-base");
    let id = agent(&f.store);
    plan(&f.store, "p", &id);
    write(&f.store, &id, vec![pin("@acme/a", "1.0.0", true)]);
    refused(
        f.store
            .catalog_plan_ready("p", &id, vec![pin("@acme/a", "1.0.0", true)], 4),
        "plan_stale",
    );
    ready(&f.store, "ready", &id);
    refused(
        f.store
            .catalog_plan_failed("ready", &id, json!({"code":"late"})),
        "invalid_transition",
    );
}

#[test]
fn plan_request_shape_and_approval_state_are_validated() {
    let f = Fixture::new("catalog-request-shape");
    let id = agent(&f.store);
    refused(
        f.store.catalog_create_plan(
            "",
            &id,
            CatalogTarget::Agent(id.clone()),
            "install",
            spec(),
            1,
        ),
        "invalid_request",
    );
    for invalid in [
        CatalogSpec {
            package: "bad".into(),
            ..spec()
        },
        CatalogSpec {
            range: "???".into(),
            ..spec()
        },
    ] {
        refused(
            f.store.catalog_create_plan(
                "bad",
                &id,
                CatalogTarget::Agent(id.clone()),
                "install",
                invalid,
                1,
            ),
            "invalid_request",
        );
    }
    ready(&f.store, "p", &id);
    refused(
        f.store.catalog_approve_plan("p", &id, "", 5),
        "request_key_conflict",
    );
    f.store
        .db
        .with_conn_fenced(|tx| {
            tx.execute(
                "UPDATE catalog_plan SET state='pending' WHERE plan_id='p'",
                [],
            )
        })
        .unwrap();
    refused(
        f.store.catalog_approve_plan("p", &id, "g", 5),
        "invalid_transition",
    );
}

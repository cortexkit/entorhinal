use super::*;
use crate::{
    fake_cingulate::{Effect, FakeCingulate},
    tool_provider::tests::{error_code, result, Fixture},
};
use entorhinal_core::{agent::consent::ConsentWrite, AssignWorkspaceRequest};
use std::sync::Arc;

fn clock() -> i64 {
    100
}
fn fixture(role: usize) -> (Fixture, FakeCingulate) {
    let mut f = Fixture::new();
    let fake = FakeCingulate::new();
    fake.set_now(100);
    f.handler.clock = clock;
    f.handler.consent_connector = Arc::new(fake.clone());
    f.admit(Some(&f.stamp(role)));
    (f, fake)
}
fn store(f: &Fixture) -> RegistryStore {
    f.handler.with_store(|s| Ok(s.clone())).unwrap()
}
fn hire(key: &str, name: &str) -> Value {
    json!({"request_key":key,"name":name,"tag":"worker","labels":["build"]})
}
fn sql(f: &Fixture, query: &str) {
    rusqlite::Connection::open(f.root.join("store.db"))
        .unwrap()
        .execute_batch(query)
        .unwrap();
}
fn request(f: &Fixture, tool: &str, key: &str, role: usize) -> ConsentRequest {
    store(f)
        .consent_request(&RequestIdentity {
            agent_id: f.agents[role].clone(),
            tool: tool.into(),
            request_key: key.into(),
        })
        .unwrap()
        .unwrap()
}
fn approved_commit(
    f: &Fixture,
    plan: &ConsentWrite,
    key: &str,
    role: usize,
    auth: Authorization,
) -> ConsentCommit {
    ConsentCommit {
        request: ConsentRequest {
            identity: RequestIdentity {
                agent_id: f.agents[role].clone(),
                tool: plan.tool.clone(),
                request_key: key.into(),
            },
            args_digest: key.into(),
            scope_ref: f.stamp(role).scope_ref,
            scope_epoch: 7,
            created_at_ms: 100,
            elicitation_id: None,
            settlement: None,
            state: RequestState::Awaiting,
            outcome: None,
        },
        authorization: auth,
        action: plan.tool.clone(),
        target: json!(plan),
        granted: None,
        now_ms: 100,
    }
}

#[tokio::test]
async fn hire_card_grant_cap_and_own_retirement_acceptance() {
    let (f, fake) = fixture(2);
    fake.script("consent.await", Effect::Answer("approve_always_project"));
    let first = result(f.call("agents_hire", hire("first", "Worker")).await);
    let id = first["agent"]["agent_id"].as_str().unwrap();
    let row = store(&f).agent_row(id).unwrap().unwrap();
    assert_eq!(
        row.supervisor_agent_id.as_deref(),
        Some(f.agents[2].as_str())
    );
    assert_eq!(row.tag, "worker");
    assert_eq!(row.labels, ["build"]);
    assert_eq!(fake.card_count(), 1);
    assert_eq!(fake.routes().0[0], f.stamp(2));
    let grant = store(&f).consent_grants().unwrap().pop().unwrap();
    let second = result(f.call("agents_hire", hire("second", "Other")).await);
    assert_ne!(second["agent"]["agent_id"], first["agent"]["agent_id"]);
    assert_eq!(fake.card_count(), 1, "standing grant avoids a second card");
    let audit = store(&f).agent_audit_page(None, None, None).unwrap().rows;
    assert_eq!(audit.len(), 2);
    assert_eq!(
        audit[0].authorization,
        Authorization::Grant { id: grant.id }
    );
    assert!(
        matches!(audit[1].authorization, Authorization::Card { ref answer, .. } if answer == "approve_always_project")
    );
    store(&f).set_project_hire_cap("P", 2).unwrap();
    let before = f.counts();
    let refused = f.call("agents_hire", hire("cap", "Blocked")).await;
    assert_eq!(error_code(refused), "hire_cap_reached");
    assert_eq!(f.counts(), before);
    assert_eq!(fake.card_count(), 1);
    let listed = result(f.call("agents_list_hires", json!({})).await);
    assert_eq!(listed["agents"].as_array().unwrap().len(), 2);
    let _retired = result(
        f.call(
            "agents_retire_hire",
            json!({"agent_id":id,"request_key":"retire"}),
        )
        .await,
    );
    assert_eq!(store(&f).agent_row(id).unwrap().unwrap().status, "retired");
    assert_eq!(fake.card_count(), 1);
    let audit = store(&f).agent_audit_page(None, None, None).unwrap().rows;
    assert_eq!(audit[0].authorization, Authorization::OwnHire);
    let changes = store(&f).agent_changes(0, None).unwrap().entries;
    assert_eq!(
        changes.last().unwrap().authorization,
        Some(Authorization::OwnHire)
    );
    assert!(store(&f).verify().unwrap().ok);
    store(&f).rebuild().unwrap();
    assert!(store(&f).verify().unwrap().ok);
}

#[tokio::test]
async fn janitor_workspace_register_and_head_each_apply_only_the_named_write() {
    let (f, fake) = fixture(0);
    for _ in 0..3 {
        fake.script("consent.await", Effect::Answer("approve_once"));
    }
    let workspace = result(
        f.call(
            "projects_create_workspace",
            json!({"workspace_id":"New","name":"New workspace","request_key":"workspace"}),
        )
        .await,
    );
    assert_eq!(workspace["workspaceId"], "New");
    std::fs::create_dir_all(f.root.join("new-project")).unwrap();
    let project = result(f.call("projects_register", json!({"workspace_id":"New","name":"New project","dir":f.root.join("new-project"),"request_key":"register"})).await);
    let project_id = project["projectId"].as_str().unwrap();
    let head = result(
        f.call(
            "agents_create_head",
            json!({"project_id":project_id,"name":"New head","tag":"head","request_key":"head"}),
        )
        .await,
    );
    let head = store(&f)
        .agent_row(head["agent"]["agent_id"].as_str().unwrap())
        .unwrap()
        .unwrap();
    assert_eq!(head.project_id.as_deref(), Some(project_id));
    assert_eq!(fake.card_count(), 3);
    assert_eq!(
        store(&f)
            .agent_audit_page(None, None, None)
            .unwrap()
            .rows
            .len(),
        3
    );
    assert!(store(&f).verify().unwrap().ok);
    store(&f).rebuild().unwrap();
    assert!(store(&f).verify().unwrap().ok);
    assert_eq!(
        error_code(
            f.call(
                "projects_create_workspace",
                json!({"workspace_id":"New","name":"Changed","request_key":"duplicate"})
            )
            .await
        ),
        "workspace_exists"
    );
    assert_eq!(fake.card_count(), 3);
}

#[tokio::test]
async fn validation_refusals_raise_no_cards_or_request_rows() {
    let (f, fake) = fixture(0);
    std::fs::create_dir_all(f.root.join("new")).unwrap();
    let before = f.counts();
    for (args, code) in [
        (
            json!({"workspace_id":"Absent","name":"New","dir":f.root.join("new"),"request_key":"absent"}),
            "unknown_workspace",
        ),
        (
            json!({"workspace_id":"W","name":"Project","dir":f.root.join("new"),"request_key":"taken-name"}),
            "project_exists",
        ),
    ] {
        assert_eq!(error_code(f.call("projects_register", args).await), code);
    }
    std::fs::create_dir_all(f.root.join("owned")).unwrap();
    store(&f)
        .with_principal("test")
        .register_at(
            entorhinal_core::RegisterRequest {
                name: "Existing".into(),
                roots: vec![f.root.join("owned").to_string_lossy().into()],
                workspace_id: Some("W".into()),
                ..Default::default()
            },
            100,
        )
        .unwrap();
    let after_register = f.counts();
    assert_eq!(error_code(f.call("projects_register", json!({"workspace_id":"W","name":"Renamed","dir":f.root.join("owned"),"request_key":"taken-dir"})).await), "project_exists");
    assert_eq!(f.counts(), after_register);
    assert_eq!(
        error_code(
            f.call(
                "agents_create_head",
                json!({"project_id":"P","name":"Duplicate","tag":"head","request_key":"head"})
            )
            .await
        ),
        "agent_project_taken"
    );
    assert_eq!(fake.card_count(), 0);
    assert_eq!(&f.counts()[2..], &before[2..]);
    f.admit(Some(&f.stamp(2)));
    assert_eq!(
        error_code(
            f.call(
                "agents_retire_hire",
                json!({"agent_id":f.agents[3],"request_key":"foreign"})
            )
            .await
        ),
        "not_your_hire"
    );
    store(&f).set_project_hire_cap("P", 0).unwrap();
    assert_eq!(
        error_code(f.call("agents_hire", hire("zero", "Worker")).await),
        "hire_cap_reached"
    );
    assert_eq!(fake.card_count(), 0);
}

#[tokio::test]
async fn deny_expiry_and_pending_retry_are_terminal_and_apply_once() {
    let (f, fake) = fixture(2);
    fake.script("consent.await", Effect::Answer("deny"));
    assert_eq!(
        error_code(f.call("agents_hire", hire("deny", "Denied")).await),
        "consent_declined"
    );
    assert_eq!(
        request(&f, "agents_hire", "deny", 2).state,
        RequestState::Refused
    );
    assert_eq!(
        error_code(f.call("agents_hire", hire("pending", "Worker")).await),
        "consent_pending"
    );
    let row = request(&f, "agents_hire", "pending", 2);
    fake.answer(row.elicitation_id.as_deref().unwrap(), "approve_once");
    let first = result(f.call("agents_hire", hire("pending", "Worker")).await);
    assert_eq!(
        result(f.call("agents_hire", hire("pending", "Worker")).await),
        first
    );
    assert_eq!(store(&f).project_hire_cap("P").unwrap().live_hires, 2);
    assert_eq!(
        error_code(f.call("agents_hire", hire("expiry", "Expired")).await),
        "consent_pending"
    );
    fake.set_now(700_100);
    assert_eq!(
        error_code(f.call("agents_hire", hire("expiry", "Expired")).await),
        "consent_expired"
    );
    let audit = store(&f).agent_audit_page(None, None, None).unwrap().rows;
    assert_eq!(audit.len(), 3);
    assert_eq!(audit[0].outcome["error"]["code"], "consent_expired");
}

#[tokio::test]
async fn request_conflict_and_crash_after_commit_replay_do_not_remint_revoked_grant() {
    let (mut f, fake) = fixture(2);
    fake.script("consent.await", Effect::Answer("approve_always_project"));
    let original = result(f.call("agents_hire", hire("key", "Worker")).await);
    let grant = store(&f).consent_grants().unwrap()[0].id.clone();
    store(&f)
        .consent_transaction(|tx| tx.revoke_grant(&grant, 200))
        .unwrap();
    store(&f).set_project_hire_cap("P", 0).unwrap();
    let before = f.counts();
    let calls = fake.calls().len();
    let descriptor = f.descriptor.clone();
    f.handler.store.lock().unwrap().take();
    *f.handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
    f.handler.consent_coordinator = crate::consent::ConsentCoordinator::default();
    let mut stamp = f.stamp(2);
    stamp.scope_epoch = 99;
    f.admit(Some(&stamp));
    assert_eq!(
        result(f.call("agents_hire", hire("key", "Worker")).await),
        original
    );
    assert_eq!(
        error_code(f.call("agents_hire", hire("key", "Changed")).await),
        "request_key_conflict"
    );
    assert_eq!(f.counts(), before);
    assert_eq!(fake.calls().len(), calls);
    assert_eq!(
        store(&f).consent_grants().unwrap()[0].revoked_at_ms,
        Some(200)
    );
    // A changed body conflicts before any target validation, even while awaiting.
    store(&f).set_project_hire_cap("P", 10).unwrap();
    assert_eq!(
        error_code(f.call("agents_hire", hire("awaiting", "Later")).await),
        "consent_pending"
    );
    assert_eq!(
        error_code(f.call("agents_hire", hire("awaiting", "Other")).await),
        "request_key_conflict"
    );
}

#[tokio::test]
async fn detached_approval_keeps_its_original_placement_after_restart() {
    let (mut f, fake) = fixture(2);
    assert_eq!(
        error_code(f.call("agents_hire", hire("move", "Worker")).await),
        "consent_pending"
    );
    let row = request(&f, "agents_hire", "move", 2);
    fake.answer(row.elicitation_id.as_deref().unwrap(), "approve_once");
    store(&f)
        .with_principal("relay")
        .assign_workspace_at(
            AssignWorkspaceRequest {
                project_id: "P".into(),
                workspace_id: "Other".into(),
                ..Default::default()
            },
            100,
        )
        .unwrap();
    f.handler.store.lock().unwrap().take();
    *f.handler.store.lock().unwrap() = Some(RegistryStore::open(&f.descriptor).unwrap());
    f.handler.consent_coordinator = crate::consent::ConsentCoordinator::default();
    assert_eq!(
        error_code(f.call("agents_hire", hire("move", "Worker")).await),
        "consent_stale"
    );
    assert_eq!(
        request(&f, "agents_hire", "move", 2).state,
        RequestState::Refused
    );
    assert_eq!(store(&f).project_hire_cap("P").unwrap().live_hires, 1);
    assert_eq!(fake.card_count(), 1);
    assert_eq!(
        store(&f).agent_audit_page(None, None, None).unwrap().rows[0].outcome["error"]["code"],
        "consent_stale"
    );
}

#[tokio::test]
async fn commit_rechecks_caller_role_liveness_cap_grant_and_own_hire() {
    for (mutation, expected) in [
        (
            "UPDATE agent SET role='hiree' WHERE role='head'",
            "consent_stale",
        ),
        (
            "UPDATE agent SET terminal_reason='retired',terminal_at_ms=100 WHERE role='head'",
            "gone",
        ),
        (
            "INSERT INTO project_hire_cap(project_id,cap) VALUES('P',0)",
            "hire_cap_reached",
        ),
    ] {
        let (f, _) = fixture(2);
        let plan = store(&f)
            .precheck_consent_write(
                &f.agents[2],
                "agents_hire",
                json!({"name":"Worker","tag":"worker","labels":[]}),
                100,
            )
            .unwrap();
        let commit = approved_commit(
            &f,
            &plan,
            "commit",
            2,
            Authorization::Card {
                id: "card".into(),
                answer: "approve_once".into(),
            },
        );
        sql(&f, mutation);
        let before = f.counts();
        let error = f
            .handler
            .consent_write_wait(
                plan,
                commit,
                Instant::now() + crate::consent_client::CALL_BUDGET,
            )
            .await
            .unwrap_err();
        assert_eq!(error.code, expected);
        assert_eq!(f.counts(), before);
    }
    let (f, _) = fixture(2);
    let plan = store(&f)
        .precheck_consent_write(
            &f.agents[2],
            "agents_hire",
            json!({"name":"Worker","tag":"worker","labels":[]}),
            100,
        )
        .unwrap();
    let grant = StandingGrant {
        id: "standing".into(),
        holder_agent_id: f.agents[2].clone(),
        class: plan.grant_class.clone().unwrap(),
        scope_id: "P".into(),
        granted_at_ms: 100,
        card_ref: "card".into(),
        revoked_at_ms: None,
    };
    store(&f)
        .consent_transaction(|tx| tx.put_grant(&grant))
        .unwrap();
    let commit = approved_commit(
        &f,
        &plan,
        "grant",
        2,
        Authorization::Grant {
            id: grant.id.clone(),
        },
    );
    store(&f)
        .consent_transaction(|tx| tx.revoke_grant(&grant.id, 200))
        .unwrap();
    assert_eq!(
        f.handler
            .consent_write_wait(
                plan,
                commit,
                Instant::now() + crate::consent_client::CALL_BUDGET
            )
            .await
            .unwrap_err()
            .code,
        "consent_stale"
    );
    sql(
        &f,
        &format!(
            "UPDATE agent SET supervisor_agent_id='{}' WHERE agent_id='{}'",
            f.agents[2], f.agents[3]
        ),
    );
    let plan = store(&f)
        .precheck_consent_write(
            &f.agents[2],
            "agents_retire_hire",
            json!({"agent_id":f.agents[3]}),
            100,
        )
        .unwrap();
    let commit = approved_commit(&f, &plan, "own", 2, Authorization::OwnHire);
    sql(
        &f,
        &format!(
            "UPDATE agent SET supervisor_agent_id=NULL WHERE agent_id='{}'",
            f.agents[3]
        ),
    );
    assert_eq!(
        f.handler
            .consent_write_wait(
                plan,
                commit,
                Instant::now() + crate::consent_client::CALL_BUDGET
            )
            .await
            .unwrap_err()
            .code,
        "not_your_hire"
    );
    assert_eq!(
        store(&f).agent_row(&f.agents[3]).unwrap().unwrap().status,
        "live"
    );
}

#[tokio::test]
async fn audit_failure_rolls_back_mutation_journal_grant_and_terminal_then_retry_applies() {
    let (f, fake) = fixture(2);
    let before = f.counts();
    sql(&f,"CREATE TRIGGER fail_audit BEFORE INSERT ON agent_audit BEGIN SELECT RAISE(ABORT,'audit failure'); END;");
    fake.script("consent.await", Effect::Answer("approve_always_project"));
    assert_eq!(
        error_code(f.call("agents_hire", hire("atomic", "Worker")).await),
        "storage_error"
    );
    let after = f.counts();
    assert_eq!(after[0], before[0]);
    assert_eq!(after[1], before[1]);
    assert_eq!(after[3..], before[3..]);
    assert_eq!(
        request(&f, "agents_hire", "atomic", 2).state,
        RequestState::Awaiting
    );
    sql(&f, "DROP TRIGGER fail_audit;");
    let first = result(f.call("agents_hire", hire("atomic", "Worker")).await);
    assert_eq!(
        result(f.call("agents_hire", hire("atomic", "Worker")).await),
        first
    );
    assert_eq!(f.counts()[3..], [1, 1]);
    assert_eq!(fake.card_count(), 1);
}

#[tokio::test]
async fn same_key_concurrency_uses_one_card_and_a_single_terminal_commit() {
    let (f, fake) = fixture(2);
    fake.script("consent.await", Effect::Hang);
    let mut first = Box::pin(f.call("agents_hire", hire("concurrent", "Worker")));
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    assert!(matches!(
        first.as_mut().poll(&mut Context::from_waker(Waker::noop())),
        Poll::Pending
    ));
    assert_eq!(
        error_code(f.call("agents_hire", hire("concurrent", "Worker")).await),
        "consent_pending"
    );
    assert_eq!(fake.card_count(), 1);
    let row = request(&f, "agents_hire", "concurrent", 2);
    fake.answer(row.elicitation_id.as_deref().unwrap(), "approve_once");
    drop(first);
    // Cancelling a held call releases its volatile lease, not its durable request.
    // Use a new handler to model process loss without awaiting the hung route.
    let mut restarted = ProjectsHandler::with_runtime("restart".into(), clock);
    *restarted.store.lock().unwrap() = Some(store(&f));
    restarted.consent_connector = Arc::new(fake.clone());
    restarted
        .route_admissions()
        .insert((80, 2), f.handler.admission_for((80, 2)));
    let body = json!({"name":"agents_hire","arguments":hire("concurrent","Worker")});
    let reply = restarted
        .handle_served_request(&serde_json::to_vec(&body).unwrap(), (80, 2))
        .await;
    result(reply);
    assert_eq!(f.counts()[3], 1);
    assert_eq!(fake.card_count(), 1);
}

#[tokio::test]
async fn workspace_writer_refuses_duplicates_and_round_trips_journal() {
    let (f, _) = fixture(0);
    let create = || entorhinal_core::CreateWorkspaceRequest {
        workspace_id: "  New  ".into(),
        name: "  Workspace  ".into(),
    };
    let first: Value = serde_json::from_slice(
        &store(&f)
            .with_principal("agent")
            .create_workspace_at(create(), 100)
            .unwrap(),
    )
    .unwrap();
    assert_eq!(first["result"]["workspaceId"], "New");
    assert_eq!(first["result"]["name"], "Workspace");
    let before = f.counts();
    let error = store(&f)
        .with_principal("agent")
        .create_workspace_at(create(), 100)
        .unwrap_err();
    assert!(
        matches!(error, entorhinal_core::RegistryError::Domain { ref code, .. } if code == "workspace_exists")
    );
    assert_eq!(f.counts(), before);
    assert!(store(&f).verify().unwrap().ok);
    store(&f).rebuild().unwrap();
    assert!(store(&f).verify().unwrap().ok);
    for (id, name, code) in [
        (" ", "Name", "invalid_workspace_id"),
        (&"a".repeat(513), "Name", "invalid_workspace_id"),
        ("Other", " ", "invalid_name"),
    ] {
        let error = store(&f)
            .with_principal("agent")
            .create_workspace_at(
                entorhinal_core::CreateWorkspaceRequest {
                    workspace_id: id.into(),
                    name: name.into(),
                },
                100,
            )
            .unwrap_err();
        assert!(
            matches!(error, entorhinal_core::RegistryError::Domain { code:ref got, .. } if got == code)
        );
    }
}

#[tokio::test]
async fn request_namespaces_and_grant_classes_do_not_cross_authorize() {
    let (f, fake) = fixture(0);
    fake.script("consent.await", Effect::Answer("approve_always_workspace"));
    std::fs::create_dir_all(f.root.join("registered")).unwrap();
    let project = result(f.call("projects_register",json!({"workspace_id":"W","name":"Registered","dir":f.root.join("registered"),"request_key":"same"})).await);
    assert_eq!(error_code(f.call("agents_create_head",json!({"project_id":project["projectId"],"name":"Fresh head","tag":"head","request_key":"same"})).await),"consent_pending");
    assert_eq!(
        fake.card_count(),
        2,
        "register_project permission must not authorize create_head"
    );
    f.admit(Some(&f.stamp(2)));
    assert_eq!(
        error_code(f.call("agents_hire", hire("same", "Worker")).await),
        "consent_pending"
    );
    assert_eq!(
        fake.card_count(),
        3,
        "another caller and tool own an independent key"
    );
    assert_eq!(
        error_code(
            f.call(
                "agents_hire",
                json!({"request_key":"same","name":42,"tag":"worker"})
            )
            .await
        ),
        "request_key_conflict"
    );
    let grant = store(&f).consent_grants().unwrap()[0].id.clone();
    store(&f)
        .consent_transaction(|tx| tx.revoke_grant(&grant, 200))
        .unwrap();
    f.admit(Some(&f.stamp(0)));
    std::fs::create_dir_all(f.root.join("next")).unwrap();
    assert_eq!(error_code(f.call("projects_register",json!({"workspace_id":"W","name":"Next","dir":f.root.join("next"),"request_key":"next"})).await),"consent_pending");
    assert_eq!(fake.card_count(), 4, "revocation requires a new card");
}

#[tokio::test]
async fn default_cap_refuses_the_eleventh_live_hire_even_with_a_grant() {
    let (f, fake) = fixture(2);
    fake.script("consent.await", Effect::Answer("approve_always_project"));
    for index in 0..9 {
        result(
            f.call(
                "agents_hire",
                hire(&format!("key{index}"), &format!("Worker{index}")),
            )
            .await,
        );
    }
    assert_eq!(store(&f).project_hire_cap("P").unwrap().live_hires, 10);
    let before = f.counts();
    let outcome = f.call("agents_hire", hire("eleventh", "Overflow")).await;
    match outcome {
        crate::HandlerOutcome::ErrorWithDetail { code, detail, .. } => {
            assert_eq!(code, "hire_cap_reached");
            assert_eq!(detail, json!({"cap":10,"current_count":10}));
        }
        _ => panic!("expected cap refusal with counts"),
    }
    assert_eq!(f.counts(), before);
    assert_eq!(fake.card_count(), 1);
}

#[tokio::test]
async fn ambiguous_raise_and_epoch_loss_use_the_saved_request_and_plan() {
    let (f, fake) = fixture(2);
    fake.script("consent.request", Effect::LoseReply);
    assert_eq!(
        error_code(f.call("agents_hire", hire("ambiguous", "Worker")).await),
        "consent_pending"
    );
    assert!(request(&f, "agents_hire", "ambiguous", 2)
        .elicitation_id
        .is_none());
    fake.answer("el_1", "approve_once");
    result(f.call("agents_hire", hire("ambiguous", "Worker")).await);
    assert_eq!(fake.card_count(), 1);
    assert_eq!(
        error_code(f.call("agents_hire", hire("epoch", "Later")).await),
        "consent_pending"
    );
    let mut stamp = f.stamp(2);
    stamp.scope_epoch += 1;
    f.admit(Some(&stamp));
    assert_eq!(
        error_code(f.call("agents_hire", hire("epoch", "Later")).await),
        "consent_lost"
    );
    assert_eq!(
        request(&f, "agents_hire", "epoch", 2).state,
        RequestState::Refused
    );
    assert_eq!(fake.card_count(), 2);
    let calls = fake.calls().len();
    assert_eq!(
        error_code(f.call("agents_hire", hire("epoch", "Later")).await),
        "consent_lost"
    );
    assert_eq!(fake.calls().len(), calls);
}

#[tokio::test]
async fn assistant_role_and_grant_scope_are_rechecked_inside_commit() {
    let (f, _) = fixture(0);
    let plan = store(&f)
        .precheck_consent_write(
            &f.agents[0],
            "projects_create_workspace",
            json!({"workspace_id":"New","name":"New workspace"}),
            100,
        )
        .unwrap();
    let commit = approved_commit(
        &f,
        &plan,
        "role",
        0,
        Authorization::Card {
            id: "card".into(),
            answer: "approve_once".into(),
        },
    );
    sql(
        &f,
        &format!(
            "UPDATE agent SET role='hiree',project_id='P' WHERE agent_id='{}'",
            f.agents[0]
        ),
    );
    assert_eq!(
        f.handler
            .consent_write_wait(
                plan,
                commit,
                Instant::now() + crate::consent_client::CALL_BUDGET
            )
            .await
            .unwrap_err()
            .code,
        "consent_stale"
    );
    let (f, _) = fixture(2);
    let plan = store(&f)
        .precheck_consent_write(
            &f.agents[2],
            "agents_hire",
            json!({"name":"Worker","tag":"worker","labels":[]}),
            100,
        )
        .unwrap();
    let grant = StandingGrant {
        id: "wrong-class".into(),
        holder_agent_id: f.agents[2].clone(),
        class: entorhinal_core::agent::consent::GrantClass::RegisterProject,
        scope_id: "W".into(),
        granted_at_ms: 100,
        card_ref: "card".into(),
        revoked_at_ms: None,
    };
    store(&f)
        .consent_transaction(|tx| tx.put_grant(&grant))
        .unwrap();
    let commit = approved_commit(&f, &plan, "scope", 2, Authorization::Grant { id: grant.id });
    assert_eq!(
        f.handler
            .consent_write_wait(
                plan,
                commit,
                Instant::now() + crate::consent_client::CALL_BUDGET
            )
            .await
            .unwrap_err()
            .code,
        "consent_stale"
    );
}

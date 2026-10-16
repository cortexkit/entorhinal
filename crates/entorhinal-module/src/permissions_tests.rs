use super::*;
use crate::{
    agent_ops::OperatorConfirmer,
    tests::{reserved, scratch_descriptor},
    RouteAdmission,
};
use async_trait::async_trait;
use entorhinal_core::{
    agent::consent::{AuditRow, Authorization},
    RegisterRequest,
};
use std::{
    future::Future,
    path::PathBuf,
    sync::{Arc, Mutex},
    task::{Context, Poll, Waker},
};
use subc_client_rs::HandlerOutcome;

const ROUTE: RouteKey = (170, 1);

struct Prompt {
    answer: Result<(), OperatorConfirmError>,
    calls: Mutex<Vec<String>>,
}
#[async_trait]
impl OperatorConfirmer for Prompt {
    async fn confirm_operator(
        &self,
        summary: &str,
        _: RouteKey,
    ) -> Result<(), OperatorConfirmError> {
        self.calls.lock().unwrap().push(summary.into());
        self.answer
    }
}

struct Fixture {
    root: PathBuf,
    handler: ProjectsHandler,
    prompt: Arc<Prompt>,
    agents: Vec<String>,
}
impl Fixture {
    fn new(answer: Result<(), OperatorConfirmError>) -> Self {
        let (root, descriptor) = scratch_descriptor("permissions");
        let store = RegistryStore::open(&descriptor).unwrap();
        store
            .apply_entry("agent.cutover", "{}", "test", None, |_| Ok(()))
            .unwrap();
        store
            .register(RegisterRequest {
                project_id: Some("P".into()),
                name: "Project".into(),
                workspace_id: Some("W".into()),
                ..Default::default()
            })
            .unwrap();
        let agents = ["assistant", "head"].into_iter().map(|role| {
            let body = store.agent_mutation("agent.create", json!({
                "name":role,"role":role,"tag":"test","project_id":if role == "head" { Some("P") } else { None },
                "request_key":role,
            }), 100).unwrap();
            serde_json::from_slice::<Value>(&body).unwrap()["result"]["agent"]["agent_id"].as_str().unwrap().into()
        }).collect();
        let prompt = Arc::new(Prompt {
            answer,
            calls: Mutex::new(vec![]),
        });
        let mut handler = ProjectsHandler::with_runtime("permissions-test".into(), || 100);
        handler.operator_confirmer = prompt.clone();
        *handler.store.lock().unwrap() = Some(store);
        let f = Self {
            root,
            handler,
            prompt,
            agents,
        };
        f.bind(Some(Principal::Direct));
        f
    }
    fn bind(&self, principal: Option<Principal>) {
        self.handler
            .route_admissions()
            .insert(ROUTE, RouteAdmission::from_bind(principal, None));
    }
    fn store(&self) -> RegistryStore {
        self.handler.with_store(|s| Ok(s.clone())).unwrap()
    }
    fn grant(&self, id: &str, holder: usize, class: GrantClass) -> String {
        self.store()
            .consent_transaction(|tx| {
                tx.put_grant(&StandingGrant {
                    id: id.into(),
                    holder_agent_id: self.agents[holder].clone(),
                    scope_id: if class == GrantClass::Hire { "P" } else { "W" }.into(),
                    class,
                    granted_at_ms: 1000000,
                    card_ref: "el_3".into(),
                    revoked_at_ms: None,
                })
            })
            .unwrap()
    }
    async fn call(&self, method: &str, params: Value) -> HandlerOutcome {
        self.handler
            .handle_served_request(
                &serde_json::to_vec(&json!({"method":method,"params":params})).unwrap(),
                ROUTE,
            )
            .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.handler.store.lock().unwrap().take();
        std::fs::remove_dir_all(&self.root).unwrap();
    }
}
fn result(outcome: HandlerOutcome) -> Value {
    match outcome {
        HandlerOutcome::Response(body) => {
            serde_json::from_slice::<Value>(&body).unwrap()["result"].clone()
        }
        other => panic!("expected result, got {other:?}"),
    }
}
fn error(outcome: HandlerOutcome) -> Value {
    match outcome {
        HandlerOutcome::Error { code, message }
        | HandlerOutcome::ErrorWithDetail { code, message, .. } => {
            json!({"code":code,"message":message})
        }
        other => panic!("expected error, got {other:?}"),
    }
}
fn vector(name: &str) -> Value {
    let text = match name {
        "list" => include_str!("../tests/fixtures/permissions-v1/provider-list.json"),
        "upgrade" => {
            include_str!("../tests/fixtures/permissions-v1/provider-list-after-upgrade.json")
        }
        "revoked" => include_str!("../tests/fixtures/permissions-v1/provider-revoke-revoked.json"),
        "not_found" => {
            include_str!("../tests/fixtures/permissions-v1/provider-revoke-not-found.json")
        }
        "refused" => {
            include_str!("../tests/fixtures/permissions-v1/provider-revoke-refused-agent.json")
        }
        _ => panic!("unknown vector"),
    };
    serde_json::from_str(text).unwrap()
}

#[tokio::test]
async fn provider_vectors_list_scopes_filter_and_refresh_in_place() {
    let f = Fixture::new(Ok(()));
    f.bind(Some(reserved("cingulate")));
    f.grant("grt_01", 0, GrantClass::RegisterProject);
    f.grant("grt_02", 0, GrantClass::CreateHead);
    f.grant("grt_03", 1, GrantClass::Hire);
    // The vector describes a site grant from Cerebellum, not project hiring.
    // Substitute id, agent/holder, module, scope and display labels, preserving
    // the full field set, granted_at_ms and card_ref of an always-grant row.
    let template = vector("list")["reply"]["grants"][2].clone();
    let mut expected = Vec::new();
    for (id, holder, scope, subject, sentence) in [
        (
            "grt_01",
            0,
            "workspace_projects",
            "W",
            "register projects in workspace \"W\"",
        ),
        (
            "grt_02",
            0,
            "workspace_heads",
            "W",
            "create project heads in workspace \"W\"",
        ),
        (
            "grt_03",
            1,
            "project_hires",
            "Project",
            "hire agents in project \"Project\"",
        ),
    ] {
        let mut row = template.clone();
        row["id"] = json!(id);
        row["agent_id"] = json!(f.agents[holder]);
        row["holder"]["agent_id"] = json!(f.agents[holder]);
        row["module"] = json!("entorhinal");
        row["scope"] = json!(scope);
        row["subject_label"] = json!(subject);
        row["scope_label"] = json!(sentence);
        expected.push(row);
    }
    assert_eq!(
        result(f.call(LIST, json!({})).await),
        json!({"grants":expected})
    );
    assert_eq!(
        result(f.call(LIST, json!({"agent_id":f.agents[1]})).await),
        json!({"grants":[expected[2]]})
    );
    assert_eq!(
        result(f.call(LIST, json!({"agent_id":"unknown"})).await),
        json!({"grants":[]})
    );
    let upgraded = vector("upgrade")["reply"]["grants"][0].clone();
    let same_id = f
        .store()
        .consent_transaction(|tx| {
            tx.put_grant(&StandingGrant {
                id: "replacement-id".into(),
                holder_agent_id: f.agents[0].clone(),
                class: GrantClass::RegisterProject,
                scope_id: "W".into(),
                granted_at_ms: upgraded["granted_at_ms"].as_i64().unwrap(),
                card_ref: upgraded["card_ref"].as_str().unwrap().into(),
                revoked_at_ms: None,
            })
        })
        .unwrap();
    assert_eq!(same_id, "grt_01");
    expected[0]["granted_at_ms"] = upgraded["granted_at_ms"].clone();
    expected[0]["card_ref"] = upgraded["card_ref"].clone();
    assert_eq!(
        result(f.call(LIST, json!({"agent_id":f.agents[0]})).await),
        json!({"grants":expected[..2]})
    );
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_admission_precedes_decode_and_never_prompts() {
    let f = Fixture::new(Ok(()));
    f.grant("grt_01", 0, GrantClass::RegisterProject);
    let before = f.store().generation().unwrap();
    for principal in [
        None,
        Some(reserved("plexus")),
        Some(reserved("callosum")),
        Some(reserved("prefrontal-core")),
        Some(Principal::Unverified),
    ] {
        f.bind(principal);
        for method in [LIST, REVOKE] {
            assert_eq!(
                error(f.call(method, json!({"invalid":true})).await),
                vector("refused")["error"]
            );
        }
    }
    // An agent-stamped route cannot revoke even its own grant.
    let stamp = serde_json::from_value(json!({
        "owner":{"kind":"reserved","module_id":"plexus"}, "ref":"head:alf", "scope_epoch":1,
        "kind":"head", "attributes":{"agent_id":f.agents[0]}, "owner_authorized":true,
    }))
    .unwrap();
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(Some(reserved("plexus")), Some(&stamp)),
    );
    assert_eq!(
        error(f.call(REVOKE, vector("refused")["request"].clone()).await),
        vector("refused")["error"]
    );
    assert_eq!(f.store().generation().unwrap(), before);
    assert!(f
        .store()
        .covering_consent_grant(&f.agents[0], GrantClass::RegisterProject, "W")
        .unwrap()
        .is_some());
    for principal in [Principal::Direct, reserved("cingulate")] {
        f.bind(Some(principal.clone()));
        assert_eq!(
            result(f.call(LIST, json!({})).await)["grants"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
    }
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_revoke_vectors_are_idempotent_local_and_prompt_free() {
    let f = Fixture::new(Err(OperatorConfirmError::Declined));
    f.grant("grt_03", 1, GrantClass::Hire);
    let before = f.store().agent_changes(0, None).unwrap();
    let journal = f.store().generation().unwrap();
    for principal in [Principal::Direct, reserved("cingulate")] {
        f.bind(Some(principal.clone()));
        {
            // Permission reads remain independent of the shared writer.
            let _writer = f.handler.writer.lock().await;
            let read = f.call(LIST, json!({}));
            let mut read = std::pin::pin!(read);
            assert!(matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(_)
            ));
        }
        let v = vector("revoked");
        let outcome = f.call(REVOKE, v["request"].clone()).await;
        let wanted = if principal == Principal::Direct {
            v["reply"].clone()
        } else {
            vector("not_found")["reply"].clone()
        };
        assert_eq!(result(outcome), wanted);
    }
    assert_eq!(
        result(f.call(REVOKE, vector("not_found")["request"].clone()).await),
        vector("not_found")["reply"]
    );
    assert_eq!(result(f.call(LIST, json!({})).await), json!({"grants":[]}));
    assert_eq!(
        f.store().agent_changes(0, None).unwrap().entries.len(),
        before.entries.len()
    );
    assert_eq!(f.store().generation().unwrap(), journal);
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}

#[tokio::test(start_paused = true)]
async fn revoke_during_shared_sql_answers_within_four_seconds_without_stalling_reads() {
    use subc_client_rs::ModuleHandler;
    async fn finish<F: Future>(future: F) -> F::Output {
        tokio::pin!(future);
        loop {
            tokio::select! {
                result = &mut future => return result,
                () = tokio::task::yield_now() => {},
            }
        }
    }
    assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
    let f = Fixture::new(Err(OperatorConfirmError::Declined));
    f.grant("grt_03", 1, GrantClass::Hire);
    let store = f.store();
    let writer = f.handler.writer.clone().lock_owned().await;
    let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();
    let sql = tokio::task::spawn_blocking(move || {
        let _writer = writer;
        store
            .apply_entry("test.held", "{}", "test", None, |_| {
                let _ = ready_tx.send(());
                release_rx.recv().unwrap();
                Ok(())
            })
            .unwrap();
    });
    while ready_rx.try_recv().is_err() {
        tokio::task::yield_now().await;
    }
    let request = vector("revoked")["request"].clone();
    let mut revoke = Box::pin(f.call(REVOKE, request.clone()));
    assert!(revoke
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_pending());
    let mut health = Box::pin(f.handler.health());
    assert!(health
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()))
        .is_ready());
    for method in [LIST, "agent.fleet_identity", "enumerate"] {
        let mut read = Box::pin(f.call(method, json!({})));
        assert!(
            matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(HandlerOutcome::Response(_))
            ),
            "{method} waited for SQL"
        );
    }
    tokio::time::advance(std::time::Duration::from_millis(4_001)).await;
    let answer = revoke
        .as_mut()
        .poll(&mut Context::from_waker(Waker::noop()));
    // Release before asserting so an intentionally broken deadline cannot leave
    // a blocking worker alive when the test runtime shuts down.
    release_tx.send(()).unwrap();
    while !sql.is_finished() {
        tokio::task::yield_now().await;
    }
    sql.await.unwrap();
    assert!(
        matches!(answer, Poll::Ready(HandlerOutcome::Error { ref code, ref message }) if code == "unavailable" && message.contains("shared write is in progress")),
        "{answer:?}"
    );
    drop(revoke);
    assert_eq!(
        result(finish(f.call(REVOKE, request.clone())).await)["outcome"],
        "revoked"
    );
    assert_eq!(
        result(finish(f.call(REVOKE, request)).await)["outcome"],
        "not_found"
    );
}

#[tokio::test]
async fn publisher_revoke_is_off_runtime_persisted_and_audits_the_request_principal() {
    use entorhinal_core::catalog_grants::{Operation, PublisherGrant};
    assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
    let f = Fixture::new(Err(OperatorConfirmError::Declined));
    let store = f.store();
    let grant = PublisherGrant {
        id: "pg_revoke".into(),
        publisher_fingerprint: "ab".repeat(32),
        operation: Operation::Install,
        placement_agent_id: f.agents[0].clone(),
        ceiling: vec![],
        card_ref: None,
        granted_at_ms: 99,
        revoked_at_ms: None,
    };
    store
        .insert_publisher_grant(&grant, &json!({"kind":"direct"}))
        .unwrap();
    let principal = reserved("cingulate");
    f.bind(Some(principal.clone()));
    for (id, expected) in [
        ("pg_revoke", "revoked"),
        ("pg_revoke", "not_found"),
        ("pg_unknown", "not_found"),
    ] {
        assert_eq!(
            result(f.call(REVOKE, json!({"id":id})).await)["outcome"],
            expected
        );
    }
    assert_eq!(
        store.publisher_grants().unwrap()[0].revoked_at_ms,
        Some(100)
    );
    let audit = store.catalog_audit().unwrap();
    assert_eq!(audit.len(), 2);
    assert_eq!(audit[1]["detail"]["principal"], json!(principal));
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn provider_drops_grants_when_holder_or_subject_ends() {
    for end in ["holder", "project", "workspace"] {
        let f = Fixture::new(Ok(()));
        f.store()
            .register(RegisterRequest {
                project_id: Some("other".into()),
                name: "Other project".into(),
                workspace_id: Some("other-workspace".into()),
                ..Default::default()
            })
            .unwrap();
        let (id, holder, class) = if end == "workspace" {
            ("workspace-grant", 0, GrantClass::RegisterProject)
        } else {
            ("hire-grant", 1, GrantClass::Hire)
        };
        f.grant(id, holder, class);
        assert_eq!(
            result(f.call(LIST, json!({})).await)["grants"]
                .as_array()
                .unwrap()
                .len(),
            1
        );
        match end {
            "holder" => {
                f.store()
                    .agent_mutation(
                        "agent.dispose",
                        json!({"agent_id":f.agents[1],"request_key":"end"}),
                        200,
                    )
                    .unwrap();
            }
            // Inject an absent subject while deliberately retaining its grant
            // and former dependents. Retained local rows must not grant authority.
            "project" => {
                rusqlite::Connection::open(f.root.join("store.db"))
                    .unwrap()
                    .execute_batch(
                        "PRAGMA foreign_keys=OFF; DELETE FROM project WHERE project_id='P';",
                    )
                    .unwrap();
            }
            _ => {
                rusqlite::Connection::open(f.root.join("store.db"))
                    .unwrap()
                    .execute_batch(
                        "PRAGMA foreign_keys=OFF; DELETE FROM workspace WHERE workspace_id='W';",
                    )
                    .unwrap();
            }
        }
        assert_eq!(result(f.call(LIST, json!({})).await), json!({"grants":[]}));
        assert_eq!(
            result(f.call(REVOKE, json!({"id":id})).await),
            vector("not_found")["reply"]
        );
    }
}

#[tokio::test]
async fn audit_admission_is_operator_or_callosum_only_before_decode() {
    let f = Fixture::new(Ok(()));
    for principal in [
        None,
        Some(Principal::Unverified),
        Some(reserved("cingulate")),
        Some(reserved("prefrontal-core")),
        Some(reserved("plexus")),
    ] {
        f.bind(principal);
        assert_eq!(
            error(f.call(AUDIT, json!({"invalid":true})).await)["code"],
            "audit_operator_required"
        );
    }
    for principal in [Principal::Direct, reserved("callosum")] {
        f.bind(Some(principal));
        assert_eq!(result(f.call(AUDIT, json!({})).await), json!({"rows":[]}));
        for params in [
            json!({"limit":0}),
            json!({"limit":201}),
            json!({"cursor":"bad"}),
            json!({"since_ms":"10"}),
            json!({"unknown":true}),
        ] {
            assert_eq!(
                error(f.call(AUDIT, params).await)["code"],
                "invalid_request"
            );
        }
    }
    assert!(f.prompt.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn audit_wire_pages_tied_timestamps_once_with_inclusive_since() {
    let f = Fixture::new(Ok(()));
    f.store()
        .consent_transaction(|tx| {
            for time_ms in [9, 10, 10, 10, 11] {
                tx.append_audit(&AuditRow {
                    row_id: 0,
                    time_ms,
                    agent_id: f.agents[1].clone(),
                    action: "agents_retire_hire".into(),
                    target: json!({"agent_id":"hire"}),
                    authorization: Authorization::OwnHire,
                    outcome: json!({"result":{"ok":true}}),
                })?;
            }
            Ok(())
        })
        .unwrap();
    for principal in [Principal::Direct, reserved("callosum")] {
        f.bind(Some(principal));
        let mut cursor = None;
        let mut ids = Vec::new();
        let mut times = Vec::new();
        loop {
            let page = result(
                f.call(AUDIT, json!({"since_ms":10,"limit":2,"cursor":cursor}))
                    .await,
            );
            let rows = page["rows"].as_array().unwrap();
            assert_eq!(rows.len(), 2);
            for row in rows {
                ids.push(row["row_id"].as_i64().unwrap());
                times.push(row["time_ms"].as_i64().unwrap());
                assert_eq!(row["authorization"], json!({"kind":"own_hire"}));
            }
            cursor = page["next_cursor"].as_str().map(str::to_owned);
            if cursor.is_none() {
                break;
            }
            assert!(ids.len() <= 4, "cursor repeated rows");
        }
        assert_eq!(ids, [5, 4, 3, 2]);
        assert_eq!(times, [11, 10, 10, 10]);
    }
    f.store()
        .consent_transaction(|tx| {
            for _ in 0..200 {
                tx.append_audit(&AuditRow {
                    row_id: 0,
                    time_ms: 12,
                    agent_id: f.agents[0].clone(),
                    action: "test".into(),
                    target: json!({}),
                    authorization: Authorization::Card {
                        id: "el".into(),
                        answer: "deny".into(),
                    },
                    outcome: json!({"error":{"code":"consent_declined"}}),
                })?;
            }
            Ok(())
        })
        .unwrap();
    let page = result(f.call(AUDIT, json!({})).await);
    assert_eq!(page["rows"].as_array().unwrap().len(), 200);
    assert_eq!(page["rows"][0]["row_id"], 205);
    let next = result(f.call(AUDIT, json!({"cursor":page["next_cursor"]})).await);
    assert_eq!(next["rows"].as_array().unwrap().len(), 5);
    assert!(next.get("next_cursor").is_none());
}

#[tokio::test]
async fn hire_cap_admission_validation_and_confirmation_guard_every_write() {
    let f = Fixture::new(Ok(()));
    for principal in [
        None,
        Some(Principal::Unverified),
        Some(reserved("cingulate")),
        Some(reserved("callosum")),
        Some(reserved("prefrontal-core")),
    ] {
        f.bind(principal);
        assert_eq!(
            error(f.call(SET_CAP, json!({"bad":true})).await)["code"],
            "direct_caller_required"
        );
    }
    f.bind(Some(Principal::Direct));
    for (params, code) in [
        (json!({"project_id":"P","cap":-1}), "invalid_request"),
        (json!({"project_id":"P","cap":1.5}), "invalid_request"),
        (json!({"project_id":"P","cap":"1"}), "invalid_request"),
        (
            json!({"project_id":"P","cap":0,"agent_id":"spoof"}),
            "invalid_request",
        ),
        (json!({"project_id":"missing","cap":0}), "not_found"),
    ] {
        assert_eq!(error(f.call(SET_CAP, params).await)["code"], code);
    }
    assert!(f.prompt.calls.lock().unwrap().is_empty());
    f.handler.route_admissions().insert(
        ROUTE,
        RouteAdmission::from_bind(
            Some(Principal::Direct),
            Some(&crate::tests::flow_stamp(Some("flow"))),
        ),
    );
    assert_eq!(
        error(f.call(SET_CAP, json!({"project_id":"P","cap":0})).await)["code"],
        "flow_scope_not_admitted"
    );
    f.bind(Some(Principal::Direct));
    let before = f.store().generation().unwrap();
    for cap in [0, 20, 0] {
        assert_eq!(
            result(f.call(SET_CAP, json!({"project_id":"P","cap":cap})).await),
            json!({"project_id":"P","cap":cap})
        );
        assert_eq!(f.store().project_hire_cap("P").unwrap().cap, cap);
    }
    assert_eq!(
        f.store().generation().unwrap(),
        before,
        "cap is not shared identity"
    );
    assert_eq!(
        f.prompt.calls.lock().unwrap().as_slice(),
        [
            "set hire cap for project \"Project\" (\"P\") to 0",
            "set hire cap for project \"Project\" (\"P\") to 20",
            "set hire cap for project \"Project\" (\"P\") to 0",
        ]
    );
    let sync = f.handler.handle_request(
        &serde_json::to_vec(&json!({"method":SET_CAP,"params":{"project_id":"P","cap":5}}))
            .unwrap(),
        ROUTE,
    );
    assert_eq!(error(sync)["code"], "served_path_required");
    assert_eq!(f.store().project_hire_cap("P").unwrap().cap, 0);
    for (answer, code) in [
        (OperatorConfirmError::Declined, "operator_declined"),
        (
            OperatorConfirmError::Unsupported,
            "operator_presence_unavailable",
        ),
        (
            OperatorConfirmError::PresenceUnavailable,
            "operator_presence_unavailable",
        ),
    ] {
        let f = Fixture::new(Err(answer));
        assert_eq!(
            error(f.call(SET_CAP, json!({"project_id":"P","cap":0})).await)["code"],
            code
        );
        assert_eq!(f.store().project_hire_cap("P").unwrap().cap, 10);
        assert_eq!(f.prompt.calls.lock().unwrap().len(), 1);
        assert!(f.handler.confirmation_slot.lock().unwrap().is_none());
    }
}

#[tokio::test(start_paused = true)]
async fn hire_cap_sql_runs_off_runtime_and_is_bounded_after_confirmation() {
    use subc_client_rs::ModuleHandler;
    struct Delayed;
    #[async_trait]
    impl OperatorConfirmer for Delayed {
        async fn confirm_operator(&self, _: &str, _: RouteKey) -> Result<(), OperatorConfirmError> {
            tokio::time::sleep(std::time::Duration::from_secs(10)).await;
            Ok(())
        }
    }
    assert_eq!(tokio::runtime::Handle::current().metrics().num_workers(), 1);
    for hold_writer in [true, false] {
        let mut f = Fixture::new(Ok(()));
        f.handler.operator_confirmer = Arc::new(Delayed);
        let store = f.store();
        let writer = if hold_writer {
            Some(f.handler.writer.clone().lock_owned().await)
        } else {
            None
        };
        let (ready_tx, mut ready_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let sql = tokio::task::spawn_blocking(move || {
            let _writer = writer;
            store
                .apply_entry("test.held", "{}", "test", None, |_| {
                    let _ = ready_tx.send(());
                    release_rx.recv().unwrap();
                    Ok(())
                })
                .unwrap();
        });
        while ready_rx.try_recv().is_err() {
            tokio::task::yield_now().await;
        }
        let mut cap = Box::pin(f.call(SET_CAP, json!({"project_id":"P","cap":0})));
        assert!(cap
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        tokio::time::advance(std::time::Duration::from_secs(10)).await;
        assert!(cap
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        let mut health = Box::pin(f.handler.health());
        assert!(health
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_ready());
        for method in [LIST, "agent.fleet_identity", "enumerate"] {
            let mut read = Box::pin(f.call(method, json!({})));
            assert!(matches!(
                read.as_mut().poll(&mut Context::from_waker(Waker::noop())),
                Poll::Ready(HandlerOutcome::Response(_))
            ));
        }
        tokio::time::advance(std::time::Duration::from_millis(4_001)).await;
        let answer = cap.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        // Always release the real SQL mutex before checking the deadline, so
        // a broken bound cannot strand a blocking worker at runtime shutdown.
        release_tx.send(()).unwrap();
        sql.await.unwrap();
        assert!(
            matches!(answer, Poll::Ready(HandlerOutcome::Error { ref code, .. }) if code == "unavailable"),
            "{answer:?}"
        );
        drop(cap);
        drop(health);
        while f.handler.confirmation_slot.lock().unwrap().is_some() {
            tokio::task::yield_now().await;
        }
        assert_eq!(
            f.store().project_hire_cap("P").unwrap().cap,
            if hold_writer { 10 } else { 0 }
        );
        assert!(f.handler.writer.try_lock().is_ok());
    }
}

#[test]
fn manifest_declares_permissions_and_all_local_management_operations() {
    let manifest = crate::manifest();
    assert!(manifest
        .capabilities
        .unwrap()
        .provides
        .contains(&CAPABILITY.to_owned()));
    let operations = manifest
        .provides
        .iter()
        .find_map(|role| match role {
            subc_protocol::manifest::ProviderRole::ManagementSurface { operations, .. } => {
                Some(operations)
            }
            _ => None,
        })
        .unwrap();
    for (name, kind) in [
        (
            LIST,
            subc_protocol::manifest::ManagementOperationKind::Query,
        ),
        (
            REVOKE,
            subc_protocol::manifest::ManagementOperationKind::Mutate,
        ),
        (
            AUDIT,
            subc_protocol::manifest::ManagementOperationKind::Query,
        ),
        (
            SET_CAP,
            subc_protocol::manifest::ManagementOperationKind::Mutate,
        ),
    ] {
        assert_eq!(
            operations.iter().find(|op| op.name == name).unwrap().kind,
            kind
        );
    }
}

#[tokio::test(start_paused = true)]
async fn hire_cap_cancelled_or_expired_confirmation_never_writes() {
    struct Waiting;
    #[async_trait]
    impl OperatorConfirmer for Waiting {
        async fn confirm_operator(&self, _: &str, _: RouteKey) -> Result<(), OperatorConfirmError> {
            std::future::pending().await
        }
    }
    for closed in [true, false] {
        let mut f = Fixture::new(Ok(()));
        f.handler.operator_confirmer = Arc::new(Waiting);
        let request = f.call(SET_CAP, json!({"project_id":"P","cap":0}));
        let mut request = std::pin::pin!(request);
        assert!(request
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending());
        assert_eq!(f.store().project_hire_cap("P").unwrap().cap, 10);
        if closed {
            use subc_client_rs::ModuleHandler;
            f.handler
                .on_route_gone(&subc_client_rs::RouteHandle::detached(ROUTE.0, ROUTE.1))
                .await;
        } else {
            tokio::time::advance(OPERATOR_BOUND + std::time::Duration::from_millis(1)).await;
        }
        let Poll::Ready(outcome) = request
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
        else {
            panic!("confirmation did not cancel or expire at its bound");
        };
        assert_eq!(error(outcome)["code"], "operator_declined");
        assert_eq!(f.store().project_hire_cap("P").unwrap().cap, 10);
        assert!(f.handler.confirmation_slot.lock().unwrap().is_none());
    }
}

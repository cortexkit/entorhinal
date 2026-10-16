use super::*;
use crate::{
    consent_client::{ConsentConnector, CALL_BUDGET},
    fake_cingulate::{decoder, Effect, FakeCingulate},
    tests::scratch_descriptor,
};
use std::path::PathBuf;

struct Fixture {
    root: PathBuf,
    descriptor: cortexkit_store_types::StorageDescriptor,
    store: Option<RegistryStore>,
    fake: FakeCingulate,
    coordinator: ConsentCoordinator,
    stamp: ScopeStamp,
}
impl Fixture {
    fn new() -> Self {
        let (root, descriptor) = scratch_descriptor("cingulate-consent");
        let store = RegistryStore::open(&descriptor).unwrap();
        Self {
            root,
            descriptor,
            store: Some(store),
            fake: FakeCingulate::new(),
            coordinator: ConsentCoordinator::default(),
            stamp: stamp("agent-a", "head:a", 7),
        }
    }
    fn store(&self) -> &RegistryStore {
        self.store.as_ref().unwrap()
    }
    fn reopen(&mut self) {
        drop(self.store.take());
        self.store = Some(RegistryStore::open(&self.descriptor).unwrap());
        self.coordinator = ConsentCoordinator::default();
    }
    fn row(&self, tool: &str, key: &str) -> ConsentRequest {
        row(&self.stamp, tool, key, self.fake.now())
    }
    async fn obtain(&self, row: &ConsentRequest) -> ConsentAttempt {
        self.coordinator
            .obtain(
                ConsentCall {
                    store: self.store(),
                    connector: &self.fake,
                    stamp: &self.stamp,
                    deadline: Instant::now() + CALL_BUDGET,
                    now_ms: &|| self.fake.now(),
                },
                row,
                &context(),
            )
            .await
            .unwrap()
    }
    fn saved(&self, row: &ConsentRequest) -> ConsentRequest {
        self.store()
            .consent_request(&row.identity)
            .unwrap()
            .unwrap()
    }
    fn begin(&self, row: &ConsentRequest) {
        self.store()
            .consent_transaction(|tx| tx.begin_request(row))
            .unwrap();
    }
    async fn route(&self) -> Box<dyn ConsentRoute> {
        self.fake
            .open(&self.stamp, Instant::now() + CALL_BUDGET)
            .await
            .unwrap()
    }
    async fn capture(&self) -> CaptureReport {
        capture_feed(
            self.store(),
            self.route().await.as_ref(),
            &Owner::from_stamp(&self.stamp).unwrap(),
            Instant::now() + CALL_BUDGET,
        )
        .await
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.store.take();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}
fn stamp(agent: &str, scope: &str, epoch: i64) -> ScopeStamp {
    serde_json::from_value(json!({"owner":{"kind":"reserved", "module_id":"prefrontal"}, "ref":scope, "scope_epoch":epoch, "kind":"head", "attributes":{"agent_id":agent}, "owner_authorized":true})).unwrap()
}
fn row(stamp: &ScopeStamp, tool: &str, key: &str, now: i64) -> ConsentRequest {
    ConsentRequest {
        identity: RequestIdentity {
            agent_id: stamp.attributes.agent_id.clone().unwrap(),
            tool: tool.into(),
            request_key: key.into(),
        },
        args_digest: "argument-digest".into(),
        scope_ref: stamp.scope_ref.clone(),
        scope_epoch: stamp.scope_epoch as i64,
        created_at_ms: now,
        elicitation_id: None,
        settlement: None,
        state: RequestState::Awaiting,
        outcome: None,
    }
}
fn context() -> CardContext {
    CardContext {
        caller_label: "ALF".into(),
        target_kind: "project".into(),
        target_label: "Project P".into(),
        target_scope: json!({"project_id":"P", "workspace_id":"W"}),
    }
}
fn id(attempt: ConsentAttempt) -> String {
    match attempt.decision {
        ConsentDecision::Pending(Some(id)) => id,
        other => panic!("expected pending id, got {other:?}"),
    }
}
async fn call(route: &dyn ConsentRoute, method: &str, params: Value) -> Value {
    bounded_call(route, method, params, Instant::now() + CALL_BUDGET)
        .await
        .unwrap()
}

#[test]
fn every_card_matches_frozen_cingulate_decoder_and_external_key_vector() {
    let f = Fixture::new();
    let key = RequestIdentity {
        agent_id: "agent-a".into(),
        tool: "agents_hire".into(),
        request_key: "same key".into(),
    };
    assert_eq!(
        external_key(&key),
        "ent1:b35a91200ba1a49b6470d8a37f7b607ba1a0bb2f967d0b62fe2925c36fa229bd"
    );
    for (tool, choices) in [
        ("projects_create_workspace", vec!["approve_once", "deny"]),
        (
            "projects_register",
            vec!["approve_once", "approve_always_workspace", "deny"],
        ),
        (
            "agents_create_head",
            vec!["approve_once", "approve_always_workspace", "deny"],
        ),
        (
            "agents_hire",
            vec!["approve_once", "approve_always_project", "deny"],
        ),
    ] {
        let row = f.row(tool, "same key");
        let body = render_card(&row, &context()).unwrap();
        decoder::decode(body.clone()).unwrap();
        assert_eq!(
            body["options"]
                .as_array()
                .unwrap()
                .iter()
                .map(|o| o["id"].as_str().unwrap())
                .collect::<Vec<_>>(),
            choices
        );
        assert_eq!(body["default"], "deny");
        assert_eq!(body["on_expiry"], "deny");
        assert_eq!(body["late_execution"], "notify_only");
        assert_eq!(body["urgency"], "high");
        assert_eq!(body["material_damage"], false);
        assert_eq!(body["expires_in_ms"], 600_000);
        assert_eq!(body["binding"], json!(Binding::for_request(&row)));
        assert_eq!(body["dedup_key"], external_key(&row.identity));
        assert_eq!(body["requester_ref"], body["dedup_key"]);
        assert_eq!(
            body["scope"],
            json!({"action":tool, "target":{"project_id":"P", "workspace_id":"W"}})
        );
        assert!(body["title"].as_str().unwrap().contains("ALF"));
        assert!(body["prompt"].as_str().unwrap().contains("Project P"));
    }
    let mut other = key.clone();
    other.tool = "projects_register".into();
    assert_ne!(external_key(&key), external_key(&other));
    other = key.clone();
    other.agent_id = "agent-b".into();
    assert_ne!(external_key(&key), external_key(&other));
}

#[tokio::test]
async fn awaits_100000_then_detaches_and_closes_the_stamped_carrier() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    let id = id(f.obtain(&row).await);
    let calls = f.fake.calls();
    assert_eq!(
        calls.iter().map(|c| c.method.as_str()).collect::<Vec<_>>(),
        ["consent.request", "consent.await", "consent.detach"]
    );
    assert_eq!(
        calls[1].params,
        json!({"elicitation_id":id, "timeout_ms":100000})
    );
    assert!(calls.iter().all(
        |c| c.owner == Owner::from_stamp(&f.stamp).unwrap() && c.deadline == calls[0].deadline
    ));
    let (opened, closed) = f.fake.routes();
    assert_eq!(opened.len(), 1);
    assert_eq!(closed, 1);
    assert_eq!(opened[0].scope_ref, f.stamp.scope_ref);
    assert_eq!(opened[0].scope_epoch, 7);
    assert_eq!(f.fake.record(&id)["state"], "pending");
    assert_eq!(f.saved(&row).state, RequestState::Awaiting);
}

#[tokio::test]
async fn settled_detach_is_consumed_in_the_same_call() {
    for (choice, expected) in [("approve_once", "approved"), ("deny", "consent_declined")] {
        let f = Fixture::new();
        let row = f.row("agents_hire", "key");
        f.fake.script("consent.detach", Effect::Answer(choice));
        let attempt = f.obtain(&row).await;
        match &attempt.decision {
            ConsentDecision::Approved(answer) => {
                assert_eq!(expected, "approved");
                assert_eq!(answer.choice.as_deref(), Some(choice));
            }
            ConsentDecision::Refused(code) => assert_eq!(*code, expected),
            other => panic!("settled detach returned {other:?}"),
        }
        assert_eq!(
            f.saved(&row).settlement.unwrap().choice.as_deref(),
            Some(choice)
        );
        assert_eq!(
            f.saved(&row).state,
            RequestState::Awaiting,
            "executor, not transport, makes terminal writes"
        );
        assert_eq!(f.fake.record("el_1").get("detached"), None);
    }
}

#[tokio::test]
async fn await_settlement_and_expiry_are_not_pending_or_implicit_approval() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    f.fake
        .script("consent.await", Effect::Answer("approve_always_project"));
    assert!(matches!(
        f.obtain(&row).await.decision,
        ConsentDecision::Approved(_)
    ));
    assert!(!f.fake.calls().iter().any(|c| c.method == "consent.detach"));
    let g = Fixture::new();
    let row = g.row("agents_hire", "key");
    let id = id(g.obtain(&row).await);
    g.fake.set_now(601_000);
    assert_eq!(
        g.obtain(&row).await.decision,
        ConsentDecision::Refused("consent_expired")
    );
    assert_eq!(g.fake.record(&id)["state"], "expired");
}

#[tokio::test]
async fn ambiguous_request_captures_before_deciding_and_never_resends() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    f.fake.script("consent.request", Effect::LoseReply);
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Pending(None)
    );
    assert_eq!(f.saved(&row).elicitation_id, None);
    f.fake.answer("el_1", "approve_once");
    assert!(matches!(
        f.obtain(&row).await.decision,
        ConsentDecision::Approved(_)
    ));
    assert_eq!(f.saved(&row).elicitation_id.as_deref(), Some("el_1"));
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        1
    );
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .map(|c| c.method.as_str())
            .collect::<Vec<_>>(),
        ["consent.request", "consent.answers", "consent.ack"]
    );
    assert_eq!(f.saved(&row).state, RequestState::Awaiting);
    assert_eq!(
        f.store()
            .consent_feed_cursor(&row.scope_ref, row.scope_epoch)
            .unwrap(),
        1
    );
}

#[tokio::test]
async fn never_committed_request_waits_eleven_minutes_and_captures_even_at_cutoff() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    f.fake.script("consent.request", Effect::FailBefore);
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Pending(None)
    );
    f.fake.set_now(660_999);
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Pending(None)
    );
    f.fake.set_now(661_000);
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Refused("consent_lost")
    );
    assert_eq!(f.fake.card_count(), 0);
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        1
    );
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.answers")
            .count(),
        2
    );
    let g = Fixture::new();
    let row = g.row("agents_hire", "late-key");
    g.fake.script("consent.request", Effect::LoseReply);
    drop(g.obtain(&row).await);
    g.fake.answer("el_1", "approve_once");
    g.fake.set_now(661_000);
    assert!(
        matches!(g.obtain(&row).await.decision, ConsentDecision::Approved(_)),
        "capture precedes the cutoff decision"
    );
}

#[tokio::test]
async fn definite_refusal_deletes_row_but_cancelled_preserves_ambiguous_request() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    f.fake.script(
        "consent.request",
        Effect::Refuse("elicitation_requester_refused"),
    );
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Refused("consent_unavailable")
    );
    assert!(f.store().consent_request(&row.identity).unwrap().is_none());
    f.fake
        .script("consent.request", Effect::Refuse("cancelled"));
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Pending(None)
    );
    assert_eq!(f.saved(&row).state, RequestState::Awaiting);
    drop(f.obtain(&row).await);
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        2
    );
}

#[test]
fn decoder_port_matches_frozen_v2_inputs_and_rejects_closed_shape_and_bad_reference() {
    for input in [
        include_str!("../tests/fixtures/cingulate-v2/cerebellum-request.json"),
        include_str!("../tests/fixtures/cingulate-v2/cerebellum-ref-request.json"),
    ] {
        let envelope: Value = serde_json::from_str(input).unwrap();
        let params = envelope["params"].clone();
        decoder::decode(params.clone()).unwrap();
        for bad in [
            Value::Null,
            json!(0),
            json!(""),
            json!("a\n"),
            json!("é"),
            json!("x".repeat(129)),
        ] {
            let mut mutant = params.clone();
            mutant["requester_ref"] = bad;
            assert_eq!(decoder::decode(mutant), Err("elicitation_invalid_request"));
        }
        for key in ["agent_id", "owner", "call_key", "unknown"] {
            let mut mutant = params.clone();
            mutant[key] = json!("spoof");
            assert_eq!(decoder::decode(mutant), Err("elicitation_invalid_request"));
        }
        let mut mutant = params.clone();
        mutant["options"][0]["next_step"] = json!("retry");
        assert_eq!(decoder::decode(mutant), Err("elicitation_invalid_request"));
        let mut mutant = params.clone();
        mutant["default"] = json!("allow_once");
        assert_eq!(
            decoder::decode(mutant),
            Err("elicitation_fail_closed_required")
        );
        let mut mutant = params.clone();
        mutant["expires_in_ms"] = json!(0);
        assert_eq!(decoder::decode(mutant), Err("elicitation_invalid_request"));
    }
}

#[tokio::test]
async fn binding_mismatch_cannot_authorize_read_or_ambiguous_feed_recovery() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    let id = id(f.obtain(&row).await);
    f.fake.answer(&id, "approve_once");
    let mut wrong = f.fake.record(&id);
    wrong["binding"]["args_digest"] = json!("other-arguments");
    f.fake.script("consent.get", Effect::Record(wrong));
    assert_eq!(
        f.obtain(&row).await.decision,
        ConsentDecision::Pending(Some(id.clone()))
    );
    assert_eq!(f.saved(&row).settlement, None);
    let g = Fixture::new();
    let row = g.row("agents_hire", "key");
    g.begin(&row);
    let mut wrong = f.fake.record(&id);
    wrong["binding"]["tool"] = json!("projects_register");
    g.fake.script(
        "consent.answers",
        Effect::Record(json!({"records":[wrong], "cursor":1})),
    );
    assert_eq!(
        g.obtain(&row).await.decision,
        ConsentDecision::Pending(None)
    );
    assert_eq!(g.saved(&row).settlement, None);
}

#[tokio::test]
async fn fake_enforces_owner_dedup_cumulative_ack_tombstones_and_nonunique_reference() {
    let f = Fixture::new();
    let route = f.route().await;
    let row = f.row("agents_hire", "key");
    let body = render_card(&row, &context()).unwrap();
    let a = call(route.as_ref(), "consent.request", body.clone()).await["elicitation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    let mut refresh = body.clone();
    refresh["requester_ref"] = json!("changed ref");
    assert_eq!(
        call(route.as_ref(), "consent.request", refresh).await["elicitation_id"],
        a
    );
    let mut same_ref = body.clone();
    same_ref["dedup_key"] = json!("different key");
    let b = call(route.as_ref(), "consent.request", same_ref).await["elicitation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(a, b);
    let another = f
        .fake
        .open(&stamp("agent-b", "head:b", 7), Instant::now() + CALL_BUDGET)
        .await
        .unwrap();
    let c = call(another.as_ref(), "consent.request", body.clone()).await["elicitation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(a, c);
    assert!(
        matches!(bounded_call(another.as_ref(), "consent.get", json!({"elicitation_id":a}), Instant::now()+CALL_BUDGET).await, Err(ConsentError::Refused(code)) if code == "elicitation_not_owner")
    );
    f.fake.answer(&a, "approve_once");
    f.fake.answer(&c, "approve_once");
    f.fake.answer(&b, "approve_once");
    let answers = call(route.as_ref(), "consent.answers", json!({"since":0})).await;
    assert_eq!(answers["cursor"], 3);
    assert_eq!(answers["records"].as_array().unwrap().len(), 2);
    assert!(answers["records"]
        .as_array()
        .unwrap()
        .iter()
        .all(|r| r.get("cursor").is_none()));
    call(route.as_ref(), "consent.ack", json!({"cursor":3})).await;
    assert!(
        call(route.as_ref(), "consent.answers", json!({"since":0})).await["records"]
            .as_array()
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        call(another.as_ref(), "consent.answers", json!({"since":0})).await["records"]
            .as_array()
            .unwrap()
            .len(),
        1
    );
    let d = call(route.as_ref(), "consent.request", body).await["elicitation_id"]
        .as_str()
        .unwrap()
        .to_owned();
    assert_ne!(a, d, "settled dedup never suppresses a new request");
    f.fake.answer(&d, "approve_once");
    f.fake.set_now(1_000 + RETENTION_MS);
    let tombstones = call(route.as_ref(), "consent.answers", json!({"since":3})).await;
    assert_eq!(
        tombstones["records"][0],
        json!({"elicitation_id":d, "state":"expired", "settled_at":1000})
    );
    assert!(settlement(&tombstones["records"][0]).is_none());
}

#[tokio::test]
async fn feed_commit_failure_rolls_back_settlement_and_cursor_without_ack() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    let id = id(f.obtain(&row).await);
    f.fake.answer(&id, "approve_once");
    let db = rusqlite::Connection::open(f.root.join("store.db")).unwrap();
    db.execute_batch("CREATE TRIGGER fail_cursor BEFORE INSERT ON consent_feed_cursor BEGIN SELECT RAISE(ABORT, 'page commit failed'); END;").unwrap();
    let capture = f.capture().await;
    assert!(!capture.complete);
    assert_eq!(capture.cursor, 0);
    assert_eq!(f.saved(&row).settlement, None);
    assert_eq!(
        f.store()
            .consent_feed_cursor(&row.scope_ref, row.scope_epoch)
            .unwrap(),
        0
    );
    assert!(!f.fake.calls().iter().any(|c| c.method == "consent.ack"));
    db.execute_batch("DROP TRIGGER fail_cursor;").unwrap();
    assert!(f.capture().await.complete);
    assert!(f.saved(&row).settlement.is_some());
    assert_eq!(f.saved(&row).state, RequestState::Awaiting);
}

#[tokio::test]
async fn reversed_retries_and_restart_preserve_other_answer_before_cumulative_ack() {
    let mut f = Fixture::new();
    let a = f.row("agents_hire", "a");
    let b = f.row("agents_create_head", "b");
    let aid = id(f.obtain(&a).await);
    let bid = id(f.obtain(&b).await);
    f.fake.answer(&aid, "approve_once");
    f.fake.answer(&bid, "approve_always_workspace");
    f.fake.page_size(1);
    let capture = f.capture().await;
    assert!(capture.complete);
    assert_eq!(capture.cursor, 2);
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.ack")
            .map(|c| c.params["cursor"].as_i64().unwrap())
            .collect::<Vec<_>>(),
        [1, 2]
    );
    let outcome = json!({"applied":"b"});
    assert!(matches!(
        f.obtain(&b).await.decision,
        ConsentDecision::Approved(_)
    ));
    f.store()
        .consent_transaction(|tx| {
            tx.finish_request(&b.identity, &b.args_digest, RequestState::Applied, &outcome)
        })
        .unwrap();
    f.reopen();
    let restarted = ConsentCoordinator::default();
    let attempt = restarted
        .obtain(
            ConsentCall {
                store: f.store(),
                connector: &f.fake,
                stamp: &f.stamp,
                deadline: Instant::now() + CALL_BUDGET,
                now_ms: &|| f.fake.now(),
            },
            &a,
            &context(),
        )
        .await
        .unwrap();
    assert!(
        matches!(attempt.decision, ConsentDecision::Approved(ref answer) if answer.elicitation_id == aid)
    );
    assert_eq!(f.saved(&a).state, RequestState::Awaiting);
    assert_eq!(f.saved(&b).outcome, Some(outcome));
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        2
    );
}

#[tokio::test]
async fn terminal_replay_ignores_ack_retention_and_epoch_but_awaiting_does_not() {
    let f = Fixture::new();
    let row = f.row("agents_hire", "key");
    let id = id(f.obtain(&row).await);
    f.fake.answer(&id, "approve_once");
    assert!(f.capture().await.complete);
    let outcome = json!({"result":{"applied":true}});
    f.store()
        .consent_transaction(|tx| {
            tx.finish_request(
                &row.identity,
                &row.args_digest,
                RequestState::Applied,
                &outcome,
            )
        })
        .unwrap();
    f.fake.set_now(1_000 + RETENTION_MS);
    let current_stamp = stamp("agent-a", "head:a", 8);
    let mut current = row.clone();
    current.scope_epoch = 8;
    let count = f.fake.calls().len();
    let replay = f
        .coordinator
        .obtain(
            ConsentCall {
                store: f.store(),
                connector: &f.fake,
                stamp: &current_stamp,
                deadline: Instant::now() + CALL_BUDGET,
                now_ms: &|| f.fake.now(),
            },
            &current,
            &context(),
        )
        .await
        .unwrap();
    assert_eq!(replay.decision, ConsentDecision::Replay(outcome));
    assert_eq!(f.fake.calls().len(), count);
    let awaiting = f.row("agents_hire", "awaiting");
    f.begin(&awaiting);
    let mut changed = awaiting.clone();
    changed.scope_epoch = 8;
    assert_eq!(
        f.coordinator
            .obtain(
                ConsentCall {
                    store: f.store(),
                    connector: &f.fake,
                    stamp: &current_stamp,
                    deadline: Instant::now() + CALL_BUDGET,
                    now_ms: &|| f.fake.now()
                },
                &changed,
                &context()
            )
            .await
            .unwrap()
            .decision,
        ConsentDecision::Refused("consent_lost")
    );
    let mut conflict = current.clone();
    conflict.args_digest = "other".into();
    assert_eq!(
        f.coordinator
            .obtain(
                ConsentCall {
                    store: f.store(),
                    connector: &f.fake,
                    stamp: &current_stamp,
                    deadline: Instant::now() + CALL_BUDGET,
                    now_ms: &|| f.fake.now()
                },
                &conflict,
                &context()
            )
            .await
            .err()
            .unwrap()
            .code,
        "request_key_conflict"
    );
}

#[tokio::test]
async fn recorded_retention_loss_and_missing_card_refuse_without_reraising() {
    let f = Fixture::new();
    let a = f.row("agents_hire", "a");
    let b = f.row("agents_hire", "b");
    let aid = id(f.obtain(&a).await);
    let bid = id(f.obtain(&b).await);
    f.fake.answer(&aid, "approve_once");
    assert!(f.capture().await.complete);
    f.fake.answer(&bid, "approve_once");
    f.fake.set_now(1_000 + RETENTION_MS);
    assert_eq!(
        f.obtain(&a).await.decision,
        ConsentDecision::Refused("consent_lost")
    );
    assert_eq!(
        f.obtain(&b).await.decision,
        ConsentDecision::Refused("consent_lost")
    );
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        2
    );
}

#[tokio::test(start_paused = true)]
async fn feed_hangs_share_deadline_and_do_not_change_committed_outcome() {
    for method in ["consent.answers", "consent.ack"] {
        let f = Fixture::new();
        let a = f.row("agents_hire", "a");
        let b = f.row("agents_hire", "b");
        let aid = id(f.obtain(&a).await);
        let bid = id(f.obtain(&b).await);
        f.fake.answer(&aid, "approve_once");
        f.fake.answer(&bid, "approve_once");
        let outcome = json!({"applied":"a"});
        f.store()
            .consent_transaction(|tx| {
                tx.finish_request(&a.identity, &a.args_digest, RequestState::Applied, &outcome)
            })
            .unwrap();
        f.fake.script(method, Effect::Hang);
        let deadline = Instant::now() + CALL_BUDGET;
        let route = f.route().await;
        let report = capture_feed(
            f.store(),
            route.as_ref(),
            &Owner::from_stamp(&f.stamp).unwrap(),
            deadline,
        )
        .await;
        assert!(!report.complete);
        assert!(Instant::now() <= deadline, "paused clock deadline");
        assert_eq!(f.saved(&a).outcome, Some(outcome.clone()));
        assert_eq!(f.saved(&a).state, RequestState::Applied);
        assert_eq!(f.saved(&b).state, RequestState::Awaiting);
        if method == "consent.answers" {
            assert_eq!(report.cursor, 0);
            assert_eq!(f.saved(&b).settlement, None);
        } else {
            assert_eq!(report.cursor, 2);
            assert!(f.saved(&b).settlement.is_some());
        }
        assert!(f.capture().await.complete);
        assert!(matches!(
            f.obtain(&b).await.decision,
            ConsentDecision::Approved(_)
        ));
        assert_eq!(
            f.obtain(&a).await.decision,
            ConsentDecision::Replay(outcome)
        );
        assert_eq!(
            f.fake
                .calls()
                .iter()
                .filter(|c| c.method == "consent.request")
                .count(),
            2
        );
    }
}

#[tokio::test]
async fn feed_page_or_ack_failure_stops_at_last_committed_cursor_and_later_capture_recovers() {
    for method in ["consent.answers", "consent.ack"] {
        let f = Fixture::new();
        let a = f.row("agents_hire", "a");
        let b = f.row("agents_hire", "b");
        let aid = id(f.obtain(&a).await);
        let bid = id(f.obtain(&b).await);
        f.fake.answer(&aid, "approve_once");
        f.fake.answer(&bid, "approve_once");
        f.fake.page_size(1);
        if method == "consent.answers" {
            let first = call(
                f.route().await.as_ref(),
                "consent.answers",
                json!({"since":0}),
            )
            .await;
            f.fake.script(method, Effect::Record(first));
        }
        f.fake.script(method, Effect::Refuse("consent_unavailable"));
        let report = f.capture().await;
        assert!(!report.complete);
        assert_eq!(report.cursor, 1);
        assert!(f.saved(&a).settlement.is_some());
        assert!(f.saved(&b).settlement.is_none());
        assert!(f
            .fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.ack")
            .all(|c| c.params["cursor"] == 1));
        assert!(f.capture().await.complete);
        assert!(f.saved(&b).settlement.is_some());
        assert_eq!(f.saved(&a).state, RequestState::Awaiting);
    }
}

#[tokio::test(start_paused = true)]
async fn preterminal_capture_timeout_returns_pending_then_uses_saved_answer_without_resend() {
    for method in ["consent.answers", "consent.ack"] {
        let f = Fixture::new();
        let row = f.row("agents_hire", "key");
        f.fake.script("consent.request", Effect::LoseReply);
        drop(f.obtain(&row).await);
        f.fake.answer("el_1", "approve_once");
        f.fake.script(method, Effect::Hang);
        assert_eq!(
            f.obtain(&row).await.decision,
            ConsentDecision::Pending(None)
        );
        assert_eq!(f.saved(&row).state, RequestState::Awaiting);
        assert!(matches!(
            f.obtain(&row).await.decision,
            ConsentDecision::Approved(_)
        ));
        assert_eq!(
            f.fake
                .calls()
                .iter()
                .filter(|c| c.method == "consent.request")
                .count(),
            1
        );
    }
}

#[tokio::test(start_paused = true)]
async fn lease_serializes_same_key_through_commit_but_not_two_tools_with_one_key() {
    let f = Fixture::new();
    let a = f.row("agents_hire", "same");
    let b = f.row("agents_create_head", "same");
    f.fake.script("consent.await", Effect::Hang);
    let future = f.obtain(&a);
    tokio::pin!(future);
    loop {
        tokio::select! {
            biased;
            _ = &mut future => panic!("wait must remain held"),
            _ = tokio::task::yield_now() => {
                if f.store().consent_request(&a.identity).unwrap().is_some_and(|r| r.elicitation_id.is_some()) { break; }
            }
        }
    }
    assert_eq!(f.saved(&a).elicitation_id.as_deref(), Some("el_1"));
    assert_eq!(
        f.obtain(&a).await.decision,
        ConsentDecision::Pending(Some("el_1".into()))
    );
    let bid = id(f.obtain(&b).await);
    assert_ne!(bid, "el_1");
    let mut conflict = a.clone();
    conflict.args_digest = "different".into();
    assert_eq!(
        f.coordinator
            .obtain(
                ConsentCall {
                    store: f.store(),
                    connector: &f.fake,
                    stamp: &f.stamp,
                    deadline: Instant::now() + CALL_BUDGET,
                    now_ms: &|| f.fake.now()
                },
                &conflict,
                &context()
            )
            .await
            .err()
            .unwrap()
            .code,
        "request_key_conflict"
    );
    f.fake.answer(&bid, "approve_once");
    assert!(
        matches!(f.obtain(&b).await.decision, ConsentDecision::Approved(ref answer) if answer.elicitation_id == bid)
    );
    let first = future.await;
    assert_eq!(
        first.decision,
        ConsentDecision::Pending(Some("el_1".into()))
    );
    let count = f.fake.calls().len();
    assert_eq!(
        f.obtain(&a).await.decision,
        ConsentDecision::Pending(Some("el_1".into()))
    );
    assert_eq!(
        f.fake.calls().len(),
        count,
        "lease is retained until executor drops its attempt after commit"
    );
    drop(first);
    assert_eq!(
        f.fake
            .calls()
            .iter()
            .filter(|c| c.method == "consent.request")
            .count(),
        2
    );
}

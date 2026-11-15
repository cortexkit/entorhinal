use super::*;
use entorhinal_core::{SessionRegistration, SESSION_RETENTION_MS};

fn register(f: &Fixture, agent: &str, reference: &str, harness: &str, session: &str, runner: &str) {
    store(f)
        .register_session(&SessionRegistration {
            agent_id: agent,
            ref_kind: harness,
            session_ref: reference,
            harness,
            session_id: session,
            project_root: None,
            registered_by: "orchestrator",
            runner: Some(runner),
            network: None,
            now_ms: 700,
        })
        .unwrap();
}

fn listed(f: &Fixture, open: bool) -> Value {
    let bytes = f
        .handler
        .agent_sessions(json!({"agent_id":HEAD,"open":open}))
        .unwrap();
    serde_json::from_slice::<Value>(&bytes).unwrap()["result"]["sessions"].clone()
}

#[tokio::test(start_paused = true)]
async fn sessions_watch_other_agents_and_noop_writes_emit_nothing() {
    let f = seeded("sessions-watch-isolation").await;
    let (sink, mut receiver) = Sink::new();
    let body = request(SESSIONS, json!({"agent_id":HEAD}));
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"resync","sessions":[]})
    );
    register(&f, ASSISTANT, "other", "broca", "other", "broca");
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);
    register(&f, HEAD, "ours", "broca", "ours", "broca");
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"], listed(&f, false));
    store(&f)
        .set_session_network("broca", "ours", "yes", || 701)
        .unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"][0]["network"], "yes");
    store(&f)
        .set_session_network("broca", "ours", "yes", || 702)
        .unwrap();
    debounce(watch.as_mut()).await;
    quiet(&mut receiver);
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn sessions_watch_rekeys_rebinds_reopens_and_prunes() {
    let f = seeded("sessions-watch-lifecycle").await;
    register(&f, HEAD, "old", "broca", "old", "runner");
    let (sink, mut receiver) = Sink::new();
    let body = request(SESSIONS, json!({"agent_id":HEAD}));
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"], listed(&f, false));
    register(&f, HEAD, "rekeyed", "broca", "old", "runner");
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"][0]["ref"], "rekeyed");
    register(&f, HEAD, "rekeyed", "broca", "new", "runner2");
    debounce(watch.as_mut()).await;
    let change = notice(&mut receiver);
    assert_eq!(change["sessions"][0]["runner"], "runner2");
    assert_eq!(
        change["removed"],
        json!([{"harness":"broca","session":"old"}])
    );
    register(&f, HEAD, "host-ref", "opencode2", "new", "runner2");
    debounce(watch.as_mut()).await;
    let change = notice(&mut receiver);
    assert_eq!(change["sessions"][0]["harness"], "opencode2");
    assert_eq!(
        change["removed"],
        json!([{"harness":"broca","session":"new"}])
    );
    store(&f)
        .end_session("opencode2", "new", None, 701)
        .unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"][0]["ended_at_ms"], 701);
    register(&f, HEAD, "host-ref", "opencode2", "new", "runner2");
    debounce(watch.as_mut()).await;
    assert!(notice(&mut receiver)["sessions"][0]["ended_at_ms"].is_null());
    store(&f)
        .stamp_gone_sessions(&[entorhinal_core::GoneSession {
            harness: "opencode2".into(),
            session_id: "new".into(),
            runner: None,
            ended_at_ms: 702,
        }])
        .unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(notice(&mut receiver)["sessions"][0]["ended_at_ms"], 702);
    store(&f)
        .prune_ended_sessions(703 + SESSION_RETENTION_MS)
        .unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"changes","sessions":[],"removed":[{"harness":"opencode2","session":"new"}]})
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn sessions_watch_truncates_opening_and_recovers_broadcast_lag() {
    let f = seeded("sessions-watch-overflow").await;
    for i in 0..=crate::sessions::SESSIONS_LIMIT_DEFAULT {
        register(
            &f,
            HEAD,
            &format!("r-{i}"),
            "broca",
            &format!("s-{i}"),
            "runner",
        );
    }
    let (sink, mut receiver) = Sink::new();
    let body = request(SESSIONS, json!({"agent_id":HEAD,"open":true}));
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"resync","truncated":true})
    );
    store(&f).end_session("broca", "s-100", None, 701).unwrap();
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver)["removed"],
        json!([{"harness":"broca","session":"s-100"}])
    );
    for _ in 0..300 {
        register(&f, HEAD, "overflow", "broca", "overflow", "runner");
    }
    debounce(watch.as_mut()).await;
    assert_eq!(
        notice(&mut receiver),
        json!({"kind":"resync","truncated":true})
    );
    sink.cancel.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn sessions_watch_validates_like_sessions_and_ends_on_drain() {
    let f = seeded("sessions-watch-validation").await;
    for (params, code) in [
        (json!({"agent_id":HEAD,"cursor":"x"}), "invalid_request"),
        (json!({"agent_id":HEAD,"limit":1}), "invalid_request"),
        (json!({"agent_id":HEAD,"open":null}), "invalid_request"),
        (json!({"agent_id":HEAD,"open":"yes"}), "invalid_request"),
        (json!({"agent_id":UNKNOWN}), "unknown_agent"),
    ] {
        let (sink, _) = Sink::new();
        let outcome = f
            .handler
            .serve_watch(&sink, &request(SESSIONS, params), READER)
            .await;
        assert!(matches!(outcome, HandlerOutcome::Error { code: actual, .. } if actual == code));
    }
    for principal in [None, Some(Principal::Direct), Some(reserved("any-module"))] {
        f.handler
            .route_admissions()
            .insert(READER, RouteAdmission::from_bind(principal, None));
        let (sink, mut receiver) = Sink::new();
        let body = request(SESSIONS, json!({"agent_id":HEAD}));
        let watch = f.handler.serve_watch(&sink, &body, READER);
        tokio::pin!(watch);
        pending(watch.as_mut()).await;
        notice(&mut receiver);
        sink.cancel.send_replace(true);
        assert!(matches!(watch.await, HandlerOutcome::Streamed));
    }
    let (sink, mut receiver) = Sink::new();
    let body = request(SESSIONS, json!({"agent_id":HEAD}));
    let watch = f.handler.serve_watch(&sink, &body, READER);
    tokio::pin!(watch);
    pending(watch.as_mut()).await;
    notice(&mut receiver);
    f.handler.draining.send_replace(true);
    assert!(matches!(watch.await, HandlerOutcome::Streamed));
}

#[tokio::test(start_paused = true)]
async fn served_vectors_sessions_watch_records_match_published_file() {
    let mut s = Served::with_sessions("served-sessions-watch", true).await;
    let params = json!({"agent_id":HEAD,"open":true});
    let corr = s.start(SESSIONS, params.clone()).await;
    let opening = s.frame(corr).await;
    assert_eq!(opening.header.ty, FrameType::StreamData);
    assert_eq!(
        serde_json::from_slice::<Value>(&opening.body).unwrap(),
        json!({"kind":"resync","sessions":[]})
    );
    s.success(
        "agent.register_session",
        json!({
            "agent_id":HEAD,"ref_kind":"broca","ref":"sessions-watch-ref",
            "harness":"broca","session_id":"sessions-watch-session","runner":"broca",
        }),
    )
    .await;
    let registered = s.frame(corr).await;
    assert_eq!(registered.header.ty, FrameType::StreamData);
    let registered_body: Value = serde_json::from_slice(&registered.body).unwrap();
    let listed = s.handler.agent_sessions(params.clone()).unwrap();
    assert_eq!(registered_body["kind"], "changes");
    assert_eq!(
        registered_body["sessions"],
        serde_json::from_slice::<Value>(&listed).unwrap()["result"]["sessions"]
    );
    s.success(
        "agent.end_session",
        json!({"harness":"broca","session":"sessions-watch-session"}),
    )
    .await;
    let ended = s.frame(corr).await;
    assert_eq!(ended.header.ty, FrameType::StreamData);
    assert_eq!(
        serde_json::from_slice::<Value>(&ended.body).unwrap(),
        json!({
            "kind":"changes","sessions":[],"removed":[{"harness":"broca","session":"sessions-watch-session"}]
        })
    );
    published_file(
        "agent-sessions-v1",
        "sessions-watch.json",
        vec![record(SESSIONS, params, &[opening, registered, ended])],
    );
    s.cancel(corr).await;
    s.stop().await;
}

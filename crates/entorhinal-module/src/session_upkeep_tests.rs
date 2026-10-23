//! Session end, the per-agent session list, the liveness feed's end stamps and
//! retention, through the real handler.

use serde_json::{json, Value};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;
use tokio::time::Duration;

use super::{RETENTION_FIRST_DELAY, RETENTION_INTERVAL};
use crate::{
    sessions::{
        tests::{code, provider, refusal, registration, result, Fixture, CALLER, EXECUTIVE, NOW},
        ProviderResolution, END_SESSION, ORCHESTRATOR_CAPABILITY, SESSIONS,
    },
    tests::reserved,
    WRITER_MODULE,
};
use entorhinal_core::{RegistryStore, SESSION_RETENTION_MS};

const HOST_MODULE: &str = "thalamus-harness";
/// A clock far enough past the epoch that thirty days before it is positive.
const LATE: i64 = 40 * 24 * 60 * 60 * 1000;

fn store(f: &Fixture) -> RegistryStore {
    f.handler.with_store(|s| Ok(s.clone())).unwrap()
}

async fn end(f: &Fixture, route: crate::RouteKey, params: Value) -> HandlerOutcome {
    f.served(route, END_SESSION, params).await
}

fn sessions(f: &Fixture, params: Value) -> Value {
    result(f.sync(CALLER, SESSIONS, params))
}

/// `(session, ended_at_ms, liveness)` for each listed row, in list order.
fn listed(f: &Fixture, agent: &str) -> Vec<(Value, Value, Value)> {
    sessions(f, json!({"agent_id":agent}))["sessions"]
        .as_array()
        .unwrap()
        .iter()
        .map(|row| {
            (
                row["session"].clone(),
                row["ended_at_ms"].clone(),
                row["liveness"].clone(),
            )
        })
        .collect()
}

fn ended_at(f: &Fixture, agent: &str, session: &str) -> Value {
    listed(f, agent)
        .into_iter()
        .find(|(listed, _, _)| listed == session)
        .map(|(_, ended, _)| ended)
        .unwrap_or_else(|| panic!("{session} is not listed"))
}

/// Wait until the upkeep task has finished enough passes for `done` to hold.
/// Each pass signals once, and a pass may finish work queued before the work
/// a test waits for, so the condition is rechecked after every signal. A
/// stamp wait gives up after a bound so a missing stamp fails the test
/// instead of hanging it; retention waits run on tokio's paused clock, where
/// such a bound would fire early, and the clock is advanced explicitly.
async fn until(f: &Fixture, prune: bool, done: impl Fn() -> bool) {
    loop {
        let signal = if prune {
            f.handler.session_upkeep.prune_done.notified()
        } else {
            f.handler.session_upkeep.stamps_done.notified()
        };
        tokio::pin!(signal);
        signal.as_mut().enable();
        if done() {
            return;
        }
        if prune {
            signal.await;
        } else {
            tokio::time::timeout(Duration::from_secs(30), signal)
                .await
                .expect("the upkeep task finished no stamp pass");
        }
    }
}

/// Only the sole provider of agent-orchestrator/v1 may end a session. The
/// host provider may register sessions but not end them, the executive is
/// not admitted by its name, a flow route is refused before any resolution,
/// and a request through the synchronous `handle_request` path, which
/// never resolves providers, is refused.
#[tokio::test]
async fn end_session_admits_only_the_orchestrator_provider() {
    let f = Fixture::new("end-admission", true);
    let agent = f.create("Ended");
    f.providers(provider(WRITER_MODULE), provider(HOST_MODULE));
    result(
        f.register(EXECUTIVE, registration(&agent, "r1", "broca", "s1"))
            .await,
    );
    let calls = f.resolver.calls();
    let params = json!({"harness":"broca","session":"s1"});

    f.bind(CALLER, Some(reserved(WRITER_MODULE)), true);
    assert_eq!(
        code(end(&f, CALLER, params.clone()).await),
        "flow_scope_not_admitted"
    );
    assert_eq!(f.resolver.calls(), calls, "a flow route resolves nothing");

    for principal in [
        Some(reserved(HOST_MODULE)),
        Some(reserved("rogue")),
        Some(Principal::Direct),
        None,
    ] {
        f.bind(CALLER, principal.clone(), false);
        assert_eq!(
            code(end(&f, CALLER, params.clone()).await),
            "caller_forbidden",
            "{principal:?}"
        );
    }
    // A fresh route looks up the orchestrator alone: ending a session never
    // asks who provides the host capability.
    f.bind(CALLER, Some(reserved("rogue")), false);
    let before = f.resolver.calls();
    end(&f, CALLER, params.clone()).await;
    assert_eq!(f.resolver.calls() - before, 1);

    // With no sole orchestrator nobody ends sessions, not even the sole host.
    f.providers(ProviderResolution::Unprovided, provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(HOST_MODULE)), false);
    let (refused, detail) = refusal(end(&f, CALLER, params.clone()).await);
    assert_eq!(refused, "orchestrator_unbound");
    assert_eq!(
        detail,
        Some(json!({ORCHESTRATOR_CAPABILITY: "capability_unprovided"}))
    );
    // The executive is admitted only as the provider, never by its name.
    f.providers(provider("prefrontal-other"), provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(WRITER_MODULE)), false);
    assert_eq!(
        code(end(&f, CALLER, params.clone()).await),
        "caller_forbidden"
    );
    assert_eq!(
        code(f.sync(EXECUTIVE, END_SESSION, params.clone())),
        "orchestrator_unbound"
    );
    assert_eq!(
        ended_at(&f, &agent, "s1"),
        Value::Null,
        "refusals end nothing"
    );

    f.providers(provider(WRITER_MODULE), provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(WRITER_MODULE)), false);
    assert_eq!(
        result(end(&f, CALLER, params).await),
        json!({"found":true,"ended_at_ms":NOW})
    );
}

/// Ending answers `found` with the end time, repeats with the first time,
/// answers `{found: false}` for an unknown session, fills a missing runner
/// and refuses a different one.
#[tokio::test]
async fn end_session_replies_found_repeat_unknown_and_runner_mismatch() {
    let f = Fixture::new("end-replies", true);
    let agent = f.create("Ender");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    let mut hosted = registration(&agent, "r1", "broca", "s1");
    hosted["runner"] = json!("broca");
    result(f.register(EXECUTIVE, hosted).await);
    result(
        f.register(EXECUTIVE, registration(&agent, "r2", "broca", "s2"))
            .await,
    );

    assert_eq!(
        code(
            end(
                &f,
                EXECUTIVE,
                json!({"harness":"broca","session":"s1","runner":"other"})
            )
            .await
        ),
        "runner_mismatch"
    );
    assert_eq!(ended_at(&f, &agent, "s1"), Value::Null);
    assert_eq!(
        result(
            end(
                &f,
                EXECUTIVE,
                json!({"harness":"broca","session":"s1","runner":"broca"})
            )
            .await
        ),
        json!({"found":true,"ended_at_ms":NOW})
    );
    assert_eq!(
        result(end(&f, EXECUTIVE, json!({"harness":"broca","session":"s1"})).await),
        json!({"found":true,"ended_at_ms":NOW})
    );
    assert_eq!(
        result(end(&f, EXECUTIVE, json!({"harness":"broca","session":"nobody"})).await),
        json!({"found":false})
    );
    // The same id under another harness is another session.
    assert_eq!(
        result(end(&f, EXECUTIVE, json!({"harness":"pi","session":"s1"})).await),
        json!({"found":false})
    );
    result(
        end(
            &f,
            EXECUTIVE,
            json!({"harness":"broca","session":"s2","runner":"broca"}),
        )
        .await,
    );
    let rows = sessions(&f, json!({"agent_id":agent}))["sessions"].clone();
    assert!(rows
        .as_array()
        .unwrap()
        .iter()
        .all(|row| row["runner"] == "broca" && row["ended_at_ms"] == NOW));

    for params in [
        json!({"harness":"broca"}),
        json!({"harness":"broca","session":"s1","extra":1}),
        json!({"harness":"broca","session":""}),
        json!({"harness":"broca","session":"s1","runner":""}),
        json!({"harness":"broca","session":"s1","runner":"x".repeat(65)}),
        json!({"harness":"broca","session":"s1","runner":"bro\nca"}),
    ] {
        assert_eq!(
            code(end(&f, EXECUTIVE, params.clone()).await),
            "invalid_request",
            "{params}"
        );
    }
}

/// Registration takes an optional runner without changing its reply bytes,
/// keeps the runner when a later registration names none, and clears an end
/// time because the session is live again.
#[tokio::test]
async fn registration_runner_is_optional_validated_and_not_echoed() {
    let f = Fixture::new("register-runner", true);
    let agent = f.create("Runner");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    let plain = registration(&agent, "r1", "broca", "s1");
    let mut with_runner = plain.clone();
    with_runner["runner"] = json!("broca");
    let HandlerOutcome::Response(first) = f.register(EXECUTIVE, with_runner).await else {
        panic!("registration with a runner refused");
    };
    result(end(&f, EXECUTIVE, json!({"harness":"broca","session":"s1"})).await);
    let HandlerOutcome::Response(second) = f.register(EXECUTIVE, plain).await else {
        panic!("registration without a runner refused");
    };
    assert_eq!(first, second, "the runner is not part of the reply");
    let row = sessions(&f, json!({"agent_id":agent}))["sessions"][0].clone();
    assert_eq!(row["runner"], "broca", "an absent runner keeps the old one");
    assert_eq!(row["ended_at_ms"], Value::Null, "re-registration reopens");

    let mut null_runner = registration(&agent, "r2", "broca", "s2");
    null_runner["runner"] = Value::Null;
    result(f.register(EXECUTIVE, null_runner).await);
    for runner in [json!(""), json!("x".repeat(65)), json!("a\u{7}b"), json!(5)] {
        let mut params = registration(&agent, "r3", "broca", "s3");
        params["runner"] = runner.clone();
        assert_eq!(
            code(f.register(EXECUTIVE, params).await),
            "invalid_request",
            "{runner}"
        );
    }
    assert_eq!(
        listed(&f, &agent).len(),
        2,
        "refused registrations bind nothing"
    );
}

/// The list pages newest first with an opaque cursor, maps liveness as
/// `agent.session_liveness` does, reports an ended session the mirror cannot
/// vouch for as `gone`, and is open to every caller.
#[tokio::test]
async fn agent_sessions_pages_and_maps_liveness() {
    let f = Fixture::new("sessions-list", true);
    let agent = f.create("Lister");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    for (session_ref, session_id) in [
        ("r-active", "s-active"),
        ("r-idle", "s-idle"),
        ("r-gone", "s-gone"),
        ("r-lapsed", "s-lapsed"),
        ("r-ended", "s-ended"),
    ] {
        result(
            f.register(
                EXECUTIVE,
                registration(&agent, session_ref, "broca", session_id),
            )
            .await,
        );
    }
    result(
        end(
            &f,
            EXECUTIVE,
            json!({"harness":"broca","session":"s-ended"}),
        )
        .await,
    );
    f.feed(json!({"seq":1,"snapshot":true,"sessions":[
        {"harness":"broca","sessionId":"s-active","canonicalRoot":"/w","state":"active"},
        {"harness":"broca","sessionId":"s-idle","canonicalRoot":"/w","state":"idle"},
        {"harness":"broca","sessionId":"s-gone","canonicalRoot":"/w","state":"active"},
        {"harness":"broca","sessionId":"s-lapsed","canonicalRoot":"/w","state":"lapsed"}]}))
        .await;
    f.feed(json!({"seq":2,"sessions":[
        {"harness":"broca","sessionId":"s-gone","canonicalRoot":"/w","state":"gone"}]}))
        .await;

    let mut seen = Vec::new();
    let mut cursor = Value::Null;
    loop {
        let mut params = json!({"agent_id":agent,"limit":2});
        if !cursor.is_null() {
            params["cursor"] = cursor.clone();
        }
        let page = sessions(&f, params);
        let rows = page["sessions"].as_array().unwrap();
        assert!(rows.len() <= 2);
        for row in rows {
            seen.push((
                row["ref"].as_str().unwrap().to_owned(),
                row["liveness"].as_str().unwrap().to_owned(),
            ));
        }
        cursor = page["next_cursor"].clone();
        if cursor.is_null() {
            break;
        }
    }
    // Every registration has the same time, so ties order by reference.
    let unfiltered = sessions(&f, json!({"agent_id":agent,"limit":1}));
    assert_eq!(
        unfiltered,
        sessions(&f, json!({"agent_id":agent,"limit":1,"open":false}))
    );
    let first_open = sessions(&f, json!({"agent_id":agent,"limit":1,"open":true}));
    let mut open_seen = Vec::new();
    let mut page = first_open.clone();
    loop {
        for row in page["sessions"].as_array().unwrap() {
            assert!(row["ended_at_ms"].is_null());
            open_seen.push(row["ref"].as_str().unwrap().to_owned());
        }
        if page["next_cursor"].is_null() {
            break;
        }
        page = sessions(
            &f,
            json!({"agent_id":agent,"limit":1,"open":true,"cursor":page["next_cursor"]}),
        );
    }
    assert_eq!(open_seen, ["r-active", "r-gone", "r-idle", "r-lapsed"]);
    for params in [
        json!({"agent_id":agent,"open":true,"cursor":unfiltered["next_cursor"]}),
        json!({"agent_id":agent,"cursor":first_open["next_cursor"]}),
        json!({"agent_id":agent,"open":false,"cursor":first_open["next_cursor"]}),
        json!({"agent_id":agent,"open":"true"}),
    ] {
        assert_eq!(code(f.sync(CALLER, SESSIONS, params)), "invalid_request");
    }
    assert_eq!(
        seen,
        [
            ("r-active", "active"),
            ("r-ended", "gone"),
            ("r-gone", "gone"),
            ("r-idle", "active"),
            ("r-lapsed", "unknown"),
        ]
        .map(|(r, l)| (r.to_owned(), l.to_owned()))
    );

    for principal in [None, Some(Principal::Direct), Some(reserved("phone"))] {
        f.bind(CALLER, principal, true);
        assert!(matches!(
            f.served(CALLER, SESSIONS, json!({"agent_id":agent})).await,
            HandlerOutcome::Response(_)
        ));
    }
    for (params, expected) in [
        (
            json!({"agent_id":"agent_ffffffffffffffff"}),
            "unknown_agent",
        ),
        (json!({"agent_id":"not-an-id"}), "unknown_agent"),
        (json!({"agent_id":agent,"limit":0}), "invalid_request"),
        (json!({"agent_id":agent,"limit":501}), "invalid_request"),
        (json!({"agent_id":agent,"cursor":"%%"}), "invalid_request"),
        (
            json!({"agent_id":agent,"cursor":"bm90IGEgY3Vyc29y"}),
            "invalid_request",
        ),
        (json!({"agent_id":agent,"extra":true}), "invalid_request"),
    ] {
        assert_eq!(
            code(f.sync(CALLER, SESSIONS, params.clone())),
            expected,
            "{params}"
        );
    }
    assert_eq!(
        sessions(&f, json!({"agent_id":agent,"limit":500}))["sessions"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
}

/// A reference moved in from another machine lists with no runner, no session
/// and `unknown` liveness until this machine's host registers it, which fills
/// all three. A retired agent still lists its rows.
#[tokio::test]
async fn moved_in_reference_lists_unknown_until_registered() {
    let f = Fixture::new("sessions-moved-in", true);
    let agent = f.create("Mover");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    let agent_id = agent.clone();
    store(&f)
        .apply_entry("fixture.moved_in", "{}", "fixture", None, move |tx| {
            // The row a move's activation inserts for an incoming reference.
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms)
                 VALUES('pi','r1',?1,NULL,NULL,'','move',5)",
                [&agent_id],
            )?;
            Ok(())
        })
        .unwrap();
    let row = sessions(&f, json!({"agent_id":agent}))["sessions"][0].clone();
    assert_eq!(
        row,
        json!({"runner":null,"session":null,"harness":null,"ref_kind":"pi","ref":"r1",
            "project_root":null,"registered_at_ms":5,"ended_at_ms":null,"liveness":"unknown",
            "network":"unknown"})
    );
    let mut params = registration(&agent, "r1", "broca", "s1");
    params["runner"] = json!("broca");
    result(f.register(EXECUTIVE, params).await);
    let row = sessions(&f, json!({"agent_id":agent}))["sessions"][0].clone();
    assert_eq!(row["runner"], "broca");
    assert_eq!(row["session"], "s1");
    assert_eq!(row["registered_at_ms"], NOW);

    f.sync_ok(
        EXECUTIVE,
        "agent.dispose",
        json!({"agent_id":agent,"request_key":"retire-mover"}),
    );
    assert_eq!(
        listed(&f, &agent).len(),
        1,
        "a retired agent lists its rows"
    );
}

/// An explicit `gone` on the feed stamps the binding's end time on the
/// blocking pool after the batch replied. `lapsed`, `unknown`, absence from a
/// snapshot and a `gone` naming another runner stamp nothing.
#[tokio::test]
async fn liveness_gone_stamps_the_binding_but_lapsed_and_absence_do_not() {
    let f = Fixture::new("upkeep-gone", true);
    let agent = f.create("Watched");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    for (session_ref, session_id, runner) in [
        ("r-gone", "s-gone", None),
        ("r-lapsed", "s-lapsed", None),
        ("r-unknown", "s-unknown", None),
        ("r-absent", "s-absent", None),
        ("r-other", "s-other", Some("broca")),
        ("r-same", "s-same", Some("broca")),
    ] {
        let mut params = registration(&agent, session_ref, "broca", session_id);
        if let Some(runner) = runner {
            params["runner"] = json!(runner);
        }
        result(f.register(EXECUTIVE, params).await);
    }
    let active =
        |id: &str| json!({"harness":"broca","sessionId":id,"canonicalRoot":"/w","state":"active"});
    f.feed(json!({"seq":1,"snapshot":true,"sessions":[
        active("s-gone"), active("s-lapsed"), active("s-unknown"), active("s-absent"),
        active("s-other"), active("s-same")]}))
        .await;
    // The writer task is not running yet, so the batch can only have queued
    // its stamps: its reply does not wait for SQLite.
    f.feed(json!({"seq":2,"sessions":[
        {"harness":"broca","sessionId":"s-gone","canonicalRoot":"/w","state":"gone"},
        {"harness":"broca","sessionId":"s-lapsed","canonicalRoot":"/w","state":"lapsed"},
        {"harness":"broca","sessionId":"s-unknown","canonicalRoot":"/w","state":"unknown"},
        {"harness":"broca","sessionId":"s-other","canonicalRoot":"/w","state":"gone","runner":"thalamus-harness"},
        {"harness":"broca","sessionId":"s-same","canonicalRoot":"/w","state":"gone","runner":"broca"}]}))
    .await;
    f.feed(json!({"seq":3,"snapshot":true,"sessions":[active("s-lapsed")]}))
        .await;
    assert_eq!(ended_at(&f, &agent, "s-gone"), Value::Null);

    f.handler.start_session_upkeep();
    until(&f, false, || ended_at(&f, &agent, "s-same") == NOW).await;
    assert_eq!(ended_at(&f, &agent, "s-gone"), NOW);
    for open in ["s-lapsed", "s-unknown", "s-absent", "s-other"] {
        assert_eq!(ended_at(&f, &agent, open), Value::Null, "{open}");
    }
    assert_eq!(
        f.handler
            .session_upkeep
            .stamped
            .load(std::sync::atomic::Ordering::Relaxed),
        2
    );
    f.handler.stop_session_upkeep().await;
}

/// A stamp that cannot be written is counted and never refuses the batch.
#[tokio::test]
async fn a_failed_stamp_is_counted_and_never_refuses_the_batch() {
    let f = Fixture::new("upkeep-failure", true);
    let agent = f.create("Unlucky");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    result(
        f.register(EXECUTIVE, registration(&agent, "r1", "broca", "s1"))
            .await,
    );
    f.feed(json!({"seq":1,"snapshot":true,"sessions":[]})).await;
    let held = f.handler.store.lock().unwrap().take();
    let failures = || {
        f.handler
            .session_upkeep
            .stamp_failures
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    f.handler.start_session_upkeep();
    let reply = f
        .feed(json!({"seq":2,"sessions":[
            {"harness":"broca","sessionId":"s1","canonicalRoot":"/w","state":"gone"}]}))
        .await;
    assert_eq!(reply["accepted"], true);
    until(&f, false, || failures() == 1).await;
    f.handler.stop_session_upkeep().await;
    *f.handler.store.lock().unwrap() = held;
    assert_eq!(ended_at(&f, &agent, "s1"), Value::Null);
}

/// Retention runs once shortly after the task starts and then hourly, pruning
/// a binding that ended more than thirty days ago and keeping one that ended
/// exactly thirty days ago and one that never ended. Stopping the task waits
/// for its blocking work, so the store can then be closed.
#[tokio::test]
async fn retention_runs_after_startup_then_hourly() {
    let f = Fixture::with_clock("upkeep-retention", true, || LATE);
    let agent = f.create("Retained");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    for (session_ref, session_id) in [
        ("r-old", "s-old"),
        ("r-edge", "s-edge"),
        ("r-open", "s-open"),
    ] {
        result(
            f.register(
                EXECUTIVE,
                registration(&agent, session_ref, "broca", session_id),
            )
            .await,
        );
    }
    let s = store(&f);
    s.end_session("broca", "s-old", None, LATE - SESSION_RETENTION_MS - 1)
        .unwrap();
    s.end_session("broca", "s-edge", None, LATE - SESSION_RETENTION_MS)
        .unwrap();
    let present = |id: &str| listed(&f, &agent).iter().any(|(s, _, _)| s == id);
    let pruned = || {
        f.handler
            .session_upkeep
            .pruned
            .load(std::sync::atomic::Ordering::Relaxed)
    };

    tokio::time::pause();
    f.handler.start_session_upkeep();
    tokio::time::advance(RETENTION_FIRST_DELAY - Duration::from_millis(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(present("s-old"), "no pass before the first delay");
    tokio::time::advance(Duration::from_millis(1)).await;
    until(&f, true, || pruned() == 1).await;
    assert!(!present("s-old"));
    assert!(present("s-edge") && present("s-open"));

    s.end_session("broca", "s-open", None, 0).unwrap();
    tokio::time::advance(RETENTION_INTERVAL - Duration::from_millis(1)).await;
    for _ in 0..8 {
        tokio::task::yield_now().await;
    }
    assert!(present("s-open"), "no second pass before an hour");
    tokio::time::advance(Duration::from_millis(1)).await;
    until(&f, true, || pruned() == 2).await;
    assert!(!present("s-open"));
    assert!(present("s-edge"));

    f.handler.stop_session_upkeep().await;
    assert!(f.handler.session_upkeep_task.lock().unwrap().is_none());
}

/// The end operation is a self-admitted mutation listed with the others, so
/// the central write gate and the flow refusal cover it; the list is a query.
#[test]
fn end_session_is_a_self_admitted_mutation_and_sessions_a_query() {
    assert!(crate::MUTATING_METHODS.contains(&END_SESSION));
    assert!(crate::SELF_ADMITTED_METHODS.contains(&END_SESSION));
    assert!(!crate::MUTATING_METHODS.contains(&SESSIONS));
    assert!(crate::sessions::READ_METHODS.contains(&SESSIONS));
}

/// A runner and session id resolve to their binding for any caller; an
/// unknown pair and a binding registered without a runner answer
/// `not_found` with no detail, as `agent.for_session` does; and a pair that
/// another reference already holds refuses a second registration.
#[tokio::test]
async fn session_for_runner_resolves_runner_rows_for_any_caller() {
    let f = Fixture::new("session-for-runner", true);
    let agent = f.create("Resolved");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    let mut hosted = registration(&agent, "r1", "broca", "s1");
    hosted["runner"] = json!("broca");
    result(f.register(EXECUTIVE, hosted).await);
    result(
        f.register(EXECUTIVE, registration(&agent, "r2", "pi", "s2"))
            .await,
    );
    for principal in [None, Some(Principal::Direct), Some(reserved("thalamus"))] {
        f.bind(CALLER, principal, true);
        assert_eq!(
            result(
                f.served(
                    CALLER,
                    crate::sessions::SESSION_FOR_RUNNER,
                    json!({"runner":"broca","session":"s1"})
                )
                .await
            ),
            json!({"agent_id":agent,"runner":"broca","session":"s1","harness":"broca",
                "ref_kind":"pi","ref":"r1","project_root":"/project","ended_at_ms":null,
                "network":"unknown","network_history":[],
                "incarnation":"0123456789abcdef"})
        );
    }
    let missing = refusal(f.sync(
        CALLER,
        crate::sessions::SESSION_FOR_RUNNER,
        json!({"runner":"broca","session":"nobody"}),
    ));
    let for_session_missing = refusal(f.sync(
        CALLER,
        crate::sessions::FOR_SESSION,
        json!({"harness":"broca","session_id":"nobody"}),
    ));
    assert_eq!(missing, ("not_found".into(), None));
    assert_eq!(missing, for_session_missing);
    for params in [
        json!({"runner":"pi","session":"s2"}),
        json!({"runner":"broca","session":"s2"}),
    ] {
        assert_eq!(
            code(f.sync(CALLER, crate::sessions::SESSION_FOR_RUNNER, params)),
            "not_found"
        );
    }
    for params in [
        json!({"runner":"","session":"s1"}),
        json!({"runner":"broca"}),
        json!({"runner":"broca","session":"s1","harness":"broca"}),
    ] {
        assert_eq!(
            code(f.sync(CALLER, crate::sessions::SESSION_FOR_RUNNER, params.clone())),
            "invalid_request",
            "{params}"
        );
    }

    let mut taken = registration(&agent, "r3", "other", "s1");
    taken["runner"] = json!("broca");
    let (refused, detail) = refusal(f.register(EXECUTIVE, taken).await);
    assert_eq!(refused, "session_bound_elsewhere");
    assert_eq!(
        detail,
        Some(json!({"ref_kind":"pi","ref":"r1","agent_id":agent}))
    );
    assert_eq!(listed(&f, &agent).len(), 2);
}

//! `agent.set_session_network`, the optional `network` on registration, and
//! the network fields of the session reads, through the real handler.

use std::sync::atomic::{AtomicI64, Ordering};

use serde_json::{json, Value};
use subc_client_rs::HandlerOutcome;
use subc_protocol::Principal;

use super::{
    tests::{code, provider, refusal, registration, result, Fixture, CALLER, EXECUTIVE, NOW},
    ProviderResolution, ORCHESTRATOR_CAPABILITY, SESSIONS, SESSION_FOR_RUNNER, SET_SESSION_NETWORK,
};
use crate::{tests::reserved, WRITER_MODULE};

const HOST_MODULE: &str = "thalamus-harness";

async fn set(f: &Fixture, route: crate::RouteKey, params: Value) -> HandlerOutcome {
    f.served(route, SET_SESSION_NETWORK, params).await
}

/// `(network, since_ms)` of each history entry of the `pi` binding
/// `session_ref`, oldest first.
fn history(f: &Fixture, session_ref: &str) -> Vec<(String, i64)> {
    f.handler
        .with_store(|store| store.session_network_history("pi", session_ref))
        .unwrap()
        .into_iter()
        .map(|entry| (entry.network, entry.since_ms))
        .collect()
}

/// A registration that names its runner, so `agent.session_for_runner`
/// finds it.
fn hosted(agent: &str, session_ref: &str, session: &str) -> Value {
    let mut params = registration(agent, session_ref, "broca", session);
    params["runner"] = json!("broca");
    params
}

/// Only the sole provider of agent-orchestrator/v1 may record network
/// access: not the host provider, not the executive by its name, not a
/// flow route, and never through the synchronous path.
#[tokio::test]
async fn set_session_network_admits_only_the_orchestrator_provider() {
    let f = Fixture::new("network-admission", true);
    let agent = f.create("Networked");
    f.providers(provider(WRITER_MODULE), provider(HOST_MODULE));
    result(
        f.register(EXECUTIVE, registration(&agent, "r1", "broca", "s1"))
            .await,
    );
    let calls = f.resolver.calls();
    let params = json!({"harness":"broca","session":"s1","network":"yes"});

    f.bind(CALLER, Some(reserved(WRITER_MODULE)), true);
    assert_eq!(
        code(set(&f, CALLER, params.clone()).await),
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
            code(set(&f, CALLER, params.clone()).await),
            "caller_forbidden",
            "{principal:?}"
        );
    }
    f.providers(ProviderResolution::Unprovided, provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(HOST_MODULE)), false);
    let (refused, detail) = refusal(set(&f, CALLER, params.clone()).await);
    assert_eq!(refused, "orchestrator_unbound");
    assert_eq!(
        detail,
        Some(json!({ORCHESTRATOR_CAPABILITY: "capability_unprovided"}))
    );
    f.providers(provider("prefrontal-other"), provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(WRITER_MODULE)), false);
    assert_eq!(
        code(set(&f, CALLER, params.clone()).await),
        "caller_forbidden"
    );
    assert_eq!(
        code(f.sync(EXECUTIVE, SET_SESSION_NETWORK, params.clone())),
        "orchestrator_unbound"
    );
    assert!(history(&f, "r1").is_empty(), "refusals record nothing");

    f.providers(provider(WRITER_MODULE), provider(HOST_MODULE));
    f.bind(CALLER, Some(reserved(WRITER_MODULE)), false);
    assert_eq!(
        result(set(&f, CALLER, params).await),
        json!({"network":"yes","since_ms":NOW})
    );
}

/// A new value replies its entry, the current value again replies the
/// current entry and writes nothing, an unknown session refuses
/// `not_found`, and a malformed request refuses `invalid_request`.
#[tokio::test]
async fn set_session_network_replies_new_repeat_and_refusals() {
    let f = Fixture::new("network-replies", true);
    let agent = f.create("Replies");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    result(
        f.register(EXECUTIVE, registration(&agent, "r1", "broca", "s1"))
            .await,
    );
    let yes = json!({"harness":"broca","session":"s1","network":"yes"});
    assert_eq!(
        result(set(&f, EXECUTIVE, yes.clone()).await),
        json!({"network":"yes","since_ms":NOW})
    );
    assert_eq!(
        result(set(&f, EXECUTIVE, yes).await),
        json!({"network":"yes","since_ms":NOW})
    );
    assert_eq!(
        history(&f, "r1"),
        [("yes".into(), NOW)],
        "a repeat writes nothing"
    );
    assert_eq!(
        code(
            set(
                &f,
                EXECUTIVE,
                json!({"harness":"broca","session":"nobody","network":"no"})
            )
            .await
        ),
        "not_found"
    );
    for params in [
        json!({"harness":"broca","session":"s1"}),
        json!({"harness":"broca","session":"s1","network":"no","extra":1}),
        json!({"harness":"broca","session":"","network":"no"}),
        json!({"harness":"broca","session":"s1","network":false}),
    ] {
        assert_eq!(
            code(set(&f, EXECUTIVE, params.clone()).await),
            "invalid_request",
            "{params}"
        );
    }
    for value in ["", "NO", "maybe"] {
        let HandlerOutcome::Error { code, .. } = set(
            &f,
            EXECUTIVE,
            json!({"harness":"broca","session":"s1","network":value}),
        )
        .await
        else {
            panic!("{value} was not refused without detail");
        };
        assert_eq!(code, "invalid_request", "{value}");
    }
    assert_eq!(history(&f, "r1"), [("yes".into(), NOW)]);
}

static STEPPING: AtomicI64 = AtomicI64::new(1_000);

fn stepping_clock() -> i64 {
    STEPPING.load(Ordering::Relaxed)
}

/// The served path stamps with the handler's clock, and a clock that steps
/// back still gives each entry a later time than the one before.
#[tokio::test]
async fn set_session_network_stamps_strictly_increasing_when_the_clock_steps_back() {
    let f = Fixture::with_clock("network-clock", true, stepping_clock);
    let agent = f.create("Clocked");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    result(
        f.register(EXECUTIVE, registration(&agent, "r1", "broca", "s1"))
            .await,
    );
    let params = |network: &str| json!({"harness":"broca","session":"s1","network":network});
    assert_eq!(
        result(set(&f, EXECUTIVE, params("yes")).await)["since_ms"],
        1_000
    );
    STEPPING.store(400, Ordering::Relaxed);
    assert_eq!(
        result(set(&f, EXECUTIVE, params("no")).await)["since_ms"],
        1_001
    );
    assert_eq!(
        history(&f, "r1"),
        [("yes".into(), 1_000), ("no".into(), 1_001)]
    );
}

/// Registration takes an optional network value without changing its reply
/// bytes, appends it only when it differs from the current one, reads null
/// as absent, and refuses a value outside the three with nothing written.
#[tokio::test]
async fn registration_network_is_optional_validated_and_not_echoed() {
    let f = Fixture::new("register-network", true);
    let agent = f.create("Registered");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    let plain = registration(&agent, "r1", "broca", "s1");
    let HandlerOutcome::Response(without) = f.register(EXECUTIVE, plain.clone()).await else {
        panic!("registration without a network value refused");
    };
    assert!(
        history(&f, "r1").is_empty(),
        "an absent value records nothing"
    );
    let mut with_yes = plain.clone();
    with_yes["network"] = json!("yes");
    let HandlerOutcome::Response(with) = f.register(EXECUTIVE, with_yes.clone()).await else {
        panic!("registration with a network value refused");
    };
    assert_eq!(without, with, "the network value is not part of the reply");
    result(f.register(EXECUTIVE, with_yes).await);
    let mut null = plain.clone();
    null["network"] = Value::Null;
    result(f.register(EXECUTIVE, null).await);
    assert_eq!(history(&f, "r1"), [("yes".into(), NOW)]);

    for network in [json!("maybe"), json!(""), json!(1)] {
        let mut params = registration(&agent, "r2", "broca", "s2");
        params["network"] = network.clone();
        assert_eq!(
            code(f.register(EXECUTIVE, params).await),
            "invalid_request",
            "{network}"
        );
    }
    assert!(
        f.handler
            .with_store(|store| store.session_binding_for("broca", "s2"))
            .unwrap()
            .is_none(),
        "a refused registration binds nothing"
    );
}

/// With no history both reads say `unknown`, never `no`; after a change
/// `agent.sessions` shows the current value and `agent.session_for_runner`
/// the value and its history oldest first.
#[tokio::test]
async fn reads_show_current_network_and_unknown_when_absent() {
    let f = Fixture::with_clock("network-reads", true, stepping_reads_clock);
    let agent = f.create("Reader");
    f.providers(provider(WRITER_MODULE), provider(WRITER_MODULE));
    result(f.register(EXECUTIVE, hosted(&agent, "r1", "s1")).await);
    let runner = |f: &Fixture| {
        result(f.sync(
            CALLER,
            SESSION_FOR_RUNNER,
            json!({"runner":"broca","session":"s1"}),
        ))
    };
    let listed = |f: &Fixture| {
        result(f.sync(CALLER, SESSIONS, json!({"agent_id":agent})))["sessions"][0]["network"]
            .clone()
    };
    assert_eq!(runner(&f)["network"], "unknown");
    assert_eq!(runner(&f)["network_history"], json!([]));
    assert_eq!(listed(&f), "unknown");

    let params = |network: &str| json!({"harness":"broca","session":"s1","network":network});
    result(set(&f, EXECUTIVE, params("yes")).await);
    READS_CLOCK.store(20, Ordering::Relaxed);
    result(set(&f, EXECUTIVE, params("no")).await);
    assert_eq!(runner(&f)["network"], "no");
    assert_eq!(
        runner(&f)["network_history"],
        json!([{"network":"yes","since_ms":10},{"network":"no","since_ms":20}])
    );
    assert_eq!(listed(&f), "no");
}

static READS_CLOCK: AtomicI64 = AtomicI64::new(10);

fn stepping_reads_clock() -> i64 {
    READS_CLOCK.load(Ordering::Relaxed)
}

#[test]
fn set_session_network_is_a_self_admitted_mutation() {
    assert!(crate::MUTATING_METHODS.contains(&SET_SESSION_NETWORK));
    assert!(crate::SELF_ADMITTED_METHODS.contains(&SET_SESSION_NETWORK));
}

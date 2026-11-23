use super::*;

const AGENT: &str = "agent_0000000000000201";
const DATA_MACHINE: &str = "machine-fern-archive";
const OP: &str = "agent.session_residence";

async fn fixture(case: &str) -> SessionFixture {
    let f = SessionFixture::new(&format!("session-residence-{case}")).await;
    f.store()
        .apply_entry("fixture.residence", "{}", "fixture", None, |tx| {
            seed_agent(tx, AGENT, "Fern", None, false, false);
            tx.execute(
                "INSERT INTO session_binding(ref_kind,ref,agent_id,harness,session_id,project_root,registered_by,registered_at_ms,runner)
                 VALUES('broca-session','residence-ref',?1,'pi','restore','','host',700,'broca')",
                [AGENT],
            )?;
            if case != "sharing-off" && case != "agreement" {
                tx.execute("UPDATE identity_log_state SET state='enabling',enable_id='sample-plan' WHERE id=1", [])?;
            }
            Ok(())
        })
        .unwrap();
    f
}

fn open_move(f: &SessionFixture) -> AgentMove {
    let row = AgentMove {
        cut_id: "cut-fern".into(),
        agent_id: AGENT.into(),
        side: MoveSide::Source,
        peer: OTHER_AUTHOR.into(),
        step: MoveStep::Published,
        plan: json!({"_source":{"published_at_ms":650}}),
        request_body: None,
        record_version: 0,
        claim_version: None,
        claim_take_body: None,
        claim_take_version: None,
        claim_landed_version: None,
        seal_sent: false,
        abort_requested: None,
        hold: None,
        cut_accept: None,
        reason: None,
        detail: None,
        request_key: None,
        attempts: 0,
        activated_at_ms: None,
        created_at_ms: 600,
        updated_at_ms: 650,
    };
    f.store().insert_agent_move(&row).unwrap();
    row
}

async fn answer(f: &SessionFixture) -> Value {
    f.record(
        SESSION_READER,
        READER_MODULE,
        OP,
        json!({"runner":"broca","session":"restore"}),
    )
    .await
    .1["result"]
        .clone()
}

async fn whereabouts(f: &SessionFixture) -> Value {
    f.record(
        SESSION_READER,
        READER_MODULE,
        "agent.where",
        json!({"agent_id":AGENT}),
    )
    .await
    .1
}

#[tokio::test]
async fn session_residence_not_registered() {
    let f = fixture("not-registered").await;
    open_move(&f);
    for params in [
        json!({"runner":"broca","session":"missing"}),
        json!({"runner":"other","session":"restore"}),
    ] {
        assert_eq!(
            f.record(SESSION_READER, READER_MODULE, OP, params).await.1,
            json!({"result":{"state":"not_registered"}})
        );
    }
    f.store()
        .apply_entry("fixture.legacy", "{}", "fixture", None, |tx| {
            tx.execute("UPDATE session_binding SET runner=NULL", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(answer(&f).await, json!({"state":"not_registered"}));
}

#[tokio::test]
async fn session_residence_held_on_precedes_move_and_claim() {
    let f = fixture("held-on").await;
    open_move(&f);
    f.store()
        .apply_entry("fixture.held_on", "{}", "fixture", None, |tx| {
            tx.execute("UPDATE session_binding SET held_on=?1", [DATA_MACHINE])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        answer(&f).await,
        json!({"state":"elsewhere","agent_id":AGENT,"held_on":DATA_MACHINE})
    );
}

#[tokio::test]
async fn session_residence_moving_precedes_unusable_claim() {
    let f = fixture("moving").await;
    let mut row = open_move(&f);
    assert_eq!(answer(&f).await, json!({"state":"moving","agent_id":AGENT}));
    // Terminal moves must not keep a restored session waiting.
    row.step = MoveStep::Released;
    assert!(f.store().update_agent_move(&row).unwrap());
    assert_eq!(answer(&f).await["state"], "unknown");
}

#[tokio::test]
async fn session_residence_unknown_unread_with_sharing_on() {
    let f = fixture("unknown").await;
    for (state, plan, base) in [
        ("enabling", Some("sample-plan"), None),
        ("enabled", None, Some(0)),
    ] {
        f.store().apply_entry("fixture.sharing", "{}", "fixture", None, |tx| {
            tx.execute("UPDATE identity_log_state SET state=?1,enable_id=?2,hub_log_base=?3 WHERE id=1", params![state, plan, base])?;
            Ok(())
        }).unwrap();
        assert_eq!(
            answer(&f).await,
            json!({"state":"unknown","agent_id":AGENT})
        );
        let old = whereabouts(&f).await;
        assert_eq!(old["result"]["known"], false);
        assert_eq!(old["result"]["status"], "unclaimed");
    }
}

#[tokio::test]
async fn session_residence_agrees_with_where_for_sharing_off() {
    let f = fixture("agreement").await;
    assert_eq!(
        answer(&f).await,
        json!({"state":"unclaimed","agent_id":AGENT,"epoch":0})
    );
    assert_eq!(whereabouts(&f).await["result"]["known"], true);
    assert_eq!(whereabouts(&f).await["result"]["status"], "unclaimed");
}

#[tokio::test]
async fn session_residence_strict_bounds_and_open_read_admission() {
    let f = fixture("admission").await;
    let head = f.store().generation().unwrap();
    for principal in [
        None,
        Some(Principal::Direct),
        Some(Principal::Unverified),
        Some(reserved("other")),
    ] {
        f.handler
            .route_admissions()
            .insert(SESSION_READER, RouteAdmission::from_bind(principal, None));
        assert_eq!(answer(&f).await["state"], "unknown");
    }
    for params in [
        json!({"runner":"broca","session":"restore","extra":true}),
        json!({"runner":"x".repeat(65),"session":"restore"}),
        json!({"runner":"broca","session":"x".repeat(257)}),
        json!({"runner":"","session":"restore"}),
        json!({"runner":"broca","session":""}),
        json!({"runner":"broca\n","session":"restore"}),
        json!({"runner":"broca"}),
        json!({"runner":42,"session":"restore"}),
    ] {
        assert_eq!(
            f.record(SESSION_READER, READER_MODULE, OP, params).await.1["code"],
            "invalid_request"
        );
    }
    assert_eq!(
        f.record(
            SESSION_READER,
            READER_MODULE,
            OP,
            json!({"runner":"x".repeat(64),"session":"x".repeat(256)})
        )
        .await
        .1["result"]["state"],
        "not_registered"
    );
    assert_eq!(
        f.store().generation().unwrap(),
        head,
        "reads and refusals must not write"
    );
}

#[tokio::test]
async fn session_residence_records_match_published_files() {
    for (case, expected) in [
        ("not-registered", "not_registered"),
        ("held-on", "elsewhere"),
        ("moving", "moving"),
        ("unknown", "unknown"),
        ("sharing-off", "unclaimed"),
    ] {
        let f = fixture(case).await;
        match case {
            "held-on" => {
                f.store()
                    .apply_entry("fixture.held_on", "{}", "fixture", None, |tx| {
                        tx.execute("UPDATE session_binding SET held_on=?1", [DATA_MACHINE])?;
                        Ok(())
                    })
                    .unwrap();
            }
            "moving" => {
                open_move(&f);
            }
            _ => {}
        }
        let (record, reply) = f.record(SESSION_READER, READER_MODULE, OP,
            json!({"runner":"broca","session":if case == "not-registered" { "missing" } else { "restore" }})).await;
        assert_eq!(reply["result"]["state"], expected, "{case}");
        published_file(
            "agent-session-residence-v1",
            &format!("{case}.json"),
            vec![record],
        );
    }
}

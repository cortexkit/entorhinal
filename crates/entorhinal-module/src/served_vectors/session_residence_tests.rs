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
            tx.execute("UPDATE identity_log_state SET state='enabled' WHERE id=1", [])?;
            tx.execute("INSERT INTO member_name VALUES(?1,'Cedar')", [SELF_AUTHOR])?;
            tx.execute("INSERT INTO member_name VALUES(?1,'Willow')", [OTHER_AUTHOR])?;
            Ok(())
        })
        .unwrap();
    f
}

fn claim(f: &SessionFixture, holder: &str, stale: bool) {
    f.store()
        .cache_agent_claims(
            &[(
                AGENT.into(),
                ClaimObservation::Present {
                    claim: CachedClaim {
                        holder: Some(holder.into()),
                        epoch: 3,
                        cut_id: Some("cut-fern".into()),
                        version: 3,
                        read_at_ms: 700,
                        stale,
                        integrity_failed: false,
                    },
                    self_author: SELF_AUTHOR.into(),
                },
            )],
            700,
        )
        .unwrap();
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
    claim(&f, SELF_AUTHOR, false);
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
    claim(&f, SELF_AUTHOR, false);
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
    claim(&f, SELF_AUTHOR, true);
    f.store()
        .cache_agent_claims(&[(AGENT.into(), ClaimObservation::IntegrityFailed)], 701)
        .unwrap();
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
    for state in ["enabling", "joining", "enabled"] {
        f.store()
            .apply_entry("fixture.sharing", "{}", "fixture", None, |tx| {
                tx.execute("UPDATE identity_log_state SET state=?1 WHERE id=1", [state])?;
                Ok(())
            })
            .unwrap();
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
async fn session_residence_unknown_stale_preserves_claim_fields() {
    let f = fixture("stale").await;
    claim(&f, SELF_AUTHOR, true);
    assert_eq!(
        answer(&f).await,
        json!({"state":"unknown","agent_id":AGENT,
        "holder":SELF_AUTHOR,"holder_name":"Cedar","epoch":3,"cut_id":"cut-fern","read_at_ms":700})
    );
    assert_eq!(whereabouts(&f).await["result"]["status"], "held_here");
}

#[tokio::test]
async fn session_residence_unknown_integrity_is_not_a_refusal() {
    let f = fixture("integrity").await;
    claim(&f, SELF_AUTHOR, false);
    f.store()
        .cache_agent_claims(&[(AGENT.into(), ClaimObservation::IntegrityFailed)], 701)
        .unwrap();
    assert_eq!(answer(&f).await["state"], "unknown");
    assert_eq!(answer(&f).await["epoch"], 3);
    assert_eq!(whereabouts(&f).await["code"], "claim_integrity_failed");
}

#[tokio::test]
async fn session_residence_unknown_self_identity_never_infers_local_hosting() {
    let f = fixture("no-self").await;
    claim(&f, SELF_AUTHOR, false);
    f.store()
        .apply_entry("fixture.no_self", "{}", "fixture", None, |tx| {
            tx.execute("DELETE FROM self_identity", [])?;
            Ok(())
        })
        .unwrap();
    assert_eq!(answer(&f).await["state"], "unknown");
    assert_eq!(answer(&f).await["holder"], SELF_AUTHOR);
    assert_eq!(whereabouts(&f).await["result"]["status"], "held_elsewhere");
}

#[tokio::test]
async fn session_residence_here() {
    let f = fixture("here").await;
    claim(&f, SELF_AUTHOR, false);
    assert_eq!(
        answer(&f).await,
        json!({"state":"here","agent_id":AGENT,
        "holder":SELF_AUTHOR,"holder_name":"Cedar","epoch":3,"cut_id":"cut-fern","read_at_ms":700})
    );
}

#[tokio::test]
async fn session_residence_elsewhere_claim() {
    let f = fixture("elsewhere").await;
    claim(&f, OTHER_AUTHOR, false);
    assert_eq!(
        answer(&f).await,
        json!({"state":"elsewhere","agent_id":AGENT,
        "holder":OTHER_AUTHOR,"holder_name":"Willow","epoch":3,"cut_id":"cut-fern","read_at_ms":700})
    );
}

#[tokio::test]
async fn session_residence_unclaimed() {
    let f = fixture("unclaimed").await;
    f.store()
        .cache_agent_claims(
            &[(
                AGENT.into(),
                ClaimObservation::Absent {
                    stale: false,
                    self_author: SELF_AUTHOR.into(),
                },
            )],
            700,
        )
        .unwrap();
    assert_eq!(
        answer(&f).await,
        json!({"state":"unclaimed","agent_id":AGENT,"epoch":0,"read_at_ms":700})
    );
}

#[tokio::test]
async fn session_residence_agrees_with_where_for_usable_claims_and_sharing_off() {
    let f = fixture("agreement").await;
    f.store()
        .apply_entry("fixture.sharing_off", "{}", "fixture", None, |tx| {
            tx.execute(
                "UPDATE identity_log_state SET state='disabled' WHERE id=1",
                [],
            )?;
            Ok(())
        })
        .unwrap();
    assert_eq!(
        answer(&f).await,
        json!({"state":"unclaimed","agent_id":AGENT,"epoch":0})
    );
    assert_eq!(whereabouts(&f).await["result"]["known"], true);
    assert_eq!(whereabouts(&f).await["result"]["status"], "unclaimed");
    for (holder, state, status) in [
        (OTHER_AUTHOR, "elsewhere", "held_elsewhere"),
        (SELF_AUTHOR, "here", "held_here"),
    ] {
        claim(&f, holder, false);
        let residence = answer(&f).await;
        let old = whereabouts(&f).await;
        assert_eq!(residence["state"], state);
        assert_eq!(old["result"]["status"], status);
        for field in ["epoch", "holder", "holder_name", "cut_id", "read_at_ms"] {
            assert_eq!(residence[field], old["result"][field], "{field}");
        }
    }
    f.store()
        .cache_agent_claims(
            &[(
                AGENT.into(),
                ClaimObservation::Absent {
                    stale: false,
                    self_author: SELF_AUTHOR.into(),
                },
            )],
            701,
        )
        .unwrap();
    assert_eq!(answer(&f).await["state"], "unclaimed");
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
        ("stale", "unknown"),
        ("elsewhere", "elsewhere"),
        ("here", "here"),
        ("unclaimed", "unclaimed"),
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
            "stale" => claim(&f, SELF_AUTHOR, true),
            "elsewhere" => claim(&f, OTHER_AUTHOR, false),
            "here" => claim(&f, SELF_AUTHOR, false),
            "unclaimed" => {
                f.store()
                    .cache_agent_claims(
                        &[(
                            AGENT.into(),
                            ClaimObservation::Absent {
                                stale: false,
                                self_author: SELF_AUTHOR.into(),
                            },
                        )],
                        700,
                    )
                    .unwrap();
            }
            "sharing-off" => {
                f.store()
                    .apply_entry("fixture.sharing_off", "{}", "fixture", None, |tx| {
                        tx.execute(
                            "UPDATE identity_log_state SET state='disabled' WHERE id=1",
                            [],
                        )?;
                        Ok(())
                    })
                    .unwrap();
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

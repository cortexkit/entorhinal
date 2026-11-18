use super::*;
use crate::tests::reserved;
use sha2::{Digest, Sha256};

const DIRECTORY: &str = "agent-move-refs-v1";
const CUT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const UNKNOWN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const READER: RouteKey = (920, 1);

async fn record(node: &MoveMachine, cut: &str) -> (String, Value) {
    node.machine.handler.route_admissions().insert(
        READER,
        RouteAdmission::from_bind(Some(reserved("occipital")), None),
    );
    let params = json!({"cutId":cut});
    let request = serde_json::to_vec(&json!({"method":"agent.move_refs","params":params})).unwrap();
    let reply = match node
        .machine
        .handler
        .handle_served_request(&request, READER)
        .await
    {
        HandlerOutcome::Response(bytes) => String::from_utf8(bytes).unwrap(),
        HandlerOutcome::Error { code, message } => {
            json!({"code":code,"message":message,"detail":null}).to_string()
        }
        other => panic!("unexpected move_refs outcome: {other:?}"),
    };
    let decoded = serde_json::from_str(&reply).unwrap();
    // Admission generates a random cut ID. Normalize only that string in
    // captured bytes; successful handler replies stay otherwise verbatim.
    let record = format!(
        "  {{\"op\":\"agent.move_refs\",\"request\":{},\"principal\":{{\"kind\":\"reserved\",\"moduleId\":\"occipital\"}},\"reply\":{reply}}}",
        params,
    );
    (
        record.replace(cut, if cut == UNKNOWN { UNKNOWN } else { CUT }),
        decoded,
    )
}

#[tokio::test(start_paused = true)]
async fn move_refs_records_match_published_files() {
    let pair = super::agent_refs::pair_with_refs(
        &[
            ("whole-agent", &["agent"]),
            ("sessions", &["session", "pi"]),
            ("duplicate", &["session"]),
        ],
        &[
            AgentRef::new("session", "z"),
            AgentRef::new("session", "a"),
            AgentRef::new("pi", "p"),
        ],
    )
    .await;
    let cut = pair.published("move-refs").await;
    assert_eq!(
        pair.a.row(&cut).plan["participants"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p["module_id"] == "whole-agent")
            .unwrap()["refs"],
        json!([{"ref_kind":"agent","ref":AGENT}])
    );
    // Ending bindings after admission must not remove them from the cut.
    pair.a
        .store()
        .apply_entry("fixture", "{}", "test", None, |tx| {
            tx.execute(
                "UPDATE session_binding SET ended_at_ms=800 WHERE agent_id=?1",
                [AGENT],
            )?;
            Ok(())
        })
        .unwrap();
    assert!(pair.a.store().agent_bound_refs(AGENT).unwrap().is_empty());
    pair.b.pickup().await;
    assert!(pair.b.store().agent_bound_refs(AGENT).unwrap().is_empty());
    let (source, source_reply) = record(&pair.a, &cut).await;
    let (receiver, receiver_reply) = record(&pair.b, &cut).await;
    let expected = json!({"result":{"cutId":cut,"agentId":AGENT,"refs":[
        {"ref_kind":"pi","ref":"p"},
        {"ref_kind":"session","ref":"a"},
        {"ref_kind":"session","ref":"z"}
    ]}});
    assert_eq!(source_reply, expected);
    assert_eq!(receiver_reply, expected);
    let (unknown, refusal) = record(&pair.b, UNKNOWN).await;
    assert_eq!(refusal["code"], "unknown_move");
    let mut sums = String::new();
    for (name, record) in [
        ("source.json", source),
        ("receiver.json", receiver),
        ("unknown-cut.json", unknown),
    ] {
        let bytes = format!("[\n{record}\n]\n");
        crate::served_vectors::published_file(DIRECTORY, name, vec![record]);
        sums.push_str(&format!("{:x}  {name}\n", Sha256::digest(bytes.as_bytes())));
    }
    let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../test-vectors")
        .join(DIRECTORY)
        .join("SHA256SUMS");
    if std::env::var("ENTO_WRITE_SERVED_VECTORS").as_deref() == Ok("1") {
        std::fs::write(path, sums).unwrap();
    } else {
        assert_eq!(std::fs::read_to_string(path).unwrap(), sums);
    }
    pair.finish(&cut).await;
    pair.clean();
}

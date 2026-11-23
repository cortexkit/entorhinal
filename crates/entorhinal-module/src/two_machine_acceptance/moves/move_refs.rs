use crate::{
    tests::{reserved, scratch_descriptor},
    ProjectsHandler, RouteAdmission, RouteKey,
};
use entorhinal_core::{
    move_store::{AgentMove, MoveSide, MoveStep},
    RegistryStore,
};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use subc_client_rs::HandlerOutcome;

const DIRECTORY: &str = "agent-move-refs-v1";
const CUT: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const UNKNOWN: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
const AGENT: &str = "agent_0123456789abcdef";
const READER: RouteKey = (920, 1);

fn node(side: MoveSide) -> (ProjectsHandler, PathBuf) {
    let (dir, descriptor) = scratch_descriptor("move-refs-vector");
    let handler = ProjectsHandler::with_runtime("move-refs-vector".into(), || 800);
    *handler.store.lock().unwrap() = Some(RegistryStore::open(&descriptor).unwrap());
    handler.route_admissions().insert(
        READER,
        RouteAdmission::from_bind(Some(reserved("occipital")), None),
    );
    handler
        .store
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .insert_agent_move(&AgentMove {
            cut_id: CUT.into(),
            agent_id: AGENT.into(),
            side,
            peer: "02020202020202020202020202020202".into(),
            step: if side == MoveSide::Source {
                MoveStep::Published
            } else {
                MoveStep::PickedUp
            },
            plan: json!({"agent_id":AGENT,"from":"01".repeat(16),"to":"02".repeat(16),
            "claim_version":1,"hub_gen":1,"participants":[
                {"module_id":"whole-agent","refs":[{"ref_kind":"agent","ref":AGENT}],"external":[]},
                {"module_id":"sessions","refs":[
                    {"ref_kind":"session","ref":"z"},{"ref_kind":"pi","ref":"p"}
                ],"external":[]},
                {"module_id":"duplicate","refs":[{"ref_kind":"session","ref":"a"}],"external":[]},
                {"module_id":"entorhinal","refs":[
                    {"ref_kind":"package-closure","ref":"closure"},
                    {"ref_kind":"session-bindings","ref":"bindings"}
                ],"external":[]}
            ]}),
            request_body: None,
            record_version: 1,
            claim_version: Some(1),
            claim_take_body: None,
            claim_take_version: None,
            claim_landed_version: None,
            seal_sent: true,
            abort_requested: None,
            hold: None,
            cut_accept: None,
            reason: None,
            detail: None,
            request_key: None,
            attempts: 0,
            activated_at_ms: None,
            created_at_ms: 800,
            updated_at_ms: 800,
        })
        .unwrap();
    (handler, dir)
}

async fn record(handler: &ProjectsHandler, cut: &str) -> (String, Value) {
    let params = json!({"cutId":cut});
    let request = serde_json::to_vec(&json!({"method":"agent.move_refs","params":params})).unwrap();
    let reply = match handler.handle_served_request(&request, READER).await {
        HandlerOutcome::Response(bytes) => String::from_utf8(bytes).unwrap(),
        HandlerOutcome::Error { code, message } => {
            json!({"code":code,"message":message,"detail":null}).to_string()
        }
        other => panic!("unexpected move_refs outcome: {other:?}"),
    };
    let decoded = serde_json::from_str(&reply).unwrap();
    let record = format!(
        "  {{\"op\":\"agent.move_refs\",\"request\":{},\"principal\":{{\"kind\":\"reserved\",\"moduleId\":\"occipital\"}},\"reply\":{reply}}}",
        params,
    );
    (record, decoded)
}

#[tokio::test(start_paused = true)]
async fn move_refs_records_match_published_files() {
    let (source, source_dir) = node(MoveSide::Source);
    let (receiver, receiver_dir) = node(MoveSide::Receiver);
    let (source_record, source_reply) = record(&source, CUT).await;
    let (receiver_record, receiver_reply) = record(&receiver, CUT).await;
    let expected = json!({"result":{"cutId":CUT,"agentId":AGENT,"refs":[
        {"ref_kind":"pi","ref":"p"},
        {"ref_kind":"session","ref":"a"},
        {"ref_kind":"session","ref":"z"}
    ]}});
    assert_eq!(source_reply, expected);
    assert_eq!(receiver_reply, expected);
    let (unknown, refusal) = record(&receiver, UNKNOWN).await;
    assert_eq!(refusal["code"], "unknown_move");
    let mut sums = String::new();
    for (name, record) in [
        ("source.json", source_record),
        ("receiver.json", receiver_record),
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
    drop(source);
    drop(receiver);
    std::fs::remove_dir_all(source_dir).unwrap();
    std::fs::remove_dir_all(receiver_dir).unwrap();
}

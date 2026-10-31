use super::*;
use crate::{fake_cingulate::FakeCingulate, tests::scratch_descriptor};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{
    authenticate_server, generate_daemon_id, generate_key, read_frame, write_atomic, write_frame,
    ConnectionInfo, Endpoint, SCHEMA_VERSION,
};
use tokio::io::AsyncWriteExt;

#[tokio::test]
async fn production_client_opens_scoped_cingulate_target_sends_envelope_and_closes() {
    let (root, _) = scratch_descriptor("cingulate-wire");
    let path = root.join("subc.json");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let info = ConnectionInfo {
        schema: SCHEMA_VERSION,
        wire_version: None,
        endpoints: vec![Endpoint {
            host: "127.0.0.1".into(),
            port: listener.local_addr().unwrap().port(),
        }],
        key: generate_key().unwrap(),
        daemon_id: generate_daemon_id().unwrap(),
        pid: std::process::id(),
        // The SDK only sends project-free binds to compatible daemon versions.
        daemon_ver: "0.39.0".into(),
    };
    write_atomic(&path, &info).unwrap();
    let stamp: ScopeStamp = serde_json::from_value(json!({"owner":{"kind":"reserved", "module_id":"prefrontal"}, "ref":"head:alf", "scope_epoch":7, "kind":"head", "attributes":{"agent_id":"agent-a"}, "owner_authorized":true})).unwrap();
    let expected_stamp = stamp.clone();
    let observed = Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let seen = observed.clone();
    let fake = FakeCingulate::new();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        authenticate_server(
            &mut stream,
            &info.key,
            &info.daemon_id,
            &info.daemon_ver,
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        let mut route = None;
        while let Some(frame) = read_frame(&mut stream).await.unwrap() {
            if frame.header.ty == FrameType::Goodbye {
                seen.lock()
                    .unwrap()
                    .push(json!({"goodbye_channel":frame.header.channel}));
                break;
            }
            let (ty, body) = if frame.header.ty == FrameType::Ping {
                (FrameType::Pong, vec![])
            } else if frame.header.ty == FrameType::Request {
                let body: Value = serde_json::from_slice(&frame.body).unwrap();
                seen.lock().unwrap().push(body.clone());
                let result = if frame.header.channel == 0 {
                    match body["op"].as_str().unwrap() {
                        "server.describe" => {
                            json!({"op":"server.describe", "protocol_ver":subc_protocol::PROTOCOL_VERSION, "subc_ops":[], "capabilities":[], "connected_clients":1})
                        }
                        "route.open" => {
                            let identity = body["identity"].as_object().unwrap();
                            assert_eq!(identity["harness"], "ck-entorhinal");
                            assert!(!identity.contains_key("project_root"));
                            assert!(!identity.contains_key("project_id"));
                            assert_eq!(
                                body["target"],
                                json!({"kind":"management_surface", "module_id":"cingulate"})
                            );
                            assert_eq!(body["scope"]["owner"], json!(expected_stamp.owner));
                            assert_eq!(body["scope"]["ref"], expected_stamp.scope_ref);
                            assert_eq!(body["scope"]["scope_epoch"], 7);
                            route = Some(
                                fake.open(&expected_stamp, Instant::now() + CALL_BUDGET)
                                    .await
                                    .unwrap(),
                            );
                            json!({"op":"route.open", "route_channel":41, "route_epoch":1})
                        }
                        op => panic!("unexpected daemon op {op}"),
                    }
                } else {
                    assert_eq!(frame.header.channel, 41);
                    let result = route
                        .as_ref()
                        .unwrap()
                        .call(
                            body["method"].as_str().unwrap(),
                            body["params"].clone(),
                            Instant::now() + CALL_BUDGET,
                        )
                        .await
                        .unwrap();
                    json!({"result":result})
                };
                (FrameType::Response, serde_json::to_vec(&result).unwrap())
            } else {
                continue;
            };
            let response = Frame::build_with_version(
                frame.header.ver,
                ty,
                Flags::new(false, Priority::Interactive, false),
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                body,
            )
            .unwrap();
            write_frame(&mut stream, &response).await.unwrap();
            stream.flush().await.unwrap();
        }
    });
    let connector = Arc::new(SubcConnector::from_args([
        std::ffi::OsString::from("--subc"),
        path.into_os_string(),
    ]));
    let client = SubcCingulate {
        connector: connector.clone(),
    };
    let route = client
        .open(&stamp, Instant::now() + CALL_BUDGET)
        .await
        .unwrap();
    let params = json!({"since":0});
    assert_eq!(
        bounded_call(
            route.as_ref(),
            "consent.answers",
            params.clone(),
            Instant::now() + CALL_BUDGET
        )
        .await
        .unwrap(),
        json!({"records":[], "cursor":0})
    );
    route.close().await;
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    let observed = observed.lock().unwrap().clone();
    assert!(observed.contains(&json!({"method":"consent.answers", "params":params})));
    assert_eq!(observed.last().unwrap(), &json!({"goodbye_channel":41}));
    connector.consumer().await.unwrap().close().await;
    std::fs::remove_dir_all(root).unwrap();
}

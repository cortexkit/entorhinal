//! Local credential fixture for tests of the published watch APIs.

use super::*;
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{
    authenticate_server, generate_daemon_id, generate_key, read_frame, write_atomic, write_frame,
    ConnectionInfo, Endpoint, SCHEMA_VERSION,
};
use tokio::io::AsyncWriteExt;

pub(super) struct Issuer(tokio::task::JoinHandle<()>);

impl Drop for Issuer {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(super) async fn start(
    dir: &std::path::Path,
    nats_port: u16,
    box_id: &str,
) -> (Issuer, SubcConnector) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
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
        daemon_ver: "0.39.0".into(),
    };
    let path = dir.join("watch-issuer.json");
    write_atomic(&path, &info).unwrap();
    let box_id = box_id.to_owned();
    let task = tokio::spawn(async move {
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
        while let Some(frame) = read_frame(&mut stream).await.unwrap() {
            let (ty, bytes) = match frame.header.ty {
                FrameType::Ping => (FrameType::Pong, Vec::new()),
                FrameType::Request => {
                    let request: Value = serde_json::from_slice(&frame.body).unwrap();
                    let reply = if frame.header.channel == 0 {
                        match request["op"].as_str().unwrap() {
                            "server.describe" => {
                                json!({"op":"server.describe", "protocol_ver":subc_protocol::PROTOCOL_VERSION, "subc_ops":[], "capabilities":[], "connected_clients":1})
                            }
                            "route.open" => {
                                assert_eq!(
                                    request["target"],
                                    json!({"kind":"management_surface", "module_id":"ckbus"})
                                );
                                json!({"op":"route.open", "route_channel":41, "route_epoch":1})
                            }
                            op => panic!("unexpected credential server operation {op}"),
                        }
                    } else {
                        let exp = SystemTime::UNIX_EPOCH.elapsed().unwrap().as_secs() + 3600;
                        let mut fields = json!({
                            "jwt":"fixture", "credential_public":"UFIXTURE", "exp":exp,
                            "inbox_prefix":format!("ckdel.{box_id}.{}", crate::MODULE_ID),
                            "server_url":format!("nats://127.0.0.1:{nats_port}"),
                        });
                        match request["method"].as_str().unwrap() {
                            "ckbus.hub_credential" => {
                                fields["acct"] = json!(format!("fed_{box_id}"));
                                fields["domain"] = json!(DOMAIN);
                                fields["box_id"] = json!(box_id);
                            }
                            "ckbus.credential" => fields["acct"] = json!(format!("box_{box_id}")),
                            "ckbus.nonce_sign" => {
                                use base64::Engine as _;
                                fields = json!({"signature_b64": base64::engine::general_purpose::STANDARD.encode([0; 64])});
                            }
                            method => panic!("unexpected credential method {method}"),
                        }
                        json!({"result":fields})
                    };
                    (FrameType::Response, serde_json::to_vec(&reply).unwrap())
                }
                _ => continue,
            };
            let response = Frame::build_with_version(
                frame.header.ver,
                ty,
                Flags::new(false, Priority::Interactive, false),
                frame.header.channel,
                frame.header.epoch,
                frame.header.corr,
                bytes,
            )
            .unwrap();
            write_frame(&mut stream, &response).await.unwrap();
            stream.flush().await.unwrap();
        }
    });
    let connector = SubcConnector::from_args(["--subc".into(), path.into_os_string()]);
    (Issuer(task), connector)
}

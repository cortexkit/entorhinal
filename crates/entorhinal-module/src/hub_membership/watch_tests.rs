use super::*;
use futures_util::poll;
use std::{future::Future, path::PathBuf};
use subc_protocol::{Flags, Frame, FrameType, Priority};
use subc_transport::{
    authenticate_server, generate_daemon_id, generate_key, read_frame, write_atomic, write_frame,
    ConnectionInfo, Endpoint, SCHEMA_VERSION,
};
use tokio::{io::AsyncWrite, sync::mpsc, task::JoinHandle};

const WATCH_METHOD: &str = "callosum.peer_keys_watch";
const BARRIER: &str = "test.barrier";

struct Daemon {
    scratch: PathBuf,
    task: JoinHandle<()>,
    requests: mpsc::UnboundedReceiver<String>,
    replies: mpsc::UnboundedSender<(FrameType, Value)>,
}

impl Daemon {
    async fn start() -> (Self, Arc<Callosum>) {
        let (scratch, _) = crate::tests::scratch_descriptor("peer-keys-watch");
        let path = scratch.join("subc.json");
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
            daemon_ver: "0.39.0".into(),
        };
        write_atomic(&path, &info).unwrap();
        let (observed, requests) = mpsc::unbounded_channel();
        let (replies, mut scripted) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            stream.set_nodelay(true).unwrap();
            authenticate_server(
                &mut stream,
                &info.key,
                &info.daemon_id,
                &info.daemon_ver,
                Duration::from_secs(2),
            )
            .await
            .unwrap();
            let (mut reader, mut writer) = stream.into_split();
            let mut watch = None;
            loop {
                let frame = read_frame(&mut reader);
                tokio::pin!(frame);
                let Some(frame) = (loop {
                    tokio::select! {
                        biased;
                        Some((ty, body)) = scripted.recv() => {
                            let request = watch.as_ref().expect("a watch request to answer");
                            reply(&mut writer, request, ty, body).await;
                            if ty != FrameType::StreamData {
                                watch = None;
                            }
                        }
                        frame = &mut frame => break frame.unwrap(),
                    }
                }) else {
                    break;
                };
                match frame.header.ty {
                    FrameType::Request => {}
                    FrameType::Cancel => {
                        watch = None;
                        continue;
                    }
                    FrameType::Ping => {
                        reply(&mut writer, &frame, FrameType::Pong, Value::Null).await;
                        continue;
                    }
                    FrameType::Goodbye => break,
                    ty => panic!("unexpected frame: {ty:?}"),
                }
                let body: Value = serde_json::from_slice(&frame.body).unwrap();
                let result = if frame.header.channel == 0 {
                    match body["op"].as_str().unwrap() {
                        "server.describe" => json!({"op":"server.describe", "protocol_ver":
                            subc_protocol::PROTOCOL_VERSION, "subc_ops":[], "capabilities":[], "connected_clients":1}),
                        "route.open" => {
                            assert_eq!(
                                body["target"],
                                json!({"kind":"management_surface", "module_id":"callosum"})
                            );
                            assert_eq!(body["identity"]["harness"], "entorhinal");
                            assert!(body["scope"].is_null());
                            json!({"op":"route.open", "route_channel":41, "route_epoch":1})
                        }
                        op => panic!("unexpected daemon op: {op}"),
                    }
                } else {
                    assert_eq!(frame.header.channel, 41);
                    assert_eq!(body["params"], json!({}));
                    let method = body["method"].as_str().unwrap();
                    if method != BARRIER {
                        observed.send(method.into()).unwrap();
                    }
                    match method {
                        WATCH_METHOD => {
                            assert!(watch.replace(frame).is_none());
                            continue;
                        }
                        METHOD => json!({"result":roster(9)}),
                        BARRIER => json!({}),
                        _ => panic!("unexpected Callosum method: {method}"),
                    }
                };
                reply(&mut writer, &frame, FrameType::Response, result).await;
            }
        });
        let callosum = Arc::new(Callosum {
            connector: SubcConnector::from_args(["--subc".into(), path.into_os_string()]),
            local: Arc::new(LocalIdentity::default()),
        });
        callosum
            .connector
            .consumer()
            .await
            .unwrap()
            .open_route(target(), bind_identity(), CallOptions::default())
            .await
            .unwrap();
        (
            Self {
                scratch,
                task,
                requests,
                replies,
            },
            callosum,
        )
    }

    fn notice(&self, generation: u64) {
        self.replies
            .send((FrameType::StreamData, roster(generation)))
            .unwrap();
    }

    fn refuse(&self, code: &str) {
        self.replies
            .send((FrameType::Error, json!({"code":code,"message":code})))
            .unwrap();
    }
}

impl Drop for Daemon {
    fn drop(&mut self) {
        self.task.abort();
        std::fs::remove_dir_all(&self.scratch).unwrap();
    }
}

async fn reply(
    writer: &mut (impl AsyncWrite + Unpin),
    request: &Frame,
    ty: FrameType,
    body: Value,
) {
    let header = request.header;
    let frame = Frame::build_with_version(
        header.ver,
        ty,
        Flags::new(false, Priority::Interactive, false),
        header.channel,
        header.epoch,
        header.corr,
        if matches!(ty, FrameType::Pong | FrameType::StreamEnd) {
            Vec::new()
        } else {
            serde_json::to_vec(&body).unwrap()
        },
    )
    .unwrap();
    write_frame(writer, &frame).await.unwrap();
}

fn roster(generation: u64) -> Value {
    let peers = if generation == 0 {
        vec![]
    } else {
        vec![json!({
            "fingerprint":"ab".repeat(16),
            "machine_id":"cd".repeat(16),
            "key_record":{"keys":{"fleet_record_ed25519":{
                "pub":"01".repeat(32), "version":generation.to_string()
            }}}
        })]
    };
    json!({"peers":peers})
}

// Socket I/O is real. Keep the paused runtime from jumping to a backoff or
// timeout during a reply. A few millisecond ticks let Tokio round deadlines
// to its timer resolution without advancing as far as the next retry.
async fn drive<T>(future: impl Future<Output = T>) -> T {
    tokio::select! {
        result = future => result,
        _ = async {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            for _ in 0..8 {
                for _ in 0..128 {
                    tokio::task::yield_now().await;
                }
                time::advance(Duration::from_millis(1)).await;
            }
            while std::time::Instant::now() < deadline {
                tokio::task::yield_now().await;
            }
        } => panic!("consumer and fake daemon did not make progress"),
    }
}

#[tokio::test]
async fn hub_membership_peer_keys_live_watch_performs_no_periodic_reads() {
    let (mut daemon, callosum) = Daemon::start().await;
    time::pause();
    let mut updates = callosum.peer_key_updates();
    // Allow a polling implementation to make its first read so the test
    // fails on the read count, not on a missing watch request.
    time::advance(Duration::from_secs(5)).await;
    let first_method = {
        let first = updates.next();
        tokio::pin!(first);
        let method = drive(async {
            tokio::select! {
                biased;
                method = daemon.requests.recv() => method.unwrap(),
                _ = &mut first => panic!("first roster arrived without a request"),
            }
        })
        .await;
        if method == WATCH_METHOD {
            daemon.notice(1);
        }
        drive(first).await.unwrap().unwrap();
        method
    };

    for _ in 0..120 {
        time::advance(Duration::from_secs(5)).await;
        let _ = poll!(updates.next());
        tokio::task::yield_now().await;
    }
    let reads = usize::from(first_method == METHOD)
        + std::iter::from_fn(|| daemon.requests.try_recv().ok())
            .filter(|method| method == METHOD)
            .count();
    assert_eq!(reads, 0, "peer_keys_read calls during a live watch");
    assert_eq!(first_method, WATCH_METHOD);
    daemon.notice(2);
    let changed = drive(updates.next()).await.unwrap().unwrap();
    assert_eq!(changed[0].generation, Some(2));
    daemon.notice(0);
    assert!(drive(updates.next()).await.unwrap().unwrap().is_empty());
}

async fn observe_request(
    daemon: &mut Daemon,
    updates: &mut BoxStream<'static, PeerRoster>,
    method: &str,
) {
    drive(async {
        tokio::select! {
            biased;
            seen = daemon.requests.recv() => assert_eq!(seen.as_deref(), Some(method)),
            _ = updates.next() => panic!("roster arrived before {method}"),
        }
    })
    .await;
}

async fn flush_refusal(callosum: &Callosum, updates: &mut BoxStream<'static, PeerRoster>) {
    // A round trip on the same route orders processing of the scripted Error
    // frame before the test polls the stream to schedule its next retry.
    drive(async {
        callosum
            .connector
            .consumer()
            .await
            .unwrap()
            .call(
                target(),
                bind_identity(),
                request(BARRIER),
                CallOptions::default(),
            )
            .await
            .unwrap();
    })
    .await;
    assert!(poll!(updates.next()).is_pending());
}

#[tokio::test]
async fn hub_membership_peer_keys_read_fallback_continues_during_watch_backoff() {
    let (mut daemon, callosum) = Daemon::start().await;
    time::pause();
    let mut updates = callosum.clone().peer_key_updates();

    observe_request(&mut daemon, &mut updates, WATCH_METHOD).await;
    daemon.refuse("unknown_operation");
    observe_request(&mut daemon, &mut updates, METHOD).await;
    assert_eq!(
        drive(updates.next()).await.unwrap().unwrap()[0].generation,
        Some(9)
    );

    assert!(poll!(updates.next()).is_pending());
    time::advance(Duration::from_secs(1)).await;
    observe_request(&mut daemon, &mut updates, WATCH_METHOD).await;
    daemon.refuse("op_not_found");
    flush_refusal(&callosum, &mut updates).await;

    time::advance(Duration::from_secs(2)).await;
    observe_request(&mut daemon, &mut updates, WATCH_METHOD).await;
    daemon.refuse("fed_not_ready");
    flush_refusal(&callosum, &mut updates).await;

    time::advance(Duration::from_secs(2)).await; // 5 s: fallback read, no watch yet.
    observe_request(&mut daemon, &mut updates, METHOD).await;
    assert_eq!(
        drive(updates.next()).await.unwrap().unwrap()[0].generation,
        Some(9)
    );
    assert!(poll!(updates.next()).is_pending());
    time::advance(Duration::from_secs(2)).await; // 7 s: the backed-off reopen.
    observe_request(&mut daemon, &mut updates, WATCH_METHOD).await;
    daemon.notice(3);
    assert_eq!(
        drive(updates.next()).await.unwrap().unwrap()[0].generation,
        Some(3)
    );

    daemon
        .replies
        .send((FrameType::StreamEnd, Value::Null))
        .unwrap();
    observe_request(&mut daemon, &mut updates, METHOD).await;
    assert_eq!(
        drive(updates.next()).await.unwrap().unwrap()[0].generation,
        Some(9)
    );
    assert!(poll!(updates.next()).is_pending());
    time::advance(Duration::from_secs(1)).await; // A live watch reset the backoff.
    observe_request(&mut daemon, &mut updates, WATCH_METHOD).await;
    daemon.notice(0);
    assert!(drive(updates.next()).await.unwrap().unwrap().is_empty());
}

#[test]
fn hub_membership_peer_keys_watch_decodes_published_callosum_notices() {
    // This fixture is Callosum's published cases.json from the
    // peer-keys-watch-vectors-v1.0 tag.
    let bytes = include_bytes!("../../tests/fixtures/callosum-peer-keys-watch-v1.json");
    assert_eq!(
        entorhinal_catalog::hex(&Sha256::digest(bytes)),
        "11e987c87f461a3179ff096b1ccd457dab74ff2327e09eac6b754df762811ecd"
    );
    let fixture: Value = serde_json::from_slice(bytes).unwrap();
    assert_eq!(fixture["version"], 1);
    let cases = fixture["cases"].as_array().unwrap();
    let mut counts = Vec::new();
    for case in cases {
        assert_eq!(
            case["request"],
            json!({"method": WATCH_METHOD, "params": {}})
        );
        let notices = case["notices"].as_array().unwrap();
        counts.push(notices.len());
        for notice in notices {
            let wire = serde_json::to_vec(notice).unwrap();
            // The published samples have message-signing keys, not fleet
            // record keys. None should be admitted as a membership anchor.
            assert!(decode(&decode_reply(&wire).unwrap()).unwrap().is_empty());
        }
    }
    assert_eq!(counts, [1, 1, 1, 0, 0]);
    assert_eq!(cases[4]["refusal"]["code"], "fed_not_ready");
}

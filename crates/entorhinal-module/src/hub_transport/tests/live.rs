use std::{
    fs::{self, File},
    path::PathBuf,
    process::{Child, Command, Stdio},
    time::SystemTime,
};

use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::sleep,
};

use super::*;

mod identity_log;
mod watch_credentials;

const PREFIX: &str = "ckdel.0123456789abcdef0123456789abcdef.entorhinal";

#[tokio::test]
async fn liveness_bus_ignores_foreign_bucket_traffic_and_keeps_reading() {
    if Command::new("nats-server").arg("-v").output().is_err() {
        eprintln!("nats-server not installed; skipping live liveness watch");
        return;
    }
    let mut rig = Rig::new();
    let ports = rig
        .start(
            "local",
            &format!("jetstream {{ store_dir: {:?} }}", rig.dir.join("local")),
            false,
        )
        .await;
    let box_id = "0123456789abcdef0123456789abcdef";
    let (_issuer, connector) = watch_credentials::start(&rig.dir, ports.client, box_id).await;
    let admin = jetstream::new(
        async_nats::connect(format!("127.0.0.1:{}", ports.client))
            .await
            .unwrap(),
    );
    let name = cortexkit_bus_naming::AccountNames::derive(&format!("box_{box_id}"))
        .unwrap()
        .module_bucket_name(crate::WRITER_MODULE, "session-liveness")
        .unwrap();
    let bucket = admin
        .create_key_value(kv::Config {
            bucket: name.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    let foreign = admin
        .create_key_value(kv::Config {
            bucket: "foreign".into(),
            ..Default::default()
        })
        .await
        .unwrap();
    let handler = crate::ProjectsHandler::with_runtime("live-liveness".into(), || 7);
    handler.start_liveness_bus_with(connector);
    bucket
        .put("_reconciled", r#"{"bootId":"fixture","sessions":0}"#.into())
        .await
        .unwrap();
    within(async {
        while !handler
            .health
            .liveness_bus_authoritative
            .load(Ordering::Relaxed)
        {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;

    // This foreign tombstone would revoke authority if it were read as a
    // session-liveness update. A later valid PUT proves the reader is alive.
    foreign.delete("_reconciled").await.unwrap();
    bucket
        .put(
            "session",
            r#"{"harness":"broca","sessionId":"s","lastActivityMs":7,"state":"active"}"#.into(),
        )
        .await
        .unwrap();
    within(async {
        while handler.health.liveness_bus_sessions.load(Ordering::Relaxed) != 1 {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(handler
        .health
        .liveness_bus_authoritative
        .load(Ordering::Relaxed));
    assert_eq!(
        handler.health.liveness_bus_failures.load(Ordering::Relaxed),
        0
    );
    admin.delete_key_value(&name).await.unwrap();
    let replacement = admin
        .create_key_value(kv::Config {
            bucket: name,
            ..Default::default()
        })
        .await
        .unwrap();
    within(async {
        while handler
            .health
            .liveness_bus_authoritative
            .load(Ordering::Relaxed)
            || handler.health.liveness_bus_sessions.load(Ordering::Relaxed) != 0
        {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    replacement
        .put(
            "_reconciled",
            r#"{"bootId":"replacement","sessions":0}"#.into(),
        )
        .await
        .unwrap();
    within(async {
        while !handler
            .health
            .liveness_bus_authoritative
            .load(Ordering::Relaxed)
        {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    handler.stop_liveness_bus().await;
}

#[derive(serde::Deserialize)]
struct PortsFile {
    nats: Vec<String>,
    #[serde(default)]
    monitoring: Vec<String>,
}

struct BoundPorts {
    client: u16,
    leaf: Option<u16>,
}

struct Rig {
    dir: PathBuf,
    children: Vec<Child>,
}

impl Drop for Rig {
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
        let _ = fs::remove_dir_all(&self.dir);
    }
}

/// Drop stops the servers when a test fails normally, but not when the test
/// process aborts (a stack overflow, a SIGKILL): Drop never runs, and the
/// server outlives the test for hours. On Unix, a watcher subshell blocks
/// reading a pipe whose only writer is this test process. The OS closes that
/// pipe when the process dies however it dies, and the watcher then kills the
/// server. `exec` keeps the server's pid as the child's pid, so the ports file
/// name and `try_wait` still refer to the server itself. The pipe is passed on
/// fd 3 because `sh` gives a background job `/dev/null` as its stdin, which
/// would make the watcher's read return at once.
#[cfg(unix)]
fn nats_server_command(config: &std::path::Path) -> Command {
    let mut command = Command::new("sh");
    command
        .arg("-c")
        .arg(r#"exec 3<&0; ( read _ <&3; kill "$$" ) & exec nats-server -c "$1" </dev/null 3<&-"#)
        .arg("sh")
        .arg(config)
        .stdin(Stdio::piped());
    command
}

#[cfg(not(unix))]
fn nats_server_command(config: &std::path::Path) -> Command {
    let mut command = Command::new("nats-server");
    command.arg("-c").arg(config);
    command
}

impl Rig {
    fn new() -> Self {
        // The counter keeps names unique within this test process: macOS's clock
        // ticks in microseconds, so two rigs started together can read the same
        // time and would otherwise share one folder and one store lease.
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/scratch")
            .join(format!(
                "hub-transport-{}-{}-{}",
                std::process::id(),
                SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos(),
                NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            ));
        fs::create_dir_all(&dir).unwrap();
        Self {
            dir,
            children: Vec::new(),
        }
    }

    async fn start(&mut self, name: &str, config: &str, has_leaf_listener: bool) -> BoundPorts {
        let path = self.dir.join(format!("{name}.conf"));
        let log = self.dir.join(format!("{name}.log"));
        let ports_dir = serde_json::to_string(&self.dir).unwrap();
        let monitor = if has_leaf_listener {
            "http: \"127.0.0.1:-1\"\n"
        } else {
            ""
        };
        // NATS binds its own ports. Picking a free port in the test first
        // leaves it open for another parallel test to claim before NATS starts.
        fs::write(
            &path,
            format!("listen: \"127.0.0.1:-1\"\nports_file_dir: {ports_dir}\n{monitor}{config}"),
        )
        .unwrap();
        self.children.push(
            nats_server_command(&path)
                .stdout(Stdio::null())
                .stderr(File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        let child = self.children.last_mut().unwrap();
        let ports_file = self.dir.join(format!("nats-server_{}.ports", child.id()));
        let failure = |reason: &str| -> ! {
            panic!(
                "{name} did not start ({reason}): {}",
                fs::read_to_string(&log).unwrap()
            );
        };
        tokio::time::timeout(Duration::from_secs(60), async {
            let ports = loop {
                if let Some(status) = child.try_wait().unwrap() {
                    failure(&format!("nats-server exited with {status}"));
                }
                match fs::read(&ports_file) {
                    Ok(bytes) => match serde_json::from_slice::<PortsFile>(&bytes) {
                        Ok(ports) => break ports,
                        Err(error) if error.is_eof() => {} // NATS may still be writing the file.
                        Err(error) => failure(&format!("invalid ports file: {error}")),
                    },
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                    Err(error) => failure(&format!("could not read ports file: {error}")),
                }
                sleep(Duration::from_millis(20)).await;
            };
            let client = ports
                .nats
                .first()
                .and_then(|url| reqwest::Url::parse(url).ok()?.port())
                .unwrap_or_else(|| failure("missing client port"));
            let leaf = if has_leaf_listener {
                // NATS reports client and monitor addresses in the ports
                // file, but only /varz reports the bound leaf listener.
                let monitor_url = ports
                    .monitoring
                    .first()
                    .and_then(|url| reqwest::Url::parse(url).ok()?.join("/varz").ok())
                    .unwrap_or_else(|| failure("missing monitoring address"));
                let response = reqwest::Client::builder()
                    .no_proxy()
                    .build()
                    .unwrap()
                    .get(monitor_url)
                    .send()
                    .await
                    .and_then(reqwest::Response::error_for_status)
                    .unwrap_or_else(|error| {
                        failure(&format!("could not query leaf port: {error}"))
                    });
                let bytes = response
                    .bytes()
                    .await
                    .unwrap_or_else(|error| failure(&format!("could not read /varz: {error}")));
                let varz: Value = serde_json::from_slice(&bytes)
                    .unwrap_or_else(|error| failure(&format!("invalid /varz: {error}")));
                Some(
                    varz["leaf"]["port"]
                        .as_u64()
                        .and_then(|port| u16::try_from(port).ok())
                        .filter(|port| *port != 0)
                        .unwrap_or_else(|| failure("missing leaf listener in /varz")),
                )
            } else {
                None
            };
            for port in std::iter::once(client).chain(leaf) {
                TcpStream::connect(("127.0.0.1", port))
                    .await
                    .unwrap_or_else(|error| {
                        failure(&format!("listener {port} was not ready: {error}"))
                    });
            }
            BoundPorts { client, leaf }
        })
        .await
        .unwrap_or_else(|_| failure("startup hang guard elapsed"))
    }
}

/// Cuts the leaf-to-hub link without disconnecting Entorhinal from its leaf.
async fn leaf_link(hub_port: u16) -> (u16, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let (state, mut online) = watch::channel(true);
    tokio::spawn(async move {
        let mut links = JoinSet::new();
        loop {
            tokio::select! {
                biased;
                changed = online.changed() => {
                    if changed.is_err() { break; }
                    if !*online.borrow_and_update() { links.abort_all(); }
                }
                accepted = listener.accept() => {
                    let (mut leaf, _) = accepted.unwrap();
                    if *online.borrow() {
                        links.spawn(async move {
                            if let Ok(mut hub) = TcpStream::connect(("127.0.0.1", hub_port)).await {
                                let _ = tokio::io::copy_bidirectional(&mut leaf, &mut hub).await;
                            }
                        });
                    }
                }
                _ = links.join_next(), if !links.is_empty() => {}
            }
        }
    });
    (port, state)
}

#[tokio::test]
async fn hub_transport_live_leaf_outage_resumes_and_never_queues_unsent_write() {
    match Command::new("nats-server").arg("-v").output() {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            eprintln!("nats-server not installed; skipping live hub transport rig");
            return;
        }
        result => assert!(result.unwrap().status.success()),
    }
    let mut rig = Rig::new();
    let hub_ports = rig
        .start(
            "hub",
            &format!(
                r#"
        server_name: hub
        jetstream {{ domain: hub, store_dir: {:?} }}
        leafnodes {{ listen: "127.0.0.1:-1", no_advertise: true }}
        mappings {{ "$JS.hub.API.$KV.residence.>" : "$KV.residence.>" }}
    "#,
                rig.dir.join("store").to_string_lossy()
            ),
            true,
        )
        .await;
    let (proxy_port, link) = leaf_link(hub_ports.leaf.unwrap()).await;
    let grant = cortexkit_bus_naming::federation::hub_module_grant(
        &"0123456789abcdef0123456789abcdef".parse().unwrap(),
        crate::MODULE_ID,
    )
    .unwrap();
    let publish = serde_json::to_string(&grant.publish).unwrap();
    let subscribe = serde_json::to_string(&grant.subscribe).unwrap();
    let leaf_ports = rig
        .start(
            "leaf",
            &format!(
                r#"
        server_name: leaf
        leafnodes {{ remotes: [{{ url: "nats-leaf://127.0.0.1:{proxy_port}" }}] }}
        authorization {{ users: [{{ user: test, pass: test,
            permissions: {{
                 publish: {publish},
                 subscribe: {subscribe}
            }}
        }}] }}
    "#
            ),
            false,
        )
        .await;

    let admin = async_nats::connect(format!("127.0.0.1:{}", hub_ports.client))
        .await
        .unwrap();
    let admin = jetstream::new(admin);
    let kv = admin
        .create_key_value(kv::Config {
            bucket: BUCKET.into(),
            history: 1,
            max_value_size: MAX_KV_VALUE,
            max_bytes: MAX_KV_BYTES,
            ..Default::default()
        })
        .await
        .unwrap();
    admin
        .create_stream(jetstream::stream::Config {
            name: LOG_STREAM.into(),
            subjects: vec![LOG_SUBJECT.into()],
            allow_direct: true,
            ..Default::default()
        })
        .await
        .unwrap();

    let client = async_nats::ConnectOptions::new()
        .user_and_password("test".into(), "test".into())
        .custom_inbox_prefix(PREFIX)
        .connect(format!("127.0.0.1:{}", leaf_ports.client))
        .await
        .unwrap();
    let health = Arc::new(HealthGauges::default());
    let session = || Session::new(client.clone(), health.clone());
    let mut watch = Feed::kv(health.clone(), {
        let client = client.clone();
        let health = health.clone();
        move |cursor| {
            Binding {
                session: Session::new(client.clone(), health.clone()),
                changes: None,
            }
            .watch(cursor)
        }
    });
    let mut tail = Feed::log(health.clone(), 1, {
        let client = client.clone();
        let health = health.clone();
        move |cursor| {
            Binding {
                session: Session::new(client.clone(), health.clone()),
                changes: None,
            }
            .tail(cursor.unwrap())
        }
    });
    within(async {
        while session().log().await.is_err() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    let deadline = || Instant::now() + Duration::from_secs(5);
    let first = session()
        .update("residence.alfa", b"initial".to_vec(), 0, "kv1", deadline())
        .await
        .unwrap();
    assert_eq!(
        session()
            .append(b"skip".to_vec(), 0, "log1", deadline())
            .await
            .unwrap(),
        1
    );
    assert_eq!(
        session()
            .append(b"initial".to_vec(), 1, "log2", deadline())
            .await
            .unwrap(),
        2
    );
    let (entry, message) = within(async { tokio::join!(watch.next(), tail.next()) }).await;
    assert_eq!(
        (entry.revision, entry.value.as_ref()),
        (first, b"initial".as_slice())
    );
    assert_eq!(
        (message.sequence, message.bytes.as_slice()),
        (2, b"initial".as_slice())
    );

    link.send_replace(false);
    let link_cut = Instant::now();
    let pending = session();
    let (entry, message) = tokio::join!(
        async {
            // Observe that the separate hub route is gone while the leaf client is connected.
            within(async {
                while session().log().await.is_ok() {
                    sleep(Duration::from_millis(20)).await;
                }
            })
            .await;
            assert_eq!(client.connection_state(), State::Connected);
            let refused = pending
                .update(
                    "residence.unsent",
                    b"never".to_vec(),
                    0,
                    "no-send",
                    deadline(),
                )
                .await
                .unwrap_err();
            assert_eq!(refused.code, "hub_unavailable");

            kv.update(
                "residence.alfa",
                b"changed during outage".as_slice().into(),
                first,
            )
            .await
            .unwrap();
            admin
                .publish(LOG_SUBJECT, b"changed during outage".as_slice().into())
                .await
                .unwrap()
                .await
                .unwrap();
            tokio::time::sleep_until(link_cut + Duration::from_secs(21)).await;
            assert!(!health.hub.kv_watch_live.load(Ordering::Relaxed));
            assert!(!health.hub.log_tail_live.load(Ordering::Relaxed));
            link.send_replace(true);
        },
        tokio::time::timeout(Duration::from_secs(55), async {
            tokio::join!(watch.next(), tail.next())
        })
    )
    .1
    .expect("watch and tail did not redeliver after leaf recovery");
    assert_eq!(entry.value.as_ref(), b"changed during outage");
    assert_eq!(message.sequence, 3);
    assert_eq!(message.bytes, b"changed during outage");
    assert!(health.hub.kv_watch_live.load(Ordering::Relaxed));
    assert!(health.hub.log_tail_live.load(Ordering::Relaxed));
    assert!(kv.entry("residence.unsent").await.unwrap().is_none());
    let stale = session()
        .update(
            "residence.alfa",
            b"stale".to_vec(),
            first,
            "stale",
            deadline(),
        )
        .await
        .unwrap_err();
    assert_eq!(stale.code, "revision_moved");
    let current = session()
        .kv_fence_entry("residence.alfa", deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(current.revision, entry.revision);
    assert_eq!(
        current.value.as_deref(),
        Some(b"changed during outage".as_slice())
    );
    assert!(session()
        .kv_fence_entry("residence.unsent", deadline())
        .await
        .unwrap()
        .is_none());
    assert_eq!(session().log_head(deadline()).await.unwrap(), 3);
    assert_eq!(
        session().log_entry(2, deadline()).await.unwrap().unwrap(),
        b"initial"
    );
    assert!(session().log_entry(4, deadline()).await.unwrap().is_none());
    let forbidden = session()
        .delete(
            "residence.alfa",
            entry.revision,
            "forbidden-delete",
            deadline(),
        )
        .await
        .unwrap_err();
    assert_eq!(forbidden.code, "kv_delete_forbidden");
    let target = "wrapreq.alfa";
    let revision = session()
        .update(target, b"request".to_vec(), 0, "wrap-request", deadline())
        .await
        .unwrap();
    assert_eq!(within(watch.next()).await.revision, revision);
    let stale = session()
        .delete(target, first, "stale-delete", deadline())
        .await
        .unwrap_err();
    assert_eq!(stale.code, "revision_moved");
    session()
        .delete(target, revision, "delete", deadline())
        .await
        .unwrap();
    let marker = within(watch.next()).await;
    assert!(marker.revision > entry.revision);
    assert_eq!(marker.operation, kv::Operation::Delete);
    let current = session()
        .kv_entry(target, deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (current.revision, current.operation),
        (marker.revision, kv::Operation::Delete)
    );
    let deleted = session()
        .kv_fence_entry(target, deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((deleted.revision, deleted.value), (marker.revision, None));

    kv.purge(target).await.unwrap();
    let marker = within(watch.next()).await;
    assert_eq!(marker.operation, kv::Operation::Purge);
    let purged = session()
        .kv_fence_entry(target, deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!((purged.revision, purged.value), (marker.revision, None));
}

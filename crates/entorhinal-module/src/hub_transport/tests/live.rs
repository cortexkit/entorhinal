use std::{
    fs::{self, File},
    net::TcpListener as PortReservation,
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

const PREFIX: &str = "ckdel.0123456789abcdef0123456789abcdef.entorhinal";

fn port() -> u16 {
    PortReservation::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
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

impl Rig {
    fn new() -> Self {
        let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("../../target/scratch")
            .join(format!(
                "hub-transport-{}-{}",
                std::process::id(),
                SystemTime::UNIX_EPOCH.elapsed().unwrap().as_nanos()
            ));
        fs::create_dir_all(&dir).unwrap();
        Self {
            dir,
            children: Vec::new(),
        }
    }

    async fn start(&mut self, name: &str, config: &str, port: u16) {
        let path = self.dir.join(format!("{name}.conf"));
        let log = self.dir.join(format!("{name}.log"));
        fs::write(&path, config).unwrap();
        self.children.push(
            Command::new("nats-server")
                .arg("-c")
                .arg(&path)
                .stdout(Stdio::null())
                .stderr(File::create(&log).unwrap())
                .spawn()
                .unwrap(),
        );
        tokio::time::timeout(Duration::from_secs(5), async {
            while TcpStream::connect(("127.0.0.1", port)).await.is_err() {
                sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{name} did not start: {}", fs::read_to_string(log).unwrap()));
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
    let hub_port = port();
    let leaf_port = port();
    let hub_leaf_port = port();
    let (proxy_port, link) = leaf_link(hub_leaf_port).await;
    rig.start(
        "hub",
        &format!(
            r#"
        listen: 127.0.0.1:{hub_port}
        server_name: hub
        jetstream {{ domain: hub, store_dir: {:?} }}
        leafnodes {{ listen: 127.0.0.1:{hub_leaf_port}, no_advertise: true }}
        mappings {{ "$JS.hub.API.$KV.residence.>" : "$KV.residence.>" }}
    "#,
            rig.dir.join("store").to_string_lossy()
        ),
        hub_port,
    )
    .await;
    rig.start(
        "leaf",
        &format!(
            r#"
        listen: 127.0.0.1:{leaf_port}
        server_name: leaf
        leafnodes {{ remotes: [{{ url: "nats-leaf://127.0.0.1:{proxy_port}" }}] }}
        authorization {{ users: [{{ user: test, pass: test,
            permissions: {{
                publish: ["$JS.hub.API.>", "ck.fleet.idlog", "$JS.FC.>"],
                subscribe: ["{PREFIX}.>"]
            }}
        }}] }}
    "#
        ),
        leaf_port,
    )
    .await;

    let admin = async_nats::connect(format!("127.0.0.1:{hub_port}"))
        .await
        .unwrap();
    let admin = jetstream::new(admin);
    let kv = admin
        .create_key_value(kv::Config {
            bucket: BUCKET.into(),
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
        .connect(format!("127.0.0.1:{leaf_port}"))
        .await
        .unwrap();
    let health = Arc::new(HealthGauges::default());
    let session = || Session::new(client.clone(), health.clone());
    let box_id: BoxId = "0123456789abcdef0123456789abcdef".parse().unwrap();
    let mut watch = Feed::kv(health.clone(), {
        let client = client.clone();
        let health = health.clone();
        let box_id = box_id.clone();
        move |cursor| {
            Binding {
                box_id: box_id.clone(),
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
                box_id: box_id.clone(),
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
    assert_eq!(session().log_head(deadline()).await.unwrap(), 3);
    assert_eq!(
        session().log_entry(2, deadline()).await.unwrap().unwrap(),
        b"initial"
    );
    assert!(session().log_entry(4, deadline()).await.unwrap().is_none());
    let stale = session()
        .delete("residence.alfa", first, "stale-delete", deadline())
        .await
        .unwrap_err();
    assert_eq!(stale.code, "revision_moved");
    session()
        .delete("residence.alfa", entry.revision, "delete", deadline())
        .await
        .unwrap();
    let marker = within(watch.next()).await;
    assert!(marker.revision > entry.revision);
    assert_eq!(marker.operation, kv::Operation::Delete);
    let current = session()
        .kv_entry("residence.alfa", deadline())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (current.revision, current.operation),
        (marker.revision, kv::Operation::Delete)
    );
}

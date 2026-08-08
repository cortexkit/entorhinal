#![forbid(unsafe_code)]

//! Real supervision proof for the walking skeleton.
//!
//! The test is ignored by default because it builds the sibling `ck-subc` binary
//! and starts loopback processes. Run it explicitly with:
//! `cargo test -p projects-module --test real_daemon_e2e -- --ignored --nocapture`.

use std::{
    path::{Path, PathBuf},
    process::Stdio,
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

use serde_json::Value;
use subc_core::{read_frame, write_frame, Frame};
use subc_protocol::{BindIdentity, Flags, FrameType, Priority, RouteTarget};
use subc_transport::{authenticate_client, connection_file};
use tokio::{
    io::AsyncWriteExt,
    net::TcpStream,
    process::{Child, Command},
    time::{sleep, Instant},
};

// Must match the module's own MODULE_ID. This test spawns a real daemon and
// dials the module by this name, so a stale value here does not fail loudly --
// it dials a module that does not exist, which is the same unknown_module shape
// the rename was fixing. The half-renamed pair is the hazard: renaming the
// module and leaving its e2e test pinned to the old name yields a test that
// exercises nothing.
const MODULE_ID: &str = "entorhinal";
const START_TIMEOUT: Duration = Duration::from_secs(15);
const READ_TIMEOUT: Duration = Duration::from_secs(10);
const SUBCONSCIOUS_RELATIVE_TO_CRATE: &str = "../../../subconscious";
static TEMP_COUNTER: AtomicU64 = AtomicU64::new(0);

struct RealDaemon {
    child: Child,
    root: PathBuf,
    connection_file: PathBuf,
}

impl Drop for RealDaemon {
    fn drop(&mut self) {
        let _ = self.child.start_kill();
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn subconscious_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(SUBCONSCIOUS_RELATIVE_TO_CRATE)
}

fn unique_temp_dir(label: &str) -> PathBuf {
    let id = TEMP_COUNTER.fetch_add(1, Ordering::Relaxed);
    std::env::temp_dir().join(format!("ck-projects-{label}-{}-{id}", std::process::id()))
}

fn build_subc_core() -> PathBuf {
    let root = subconscious_root();
    let status = std::process::Command::new(env!("CARGO"))
        .current_dir(&root)
        .args(["build", "--bin", "ck-subc"])
        .status()
        .expect("run cargo build for ck-subc");
    assert!(status.success(), "building ck-subc failed");
    let binary = root.join("target/debug/ck-subc");
    assert!(
        binary.exists(),
        "ck-subc binary missing at {}",
        binary.display()
    );
    binary
}

async fn start_real_daemon() -> RealDaemon {
    let daemon_bin = build_subc_core();
    let module_bin = PathBuf::from(env!("CARGO_BIN_EXE_ck-projects"));
    let root = unique_temp_dir("real-daemon");
    let config_dir = root.join("config/cortexkit");
    let runtime_dir = root.join("runtime");
    std::fs::create_dir_all(&config_dir).expect("create config directory");
    std::fs::create_dir_all(&runtime_dir).expect("create runtime directory");

    let config = serde_json::json!({
        "version": 1,
        "storage": { "backend": "sqlite", "data_home": root.join("data") },
        "modules": {
            MODULE_ID: { "program": module_bin, "args": [], "env": {} }
        }
    });
    std::fs::write(
        config_dir.join("subc.jsonc"),
        serde_json::to_vec_pretty(&config).expect("encode daemon config"),
    )
    .expect("write daemon config");

    let child = Command::new(daemon_bin)
        .env("XDG_CONFIG_HOME", root.join("config"))
        .env("XDG_RUNTIME_DIR", &runtime_dir)
        .env("SUBC_PORT", "0")
        .stdout(Stdio::null())
        .stderr(Stdio::inherit())
        .kill_on_drop(true)
        .spawn()
        .expect("spawn ck-subc");
    let connection_file = runtime_dir.join("subc-connection.json");
    let deadline = Instant::now() + START_TIMEOUT;
    while !connection_file.exists() {
        assert!(
            Instant::now() < deadline,
            "daemon did not publish connection file"
        );
        sleep(Duration::from_millis(50)).await;
    }
    RealDaemon {
        child,
        root,
        connection_file,
    }
}

async fn connect_consumer(path: &Path) -> TcpStream {
    let connection = connection_file::read(path).expect("read connection file");
    let endpoint = connection.endpoints.first().expect("daemon endpoint");
    let mut stream = TcpStream::connect((endpoint.host.as_str(), endpoint.port))
        .await
        .expect("connect to daemon");
    authenticate_client(&mut stream, &connection, Duration::from_secs(2))
        .await
        .expect("authenticate consumer");
    stream
}

async fn read_frame_timeout(stream: &mut TcpStream) -> Frame {
    tokio::time::timeout(READ_TIMEOUT, async {
        read_frame(stream)
            .await
            .expect("read wire frame")
            .expect("daemon connection remains open")
    })
    .await
    .expect("timed out waiting for wire frame")
}

async fn control_rpc(stream: &mut TcpStream, corr: u64, body: Value) -> Frame {
    let frame = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Passive, false),
        0,
        0,
        corr,
        serde_json::to_vec(&body).expect("encode control request"),
    )
    .expect("build control frame");
    write_frame(stream, &frame)
        .await
        .expect("write control frame");
    loop {
        let response = read_frame_timeout(stream).await;
        if response.header.channel == 0 && response.header.corr == corr {
            return response;
        }
    }
}

async fn wait_for_module(stream: &mut TcpStream) {
    let deadline = Instant::now() + START_TIMEOUT;
    let mut corr = 10;
    loop {
        let frame = control_rpc(stream, corr, serde_json::json!({"op":"catalog.list"})).await;
        assert_eq!(frame.header.ty, FrameType::Response);
        let body: Value = serde_json::from_slice(&frame.body).expect("decode catalog");
        if body["modules"]
            .as_array()
            .into_iter()
            .flatten()
            .any(|module| module["module_id"] == MODULE_ID)
        {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "projects module did not register"
        );
        corr += 1;
        sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "builds ck-subc in the sibling subconscious checkout"]
async fn real_daemon_supervises_projects_and_routes_resolve() {
    let daemon = start_real_daemon().await;
    let mut consumer = connect_consumer(&daemon.connection_file).await;
    wait_for_module(&mut consumer).await;

    let project_root = unique_temp_dir("query-root");
    std::fs::create_dir_all(&project_root).expect("create query root");
    let route_open = control_rpc(
        &mut consumer,
        100,
        serde_json::json!({
            "op": "route.open",
            "target": RouteTarget::ManagementSurface { module_id: MODULE_ID.to_string() },
            "identity": BindIdentity {
                project_root: project_root.clone(),
                harness: "projects-real-e2e".to_string(),
                session: "session-1".to_string(),
            }
        }),
    )
    .await;
    assert_eq!(route_open.header.ty, FrameType::Response);
    let route: Value = serde_json::from_slice(&route_open.body).expect("decode route");
    let channel = route["route_channel"].as_u64().expect("route channel") as u16;
    let epoch = route["route_epoch"].as_u64().expect("route epoch") as u32;

    let request = Frame::build(
        FrameType::Request,
        Flags::new(false, Priority::Interactive, false),
        channel,
        epoch,
        101,
        serde_json::to_vec(&serde_json::json!({
            "method": "resolve",
            "params": { "canonicalRoot": project_root }
        }))
        .expect("encode resolve"),
    )
    .expect("build resolve frame");
    write_frame(&mut consumer, &request)
        .await
        .expect("write resolve");
    consumer.flush().await.expect("flush resolve");
    let response = read_frame_timeout(&mut consumer).await;
    assert_eq!(response.header.ty, FrameType::Response);
    let body: Value = serde_json::from_slice(&response.body).expect("decode resolve reply");
    assert_eq!(body["result"]["via"], "implicit");
    assert_eq!(body["result"]["gone"], false);
}

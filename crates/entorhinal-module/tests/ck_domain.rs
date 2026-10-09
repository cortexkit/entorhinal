//! `ck projects`, `ck workspaces` and `ck agents` reach this binary only through the ck
//! dispatcher's domain handshake: `ck-<name> --ck-domain` must exit 0 and print
//! exactly one headline line within 2 seconds, or ck refuses the command. The
//! face comes from argv[0], so the test supplies each face's name through arg0
//! on Unix and through a named executable copy on Windows.
//!
//! The 2-second limit is met by doing no slow work at all, so that's what the
//! test checks: the face answers with no home directory, no data directory and
//! no daemon to reach, which it couldn't if it opened the store or connected.
//! Elapsed time isn't asserted, because the shared test machine can take
//! seconds just to start a process under load; the deadline below only stops a
//! hang.
#[cfg(unix)]
use std::ffi::OsStr;
#[cfg(unix)]
use std::os::unix::process::CommandExt;
use std::{
    io::Read,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use cortexkit_test_support::ckdev_binary;

/// Only stops a hang: a handshake that opened the store or waited on a daemon
/// would block here instead of answering.
const HANG_GUARD: Duration = Duration::from_secs(30);

/// Runs the binary under its `ckdev-` dev name, with an empty environment and
/// no daemon connection file to find.
///
/// Both the executable and argv[0] carry `ckdev-` names, so a test process
/// cannot be mistaken for a production module. Unix overrides argv[0];
/// Windows selects the domain through an executable copy with that name.
fn run_as(face: &str) -> (std::process::ExitStatus, String) {
    let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
    let dev_face = face.replacen("ck-", "ckdev-", 1);

    // The clock alone can't name the directory: Windows' clock is coarse, and
    // the tests in this file run in parallel, so two calls can read the same
    // time. The per-process counter keeps every directory distinct.
    #[cfg(windows)]
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    #[cfg(windows)]
    let scratch = std::env::temp_dir().join(format!(
        "ck-domain-{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
        NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    #[cfg(windows)]
    let binary = {
        std::fs::create_dir(&scratch).expect("create face scratch directory");
        let copy = scratch.join(format!("{dev_face}.exe"));
        std::fs::copy(binary, &copy).expect("copy the face binary");
        copy
    };

    let mut command = Command::new(&binary);
    command.arg("--ck-domain").env_clear();
    #[cfg(unix)]
    command
        .arg0(OsStr::new(&dev_face))
        .env("PATH", "/usr/bin:/bin");
    #[cfg(windows)]
    for name in ["SYSTEMROOT", "PATH"] {
        command.env(
            name,
            std::env::var_os(name).expect("Windows child environment"),
        );
    }
    let mut child = command
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run the face");
    let started = Instant::now();
    let status = loop {
        if let Some(status) = child.try_wait().expect("poll the face") {
            break status;
        }
        if started.elapsed() > HANG_GUARD {
            let _ = child.kill();
            panic!("{face} --ck-domain did not answer: it hung");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    let mut stdout = String::new();
    child
        .stdout
        .take()
        .expect("piped stdout")
        .read_to_string(&mut stdout)
        .expect("read stdout");
    #[cfg(windows)]
    std::fs::remove_dir_all(scratch).expect("remove face scratch directory");
    (status, stdout)
}

#[test]
fn operator_faces_answer_the_ck_domain_handshake_with_one_headline() {
    for (face, headline) in [
        ("ck-projects", "ck projects"),
        ("ck-workspaces", "ck workspaces"),
        ("ck-agents", "ck agents"),
    ] {
        let (status, stdout) = run_as(face);
        assert!(status.success(), "{face} --ck-domain must exit 0: {status}");
        let lines: Vec<&str> = stdout.lines().collect();
        assert_eq!(lines.len(), 1, "{face}: exactly one line, got {stdout:?}");
        assert!(
            lines[0].starts_with(headline),
            "{face}: headline names the domain, got {:?}",
            lines[0]
        );
    }
}

#[test]
fn module_face_is_not_a_ck_domain() {
    let (status, stdout) = run_as("ck-entorhinal");
    assert!(!status.success(), "the module face must refuse --ck-domain");
    assert!(stdout.is_empty());
}

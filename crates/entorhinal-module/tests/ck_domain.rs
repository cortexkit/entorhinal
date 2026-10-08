//! `ck projects`, `ck workspaces` and `ck agents` reach this binary only through the ck
//! dispatcher's domain handshake: `ck-<name> --ck-domain` must exit 0 and print
//! exactly one headline line within 2 seconds, or ck refuses the command. The
//! face comes from argv[0], so the test calls the real binary through a symlink
//! with each face's name, which is how the dispatcher reaches it. Unix only:
//! the faces are symlinks there.
//!
//! The 2-second limit is met by doing no slow work at all, so that's what the
//! test checks: the face answers with no home directory, no data directory and
//! no daemon to reach, which it couldn't if it opened the store or connected.
//! Elapsed time isn't asserted, because the shared test machine can take
//! seconds just to start a process under load; the deadline below only stops a
//! hang.
#![cfg(unix)]

use std::os::unix::process::CommandExt;
use std::{
    ffi::OsStr,
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
/// The executable is a `ckdev-entorhinal` copy, and argv[0] selects the command
/// domain (`ckdev-projects`, `ckdev-workspaces`, or `ckdev-agents`). Both matter:
/// the binary selects its handshake headline from argv[0], and macOS's process
/// list shows argv[0], so a test copy must never carry a `ck-` name there, where
/// it would look like a production binary.
fn run_as(face: &str) -> (std::process::ExitStatus, String) {
    let binary = ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"));
    let dev_face = face.replacen("ck-", "ckdev-", 1);
    let mut child = Command::new(&binary)
        .arg0(OsStr::new(&dev_face))
        .arg("--ck-domain")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
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

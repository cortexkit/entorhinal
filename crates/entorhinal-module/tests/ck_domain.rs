//! `ck projects` and `ck workspaces` reach this binary only through the ck
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

use std::{
    io::Read,
    path::PathBuf,
    process::{Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Only stops a hang: a handshake that opened the store or waited on a daemon
/// would block here instead of answering.
const HANG_GUARD: Duration = Duration::from_secs(30);

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "ck-entorhinal-ck-domain-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).expect("create scratch dir");
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// Runs the binary through a symlink named `face` (the binary picks its role
/// from the name it was run as), with an empty environment: no HOME, no XDG
/// directories and no daemon connection file to find.
fn run_as(face: &str) -> (std::process::ExitStatus, String) {
    let scratch = Scratch::new();
    let link = scratch.0.join(face);
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_ck-entorhinal"), &link).unwrap();
    let mut child = Command::new(&link)
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

//! `ck projects` and `ck workspaces` reach this binary only through the ck
//! dispatcher's domain handshake: `ck-<name> --ck-domain` must exit 0 and print
//! exactly one headline line within 2 seconds, or ck refuses the command. The
//! face comes from argv[0], so the test calls the real binary through a symlink
//! with each face's name, which is how the dispatcher reaches it. Unix only:
//! the faces are symlinks there.
#![cfg(unix)]

use std::{
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

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

fn run_as(face: &str) -> (std::process::Output, Duration) {
    let scratch = Scratch::new();
    let link = scratch.0.join(face);
    std::os::unix::fs::symlink(env!("CARGO_BIN_EXE_ck-entorhinal"), &link).unwrap();
    let started = Instant::now();
    let output = Command::new(&link)
        .arg("--ck-domain")
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .output()
        .expect("run the face");
    (output, started.elapsed())
}

#[test]
fn operator_faces_answer_the_ck_domain_handshake_with_one_headline() {
    for (face, headline) in [
        ("ck-projects", "ck projects"),
        ("ck-workspaces", "ck workspaces"),
    ] {
        let (output, elapsed) = run_as(face);
        assert!(
            output.status.success(),
            "{face} --ck-domain must exit 0: {output:?}"
        );
        let stdout = String::from_utf8(output.stdout).unwrap();
        let lines: Vec<&str> = stdout.lines().collect();
        assert_eq!(lines.len(), 1, "{face}: exactly one line, got {stdout:?}");
        assert!(
            lines[0].starts_with(headline),
            "{face}: headline names the domain, got {:?}",
            lines[0]
        );
        assert!(
            elapsed < Duration::from_secs(2),
            "{face}: handshake took {elapsed:?}"
        );
    }
}

#[test]
fn module_face_is_not_a_ck_domain() {
    let (output, _) = run_as("ck-entorhinal");
    assert!(
        !output.status.success(),
        "the module face must refuse --ck-domain"
    );
    assert!(output.stdout.is_empty());
}

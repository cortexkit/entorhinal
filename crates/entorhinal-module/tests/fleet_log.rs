//! Module mode logs through the fleet logger (docs/specs/fleet-logging.md in
//! the subconscious repo): a dated segment under the module's data dir, which
//! is what `ck module logs entorhinal` reads. Both arms spawn the real binary
//! with an isolated environment, because the path is resolved by the child
//! from its own environment and no shared test helper can isolate that.

use std::{
    path::{Path, PathBuf},
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

/// A scratch home removed on drop, named per process and per call so
/// parallel tests never share one.
struct Scratch(PathBuf);

impl Scratch {
    fn new(label: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ck-entorhinal-fleet-log-{label}-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir_all(&path).expect("create scratch home");
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn run_module(home: &Path, module_id: Option<&str>) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_ck-entorhinal"));
    command
        .env_clear()
        .env("PATH", "/usr/bin:/bin")
        .env("HOME", home)
        .env("XDG_DATA_HOME", home.join("data"))
        .env("XDG_RUNTIME_DIR", home.join("run"))
        .env("TMPDIR", home)
        // A connection file that does not exist: serving fails right after
        // start, which is enough to prove where the process logs.
        .args(["--subc", home.join("absent.json").to_str().unwrap()]);
    if let Some(id) = module_id {
        command.env("SUBC_MODULE_ID", id);
    }
    command.output().expect("spawn ck-entorhinal")
}

fn segments(home: &Path) -> Vec<PathBuf> {
    let dir = home.join("data/cortexkit/entorhinal/logs");
    let Ok(entries) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    entries
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| {
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with("entorhinal.") && name.ends_with(".log"))
        })
        .collect()
}

#[test]
fn supervised_module_writes_its_start_and_failure_to_a_dated_segment() {
    let home = Scratch::new("supervised");
    let output = run_module(home.path(), Some("entorhinal"));
    assert!(
        !output.status.success(),
        "serving without a daemon must fail"
    );

    let found = segments(home.path());
    assert_eq!(
        found.len(),
        1,
        "expected one dated segment, found {found:?}"
    );
    let text = std::fs::read_to_string(&found[0]).unwrap();
    assert!(
        text.contains("INFO  entorhinal: entorhinal module starting"),
        "start line missing from {text:?}"
    );
    assert!(
        text.contains("ERROR entorhinal: module exited:"),
        "the fatal error belongs in the log, found {text:?}"
    );
    // Once the logger is installed, nothing is left for stderr to carry.
    assert!(
        output.stderr.is_empty(),
        "stderr must be empty after logger start, got {:?}",
        String::from_utf8_lossy(&output.stderr)
    );
}

#[test]
fn module_without_a_supervised_id_refuses_by_name_and_writes_no_segment() {
    let home = Scratch::new("unsupervised");
    let output = run_module(home.path(), None);
    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("cannot start the fleet logger") && stderr.contains("SUBC_MODULE_ID"),
        "refusal must name the missing variable, got {stderr:?}"
    );
    assert!(segments(home.path()).is_empty());
}

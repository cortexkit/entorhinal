//! Checks both crates' test sources for production binaries spawned without
//! first staging them as `ckdev-*` copies, so test processes cannot be mistaken
//! for production processes. Source fixtures exercise accepted and rejected
//! spawns through the same shared guard API used for the repository scan.

use std::{
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use cortexkit_test_support::assert_test_binary_spawns;

struct Scratch(PathBuf);

impl Scratch {
    fn new(file_name: &str, source: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let id = NEXT.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "ck-entorhinal-spawn-guard-{}-{id}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("create spawn guard fixture");
        let fixture = Self(path);
        std::fs::write(fixture.0.join(file_name), source).expect("write spawn guard fixture");
        fixture
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn scan_source(file_name: &str, source: &str) {
    let fixture = Scratch::new(file_name, source);
    assert_test_binary_spawns(&[&fixture.0]);
}

fn assert_rejected(file_name: &str, source: &str) {
    let failure = std::panic::catch_unwind(|| scan_source(file_name, source))
        .expect_err(&format!("spawn guard accepted {file_name}"));
    let message = failure
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| failure.downcast_ref::<&str>().copied())
        .expect("spawn guard panic contains a message");
    assert!(
        message.contains(&format!("{file_name}:"))
            && message.contains("binary source is not passed through ckdev_binary"),
        "expected an unwrapped spawn in {file_name}, got: {message}"
    );
}

#[test]
fn test_sources_only_execute_ckdev_named_binaries() {
    let module_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    assert_test_binary_spawns(&[
        module_root.join("tests"),
        module_root.join("../entorhinal-core/tests"),
    ]);
}

#[test]
fn binary_spawn_guard_controls_reject_direct_and_adjacent_spawns() {
    let direct = r#"
        fn test() {
            Command::new(env!("CARGO_BIN_EXE_ck-entorhinal")).spawn();
        }
    "#;
    assert_rejected("direct.rs", direct);

    let adjacent = r#"
        fn test() {
            Command::new(ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"))).spawn();
            Command::new(env!("CARGO_BIN_EXE_ck-entorhinal")).spawn();
        }
    "#;
    assert_rejected("adjacent.rs", adjacent);

    let wrapped = r#"
        fn test() {
            Command::new(ckdev_binary(env!("CARGO_BIN_EXE_ck-entorhinal"))).spawn();
        }
    "#;
    scan_source("wrapped.rs", wrapped);

    let commented = r#"
        /// Copies `ck-<name>` into `target/` as `ckdev-<name>`.
        pub fn ckdev_binary(source: &Path) -> PathBuf {
            // env!("CARGO_BIN_EXE_ck-entorhinal") is never spawned here.
            let copy = Command::new("cp").arg(source).status();
        }
    "#;
    scan_source("commented.rs", commented);

    let literal = r#"
        fn test() {
            Command::new("target/debug/ck-subc").spawn();
        }
    "#;
    assert_rejected("literal.rs", literal);
}

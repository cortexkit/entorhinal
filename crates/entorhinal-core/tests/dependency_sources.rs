//! Every dependency must resolve to a published registry or a pinned git
//! revision, never to a path outside this repository.
//!
//! A path dependency on a sibling checkout (for example `../subconscious`)
//! makes this repository's `Cargo.lock` depend on whatever that checkout holds
//! today. When the sibling publishes a release, every `--locked` gate here
//! breaks until the lock is refreshed. Published crates come from crates.io
//! instead. The check reads `cargo metadata`, so `[patch]` entries, which
//! resolve into the same package list, are covered too.

use std::{
    path::{Path, PathBuf},
    process::Command,
};

use serde_json::Value;

/// Every package whose manifest lies outside `workspace_root` and that came
/// from no registry or git source. Each entry names the package and its path.
fn outside_path_dependencies(metadata: &Value) -> Vec<String> {
    let root = PathBuf::from(
        metadata["workspace_root"]
            .as_str()
            .expect("cargo metadata always reports workspace_root"),
    );
    metadata["packages"]
        .as_array()
        .expect("cargo metadata always reports packages")
        .iter()
        // A null source is a path dependency; registry and git packages carry
        // a source string and are allowed wherever cargo cached them.
        .filter(|package| package["source"].is_null())
        .filter_map(|package| {
            let manifest = Path::new(package["manifest_path"].as_str()?);
            (!manifest.starts_with(&root)).then(|| {
                format!(
                    "{} at {}",
                    package["name"].as_str().unwrap_or("?"),
                    manifest.display()
                )
            })
        })
        .collect()
}

#[test]
fn no_dependency_resolves_to_a_path_outside_the_repository() {
    let output = Command::new(env!("CARGO"))
        .args(["metadata", "--format-version", "1", "--locked", "--offline"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .expect("cargo metadata runs");
    assert!(
        output.status.success(),
        "cargo metadata failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let metadata: Value = serde_json::from_slice(&output.stdout).expect("metadata is JSON");
    let outside = outside_path_dependencies(&metadata);
    assert!(
        outside.is_empty(),
        "path dependencies outside the repository: {outside:?}; take published crates from crates.io, or pin a git revision"
    );
}

/// The check above passes vacuously if it can't recognise a violation, so this
/// plants one: a path package outside the workspace root, next to an allowed
/// workspace member and an allowed registry package.
#[test]
fn a_planted_outside_path_dependency_is_refused() {
    let metadata = serde_json::json!({
        "workspace_root": "/repo",
        "packages": [
            {"name": "member", "source": null, "manifest_path": "/repo/crates/member/Cargo.toml"},
            {"name": "published", "source": "registry+https://github.com/rust-lang/crates.io-index",
             "manifest_path": "/home/u/.cargo/registry/src/published/Cargo.toml"},
            {"name": "sibling", "source": null, "manifest_path": "/sibling/crates/sibling/Cargo.toml"}
        ]
    });
    assert_eq!(
        outside_path_dependencies(&metadata),
        vec!["sibling at /sibling/crates/sibling/Cargo.toml".to_string()]
    );
}

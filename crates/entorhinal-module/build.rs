//! Embeds release-build git provenance so the daemon can identify the source
//! running in production without making ordinary development builds depend on git.
//!
//! Release builds set `ENTORHINAL_BUILD_REV` (the 40-character HEAD commit) and
//! `ENTORHINAL_BUILD_TREE` (`clean` or `dirty`) when git is available. Debug
//! builds do not invoke git and intentionally leave both unset.

use std::path::Path;
use std::process::Command;

fn main() {
    println!("cargo:rerun-if-changed=build.rs");
    if std::env::var("PROFILE").ok().as_deref() != Some("release") {
        return;
    }

    // Cargo treats a missing watched path as changed on every invocation. This
    // keeps every release stamp current even when tracked source edits do not
    // touch the Git index.
    let out_dir = std::env::var_os("OUT_DIR").expect("Cargo sets OUT_DIR for build scripts");
    let always_restamp = Path::new(&out_dir).join("provenance-always-restamp");
    println!("cargo:rerun-if-changed={}", always_restamp.display());

    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let Some(revision) = git(&manifest_dir, &["rev-parse", "HEAD"]) else {
        return;
    };
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        return;
    }
    // A dirty tree runs code HEAD does not describe, so the tree state travels
    // with the revision and the provenance helper declines to attest it.
    // `--no-optional-locks` keeps this read from taking the index lock to
    // refresh stat data, so provenance checks do not write to the checkout.
    let Some(status) = git(
        &manifest_dir,
        &["--no-optional-locks", "status", "--porcelain"],
    ) else {
        return;
    };
    let tree = if status.is_empty() { "clean" } else { "dirty" };
    println!("cargo:rustc-env=ENTORHINAL_BUILD_REV={revision}");
    println!("cargo:rustc-env=ENTORHINAL_BUILD_TREE={tree}");
}

fn git(dir: &str, args: &[&str]) -> Option<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(dir)
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8(output.stdout).ok()?.trim().to_string())
}

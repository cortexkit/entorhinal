//! Embeds the git revision this binary was built from, so the daemon's
//! `ck provenance entorhinal` can say which source is running.
//!
//! Two variables, both optional: `ENTORHINAL_BUILD_REV` (the 40-character HEAD
//! commit) and `ENTORHINAL_BUILD_TREE` (`clean` or `dirty`). They are left unset
//! when git is unavailable, as in a crates.io source tarball, and the module
//! then declares the revision absent with a reason rather than a placeholder
//! that would compare equal between two unidentified builds.

use std::path::Path;
use std::process::Command;

fn main() {
    let manifest_dir = std::env::var("CARGO_MANIFEST_DIR").unwrap_or_default();
    let git_dir = Path::new(&manifest_dir).join("../../.git");
    if git_dir.is_dir() {
        println!("cargo:rerun-if-changed={}", git_dir.join("HEAD").display());
        println!("cargo:rerun-if-changed={}", git_dir.join("refs").display());
    }
    let Some(revision) = git(&manifest_dir, &["rev-parse", "HEAD"]) else {
        return;
    };
    if revision.len() != 40 || !revision.bytes().all(|b| b.is_ascii_hexdigit()) {
        return;
    }
    // A dirty tree runs code HEAD does not describe, so the tree state travels
    // with the revision and the provenance helper declines to attest it.
    // `--no-optional-locks` keeps this read from taking the index lock to
    // refresh stat data: a build killed mid-script would otherwise leave a
    // stale `index.lock` that blocks every later `git add` in the checkout. The
    // answer is the same either way; only the cached stat data isn't saved.
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

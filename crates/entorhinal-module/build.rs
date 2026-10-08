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
    watch_git_state(&manifest_dir);
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

/// Reruns this script exactly when the stamped revision or tree state can
/// change: HEAD moves, the current branch's ref moves, refs are packed, or the
/// index changes. Paths come from `git rev-parse --git-path`, because in a git
/// worktree `.git` is a file and the real HEAD and index live elsewhere; a
/// hard-coded `.git/HEAD` there names nothing. Only files that exist are
/// named: cargo treats a missing watched path as always changed and would
/// rebuild this crate on every run. The shared `refs` directory is not
/// watched, since another branch's commit would rebuild this one for nothing.
fn watch_git_state(dir: &str) {
    let mut paths = vec!["HEAD".to_string(), "packed-refs".into(), "index".into()];
    if let Some(branch) = git(dir, &["symbolic-ref", "-q", "HEAD"]) {
        paths.push(branch);
    }
    for path in paths {
        let Some(resolved) = git(dir, &["rev-parse", "--git-path", &path]) else {
            continue;
        };
        let resolved = Path::new(dir).join(resolved);
        if resolved.is_file() {
            println!("cargo:rerun-if-changed={}", resolved.display());
        }
    }
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

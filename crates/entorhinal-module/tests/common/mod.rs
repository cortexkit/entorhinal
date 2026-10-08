use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// Gives a test-run binary a name that can't be mistaken for a placed fleet
/// binary: `ckdev-<name>` instead of `ck-<name>`.
///
/// The link lives in cargo's per-target scratch directory
/// (`CARGO_TARGET_TMPDIR`), which is always on the same filesystem as the
/// built binary, so a hard link works and nothing is written. That matters on
/// Linux: executing a file another thread has just written can fail with
/// "text file busy" (ETXTBSY), because a process forked by a parallel test can
/// briefly inherit the writing descriptor. A per-test directory under the
/// system temp dir was often on another filesystem, which forced a copy and
/// hit exactly that race.
///
/// Every caller in a run shares one fixed path per binary, so links don't pile
/// up across runs and keep old binaries alive. Each call publishes the link by
/// atomic rename, and a concurrent exec sees either the old or the new link,
/// both the same build in practice.
pub fn ckdev_binary(source: impl AsRef<Path>) -> PathBuf {
    let source = source.as_ref();
    let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("ck-"))
        .expect("binary source name is UTF-8 and starts with ck-");
    let dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("ckdev");
    std::fs::create_dir_all(&dir).expect("create the ckdev link directory");
    let destination = dir.join(format!("ckdev-{name}"));

    static NEXT: AtomicU64 = AtomicU64::new(0);
    let staging = dir.join(format!(
        ".ckdev-{name}.{}.{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    if std::fs::hard_link(source, &staging).is_err() {
        // Only on a filesystem without hard links. The copy is complete and
        // closed before the rename publishes it, and it happens before this
        // caller execs it.
        std::fs::copy(source, &staging).unwrap_or_else(|error| {
            panic!(
                "hard-link or copy {} to {}: {error}",
                source.display(),
                staging.display()
            )
        });
    }
    std::fs::rename(&staging, &destination).expect("publish the ckdev link");
    // When the destination is already a link to the same file (every call
    // after the first in a build), POSIX rename succeeds without doing
    // anything and leaves the staging name in place. Remove it, so staging
    // links don't accumulate and keep old builds alive.
    let _ = std::fs::remove_file(&staging);
    destination
}

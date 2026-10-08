use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::UNIX_EPOCH;

/// Gives a test-run binary a name that can't be mistaken for a placed fleet
/// binary: `ckdev-<name>` instead of `ck-<name>`.
///
/// The binary is a copy, made once per build and reused by every later call:
/// `<CARGO_TARGET_TMPDIR>/ckdev/<name>-<size>-<mtime>/ckdev-<name>`. Two
/// hazards shape this:
/// - On macOS under heavy load, binaries spawned through a fresh hard link to
///   cargo's output were killed with SIGKILL and no log entry (measured in
///   other fleet repos); a copy beside the build output was not.
/// - On Linux, executing a file that is open for writing fails with "text file
///   busy" (ETXTBSY), and a process forked by a parallel test briefly inherits
///   every descriptor this process holds. So the copy is made by a separate
///   `cp` process: this test process never holds a write handle on the file,
///   and once `cp` exits nobody does.
///
/// The copy is published by atomic rename, so a caller sees either no file or
/// a complete one. Copies from earlier builds are removed when a new build
/// publishes, so they don't accumulate in `target/`.
pub fn ckdev_binary(source: impl AsRef<Path>) -> PathBuf {
    let source = source.as_ref();
    let name = source
        .file_name()
        .and_then(|name| name.to_str())
        .and_then(|name| name.strip_prefix("ck-"))
        .expect("binary source name is UTF-8 and starts with ck-");
    let metadata = std::fs::metadata(source)
        .unwrap_or_else(|error| panic!("stat {}: {error}", source.display()));
    let built_at = metadata
        .modified()
        .ok()
        .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
        .map(|age| age.as_nanos())
        .expect("the built binary has a modification time");
    // Size and modification time identify one build of the binary: every
    // rebuild rewrites the file and moves its time.
    let build = format!("{name}-{}-{built_at}", metadata.len());
    let root = Path::new(env!("CARGO_TARGET_TMPDIR")).join("ckdev");
    let dir = root.join(&build);
    let destination = dir.join(format!("ckdev-{name}"));
    if destination.exists() {
        return destination;
    }

    std::fs::create_dir_all(&dir).expect("create the ckdev copy directory");
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let staging = dir.join(format!(
        ".ckdev-{name}.{}.{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    copy_in_another_process(source, &staging);
    // Two callers may race to publish the same build. Both copies are
    // complete and identical, and the rename makes either one visible whole.
    std::fs::rename(&staging, &destination).expect("publish the ckdev copy");

    // Best effort: a copy from an earlier build is only ever used by that
    // build's own test run, which has finished by the time cargo has rebuilt.
    if let Ok(entries) = std::fs::read_dir(&root) {
        for entry in entries.flatten() {
            let file_name = entry.file_name();
            let Some(other) = file_name.to_str() else {
                continue;
            };
            if other != build && other.starts_with(&format!("{name}-")) {
                let _ = std::fs::remove_dir_all(entry.path());
            }
        }
    }
    destination
}

#[cfg(unix)]
fn copy_in_another_process(source: &Path, staging: &Path) {
    let status = std::process::Command::new("cp")
        .arg(source)
        .arg(staging)
        .status()
        .unwrap_or_else(|error| panic!("run cp: {error}"));
    assert!(
        status.success(),
        "cp {} {} failed: {status}",
        source.display(),
        staging.display()
    );
}

// Windows has no "text file busy" race (a running image blocks writes to the
// file itself, not to a new copy), so an in-process copy is safe there.
#[cfg(not(unix))]
fn copy_in_another_process(source: &Path, staging: &Path) {
    std::fs::copy(source, staging).unwrap_or_else(|error| {
        panic!(
            "copy {} to {}: {error}",
            source.display(),
            staging.display()
        )
    });
}

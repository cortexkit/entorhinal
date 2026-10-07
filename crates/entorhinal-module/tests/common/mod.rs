use std::path::{Path, PathBuf};

/// Gives test-run binaries a name that cannot be mistaken for a fleet binary.
/// Hard links avoid copying the signed executable on the usual same-volume path.
pub fn ckdev_binary(source: impl AsRef<Path>, scratch_dir: impl AsRef<Path>) -> PathBuf {
    let source = source.as_ref();
    let source_name = source
        .file_name()
        .and_then(|name| name.to_str())
        .expect("binary source has a UTF-8 file name");
    let name = source_name
        .strip_prefix("ck-")
        .expect("binary source name starts with ck-");
    let destination = scratch_dir.as_ref().join(format!("ckdev-{name}"));

    if std::fs::hard_link(source, &destination).is_err() {
        std::fs::copy(source, &destination).unwrap_or_else(|error| {
            panic!(
                "hard-link or copy {} to {}: {error}",
                source.display(),
                destination.display()
            )
        });
    }
    destination
}

use std::{fs, io::ErrorKind, path::PathBuf};

/// Declare this guard after every store, connection and task owning the files.
/// Rust drops fields in declaration order, so cleanup then runs after Windows
/// file handles and SQLite leases have closed, including during unwinding.
pub struct ScratchCleanup(pub PathBuf);

impl Drop for ScratchCleanup {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            if error.kind() == ErrorKind::NotFound {
                return;
            }
            if std::thread::panicking() {
                eprintln!("remove scratch directory {}: {error}", self.0.display());
            } else {
                panic!("remove scratch directory {}: {error}", self.0.display());
            }
        }
    }
}

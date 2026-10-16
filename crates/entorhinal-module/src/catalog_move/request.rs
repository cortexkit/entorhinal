//! Admit one transfer request per move ID without making status or cancellation
//! requests wait for its network I/O.
use super::*;
use std::collections::BTreeSet;
use std::sync::LazyLock;

static ACTIVE: LazyLock<Mutex<BTreeSet<PathBuf>>> = LazyLock::new(Mutex::default);
static WRITING: LazyLock<Mutex<BTreeSet<PathBuf>>> = LazyLock::new(Mutex::default);
pub(super) struct CheckpointWrite(PathBuf);
impl CheckpointWrite {
    pub fn take(path: PathBuf) -> Result<Self, HandlerError> {
        if !WRITING.lock().unwrap().insert(path.clone()) {
            return Err(unavailable("catalog checkpoint writer is busy"));
        }
        Ok(Self(path))
    }
}
impl Drop for CheckpointWrite {
    fn drop(&mut self) {
        WRITING.lock().unwrap().remove(&self.0);
    }
}
pub(super) struct Admission(PathBuf);
impl Admission {
    pub fn take(path: PathBuf) -> Result<Self, HandlerError> {
        if !ACTIVE.lock().unwrap().insert(path.clone()) {
            return Err(unavailable("catalog cut already has an active request"));
        }
        Ok(Self(path))
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        ACTIVE.lock().unwrap().remove(&self.0);
    }
}

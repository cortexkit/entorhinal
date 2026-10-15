use super::*;
use std::io::Write;

pub(super) fn path(folder: &Path, registry: &str, scope: &str, archive_sha256: &str) -> PathBuf {
    let identity =
        serde_json::to_vec(&(registry, scope, archive_sha256)).expect("strings serialize");
    folder
        .join("catalog")
        .join("publishes")
        .join(format!("{}.json", catalog::sha256_hex(&identity)))
}
pub(super) fn load(path: &Path) -> Result<Option<Signed>, HandlerError> {
    let file = match std::fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(storage(e)),
    };
    use std::io::Read;
    let mut bytes = Vec::new();
    file.take(1_048_577)
        .read_to_end(&mut bytes)
        .map_err(storage)?;
    if bytes.len() > 1_048_576 {
        return Err(error("record_invalid", "stored publish exceeds cap"));
    }
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| error("record_invalid", e.to_string()))
}
pub(super) fn save(path: &Path, signed: &Signed) -> Result<(), HandlerError> {
    let folder = path
        .parent()
        .ok_or_else(|| storage("missing publish folder"))?;
    std::fs::create_dir_all(folder).map_err(storage)?;
    let mut random = [0u8; 16];
    getrandom::getrandom(&mut random).map_err(storage)?;
    let temp = folder.join(format!(".{}.tmp", catalog::hex(&random)));
    let result = (|| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .map_err(storage)?;
        file.write_all(&serde_json::to_vec(signed).map_err(storage)?)
            .map_err(storage)?;
        file.sync_all().map_err(storage)?;
        std::fs::rename(&temp, path).map_err(storage)?;
        #[cfg(unix)]
        std::fs::File::open(folder)
            .map_err(storage)?
            .sync_all()
            .map_err(storage)?;
        Ok(())
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(temp);
    }
    result
}

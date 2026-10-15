use super::*;
use std::io::Read;

pub(super) fn pack(directory: &Path) -> Result<Vec<u8>, HandlerError> {
    fn walk(
        root: &Path,
        dir: &Path,
        files: &mut Vec<(String, Vec<u8>)>,
        total: &mut usize,
    ) -> Result<(), HandlerError> {
        let mut entries = std::fs::read_dir(dir)
            .map_err(storage)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(storage)?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let metadata = std::fs::symlink_metadata(entry.path()).map_err(storage)?;
            if metadata.is_dir() {
                walk(root, &entry.path(), files, total)?;
            } else if metadata.is_file() {
                let path = entry.path();
                let relative = path.strip_prefix(root).map_err(storage)?;
                let name = relative
                    .to_str()
                    .ok_or_else(|| error("archive_invalid", "non-Unicode member name"))?
                    .replace('\\', "/");
                catalog::archive::check_path(&name).map_err(|e| {
                    let code = match e {
                        catalog::archive::ArchiveRule::PathInvalid => "path_invalid",
                        catalog::archive::ArchiveRule::PathTooLong => "path_too_long",
                        _ => "path_not_ustar",
                    };
                    error(code, format!("invalid archive path: {name}"))
                })?;
                // Bound allocation even when a file grows between metadata and read.
                let limit = if name == "ck.json" {
                    catalog::archive::MANIFEST_MAX
                } else {
                    catalog::archive::ARCHIVE_MAX.saturating_sub(*total)
                };
                let mut bytes = Vec::new();
                std::fs::File::open(&path)
                    .map_err(storage)?
                    .take(limit as u64 + 1)
                    .read_to_end(&mut bytes)
                    .map_err(storage)?;
                if bytes.len() > limit {
                    return Err(error(
                        if name == "ck.json" {
                            "manifest_too_large"
                        } else {
                            "archive_too_large"
                        },
                        "package exceeds cap",
                    ));
                }
                *total = total.saturating_add(bytes.len() + 512);
                if *total > catalog::archive::ARCHIVE_MAX {
                    return Err(error("archive_too_large", "package exceeds cap"));
                }
                files.push((name, bytes));
            } else {
                return Err(error(
                    "archive_invalid",
                    "only regular files and directories may be packed",
                ));
            }
        }
        Ok(())
    }
    let mut files = Vec::new();
    walk(directory, directory, &mut files, &mut 1024)?;
    files.sort_by(|a, b| {
        (a.0 != "ck.json", a.0.as_bytes()).cmp(&(b.0 != "ck.json", b.0.as_bytes()))
    });
    let members = files
        .iter()
        .map(|(name, bytes)| catalog::archive::Member::regular(name, bytes))
        .collect::<Vec<_>>();
    catalog::archive::write(&members).map_err(|e| error(e.code.as_str(), e.to_string()))
}

//! Machine-local content-addressed package bytes; SQLite stores only catalog metadata.
use base64::Engine;
use entorhinal_catalog::{is_hex, sha256_hex};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

pub const CHUNK_LIMIT: u64 = 1_048_576;

#[derive(Debug)]
pub struct CatalogError {
    pub code: &'static str,
    pub message: String,
    pub detail: Option<serde_json::Value>,
}
impl CatalogError {
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            detail: None,
        }
    }
}
impl std::fmt::Display for CatalogError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}
impl std::error::Error for CatalogError {}
impl From<std::io::Error> for CatalogError {
    fn from(error: std::io::Error) -> Self {
        Self::new("blob_io", error.to_string())
    }
}

#[derive(Clone)]
pub struct BlobStore {
    root: PathBuf,
}
impl BlobStore {
    pub fn new(store_folder: &Path) -> Result<Self, CatalogError> {
        let catalog = store_folder.join("catalog");
        let root = catalog.join("blobs");
        for path in [&catalog, &root] {
            fs::create_dir_all(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
            }
        }
        Ok(Self { root })
    }
    fn path(&self, digest: &str) -> Result<PathBuf, CatalogError> {
        if !is_hex(digest, 64) {
            return Err(CatalogError::new(
                "blob_request_invalid",
                "sha256 must be 64 lower-case hex digits",
            ));
        }
        Ok(self.root.join(digest))
    }
    // Package-signature and archive validation belong to the caller; this method
    // only atomically publishes bytes under their computed content address.
    #[allow(dead_code)]
    pub fn publish(&self, bytes: &[u8]) -> Result<String, CatalogError> {
        self.publish_before_rename(bytes, || Ok(()))
    }
    fn publish_before_rename(
        &self,
        bytes: &[u8],
        before_rename: impl FnOnce() -> Result<(), CatalogError>,
    ) -> Result<String, CatalogError> {
        let digest = sha256_hex(bytes);
        let destination = self.path(&digest)?;
        if destination.exists() {
            self.read_verified(&digest)?;
            return Ok(digest);
        }
        let mut random = [0u8; 16];
        getrandom::getrandom(&mut random)
            .map_err(|e| CatalogError::new("blob_io", e.to_string()))?;
        let temporary = self
            .root
            .join(format!(".{}.tmp", entorhinal_catalog::hex(&random)));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.write(true).create_new(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&temporary)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            before_rename()?;
            fs::rename(&temporary, &destination)?;
            #[cfg(unix)]
            fs::File::open(&self.root)?.sync_all()?;
            Ok::<_, CatalogError>(())
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temporary);
        }
        result?;
        Ok(digest)
    }
    pub fn read_verified(&self, digest: &str) -> Result<Vec<u8>, CatalogError> {
        let path = self.path(digest)?;
        let metadata = fs::symlink_metadata(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                CatalogError::new("blob_unknown", "unknown digest")
            } else {
                error.into()
            }
        })?;
        if !metadata.file_type().is_file() {
            return Err(CatalogError::new("blob_io", "blob is not a regular file"));
        }
        let file = fs::File::open(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                CatalogError::new("blob_unknown", "unknown digest")
            } else {
                error.into()
            }
        })?;
        let mut bytes = Vec::new();
        file.take(10_485_761).read_to_end(&mut bytes)?;
        if bytes.len() > 10_485_760 {
            return Err(CatalogError::new(
                "blob_corrupt",
                "blob exceeds the archive cap",
            ));
        }
        if sha256_hex(&bytes) != digest {
            return Err(CatalogError::new(
                "blob_corrupt",
                format!("corrupted blob {digest}"),
            ));
        }
        Ok(bytes)
    }
    pub fn read_chunk(
        &self,
        digest: &str,
        offset: u64,
        length: u64,
    ) -> Result<serde_json::Value, CatalogError> {
        if length == 0 || length > CHUNK_LIMIT || offset.checked_add(length).is_none() {
            return Err(CatalogError::new(
                "blob_request_invalid",
                "length must be 1..=1 MiB and the range must not overflow",
            ));
        }
        let bytes = self.read_verified(digest)?;
        let start = usize::try_from(offset)
            .ok()
            .filter(|s| *s <= bytes.len())
            .ok_or_else(|| {
                CatalogError::new("blob_request_invalid", "offset exceeds blob length")
            })?;
        let end = start.saturating_add(length as usize).min(bytes.len());
        Ok(
            serde_json::json!({"bytes": base64::engine::general_purpose::STANDARD.encode(&bytes[start..end]), "offset": offset, "total": bytes.len()}),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn blob_roundtrip_and_chunks() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-blob");
        let store = BlobStore::new(&dir).unwrap();
        let bytes = vec![42; CHUNK_LIMIT as usize + 3];
        let digest = store.publish(&bytes).unwrap();
        assert_eq!(store.read_verified(&digest).unwrap(), bytes);
        assert_eq!(
            store.read_chunk(&digest, CHUNK_LIMIT, 10).unwrap()["bytes"],
            "Kioq"
        );
        assert!(store.read_chunk(&digest, 0, CHUNK_LIMIT + 1).is_err());
        assert!(store.read_chunk(&digest, u64::MAX, 1).is_err());
        assert_eq!(
            store.read_chunk("../store.db", 0, 1).unwrap_err().code,
            "blob_request_invalid"
        );
        assert!(store.read_chunk(&digest, 0, 0).is_err());
        assert!(store.read_chunk(&digest, u64::MAX, CHUNK_LIMIT).is_err());
        assert_eq!(
            store.read_chunk(&digest, bytes.len() as u64, 1).unwrap()["bytes"],
            ""
        );
        assert_eq!(store.publish(&bytes).unwrap(), digest);
        assert_eq!(
            BlobStore::new(&dir)
                .unwrap()
                .read_verified(&digest)
                .unwrap(),
            bytes
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            assert_eq!(
                fs::metadata(&store.root).unwrap().permissions().mode() & 0o777,
                0o700
            );
            assert_eq!(
                fs::metadata(dir.join("catalog"))
                    .unwrap()
                    .permissions()
                    .mode()
                    & 0o777,
                0o700
            );
        }
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn failed_blob_publication_leaves_no_digest_or_partial_file() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-atomic");
        let store = BlobStore::new(&dir).unwrap();
        assert!(store
            .publish_before_rename(b"unfinished", || Err(CatalogError::new(
                "blob_io",
                "injected pre-rename failure"
            )))
            .is_err());
        assert!(fs::read_dir(&store.root).unwrap().next().is_none());
        assert_eq!(
            store
                .read_chunk(&sha256_hex(b"unfinished"), 0, 1)
                .unwrap_err()
                .code,
            "blob_unknown"
        );
        let digest = store.publish(b"finished").unwrap();
        assert_eq!(
            BlobStore::new(&dir)
                .unwrap()
                .read_verified(&digest)
                .unwrap(),
            b"finished"
        );
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn oversized_stored_file_is_bounded_and_refused() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-large-store");
        let store = BlobStore::new(&dir).unwrap();
        let bytes = vec![0; 10_485_761];
        let digest = sha256_hex(&bytes);
        fs::write(store.path(&digest).unwrap(), bytes).unwrap();
        assert_eq!(
            store.read_verified(&digest).unwrap_err().code,
            "blob_corrupt"
        );
        fs::remove_dir_all(dir).unwrap();
    }
    #[cfg(unix)]
    #[test]
    fn symlink_blob_is_not_a_verified_regular_file() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-link");
        let store = BlobStore::new(&dir).unwrap();
        let digest = sha256_hex(b"target");
        fs::write(dir.join("outside"), b"target").unwrap();
        std::os::unix::fs::symlink(dir.join("outside"), store.path(&digest).unwrap()).unwrap();
        assert_eq!(store.read_verified(&digest).unwrap_err().code, "blob_io");
        fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn corrupted_blob_is_refused() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-corrupt");
        let store = BlobStore::new(&dir).unwrap();
        let digest = store.publish(b"original").unwrap();
        fs::write(store.path(&digest).unwrap(), b"corrupted").unwrap();
        assert_eq!(
            store.read_chunk(&digest, 0, 1).unwrap_err().code,
            "blob_corrupt"
        );
        assert_eq!(store.publish(b"original").unwrap_err().code, "blob_corrupt");
        fs::remove_dir_all(dir).unwrap();
    }
}

//! Deterministic, bounded framing for the package bytes owned by a move.
use super::{invalid, CatalogPin, HandlerError};
use crate::catalog_blob::BlobStore;
use entorhinal_catalog::{archive, hex, sha256_hex};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{Read, Write},
    path::Path,
};

pub(super) const MAGIC: &[u8] = b"ck-package-closure/v1\n";
pub(super) const BLOB_CAP: u64 = 10_485_760;
/// The session door's per-call cap on raw bytes, for both `session.admit`
/// `bytes` and `session.read` `limit`. The door carries bytes hex-encoded in
/// JSON, so one full call is about 2 MiB on the wire, inside the daemon's
/// 4 MiB frame limit. Larger package files are sent and read in chunks.
pub(super) const ADMIT_CAP: usize = 1_048_576;
pub(super) fn digests(pins: &[CatalogPin]) -> BTreeSet<String> {
    pins.iter()
        .flat_map(|p| [p.manifest_sha256.clone(), p.archive_sha256.clone()])
        .collect()
}
pub(super) fn unhex(text: &str) -> Result<Vec<u8>, HandlerError> {
    if !text.len().is_multiple_of(2)
        || !text
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("invalid hexadecimal bytes"));
    }
    text.as_bytes()
        .as_chunks::<2>()
        .0
        .iter()
        .map(|s| {
            u8::from_str_radix(std::str::from_utf8(s).unwrap(), 16)
                .map_err(|_| invalid("invalid hex"))
        })
        .collect()
}
fn blob(store: &BlobStore, pins: &[CatalogPin], digest: &str) -> Result<Vec<u8>, HandlerError> {
    match store.read_verified(digest) {
        Ok(bytes) => Ok(bytes),
        Err(error) if error.code == "blob_unknown" => {
            let pin = pins
                .iter()
                .find(|p| p.manifest_sha256 == digest)
                .ok_or_else(|| HandlerError::new("unavailable", "pinned blob missing"))?;
            let bytes = store.read_verified(&pin.archive_sha256)?;
            let members = archive::read(&bytes).map_err(|e| invalid(e.to_string()))?;
            let manifest = members
                .get("ck.json")
                .ok_or_else(|| invalid("archive has no manifest"))?
                .to_vec();
            if sha256_hex(&manifest) != digest {
                return Err(invalid("manifest hash differs from pin"));
            }
            Ok(manifest)
        }
        Err(e) => Err(e.into()),
    }
}
pub(super) fn encode(
    path: &Path,
    store: &BlobStore,
    pins: &[CatalogPin],
) -> Result<(u64, String), HandlerError> {
    let mut file = File::create(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut length = 0;
    let mut write = |bytes: &[u8]| -> Result<(), HandlerError> {
        file.write_all(bytes).map_err(io)?;
        hash.update(bytes);
        length += bytes.len() as u64;
        Ok(())
    };
    let wanted = digests(pins);
    write(MAGIC)?;
    write(&(wanted.len() as u32).to_be_bytes())?;
    for digest in wanted {
        let bytes = blob(store, pins, &digest)?;
        write(&unhex(&digest)?)?;
        write(&(bytes.len() as u64).to_be_bytes())?;
        write(&bytes)?;
    }
    file.sync_all().map_err(io)?;
    Ok((length, hex(&hash.finalize())))
}
pub(super) fn io(error: std::io::Error) -> HandlerError {
    HandlerError::new("unavailable", error.to_string())
}
fn exact<const N: usize>(reader: &mut impl Read) -> Result<[u8; N], HandlerError> {
    let mut bytes = [0; N];
    reader
        .read_exact(&mut bytes)
        .map_err(|_| invalid("truncated package closure"))?;
    Ok(bytes)
}
/// Verify every frame before the caller publishes any staged content.
pub(super) fn decode(path: &Path, staging: &Path, pins: &[CatalogPin]) -> Result<(), HandlerError> {
    let mut reader = File::open(path).map_err(io)?;
    if exact::<22>(&mut reader)?.as_slice() != MAGIC {
        return Err(invalid("bad package closure magic"));
    }
    let wanted = digests(pins);
    let count = u32::from_be_bytes(exact(&mut reader)?) as usize;
    if count != wanted.len() || count > 512 {
        return Err(invalid("package closure count differs from pins"));
    }
    let mut previous = None;
    for _ in 0..count {
        let digest = exact::<32>(&mut reader)?;
        if previous.is_some_and(|p| p >= digest) {
            return Err(invalid("package digests are not strictly ascending"));
        }
        previous = Some(digest);
        let digest = hex(&digest);
        let length = u64::from_be_bytes(exact(&mut reader)?);
        if length > BLOB_CAP {
            return Err(invalid("package blob exceeds archive cap"));
        }
        let mut bytes = vec![0; length as usize];
        reader
            .read_exact(&mut bytes)
            .map_err(|_| invalid("truncated package blob"))?;
        if sha256_hex(&bytes) != digest {
            return Err(invalid("package blob hash differs from pin"));
        }
        if !wanted.contains(&digest) {
            return Err(invalid("package blob is not pinned"));
        }
        let mut file = File::create(staging.join(digest)).map_err(io)?;
        file.write_all(&bytes).map_err(io)?;
        file.sync_all().map_err(io)?;
    }
    let mut tail = [0];
    if reader.read(&mut tail).map_err(io)? != 0 {
        return Err(invalid("trailing package closure bytes"));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    const A: &str = "ca978112ca1bbdcafac231b39a23dc4da786eff8147c4e72b9807785afee48bb";
    const B: &str = "3e23e8160039594a33894f6564e1b1348bbd7a0088d42c4acb73eeaed59c009d";
    fn pin() -> CatalogPin {
        CatalogPin {
            name: "@test/a".into(),
            version: "1.0.0".into(),
            kind: entorhinal_catalog::Kind::Skill,
            manifest_sha256: A.into(),
            archive_sha256: B.into(),
            publisher_fingerprint: "00".repeat(32),
            root: true,
            range: None,
        }
    }
    fn golden() -> Vec<u8> {
        let mut bytes = b"ck-package-closure/v1\n\0\0\0\x02".to_vec();
        bytes.extend(unhex(B).unwrap());
        bytes.extend([0, 0, 0, 0, 0, 0, 0, 1]);
        bytes.push(b'b');
        bytes.extend(unhex(A).unwrap());
        bytes.extend([0, 0, 0, 0, 0, 0, 0, 1]);
        bytes.push(b'a');
        bytes
    }
    fn refuses(bytes: Vec<u8>, pins: Vec<CatalogPin>, message: &str) {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-move-frame");
        fs_setup(&dir, &bytes);
        let error = decode(&dir.join("payload"), &dir.join("staging"), &pins).unwrap_err();
        assert_eq!(error.code, "digest_mismatch");
        assert_eq!(error.message, message);
        std::fs::remove_dir_all(dir).unwrap();
    }
    fn fs_setup(dir: &Path, bytes: &[u8]) {
        std::fs::create_dir_all(dir.join("staging")).unwrap();
        std::fs::write(dir.join("payload"), bytes).unwrap();
    }
    #[test]
    fn package_closure_binary_golden_two_blobs() {
        let (dir, _) = crate::tests::scratch_descriptor("catalog-move-golden");
        let store = BlobStore::new(&dir).unwrap();
        assert_eq!(store.publish(b"a").unwrap(), A);
        assert_eq!(store.publish(b"b").unwrap(), B);
        let (length, digest) = encode(&dir.join("payload"), &store, &[pin()]).unwrap();
        let expected = golden();
        assert_eq!(std::fs::read(dir.join("payload")).unwrap(), expected);
        assert_eq!(length, 108);
        assert_eq!(digest, sha256_hex(&expected));
        std::fs::create_dir(dir.join("staging")).unwrap();
        decode(&dir.join("payload"), &dir.join("staging"), &[pin()]).unwrap();
        assert_eq!(std::fs::read(dir.join("staging").join(A)).unwrap(), b"a");
        assert_eq!(std::fs::read(dir.join("staging").join(B)).unwrap(), b"b");
        std::fs::remove_dir_all(dir).unwrap();
    }
    #[test]
    fn package_closure_bad_magic() {
        let mut b = golden();
        b[0] = b'x';
        refuses(b, vec![pin()], "bad package closure magic");
    }
    #[test]
    fn package_closure_bad_count() {
        let mut b = golden();
        b[25] = 1;
        refuses(b, vec![pin()], "package closure count differs from pins");
    }
    #[test]
    fn package_closure_duplicate_digest() {
        let mut b = golden();
        let d = b[26..58].to_vec();
        b[67..99].copy_from_slice(&d);
        refuses(b, vec![pin()], "package digests are not strictly ascending");
    }
    #[test]
    fn package_closure_nonascending_digest() {
        let mut b = golden();
        let first = b[26..67].to_vec();
        let second = b[67..108].to_vec();
        b[26..67].copy_from_slice(&second);
        b[67..108].copy_from_slice(&first);
        refuses(b, vec![pin()], "package digests are not strictly ascending");
    }
    #[test]
    fn package_closure_oversized_length() {
        let mut b = golden();
        b[58..66].copy_from_slice(&(BLOB_CAP + 1).to_be_bytes());
        refuses(b, vec![pin()], "package blob exceeds archive cap");
    }
    #[test]
    fn package_closure_hash_mismatch() {
        let mut b = golden();
        b[66] = b'x';
        refuses(b, vec![pin()], "package blob hash differs from pin");
    }
    #[test]
    fn package_closure_trailing_bytes() {
        let mut b = golden();
        b.push(0);
        refuses(b, vec![pin()], "trailing package closure bytes");
    }
    #[test]
    fn package_closure_wrong_pinned_set() {
        let mut p = pin();
        p.archive_sha256 = "00".repeat(32);
        refuses(golden(), vec![p], "package blob is not pinned");
    }
    #[test]
    fn package_closure_truncated_blob() {
        let mut b = golden();
        b.pop();
        refuses(b, vec![pin()], "truncated package blob");
    }
}

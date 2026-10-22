//! The bytes of a session-bindings stream: deterministic, bounded and
//! self-describing.
//!
//! ```text
//! ck-session-bindings/v1\n
//! {"agent_id":"…","count":N}\n
//! N lines, one CarriedBinding as JSON each, in strictly ascending (ref_kind, ref)
//! ```
//!
//! The first line names the format. A receiver that meets another version of
//! it refuses `incompatible_version` naming both, and anything else in that
//! place is corrupt. Every JSON line decodes strictly (unknown fields are
//! refused), and the whole stream is hashed with SHA-256: the digest goes in
//! the cut item and the receiver checks it while reading the bytes back.
use super::invalid;
use crate::HandlerError;
use entorhinal_catalog::hex;
use entorhinal_core::session_carry::CarriedBinding;
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use std::{
    fs::File,
    io::{BufRead, BufReader, Read, Write},
    path::Path,
};

pub(super) const MAGIC: &[u8] = b"ck-session-bindings/v1\n";
const MAGIC_PREFIX: &str = "ck-session-bindings/";
/// The most bindings one stream may carry. The shared move record already
/// limits a move to the refs that fit in its 64 KiB body (a few hundred
/// bindings), so this only bounds a stream that is corrupt or hostile.
pub(super) const ROW_CAP: u64 = 16_384;
/// The longest encoded binding. A binding holding the full 1,024-entry
/// network history encodes to about 45 KiB, so this leaves room for long
/// paths and session ids.
pub(super) const LINE_CAP: u64 = 1_048_576;
/// The longest stream: the uploaded file and the bytes read back from the
/// session door both stop here.
pub(super) const STREAM_CAP: u64 = 64 * 1_048_576;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Header {
    agent_id: String,
    count: u64,
}

fn io(error: std::io::Error) -> HandlerError {
    HandlerError::new("unavailable", error.to_string())
}

/// Write the stream for `rows` (already in `(ref_kind, ref)` order) to
/// `path`, returning its length and SHA-256.
pub(super) fn encode(
    path: &Path,
    magic: &[u8],
    agent: &str,
    rows: &[CarriedBinding],
) -> Result<(u64, String), HandlerError> {
    if rows.len() as u64 > ROW_CAP {
        return Err(HandlerError::new(
            "not_exportable",
            format!("the agent has more than {ROW_CAP} session bindings"),
        ));
    }
    let mut file = File::create(path).map_err(io)?;
    let mut hash = Sha256::new();
    let mut length = 0u64;
    let mut write = |bytes: &[u8]| -> Result<(), HandlerError> {
        file.write_all(bytes).map_err(io)?;
        hash.update(bytes);
        length += bytes.len() as u64;
        Ok(())
    };
    write(magic)?;
    let mut line = |value: Vec<u8>| -> Result<(), HandlerError> {
        if value.len() as u64 + 1 > LINE_CAP {
            return Err(HandlerError::new(
                "not_exportable",
                "a session binding is too large to carry",
            ));
        }
        write(&value)?;
        write(b"\n")
    };
    line(
        serde_json::to_vec(&Header {
            agent_id: agent.into(),
            count: rows.len() as u64,
        })
        .expect("a header always serializes"),
    )?;
    for row in rows {
        line(serde_json::to_vec(row).expect("a binding always serializes"))?;
    }
    drop(line);
    if length > STREAM_CAP {
        return Err(HandlerError::new(
            "not_exportable",
            "the agent's session bindings exceed the stream cap",
        ));
    }
    file.sync_all().map_err(io)?;
    Ok((length, hex(&hash.finalize())))
}

/// Read one newline-terminated line of at most `cap` bytes, the newline
/// included, and return it without the newline. `None` at a clean end of
/// the stream.
fn next_line(reader: &mut impl BufRead, cap: u64) -> Result<Option<Vec<u8>>, HandlerError> {
    let mut line = Vec::new();
    let read = reader.take(cap).read_until(b'\n', &mut line).map_err(io)?;
    if read == 0 {
        return Ok(None);
    }
    if line.pop() != Some(b'\n') {
        return Err(invalid("session bindings line is truncated or too long"));
    }
    Ok(Some(line))
}

/// Decode and check a whole stream for `agent` before anything is staged.
/// Refuses `incompatible_version` for another version of this format and
/// `digest_mismatch` for anything malformed.
pub(super) fn decode(path: &Path, agent: &str) -> Result<Vec<CarriedBinding>, HandlerError> {
    let mut reader = BufReader::new(File::open(path).map_err(io)?);
    let first = next_line(&mut reader, 64)?.unwrap_or_default();
    if first != MAGIC[..MAGIC.len() - 1] {
        let text = String::from_utf8_lossy(&first);
        if let Some(version) = text.strip_prefix(MAGIC_PREFIX) {
            let found = format!("session-bindings/{version}");
            return Err(HandlerError {
                code: "incompatible_version".into(),
                message: format!("this Mac reads {}, not {found}", super::FORMAT),
                detail: Some(json!({"found":found,"required":super::FORMAT})),
            });
        }
        return Err(invalid("bad session bindings magic"));
    }
    let header: Header = next_line(&mut reader, LINE_CAP)?
        .ok_or_else(|| invalid("session bindings stream has no header"))
        .and_then(|line| {
            serde_json::from_slice(&line).map_err(|e| invalid(format!("bad header: {e}")))
        })?;
    if header.agent_id != agent {
        return Err(invalid("session bindings belong to another agent"));
    }
    if header.count > ROW_CAP {
        return Err(invalid("session bindings count exceeds its cap"));
    }
    let mut rows: Vec<CarriedBinding> = Vec::with_capacity(header.count as usize);
    for _ in 0..header.count {
        let line = next_line(&mut reader, LINE_CAP)?
            .ok_or_else(|| invalid("session bindings stream is truncated"))?;
        let row: CarriedBinding = serde_json::from_slice(&line)
            .map_err(|e| invalid(format!("bad session binding: {e}")))?;
        row.check().map_err(invalid)?;
        if rows.last().is_some_and(|last| last.key() >= row.key()) {
            return Err(invalid(
                "session bindings are not in strictly ascending ref order",
            ));
        }
        rows.push(row);
    }
    let mut tail = [0];
    if reader.read(&mut tail).map_err(io)? != 0 {
        return Err(invalid("trailing session bindings bytes"));
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use entorhinal_core::NetworkEntry;

    const AGENT: &str = "agent_0123456789abcdef";

    fn row(session: &str, history: Vec<(&str, i64)>) -> CarriedBinding {
        CarriedBinding {
            ref_kind: "broca-session".into(),
            session_ref: format!("ref-{session}"),
            runner: Some("broca".into()),
            harness: Some("broca".into()),
            session_id: Some(session.into()),
            project_root: "/work/project".into(),
            registered_by: "host".into(),
            registered_at_ms: 40,
            ended_at_ms: Some(90),
            network_history: history
                .into_iter()
                .map(|(network, since_ms)| NetworkEntry {
                    network: network.into(),
                    since_ms,
                })
                .collect(),
        }
    }

    fn golden() -> Vec<u8> {
        let mut bytes = b"ck-session-bindings/v1\n".to_vec();
        bytes.extend(br#"{"agent_id":"agent_0123456789abcdef","count":1}"#);
        bytes.push(b'\n');
        bytes.extend(br#"{"ref_kind":"broca-session","ref":"ref-s1","runner":"broca","harness":"broca","session_id":"s1","project_root":"/work/project","registered_by":"host","registered_at_ms":40,"ended_at_ms":90,"network_history":[{"network":"yes","since_ms":100},{"network":"no","since_ms":250}]}"#);
        bytes.push(b'\n');
        bytes
    }

    fn scratch(name: &str) -> std::path::PathBuf {
        let (dir, _) = crate::tests::scratch_descriptor(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn refuses(bytes: Vec<u8>, code: &str, message: &str) {
        let dir = scratch("session-bindings-frame");
        std::fs::write(dir.join("payload"), bytes).unwrap();
        let error = decode(&dir.join("payload"), AGENT).unwrap_err();
        assert_eq!(
            (error.code.as_str(), error.message.as_str()),
            (code, message)
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn session_bindings_stream_golden_round_trip() {
        let dir = scratch("session-bindings-golden");
        let rows = vec![row("s1", vec![("yes", 100), ("no", 250)])];
        let (length, digest) = encode(&dir.join("payload"), MAGIC, AGENT, &rows).unwrap();
        let expected = golden();
        assert_eq!(std::fs::read(dir.join("payload")).unwrap(), expected);
        assert_eq!(length, expected.len() as u64);
        assert_eq!(digest, entorhinal_catalog::sha256_hex(&expected));
        assert_eq!(decode(&dir.join("payload"), AGENT).unwrap(), rows);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn session_bindings_stream_refuses_another_format_version() {
        let mut bytes = golden();
        bytes[21] = b'2';
        let dir = scratch("session-bindings-version");
        std::fs::write(dir.join("payload"), bytes).unwrap();
        let error = decode(&dir.join("payload"), AGENT).unwrap_err();
        assert_eq!(error.code, "incompatible_version");
        assert_eq!(
            error.detail,
            Some(json!({"found":"session-bindings/v2","required":"session-bindings/v1"}))
        );
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn session_bindings_stream_refuses_malformed_bytes() {
        let mut bad_magic = golden();
        bad_magic[0] = b'x';
        refuses(bad_magic, "digest_mismatch", "bad session bindings magic");
        let mut trailing = golden();
        trailing.push(b'x');
        refuses(
            trailing,
            "digest_mismatch",
            "trailing session bindings bytes",
        );
        let mut truncated = golden();
        truncated.pop();
        refuses(
            truncated,
            "digest_mismatch",
            "session bindings line is truncated or too long",
        );
        let other = String::from_utf8(golden())
            .unwrap()
            .replace(AGENT, "agent_other");
        refuses(
            other.into_bytes(),
            "digest_mismatch",
            "session bindings belong to another agent",
        );
        let unknown = String::from_utf8(golden())
            .unwrap()
            .replace(r#""ended_at_ms":90"#, r#""ended_at_ms":90,"extra":1"#);
        assert!(decode_text(unknown).starts_with("bad session binding"));
        let reordered = String::from_utf8(golden()).unwrap().replace(
            r#"{"network":"yes","since_ms":100},{"network":"no","since_ms":250}"#,
            r#"{"network":"no","since_ms":250},{"network":"yes","since_ms":100}"#,
        );
        assert_eq!(
            decode_text(reordered),
            "network history is not in strictly rising time order"
        );
        let doubled = String::from_utf8(golden()).unwrap();
        let line = doubled.lines().nth(2).unwrap().to_owned();
        let doubled = doubled.replace(r#""count":1"#, r#""count":2"#) + &line + "\n";
        assert_eq!(
            decode_text(doubled),
            "session bindings are not in strictly ascending ref order"
        );
    }

    fn decode_text(text: String) -> String {
        let dir = scratch("session-bindings-text");
        std::fs::write(dir.join("payload"), text).unwrap();
        let error = decode(&dir.join("payload"), AGENT).unwrap_err();
        std::fs::remove_dir_all(dir).unwrap();
        assert_eq!(error.code, "digest_mismatch");
        error.message
    }

    /// The largest agent seen on a working Mac had about 1,300 Broca
    /// bindings. With realistic ids, paths and a short network history each,
    /// that stream is well inside one session-door call.
    #[test]
    fn session_bindings_stream_size_for_a_large_agent() {
        let dir = scratch("session-bindings-size");
        let rows: Vec<_> = (0..1300)
            .map(|i| {
                let mut r = row(
                    &format!("0f8fad5b-d9cb-469f-a165-{i:012}"),
                    vec![("unknown", 1_700_000_000_000), ("yes", 1_700_000_100_000)],
                );
                r.session_ref = format!("broca:0f8fad5b-d9cb-469f-a165-{i:012}");
                r.project_root = "/Users/someone/Work/Projects/CortexKit/entorhinal".into();
                r.registered_at_ms = 1_700_000_000_000;
                r
            })
            .collect();
        let (length, _) = encode(&dir.join("payload"), MAGIC, AGENT, &rows).unwrap();
        assert!(length < 1_048_576, "{length}");
        assert_eq!(decode(&dir.join("payload"), AGENT).unwrap().len(), 1300);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

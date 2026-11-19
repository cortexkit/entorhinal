# Hub snapshot v1

`part.json` contains the entry id and canonical plaintext body emitted by the
hub snapshot framer, before the log envelope signs and seals the body. The
sample carries the legacy position, an excluded box, and roles held by a box
and an unmapped legacy author. `chunk` is standard base64 of the canonical
snapshot bytes, and `snapshot_digest` is SHA-256 over those bytes. Multipart
snapshots concatenate chunks by zero-based `part` before verifying the digest.
The part's entry id is derived from the migration id and part index and does
not change if the same part is resealed under a new epoch.

Regenerate with `ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked -- hub_cutover_snapshot_sample`.
Verify with `shasum -a 256 -c SHA256SUMS` from this directory.

# Served automation packs v1

These samples pin flow installation, fifteen-minute freshness markers and held
status invalidations described in [the catalog contract](../../docs/catalog.md).
`flow-record.json` contains the signed script-flow record returned by the
real catalog verifier against the contract's loopback registry. The package's
body is `{manifest, script}`. The other files contain exact reply or notice bytes
from the real handlers, embedded as `reply_bytes` beside `request_bytes` and the
expected response/error/notice outcome. Re-encoding parsed replies is not a byte
comparison.

## Samples

- `flow-record.json`: the verified `@example/hello-flow@1.0.0` script-flow record
  and registry-served record signature; its digests bind the contract archive.
- `generations.json`:
  - `fresh-flow-generation`: an active generation listing the flow pin, with no
    `pinned_status` member while status is available and fresh;
  - `stale-available-flow-generation`: the same active generation, still available,
    with `stale_since_ms: 900100`;
  - `revoked-flow-generation`: that generation degraded with `pinned_revoked`;
  - `revoked-flow-activation-blocked`: a retry of the earlier successful activation
    refused with `activation_blocked` after revocation.
- `context.json`: `fresh-flow-context` and `stale-flow-context`, showing the same
  flow pin's per-pin status before and after the freshness deadline.
- `status-watch.json`, all emitted by `catalog.status_watch`:
  - `opening-resync`: the first notice of a new watch;
  - `stale-pin-changes`: the flow pin passed its freshness deadline;
  - `foreign-token-resync`: a watch opened with another process's token;
  - `stale-cleared-changes`: a complete registry feed confirmed the held status,
    clearing `stale_since_ms` (added in `catalog-packs-v1.1`);
  - `revoked-pin-changes`: the registry revoked the pinned version (added in
    `catalog-packs-v1.1`).
  A `changes` notice names the versions whose served status moved; read the
  generation or `context.session` again for the new status.

Agent, plan, generation and process-incarnation identities are fixed fixture
values. Local times are fixed: lane entry/confirmation at 100 ms, plan readiness
at 200 ms, generation creation at 300 ms and activation at 700 ms. The freshness
tick observes the 900100 ms deadline, a feed confirms the status again at
950000 ms, and the revocation feed applies at 1000000 ms.
published TEST-ONLY catalog vectors, not operator keys. No temporary paths or
random ids enter the samples. Setup uses store transitions; the separate flow
installation regression tests cover prepare, consumer checks, consent and install.

## Reproduce and verify

From the repository root, serialize cargo invocations:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked -- catalog_packs_v1_records_match_published_files
cargo test --workspace --all-targets --locked -- catalog_packs_v1_records_match_published_files
```

The first command writes these files and `SHA256SUMS`. The second regenerates
replies in memory and compares each published file byte for byte, and verifies
the checksums. Existing `catalog-generation-v1` and `memory-context-v1` samples
are intentionally unchanged.

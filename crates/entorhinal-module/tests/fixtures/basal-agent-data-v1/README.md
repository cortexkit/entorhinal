# Basal `agent-data/v1` exchanges

These samples exercise Basal's real `agent-data` internal service against
scratch stores and an in-memory stand-in for Engram's door. The
`cortexkit-role-agent-data` 0.3.1 contract defines the wire shapes. Each JSON
file is an array of named exchanges. `request` contains `method` and `params`;
`reply` is the success envelope (`result`), and `error` is the daemon ERROR
body (`code`, `message`, and optional `detail`). All fixture IDs are synthetic.
A cut is the sealed list of data streams (with their lengths and digests)
for one move, identified by `cutId`. A cut can contain no stream from Basal.

| File | Cases |
| --- | --- |
| `source-flows.json` | `describe`; `busy-with-flows` (idle); `prepare-with-flows` (one frozen item); `export-with-flows`; `release-enforced` after receiver activation. |
| `receiver-flows.json` | `diverged-at-cut`; `import-with-drops` (`dropped.queued_event: 1`); `activate-with-flows`. Shares the cut in `source-flows.json`. |
| `source-empty.json` | `busy-without-flows` (idle); `prepare-empty`; `export-empty`. Both prepare and export return zero items. |
| `receiver-empty.json` | `import-empty` (zero items); `activate-empty` (no flow written). The cut in `source-empty.json` holds an item from another participant but no Basal item. |
| `receiver-clean.json` | `import-without-drops` (one item with no `dropped` field), from a flow with no queued events. |
| `source-refusals.json` | `prepare-approval-open` (`busy` while the flow version awaits the operator's approval); `prepare-run-active` (`busy` while a run is live); `release-before-export` (`invalid_state` at step `prepared`); `describe-wrong-caller` (`caller_forbidden` on an unscoped `reserved:broca` route instead of `reserved:entorhinal`). |
| `receiver-refusals.json` | `activate-run-active` (`busy` for a local run after staging the clean import); `import-flow-id-clash` (`local_advanced` on a separate receiver where a different agent owns the incoming flow ID). |

`SHA256SUMS` pins this README and all seven JSON files. To regenerate the
vectors and checksums on Linux from the repository root:

```sh
BASAL_WRITE_VECTORS=1 cargo test --locked -p basal-module --lib basal_agent_data_vectors_match_service
```

Without `BASAL_WRITE_VECTORS=1`, the test compares the generated text and
checksums to the files byte for byte. The test also checks every refusal
against the contract's allowed codes for its operation.

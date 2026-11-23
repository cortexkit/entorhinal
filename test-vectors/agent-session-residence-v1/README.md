# Agent session residence samples, version 1

These are real `agent.session_residence` served-handler replies, captured by
`served_vectors::session_residence_tests::session_residence_records_match_published_files`.
The fixed incarnation is `0123456789abcdef`; this read does not add incarnation
or generation to its result. Agent Fern and the archive machine ID are made up.
The reader is the daemon-stamped reserved principal `phone`; the read is open
to every principal.

Each JSON file is an array of request/principal/reply records. Replies use the
normal `{result: ...}` envelope. Optional members are omitted, never null.

| File | State | Evidence |
|---|---|---|
| `not-registered.json` | `not_registered` | No binding for the runner/session pair; only state is returned. |
| `held-on.json` | `elsewhere` | Session data stays on `machine-fern-archive`; `agent_id` and `held_on` are returned, without claim fields. |
| `moving.json` | `moving` | An open source move takes precedence over the unread claim. |
| `unknown.json` | `unknown` | Sharing is starting, without a selected hub watch. No claim fields are invented. |
| `sharing-off.json` | `unclaimed` | Local sharing is disabled: known epoch 0, matching `agent.where`. |

The precedence is no binding, binding’s `held_on`, open move, then the
residence claim or the sharing-off fallback. `held_on` is a store-origin
machine ID, not a claim holder. The held-on reply deliberately omits
`holder` and `holder_name`.

The binding, move and whereabouts are read in one store transaction. Unlike
`agent.where`, residence answers
`unknown` for stale or integrity-failed claims, or positive-epoch claims with
unknown self identity. Restores may become writable only for `here` or
`unclaimed` at epoch 0; `elsewhere` and `unknown` are read-only, `moving` waits,
and `not_registered` means no move evidence.

To regenerate only these files from the repository root:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked -- session_residence_records_match_published_files
(cd test-vectors/agent-session-residence-v1 && shasum -a 256 *.json > SHA256SUMS)
```

Without `ENTO_WRITE_SERVED_VECTORS`, the test byte-compares replies with the
published files. Verify hashes from this directory with `shasum -a 256 -c
SHA256SUMS`. The hashes cover all JSON files; this README is not hashed.

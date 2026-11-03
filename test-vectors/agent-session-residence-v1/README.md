# Agent session residence samples, version 1

These are real `agent.session_residence` served-handler replies, captured by
`served_vectors::session_residence_tests::session_residence_records_match_published_files`.
The fixed incarnation is `0123456789abcdef`; this read does not add incarnation
or generation to its result. Agent Fern, the machine names Cedar and Willow,
the author IDs and the archive machine ID are made up. The reader is the
daemon-stamped reserved principal `phone`; the read is open to every principal.

Each JSON file is an array of request/principal/reply records. Replies use the
normal `{result: ...}` envelope. Optional members are omitted, never null.

| File | State | Evidence |
|---|---|---|
| `not-registered.json` | `not_registered` | No binding for the runner/session pair; only state is returned. |
| `held-on.json` | `elsewhere` | Session data stays on `machine-fern-archive`; `agent_id` and `held_on` are returned, without claim fields. |
| `moving.json` | `moving` | An open source move takes precedence over the unread claim. |
| `unknown.json` | `unknown` | Sharing is enabled but no claim has been read. No claim fields are invented. |
| `stale.json` | `unknown` | A cached claim names this machine's author but is stale; cached fields remain available. |
| `elsewhere.json` | `elsewhere` | A fresh epoch-3 claim is held by another author, Willow. |
| `here.json` | `here` | A fresh epoch-3 claim is held by this machine's author, Cedar. |
| `unclaimed.json` | `unclaimed` | A verified absent claim is cached at epoch 0. |
| `sharing-off.json` | `unclaimed` | No claim row, but sharing has never been enabled: verified epoch 0, matching `agent.where`. |

The precedence is no binding, binding's `held_on`, open move, unusable claim,
then the usable claim's holder (or unclaimed epoch 0). `held_on` is a store-origin
machine ID, whereas `holder` is an engram author. They are different namespaces;
there is no trusted mapping to an author display name for `held_on`, so the
held-on reply deliberately omits `holder` and `holder_name`.

The binding, move, claim and self identity come from one store read transaction.
No engram request or write is made. Unlike `agent.where`, residence answers
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

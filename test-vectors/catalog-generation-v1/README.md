# Catalog generation v1 served vectors

`records.json` contains six sequential examples produced by the real
`ProjectsHandler::handle_served_request`, admitted as `reserved:prefrontal-core`
without a flow scope. `SHA256SUMS` covers this README and `records.json`.

Each record has exactly:

- `case`: descriptive case name below;
- `request_bytes`: UTF-8 full request bytes as a JSON string. Decode the string,
  not the contained JSON, when comparing bytes;
- `outcome`: `response` or `error`, matching `HandlerOutcome`;
- `reply_bytes`: exact successful response bytes as a JSON string. For an error,
  this is the existing served-vector encoding of the handler's error outcome:
  `{code, detail, message}`. The handler returns structured errors, not a serialized
  response body, so this encoding is explicitly **not** a transport error envelope.

Requests contain `method` and `params`. `catalog.agent_generation` params contain
only `agent_id`; `catalog.ack_activation` also has `generation_id`. There is no
request id. Successful replies wrap their value in `result`.

Cases, in order:

1. `ready`: no active generation (`active: null`), one ready generation.
2. `ack_success`: the ready generation becomes active, returned in `generation`.
3. `active_available`: `active` and an empty `ready` list; no `pinned_status`.
4. `active_withdrawn`: still active, `error: null`; `pinned_status` reports withdrawal.
5. `degraded_and_ready_blocked`: the active generation became degraded; the newer
   ready generation also holds the revoked version and cannot be activated.
6. `ack_blocked`: the ready generation's acknowledgement is refused, with
   `code: "activation_blocked"`, `message: "activation_blocked"`, and
   `detail: {name, reason, version}` naming the blocked pin. No `result` is present.

Generation fields (shared by the `catalog_generation` agent tool and the
`catalog.agent_generation` / `catalog.ack_activation` operations reserved to core):

- `generation_id`, `agent_id`, `plan_id`: opaque local generation, owner and plan ids;
- `state`: `installed`, `approved`, `ready`, `active`, or `degraded` (only the latter
  three occur here; local `superseded` bookkeeping is never sent);
- `closure`: exact package pins. Every pin has `name`, `version`, `kind` (`skill`),
  `manifest_sha256`, `archive_sha256`, `publisher_fingerprint`, `root`, and `range`
  (a root's semver range; non-root pins use null). Identities and hashes are
  unchanged by status news;
- `pin_digest`: SHA-256 of the canonical pin set, unchanged by status news;
- `created_at_ms`: 300 in these fixtures;
- `activated_at_ms`: null before activation, 700 after acknowledgement;
- `error`: null when not degraded; on status degradation exactly
  `{code, name, version, reason}`, with `code: "pinned_revoked"` here and
  `pinned_tombstoned` for a tombstone;
- optional `pinned_status`: entries `{name, version, state, reason}` for held
  non-available closure versions only. **Absent entirely**, never `[]`, when no
  such versions exist. Withdrawal is not an error or a generation state change.
  Both the degraded generation and the ready generation whose activation is
  blocked report the revoked pin here.

The generation read returns `{active, ready}`. `active` may be null or the current
active/degraded generation; `ready` is always an array of newer ready generations.
A successful ack returns `{generation}` instead. No reply contains an incarnation,
module registry generation counter, or additional reason field.

The fixture fixes agent id `agent_0000000000000002`, ids
`generation:available` / `generation:blocked` and their `plan:`-prefixed plan ids,
clock 700 ms, and handler incarnation `0123456789abcdef`. It seeds local plans and
pins, then applies status news through the store's trust transaction, which also
updates generation effects; the queries and acknowledgements are real handler
calls. Separate HTTP tests authenticate signed feeds and exercise preparation.

Regenerate from the repository root:

```
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked catalog_generation_v1_records_match_published_files
(cd test-vectors/catalog-generation-v1 && shasum -a 256 README.md records.json > SHA256SUMS)
```

Without `ENTO_WRITE_SERVED_VECTORS=1`, the same test compares all published bytes
without normalizing object order or whitespace. The native mutation control
`catalog-generation-served-bytes` changes a serialized digest and must fail the
byte comparison in this test rather than re-generating its expectation from the
altered implementation. `catalog-generation-served-status` separately removes the
status overlay to prove that the withdrawal notice is observed.

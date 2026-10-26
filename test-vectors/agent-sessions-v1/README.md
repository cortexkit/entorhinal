# Served agent session and claim reads v1

The original records were captured through entorhinal **0.1.26**'s real `ProjectsHandler`
request path. Successful replies contain the exact UTF-8 JSON bytes returned by
the handler, including the `result` envelope; refusals contain the handler's
`code`, `message`, and `detail` (`null` when no detail was supplied). Each
`request` is the complete request frame `{method, params}`, not just its params.
The recorded `principal` is the module ID stamped on the request route; it is not
a capability name. The fixture's fake daemon catalog lists `prefrontal-core` as
the provider of both `agent-host/v1` and `agent-orchestrator/v1`, and the
registration route carries principal `prefrontal-core`. Admission compares the
caller principal with whichever module the catalog lists for either role; it
does not compare against a fixed module name. Since one module provides both
roles here, it is admitted as the orchestrator. Read calls use the reserved
`phone` route, and the refusal vector uses `plexus`, which provides neither
role. Claim records and machine display names are fixed in the scratch registry; no
network or machine-local state is used.

The original fixture imports seven live agents with fixed IDs, uses clock `700`
and incarnation `0123456789abcdef`. Reference kinds and harnesses are open
strings, not closed enums; familiar kinds include `pi-session`,
`opencode-session`, `claude-code-session`, `broca-session`, and `agent` for core's
own rows. These vectors pair `pi-session` with harness `pi` and
`opencode-session` with harness `opencode`. `cortexkit-role-llm-runner` is not in
entorhinal's dependency graph, so `ref` uses a fixed 32-lowercase-hex value
`0123456789abcdef0123456789abcdef`, not a computed `session_ref`. The re-key
sample uses opaque fixed references for its new keys; entorhinal does not
compute references from the project root or interpret their contents.

Releases after 0.1.44 renamed the fixture's display names from `This Mac` and
`Other Mac` to `This machine` and `Other machine`, the source machine id in
`sessions-held-on.json` from `source-mac` to `source-machine`, and the waiting
message in `where-moving-source.json` to "waiting for the target machine to
pick up the move". Codes, keys and every other byte are unchanged, but the six
affected files have new hashes, so consumers that pin them must re-pin.

## Params schemas

Fields are required JSON strings unless marked optional. These operation request
objects deny unknown fields; a field outside the listed schema is an
`invalid_request`.

- `agent.register_session`: exactly
  `{agent_id, ref_kind, ref, harness, session_id, project_root?}`. The reference
  kind is 1–64 bytes, reference 1–512 bytes, harness 1–32 bytes, session ID
  1–256 bytes, and a supplied project root 1–4096 bytes (not null). Omitting the
  root keeps a stored root on re-registration or re-key; a new binding without
  one stores unknown, which reads as null in `agent.session_for_ref`,
  `agent.sessions` and `agent.session_for_runner`. The success records register an
  unclaimed live agent as `orchestrator`, then register the same reference again
  for that agent. A separate OpenCode registration uses `opencode-session` and
  the `opencode` harness. Refusals cover `ref_bound_elsewhere`, a cached
  `held_elsewhere` claim with the other machine's name, a `caller_forbidden` request
  from `plexus`, and an unknown field.
  A different reference for the same agent, reference kind and host session
  re-keys that binding in place, keeping its caller role, runner unless supplied,
  and network history, and clearing its end time. A different agent still
  refuses `session_bound_elsewhere`; an occupied re-key destination refuses
  `ref_bound_elsewhere`. Open moves and claim checks still apply.
- `agent.for_session`: exactly `{harness, session_id}`. Harness is 1–32 bytes
  and session ID 1–256 bytes. The file includes a found binding and `not_found`.
- `agent.session_for_ref`: exactly `{ref_kind, ref}`. Reference kind is 1–64
  bytes and reference 1–512 bytes. The file includes a binding with its current
  `project_root`, a migrated legacy row represented by schema 9's empty-root
  backfill for a binding predating `project_root` (whose reply has
  `project_root: null`), and `not_found`. The reply carries `runner`, the
  module ID of the llm-runner hosting the session, only when the binding has
  one recorded; a binding without one omits the key rather than sending
  `null`. Re-registering without a runner keeps the recorded one, so the
  reply still names it.
- `agent.where`: exactly `{agent_id}`, where the string has entorhinal's form
  `agent_` followed by 8 or 16 lowercase hexadecimal digits. The file includes
  an agent never previously read from the claim cache
  (`unclaimed`), `held_here`, `held_elsewhere` with the holder name `Other machine`,
  and `unknown_agent`.
- Both moving samples keep top-level `status` at `held_here`; the presence of
  `moving` for a non-terminal move on this machine is the signal, and its `side`
  identifies which end this machine is.
- `agent.register_session` may also carry an optional `runner`: the module ID
  of the llm-runner hosting the session, 1–64 bytes with no control
  characters. It is taken out before the strict decode above, so the
  unknown-field refusal is unchanged, and it is never echoed in the reply.
- `agent.end_session`: exactly `{harness, session, runner?}`. Harness is 1–32
  bytes, session 1–256 bytes, runner as above. Accepted only from the sole
  provider of `agent-orchestrator/v1`. The end time is the handler's clock.
- `agent.register_session` may also carry an optional `network`: `yes`,
  `no` or `unknown`, whether the session's agent can reach the network. It
  is taken out before the strict decode like `runner`, `null` reads as
  absent, any other value is an `invalid_request` naming `network`, and it
  is never echoed in the reply. A value that differs from the binding's
  current one is appended to its network history, stamped with the
  handler's clock when it commits.
- `agent.set_session_network`: exactly `{harness, session, network}`.
  Harness is 1–32 bytes, session 1–256 bytes, network as above. Accepted
  only from the sole provider of `agent-orchestrator/v1`. Replies
  `{network, since_ms}`: the new entry, or the current one when the value
  is already current (nothing written). `since_ms` is the handler's clock at
  commit, raised to the previous entry's time plus one if the clock is not
  past it. An unknown session is `not_found`; a binding already holding
  1,024 entries refuses another value `network_history_full`.
- `agent.sessions`: exactly `{agent_id, cursor?, limit?, open?}`. Optional boolean
  `open: true` selects only bindings with no end time; omitted or false preserves
  the full list and existing bytes. Cursors are filter-specific: cross-filter
  use refuses `invalid_request`. `limit` is 1–500
  and defaults to 100; `cursor` is the opaque `next_cursor` of the previous
  page. Rows run newest registration first, ties in `(ref_kind, ref)` order.
  Each row carries `network`, the current value, `unknown` with no history.
- `agent.session_for_runner`: exactly `{runner, session}`. A session
  registered without a runner is `not_found` here; look it up with
  `agent.for_session`. The reply carries `network`, the current value
  (`unknown` with no history), and `network_history`, every
  `{network, since_ms}` oldest first (empty with no history).
- `agent.where_many`: exactly `{agentIds}`, with 1–64 unique valid agent IDs.
  Results preserve request order and omit unknown agents; the sample requests a
  locally held and remotely held agent around an unknown ID.

Moved finished sessions retain metadata on the receiving machine while their data
stays on the source (or its recorded earlier holder). The three session reads
include `heldOn` only for such remote data. Registration, including re-keying,
refuses `session_on_other_machine` with `{machine_id}` and writes nothing until
a backfill makes the data local.

## Files

- **`sessions-open.json`**: two open-only pages skip the ended worker and show
  the filter-specific cursor.
- **`sessions-held-on.json`**: lists a finished worker with `heldOn`, followed by
  a served `session_on_other_machine` registration refusal. All older JSON
  samples remain byte-identical.

- **`register-session.json`**: orchestrator registration success, same-reference
  re-registration, a distinct OpenCode session registration, `ref_bound_elsewhere`,
  `held_elsewhere`, `caller_forbidden` from `plexus`, and rejection of an
  unknown registration field.
- **`for-session.json`**: Pi and OpenCode lookups of registered sessions and a
  missing harness/session key.
- **`session-for-ref.json`**: Pi and OpenCode registrations with
  `project_root` and no runner (no `runner` key), a migrated legacy row with a
  null root, a Broca session registered with runner `broca` (whose reply
  carries `runner: "broca"`), and a missing reference. Releases after 0.1.46
  added the Broca record; the other four records are byte-identical to
  before, but the file has a new hash, so consumers that pin it must re-pin.
- **`where.json`**: on a machine with sharing on: never-read/unclaimed, locally
  held, remotely held with a holder name, and unknown agent.
- **`where-many.json`**: a local and remotely held agent, with an unknown ID
  between them to pin request order and omission.
- **`where-sharing-off.json`**: on a machine that has never turned sharing on, an
  agent with no claim is a verified `unclaimed` (`known: true`), because no
  other machine can hold it.
- **`where-moving-source.json`**: a locally held agent during the source-side
  published step, waiting for the receiver to pick up the move.
- **`where-moving-receiver.json`**: the same locally held agent during the
  receiver-side claimed step, before activation completes.
- **`register-session-runner.json`**: a Broca worker registration naming
  runner `broca`; the reply bytes have the same shape as a registration
  without one.
- **`register-session-rekey.json`**: captured from the real handler on the
  unreleased session re-key implementation after 0.1.42, with a separate
  two-agent fixture and the same fixed clock. Four records show the initial
  binding, its same-agent re-key without a root or runner, another agent's
  refused re-key, and a new registration without `project_root` (null in the
  reply). All older JSON samples remain byte-identical.
- **`end-session.json`**: ending a worker (`found: true` with the end time),
  the repeat (the same first end time, nothing written), an unknown session
  (`found: false`), and `runner_mismatch` for a runner other than the
  recorded one.
- **`sessions.json`**: one agent's two pages at `limit: 2`: a live Broca worker
  (`active` from the host's liveness snapshot, network `no` after `yes`) and
  an ended worker (`gone`, network `unknown` with no history), then a Pi
  session registered with `runner: null` (`unknown`, network `unknown`),
  whose page has `next_cursor: null`.
- **`session-for-runner.json`**: the live worker found by `{runner, session}`
  with its history `yes` at 700 then `no` at 710, the ended worker with no
  history (`unknown`, `[]`), an unknown pair, and a session registered
  without a runner, both `not_found` with the same shape as
  `agent.for_session`'s miss.
- **`set-session-network.json`**: the live worker's `yes` (a new entry at
  700), the same `yes` again (nothing written, the same 700), `no` at 710
  after the clock moved on, `network_history_full` for the ended worker once
  the handler filled its history with 1,024 alternating entries, and
  `not_found` for an unknown session.
- **`session-lookup-cases.json`**: nineteen additional session and fleet lookup
  records from real orchestrator registrations, without changing any older JSON
  file. The fixture imports invented head agent `agent_0000000000000201`
  (`Lookup Explorer`) with a versioned avatar, registers a rooted project through
  a Direct route, and uses the
  same fixed clock/incarnation, and registers three sessions. The populated
  session is ended through `agent.end_session`; production move export and
  receiver activation carry its metadata with `heldOn: "lookup-source-machine"`.
  No session rows are created or altered with raw SQL. The source and receiver
  origins are initialized through `check_store_origin` with fixed invented IDs.
  The project root is a canonical scratch directory at registration (including
  macOS's `/private/var` spelling); its entire JSON-escaped path is explicitly
  mapped to `/fixture/session-lookups/populated`, so Windows cannot leave a
  backslash in a suffix.

  | Record | Operation | Case / reply |
  | --- | --- | --- |
  | 1 | `agent.session_for_ref` | Found, all fields populated: runner, project root and `heldOn`. |
  | 2 | `agent.session_for_ref` | Found, root omitted at registration → `project_root: null`; no recorded runner → key omitted, not null. |
  | 3 | `agent.session_for_ref` | Unknown reference → `not_found`. |
  | 4 | `agent.session_for_ref` | Missing `ref` → `invalid_request`. |
  | 5 | `agent.session_for_ref` | Numeric `ref` → `invalid_request`. |
  | 6 | `agent.session_for_ref` | Extra `unexpected` member → `invalid_request`. |
  | 7 | `agent.session_for_runner` | Found, all fields populated: runner, root, end time, current `yes`, nonempty history and `heldOn`. |
  | 8 | `agent.session_for_runner` | Found, `project_root` and `ended_at_ms` null, current `unknown`, empty history; runner remains required. |
  | 9 | `agent.session_for_runner` | Unknown pair → `not_found`. |
  | 10 | `agent.session_for_runner` | Missing `session` → `invalid_request`. |
  | 11 | `agent.session_for_runner` | Numeric `session` → `invalid_request`. |
  | 12 | `agent.session_for_runner` | Extra `session_id` → `invalid_request`, proving it is not an alias for `session`. |
  | 13 | `agent.session_for_runner` | Existing session registered without a runner → `not_found`. |
  | 14 | `agent.fleet_identity` | Full `{}` reply: versioned avatar fingerprint, nonempty tag/labels, rooted project/workspace placement, and `logPosition: 0`. |
  | 15 | `agent.fleet_identity` | Current issued token → `unchanged: true`, no `agents` member. |
  | 16 | `agent.fleet_identity` | Previously issued token after real `agent.update_tag` → full reply with new token/generation, not a stale-token refusal. |
  | 17 | `agent.fleet_identity` | Extra `unexpected` member → `invalid_request`. |
  | 18 | `agent.fleet_identity` | Numeric `token` → full reply, **not** `invalid_request`; non-string tokens are cache misses. |
  | 19 | `agent.fleet_identity` | Bad opaque string token → full reply, not a bad-token refusal. |

  The session replies are **not the same row type**: the ref read has nullable
  `session_id` and optional/omitted `runner`; the runner read has required
  `session`, required `runner`, and adds `ended_at_ms`, `network` and
  `network_history`. Both include `incarnation`. `heldOn` is omitted unless
  finished data is remote. No reply field remains without a populated example
  for either op. Null harness/session-ID placeholder cases are not produced by
  registration, which requires both strings
  (`crates/entorhinal-module/src/sessions.rs:361-364`). Placeholders created by
  move activation have null harness/session ID and no runner
  (`crates/entorhinal-core/src/session_carry.rs:372-389`), so they cannot be
  found by runner/session. Neither lookup ever returns `runner: null`.
  Fleet identity accepts `{}` or `{token: <any JSON value>}` and denies extra
  members. Only an exact current string token returns the unchanged shape;
  stale, bad, null and non-string tokens all return the full shape. Sharing is
  off in this fixture, so `logPosition` honestly remains `0`, distinct from the
  local journal `generation`. The existing
  `test-vectors/agent-identity-reads-v1/fleet-identity.json` stays unchanged.
  It already covers absent/unversioned avatars and rootless placement; the new
  records add a populated `project.canonicalRoot`, tag and labels. No fleet
  field lacks a populated example after these additions. Nonzero log position,
  wild avatars, workspace-head/hiree roles and an empty fleet remain unsampled.
- **`SHA256SUMS`**: SHA-256 hashes of the seventeen JSON vector files, with
  names relative to this directory. The first ten lines are pinned by
  `served_vectors::agent_session_samples_keep_their_pinned_bytes_and_checksums`.

## SHA-256 hashes

| File | SHA-256 |
| --- | --- |
| `for-session.json` | `dd41914934a9b85b35670a815bef17de84752d8da7d05a1bf8833f3edbd1996b` |
| `register-session.json` | `756975d9e0697109b0f7a70c0a08dfa18b2fc5222915e1051870948b1836d6a0` |
| `session-for-ref.json` | `3e9e99950c167a2b2c5b3315fca35d353f50bc29e8ad93afe616c15e7cb703f6` |
| `where.json` | `faacea127dcaaa7181329562e4de553851aebf700358b3267864e894166b911e` |
| `where-many.json` | `422b8592c531c3c22e161b7a926f3ac61df6155fca2a9660bb159b1a0054d565` |
| `where-moving-receiver.json` | `cd7a4e475915787cf1a1680a05ad4421b3da19fe90a0a106c3b878b71064b9a6` |
| `where-moving-source.json` | `87e6cf886a685406c0f7b51c9e372f2ac781a138372ea19ea926a0095df75bb1` |
| `where-sharing-off.json` | `0b8fe82664439e2eb38e0816f900c08f7822bdfe29373a77579cfbbd080c6b63` |
| `end-session.json` | `efd3a0b6c6ac2463ec20e3be9ee26593ad3fd0a4a62c3457ad65f5e876ff6d18` |
| `register-session-runner.json` | `8ec0505594c6336a924595edf8af73304f1e2bd4d80f197b26b8e18628a4dca3` |
| `session-for-runner.json` | `b62878e6c91d98cc81ff0f993205edd4a88b5eaf49d649bab81feca02696203e` |
| `sessions.json` | `e9b77127661e0352b508d923c5cd2e718c0b14c63662b8b2cb8e2f8b872f79e1` |
| `set-session-network.json` | `d6bdd5717154e0c6b5d6675b9f6f03580be9e8cb41c2cf8ae5cd2925ffe831f9` |
| `register-session-rekey.json` | `1dcb2af2bcc9f346a9ebac36369f70cb9805e53c0585110d9b675f25f4442b96` |
| `sessions-held-on.json` | `ce3ab0bb16f03a0f7aa605805234c80b42e3f92b2d0e384dd15ac8a7e2af9caa` |
| `sessions-open.json` | `01d785e934526b457d9f958ac94d8a0829526729e0f418e114ea8faab3a86af7` |
| `session-lookup-cases.json` | `5e2a306b6a05bd4ab1c922c0c57541d16e3fb33c2c403a797fe5a34edb774107` |

## Reproduce and verify

From the repository root, generate the vector files with:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked session_records_match_published_files -- --nocapture
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked session_lifecycle_records_match_published_files -- --nocapture
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked session_rekey_records_match_published_files -- --nocapture
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked served_vectors::session_lookup_cases_match_published_file -- --exact --nocapture
```

Refresh their hashes from the repository root with:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/agent-sessions-v1')
names = ['for-session.json', 'register-session.json', 'session-for-ref.json', 'where.json', 'where-many.json', 'where-moving-receiver.json', 'where-moving-source.json', 'where-sharing-off.json', 'end-session.json', 'register-session-runner.json', 'session-for-runner.json', 'sessions.json', 'set-session-network.json', 'register-session-rekey.json', 'sessions-held-on.json', 'sessions-open.json', 'session-lookup-cases.json']
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n'
    for name in names), encoding='utf-8')
PY
```

Without `ENTO_WRITE_SERVED_VECTORS=1`, the non-ignored session, lifecycle,
re-key and lookup-case tests regenerate their JSON from the real handler and
compare the bytes against the published files on each run. Verify the new file
with both Cargo's test runner and nextest:

```sh
cargo test --workspace --all-targets --locked served_vectors::session_lookup_cases_match_published_file -- --exact
cargo nextest run --workspace --all-targets --locked -E 'test(served_vectors::session_lookup_cases_match_published_file)'
ckdev-mutate check --strict
ckdev-mutate run --only served-session-lookup-runner
```

The `served-session-lookup-runner` control in root `mutations.toml` removes the
recorded runner from `agent.session_for_ref`; the lookup-case generator test must
fail its explicit expected-field assertion.

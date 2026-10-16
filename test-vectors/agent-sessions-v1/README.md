# Served agent session and claim reads v1

These records are captured through entorhinal **0.1.26**'s real `ProjectsHandler`
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
role. Claim records and Mac display names are fixed in the scratch registry; no
network or machine-local state is used.

The fixture imports seven live agents with fixed IDs, uses clock `700` and
incarnation `0123456789abcdef`. The closed `ref_kind` set is `pi-session`,
`opencode-session`, `claude-code-session`, `broca-session`, and `agent` for core's
own rows. These vectors pair `pi-session` with harness `pi` and
`opencode-session` with harness `opencode`. `cortexkit-role-llm-runner` is not in
entorhinal's dependency graph, so `ref` uses a fixed 32-lowercase-hex value
`0123456789abcdef0123456789abcdef`, not a computed `session_ref`.

## Params schemas

All fields listed here are required JSON strings. These operation request
objects deny unknown fields; a field outside the listed schema is an
`invalid_request`.

- `agent.register_session`: exactly
  `{agent_id, ref_kind, ref, harness, session_id, project_root}`. The reference
  kind is 1–64 bytes, reference 1–512 bytes, harness 1–32 bytes, session ID
  1–256 bytes, and project root 1–4096 bytes. The success records register an
  unclaimed live agent as `orchestrator`, then register the same reference again
  for that agent. A separate OpenCode registration uses `opencode-session` and
  the `opencode` harness. Refusals cover `ref_bound_elsewhere`, a cached
  `held_elsewhere` claim with the other Mac's name, a `caller_forbidden` request
  from `plexus`, and an unknown field.
- `agent.for_session`: exactly `{harness, session_id}`. Harness is 1–32 bytes
  and session ID 1–256 bytes. The file includes a found binding and `not_found`.
- `agent.session_for_ref`: exactly `{ref_kind, ref}`. Reference kind is 1–64
  bytes and reference 1–512 bytes. The file includes a binding with its current
  `project_root`, a migrated legacy row represented by schema 9's empty-root
  backfill for a binding predating `project_root` (whose reply has
  `project_root: null`), and `not_found`.
- `agent.where`: exactly `{agent_id}`, where the string has entorhinal's form
  `agent_` followed by 8 or 16 lowercase hexadecimal digits. The file includes
  an agent never previously read from the claim cache
  (`unclaimed`), `held_here`, `held_elsewhere` with the holder name `Other Mac`,
  and `unknown_agent`.
- `agent.where_many`: exactly `{agentIds}`, with 1–64 unique valid agent IDs.
  Results preserve request order and omit unknown agents; the sample requests a
  locally held and remotely held agent around an unknown ID.

## Files

- **`register-session.json`**: orchestrator registration success, same-reference
  re-registration, a distinct OpenCode session registration, `ref_bound_elsewhere`,
  `held_elsewhere`, `caller_forbidden` from `plexus`, and rejection of an
  unknown registration field.
- **`for-session.json`**: Pi and OpenCode lookups of registered sessions and a
  missing harness/session key.
- **`session-for-ref.json`**: Pi and OpenCode registrations with
  `project_root`, a migrated legacy row with a null root, and a missing
  reference.
- **`where.json`**: on a Mac with sharing on: never-read/unclaimed, locally
  held, remotely held with a holder name, and unknown agent.
- **`where-many.json`**: a local and remotely held agent, with an unknown ID
  between them to pin request order and omission.
- **`where-sharing-off.json`**: on a Mac that has never turned sharing on, an
  agent with no claim is a verified `unclaimed` (`known: true`), because no
  other Mac can hold it.
- **`SHA256SUMS`**: SHA-256 hashes of the six JSON vector files, with names
  relative to this directory.

## SHA-256 hashes

| File | SHA-256 |
| --- | --- |
| `for-session.json` | `dd41914934a9b85b35670a815bef17de84752d8da7d05a1bf8833f3edbd1996b` |
| `register-session.json` | `20015ad11322866712bf4e43666ba3356c87700b3905884b8deed67fb3d1edbb` |
| `session-for-ref.json` | `f8314b13df331e185f2c4960a314a3e8fac97af5d8e9904150ba024973fbb69a` |
| `where.json` | `da88d465b4a190dbf14f7224ed8fd4a6cae10a8639fbbc7953b7d8b1f11dce8d` |
| `where-many.json` | `93cc230f690999df6e7876f0bcabeaf391e5d8c9c7166e16a61d4c9112c153e6` |
| `where-sharing-off.json` | `0b8fe82664439e2eb38e0816f900c08f7822bdfe29373a77579cfbbd080c6b63` |

## Reproduce and verify

From the repository root, generate the vector files with:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked session_records_match_published_files -- --nocapture
```

Refresh their hashes from the repository root with:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/agent-sessions-v1')
names = ['for-session.json', 'register-session.json', 'session-for-ref.json', 'where.json', 'where-many.json', 'where-sharing-off.json']
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n'
    for name in names), encoding='utf-8')
PY
```

Without the write environment variable, the non-ignored
`served_vectors::session_records_match_published_files` test regenerates every
JSON file from the real handler and compares the bytes against the published
files on each run.

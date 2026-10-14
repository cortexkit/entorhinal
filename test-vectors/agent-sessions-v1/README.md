# Served agent session and claim reads v1

These records are captured through entorhinal **0.1.26**'s real `ProjectsHandler`
request path. Successful replies contain the exact UTF-8 JSON bytes returned by
the handler, including the `result` envelope; refusals contain the handler's
`code`, `message`, and `detail` (`null` when no detail was supplied). Each
`request` is the complete request frame `{method, params}`, not just its params.
The recorded principals identify which route was allowed to make each request.

The fixture imports seven live agents with fixed IDs, uses the fixed clock
`700` and incarnation `0123456789abcdef`, and registers through the reserved
module `agent-host/v1`; the fixture's fake capability resolver maps the
`agent-host/v1` capability to that module ID.
Read calls use the reserved `phone` route. Claim records and Mac display names
are fixed in the scratch registry; no network or machine-local state is used.
`cortexkit-role-llm-runner` is not in entorhinal's dependency graph, so the
`llm-runner` reference is the fixed 32-lowercase-hex value
`0123456789abcdef0123456789abcdef`, not a computed `session_ref`.

## Params schemas

All fields listed here are required JSON strings. These operation request
objects deny unknown fields; a field outside the listed schema is an
`invalid_request`.

- `agent.register_session`: exactly
  `{agent_id, ref_kind, ref, harness, session_id, project_root}`. The reference
  kind is 1–64 bytes, reference 1–512 bytes, harness 1–32 bytes, session ID
  1–256 bytes, and project root 1–4096 bytes. The success records register an
  unclaimed live agent as `host`, then register the same reference again for
  that agent. Refusals cover `ref_bound_elsewhere`, a cached `held_elsewhere`
  claim with the other Mac's name, a `caller_forbidden` reader, and an unknown
  field.
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

## Files

- **`register-session.json`**: host registration success, same-reference
  re-registration, `ref_bound_elsewhere`, `held_elsewhere`, `caller_forbidden`,
  and rejection of an unknown registration field.
- **`for-session.json`**: lookup of the re-registered session and a missing
  harness/session key.
- **`session-for-ref.json`**: current registration with `project_root`, a
  migrated legacy row with a null root, and a missing reference.
- **`where.json`**: never-read/unclaimed, locally held, remotely held with a
  holder name, and unknown agent.
- **`SHA256SUMS`**: SHA-256 hashes of the four JSON vector files, with names
  relative to this directory.

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
names = ['for-session.json', 'register-session.json', 'session-for-ref.json', 'where.json']
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n'
    for name in names), encoding='utf-8')
PY
```

Without the write environment variable, the non-ignored
`served_vectors::session_records_match_published_files` test regenerates every
JSON file from the real handler and compares the bytes against the published
files on each run.

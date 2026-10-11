# Served agent identity reads v1

These records come from entorhinal's real `ProjectsHandler`, through admitted
routes and the serving request path. Project/workspace registration and the
`agent.import` cutover are real handler requests; import runs with
`Reserved(prefrontal-core)`. Reads run with `Reserved(phone)` (recorded as
`{"kind":"reserved","moduleId":"phone"}`). The fixture binds this synthetic
reader identity to exercise handler admission and dispatch. A deployed phone
peer still needs permission from the SUBC router to call these operations;
this fixture does not check or grant that production access.

Each JSON file is an array of `{op, request, principal, reply}` records. `request`
is the operation's params object. Successful `reply` values embed the exact
UTF-8 JSON bytes returned by the handler, including its `result` envelope;
they are not parsed and re-serialized. Refusals are handler outcomes, not
response bytes, and are recorded as `{code, message, detail}` (`null` when no
detail was supplied).

- **`fleet-identity.json`**: a full `{}` read, the unchanged read using its
  returned token, and an unknown-field `invalid_request` refusal. The fixture
  has a head with a versioned avatar, an assistant with an unversioned avatar,
  an assistant without an avatar, and a retired assistant with an avatar.
  Only the three live agents appear in fleet identity.
- **`avatar-read.json`**: both live stored avatars; then an ordered mix of a
  present avatar, an unknown id, an unset avatar, a retired avatar, another
  present avatar, and a duplicate of the first id. Unknown/unset avatars are
  omitted, while retired avatars and duplicates are retained in request order.
  The last three records refuse zero ids, 65 ids, and an empty-string id.
- **`SHA256SUMS`**: SHA-256 hashes of the two JSON files, with filenames relative
  to this directory.

## Reproduce and verify

From the repository root, the generation command is:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked served_vectors -- --nocapture
```

On PowerShell, set `$env:ENTO_WRITE_SERVED_VECTORS = '1'` before the same cargo
command, and remove that environment variable afterwards. Generation rewrites
only the two JSON files. Refresh the hashes from the repository root with:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/agent-identity-reads-v1')
names = ['avatar-read.json', 'fleet-identity.json']
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n'
    for name in names), encoding='utf-8')
PY
```

Without the write environment variable, the non-ignored
`served_vectors::served_records_match_published_files` test byte-compares the
captured records with these files on every CI run, including Linux, macOS, and
Windows. The fixed process incarnation is `0123456789abcdef`. The project is
registered with an empty roots list, a supported rootless placement, so its
optional `canonicalRoot` is absent. The workspace's scratch root is registered
through a real request but is not part of either read reply. No platform path
or temporary-directory name enters the captured replies.

Fleet avatar fingerprints are the first 32 hexadecimal characters of BLAKE3
over `genome + "\n" + type + "\n" + version`, with the version written in decimal
and an empty string used for a missing version. The avatar-read operation
returns the stored genome/type/version, not the fingerprint.

## Core v1.0 compatibility boundary

The non-ignored `served_vectors::avatar_read_matches_core_contract_v1_0` test
reads the unchanged published fixture at
`crates/entorhinal-module/tests/fixtures/core-avatar-read-vectors-v1.0.json` and
attempts to import exactly its agent ids and stored avatar triples. Entorhinal
currently refuses `agent-avatar-00` with `import_invariant_failed`, detail
`{"agent_id":"agent-avatar-00","check":"agent_id"}`. Imported ids must be
`agent_` followed by 8 or 16 lowercase hex characters. The test checks this
refusal and stops there; it does **not** prove core-vector reply parity by
remapping ids or bypassing import. If those ids become importable, the test
will compare every vector's `avatars` exactly and allow only `generation` and
`incarnation` as extra top-level keys. The same strict reply-key assertion is
also exercised by the served-record test with valid imported ids today.

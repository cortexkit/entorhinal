# Served agent identity changes v1

`agent-changes.json` is one `agent.changes` reply captured from the real
`ProjectsHandler`, through admitted routes and the serving request path. It
uses the reads-v1 record format: an array of `{op, request, principal, reply}`.
`request` is the params object; `reply` embeds the exact UTF-8 handler bytes,
including the `result` envelope, without parsing and re-serializing them.
The reader is a synthetic reserved-module principal named `phone`, bound by the
fixture to exercise ordinary non-executive read admission; this fixture does
not grant that identity access to a deployed SUBC router.

The fixture registers three placed projects with empty roots lists (no local
filesystem directories) and imports fixed ids
through `agent.import` from `Reserved(prefrontal-core)`. One imported head
stores its project id as its workspace and has a claim keyed on that project
id. Another head stores its actual placement and already claims its name
there. A live hiree belongs to a third placed project and stores no workspace.
The correction is sent from `Direct` using a stub `OperatorConfirmer`; then a
Direct placement write carries the hiree to `destination`, also confirmed.
The cursor predates both writes. The three `agent.set_namespace` entries show
one correction claim move (released and active claims), one pure clear (empty
`claims`), and one carried hiree (released and active claims). Every after-image
has `workspace_id: null`.

## Reproduce and verify

From the repository root:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked served_changes_match_published_file -- --nocapture
```

In PowerShell, set `$env:ENTO_WRITE_SERVED_VECTORS = '1'` before the same cargo
command and remove it afterwards. This command rewrites `agent-changes.json`.
Refresh its directory-relative SHA-256 digest with:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/agent-identity-changes-v1')
name = 'agent-changes.json'
root.joinpath('SHA256SUMS').write_text(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n',
    encoding='utf-8')
PY
```

Without the generation switch, the non-ignored
`served_vectors::served_changes_match_published_file` test byte-compares the
published record with the handler's response in the workspace test suite.
CI runs that suite on Linux, macOS and Windows. The process incarnation is
fixed to `0123456789abcdef`, and all row timestamps are deterministic. No
platform path or temporary-directory name enters the recorded reply.

The identity-log envelope version remains 1. All sharing nodes must be upgraded
before a carry or correction is sent: older nodes cannot apply the embedded
`agent.set_namespace` changes. Consumers must accept this op and null stored
workspaces on heads and hirees before consuming this feed.

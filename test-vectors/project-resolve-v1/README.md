# Served project resolution v1

These vectors capture entorhinal **0.1.28**'s real `ProjectsHandler` replies for project path resolution, durable project-id lookup, and enumeration. The fixture uses fixed project/workspace ids, clock `700`, and module incarnation `0123456789abcdef`. It creates checkout and linked-worktree metadata under a scratch root; the linked worktree uses the `.git` file, gitdir, and `commondir` layout used by entorhinal's binding tests.

Scratch paths are normalized to `/fixture` so the checked-in bytes do not depend on the host's temporary-directory spelling. The implicit ids are re-derived from those normalized canonical paths. The registered root's generated registration epoch is normalized to `00000000000000000000000000000001`; its checkout incarnation token is seeded with the fixed 64-hex value recorded in the reply. The vector replies otherwise preserve the handler's exact UTF-8 response bytes, including the `result` envelope and key order.

## Route

The management surface sends these project operations to module `entorhinal` (`docs/module-surface.md`, **Projects**). The request is the full `{method, params}` envelope, with `method` set to `resolve`, `resolve_project_id`, or `enumerate`. Successful handler replies have a `result` envelope; refusals have `code`, `message`, and `detail`.

The `resolve` request parameter `canonicalRoot` is named for its usual input, a project's canonical root path, but accepts **any absolute path**, including paths that are not registered roots or repository roots. The reply also has a `canonicalRoot` field, which holds the canonical form of the queried path.

## Admission

These reads are open to every principal, including flow-scoped routes. `ProjectsHandler::admit_with` in `crates/entorhinal-module/src/main.rs` applies the general permission, flow-write, and write-authorization checks before dispatch. The permission gate passes unlisted query methods, and flow-scoped routes refuse mutations rather than these reads.

## Versions

These vectors capture replies from entorhinal module version **0.1.28**.

## Files and fields

Each JSON file is an array of records with these fields:

- **`request`**: the complete `{method, params}` request object. `resolve` uses `params.canonicalRoot`; the one malformed request omits it. `resolve_project_id` uses `params.projectId`. `enumerate` uses empty params `{}`.
- **`principal`**: the reserved module principal that opened the read route, always `{"kind":"reserved","moduleId":"phone"}` in these records.
- **`reply`**: the exact successful response bytes embedded as JSON, or the refusal outcome. Successful responses are `{ "result": ... }`; refusals are `{ "code", "message", "detail" }`. `detail` is `null` when the handler supplied no details. The malformed `resolve` request is refused with code `invalid_params` and message `missing field canonicalRoot`.

For every successful `resolve`, the result includes `projectId`, `via`, `gone`, `generation`, and the handler-added `incarnation`. `workspaceId`, `workspaceRoot`, and `projectName` are `null` when the path has no corresponding workspace or project metadata. `canonicalRoot` is the canonical queried path. Root-record mode adds `matchedRoot`, `sourceRoot`, `rootRecords`, and `bindingStatus`; fields that do not apply are `null`, and `rootRecords` is an empty array when there are no matching records. A worktree result omits those root-record fields and includes `worktreeOf`; `worktreeOf` is otherwise absent.

A root record includes `root`, `remotes`, `identity`, `incarnation`, `registrationEpoch`, and `approval`. `incarnation` and `registrationEpoch` are `null` for an unbound or unverifiable root. Optional `remotesError`, `rootKey`, and `rootKeyMismatch` are omitted when unavailable. `approval.journalSeq` is `null` until approval has a journal entry.

A successful `resolve_project_id` result always includes `projectId`, `via`, `gone`, `generation`, and the handler-added `incarnation`. The records cover `current`, `alias`, and unknown-id `gone` results.

A successful `enumerate` result includes `workspaces`, `projects`, `generation`, and the handler-added `incarnation`. Workspace entries contain `name`, `root`, and `workspaceId`; `root` is `null` when no workspace root is set. Project entries contain `projectId`, `name`, `roots`, `implicit`, `refKind`, and `rootRecords` in this root-record-enabled fixture. `workspaceId` is omitted for unplaced projects. `deviceFingerprint` and `lastRouteActivityMs` are omitted when unavailable. Empty `roots` and `rootRecords` are represented as arrays.

The implicit path id `pj-implicit1-30f9d5fcd6aa4ef2` demonstrates the derivation: BLAKE3 of the canonical path's UTF-8 bytes, taking the first 8 digest bytes as 16 lowercase hexadecimal characters and prefixing `pj-implicit1-`. The alias input is the corresponding id for `/fixture/alias-source`.

- **`resolve.json`**: exact registered root (`via: root`), its subfolder (`via: walk`), an external linked worktree (`via: worktree`, with `worktreeOf`), an unregistered implicit directory, a removed registered root (`gone: true`), and the missing-`canonicalRoot` refusal.
- **`resolve-project-id.json`**: a current project id, an implicit id retained as an alias after promotion, and an unknown id (`via: gone`, `gone: true`).
- **`enumerate.json`**: an unfiltered `{}` request returning two projects in one workspace, with names for mapping project ids to dashboard labels.
- **`SHA256SUMS`**: SHA-256 hashes of the three JSON files, with names relative to this directory.

## SHA-256 hashes

| File | SHA-256 |
| --- | --- |
| `enumerate.json` | `89afedb88fa4ddec219f4932df22f26ca8395022c5c1dd399e3e50c9d50b94dd` |
| `resolve-project-id.json` | `4af3d3d6dd6b279146da2e50dd48dd507674f2806f6ac35e8f04b380bc5e901e` |
| `resolve.json` | `a15d8e09bf4e5ee852097ee423c33e5feca8bff205797c01394d8667a9596bce` |

## Reproduce and verify

From the repository root, regenerate the vector files from the real handler with:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked project_resolve_v1_records_match_published_files -- --nocapture
```

Refresh the `project-resolve-v1` manifest from the repository root with:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/project-resolve-v1')
names = ['enumerate.json', 'resolve-project-id.json', 'resolve.json']
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(root.joinpath(name).read_bytes()).hexdigest()}  {name}\n'
    for name in names), encoding='utf-8')
PY
```

Without `ENTO_WRITE_SERVED_VECTORS=1`, the non-ignored `served_vectors::project_resolve_v1_records_match_published_files` test regenerates all three records from entorhinal's real request handling and compares each file byte for byte.

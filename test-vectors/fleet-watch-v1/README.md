# Served fleet watches v1

These records capture the real `ProjectsHandler` through the SUBC SDK's
in-process `ModuleHarness`. Each request is a management-surface call from
`reserved:callosum`. The stand-in does not grant production router access.
Bodies of `StreamData` and `Error` frames are embedded byte for byte, without
parsing and re-serializing the handler output. The enclosing records are arrays
of `{op, request, principal, frames}`; `request` is the params object. The relay
sends `{"method":op,"params":request}` in one ordinary SUBC Request.

- `identity-resync.json`: missing token requires a full identity read.
- `identity-changes.json`: a resumed watch returns one renamed agent and one
  retired agent. The avatar is a fingerprint, never the genome. Each version is
  the local journal generation at which that row changed.
- `identity-resumed.json`: two observations of **one** subscription, not two
  requests. The first has no frames, causally checked after the handler enters
  its wait. Updating the tag then yields the second observation's changes frame.
- `where-resync.json`: a missing cursor requires a full whereabouts read.
- `where-changes.json`: resume from seq 0 yields one cached whereabouts entry,
  including its process-local version. Unclaimed/known/null values retain the
  `agent.where_many` encoding.
- `watch-capacity.json`: the seventeenth watch, with eight identity watches and
  eight whereabouts watches already open, refuses with error code
  `watch_capacity` before starting.
- `SHA256SUMS`: hashes of the six JSON files, relative to this directory.

The incarnation is fixed to `0123456789abcdef`. Identity tokens/generations and
whereabouts seqs are local to one process on one machine; do not compare them between
machines. Identity notices also carry the last applied shared `logPosition`. On
`resync`, read `agent.fleet_identity` or `agent.where_many` and open a watch from
the new resume point. There is no full snapshot in a watch notice. A normal end,
including route cancellation or module draining, is `StreamEnd`, never Response.

Notices are debounced for 200 ms of quiet with a 1 s ceiling. Any changes body
over 192 KiB becomes a single small `resync`; notices are never split. Identity
resume is conservative: every journal entry in the bounded range must be an
agent identity after-image; project/workspace/root entries force resync because
they can change joined fleet fields without listing the affected agent ids.
Whereabouts retains 256 changed cache transactions or derived-view invalidations; a cursor outside that
ring (or from another incarnation) forces resync. A transaction increments seq
once if at least one cached claim changes, including known/stale/read timestamp
changes; unchanged transactions do not advance it. Member renames, learning the
machine's own author identity, sharing-mode changes, agent liveness changes and move
progress also advance seq with an all-agents resync marker. These fields are
derived outside the claim cache; the core's post-commit listener invalidates the
watch history without polling or adding engram calls.

## Reproduce and verify

From the repository root:

```sh
ENTO_WRITE_SERVED_VECTORS=1 cargo test --workspace --all-targets --locked served_fleet_watch_v1_records_match_published_files -- --nocapture
```

Without `ENTO_WRITE_SERVED_VECTORS`, that non-ignored test byte-compares the
served records with these files. All subscription tests use a paused Tokio
clock and causal readiness assertions rather than wall-clock delays.

Refresh hashes from the repository root:

```sh
python3 - <<'PY'
import hashlib
from pathlib import Path
root = Path('test-vectors/fleet-watch-v1')
root.joinpath('SHA256SUMS').write_text(''.join(
    f'{hashlib.sha256(p.read_bytes()).hexdigest()}  {p.name}\n'
    for p in sorted(root.glob('*.json'))), encoding='utf-8')
PY
```

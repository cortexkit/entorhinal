# Memory context v1

Entorhinal stores opaque project domains and operator-accepted workspace text.
It does not interpret domains or parse documents; agents produce drafts outside
entorhinal. `resolve` and `enumerate` are unchanged. Use the dedicated reads
below rather than extending the strict project-identity replies.

## Fleet upgrade ordering

Upgrade **every machine before setting a project domain or accepting a workspace
document**. Database migration 13 adds `project.domain` and `workspace_document`.
Pre-13 project after-images omit `domain`; upgraded readers treat only that
missing column as null. Upgraded writers also omit a null domain, preserving the
pre-13 project after-image bytes. Thus machines on the previous build can keep
applying ordinary unset-domain project changes during rollout. A non-null
domain or a workspace document requires the new build on all machines. Every other
missing or unknown shared column is still refused. Old shared history continues
to apply, join, replay, rebuild and verify without rewriting log bytes.

## Project domains

`register` accepts optional `domain` alongside its existing camelCase fields.
A domain is a canonical catalog package name (`@scope/name`, using
`entorhinal_catalog::names::map_name` and requiring the returned canonical
spelling). No domain allowlist is imposed; third parties can supply names.
Omission preserves an existing project's domain. Null means unset in storage.

`project.set_domain {project_id, domain, request_key}` sets a domain or clears it
with an explicit null. It is admitted from `reserved:prefrontal-core`, or Direct
after operator confirmation; all flow-scoped writes refuse. A domain changes the default
memory behavior of every agent in its project, so a local shell cannot silently
change it. Direct registration also prompts
when its supplied domain would change (including a new project's domain), but
not for omission or an unchanged value. A simultaneous name-claim carry and
domain change use one combined approval. Summaries over 200 Unicode scalars
refuse `operator_summary_too_long`, never truncate. The writer rechecks the
approved summary inside its transaction.

```
ck projects domain <project-id> @scope/name
ck projects domain <project-id> --clear
```

## Accepted workspace documents

`workspace.set_document {workspace_id, expected_revision, charter, document,
request_key}` is **Direct only, after operator confirmation**, including when core or an
agent tries to call it. The text is operator-authored policy for every member
project, not an agent-writable draft. The first write expects revision 0; each accepted write
inserts an immutable new revision. A stale expectation refuses
`revision_conflict` with `detail.current_revision`. Request-key retries return
the committed reply without another prompt. The prompt identifies workspace,
new revision, UTF-8 sizes and a 12-hex digest prefix. `accepted_via` is `direct`;
`accepted_at_ms` is the accepted write's fixed Unix-millisecond timestamp.

Charter is at most 4096 UTF-8 bytes; document at most 65536 UTF-8 bytes. Oversize
refuses `document_too_large` with `detail.field` and `detail.limit`. Both fields
refuse Unicode control characters other than tab, LF and CR as
`document_invalid_text`, naming `field` and the UTF-8 `byte_offset`. No newline
or Unicode normalization is performed. The encoded shared entry is independently
checked against the 262144-byte log cap before prompting and before commit;
`document_too_large` names `encoded_entry` if even its metadata exceeds that cap.
Tab, LF, CR, quotes and backslashes can double in JSON; a maximally escaped
within-limit document is replicated between two independent test stores to
check that the actual submitted entry fits the log cap.

The `sha256` identifies both accepted text fields, independent of revision and
acceptance metadata. Its input is exactly the concatenation:

1. ASCII `ck-workspace-document-v1` followed by a NUL byte;
2. unsigned 64-bit big-endian byte length of charter, then its UTF-8 bytes;
3. unsigned 64-bit big-endian byte length of document, then its UTF-8 bytes.

For charter `ab` and document `c`, SHA-256 is
`0b38e4d533a7177179b91823642e24568bcc8148dfb704e0760812c0edbc9864`.
Length framing distinguishes (`ab`, `c`) from (`a`, `bc`).

```
ck workspaces document set W --charter-file charter.txt --document-file policy.md
ck workspaces document set W --document-file revised.md --yes
ck workspaces document show W
ck workspaces document show W --revision 1
```

Without `--yes`, set prints a unified diff and changes nothing. With `--yes`,
it shows the diff, asks the operator to approve acceptance through the daemon's
presence prompt, and submits against the revision it just read.
Either file may be omitted to keep its accepted part (empty on the first write).
Concurrent acceptance refuses rather than overwriting another revision.
`workspace.document {workspace_id, revision?}` returns the full accepted record,
latest when omitted. Missing history refuses `revision_unknown`. Revisions are
shared and replayed, unlike machine-local workspace root directories. Removing
a workspace removes its document history with it.

## Coherent reads

These reads are open to every caller, including flows and unverified routes.
Each uses **one read transaction on the read-only WAL connection**, including
resolution, all rows, and its stamp. They do not call independent store reads
while assembling a response and never contact engram, the shared-log service.

`context.project {folder}` uses the exact `resolve` implementation, including
root-record mode and worktree discovery. Its reply contains:

- `project_id` (null if unregistered), `via`, `workspace_id`, `domain`;
- `document`: `{revision, sha256, charter_bytes, document_bytes}`, or null;
- `members_digest`: SHA-256 of workspace member project ids sorted in binary
  order and joined by LF, with no trailing LF (hash of empty bytes for no members);
  this uses shared `project_workspace`, not machine-local `workspace_member`;
- `stamp`: `{incarnation, generation, logPosition}`. Generation is this machine's
  journal sequence, logPosition its applied shared-log position (0 when disabled).

Without a workspace, document and members_digest are null, including an
agentless unplaced project. An unregistered folder has no invented project id.

`context.session {agent_id}` returns `agent` with exactly the fields from
`agent.resolve` (`agent_id`, `status`, `merged_into`, `gone`, `generation`),
`catalog_generations` (generation_id and state for active, degraded and ready
local generations), `pins` (memory-profile/persona only: kind, name, version,
manifest_sha256, archive_sha256, publisher_fingerprint, status), `where` (the same
cached answer as `agent.where`), and `stamp`. Unknown, gone and integrity-failed
agents refuse just as `agent.where` does. Local superseded/installing generations
and pins of unrelated manifest kinds are not exposed.

Each pin's `status` is the registry status entorhinal holds for that exact
version, read in the same transaction: `{"state", "blocked"}`, or `null` when no
status record exists yet. `state` is the signed record's state verbatim
(`available`, `withdrawn`, `revoked` or `tombstoned`). `blocked` is entorhinal's
own judgement: a tombstone always blocks, a revocation blocks only versions
published inside its compromise window, and a withdrawal never blocks. A reader
polling this reply to catch a mid-session revocation should act on `blocked`
rather than re-derive it from `state`.

Served fixtures for consumers are in
[`test-vectors/memory-context-v1/`](../test-vectors/memory-context-v1/).

# Module surface

Every operation entorhinal serves through the subc daemon, grouped by topic,
with the rule that decides who may call it. The manifest
(`manifest()` in `crates/entorhinal-module/src/main.rs`) declares each one as
a query or a mutation; `ck-entorhinal --manifest` prints it.

## Placement facts

- `placement.read {after?, limit?}` is open to every principal and uses the read
  connection. It returns `{"result":{"facts":[…],"next":…|null,"through":N}}`.
  `limit` defaults to 256 and must be 1–1024; unknown parameters and invalid
  limits or cursors refuse `invalid_request`. The opaque cursor carries the
  serving process's incarnation (its random per-start identifier); a cursor
  naming another incarnation restarts at position 0. `through`
  is the highest position served (or the starting position for an empty page).
- Each fact has fields in order: `fact_id`, `supersedes`, `project_id`,
  `workspace_id`, `effective_from_ms`, `recorded_at_ms`. The nullable fields
  `supersedes` and `workspace_id` are always present. Effective time is clamped
  non-decreasing per project; recorded time
  retains the writer clock. `supersedes` is currently always null.
- `placement.backfill {requestKey?}` is a one-time shared write for Direct clients
  (local processes such as `ck`), with operator presence confirmation
  (`ck projects placement-backfill`). It replays
  this machine's journal and ships exact rows, including deterministic ids.
  It refuses `sharing_not_enabled`, `placement_already_backfilled`,
  `placement_facts_exist`, or `placement_history_time_unknown` (with the seq).
  An empty history still activates future facts. After activation, every change
  to project existence or effective workspace journals its facts atomically,
  including registration, upgrades, seed import, compound writes and removal;
  same-workspace assignment produces none. Snapshots carry facts and activation
  to joined machines. Backfill ids are `b:<author>:<seq>:<project_id>`; later ids
  are `e:<entry_id>:<project_id>`. Request-key retries add nothing.
- The existing bus publisher also sends each unpublished fact, in insertion
  order, to `ck.{acct}.event.entorhinal.placement_fact.v1`. The body is exactly
  the serialized fact object above, including null fields and without a result
  wrapper, and must be under 4 KiB. `Nats-Msg-Id` is the `fact_id`. Local
  checkpoints are written only after the EVENT server acknowledges delivery;
  remote-applied facts are published too. Failures retry with the change
  publisher's backoff, independently of KV marker updates.

## Capabilities

entorhinal provides `project-identity/v1`, `agent-identity/v1`,
`permissions/v1` and `tool-provider/v1` (with the call-modes capability of the
tool-provider role). It requires none. Shared registry state lives on the hub;
reads use the local store and watches. Agent moves use engram's session door
to carry data, not to coordinate the move.

## Health

`ck health entorhinal` reports cached health gauges and request latency without
waiting for the store slot. The `latency` object has fixed `resolve`,
`resolve_project_id`, `enumerate`, `agent.resolve` and `other` buckets; each
contains `count`, `totalUs`, `maxUs`, `over500ms` and `over3s`. `storeUnavailable`
counts requests refused before storage was ready, and `readyAfterMs` records
the first successful store-open time from process start. Requests slower than
500 ms also emit one fleet warning; a `resolve` warning includes its
  `canonical_root`.

The following fields no longer change:

| Value | Metric keys |
|---|---|
| `0` | `logOwnEntriesAppliedFromLog`, `claimRefreshed`, `claimConflicts`, `claimFailed`, `claimVersionUnknown`, `claimSubjectMismatch`, `claimMalformed`, `selfAuthorConflicts`, `moveInboxRejected` |
| `false` | `roleRefreshFailed` |
| `null` | `claimError` |

Moves add three counters, each counting since the module started:
`sessionBindingsCarried` (session bindings this machine wrote into the
session-bindings stream of a move out), `sessionBindingsInstalled` (carried
bindings this machine installed as they were on the source) and
`sessionBindingsPlaceholders` (carried bindings it installed as placeholders
because another binding here already held their host or runner session).
See [agent-move-contract.md](agent-move-contract.md) section 11.

The bus publisher adds `busChangesPuts` (successful KV puts),
`busChangesFailures` (failed publishing attempts) and `busChangesConnected`
(whether the last publishing pass succeeded for both the KV change markers
described below and placement facts).
`placementFactsPublished` counts current facts with a local acknowledged
checkpoint; `placementFactsUnpublished` counts current facts still awaiting one.
Checkpoints whose facts no longer exist do not count. All are cached atomics;
a bus outage never delays health or registry reads.

After 60 seconds since the first failed explicit connection to the local
account bus, with no successful connection since, health reports `Degraded`
with detail `bus_unreachable` unless a store detail takes precedence. The
`busUnreachableSinceMs` metric gives the first failure's Unix timestamp in
milliseconds, even during the 60-second grace period. Any successful local
connection clears the streak and omits the metric. Deliberate closes do not
start a streak, and failures inside the bus SDK's automatic reconnect loop
are not visible to the module. The federated hub is still reported separately,
including `hubConnected` and `hubResetBucket`.

## Bus change markers

Entorhinal writes the account-local `entorhinal.changes` KV bucket, whose
physical name comes from `AccountNames::module_bucket_name("entorhinal", "changes")`.
ck-bus provisions it with history 1, no TTL and a 512-byte value cap;
prefrontal, Plexus and Magic Context have read access. The values are
revision hints only, never names, paths or rows. Watch a key, then re-read the
owning operation for data.

| Key | Value | Owning operation |
|---|---|---|
| `registry` | `{"generation":N,"incarnation":"…"}` | `enumerate`: journal head and process incarnation; any journal write may advance it. |
| `agents` | `{"generation":N,"incarnation":"…"}` | `agent.changes`: newest identity-change journal sequence, excluding project-only writes. |
| `profile` | `{"token":"…"}` | Unscoped `user.get`: exactly its content-derived change token. |
| `role.connector_home` | `{"epoch":0}` | Compatibility marker. It does not track hub role epochs; read `role.read` for the role's current view. |
| `catalog.status` | `{"generation":N}` | `catalog.status_watch`: the process-local pinned-version status generation. |
| `fleet_epoch` | `{"generation":N,"incarnation":"…"}` | `fleet.epoch_current`: advances when the verified committed hub epoch or highest committed epoch held here changes. |

One background task publishes all six keys on startup, including the zero
epoch marker. It wakes on journal commits, catalog status changes and fleet
epoch cache changes, and publishes only values that differ from its last
successful puts. When the bus connection changes, it republishes every key.
Role updates do not wake it. It uses a separate SubcConsumer client connection
and BusClient credentials, not the module's
registered connection. A missing bucket or unavailable bus is retryable with
1-second exponential backoff capped at 60 seconds: one warning starts the
outage and one recovery message reports the failed-attempt count. Nothing is
published without an open store. Stores from another machine do not publish.

## Admission, in general

The daemon stamps each route with a principal when it opens. entorhinal
applies these checks, in order, before decoding a request
(`ProjectsHandler::admit_with` and `authorize_write` in `main.rs`):

1. Operation-specific rules for the permission and user-profile operations.
2. **Flow scope.** Any mutation on a route opened under a flow's scope refuses
   `flow_scope_not_admitted`, whoever opened it.
3. **Write rule.** By group, as the tables below say. A route with no recorded
   principal never passes a write rule.

Except for the permission and user-profile queries, queries are open to every caller,
including flow-scoped and unverified routes. A mutation that needs operator
confirmation shows a prompt through the daemon, naming
the exact change. One prompt is open at a time (a second request refuses
`operator_confirmation_busy`); it is withdrawn when the caller's route closes;
the request gives up after 270 seconds. A decline or timeout refuses
`operator_declined`; a daemon or machine that cannot show the prompt refuses
`operator_presence_unavailable`. Linux has no presence provider, so on Linux
every such mutation refuses that way.

Principals in the tables:

| Name | Meaning |
|---|---|
| Direct | A local client such as `ck`. Every local process, including an agent's shell, connects as Direct. |
| executive | `reserved:prefrontal`, the module that attaches worker containers and relays the operator's agent changes. |
| orchestrator, host | The daemon's sole provider of `agent-orchestrator/v1` or `agent-host/v1`, resolved per route. |
| any | Every caller. |

Tests: `writes_are_refused_unless_the_operator_or_the_executive_opened_the_route`,
`self_admitted_methods_pass_the_write_gate_for_every_principal`,
`queries_are_answered_for_every_principal`,
`a_flow_scoped_route_cannot_write_even_with_the_executives_principal`,
`the_handler_refuses_a_write_on_a_bound_flow_route`,
`every_declared_mutation_is_write_checked`,
`route_closure_wins_ready_prompt_and_lock_and_admission_slot_gap`.

## Projects

Most project mutations are admitted from Direct or the executive; anyone else
gets `write_not_permitted`. `register`, `attach_root`, `add_root`, `approve_root`,
`approve_project`, `approve_projects`, `unapprove_root`, `unapprove_project`,
`set_workspace_root`,
`approve_workspace_root`, and `unapprove_workspace_root` are Direct-only, and
refuse the executive with `write_not_permitted`.
`attach_derived_parent` remains admitted from the executive without a prompt.
A Direct `assign_workspace`, `upgrade_implicit` or `seed_import` that would
move active agents' name claims into another workspace first asks for operator
confirmation. Domain changes
have the additional confirmation rule below. [projects-and-roots.md](projects-and-roots.md) covers the
concepts.

`register`, `attach_root`, `add_root`, `approve_root`, and `approve_project`
require one presence prompt naming the project and folders. Validation comes
first; if the checked target changes before committing (including checkout
replacement at the same path), the request refuses `operator_approval_stale`.
The registration and root additions approve bound checkouts in the same local
transaction. Neither folder approval nor checkout identity is sent to other
machines. The replies
include `folderApprovals` with `{root, state}` for each root and a `reason` for
each folder left unapproved because it could not be bound. Unapproving and
clearing a workspace root do not prompt.

`approve_projects` takes `{canonicalRoots, expected: [{projectId, root}], requestKey?, actor?}`
and resolves those paths to distinct projects. `expected` is the exact set of
folders shown in the client's preview as awaiting approval. If it differs from
the server's set, the batch refuses `operator_approval_stale` before prompting.
It also refuses before prompting if any root to be approved is unbound or
unverifiable, naming all such folders. It prompts
only if there are roots to approve. The reply is
`{approved: [{projectId, name, roots}], alreadyApproved: [{projectId, name}], generation}`.
`roots` lists only the folders newly approved on this machine. If any checked
project or folder changes while the prompt is open, the entire batch refuses
`operator_approval_stale`. Retrying a batch that wrote approvals with the same
`requestKey` returns the original reply without prompting again.

Setting or approving a workspace root prompts once, naming the workspace and
folder. Unlike a project checkout, a workspace root is approved for its current
`fileId`; a replacement at the same path reads as unapproved. A change during
the prompt refuses `operator_approval_stale`. When setting a root whose file
identity cannot be read, the write records the path without approving it, and
the reply includes `unapprovedReason: "file_id_unavailable"`. The three workspace
root mutations return the resulting `approval` when a root remains set.

| Operation | Kind | What it does |
|---|---|---|
| `resolve` | query | Resolve a filesystem path to its registered or implicit project identity. |
| `resolve_project_id` | query | Look up a project by its durable id and return its registration record. |
| `resolve_remote` | query | Find the project owning a GitHub repository through its live owned remotes. |
| `resolve_root_key` | query | List local roots mapped to a project's machine-neutral root key. |
| `preview_attach_root` | query | Match a checkout to an existing project and root key without attaching it. |
| `enumerate` | query | List registered projects with workspace assignments, tags and liveness activity. Workspace roots and root records carry an opaque `fileId` when the folder can be read. Workspaces with a root also include `approval: {journalSeq, state}`. |
| `journal_tail` | query | Read the newest registry journal entries. |
| `trust` | query | Show the project a path belongs to and each root's identity and approval. |
| `verify` | query | Check registry invariants and replay the journal on a copy, reporting drift without writing. |
| `register` | mutate | Register project roots under a workspace and approve those that can be bound. |
| `assign_workspace` | mutate | Move a registered project to a different workspace. |
| `set_workspace_root` | mutate | Set and approve a workspace's root directory in one write after confirmation, or clear it with `root: null` without a prompt. |
| `approve_workspace_root` | mutate | Approve the current root of `{workspaceId}` after confirmation. |
| `unapprove_workspace_root` | mutate | Revoke approval for `{workspaceId}` without changing its root. |
| `set_owned_remotes` | mutate | Replace a root's owned remote names, or reset to `origin` with null. |
| `upgrade_implicit` | mutate | Promote an implicit project to a registered one, keeping its id. |
| `remove` | mutate | Remove a project registration; its id stops resolving. |
| `add_root` | mutate | Add a root to a registered project and approve it if it can be bound. |
| `refresh_root_key` | mutate | Fill a registered root's missing repository key from its current owned remotes, preserving its binding and approval. |
| `attach_root` | mutate | Attach a local folder to an existing project's root key and approve it if it can be bound. |
| `rebind_root` | mutate | Rebind a replaced or unbound checkout, keeping its project id; approval is still required. |
| `remove_root` | mutate | Remove one root, retiring its binding and worker containers. |
| `move_root_to_new_project` | mutate | Operator only, without a presence prompt: move a checkout and its recorded repository key into a newly registered project, in one shared write. |
| `attach_derived_parent` | mutate | Attach a worker container to a root's current approved binding. |
| `approve_root`, `unapprove_root` | mutate | Approve or withdraw approval of one root's current binding. |
| `approve_project`, `unapprove_project` | mutate | The same for every root of the project a path belongs to; approval refuses unless every root is identified. |
| `approve_projects` | mutate | Approve the remaining roots of up to 100 distinct projects together, locally, with one presence prompt and one transaction. |
| `seed_import` | mutate | Bulk-import registrations from a seed file (recovery). |
| `rebuild` | mutate | Rebuild the derived tables by replaying the journal (recovery). |

`refresh_root_key` takes `{root}` and returns `{projectId, name, root, rootKey,
generation, noop}`. It fills only a missing key; repeating the same refresh is a
no-op. It requires the identity log to be enabled and refuses `identity_log_disabled`,
`remotes_unreadable`, `root_key_unavailable`, `root_key_exists`, or
`root_key_present` rather than replacing an existing different key.

`move_root_to_new_project` takes `{fromProjectId, root, name, actor?}` and
returns `{fromProjectId, projectId, name, root, rootKey, generation}` (plus the
usual `noop: false`). The new project has no workspace and the root is not
approved. Use `ck projects approve <dir>` and
`ck workspaces assign <project-id> <workspace-id>` before creating the project's
head agent (its lead agent).
The source must retain at least one local root; rename a single-root project
instead. A root outside the source project is refused, and `root_key_in_use`
names another local root if it still carries the same key. Refusals leave the
whole write unapplied. Roots without a recorded key remain unkeyed (`rootKey:
null`).

The journal reuses `remove_root`, `register` and `root_key.assign`. The key
transfer adds `root_key.release` with payload
`{project_id, kind, root_key}`. Like `root_key.assign`, it is visible in
`journal_tail`, never in the agent identity feed. Sharing sends one project
delta, not separate remove/register requests. A receiving machine keeps any
checkout registered under the old project but clears its released key; it
does not move that machine's folders to the new project. Ordinary `remove_root`
still retains the project's repository keys.

Tests: `attach_surface_writes_are_attributed_only_to_direct_and_core`,
`ownership_write_admission_and_principal_attribution`,
`rebind_wire_refuses_flow_and_unadmitted_principal_without_binding`,
`carrying_placement_prompts_exactly_and_cached_reply_is_unchanged`.

### Daemon control op

The daemon has no route of its own, so its settings gate reads the registry
through one channel-0 control op that entorhinal lists in its HELLO
`control_ops`. Only the daemon can send it, so there is no principal check.

| Op | Reply |
|---|---|
| `projects.enumerate` | `{op: "projects.enumerate", result}`, where `result` equals a routed `enumerate {}` result (`fileId` and `incarnation` included). It reads on the blocking pool with a 5 s deadline, inside the daemon's 10 s timeout. Before the store opens, or on a failed or slow read, it answers with an ERROR frame. |

Test: `projects_enumerate_control_op_matches_routed_enumerate`.

## User profile

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `user.get` | query | Direct, `reserved:prefrontal`, `reserved:callosum` | Read the fleet profile, all scoped expertise, and optionally a project's effective expertise. |
| `user.set` | mutate | Core and callosum without a prompt; Direct with operator confirmation | Set or clear one field, idempotently by request key. |

Both operations refuse flows and all other principals with `caller_forbidden`;
request fields cannot grant access. Callosum excludes `user.set` from its simulator
profile's own allowed-operation list; entorhinal does not distinguish that profile
from a phone profile.
There is no profile CLI command.

`user.get` accepts `{}`, `{"project_id":"pj-…"}`, `{"token":"…"}`, or both
keys. Its result contains `token`, `unchanged:false`, `user`, and `expertise`.
Unset global fields are omitted, never null. Expertise is ordered by scope
(workspaces first, then projects) and id. With `project_id`, an applicable line
also appears as `effective_expertise:{"text":"…","from":{"kind":"…","id":"…"}}`:
the project's own line overrides its placed workspace's. Unknown or implicit
project ids refuse `unknown_project`. A matching token returns only `token` and
`unchanged:true`. Tokens are opaque state digests; a project-context token also
covers effective expertise, so placement changes cannot yield a stale cache hit.
A token is meaningful only for the same request context it came from: reuse it
with the same `project_id`, or with none if the original read was unscoped.

`user.set` accepts `field`, required `value` (null clears), required
`request_key` (1–128 printable ASCII), optional `actor` (audit label, never in
replies), and `scope:{"kind":"workspace|project","id":"…"}` only for expertise.
Fields are `name` (1–64 characters), `address_as` (1–64), `pronouns` (1–32),
`computer_savviness` (integer 1–3), `verbosity` (`concise|balanced|verbose`),
`time_zone` (1–64), and `expertise` (1–300). Actor is 1–128 characters. Text is
trimmed and must be nonempty; controls, newlines, tabs, Unicode line separators,
bidi overrides and isolates are refused. Limits count Unicode characters, not bytes.

`time_zone` is person-level and global, returned inside `user` beside the other
global fields and omitted when unset. Local sets require an IANA name in the
bundled time zone database, including `UTC`, `Etc/GMT+3` and aliases such as
`US/Pacific`. Trimming preserves the exact name and casing without
canonicalization; unknown names refuse `invalid_value`. Null clears it.
Shared entries and join snapshots validate only the name's grammar: 1–64 ASCII
letters, digits, `/`, `_`, `+` or `-`, with no empty slash-separated path parts.
They never consult the receiving computer's time zone database, so an older
database cannot interrupt catch-up of a zone added on another computer.

Scopes must exist; implicit project ids refuse `unknown_scope`. Scope removal
cascades to expertise in the same transaction on either machine. A successful
set returns the same shape as an unscoped get. Repeating a request key with the
same body returns the original reply; another body refuses `request_key_reused`.
A no-op leaves the token unchanged. Bad shapes, unknown fields or global-field
scopes refuse `invalid_request`; bad values refuse `invalid_value`.

Direct writes validate before prompting, hold no store/writer lock during
confirmation, then revalidate and apply. The summary names the field, exact
scope and new value or `clear`; summaries over 200 characters refuse
`operator_summary_too_long`, never truncate. The usual operator confirmation
refusals apply. Longer values can be relayed
through core or callosum.

The two shared tables travel through the identity log as `user.set` entries and
join snapshots; sharing off keeps writes local. They participate in verify and
rebuild but never appear in `agent.changes` or project `journal_tail`. The log
retains earlier values after clearing, and its hosted entries are encrypted.
Time zones use the existing `user.set` field/value entry envelope; entries for
other fields keep their original bytes. Older readers refuse an unknown field,
so deploy readers that understand `time_zone` before publishing writes to it.
Published handler-generated samples: [user-profile-v1](../test-vectors/user-profile-v1/README.md).

## Memory context

[Memory context v1](memory-context.md) defines project domains, versioned accepted
workspace documents, coherent reads, digest encodings and fleet upgrade order.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `project.set_domain` | mutate | Direct with operator confirmation, executive | Set or clear a project's opaque memory domain. |
| `workspace.set_document` | mutate | Direct with operator confirmation only | Accept the next charter/document revision with optimistic concurrency. |
| `workspace.document` | query | any | Read an accepted revision, latest by default. |
| `context.project` | query | any | Resolve a folder and read its memory policy, document metadata, membership digest and stamp in one snapshot. |
| `context.session` | query | any | Read agent resolution, memory/persona pins, catalog generations, whereabouts and stamp in one snapshot. |

Direct `register` also asks for operator confirmation when a supplied domain differs from
the stored value (including a new project's first domain). Omission or the same
domain needs no domain confirmation. When registration also carries name claims, one combined prompt covers both changes.

## Agents

Agent identity begins in the executive and is imported once
(`agent.import`). Until then every identity write and session registration
refuses `authority_not_cut_over`, before any prompt. After that:

- `agent.create`, `agent.rename`, `agent.update_tag`, `agent.set_labels` and
  `agent.dispose` are admitted from the executive, or from Direct after Touch
  ID. A Direct write that would fail anyway is refused before the prompt.
- `agent.set_avatar`, `agent.set_github_identity`, `agent.merge` and
  `agent.import` are admitted from the executive only. Direct gets
  `direct_identity_write_not_admitted`; others `write_not_permitted`.
- `agent.repair_workspaces` is Direct only, after operator confirmation
  (`direct_caller_required` otherwise).
- `agent.apply_avatar_manifest` is also Direct only, with one operator confirmation for
  the batch. An all-skipped seed-only run succeeds without prompting or writing.
  Request fields are `entries` (the owner's exact per-agent manifest objects),
  `replace` (default false), `request_key`, and optional `actor`. The read-only
  `agent.preview_avatar_manifest` takes only `entries` and optional `replace`.
  All entries validate before approval, and the writer re-plans and checks the
  approved plan before applying anything. Each change uses its own transaction,
  standard `agent.set_avatar` shared entry, and `<batch key>:<agent_id>` request
  key, so replicas need no new identity entry type. Interrupted batches report
  written and remaining agents; retries replay completed entries and confirm the
  remaining writes once. Completed batches and all-skipped runs need no prompt.
- Writes carry a request key. A retry with the same key returns the stored
  reply without prompting again.

[agents.md](agents.md) is the operator guide.

| Operation | Kind | What it does |
|---|---|---|
| `agent.resolve` | query | One agent by id, including retired and merged agents. |
| `agent.resolve_name` | query | An agent by name within a workspace or the global namespace. |
| `agent.list` | query | Agents filtered by role, project or workspace, paged by agent id. |
| `agent.peer_roster` | query | A workspace's live heads: its workspace head and its projects' heads. |
| `agent.avatar_read` | query | The avatars of up to 64 agents. Each entry includes a `fingerprint` computed from the returned avatar; it equals `avatar.fingerprint` in `agent.fleet_identity` for the same stored avatar. |
| `agent.github_identity` | query | The GitHub identity bound to an agent; it names credentials and holds none. |
| `agent.fleet_identity` | query | Live agents with their project and workspace placement. |
| `agent.snapshot` | query | Every agent and name claim at one generation, to seed a copy. |
| `agent.changes` | query | Identity changes after a cursor, optionally waiting up to 25 seconds for the next one. |
| `agent.preview_repair_workspaces` | query | Preview what `agent.repair_workspaces` would change, including any refusal. |
| `agent.preview_avatar_manifest` | query | Validate and preview an avatar manifest, whose genomes come from the avatar owner's export tool, listing each agent by name. Without `replace`, agents that already have an avatar are listed as skipped. |
| `agent.create` | mutate | Create an agent. |
| `agent.rename` | mutate | Rename an agent. |
| `agent.update_tag` | mutate | Change an agent's tag. |
| `agent.set_labels` | mutate | Replace an agent's labels. |
| `agent.set_avatar` | mutate | Set an agent's avatar. |
| `agent.set_github_identity` | mutate | Bind or clear an agent's GitHub identity. |
| `agent.dispose` | mutate | Retire an agent; its id stays reserved. |
| `agent.merge` | mutate | Merge one agent into another and retire the source. |
| `agent.import` | mutate | Import the executive's agent registry once. |
| `agent.repair_workspaces` | mutate | Clear stored workspaces on heads and hirees and move their live name claims to their project's workspace. |
| `agent.apply_avatar_manifest` | mutate | Seed or replace owner-minted avatars with one confirmation and resumable per-agent writes. |

Tests: `agent_principal_matrix_precedes_decode_keys_and_cached_replies_before_and_after_cutover`,
`every_agent_mutation_is_flow_refused_for_every_principal_before_and_after_cutover`,
`all_reads_are_open_to_every_principal_before_and_after_cutover`,
`confirmed_direct_create_commits_with_the_exact_summary_and_route`,
`prechecks_replay_and_domain_refusals_do_not_wait_for_writer_or_prompt`,
`repair_preview_is_open_and_cutover_refusal_precedes_confirmation`.

## Agent tools and permissions

The tools (`projects_create_workspace`, `projects_register`,
`agents_create_head`, `agents_hire`, `agents_retire_hire`,
`agents_list_hires`, `catalog_prepare`, `catalog_plan`, `catalog_install`,
`catalog_generation`) are served through `tool-provider/v1`, with the caller
taken from the daemon's scope stamp. Their rules are in
[consent-contract.md](consent-contract.md).
Catalog tools are available to all live agent roles, and only for the calling agent:
`catalog_prepare` must target the caller, and `catalog_plan`, `catalog_install` and
`catalog_generation` act only on the caller's own plans and generations. Preparation
is asynchronous, installation reuses the consent coordinator, and generation reads
omit superseded generations, which are local bookkeeping. See [catalog.md](catalog.md#agent-tools-and-consumer-protocol)
for schemas, consumer kinds, refusals, idempotence and capability-token declarations.

All agent tool replies use legacy JSON and are checked against the tool-provider
limit of 65,536 model-visible bytes before sending. Oversized replies refuse with
`tool_result_too_large`; they are never silently shortened. Refusals' messages obey
the same limit. Write inputs are bounded before consent and revalidated before
commit, so their complete success replies fit with room to spare. The encoder
also checks writes as a safety net and logs a warning naming the tool if that
bound is ever violated.

For tool writes, workspace and project names are at most 1,024 UTF-8 bytes
(workspace names are measured after trimming). A registered root is at most
4,096 bytes, matching the session `project_root` limit. Workspace/project ids
retain their existing 512-byte identity limit; agent names retain their existing
24-scalar normalization rules, tags 256 bytes, and hire labels at most 16 strings
of 32 scalars each. A root's existing implicit-alias owner id must also fit 512
bytes, because registration echoes that id in collision warnings; choose a root
without that collision if it does not fit. These additional tool-only size
checks refuse `invalid_request` before a consent card or mutation. Existing
identity validation codes still apply. `catalog_install` accepts a `plan_id` of
1–128 printable ASCII bytes, the same limit as its consent request key.
Existing records and management reads are not shortened or rewritten.
A cached outcome stored before these caps could in principle still exceed the
limit on replay; the safety guard and tool-naming warning also cover that path.

`agents_list_hires {next?}` returns `{agents, next?}`: at most 50 of the caller's
live hires per page, fewer when needed to fit the byte limit. Pass the opaque
`next` string unchanged to get the following page; its absence marks the end.
Pagination follows agent id order and is not a snapshot: hires created or retired
between calls can change later pages. A single record that cannot fit refuses
`tool_result_too_large` instead of returning an empty, non-advancing page.

Catalog plans and generations are complete, internally related records (package
closures, checks, moves and consent previews). They are not split across pages.
If `catalog_plan` or `catalog_generation` refuses `tool_result_too_large`, prepare
a smaller package dependency set with fewer packages, checks or moves.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `permissions.list {agent_id?}` | query | Direct, `reserved:cingulate` | List live grants on this machine. |
| `permissions.revoke {id}` | mutate | Direct, `reserved:cingulate` | Revoke a grant without prompting. |
| `agent.audit_list {since_ms?, limit?, cursor?}` | query | Direct, `reserved:callosum` | Page the local audit of agent tool writes. |
| `agent.set_hire_cap {project_id, cap}` | mutate | Direct, with operator confirmation | Set this machine's hiring limit for a project. |
| `catalog.agent_generation {agent_id}` | query | `reserved:prefrontal` only | Read the agent's active or degraded package generation and any newer ready ones, with exact pin digests, from local state. A generation is one installed set of the agent's pinned packages, where a pin fixes a package's exact version and digests. `ready` generations wait for core to activate them, `active` is the set in use, and `degraded` is an active set that hit an error such as a revoked pin. Optional `pinned_status` lists non-available pins; a degradation error identifies the revoked skill by name, version and reason. |
| `catalog.ack_activation {agent_id, generation_id}` | mutate | `reserved:prefrontal` only | Idempotently activate a ready generation after core renders it. `activation_blocked` names a revoked/tombstoned pin; withdrawn pins remain usable. No network or consumer discovery. |

Refusals: `permissions_operator_required`, `audit_operator_required`,
`direct_caller_required`, `caller_forbidden`. These permission and catalog reads
are not open to every caller.

## Sessions, claims and roles

Claims say which of the operator's machines holds an agent or a role. The
hub's `KV_residence` watch maintains the local view; reads never wait on the
hub. Callers must not treat a stale claim as authority to act. Without a
selected hub, agent whereabouts report `unclaimed` at epoch 0: known on a
local store with sharing disabled, otherwise unknown and stale. Role reads
without a selected hub report an unobserved role: epoch 0, no holder or
source, unknown and stale. The fence version is 0. Local role leases still
work with sharing disabled.

`agent.register_session` accepts optional `role: "head" | "delegated"`.
Omission stores `head`, including for existing bindings; any other value
(including null or a non-string) refuses `invalid_request` before writing.
Re-registration, reference re-keying and host-kind changes update the session
role in place while preserving the existing network history. This field is
independent of `registered_by`, which records the admitting capability.
Registration replies and binding rows in `agent.sessions`, `agent.sessions_watch`,
`agent.session_for_ref`, `agent.session_for_runner` and `agent.session_for_harness`
include `role` only for `delegated`; head replies omit it, preserving existing
bytes. Head sessions move with the agent. Delegated sessions and their records
stay on the computer that launched them and do not block a move.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.register_session` | mutate | orchestrator or host | Bind an agent's session reference to its host session. Optional `project_root` is a 1–4096-byte string when sent (not null); omission keeps a stored root, or stores unknown for a new binding. Unknown roots read as null in `agent.session_for_ref`, `agent.sessions` and `agent.session_for_runner`. Required `runner` names the llm-runner module hosting it (1–64 bytes, no control characters); omission, null or an empty value refuses `invalid_request` before writing. Re-registration records the named runner, and any re-registration clears the end time. A runner and session id already bound to another binding refuse `session_bound_elsewhere`. Optional `network` (`yes`, `no` or `unknown`; `null` is absent, anything else refuses `invalid_request`) is appended to the binding's network history when it differs from the current value, stamped at commit; a full history refuses `network_history_full` and registers nothing. The reply never echoes `runner` or `network`. |
| `agent.set_session_network {harness, session, network}` | mutate | orchestrator only | Record whether the agent behind a host session's binding can reach the network: `yes`, `no` or `unknown`. Replies `{network, since_ms}`. The current value again writes nothing and replies the current entry's `since_ms`. `since_ms` is the module's clock when the entry commits, raised to one past the previous entry's if the clock is not past it, so each binding's history is strictly increasing. An unknown session refuses `not_found`; a value outside the three `invalid_request`; a binding already holding 1,024 entries `network_history_full`, with nothing written. |
| `agent.end_session {harness, session, runner?}` | mutate | orchestrator only | Mark the binding of a host session ended at the module's clock. Replies `{found: true, ended_at_ms}`, the first end time again on a repeat (nothing written), or `{found: false}` for an unknown session. A runner different from the recorded one refuses `runner_mismatch`. |
| `agent.sessions {agent_id, cursor?, limit?, open?}` | query | any | One agent's session bindings, newest registration first, ties by `(ref_kind, ref)`: `{sessions: [{runner, session, harness, ref_kind, ref, project_root, registered_at_ms, ended_at_ms, liveness, network}], next_cursor}`. `limit` defaults to 100, at most 500; `next_cursor` is opaque and null on the last page. `liveness` is `active`, `gone` or `unknown` as `agent.session_liveness` answers it, and `gone` when the mirror cannot vouch for a session whose binding has ended. `network` is the binding's current network value, `unknown` with no history. A retired agent still lists; an unknown id refuses `unknown_agent`. |
| `agent.sessions_watch {agent_id, open?}` | watch | any | Session binding changes for one agent, with the same rows and ordering as `agent.sessions`. Opens with `{kind:"resync", sessions:[...]}` when all rows fit its default page; otherwise `{kind:"resync", truncated:true}` tells the client to read `agent.sessions`. Subsequent `{kind:"changes", sessions:[...], removed:[{harness,session}]}` notices report committed registrations, rekeys, runner/harness changes, ends, reopenings, network changes, move imports and pruning. `open:true` excludes ended bindings and reports an end as removed. Unknown fields refuse `invalid_request`; unknown agents refuse `unknown_agent`. Buffer lag or an oversized notice forces resync rather than losing changes. Legacy bindings without host keys appear only in resyncs. Shares the 16-watch cap, 192 KiB notice limit and 200 ms quiet / 1 s ceiling debounce; ends on cancellation or drain. |
| `agent.session_for_runner {runner, session}` | query | any | The agent, harness, reference, project root and end time bound to a runner's session id, with `network` (the current value, `unknown` with no history) and `network_history: [{network, since_ms}]`, oldest first and empty with no history. Sessions registered without a runner are `not_found` here; look those up with `agent.session_for_harness {harness, session}`. |
| `agent.session_residence {runner, session}` | query | any | Whether this machine holds a session's agent, from one local read snapshot of its binding, open move, cached claim and self identity. Replies `{state, agent_id?, held_on?, holder?, holder_name?, epoch?, cut_id?, read_at_ms?}` with absent fields omitted, never null. Unknown fields, an empty or over-64-byte runner, control characters in the runner, or an empty or over-256-byte session refuse `invalid_request`. |
| `agent.session_for_harness {harness, session}` | query | any | Resolve a host session regardless of its runner, with exactly the same reply as `agent.session_for_runner`: `agent_id`, `runner` (null when unrecorded), `session`, `harness`, `ref_kind`, `ref`, `project_root`, `ended_at_ms`, `network`, `network_history` and `incarnation`, plus `heldOn` when data is on another machine. Requires a 1–32-byte harness and 1–256-byte session; unknown fields or malformed requests refuse `invalid_request`, and unknown sessions refuse `not_found`. |
| `agent.for_session` | query | any | The agent and reference bound to a host session. |
| `agent.session_for_ref` | query | any | The agent, host session and project root bound to a reference, with `runner` (the bare module id of the llm-runner hosting the session) only when the binding has one recorded; a binding without one omits the key rather than sending `null`. Re-registration must name a runner, including when the existing binding has none recorded. |
| `agent.session_liveness` | query | any | `active`, `gone` or `unknown` for up to 256 references, in order, from the trusted `prefrontal.session-liveness` bus mirror; `unknown` while the watch is unavailable or unreconciled. See [session-liveness-contract.md](session-liveness-contract.md). |
| `agent.where` | query | any | Which machine holds an agent, from the cached claim, plus any blocked move's participant and ref explanation. |
| `agent.where_many` | query | any | Cached claim results for 1–64 agent ids in request order; unknown agents are omitted. |
| `node.self {}` | query | any | This machine's identity-log identity: `{author: string or null, display_name: string or null, box_id?: string, sharing: "disabled" or "enabling" or "enabled"}` inside the usual `{result: ...}` envelope. Unknown params refuse `invalid_request`. |
| `agent.fleet_identity_watch` | watch | any | Stream of changes to `agent.fleet_identity`. Opens with `{token?}` and sends `resync` or `changes` notices; on `resync`, read `agent.fleet_identity` once. |
| `agent.where_watch` | watch | any | Stream of changes to whereabouts. Opens with `{agentIds?, cursor?}` and sends `resync` or `changes` notices carrying a cursor; on `resync`, read `agent.where_many` once. |
| `catalog.status_watch` | watch | any | Pinned package status invalidations. Opens with `{token?}`; `changes` lists `{name, version}` identities whose state, blocking judgement or stale marker changed. `resync` requires rereading generation or context status. Tokens bind the process incarnation and local status generation. Shares the 16-watch cap and 192 KiB notice limit. |
| `agent.refresh_claims` | mutate | Direct | Compatibility operation. Refuses `identity_log_disabled` when sharing is not enabled and `hub_claims_watched` when the hub backend is selected. While enabled but waiting for selection, returns empty refresh counts. |
| `role.read` | query | any | Which machine holds a role, from the cached claim. |
| `role.fence` | query | any | Cached fence for curation; on the hub the fence version is the KV revision of `role.<name>`. |
| `role.take` | mutate | Direct, with operator confirmation | Take a role or request handback on the hub. Setting the preferred machine while another current member holds the role replies `{epoch, held_here: false, handback_requested: true}`; a retry replays that outcome, not current possession. Without a selected hub, refuses `identity_log_disabled` if sharing is not enabled or `hub_not_ready` if it is. |
| `agent.take` | mutate | Direct, with operator confirmation | On a ready hub, take an agent whose holder can no longer serve it: a removed computer, a computer's earlier membership (it was removed and later paired again, so its old claims lapsed), or a legacy Engram author not mapped to a hub member (`engram:<author>`). It also claims agents carried over from Engram that have no location record on the hub yet. Refuses `holder_is_member` for a current holder, `claim_pending` while the creator must write the first claim, `claim_integrity_failed` for a refused claim, `hub_not_ready` while the hub view catches up, and `caller_forbidden`, `operator_declined`, or `operator_presence_unavailable` when authorization or confirmation fails. |

`node.self` reads one local read-only snapshot without contacting the hub or
writing a journal entry. `author` is the opaque identity-log author compared with
`agent.where` and `agent.where_many`'s `holder`, or null until this machine has
learned its author. `display_name` is this author's own member name, as set by
`ck projects log name` (`member.set_name`): the same value another machine sees
as `holder_name`, or null when unset or when there is no author. `sharing` is the
local store's sharing mode, without the `identity_log.status` health overlay.
`box_id` is this computer's daemon
machine id (32 lowercase hex), present once HELLO_ACK provides it even when
sharing is disabled.

Hub holder ids are daemon box ids, not transport fingerprints. Use
the Callosum roster to map a box id to its current fingerprint. A migrated
holder without a known box is `engram:<author>` and must not be dialed or
looked up in the roster.
`agent.where` and each `agent.where_many` row include `holder_box` when the box
is known; `holder_name` remains the display name. A changed box mapping
invalidates `agent.where_watch` just like a changed member name. With the hub
backend selected, an observed claim for this computer's own box remains
`held_here` but stale while membership loads.

Hub examples: [single location](../test-vectors/agent-sessions-v1/where-hub.json),
[batch locations](../test-vectors/agent-sessions-v1/where-many-hub.json), and
[fleet identity](../test-vectors/agent-identity-reads-v1/fleet-identity-hub.json).
Fleet identity identifies agents, not holders; join it to the locations by
agent id. Plexus and Magic Context can pin the hub
[role read](../test-vectors/role-claims-v1/read-hub.json),
[handback reply](../test-vectors/role-claims-v1/handback-requested.json), and
[fence](../test-vectors/role-claims-v1/fence-hub.json) examples.

On the hub, service modules hold a `role.lease {role}` watch to be candidates.
The holder renews every 20 s; act only within the confirmed monotonic window
in the notice (`issued_at_ms + valid_for_ms`), at most 45 s from the renewal's
send. Ack each notice via `role.lease.ack {role, seq}` within 5 s, and use
`role.lease.release {role, gen}` after stopping or `role.lease.yield {role, gen}`
after honoring a handback. These calls must use the route that opened the
watch. If no watch is registered for the role (for example, after a missed
ack), the call returns `lease_watch_closed`; open a new watch. If the watch is
still open on another route, the call returns `caller_forbidden`. An ack that
does not match a notice still awaiting acknowledgement returns `lease_ack_stale`.
Fence irreversible work by the lease `gen`; this is distinct from the
changing KV revision returned by `role.fence`. A reader must not infer a live
local lease from `role.read` or a successful `role.take`.
Once a member's removal is applied, the role lease held by that episode is void.
A candidate can acquire it without waiting for expiry or an operator take. A
computer that sees the replacement before the removal holds it until it applies
the removal or its own timer for the previous lease expires. A computer missing
only from the local roster does not count as removed. Operator takes remain
available; agent residence still requires a move or a forced take.

`agent.session_residence` is intended for restoring a session from backup before
making it writable. Its precedence is:

1. No `(runner, session)` binding: only `{state: "not_registered"}` (no move evidence).
2. The binding has `held_on`: `elsewhere`, with `agent_id` and `held_on` exactly as
   stored. This is a store-origin machine ID, not an engram author. No name is
   given because there is no trustworthy join to the author-keyed `member_name`
   table. `holder`, `holder_name` and claim fields are omitted in this branch.
3. The agent has a local open move, using the same definition as `agent.where`:
   `moving`, with `agent_id`. Wait before restoring writability.
4. An unread claim when sharing is not disabled, a stale or integrity-failed claim,
   or an epoch at least 1 without this machine's author identity: `unknown`.
5. A usable claim at epoch at least 1: `here` when held by this machine's author,
   otherwise `elsewhere`.
6. Epoch 0 with no holder: `unclaimed`. An absent claim also means verified
   `unclaimed` at epoch 0 while sharing has never been enabled, just as in `agent.where`.

From the claim checks onward the reply includes `agent_id` and the available
claim fields. With no cache row and sharing on, no claim fields are invented.
Only `here`, or `unclaimed` at epoch 0, permits making the restored session
writable; `elsewhere` and `unknown` require read-only access. Residence is stricter
than `agent.where`: the latter retains its existing status even for stale claims,
refuses integrity-failed claims, and reports a positive-epoch claim without self
identity as `held_elsewhere`. Neither read contacts the hub. Served examples are in
[`agent-session-residence-v1`](../test-vectors/agent-session-residence-v1/README.md).

Registering a different `ref` for the same `(harness, session_id)`, `agent_id`
and `ref_kind` re-keys the existing binding in the registration transaction.
Its network history follows the new key, its `registered_by` stays unchanged,
and its end time is cleared because registration means the session is live.
The runner is kept unless one is supplied, and the registration time is updated.
An occupied destination reference refuses `ref_bound_elsewhere`, even if it
belongs to the same agent; another agent or reference kind on the host session
still refuses `session_bound_elsewhere` with the existing binding's detail.
Open moves and cached claims fence re-keys exactly as other registrations.
Re-registering the same reference for its agent may still move it to a different
host session and record the new caller role, as before.

`agent.sessions` accepts optional boolean `open`. True selects only bindings whose
`ended_at_ms` is null, not a liveness-state filter; omitted or false lists every
binding with unchanged reply and cursor bytes. Ordering and keyset pagination
are unchanged. Open-only cursors carry an `open:` tag; using a cursor with the
other filter refuses `invalid_request`. Migration 17 adds a partial ordered
index for open-only paging, avoiding scans of an agent's finished backlog.

A moved agent's finished sessions are listed on the new machine, marked as held on
the source machine (or an earlier data-holding machine), and cannot be opened there
until a backfill. The nullable `session_binding.held_on` column records that
machine's store-origin ID, not its claim author. `agent.sessions`,
`agent.session_for_runner`, `agent.session_for_harness` and `agent.session_for_ref` include `heldOn` only
when non-null; local rows and returning data use null and omit the key.
Registration and same-agent re-keying refuse held-elsewhere rows as
`session_on_other_machine`, detail `{machine_id}`, with nothing written.
Moves carry only live-at-admission sessions as participant refs, fixed even if
they end during the move; finished rows travel only as metadata and network
history. More than 256 live refs refuses `live_ref_cap` with `{live_refs, cap}`
before recording anything. See [session binding moves](agent-move-contract.md#11-session-bindings)
for stream versions, installation and collision behavior.

Session bindings are machine-local and never journaled. A binding is marked
ended by `agent.end_session` or an observed DEL for a previously decoded
session in the authoritative bus mirror, never from missing keys or an
unavailable watch. The mirror requires a live watch, a finished initial replay
and a valid `_reconciled` marker. Bindings that ended more than 30 days ago are pruned shortly after
startup and hourly after that, except every binding of an agent with an open
move. A binding that never ended is never pruned. Held-elsewhere ended rows
follow the same 30-day rule; pruning removes only metadata and history, never
calls a participant, and never deletes the data still held on another machine.

Each binding keeps a history of whether its agent could reach the network,
written by the orchestrator, which knows it from the session's plan. The
history is machine-local and never journaled like the binding, and it is
deleted with the binding, by retention or any other path. Entorhinal stores
the values without interpreting them, and an absent value reads `unknown`,
never `no`. Readers find the value in effect at a time from the last entry at
or before it.

The orchestrator writes in this order. It writes `no` before it removes the
agent's network, and removes the network only after that write has landed.
It writes `yes` only after network has been granted. Each entry is stamped
when it commits, so with this order any lag between the real change and its
entry errs toward `no`: an entry never says `yes` for a time the agent had no
network.

Both watches are declared with watch delivery in the manifest. A notice is
at most 192 KiB; a larger batch becomes one `resync`. One route may hold at most
16 watches open, of any kind, and at most 64 are open on the machine; a watch past
either limit is refused `watch_capacity` before it streams. A watch
ends when its caller cancels, its route closes or the module drains. Served
examples of every notice shape are in `test-vectors/fleet-watch-v1/`.

Role names are 1–64 bytes of lowercase letters, digits and underscores.
`role.take` carries a request id and the epoch it expects; a retry resends
the approved attempt without prompting again. Other callers of
`agent.refresh_claims` and `role.take` get `caller_forbidden`.

Tests: `refresh_admission_empty_disabled_and_sync_seam_are_closed`,
`role_take_refuses_before_hub_selection_without_prompting_or_writing`,
`lease_handback_role_take_replays_preference_and_force_takes_removed_holder`.

## Moving agents between machines

Rules are in [agent-move-contract.md](agent-move-contract.md); the guide is
[moves.md](moves.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.move {agent_id, to, request_key}` | mutate | Direct with operator confirmation; orchestrator; `reserved:engram` while lid-close moves are on | Start moving an idle agent to a named machine. |
| `agent.move_status {cut_id}` | query | any | One move's local checkpoint, including any optional participant explanation for a diverged block. |
| `agent.move_refs {cutId}` | query | any | Read `{cutId, agentId, refs: [{ref_kind, ref}]}` from the local move's frozen plan, on source or receiver. Refs are sorted, with synthetic `agent`, `package-closure` and `session-bindings` refs excluded. `cutId` is 32 lowercase hex characters; an unknown cut refuses `unknown_move`, and unknown request members refuse `invalid_request`. |
| `agent.moves` | query | any | Open moves, then the 32 most recently finished, including optional participant explanations for diverged blocks. |
| `agent.move_abort {cut_id, request_key}` | mutate | Direct with operator confirmation; orchestrator | Cancel a move before its boundary. Only Direct can retry a stuck abort. |
| `agent.set_lid_close_moves {enabled, request_key}` | mutate | Direct with operator confirmation | Allow or deny engram's lid-close moves on this machine. |

## Shared identity log

Rules are in [hub-log-contract.md](hub-log-contract.md); the guide
is [sharing.md](sharing.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `identity_log.status` | query | any | Local log state, progress and pending writes, without contacting the hub. |
| `fleet.epoch_current {}` | query | any | Cached `{current?, highest_held?}`: the committed epoch named by the verified hub record and the highest committed epoch in this computer's vault. Pending mints do not count. Omit unknown members; reject unknown params with `invalid_request`. The read never contacts the hub or vault. Hold messages above `current` rather than dropping them: this computer's view can briefly lag another computer's rotation. |
| `identity_log.enable {without_agents?}` | mutate | Direct | Enable the shared log, or join it. |
| `member.set_name {display_name}` | mutate | Direct | Name this machine in the shared log. |

Other callers get `write_not_permitted` (`identity_log.enable`) or
`caller_forbidden` (`member.set_name`).

## Errors

A refusal is an error envelope with a `code`, a `message` and sometimes a
`detail`. Codes common to every operation:

| Code | Meaning |
|---|---|
| `invalid_request` | The request does not decode, or a field is out of bounds. |
| `invalid_params` | A project operation's parameters do not decode. |
| `unknown_method` | No such operation. |
| `storage_unavailable` | The store is not open yet. |
| `store_unavailable` | The store could not be opened at startup. |
| `legacy_sharing_unsupported` | The store still shares the old way and cannot open. See [the recovery instructions](sharing.md#when-something-is-wrong). |
| `storage_error` | A database error. |

Operation-specific codes are listed in the contracts.

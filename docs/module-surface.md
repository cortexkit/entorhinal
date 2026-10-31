# Module surface

Every operation entorhinal serves through the subc daemon, grouped by topic,
with the rule that decides who may call it. The manifest
(`manifest()` in `crates/entorhinal-module/src/main.rs`) declares each one as
a query or a mutation; `ck-entorhinal --manifest` prints it.

## Capabilities

entorhinal provides `project-identity/v1`, `agent-identity/v1`,
`permissions/v1` and `tool-provider/v1` (with the call-modes capability of the
tool-provider role). It requires none. It calls engram only to write or read
shared state and serves every read without it, so declaring engram `required`
would make the daemon refuse reads while engram is down.

## Health

`ck health entorhinal` reports cached health gauges and request latency without
waiting for the store slot. The `latency` object has fixed `resolve`,
`resolve_project_id`, `enumerate`, `agent.resolve` and `other` buckets; each
contains `count`, `totalUs`, `maxUs`, `over500ms` and `over3s`. `storeUnavailable`
counts requests refused before storage was ready, and `readyAfterMs` records
the first successful store-open time from process start. Requests slower than
500 ms also emit one fleet warning; a `resolve` warning includes its
`canonical_root`.

Moves add three counters, each counting since the module started:
`sessionBindingsCarried` (session bindings this machine wrote into the
session-bindings stream of a move out), `sessionBindingsInstalled` (carried
bindings this machine installed as they were on the source) and
`sessionBindingsPlaceholders` (carried bindings it installed as placeholders
because another binding here already held their host or runner session).
See [agent-move-contract.md](agent-move-contract.md) section 11.

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
| executive | `reserved:prefrontal-core`, the module that registers repositories and relays the operator's agent changes. |
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

Project mutations are admitted from Direct or the executive; anyone else gets
`write_not_permitted`. A Direct `register`, `assign_workspace`,
`upgrade_implicit` or `seed_import` that would carry live heads' and hirees'
name claims into another workspace first asks for operator confirmation. Domain changes
have the additional confirmation rule below. [projects-and-roots.md](projects-and-roots.md) covers the
concepts.

| Operation | Kind | What it does |
|---|---|---|
| `resolve` | query | Resolve a filesystem path to its registered or implicit project identity. |
| `resolve_project_id` | query | Look up a project by its durable id and return its registration record. |
| `resolve_remote` | query | Find the project owning a GitHub repository through its live owned remotes. |
| `resolve_root_key` | query | List local roots mapped to a project's machine-neutral root key. |
| `preview_attach_root` | query | Match a checkout to an existing project and root key without attaching it. |
| `enumerate` | query | List registered projects with workspace assignments, tags and liveness activity. |
| `journal_tail` | query | Read the newest registry journal entries. |
| `trust` | query | Show the project a path belongs to and each root's identity and approval. |
| `verify` | query | Check registry invariants and replay the journal on a copy, reporting drift without writing. |
| `register` | mutate | Register a project root under a workspace, minting its durable id. |
| `assign_workspace` | mutate | Move a registered project to a different workspace. |
| `set_workspace_root` | mutate | Set or clear a workspace's root directory. |
| `set_owned_remotes` | mutate | Replace a root's owned remote names, or reset to `origin` with null. |
| `upgrade_implicit` | mutate | Promote an implicit project to a registered one, keeping its id. |
| `remove` | mutate | Remove a project registration; its id stops resolving. |
| `add_root` | mutate | Add a root to a registered project; it starts unapproved. |
| `attach_root` | mutate | Attach a local checkout to an existing project's root key; it starts unapproved. |
| `rebind_root` | mutate | Rebind a replaced or unbound checkout, keeping its project id; approval is still required. |
| `remove_root` | mutate | Remove one root, retiring its binding and worker containers. |
| `move_root_to_new_project` | mutate | Operator only, without a presence prompt: move a checkout and its recorded repository key into a newly registered project, in one shared write. |
| `attach_derived_parent` | mutate | Attach a worker container to a root's current approved binding. |
| `approve_root`, `unapprove_root` | mutate | Approve or withdraw approval of one root's current binding. |
| `approve_project`, `unapprove_project` | mutate | The same for every root of the project a path belongs to; approval refuses unless every root is identified. |
| `seed_import` | mutate | Bulk-import registrations from a seed file (recovery). |
| `rebuild` | mutate | Rebuild the derived tables by replaying the journal (recovery). |
| `projects.session_liveness` | mutate | The session liveness feed. Host only; never journaled. See [session-liveness-contract.md](session-liveness-contract.md). |

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

## User profile

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `user.get` | query | Direct, `reserved:prefrontal-core`, `reserved:callosum` | Read the fleet profile, all scoped expertise, and optionally a project's effective expertise. |
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
and `expertise` (1–300). Actor is 1–128 characters. Text is trimmed and must be
nonempty; controls, newlines, tabs, Unicode line separators, bidi overrides and
isolates are refused. Limits count Unicode characters, not bytes.

Scopes must exist; implicit project ids refuse `unknown_scope`. Scope removal
cascades to expertise in the same transaction on either machine. A successful
set returns the same shape as an unscoped get. Repeating a request key with the
same body returns the original reply; another body refuses `request_key_reused`.
A no-op leaves the token unchanged. Bad shapes, unknown fields or global-field
scopes refuse `invalid_request`; bad values refuse `invalid_value`.

Direct writes validate before prompting, hold no store/writer lock during
confirmation, then revalidate and apply. The summary names the field, exact
scope and new value or `clear`; summaries over 200 characters refuse
`operator_summary_too_long`, never truncate. The normal confirmation busy,
declined and presence-unavailable refusals apply. Longer values can be relayed
through core or callosum.

The two shared tables travel through the identity log as `user.set` entries and
join snapshots; sharing off keeps writes local. They participate in verify and
rebuild but never appear in `agent.changes` or project `journal_tail`. The log
retains earlier values after clearing, and its hosted entries are encrypted.
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
| `agent.avatar_read` | query | The avatars of up to 64 agents. |
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

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `permissions.list {agent_id?}` | query | Direct, `reserved:cingulate` | List live grants on this machine. |
| `permissions.revoke {id}` | mutate | Direct, `reserved:cingulate` | Revoke a grant without prompting. |
| `agent.audit_list {since_ms?, limit?, cursor?}` | query | Direct, `reserved:callosum` | Page the local audit of agent tool writes. |
| `agent.set_hire_cap {project_id, cap}` | mutate | Direct, with operator confirmation | Set this machine's hiring limit for a project. |
| `catalog.agent_generation {agent_id}` | query | `reserved:prefrontal-core` only | Read the agent's active or degraded package generation and any newer ready ones, with exact pin digests, from local state. A generation is one installed set of the agent's pinned packages, where a pin fixes a package's exact version and digests. `ready` generations wait for core to activate them, `active` is the set in use, and `degraded` is an active set that hit an error such as a revoked pin. Optional `pinned_status` lists non-available pins; a degradation error identifies the revoked skill by name, version and reason. |
| `catalog.ack_activation {agent_id, generation_id}` | mutate | `reserved:prefrontal-core` only | Idempotently activate a ready generation after core renders it. `activation_blocked` names a revoked/tombstoned pin; withdrawn pins remain usable. No network or consumer discovery. |

Refusals: `permissions_operator_required`, `audit_operator_required`,
`direct_caller_required`, `caller_forbidden`. These permission and catalog reads
are not open to every caller.

## Sessions, claims and roles

Claims say which of the operator's machines holds an agent or a role. They are
records in engram that entorhinal caches locally; every read here answers from
the cache, so it works offline and never waits on engram.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.register_session` | mutate | orchestrator or host | Bind an agent's session reference to its host session. Optional `project_root` is a 1–4096-byte string when sent (not null); omission keeps a stored root, or stores unknown for a new binding. Unknown roots read as null in `agent.session_for_ref`, `agent.sessions` and `agent.session_for_runner`. Optional `runner` names the llm-runner module hosting it; re-registering without one keeps the recorded runner, and any re-registration clears the end time. A runner and session id already bound to another binding refuse `session_bound_elsewhere`. Optional `network` (`yes`, `no` or `unknown`; `null` is absent, anything else refuses `invalid_request`) is appended to the binding's network history when it differs from the current value, stamped at commit; a full history refuses `network_history_full` and registers nothing. The reply never echoes `runner` or `network`. |
| `agent.set_session_network {harness, session, network}` | mutate | orchestrator only | Record whether the agent behind a host session's binding can reach the network: `yes`, `no` or `unknown`. Replies `{network, since_ms}`. The current value again writes nothing and replies the current entry's `since_ms`. `since_ms` is the module's clock when the entry commits, raised to one past the previous entry's if the clock is not past it, so each binding's history is strictly increasing. An unknown session refuses `not_found`; a value outside the three `invalid_request`; a binding already holding 1,024 entries `network_history_full`, with nothing written. |
| `agent.end_session {harness, session, runner?}` | mutate | orchestrator only | Mark the binding of a host session ended at the module's clock. Replies `{found: true, ended_at_ms}`, the first end time again on a repeat (nothing written), or `{found: false}` for an unknown session. A runner different from the recorded one refuses `runner_mismatch`. |
| `agent.sessions {agent_id, cursor?, limit?, open?}` | query | any | One agent's session bindings, newest registration first, ties by `(ref_kind, ref)`: `{sessions: [{runner, session, harness, ref_kind, ref, project_root, registered_at_ms, ended_at_ms, liveness, network}], next_cursor}`. `limit` defaults to 100, at most 500; `next_cursor` is opaque and null on the last page. `liveness` is `active`, `gone` or `unknown` as `agent.session_liveness` answers it, and `gone` when the mirror cannot vouch for a session whose binding has ended. `network` is the binding's current network value, `unknown` with no history. A retired agent still lists; an unknown id refuses `unknown_agent`. |
| `agent.session_for_runner {runner, session}` | query | any | The agent, harness, reference, project root and end time bound to a runner's session id, with `network` (the current value, `unknown` with no history) and `network_history: [{network, since_ms}]`, oldest first and empty with no history. Sessions registered without a runner are `not_found` here; look those up with `agent.session_for_harness {harness, session}`. |
| `agent.session_for_harness {harness, session}` | query | any | Resolve a host session regardless of its runner, with exactly the same reply as `agent.session_for_runner`: `agent_id`, `runner` (null when unrecorded), `session`, `harness`, `ref_kind`, `ref`, `project_root`, `ended_at_ms`, `network`, `network_history` and `incarnation`, plus `heldOn` when data is on another machine. Requires a 1–32-byte harness and 1–256-byte session; unknown fields or malformed requests refuse `invalid_request`, and unknown sessions refuse `not_found`. |
| `agent.for_session` | query | any | The agent and reference bound to a host session. |
| `agent.session_for_ref` | query | any | The agent, host session and project root bound to a reference, with `runner` (the bare module id of the llm-runner hosting the session) only when the binding has one recorded; a binding without one omits the key rather than sending `null`. Re-registering without a runner keeps the recorded one, so the reply still names it. |
| `agent.session_liveness` | query | any | `active`, `gone` or `unknown` for up to 256 references, in order. |
| `agent.where` | query | any | Which machine holds an agent, from the cached claim, plus any blocked move's participant and ref explanation. |
| `agent.where_many` | query | any | Cached claim results for 1–64 agent ids in request order; unknown agents are omitted. |
| `agent.fleet_identity_watch` | watch | any | Stream of changes to `agent.fleet_identity`. Opens with `{token?}` and sends `resync` or `changes` notices; on `resync`, read `agent.fleet_identity` once. |
| `agent.where_watch` | watch | any | Stream of changes to whereabouts. Opens with `{agentIds?, cursor?}` and sends `resync` or `changes` notices carrying a cursor; on `resync`, read `agent.where_many` once. |
| `agent.refresh_claims` | mutate | Direct | Refresh live agents' cached claims from engram. |
| `role.read` | query | any | Which machine holds a role, from the cached claim. |
| `role.take` | mutate | Direct, with operator confirmation | Make this machine the role's holder. |

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
ended by `agent.end_session` or when the host's liveness feed reports the
session `gone` (never from `lapsed`, `unknown` or absence from a snapshot); a
feed entry's optional `runner` limits that to a binding of the same runner or
of none. Bindings that ended more than 30 days ago are pruned shortly after
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
at most 192 KiB; a larger batch becomes one `resync`. At most 16 watches are
open at once, and a 17th is refused `watch_capacity` before it streams. A watch
ends when its caller cancels, its route closes or the module drains. Served
examples of every notice shape are in `test-vectors/fleet-watch-v1/`.

Role names are 1–64 bytes of lowercase letters, digits and underscores.
`role.take` carries a request id and the epoch it expects; a retry resends
the approved attempt without prompting again. Other callers of
`agent.refresh_claims` and `role.take` get `caller_forbidden`.

Tests: `refresh_admission_empty_disabled_and_sync_seam_are_closed`,
`role_take_is_confirmed_direct_only_and_ledger_precedes_prompts`,
`role_lost_reply_retries_exact_attempt_without_another_prompt`.

## Moving agents between machines

Rules are in [agent-move-contract.md](agent-move-contract.md); the guide is
[moves.md](moves.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.move {agent_id, to, request_key}` | mutate | Direct with operator confirmation; orchestrator; `reserved:engram` while lid-close moves are on | Start moving an idle agent to a named machine. |
| `agent.move_status {cut_id}` | query | any | One move's local checkpoint, including any optional participant explanation for a diverged block. |
| `agent.moves` | query | any | Open moves, then the 32 most recently finished, including optional participant explanations for diverged blocks. |
| `agent.move_abort {cut_id, request_key}` | mutate | Direct with operator confirmation; orchestrator | Cancel a move before its boundary. Only Direct can retry a stuck abort. |
| `agent.set_lid_close_moves {enabled, request_key}` | mutate | Direct with operator confirmation | Allow or deny engram's lid-close moves on this machine. |

## Shared identity log

Rules are in [identity-log-contract.md](identity-log-contract.md); the guide
is [sharing.md](sharing.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `identity_log.status` | query | any | Local log state, progress and pending writes, without contacting engram. |
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
| `storage_error` | A database error. |

Operation-specific codes are listed in the contracts.

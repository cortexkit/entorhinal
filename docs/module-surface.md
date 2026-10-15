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

## Admission, in general

The daemon stamps each route with a principal when it opens. entorhinal
applies these checks, in order, before decoding a request
(`ProjectsHandler::admit_with` and `authorize_write` in `main.rs`):

1. Operation-specific rules for the permission operations.
2. **Flow scope.** Any mutation on a route opened under a flow's scope refuses
   `flow_scope_not_admitted`, whoever opened it.
3. **Write rule.** By group, as the tables below say. A route with no recorded
   principal never passes a write rule.

Every query is open to every caller, including flow-scoped and unverified
routes. A mutation that needs the operator's confirmation shows a Touch ID
prompt through the daemon, naming the exact change. One prompt is open at a
time (a second request refuses `operator_confirmation_busy`); it is withdrawn
when the caller's route closes; the request gives up after 270 seconds. A
decline or timeout refuses `operator_declined`; a daemon or Mac that cannot
show the prompt refuses `operator_presence_unavailable`.

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
name claims into another workspace first asks for Touch ID; otherwise none of
these prompt. [projects-and-roots.md](projects-and-roots.md) covers the
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
| `attach_derived_parent` | mutate | Attach a worker container to a root's current approved binding. |
| `approve_root`, `unapprove_root` | mutate | Approve or withdraw approval of one root's current binding. |
| `approve_project`, `unapprove_project` | mutate | The same for every root of the project a path belongs to; approval refuses unless every root is identified. |
| `seed_import` | mutate | Bulk-import registrations from a seed file (recovery). |
| `rebuild` | mutate | Rebuild the derived tables by replaying the journal (recovery). |
| `projects.session_liveness` | mutate | The session liveness feed. Host only; never journaled. See [session-liveness-contract.md](session-liveness-contract.md). |

Tests: `attach_surface_writes_are_attributed_only_to_direct_and_core`,
`ownership_write_admission_and_principal_attribution`,
`rebind_wire_refuses_flow_and_unadmitted_principal_without_binding`,
`carrying_placement_prompts_exactly_and_cached_reply_is_unchanged`.

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
- `agent.repair_workspaces` is Direct only, after Touch ID
  (`direct_caller_required` otherwise).
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
Catalog tools are available to all live agent roles, self-targeted only; preparation
is asynchronous, installation reuses the consent coordinator, and generation reads
omit superseded handoff bookkeeping. See [catalog.md](catalog.md#agent-tools-and-consumer-protocol)
for schemas, consumer kinds, refusals, idempotence and capability-token declarations.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `permissions.list {agent_id?}` | query | Direct, `reserved:cingulate` | List live grants on this Mac. |
| `permissions.revoke {id}` | mutate | Direct, `reserved:cingulate` | Revoke a grant without prompting. |
| `agent.audit_list {since_ms?, limit?, cursor?}` | query | Direct, `reserved:callosum` | Page the local audit of agent tool writes. |
| `agent.set_hire_cap {project_id, cap}` | mutate | Direct, with Touch ID | Set this Mac's hiring limit for a project. |
| `catalog.agent_generation {agent_id}` | query | `reserved:prefrontal-core` only | Read active and newer ready package generations and exact pin digests from local state. |
| `catalog.ack_activation {agent_id, generation_id}` | mutate | `reserved:prefrontal-core` only | Idempotently activate a ready generation after core renders it. No network or consumer discovery. |

Refusals: `permissions_operator_required`, `audit_operator_required`,
`direct_caller_required`, `caller_forbidden`. These permission and catalog reads
are not open to every caller.

## Sessions, claims and roles

Claims say which of the operator's Macs holds an agent or a role. They are
records in engram that entorhinal caches locally; every read here answers from
the cache, so it works offline and never waits on engram.

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.register_session` | mutate | orchestrator or host | Bind an agent's session reference to its host session. |
| `agent.for_session` | query | any | The agent and reference bound to a host session. |
| `agent.session_for_ref` | query | any | The agent, host session and project root bound to a reference. |
| `agent.session_liveness` | query | any | `active`, `gone` or `unknown` for up to 256 references, in order. |
| `agent.where` | query | any | Which Mac holds an agent, from the cached claim. |
| `agent.refresh_claims` | mutate | Direct | Refresh live agents' cached claims from engram. |
| `role.read` | query | any | Which Mac holds a role, from the cached claim. |
| `role.take` | mutate | Direct, with Touch ID | Make this Mac the role's holder. |

Role names are 1–64 bytes of lowercase letters, digits and underscores.
`role.take` carries a request id and the epoch it expects; a retry resends
the approved attempt without prompting again. Other callers of
`agent.refresh_claims` and `role.take` get `caller_forbidden`.

Tests: `refresh_admission_empty_disabled_and_sync_seam_are_closed`,
`role_take_is_confirmed_direct_only_and_ledger_precedes_prompts`,
`role_lost_reply_retries_exact_attempt_without_another_prompt`.

## Moving agents between Macs

Rules are in [agent-move-contract.md](agent-move-contract.md); the guide is
[moves.md](moves.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `agent.move {agent_id, to, request_key}` | mutate | Direct with Touch ID; orchestrator; `reserved:engram` while lid-close moves are on | Start moving an idle agent to a named Mac. |
| `agent.move_status {cut_id}` | query | any | One move's local checkpoint, holds, warnings and diagnostics. |
| `agent.moves` | query | any | Open moves, then the 32 most recently finished. |
| `agent.move_abort {cut_id, request_key}` | mutate | Direct with Touch ID; orchestrator | Cancel a move before its boundary. Only Direct can retry a stuck abort. |
| `agent.set_lid_close_moves {enabled, request_key}` | mutate | Direct with Touch ID | Allow or deny engram's lid-close moves on this Mac. |

## Shared identity log

Rules are in [identity-log-contract.md](identity-log-contract.md); the guide
is [sharing.md](sharing.md).

| Operation | Kind | Who | What it does |
|---|---|---|---|
| `identity_log.status` | query | any | Local log state, progress and pending writes, without contacting engram. |
| `identity_log.enable {without_agents?}` | mutate | Direct | Enable the shared log, or join it. |
| `member.set_name {display_name}` | mutate | Direct | Name this Mac in the shared log. |

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

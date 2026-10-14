# entorhinal

The project registry for [CortexKit](https://github.com/cortexkit/subconscious).
It gives every project a durable id (`pj-…`) and answers which project a
directory belongs to, so modules that keep per-project state agree on what a
project is, even when a directory moves or has several names.

entorhinal runs as a module supervised by the subc daemon. It provides the
`project-identity/v1` capability; modules that cannot work without project
identity declare that capability `required`, and the daemon holds them
unroutable until entorhinal has registered.

## Commands

The binary `ck-entorhinal` is the module itself. People use it through `ck`:

```
ck projects list [workspace]        projects, optionally scoped to one workspace
ck projects resolve <dir>           which project owns a directory
ck projects register <name> <dir>   register a project rooted at a directory
ck projects rebind <dir>            repair a replaced checkout; then approve it
ck projects remove <project>        remove a project from the registry
ck projects verify                  check the store against its journal
ck projects log status              local state of the shared identity log
ck projects log name <display name> name this Mac in the shared log

ck workspaces list                          workspaces known to the registry
ck workspaces assign <project> <workspace>  put a project in a workspace

ck agents list [--project <id>] [--workspace <id>]
ck agents show <agent>                      one agent, live or retired
ck agents create <name> --role <role> --tag <text> [options]
ck agents rename <agent> <new-name>
ck agents retire <agent>
ck agents tag <agent> <text>
ck agents labels <agent> <label>...         replace the whole label set
ck agents labels <agent> --none             clear the label set
ck agents hire-cap <project> <n>            set this Mac's agent-hiring limit (Touch ID)
ck agents repair-workspaces [--yes]         preview correction; --yes sends the write
ck agents where <agent> [--refresh]         which Mac holds the agent, from the cache
ck agents move <agent> --to <mac>           move an idle agent to another named Mac (Touch ID)
ck agents moves                             open moves and the last 32 finished ones
ck agents move-abort <cut_id>               cancel a move before it activates (Touch ID)
ck agents lid-close-moves on|off            allow or deny engram's lid-close moves (Touch ID)

ck roles read <role>                        which Mac holds a role, from the cache
ck roles take <role> --reason <text>        make this Mac the role's holder
```

`ck projects`, `ck workspaces`, `ck agents` or `ck roles` on its own prints the full usage,
including the roles and options `create` accepts. `--json` prints the raw
response. `ck-entorhinal --manifest` prints the module manifest as JSON without
connecting to anything, for offline checks such as `ck fleet lint`.

An agent write from `ck agents` lands only after the operator approves it at a
Touch ID prompt that names the exact change; so does `ck roles take`. The subc daemon shows the prompt,
one at a time, and a write that would fail anyway is refused before any prompt
appears. The write waits up to five minutes for the answer. If its outcome is
unknown, for example after a timeout, the command prints a retry command that
is safe to run: the retry can neither create a duplicate nor change a different
agent. Until agent identity has moved from prefrontal-core to entorhinal, every
identity write is refused with `authority_not_cut_over`, before any prompt.

`hire-cap` takes a project id and a non-negative integer. The default is 10 live
hires per project; 0 stops agent hiring. Lowering the cap below the current count
does not retire anyone. Only agent-requested hires are capped: operator creation,
the executive relay, imports and synced identities are not. Caps are local to
this Mac, and changing one always requires Touch ID.

## Operations

The registry store must be on a local disk. SQLite WAL and shared memory are
not supported on network shares.

Served to other modules through the subc daemon, which routes management calls
between CortexKit modules. Identity reads are open to every caller; permission
inventory and the agent authorization audit have the operator-only rules below.
Project writes are accepted from the operator (`ck`) or the executive
(prefrontal-core). Agent writes are accepted from the executive, which relays
the operator's own changes, and from the operator through `ck agents` once the
Touch ID prompt approves them. A flow (an automation an agent installs to run
without a session) can only read: the daemon marks each request a flow makes,
even when the executive forwards it, and entorhinal refuses every write that
carries that mark, so a flow can't borrow the executive's write access.

**Projects**

| Operation | Kind | What it does |
|---|---|---|
| `resolve` | query | Resolve a filesystem path to its registered or implicit project identity. |
| `resolve_project_id` | query | Look up a project by its durable project id and return its registration record. |
| `resolve_remote` | query | Find the project owning a GitHub repository through its live owned remotes. |
| `enumerate` | query | List all registered projects with workspace assignments and tags. |
| `journal_tail` | query | Read the newest registry journal entries for operator inspection. |
| `trust` | query | Show the project a path belongs to and each root's identity and approval. |
| `verify` | query | Check registry invariants and replay the journal to report drift, without writing. |
| `register` | mutate | Register a project root under a workspace, minting its durable project id. |
| `assign_workspace` | mutate | Move a registered project to a different workspace. |
| `set_workspace_root` | mutate | Set or clear a workspace's root directory (operator-set, never derived). |
| `upgrade_implicit` | mutate | Promote an implicit (path-derived) project to a registered one, keeping its id. |
| `remove` | mutate | Remove a project registration; its id stops resolving. |
| `add_root` | mutate | Add a root to a registered project; it starts unapproved. |
| `rebind_root` | mutate | Rebind a replaced or unbound registered checkout, keeping its project id; approval is still required. An intact binding is unchanged. |
| `remove_root` | mutate | Remove one root from a project, retiring its binding and worker containers. |
| `attach_derived_parent` | mutate | Attach a worker container to a root's current approved binding. |
| `approve_root` | mutate | Approve a root's current binding for autonomous work. |
| `unapprove_root` | mutate | Withdraw a root's approval. |
| `approve_project` | mutate | Approve every root of the project a path belongs to; refused unless all are identified. |
| `unapprove_project` | mutate | Withdraw approval from every root of the project a path belongs to. |
| `set_owned_remotes` | mutate | Replace a root's owned remote names, or reset to `origin` with null. |
| `seed_import` | mutate | Bulk-import registrations from a seed file (operator recovery path). |
| `rebuild` | mutate | Rebuild registry tables by replaying the journal, reporting what changed (operator recovery path). |
| `projects.session_liveness` | mutate | Ingest session liveness snapshots and deltas from the executive for dead-folder detection. |

**Agents**

| Operation | Kind | What it does |
|---|---|---|
| `agent.resolve` | query | Look up one agent by id, including retired and merged agents. |
| `agent.resolve_name` | query | Find an agent by name within a workspace or the global namespace. |
| `agent.list` | query | List agents, filtered by role, project or workspace and paged by agent id. |
| `agent.peer_roster` | query | List a workspace's live heads: its workspace head and the heads of its projects. |
| `agent.avatar_read` | query | Read the avatars of up to 64 agents. |
| `agent.github_identity` | query | Read the GitHub identity bound to an agent; it names credentials and holds none. |
| `agent.fleet_identity` | query | List live agents with their project and workspace placement for the fleet view. |
| `agent.snapshot` | query | Read every agent and name claim at one generation, to seed a copy. |
| `agent.changes` | query | Read identity changes after a cursor, optionally waiting up to 25 s for the next one. |
| `agent.create` | mutate | Create an agent. |
| `agent.rename` | mutate | Rename an agent. |
| `agent.update_tag` | mutate | Change an agent's tag. |
| `agent.set_labels` | mutate | Replace an agent's labels. |
| `agent.set_avatar` | mutate | Set an agent's avatar. |
| `agent.set_github_identity` | mutate | Bind or clear an agent's GitHub identity. |
| `agent.dispose` | mutate | Retire an agent; its id stays reserved. |
| `agent.merge` | mutate | Merge one agent into another and retire the source. |
| `agent.import` | mutate | Import the executive's agent registry once, at cutover. |
| `agent.preview_repair_workspaces` | query | Preview the stored workspaces and name claims the operator's correction would change, including any refusal. |
| `agent.repair_workspaces` | mutate | Clear heads' and hirees' stored workspaces and move live name claims to their project's workspace (operator only, after confirmation). |
| `agent.set_hire_cap {project_id, cap}` | mutate | Set this Mac's project hiring limit (Direct only, after Touch ID). |
| `agent.audit_list {since_ms?, limit?, cursor?}` | query | Page local agent-authorized writes and refusals after consent was requested (the local operator's Direct route or `reserved:callosum` only). |

**Agent tools and permissions**

Entorhinal provides `tool-provider/v1` and `permissions/v1`. Tools take the caller
from the daemon's agent stamp, never a body field. Assistants can request
`projects_create_workspace`, `projects_register` and `agents_create_head`;
project heads can request `agents_hire`, `agents_retire_hire` and
`agents_list_hires`. Write tools require a `request_key` of 1–128 printable ASCII
bytes. Reusing a key with different arguments refuses `request_key_conflict`;
retrying the same request returns its stored terminal outcome or resumes consent.

Workspace creation always asks on the phone. Project registration and head
creation can be approved always for a workspace, separately; hiring can be
approved always for a project. The inventory uses exactly `workspace_projects`,
`workspace_heads` and `project_hires`, labelled with workspace or project names.
A head can retire its own live hire immediately without a phone consent card (`own_hire`).
Flows, unstamped callers and retired agents cannot use these tools.

| Operation | Kind | What it does |
|---|---|---|
| `permissions.list {agent_id?}` | query | List live grants on this Mac, optionally for one agent. |
| `permissions.revoke {id}` | mutate | Revoke a grant without prompting; unknown or already-ended grants return `not_found`. |

Both permission operations admit only Direct and `reserved:cingulate`; everyone
else, including agents and Callosum, gets `permissions_operator_required`.
Cingulate aggregates the providers for the phone permissions page. Entorhinal
does not implement that aggregator or its phone/simulator admission rules.
Revocation never prompts and prevents the grant from covering the next request.
Grants also cease covering requests when the holding agent is retired or merged,
or when the workspace or project they cover is removed.

The audit is ordered by time descending, then row id descending. `since_ms` is
inclusive, `limit` defaults to 200 and must be 1–200, and `next_cursor` is opaque;
pass it unchanged to resume after the last row, including across tied timestamps.
Each row names the caller, action, target, outcome and authorization: a card and
answer, a grant id or `own_hire`. Agent writes carry that same authorization in
the identity journal, so verify and rebuild need no live card or grant. Grants,
request state, feed cursors and audit rows stay machine-local and are not synced.

**Sessions, claims and roles**

These operations report which of the operator's Macs holds an agent or a role. The claims behind them are read from engram (the module that stores CortexKit's shared records) and cached locally. Every read here answers from that cache, so it works offline and never waits on engram.

| Operation | Kind | What it does |
|---|---|---|
| `agent.register_session` | mutate | Bind an agent's session to its host session; accepted only from the module the operator assigns the orchestrator or host role (`agent-orchestrator/v1` or `agent-host/v1` in the daemon's config). |
| `agent.session_liveness` | query | Answer active, gone or unknown for up to 256 sessions, in request order. |
| `agent.for_session` | query | Find the agent and session bound to a host session. |
| `agent.session_for_ref` | query | Resolve a session reference to its registered agent, host session and project root. |
| `agent.where` | query | Which Mac holds an agent, from the cached claim. |
| `agent.refresh_claims` | mutate | Refresh live agents' cached claims from engram (operator only). |
| `role.read` | query | Which Mac holds a role, from the cached claim. |
| `role.take` | mutate | Make this Mac a role's holder (operator only, after confirmation). |

**Moving agents between Macs**

A move hands an idle agent, with its sessions and their data, from the Mac that holds it to another of the operator's Macs. The two Macs coordinate through an encrypted record in engram. Each module that keeps agent data takes part through `agent-data/v1`. A move can be cancelled until the receiving Mac activates the agent; after that it always completes. If both Macs changed the agent, the move holds and reports a fork instead of choosing a side.

| Operation | Kind | What it does |
|---|---|---|
| `agent.move` | mutate | Start moving an idle agent to a named Mac: the operator with Touch ID, a phone-confirmed request relayed by the orchestrator, or engram's lid-close move when the operator has enabled it. |
| `agent.move_status` | query | One move's progress, holds, warnings and diagnostics, from local state. |
| `agent.moves` | query | Open moves, then the last 32 finished ones. |
| `agent.move_abort` | mutate | Cancel a move before activation (the operator with Touch ID, or the orchestrator); only the operator can retry a stuck cleanup. |
| `agent.set_lid_close_moves` | mutate | Allow or deny engram's lid-close moves on this Mac (operator only, with Touch ID). |

**Shared identity log**

Agents, projects and workspaces are shared between the operator's Macs through an append-only log that engram stores. Each Mac keeps a full copy and answers reads from it; folder paths and approvals stay on each Mac.

| Operation | Kind | What it does |
|---|---|---|
| `identity_log.status` | query | Local identity log state, progress and pending writes, without contacting engram. |
| `identity_log.enable` | mutate | Enable or join the shared identity log (operator only). |
| `member.set_name` | mutate | Name this Mac in the shared log (operator only). |

### macOS privacy permissions

Entorhinal needs none. It spawns no processes, talks only to the daemon over
loopback, and reads files only under registered project roots: each root's git
config and `.git` marker. One exception to keep in mind: a root under a
folder macOS protects (Desktop, Documents, Downloads, iCloud Drive) needs
Files & Folders access under entorhinal's own name before its remotes can be
read. Without that access, entorhinal can't read the root's git config, so
the root reports no remotes with `remotesError` containing the OS error, and
`resolve_remote` finds no repository it owns. Conditional git-config includes
(`includeIf`) and URL rewrites (`insteadOf`) are unsupported; only remote URLs
written directly in the common directory's config are read.

## Building

```
cargo build --release
cargo test --workspace
```

Every CortexKit dependency comes from crates.io, so a fresh clone builds on its
own.

## Mutation proofs

`mutations.toml` records independent breaks of costly, silent safety properties
and the exact tests that must fail; CI uses `ckdev-mutate` 0.9.8 pinned to commons
`0c99c7e16d8ae22b6e114136b6b7b68be6f1394e` to check every catalogue, replay touched
rows on pushes and PRs, replay all rows on main, and audit every package target
nightly with `--broad`.

```sh
cargo install --locked --git https://github.com/cortexkit/commons --rev 0c99c7e16d8ae22b6e114136b6b7b68be6f1394e cortexkit-mutate
ckdev-mutate check
ckdev-mutate run --only admission-flow-call-site --report target/mutations/one.json
ckdev-mutate run --all --report target/mutations/all.json
```

Copy anchors from current code and break guarded logic independently: prove
exactly-once guards through the production path, use failure-message checks when
needed, and plant a violation for every scan guard. When a break is caught by a
test other than the one its row names, narrow the break; only when several
tests guard one property by design, mark the row `hub` and name that property.
Tests must assert order and outcome,
not elapsed time, with deadlines sized for clean CI only to stop hangs. Never
edit or check out source during replay; investigate survivors as coverage
findings rather than weakening a test or calling a mutant equivalent without a
concrete code fact.

## License

MIT. See [LICENSE](LICENSE).

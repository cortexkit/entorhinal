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
ck projects remove <project>        remove a project from the registry
ck projects verify                  check the store against its journal

ck workspaces list                          workspaces known to the registry
ck workspaces assign <project> <workspace>  put a project in a workspace
```

`--json` prints the raw response. `ck-entorhinal --manifest` prints the module
manifest as JSON without connecting to anything, for offline checks such as
`ck fleet lint`.

## Operations

Served to other modules through the subc daemon, which routes management calls
between CortexKit modules. Reads are open to every caller.
Project writes are accepted from the operator (`ck`) or the executive
(prefrontal-core); agent writes only from the executive, which relays the
operator's own changes. A route opened for a flow can't write at all.

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

### macOS privacy permissions

Entorhinal needs none. It spawns no processes, talks only to the daemon over
loopback, and reads files only under registered project roots: each root's git
config and `.git` marker. One exception to keep in mind: a root under a
folder macOS protects (Desktop, Documents, Downloads, iCloud Drive) needs
Files & Folders access under entorhinal's own name before its remotes can be
read. Without that access, entorhinal can't read the root's git config, so
the root reports no remotes and `resolve_remote` finds no repository it owns.

## Building

```
cargo build --release
cargo test --workspace
```

Every CortexKit dependency comes from crates.io, so a fresh clone builds on its
own.

## Mutation proofs

`mutations.toml` records independent breaks of costly, silent safety properties
and the exact tests that must fail; CI uses `ckdev-mutate` 0.9.5 pinned to commons
`73c7e66145e131eadffdd874c82d93548868b668` to check every catalogue, replay touched
rows on pushes and PRs, replay all rows on main, and audit every package target
nightly with `--broad`.

```sh
cargo install --locked --git https://github.com/cortexkit/commons --rev 73c7e66145e131eadffdd874c82d93548868b668 cortexkit-mutate
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

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

Served to other modules over the subc wire:

| Operation | Kind | What it does |
|---|---|---|
| `resolve` | query | Resolve a filesystem path to its registered or implicit project identity. |
| `resolve_project_id` | query | Look up a project by its durable project id and return its registration record. |
| `enumerate` | query | List all registered projects with workspace assignments and tags. |
| `journal_tail` | query | Read the newest registry journal entries for operator inspection. |
| `register` | mutate | Register a project root under a workspace, minting its durable project id. |
| `assign_workspace` | mutate | Move a registered project to a different workspace. |
| `upgrade_implicit` | mutate | Promote an implicit (path-derived) project to a registered one, keeping its id. |
| `remove` | mutate | Remove a project registration; its id stops resolving. |
| `seed_import` | mutate | Bulk-import registrations from a seed file (operator recovery path). |
| `verify` | query | Check registry invariants and report inconsistencies without mutating. |
| `rebuild` | mutate | Rebuild registry projections by replaying the journal (operator recovery path). |
| `projects.session_liveness` | mutate | Ingest session liveness snapshots and deltas for dead-folder detection. |

## Building

```
cargo build --release
cargo test --workspace
```

Every CortexKit dependency comes from crates.io, so a fresh clone builds on its
own.

## License

MIT. See [LICENSE](LICENSE).

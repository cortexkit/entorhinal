# entorhinal

entorhinal is the project and agent registry for
[CortexKit](https://github.com/cortexkit/subconscious). It gives every project
a durable id (`pj-…`) and answers which project a directory belongs to, so
modules that keep per-project state agree on what a project is, even when a
directory moves or has several names. It also holds agent identity (names,
roles, placement), records which of the operator's Macs holds an agent or a
role, moves idle agents between Macs, and keeps the consent state behind the
tools agents use to create workspaces, projects and hires. It runs as a module
supervised by the subc daemon and provides the `project-identity/v1` and
`agent-identity/v1` capabilities; modules that cannot work without them declare
them `required`, and the daemon holds those modules unroutable until
entorhinal has registered.

| Crate | Role |
|---|---|
| `crates/entorhinal-core` | The store and every domain rule: projects, roots, agents, the registry journal, shared-log entries, move checkpoints and consent state. Knows nothing about subc. |
| `crates/entorhinal-module` | The `ck-entorhinal` binary: the supervised module (admission, dispatch, the shared-log writer, move and consent drivers) and the `ck projects`, `ck workspaces`, `ck agents` and `ck roles` commands. |

## Build and test

These need nothing but a Rust toolchain. Every CortexKit dependency comes from
crates.io, so a fresh clone builds on its own.

```sh
cargo build --workspace --release
cargo test --workspace --all-targets --locked
```

The tests open their own scratch stores and run the other modules they talk to
as in-process stand-ins, so they need no daemon. `CONTRIBUTING.md` lists the
full gate CI runs.

One test needs more: `real_daemon_e2e` starts a real subc daemon built from a
`subconscious` checkout placed next to this repository, so it is ignored by
default. Run it explicitly with:

```sh
cargo test --workspace --test real_daemon_e2e -- --ignored --nocapture
```

`ck-entorhinal --manifest` prints the module manifest as JSON without
connecting to anything. Everything else needs a running subc daemon.

## Commands

The binary `ck-entorhinal` is the module. People use it through `ck`, which
runs it under the names `ck-projects`, `ck-workspaces`, `ck-agents` and
`ck-roles`:

```
ck projects list | resolve | register | remove | add-root | attach | remove-root
            rebind | owned-remotes | approve | unapprove | trust | verify
            log status | log name | log enable
ck workspaces list | assign | set-root | clear-root
ck agents   create | rename | retire | tag | labels | hire-cap | list | show
            repair-workspaces | where | move | moves | move-abort | lid-close-moves
ck roles    read | take
```

`ck projects`, `ck workspaces`, `ck agents` or `ck roles` on its own prints the
full usage. `--json` prints the raw response. Agent identity writes, hire caps,
role takes and moves from `ck` land only after the operator approves the exact
change at a Touch ID prompt the daemon shows.

## Documentation

Start with [docs/architecture.md](docs/architecture.md).

- Architecture: [architecture](docs/architecture.md),
  [data model](docs/data-model.md), [module surface](docs/module-surface.md).
- Contracts: [identity log](docs/identity-log-contract.md),
  [agent moves](docs/agent-move-contract.md),
  [consent](docs/consent-contract.md),
  [session liveness](docs/session-liveness-contract.md).
- Operator guides: [projects and roots](docs/projects-and-roots.md),
  [agents](docs/agents.md), [sharing between Macs](docs/sharing.md),
  [moving agents](docs/moves.md), [mutation proofs](docs/mutations.md).
- [CONTRIBUTING.md](CONTRIBUTING.md) and [SECURITY.md](SECURITY.md).

## License

MIT. See [LICENSE](LICENSE).

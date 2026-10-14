# Contributing to entorhinal

This file says what a change must pass, how to commit it, and where code
goes. For what the module does and how its parts fit together, start with
[docs/architecture.md](docs/architecture.md).

## The gate

CI (`.github/workflows/ci.yml`) runs these checks on every push and pull
request. Run them from the repository root before you send a change, one
cargo command at a time.

```sh
scripts/check-cortexkit-layout.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
ckdev-mutate check --strict
```

- `scripts/check-cortexkit-layout.sh` confirms that nothing under
  `.cortexkit/` is tracked except the folders the layout allows
  (`release-notes/`, `skills/`, `rules/`) and `WORKER.md`. It exits 0 when
  clean, 1 when a disallowed file is tracked, and 2 when the `.gitignore`
  rules it relies on are missing.
- `cargo fmt --all -- --check` prints nothing and exits 0 when the tree is
  formatted.
- Clippy runs with `-D warnings`, so any warning fails the gate.
- The test suite must report `ok` on every `test result:` line. CI runs it on
  Linux, macOS and Windows; the macOS and Windows jobs use `--no-fail-fast`.
  Some tests only run on one platform (for example the Windows ACL tests in
  `crates/entorhinal-module/tests/windows_owner_only.rs`).
- `ckdev-mutate check --strict` validates the mutation catalogue in
  `mutations.toml`: every anchor must still match the source and every named
  test in an active row must exist. Active command rows must supply a
  `list_command`. Rows restricted to another host by `platforms`, or marked
  `unreachable` or `desk_only` with a reason automation cannot replay them,
  are exempt from test-name verification.
  See [docs/mutations.md](docs/mutations.md) for installing the runner and
  for what the catalogue is.

Always build, check and test with `--workspace`. Building one crate on its own
compiles a second variant of shared dependencies, because workspace feature
unification no longer applies. To run a subset of tests, filter by name:

```sh
cargo test --workspace --all-targets --locked shared_write::tests::
```

After the main test job, CI also replays mutation rows: rows your change
touches on every push and pull request, the full catalogue on `main`, and a
broader audit of every package target nightly. A replay that finds a break no
test catches fails the job.

### What not to run

- `rm -rf target/` to reclaim space. Use `cargo sweep --time 7` instead.
- Tests or tools against a live registry store. Tests create their own
  scratch stores. A binary refuses a store whose schema is ahead of it (see
  [docs/data-model.md](docs/data-model.md#5-migrations)), so a stray write
  from a newer build can stop an older module from starting.

## Tests

- Tests assert order and outcome, never elapsed time. Where a test needs a
  deadline, size it only to stop a hang on a clean CI runner.
- A test that sends a request goes through the serving path the daemon uses
  (`ProjectsHandler` in `crates/entorhinal-module/src/main.rs`), with an
  admitted route, rather than calling a store method directly, when the rule it
  proves is an admission or wire rule.
- If your change makes an existing test fail, do not rewrite the test to
  expect the new behaviour without saying why in the commit message. A test
  records that a property mattered.
- The published vectors under `test-vectors/` are compared byte for byte with
  the handler's replies. Each folder's `README.md` says how to regenerate it
  and its `SHA256SUMS`.

## Commits

- One logical change per commit, with the gate passing at that commit.
- The summary line says what changed, in the imperative ("Refuse a missing
  request key in the tool write path"). The body says why, and names any test
  whose expectation changed.
- Never commit `target/`, editor files, secrets or local tool state.
  Internal notes live under `.cortexkit/`, which git ignores; the layout check
  fails if one is force-added.
- A change to the wire surface, an admission rule, the store schema or a
  contract updates the matching document under `docs/` in the same commit.

## Where code goes

The workspace has two crates. The rule that separates them: anything that can
be decided from the store alone belongs in `entorhinal-core`; anything that
talks to the subc daemon or another module belongs in `entorhinal-module`.

| Path | What goes there |
|---|---|
| `crates/entorhinal-core/src/lib.rs` | `RegistryStore`: opening the store, the migration chain, resolve and the other project reads. |
| `crates/entorhinal-core/src/mutations.rs` | Project and workspace writes, the registry journal, `verify` and `rebuild`. |
| `crates/entorhinal-core/src/binding.rs`, `root_keys.rs`, `ownership.rs` | Root bindings and approval, machine-neutral root keys, owned remotes. |
| `crates/entorhinal-core/src/agent/` | Agent identity: schema, validation, names, reads, the change feed, workspace carry, and machine-local consent state. |
| `crates/entorhinal-core/src/shared_entry.rs`, `remote_apply.rs`, `log_schema.rs`, `enable_state.rs`, `pending.rs` | Shared identity-log entries, applying remote entries, and the local log state. |
| `crates/entorhinal-core/src/move_record.rs`, `move_store.rs` | Move record codecs and the machine-local move checkpoint. |
| `crates/entorhinal-core/src/claim_cache.rs`, `role_cache.rs` | Cached agent and role claims. |
| `crates/entorhinal-module/src/main.rs` | The module: manifest, route admission, request dispatch, health. |
| `crates/entorhinal-module/src/cli.rs` | The `ck projects`, `ck workspaces`, `ck agents` and `ck roles` commands. |
| `crates/entorhinal-module/src/shared_write.rs`, `catch_up.rs`, `enable.rs`, `log_client.rs`, `member.rs` | Writing to, reading from and joining the shared identity log. |
| `crates/entorhinal-module/src/move_*.rs`, `participant_*.rs`, `door_client.rs`, `record_client.rs` | Moving agents between Macs. |
| `crates/entorhinal-module/src/tool_*.rs`, `consent*.rs`, `permissions.rs` | Agent tools, consent cards, grants and the audit. |
| `crates/entorhinal-module/src/sessions.rs`, `claims.rs`, `roles.rs` | Sessions, session liveness, claims and roles. |
| `crates/entorhinal-module/src/fake_*.rs` | In-process stand-ins for other modules, compiled only for tests. |
| `crates/*/tests/` | Integration tests that need their own binary or golden files. |
| `mutations.toml` | Mutation proofs; see [docs/mutations.md](docs/mutations.md). |

Every CortexKit dependency comes from crates.io. Do not add a `path`
dependency on a sibling checkout: `crates/entorhinal-core/tests/dependency_sources.rs`
fails when any dependency resolves to a path outside this repository, so a
fresh clone keeps building on its own.

# Contributing to entorhinal

This file says what a change must pass, how to commit it, and where code
goes. For what the module does and how its parts fit together, start with
[ARCHITECTURE.md](ARCHITECTURE.md).

## The gate

CI (`.github/workflows/ci.yml`) runs these checks on every push and pull
request. Run them from the repository root before you send a change, one
cargo command at a time.

```sh
scripts/check-cortexkit-layout.sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
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

Always build, check and test with `--workspace`. Building one crate on its own
compiles a second variant of shared dependencies, because workspace feature
unification no longer applies. To run a subset of tests, filter by name:

```sh
cargo test --workspace --all-targets --locked shared_write::tests::
```

### What not to run

- `rm -rf target/` to reclaim space. Use `cargo sweep --time 7` instead.
- Tests or tools against a live registry store. Tests create their own
  scratch stores. A binary refuses a store whose schema is ahead of it (see
  [docs/data-model.md](docs/data-model.md#5-migrations)), so a stray write
  from a newer build can stop an older module from starting.

## Tests

- A bug fix comes with a test that fails without the fix. Show that once, by
  running the new test against the unfixed code; there is no separate
  catalogue of proofs to keep.
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

[STRUCTURE.md](STRUCTURE.md) maps every file and says where new code goes.
The rule that separates the crates: anything that can be decided from the
store alone belongs in `entorhinal-core`; anything that talks to the subc
daemon, another module or the network belongs in `entorhinal-module`; pure
package-catalog checks belong in `entorhinal-catalog`.

Every CortexKit dependency comes from crates.io. Do not add a `path`
dependency on a sibling checkout: `crates/entorhinal-core/tests/dependency_sources.rs`
fails when any dependency resolves to a path outside this repository, so a
fresh clone keeps building on its own.

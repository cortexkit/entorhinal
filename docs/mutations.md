# Mutation proofs

Some properties of entorhinal fail silently when they break: a refusal that
stops refusing, a retry that stops reusing its bytes, a guard that stops
being called. A green test suite does not prove that the tests guarding them
would notice. `mutations.toml` is the catalogue of proofs that they would:
each row breaks one property in the source, and names the tests that must
fail when it is broken.

The rows are run by `ckdev-mutate`, from the `cortexkit-mutate` package in the
[cortexkit/commons](https://github.com/cortexkit/commons) repository. The
catalogue's header and CI pin the runner version and commit.

## Install the runner

Use ckdev-mutate 0.12.3 at the commit CI pins (`CK_MUTATE_REV` in
`.github/workflows/ci.yml`):

```sh
cargo install --locked --git https://github.com/cortexkit/commons \
  --rev 8200b61fe4250989d315ed8064d16754c5032d10 cortexkit-mutate
ckdev-mutate --version
```

## Check the catalogue

```sh
ckdev-mutate check --strict
```

This is part of the CI gate ([CONTRIBUTING.md](../CONTRIBUTING.md)). It
confirms that every row's anchor text still appears in its file and that every
named Cargo test exists under its exact full name for active rows, without
changing any source. Custom command rows use their runner's selection rules
through `list_command`. Strict mode rejects active command rows without one.
Test-name verification skips rows whose `platforms` exclude the host, or whose
`unreachable` or `desk_only` fields document why automation cannot replay them
(for example, a proof requiring a human or physical hardware). Run the check
after any change to code a row anchors on.

## Run rows

```sh
mkdir -p target/mutations
ckdev-mutate run --only admission-flow-writes --report target/mutations/one.json
ckdev-mutate run --diff origin/main --report target/mutations/diff.json
ckdev-mutate run --all --report target/mutations/all.json
```

`--only` replays one row by id; `--diff` replays the rows a change touches;
`--all` replays the catalogue. CI replays touched rows on every push and pull
request, every row on `main`, and adds `--broad` on the nightly run to watch
every package target rather than only the row's own. CI splits the work with
`--shard N/8`; give each shard its own checkout.

Rows with `select = "expected"` keep their exact `expect_red` selection even
under `--broad`; omit `select` to audit the full target instead.

A replay applies the break, builds, runs the named tests, and restores the
file. A row passes when its expected tests fail; its `only` field decides
whether other tests in the same target may fail too. Reports go under
`target/mutations/`, which git ignores.

While a replay runs, do not edit, check out or stash anything in that
checkout: the break is in the working tree.

## CI replay cost

A row without `select = "expected"` runs its whole test target, not only the
tests in `expect_red`. For rows in the module crate that is the `ck_entorhinal`
binary, about 690 tests. On the Linux runner such a row spends 5 to 8 s
building and linking (the link itself is under half a second) and 35 to 55 s
running tests. The same row run expected-only spends the same build time and
under 0.1 s in tests, and is still caught. Lighter debug info, LLD and more
test threads did not shorten replays measurably.

411 of the 670 rows have no `select`, so a full replay of the catalogue takes
50 to 80 minutes per shard. `select = "expected"` can't simply be added to
those rows: it also narrows them in the nightly `--broad` sweep, which exists
to catch breaks that show up in tests other than the named ones. So push and
main replays run with `--narrow`, which runs each row's named tests only,
while the nightly sweep stays broad. Rows with `only = true` stay broad in
every run, because `only` claims every other test stays green.
## A row

```toml
[[control]]
id = "admission-flow-writes"
guards = "Flow-scoped routes refuse every registry and identity mutation regardless of principal"
file = "crates/entorhinal-module/src/main.rs"
old = "Some(flow_id) if MUTATING_METHODS.contains(&method) => Err(HandlerError {"
new = "Some(flow_id) if false /* NON-VACUITY BREAK */ => Err(HandlerError {"
test_file = "crates/entorhinal-module/src/main.rs"
runner = "cargo"
package = "entorhinal-module"
target = "--bin ck-entorhinal"
expect_red = ["tests::a_flow_scoped_route_cannot_write_even_with_the_executives_principal"]
expect_message = "a flow must not change the registry"
```

| Field | Meaning |
|---|---|
| `id` | A unique kebab-case name. |
| `guards` | The property, in one sentence. |
| `file`, `old`, `new` | The break: `old` must occur exactly once in `file` and is replaced by `new`. |
| `edits` | Instead of `file`/`old`/`new`, a list of `{file, old, new}` edits applied together, for a break that needs several. |
| `test_file` | Where the expected tests live. |
| `runner` | Prefer `cargo`, with `package` and `target` (for example `--lib`, `--bin ck-entorhinal`, `--test replay_verification`). Use `command` only for a custom runner, with a `command` array that contains `{test}` and a `test_count_pattern` that confirms the test ran. |
| `list_command` | Required for active named-test `command` rows: an argument array with `{test}` as one whole argument exactly once. The command must discover the same tests as `command` and print one JSON array of selected test names, so `check --strict` can verify `expect_red`. |
| `expect_red` | The exact test names that must fail. |
| `select` | Optional `"expected"` for cargo rows that should run only the exact `expect_red` tests, not the full target. Cannot be combined with `only = true`, `hub` or `hub_targets`. |
| `expect_message` | Optional text the failure output must contain, so the test fails for the reason it claims. |
| `only` | `true` requires that no other test in the target fails; `false` lets related assertions in the same target fail too. |
| `hub`, `hub_targets` | For a property several tests guard by design: the shared property, and the targets those tests live in. |
| `platforms` | Restricts a row to the platforms its test runs on (for example `["linux"]`). |
| `timeout_s`, `build_timeout_s` | Override the default limits of 600 seconds to test and 1800 seconds to build. |

## Add a row

1. **Pick a silent property.** One whose failure no other test or type check
   would reveal: a guard, a fence, a refusal, an exactly-once rule. A test
   that asserts a plain return value needs no row.
2. **Copy the anchor from the code.** `old` is copied verbatim from the
   current source and must match exactly once. Mark the replacement with the
   token `NON-VACUITY BREAK` in a comment, so a break left in a tree by
   accident is easy to find.
3. **Break one thing.** Each row breaks one property independently of the
   others. Prove exactly-once guards through the production path, not a
   helper. For a scan guard (a test that scans sources for a forbidden
   pattern), plant a violation instead of disabling the scan.
4. **Name the test that catches it.** Put its exact name in `expect_red`.
   Add `expect_message` when the test could fail for an unrelated reason.
5. **Narrow until only that test fails.** If a different test catches the
   break, narrow the break. Mark a row `hub` only when several tests guard one
   property by design, and name that property.
6. **Assert order and outcome, not time.** Expected tests must fail, not time
   out. Deadlines in tests exist only to stop a hang on a clean CI runner.
7. **Check and replay it.**

   ```sh
   ckdev-mutate check --strict
   ckdev-mutate run --only <id> --report target/mutations/one.json
   ```

A row the replay reports as surviving (the break applied, the expected test
stayed green) is a gap in the tests, not a reason to weaken the test or the
row. Do not call a surviving break equivalent to the original code without a
concrete fact about the code that shows it.

Platform-only code that Linux replays cannot compile has no row. The comments
in `mutations.toml` name the native test that covers each such case.

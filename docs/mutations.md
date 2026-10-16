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

Use ckdev-mutate 0.12.1 at the commit CI pins (`CK_MUTATE_REV` in
`.github/workflows/ci.yml`):

```sh
cargo install --locked --git https://github.com/cortexkit/commons \
  --rev 9d6b663dac487bb982c7bfd18a96eee2292164ad cortexkit-mutate
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

I measured five representative rows on the Linux build server with
`ckdev-mutate 0.12.1`. “Cold” is the first replay of an id; “warm” is the
immediate repeat. The wall time covers the whole `run --only` invocation. The
runner’s baseline timing separates the clean-source build (including link) from
test execution; it does not include the mutant’s second build/test in that
baseline line.

| Row | Wall, cold / warm | Baseline build + link, cold / warm | Baseline tests, cold / warm |
| --- | ---: | ---: | ---: |
| `admission-flow-writes` (module) | 110.4 / 136.7 s | 0.32 / 7.77 s | 54.03 / 49.09 s |
| `move-driver-stop-drains-blocking-sql` (module) | 106.9 / 132.8 s | 6.54 / 5.18 s | 52.15 / 51.77 s |
| `verify-drift-affects-ok` (core) | 1.9 / 2.9 s | 0.12 / 1.47 s | 0.16 / 0.14 s |
| `identity-one-live-head-index` (core) | 19.5 / 23.6 s | 1.29 / 1.83 s | 7.04 / 9.77 s |
| `s1-path-not-unicode-cli` (Cargo/Linux) | 119.1 / 87.4 s | 5.88 / 3.18 s | 47.86 / 41.59 s |

These values are from two-vCPU Linux runs; all five rows reported CAUGHT. There
are no command-runner rows in the catalogue, so the fifth sample is a
Linux-scoped Cargo row. The `admission-flow-writes` report executed 687 runnable
tests in the `ck_entorhinal` target, not only its one expected failure. A
linker wrapper around the system `cc` measured that large executable’s link at
0.38–0.40 s. Linking is only a small part of the build, and the test target
dominates the slow rows.

As a comparison, a temporary expected-only copy of `admission-flow-writes`
reported CAUGHT with a 5.32 s mutant build and a 0.06 s test; its clean baseline
build/test was 5.20 / 0.06 s. The default four-vCPU run’s mutant build/test was
5.86 / 36.07 s. This shows the expected test alone catches this sample row, but
does not prove the same for every catalogue row.

I tried `CARGO_PROFILE_DEV_DEBUG=0` plus `CARGO_PROFILE_TEST_DEBUG=0`, then
clang with LLD, separately. For the warm module baseline, default build/test
was 7.77 / 49.09 s; with no debug info it was 7.83 / 49.57 s, and with LLD it
was 6.73 / 50.71 s. No-debug did not lower the warmed build, while LLD saved
about one second in one replay and did not shorten the test. The Linux build
worker had `ld.lld` but no `mold`; the checked Ubuntu runner image lists Clang
but not LLD, so LLD would also need installation in CI. On the four-vCPU
worker, `RUST_TEST_THREADS=8` was inconsistent across repeats: whole-row times
were 88.1 and 101.6 s versus 93.2 and 80.1 s at the default. None of these
settings is enabled in CI. The test job already builds all workspace targets
before the mutation job, and both use `rust-cache`, so dependency cache warmth
is not the source of the long row time.

At the pinned runner revision, expected-only selection is a row-level setting;
there is no run-wide option. Currently 411 of the 670 catalogue rows run their
full selected test target on push replays. Adding `select = "expected"` to the
catalogue rows would also narrow those rows under `--broad`, and the runner
refuses that selection on rows with `only = true`; the nightly sweep would no
longer audit the same breadth. No catalogue or replay behavior is changed here.
The planned improvement is a run-wide expected-only option in `ckdev-mutate`:
use it for push/PR and main replays so each row runs its named tests, while
leaving the nightly `--all --broad` sweep unfiltered.

A full shard was not replayed. The current CI projection remains the measured
52–80 minutes per shard; the future expected-only projection is roughly 15–20
minutes for 84 rows if build/link stays near 5–8 s and the selected test remains
short. That projection is not a full-shard measurement.

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

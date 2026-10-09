---
title: "Entorhinal on Linux and Windows: fix the module's own platform gaps"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - "crates/entorhinal-module/src/cli.rs:400-470"
    - "crates/entorhinal-module/src/cli.rs:950-1130"
    - "crates/entorhinal-module/src/cli.rs:2030-2080"
    - "crates/entorhinal-module/src/main.rs:75-130"
    - "crates/entorhinal-module/src/main.rs:955-995"
    - "crates/entorhinal-module/src/main.rs:1590-1645"
    - "crates/entorhinal-module/src/log_client.rs:340-430"
    - "crates/entorhinal-module/Cargo.toml"
    - "crates/entorhinal-core/src/lib.rs:220-240"
    - "crates/entorhinal-core/src/lib.rs:320-340"
    - "crates/entorhinal-core/src/lib.rs:565-590"
    - "crates/entorhinal-core/src/lib.rs:905-1020"
    - "crates/entorhinal-core/src/mutations.rs:120-170"
    - "crates/entorhinal-core/src/mutations.rs:270-290"
    - "crates/entorhinal-core/src/mutations.rs:920-985"
    - "crates/entorhinal-core/src/mutations.rs:2280-2320"
    - "crates/entorhinal-core/src/binding.rs:180-320"
    - "crates/entorhinal-core/src/binding.rs:680-700"
    - "crates/entorhinal-core/src/root_keys.rs:1-60"
    - "crates/entorhinal-core/Cargo.toml"
    - "crates/entorhinal-module/tests/ck_domain.rs"
    - "crates/entorhinal-core/tests/agent_import.rs:740-780"
    - "crates/entorhinal-core/tests/disabled_journal_golden.rs:30-80"
    - ".github/workflows/ci.yml"
    - "Cargo.toml"
---

## intent
The operator ruled that every CortexKit module supports macOS, Linux and Windows. A report-only census of entorhinal (36 findings, F01-F36) found it builds and passes its whole suite on Linux, and cross-compiles for Windows, but several operator paths fail or silently degrade off macOS. Some findings are shared hazards on macOS too.

This campaign fixes the findings that live in entorhinal's own code and dependency pins, and adds native macOS and Windows CI so the result stays fenced. Findings owned by the subc daemon or its libraries are handed to their owner and are not in scope (see non_goals).

End state:
- `ck projects`, `ck workspaces` and `ck agents` find a running daemon wherever the daemon publishes its connection file, on all three systems.
- On Windows, every CLI mutation sends a path in exactly the form core compares, so register, add-root, attach and workspace set-root work.
- The module never guesses its store location. A missing storage descriptor is a startup refusal, so no test or misconfigured launch can open the live registry by accident.
- Store, lease and log files are owner-only on Windows, as they already are on Unix.
- Path guards (the home-ancestor guard, liveness containment, the volume boundary) compare native path components, not `/`-joined strings.
- Git metadata edge cases (a `.git` symlink, a separate git dir, an unreadable config, a CRLF marker) are handled or refused by name instead of silently losing remotes.
- CI runs clippy and the full test suite natively on Linux, macOS and Windows.

## non_goals
These are owned elsewhere. The chair hands them to their owners; this campaign changes none of them:
- F20 connection-file owner and DACL checks on Windows: `subc-transport` (SUBC).
- F21 Windows launch-nonce handle inheritance: `subc-os` and the daemon (SUBC).
- F22 operator-presence providers on Linux and Windows: the subc daemon (SUBC). Entorhinal's confirmation gate stays exactly as it is: no fallback approval on any platform.
- F24 installing the `ck-projects`, `ck-workspaces` and `ck-agents` faces (links, or `.exe` copies on Windows): SUBC's placement tooling.
- F32 a real-daemon test in CI: it needs a `ck-subc` binary that CI can fetch without a sibling checkout, which doesn't exist yet. `real_daemon_e2e.rs` stays `#[ignore]`d. The only change there is native executable naming (`.exe`), so it still compiles on Windows.
Also out of scope:
- F25: the test-binary helper now comes from `cortexkit-test-support`. That crate owns its Windows behaviour.
- F01 (resolved: the runner has mingw-w64), and F34, F35, F36 (informational: already correct).
- Changing how ids are minted, or folding path case globally (see F08 below).
- Any change to the wire shape of existing replies, except the one additive field in F13.

## constraints

### Dependencies (F17, F18, F19)
- Move to the published `cortexkit-store` 0.2.4, `cortexkit-lease` 0.1.2 and `cortexkit-log` 0.3.5. Together they make the store directory, the SQLite database and its `-wal`/`-shm` files, the lease file and log files owner-only on Windows through protected DACLs. On Unix they keep the existing 0700/0600 behaviour.
- All dependencies come from crates.io (`crates/entorhinal-core/tests/dependency_sources.rs` enforces that).
- Any API change these versions require is taken as-is. No compatibility shim.

### Daemon discovery (F03)
- The CLI finds the daemon through `subc_client_rs::discover(explicit)`, the same function `SubcConsumer::connect_default` uses. `--subc <path>` stays an exclusive override, passed as `explicit`.
- Delete the hand-rolled lookup in `cli.rs` (`SUBC_CONNECTION_FILE` plus `HOME/.local/share/cortexkit/run`). No second implementation of the ladder may remain.
- A discovery failure prints the candidates that were tried, which the SDK's error reports.

### Store location (F04)
- When HELLO_ACK carries no storage descriptor, the module refuses to start with a named error (`storage_descriptor_missing`) and exits non-zero. Delete the hand-rolled XDG/HOME/temp fallback in `main.rs`, including its wrong `ck-entorhinal` directory name.
- The worker must cite, from the subc daemon or protocol source, that a supervised module with a declared store always receives a descriptor. If it can't establish that, stop and ask: don't refuse a launch the daemon considers valid.
- Test harnesses that start the handler without a descriptor must supply a scratch one. A test that forgets fails to start, and can never open the live store.
- This touches daemon coupling, so the chair routes the merged change to SUBC for review before pushing.

### CLI paths (F02, F09, F33, F10, F11)
- Every path the CLI sends for a mutation goes through `RegistryStore::canonical_mutation_root`, the same function core compares against. The CLI keeps no parallel canonicalization. For a lookup of a path that may not exist, the CLI uses the same function core uses for that lookup (read `lib.rs` to find it). If core has none, add one to core and call it from both sides.
- On Windows, a drive-relative path (`C:repo`) or a rooted path with no drive (`\repo`) is refused at the CLI with a usage error naming the problem. It is never sent to the daemon.
- If the current directory can't be read, the CLI and the identity-log connector refuse with a clear error. They no longer fall back to `/` as the bind identity.
- `main.rs` reads arguments with `std::env::args_os`. An argument that isn't valid Unicode is refused with a usage error, not a panic.
- A path that can't be converted to a string losslessly is refused with `path_not_unicode` at the CLI and at core's mutation entry points. `to_string_lossy` or `display()` must never feed a comparison, a key or a hash.

### Path guards in core (F05, F06, F07, F08)
- Home-ancestor guard (`mutations.rs`): canonicalize first, then compare by path components (`Path::starts_with` and `components()`), never by string prefix with `/`. Homes come from one function covering `HOME`, plus `USERPROFILE` on Windows. A filesystem root, including `C:\` and a UNC share root, counts as an ancestor of everything.
- Liveness containment (`main.rs`, `annotate_liveness` or its helper): a session below a project root matches by path components, using the same canonical form as project resolution.
- Volume boundary (`lib.rs` `device_id`): implement on Windows with the volume serial number from `GetFileInformationByHandle` (through `windows-sys`, which commons already uses). Add a test that two paths on one volume compare equal. Cross-volume containment is tested where CI can create a second volume; otherwise the test is skipped by name, with the reason.
- Missing-path case (F08): no global case folding. Pin today's rule with tests on every platform: existing path prefixes are canonicalized, and missing tail components keep the caller's spelling. Document that rule in `lib.rs` where missing roots are resolved.

### Git metadata (F12, F13, F14, F15)
- A `.git` that is a symlink to a directory is followed and read as a directory.
- A `.git` file whose `gitdir:` points at a separate git directory with no `commondir` (a submodule, or `git init --separate-git-dir`) reads that directory's `config`.
- An unreadable or unparsable git config is no longer reported as "no remotes". The root's reported remote set carries `remotes_error: "<reason>"`, an additive field that decoders ignore when absent. Ownership resolution treats that root as unknown, never as owning nothing.
- The incarnation marker accepts exactly one trailing `\n` or `\r\n`. Anything else stays malformed.
- After creating the incarnation marker, the containing directory is fsynced on Unix before the binding commits. On Windows, flushing the file is the strongest step available. Say so in a comment.
- Conditional includes and URL rewrites stay unsupported. The README says so.

### Retry output (F23)
- The approval-pending message says the approval is waiting at the operator prompt. It doesn't mention a Mac.
- Retry commands are quoted for the platform the CLI runs on: POSIX single quotes on Unix, PowerShell quoting on Windows. A test pins both quoting functions on every platform, including embedded quotes. The retry keeps the original request key.

### Tests and fixtures (F26, F27, F28, F29, F30)
- Shared test fixtures that register roots use the production canonical form, so they pass on Windows. Keep one deliberate test showing that a non-canonical (verbatim) path is refused.
- Golden comparisons normalize native paths in a serialization-aware way: replace the path value before serializing, or compare parsed JSON. Byte-level wire goldens stay byte-level.
- The CLI path test (`cli.rs`, around the symlink canonicalization test) compares the CLI's output with what core accepts, through core's own function. Its expected value can't come from `std::fs::canonicalize`.
- `ck_domain.rs` runs on Windows by starting copies of the binary named after each face (`ckdev-projects.exe` and so on). On Unix it keeps `arg0`.
- `agent_import.rs`'s unreadable-snapshot test gets a Windows arm that denies read through an ACL.

### CI (F31, F16)
- CI runs `cargo clippy --workspace --all-targets --locked -- -D warnings` and `cargo test --workspace --all-targets --locked` natively on `ubuntu-24.04`, `macos-latest` and `windows-latest`. `cargo fmt --check` runs once. Mutation shards stay on Linux.
- The README says the store must be on a local disk: WAL and shared memory aren't supported on network shares.

### Mutation catalogue
- Every new guard gets a row in `mutations.toml`: discovery through the SDK, the missing-descriptor refusal, the drive-relative refusal, `path_not_unicode`, component-wise home and liveness matching, the Windows volume serial, the `.git` layouts, `remotes_error`, the CRLF marker, and both retry quoting functions. Each row is replayed CAUGHT with `ckdev-mutate` 0.9.5 before delivery.
- Rows whose anchors move are re-anchored, and replayed CAUGHT.

## acceptance_sketch
1. With `HOME` unset and `XDG_RUNTIME_DIR` set, `ck projects list` finds a connection file published under `XDG_RUNTIME_DIR` (test with a scratch file and a scratch environment, no daemon needed beyond the discovery result).
2. A handler started with no storage descriptor exits with `storage_descriptor_missing`, and opens no file under the real data home (test with a scratch `HOME`, asserting nothing was created).
3. On Windows CI, `register` and `add-root` through the CLI's path function produce exactly the string `canonical_mutation_root` returns, and core accepts it (no `not_canonical`).
4. `C:repo` and `\repo` are refused at the CLI on Windows, and nothing is sent.
5. The home guard refuses a root equal to an ancestor of `HOME` or `USERPROFILE`, including through a symlink or case alias on macOS, and including `C:\`.
6. A session at `<root>\sub` (Windows) or `<root>/sub` (Unix) counts toward the project's liveness. A session at `<root>-other` doesn't.
7. Each `.git` layout in the git metadata constraints has a test: a symlinked dir and a separate git dir both read remotes, and an unreadable config yields `remotes_error`, with no ownership.
8. A marker ending in `\r\n` binds, and one ending in `\r` or `\n\n` stays malformed.
9. Retry quoting round-trips a request containing spaces, single quotes and double quotes, for both POSIX and PowerShell.
10. CI is green on all three operating systems, and every existing test still passes on Linux and macOS.
11. Every new mutation row is CAUGHT, and `ckdev-mutate check` passes.

## open_questions
None.

## slice_hints
Four slices, run in order on `main`, because they share test fixtures:
1. **Module launch and CLI** (`crates/entorhinal-module/src/{cli,main,log_client}.rs`, the module's `Cargo.toml`, the workspace `Cargo.toml`/`Cargo.lock`): dependency bumps, discovery, the missing descriptor, CLI paths (F02, F09, F33, F10, F11 on the CLI side), liveness containment (F06), retry output (F23), and the CLI-to-core test (F30).
2. **Core path identity** (`crates/entorhinal-core/src/{lib,mutations}.rs` and core tests): the home guard, the volume serial, the missing-path rule, `path_not_unicode` in core, and fixture and golden normalization (F28, F29).
3. **Git metadata** (`crates/entorhinal-core/src/{binding,root_keys}.rs`): F12, F13, F14, F15, and the README note on unsupported git config.
4. **Native CI** (`.github/workflows/ci.yml`, `ck_domain.rs`, `agent_import.rs`, `README.md`): the three-OS matrix, Windows face tests, the ACL arm, the local-disk note, and fixing any Windows-only failure the matrix reveals in files the earlier slices own (they're merged by then).

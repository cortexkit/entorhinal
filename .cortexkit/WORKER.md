# Worker guide

Run every command from the worktree root. Serialize cargo invocations: one cargo command at a time, never in parallel; other workers share this machine.

Always build, check and test with `--workspace` (filter tests by name), never run `-p entorhinal-core` alone; workspace feature unification avoids compiling a second variant of shared dependencies.

## Gates

These are the three CI gates in `.github/workflows/ci.yml`.

- Format: `cargo fmt --all -- --check`. Ran when: exit 0 and no diff printed (this one is silent on success; confirm with `cargo fmt --version`).
- Lint: `cargo clippy --workspace --all-targets --locked -- -D warnings`. Ran when: exit 0 and the last line is `Finished ...` with no `warning:` or `error:` lines.
- Tests: `cargo test --workspace --all-targets --locked`. Ran when: every `test result:` line reads `ok` and the passed counts sum to more than 0.

## Commands to avoid

- Bare package runners (`npx <tool>`, `bunx <tool>`): they can fetch an unrelated package of that name, print nothing and exit 0.
- `rm -rf target/`: use `cargo sweep --time 7` to reclaim space.
- Opening or editing the live registry store (`~/.local/share/cortexkit/entorhinal/store.db`): tests use their own scratch stores. A binary refuses a store it does not know, so a stray write can stop the module.
- `git push` from a worker worktree: the chair merges and pushes.

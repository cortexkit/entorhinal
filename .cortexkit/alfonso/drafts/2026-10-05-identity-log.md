---
title: "Share entorhinal's fleet identity through engram's identity log"
date: 2026-10-05
status: draft
rigor_proposed: r3
evidence:
  include:
    - crates/entorhinal-core/src/lib.rs
    - crates/entorhinal-core/src/mutations.rs
    - crates/entorhinal-core/src/binding.rs
    - crates/entorhinal-core/src/ownership.rs
    - crates/entorhinal-core/src/agent/journal.rs
    - crates/entorhinal-core/src/agent/feed.rs
    - crates/entorhinal-core/src/agent/import.rs
    - crates/entorhinal-core/src/agent/schema.rs
    - crates/entorhinal-module/src/main.rs
    - crates/entorhinal-module/src/agent_ops.rs
    - crates/entorhinal-module/src/agent_reads.rs
    - crates/entorhinal-module/src/cli.rs
    - docs/designs/repository-ownership.md
    - /Users/ufukaltinok/Work/Projects/CortexKit/engram/AGENT_SYNC.md
---

## intent

Step 1 of the approved agent-sync build (engram `AGENT_SYNC.md`, "Identity first" and "The v1 root: engram"). An agent and a project must have the same id, name and placement on every machine the user owns. Today entorhinal's registry exists on one machine only.

After this work, entorhinal's **shared state** is replicated through engram's per-fleet identity log, and its **machine-local state** stays on each machine. The shared state is agent identity, logical projects and workspaces, and each project's root keys. The machine-local state is root paths, clone markers, approvals and overrides.
- Every machine keeps a full local copy and answers every read from it.
- A shared write is validated locally, appended to the log only if the log head is still where this machine last saw it, and then committed locally.
- Writes made on other machines arrive from the log and are applied locally.

Engram is the crypto boundary: entorhinal sends plaintext entries over the daemon and holds no key.

A machine that has never enabled the log behaves exactly as entorhinal does today. Shipping this work changes nothing until an operator enables the log.

This spec also adds root keys and the attach flow (`AGENT_SYNC.md`, "Joining an existing project"). A checkout of an existing project on a second machine joins that project instead of minting a duplicate.

## constraints

### State classes

**Shared tables:** `project`, `workspace` (minus its root column, see below), `project_workspace`, `project_alias` (maps old project ids to project ids, no paths), `agent`, `agent_name_claim`, and the new `project_root_key`.

**Machine-local tables:** `project_root`, `derived_root_parent`, `root_binding`, `retired_binding`, `root_approval`, `root_owned_remotes`, the workspace root path, and `workspace_member`.
- `workspace_member` is derived per machine. Live data has only `ref_kind = 'local'` rows, and they mirror `project_workspace` one to one (30 and 30). It is recomputed from shared placement on each machine and never travels.
- The workspace root path moves out of `workspace` into a machine-local table, because a path is machine-specific. One workspace has a root set today, so the migration carries it over.

### One local journal, with shared rows tagged

Entorhinal keeps its single `registry_journal`. Every row keeps a local `seq`, so `generation` stays the local journal head and the `agent.changes` and `journal_tail` cursors stay local seqs. Core's identity campaign depends on exactly this: ALF confirmed that core treats generation as per machine, paired with incarnation, and never sees log positions.
- A migration adds journal columns: `stream` (`shared` or `local`), `entry_id` (nullable), `log_position` (nullable) and `origin` (`here` or `log`).
- Existing rows are `local` with null log fields.

### What a shared entry carries

- **Agent writes:** the existing after-image (`AgentChangeEntry`) under the op's own name (`agent.create` and so on), unchanged.
- **Project-layer writes:** a generic after-image of the shared project-layer rows the write changed, computed by comparing those tables before and after the write inside its transaction. It records upserted rows in full and deleted rows by key. It is journaled as the new op `project.shared`, one entry per write.
  - The live tables are small (35 projects, 1 workspace, 38 aliases), so a full-table comparison per write is acceptable.
  - The comparison must cover every shared table, so a future shared table can't be missed. A test compares the set of shared tables with the schema.
- **Bootstrap:** `shared.snapshot`, the full shared state.

Entry bodies are JSON, sorted the same way every time, so identical state produces identical bytes. A retry with the same `entry_id` must send identical bytes.

### Replay stays correct, and legacy history is untouched

`rebuild` and `verify` replay journal rows in local `seq` order.
- Local and pre-existing rows replay exactly as today. The journal is append-only, so no existing row is rewritten.
- A `project.shared` or `shared.snapshot` row restores the shared tables it carries from its after-image.
- A shared row with `origin = log` that deletes a project also removes that project's machine-local rows on this machine: roots, derived parents, bindings, approvals and overrides. This cascade is a rule of remote apply, and replay reproduces it.
- `verify`'s journal replay check stays clean across mixed histories. A test builds a store with legacy rows, then local shared writes, then entries applied from a fake log, and asserts that `verify` reports a clean replay.

### The log interface (engram, agreed in #agent-sync, seq 191-225)

- **Append:** `identity_log.append {expected_head, entry_id, entry}` → `{position}`. The refusals are:
  - `head_moved {head}`;
  - `not_member`;
  - `unavailable`.
  An `entry_id` resent with identical bytes answers "already appended" with its position. The same id with different bytes is refused.
- **Read:** `identity_log.read {after, limit}` → `{head, entries: [{position, entry_id, signer, key_id, envelope_version, kind, entry}]}`. `kind` is `change` or `snapshot`, and positions are dense.
- **Who may call:** engram admits only the daemon-verified `reserved:entorhinal` principal.
- **Pinning:** if engram's merged spec names these differently, that spec wins. The log-client slice pins the names against engram's main before it starts.

### The shared write path

Only one shared write is in flight at a time, held by an async writer lock in the module. The writer:
1. Catches up: it reads from its last applied position and applies any remote entries.
2. Opens the write transaction and runs the operation with today's checks, against local state at log head N. It computes the shared entry. The operation's random ids, epochs and timestamps are drawn once, before the transaction, so the entry is fixed.
3. Appends to the log with `expected_head = N` and an `entry_id` minted for this attempt, keeping the transaction open.
4. Acts on the result:
   - **Success:** record `log_position` and `origin = here` on the row, then commit.
   - **`head_moved`:** roll back, catch up, then rerun the whole write from step 2 with a new `entry_id`. A conflict, such as a name taken on another machine, then surfaces through the operation's normal refusal. Retry at most 5 times, then refuse `identity_log_contended`.
   - **`unavailable`:** roll back, and refuse `engram_unavailable`, which is retryable. Core's relay already retries it with the same `request_key`.
   - **`not_member`:** roll back, refuse `identity_log_not_member`, and set the health state.
   - **No reply** (timeout or lost connection): resend the same `entry_id` and bytes until a definite answer comes or 20 s pass. The bound sits under the 30 s default call timeout of core's relay, so core receives this refusal rather than its own timeout. After the bound, roll back and refuse `engram_outcome_unknown`. The entry may have landed. If it did, catch-up later applies it like a remote entry. In that residual case, the operation's machine-local effects (for `register`, the root and binding) are not applied, and health counts it in `logOwnEntriesAppliedFromLog`. The operator re-adds the root. This window is stated, not hidden.

**Reads never wait on engram or on an in-flight shared write.** The write transaction stays open across the network call. So every read path (`resolve`, `resolve_remote`, `enumerate`, `trust`, agent reads, `agent.changes`, liveness, health) is served from a separate read connection under SQLite WAL, which sees the last committed state. Health keeps reading atomics only.

**Machine-local writes never touch engram:** `approve_root`, `unapprove_root`, `attach_derived_parent`, `set_owned_remotes`, `remove_root` without project deletion, the workspace-root setter and liveness all work with engram absent or the log disabled.

**What counts as a shared write:** a write that changes any shared table takes the shared path. That's every agent mutation, `register`, `assign_workspace`, `upgrade_implicit`, `remove`, `seed_import` and `attach_root`, plus `add_root` and `remove_root` whenever they change a root key. Classification comes from the comparison, not a hand list: a write whose comparison is empty commits locally, with no append.

### Catch-up

- **When:** a background task under the writer lock reads from the last applied position every 30 s, and before every shared write.
- **How:** each remote entry is applied in its own transaction. It becomes a journal row with `origin = log`, its `log_position` and `entry_id`, and the entry's op name (`agent.*` keeps its name, so core's `agent.changes` delivers it).
- **Feed:** applying a remote entry wakes `agent.changes` waiters, as a local commit does.
- **Own entries:** an entry whose `entry_id` matches a committed local row is skipped, because it's already applied.
- **Signatures:** engram verifies them. Entorhinal records `signer` and `key_id` on the journal row for audit, and applies entries in position order only. A gap in positions stops catch-up and sets the health state `log_gap` rather than skipping.

### Enabling and joining

The log is off until an operator enables it. While it's off, there's no append, no catch-up and no route to engram, and every reply is byte-identical to today's. That's pinned by the existing goldens.

`identity_log.enable` is a mutating op, admitted from `Direct` (the operator's `ck`) only. Its CLI is `ck projects log enable`.
- **The log is empty:** append one `shared.snapshot` entry of the current shared state as position 1, record it, and switch on.
- **The log is not empty (joining):** this machine's shared tables must be empty (no projects, workspaces or agents). Otherwise refuse `join_requires_empty_registry`, naming the counts. Merging two independent registries is a non-goal. A joining machine then applies every entry from position 1. Its own checkouts join projects through `attach_root`.

`identity_log.status` is a query returning:
- the state: `disabled`, `enabled`, `not_member`, `unavailable` or `log_gap`;
- the last applied position, and the last seen head;
- the pending-write count;
- the last error and its time.
Health carries the same values as atomics.

### Root keys and attaching

A root key is a project root's machine-neutral name: its owned remote, written `owner/repo` in lowercase, or an operator label when it has no owned remote.
- **Shared table:** `project_root_key(project_id, root_key, kind, created_at)`.
  - `kind` is `remote` or `label`.
  - A `remote` key is unique fleet-wide, which is the repository-ownership rule.
  - A `label` key is unique within its project.
- **Local mapping:** a new local column, `project_root.root_key`, maps each local root to its key.
- **Stability:** a root's key is fixed when the root is added. A later `set_owned_remotes` doesn't change it. A mismatch between a root's key and its current owned remote is reported by `trust` and `verify`, never rewritten silently.
- **Roots with neither:** a root with no owned remote and no label has no key. It stays machine-local and can't be attached elsewhere. That's allowed, so registering a non-git folder keeps working.
- **`register` and `add_root`** accept an optional `label`.
- **The uniqueness check** in `register` and `add_root` also compares the incoming owned remote with every shared `remote` root key, not only with the remotes of local roots. A match with another project refuses `root_key_exists {project_id, root_key}`, and the message points to `attach_root`.
- **`resolve_root_key {project_id, root_key}`** (query) returns `{root}` or `none`. It's the lookup core's import path check uses.
- **`attach_root {path, project_id?, label?}`** (mutate, `Direct` or core) adds an existing checkout on this machine as a root of an existing project.
  - With an owned remote, it matches the project by that root key. If `project_id` is given and disagrees, it refuses.
  - Without an owned remote, both `project_id` and `label` must name an existing label key.
  - The new root starts unapproved, with a fresh binding, as `add_root` does today. Attaching never mints a project.
- **CLI:** `ck projects attach <path> [--project ID] [--label L]` prints the matched project and root key, and needs `--yes` to write.

### The engram dependency

Entorhinal becomes a consumer of engram. It opens the route lazily, on the first shared write or catch-up after enabling, never at startup. Startup, readiness and every read must work with engram absent. Whether the manifest declares engram under `requires` depends on SUBC's answer in #agent-sync (seq 225). The default is not to declare it, unless that refuses the route.

### Admission

Today's rules are unchanged for existing ops:
- **Agent writes:** core only, `Direct` refused.
- **Project writes:** `Direct` or core.
- **Flow-scoped routes:** refused.
New ops:
- **`identity_log.enable`:** `Direct` only.
- **`attach_root`:** `Direct` or core.
- **`identity_log.status` and `resolve_root_key`:** open reads.
New error codes: `engram_unavailable`, `engram_outcome_unknown`, `identity_log_not_member`, `identity_log_contended`, `identity_log_disabled` (for `attach_root` and `resolve_root_key` on a disabled log, where they still work locally and refuse only a cross-machine need), `join_requires_empty_registry` and `root_key_exists`.

### Process rules

- **Comments:** they say what and why, for a reader with no context. No task ids or plan references.
- **Gates:** the three gates in `.cortexkit/WORKER.md`.
- **Pins:** don't change any `subc-*` crate pin.
- **Core's contract:** don't change the shape of any existing reply. New fields are additive.

## acceptance sketch

Each test must fail when the behaviour it names breaks. Each slice reports one deliberate break per test and shows the failure.

1. **Disabled is today.** With the log never enabled:
   - every existing golden is unchanged;
   - a full write workload produces the same journal rows as today, apart from the new columns' defaults;
   - no route to engram is opened, which a test asserts with a fake connector that fails on any call.
2. **Two machines converge.** Two stores share one in-process fake log implementing the contract above. On machine A:
   - register a project with a workspace;
   - create and rename an agent;
   - remove a project.
   After B catches up:
   - B's shared tables equal A's, row for row;
   - B's local tables are untouched, apart from the removal cascade;
   - B's `agent.changes` delivers the create and the rename under their own op names;
   - `verify` is clean on both.
3. **Races.** A and B write concurrently:
   - **Conflicting:** both register a project with the same root key. One commits; the other catches up and refuses `root_key_exists`.
   - **Independent:** both append. Both commit, the positions are dense, and the states converge.
4. **Lost reply.** The fake appends and drops the reply. The writer resends the same `entry_id` and bytes, gets "already appended", and commits once. The log holds no duplicate.
5. **Outcome unknown.** The fake hangs past the bound. The write refuses `engram_outcome_unknown` and the transaction rolls back. When the fake answers later, catch-up applies the entry, and health counts it.
6. **Reads don't wait.** While a shared write is parked inside the fake's append:
   - `resolve`, `enumerate`, `agent.resolve` and health each answer within 100 ms;
   - the answers show the pre-write state.
7. **Engram absent.**
   - Startup and every read succeed.
   - A shared write refuses `engram_unavailable`.
   - `approve_root` and `set_owned_remotes` succeed.
   - Core's retry with the same `request_key`, once engram is back, commits exactly once.
8. **Enable and join.**
   - Enabling on an empty log writes position 1 as a snapshot.
   - Enabling on a non-empty log with a non-empty registry refuses `join_requires_empty_registry`, with the counts.
   - A fresh machine joins and reaches A's shared state.
   - Enabling twice is idempotent.
9. **Root keys and attach.**
   - **Machine B, matching remote:** a checkout whose owned remote matches A's project root key is refused by `register` with `root_key_exists`, naming the project. `attach_root` then adds it unapproved, and `resolve_root_key` returns B's path.
   - **No remote:** a root needs a label to be attachable.
   - **Key stability:** after `set_owned_remotes`, `trust` reports the key mismatch and the key is unchanged.
10. **Gaps stop catch-up.** If the fake skips a position, catch-up stops in `log_gap` and applies nothing past the gap.
11. **Every shared table is covered.** A test fails if a shared table exists that the comparison doesn't cover.
12. **Admission.**
    - `identity_log.enable` from core or another module is refused.
    - `attach_root` from a flow-scoped route is refused.
    - Every new mutating op is in the module's mutating list.

## non-goals

- The hosted track: the phone-rooted member list, the fleet group key, `identity_log_ed25519`, attestation. Engram's per-entry key id lets that track re-key later without changing entorhinal.
- Merging two non-empty registries into one log.
- The checkout claim (step 2, engram and prefrontal) and every later step.
- Sharing workspace root paths, `ref_kind = 'remote'` membership, or any machine-local table.
- Automatic attach. Attaching always needs an operator's or core's explicit call.
- Changing core's contract: `generation`, incarnation, the `agent.changes` cursor and every reply shape stay as they are.

## open_questions

None. Engram's interface was settled in #agent-sync (seq 191-225). SUBC's answer on `requires` decides one manifest line, with a stated default.

---
title: "2026-10-05-identity-log refire 1"
status: draft
rounds_cap: 1
mint: auto
refired_from: "ct_00000000-0000-4158-98db-f286ff4dcf60"
integration_ref: "main"
evidence:
  include:
    - "crates/entorhinal-core/src/lib.rs"
    - "crates/entorhinal-core/src/mutations.rs"
    - "crates/entorhinal-core/src/binding.rs"
    - "crates/entorhinal-core/src/ownership.rs"
    - "crates/entorhinal-core/src/agent/journal.rs"
    - "crates/entorhinal-core/src/agent/feed.rs"
    - "crates/entorhinal-core/src/agent/import.rs"
    - "crates/entorhinal-core/src/agent/schema.rs"
    - "crates/entorhinal-module/src/main.rs"
    - "crates/entorhinal-module/src/agent_ops.rs"
    - "crates/entorhinal-module/src/agent_reads.rs"
    - "crates/entorhinal-module/src/cli.rs"
    - "docs/designs/repository-ownership.md"
---

## acceptance_sketch
Each test must fail when the behaviour it names breaks. Each slice reports one deliberate break per test and shows the failure.

1. **Disabled is today.** With the log never enabled:
   - every existing golden is unchanged;
   - a full write workload produces the same journal rows as today, apart from the new columns' defaults, and no `root_key.assign` row;
   - `remove_root` of a project's last root still refuses `last_root`;
   - no route to engram is opened, which a test asserts with a fake connector that fails on any call.
2. **Upgrade.** A v6 fixture sets a workspace root at T1, then renames that workspace at T2. After the migration, the local workspace-root row's `updated_at` is T1, and `verify` and `rebuild` reproduce every migrated cell.
3. **Two machines converge.** Two stores share one in-process fake log implementing the contract above. On machine A:
   - register project P1 placed in a workspace, and project P2;
   - create and rename an agent;
   - remove P2.

   After B catches up:
   - B's shared tables equal A's, row for row;
   - B's `workspace_member` holds P1's derived placement row;
   - B's other local tables are untouched, apart from the removal cascade;
   - B's `agent.changes` delivers the create and the rename under their own op names;
   - `verify` is clean on both.
4. **Remote cascade.** With the fail-closed foreign keys on:
   - B attaches a checkout to P (creating an implicit alias), then A removes P outright. B's catch-up succeeds, and B's roots, mappings and implicit aliases for P are gone.
   - B holds an approved root of P with a captured execution binding, then A removes P into a successor. On B the binding is in `retired_binding` with reason `removed`, its approval is gone, and the root, mapping and aliases now belong to the successor with no binding or approval.
   - B sets a local root on workspace W, then A deletes W. B's workspace-root row is gone, and recreating W's id carries no path.
   - `rebuild` on B reproduces each result.
5. **Races.** A and B write concurrently.
   - **Conflicting:** both register a project with the same root key. One commits; the other catches up and refuses `root_key_exists`.
   - **Independent:** both append. Both commit, the positions are dense, and the states converge.
6. **Lost reply.** The fake appends and drops the reply. The writer resends the same `entry_id` and bytes, with a stale `expected_head`, gets "already appended", and commits once. The log holds no duplicate.
7. **Outcome unknown and crash recovery.**
   - The fake hangs past the deadline. The write refuses `engram_outcome_unknown` and the transaction rolls back. When the fake answers later, catch-up applies the entry once, and health counts it in `logOwnEntriesAppliedFromLog`.
   - The same holds for a process killed between a successful append and the local commit.
   - **In-flight append across a restart:** the fake holds an unresolved append; the process restarts, reads the unchanged head, and keeps the pending row. The fake then accepts the original append, and catch-up applies it once as an own entry and counts it. Once position `expected_head + 1` holds another entry, the pending row is cleared.
   - A process killed right after the local commit restarts with engram absent: the pending-write count is 0, nothing is reapplied, and `logOwnEntriesAppliedFromLog` is unchanged.
8. **Deadlines.** Each of these gets its specified refusal within 25 s:
   - a second writer queued behind a parked append;
   - a catch-up read that hangs before any append;
   - repeated `head_moved` followed by an ambiguous append.
9. **Reads don't wait, and each is one snapshot.**
   - While a shared write is parked inside the fake's append, `resolve`, `enumerate`, `verify`, `agent.resolve` and health each answer within 100 ms, showing the pre-write state.
   - Under a concurrent stream of commits, every `enumerate` and `resolve` reply's rows and `generation` belong to one committed state.
10. **Engram absent.**
    - Startup and every read succeed.
    - A shared write refuses `engram_unavailable`.
    - With a connector that fails on any call, these all succeed:
      - `approve_root` and `set_owned_remotes`;
      - `remove_root` of a root with an implicit alias;
      - `remove_root` of a project's last local root on an enabled log, leaving the project and its shared keys;
      - two `set_workspace_root` calls at distinct timestamps.
    - Core's retry with the same `request_key`, once engram is back, commits exactly once.
11. **Request cache and rebuild.** An enabled `register` with local roots, a derived parent and a `request_key`:
    - survives `rebuild` exactly;
    - after a restart, a same-key retry returns identical cached bytes with no append.
    - A remote-backed root registered after enabling, with its git config then removed, is rebuilt with the same shared key row and local mapping.
12. **Enable and join.**
    - Enabling on an empty log writes position 1 as a snapshot.
    - A crash after that append and before the local commit, followed by a retry, completes enabling rather than refusing as a join.
    - An empty machine with no `agent.cutover` marker enables successfully, and a core-admitted `agent.create` then succeeds. A machine with agents and no marker refuses `authority_not_cut_over`.
    - Enabling on a non-empty log with a non-empty registry refuses `join_requires_empty_registry`, with the counts. On a store with projects, no agents and no marker, that refusal leaves no `agent.cutover` marker, and a later valid `agent.import` succeeds.
    - `agent.import` while the state is `enabling` refuses `identity_log_enabling`.
    - A fresh machine joins and reaches A's shared state. A crash mid-join resumes without duplicates.
    - An `agent.changes` consumer that snapshotted B before the join, with a waiter outstanding during it, receives every bootstrap agent.
    - After the join, a core-admitted `agent.rename` on B succeeds.
    - Enabling twice is idempotent.
13. **Root keys and attach.**
    - **Populated store:** a v6 store with existing roots is enabled after upgrade. The bootstrap carries their backfilled keys, and B can attach a matching checkout. `rebuild` reproduces the mappings with the original git config removed.
    - **Machine B, matching remote:** a checkout whose owned remote matches A's project key is refused by `register` with `root_key_exists`, naming the project. `attach_root` then adds it unapproved, and `resolve_root_key` returns B's path.
    - **Multiple remotes and clones:** a root with several owned remotes gets the smallest. Two local clones sharing a key are both returned, sorted. A label whose text equals a remote key matches neither the remote nor the other label.
    - **Secondary remotes (stated limitation):** A's root owns `a/a` and `z/z`; B registers a new project whose only remote is `z/z`, and it succeeds.
    - **No remote:** a root needs a label to be attachable.
    - **Key stability:** after `set_owned_remotes`, `trust` reports the mismatch and the key is unchanged.
    - **Lifecycle:** after A removes its last root of a project still attached on B, B's key and root survive catch-up and `rebuild`. A `remove` into a successor moves the keys.
    - **Same path, different repository:** A registers P at `/checkout`, and B registers a different repository Q at its own `/checkout`. On B, the path and its implicit id both resolve to Q.
14. **Catch-up fails closed.** Each of these stops catch-up with nothing applied past the failing position and `last_applied_position` unchanged, and a following shared write refuses `identity_log_stalled`:
    - the fake skips a position (`log_gap`);
    - an entry with an unsupported op (`apply_failed`);
    - an after-image that violates a database constraint (`apply_failed`).
15. **Every shared table is covered.** A test fails if a shared table exists that the comparison doesn't cover.
16. **Admission.**
    - `identity_log.enable` from core or another module is refused.
    - `attach_root` from a flow-scoped route is refused.
    - Every new mutating op is in the module's mutating list.

## constraints
### State classes

**Shared tables:** `project`, `workspace` (minus its root column, see below), `project_workspace`, `project_alias` rows that are not implicit-path aliases, `agent`, `agent_name_claim`, and the new `project_root_key`.

**Machine-local tables:** `project_root`, `derived_root_parent`, `root_binding`, `retired_binding`, `root_approval`, `root_owned_remotes`, the workspace root path, `workspace_member`, and the new `identity_log_state`.
- **`workspace_member`** is derived per machine. Live data has only `ref_kind = 'local'` rows, and they mirror `project_workspace` one to one (30 and 30). Each machine recomputes it from shared placement, and it never travels.
- **Workspace root path:** it moves out of `workspace` into a machine-local table that carries its own `updated_at`, because a path is machine-specific. One workspace has a root set today, so the migration carries it over.
  - The migrated `updated_at` is the timestamp of that workspace's latest `set_workspace_root` journal row, which is the value replay produces. It is not the current `workspace.updated_at`, which a later rename may have moved. The root is set only through `set_workspace_root` (`crates/entorhinal-core/src/lib.rs:103-106`).
  - When the log is enabled, `set_workspace_root` writes only that table and leaves `workspace.updated_at` alone. Today it bumps `workspace.updated_at` (`crates/entorhinal-core/src/mutations.rs:681-684`).
- **Implicit-path aliases are machine-local.** `implicit_project_id` hashes only the canonical path (`crates/entorhinal-core/src/lib.rs:850-855`), so the same path on two machines can name different repositories. `project_alias` rows whose `old_id` is an implicit id (`pj-implicit1-`) are excluded from the shared comparison and never travel.
  - `remove_root`'s alias delete (`crates/entorhinal-core/src/binding.rs:1092-1095`) is therefore a local effect.
  - Stated limitation: an implicit id resolves only on the machine that registered its path.

### One local journal, with shared rows tagged

Entorhinal keeps its single `registry_journal`. Every row keeps a local `seq`, so `generation` stays the local journal head and the `agent.changes` and `journal_tail` cursors stay local seqs. Core's identity campaign depends on exactly this: ALF confirmed that core treats generation as per machine, paired with incarnation, and never sees log positions.
- **New columns:** a migration adds `stream` (`shared` or `local`), `entry_id` (nullable), `log_position` (nullable), `origin` (`here` or `log`) and `entry` (nullable: the shared entry bytes). Existing rows are `local` with null log fields.
- **A shared write made here keeps today's row.** It is journaled under its own op (`register`, `agent.rename`, and so on), with the same payload, `request_key` and cached reply as today, tagged `stream = shared`, `origin = here`, its `entry_id`, its `log_position` and its `entry`.
  - The request cache (`crates/entorhinal-core/src/mutations.rs:304-318`, which keys on `request_key` and op) therefore works unchanged. A same-key retry, including one after a restart, returns the cached bytes without another append.
- **Root-key choices are journaled.** Whenever an enabled-mode write chooses a root's key (`register`, `add_root`, `attach_root`), the same transaction journals a `root_key.assign` row (`stream = local`, no `request_key`) carrying each `{canonical_root, kind, root_key}` mapping. The shared `project_root_key` rows travel in the op row's `entry`. A disabled log chooses no keys and writes neither, so disabled histories stay as they are today.
- **A remote entry** becomes a row with `origin = log`. Agent entries keep their op name. Project-layer entries use op `project.shared`, and bootstrap uses `shared.snapshot`. These rows carry no `request_key`.
- **Enable marker:** enabling journals an `identity_log.enable` row. Replay applies enabled-mode rules to rows after it: `set_workspace_root` touches only the local table, and `remove_root` may remove a project's last local root.

### What a shared entry carries

- **Agent writes:** the existing after-image (`AgentChangeEntry`) under the op's own name, unchanged.
- **Project-layer writes:** a generic after-image of the shared project-layer rows the write changed. It is computed by comparing those tables before and after the write inside its transaction, and records upserted rows in full and deleted rows by key. There is one entry per write, applied remotely as `project.shared`.
  - The live tables are small (35 projects, 1 workspace, 38 aliases), so a full-table comparison per write is acceptable.
  - The comparison must cover every shared table. A test compares the set of shared tables with the schema.
- **Bootstrap:** `shared.snapshot`, the full shared state, with agents in `AgentChangeEntry` form.

Entry bodies are JSON, sorted the same way every time, so identical state produces identical bytes. A retry with the same `entry_id` must send identical bytes.

### Replay and verify

`rebuild` and `verify` replay journal rows in local `seq` order.
- **Local and pre-existing rows** replay exactly as today. The journal is append-only, so no existing row is rewritten.
- **`origin = here` shared rows** run the op's existing replay for machine-local effects (roots, derived parents, bindings), then restore shared tables from the stored `entry`. `root_key.assign` and `root_key.backfill` rows restore local mappings. `rebuild` therefore never rereads git.
- **`project.shared` and `shared.snapshot` rows** restore the shared tables they carry from their after-images.
- **Remote cascade:** applying an `origin = log` row, and replaying it, keeps this machine's local rows consistent with `project`'s foreign keys (`crates/entorhinal-core/src/lib.rs:81-100`) and with today's `remove` lifecycle:
  - a removed project, with or without a successor, first has its active bindings retired (reason `removed`) and their approvals deleted, as `retire_project_bindings` does (`crates/entorhinal-core/src/binding.rs:506-527`, called at `crates/entorhinal-core/src/mutations.rs:1475`). No approved registration epoch survives under another project;
  - a project deleted outright then also loses its local roots, derived parents, overrides, root-key mappings and implicit aliases pointing at it;
  - a project removed into a successor then moves its local roots (with their root-key mappings), derived parents and aliases to the successor, as today's `remove` replay does (`crates/entorhinal-core/src/mutations.rs:1476-1490`). The moved roots carry no binding or approval until re-bound and re-approved;
  - a deleted workspace also deletes its local workspace-root row and derived `workspace_member` rows.
- **`verify` never writes the live database.** Today `replay_and_compare` deletes and replays the projection inside a transaction before rolling back (`crates/entorhinal-core/src/mutations.rs:1149-1174`). That needs the writer lock, which an in-flight shared write holds. Instead, `verify` copies the committed state from a read connection into a private in-memory database and runs the same replay and comparison there.
- **Mixed histories:** a test builds a store with legacy rows, then local shared writes, then entries applied from a fake log, and asserts that `verify` reports a clean replay.

### Durable log state

`identity_log_state` is a single machine-local row:
- `state`: `disabled`, `enabling`, `joining` or `enabled`;
- `last_applied_position` and `last_seen_head`;
- `enable_entry_id`, plus, while `enabling`, the stored snapshot bytes (`enable_entry`) and the backfill mappings (`enable_backfill`) that enabling will commit.

It also holds a `pending_entry` table of attempts, each an `entry_id` and the `expected_head` it was appended against.
- **Applying entries:** each applied remote entry advances `last_applied_position` in the same transaction as its journal row and projection changes. A crash therefore never applies an entry twice or skips one.
- **Pending attempts:** before opening a shared write's transaction, the writer commits the attempt's `entry_id` and `expected_head` to `pending_entry` in its own short transaction.
  - A successful write deletes its pending row **inside its own commit**, so no committed write is ever left pending. A definite refusal (`head_moved`, `unavailable`, `not_member`) proves nothing was appended, so the row is deleted in a short transaction.
  - An entry that catch-up finds in `pending_entry`, but with no committed journal row, is the writer's own entry that landed without a local commit. Examples are `engram_outcome_unknown` and a crash between append and commit. Catch-up applies it as a remote entry, clears it, and counts it in `logOwnEntriesAppliedFromLog`.
  - A row with no definite outcome (`engram_outcome_unknown`, or a crash, including on startup) stays pending until catch-up has read position `expected_head + 1` and found a different `entry_id` there. An append conditioned on `expected_head` can land only at that position, so the attempt can then never land. Reaching the current head is not proof: an append still in flight can land after the read.

### The log interface (engram, agreed in #agent-sync, seq 191-225)

- **Append:** `identity_log.append {expected_head, entry_id, entry}` → `{position}`. The refusals are `head_moved {head}`, `not_member` and `unavailable`.
  - An `entry_id` resent with identical bytes answers "already appended" with its position, **whatever `expected_head` says**. A resend after a lost reply depends on this.
  - The same id with different bytes is refused.
  - `unavailable` must mean nothing was appended.
- **Read:** `identity_log.read {after, limit}` → `{head, entries: [{position, entry_id, signer, key_id, envelope_version, kind, entry}]}`. `kind` is `change` or `snapshot`, and positions are dense.
- **Who may call:** engram admits only the daemon-verified `reserved:entorhinal` principal.
- **Pinning:** if engram's merged spec names these differently, that spec wins. The log-client slice pins the names, the deduplication precedence, entry-size limits and read paging against engram's main before it starts.

### The shared write path

Only one shared write is in flight at a time, held by an async writer lock in the module. Every shared write has a **25 s end-to-end deadline** from receipt, under the 30 s default call timeout of core's relay. The deadline covers the lock wait, connecting to engram, catch-up and every attempt. The writer:
1. Catches up: it reads from `last_applied_position` and applies any remote entries. If catch-up is stopped (`log_gap` or `apply_failed`), the write refuses `identity_log_stalled`.
2. Mints this attempt's `entry_id` and records it, with `expected_head = N`, in `pending_entry`.
3. Opens the write transaction and runs the operation with today's checks (except enabled-mode `remove_root`, see Root keys), against local state at log head N. It computes the shared entry. The operation's random ids, epochs and timestamps are drawn once, before the first attempt, so the entry is fixed.
4. Appends with `expected_head = N`, keeping the transaction open.
5. Acts on the result:
   - **Success:** tag the row with its `log_position`, delete the pending row, and commit.
   - **`head_moved`:** roll back, catch up, and rerun from step 2. A conflict, such as a name taken on another machine, then surfaces through the operation's normal refusal. After 5 retries, refuse `identity_log_contended`.
   - **`unavailable`:** roll back, and refuse `engram_unavailable`, which is retryable.
   - **`not_member`:** roll back, refuse `identity_log_not_member`, and set the health state.
   - **No reply** (timeout or lost connection): resend the same `entry_id` and bytes until a definite answer comes or the deadline passes. Then roll back and refuse `engram_outcome_unknown`. The entry may have landed, in which case catch-up applies it as a remote entry (see Durable log state). In that residual case, the operation's machine-local effects (for `register`, the root and binding) are not applied, and health counts it. The operator re-adds the root. This window is stated, not hidden.
- **Deadline expiry before any append was sent:**
  - while waiting for the lock, the refusal is `identity_log_contended`;
  - while connecting or catching up, it is `engram_unavailable`.
  Either way nothing has landed.

**`agent.import` is never a shared write.** It is a one-time, multi-row operation (`crates/entorhinal-core/src/agent/import.rs:259-300`), and it must happen before enabling (see Enabling). It refuses `identity_log_enabling` while the state is `enabling` or `joining`, so no agent can enter after the bootstrap snapshot is computed. On a joined machine, the bootstrap writes the `agent.cutover` marker, so a later import refuses `import_already_done` (`crates/entorhinal-core/src/agent/import.rs:272-275`).

**Reads never wait on engram or on an in-flight shared write.** The write transaction stays open across the network call. So every read path is served from a separate read connection under SQLite WAL: `resolve`, `resolve_remote`, `enumerate`, `trust`, `verify`, agent reads, `agent.changes`, liveness and health.
- Each read response runs inside **one read transaction**, so all its statements and the `generation` it reports come from one committed state.
- Health keeps reading atomics only.

**Machine-local writes never touch engram:**
- `approve_root`, `unapprove_root`, `attach_derived_parent` and `set_owned_remotes`;
- `remove_root` (it never changes a shared table; see Root keys);
- `set_workspace_root` while the log is enabled;
- liveness.
All of them work with engram absent or the log disabled.

**What counts as a shared write:** a write whose comparison changes any shared table. That's every agent mutation except import, `register`, `assign_workspace`, `upgrade_implicit`, `remove`, `seed_import` and `attach_root`, plus `add_root` when it creates a new root key. Classification comes from the comparison, not a hand list: a write whose comparison is empty commits locally, with no append.

### Catch-up

- **When:** a background task under the writer lock reads from `last_applied_position` every 30 s, and before every shared write.
- **How:** each remote entry is applied in its own transaction, as a journal row with `origin = log`, its `log_position`, `entry_id`, `signer` and `key_id`.
- **Feed:** applying an entry wakes `agent.changes` waiters, as a local commit does.
- **Own entries:** an entry whose `entry_id` matches a committed local row is skipped and only advances the position.
- **Signatures and gaps:** engram verifies signatures. Entorhinal applies entries in position order only. A gap in positions stops catch-up and sets the health state `log_gap` rather than skipping.
- **Fail closed:** an entry this binary cannot decode or apply (unknown `kind`, op, table, column or `envelope_version`, or an after-image that violates a database constraint) rolls back its transaction: no journal row, no projection change, `last_applied_position` unchanged. Catch-up stops, the health state becomes `apply_failed` with the position and error, and shared writes refuse `identity_log_stalled`. Remote apply must not reuse replay's silent skip of unknown ops (`crates/entorhinal-core/src/mutations.rs:1507`).

### Enabling and joining

The log is off until an operator enables it. While it's off, there's no append, no catch-up and no route to engram, and every reply is byte-identical to today's. That's pinned by the existing goldens.

`identity_log.enable` is a mutating op, admitted from `Direct` (the operator's `ck`) only. Its CLI is `ck projects log enable`. It runs under the writer lock, so no shared write interleaves.

**Preconditions come before any durable transition.** From `disabled`, enable first reads the log head and checks, without writing anything:
- **cutover:** a machine with agents but no `agent.cutover` marker refuses `authority_not_cut_over`;
- **empty log:** the backfill (see Root keys) is computed in memory; two projects resolving to the same remote key refuse `root_key_exists`, naming both;
- **non-empty log:** this machine's shared tables must be empty (no projects, workspaces or agents), else it refuses `join_requires_empty_registry`, naming the counts. Merging two independent registries is a non-goal.

A refusal here, or a failed log read (`engram_unavailable`), leaves the state `disabled`, no marker and no new rows, so `agent.import` still works. A machine with no agents and no marker gets the `agent.cutover` marker (an import of zero agents) only in the step-3 transaction when enabling, or in the transaction applying the bootstrap snapshot when joining. Once the state has left `disabled`, failures are resumable, not undone, except as stated below. An enabled machine can always perform admitted agent writes.

**The log is empty (enabling):**
1. In one transaction, set the state to `enabling` and store `enable_entry_id`, the `shared.snapshot` bytes (current shared state plus the backfilled keys) as `enable_entry`, and the backfill mappings as `enable_backfill`. Nothing shared or journaled changes yet.
2. Append `enable_entry` as position 1 under `enable_entry_id`, with `expected_head = 0`.
3. In one transaction: write the backfilled `project_root_key` rows and local mappings and journal them as one `root_key.backfill` row, journal the `agent.cutover` marker if needed, journal `identity_log.enable`, clear the stored enable fields, and switch to `enabled`.

**Recovery from `enabling`:** a retry, or a restart, resends the stored `enable_entry` under the same `enable_entry_id`, so the bytes are identical. `unavailable`, no reply or a crash keep the state `enabling` (import stays fenced; `identity_log.status` reports it) until a retry gets a definite answer. If position 1 carries `enable_entry_id`, enabling completes at step 3 and is not treated as a join. If position 1 holds a different `entry_id` (another machine enabled first), this snapshot can never land, so one transaction returns the state to `disabled` and clears the enable fields; since step 1 wrote nothing else, the store is as before the attempt. The enable then refuses as the non-empty-log precondition dictates.

**The log is not empty (joining):**
- After the preconditions pass, the state becomes `joining`, and the machine applies every entry from position 1.
- Applying the bootstrap `shared.snapshot` journals:
  - one `agent.import` row per agent, which `agent.changes` delivers (`crates/entorhinal-core/src/agent/journal.rs:19-29`);
  - then the `agent.cutover` marker, so core-admitted agent mutations work on the joined machine;
  - then a `shared.snapshot` row for the project layer.
- A consumer whose cursor predates the join therefore receives every bootstrap agent.
- A failure after `joining` committed, including a log read failing after the bootstrap was applied, keeps `joining` and any marker already written. A restart or retry resumes from `last_applied_position` without rechecking emptiness, and applies no entry twice.
- The machine's own checkouts join projects through `attach_root`.

Enabling twice is idempotent.

**`identity_log.status`** is a query returning:
- the state (`disabled`, `enabling`, `joining`, `enabled`, `not_member`, `unavailable`, `log_gap` or `apply_failed`);
- the last applied position and the last seen head;
- the pending-write count;
- the last error and its time.
Health carries the same values as atomics.

### Root keys and attaching

A root key is a project root's machine-neutral name, a pair of `kind` and text:
- `remote`: an owned remote, written `owner/repo` in lowercase;
- `label`: an operator label.
Keys always match on both parts, so a label whose text looks like a remote never matches a remote key.

**Shared table:** `project_root_key(project_id, kind, root_key, created_at)`.
- A `remote` key belongs to at most one project fleet-wide.
- A `label` key is unique within its project.

**Local mapping:** a new local column, `project_root.root_key`, maps each local root to at most one key. Several local roots may map to the same key. Every mapping is journaled (`root_key.assign` or `root_key.backfill`).

**Choosing a root's key:** this happens when the root is added, attached or backfilled.
- One effective owned remote: that remote.
- Several: the lexicographically smallest, so two machines with the same remotes agree. `trust` lists the others.
- None: the given label.
- No remote and no label: no key. The root stays machine-local and can't be attached elsewhere, so registering a non-git folder keeps working.
- **Stability:** a later `set_owned_remotes` doesn't change the key. `trust` and `verify` report a mismatch rather than rewriting it.
- **Stated limitation, secondary remotes:** fleet-wide uniqueness covers only each root's chosen key. Today's `repository_owner` check (`crates/entorhinal-core/src/binding.rs:1018`) sees only local roots, so if A's root owns `a/a` and `z/z`, B can register another project whose only remote is `z/z`. Both then own `z/z`. This is accepted for this step and is not detected.

**Backfill at enable:** roots that exist before enabling get keys by the rule above, read from current effective remotes once, during the enable preconditions. The result is stored with the enabling state and journaled once as a `root_key.backfill` row at enable step 3, so neither a resend nor `rebuild` rereads git. The backfilled shared rows are part of the bootstrap snapshot.

**Lifecycle:** shared keys are deleted only with their project, because no machine can see whether another machine still uses a key.
- **`remove_root`** removes only the local root and its mapping. Releasing a remote key without removing its project is out of scope.
  - When the log is enabled, `remove_root` may remove a project's last local root. The project and its shared keys stay, because other machines may still hold roots. A disabled store keeps today's `last_root` refusal (`crates/entorhinal-core/src/binding.rs:1291-1304`).
- **`remove` with `successor_project_id`** moves the project's keys to the successor. A label key the successor already holds collapses into one row. A remote key cannot collide, because each remote key belongs to one project.

**Operations:**
- **`register` and `add_root`** accept an optional `label`. Their uniqueness check also compares the incoming key with every shared `remote` key. A match with another project refuses `root_key_exists {project_id, root_key}`, and the message points to `attach_root`.
- **`resolve_root_key {project_id, kind, root_key}`** (query) returns `{roots: [...]}`: every local root mapped to that key, sorted by canonical path, possibly empty. It's the lookup core's import path check uses.
- **`attach_root {path, project_id?, label?}`** (mutate, `Direct` or core) adds an existing checkout on this machine as a root of an existing project.
  - With an owned remote, it matches the project by that key. If `project_id` is given and disagrees, it refuses.
  - Without one, `project_id` and `label` must name an existing label key.
  - The new root starts unapproved, with a fresh binding, as `add_root` does today. Attaching never mints a project.
- **CLI:** `ck projects attach <path> [--project ID] [--label L]` prints the matched project and root key, and needs `--yes` to write.

### The engram dependency

Entorhinal becomes a consumer of engram. It opens the route lazily, on the first shared write or catch-up after enabling (or on `identity_log.enable` itself), never at startup. Startup, readiness and every read must work with engram absent. Whether the manifest declares engram under `requires` depends on SUBC's answer in #agent-sync (seq 225). The default is not to declare it, unless that refuses the route.

### Admission

Today's rules are unchanged for existing ops:
- **Agent writes:** core only, `Direct` refused.
- **Project writes:** `Direct` or core.
- **Flow-scoped routes:** refused.

New ops:
- **`identity_log.enable`:** `Direct` only.
- **`attach_root`:** `Direct` or core.
- **`identity_log.status` and `resolve_root_key`:** open reads.

New error codes:
- `engram_unavailable`, `engram_outcome_unknown`, `identity_log_not_member`, `identity_log_contended` and `identity_log_stalled`;
- `identity_log_disabled`, for `attach_root` and `resolve_root_key` on a disabled log, where they still work locally and refuse only a cross-machine need;
- `identity_log_enabling`, for `agent.import` while enabling or joining;
- `join_requires_empty_registry` and `root_key_exists`.

### Process rules

- **Comments:** they say what and why, for a reader with no context. No task ids or plan references.
- **Gates:** the three gates in `.cortexkit/WORKER.md`.
- **Pins:** don't change any `subc-*` crate pin.
- **Core's contract:** don't change the shape of any existing reply. New fields are additive.

## intent

Step 1 of the approved agent-sync build (engram `AGENT_SYNC.md`, "Identity first" and "The v1 root: engram"). An agent and a project must have the same id, name and placement on every machine the user owns. Today entorhinal's registry exists on one machine only.

After this work, entorhinal's **shared state** is replicated through engram's per-fleet identity log, and its **machine-local state** stays on each machine. The shared state is agent identity, logical projects and workspaces, and each project's root keys. The machine-local state is root paths, clone markers, approvals and overrides.
- Every machine keeps a full local copy and answers every read from it.
- A shared write is validated locally, appended to the log only if the log head is still where this machine last saw it, and then committed locally.
- Writes made on other machines arrive from the log and are applied locally.

Engram is the crypto boundary: entorhinal sends plaintext entries over the daemon and holds no key.

A machine that has never enabled the log behaves exactly as entorhinal does today. Shipping this work changes nothing until an operator enables the log.

This spec also adds root keys and the attach flow (`AGENT_SYNC.md`, "Joining an existing project"). A checkout of an existing project on a second machine joins that project instead of minting a duplicate.


## non_goals

- The hosted track: the phone-rooted member list, the fleet group key, `identity_log_ed25519`, attestation. Engram's per-entry key id lets that track re-key later without changing entorhinal.
- Merging two non-empty registries into one log.
- The checkout claim (step 2, engram and prefrontal) and every later step.
- Sharing workspace root paths, `ref_kind = 'remote'` membership, or any machine-local table.
- Automatic attach. Attaching always needs an operator's or core's explicit call.
- Changing core's contract: `generation`, incarnation, the `agent.changes` cursor and every reply shape stay as they are.


## open_questions

None. Engram's interface was settled in #agent-sync (seq 191-225). SUBC's answer on `requires` decides one manifest line, with a stated default.

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded owner answer 1

Campaign `ct_00000000-0000-4158-98db-f286ff4dcf60`, round 3, decision sequence 3 (source: owner decision):

park for chair rulings

### recorded supplied ruling 2

Campaign `ct_00000000-0000-4158-98db-f286ff4dcf60`, round 4, decision sequence 0 (source: {"body_bytes":4809,"boundary":"ledger_payload","order":4,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4158-98db-f286ff4dcf60-20261005T193400Z.md"}):

These rulings bring in seven facts settled in #agent-sync and in round 3, then one review round, then the slice plan. They're also committed at `.cortexkit/alfonso/drafts/2026-10-05-identity-log-rulings-r1.md` (entorhinal e247ed6).

R1. Engram's append has a fourth refusal, `id_reused`: a retry that sends the same entry_id with different bytes. Entorhinal never sends different bytes under one id, because the id is minted per attempt and a resend reuses the exact bytes. So `id_reused` means an entorhinal bug. On `id_reused`, the writer rolls back, refuses `identity_log_invariant`, sets the health state, and logs a warning naming the entry_id. It never retries with a new id. Acceptance: a fake that answers `id_reused` produces that refusal and leaves no partial state. Revise constraints and acceptance_sketch.

R2. Entorhinal declares nothing about engram in its manifest `requires`. A `requires` entry whose capability has no provider makes the daemon refuse route.open to entorhinal itself (`module_warming`, reason `required_capability_unprovided`), which would refuse every read while engram is down. A consumer route from entorhinal to engram needs no declaration and is admitted on engram's readiness and entorhinal's verified identity. The manifest test keeps `requires` empty. Revise constraints ("The engram dependency"): drop its conditional sentence and state this rule.

R3. Entry shape. `append`'s `entry` is `{kind, data}`: `kind` is `change` or `snapshot`, chosen by entorhinal and never inferred by engram, and `data` is opaque bytes, sent as lowercase hex on the frame. `read` returns each entry's `kind` plus `entry` (the appended `data`). The other fields are as drafted. Entorhinal's sorted JSON entry body is the `data` bytes. Reusing an `entry_id` with only `kind` changed also refuses `id_reused`. Revise constraints ("The log interface").

R4. Size caps: `data` is at most 256 KiB; a `read` page is at most 128 entries and 1 MiB.
- A shared entry whose body exceeds 256 KiB is refused before any append, with `shared_entry_too_large` naming the size.
- A snapshot is written as consecutive `snapshot` entries, each at most 200 KiB. Each carries `{snapshot_id, part, parts}` and a slice of the shared rows; together they form one snapshot. A joining machine applies a snapshot only once all its parts are present, in order, in one transaction. A part missing at the head means "snapshot incomplete": catch-up waits and applies nothing from it.
- Catch-up pages with `limit` at most 128 and follows `head` until it's reached.
Acceptance: a snapshot larger than one entry round-trips through the fake log in parts and reproduces the shared tables exactly. An oversize change entry refuses with nothing written. Revise constraints and acceptance_sketch.

R5. Two more engram errors:
- `verify_failed` on `read` means an entry failed engram's signature, roster or rollback checks. Catch-up stops at that position, sets the health state `log_verify_failed`, and applies nothing at or after it, the same rule as a gap.
- `key_unavailable` (engram can't derive the log key) is refused as `engram_key_unavailable`, which is retryable. It's reported distinctly from `engram_unavailable` and sets its own health state.
Add an acceptance case for each. Revise constraints and acceptance_sketch.

R6. Signatures prove authorship, not freshness. Catch-up never moves its applied position backwards: a `read` whose `head` is below entorhinal's last applied position sets the health state `log_head_regressed` and is ignored. Add this to constraints ("Catch-up") and one acceptance case.

R7. Enable and join state transitions (resolves round 3's one blocking finding). Validate every enable or join precondition before committing any state transition: an empty registry for a join, no root_key_exists conflict in the backfill, a reachable log, and membership. A precondition failure leaves the state `disabled` with nothing written, so `agent.import` and every other write stay callable. Once the first snapshot part has been appended (enable) or the bootstrap has been applied (join), the state is durable (`enabling` or `joining`). Any later failure (log read, outage, verify_failed) keeps that state and its import fence, and the next enable call or catch-up resumes from the recorded position without duplicating entries. Only a failure proven to precede any append returns to `disabled`. Acceptance: a backfill root-key conflict leaves `disabled`, writes nothing, and `agent.import` still works. An outage after the join bootstrap keeps `joining` and its fence, and the resume reaches the head with no duplicate journal rows. Revise constraints and acceptance_sketch.

R8. The intent, non_goals and open_questions sections need no revision for R1 to R7: keep their current text exactly.

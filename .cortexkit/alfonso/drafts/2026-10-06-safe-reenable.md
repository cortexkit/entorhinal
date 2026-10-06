---
title: "Safe re-enable of the identity log: import first, then supersede this machine's own earlier snapshot"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - "crates/entorhinal-module/src/enable.rs"
    - "crates/entorhinal-core/src/enable_state.rs"
    - "crates/entorhinal-module/src/catch_up.rs"
    - "crates/entorhinal-core/src/remote_apply.rs"
    - "crates/entorhinal-core/src/shared_entry.rs"
    - "crates/entorhinal-module/src/log_client.rs"
    - "crates/entorhinal-module/src/fake_log.rs"
    - "crates/entorhinal-module/src/cli.rs"
    - "crates/entorhinal-core/src/agent/import.rs"
    - "crates/entorhinal-core/src/agent/reads.rs"
    - "crates/entorhinal-module/src/two_machine_acceptance.rs"
    - ".cortexkit/alfonso/specs/ct_00000000-0000-41b6-98db-ff541db0ce88.md"
---

## intent
Two defects surfaced when the shared identity log was first enabled on the operator's Mac. They have one root cause: enable assumed it was always the first and only bootstrap.

1. **Enable closed the agent import.** On a machine with no agents and no `agent.cutover` marker, enable writes the marker as an import of zero agents (`enable_state.rs` `ensure_cutover`). After that, `agent.import` refuses `import_already_done` (`agent/import.rs`). The reviewed identity-log spec says the import must run before enabling, but nothing enforced that order. So one operator command, run in the wrong order, silently closed prefrontal-core's only path for moving its 35 agents into entorhinal.
2. **Enable can't be undone and redone.** Engram's log is append-only by design: a rewound head is exactly what members treat as tampering. After the undo, the operator's store was restored to its pre-enable state, but the log still holds that store's first snapshot at position 1. Enabling again today refuses `join_requires_empty_registry`, because head > 0 and the registry isn't empty. Nothing can recover from this short of building a new path.

The end state:
- enable refuses to close the agent import by accident;
- a machine can supersede its own earlier snapshot, and only its own;
- every reader, joining or already a member, ends with exactly the newest snapshot's state plus the entries after it;
- this cannot be used to erase another machine's entries.

## constraints

### The import-order guard
- `identity_log.enable` refuses `agents_not_imported` when the store has zero agents and no `agent.cutover` marker. The message says to run the agent import first, or to pass `--without-agents` when the fleet has no agents to import. It refuses before reading the log and writes nothing, so the state stays `disabled`.
- `ck projects log enable --without-agents` passes `{"without_agents": true}`. Only then may enable write the zero-agent marker, exactly as today. The flag is refused `invalid_request` when the store already has agents or a marker, so it can never mask a real import.
- A join (head > 0, empty registry) is unaffected. Its marker comes from the bootstrap snapshot it applies, as today.

### Authorship from engram
- `identity_log.read` marks each entry `signed_by_self: true|false`. Engram computes it from the roster pseudonym bound to the entry's signing key at the head it was signed under, so it survives key rotation. Entorhinal never learns, stores or shows a device id.
- Each read entry also carries `author`: that same rotation-stable pseudonym, as 32 lowercase hex characters. It is the entry's author, not the reader's. Entorhinal compares authors only for equality between entries, and never stores, logs or shows them. Both fields are on engram master (commits d9ec33d and ee1d519).
- `log_client::LogEntry` gains `signed_by_self: bool` and `author: String`. A reply missing either field is a protocol error, which stalls catch-up exactly as an undecodable entry does today. The in-tree fake log models both, with an explicit author per appending store.

### Superseding a machine's own snapshot (the writer)
- Enable reads the whole log, 1..head (paged as today). It then decides by these rules, in this order:
  - head = 0: today's bootstrap, with the import-order guard above.
  - head > 0 and the registry is empty: today's join.
  - head > 0, the registry is not empty, and every entry 1..head is `signed_by_self`: supersede.
  - Anything else: `join_requires_empty_registry`, as today, with the counts.
- A supersede appends a fresh snapshot of the current local state, built by today's `prepare_enable`, at positions head+1 .. head+parts:
  - part i is sent with `expected_head: head + i`;
  - every part's body carries `supersedes_through: head`;
  - the saved enable parts record the base head, so resume recomputes each part's expected position;
  - resume reads from the base, not from 0.
- Lost race: a `head_moved` on the first part means something landed after the read. Enable never retries blindly. It saves nothing new, abandons the attempt back to `disabled`, and returns `identity_log_contended` so the operator re-runs it, which re-reads and re-checks. Engram's compare-and-swap guarantees nothing slips between the read and the first append.
- On finish, `last_applied_position` and `last_seen_head` are set to head+parts, so this machine never applies its own superseded entries or its own new snapshot. The journal row `identity_log.enable` records `supersedes_through`.
- The import-order guard applies to a supersede exactly as to a first bootstrap.

### Applying a superseding snapshot (every reader)
- A snapshot whose parts carry `supersedes_through: S` is valid only if:
  - S equals the first part's position − 1;
  - every entry at positions 1..S has the same `author` as every part of the snapshot.

  Otherwise catch-up stalls with an invariant error naming the position, and applies nothing. This is the reader's guard: without it, any member could erase another member's entries by writing one snapshot. The writer-side `signed_by_self` check alone protects only honest writers.
- Catch-up never skips the superseded range. It reads and verifies every entry 1..S through engram as today, because the author check above needs them.
- A valid superseding snapshot replaces the reader's shared state wholesale, in one SQLite transaction:
  - shared rows (projects, workspaces and every other shared table) absent from the snapshot are deleted, cascading machine-local rows (roots, root approvals, root keys, workspace roots) exactly as a received project delete does today;
  - rows present are upserted;
  - the applied position advances.

  This is correct only because the author check guarantees every replaced row came from the snapshot's own author, and that coupling must be stated where the replace happens.
- Agents are never deleted: tombstones are permanent (core keeps foreign keys to them). An agent present locally but absent from a superseding snapshot is an invariant error, and nothing is applied. An agent present in both takes the snapshot's row. Name claims follow their agents.
- The cascade produces journal rows exactly as received deletes do today, so `verify` and `rebuild` replay a superseded history to the same tables.
- A joining machine takes the same path: it applies the first snapshot, then the superseding snapshot replaces it. There is one code path for joiners and members, not a separate "start from the newest snapshot" rule.
- A snapshot with no `supersedes_through` at position > 1 keeps today's behaviour, unchanged. (No writer produces one; this keeps old entries readable.)

### Unchanged
- With the log disabled, every reply and journal row stays byte-identical to today; the existing invariance test must keep passing unchanged.
- `agent.import` is still never a shared write, and still refuses after the marker.
- No change to admission: enable stays Direct-only.

## acceptance_sketch
Each test must fail when the behaviour it names breaks. Each slice reports one deliberate break per test and shows the failure.

1. **Import-order guard.** A store with projects, no agents and no marker refuses `agents_not_imported` on enable, and writes nothing: the journal, log state and fake log are all unchanged. `agent.import` then succeeds, and enable afterwards succeeds. The same store with `--without-agents` enables and writes the zero-agent marker. `--without-agents` on a store with agents refuses `invalid_request`.
2. **The incident, replayed.**
   - Store A enables with `--without-agents` (log head 1). A's pre-enable copy is restored into store A2 with the same fake-log author.
   - A2 imports agents and enables. It supersedes: its snapshot lands at position 2 with `supersedes_through: 1`, and A2's applied position is 2.
   - A fresh store B joins. It ends with A2's state, agents included, row for row.
   - `verify` is clean on A2 and B.
3. **Someone else's entry blocks supersede.** The log holds A's snapshot, then one change by author C. A2's enable refuses `join_requires_empty_registry` and appends nothing.
4. **Reader refuses a forged supersede.** A fake-log writer appends a snapshot with `supersedes_through: 2` over entries by two different authors. A joining reader and a member reader both stall with the invariant error, and apply nothing past position 2.
5. **Replace with cascade.**
   - Member B applied A's first snapshot (projects P1, P2) and approved a local root on P2.
   - A's superseding snapshot (P1 only) arrives.
   - B deletes P2 and its machine-local root and approval, keeps P1, and `verify` and `rebuild` on B are clean.
6. **Agents are never dropped.** A superseding snapshot missing an agent that B holds stalls B with the invariant error, and B's tables are unchanged.
7. **Lost race.** The fake log lands another author's entry between enable's read and its first append. Enable returns `identity_log_contended`, the state is `disabled`, and a re-run refuses `join_requires_empty_registry`.
8. **Resume after a crash mid-supersede.** Kill after part 1 of 2 lands. A restart resumes from the base head, sends only part 2 with its original expected head, and finishes with no duplicate journal rows.
9. **Wire.** A read entry missing `signed_by_self` or `author` stalls catch-up as a protocol error.
10. **Disabled is today.** The existing byte-invariance test passes unchanged.

## non_goals
- Resetting or rewinding engram's log. That doesn't exist, by design, and won't.
- Superseding entries written by other machines, under any flag.
- Compaction or snapshot-triggered pruning of old log entries.
- Recording entry ids as a cache of authorship. `signed_by_self` and `author` are the authority.

## open_questions
- None on the design. Deployment dependency: `signed_by_self` and `author` ship in engram's next build. Code and tests land against the in-tree fake log; the live re-enable waits until a placed engram serves both fields.

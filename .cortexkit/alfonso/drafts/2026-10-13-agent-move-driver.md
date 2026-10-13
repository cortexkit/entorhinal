---
title: "Agent sync in entorhinal, part 2: agent.move, the move record and the move driver"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - ".cortexkit/alfonso/context/agent-sync-draft11-excerpt.md"
    - "docs/designs/agent-data-v1.md"
    - "crates/entorhinal-core/src/claim_cache.rs"
    - "crates/entorhinal-core/src/role_cache.rs"
    - "crates/entorhinal-core/src/lib.rs:100-260"
    - "crates/entorhinal-module/src/claims.rs"
    - "crates/entorhinal-module/src/claim_codec.rs"
    - "crates/entorhinal-module/src/record_client.rs"
    - "crates/entorhinal-module/src/fake_record.rs"
    - "crates/entorhinal-module/src/roles.rs:60-250"
    - "crates/entorhinal-module/src/sessions.rs:1-120"
    - "crates/entorhinal-module/src/sessions.rs:360-560"
    - "crates/entorhinal-module/src/member.rs"
    - "crates/entorhinal-module/src/shared_write.rs:2370-2490"
    - "crates/entorhinal-module/src/agent_ops.rs"
    - "crates/entorhinal-module/src/cli.rs:1-140"
    - "mutations.toml"
---

## intent
The operator approved AGENT_SYNC draft 11: an agent can move from one of the operator's Macs to another. Entorhinal owns the move. Part 1 (on main) gave entorhinal the claim cache, `agent.where`, the session registry and liveness, member names and role claims. This campaign builds the move itself:

- `agent.move {agent_id, to}` runs on the Mac that holds the agent and moves it to another enrolled Mac.
- One encrypted compare-and-swap move record per move, in engram's cloud, coordinates the two Macs. Its states are `published`, `staged`, `claimed`, `activated`, then `released`, or `aborted`.
- A durable machine-local driver on each Mac advances its side of the move and survives restarts and sleep. Nothing is decided from a timeout alone.
- Each participant module moves its own data through the `agent-data/v1` contract (commons crate `cortexkit-role-agent-data`, from the agreed draft `docs/designs/agent-data-v1.md`). The bytes travel through engram's session door.

Fork resolution (the operator choosing a side when both Macs changed an agent) is part 3. Here, a diverged receiver stops the move and reports it.

## non_goals
- Fork resolution: `agent.resolve_fork`, minting a fork agent, the fork choice UI. A diverged receiver aborts the move with reason `diverged`.
- Choosing the target automatically. `to` is always given.
- The lid-close trigger. Engram calls `agent.move`; this campaign only admits that call under the operator's standing setting.
- Phone UI. The orchestrator relay path is admitted; the phone itself is CKIOS's.
- Implementing any participant. Participants (MC, BROCA, OC, PI, THALAMUS) implement `agent-data/v1` in their own repos.
- Changes to engram, the daemon or the commons crate.

## constraints

### External dependencies
- **The `agent-data/v1` crate:** `cortexkit-role-agent-data` 0.1.x from crates.io, once SUBC publishes it, and pinned before it's used. Until it's published, slices use the reviewed branch's types copied verbatim into a test-only fake, and the chair swaps in the published crate before release.
- **Engram's session door:** service `session-door` on module `engram`, reached by route.open target `{kind: internal_service, module_id: engram, service_id: session-door}`. Every `cut.*` op is accepted only from `reserved:entorhinal`. Built against an in-tree fake that enforces these rules, from engram's reviewed door spec:
  - **`cut.seal {cutId, items: [{sessionRef, stream, endOffset, digest}]}`** → `{state: pending|published|failed|aborted}`. It records the seal and returns at once.
    - Idempotent on `cutId`: a resend with the same items returns the current state; different items refuse `id_reused`.
    - Refusals: `invalid_request`, `cut_digest_mismatch`, `cut_item_unknown`, `cut_end_mismatch`, `cut_item_duplicate`, `too_large` (more than 1,024 items).
    - From the seal on, the door refuses participant admits into those containers (`cut_pending_handoff`) until `cut.handoff` or `cut.abort`.
  - **`cut.status {cutId}`** → `{state, handoff?, takeover?}`. `handoff` is one of `not_started`, `in_progress`, `released`, `taken_over`, `aborted`, `failed`.
  - **`cut.read {cutId}`** → `{items, author}`. `author` is the sealing device's pseudonym. Refuses `not_found`, `verify_failed`.
  - **`cut.handoff {cutId}`:** sealing device, after the move record reads `activated`. Returns the `handoff` object. Idempotent. `unavailable` and `handoff_not_covered` are retryable; `taken_over` is final and counts as done.
  - **`cut.takeover {cutId}`:** receiving device → `{takeover: in_progress|taken|failed}`. Idempotent. Refuses `invalid_request` for a cut this device sealed.
  - **`cut.abort {cutId}`:** sealing device, only before the move record reads `activated` → `{handoff: aborted, state}`. Idempotent, and cancels a still-pending seal. Refuses `cut_handoff_started` after a handoff began.
  - **`cut.accept {cutId}`:** on the Mac receiving a move back, after `activated` → `{lifted: [sessionRef]}`. Idempotent. Refuses `invalid_request` for a self-sealed cut, and `cut_not_published`.
  - **`cutId`:** 16 random bytes, lowercase hex, minted by entorhinal.
- **Engram's generic record:** as part 1 already uses it (`record.read`, `record.read_many`, `record.cas`; write_ids remembered for 64 versions; settle by reading past that).

### Where a move runs
- `agent.move` runs on the Mac that holds the agent's claim. Anywhere else it's refused `not_holder`, naming the holder and its display name.
- `to` is a member author pseudonym, or a member display name that resolves uniquely through `member_name`. It must be a different member from this Mac. Otherwise `unknown_member` or `invalid_request`.

### Preconditions, checked in this order before anything is written
1. Agent authority is cut over here (`authority_not_cut_over`).
2. The identity log is enabled here (`identity_log_disabled`).
3. The agent is live (`unknown_agent`, `gone`).
4. This Mac holds the claim, from a cache read no older than one refresh (`not_holder`). A stale cache is refreshed once first; a still-stale one is refused `claim_stale`.
5. `to` is a known member other than this Mac.
6. The agent has no open move (`move_in_progress`, naming its `cutId` and state), and fewer than 4 moves are open on this Mac (`move_cap`).
7. Every module in the daemon's `supervisor.list` is registered and ready, and every ref kind the agent holds is declared by a participant's `describe` (`participant_unavailable`, naming the module or kind).
8. Every registered ref of the agent reads `gone` in `agent.session_liveness`, and every participant's `busy` answers `idle` for its refs. Otherwise refused `agent_busy`, listing each ref with its participant, `reason_code` and `reason`. Liveness `active` counts as busy, because an open host can write.

### The move record
- Subject `move:<cutId>`.
- Body: versioned JSON defined by entorhinal, `{v: 1, cut_id, agent_id, from, to, state, claim_version, reason}`.
  - `from` and `to` are author pseudonyms.
  - `claim_version` is the claim record version the source saw when it published.
  - `reason` is set only when aborted.
- The codec refuses an unknown `v`, a `cut_id` that differs from the subject, and a malformed body, by name.
- Every transition is a `record.cas` from the version the writer just read:

| From | To | Written by |
| --- | --- | --- |
| absent | `published` | source |
| `published` | `staged` | receiver |
| `staged` | `claimed` | receiver, only after its claim write succeeded |
| `claimed` | `activated` | receiver |
| `activated` | `released` | source |
| `published`, `staged` | `aborted` | either side |
| `claimed` | `aborted` | receiver only, after returning the claim to the source |

- A lost CAS reply is settled the record contract's way: same write_id resent within 64 versions, else read and compare author and body.
- A `version_moved` re-reads, and the writer acts on the state it finds. Each side therefore always acts on the record's real state, and races between the two sides settle on the CAS.

### The inbox record
- Each member has subject `inbox:<author>`. Its body is `{v: 1, pending: [{cut_id, from, agent_id}]}`, at most 64 entries.
- The source appends an entry (CAS) right after publishing the move record. The receiver removes the entry (CAS) once its side reaches `activated` or `aborted`.
- A full inbox refuses the move at publish with `receiver_inbox_full`; the source then aborts its move record and unfreezes.

### The machine-local driver
- **Storage:** store migration 9 adds two machine-local tables, outside the identity journal and never shared:
  - `agent_move`: `cut_id` (primary key), `agent_id`, `side` (source or receiver), `peer` (author), `step`, `record_version`, `reason`, `attempts`, `created_at_ms`, `updated_at_ms`. A unique index allows one open move per agent.
  - `move_setting`: `lid_close_moves` (boolean, off by default).
- `verify` and `rebuild` ignore both tables.
- **Persistence:** every step is recorded in `agent_move` before the next one starts. At startup, every open row resumes from its recorded step.
- **Its own task:** the driver runs on its own task, the same way as the claim refresh.
  - It holds the store slot weakly and takes a store handle only while touching SQLite, never across a call to a participant, engram or the daemon.
  - Machine-local writes go through the fenced writer (`with_conn_fenced`) and never append a journal row.
  - Health stays lock-free.
- **Polling:** while any move is open, the driver re-reads each open move record every 30 s, and once after startup and on engram reconnect. While the identity log is enabled, it reads this Mac's inbox on the same schedule.
- **Unknown outcomes:** a participant or door call whose outcome is unknown (transport failure, timeout, dropped connection) is resolved with the participant's `status` or `cut.status`, then repeated; idempotency makes the repeat safe.
- **Refusals:**
  - `busy` and `cut_not_ready` are retried with doubling backoff, capped at 5 minutes.
  - `busy` on the source after the freeze aborts the move.
  - Any non-retryable or unknown code aborts the move, and is recorded as the reason.

### Source steps
1. Preconditions pass. Mint the `cutId`. Record `agent_move` (side source, step `preparing`).
2. `prepare` on every participant, in module-id order, with that participant's refs sorted by `(ref_kind, ref)`. Record the items and external entries. Any refusal aborts: `abort` on every participant that prepared.
3. `export` on every participant.
4. `cut.seal` with every item, then poll `cut.status` until `published`. `failed` or `aborted` abort the move.
5. CAS the move record from absent to `published`, with `claim_version` set to the claim version this Mac holds. Append to the receiver's inbox.
6. Watch the move record.
   - `aborted`: call `cut.abort` and `abort` on every participant; the move ends aborted.
   - `activated`: call `cut.handoff` and poll `cut.status` until `handoff` is `released` or `taken_over`; both mean done. Call `release` on every participant with `holder` set to the receiver. CAS the record to `released`. The move ends released.
- **Operator abort:** the source may abort while the record reads `published` or `staged` (CAS to `aborted`, then as above). If the CAS loses, it acts on the state found.

### Receiver steps
1. **Pick up:** for each inbox entry with no `agent_move` row, read the move record. A record that isn't `published` is skipped.
   - Check `cut.read`'s `author` equals the record's `from` (`cut_author_mismatch`, which aborts).
   - Check this Mac's own preconditions 1, 2, 3 and 7, and that the agent's project has a root registered here (`project_not_checked_out`). Any failure aborts the move with that reason.
   - Record `agent_move` (side receiver, step `picked_up`).
2. **Diverged:** call `diverged` on every participant.
   - Any `advanced` or `unknown` aborts with reason `diverged`.
   - `external_not_present` holds at this step, retried with backoff, and is shown in `agent.where`.
3. **Import:** call `import` on every participant. Any refusal aborts. Then CAS the record `published` → `staged`.
4. **Take the claim:** CAS the claim record from `claim_version` to `{holder: this Mac, epoch: old epoch + 1, cut_id}`.
   - `version_moved` aborts with reason `claim_moved`.
   - Then CAS the record → `claimed`, and refresh this agent's claim cache entry.
5. **Activate:** call `activate` on every participant. `external_not_present` holds at this step, retried. Any other refusal returns the claim: CAS it back to `{holder: from, epoch + 1, cut_id: null}`, then CAS the record `claimed` → `aborted`, then call `abort` on every participant.
6. **Activated:** CAS the record → `activated`, and remove the inbox entry.
7. **Takeover:** if the record hasn't read `released` 10 minutes after `activated`, call `cut.takeover` once and poll its status. The source still writes `released` when it wakes.
8. **Move back:** a move back is the same path. When this Mac had released this agent before, it calls `cut.accept` after `activated` and before telling participants to resume.

### Admission for `agent.move`
- From `direct`: through the operator confirmation flow (`write_wait`), with a summary such as `move agent "ENTO" ("agent_…") to "Studio Mac"`. Refused `operator_presence_unavailable` where there's no presence provider.
- From the module providing `agent-orchestrator/v1`, resolved the same way as `agent.register_session` (cached per route): relaying a move the operator confirmed on the phone. No prompt.
- From `reserved:engram`: only while `lid_close_moves` is on, and only when every precondition holds, idle included. Otherwise `caller_forbidden` or the failing precondition.
- From anyone else: `caller_forbidden`. On a flow-scoped route: `flow_scope_not_admitted`.
- Request: `{agent_id, to, request_key}`. Reply: `{cut_id, state}` once step 5 of the source steps is recorded. The move then continues in the background.

### Other ops and CLI
- **`agent.move_status {cut_id}`** and **`agent.moves {}`** (open moves first, then the last 32 finished ones): reads, from `agent_move` only.
- **`agent.move_abort {cut_id, request_key}`:** Direct with confirmation, or the orchestrator. It follows the abort rules above.
- **`agent.set_lid_close_moves {enabled, request_key}`:** Direct with confirmation only.
- **`agent.where`** gains `moving: {cut_id, side, step, peer, peer_name}` while a move is open here.
- **`agent.register_session`** for an agent with an open source-side move refuses `agent_moving`: a new session there would write past the cut.
- **CLI:** `ck agents move <agent> --to <mac>`, `ck agents moves`, `ck agents move-abort <cut_id>`, `ck agents lid-close-moves on|off`.

### Tests and fences
- **Fakes:** the in-tree fakes enforce the engram rules: door idempotency and author, record CAS and write_id retention, and inbox CAS. A fake participant implements `agent-data/v1` exactly as the crate pins it (idempotency on `(cutId, op)` with refs compared as a set, fixed ref set per cut, step machine).
- **Two-Mac acceptance:** two real handlers sharing the fakes. A full move runs to `released`. Each abort path runs from each state. A receiver diverged leads to `aborted`. A receiver that never picks up is aborted by the source. A source asleep after `activated` is taken over, then releases on wake. A move back calls `cut.accept`. A restart at every recorded step resumes. A lost reply at every CAS and participant call settles.
- Every refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`. No test asserts real elapsed time. All tests pass on Linux, macOS and Windows CI.

## acceptance_sketch
1. A move from A to B, with three fake participants, reaches `released`: B holds the claim at the next epoch, A's participants are released naming B, and both Macs' `agent.where` agree.
2. Each precondition refuses in order, before anything is written, with its named code.
3. An abort from `published` or `staged` by either side, or from `claimed` by the receiver (claim returned first), leaves every participant unfrozen or unstaged and the claim with its holder before the move.
4. A diverged participant on B aborts the move with reason `diverged` before anything is imported.
5. `external_not_present` holds B at `picked_up` or `claimed` until the fake host data appears, then the move completes.
6. A killed handler at every recorded step resumes after restart, and finishes the move once.
7. B takes over after A stays silent past 10 minutes after `activated`; A releases when it returns.
8. A move back from B to A calls `cut.accept` on A and completes.
9. `agent.move` admission: Direct prompts with the exact summary; the orchestrator relay needs no prompt; engram is refused unless lid-close moves are on and the agent is idle; any other caller is refused.
10. `agent.register_session` on A for the moving agent refuses `agent_moving` until the move ends.

## open_questions
None.

## slice_hints
- s1, core: migration 9 (`agent_move`, `move_setting`), typed `RegistryStore` APIs, the move record and inbox codecs and their transition table as pure functions.
- s2, module: the participant client (discovery via catalog and `supervisor.list`, ref routing by kind, per-op calls and unknown-outcome resolution) and the door client, each with its fake.
- s3, module: the source driver and its admission, preconditions and CLI.
- s4, module: the receiver driver: inbox, pickup, diverged, import, claim take, activate, takeover, move back.
- s5, module: two-Mac acceptance, `agent.where` moving, `register_session` refusal, lid-close setting.

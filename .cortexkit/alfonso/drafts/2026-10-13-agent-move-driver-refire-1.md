---
title: "2026-10-13-agent-move-driver refire 1"
status: draft
rounds_cap: 1
mint: auto
refired_from: "ct_00000000-0000-44ad-98dd-9cea6b738898"
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


## constraints
### External dependencies
- **The `agent-data/v1` contract:** `cortexkit-role-agent-data` 0.1.x from crates.io, once SUBC publishes it, pinned before it's used. Until then, `entorhinal-module` defines its own serde request and reply types for every op in `docs/designs/agent-data-v1.md:50-122`, field for field: `deny_unknown_fields` on typed objects, with `detail`, `format` and `check` kept opaque. The production participant client and the fake share these types. When the crate is pinned, the chair swaps it in and the same fixtures must still pass.
- **Participant discovery through the daemon:** a participant is a module that declares `agent-data/v1` in `capabilities.provides`, found through the daemon catalog (`docs/designs/agent-data-v1.md:9`). The repository verifies only that `catalog.list` is a channel-0 control request whose reply carries `modules[].module_id` (`crates/entorhinal-module/tests/real_daemon_e2e.rs:164-207`). The op that reports each module's provided capabilities, and the one that reports registered and ready (`supervisor.list` in the draft), are not pinned: see open question 1. The participant client reads them only through a trait, the same seam as `ProviderResolver` (`crates/entorhinal-module/src/sessions.rs:86-91`). An in-tree daemon fake implements that trait.
- **Engram's session door:** service `session-door` on module `engram`, reached by route.open target `{kind: internal_service, module_id: engram, service_id: session-door}`. Every door `cut` op is accepted only from `reserved:entorhinal`. Built against an in-tree fake that enforces these rules, from engram's reviewed door spec:
  - **`cut.seal {cutId, items: [{sessionRef, stream, endOffset, digest}]}`** → `{state: pending|published|failed|aborted}`. It records the seal and returns at once.
    - Idempotent on `cutId`: a resend with the same items returns the current state; different items refuse `id_reused`.
    - Refusals: `invalid_request`, `cut_digest_mismatch`, `cut_item_unknown`, `cut_end_mismatch`, `cut_item_duplicate`, `too_large` (more than 1,024 items).
    - From the seal on, the door refuses participant admits into those containers (`cut_pending_handoff`) until `cut.handoff` or `cut.abort`.
  - **`cut.status {cutId}`** → `{state, handoff?, takeover?}`. `handoff` is one of `not_started`, `in_progress`, `released`, `taken_over`, `aborted`, `failed`.
  - **`cut.read {cutId}`** → `{items, author}`. `author` is the sealing device's pseudonym. Refuses `not_found`, `verify_failed`.
  - **`cut.handoff {cutId}`:** sealing device, after the move record reads `activated`. Returns the `handoff` object. Idempotent. `taken_over` is final and counts as done.
  - **`cut.takeover {cutId}`:** receiving device → `{takeover: in_progress|taken|failed}`. Idempotent. Refuses `invalid_request` for a cut this device sealed.
  - **`cut.abort {cutId}`:** sealing device, only before the move record reads `activated` → `{handoff: aborted, state}`. Idempotent, and cancels a still-pending seal. Refuses `cut_handoff_started` after a handoff began.
  - **`cut.accept {cutId}`:** receiving device, after the move record reads `activated` → `{lifted: [sessionRef]}`. Idempotent; lifts nothing where this device released nothing. Refuses `invalid_request` for a self-sealed cut, and `cut_not_published`.
  - **`cutId`:** 16 random bytes, lowercase hex, minted by entorhinal.
- **Engram's generic record:** as part 1 already uses it (`record.read`, `record.read_many`, `record.cas`; write_ids remembered for 64 versions; settle by reading past that; bodies capped at 64 KiB, `BODY_CAP` in `crates/entorhinal-module/src/record_client.rs:31`).

### Vocabulary
- **Source steps:** `preparing`, `exported`, `sealed`, `published`, `releasing`; terminal `released`, `aborted`.
- **Receiver steps:** `picked_up`, `imported`, `staged`, `claimed`, `activated`; terminal `done`, `taken_over`, `aborted`.
- A move is **open** on a Mac while its `agent_move` row is at a non-terminal step.
- **`reason`** (row and move record) is one of `operator`, `receiver_inbox_full`, `move_too_large`, `participant_refused`, `door_refused`, `cut_author_mismatch`, `diverged`, `claim_moved`, `project_not_checked_out`, `gone`, `receiver_precondition`. `detail` carries the specifics as JSON: `{module_id, op, code}` for a participant, `{op, code}` for the door (a seal that ends `failed` or `aborted` is `door_refused` with `{op: "cut.status", code: <state>}`), the failing precondition's code and its detail for `receiver_precondition`. A row that reads the peer's abort copies the record's `reason`.
- Every reply that reports a move carries `step`, never a record state alone.

### Where a move runs
- `agent.move` runs on the Mac that holds the agent's claim, or on any Mac when the agent is unclaimed (below). Where the claim is held elsewhere it's refused `not_holder`, naming the holder and its display name.
- `to` is a member author pseudonym, or a member display name resolved through `member_name`. No matching author or name refuses `unknown_member`; a display name matching more than one member, or naming this Mac, refuses `invalid_request`.

### Preconditions, checked in this order before anything is written
1. Agent authority is cut over here (`authority_not_cut_over`).
2. The identity log is enabled here (`identity_log_disabled`).
3. The agent is live (`unknown_agent`, `gone`).
4. The claim: the handler runs one claim refresh pass through `RefreshQueue` (`crates/entorhinal-module/src/claims.rs:30-49`) and waits for it. The cached claim must then read held here, or unclaimed (`epoch == 0`, not stale); epoch 0 is admitted the same way `check_registration_claim` admits it (`crates/entorhinal-core/src/claim_cache.rs:104-130`). Held elsewhere refuses `not_holder`; a failed pass or a still-stale row refuses `claim_stale`; an integrity mark refuses `claim_integrity_failed`.
5. `to` is a known member other than this Mac.
6. The agent has no open move on this Mac, on either side (`move_in_progress`, naming its `cut_id` and `step`), and fewer than 4 moves of either side are open on this Mac (`move_cap`).
7. Every participant module is registered and ready, and every ref kind the agent holds is in some participant's `agent_data.describe` `ref_kinds` (`participant_unavailable`, naming the module or kind). The daemon fields behind "registered and ready" are open question 1.
8. Every registered ref of the agent reads `gone` in `agent.session_liveness`, and every participant's `busy` answers `idle` for its refs. Otherwise refused `agent_busy`, listing each ref with its participant, `reason_code` and `reason`. Liveness answers use `reason_code` `session_active` (for `active`, because an open host can write) or `liveness_unknown`, with the liveness cause as `reason`.
- After a Direct confirmation, checks 4, 6 and 8 run again before step 1 of the source writes anything.

### The move record
- Subject `move:<cutId>`.
- Body: versioned JSON defined by entorhinal, `{v: 1, cut_id, agent_id, from, to, state, claim_version, participants, reason, detail}`.
  - `from` and `to` are author pseudonyms.
  - `claim_version` is the claim record version the source read at precondition 4 (0 when the claim record is absent).
  - `participants` is `[{module_id, refs: [{ref_kind, ref}], external: [{ref_kind, ref, note, check}]}]` in module-id order, refs sorted by `(ref_kind, ref)`, taken from the `prepare` replies. It is the only source of the receiver's modules, refs and checks: the receiver calls exactly the listed modules with exactly their listed refs, and never calls an unlisted module.
  - `reason` and `detail` are set only when aborted.
- The source encodes the body right after `prepare`. A body over `BODY_CAP` aborts the move with reason `move_too_large`, naming the cap, before export or seal.
- The codec refuses an unknown `v`, a `cut_id` that differs from the subject, an unsorted or duplicated participant or ref, and a malformed body, by name.
- Every transition is a `record.cas` from the version the writer just read:

| From | To | Written by |
| --- | --- | --- |
| absent | `published` | source |
| `published` | `staged` | receiver |
| `staged` | `claimed` | receiver, only after its claim write succeeded |
| `claimed` | `activated` | receiver |
| `activated` | `released` | source |
| `published` | `aborted` | either side |
| `staged` | `aborted` | receiver only |
| `claimed` | `aborted` | receiver only, after returning the claim to the source |

- The source never writes `aborted` once the record reads `staged`: the receiver's claim take may already have landed, and only the receiver can return it.
- **write_ids:** each move-record CAS uses the first 16 bytes of blake3 (a workspace dependency, `Cargo.toml:19`) over `move:<cut_id>:<side>:<to_state>`, and each claim CAS over `claim:<cut_id>:take` or `claim:<cut_id>:return`. Bodies are a pure function of the row, so a restarted writer resends a byte-identical attempt. A lost CAS reply is settled the record contract's way: same write_id resent within 64 versions, else read and compare author and body.
- A `version_moved` re-reads, and the writer acts on the state it finds. Races between the two sides settle on the CAS.
- **Authors:** every read of the record checks the reply `author` against the table's writer for the state read: `published` and `released` by `from`; `staged`, `claimed` and `activated` by `to`; `aborted` by either. A mis-authored state is treated as not read: the side keeps polling, never acts on it (in particular the source never releases on a mis-authored `activated`), and records it in `detail` and the health `moveErrors` count.

### The claim writes
- Bodies are the full `AgentClaim` (`crates/entorhinal-module/src/claim_codec.rs:14-23`), `reason: null` included. `holder` for this Mac is its persisted self author.
- **Take:** read `claim:<agent_id>` first. A reply version other than `claim_version` aborts with reason `claim_moved` without a CAS. Otherwise CAS from `claim_version` to `{holder: this Mac, epoch: read epoch + 1, cut_id}`; a `version_moved` also aborts `claim_moved`. The landed version is stored in the row's `claim_version`.
- **Return:** CAS from the stored landed version to `{holder: from, epoch: taken epoch + 1, cut_id: null}`, even when `from` held nothing before (an unclaimed agent then ends held by its source at epoch 2). If the CAS loses, re-read; `holder == from` counts as done.

### The inbox record
- Each member has subject `inbox:<author>`. Its body is `{v: 1, pending: [{cut_id, from, agent_id}]}`, at most 64 entries.
- The source appends an entry right after publishing the move record. The receiver removes it once its row reaches `activated` or `aborted`, and at pickup removes, without creating a row, any entry whose record reads `aborted` or `released` or whose `to` is not this Mac's persisted self author.
- Both edits are idempotent by content: append only if `cut_id` is absent, remove only if present. `version_moved` re-reads and re-applies until it lands. Each attempt carries a fresh random write_id; a lost reply is settled by re-reading.
- **Full:** fullness is seen on the read before the append. The move then takes the source abort path with reason `receiver_inbox_full`.

### The machine-local driver
- **Storage:** store migration 9 adds two machine-local tables, outside the identity journal and never shared:
  - `agent_move`: `cut_id` (primary key), `agent_id`, `side` (source or receiver), `peer` (author), `step`, `record_version`, `claim_version` (nullable), `reason`, `detail`, `request_key` (nullable, unique), `attempts`, `activated_at_ms` (nullable), `created_at_ms`, `updated_at_ms`. Index `UNIQUE(agent_id) WHERE step NOT IN ('released','aborted','done','taken_over')`.
  - `move_setting`: `lid_close_moves` (boolean, off by default).
- `verify` and `rebuild` ignore both tables. `agent.move`'s `request_key` lives in `agent_move.request_key`, never in the journal's request-key namespace (`crates/entorhinal-core/src/pending.rs:34-61`).
- **Persistence:** every step is recorded in `agent_move` before the next one starts. At startup, every open row resumes from its recorded step. A source row resumed before `published` calls `prepare` again with the same refs (idempotent: it returns the first reply, `docs/designs/agent-data-v1.md:25`) instead of persisting items, then `export` and `cut.seal`, both idempotent. Registration is refused for the moving agent, so its ref set cannot change meanwhile.
- **Its own task:** the driver runs on its own task, the same way as the claim refresh (`crates/entorhinal-module/src/claims.rs:51-68`).
  - It holds the store slot weakly and takes a store handle only while touching SQLite, never across a call to a participant, engram or the daemon.
  - Machine-local writes go through the fenced writer (`with_conn_fenced`) and never append a journal row.
  - Health stays lock-free. The health reply gains `movesOpen` and `moveErrors` beside `selfAuthorConflicts` (`crates/entorhinal-module/src/main.rs:962-963`).
- **Polling:** while any move is open, the driver re-reads each open move record every 30 s, and once after startup and on engram reconnect. While the identity log is enabled, it reads this Mac's inbox on the same schedule. `cut.status` polls use the backoff below, reset on each state change. Polling cadence uses tokio time; recorded instants (`activated_at_ms`, backoff deadlines) come from the handler's `clock`.
- **Unknown outcomes:** a participant or door call whose outcome is unknown (transport failure, timeout, dropped connection) is resolved with the participant's `status` or `cut.status`, then repeated; idempotency makes the repeat safe.
- **Refusals, one table:**

| Code | Action |
| --- | --- |
| `busy` at `prepare` | retry with the backoff, at most 8 attempts, then abort `participant_refused` with the `busy` detail |
| `cut_not_ready`, `unavailable`, `handoff_not_covered` | retry, doubling backoff capped at 5 minutes, unbounded |
| `external_not_present` at receiver diverged or activate | hold with the same backoff, shown in `agent.where` |
| `busy` after the freeze, every other listed code | abort, code in `detail` |
| any unknown code | abort, code in `detail` |

- `attempts` counts retries within the current step and resets on each step change. Tests never sleep in real time.

### Source steps
1. Preconditions pass. Mint the `cutId`. Record `agent_move` (side source, step `preparing`, `request_key`).
2. `prepare` on every participant, in module-id order, with that participant's refs sorted by `(ref_kind, ref)`. Encode the move record body (size check above). Any refusal aborts: `abort` on every participant (an unseen `cutId` is a no-op, so a resumed driver needs no list of who prepared).
3. `export` on every participant. Record `exported`.
4. `cut.seal` with every item, then poll `cut.status` until `published`. `failed` or `aborted` abort with reason `door_refused`. Record `sealed`.
5. Read the receiver's inbox; if full, abort as above. CAS the move record from absent to `published`. Append to the receiver's inbox. Record `published`.
6. Watch the move record.
   - `aborted`: read `claim:<agent_id>` and apply it through `cache_agent_claims` (`crates/entorhinal-core/src/claim_cache.rs:235`) as a present observation; call `cut.abort` and `abort` on every participant; the row ends `aborted`.
   - `activated`: record `releasing`. Read `claim:<agent_id>` and apply it the same way; if the read fails, keep polling and do not release. Call `cut.handoff` and poll until `handoff` is `released` or `taken_over`; both mean done. Call `release` on every participant with `holder` set to the receiver. CAS the record to `released`. The row ends `released`.
- **Operator abort:** at `preparing` through `sealed`, `abort` every participant, `cut.abort` if sealed, and end the row `aborted` with reason `operator`. At `published` while the record reads `published`, CAS it to `aborted`, then as in step 6. If the record reads `staged` or later (before or on the CAS), refuse `invalid_state` naming the record state and that the receiver must abort the move. At `releasing` and after, refuse `invalid_state` naming the step.

### Receiver steps
1. **Pick up:** for each inbox entry with no `agent_move` row, read the move record. Entries whose record reads `aborted` or `released`, or whose `to` is not this Mac, are removed; other non-`published` records are skipped.
   - Record `agent_move` (side receiver, step `picked_up`).
   - The record reply's `author` and `cut.read`'s `author` must both equal `from` (`cut_author_mismatch`).
   - This Mac's preconditions 1 and 2 must hold. In place of precondition 7, checked against the record: every listed `module_id` is registered and ready here, and its `describe` declares every `ref_kind` listed for it. A listed module absent here aborts `receiver_precondition` with detail `participant_unavailable` naming it; one present but not ready holds at `picked_up` with the backoff.
   - For an agent with a `project_id`, the project must have a root registered here (`project_not_checked_out`). Agents without a project skip that check.
   - `unknown_agent` holds at `picked_up` with the backoff (the identity-log replica may lag); `gone` aborts.
   - Any other failure aborts with that reason.
2. **Diverged:** call `diverged` on every listed participant with its listed refs. Any `advanced` or `unknown` aborts with reason `diverged`. `external_not_present` holds.
3. **Import:** call `import` on every listed participant. Any refusal aborts. Record `imported`, then CAS the record `published` → `staged` and record `staged`.
4. **Take the claim** (above). Then CAS the record → `claimed`, record `claimed`, and refresh this agent's claim cache entry. If that CAS finds `aborted` after the take landed, return the claim before ending the row `aborted`.
5. **Activate:** call `activate` on every listed participant. `external_not_present` holds. Any other refusal aborts.
6. **Activated:** CAS the record → `activated`, record `activated` with `activated_at_ms` from the handler clock, remove the inbox entry, then call `cut.accept {cutId}`. Every receiver calls it: a first-time receiver gets `lifted: []`, a move back lifts the cut this Mac released before. Participants are told nothing further; `activate` already made the data live, and there is no resume op (`docs/designs/agent-data-v1.md:126`). Retryable codes retry; any other refusal is recorded in `detail`, counted in `moveErrors`, shown in `agent.where`, and the move stands.
7. **Finish:** when the record reads `released`, the row ends `done`. If the handler clock reaches `activated_at_ms` + 10 minutes first (also across a restart), call `cut.takeover` once and poll its status; `taken` ends the row `taken_over`, and `failed` is retried with the backoff. The source still writes `released` when it wakes. This is the only timed action, and it never changes the move record.
- **Receiver abort, one procedure** (also the operator's `agent.move_abort` on the receiver before `activated`): CAS the record from the state just read to `aborted` with `reason` and `detail` (from `claimed`, or after a landed take, return the claim first); call `abort` on every listed participant with its listed refs; remove the inbox entry; end the row `aborted`.

### Admission for `agent.move`
- `agent.move`, `agent.move_abort` and `agent.set_lid_close_moves` are their own served arms, like `role.take`: each gets a branch in `handle_served_request` (`crates/entorhinal-module/src/main.rs:1005-1027`) ahead of the `write_wait` fallback, and the synchronous path answers `served_path_required` for them as it does for `role.take` and claim refresh (`crates/entorhinal-module/src/main.rs:1124-1132`). They are listed in `MUTATING_METHODS` and `SELF_ADMITTED_METHODS` (`crates/entorhinal-module/src/main.rs:473-520`), so flow-scoped routes refuse `flow_scope_not_admitted` and `authorize_write` passes them to these rules. They never go through `write_wait`, never lock the shared `writer`, and append no journal row.
- From `direct`: through `ConfirmationGuard` and `OperatorConfirmer`, bounded by `OPERATOR_BOUND`, with a summary naming the resolved display name, such as `move agent "ENTO" ("agent_…") to "Studio Mac"`. Refused `operator_presence_unavailable` where there's no presence provider.
- From the module providing `agent-orchestrator/v1`, resolved the same way as `agent.register_session` (cached per route): relaying a move the operator confirmed on the phone. No prompt.
- From `reserved:engram`: only while `lid_close_moves` is on, and only when every precondition holds, idle included. Otherwise `caller_forbidden` or the failing precondition.
- From anyone else: `caller_forbidden`.
- Request: `{agent_id, to, request_key}`. Reply: `{cut_id, step: "preparing"}` once source step 1 is recorded; the move continues in the background. A resend with the same `request_key` and body returns `{cut_id, step}` of the existing row; a different body under the same key refuses `request_key_reused`.

### Other ops and CLI
- **`agent.move_status {cut_id}`** and **`agent.moves {}`** (open moves first, then the last 32 finished ones): open reads, from `agent_move` only. Each row is returned as its columns plus `peer_name` from `member_name`, with `detail` as JSON.
- **`agent.move_abort {cut_id, request_key}`:** Direct with confirmation, or the orchestrator. It follows the abort rules of the row's side above; a resend on an already aborted row returns that row.
- **`agent.set_lid_close_moves {enabled, request_key}`:** Direct with confirmation only; setting the current value is a no-op.
- On `agent.move_abort` and `agent.set_lid_close_moves`, `request_key` is required but idempotency is by state, not by a key cache.
- Each of these five ops has an `agent_operation_description` entry (`crates/entorhinal-module/src/main.rs:1943`); the writes are in `MUTATING_METHODS`.
- **`agent.where`** gains `moving: {cut_id, side, step, peer, peer_name}` while a move is open here. The key is omitted, not null, otherwise, so the pinned part 1 reply bytes are unchanged.
- **`agent.register_session`** refuses `agent_moving`, with detail `{cut_id, side, step}`, while this Mac has a source row for the agent at any open step, or a receiver row at `picked_up`, `imported`, `staged` or `claimed`: a new session there would write past the cut. `check_registration_claim` cannot cover the receiver, because it admits an epoch-0 claim and an uncached one (`crates/entorhinal-core/src/claim_cache.rs:104-122`). After the move, the source's refreshed cache refuses `held_elsewhere`.
- **CLI:** `ck agents move <agent> --to <mac>`, `ck agents moves`, `ck agents move-abort <cut_id>`, `ck agents lid-close-moves on|off`.

### Tests and fences
- **Fakes:** the in-tree fakes enforce the engram rules: door idempotency and author, record CAS, body cap and write_id retention, and inbox CAS. A fake participant implements `agent-data/v1` exactly as the contract pins it (idempotency on `(cutId, op)` with refs compared as a set, fixed ref set per cut, step machine). A daemon fake scripts a stopped module, an unregistered module, and a module that provides no `agent-data/v1`.
- **Wire:** a round-trip test pins each op's JSON against literal fixtures from `docs/designs/agent-data-v1.md` (`prepare` with `external`, `busy` with `reason_code`, `status` steps, `invalid_state` with `detail.step`). The manifest test enumerates the five new ops; the synchronous path refuses the three writes `served_path_required`. `agent.where` without a move equals the pinned string; `agent.moves` is pinned by a literal fixture.
- **Two-Mac acceptance:** two real handlers sharing the fakes.
  - A full move runs to `released`/`done`, with three participants of distinct ref kinds; B's per-op ref sets equal A's. A's `agent.where` then reads `held_elsewhere` naming B without `agent.refresh_claims`, and registration on A refuses `held_elsewhere`.
  - A move of an agent with no claim record ends with B at epoch 1; one cached held elsewhere refuses `not_holder` before any write. A take against a moved claim aborts `claim_moved` with no CAS on the fake.
  - Each abort path runs from each state the table allows it from; with the receiver's take scripted to land between the source's read of `staged` and its abort, the source's abort refuses `invalid_state` and the move completes; a receiver whose `claimed` CAS reads `aborted` ends with the claim holder equal to `from`. An oversized `prepare` reply aborts `move_too_large` with no seal and every participant unfrozen.
  - A receiver diverged leads to `aborted`. A receiver that never picks up is aborted by the source; 64 such moves leave B's inbox empty after B polls. Two sources appending to one inbox concurrently both land. A record from a third author aborts `cut_author_mismatch`; an `activated` written by a third author never releases A's participants; an inbox entry naming another `to` is dropped without a row. A lagging replica completes once the agent arrives. An assistant moves without a project root.
  - On B: a record listing a module B lacks aborts naming it; a listed module that is not ready holds then completes; an unlisted participant on B is never called. On A: `participant_unavailable` names a stopped participant.
  - `unavailable` twice then success completes; `handoff_not_covered` twice then `released` completes; `busy` at `prepare` nine times aborts `participant_refused`; a seal reaching `failed` aborts `door_refused` with every participant unfrozen; an unknown code aborts naming it. Two members sharing a display name refuse `invalid_request`.
  - A source asleep after `activated` is taken over at the recorded instant, with tests advancing the handler clock, across a restart; the receiver row ends `taken_over`, a later move of the agent from B is admitted, and A releases on wake.
  - A move back calls `cut.accept` with the B-sealed `cutId` and `lifted` names A's earlier sessionRefs; a first-time receiver's `cut.accept` returns `lifted: []`.
  - Registration on B refuses `agent_moving` at `picked_up`, `staged` and `claimed` (also for an agent with no claim record, and with B's `claim_cache` row deleted) and succeeds after `activated`; on A it refuses from `preparing` through `released`.
  - A restart at exactly each enumerated step resumes; a crash between `prepare` and seal seals the same items once; a crash between the claim take and the record CAS resumes without a second take. A lost reply at every CAS and participant call settles.
  - The unique index refuses a second open row for one agent on either side.
  - A hung catch-up holding `writer` does not delay `agent.move`; the journal generation is unchanged after a full move.
- Every refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`. No test asserts real elapsed time. All tests pass on Linux, macOS and Windows CI.

## intent
The operator approved AGENT_SYNC draft 11: an agent can move from one of the operator's Macs to another. Entorhinal owns the move. Part 1 (on main) gave entorhinal the claim cache, `agent.where`, the session registry and liveness, member names and role claims. This campaign builds the move itself:

- `agent.move {agent_id, to}` runs on the Mac that holds the agent (or on any Mac while the agent is still unclaimed) and moves it to another enrolled Mac.
- One encrypted compare-and-swap move record per move, in engram's cloud, coordinates the two Macs. Its states are `published`, `staged`, `claimed`, `activated`, then `released`, or `aborted`.
- A durable machine-local driver on each Mac advances its side of the move and survives restarts and sleep. No move-record transition is ever inferred from a timeout. The one timed action is the receiver's door takeover, 10 minutes after `activated`, when the claim has already moved.
- Each participant module moves its own data through the `agent-data/v1` contract (commons crate `cortexkit-role-agent-data`, from the agreed draft `docs/designs/agent-data-v1.md`). The bytes travel through engram's session door.

Fork resolution (the operator choosing a side when both Macs changed an agent) is part 3. Here, a diverged receiver stops the move and reports it.

## non_goals
- Fork resolution: `agent.resolve_fork`, minting a fork agent, the fork choice UI. A diverged receiver aborts the move with reason `diverged`.
- Choosing the target automatically. `to` is always given.
- The lid-close trigger. Engram calls `agent.move`; this campaign only admits that call under the operator's standing setting.
- Phone UI. The orchestrator relay path is admitted; the phone itself is CKIOS's.
- Implementing any participant. Participants (MC, BROCA, OC, PI, THALAMUS) implement `agent-data/v1` in their own repos.
- Changes to engram, the daemon or the commons crate.


## open_questions
None.


## slice_hints
- s1, core: migration 9 (`agent_move`, `move_setting`), typed `RegistryStore` APIs, the move record and inbox codecs and their transition table as pure functions.
- s2, module: the participant client (discovery via catalog and `supervisor.list`, ref routing by kind, per-op calls and unknown-outcome resolution) and the door client, each with its fake.
- s3, module: the source driver and its admission, preconditions and CLI.
- s4, module: the receiver driver: inbox, pickup, diverged, import, claim take, activate, takeover, move back.
- s5, module: two-Mac acceptance, `agent.where` moving, `register_session` refusal, lid-close setting.

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded owner answer 1

Campaign `ct_00000000-0000-44ad-98dd-9cea6b738898`, round 1, decision sequence 2 (source: owner decision):

The chair is getting the daemon discovery facts from SUBC (which op and reply fields report a module's provided capabilities and its registered/ready state, and how a module reads the configured supervisor list). It refires with them as a ruling.

### recorded supplied ruling 2

Campaign `ct_00000000-0000-44ad-98dd-9cea6b738898`, round 2, decision sequence 0 (source: {"body_bytes":1873,"boundary":"ledger_payload","order":3,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-44ad-98dd-9cea6b738898-20261013T020238Z.md"}):

Chair ruling R1: the daemon discovery surface, from SUBC (subconscious master, subc-control lib.rs).
- **Configured modules:** `supervisor.list`, request `{"op":"supervisor.list"}`. Reply `{generation, modules: [SupervisorEntry]}`, listing every configured module, stopped and disabled included. SupervisorEntry fields used: `module_id`, `state` (string), `enabled`, `live`, `health`.
  - It's a client op. Entorhinal calls it on a `SubcConsumer` connection (as `resolve_provider` does), never on its registered module connection, where the daemon refuses client ops.
  - The typed wrapper `SubcConsumer::supervisor_list()` ships in subc-client-rs 0.28.4. Until that's on crates.io, the slice builds against a trait with an in-tree fake, and the chair pins 0.28.4 before release.
- **Registered and ready:** `catalog.list` → `CatalogList.modules: Vec<CatalogEntry>`. `CatalogEntry.ready: bool`, with `not_ready: Option<NotReadyReason>` saying why.
  - From daemon 0.34.3 on, a module is listed only once it's routable.
  - A participant counts as available when it is enabled in `supervisor.list` AND listed in `catalog.list` with `ready: true`. Anything else is `participant_unavailable`, naming the module and its state or `not_ready` reason.
  - This list only ever refuses a move; it never admits anything.
- **Capabilities:** `CatalogEntry.capabilities: Option<CapabilityDeclarations>`, filled from the manifest. `.provides` is matched against `agent-data/v1`. `None` means no capabilities.
- **Which modules must be ready:** precondition 7 applies to every enabled module whose catalog entry provides `agent-data/v1`. It also applies to every enabled configured module not in the catalog at all (it might be a participant that's down). A disabled module is ignored.
Fold R1 into constraints (precondition 7 and the s2 participant client), and close open question 1.

# Agent move contract

A move hands an idle agent, with its sessions and their data, from the Mac
that holds it (the **source**) to another of the operator's Macs (the
**receiver**). Both Macs run entorhinal; each drives its own half. They
coordinate through records that engram stores, and every module that keeps
agent data takes part through the `agent-data/v1` participant contract.
[moves.md](moves.md) is the operator guide.

Code: `crates/entorhinal-module/src/move_admission.rs` (starting a move),
`move_source.rs` and `move_receiver.rs` (the two halves), `move_driver.rs`
(scheduling and restart), `move_ops.rs` (status, abort, lid-close setting),
`participant_client.rs` and `participant_discovery.rs` (participants),
`door_client.rs` (engram's session door), `record_client.rs` (engram's
records); `crates/entorhinal-core/src/move_record.rs` (the shared record and
its transition rules) and `move_store.rs` (the local checkpoint).

## 1. Vocabulary

| Term | Meaning |
|---|---|
| cut | One move, named by its `cut_id` (16 random bytes in lowercase hex). |
| move record | The shared record both Macs read and write by compare-and-swap. Its states are listed in section 2. |
| inbox | A per-Mac record the source appends to so the receiver finds the move. |
| checkpoint | The local `agent_move` row on each Mac: side, step, the saved plan, and any hold. |
| step | The local progress of one side (section 3). |
| claim | engram's record of which Mac holds an agent, with an epoch. The receiver takes it during the move. |
| participant | A module that provides `agent-data/v1` and declares the session reference kinds it holds data for. |
| boundary | The point after which a side can no longer abort: the receiver starting activation. |
| hold | A durable note on the checkpoint that stops the driver from retrying until something changes. |
| fork | Both Macs changed the agent; the receiver holds instead of choosing a side. |

## 2. The move record

States are closed: `published`, `staged`, `claimed`, `activated`, `released`,
`aborted`. These are the only allowed transitions, with the side that may
write each (`check_transition` and the author check in `move_record.rs`):

| From | To | Writer |
|---|---|---|
| (none) | `published` | source |
| `published` | `staged` | receiver |
| `staged` | `claimed` | receiver |
| `claimed` | `activated` | receiver |
| `activated` | `released` | source |
| `published` | `aborted` | source or receiver |
| `staged` | `aborted` | receiver |
| `claimed` | `aborted` | receiver |

Every other row is refused. "Source" means the record's `from` Mac and
"receiver" its `to` Mac, as authored in engram; a record written by the wrong
Mac is not acted on. A record or inbox body that is malformed, has an unknown
version or state, or exceeds its size bound is refused by name.

For an agent with catalog package pins, the immutable plan also includes
`pin_digest`, the digest reported by `catalog.agent_generation`. The source
captures it at admission and checks that the selection did not change before
committing the checkpoint. An empty pin set omits the field entirely, retaining
the original record bytes for older Macs. Updated entorhinal decoders accept
records both with and without `pin_digest`.
Moving an agent with catalog pins requires **both Macs to support catalog
moves**. Older record decoders reject fields they do not recognize, including
`pin_digest`, so they do not pick up a pinned-package move. The source remains
at `published`, with status `waiting_for_target`,
the message "waiting for the target Mac to pick up the move", `waiting_since_ms`
and elapsed `waiting_ms` in move status and `agent.where`. It never interprets
an absent inbox entry as evidence of rejection. The operator can abort this wait
with the existing move-abort command, upgrade the target, and retry.

Tests: `transition_table_exhaustively_refuses_every_unlisted_row`,
`author_check_maps_every_state_to_its_required_writer`,
`move_codec_refuses_unknown_state`,
`forged_activated_author_or_plan_cannot_release_source_participants`,
`source_activated_wrong_author_never_releases`,
`largest_admitted_body_always_has_room_for_an_abort`.

## 3. Local steps

Steps are closed. Source: `preparing` → `exported` → `sealed` → `published` →
`releasing` → `released`, or `aborted`. Receiver: `picked_up` → `imported` →
`staged` → `claimed` → `activating` → `activated` → `done` or `taken_over`, or
`aborted`. Terminal steps are `released`, `aborted`, `done` and `taken_over`.

| Side | Step | Work done to reach the next step |
|---|---|---|
| source | `preparing` | `agent_data.prepare` and `agent_data.export` on each participant in the saved plan. |
| source | `exported` | Seal the cut at engram's session door; `seal_sent` is saved before the call. |
| source | `sealed` | Publish the move record (`published`) and append the receiver's inbox. |
| source | `published` | Wait until the record reads `activated`. |
| source | `releasing` | Apply the claim, complete the door handoff, `agent_data.release` on each participant, then write `released`. |
| receiver | `picked_up` | Check the inbox entry against the record and `agent_data.import` on each participant. |
| receiver | `imported` | Write `staged`. |
| receiver | `staged` | Take the agent's claim from the source by compare-and-swap, then write `claimed`. |
| receiver | `claimed` | Re-check local writers, then save `activating` (the boundary). |
| receiver | `activating` | `agent_data.activate` on each participant, then write `activated`. |
| receiver | `activated` | Finish when the record reads `released` (`done`) or the door reports takeover (`taken_over`). |

Each Mac allows at most one open move per agent, whichever side it is on, and
at most four open moves in total. A receiver's inbox holds at most 64 entries.

Tests: `restarted_source_and_receiver_resume_at_every_recorded_step`,
`full_move_three_participants_preserves_refs_claim_and_local_journals`,
`one_open_move_per_agent_on_either_side`,
`received_move_acquires_slot_and_refuses_open_agent_and_cap`,
`receiver_inbox_defers_open_agent_and_four_row_cap`,
`inbox_caps_at_64_and_allows_resend_when_full`.

## 4. Starting a move

`agent.move {agent_id, to, request_key}` is admitted on three paths. A flow
scoped route is refused first (`flow_scope_not_admitted`).

| Caller | Rule |
|---|---|
| `Direct` (`ck agents move`) | The operator approves the exact move at a Touch ID prompt. After approval the claim, the open-move slot and idleness are checked again. |
| The sole provider of `agent-orchestrator/v1` | Accepted without a local prompt. That module relays moves the operator confirmed on the phone; entorhinal checks only that the caller is the resolved provider. |
| `reserved:engram` | Accepted only while this Mac's lid-close setting is on (`caller_forbidden` otherwise). |

Anyone else gets `caller_forbidden`. Then, in order, and with nothing written
on refusal:

1. `invalid_request` for unknown fields or an empty `request_key`. A request
   key already used with the same `agent_id` and `to` returns the existing
   move; the submitted target string, not today's name for it, identifies a
   resend.
2. `authority_not_cut_over`; `identity_log_disabled` (moves need the shared
   identity log).
3. `unknown_agent`; `gone`.
4. The agent's claim must be current, intact and held by this Mac
   (`claim_stale`, `claim_integrity_failed`, `not_holder`).
5. `to` must name exactly one other Mac by its display name in the identity log
   (`unknown_member`; `invalid_request` when the target is this Mac).
6. `move_in_progress` when the agent already has an open move; `move_cap` at
   four open moves.
7. Participant discovery: every required participant must be available, and
   every bound session reference kind must be covered by a participant
   (`participant_unavailable`).
8. Idle: every bound session must read `gone` in session liveness, and every
   participant must report the agent idle. Otherwise `agent_busy`, with a list
   naming each reference and reason (`session_active`, `liveness_unknown`, a
   participant's reason code, or `busy_unknown`).
9. The checkpoint is inserted at `preparing` in one transaction that re-checks
   the slot and cap and fences new session registrations for the agent
   (`agent_moving` in [session-liveness-contract.md](session-liveness-contract.md)).

The reply is `{cut_id, step}`.

Tests: `preconditions_refuse_in_order_with_named_codes`,
`busy_refusal_is_read_only_and_names_liveness_and_participant_reasons`,
`engram_requires_setting_and_idle_while_orchestrator_never_prompts`,
`direct_rechecks_claim_open_moves_and_idle_after_confirmation`,
`request_body_is_submitted_target_and_survives_target_rename`,
`crash_before_commit_leaves_nothing_and_same_key_concurrent_resends_share_checkpoint`,
`registration_races_real_admission_insert_from_both_orders`,
`held_elsewhere_and_busy_admission_have_no_move_side_effects`.

## 5. Crash and retry

1. **Durable first.** Every step, the participant plan and every body that
   will be compared-and-swapped are saved in the checkpoint before the call
   that depends on them. A restart resumes from the saved step using the saved
   plan, never a fresh participant discovery.
2. **The driver.** Open moves run on a driver task that wakes when a move is
   admitted or aborted, on reconnect, and every 30 seconds. A remote call never
   holds a store handle or the shared-log writer.
3. **Unknown outcomes.** When a participant or door call's outcome is unknown,
   the client asks for the cut's status and then resends the identical
   request. A participant's reply for a cut is durable, so a resend returns the
   first reply. A still-unknown outcome is left to the driver's backoff.
4. **Compare-and-swap settles once.** A lost reply to a record or claim write
   is settled by reading what landed, so a crash on either side of the claim
   take never takes the claim twice and never strands it.
5. **Refusals.** Each participant or door refusal maps to an action for the
   driver. A retryable refusal (for example the door's `cut_not_ready` or
   `unavailable`) is saved as a hold carrying its code and retried with
   backoff. Before the receiver's activation boundary, a refusal that cannot be retried, including
   an unknown code, aborts the move with a named reason such as
   `door_refused`. A refusal code the contract does not allow for that
   operation is a contract violation and is never retried. An import digest
   mismatch aborts the move.
6. **Task failure.** A move task that panics records a hold with code
   `move_task_failed` and is driven again after a backoff that doubles from one
   second up to five minutes.

Tests: `restarted_source_and_receiver_resume_at_every_recorded_step`,
`source_retryable_refusals_cap_backoff_and_unknown_codes_abort_by_name`,
`lost_replies_for_all_state_changes_are_reconciled_and_return_first_reply`,
`lost_participant_and_record_replies_settle_without_duplicate_claim_epochs`,
`crashes_on_both_sides_of_take_and_return_cas_never_strand_or_retake_claim`,
`receiver_restart_after_claimed_record_cas_does_not_send_a_second_claimed_cas`,
`participant_contract_violations_are_never_retryable_by_code`,
`receiver_digest_mismatch_import_aborts`,
`panicked_cut_releases_owner_records_hold_and_redrives_after_backoff`.

## 6. Aborting

`agent.move_abort {cut_id, request_key}` asks this Mac's side of the move to
stop and undo its work. Either Mac can ask, within the limits below.

1. **Who.** `Direct` with a Touch ID prompt, or the orchestrator without one.
   Others get `caller_forbidden`. `unknown_move` when this Mac has no such cut.
2. **Until when.** Only before the side's boundary: source steps up to
   `published`, receiver steps up to `claimed`. Later steps refuse
   `invalid_state` and write nothing. An abort request and the receiver's
   `activating` checkpoint are decided in one transaction, so exactly one wins.
3. **Source limits.** The source can abort only while the shared record is
   still `published`. Once the receiver has staged it, the source's request is
   cleared with detail `invalid_state` ("the receiver must abort"), and the
   source keeps the agent frozen.
4. **Cleanup.** Aborting writes `aborted` to the record where one exists,
   returns the claim if the receiver took it, aborts the door cut if it was
   sealed, and calls `agent_data.abort` on each participant, which unfreezes
   the agent on the source.
5. **Repeats.** Aborting an `aborted` move, or one whose abort is already in
   progress, returns its status without prompting.

Tests: `abort_boundary_refuses_without_writes_for_both_sides`,
`abort_and_activation_boundary_race_has_one_fenced_winner`,
`move_controls_refuse_wrong_callers_missing_keys_and_boundary_without_writes`,
`source_abort_at_staged_or_later_is_cleared_and_never_unfreezes`,
`source_abort_cleans_every_preboundary_checkpoint_with_and_without_a_record`,
`moved_claim_aborts_without_take_and_claimed_abort_returns_epoch_two_to_a`,
`receiver_abort_at_every_irreversible_step_writes_nothing_and_restart_never_cleans_up`,
`abort_matrix_cleans_every_pre_boundary_checkpoint_across_restarts`.

## 7. Holds

A hold is saved on the checkpoint and reported in the move's status. The
driver stops retrying a held move until something changes.

Before import, a participant's `diverged` answer of `unknown` or `advanced`
blocks the move and causes an abort. Either answer may include an optional
human-readable `reason`. Entorhinal stores and displays it with the participant
and ref; it never interprets the text. An absent reason stays absent, and a
malformed reason remains a participant contract violation.

| Status | When | What clears it |
|---|---|---|
| `abort_stuck` | A participant refused `agent_data.abort` with an operator-retry action. The source agent stays frozen. | An operator retry (`ck agents move-abort <cut_id>` again, `Direct` only, with Touch ID), which retries once; or one fresh attempt after the driver restarts. A second refusal holds again. |
| `held_as_fork` | At the receiver, a participant refused activation terminally, or broke its contract during activation. The receiver stays at `activating` and keeps the claim. | Operator repair. entorhinal does not retry and does not return the claim. |
| `contract_violation` | A participant replied outside its contract at another step. | Operator repair. |

A receiver also holds, rather than aborts, when it cannot prove the claim it
must take: the source is not the holder at the pinned version, or the claim
names the wrong Mac or epoch.

Tests: `source_digest_mismatch_abort_stuck_frozen_retries_once_on_restart`,
`receiver_digest_mismatch_abort_stuck_retries_once_on_restart`,
`stuck_abort_retry_requires_direct_confirmation`,
`operator_abort_retry_wakes_only_the_requested_stuck_cut_once`,
`receiver_digest_mismatch_activate_holds_fork_across_restart`,
`receiver_activation_fork_and_contract_violation_hold_without_retry_across_restart`,
`receiver_wrong_author_wrong_epoch_or_no_persisted_take_holds_claim_moved`,
`move_views_explain_forks_stuck_aborts_contract_violations_and_release_warnings`.

## 8. Guarantees

Guaranteed:

- After the receiver saves `activating`, the move always completes or holds;
  it never aborts.
- The claim moves at most once per move, and an aborted move after the claim
  take returns it.
- The source releases participants only after reading an `activated` record
  written by the receiver for the plan it published.
- A move never chooses between two divergent copies; it holds as a fork.

Not guaranteed:

- That a participant enforces the freeze on the source. A participant that
  cannot block writes reports it, and the move carries a
  `release_not_enforced` warning: anything written on the old Mac after that
  point becomes a fork.
- That a move finishes while engram, the other Mac or a participant is
  unreachable. Transient failures are retried with a backoff that grows to
  five minutes; refusals that cannot be retried before the activation boundary
  abort the move.

## 9. Status and the lid-close setting

`agent.move_status {cut_id}` and `agent.moves` read local checkpoints only and
never contact engram or a participant. `agent.moves` lists open moves, oldest
first, then the 32 most recently finished. Each entry shows `cut_id`,
`agent_id`, `side`, `peer` and `peer_name`, `step`, `reason`, `detail`,
`hold`, `attempts` and timestamps, plus `warnings`, `diagnostics` and a
`status` with `message` when a hold applies. Saved plans and claim bodies are
not shown. A participant's optional `diverged` explanation appears in `detail`
next to its `module_id`, `ref` and state, and is also included in `agent.where`
while the move is open or in its move diagnostics after completion.

`agent.set_lid_close_moves {enabled, request_key}` is `Direct` only
(`caller_forbidden` otherwise) and asks for Touch ID when the value changes.
It defaults to off and is local to the Mac.

Tests: `moves_view_orders_open_first_and_limits_finished_to_latest_32`,
`served_move_controls_admit_confirm_and_bypass_shared_writer`,
`move_controls_presence_failure_and_aborted_noop_preserve_state`.

## 10. Catalog package closures

Catalog pins are shared `catalog.pin` identity-log entries; a move does not copy
them. For a nonempty selection the saved participant plan contains one synthetic
`package-closure` ref, whose `ref` is the agent ID. Entorhinal handles the package
blob export and import for this ref. It is not
a session binding and never enters session liveness. An unpinned agent does not
include this participant, even if daemon discovery finds entorhinal advertising
`agent-data/v1`: there are no package bytes for it to carry.

Before divergence checks or import, the receiver compares its own pin digest
with the plan's digest. On a difference it catches up the identity log once and
compares again. A persistent difference aborts with `pin_digest_mismatch`, naming
`source_pin_digest` and `receiver_pin_digest`. The receiver checks local publisher
trust before staging and repeats its preflight before taking the claim. Missing
trust aborts with `trust_missing`, listing publisher scopes and key fingerprints;
it also refuses moves initiated automatically when the source Mac's lid closes,
rather than parking them until trust changes. Trust keys on the target with
`ck catalog publisher trust <fingerprint> --agent <agent_id>` and retry.
These are move abort reasons, not new participant refusal codes.

Publisher grants and source approvals never travel. Non-CortexKit publishers
need a live target-local install grant for that agent. CortexKit keys must be
certified against the receiver's own built-in root through its configured
registry's existing key-chain verifier, against the built-in CortexKit root
`catalog_trust::CORTEXKIT_ROOT`. The scope name alone never grants trust.

Entorhinal exports an immutable snapshot of the pinned content-store blobs on
its `reserved:entorhinal` session-door stream. Each cut has a new, deterministic
session container derived from the agent ID and cut ID, so a later move cannot
reuse a container fenced by an earlier handoff. Prepare durably saves the
snapshot, admits it, and checks cloud coverage; export confirms coverage before
the driver seals the ordinary cut. Lease and offline errors map to the existing
retryable participant `unavailable` refusal. Lost admits are reconciled by
checking the already admitted prefix, not by duplicating bytes.

The opaque stream format is `package-closure/v1`: ASCII
`ck-package-closure/v1\n`, a big-endian u32 blob count, then ascending unique
raw SHA-256 digests (32 bytes), each followed by a big-endian u64 byte length and
the raw blob bytes. There are no pins, permissions or generation IDs in it.
The blobs are the pinned packages' manifests and archives: each frame's digest is
a pin's `manifest_sha256` or `archive_sha256`, and the source writes each distinct
digest once. Each admission carries at most 1,048,576 raw bytes, the engram
session service's per-call limit, and a blob may span admissions. The receiver hashes
the stream incrementally, checks its sealed cut digest, then verifies every
frame in bounded staging files before publishing any blob. Bad magic/count,
nonascending or duplicate digests, blobs over 10 MiB, hash mismatches, trailing
bytes, or a digest set different from the local pinned closure refuse the
existing `digest_mismatch` code. A local generation is built from those pins and
the verified manifests, checked and prefetched to catalog consumers, then marked
`ready`. Aborting before activation removes its eligibility for activation but
does not change shared pins or local publisher grants.

Participant checkpoints live under `catalog/move-checkpoints`, separately from
the payloads under `catalog/moves`. An abort is durable even when import never
started, without staging any package bytes. `agent_data.status` returns `none`
when this participant has no checkpoint for the requested cut;
a fresh participant instance still reports an aborted cut as `aborted` and
refuses to import it again. Internal storage errors use the participant's
existing retryable `unavailable` code, not private catalog refusal codes.

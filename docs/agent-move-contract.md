# Agent move contract

A move hands an idle agent, with its sessions and their data, from the machine
that holds it (the **source**) to another of the operator's machines (the
**receiver**). Both machines run entorhinal; each drives its own half. They
coordinate through the hub's `KV_residence` bucket: a move record tracks the
handoff and an agent residence claim identifies the holder. The data itself
goes through engram's session door as a sealed cut. Every module that keeps
agent data takes part through the `agent-data/v1` participant contract.
[moves.md](moves.md) is the operator guide.

Code: `crates/entorhinal-module/src/move_admission.rs` (starting a move),
`move_source.rs` and `move_receiver.rs` (the two halves), `move_driver.rs`
(scheduling and restart), `move_ops.rs` (status, abort, lid-close setting),
`participant_client.rs` and `participant_discovery.rs` (participants),
`hub_moves.rs` and `hub_residence/moves.rs` (hub coordination),
`door_client.rs` (engram's session door); `crates/entorhinal-core/src/move_record.rs`
(participant plan) and `move_store.rs` (the local checkpoint).

## 1. Vocabulary

| Term | Meaning |
|---|---|
| cut | One move, named by its `cut_id` (16 random bytes in lowercase hex). |
| move record | The `move.<cut_id>` key in `KV_residence`. Both machines watch it and update it by compare-and-swap. Its states are listed in section 2. |
| checkpoint | The local `agent_move` row on each machine: side, step, the saved plan, and any hold. |
| step | The local progress of one side (section 3). |
| claim | The hub's `residence.<agent>` key records the holder, its membership episode and the claim generation. The source transfers it to the receiver during the handoff. |
| participant | A module that provides `agent-data/v1` and declares the session reference kinds it holds data for. |
| boundary | The point after which a side can no longer abort: the receiver starting activation. |
| hold | A durable note on the checkpoint that stops the driver from retrying until something changes. |
| fork | Both machines changed the agent; the receiver holds instead of choosing a side. |

## 2. The move record

These are the allowed hub move record transitions and their writers
(`check_send` in `hub_moves.rs`). The local `claimed` checkpoint is not a
hub record state.

| From | To | Writer |
|---|---|---|
| (none) | `published` | source |
| `published` | `staged` | receiver |
| `staged` | `handing` | source |
| `handing` | `handed` | source |
| `handing` or `handed` | `activated` | receiver |
| `activated` | `released` | source |
| `published` | `aborted` | source or receiver |
| `staged` | `aborted` | source or receiver |
| `handing` or `handed` | `aborted` | receiver |

The hub reader checks the signed writer against the source or receiver for the
state and checks the transition against the pinned move plan. A watch may skip
intermediate revisions, so the reader checks reachability as well. A malformed
record or one written by the wrong machine is not acted on.

For an agent with catalog package pins, the immutable plan also includes
`pin_digest`, the digest reported by `catalog.agent_generation`. The source
captures it at admission and checks that the selection did not change before
committing the checkpoint. An empty pin set omits the field entirely, retaining
the original record bytes for older machines. Updated entorhinal decoders accept
records both with and without `pin_digest`.
Moving an agent with catalog pins requires **both machines to support catalog
moves**. The source checks the target's participant advertisement before
sealing. An older receiver with a strict record decoder may also fail to
recognize `pin_digest`. A lack of progress does not prove rejection. The
operator can abort before handoff, upgrade the target, and retry.

The source copies `optional` from `agent_data.describe` into the immutable
participant plan. Each participant entry writes `optional: true` only when
opted in; absent or false means required. `skipped` is a sorted, unique list of
module IDs from the optional participants in that plan. It is omitted when
empty. The receiver first writes it on `staged` and carries it unchanged to
`handing`, `handed`, `activated`, an abort after `staged`, and `released`.
Neither `published` nor an abort
directly from `published` carries `skipped`.

Moves without optional participants retain their prior wire bytes. Both
machines must run an Entorhinal version that understands these members before
any participant sends `optional: true` (agent-data 0.3.2): older strict decoders
reject the new describe and move record members.

Test: `hub_move_reader_refuses_wrong_side_and_changed_agent_without_advancing`.

## 3. Local steps

Steps are closed. Source: `preparing` → `exported` → `sealed` → `published` →
`releasing` → `released`, or `aborted`. Receiver: `picked_up` → `imported` →
`staged` → `claimed` → `activating` → `activated` → `done` or `taken_over`, or
`aborted`. Terminal steps are `released`, `aborted`, `done` and `taken_over`.

| Side | Step | Work done to reach the next step |
|---|---|---|
| source | `preparing` | `agent_data.prepare` and `agent_data.export` on each participant in the saved plan. |
| source | `exported` | Seal the cut at engram's session door; `seal_sent` is saved before the call. |
| source | `sealed` | Publish `move.<cut_id>` as `published`. |
| source | `published` | Once the receiver stages the move, write `handing`, transfer residence by compare-and-swap, and write `handed` unless the receiver already advanced. Wait for `activated`. |
| source | `releasing` | After verifying the activated move, complete the door handoff, call `agent_data.release` on the non-skipped participants, then write `released`. |
| receiver | `picked_up` | Verify the watched move and its source claim, fetch the sealed cut through the session door, and call `agent_data.import` on the participants. |
| receiver | `imported` | Write `staged`. |
| receiver | `staged` | Wait for the source's handoff. When the move and residence confirm the transfer, save the local `claimed` checkpoint. |
| receiver | `claimed` | Re-check local writers, then save `activating` (the boundary). |
| receiver | `activating` | Accept the cut at the session door, call `agent_data.activate` on the non-skipped participants, then write `activated`. |
| receiver | `activated` | Finish when the record reads `released` (`done`) or the door reports takeover (`taken_over`). |

Each machine allows at most one open move per agent, whichever side it is on, and
at most four open moves in total.

Test: `hub_move_restarts_through_the_full_handoff`.

## 4. Starting a move

`agent.move {agent_id, to, request_key}` is admitted on three paths. A flow
scoped route is refused first (`flow_scope_not_admitted`).

| Caller | Rule |
|---|---|
| `Direct` (`ck agents move`) | The operator approves the exact move at a presence prompt. After approval the claim, the open-move slot and idleness are checked again. |
| The sole provider of `agent-orchestrator/v1` | Accepted without a local prompt. That module relays moves the operator confirmed on the phone; entorhinal checks only that the caller is the resolved provider. |
| `reserved:engram` | Accepted only while this machine's lid-close setting is on (`caller_forbidden` otherwise). |

Anyone else gets `caller_forbidden`. Then, in order, and with nothing written
on refusal:

1. `invalid_request` for unknown fields or an empty `request_key`. A request
   key already used with the same `agent_id` and `to` returns the existing
   move; the submitted target string, not today's name for it, identifies a
   resend.
2. `authority_not_cut_over`; `identity_log_disabled` (moves need the shared
   identity log).
3. `unknown_agent`; `gone`.
4. `hub_not_ready` if the hub backend is not selected or the residence watch
   is not installed. This is before reading the claim, prompting or saving a
   checkpoint, including for lid-close moves.
5. The agent's claim must be current, intact and held by this machine
   (`hub_not_ready`, `claim_integrity_failed`, `not_holder`).
6. `to` must identify a current member by display name or box id
   (`unknown_member`; `invalid_request` when the target is this machine).
7. `move_in_progress` when the agent already has an open move; `move_cap` at
   four open moves.
8. Participant discovery: every required participant must be available, and
   every carried session reference kind must be covered by a participant
   (`participant_unavailable`).
9. Idle: every carried session must read `gone` in session liveness, or `idle`
   when the session's recorded runner is itself a participant in this move
   holding that reference (its `prepare` refuses while a turn runs and freezes
   the session before answering). A host that is not a participant, such as an
   OpenCode or Pi process, could keep writing during the move, so its sessions
   must be closed. Every participant must also report the agent idle.
   Otherwise `agent_busy`, with a list naming each reference and reason
   (`session_active`, `liveness_unknown`, a participant's reason code, or
   `busy_unknown`).
10. The checkpoint is inserted at `preparing` in one transaction that re-checks
   the slot and cap and fences new session registrations for the agent
   (`agent_moving` in [session-liveness-contract.md](session-liveness-contract.md)).

The reply is `{cut_id, step}`.

Tests: `hub_move_admission_routes_agent_ref_and_replays_key`,
`hub_move_admission_idle_session_requires_its_freezing_runner`.

## 5. Crash and retry

1. **Durable first.** Every step, the participant plan and every body that
   will be compared-and-swapped are saved in the checkpoint before the call
   that depends on them. A restart resumes from the saved step using the saved
   plan, never a fresh participant discovery.
2. **The driver.** Open moves run on a driver task that wakes on local move
   changes and hub watch changes. A remote call never holds a store handle
   or the shared-log writer.
3. **Unknown outcomes.** When a participant or door call's outcome is unknown,
   the client asks for the cut's status and then resends the identical
   request. A participant's reply for a cut is durable, so a resend returns the
   first reply. A still-unknown outcome is left to the driver's backoff.
4. **Compare-and-swap settles once.** A lost reply to a record or claim write
   is settled by reading what landed, so a crash on either side of the claim
   take never takes the claim twice and never strands it.
5. **Refusals.** Each participant or door refusal maps to an action for the
   driver. A retryable refusal (for example, an unavailable session door)
   is saved as a hold carrying its code and retried with
   backoff. Before the receiver's activation boundary, a refusal that cannot be retried, including
   an unknown code, aborts the move with a named reason such as
   `door_refused`. A refusal code the contract does not allow for that
   operation is a contract violation and is never retried. An import digest
   mismatch aborts the move.
6. **Task failure.** A move task that panics records a hold with code
   `move_task_failed` and is driven again after a backoff that doubles from one
    second up to five minutes.
7. **Cut reservations.** The receiver records a pending `cut.release {cutId}`
   in the same local transaction as `activated`. The source records one when
   it finishes `aborted` after sending `cut.abort`; an abort before any cut was
   sealed has nothing to release. This metadata lives only in the move row's
   `plan._cut_release`, never in the shared move record or identity journal.
   An independent task sends the request over engram's `session-door` as
   `reserved:entorhinal`, without holding a store handle or write lock across
   the call. It starts and stops with the move driver, including store shutdown.
   Neither a pending nor a stopped release holds a move open or prevents another
   move. A lost reply resends the same idempotent request.
   `published` and `aborted` outcomes finish cleanup, including when
   `reservationDropped` is false (another commit still lists the cut).
   `invalid_request`, `caller_forbidden` and `cut_release_unknown` stop cleanup
   with that code and one log entry. Temporary door outages and inactive door
   leases retry from one second, doubling to five minutes.
   `plane_version_unsupported` means the door does not yet support removing
   the cut's storage reservation. It retries at most every six hours without
   an error log. Attempts and deadlines survive restart; a crash during
   an unanswered call conservatively keeps its six-hour deadline.
   `agent.move_status` and `agent.moves` include `cut_release` only when it
   exists: `pending`, `released` with `outcome` and `reservationDropped`, or
   `stopped` with `code`. Health reads atomic `cutReleasesPending`,
   `cutReleasesReleased` and `cutReleasesStopped`; the latter two count
   completions in the current process.

Tests: `hub_move_restarts_through_the_full_handoff`,
`hub_move_prepare_refusals_retry_or_abort`,
`hub_move_handed_abort_returns_residence_even_with_a_lost_reply`,
`participant_contract_violations_are_never_retryable_by_code`,
`panicked_cut_releases_owner_records_hold_and_redrives_after_backoff`.

## 6. Aborting

`agent.move_abort {cut_id, request_key}` asks this machine's side of the move to
stop and undo its work. Either machine can ask, within the limits below.

1. **Who.** `Direct` with a presence prompt, or the orchestrator without one.
   Others get `caller_forbidden`. `unknown_move` when this machine has no such cut.
2. **Until when.** Only before the side's boundary: source steps up to
   `published`, receiver steps up to `claimed`. Later steps refuse
   `invalid_state` and write nothing. An abort request and the receiver's
   `activating` checkpoint are decided in one transaction, so exactly one wins.
3. **Source limits.** The source can abort before publication, or while the
   hub record is `published` or `staged`. Once handoff starts at `handing`,
   the source's request is cleared with detail `invalid_state` ("the receiver
   must abort"), and the source keeps the agent frozen.
4. **Cleanup.** Aborting writes `aborted` to the record where one exists,
   returns residence if it was transferred to the receiver, aborts a sealed
   door cut, and calls `agent_data.abort` on the participants that took part.
   This unfreezes the agent on the source.
   When a cleanup call is refused, the next attempt waits 1 second, doubling
   to 5 minutes; the wait is saved in the checkpoint, so a restart keeps it,
   and reaching a new step clears it.
5. **Repeats.** Aborting an `aborted` move, or one whose abort is already in
   progress, returns its status without prompting.

Tests: `hub_move_activation_boundary_retries_without_abort`,
`hub_move_receiver_abort_fences_a_held_handing_cas_after_source_restart`,
`hub_move_handed_abort_returns_residence_even_with_a_lost_reply`.

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
| `abort_stuck` | A participant refused `agent_data.abort` with an operator-retry action. The source agent stays frozen. | An operator retry (`ck agents move-abort <cut_id>` again, `Direct` only, with operator confirmation), which retries once; or one fresh attempt after the driver restarts. A second refusal holds again. |
| `held_as_fork` | At the receiver, a participant refused activation terminally, or broke its contract during activation. The receiver stays at `activating` and keeps the claim. | Operator repair. entorhinal does not retry and does not return the claim. |
| `contract_violation` | A participant replied outside its contract at another step. | Operator repair. |

A receiver does not advance when the hub move and residence do not prove the
expected handoff. A cut already accepted at the door during an abort can
also hold as a fork.

Tests: `hub_move_activation_failures_hold_the_fork_across_restart`,
`hub_move_cut_accepted_during_abort_holds_the_fork`.

## 8. Guarantees

Guaranteed:

- After the receiver saves `activating`, the move always completes or holds;
  it never aborts.
- The source transfers the claim at most once per move. A receiver that
  aborts after the transfer returns it.
- The source releases participants only after reading an `activated` record
  written by the receiver for the plan it published. It never releases an
  optional participant named in `skipped`. That participant's data stays on the
  source, so a later move can still carry it: back to the source, or onward once
  the receiver runs that participant.
- A move never chooses between two divergent copies; it holds as a fork.

Not guaranteed:

- That a participant enforces the freeze on the source. A participant that
  cannot block writes reports it, and the move carries a
  `release_not_enforced` warning: anything written on the old machine after that
  point becomes a fork.
- That a move finishes while the hub, the session door, the other machine or a participant is
  unreachable. Transient failures are retried with a backoff that grows to
  five minutes; refusals that cannot be retried before the activation boundary
  abort the move.

## 9. Status and the lid-close setting

`agent.move_status {cut_id}` and `agent.moves` read local checkpoints only and
never contact the hub, the session door or a participant. `agent.moves` lists open moves, oldest
first, then the 32 most recently finished. Each entry shows `cut_id`,
`agent_id`, `side`, `peer` and `peer_name`, `step`, `reason`, `detail`,
`hold`, `attempts` and timestamps, plus `warnings`, `diagnostics` and a
`status` with `message` when a hold applies. Saved plans and claim bodies are
not shown. A participant's optional `diverged` explanation appears in `detail`
next to its `module_id`, `ref` and state, and is also included in `agent.where`
while the move is open or in its move diagnostics after completion.

On the receiver, an `agent_data.import` reply item may carry `dropped`, a map
from participant-defined reasons to counts of entries the participant dropped
while importing (agent-data 0.2.4). The receiver keeps each participant's
drops in its machine-local checkpoint, in the same saved plan as the rest of
its progress, before it moves on. A later reply from the same participant for
the cut, such as the one recovered after a lost reply or a restart, replaces
that participant's drops instead of adding to them. While any are recorded,
and the move has not aborted, `agent.move_status` and `agent.moves` include
`losses`: one `{participant, session_ref, reason, count}` entry per reason,
sorted by participant, then `session_ref` (the item's `sessionRef`), then
reason. Reasons and counts are shown as given; entorhinal never branches on a
reason. These import drops are not written to the shared move record, so only
the receiving machine shows them. A `dropped` map that breaks the
contract's rules (empty, more than 16 reasons, a reason outside
`[a-z][a-z0-9_]{0,63}`, or a count outside 1 to 2^53 - 1) is an invalid reply:
the receiver records a `contract_violation` naming the broken rule and stops
the step, as it does for an out-of-contract refusal code.

On both machines, each sealed stream of a skipped optional participant adds a
loss using the same entry shape, with reason `optional_participant_absent`,
count `1`, `source` (the source machine's author ID), and `source_name` if its
display name is known. Only the participant IDs are shared in `skipped`; both
machines record the losses in their local checkpoints. An optional participant
that sealed no items is still skipped but adds no loss. A move without losses
has no `losses` key, and aborted moves show no losses.

`agent.set_lid_close_moves {enabled, request_key}` is `Direct` only
(`caller_forbidden` otherwise) and asks for operator confirmation when the value changes.
It defaults to off and is local to the machine.

Tests: `moves_view_orders_open_first_and_limits_finished_to_latest_32`,
`served_move_controls_admit_confirm_and_bypass_shared_writer`,
`move_controls_presence_failure_and_aborted_noop_preserve_state`,
`hub_move_basal_import_drops_reach_move_status`,
`import_drop_vectors_decode_and_reject_broken_maps_naming_the_rule`.

## 10. Catalog package closures

Catalog pins are shared `catalog.pin` identity-log entries; a move does not copy
them. For a nonempty selection the saved participant plan contains one synthetic
`package-closure` ref, whose `ref` is the agent ID. Entorhinal handles the package
blob export and import for this ref. It is not
a session binding and never enters session liveness. An unpinned agent with no
session bindings does not include this participant, even if daemon discovery
finds entorhinal advertising `agent-data/v1`: there is nothing for it to carry.
An unpinned agent with session bindings includes it for its `session-bindings`
ref only (section 11).

Before divergence checks or import, the receiver compares its own pin digest
with the plan's digest. On a difference it catches up the identity log once and
compares again. A persistent difference aborts with `pin_digest_mismatch`, naming
`source_pin_digest` and `receiver_pin_digest`. The receiver checks local publisher
trust before staging and repeats its preflight before taking the claim. Missing
trust aborts with `trust_missing`, listing publisher scopes and key fingerprints;
it also refuses moves initiated automatically when the source machine's lid closes,
rather than parking them until trust changes. Trust keys on the target with
`ck packages publisher trust <fingerprint> --agent <agent_id>` and retry.
These are move abort reasons, not new participant refusal codes.

Publisher grants and source approvals never travel. Non-CortexKit publishers
need a live target-local install grant for that agent whose capability ceiling
covers the package. The receiver calls the normal install `coverage()` policy
on the staged manifests, including its scripts and live-agent rules, and checks
again from verified local blobs before taking the claim. Refusals name each
publisher, package and missing permission. CortexKit certification is verified
against `catalog_trust::CORTEXKIT_ROOT`, and its key window must cover the
authenticated package publication time. When no registry is mapped, a persisted
starter-bundle proof can supply this certification and initial status. The scope
name alone never grants trust, and certification is carried into the prepared
generation rather than inferred from the package name.

For every mapped pinned scope, preflight refreshes authenticated status before
import, generation approval and claim take. Revoked and tombstoned versions
refuse with `trust_missing`. Unknown status or an unreachable registry holds the
move for retry (`trust_unavailable`), never aborts it or authorizes activation.

Entorhinal exports an immutable snapshot of the pinned content-store blobs on
its `reserved:entorhinal` session-door stream. Each cut has a new, deterministic
session container derived from the agent ID and cut ID, so a later move cannot
reuse a container fenced by an earlier handoff. Prepare durably saves the
snapshot, admits it, and checks cloud coverage; export confirms coverage before
the driver seals the cut. Lease and offline errors are retryable participant
refusals. If the reply to a `session.admit` call (which hands a chunk of the
stream to the session door) is lost, the participant asks the door how much it
already holds and resends only the rest, never duplicating bytes.

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
existing `digest_mismatch` code. The verified, authorized blobs are published
before consumer checks so consumers can read both digests. Only successful checks
allow generation approval; successful prefetch and a final preflight mark it
`ready`. Aborting before activation removes its eligibility for activation but
does not change shared pins or local publisher grants.

Participant checkpoints live under `catalog/move-checkpoints`, separately from
the payloads under `catalog/moves`. An abort is durable even when import never
started, without staging any package bytes. `agent_data.status` returns `none`
when this participant has no checkpoint for the requested cut;
a fresh participant instance still reports an aborted cut as `aborted` and
refuses to import it again. Internal storage errors are retryable participant
refusals, not private catalog refusal codes.

Each catalog participant request and receiver preflight has a 30-second total
deadline. Local generation writes and aborts allow at most one second for mutex
admission; no move mutex or store handle is held across network I/O. Transfers
are admitted per cut, while status and abort bypass transfer admission. Deadline
or admission refusals are retryable; an aborted checkpoint cannot
be overwritten by a late transfer completion.

## 11. Session bindings

A binding row (`session_binding`) and its network history (`session_network`)
are machine-local. Only head bindings with `ended_at_ms IS NULL` at admission
enter the participant ref set as `(ref_kind, ref)`. That set is fixed for the
whole move: ending a session afterwards does not remove its ref. Delegated
bindings, open or finished, stay bound on the origin computer with their runner
sessions and network history: neither refs nor metadata for them travel. Their
liveness and number do not affect admission, its transactional session-set
recheck, or the 256-ref cap. An omitted session role means head. Finished head
bindings never enter participant prepare, export, import, activate, release
or abort calls; only their metadata travels in entorhinal's own stream.
Admission refuses more than 256 live session refs as `live_ref_cap`, with detail
`{live_refs, cap: 256}`, before recording a move or contacting participants.
Admission-generated refs, including the agent ref described below, do not count toward
this session limit.
The agent-data wire contract also limits each participant's routed ref set to
256 refs. Admission checks the largest routed set after describe and before
busy or state-changing participant calls. Exceeding it refuses `live_ref_cap`
with `{live_refs, cap: 256, module_id, participant_refs}`. Thus a participant
declaring both `agent` and a session kind can carry at most 255 session refs
alongside the agent ref; separate agent-only and session-only participants can
carry the agent ref and all 256 live session refs.
When the agent has at
least one binding and discovery lists entorhinal, the saved participant plan
gives entorhinal one more synthetic ref, `session-bindings`, whose `ref` is the
agent ID, beside `package-closure` when the agent also has pins. Entorhinal's
`agent_data.describe` lists both ref kinds and both stores
(`package-closure/v1`, `session-bindings/v1`). A request naming only the
package closure is answered exactly as before; one naming both is split and
the two answers joined in ref order. An agent with no bindings and no pins
moves with the same participants as before and opens no stream of its own.

Admission describes each available participant once. If at least one declares
`agent` in `ref_kinds`, the move also includes exactly one
`{"ref_kind":"agent","ref":"<agent_id>"}` for the moving agent. Normal
routing sends it to every participant declaring that kind, alongside any
session refs that participant declares. No declarer means no agent ref: the
ref sets, saved plan, move record and wire requests remain unchanged.
Entorhinal's own participant does not declare `agent`; it continues to carry
only package closures and session bindings. The agent ref is frozen with the
other refs for every step, including a move back. It obeys the same receiver
set-matching and absent-participant rules. A participant is absent only when it
is neither configured in the receiver's supervisor nor registered in its
catalog; a configured-but-disabled or warming module is not absent and still
holds the move until it is ready. An absent optional participant is skipped
even if it has items. An absent required participant is skipped if it has no
planned refs, or if the sealed cut manifest contains no item with its
`reserved:<module_id>` stream, even when it was routed the agent ref. The
receiver reads engram's authenticated `cut.read` manifest for these stream
checks and requires its author to match the move's source; an unreadable
manifest or author mismatch holds without deciding a skip. An absent required
participant with planned refs and sealed items still stops the move with
`participant_unavailable` and `absent: true`.
Required no-items skips remain receiver-local, and the source releases those
participants as usual. A present optional participant follows the normal
participant protocol, including release.
The receiver persists the skipped set once and reuses it through import,
activation, abort cleanup and restarts, including when an abort arrives before
preconditions run.

**Source.** Admission inserts the source checkpoint in the transaction that
fences new registrations for the agent (section 4). `prepare` then reads every
binding of the agent, ended ones included, with its whole history, in one
read snapshot that must still show that open source move and whose bindings
must contain every frozen binding ref and no additional live binding
(`invalid_state` otherwise). Finished rows outside that set are marked with
`held_on`: their existing holder, or the source's machine ID recorded by the
store-origin guard, never the source's claim author. An unknown store origin
refuses export rather than guessing a location. Setting a
session's network value or ending a session is not fenced: a change made after
`prepare` stays on the source and does not travel. The stream is written once
per cut and saved under `session-moves/<cut_id>/` in the store folder, so a
repeated `prepare` returns the same item.

**Stream.** Format `session-bindings/v1` when all data travels as refs, or
`session-bindings/v2` when finished metadata names a data holder, on the `reserved:entorhinal`
session-door stream, in its own session container
`sha256("session-bindings:<agent_id>:<cut_id>")[..32]`, so each cut has a new
one. The bytes are the line `ck-session-bindings/v1`, a JSON header line
`{"agent_id","count"}`, then one JSON line per binding in strictly ascending
`(ref_kind, ref)`: `ref_kind`, `ref`, `runner`, `harness`, `session_id`,
`project_root`, `registered_by`, `registered_at_ms`, `ended_at_ms` and
`network_history` (`[{network, since_ms}]`, oldest first, exactly as stored).
Version 2 uses first line `ck-session-bindings/v2` and adds optional `held_on`
to finished metadata rows; only ended rows may name a nonempty holder.
Older readers reject its version as `incompatible_version` before taking the
claim, rather than silently installing reopenable metadata. New readers also
accept v1. Without finished metadata, all package and session stream bytes
remain identical; absent `held_on` is omitted, not serialized as null.
Caps: 16,384 bindings, 1 MiB per line, 64 MiB per stream; a source over a cap
refuses `not_exportable`. Admission and reads go in calls of at most
1,048,576 raw bytes, like the package closure. About 1,300 bindings with short
histories encode to about 530 KiB, one call; a binding with the full 1,024-entry
history is about 47 KiB.

**Receiver.** `import` reads the stream back, hashing it, and refuses
`digest_mismatch` unless it matches the sealed cut digest, then decodes every
line strictly (unknown fields, a bad value, unsorted refs, a history out of
time order or trailing bytes refuse `digest_mismatch`). A first line naming
another `session-bindings` version refuses `incompatible_version` with
`{found, required}`, which aborts the move with that reason. After import, and
before the claim is taken, the driver checks that every carried row is either
one of the move's frozen binding refs or finished metadata with `held_on`;
one that is neither holds the move as a
`contract_violation`. `activate` rechecks the kept bytes' digest
(`digest_mismatch` holds as a fork).

The rows are installed in the transaction that records `activated`, in place
of the placeholders written before rows were carried:

- A binding this machine already has, with the same `(ref_kind, ref)`, keeps its
  own row and history; the carried copy is skipped.
- A live-at-admission carried row whose `(harness, session_id)` or `(runner, session_id)` is held
  by another binding here is installed as a placeholder (no host session,
  runner or end time, `registered_by` `move`), still with its carried history.
  The move goes on; the count is logged once and kept in
  `sessionBindingsPlaceholders`.
- A finished metadata row with a colliding host or runner key is skipped,
  never replaced by a placeholder. Its local owner and history stay untouched.
  The activation result counts and names each skip and its local owner; the
  receiver logs that collision list once per move.
- Any other carried row is installed as it was, and its history verbatim:
  `since_ms` values are the source machine's clock, like the times of the content
  they are compared with.
- A move ref with no carried row gets a placeholder, as before.

The receiver stores a finished row's incoming `held_on`, clearing it to null
when it equals the receiver's store-origin machine ID. A→B→C therefore keeps
the original holder A; returning to A clears the marker. Locally registered
rows always start with null. The existing 30-day pruning rule also deletes
held-elsewhere metadata and its network-history rows, never participant data
and never by calling a participant.

A moved agent's finished sessions are listed on the new machine, marked as held
on the source machine (or the earlier machine that still holds their data), and cannot
be opened on the new machine until a backfill. `agent.sessions`,
`agent.session_for_runner` and `agent.session_for_ref` include `heldOn` only
when non-null. `agent.register_session`, including re-keying, refuses these
rows as `session_on_other_machine`, with detail `{machine_id}`, without writes.

**Older receivers.** A machine whose entorhinal does not describe
`session-bindings` refuses the move in its preconditions
(`receiver_precondition`, detail `participant_unavailable` with that
`ref_kind`), before any import, claim or binding write. A receiver on this
release then skips the abort call to such a participant, which was never
called for the cut. A receiver on an earlier release does not: its own
entorhinal refuses that abort `invalid_request`, and the receiver resends it
without backoff, so its move stays open until the machine is upgraded. After the
upgrade its entorhinal accepts the abort and the move finishes aborting.

Tests: `export_reads_every_binding_with_its_history_under_the_open_source_move`,
`activation_installs_carried_rows_and_history_verbatim`,
`activation_keeps_an_existing_local_binding_and_its_history`,
`activation_downgrades_a_colliding_row_to_a_placeholder_and_still_activates`,
`session_bindings_stream_golden_round_trip`.

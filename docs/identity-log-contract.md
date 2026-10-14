# Identity log contract

The identity log is how the operator's Macs share projects, workspaces and
agents. engram stores it: an append-only log that engram signs, encrypts and
verifies, so entorhinal sends and receives plaintext entries. Each Mac keeps a
full copy of the shared state in its own store and answers every read from
that copy. Folder paths, approvals and every kind of local authority stay on
the Mac that owns them.

The log is optional. A store starts with it disabled, and everything works on
one Mac without it. [sharing.md](sharing.md) is the operator guide.

Code: `crates/entorhinal-module/src/shared_write.rs` (writing),
`catch_up.rs` (reading), `enable.rs` (enabling and joining),
`log_client.rs` (the engram client), `member.rs` (Mac names);
`crates/entorhinal-core/src/shared_entry.rs` (entry bodies),
`remote_apply.rs` (applying received entries), `enable_state.rs`,
`log_schema.rs` and `pending.rs` (local log state).

## 1. Vocabulary

| Term | Meaning |
|---|---|
| entry | One log record: a position, an entry id, an author, a kind and a body. |
| position | The entry's place in the log, starting at 1. The head is the highest position. |
| author | A 32-character lowercase hex pseudonym for the Mac that wrote the entry, supplied by engram. |
| `signed_by_self` | engram's answer to "did this Mac write this entry". |
| applied position | `last_applied_position`: the highest position this store has applied. |
| pending entry | A write this Mac sent, or may have sent, whose outcome it has not settled yet. |

Entry kinds are closed: `change` and `snapshot`. Entry operations are closed:

| Kind | Operation | Body |
|---|---|---|
| `change` | `project.shared` | Row upserts and deletes for the shared project tables, plus any agents a placement change carried. |
| `change` | `member.set_name` | One Mac's display name. |
| `change` | `agent.create`, `agent.rename`, `agent.update_tag`, `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`, `agent.dispose`, `agent.merge`, `agent.set_namespace` | The complete after-image of one agent and its name claims. |
| `snapshot` | `shared.snapshot` | One part of a complete copy of the shared state. |

Any other kind, operation or envelope version (currently `1`) is refused.

Local log states are closed: `disabled`, `enabling`, `joining`, `enabled`.

## 2. What is shared

Only these tables travel (`PROJECT_SHARED_TABLES`, `AGENT_SHARED_TABLES` and
`MEMBER_SHARED_TABLES` in `shared_entry.rs`):

- `workspace`, `project`, `project_workspace`, `project_alias`,
  `project_root_key`;
- `agent`, `agent_name_claim`;
- `member_name`.

Aliases of implicit (path-derived) project ids are not shared. Everything else
is machine-local: root paths, bindings, approvals, owned-remote overrides,
workspace root paths, session bindings, cached claims, move checkpoints and
all consent state. An agent write made through a consent grant carries its
authorization (card and answer, grant id or `own_hire`) in the entry, so other
Macs can verify and rebuild without the grant, but the grant itself never
travels. See [data-model.md](data-model.md) for every table.

Test: `consent_workspace_and_hire_replicate_without_local_authority_rows`.

## 3. Shared writes

These methods produce a shared entry when the log is enabled: `register`,
`assign_workspace`, `upgrade_implicit`, `remove`, `seed_import`, `add_root`,
`attach_root`, `member.set_name`, every agent identity write except
`agent.import`, and agent tool writes. Other writes stay local.

1. **One writer.** Shared writes run one at a time under the module's writer
   lock. Reads use a separate connection and never wait for it.
2. **Catch up first.** The writer applies any entries it has not seen, then
   uses its applied position as the expected head.
3. **Pending first.** The entry id and expected head are committed to
   `pending_entry` in their own transaction before the append is sent.
4. **Commit only on receipt.** The local change is prepared in a SQLite
   transaction that stays open while the append is in flight. It commits only
   when the log returns a receipt at `expected_head + 1`; the applied position
   advances in the same commit.
5. **Uncertain outcome.** If the append may have been sent but no definitive
   answer arrives in time, the local transaction rolls back, the pending entry
   stays, and the caller gets `engram_outcome_unknown`. A later catch-up finds
   the entry if the log accepted it and applies it then. If nothing could have
   been sent, the pending entry is deleted.
6. **Resends are identical.** A resend after an uncertain answer reuses the
   same entry id, the same bytes and the original expected head, so engram
   recognises a duplicate instead of appending twice.
7. **Contention.** If another Mac appended first (`head_moved`), the writer
   rolls back, catches up and retries with a new entry id. An agent id drawn
   for a create is kept across retries. After five such retries the write
   refuses `identity_log_contended`.
8. **Invariant failures.** A reused entry id or a receipt at the wrong position
   is an invariant failure: it is not retried, and health reports
   `identity_log_invariant`.
9. **Size.** A shared entry over 256 KiB refuses `shared_entry_too_large`
   before it is sent.

Tests: `pending_commits_before_parked_transaction_and_reads_show_prewrite_state`,
`lost_reply_resends_identical_bytes_id_and_stale_head_and_commits_once`,
`ambiguous_resend_refusals_keep_pending_until_receipt_relative_bound_and_recovery`,
`head_moved_uses_new_entry_id_but_agent_id_drawn_once_before_attempts`,
`five_retries_then_head_moved_refuses_identity_log_contended`,
`id_reused_rolls_back_warns_entry_id_sets_health_and_never_retries`,
`domain_and_oversize_refusals_restore_pending_journal_and_projection_on_restart`,
`local_writes_and_liveness_never_touch_engram_on_enabled_store`.

## 4. Catch-up

Catch-up runs before every shared write and in the background every 30
seconds, under the writer lock. A disabled store never connects to engram.

1. It reads forward from the applied position in pages of at most 128 entries.
2. Each entry must be exactly the next position and no higher than the
   reported head. A missing position stops catch-up with `identity_log_stalled`
   and health state `log_gap`. A head below the applied position is ignored
   and reported as `log_head_regressed`; the applied position never moves
   back.
3. Each entry is applied in one transaction with its journal row and the new
   applied position. An entry that cannot be decoded, names an unknown
   operation or would leave the store inconsistent is rolled back and stops
   catch-up (`apply_failed`). It is never skipped.
4. An entry this Mac wrote and already committed only advances the applied
   position; it is not applied twice.
5. While catch-up is stalled, shared writes refuse.

Guaranteed: every Mac applies the same entries in the same order, and a Mac
that cannot apply an entry stops rather than diverging silently. Not
guaranteed: that a stalled Mac recovers by itself. A stall caused by a bad
entry stays until the cause is fixed.

Tests: `background_reads_every_30_seconds_under_writer_lock_and_disabled_never_connects`,
`paging_journals_authorship_and_wakes_agent_changes_waiters`,
`gap_stops_at_next_position_and_shared_writes_refuse_stalled`,
`verification_failure_stalls_before_any_projection_or_position_changes`,
`unsupported_operations_and_constraint_images_fail_closed`,
`regressed_head_is_ignored_without_moving_applied_position`,
`committed_own_entry_only_advances_position_without_reapplying`.

## 5. Snapshots

A snapshot is a complete copy of the shared state, split into parts of at most
200 KiB that occupy consecutive positions.

1. **Parts are buffered.** Catch-up checks each part's snapshot id, part count
   and order, and installs nothing until every part has arrived. If it reaches
   the head with parts missing, it stops without moving the applied position,
   so a later run, including after a restart, fetches the rest.
2. **Atomic install.** A complete snapshot is installed in one transaction. A
   joining store installs it as its starting state; a store that already
   participates applies it as a replacement.
3. **Saved before sending.** The Mac that writes a snapshot saves every part's
   entry id and bytes before sending, and resends only the parts the log has
   not confirmed, with their original ids and bytes.

Tests: `snapshot_waits_across_restart_then_installs_once_with_all_part_audits`,
`snapshot_validation_and_fault_at_final_position_roll_back_the_whole_install`,
`writer_supersede_resume_from_base_resends_only_unconfirmed_parts`,
`landed_multipart_enable_finishes_after_restart_without_resending_receipts`.

## 6. Superseding snapshots

A Mac that enables the log when the log already holds entries, and its own
registry is not empty, may replace those entries with a snapshot of its own
state. This is allowed only when the Mac wrote every entry already in the log.

1. The writer reads the whole prefix and proceeds only if every entry is
   `signed_by_self`. Otherwise enabling refuses `join_requires_empty_registry`
   with the local counts.
2. Every part carries `supersedes_through`, equal to the head it replaces.
3. A reader re-reads positions `1..=supersedes_through` and accepts the
   replacement only if every one of them has the snapshot's author, and all
   parts have one author. Otherwise catch-up stalls.
4. Only then may installation delete shared rows the replacement lacks. Agent
   history must extend: a superseding snapshot may not drop, regress or fork an
   agent the earlier entries created, and may not drop a member name.

Tests: `writer_supersede_requires_every_entry_signed_by_self_before_guard`,
`writer_supersede_parts_use_base_and_finish_at_base_plus_parts`,
`forged_or_malformed_supersedes_stall_joiners_members_and_restarted_members`,
`restarted_member_supersede_rereads_paged_prefix_then_replaces_state`,
`supersede_replaces_shared_rows_cascades_local_rows_and_replays_cleared_keys`.

## 7. Author checks

engram verifies signatures. entorhinal adds these checks on what engram
returns:

1. Every entry must carry an author of exactly 32 lowercase hex characters and
   a `signed_by_self` flag; a missing or malformed one stalls catch-up.
2. A `member.set_name` entry must name the same author as the entry. A Mac can
   name only itself.
3. A snapshot that carries a member name must carry its own author's name.
4. A superseding snapshot must have one author, who wrote every entry it
   replaces (section 6).
5. The key that signed an entry is not taken as its author.

A refused entry stops catch-up with `identity_log_stalled`; nothing after it is
applied until it is resolved.

Tests: `read_requires_signed_by_self_and_catch_up_stalls`,
`read_requires_author_and_catch_up_stalls`,
`read_rejects_noncanonical_author_and_catch_up_stalls`,
`s3_member_author_mismatch_rolls_back_and_signer_is_not_author`,
`s3_forged_name_stalls_and_upgrade_retries_the_name_position`.

## 8. Enabling and joining

`identity_log.enable` is accepted only from an operator `Direct` route with no
flow scope (`ck projects log enable`).

1. **Empty log.** The Mac writes a bootstrap snapshot of its own state. State
   goes `disabled` → `enabling` → `enabled`.
2. **Non-empty log, empty registry.** The Mac joins: it catches up from
   position 1, starting with the bootstrap snapshot. State goes `disabled` →
   `joining` → `enabled` once its applied position reaches the head it saw.
3. **Non-empty log, non-empty registry.** Allowed only as a superseding
   snapshot (section 6); otherwise refused.
4. **Agents.** Agent identity is first imported from the executive
   (`agent.import`), which writes an `agent.cutover` marker into the journal.
   Enabling a store that has no agents and no marker refuses
   `agents_not_imported` unless the operator passes `--without-agents`, which
   itself refuses when agents or a marker exist. A store that has agents but
   no marker refuses `authority_not_cut_over`.
5. **Fence.** While `enabling` or `joining`, every shared write, `agent.import`,
   `set_workspace_root` and removing a project's last root refuse
   `identity_log_enabling`.
6. **Interruption.** An interrupted enable resumes from its saved parts; an
   interrupted join resumes catch-up. Both resume in the background and on the
   next `identity_log.enable`. Refusals before the plan is saved change
   nothing durable.
7. **Already enabled.** Enabling again returns the current status.

Tests: `empty_enable_is_idempotent_admits_create_and_reports_status`,
`nonempty_join_refuses_with_counts_without_marker_and_allows_import`,
`background_reader_resumes_enabling_and_completes_joining`,
`join_outage_preserves_bootstrap_fence_and_resume_has_no_duplicates`,
`import_consent_order_and_flag_rules`,
`attach_surface_enable_is_direct_only_and_fails_closed`.

## 9. Mac names

`member.set_name {display_name}` names this Mac in the log. It is `Direct`
only (`caller_forbidden` otherwise). The name is trimmed and must then be 1–64
characters (`display_name_invalid`). It refuses `identity_log_disabled` while
the log is disabled, `identity_log_enabling` during bootstrap, and
`self_author_unknown` until this Mac has learned its own author pseudonym from
the log. Names are what `ck agents move <agent> --to <mac>` matches against.

Test: `s3_name_refusals_precede_decode_writer_and_engram_in_order`.

## 10. Status

`identity_log.status` reads local state only and never contacts engram:
`state`, `lastAppliedPosition`, `lastSeenHead` and `pendingWriteCount`. The
module's health metrics add `identityLogState`, which can also read
`unavailable` or a stall reason (`engram_key_unavailable`, `not_member`,
`identity_log_invariant`, `log_gap`, `log_verify_failed`, `apply_failed`,
`log_head_regressed`), plus `lastError` and `lastErrorAt`. `lastErrorAt` is
absent when no error has been recorded.

Test: `identity_log_status_reads_durable_progress_and_pending_count_without_writing`.

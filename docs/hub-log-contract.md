# Hub log contract

The identity log is how an operator's machines share registry identity. It
lives on the NATS hub in stream `HUB_IDLOG`, on `ck.fleet.idlog`. Entorhinal
signs records through the vault and seals their bodies under the fleet epoch
key. Each machine verifies and applies entries to its own store and serves
reads from that copy. Sharing is optional; a standalone store starts with it
disabled. [Sharing between machines](sharing.md) is the operator guide.

Code: `crates/entorhinal-module/src/shared_write.rs` (routing writes),
`hub_write.rs` and `hub_identity_log.rs` (hub writes and tail), `catch_up.rs`
(restart), `enable.rs` and `hub_switch/snapshot.rs` (enable and join), and
`member.rs` (machine names). `crates/entorhinal-core/src/shared_entry.rs`,
`remote_apply.rs`, `enable_state.rs` and `pending.rs` own the shared images,
local application, snapshot install and durable attempts.

## What is shared

- Workspace identity and documents: `workspace`, `workspace_document`.
- Projects and placement: `project`, `project_workspace`, `project_alias`,
  `project_root_key`, `placement_state`, `placement_fact`. Aliases of implicit
  (path-derived) project ids do not travel.
- Agent identity: `agent`, `agent_name_claim`.
- Machine identity: `member_name`, `member_box`.
- Package pins and user profile: `catalog_pin_set`, `catalog_pin`,
  `user_profile`, `user_expertise`. Profile changes are excluded from the
  agent and project history feeds.

Root paths, bindings, approvals, owned-remote overrides, workspace root paths,
session bindings, move checkpoints and consent grants stay on their machine.
A shared agent tool write carries its authorization for verification and replay,
not the grant itself. See [Data model](data-model.md) for storage details.

The log is separate from `KV_residence`, the hub bucket carrying agent
residence, role leases, handback requests and [move records](agent-move-contract.md).
Agent claims are durable; holding a role requires a live lease. A bad KV
record affects its own key rather than stopping the entire watch. Hub holders
are daemon box ids. An existing hub may also contain a historical
`engram:<author>` holder or member-name author key. This id is preserved but
is not a Callosum peer id. See the [consumer contract](module-surface.md#sessions-claims-and-roles).

## Shared writes

The ordinary shared-write dispatcher considers these operations
(`shared_candidate` in `shared_write.rs`):

- Projects and workspaces: `project.set_domain`, `placement.backfill`,
  `workspace.set_document`, `register`, `assign_workspace`, `upgrade_implicit`,
  `remove`, `seed_import`, `add_root`, `refresh_root_key`, `attach_root`,
  `move_root_to_new_project`.
- Profile and machine name: `user.set`, `member.set_name`.
- Agent identity: `agent.repair_workspaces`, `agent.apply_avatar_manifest`,
  `agent.create`, `agent.rename`, `agent.update_tag`, `agent.set_labels`,
  `agent.set_avatar`, `agent.set_github_identity`, `agent.dispose`,
  `agent.merge`. `agent.import` is not a shared candidate.

A candidate can still be a no-op or change only local state. Shared agent tool
writes and catalog pin installs reach the hub writer through their own dispatch
paths.

The writer waits for the local tail to reach the expected hub head, previews
what would change, then records the attempt and its send bytes before asking
the hub to append. It checks the fleet epoch before sending. No local change
is committed on the strength of an unconfirmed append. A lost reply is settled
against the original position and send bytes before the writer can try a new
position. The request key lets a retry recover the original reply rather than
performing the mutation again. A contended writer can refuse
`identity_log_contended`. With sharing off, writes stay local.

Tests: `hub_snapshot_registration_binding_recovers_after_publish_and_reports_replaced_checkout`,
`hub_snapshot_tool_consent_settles_after_publish_and_restart`.

## Catch-up and snapshots

The installed tail starts at the saved position, verifies each next record,
and applies it in a transaction with the local journal and position update.
Applying a hub entry advances both `lastAppliedPosition` and `lastSeenHead`.
The tail does not skip a gap or an entry it cannot verify or apply. Reads
still use the last committed state; they never wait for the hub.

On an empty hub, enable saves a snapshot plan before publishing. The canonical
image is split into consecutive parts. A reader checks the part numbers,
identity and digest and installs only a complete snapshot. The initial
residence and role records must also be verified. A retry uses the saved plan
and settles the existing parts rather than starting a different snapshot.
The [snapshot vector](../test-vectors/hub-snapshot-v1/README.md) pins the part
format and entry id. Existing hub snapshots may have a nonzero `engram_through`
base; the snapshot ends at that base plus the number of parts.

Tests: `hub_snapshot_enable_resumes_after_part_and_seed_and_empty_member_joins`,
`hub_catch_up_resumes_saved_enable_and_joins_disabled_store_then_applies_tail`.

## Enable and join

`identity_log.enable` (`ck projects log enable`) needs a Direct route without
flow scope. It returns immediately if the hub log is already installed, and
resumes a saved native enable plan if there is one.

- **Empty hub:** check the local registry and whether agents were imported,
  then ask for operator presence. `--without-agents` is for a fleet that has
  no agents to import. Declining saves no plan. After confirmation the write
  path reads the head again, in case another machine bootstrapped meanwhile.
- **Populated hub:** install the snapshot on an empty local registry, then
  follow the tail. A non-empty registry refuses `join_requires_empty_registry`.
  An empty registry can join in the background without a command.

After a restart, catch-up finishes a saved enable plan; `identity_log.enable`
can also finish it directly. A verified existing hub snapshot can be installed
without starting a new fleet or replacing its contents.

Test: `hub_served_enable_guards`.

## Machine names

`member.set_name {display_name}` names this machine through the log. It is
Direct only (`caller_forbidden` otherwise). The trimmed name must have 1–64
characters (`display_name_invalid`). Naming requires enabled sharing and the
machine's own author id. Moves can target a current member by this display
name. An author id and its display name are distinct; a name is not a peer id.

## Status

`identity_log.status` and `ck projects log status` read local state. The reply
keeps `state`, `lastAppliedPosition`, `lastSeenHead`, `pendingWriteCount`,
`lastError` and `lastErrorAt`. `lastError` and `lastErrorAt` are null when no
error has been recorded. `hubReady` is present only when the hub backend is
selected; it reports whether the residence watch is ready. `pendingWriteCount`
counts the store's retained pending rows, not the hub writer's attempt ledger.

In normal operation `state` is `disabled`, `enabling` or `enabled`. A hub
setup check can report `hub_switch_waiting` or `hub_migration_mismatch`.
A tail failure can report `log_gap`, `log_verify_failed` or `apply_failed`.
Health exposes the same state as `identityLogState` alongside log progress
and the last-error fields.

Test: `identity_log_status_reads_durable_progress_and_pending_count_without_writing`.

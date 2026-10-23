# Data model

entorhinal keeps everything in one SQLite database per Mac: the registry
journal, the tables derived from it, and machine-local operational state. This
document lists the tables, says which are shared through the identity log and
which stay on the Mac, and describes the journal and migrations.

Code: `crates/entorhinal-core/src/lib.rs` (opening, migrations 1, 2, 5 and 8),
`binding.rs` (3), `agent/schema.rs` (4), `ownership.rs` (6), `log_schema.rs`
(7), `move_store.rs` (9), `agent/consent.rs` (10), `mutations.rs` (the journal,
`verify`, `rebuild`), `read_connection.rs` (reads).

## 1. The store

- **Location.** The subc daemon chooses the database path and passes it to the
  module in a storage descriptor when the module starts (`on_hello_ack` in
  `crates/entorhinal-module/src/main.rs`). entorhinal never picks or guesses a
  path. A missing or invalid descriptor makes every request refuse
  `store_unavailable`.
- **Permissions.** The storage library creates the store folder and files
  owner-only. See [SECURITY.md](../SECURITY.md).
- **One writer.** Opening the store takes a single-writer lease, a kernel
  advisory lock released when the holder exits. If a previous process still
  holds it, the module retries for up to 10 seconds and reports `Degraded`
  health meanwhile, then gives up.
- **WAL.** The store runs in WAL mode. Reads use a separate read-only
  connection that refuses a database not in WAL mode; each read sees one
  committed snapshot, including its journal generation, and never waits for a
  writer. The store must be on a local disk: WAL and its shared-memory file do
  not work on network shares.

Tests: `open_retries_while_a_predecessor_still_holds_the_lease`,
`open_gives_up_on_a_lease_held_past_the_budget`,
`a_non_lease_open_error_fails_once_without_retrying`,
`missing_descriptor_signals_without_opening_a_store`,
`wal_reads_and_liveness_answer_prewrite_state_before_a_parked_writer_is_released`.

## 2. The registry journal

`registry_journal` is the source of truth for everything in the derived tables
(section 3). Each row records:

| Column | Meaning |
|---|---|
| `seq` | Order. The store's **generation** is the highest `seq`, or 0 when empty. |
| `op`, `payload_json` | The operation and the values it wrote. |
| `actor` | The actor the request named, if any. |
| `principal` | The route principal that was admitted (`direct`, `reserved:<module>`, `entorhinal` for the module's own writes, `agent` for consent-authorized tool writes). Absent on rows written before the column existed. |
| `request_key`, `response_json` | The request key and the stored reply, so a retry returns the same bytes. Unique across operations. |
| `stream` | `shared` or `local`. |
| `entry_id`, `log_position`, `origin`, `entry` | Identity-log metadata: the entry's id and position, whether it was written `here` or received from the `log`, and its exact bytes. |

Rules:

1. A mutation appends its journal row and updates the derived tables in one
   transaction. A refused request appends nothing.
2. A journal row carries the values the mutation wrote (minted ids, binding
   tokens, epochs, agent after-images, the authorization of a tool write), so
   replay never re-reads the disk, re-validates, or mints new values.
3. Writes that do not change the derived tables never touch the journal:
   session bindings, claim and role caches, move checkpoints, consent state and
   the liveness mirror.

Tests: `journal_principal_labels_are_pinned_and_refused_routes_append_nothing`,
`requests_attribute_all_project_and_secondary_binding_rows_to_the_bound_principal`,
`request_keys_cache_original_reply_across_restart_and_reject_cross_op_reuse`,
`move_apis_do_not_append_journal_rows`,
`self_author_conflict_never_overwrites_identity_and_cache_never_journals`.

## 3. Tables

**Shared and derived.** Sent through the identity log when it is enabled
([identity-log-contract.md](identity-log-contract.md)), and rebuilt from the
journal:

| Table | Holds |
|---|---|
| `workspace` | Workspace ids, names and timestamps. |
| `project` | Project ids, names, and whether a project is implicit. |
| `project_workspace` | Which workspace each project is placed in. |
| `project_alias` | Old project ids that resolve to a current one. Aliases of implicit ids are not shared. |
| `project_root_key` | Machine-neutral root keys (`remote` as `owner/repo`, or `label`) per project. |
| `agent` | Agent identity: name, tag, labels, role, placement, avatar, GitHub identity, supervisor, and retirement or merge. |
| `agent_name_claim` | Active and released name claims, one active claim per agent and per name in a namespace. |
| `member_name` | Display names of the Macs in the identity log, keyed by author. |
| `catalog_pin_set`, `catalog_pin` | An agent's shared package pins, root ranges and canonical pin digest. `catalog.pin` is never an identity-feed op. |

**Local and derived.** Rebuilt from the journal, never shared:

| Table | Holds |
|---|---|
| `project_root` | Each registered root's canonical path, its project, and its root key. |
| `root_binding` | The checkout bound at each root: incarnation token, registration epoch. |
| `retired_binding` | Past bindings and why they ended (`removed` or `replaced`). |
| `root_approval` | Approvals, keyed by registration epoch. |
| `root_owned_remotes` | A root's owned-remote override. No row means `origin`; an empty list means none. |
| `derived_root_parent` | Worker containers attached to a root's binding. |
| `workspace_member` | Local and remote project membership of workspaces. |
| `workspace_root` | The operator-set root folder of each workspace. Absent means "no root", never a guessed one. |

**Local operational state.** Never shared and not rebuilt from the journal;
`rebuild` leaves these alone:

| Table | Holds |
|---|---|
| `identity_log_state` | Log state, applied position, last seen head, and a saved enable plan. |
| `pending_entry` | Shared writes whose outcome is not settled. |
| `session_binding` | Session reference to host session bindings. |
| `claim_cache`, `role_cache` | Cached agent and role claims from engram. A null holder means the record was absent. |
| `role_take_request` | Each approved role take, so a retry resends the same bytes. |
| `self_identity` | This Mac's own author pseudonym, once learned. |
| `agent_move`, `move_setting` | Move checkpoints and the lid-close setting. |
| `consent_grant`, `consent_request`, `consent_write_plan`, `consent_feed_cursor` | Consent state. |
| `agent_audit` | The agent tool audit. |
| `project_hire_cap` | Per-project hire caps. No row means the default of 10. |
| `catalog_registry`, `catalog_scope_map`, `catalog_registry_key`, `catalog_feed`, `catalog_status`, `catalog_publisher_grant`, `catalog_audit` | Local catalog trust, authenticated status and publisher authority. |
| `catalog_plan`, `catalog_generation`, `catalog_transition` | Local catalog preparation, activation and transition receipts (each applied plan or generation transition's argument digest and original result, so a repeat returns the same result); never journaled or exported. |

Tests: `consent_state_is_local_persistent_and_not_rebuilt_or_exported`,
`remote_cascades_retire_bindings_move_keys_and_clear_workspace_paths`.

## 4. Verify and rebuild

`verify` copies one committed snapshot to a private connection and checks it
without touching the live file: local workspace membership must match
placements, root keys must match the disk, agent placement must be consistent,
and replaying the whole journal into empty derived tables must reproduce every
derived table exactly. It reports each table's missing and unexpected rows,
and `ok` is true only if all of it holds.

`rebuild` performs the same replay on the live store, in one transaction, and
reports what changed. It is a recovery tool: the derived tables become exactly
what the journal says.

Tests: `clean_verify_replays_and_preserves_every_table_byte_for_byte`,
`corrupt_verify_reports_all_tables_and_rebuild_repairs_the_same_difference`,
`replay_error_returns_an_error_and_rolls_back_every_table`.

## 5. Migrations

Migrations are recorded in the store under the namespace `projects` (kept from
before the module was named entorhinal; changing it would re-run migration 1).

| Version | Adds |
|---|---|
| 1 | The journal and the project, workspace, root and alias tables. |
| 2 | A workspace root column. |
| 3 | Root bindings, retired bindings and approvals. |
| 4 | Agents and name claims. |
| 5 | The journal's `principal` column. |
| 6 | Owned-remote overrides. |
| 7 | The identity log: journal stream and entry columns, `identity_log_state`, `pending_entry`, root keys, and `workspace_root` (root paths move out of `workspace`). |
| 8 | Session bindings, claim and role caches, role takes, self identity, member names. |
| 9 | Move checkpoints and the lid-close setting; session bindings may lack a session key. |
| 10 | Consent state, the audit and hire caps. |
| 11 | Catalog registry trust, held status, publisher grants and audit. |
| 12 | Shared catalog pin sets and pins; local plans, generations and transition receipts (the recorded result of each applied plan or generation transition). |

Migration 17 adds nullable `session_binding.held_on TEXT` (null for existing
and locally created rows) and a partial index ordered by
`(agent_id, registered_at_ms DESC, ref_kind, ref)` where `ended_at_ms IS NULL`.
It records the machine retaining a moved finished session's data and supports
open-only session paging without scanning the finished backlog.

Opening a store applies every migration it lacks, once. A store whose recorded
version is ahead of the binary's highest migration is refused ("registry store
is at schema version N, ahead of this binary's highest migration M"), because
the chain is not guaranteed to be additive and a binary cannot query tables it
does not know. The fix is to run a binary at or above the store's version;
downgrading a store is not supported.

Tests: `binary_without_session_migration_refuses_migrated_store_as_ahead`,
`journal_principal_migration_keeps_legacy_cache_actor_and_projection`,
`migration_9_preserves_part1_bindings_and_session_index`,
`consent_schema_upgrades_existing_store_without_changing_the_journal`.

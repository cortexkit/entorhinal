---
title: "2026-10-03-move-agent-identity-from-prefrontal-core-to-entorhinal refire 3"
status: draft
rounds_cap: 1
refired_from: "ct_00000000-0000-4017-98db-93db545853a8"
integration_ref: "aa2eeb0c7082d1edf88b2861d916dbebee981ddd"
evidence:
  include:
    - "docs/designs/fleet-view-join-vector.md"
    - "crates/entorhinal-core/src/lib.rs"
    - "crates/entorhinal-core/src/mutations.rs"
    - "crates/entorhinal-core/src/binding.rs"
    - "crates/entorhinal-module/src/main.rs"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/agent-identity-entorhinal.md"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/consent-module.md"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/076_agent_registry.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:678-720"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_claims.rs:100-420"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:41-93"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:148-327"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:3005-3079"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_assertion.rs:1-720"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/fleet_overview.rs:1-482"
---

## acceptance_sketch
Each acceptance item below is backed by a test or a command in this repository.
Error codes, op names and wire fields are those listed under constraints, Names.
Tests run through a handler-level seam that injects the route principal,
`origin`, `actor` and a clock and returns a refusal's `code`, `message` and
`data`; no test sends `flow_id` or expects a consent, grant, hire-cap or
flow-scope code.

**Import test source.** Every import test reads a SQLite file built by applying
core's migration SQL for `076`, `079`, `080`, `082`, `109`, `112` and `126`,
copied verbatim into this repository with each source path cited beside it,
then inserting fixture rows. Project and workspace rows the binding check needs
are created in the destination entorhinal store. A fixture test fails if the
copied SQL's `agent` column list differs from the column list the import reads.

1. **The identity store holds every field the import needs, and the rules that
   protect it.** Schema covers ids, names and claims (with history and
   `name_normalization_version`), role, project and workspace binding, tag,
   labels, avatar inputs (no stored fingerprint column), GitHub identity with
   `credential_ref` and no secret column, `agent_generation` (no mutation
   trigger), `name_version`, timestamps, `terminal_reason` (`retired`/`merged`,
   CHECK refusing any other value) with `merged_into`, supervisor, create
   request key, the `agent.cutover` marker, and the journal's `principal` and
   `origin` columns; it has no persona, residence, wake, sleep, grant, cap or
   pending-consent column or table. Tests prove:
   - the one-live-head index refuses a second live head with `live_head_exists`;
   - names normalise and case-fold by core's function, tested on core's own
     pinned inputs copied verbatim; an invalid name refuses
     `invalid_agent_name`, an active collision `agent_name_taken`;
   - each illegal role shape refuses `invalid_role_shape`; a head or hire naming
     a missing project, or a workspace_head a missing workspace, refuses
     `not_found`; a head named by an alias of P is stored with P's current id;
     a create of any role whose `supervisor_agent_id` is malformed (uppercase,
     wrong length, no `agent_` prefix) refuses `invalid_agent_id`; a non-hire
     create naming a well-formed `supervisor_agent_id` refuses
     `invalid_supervisor`; a hire create naming the live head of its project
     records it, naming any other well-formed id refuses `invalid_supervisor`,
     naming none or null records NULL; a request carrying `persona_ref` or a
     residence, wake or sleep field refuses `invalid_request`;
   - rename is atomic and raises `name_version` by 1, while tag, labels,
     avatar, GitHub identity, dispose and merge leave it; after dispose, the
     same display name can be created again in that namespace; an imported head
     whose active claim is in W1 while its project is placed in W2, renamed
     after `remove(W1)`, succeeds, releases the W1 claim and claims the new name
     in W1, and the same claim history holds after `rebuild`;
   - merge into self, a terminal id or a well-formed absent id refuses
     `merge_target_not_live`, each with the source claim still active; a merge
     whose target is ill-formed refuses `invalid_agent_id`; a mutation of a
     terminal agent refuses `agent_terminal`, of an absent one
     `agent_not_found`, also when the rename's new name is taken or the merge
     target is not live;
   - two minted ids match `agent_` plus 16 lowercase hex and differ; imported
     `agent_16013c86` and a 16-hex id resolve; a never-imported 8-hex id
     resolves `unknown` and a mutation of it refuses `agent_not_found`; an
     uppercase or wrong-length id refuses `invalid_agent_id`.
2. **The import is exact, atomic, admitted once, and replayable.** It opens the
   file at `snapshot_path` read-only. Placement by migration:
   - carried: `076` (`agent`, `agent_name_claim`), `080`
     (`github_identity_json`), `082` (the `agent` rebuild; its `wake_fire`
     stays), `109` (`generation`, the column only), `112` (avatar), `126`
     (`labels_json`);
   - read as input only: `079` (`hire_identity_mapping`), for a hire's
     supervisor if it records one;
   - stays in core: `076`'s `machine_binding`, `agent_delivery`, `residence_flip`
     and activation marker, `088`, `091`, `110`, `116`, `121`, `127`, `171`,
     `177`.
   Supervisor is NULL unless `079` names one for a hire; every imported row's
   request key is NULL; `deleted` is stored as `retired`. Tests:
   - admission: before the marker, `agent.rename` from
     `reserved:prefrontal-core` refuses `authority_not_cut_over`; import from
     `Direct` refuses `direct_identity_write_not_admitted`, before and after the
     marker, also with a bad `snapshot_path`; import from
     `reserved:prefrontal-core` commits rows and the marker in one transaction,
     after which a rename is judged by the table; a zero-agent import (no
     `agent` table, with or without the other two tables) commits the marker
     and replies 0 and 0, after which an operator `agent.create` is admitted; a
     same-key rerun after a restart returns the committed counts and generation
     with the new incarnation, not `import_already_done`; a new-key import
     refuses `import_already_done` and changes nothing, also with a
     `snapshot_path` naming a missing file; before the marker, a
     `snapshot_path` naming a missing, unreadable or non-SQLite file refuses
     `invalid_request` with no marker; an import body without `snapshot_path`
     refuses `invalid_request` before and after the marker, no marker written;
   - refusals: each check refuses `import_invariant_failed` with the `data`
     object constraints, Names gives for it, pinned by a fixture per check,
     nothing written and no marker, and never a step-5a/5b code: an id outside
     `agent_` plus 8 or 16 lowercase hex (`agent_id`); an `agent` table
     present but empty with one released claim naming an absent agent, and a
     claim whose owner id is absent beside valid rows (`claim_owner`); a live
     agent whose active claim does not match re-normalisation, one with no
     active claim, and a source with `agent` but no `agent_name_claim` holding
     a live agent (`claim_mismatch`); two live heads on one project
     (`live_head`, `agent_ids` ascending); a `merged_into` absent from the
     source (`merged_into`); a live row naming a non-NULL project id that is
     not a project row in the destination store (an alias included), and a
     live workspace_head naming a workspace that is not a destination row
     (`binding`); a hire whose `079` supervisor is not a head of its project, or
     is absent from the source (`supervisor`); a source failing two checks
     reports the earlier in constraints' order; a rerun after a refusal with
     the same key then succeeds once the source is fixed;
   - successes, as a separate import: a terminal row naming a removed project, a
     terminal workspace_head naming a removed workspace, an assistant with NULL
     ids, a hire with NULL supervisor, a source without
     `hire_identity_mapping`, a hire with a live supervising head, a live hire
     whose supervisor is a terminal head of its project, a terminal hire whose
     supervisor is terminal, a head that `079` names a supervisor for (stored
     NULL), and a row with non-NULL `persona_ref`, residence, a wake policy and
     `sleep = 1` all import, hire supervisors kept, and the stored rows carry no
     persona, residence, wake or sleep value;
   - the reply's `agents_imported` and `claims_imported` equal the `agent` and
     `agent_name_claim` rows read; `agent.changes` with `limit` 1 from before
     the import returns one `agent.import` entry per agent, each with its own
     seq, none skipped, and their `claims` together equal `claims_imported`;
   - a fixture pins one value per carried field and asserts it after import and
     after `rebuild`: `agent_generation`, `name_version`,
     `name_normalization_version` (row and claim), `created_at_ms`,
     `updated_at_ms`, a merged row's non-null `terminal_at_ms`, labels (empty
     and non-empty), avatar genome with type and version absent,
     `credential_ref`, `app_slug`, `installation_id`, a released claim whose
     name can be created again, `merged_into`, tag, role, `project_id`,
     `workspace_id` on a workspace_head and on a head (including NULL),
     supervisor, `agent_16013c86` and a 16-hex id;
   - a fault injected mid-import leaves neither rows nor marker and a rerun
     succeeds; import then `rebuild` equals the imported state; project and
     workspace rows are untouched; the reply carries both counts, `incarnation`
     and `generation`.
3. **Resolves answer, before the marker as after it.** A live id resolves
   `live`, a disposed id `retired` (including one imported as `deleted`), a
   merged id `merged` with its `merged_into`, a retired id with `merged_into`
   null, an absent id `unknown`; with the store not open the call returns
   `storage_unavailable`, and with the seam dropping the `agent` table under an
   open store `storage_error`, neither as a status. Before the marker,
   `agent.resolve` of a well-formed absent id returns `unknown`, and
   `agent.snapshot` and `agent.fleet_identity` succeed with empty lists and a
   generation. The snapshot and the feed carry the same `retired`/`merged`
   values. Each is a distinct assertion.
4. **Generations can be trusted.** Every agent read and mutation success reply,
   `agent.snapshot` included, carries the fleet `(incarnation, generation)` and
   no `noop`; the project reads `resolve`, `resolve_project_id`, `enumerate`,
   `journal_tail`, `trust` and `verify` carry `incarnation` beside
   `generation`, while project mutation replies keep today's shape; a restart
   changes the incarnation on each. A create writes `agent_generation` 1; an
   imported row at 1000000 renamed in a store whose head is below 100 reads
   1000001, also in its `agent.rename` entry, and still 1000001 after
   `rebuild`. Renaming agent B advances B's `agent_generation`; renaming A or
   `approve_root` leaves B's unchanged while `generation` rises, and
   `agent.github_identity` for B shows both fields with unequal values. Across
   an identity mutation and `rebuild`, both are reproduced and a later mutation
   issues above them. A committed `agent.create`, a restart, and a same-key
   retry from `reserved:prefrontal-core` (with an unchanged body, or a changed
   body carrying no removed field) return the same agent with no new journal
   row, the commit's `generation` and the new incarnation.
5. **Admission follows R9 and the check order.** A matrix over every mutation in
   Names and the principals `reserved:prefrontal-core`, `Direct`,
   `reserved:callosum`, another reserved module, `Unverified` and a route with
   no recorded principal, before and after the marker, asserts the code and an
   unchanged journal head on every refusal:
   - `Direct` refuses `direct_identity_write_not_admitted` with a message naming
     the op and prefrontal-core's relay; every non-operator principal refuses
     `write_not_permitted`; each holds keyless, with a fresh key, and with a key
     `reserved:prefrontal-core` already committed for that op, in which case no
     cached body is returned;
   - from `reserved:prefrontal-core`: a malformed body refuses
     `invalid_request`; a same-key retry of a committed `agent.create` that adds
     `persona_ref` refuses `invalid_request`; a missing or empty key refuses
     `request_key_required`; an `agent.create` with neither `actor` nor
     `supervisor_agent_id`, and one with both null, is admitted; before the
     marker every mutation but the import refuses `authority_not_cut_over`;
     after it, creates of all four roles and every other mutation except
     `agent.import` are admitted, the import's post-marker cases being item 2's;
   - `create_project`, `create_head`, `create_hire`, `dispose_agent`,
     `agent.set_hire_cap` and `grants.list` answer `unknown_method` and write
     nothing;
   - identity reads, and the project reads `resolve`, `resolve_project_id`,
     `enumerate`, `journal_tail`, `trust` and `verify`, are admitted from every
     principal in the matrix, before and after the marker;
   - project and workspace mutations, `rebuild` included, keep today's gate:
     `Direct` and `reserved:prefrontal-core` admitted before and after the
     marker, other principals `write_not_permitted`;
   - `remove` of a bound project refuses `bound_by_live_agent`; `remove` of a
     workspace named by a live workspace_head, of one holding a project with a
     live head, and of one holding a project whose head is disposed and whose
     hire is live, refuses it and leaves placement unchanged; with an imported
     live head whose stored `workspace_id` is W1 while its project is placed in
     W2, `remove(W1)` succeeds and leaves the head unchanged and `remove(W2)`
     refuses `bound_by_live_agent`; `assign_workspace`, operator `register`
     attaching an unplaced project, `upgrade_implicit` and `seed_import` whose
     effect would place a project bound by a live head or hire refuse it,
     append no journal row, and leave placement and claims unchanged; a project
     whose agents are all terminal moves; operator `assign_workspace` to a new
     workspace id still creates it.
6. **The fleet view's identity half matches constraints, Names**, whose field
   names, invalidation rule and restart rule the shipping slice writes into
   `docs/designs/fleet-view-join-vector.md`'s "Unchanged token" section. The
   identity-half cases pass:
   - after importing one deleted, one merged and one live row,
     `agent.fleet_identity` lists only the live id, while `agent.snapshot` holds
     all three;
   - the avatar fingerprint pinned to a fixed genome, plus the type-and-version
     absent case, computed at serve time; absent optionals are omitted, not
     null;
   - the token: a call without `token`, with a token of another incarnation,
     or with a malformed token is a full reply with `unchanged: false`; a call
     with the current token answers `unchanged: true` with `agents` omitted;
     create, rename, tag, labels, avatar re-roll, GitHub identity change,
     dispose, merge, a project rename, `assign_workspace` of a project whose
     agents are all terminal, an unrelated `approve_root`, and a restart each
     invalidate it (this spec's rule, not cases the vector names);
   - `workspace` derived through the project only, so a `workspace_head` row
     omits it (`fleet_overview.rs:364-379`); for the W1/W2 head of item 5,
     `agent.fleet_identity` shows W2 while `agent.list` and `agent.snapshot`
     show the stored W1;
   - a live agent whose project has two roots appears exactly once, its
     `canonicalRoot` the first root in `canonical_root` order; an empty fleet is
     a success distinct from a failed read.
7. **Two-step create is safe to retry.** A same-key retry returns the same
   agent; a missing key refuses `request_key_required`; a key journaled under
   another op refuses `request_key_reused_across_ops`. `live_head_exists`
   carries in `data` the occupier's `agent_id` and `request_key`, with an
   explicit null for an imported head.
8. **The journal records who wrote each identity row.** After an admitted
   `reserved:prefrontal-core` agent write carrying `actor`, the journal row's
   `principal` reads `reserved:prefrontal-core`, and `origin` (explicit NULL
   when absent) and `actor` read back as sent; with `actor` absent or null the
   row records `module`. A project mutation from `Direct` records `principal`
   `direct`. A `register` journaled with NULL `principal` and key K, retried
   with K, returns the cached reply.
9. **The change feed is complete and ordered.** Tests prove:
   - a consumer using only serialized snapshot and change replies reproduces
     entorhinal's agent and claim tables, claim history predating the snapshot
     included, with mutations committed concurrently while it pages through
     `agent.changes` and none skipped at the snapshot boundary;
   - an entry of every op in the closed list, and the row and claim objects,
     decode with their listed fields, every field this spec adds present (null
     when not set by that op); a decoder test fails if one is missing or
     renamed; project-op and marker entries are skipped and the reply cursor
     advances past them, to the head when they are trailing;
   - two consecutive identity entries read with `limit` 1 come back one per
     call, the first reply's cursor being the first entry's seq;
   - a `wait` call whose only entries above the cursor are non-identity returns
     at once with no entries and the cursor at the head;
   - `limit` omitted is 500, outside 1..=1000 refuses `invalid_request`;
   - `journal_tail` returns project-op rows only; with project rows at seq 1
     and 3, non-project rows at 2 and 4, and `limit` 1, a caller following the
     advance rule consumes rows 1 and 3 exactly once and ends at 4;
   - another incarnation or a cursor above the head answers `snapshot_required`
     with no entries; a gap returns the missing entries in order;
   - an idle poll with `wait` stays open until an entry commits or 25 s on the
     injected clock pass, holds no store lock (a resolve during it succeeds),
     and on timeout returns success with the current incarnation, empty
     `entries` and the request cursor; an identity mutation committed during it
     is returned by that poll or the next with none skipped;
   - replay reproduces minted ids rather than generating new ones.
10. **The manifest is the authority surface only.** The manifest test pins: the
    management surface holds every operation listed today plus every Names read
    and mutation; `provides` is exactly `project-identity/v1` and
    `agent-identity/v1`; `requires` is empty; there is no tool surface; the
    subc-protocol and subc-client-rs pins are those this campaign started from
    (R11).
11. **Rebuild loses no state.** After import and later mutations, `rebuild`
    leaves agent rows, claims, `agent_generation` values and the marker equal.
    Each case below compares `agent.fleet_identity` rows, and the project,
    root, alias, placement and workspace tables, before and after `rebuild`
    (the token changes only through the restart's incarnation). Placement-
    changing ops run first and a live head is then bound to the resulting
    project, by `agent.create` or by the import's source; the two rename-only
    cases (marked) run with the head already bound:
    - a project created in W by `upgrade_implicit` keeps its placement;
    - (rename-only) an existing project renamed by a later `upgrade_implicit`
      without a placement change keeps the new name;
    - a project P renamed by `register` naming P's existing root and no
      `projectId` keeps its id and new name, with no extra project;
    - a project assigned by `assign_workspace` to a new workspace with
      `workspaceName` "Team" keeps that name;
    - (rename-only) a workspace renamed by a later `seed_import` keeps the new
      name;
    - a project seeded by `seed_import` from a pair's `name` with no
      `payload.names` entry keeps that name;
    - a project P that one `seed_import` claims in W1 and W2 (`multi_workspace`)
      stays unplaced, its head showing no `workspace` before and after
      `rebuild`;
    - a `seed_import` identity reported `alias_occupied` creates no project
      after `rebuild` either.
12. **Wire bodies carry exactly the listed edits.** For every op core serves
    today except `agent.resolve`: removed fields are absent and a request
    carrying one refuses `invalid_request`; `agent_generation` replaces
    `generation`; `incarnation` and fleet `generation` are present on success
    replies; `request_key` is required and `actor` optional on mutations; no
    reply carries `noop`; `agent.github_identity` carries `app_slug`,
    `credential_ref`, `installation_id`, `agent_generation` and no secret. New
    ops' fields (`agent.resolve`'s five-field reply, `agent.import`,
    `agent.snapshot`, `agent.changes`, `agent.fleet_identity`) are pinned by
    fixtures written here from constraints, Names. The remaining field names
    of core's bodies come from the quotation listed in open_questions, which
    the slice starts from; this item asserts the edits, not core's bytes.
13. **Rollback is honest.** A binary whose migration chain lacks the agent
    migrations refuses a migrated store as ahead of it
    (`crates/entorhinal-core/src/lib.rs:146-156`).

## constraints
This campaign moves the authority only (R7). Scoped tools, `ScopeAttributes`
and `flow_id` reading, consent, standing grants, the hire cap, a supervisor
retiring its hire, and `reserved:callosum` admission are deferred (R8, R12; the
list is under non_goals). Nothing below specifies them; "Keeps for the later
campaign" names the only constraints that exist so this campaign does not rule
them out. Line citations into this repository are as of 3e06dbd37b62.

**Ownership.** Entorhinal takes agent ids, names and their normalisation, name
claims, role, project and workspace binding, the one-live-head rule, tags,
labels, avatar, GitHub identity, the per-agent generation, disposal, merge, and
the supervisor of every hire. Core keeps residence, delivery, wake routing,
board state, personas and scope registration, and stamps an entorhinal-issued
`agent_id` on the scopes it registers. Core also keeps the GitHub pending-mint
ceremony (`github_identity.mint_begin`/`mint_complete`, its
`github_identity_pending_mint` table, migration 088) and completes a binding by
calling `agent.set_github_identity` on the `reserved:prefrontal-core` row.

**Names.** Entorhinal declares `agent-identity/v1` beside `project-identity/v1`
(`crates/entorhinal-module/src/main.rs:849,992-996`); `requires` stays empty.
Agent ops carry an `agent.` prefix so they never collide with project ops
(`resolve`, `enumerate`, `remove`, `journal_tail`). Agent op bodies are
snake_case, as core's are; project ops keep camelCase; `agent.fleet_identity`
uses the join vector's camelCase.
- Reads: `agent.resolve`, `agent.resolve_name`, `agent.list`,
  `agent.peer_roster`, `agent.avatar_read`, `agent.github_identity`,
  `agent.fleet_identity`, `agent.snapshot`, `agent.changes`.
- Mutations: `agent.create`, `agent.rename`, `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose`, `agent.merge`, `agent.import`. Each requires a non-empty
  `request_key` and accepts an optional `actor`. Project ops keep today's
  optional `requestKey`.
- Manifest: the management surface keeps every operation it lists today
  (`main.rs:855-962`) and adds exactly the reads and mutations above. There is
  no tool surface. Any other method name, including `create_project`,
  `create_head`, `create_hire`, `dispose_agent`, `agent.set_hire_cap`,
  `grants.*` and `agent.fleet_overview`, answers today's `unknown_method`
  (`main.rs:344-350`) and writes nothing.
- Wire bodies of ops core serves today, `agent.resolve` excepted: core's
  current request and reply body for the op of the same name with exactly these
  edits: (a) residence, wake, delivery-derived (`bounced_deliveries`), sleep and
  persona (`persona_ref`) fields removed; a request carrying any removed field
  refuses `invalid_request`, and core's relay strips them; (b) core's per-agent
  `generation` renamed `agent_generation`; (c) `incarnation` and fleet
  `generation` added, required, to every success reply, so a relay passes them
  through (R5); (d) `request_key` added to every mutation request; (e) optional
  `supervisor_agent_id` on `agent.create`; (f) optional `actor` on every
  mutation request. Core's bodies are not yet quoted (open_questions); until
  they are, these edits are pinned structurally (acceptance 12). Agent replies,
  cached ones included, never carry `noop`: the shared mutation path inserts it
  into every object reply (`crates/entorhinal-core/src/mutations.rs:343-347`)
  and agent handlers do not apply that insertion. `agent.github_identity`
  carries `app_slug`, `credential_ref`, `installation_id`, `agent_generation`
  and no secret. Avatar is stored as genome, type and version; the fingerprint
  is computed at serve time (`fleet_overview.rs:96-109`).
- Presence, replies: fields core already serves keep core's rule; every field
  this spec adds to a snake_case success reply, `agent.snapshot` row, claim or
  change entry is always present and JSON null when empty;
  `agent.fleet_identity` omits absent optionals (as `fleet_overview.rs:53-83`
  does) and omits `agents` when unchanged.
- Presence, requests: optional request fields may be omitted, and JSON null
  means the same as omission: `actor` absent or null records `module`;
  `supervisor_agent_id` absent or null means none; `agent.changes` `limit`
  defaults to 500 and `wait` to true; `agent.fleet_identity` `token` absent is
  a full reply. Only a removed field or a malformed value refuses
  `invalid_request`.
- New ops:
  - `agent.import` `{snapshot_path, request_key, actor?}`: `snapshot_path` is a
    required string; a body without it is malformed (`invalid_request` at step
    2). The path names a copy of core's store, opened SQLite read-only; a path
    naming a file that is missing, unreadable or not SQLite refuses
    `invalid_request` naming the path at step 5c, nothing written. Reply
    `{agents_imported, claims_imported, incarnation, generation}`, the counts
    being the `agent` and `agent_name_claim` rows read (0 and 0 for a
    zero-agent source).
  - `agent.snapshot` `{}`: one unpaged reply `{incarnation, generation, agents,
    claims}`, `generation` read in the same transaction and usable as the first
    `agent.changes` cursor.
  - `agent.changes` `{incarnation, cursor, limit?, wait?}`: `limit` outside
    1..=1000 refuses `invalid_request`. Reply `{incarnation, generation,
    cursor, entries}`.
  - `agent.fleet_identity` `{token?}`: reply `{incarnation, generation, token,
    unchanged, agents?}`. `token` is the fleet pair as an opaque string. A call
    whose token equals the current pair answers `unchanged: true` with `agents`
    omitted; no token, a token of another incarnation or generation, or a
    malformed token is a full reply with `unchanged: false`, never a refusal.
    These names are this spec's contract. The join vector asks for "the shape
    `session.transcript_page` already uses"
    (`docs/designs/fleet-view-join-vector.md:211-215`) without naming fields;
    the slice that ships this op amends that section to name these fields,
    this spec's invalidation rule and the restart rule (Generations), so client
    and server decode one contract.
    Lists non-terminal agents only (as `fleet_overview.rs:254-266` does), one
    row per agent whatever the number of project roots,
    `project.canonicalRoot` the project's first root in `canonical_root` order,
    `workspace` derived through the project's placement only (a workspace_head
    omits it, `fleet_overview.rs:364-379`), and omits residence, board,
    `pendingAskCount` and `latestActivityMs`. This is the only read that
    derives workspace through placement; `agent.list`, `agent.snapshot`,
    change entries and the stored column carry the row's stored `workspace_id`.
- Agent row (an `agent.snapshot` element and a change entry's `row`): the
  `agent.list` element with the edits above, plus whichever of these it lacks:
  avatar genome, type and version, the full GitHub identity with
  `credential_ref`, `name_version`, `name_normalization_version`,
  `updated_at_ms`; and always `status` (`live|retired|merged`), `merged_into`,
  `supervisor_agent_id`, `request_key`, `created_at_ms`, `terminal_at_ms`,
  `agent_generation`. Claim: `{claim_id, agent_id, namespace_kind,
  namespace_key, normalized_name, name_normalization_version, display_name,
  claimed_at_ms, released_at_ms}` (core's `076_agent_registry.sql:76-91`, per
  the panel; the version is part of the active-claim key). A decoder test pins
  both.
- Resolve reply (new shape): `{agent_id, status, merged_into, incarnation,
  generation}`, `status` one of `live`, `retired`, `merged`, `unknown`,
  `merged_into` null unless merged. Its request body is quoted from core
  (open_questions). Storage failures keep the existing handler errors
  (`main.rs:271-290`): `storage_unavailable` when the store is not open,
  `storage_error` when a read on an open store fails; neither is a status.
- Change entry: `{seq, op, agent_id, agent_generation, row, claims,
  old_display_name, new_display_name, status, merged_into}`. The fleet pair is
  on the reply, not the entry; `seq` is the entry's own journal seq and its
  cursor position. `row` is the agent's row after the entry and `claims` the
  claim rows it inserted or released, so a consumer rebuilds the agent and claim
  tables without generating any value. The last four are on every entry, null
  unless the op sets them. `op` is one of: `agent.create`, `agent.rename`
  (sets `old_display_name`, `new_display_name`), `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose` (sets `status: "retired"`), `agent.merge` (sets `status:
  "merged"`, `merged_into`), `agent.import` (one per imported agent: its row and
  every claim, terminal rows included). Every other journal row (project ops,
  the `agent.cutover` marker) is non-identity.
- Error codes this campaign adds: `authority_not_cut_over`,
  `direct_identity_write_not_admitted`, `request_key_required`,
  `invalid_agent_name`, `invalid_agent_id`, `invalid_role_shape`,
  `agent_name_taken`, `agent_not_found`, `agent_terminal`,
  `invalid_supervisor`, `merge_target_not_live`, `live_head_exists`,
  `bound_by_live_agent`, `import_already_done`, `import_invariant_failed`,
  `snapshot_required`. Existing codes are reused unchanged: `invalid_request`
  (`main.rs:304-311`), `write_not_permitted`, `request_key_reused_across_ops`
  (`mutations.rs:320-336`), `not_found` (`mutations.rs:466`, naming the missing
  project or workspace id), `entropy_unavailable` (`binding.rs:80-84`),
  `encode_failed`, `unknown_method`.
- Refusal data. Today a refusal carries only `code` and `message`
  (`main.rs:776-780`). The handler error gains an optional `data` object, which
  the test seam returns and refusal fixtures pin. All values are strings or
  JSON null; every key listed is always present:
  - `live_head_exists`: `{agent_id, request_key}`, the occupying head;
    `request_key` null for an imported head.
  - `import_invariant_failed`: `{check, ...}` by check:
    `agent_id` → `{check, agent_id}` (the bad id);
    `claim_mismatch` → `{check, agent_id}`;
    `claim_owner` → `{check, claim_id, agent_id}` (the claim and the absent
    owner id it names);
    `live_head` → `{check, project_id, agent_ids}`, `agent_ids` an array of
    every live head id of that project in ascending byte order;
    `merged_into` → `{check, agent_id, merged_into}` (the source row and the
    missing target);
    `binding` → `{check, agent_id, project_id, workspace_id}`, the id that
    failed set and the other null;
    `supervisor` → `{check, agent_id, supervisor_agent_id}` (the hire and its
    `079` supervisor).
  How `data` is serialised onto the wire depends on the refusal envelope the
  SDK offers (open_questions); it is never only prose in `message`.

**Two rules that predate this work.** The daemon never depends on entorhinal,
because it supervises it. Paths compare with the daemon's canonical path
function and are never re-normalised on entorhinal's side. Any change touching
either goes to SUBC for review before it ships.

**Versions.** This campaign starts from subc-protocol 0.29.0 and subc-client-rs
0.26.1, as shipped in entorhinal 0.1.14 (R11), and does not change either pin.
The evidence commit 3e06dbd37b62 still pins 0.28.1 and 0.25.2
(`Cargo.lock:639-686`), so every slice starts from a tree that contains
192f220.

**Agent ids are minted the opposite way to project ids.** Project ids are
deterministic (`pj-` plus a BLAKE3 hash of name and roots) and have no permanent
tombstone. Agent ids are random, never derived from name or path, and
permanently tombstoned, because a reused id would inherit the previous holder's
grants, bot ownership and approvals. New ids are `agent_` plus 16 lowercase hex
(8 random bytes from the store's id source, `binding.rs:80`), minted in the
create transaction. Ids move unchanged in the import. Validation is syntactic:
`agent_` plus 16 or 8 lowercase hex is well-formed (only imported rows have the
8-hex form, e.g. `agent_16013c86`); anything else refuses `invalid_agent_id`,
and the import refuses a source id of any other form (check `agent_id`). A
well-formed id with no row resolves `unknown`, and a mutation naming it refuses
`agent_not_found`.

**Terminal state, one schema.** Column `terminal_reason` is NULL for a live
agent, else `retired` or `merged` (CHECK IN those two; core's `deleted` is not
stored); `merged_into` is non-NULL exactly when `terminal_reason = 'merged'`.
Wire `status` is `live` when `terminal_reason` is NULL, otherwise its value. The
import maps core's `deleted` → `retired` before insert. A retired or merged id
stays resolvable with its status: core keeps tables referencing agent ids with
no database constraint (`session_scope.agent_id`,
`flow_install.author_agent_id`, `flow_install_grant.agent_id`,
`gh_route_speech_receipt`, `persona_active_revision` keyed
`agent:<agent_id>`), so a resolve never collapses retired into unknown.

**Identity invariants carried over verbatim.** `agent_id` is stable for life.
Names are NFC for display and case-folded for lookup by core's own function
(quoted once gathered, not re-derived); an invalid name refuses
`invalid_agent_name`, an active collision `agent_name_taken`. Rename claims the
new name in the namespace of the agent's active claim, not one recomputed from
current placement (`agent_claims.rs:248-254` in the evidence), releases the old
claim atomically, and sets `name_version` to its previous value plus 1
(`agent_claims.rs:262-266` in the evidence); this holds when that namespace's
workspace has since been removed. No other mutation changes `name_version`.
One active claim per namespace and per agent; released claims keep history.
Dispose and merge set the terminal row and `terminal_at_ms` and release the
active claim in one transaction; a mutation of a terminal agent refuses
`agent_terminal`; merge into self or into a terminal or absent target refuses
`merge_target_not_live` before anything is released
(`agent_claims.rs:289-339` in the evidence). At most one live head per project,
schema-enforced as an index on `(project_id) WHERE role = 'head' AND
terminal_reason IS NULL`, as core's `uq_live_head`; a violation surfaces as
`live_head_exists`, never `storage_error`.

**Create shape checks, in the handler.** As core does
(`agent_registry_ops.rs:3038-3049` in the evidence), a create refuses
`invalid_role_shape` unless: assistant has `project_id` and `workspace_id`
NULL; workspace_head has `project_id` NULL and `workspace_id` set; head and
hiree have `project_id` set and no `workspace_id` in the request. A named
`project_id` resolves through one alias hop, as project `resolve` does
(`crates/entorhinal-core/src/lib.rs:296`), and the current id is stored; an id
resolving to no current project, or a workspace_head's `workspace_id` that is
not a current workspace, refuses `not_found`. Head and hiree rows store
`workspace_id` as core does: a new create writes the resolved namespace key
(`agent_claims.rs:390-395` in the evidence), an imported row keeps its value,
NULL included; no CHECK forces NULL or NOT NULL for those roles. Each role's
namespace, including a head or hire on an unplaced project, follows core's
`namespace_for_create` once quoted.

**Supervisor.** A `supervisor_agent_id` that is not `agent_` plus 16 or 8
lowercase hex refuses `invalid_agent_id` for any role (step 5a), before the
rule below runs. For a well-formed id: a hire create records it only when it is
the live head of the hire's project, else `invalid_supervisor`; a create of any
other role naming one refuses `invalid_supervisor`. A hire naming none records
NULL; head, assistant and workspace_head rows have NULL supervisor. The import
copies a `079` supervisor onto hire rows only, under its check; any other
imported row stores NULL even when `079` names a supervisor for it, and is not
refused for it. The supervisor never changes after create.

**No personas, residence, wake or sleep.** The `agent` table has no
`persona_ref`, `residence_*`, `wake_policy_json`, `wake_policy_version` or
`sleep` column (core `076_agent_registry.sql:9-18`). The import drops those
values, `persona_ref` included, whatever they hold, and still commits; core
copies persona data out before dropping its identity tables (dependency).

**Generations.** Two values, never sharing a name:
- **`agent_generation`**, core's per-agent `agent.generation` (migration 109),
  signed as `binding_generation` (`agent_assertion.rs:352`). Only 109's column
  is carried; its `agent_generation_on_mutation` trigger is not installed. A
  create writes 1; each later mutation of that agent's row writes its previous
  value plus 1, once, in the handler; the import carries core's value. The
  written value is in the journal entry and replay assigns it rather than
  incrementing. Mutations of other agents, projects or workspaces do not change
  it. Core's bot token embeds it with the incarnation.
- **Fleet `(incarnation, generation)`**, `generation` the journal head
  `MAX(seq)`. It versions reads, the feed, relay replies and the fleet view's
  token. Because the token is the fleet pair, any journal append invalidates
  it, project ops included (a project rename or workspace move is never
  missed; an extra invalidation from an unrelated op is accepted). This rule is
  this spec's, not the join vector's.
The **incarnation** is a random id minted at process start and never persisted
(not in a column, journal payload, cached reply or the marker), because
restoring an older backup lowers both generations. Per R7 it is attached at
serve time to every reply that carries a generation: every agent-op success
reply, and the project reads `resolve`, `resolve_project_id`, `enumerate`,
`journal_tail` (`crates/entorhinal-core/src/lib.rs:436-468`), `trust`
(`main.rs:732`, a `ResolveReply`) and `verify` (`mutations.rs:201-206`), as a
required camelCase `incarnation` beside their `generation`. Project mutation
replies keep today's shape. Every restart invalidates outstanding tokens and
cursors; the join-vector amendment (Names, `agent.fleet_identity`) states it.
A same-key retry served from the cache, the import's included, returns the
`generation` recorded at commit (`mutations.rs:338-347`) with the current
incarnation.

**The journal records the attested principal.** `registry_journal` gains
`principal` and `origin`, and keeps `actor`
(`crates/entorhinal-core/src/lib.rs:41-49`). `principal` is the principal
recorded for the route at bind (`main.rs:180-183, 362-366`) written as
`principal_label` spells it (`main.rs:242-249`): `direct`,
`reserved:<module_id>` or `unverified`; NULL only on rows journaled before the
migration. `origin` is Callosum's `origin` when the handler receives one, else
NULL (its production source in the SDK is an open question). `actor` is the
request's `actor` field, defaulting to `module` as today (`mutations.rs:390`).
No request digest is stored in this campaign.

**Journal op and request key.** A row's `op` is the request's management op.
Agent mutations use today's body-insensitive cache: a same-op, same-key call
returns the stored reply (`mutations.rs:312-314`); a key journaled under another
op refuses `request_key_reused_across_ops` (`mutations.rs:320-336`). Rows
journaled before this migration (NULL `principal`) behave as today. The import
journals one `agent.import` row per imported agent (request key NULL), then the
`agent.cutover` marker row carrying the import's request key and cached reply
(counts and generation), all in one transaction; the cache and cross-op check
treat the marker row as `agent.import`'s, so a zero-agent import is cached too.
A refused call writes no row, so a rerun may reuse its key.

**Credential references, never credentials.** `credential_ref` points into
claustrum. No secret moves into entorhinal.

**Admission and check order for agent mutations (R9).** Each step runs only if
the previous passed; the first refusal wins, writes no journal row and returns
no cached body. A check that does not apply to the op is skipped, not failed.
(0) Envelope decode as today (`main.rs:304-311`): not JSON with `method` and
`params` → `invalid_request`.
(1) Principal, before and after the marker: `reserved:prefrontal-core`
continues; `Direct` → `direct_identity_write_not_admitted`, the message naming
the refused op and saying to call it through prefrontal-core's relay (any local
process, an agent's shell included, reaches entorhinal as `Direct`); every
other principal (`reserved:callosum`, other reserved modules, `Unverified`, a
route with no recorded principal) → `write_not_permitted`. This holds for a key
already committed and for any body, a bad `snapshot_path` included: a
non-operator call gets its principal's code, never the cached reply.
The handler reads no `flow_id` or `ScopeAttributes`.
(2) Params decode: a malformed body, a missing required field (an import
without `snapshot_path` included) or a removed field → `invalid_request`, also
when the key was already committed.
(3) Request key: missing or empty → `request_key_required`; otherwise the
journal cache above, the marker looked up as `agent.import`. A same-key import
rerun is served here, also after a restart, never `import_already_done`.
(4) Marker: R9 refuses every agent-identity mutation before the marker; the one
exception is `agent.import`, because R7's import is what writes the marker.
Absent → `authority_not_cut_over` for every other mutation; present and op
`agent.import` → `import_already_done`, nothing written, whatever its
`snapshot_path`.
(5a) `agent.create`: role shape, name validity, project and workspace
resolution (`not_found`), `supervisor_agent_id` syntax (`invalid_agent_id`)
and supervisor (`invalid_supervisor`), name collision in the resolved
namespace (`agent_name_taken`), one live head (`live_head_exists`).
(5b) Every mutation naming an existing agent: id syntax (`invalid_agent_id`),
existence (`agent_not_found`), terminal state (`agent_terminal`), then the
op's own checks: rename, name validity then collision in the active claim's
namespace; merge, target syntax (`invalid_agent_id`) then target self,
terminal or absent (`merge_target_not_live`). So a rename of an absent or
terminal agent to a taken name refuses `agent_not_found` or `agent_terminal`;
a merge of an absent source refuses `agent_not_found` and of a terminal source
`agent_terminal`, whatever the target.
(5c) `agent.import`: the `snapshot_path` file refusal, then the import
invariants below, refused only as `import_invariant_failed`.
Project and workspace operations, `rebuild` included, do not use this order and
keep today's gate (`main.rs:225-240`): a project or workspace mutation is
admitted from `Direct` or `reserved:prefrontal-core`, before and after the
marker, anything else `write_not_permitted`; project reads (`resolve`,
`resolve_project_id`, `enumerate`, `journal_tail`, `trust`, `verify`) are
admitted from every principal, a route with no recorded principal included
(`main.rs:226-227`).

**Reads.** Identity reads are admitted for every principal, as queries are today
(`main.rs:225-228`), and answer before the marker as after it; before the
marker, a resolve of a well-formed absent id returns `unknown`, never
`authority_not_cut_over`, and an empty store is a success (R10).

**The import.** It reads `agent`, `agent_name_claim` and `hire_identity_mapping`
from the source. One definition by table presence: no `agent` table is a
zero-agent source whatever else exists, and both counts are 0; `agent` present
with no `agent_name_claim` reads as zero claims, so any live agent fails
`claim_mismatch`; no `hire_identity_mapping` means every supervisor is NULL.
Every imported row stores request key NULL and copies `created_at_ms`,
`updated_at_ms`, `terminal_at_ms`, `name_version` and
`name_normalization_version` unchanged (core `076_agent_registry.sql:10,19-22,
29`). Before committing it runs these checks in this order, else refuses
`import_invariant_failed` with nothing written, reporting the first failing
check and, within it, the offending row with the smallest `agent_id` (or
`claim_id` for `claim_owner`):
- `agent_id`: every `agent_id` is `agent_` plus 8 or 16 lowercase hex;
- `claim_owner`: every `agent_name_claim` row, active or released, names an
  `agent_id` present in the source `agent` table, so every counted claim is
  carried by its owner's `agent.import` entry; an `agent` table present but
  empty with any claim row fails here, never reads as a zero-agent source;
- `claim_mismatch`: each live agent has exactly one active claim matching
  re-normalisation of its display name, also failing for zero;
- `live_head`: at most one live head per project;
- `merged_into`: every `merged_into` names a source row;
- `binding`: every non-terminal row's non-NULL `project_id` is a row of the
  destination entorhinal store's `project` table (an alias does not satisfy it)
  and every non-terminal workspace_head's `workspace_id` a row of its
  `workspace` table; the source has no project or workspace tables to consult;
- `supervisor`: a hire's non-NULL `079` supervisor names a source row whose
  role is head and whose `project_id` equals the hire's, live or terminal.
Terminal rows import as they are, because `remove` legitimately leaves such
rows. A successful import, from a zero-agent source too, commits its rows and
the marker; the marker, not the row count, ends `authority_not_cut_over`.
`rebuild` keeps today's gate and replays every agent op and the marker.

**Project lifecycle with agents bound.** A non-terminal agent binds a project
when its `project_id` names it; it binds a workspace when it is a workspace_head
whose `workspace_id` names it, or its project, of any role, is placed there (a
head's or hire's stored `workspace_id` alone binds nothing). `remove` of a bound
project or workspace refuses `bound_by_live_agent`, including a workspace
holding a project whose only live agent is a hire (`remove` of a workspace
deletes its `project_workspace` rows, `mutations.rs:636`), and a project
merge-by-alias that would put two heads on one project. Any op whose effect
changes a bound project's placement refuses `bound_by_live_agent` and writes
nothing, the whole call refused: `assign_workspace`, `register` attaching an
unplaced project (`mutations.rs:415,430`), `upgrade_implicit`, `seed_import`. A
project whose agents are all terminal moves. Refused ops are not journaled.
Operator `register`, `assign_workspace` (`mutations.rs:495`),
`upgrade_implicit` and `seed_import` keep creating workspace rows as today.

**Replay reproduces the live ops.** `agent.fleet_identity` derives project
name, workspace and workspace name from the replayed project tables, and
`rebuild` must never change a placement that every live op would refuse to
change; so these replay defects are fixed. Replay applies the live rule against
the replayed state, which also covers journal rows written before the fix:
- `upgrade_implicit`: the live op writes `workspace`, `project_workspace` and
  `workspace_member` rows when `workspaceId` is set, and renames an existing
  project (`mutations.rs:628`); the replay arm writes no workspace rows and
  inserts the project with `INSERT OR IGNORE` (`mutations.rs:851-863`), losing
  the rename; it must write both exactly when the live op did;
- `register`: replay mints the id from name and roots (`mutations.rs:779-781`)
  while the live op without `projectId` takes the single existing owner of its
  roots (`mutations.rs:396-398`), so a root-inferred rename replays as a new
  project; replay must take the existing owner;
- `assign_workspace`: the live op names a new workspace from `workspaceName`
  (`mutations.rs:495`); replay names it by its id (`mutations.rs:814`) and must
  use the journaled name;
- `seed_import`: the live op (`mutations.rs:666-760`) applies per-root identity
  precedence (`mutations.rs:734`), skips an identity whose minted id is an
  occupied alias or is conflicted (`alias_occupied`, no project created,
  `mutations.rs:744-747`), places a project only when exactly one workspace
  claims it (else `multi_workspace`, left unplaced), renames an existing
  workspace (`mutations.rs:739`), and names a project from `payload.names`,
  then the pair's `name`, then the root's basename (`mutations.rs:747`). The
  replay arm (`mutations.rs:864-882`) inserts every identity group and every
  root, places each project in every claiming workspace by `INSERT OR IGNORE`,
  keeps old workspace names, and falls back from `payload.names` straight to
  the identity. Replay must apply each of these live rules, so the projects,
  roots, placements and names after `rebuild` equal the live result.

**Two-step create.** Identity is minted first, residence bound second by core.
An interrupted create leaves a dormant agent, a state that already exists, is
rendered and has an operator action. `agent.create` is idempotent by its request
key, recorded on the row as an opaque correlation token. `live_head_exists`
names the occupying head's agent id and request key, or explicit null for an
imported head; entorhinal cannot see residence, so it carries no dormant bit.
No automatic sweep.

**An identity change feed that consumers pull.** Core resolves agents on almost
every delivery, so it keeps a local replica of entorhinal's agent and claim
tables, all rows, fed from entorhinal; an entorhinal restart must not stop peer
messages or room posts.
- `agent.changes` returns identity entries with seq above the cursor, in
  journal order, up to `limit`. When the page is full, the reply cursor is the
  seq of the last returned entry, even if the scan looked further; otherwise
  it is the journal head at scan time. A scan finding only non-identity
  entries returns at once with empty `entries` and the cursor at the head. With
  `wait` and no entry above the cursor, it holds the call until an entry
  commits or 25 s pass, re-reading under a short lock and holding none between
  reads (today every op runs under the store mutex, `main.rs:271-290`). A
  timeout is a success: current incarnation, empty `entries`, the request
  cursor.
- `agent.snapshot` returns every agent row (terminal included) and every claim
  (active and released) with the pair read in the same transaction.
- An `incarnation` that is not current, or a cursor above the head, answers
  `snapshot_required` with no entries; a same-incarnation gap is catch-up.
- Identity consumers use only this feed. `journal_tail`
  (`crates/entorhinal-core/src/lib.rs:436-468`) returns project-op rows only,
  `generation` read in the same transaction; a reply with fewer rows than
  `limit` advances the caller to `generation`, a full one to its last row's seq.
The feed is pulled: entorhinal opens no route to consumers and gains no
dependency on core. Merge is a retirement for every purpose outside entorhinal.
Authority checks that must be fresh (the bot-token mint) read entorhinal
directly and fail closed.

**Nothing precludes mirroring identity later.** Replay never re-mints: minted
ids and every generated value are in the journal entry. The record holds no
machine-local value; agents reference projects by `project_id`. Each process
mints its own incarnation.

**Keeps for the later campaign (R8).** Journal `op` stays unconstrained text and
replay skips an op it does not recognise (`mutations.rs:938`), so a later grant
or supervisor-retire entry needs no migration of identity rows; `agent_id` stays
a stable text key a later grant row can reference; admission is one principal
match that a later release can extend with a scoped-agent row; `requires` stays
empty. No grant, cap or pending-consent table is created here.

**Clean cutover of authority.** One import copies the registry from core's
store; in the same cut the store and the authority move to entorhinal. Core's
identity write ops become relays under `reserved:prefrontal-core` with core's
caller check unchanged (R4); `agent.create` becomes identity-first then
residence. Core's identity read ops that running host plugins call keep serving
from its replica until a bundle without them is running. An install whose core
store never applied migration 076 obtains the marker by one zero-agent import;
entorhinal cannot tell that from a premature one, so core's trigger excludes a
store that once held identity tables.

### Dependencies (other owners; none is an acceptance item here)

- **ALF, core's half:**
  - residence moves to its own table under today's all-or-nothing group CHECK;
    persona data is copied out before identity tables are dropped;
  - `agent.create` becomes identity-first then residence, sending the identity
    step under `reserved:prefrontal-core` only for a caller core's
    primary-operator check admits;
  - core's identity write ops (dispose, merge, rename, tag, labels, avatar,
    GitHub identity) become relays, plus an import relay, with the
    primary-operator check unchanged. Every relayed write, `mint_complete`'s
    included, sends a `request_key`, strips removed fields and renames
    `generation` to `agent_generation`. Relay contract (R5): refusals pass
    through verbatim, never wrapped; the reply carries the new
    `(incarnation, generation)`; the replica may lag a same-turn read by one
    poll, and the generation lets a caller tell;
  - on first start with the marker absent, core calls the import relay with a
    zero-agent source only if its ledger shows 076 never applied;
  - the bot-token mint reads claims and `agent_generation` from entorhinal,
    embeds the incarnation, and refuses when entorhinal cannot answer;
  - `agent.fleet_overview` is deleted; core serves its half keyed by
    `agent_id`, with `pendingAskCount` attributed by agent;
  - the seven operator scripts call core's relay ops, with
    `script/disable-nodark-outside-cortexkit.ts` (raw SQL on `agent`) as its
    own item;
  - the replica acts on `agent.dispose` and `agent.merge` exactly as
    terminalisation does today (`agent_claims.rs:343-365`): bounce queued
    deliveries on dispose but not merge, supersede undelivered wake fires on
    both, revoke authored flows, remove scopes;
  - core confirms the named-session mint check stays sound once its two reads
    are in different stores.
- **CKIOS, with the TUI and desktop owners:** the two-call fleet view per the
  join vector, uploaded before the cut.
- **CALLO:** expose entorhinal's identity reads to the phone profile.

### Cutover order

1. Entorhinal ships, on its current pins, the identity store, reads, operator
   mutations and import while core is authoritative, placed with `--migrates`
   so a backup is taken before first open. The store is empty, the marker
   absent, reads answer, and agent mutations other than the import refuse
   `authority_not_cut_over`. A binary without the agent migrations refuses the
   migrated store as ahead of it (`crates/entorhinal-core/src/lib.rs:146-156`),
   so rolling back means restoring that backup.
2. Callosum exposes identity reads to the phone; the phone build with the
   two-call fleet view is uploaded against the vector.
3. **The cut, in one restart window:** stop core and copy its store; place
   core's build with the replica, without its identity tables (its ledger keeps
   076 applied), with its identity write ops turned into relays; the operator
   calls core's import relay with the copy's path; until the import commits the
   marker every relayed write refuses `authority_not_cut_over`; release the
   phone build. A refused import leaves no rows and no marker: on
   `import_invariant_failed` the operator repairs the copy by SQL from the
   reply's `check` and ids and reruns, or restores the step-1 backup and the
   previous core build.
4. After the cut: compare the fleet view against the vector, check a bot-token
   mint end to end, confirm a retirement reaches core through the feed, and
   measure how long a new agent shows `unknown`, writing that bound into the
   join vector.
5. The agent-facing surface follows in the second campaign (non_goals).

## intent
Agent identity moves out of prefrontal-core and into entorhinal, the project
registry. Identity is fleet identity: wernicke posts as an agent's GitHub bot,
plexus checks per-agent grants, basal flows act as their owner agent, the phone
lists agents, and thalamus runs Claude Code heads. None of those should have to
ask the orchestrator who an agent is, and replacing prefrontal with a different
orchestrator must lose nothing in the registry.

Moving the issuer of agent ids must land before CortexKit is public. After that,
agent ids live in other people's stores and third-party grants, so changing who
issues them would need a migration on every install instead of one import on
this machine.

**This campaign moves the authority only.** It delivers the identity store, the
import, the identity reads, the pulled change feed, and operator writes admitted
from `reserved:prefrontal-core` through core's relays. The agent-facing surface
(scoped tools for the janitor and heads, consent cards, standing grants, the
hire cap, app-initiated writes) is a second campaign, specified against
cingulate's real `consent/v1` interface once it exists; its starting list is
under non_goals.

**This campaign is entorhinal's half only.** Core's half, the clients and
Callosum are separate work with named owners, listed under dependencies. Every
acceptance item here is evidenced in this repository.

## non_goals
- Flow identity. Flows are an attribute (`ScopeAttributes.flow_id`, set and
  resolved by core), not an agent kind, so entorhinal mints nothing for them.
- Personas, and `agent.role_tool_permissions`. The latter's "role" is a worker
  profile kind (`explore`, `librarian`, `mason`), and its handler cannot read
  the registry.
- Per-agent permission state of other providers. Their grants stay with them,
  keyed by entorhinal's agent id.
- Card rendering, presence, push, or any part of cingulate.
- Building `agent.fleet_overview` anywhere. It is deleted, not moved.
- Any dual-write period, or any phase where core proxies identity to a client
  per request. Core's read replica is not a proxy: it is how core avoids a
  cross-module read on its delivery paths. Core relaying operator writes to
  entorhinal through its unchanged caller check is not a per-request read proxy
  either, and stays allowed.
- Moving entorhinal off subc-protocol 0.29.0 and subc-client-rs 0.26.1. The
  move to 0.29 already shipped in entorhinal 0.1.14 (commit 192f220); this
  campaign starts from those versions.

**Deferred to the second campaign.** It starts from exactly these items,
specified against cingulate's real `consent/v1` interface once it exists:
- scoped agent tools for the janitor and heads: create workspace, create
  project, create head, create hire, dispose agent; their request keys, and how
  each tool's parameters convert to the matching journal op;
- reading the calling agent from the inbound `ScopeStamp`, including refusing
  flow-scoped calls;
- consent: card kind `entorhinal.identity_request`, targeted-carrier routes to
  cingulate opened only inside a live inbound tool call, the request digest and
  opaque `binding`, the pending and expiry rules, and the codes
  `consent_not_bound`, `consent_unavailable`, `consent_route_closed` and
  `consent_pending`;
- standing grants: per-workspace scope by default, fleet-wide only as an
  explicit option, minted only from an answer to a card entorhinal raised,
  served behind `grants.list`, `grants.revoke`, `grants.would_ask` and
  `grants.offer`;
- the per-project cap on live hires, default 10 and operator-changeable, and a
  head retiring its own hire with a notice and no card;
- admitting `reserved:callosum` for app-initiated creates. When those ops are
  exposed to the apps they go into Callosum's `phone` profile only, never
  `sim`;
- once SUBC's `ck` gate exists, admitting a gated `Direct` write as the
  operator's, and removing core's relay ops in a later cut.

## open_questions
- No open question remains. Open question 1 ("What admits an operator's
  agent-identity write?") is closed by chair rulings R1 to R5, as narrowed by
  R9; the result is in constraints under "Who may write what", the clean
  cutover paragraph, core's dependency item and cutover order.
  The measured bound on the fleet view's `unknown` state is not open: it is a
  measurement taken after the cut (cutover order, step 4).
  Facts still to be quoted from core's repository before the slices that depend
  on them start (none is a design choice): the body and pinned tests of
  `normalize_agent_name` and `namespace_for_create`; the post-082 `agent` DDL and
  its role/binding CHECK; whether `hire_identity_mapping` (079) names a
  supervisor; the request and reply structs of core's agent ops; and the
  `session.transcript_page` unchanged-token fields.
  ruling: closed by chair rulings (refire 3)
  answer: See the normative chair rulings in the chair rulings (refire) section.

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded supplied ruling 2

Campaign `ct_00000000-0000-4022-98db-89e6b948e980`, round 1, decision sequence 0 (source: {"body_bytes":2120,"boundary":"ledger_payload","order":3,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/drafts/2026-10-03-agent-identity-rulings-r1.md"}):

Chair rulings after round 0. These settle open question 1, "What admits an operator's agent-identity write?". Both entorhinal's owner and core's owner agreed to them on 2026-10-03.

R1 Until SUBC's `ck` gate lands, the operator row for agent identity is `reserved:prefrontal-core` only. Core keeps its existing agent-identity write ops (dispose, merge, rename, tag, labels, avatar, GitHub identity) and adds one for the hire cap. Each becomes a relay to entorhinal, sent under `reserved:prefrontal-core`, and core's primary-operator caller check stays exactly as it is today. Entorhinal admits every operator-row operation from `reserved:prefrontal-core`. The seven repointed operator scripts call core's relay ops, as they call core's ops today.

R2 An agent-identity write from `Direct` refuses with `direct_identity_write_not_admitted`, and the message names core's relay as the path. Admitting it would let any agent's shell write identity, which core's check prevents today. One exception: `grants.revoke` from `Direct` is admitted, because revoking only narrows authority.

R3 After the `ck` gate lands, a gated `Direct` write is admitted as the operator's, and core's relay ops are removed in a later cut. This row changes in an entorhinal release, never by detecting the gate at runtime. The panel already applied the same rule to the `reserved:callosum` row.

R4 The cut moves the store and the authority to entorhinal. Core's identity write ops become relays; they are not removed. Amend cutover step 3 and core's dependency item to say so. Amend the non-goal on proxying too: relaying operator writes through core's unchanged caller check is not a per-request read proxy, and it stays allowed.

R5 The relay contract, recorded in core's dependency item:
- core passes entorhinal's typed refusal through verbatim, never wrapped;
- the relay reply carries the new `(incarnation, generation)`;
- core's replica catches up from the change feed, so a read in the same turn may lag by one poll, and the generation in the reply lets a caller tell.

R6 Open question 1 is closed by R1 to R5. No open question remains.

### recorded owner answer 4

Campaign `ct_00000000-0000-403c-98db-8c1edc263cd8`, round 3, decision sequence 3 (source: owner decision):

{"decision":"continue","rounds":1,"through_round":4}

### recorded supplied ruling 6

Campaign `ct_00000000-0000-403c-98db-8c1edc263cd8`, round 5, decision sequence 0 (source: {"body_bytes":5500,"boundary":"ledger_payload","order":4,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/drafts/2026-10-03-agent-identity-rulings-r2-scope.md"}):

Rulings from the campaign's owner (the chair, in the spec pipeline's terms) after review round 4. They narrow this campaign to the authority move and defer the agent-facing surface to a second campaign. After five review rounds the distinct blocking findings ran 33, 24, 26, 19, 21. Each fold resolved the previous round's findings and exposed new contradictions, nearly all in the consent, tool and grant surface: how the consent digest interacts with request keys, which error wins when several rules apply, partial grant revocation while a card is pending, and tool parameter conversion. That surface depends on `consent/v1`, which is still a design sketch, and until a consent capability is bound every write that needs one refuses. So this campaign delivers the authority move only. The agent-facing surface becomes a second campaign, specified against cingulate's real interface once it exists. The owner and core's maintainer agreed to this split on 2026-10-03.

R7 In scope:
- the agent identity store, with its invariants;
- both legacy id formats;
- the supervisor of each hire, copied at import and stored;
- the create request key on the row;
- the import, with its invariant checks and the cutover marker;
- the identity reads, including the fleet view's identity half with its unchanged token;
- the pulled change feed;
- `(incarnation, generation)` on every read that carries a generation;
- the attested principal, and Callosum's `origin` when present, recorded in the journal;
- operator writes admitted from `reserved:prefrontal-core`. Core keeps its existing agent-identity write ops as relays to entorhinal, with its primary-operator caller check unchanged. Each relay passes entorhinal's typed refusals through verbatim, and its reply carries the new `(incarnation, generation)`.

R8 Out of scope, moved to the second campaign:
- scoped agent tools (create_project, create_head, create_hire, dispose_agent) and the manifest's tool surface;
- reading `ScopeAttributes` on inbound stamps, including `flow_id` handling;
- consent cards, targeted-carrier routes to cingulate, the consent digest and `binding`, and the codes `consent_not_bound`, `consent_unavailable`, `consent_route_closed` and `consent_pending`;
- standing grants and `grants.list`, `grants.revoke`, `grants.would_ask` and `grants.offer`;
- the hire cap, and a supervisor retiring its hire without a card;
- admitting `reserved:callosum`.
Delete every constraint, acceptance item and check-order step that exists only for these features. Keep a constraint only where it stops this campaign's code from ruling them out later. For example, the store must still be able to hold a grant, and admission must still be able to gain a scoped-agent row.

R9 Admission in this campaign:
- agent-identity mutations are admitted only from `reserved:prefrontal-core`;
- a `Direct` agent-identity mutation refuses `direct_identity_write_not_admitted`, and the message names core's relay as the path. Any local process, an agent's shell included, reaches entorhinal as `Direct`, so admitting it would weaken core's existing check. The earlier exception admitting `grants.revoke` from `Direct` no longer applies, since grants are out of scope;
- every other principal refuses `write_not_permitted`;
- before the cutover marker exists, every agent-identity mutation refuses `authority_not_cut_over`;
- project and workspace operations keep today's gate unchanged.
No agent-initiated write path exists in this campaign, so nothing here refuses for lack of consent.

R10 Reads are open to every principal, which is how entorhinal admits reads today. Before the cutover marker exists, a resolve of a well-formed but absent agent id returns `unknown`, never `authority_not_cut_over`.

R11 Moving entorhinal to subc-protocol 0.29 is not part of this campaign: it already shipped on its own. Entorhinal 0.1.14 (commit 192f220) runs subc-protocol 0.29.0 with subc-client-rs 0.26.1, so this campaign starts from those versions and must not move off them.

R12 The deferred list, kept here so nothing drops silently between the two campaigns. The second campaign starts from exactly these items, specified against cingulate's real `consent/v1` interface once it exists:
- scoped agent tools for the janitor and heads: create workspace, create project, create head, create hire, dispose agent; their request keys, and how each tool's parameters convert to the matching journal op;
- reading the calling agent from the inbound `ScopeStamp`, including refusing flow-scoped calls;
- consent: card kind `entorhinal.identity_request`, targeted-carrier routes to cingulate opened only inside a live inbound tool call, the request digest and opaque `binding`, the pending and expiry rules, and the codes `consent_not_bound`, `consent_unavailable`, `consent_route_closed` and `consent_pending`;
- standing grants: per-workspace scope by default, fleet-wide only as an explicit option, minted only from an answer to a card entorhinal raised, served behind `grants.list`, `grants.revoke`, `grants.would_ask` and `grants.offer`;
- the per-project cap on live hires, default 10 and operator-changeable, and a head retiring its own hire with a notice and no card;
- admitting `reserved:callosum` for app-initiated creates. When those ops are exposed to the apps they go into Callosum's `phone` profile only, never `sim`;
- once SUBC's `ck` gate exists, admitting a gated `Direct` write as the operator's, and removing core's relay ops in a later cut (R3).

### recorded owner answer 7

Campaign `ct_00000000-0000-4017-98db-93db545853a8`, round 3, decision sequence 3 (source: owner decision):

fold and mint as-is

### recorded owner answer 8

Campaign `ct_00000000-0000-4017-98db-93db545853a8`, round 4, decision sequence 0 (source: refire_from:ct_00000000-0000-403c-98db-8c1edc263cd8):

park for chair rulings

### recorded supplied ruling 9

Campaign `ct_00000000-0000-4017-98db-93db545853a8`, round 4, decision sequence 1 (source: {"body_bytes":1529,"boundary":"ledger_payload","order":4,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/drafts/2026-10-03-agent-identity-rulings-r3-pins.md"}):

These rulings settle round 3's needs_evidence question about the protocol pins (finding 13). They come from git, read directly on 2026-10-03.

R13 At HEAD `ff07aff8445f53f85b0f48f13c7a1fcada887bcb`, both files pin the 0.29 versions:
- `Cargo.toml` has `subc-protocol = "0.29.0"` and `subc-client-rs = "0.26.1"`;
- `Cargo.lock` resolves exactly `subc-protocol 0.29.0` and `subc-client-rs 0.26.1`, one copy of each.

Commit `192f220` (entorhinal 0.1.14) is an ancestor of HEAD and later than `3e06dbd37b62`, where the earlier evidence package was frozen. The pin moves landed after that freeze, in this order:
- `c355be4` (0.1.13) moved subc-protocol 0.28.1 → 0.29.0, subc-client-rs 0.25.2 → 0.26.0, subc-transport 0.9.1 → 0.10.0 and subc-control 0.27.1 → 0.28.0;
- `192f220` (0.1.14) moved subc-client-rs 0.26.0 → 0.26.1.

The package's record of 0.28.1 / 0.25.2 (ev-56, ev-57) is therefore stale, not a contradiction. "Start from 0.29.0 / 0.26.1 and change neither pin" holds at HEAD, and acceptance item 10's pin test asserts those two versions.

R14 This refire reads evidence at HEAD `ff07aff` (its integration_ref), so every file in the evidence package reflects the code the slices will be cut from. Since `3e06dbd37b62` the only source change is the session-liveness receiver in `crates/entorhinal-module/src/main.rs`: refusing older batches, the `livenessDroppedOlder` gauge, and a mirror that starts stale until a snapshot arrives. That code is outside this campaign's scope, and slices must leave it unchanged.

### recorded supplied ruling 10

Campaign `ct_00000000-0000-4017-98db-93db545853a8`, round 4, decision sequence 2 (source: {"body_bytes":2020,"boundary":"ledger_payload","order":4,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4017-98db-93db545853a8-20261004T133504Z.md"}):

These rulings move the campaign onto the current HEAD for one final review round, then the slice plan. The scope cut in R7, R8 and R12 stands unchanged: this campaign moves agent identity into entorhinal, and the deferred list in R12 is the whole of the second campaign.

R15 Read evidence and cut slices at HEAD `aa2eeb0`, not `ff07aff`. Between the two, the only source changes are in `crates/entorhinal-module/src/main.rs` (plus the version bump to 0.1.15 in `crates/entorhinal-module/Cargo.toml` and `Cargo.lock`):
- `7657d38` added route admission for flows. At `route.bind` the handler records a `RouteAdmission` (the route's principal and the `flow_id` from the bind's scope stamp, if any). `ProjectsHandler::admit` is now the single admission point the request handler calls: it first runs `refuse_flow_write`, which refuses every mutating method on a flow-scoped route with `flow_scope_not_admitted`, and then `authorize_write` on the principal.
- `aa2eeb0` reworded the warning for a refused session-liveness batch.
R14's statement that the session-liveness receiver is out of scope still holds, and covers the reworded warning too.

R16 The flow refusal is a fixed part of admission. Any slice that changes admission (adding agent-identity methods, the relay from core, or the refusal of `Direct` identity writes) extends `ProjectsHandler::admit` and keeps `refuse_flow_write` as its first check. Every new mutating method must be in the list `refuse_flow_write` consults, so a flow-scoped route can't reach it; a test asserts that for each new mutating method. Entorhinal does not declare `flow-scopes/v1`: the daemon keeps flows away from it, and this refusal is the second line behind that. Reading the calling agent from the scope stamp stays deferred to the second campaign (R12). This campaign reads only `flow_id`, which is already done.

R17 The earlier "findings lack detail" decision is closed. No question is open. Findings in this round must state their detail, since that round's detail was lost.

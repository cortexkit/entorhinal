---
title: "2026-10-03-move-agent-identity-from-prefrontal-core-to-entorhinal refire 9"
status: draft
rounds_cap: 1
refired_from: "ct_00000000-0000-4050-98da-6288006dff70"
integration_ref: "main"
evidence:
  include:
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_assertion.rs:1-720"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:41-93"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:148-327"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:445-732"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:943-989"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:1129-1144"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:1185-1289"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:3005-3431"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/fleet_overview.rs:1-482"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/projects_consumer.rs:528-538"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/projects_consumer.rs:708-756"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/projects_consumer.rs:808-860"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/projects_consumer.rs:899-905"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/tests/it/agent_fleet_overview.rs:559-582"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/Cargo.toml"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/076_agent_registry.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/079_hire_identity_mapping.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/080_agent_github_identity.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/082_wake_delivery.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/109_agent_generation.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/112_agent_avatar.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/126_agent_labels.sql"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_claims.rs:44-67"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_claims.rs:100-420"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:600-628"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:678-780"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:1004-1012"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:1148-1160"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:1237-1322"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:1553-1570"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:1784-1822"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:3653-3668"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:7533-7661"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:7733-7763"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/agent-identity-entorhinal.md"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/consent-module.md"
    - "crates/entorhinal-core/src/binding.rs"
    - "crates/entorhinal-core/src/lib.rs"
    - "crates/entorhinal-core/src/mutations.rs"
    - "crates/entorhinal-module/src/main.rs"
    - "docs/designs/fleet-view-join-vector.md"
    - "docs/designs/agent-identity-pinned-facts.md"
---

## acceptance_sketch
Each acceptance item below is backed by a test or a command in this repository.
Error codes, op names and wire fields are those listed under constraints, Names.
Tests run through a handler-level seam that injects the route principal, the
bind stamp's `flow_id`, `actor`, a clock and the incarnation (a new value per
simulated restart), and returns a refusal's `code`, `message` and `detail`; a
decoder fixture reads `detail` from the error body (R19). No test expects a
consent, grant or hire-cap code, no test sends or reads an `origin` (R20), and
only item 5's flow-scope case sends a `flow_id`.

**Import test source.** Every import test reads a SQLite file built by applying
core's migration SQL for `076`, `080`, `082`, `109`, `112` and `126`, copied
verbatim into this repository with each source path cited beside it, then
inserting fixture rows with `PRAGMA foreign_keys = OFF` (SQLite's default), so
the `claim_owner` and `merged_into` fixtures, which break 076's and 082's
`REFERENCES`, can be built; the `live_head` fixture first drops `uq_live_head`
(`082_wake_delivery.sql:90`). Project and workspace rows the binding check
needs are created in the destination entorhinal store. A fixture test fails if
the copied SQL's `agent` column list differs from the column list the import
reads.

1. **The identity store holds every field the import needs, and the rules that
   protect it.** Schema covers ids, names and claims (with history and
   `name_normalization_version`), role, project and workspace binding, tag,
   labels, avatar inputs under 112's genome/type CHECK and the version-needs-
   genome CHECK (no stored fingerprint column), GitHub identity with
   `credential_ref` and no secret column, `agent_generation` (no mutation
   trigger), `name_version`, timestamps, `terminal_reason`
   (`retired`/`merged`, CHECK refusing any other value) with `merged_into`,
   supervisor, create request key, and the journal's `principal` column (no
   `origin` column); the `agent.cutover` marker is a journal row, not a table.
   It has no persona, residence, wake, sleep, grant, cap or pending-consent
   column or table. Tests prove:
   - the one-live-head index refuses a second live head with
     `agent_project_taken`;
   - names (R23, R35), on the port built on `icu_normalizer` and `icu_casemap`
     `=1.5.0` with `normalization_version` 1 and a 24-scalar maximum: core's
     `normalization_pipeline_pins_nfc_case_fold_trim_and_scalar_order`
     (`agent_registry.rs:7533-7580`) is copied verbatim as a pure test of the
     port and compiles against its surface (constraints, Identity invariants).
     The other cited tests (`7582-7661`, `7733-7763`) are copied as their
     inputs and expected outcomes and run through entorhinal's own seams
     (`agent.create` and the claim path, or the handler), each copy citing the
     core line it came from: each collision pair as two creates in one
     namespace, the second refusing `name_conflict`; the codepoints `0x200B`,
     `0x202E`, `0x2060` refusing `invalid_name` with the codepoint, and
     `Алиса` kept; `"  Cafe\u{301}  "` stored as `Café` with
     `name_normalization_version` 1 and `name_version` 1; the uppercase-id
     lookup (`7757-7761`), which core answers `NotFound`, run through
     `agent.github_identity` and answering `unknown_agent` (its
     `agent.resolve` outcome, `unknown`, is the ids bullet's). Every assertion
     about names, claims, codes and normalisation version is kept; the
     assertions about `persona_ref`, `wake_policy_version`, residence and
     machine bindings are dropped, with a comment saying so, and no production
     field, table or code is restored to satisfy a copied assertion;
   - each illegal role shape refuses `invalid_role_shape`
     (`agent_registry_ops.rs:3038-3049`); a head or hiree naming a missing
     project, or a workspace_head a missing workspace, refuses `not_found`; a
     head named by an alias of P is stored with P's current id; a head or
     hiree create on a project with no workspace placement refuses
     `unresolved_workspace`, and one whose placement read fails (the seam
     dropping `project_workspace` under an open store) refuses
     `activation_gap`, each with no claim written (R22); an assistant claims in
     `assistant`/`"global"`, a workspace_head in `workspace`/its own
     `workspace_id`, and a head or hiree in `workspace`/its project's resolved
     workspace, which is also its stored `workspace_id`; a create of any role
     whose `supervisor_agent_id` is malformed (uppercase, wrong length, no
     `agent_` prefix) refuses `invalid_supervisor`; a non-hiree create naming a
     well-formed `supervisor_agent_id` refuses `invalid_supervisor`; a hiree
     create naming the live head of its project records it, naming any other
     well-formed id refuses `invalid_supervisor`, naming none or null records
     NULL; a request carrying `persona_ref` or a residence, wake or sleep field
     refuses `invalid_request`; step 5a's order holds: an illegal shape with a
     bad name refuses `invalid_role_shape`, a bad name with a whitespace-only
     tag `invalid_name`, and a whitespace-only tag with an occupied name
     `invalid_tag`, each writing no row, claim or journal entry;
   - tags, labels and avatars (R38), one case per bound and refusal code: a
     tag empty after trimming, and one of 257 bytes, refuse `invalid_tag`
     while one of 256 bytes is stored; 16 labels are stored and 17 refuse
     `invalid_labels`; a label of 32 scalars is stored, while one of 33, one
     empty after trimming, and two labels equal after ICU case folding refuse
     `invalid_labels`; labels are stored trimmed in request order; a
     `creature.classic` genome of exactly 2048 hex characters is stored, one of
     2047 characters or holding a non-hex character refuses as core's avatar
     decode does (`agent_registry_ops.rs:835-842`), and an unknown avatar type
     refuses `invalid_request`;
   - rename is atomic and raises `name_version` by 1, while tag, labels,
     avatar, GitHub identity, dispose and merge leave it; after dispose, the
     same display name can be created again in that namespace; an imported head
     whose active claim is in W1 while its project is placed in W2, renamed
     after `remove(W1)`, succeeds, releases the W1 claim and claims the new name
     in W1, and the same claim history holds after `rebuild`; an imported head
     on an unplaced project refuses rename with `unresolved_workspace`, its
     claim unchanged;
   - merge into self refuses `merge_self`, and into a terminal id, a
     well-formed absent id or an ill-formed id `merge_target_not_live`, each
     with the source claim still active; a mutation of a terminal agent
     refuses `gone` with the gone object as `detail`, of an absent one
     `unknown_agent`, also when the rename's new name is taken or the merge
     target is not live; with fresh keys, a second `agent.dispose` of a
     retired agent and a second `agent.merge` of a merged agent into its
     `merged_into` succeed with the gone object, append one journal row each
     and leave the row and `agent_generation` unchanged, while dispose of a
     merged agent and merge of a merged agent into another live target refuse
     `gone`;
   - two minted ids match `agent_` plus 16 lowercase hex and differ; imported
     `agent_16013c86` and a 16-hex id resolve; a never-imported 8-hex id
     resolves `unknown` and a mutation of it refuses `unknown_agent`; an
     uppercase or wrong-length id resolves `unknown` and a mutation of it
     refuses `unknown_agent`.
2. **The import is exact, atomic, admitted once, and replayable.** It opens the
   file at `snapshot_path` read-only. Placement by migration:
   - carried: `076` (`agent`, `agent_name_claim`), `080`
     (`github_identity_json`), `082` (the `agent` rebuild; its `wake_fire`
     stays), `109` (`generation`, the column only), `112` (avatar), `126`
     (`labels_json`);
   - not read: `079` (`hire_identity_mapping`), which records no supervisor
     (R21);
   - stays in core: `076`'s `machine_binding`, `agent_delivery`, `residence_flip`
     and activation marker, `079`, `088`, `091`, `110`, `116`, `121`, `127`,
     `171`, `177`.
   Every imported row's supervisor and request key are NULL; `deleted` is
   stored as `retired`. Tests:
   - admission: before the marker, `agent.rename` from
     `reserved:prefrontal-core` refuses `authority_not_cut_over`; import from
     `Direct` refuses `direct_identity_write_not_admitted`, before and after the
     marker, also with a bad `snapshot_path`; import from
     `reserved:prefrontal-core` commits rows and the marker in one transaction,
     after which a rename is judged by the table; a zero-agent import (no
     `agent` table, with or without `agent_name_claim`) commits the marker and
     replies 0 and 0, after which an operator `agent.create` is admitted; a
     same-key rerun after a restart returns the committed counts and generation
     with the new incarnation, not `import_already_done`; a new-key import
     refuses `import_already_done` and changes nothing, also with a
     `snapshot_path` naming a missing file; before the marker, a
     `snapshot_path` naming a missing, unreadable or non-SQLite file refuses
     `invalid_request` with no marker; an import body without `snapshot_path`
     refuses `invalid_request` before and after the marker, no marker written;
   - refusals: each check refuses `import_invariant_failed` with the `detail`
     object constraints, Names gives for it, pinned by a fixture per check,
     nothing written and no marker, and never a step-5a/5b code: an id outside
     `agent_` plus 8 or 16 lowercase hex (`agent_id`); an `agent` table
     present but empty with one released claim naming an absent agent, and a
     claim whose owner id is absent beside valid rows (`claim_owner`); a live
     agent with no active claim, one whose active claim's `normalized_name`
     differs from re-normalisation, one whose claim's `display_name` differs
     from `name`, and a source with `agent` but no `agent_name_claim` holding
     a live agent (`claim_mismatch`); two live heads on one project
     (`live_head`, `agent_ids` a JSON array in ascending order); a
     `merged_into` absent from the source (`merged_into`); a live row naming a
     non-NULL project id that is not a project row in the destination store
     (an alias included), and a live workspace_head naming a workspace that is
     not a destination row (`binding`); a source failing two checks reports
     the earlier in constraints' order; a rerun after a refusal with the same
     key then succeeds once the source is fixed;
   - successes, as a separate import: a terminal row naming a removed project, a
     terminal workspace_head naming a removed workspace, an assistant with NULL
     ids, a live hiree with NULL supervisor, a head and a hiree holding a
     non-NULL `workspace_id` (valid since 082, kept as stored), a live head on
     an unplaced destination project, a row with non-NULL `persona_ref`,
     residence, a wake policy and `sleep = 1`, and a row with NULL genome and
     type and `avatar_version` 2 all import; the stored rows carry no persona,
     residence, wake or sleep value, and the version-2 row stores NULL version
     and shows `avatar: null` in the snapshot, its feed entry and after
     `rebuild`;
   - the reply's `agents_imported` and `claims_imported` equal the `agent` and
     `agent_name_claim` rows read; `agent.changes` with `limit` 1 from before
     the import returns one `agent.import` entry per agent, each with its own
     seq, none skipped, and their `claims` together equal `claims_imported`;
   - a fixture pins one value per carried field and asserts it after import and
     after `rebuild`: `agent_generation`, `name_version`,
     `name_normalization_version` (row and claim), `created_at_ms`,
     `updated_at_ms`, a merged row's non-null `terminal_at_ms`, labels (empty
     and non-empty), an avatar with type set and version NULL,
     `credential_ref`, `app_slug`, `installation_id`, a released claim whose
     name can be created again, `merged_into`, tag, role, `project_id`,
     `workspace_id` on a workspace_head and on a head (including NULL),
     supervisor (NULL), `agent_16013c86` and a 16-hex id;
   - a fault injected mid-import leaves neither rows nor marker and a rerun
     succeeds; import then `rebuild` equals the imported state, the marker row
     still present; project and workspace rows are untouched; the reply carries
     both counts, `incarnation` and `generation`.
3. **Resolves answer, before the marker as after it.** A live id resolves
   `live`, a disposed id `retired` (including one imported as `deleted`), a
   merged id `merged` with its `merged_into`, a retired id with `merged_into`
   null, an absent id `unknown`; with the store not open the call returns
   `storage_unavailable`, and with the seam dropping the `agent` table under an
   open store `storage_error`, neither as a status. Gone objects (R36): a
   retired id's resolve reply carries `gone` `{"reason": "deleted", "at":
   <terminal_at_ms>}` and a merged id's `{"reason": "merged", "at":
   <terminal_at_ms>, "into_agent_id": <id>}`, exactly those keys and no
   generation, a live or unknown id `gone` null; the same objects appear
   wherever `agent.list`, `agent.dispose` or `agent.merge` carries one; the
   snapshot and feed show `retired` where the gone object shows `deleted`.
   Before the marker, `agent.resolve` of a well-formed absent id returns
   `unknown`, and `agent.snapshot` and `agent.fleet_identity` succeed with
   empty lists and a generation. The snapshot and the feed carry the same
   `retired`/`merged` values. `agent.list` with `activated_only: true` returns
   `{"agents": []}` plus the fleet pair before the marker and, after an import
   of a non-empty registry, the same live rows as `activated_only: false`;
   with `false` it lists stored rows both before (empty store) and after the
   marker; each serialized reply is pinned, the incarnation being the seam's
   injected string. `agent.peer_roster` (R32): a workspace with one
   workspace_head, two heads on projects placed in it, a head on a project
   placed elsewhere, a hiree and a retired head returns exactly the first
   three, ordered by `agent_id`, each peer with core's keys and no
   `reachability`; the same call before the marker refuses
   `registry_not_activated`. Each is a distinct assertion.
4. **Generations can be trusted.** Every agent read and mutation success reply,
   `agent.snapshot` included, carries the fleet `(incarnation, generation)`,
   `incarnation` a string of 16 lowercase hex, and no `noop`; the project reads
   `resolve`, `resolve_project_id`, `enumerate`, `journal_tail`, `trust` and
   `verify` carry `incarnation` beside `generation`, while project mutation
   replies keep today's shape; a restart changes the incarnation on each. The
   core goldens under `crates/entorhinal-core/tests/golden/resolve/` pass
   unchanged; with root records off, the module's `resolve` reply with
   `incarnation` removed equals `legacy-resolve-root.json` and nothing else
   differs. A create writes `agent_generation` 1; an imported row at 1000000
   renamed in a store whose head is below 100 reads 1000001, also in its
   `agent.rename` entry and digest, and still 1000001 after `rebuild`. Renaming
   agent B advances B's `agent_generation`; renaming A or `approve_root` leaves
   B's unchanged while `generation` rises, and `agent.github_identity` for B
   shows both fields with unequal values. `agent.update_tag` writing B's stored
   tag succeeds, appends one journal row and leaves `agent_generation` and
   `updated_at_ms` unchanged while `generation` rises. Across an identity
   mutation and `rebuild`, both are reproduced and a later mutation issues
   above them. A committed `agent.create`, a restart, and a same-key retry from
   `reserved:prefrontal-core` (with an unchanged body, or a changed body
   carrying no removed field) return the same agent with no new journal row,
   the commit's `generation` and the new incarnation. `agent.set_avatar` with
   `seedOnly: true` on an agent that has an avatar replies `applied: false`
   with the stored avatar, appends one journal row and one `agent.set_avatar`
   entry carrying the unchanged row, and leaves the row, `agent_generation`,
   `name_version` and `updated_at_ms` unchanged while `generation` rises (R38);
   on an agent with no genome it writes the avatar; after a later mutation of
   that agent and a restart, a same-key retry with a changed valid body
   returns the cached `applied: false` reply with the commit's `generation` and
   the new incarnation, appends nothing and leaves the avatar as stored; the
   same key on another op refuses `request_key_reused_across_ops`.
5. **Admission follows R9 and the check order.** A matrix over every mutation in
   Names and the principals `reserved:prefrontal-core`, `Direct`,
   `reserved:callosum`, another reserved module, `Unverified` and a route with
   no recorded principal, before and after the marker, asserts the code and an
   unchanged journal head on every refusal:
   - each mutation in Names, on a route whose bind recorded a `flow_id`,
     answers `flow_scope_not_admitted` with every principal,
     `reserved:prefrontal-core` included, before and after the marker (R16);
   - `Direct` refuses `direct_identity_write_not_admitted` with a message naming
     the op and prefrontal-core's relay; every non-operator principal refuses
     `write_not_permitted`; each holds keyless, with a fresh key, and with a key
     `reserved:prefrontal-core` already committed for that op, in which case no
     cached body is returned;
   - from `reserved:prefrontal-core`: a malformed body refuses
     `invalid_request` (never `invalid_params`); a same-key retry of a
     committed `agent.create` that adds `persona_ref` refuses
     `invalid_request`; a missing or empty key refuses `request_key_required`,
     never `invalid_request`; an `agent.create` with neither `actor` nor
     `supervisor_agent_id`, and one with both null, is admitted; before the
     marker every mutation but the import refuses `authority_not_cut_over`;
     after it, creates of all four roles and every other mutation except
     `agent.import` are admitted, the import's post-marker cases being item 2's;
   - `create_project`, `create_head`, `create_hire`, `dispose_agent`,
     `agent.set_hire_cap` and `grants.list` answer `unknown_method` and write
     nothing;
   - identity reads, and the project reads `resolve`, `resolve_project_id`,
     `enumerate`, `journal_tail`, `trust` and `verify`, are admitted from every
     principal in the matrix, before and after the marker
     (`agent.peer_roster`'s pre-marker `registry_not_activated` being item 3's);
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
   - the avatar fingerprint, computed at serve time, for genome `"a"` repeated
     2048 times, type `"creature.classic"` and version `2` equals
     `b4c04a5d931e65a48e022b8943678d55` (core's
     `tests/it/agent_fleet_overview.rs:559-582`); the type-and-version-absent
     case, which no stored row can hold under 112's CHECK, is a unit test
     feeding the fingerprint function `{genome, type: None, version: None}`
     and comparing it to a value computed once with core's recipe
     (`fleet_overview.rs:96-109`) and written into the fixture with that
     citation; neither expected value is produced by entorhinal's own function
     (R24); absent optionals are omitted, not null;
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
   another op refuses `request_key_reused_across_ops`. `agent_project_taken`
   carries in `detail` the occupier's `agent_id` and `request_key`, with an
   explicit null for an imported head.
8. **The journal records who wrote each row.** After an admitted
   `reserved:prefrontal-core` agent write carrying `actor`, the journal row's
   `principal` reads `reserved:prefrontal-core` and `actor` reads back as
   sent; with `actor` absent or null the row records `module`. A project
   mutation from `Direct` records `principal` `direct`. With root records on, a
   `Direct` `register` records `direct` on its own row and on the `bind_root`
   rows it causes, and the `bind_root` rows written at store open record
   `entorhinal`; no new row has NULL `principal`, and op names and projections
   are unchanged after `rebuild`. A `register` journaled with NULL `principal`
   and key K, retried with K, returns the cached reply.
9. **The change feed is complete and ordered.** Tests prove:
   - a consumer using only serialized snapshot and change replies reproduces
     entorhinal's agent and claim tables, claim history predating the snapshot
     included, with mutations committed concurrently while it pages through
     `agent.changes` and none skipped at the snapshot boundary;
   - an entry of every op in the closed list, and the row and claim objects,
     decode with exactly their listed keys, for a live and a terminal row, the
     row's `avatar` as an object `{genome, type, version}` or null and
     `github_identity` as core's tagged JSON or null; a decoder test fails if
     a key is missing, renamed or extra; project-op and marker entries are
     skipped and the reply cursor advances past them, to the head when they
     are trailing;
   - two consecutive identity entries read with `limit` 1 come back one per
     call, the first reply's cursor being the first entry's seq;
   - a `wait` call whose only entries above the cursor are non-identity returns
     at once with no entries and the cursor at the head;
   - `limit` omitted is 500, outside 1..=1000 refuses `invalid_request`;
   - `journal_tail` returns project-op rows only; with project rows at seq 1
     and 3, the `agent.cutover` marker at 2 and an `agent.create` at 4, and
     `limit` 1, a caller following the advance rule consumes rows 1 and 3
     exactly once, never sees the marker, and ends at 4;
   - another incarnation or a cursor above the head answers `snapshot_required`
     with no entries; a gap returns the missing entries in order;
   - an idle poll with `wait` stays open until an entry commits or 25 s on the
     injected clock pass, holds no store lock (a resolve during it succeeds),
     and on timeout returns success with the current incarnation, empty
     `entries` and the request cursor; an identity mutation committed during it
     is returned by that same call without the injected clock advancing;
   - with 8 waits held, a 9th `agent.changes` with `wait` returns without
     waiting, with whatever is ready, and a `resolve` still answers (R26);
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
    - `multi_workspace`: a project left unplaced by a `seed_import` reporting
      `multi_workspace` then receives, by import, a live head whose source row
      (created in core while its project was placed) holds an active claim in
      workspace W and stored `workspace_id` W; it shows no `workspace` and
      keeps its name claim in W, before and after `rebuild` (R22);
    - a `seed_import` identity reported `alias_occupied` creates no project
      after `rebuild` either.
12. **Wire bodies carry exactly the listed edits.** Fixtures pin, for every op
    core serves today except `agent.resolve`, constraints' "Wire bodies" rule:
    core's request field names, casing and `deny_unknown_fields` (so
    `agent.avatar_read` takes `agentIds` and `agent.set_avatar` takes
    `agentId`, `type` and `seedOnly`, a snake_case alias refusing
    `invalid_request`; `agent.set_labels` with `agent` and with `agent_id`
    resolves the same agent, while both, neither or an empty string refuses
    `invalid_request`); `callerHarness`, `callerSession`, `avatar_read`'s
    `harness`, `session_id`, `session`, `caller_directory` and
    `caller_session`, and every removed field refuse `invalid_request`; replies
    keep core's top-level keys (R27) and carry `incarnation` and fleet
    `generation`, never `noop` or `reachability` (R31: not `agent.resolve`,
    `agent.resolve_name`, `agent.list`, the `agent.peer_roster` peers or the
    agent row); the digest carries `agent_generation`, and
    `agent.github_identity` carries `github_identity` (core's tagged JSON with
    `app_slug`, `credential_ref`, `installation_id` and no secret),
    `projectId` when set and `agent_generation`, pinned for an `app`, a
    `user_token` and an unset identity; `request_key` is required and `actor`
    optional on mutations; role fixtures carry `hiree`. Inherited outcomes:
    `agent.resolve_name` with no match, and with two matches in different
    workspaces, succeeds with `{"refused": {code, details}}` (`name_unknown`,
    `name_ambiguous`) plus the fleet pair; `agent.list` with `limit` 0 or 201,
    or `cursor` `""`, refuses `invalid_cursor`; `agent.github_identity` of an
    absent or malformed id refuses `unknown_agent`, of a terminal id `gone`.
    Error-code mapping (R34): a decoder fixture per inherited code pins the
    condition that raises it and core's code for it: `invalid_name`,
    `name_conflict`, `agent_project_taken`, `unknown_agent`, `gone` (with the
    gone object as `detail`), `merge_self`, `merge_target_not_live`,
    `invalid_tag`, `invalid_labels`, `invalid_github_identity`,
    `invalid_project_id`, `invalid_workspace_id` (their conditions taken from
    core's validators), `invalid_role_shape`, `invalid_cursor`,
    `registry_not_activated`, `unresolved_workspace` and `activation_gap`; no
    fixture expects `invalid_agent_name`, `agent_name_taken`,
    `agent_not_found`, `agent_terminal`, `invalid_agent_id` or
    `live_head_exists`. New ops' fields (`agent.resolve`'s reply,
    `agent.import`, `agent.snapshot`, `agent.changes`,
    `agent.fleet_identity`) are pinned by fixtures written here from
    constraints, Names.
13. **Rollback is honest.** A binary whose migration chain lacks the agent
    migrations refuses a migrated store as ahead of it
    (`crates/entorhinal-core/src/lib.rs:146-156`).

## constraints
This campaign moves the authority only (R7). Scoped tools, `ScopeAttributes`
and `flow_id` reading, consent, standing grants, the hire cap, a supervisor
retiring its hire and `reserved:callosum` admission are deferred (R8, R12; the
list is under non_goals). Recording the requesting device beside app-initiated
creation is deferred too (R20); it is recorded here, not in non_goals' list,
because R30 kept that section's text. Nothing below specifies them; "Keeps for
the later campaign" names the only constraints that exist so this campaign does
not rule them out. Evidence and slices use `main` as the integration ref, not a
fixed commit; slices that share a file are cut from main's tip in turn, so each
one sees the slices merged before it (R18). Citations into
`crates/entorhinal-module/src/main.rs` are at `aa2eeb0`; every other line
citation was taken at 3e06dbd37b62 in files unchanged since (Versions, "Source
changes since the evidence commit"). Facts cited to the pinned-facts file are
from `docs/designs/agent-identity-pinned-facts.md`, which is normative for
every fact it states.

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
(`main.rs:922,1065-1069`); `requires` stays empty. Agent ops carry an `agent.`
prefix so they never collide with project ops (`resolve`, `enumerate`,
`remove`, `journal_tail`). Agent op bodies are snake_case, as core's are,
except that `agent.avatar_read` and `agent.set_avatar` keep core's camelCase
`agentIds`, `agentId`, `type` and `seedOnly` (pinned-facts file, "Core's agent
op bodies"), with no snake_case aliases; project ops keep camelCase;
`agent.fleet_identity` uses the join vector's camelCase. The role strings are
core's: `assistant`, `workspace_head`, `head`, `hiree`; "hire" in prose means a
`hiree` row, and no `hire` role exists.
- Reads: `agent.resolve`, `agent.resolve_name`, `agent.list`,
  `agent.peer_roster`, `agent.avatar_read`, `agent.github_identity`,
  `agent.fleet_identity`, `agent.snapshot`, `agent.changes`.
- Mutations: `agent.create`, `agent.rename`, `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose`, `agent.merge`, `agent.import`. Each requires a non-empty
  `request_key` and accepts an optional `actor`. Project ops keep today's
  optional `requestKey`.
- Manifest: the management surface keeps every operation it lists today
  (`main.rs:924-1044`) and adds exactly the reads and mutations above. There is
  no tool surface. Any other method name, including `create_project`,
  `create_head`, `create_hire`, `dispose_agent`, `agent.set_hire_cap`,
  `grants.*` and `agent.fleet_overview`, answers today's `unknown_method`
  (`main.rs:412-418`) and writes nothing.
- Wire bodies of ops core serves today, `agent.resolve` excepted: entorhinal's
  op takes core's request field names, casing and `deny_unknown_fields` for the
  op of the same name (pinned-facts file, "Core's agent op bodies"), without
  core's caller and residence fields (`callerHarness`, `callerSession`, and
  `agent.avatar_read`'s `harness`, `session_id`, `session`,
  `caller_directory`, `caller_session`); core strips those before relaying,
  and entorhinal refuses them as unknown fields (`invalid_request`). So
  `agent.set_labels` names its agent by `agent` or `agent_id`, exactly one and
  non-empty, else `invalid_request` (`agent_registry_ops.rs:609-628`). Replies
  keep core's top-level keys. On top of that, exactly these edits: (a)
  residence, wake, delivery-derived (`bounced_deliveries`), sleep and persona
  (`persona_ref`) fields removed; a request carrying any removed field refuses
  `invalid_request`, and core's relay strips them; (b) `agent_generation`
  (core's `agent.generation` column, which no core reply carries:
  `agent_registry_ops.rs:950-989,3329-3340`) added to the agent digest (every
  `{"agent": digest}` reply and the digests in `agent.list` and
  `agent.resolve_name`) and to the `agent.github_identity` reply beside
  `github_identity` and `projectId`; no agent reply has a key `generation`
  other than the fleet pair's; (c) `incarnation` and fleet `generation` added,
  required, to every success reply, so a relay passes them through (R5); (d)
  `request_key` added to every mutation request; (e) optional
  `supervisor_agent_id` on `agent.create`; (f) optional `actor` on every
  mutation request (R27); (g) `reachability` removed from every inherited
  digest key set (R31). Core computes `reachability` from the agent's
  residence and that machine's binding (`agent_registry_ops.rs:911-937`), both
  of which stay in core, so no entorhinal reply carries it: not
  `agent.resolve`, `agent.resolve_name`, `agent.list`, the `agent.peer_roster`
  peers or the agent row. Agent replies, cached ones included, never carry
  `noop`: the shared mutation path inserts it into every object reply
  (`crates/entorhinal-core/src/mutations.rs:343-347`) and agent handlers do not
  apply that insertion. `github_identity` is stored and served as core's
  tagged `GithubIdentity` JSON (`app_slug`, `installation_id` and
  `credential_ref` inside it), which holds no secret. Avatar is stored as
  genome, type and version under core's CHECK `(avatar_genome IS NULL) =
  (avatar_type IS NULL)` (`112_agent_avatar.sql:15-16`) plus entorhinal's
  CHECK `avatar_genome IS NOT NULL OR avatar_version IS NULL`, so an unset
  avatar is exactly the wire `avatar: null`; the fingerprint is computed at
  serve time (`fleet_overview.rs:96-109`).
- Inherited outcomes kept as core serves them (R27, R34): `agent.resolve_name`'s
  no-match and ambiguous results are success replies `{"refused": {code,
  details}}` with `code` `name_unknown` or `name_ambiguous`, plus the fleet
  pair, never `ErrorWithDetail` (`agent_registry_ops.rs:1185-1289`);
  `agent.list` defaults `limit` to 50 and refuses a `limit` outside 1..=200 or
  an empty `cursor` with `invalid_cursor` (`agent_registry_ops.rs:3107-3119`);
  `agent.github_identity` naming an absent or malformed id refuses
  `unknown_agent`, a terminal agent `gone`. `agent.dispose` of a retired agent
  and `agent.merge` of a merged agent into its recorded `merged_into` succeed
  with the gone object, as core's handlers do
  (`agent_registry_ops.rs:3370-3378,3407-3414`), changing no column; dispose
  of a merged agent, merge of a retired one, or merge of a merged one into any
  other target refuses `gone`.
- `agent.list`'s `activated_only` tests entorhinal's `agent.cutover` marker in
  place of core's activation marker, which stays in core (core returns
  `{"agents": []}` when its marker is absent, `agent_registry_ops.rs:3132-3137`):
  `activated_only: true` before the marker returns `{"agents": []}` plus the
  fleet pair; after it, and `activated_only: false` at any time, lists the
  stored rows under the other filters.
- `agent.peer_roster` serves core's deployed behaviour (R32): core routes it
  to `dispatch_peer_roster_with_projects` (`manager_runtime.rs:3827-3839`,
  `agent_registry_ops.rs:1760-1777`), the synchronous
  `workspace_membership_unavailable` arm being only a fallback. It decodes
  `{workspace_id}` with `deny_unknown_fields` and validates the id. Before the
  `agent.cutover` marker it refuses `registry_not_activated` ("agent registry
  identity is not activated"), as core does without its activation marker
  (`agent_registry_ops.rs:1466-1478`). Membership
  (`workspace_roster_from_enumeration`, `agent_registry_ops.rs:1585-1626`) is
  the live `workspace_head` rows whose `workspace_id` is that workspace plus
  the live `head` rows whose project is placed in that workspace, placement
  read from entorhinal's own store; no hirees and no assistants; ordered by
  `agent_id`. The reply is `{"peers": [...]}` plus the fleet pair; each peer
  (`encode_peer`, `agent_registry_ops.rs:1450-1464`) has `agent_id`, `name`,
  `tag` and `role`, plus `project_id` and `github_identity` only when set, and
  no `reachability` (R31).
- Presence, replies: fields core already serves keep core's rule; every field
  this spec adds to a snake_case success reply, `agent.snapshot` row, claim or
  change entry is always present and JSON null when empty;
  `agent.fleet_identity` omits absent optionals (as `fleet_overview.rs:53-83`
  does) and omits `agents` when unchanged.
- Presence, requests: optional request fields may be omitted, and JSON null
  means the same as omission: `actor` absent or null records `module`;
  `supervisor_agent_id` absent or null means none; `agent.changes` `limit`
  defaults to 500 and `wait` to true; `agent.fleet_identity` `token` absent is
  a full reply. Only a removed or unknown field or a malformed value refuses
  `invalid_request` (`agent.list`'s `invalid_cursor` excepted).
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
    The join vector asks for "the shape `session.transcript_page` already
    uses" without naming fields (`docs/designs/fleet-view-join-vector.md:211-215`);
    the slice shipping this op writes these fields, this spec's invalidation
    rule and the restart rule (Generations) into that section.
    Lists non-terminal agents only (as `fleet_overview.rs:254-266` does), one
    row per agent whatever the number of project roots,
    `project.canonicalRoot` the project's first root in `canonical_root` order,
    `workspace` derived through the project's placement only (a workspace_head
    omits it, `fleet_overview.rs:364-379`), and omits residence, board,
    `pendingAskCount` and `latestActivityMs`. This is the only read that
    derives workspace through placement; `agent.list`, `agent.snapshot`,
    change entries and the stored column carry the row's stored `workspace_id`.
- Agent row (an `agent.snapshot` element and a change entry's `row`), the same
  for live and terminal rows: exactly these keys, each always present and JSON
  null when empty: `agent_id`, `name`, `name_version`,
  `name_normalization_version`, `tag`, `labels` (a JSON array, `[]` when
  none), `role`, `project_id`, `workspace_id` (the stored value), `avatar`
  (an object `{genome, type, version}`, or null when no avatar is set; `type`
  is never null inside it, `version` null when absent), `github_identity`
  (core's tagged `GithubIdentity` JSON, or null), `status`
  (`live|retired|merged`), `merged_into`, `supervisor_agent_id`,
  `request_key`, `created_at_ms`, `updated_at_ms`, `terminal_at_ms`,
  `agent_generation`. It carries no other key: not core's digest-only
  `created_at` or `reachability`, and no removed field. Claim: `{claim_id,
  agent_id, namespace_kind, namespace_key, normalized_name,
  name_normalization_version, display_name, claimed_at_ms, released_at_ms}`
  (core's `076_agent_registry.sql:76-91`; the version is part of the
  active-claim key). A decoder test pins both key sets exactly, `role`
  included with the `hiree` string.
- Resolve reply (new shape): `{agent_id, status, merged_into, gone,
  incarnation, generation}`, `status` one of `live`, `retired`, `merged`,
  `unknown`, `merged_into` null unless merged, `gone` core's gone object
  (Terminal state) for a retired or merged id and null otherwise. A malformed
  id resolves `unknown`, as core's lookup answers it not found (R35). Its
  request body is core's (R27). Storage failures keep the existing handler
  errors (`main.rs:340-359`): `storage_unavailable` when the store is not open,
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
- Error codes (R34). Core's relay passes entorhinal's replies through, so for
  any condition core's op of the same name already refuses, entorhinal returns
  the code core's handler serves for it: `AgentRegistryError::code`
  (`prefrontal-core-store/src/agent_registry.rs:843-873`) plus the codes
  `agent_registry_ops.rs` raises itself. Used here: `invalid_name` (bad name),
  `name_conflict` (active claim collision), `agent_project_taken` (a second
  live head), `unknown_agent` (an absent or malformed agent id), `gone` (a
  terminal agent), `merge_self` (merge into itself, raised by core's handler
  before the store, `agent_registry_ops.rs:3417-3422`),
  `merge_target_not_live`, `invalid_tag`, `invalid_labels`,
  `invalid_github_identity`, `invalid_project_id`, `invalid_workspace_id`
  (each under core's validator for that id), `invalid_role_shape`
  (`agent_registry_ops.rs:3038-3049`), `invalid_request`, `invalid_cursor`
  (`agent.list` only), `registry_not_activated` (`agent.peer_roster` only,
  R32), and `unresolved_workspace` and `activation_gap` (R22). New codes exist
  only for conditions core has no counterpart for: `authority_not_cut_over`,
  `direct_identity_write_not_admitted`, `request_key_required`,
  `invalid_supervisor`, `bound_by_live_agent`, `import_already_done`,
  `import_invariant_failed`, `snapshot_required`.
  Existing codes are reused unchanged: `invalid_request` (`main.rs:373-381`),
  `write_not_permitted` (`main.rs:259-274`), `flow_scope_not_admitted`
  (`main.rs:285-295`), `request_key_reused_across_ops`
  (`mutations.rs:320-336`), `not_found` (`mutations.rs:466`, naming the missing
  project or workspace id), `entropy_unavailable` (`binding.rs:80-84`),
  `encode_failed`, `unknown_method`. Project ops keep `invalid_params` for a
  bad body (`main.rs:855-860`); agent ops never use it and refuse a bad body
  `invalid_request`.
- Refusal detail (R19). The pinned SDK has `HandlerOutcome::ErrorWithDetail {
  code, message, detail }`, which the serve loop sends as `ErrorBody.detail`.
  Every refusal listed here returns its fields through `ErrorWithDetail`, and
  the wire field is `detail`, never only prose in `message`; refusals with no
  listed fields keep `HandlerOutcome::Error`. The test seam returns `detail`
  and refusal fixtures pin it. Values are strings, numbers (`at`), arrays of
  strings, or JSON null; every key listed is always present:
  - `agent_project_taken`: `{agent_id, request_key}`, the occupying head;
    `request_key` null for an imported head.
  - `gone`: core's gone object (Terminal state).
  - `import_invariant_failed`: `{check, ...}` by check:
    `agent_id` → `{check, agent_id}` (the bad id);
    `claim_mismatch` → `{check, agent_id}`;
    `claim_owner` → `{check, claim_id, agent_id}` (the claim and the absent
    owner id it names);
    `live_head` → `{check, project_id, agent_ids}`, `agent_ids` a JSON array of
    strings, every live head id of that project in ascending byte order; with
    several offending projects, the one holding the smallest offending
    `agent_id`;
    `merged_into` → `{check, agent_id, merged_into}` (the source row and the
    missing target);
    `binding` → `{check, agent_id, project_id, workspace_id}`, the id that
    failed set and the other null.

**Two rules that predate this work.** The daemon never depends on entorhinal,
because it supervises it. Paths compare with the daemon's canonical path
function and are never re-normalised on entorhinal's side. Any change touching
either goes to SUBC for review before it ships.

**Versions.** This campaign starts from subc-protocol 0.29.0 and subc-client-rs
0.26.1, as shipped in entorhinal 0.1.14 (R11), and does not change either pin;
`Cargo.toml` and `Cargo.lock` pin exactly those (R13), the 0.28.1 / 0.25.2
record being stale. Every slice is cut from `main`, which contains 192f220
(R18). The name port adds `icu_normalizer` and `icu_casemap` pinned at
`=1.5.0`, new to entorhinal; R11's ban on pin changes covers only the subc
crates (R23).

**Source changes since the evidence commit.** Between 3e06dbd37b62 and
`ff07aff` the only source change is the session-liveness receiver in
`crates/entorhinal-module/src/main.rs` (refusing older batches, the
`livenessDroppedOlder` gauge, a mirror that starts stale until a snapshot
arrives) (R14). Between `ff07aff` and `aa2eeb0` the only source changes are in
that same file, plus the 0.1.15 version bump (R15): `7657d38` added flow
admission, where at `route.bind` the handler records a `RouteAdmission` holding
the route's principal and the bind stamp's `flow_id` (`main.rs:203-218,
430-436`), and `ProjectsHandler::admit` is the single admission point, running
`refuse_flow_write` and then `authorize_write` (`main.rs:334-338`); `aa2eeb0`
reworded the refused-liveness-batch warning. The liveness receiver and its
warning are outside this campaign, and slices leave them unchanged.

**Flow-scoped routes never reach an identity mutation (R16).** Every agent
mutation in Names is admitted through `ProjectsHandler::admit` with
`refuse_flow_write` as its first check, and is listed in the methods
`refuse_flow_write` refuses (`MUTATING_METHODS`, `main.rs:232-248`), so a
flow-scoped route answers `flow_scope_not_admitted` whatever its principal,
`reserved:prefrontal-core` included. Entorhinal does not declare
`flow-scopes/v1`; the daemon keeps flows away, and this refusal is the second
line. Reading the calling agent from the scope stamp stays deferred to the
second campaign (R12).

**Agent ids are minted the opposite way to project ids.** Project ids are
deterministic and have no permanent tombstone. Agent ids are random, never
derived from name or path, and permanently tombstoned, because a reused id
would inherit the previous holder's grants, bot ownership and approvals. New
ids are `agent_` plus 16 lowercase hex (8 random bytes from the store's id
source, `binding.rs:80`), minted in the create transaction, as core's create
path mints them from `randomblob(8)` (`prefrontal-core-store/src/lib.rs:1307-1310`,
called at `agent_registry.rs:1846`); core's seed ceremony uses `agent_` plus
the first 16 hex of a SHA-256 (`agent_registry.rs:3579-3583`). Ids move
unchanged in the import. Validation is syntactic: `agent_` plus 16 or 8
lowercase hex is well-formed; only imported rows have the 8-hex form, and
core's live store holds 34 16-hex ids and exactly one 8-hex id,
`agent_16013c86` (R37). The import refuses a source id of any other form
(check `agent_id`). A malformed id names no agent, as core's lookup answers it
not found (R35): `agent.resolve` returns `unknown` and a mutation or
`agent.github_identity` naming it refuses `unknown_agent`. A well-formed id
with no row resolves `unknown`, and a mutation naming it refuses
`unknown_agent`.

**Terminal state, one schema.** Column `terminal_reason` is NULL for a live
agent, else `retired` or `merged` (CHECK IN those two; core's `deleted` is not
stored); `merged_into` is non-NULL exactly when `terminal_reason = 'merged'`.
Wire `status` is `live` when `terminal_reason` is NULL, otherwise its value. The
import maps core's `deleted` → `retired` before insert. A retired or merged id
stays resolvable with its status, never collapsing into unknown: core keeps
tables referencing agent ids with no database constraint
(`session_scope.agent_id`, `flow_install.author_agent_id`,
`flow_install_grant.agent_id`, `gh_route_speech_receipt`,
`persona_active_revision`). Gone objects (R36): wherever a reply or refusal
carries core's `gone` object (`agent.resolve`, `agent.list`, `agent.dispose`,
`agent.merge`), it has exactly `encode_gone`'s keys
(`agent_registry_ops.rs:992-1001`): `{"reason": "deleted", "at": <ms>}` for a
retired agent and `{"reason": "merged", "at": <ms>, "into_agent_id": <id>}`
for a merged one, `at` being `terminal_at_ms`, with no generation. The snapshot
and change feed keep the `status` vocabulary, where `retired` corresponds to
`deleted`.

**Identity invariants carried over verbatim.** `agent_id` is stable for life.
Names are NFC for display and case-folded for lookup by a port of core's
`normalize_agent_name` with the same pipeline, crates and versions:
`icu_normalizer` and `icu_casemap` at `=1.5.0` (Unicode 15.1),
`normalization_version` 1, and at most 24 scalars (R23). The port keeps core's
Rust surface (`normalize_agent_name(&str) -> Result<NormalizedAgentName, _>`,
reasons `Empty`, `TooLong`, `DisallowedCharacter { codepoint }`, code
`invalid_name`) so core's pure normaliser test compiles unchanged; the handler
returns that code, core's `invalid_name`, on the wire, and an active collision
refuses `name_conflict` (R34). Rename of a head or hiree first requires its
project to be placed, refusing `unresolved_workspace` or `activation_gap` as
core's rename does (`agent_registry_ops.rs:3198-3205`); it then claims the new
name in the namespace of the agent's active claim, not one recomputed from
current placement (`agent_claims.rs:248-254`), releases the old claim
atomically, and sets `name_version` to its previous value plus 1
(`agent_claims.rs:262-266`); this holds when that namespace's workspace has
since been removed. No other mutation changes `name_version`. One active claim
per namespace and per agent; released claims keep history. Dispose and merge
set the terminal row and `terminal_at_ms` and release the active claim in one
transaction; a mutation of a terminal agent refuses `gone`, except the two
idempotent successes under Names' inherited outcomes; merge into self refuses
`merge_self`, and into a terminal, absent or malformed target
`merge_target_not_live`, before anything is released
(`agent_claims.rs:289-339`). At most one live head per project,
schema-enforced as an index on `(project_id) WHERE role = 'head' AND
terminal_reason IS NULL`, as core's `uq_live_head`; a violation surfaces as
`agent_project_taken`, never `storage_error`.

**Tags, labels and avatars (R38).** Entorhinal applies core's rules
(`agent_registry.rs:1324-1361`, constants at `30-32`) with core's codes: a tag
is trimmed under Unicode 15.1, non-empty and at most 256 bytes, else
`invalid_tag`; labels number at most 16, each trimmed, non-empty and at most 32
scalars, no two equal after ICU case folding, else `invalid_labels`, and are
stored trimmed in request order; the only known avatar type is
`creature.classic`, whose genome is exactly 2048 hex characters
(`agent_registry_ops.rs:835-842`), and an unknown type refuses
`invalid_request`. `agent.set_avatar` with `seedOnly: true`
(`agent_registry.rs:3112-3169`) writes only when the agent has no genome;
otherwise it returns `applied: false` with the stored avatar and changes no
column (Journal op and request key).

**Create shape checks, in the handler.** As core does
(`agent_registry_ops.rs:3038-3049`), a create refuses `invalid_role_shape`
unless: assistant has `project_id` and `workspace_id` NULL; workspace_head has
`project_id` NULL and `workspace_id` set; head and hiree have `project_id` set
and no `workspace_id` in the request. A named `project_id` resolves through one
alias hop, as project `resolve` does (`crates/entorhinal-core/src/lib.rs:296`),
and the current id is stored; an id resolving to no current project, or a
workspace_head's `workspace_id` that is not a current workspace, refuses
`not_found`. As core does (`agent_registry_ops.rs:1129-1144`), a head or hiree
create on a project with no workspace placement refuses `unresolved_workspace`,
and one whose placement read (`project_workspace`, `workspace`) fails after the
project resolved refuses `activation_gap` in place of `storage_error`,
entorhinal's store being its workspace authority; both refuse before anything
is claimed (R22). The name namespace is `assistant`/`"global"` for an
assistant, `workspace`/its own `workspace_id` for a workspace_head, and
`workspace`/the project's resolved workspace for a head or hiree, which is also
stored as that row's `workspace_id`. An imported row keeps its stored
`workspace_id`; since 082 a head or hiree may hold a non-NULL one, so no CHECK
forces NULL or NOT NULL for those roles.

**Supervisor.** Core has no supervisor, so its codes are this campaign's (R34).
A `supervisor_agent_id` that is not `agent_` plus 16 or 8 lowercase hex refuses
`invalid_supervisor` for any role (step 5a). For a well-formed id: a hire
create records it only when it is the live head of the hire's project, else
`invalid_supervisor`; a create of any other role naming one refuses
`invalid_supervisor`. A hire naming none records NULL; head, assistant and
workspace_head rows have NULL supervisor. The import reads no supervisor: core
records none anywhere (`hire_identity_mapping`, 079, holds only `hire_id`,
`agent_id`, `hire_class` and `created_at`), so every imported row stores NULL,
and an imported hiree with a NULL supervisor is valid (R21). The supervisor
never changes after create.

**No personas, residence, wake or sleep.** The `agent` table has no
`persona_ref`, `residence_*`, `wake_policy_json`, `wake_policy_version` or
`sleep` column (core `076_agent_registry.sql:9-18`). The import drops those
values, `persona_ref` included, whatever they hold, and still commits; core
copies persona data out before dropping its identity tables (dependency).

**Generations.** Two values, never sharing a name:
- **`agent_generation`**, core's per-agent `agent.generation` (migration 109),
  signed as `binding_generation` (`agent_assertion.rs:352`). Only 109's column
  is carried; its `agent_generation_on_mutation` trigger is not installed. A
  create writes 1; each later mutation that changes a value of that agent's
  row writes its previous value plus 1, once, in the handler; the import
  carries core's value. The written value is in the journal entry and replay
  assigns it rather than incrementing. Mutations of other agents, projects or
  workspaces, and a no-change success (Journal op and request key), do not
  change it. Core's bot token embeds it with the incarnation.
- **Fleet `(incarnation, generation)`**, `generation` the journal head
  `MAX(seq)`. It versions reads, the feed, relay replies and the fleet view's
  token. Because the token is the fleet pair, any journal append invalidates
  it, project ops included (a project rename or workspace move is never
  missed; an extra invalidation from an unrelated op is accepted). This rule is
  this spec's, not the join vector's.
The **incarnation** is a JSON string of 16 lowercase hex, 8 random bytes drawn
the way the id source draws them (`crates/entorhinal-core/src/binding.rs:79-92`),
minted at process start and never persisted (not in a column, journal payload,
cached reply or the marker), because restoring an older backup lowers both
generations. The test seam injects it, as the id source's counter does for
binding tokens, so pinned replies are byte-stable and a simulated restart
supplies a new value. Per R7 it is attached at serve time to every reply that
carries a generation: every agent-op success reply, and the project reads
`resolve`, `resolve_project_id`, `enumerate`, `journal_tail`
(`crates/entorhinal-core/src/lib.rs:436-468`), `trust` (`main.rs:805-810`, a
`ResolveReply`) and `verify` (`mutations.rs:201-206`), as a required camelCase
`incarnation` beside their `generation`. For project reads it is inserted by
`ProjectsHandler` into the encoded `result` object after `entorhinal-core`
serialises it; core's reply structs, the legacy-shape promise on
`RegistryStore.root_records` (`crates/entorhinal-core/src/lib.rs:123-126`) and
the core goldens (`crates/entorhinal-core/tests/golden/resolve/legacy-resolve-root.json`,
`legacy-enumerate.json`, `enumerate.json` and the `resolve-*.json` files) stay
unchanged. At the module wire, a project read reply is core's bytes plus that
one additive key. Project mutation replies keep today's shape. Of those six
reads, prefrontal-core decodes only `resolve` and `enumerate`, and neither
decoder uses `deny_unknown_fields`, so the extra `incarnation` is ignored; it
decodes none of the other four (pinned-facts file, R25). Every restart
invalidates outstanding tokens and cursors; the join-vector amendment (Names,
`agent.fleet_identity`) states it. A same-key retry served from the cache, the
import's included, returns the `generation` recorded at commit
(`mutations.rs:338-347`) with the current incarnation.

**The journal records the attested principal.** `registry_journal` gains
`principal` and keeps `actor` (`crates/entorhinal-core/src/lib.rs:41-49`).
`principal` is the principal recorded for the route at bind
(`main.rs:203-218, 430-436`) written as `principal_label` spells it
(`main.rs:297-304`): `direct`, `reserved:<module_id>` or `unverified`. Every row
a request causes records that request's route principal, including secondary
rows the handler appends in the same call (the `bind_root` rows after
`register` and `add_root`, `main.rs:724-775`). A row appended with no request,
the startup `bind_unbound_roots` (`main.rs:1146-1151`), records the literal
`entorhinal`. `principal` is NULL only on rows journaled before the migration.
`actor` is the request's `actor` field, defaulting to `module` as today
(`mutations.rs:390`). No `origin` column is added: nothing in the pinned SDK
(`RequestCtx`, `RouteBindRequest`, `ScopeStamp`, `ScopeAttributes`,
`Principal`) carries a Callosum origin, and nothing here rules out adding the
column later (R20). No request digest is stored in this campaign.

**Journal op and request key.** An agent row's `op` is the request's management
op; project and binding rows keep the op names the store writes today
(`bind_root`, `approve_root`, `add_root` and the rest, `binding.rs:654,757-764`),
and their replay is unchanged. Agent mutations use today's body-insensitive
cache: a same-op, same-key call returns the stored reply
(`mutations.rs:312-314`); a key journaled under another op refuses
`request_key_reused_across_ops` (`mutations.rs:320-336`). Every successful agent
mutation journals exactly one row carrying its key and cached reply, including
a no-change success: `agent.set_avatar` with `seedOnly` true on an agent that
already has an avatar (`applied: false`); `agent.update_tag`,
`agent.set_labels`, `agent.set_avatar` or `agent.set_github_identity` writing
values equal to the stored ones, as core's trigger then leaves `generation`
(`126_agent_labels.sql:36-61`); and the idempotent dispose and merge (Names).
The shared path journals and caches only changed actions
(`mutations.rs:349-358`), so agent handlers report every success as changed. A
no-change row's change entry carries the unchanged row; `agent_generation`,
`name_version` and `updated_at_ms` stay as they were and the fleet generation
advances. Rows journaled before this migration (NULL `principal`) behave as
today. The import journals one `agent.import` row per imported agent (request
key NULL), then the `agent.cutover` marker row carrying the import's request
key and cached reply (counts and generation), all in one transaction; the cache
and cross-op check treat the marker row as `agent.import`'s, so a zero-agent
import is cached too. The marker exists only as that journal row: step (4),
`activated_only` and `agent.peer_roster` test for a `registry_journal` row with
op `agent.cutover`. `rebuild` deletes projections and replays the journal
(`mutations.rs:757-758`); it adds `agent` and `agent_name_claim` to its delete
list, replays every agent op, and leaves the marker row in place. A refused
call writes no row, so a rerun may reuse its key.

**Credential references, never credentials.** `credential_ref` points into
claustrum. No secret moves into entorhinal.

**Admission and check order for agent mutations (R9).** Each step runs only if
the previous passed; the first refusal wins, writes no journal row and returns
no cached body. A check that does not apply to the op is skipped, not failed.
(0) Envelope decode as today (`main.rs:373-381`): not JSON with `method` and
`params` → `invalid_request`.
(1) Admission in `ProjectsHandler::admit`, before and after the marker. First
`refuse_flow_write`: a route whose bind recorded a `flow_id` →
`flow_scope_not_admitted`, whatever its principal (R16). Then, for an agent
mutation, a principal rule in `admit` itself, in place of `authorize_write`
(which keeps the project gate and its "Direct admitted" doc for project
methods only; the existing test iterating `MUTATING_METHODS` through
`authorize_write`, `main.rs:1229-1240`, is narrowed to the project methods):
`reserved:prefrontal-core` continues; `Direct` →
`direct_identity_write_not_admitted`, the message naming the refused op and
saying to call it through prefrontal-core's relay (any local process, an
agent's shell included, reaches entorhinal as `Direct`); every other principal
(`reserved:callosum`, other reserved modules, `Unverified`, a route with no
recorded principal) → `write_not_permitted`. This holds for a key already
committed and for any body, a bad `snapshot_path` included: a refused call
gets its route's code, never the cached reply. Beyond `refuse_flow_write`'s
check of the route's recorded `flow_id`, the handler reads no `flow_id` or
`ScopeAttributes`.
(2) Params decode: a malformed body, a missing required field of the op (an
import without `snapshot_path` included), an unknown field or a removed field
→ `invalid_request`, also when the key was already committed. `request_key`
and `actor` decode as optional strings, so their absence never refuses here.
(3) Request key: missing or empty → `request_key_required`; otherwise the
journal cache above, the marker looked up as `agent.import`. A same-key import
rerun is served here, also after a restart, never `import_already_done`.
(4) Marker: R9 refuses every agent-identity mutation before the marker; the one
exception is `agent.import`, because R7's import is what writes the marker.
Absent → `authority_not_cut_over` for every other mutation; present and op
`agent.import` → `import_already_done`, nothing written, whatever its
`snapshot_path`.
(5a) `agent.create`, in core's order: project and workspace id validity
(`invalid_project_id`, `invalid_workspace_id`; core checks them before role
shape, `agent_registry_ops.rs:3026-3037`), role shape (`invalid_role_shape`),
name validity (`invalid_name`), tag (`invalid_tag`; core validates it after the
name, `agent_registry.rs:1784-1788`), project and workspace resolution
(`not_found`), a head's or hiree's workspace placement
(`unresolved_workspace`, `activation_gap`), supervisor (`invalid_supervisor`,
a malformed id included), name collision in the resolved namespace
(`name_conflict`), one live head (`agent_project_taken`).
(5b) Every mutation naming an existing agent: existence (`unknown_agent`, a
malformed id included), terminal state (`gone`, its `detail` the gone object,
except the idempotent dispose and merge), then the op's own checks: rename, a
head's or hiree's project placement (`unresolved_workspace`,
`activation_gap`), name validity (`invalid_name`), then collision
(`name_conflict`) in the active claim's namespace; merge, target self
(`merge_self`), then target terminal, absent or malformed
(`merge_target_not_live`); tag, labels, avatar and GitHub identity, core's
bounds (`invalid_tag`, `invalid_labels`, `invalid_request`,
`invalid_github_identity`). So a rename of an absent or terminal agent to a
taken name refuses `unknown_agent` or `gone`; a merge of an absent source
refuses `unknown_agent` and of a terminal source `gone` (bar the idempotent
case), whatever the target.
(5c) `agent.import`: the `snapshot_path` file refusal, then the import
invariants below, refused only as `import_invariant_failed`.
Project and workspace operations, `rebuild` included, do not use this order and
keep today's gate (`main.rs:259-274`): a project or workspace mutation is
admitted from `Direct` or `reserved:prefrontal-core`, before and after the
marker, anything else `write_not_permitted`; project reads (`resolve`,
`resolve_project_id`, `enumerate`, `journal_tail`, `trust`, `verify`) are
admitted from every principal, a route with no recorded principal included
(`main.rs:260-261`).

**Reads.** Identity reads are admitted for every principal, as queries are today
(`main.rs:259-262`), and answer before the marker as after it, except
`agent.peer_roster`, which refuses `registry_not_activated` before the marker
(R32); before the marker, a resolve of a well-formed absent id returns
`unknown`, never `authority_not_cut_over`, and an empty store is a success
(R10).

**The import.** It reads `agent` and `agent_name_claim` from the source, and
not `hire_identity_mapping` (R21). One definition by table presence: no
`agent` table is a zero-agent source whatever else exists, and both counts are
0; `agent` present with no `agent_name_claim` reads as zero claims, so any live
agent fails `claim_mismatch`. Every imported row stores request key NULL and
supervisor NULL, keeps its stored `workspace_id`, and copies `created_at_ms`,
`updated_at_ms`, `terminal_at_ms`, `name_version` and
`name_normalization_version` unchanged (core `076_agent_registry.sql:10,19-22,
29`). One value is dropped: a row with NULL genome keeps NULL
`avatar_version` whatever the source holds, since 112 constrains only genome
and type and a version without a genome is no avatar. Before committing it
runs these checks in this order, else refuses `import_invariant_failed` with
nothing written, reporting the first failing check and, within it, the
offending row with the smallest `agent_id` (or `claim_id` for `claim_owner`):
- `agent_id`: every `agent_id` is `agent_` plus 8 or 16 lowercase hex;
- `claim_owner`: every `agent_name_claim` row, active or released, names an
  `agent_id` present in the source `agent` table, so every counted claim is
  carried by its owner's `agent.import` entry; an `agent` table present but
  empty with any claim row fails here, never reads as a zero-agent source;
- `claim_mismatch`: each live agent has exactly one claim with
  `released_at_ms` NULL, whose `normalized_name` equals re-normalisation of the
  row's `name`, whose `display_name` equals `name` and whose
  `name_normalization_version` equals the row's (namespace not compared);
  zero such claims fails too;
- `live_head`: at most one live head per project (core's `uq_live_head`
  already prevents this in a genuine copy; the check is defence in depth);
- `merged_into`: every `merged_into` names a source row;
- `binding`: every non-terminal row's non-NULL `project_id` is a row of the
  destination entorhinal store's `project` table (an alias does not satisfy it)
  and every non-terminal workspace_head's `workspace_id` a row of its
  `workspace` table; the source has no project or workspace tables to consult.
  The destination project need not be placed: a live head may import onto an
  unplaced project.
Terminal rows import as they are, because `remove` legitimately leaves such
rows. A successful import, from a zero-agent source too, commits its rows and
the marker; the marker, not the row count, ends `authority_not_cut_over`.
`rebuild` keeps today's gate.

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
key, recorded on the row as an opaque correlation token. `agent_project_taken`
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
  commits or 25 s pass, woken by a notification signalled on every journal
  commit (not by a poll interval), re-reading under a short lock and holding
  none while it waits (today every op runs under the store mutex,
  `main.rs:340-359`). A woken call returns the committed identity entry
  without waiting out the timeout. A timeout is a success: current
  incarnation, empty `entries`, the request cursor.
- Wait capacity (R26): the SDK runs each request in its own task, with at most
  64 handler tasks and no per-request deadline, and its default call timeout is
  30 s. A held wait occupies one handler task and no store lock. At most 8
  waits are held at once; a 9th returns immediately with whatever is ready,
  which may be nothing, rather than refusing.
- `agent.snapshot` returns every agent row (terminal included) and every claim
  (active and released) with the pair read in the same transaction.
- An `incarnation` that is not current, or a cursor above the head, answers
  `snapshot_required` with no entries; a same-incarnation gap is catch-up.
- Identity consumers use only this feed. `journal_tail`
  (`crates/entorhinal-core/src/lib.rs:436-468`) returns project-op rows only,
  `generation` read in the same transaction; a reply with fewer rows than
  `limit` advances the caller to `generation`, a full one to its last row's seq.
  The `agent.cutover` marker is neither a project op nor an identity entry, so
  neither feed returns it and both cursors pass over it.
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
store; in the same cut the store and the authority move to entorhinal, and
core's identity write ops become relays (R4; dependency item). Core's identity
read ops that running host plugins call keep serving from its replica until a
bundle without them is running. An install whose core store never applied
migration 076 obtains the marker by one zero-agent import; entorhinal cannot
tell that from a premature one, so core's trigger excludes a store that once
held identity tables.

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
    included, sends a `request_key` and strips removed fields and core's caller
    and residence fields. The replica and relay callers read the per-agent
    value from `agent_generation`, never from the fleet `generation`. Relay
    contract (R5): refusals pass through verbatim, never wrapped; the reply
    carries the new `(incarnation, generation)`; the replica may lag a
    same-turn read by one poll, and the generation lets a caller tell;
  - `reachability` stays core's (R31): residence and machine bindings stay in
    core, so entorhinal's replies omit it, and core decides whether its own
    relay or replica-served reads add it back;
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
3. **The cut, in one restart window:** stop core cleanly and copy its store
   after that close, with any `-wal` and `-shm` files beside it, because a
   read-only open of a copy missing them reads a stale image; place core's
   build with the replica, without its identity tables (its ledger keeps 076
   applied), with its identity write ops turned into relays; the operator
   calls core's import relay with the copy's path; until the import commits the
   marker every relayed write refuses `authority_not_cut_over`; release the
   phone build. A refused import leaves no rows and no marker: on
   `import_invariant_failed` the operator repairs the copy by SQL from the
   reply's `detail` (`check` and ids) and reruns, or restores the step-1
   backup and the previous core build. A path naming a valid SQLite file with
   no `agent` table imports as zero-agent and commits the marker; recovering
   from such a wrong path means restoring the step-1 backup.
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
  The facts once left to be quoted from core's repository are recorded in
  `docs/designs/agent-identity-pinned-facts.md` and settled by R18 to R27; the
  `session.transcript_page` unchanged-token fields belong to core's half of the
  fleet view, not to this campaign.
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

### recorded supplied ruling 14

Campaign `ct_00000000-0000-4027-98db-bb64f1513a68`, round 3, decision sequence 1 (source: {"body_bytes":1738,"boundary":"ledger_payload","order":1,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4027-98db-bb64f1513a68-20261005T001822Z.md"}):

These rulings move the campaign onto the current HEAD for one final review round, then the slice plan. The scope cut in R7, R8 and R12 stands unchanged.

R15 Read evidence and cut slices at HEAD `aa2eeb0`, not `ff07aff`. Revise only the constraints section for this ruling. Add one constraint stating that, between `ff07aff` and `aa2eeb0`, the only source changes are in `crates/entorhinal-module/src/main.rs` (plus the 0.1.15 version bump): `7657d38` added flow admission (at `route.bind` the handler records a `RouteAdmission` holding the route's principal and the bind stamp's `flow_id`; `ProjectsHandler::admit` is the single admission point, running `refuse_flow_write` and then `authorize_write`), and `aa2eeb0` reworded the refused-liveness-batch warning, which stays out of scope under R14.

R16 Revise the constraints and acceptance_sketch sections for this ruling. Constraint: every new agent-identity mutating method is admitted through `ProjectsHandler::admit` with `refuse_flow_write` as its first check, and is listed in the methods `refuse_flow_write` refuses, so a flow-scoped route can never reach it. Entorhinal does not declare `flow-scopes/v1`; the daemon keeps flows away, and this refusal is the second line. Reading the calling agent from the scope stamp stays deferred to the second campaign (R12). Acceptance: a test asserts that each new mutating method answers `flow_scope_not_admitted` on a flow-scoped route, even with the `reserved:prefrontal-core` principal.

R17 The intent, non_goals and open_questions sections need no revision for R15 to R17: keep their current text exactly. The earlier "findings lack detail" decision is closed, and no question is open. Findings in this round must state their detail.

### recorded owner answer 16

Campaign `ct_00000000-0000-400c-98da-67aadcc367a8`, round 3, decision sequence 0 (source: refire_from:ct_00000000-0000-4027-98db-bb64f1513a68):

fold and mint as-is

### recorded supplied ruling 17

Campaign `ct_00000000-0000-400c-98da-67aadcc367a8`, round 3, decision sequence 1 (source: {"body_bytes":6559,"boundary":"ledger_payload","order":4,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-400c-98da-67aadcc367a8-20261005T005918Z.md"}):

These rulings close the facts the spec left "to be quoted" and settle the slice plan's fence overlap. They come from source, read on 2026-10-05 and recorded in `docs/designs/agent-identity-pinned-facts.md` (entorhinal `11cb679`), which is normative for every fact it states. The scope cut in R7, R8 and R12 stands unchanged. The evidence now includes the prefrontal ranges that file cites.

R18 Evidence and slices use `main` as the integration ref, not a fixed commit. Slices that share a file are cut from main's tip in turn, so each one sees the slices merged before it. Revise constraints only: replace the R15 sentence naming `aa2eeb0` as the cut with this, and keep the list of what changed between `ff07aff` and `aa2eeb0`.

R19 Refusal detail. The pinned SDK has `HandlerOutcome::ErrorWithDetail { code, message, detail }`, which the serve loop sends as `ErrorBody.detail`. Revise constraints and acceptance_sketch: every refusal whose spec lists structured fields returns them through `ErrorWithDetail`, and the wire field is `detail`. Rename the refusal "data" to "detail" throughout, and remove the open item about how it is serialised. Refusals with no listed fields keep `HandlerOutcome::Error`. A decoder fixture reads `detail` from the error body.

R20 No origin. Nothing in the pinned SDK carries a Callosum origin: not `RequestCtx`, `RouteBindRequest`, `ScopeStamp`, `ScopeAttributes` nor `Principal`. Revise constraints and acceptance_sketch: remove the journal's `origin` column, the handler's `origin` input and every test case about origin. The journal records `principal` and `actor`. Recording the requesting device moves to the second campaign, next to app-initiated creation (add it to the R12 list). Nothing in this campaign may rule out adding the column later.

R21 No supervisor in core. `hire_identity_mapping` (079) has only `hire_id`, `agent_id`, `hire_class` and `created_at`, and core records no supervisor anywhere. Revise constraints and acceptance_sketch: the import reads no supervisor and doesn't read 079. Remove the `supervisor` import check and its cases. Every imported row has a NULL supervisor, and an imported hiree with a NULL supervisor is valid. Supervisor rules for creates through core's relay stay as they are.

R22 Workspaces and namespaces. Core refuses a head or hiree create on a project with no workspace placement (`unresolved_workspace`), before claiming anything, and `activation_gap` when it can't consult the workspace authority. Revise constraints and acceptance_sketch:
- entorhinal refuses the same creates with the same codes;
- the name namespace is `assistant`/`"global"` for an assistant, `workspace`/its own `workspace_id` for a workspace_head, and `workspace`/the project's resolved workspace for a head or hiree, which is also stored as the row's `workspace_id`. This replaces "follows core's `namespace_for_create` once quoted";
- import keeps each row's stored `workspace_id`. Since 082, a head or hiree may hold a non-NULL `workspace_id`.
Item 11's `multi_workspace` case becomes: a head created while its project was placed, whose project is then unplaced, shows no `workspace` and keeps its name claim.

R23 Name normalisation. Port `normalize_agent_name` with the same pipeline, crates and versions: `icu_normalizer` and `icu_casemap` pinned at `=1.5.0` (Unicode 15.1), `normalization_version` 1, and a maximum of 24 scalars. Copy core's tests (`agent_registry.rs:7533-7661`, `7733-7763`) verbatim, as the acceptance for names. Revise constraints and acceptance_sketch to say so. These two dependencies are new to entorhinal, and R11's ban on pin changes covers only the subc crates.

R24 Avatar fingerprint. Acceptance item 6 pins core's vector: genome `"a"` repeated 2048 times, type `"creature.classic"` and version `2` give `b4c04a5d931e65a48e022b8943678d55` (`tests/it/agent_fleet_overview.rs:559-582`). The type-and-version-absent case is computed once with core's recipe and written into the fixture with that citation. Revise acceptance_sketch only.

R25 Core's decoders. Core decodes only `resolve` and `enumerate`, and neither decoder uses `deny_unknown_fields`, so an extra `incarnation` is ignored. Core decodes none of the other four reads. Revise constraints: the step-1 tolerance assumption is now a fact with this citation, and its fallback is removed.

R26 Long polls. The SDK runs each request in its own task, with at most 64 handler tasks and no per-request deadline, and the default call timeout is 30 s. Revise constraints and acceptance_sketch: an `agent.changes` wait holds no store lock and occupies one handler task. At most 8 waits are held at once, and a 9th returns immediately with whatever is ready, which may be nothing, rather than refusing. Acceptance: with 8 waits held, a 9th returns without waiting, and a `resolve` still answers.

R27 Op bodies. Entorhinal's agent ops take core's request field names, casing and `deny_unknown_fields` for the op of the same name (pinned-facts file, "Core's agent op bodies"), without core's caller and residence fields (`callerHarness`, `callerSession`, and `avatar_read`'s `harness`, `session_id`, `session`, `caller_directory`, `caller_session`). Core strips those fields before relaying, and entorhinal refuses them as unknown fields. Replies keep core's top-level keys. `github_identity` is stored as core's tagged `GithubIdentity` JSON, which holds no secret. Revise constraints and acceptance_sketch: this replaces "core's bodies are not yet quoted".

R28 Revise open_questions: replace the "Facts still to be quoted" paragraph with one sentence saying those facts are recorded in `docs/designs/agent-identity-pinned-facts.md` and settled by R18 to R27. The `session.transcript_page` unchanged-token fields belong to core's half of the fleet view, not to this campaign. Keep the rest of open_questions exactly.

R29 Slice plan. Prefer narrow fences: each slice fences the specific files it changes, with new code in new files it owns. When several slices must change the same file (expected for `crates/entorhinal-core/src/lib.rs`, `crates/entorhinal-core/src/mutations.rs`, `crates/entorhinal-module/src/main.rs` and the `Cargo.toml` files), list that fence string verbatim in `slice_plan.shared_serialized_paths`. The campaign runs on integration ref `main` (R18), so the mint cuts each slice that fences it from main's tip in turn. Never fence a whole crate with `**` when specific files will do.

R30 The intent and non_goals sections need no revision for R18 to R29: keep their current text exactly.

### recorded supplied ruling 20

Campaign `ct_00000000-0000-4030-98da-61f6ceb7f0f0`, round 1, decision sequence 0 (source: {"body_bytes":2820,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4030-98da-61f6ceb7f0f0-20261005T012145Z.md"}):

These rulings settle round 0's two needs_evidence questions from core's source at prefrontal `873870be8`, read directly. Everything else in the round's fold stands.

R31 Reachability is core's, not entorhinal's. Core computes `reachability` from the agent's residence and that machine's binding (`agent_registry_ops.rs:911-937`): with no residence it is `{"state":"unknown"}`; with a residence it depends on `machine_binding.binding_kind` and `pairing_state`, so a local binding gives `unknown` and a remote one gives `unreachable` with reason `never_paired`, `revoked` or `remote_not_activated`. Residence and machine bindings stay in core, so entorhinal can't compute it. Revise constraints and acceptance_sketch: no entorhinal reply carries `reachability`. That covers `agent.resolve`, `agent.resolve_name`, `agent.list` and the `agent.peer_roster` peers, as well as the agent row. Remove it from the inherited digest keys as one more listed edit. Core decides whether its own relay adds it back. That belongs to core's half, so add it to core's dependency item.

R32 `agent.peer_roster`. In deployed core, `manager_runtime.rs:3827-3839` routes it to `dispatch_peer_roster_with_projects` (`agent_registry_ops.rs:1760-1777`). The synchronous arm at `3171-3179`, which refuses `workspace_membership_unavailable`, is only the fallback when no projects consumer is attached. Core's served behaviour:
- It decodes `{workspace_id}` with `deny_unknown_fields` and validates the id.
- Without the registry activation marker, it refuses `registry_not_activated` ("agent registry identity is not activated") (`1466-1478`).
- Membership (`workspace_roster_from_enumeration`, `1585-1626`) is the live `workspace_head` rows whose `workspace_id` is that workspace, plus the live `head` rows whose project is placed in that workspace. It contains no hirees and no assistants, and is ordered by `agent_id`.
- The reply is `{"peers": [...]}`. Each peer (`encode_peer`, `1450-1464`) has `agent_id`, `name`, `tag` and `role`, plus `project_id` and `github_identity` only when set, plus `reachability`.
Revise constraints and acceptance_sketch: entorhinal serves the same request, the same membership and the same order, with project placement read from its own store. Before the `agent.cutover` marker it refuses `registry_not_activated`, matching what `activated_only` already tests. Peers carry the same keys minus `reachability` (R31). Acceptance:
- a workspace with one workspace_head, two heads on projects placed in it, a head on a project placed elsewhere, a hiree and a retired head returns exactly the first three, ordered by `agent_id`;
- the same call before the marker refuses `registry_not_activated`.

R33 The intent, non_goals and open_questions sections need no revision for R31 and R32: keep their current text exactly.

### recorded supplied ruling 23

Campaign `ct_00000000-0000-4038-98da-60305bcbce78`, round 1, decision sequence 0 (source: {"body_bytes":4648,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4038-98da-60305bcbce78-20261005T015204Z.md"}):

These rulings answer round 0's chair-supply item (finding 1) and its four needs_evidence questions, from core's source at prefrontal `873870be8` and core's live store, read directly. They also set one rule the round showed is missing: which error codes inherited ops use. Everything else in the round's fold stands.

R34 Error codes for inherited ops. Core's relay passes entorhinal's replies through, so for any condition that core's op of the same name already refuses, entorhinal returns core's code: `AgentRegistryError::code` (`prefrontal-core-store/src/agent_registry.rs:843-873`), plus `invalid_request` and `invalid_cursor` from `agent_registry_ops.rs`. For example: `invalid_name`, `invalid_tag`, `invalid_labels`, `name_conflict`, `agent_project_taken`, `unknown_agent`, `gone`, `merge_target_not_live`, `invalid_github_identity`, `invalid_project_id` and `invalid_workspace_id`. `agent.list` refuses an empty `cursor`, or a `limit` outside 1–200, with `invalid_cursor` (`3107-3119`). `agent.resolve_name` keeps core's success-shaped refusals `{"refused": {"code": "name_unknown" | "name_ambiguous", "details": ...}}` (`1185-1289`). These are Response replies, not Error replies, and carry the fleet pair like any success. New codes exist only for conditions core has no counterpart for: the import checks, `flow_scope_not_admitted`, the refusal of `Direct` identity writes, request-key reuse across ops, and the cutover marker. Revise constraints and acceptance_sketch: replace every spec-invented code that duplicates a core condition with core's code, and pin each mapping in item 12's decoder fixtures.

R35 How R23's "verbatim" applies (finding 1). Copy `normalization_pipeline_pins_nfc_case_fold_trim_and_scalar_order` (`agent_registry.rs:7533-7580`) verbatim, as a pure test of the ported normaliser. Copy the other cited tests (`7582-7661`, `7733-7763`) as their inputs and expected outcomes, run through entorhinal's own seams (the create and claim path, or the handler). Each copy cites the core line it came from. Keep every assertion about names, claims, codes and normalisation version. Drop the assertions about fields this campaign removes (`persona_ref`, `wake_policy_version`, residence and machine bindings), and say so in a comment. Under R34 the expected code stays core's `invalid_name`, and an id lookup that core answers `NotFound` answers `unknown_agent`. No production field, table or code is restored to satisfy a copied assertion. Revise acceptance_sketch only.

R36 Gone replies. Core's `encode_gone` (`agent_registry_ops.rs:992-1001`) emits `{"reason": "deleted", "at": <ms>}` for a disposed agent and `{"reason": "merged", "at": <ms>, "into_agent_id": <id>}` for a merged one, with no generation. Entorhinal's `gone` objects (in `agent.resolve`, `agent.list`, `agent.dispose` and `agent.merge`) keep exactly these keys and the reason string `deleted`. The snapshot and change feed keep the spec's `status` vocabulary, where `retired` corresponds to `deleted`. Revise constraints and acceptance_sketch to state this mapping.

R37 Agent ids. Core's create path mints `agent_` plus 16 lowercase hex from `randomblob(8)` (`prefrontal-core-store/src/lib.rs:1307-1310`, called at `agent_registry.rs:1846`). The seed ceremony uses `agent_` plus the first 16 hex of a SHA-256 (`agent_registry.rs:3579-3583`). The live store holds 34 ids with 16 hex and exactly one with 8 hex, `agent_16013c86`. The spec's import rule (`agent_` plus 8 or 16 lowercase hex) is correct. Entorhinal mints 16 hex. Revise constraints only to cite this.

R38 Tags, labels and avatars. These are core's rules (`agent_registry.rs:1324-1361`, constants at `30-32`), and entorhinal applies them with core's codes (R34):
- **Tag:** Unicode 15.1 trimmed, non-empty, at most 256 bytes, else `invalid_tag`.
- **Labels:** at most 16; each one trimmed, non-empty and at most 32 scalars; no two equal after ICU case folding; else `invalid_labels`. Labels are stored trimmed, in request order.
- **Avatar type:** only `creature.classic` is known, and its genome is exactly 2048 hex characters (`agent_registry_ops.rs:835-842`). An unknown type refuses `invalid_request`.
- **`seedOnly: true`** (`agent_registry.rs:3112-3169`): it writes only when the agent has no genome. Otherwise it returns `applied: false` with the stored avatar, and changes no column, so `updated_at_ms` and the per-agent generation stay unchanged.
Revise constraints and acceptance_sketch: add one acceptance case for each bound and for each refusal code.

R39 The intent, non_goals and open_questions sections need no revision for R34 to R38: keep their current text exactly.

### recorded owner answer 25

Campaign `ct_00000000-0000-4050-98da-6288006dff70`, round 0, decision sequence 4 (source: owner decision):

park for chair rulings

### recorded supplied ruling 26

Campaign `ct_00000000-0000-4050-98da-6288006dff70`, round 1, decision sequence 0 (source: {"body_bytes":3121,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-4050-98da-6288006dff70-20261005T022108Z.md"}):

These rulings answer round 0's needs_evidence questions from core's source at prefrontal `873870be8`, read directly, and mint the slice plan. Round 0 folded all its blockers. They were all exact wire details, of the kind each slice's tests pin best. Everything else in the round's fold stands.

R40 Project and workspace ids. Core's `validate_project_id` and `validate_workspace_id` (`prefrontal-core-store/src/agent_registry.rs:1379-1395`) trim with Unicode 15.1 and refuse an empty result, or one over 512 bytes (`MAX_SCOPE_BYTES`, line 33), with `invalid_project_id` or `invalid_workspace_id`. They check no character set and no prefix. No other variant maps onto those codes. Entorhinal applies the same rule to the `project_id` and `workspace_id` of agent ops. Project ops keep their own rules. On create, the order is the same as core's (`agent_registry_ops.rs:3026-3049`, then `prepare_agent_create` at `agent_registry.rs:1784-1822`):
1. id validity;
2. role shape (`invalid_role_shape`);
3. name (`invalid_name`);
4. tag (`invalid_tag`);
5. binding resolution;
6. the name claim.

R41 Workspace match in `agent.resolve_name`. `row_workspace_matches` (`agent_registry_ops.rs:1174-1184`) compares only the row's stored `workspace_id`, passed through `normalize_agent_name` (`normalized_workspace_id`, `1168-1172`). A row with a NULL `workspace_id` matches only the scope `"global"`. It doesn't consult project placement. Entorhinal does the same.

R42 Avatars and GitHub identity.
- **Genome:** core checks only the genome's length, which must be exactly 2048 characters for `creature.classic`. It does not check that the characters are hex (`agent_registry_ops.rs:3276-3285`). A missing `type` or a wrong length refuses `invalid_request`. Entorhinal matches this and adds no hex check, since a stricter rule would refuse what core accepts today.
- **Avatar in replies:** `AgentAvatar` (`agent_registry.rs:638-647`) serialises as `{genome, type, version}`, camelCase, with `type` and `version` omitted when absent, not null. The `avatar` in `agent.set_avatar`'s reply keeps that shape. The agent row's `avatar` object keeps the spec's own null rule.
- **GitHub identity:** a `github_identity` that doesn't decode as the tagged enum (an unknown `kind` or an unknown field) refuses `invalid_request` (`decode_github_identity`, `agent_registry_ops.rs:844-851`). One that decodes but fails `validate_github_identity` (`agent_registry.rs:1507-1552`) refuses `invalid_github_identity`. That covers `app_id <= 0`, an `installation_id` present and `<= 0`, an empty `app_slug`, `credential_ref`, `coauthor_line` or `login`, and a `client_id` or `coauthor_line` that is present but empty.

R43 Every slice that ports a core validator or serialiser cites the core function it copies, and pins core's behaviour with a test whose expected values come from that function, not from entorhinal's own output. That is the general form of R35, and it applies to any core fact the spec cites but doesn't spell out.

R44 The intent, non_goals and open_questions sections need no revision for R40 to R43: keep their current text exactly.

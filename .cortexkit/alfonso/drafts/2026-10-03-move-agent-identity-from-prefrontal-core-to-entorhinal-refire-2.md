---
title: "2026-10-03-move-agent-identity-from-prefrontal-core-to-entorhinal refire 2"
status: draft
rounds_cap: 4
refired_from: "ct_00000000-0000-403c-98db-8c1edc263cd8"
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
Stamped and consent cases run through a handler-level seam that injects the
stamp (principal, agent id, `flow_id`), `origin`, `actor`, a clock, and a fake
`consent/v1` provider behind the consent switch; the daemon's carrier check at
`route.open` is SUBC's.

1. **The identity store holds every field the import needs, and the rules that
   protect it.** Schema covers ids, names and claims (with history), role,
   project and workspace binding, tag, labels, avatar inputs (no stored
   fingerprint column), GitHub identity with `credential_ref` and no secret
   column, `agent_generation` (no mutation trigger), `name_version`,
   timestamps, terminal rows (`retired`/`merged`) with `merged_into`,
   supervisor, create request key, hire caps, grants, the `agent.cutover`
   marker, pending consent records, and the journal's `principal`, `origin` and
   `request_digest` columns; it has no persona, residence, wake or sleep
   column. Tests prove:
   - the one-live-head index refuses a second live head with `live_head_exists`;
   - names normalise and case-fold by core's function, tested on core's own
     pinned inputs copied verbatim; an invalid name refuses
     `invalid_agent_name`, an active collision `agent_name_taken`;
   - each illegal role shape refuses `invalid_role_shape`; a head or hire naming
     a missing project, or a workspace_head a missing workspace, refuses
     `not_found`; a head named by an alias of P is stored with P's current id;
     a create of a non-hire role naming `supervisor_agent_id` refuses
     `invalid_supervisor`; a request carrying `persona_ref` or a residence field
     refuses `invalid_request`;
   - rename is atomic and raises `name_version` by 1, while tag, labels,
     avatar, GitHub identity, dispose and merge leave it; after dispose, the
     same display name can be created again in that namespace;
   - merge into self or a terminal id refuses `merge_target_not_live` and the
     source claim stays active; a mutation of a terminal agent refuses
     `agent_terminal`, of an absent one `agent_not_found`;
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
   - read as input only: `079` (`hire_identity_mapping`), for supervisor if it
     records one;
   - stays in core: `076`'s `machine_binding`, `agent_delivery`, `residence_flip`
     and activation marker, `088`, `091`, `110`, `116`, `121`, `127`, `171`,
     `177`.
   Supervisor is NULL unless `079` names one; every imported row's request key
   is NULL; `deleted` is stored as `retired`. Tests:
   - admission: before the marker, `agent.rename` from
     `reserved:prefrontal-core` refuses `authority_not_cut_over`; import from
     `Direct` refuses `direct_identity_write_not_admitted`, before and after the
     marker; import from `reserved:prefrontal-core` commits rows and the marker,
     after which a rename is judged by the table; a zero-agent import, and an
     import from a source with no `agent` table, commit the marker, after which
     an operator-row `agent.create` is admitted; a same-key rerun after a
     restart returns the committed counts and generation with the new
     process's incarnation; a new-key import refuses `import_already_done` and
     changes nothing;
   - refusals: each check refuses `import_invariant_failed` with its `check`
     value and nothing written: an id outside `agent_` plus 8 or 16 lowercase
     hex (`agent_id`); a non-NULL `persona_ref`; a live agent whose active claim
     does not match re-normalisation, and one with no active claim
     (`claim_mismatch`); two live heads on one project; a `merged_into` absent
     from the source; a live row naming a non-NULL project id that is not a
     current project row, and a live workspace_head naming a workspace that is
     not a current row (`binding`); a hire whose `079` supervisor is not a head
     of its project, or is absent from the source (`supervisor`);
   - successes, as a separate import: a terminal row naming a removed project, a
     terminal workspace_head naming a removed workspace, an assistant with NULL
     ids, a hire with NULL supervisor, a hire with a live supervising head, a
     live hire whose supervisor is a terminal head of its project, a terminal
     hire whose supervisor is terminal, and a row with non-NULL residence, a
     wake policy and `sleep = 1` all import, supervisors kept, and the stored
     rows carry no residence, wake or sleep value;
   - the reply's `agents_imported` and `claims_imported` equal the source's
     row counts; `agent.changes` with `limit` 1 from before the import returns
     one `agent.import` entry per agent, each with its own seq, none skipped;
   - a fixture pins one value per carried field and asserts it after import and
     after `rebuild`: `agent_generation`, `name_version`,
     `name_normalization_version`, `created_at_ms`, `updated_at_ms`, a merged
     row's non-null `terminal_at_ms`, labels (empty and non-empty), avatar
     genome with type and version absent, `credential_ref`, `app_slug`,
     `installation_id`, a released claim whose name can be created again,
     `merged_into`, tag, role, `project_id`, `workspace_id` on a workspace_head
     and on a head (including NULL), supervisor, `agent_16013c86` and a 16-hex
     id;
   - 11 live hires on one project import (the cap is ignored) and all resolve
     live;
   - a fault injected mid-import leaves neither rows nor marker and a rerun
     succeeds; import then `rebuild` equals the imported state; project and
     workspace rows are untouched; the reply carries both counts, `incarnation`
     and `generation`.
3. **Resolves answer, before the marker as after it.** A live id resolves
   `live`, a disposed id `retired` (including one imported as `deleted`), a
   merged id `merged` with its `merged_into`, a retired id with `merged_into`
   null, an absent id `unknown`; with the store not open the call returns
   `storage_unavailable`, and a failing read on an open store `storage_error`,
   neither as a status. Before the marker, `agent.resolve` of a well-formed
   absent id returns `unknown`, and `agent.snapshot` and `agent.fleet_identity`
   succeed with empty lists and a generation. The snapshot and the feed carry
   the same `retired`/`merged` values. Each is a distinct assertion.
4. **Generations can be trusted.** Every agent and grants read and mutation
   success reply, `agent.snapshot` included, carries the fleet
   `(incarnation, generation)` and no `noop`; project op replies keep today's
   shape without `incarnation`; a restart changes the incarnation. A create
   writes `agent_generation` 1; an imported row at 1000000 renamed in a store
   whose head is below 100 reads 1000001, also in its `agent.rename` entry, and
   still 1000001 after `rebuild`. Renaming agent B advances B's
   `agent_generation`; renaming A or `approve_root` leaves B's unchanged while
   `generation` rises, and `agent.github_identity` for B shows both fields with
   unequal values. Across an identity mutation and `rebuild`, both are
   reproduced and a later mutation issues above them. A committed
   `agent.create`, a restart, and a same-key retry return the same agent with
   no new journal row, the commit's `generation` and the new incarnation.
5. **Every call is admitted by exactly one row of the read or write table.** A
   test matrix calls each operation-by-principal cell, each refusal with no
   journal row, including:
   - before the marker: each agent-identity mutation from `Direct` refuses
     `direct_identity_write_not_admitted`, from a stamp with a non-empty
     `flow_id` (with or without an agent id) `flow_scope_not_admitted`, from
     `reserved:callosum` `write_not_permitted`, from a scoped agent on an op
     outside the scoped column `write_not_permitted`, and from
     `reserved:prefrontal-core` or a scoped agent on an in-column op
     `authority_not_cut_over` (the import excepted); `grants.revoke` from
     `Direct` refuses `authority_not_cut_over` before the marker and is
     admitted after it;
   - keyless calls: from `Direct`, a flow stamp and `reserved:callosum` they
     get the principal-row code above; from `reserved:prefrontal-core`
     (`agent.rename` included) and from a scoped agent stamp (`create_head`
     included) `request_key_required`;
   - stamps: no agent id → `write_not_permitted`; an unknown or retired agent id
     → `requester_not_live`, except a retired stamp's `grants.revoke` of its own
     grants, which is admitted, while its `grants.revoke` of another agent's
     grants refuses `write_not_permitted`; a null `flow_id` with a live agent id
     reaches the scoped column; stamps of every role reach the same scoped
     cells;
   - consent switch off, with a fake provider registered: a call needing a card
     gets `consent_not_bound`; switch on and provider down:
     `consent_unavailable`; the manifest's `requires` is empty, and with no
     provider project ops, identity reads and operator writes answer;
   - an invalid name, a taken name, or an existing project id refuses before
     the card step, with no pending record, card or grant;
   - head H creates hire R by card or grant: R's supervisor is H; a
     management-op create naming another supervisor refuses
     `invalid_supervisor`; with consent unbound, H disposes R by
     `dispose_agent`: admitted, `hire_retired_by_supervisor` in the feed once,
     and a same-key `agent.dispose` retry returns the cached reply; head B
     disposing R: card when bound, `consent_not_bound` when unbound; a
     NULL-supervisor hire: card;
   - after H is retired, H's still-stamped `create_head`, `create_hire` and its
     `dispose_agent` of R refuse `requester_not_live`; the live head of P2
     calling `create_hire` for P1 refuses `write_not_permitted`, and for an
     absent project `not_found`;
   - a scoped assistant or workspace_head create, `agent.import` and
     `agent.set_hire_cap` refuse; operator-row creates of all four roles are
     admitted; a scoped `agent.rename` on the management op reaches its card
     cell; a create stamped with a session that has no entorhinal agent refuses
     `write_not_permitted`;
   - every `reserved:callosum` mutation refuses `write_not_permitted` and mints
     no grant;
   - grants: a grant admits within its scope only; a create-head grant at W1
     does not admit `create_head` in W2 or `create_project`; `create_head` for
     an unplaced project offers Always fleet-wide only; a scoped
     `create_project` whose resolved project id exists refuses
     `write_not_permitted`; operator `assign_workspace` to a new workspace id
     still creates it;
   - two-class create: janitor creates P1 in new W1 with neither class held, one
     card offers Always fleet-wide only, the answer mints two grant rows citing
     one card id, and P2 in new W2 is then admitted with no card; with
     create-workspace held fleet-wide, a `create_project` into new W3 gets a
     card covering create-project only, offering Always fleet-wide only, and an
     Always answer mints one fleet-scoped create-project row;
   - hire cap: with cap 10 and 10 live hires, a head create is still admitted and
     the 11th hire refuses `hire_cap_reached` naming 10, retiring one admits the
     next, cap 2 set over 3 live hires leaves them live and refuses the next
     naming 2, `set_hire_cap {cap: null}` replies `cap: 10`, and
     `grants.would_ask` reports the refusal and the cap;
   - an unrelated agent's `grants.revoke`, `grants.list` and `grants.would_ask`
     for another agent refused;
   - identity reads admitted from `reserved:callosum`, an unrelated reserved
     module and a flow-stamped call;
   - `remove` of a bound project refuses `bound_by_live_agent`; `remove` of a
     workspace named by a live workspace_head, and of one holding a project
     with a live head, refuses it; with an imported live head whose stored
     `workspace_id` is W1 while its project is placed in W2, `remove(W1)`
     succeeds and leaves the head unchanged and `remove(W2)` refuses
     `bound_by_live_agent`; `assign_workspace`, operator `register` attaching an
     unplaced project, `upgrade_implicit` and `seed_import` whose effect would
     place a project bound by a live head or hire refuse it, append no journal
     row, and leave placement and claims unchanged; a project whose agents are
     all terminal moves; removing a project revokes its grants, and recreating
     the id revives none;
   - operator row: every operator-row operation from `reserved:prefrontal-core`
     is admitted; each agent-identity write from `Direct`, by op or by tool name
     on the management surface, refuses `direct_identity_write_not_admitted`
     with a message naming core's relay.
6. **Consent is used only as designed.** Tests prove:
   - the cingulate route opens only inside a live inbound scoped request from
     that session, by tool name or management op (a scoped `agent.rename` raises
     its card); a closed route returns `consent_route_closed` without retrying;
   - no store lock is held during a consent wait, and a resolve during one
     succeeds; a wait reaching the injected 2-minute cap returns
     `consent_pending` with the card id and no journal row;
   - an approved card executes only a request whose binding digest matches; a
     changed field, another agent's card, or a second concurrent retry produce
     no effect beyond one execution; a deny returns `consent_denied`, and so
     does a same-key call after it;
   - each race refuses on the approved retry and on a later same-key call, with
     no identity mutation and no grant minted: `consent_binding_mismatch` for a
     `create_project` whose identical name and roots another writer registers
     while pending, a `create_head` for P whose id is re-pointed while pending,
     a `create_head` for P in W1 after P moves to W2 (an unchanged same-key call
     before approval still returns the original card), and a `create_project`
     into a new workspace whose create-workspace grant is revoked while its
     project-only card is pending; `live_head_exists` for a `create_head` whose
     project gains a live head while pending; `agent_name_taken` for a name
     claimed while pending;
   - a `create_head` left pending, a restart, and an approved same-key retry
     create exactly one agent;
   - a same-key call while pending returns the same card and writes no journal
     row; two simultaneous first calls raise one card; a same-key call with a
     changed body, or from another agent, refuses `request_key_conflict`; a key
     journaled under another op refuses `request_key_reused_across_ops`; after
     the injected clock passes expiry a same-key call raises a new card;
   - approval still executes after a restart and after `rebuild` within the
     card's lifetime; a requester retired while pending refuses
     `requester_not_live` and an Always answer then mints nothing;
   - grants are minted only from answers to entorhinal's own cards, and only
     for the classes the card offered.
7. **The fleet view's identity half matches the join vector**
   (`docs/designs/fleet-view-join-vector.md`). The identity-half cases pass:
   - after importing one deleted, one merged and one live row,
     `agent.fleet_identity` lists only the live id, while `agent.snapshot` holds
     all three;
   - the avatar fingerprint pinned to a fixed genome, plus the type-and-version
     absent case, computed at serve time; absent optionals are omitted, not
     null;
   - the unchanged token: a call without `token` is a full reply with
     `unchanged: false`; a call with the current token answers `unchanged: true`
     with `agents` omitted; create, rename, tag, labels, avatar re-roll, GitHub
     identity change, dispose, merge, a project rename, `assign_workspace` of a
     project whose agents are all terminal, and a restart each invalidate it;
   - `workspace` derived through the project only, so a `workspace_head` row
     omits it, as today (`fleet_overview.rs:364-379`);
   - a live agent whose project has two roots appears exactly once, its
     `canonicalRoot` the first root in `canonical_root` order; an empty fleet is
     a success distinct from a failed read.
8. **Two-step create is safe to retry.** A same-key retry returns the same
   agent; a missing key refuses `request_key_required`. `live_head_exists` names
   the occupier's agent id and request key, with an explicit null for an
   imported head.
9. **The journal records who wrote each identity row.** After an admitted
   `reserved:prefrontal-core` write and after a scoped write, the journal row's
   `principal`, `origin` (explicit NULL when absent) and `actor` (the request's
   field, `module` when absent) read back as sent. A `register` journaled with
   NULL `principal` and key K, retried with K, returns the cached reply; an
   agent op's cached reply requested by another principal refuses
   `request_key_conflict`; a committed scoped `create_project`'s key, sent by
   another agent or with a changed body, refuses `request_key_conflict`, by card
   and by grant.
10. **The change feed is complete and ordered.** Tests prove:
    - a consumer using only serialized snapshot and change replies reproduces
      entorhinal's agent and claim tables, claim history predating the snapshot
      included, with mutations committed concurrently while it pages through
      `agent.changes` and none skipped at the snapshot boundary;
    - an entry of every op in the closed list, and the row and claim objects,
      decode with their listed fields, every field this spec adds present (null
      when empty or not set by that op); a decoder test fails if one is missing
      or renamed; cap, grant and marker entries are skipped and the reply cursor
      advances past them, to the head when they are trailing;
    - a `wait` call whose only entries above the cursor are non-identity (the
      marker after an import) returns at once with no entries and the cursor at
      the head;
    - `journal_tail` returns project-op rows only; with project rows at seq 1
      and 3, non-project rows at 2 and 4, and `limit` 1, a caller following the
      advance rule consumes rows 1 and 3 exactly once and ends at 4;
    - another incarnation or a cursor above the head answers `snapshot_required`
      with no entries; a gap returns the missing entries in order;
    - an idle poll with `wait` stays open until an entry commits or 25 s pass,
      holds no store lock, and an identity mutation committed during it is
      returned by that poll or the next with none skipped;
    - replay reproduces minted ids rather than generating new ones.
11. **Entorhinal runs on subc-protocol 0.29** (0.28.1 today, `Cargo.toml:9`),
    decodes stamps carrying `flow_id`, and its manifest test pins: the
    management surface holds every operation listed today plus every Names read
    and mutation and the four tool names; `provides` is exactly
    `project-identity/v1` and `agent-identity/v1`; `requires` is empty; the tool
    surface is exactly the four scoped tools. A scoped call to each tool,
    stamped with no `flow_id`, reaches the same handler and journal op as the
    named management op.
12. **Rebuild loses no new state.** With no cap row the 11th hire refuses naming
    10; after setting cap 2, minting one grant and revoking another, `rebuild`
    leaves cap 2, the minted grant present, the revoked one absent, and the
    marker present. A project with cap 2 whose agents are disposed, removed and
    registered again with the same name and roots starts at 10, before and
    after `rebuild`. A project created in W by `upgrade_implicit`, with a head,
    keeps its placement across `rebuild`, and `agent.fleet_identity` returns the
    same agents before and after.
13. **Wire bodies match core's.** For every op core serves today except
    `agent.resolve`, the fixture is core's golden fixture with that op's listed
    edits applied, or, where core keeps none, a fixture written here from core's
    decoder type for that op with the file and type cited beside it; each
    decodes against entorhinal's types and re-encodes byte for byte. The new
    ops' fields (`agent.resolve`'s five-field reply, `agent.import`,
    `agent.set_hire_cap`, `agent.snapshot`, `agent.changes`,
    `agent.fleet_identity`, the scoped tools) are pinned by fixtures written in
    this repository from constraints, Names. The commons `grants.*` vectors for
    `grants.list`, `grants.would_ask` and `grants.revoke`, copied verbatim, decode
    and re-encode byte for byte, with entorhinal's classes, scopes and the
    `hire_cap_reached` answer as named cases. A tool fixture pins
    `create_project` tool JSON in and `register`'s camelCase body out; a scoped
    `create_project` naming a new workspace creates it, journals `register`
    with the supplied key, and a same-key retry returns the cached reply with
    no second card; a `create_head` with `request_key` and no `tag` is admitted
    and stores tag `""`; a tool body with an extra field refuses
    `invalid_request`. A scoped `create_project` refused by `register` returns
    `register`'s code unchanged.
14. **Rollback is honest.** A binary whose migration chain lacks the agent
    migrations refuses a migrated store as ahead of it
    (`crates/entorhinal-core/src/lib.rs:146-156`).

## constraints
**Ownership.** Entorhinal takes agent ids, names and their normalisation, name
claims, role, project and workspace binding, the one-live-head rule, tags,
labels, avatar, GitHub identity, the per-agent generation, disposal, merge, and
the supervisor of every hire. Core keeps residence, delivery, wake routing,
board state, personas and scope registration, and stamps an entorhinal-issued
`agent_id` on the scopes it registers. Core also keeps the GitHub pending-mint
ceremony (`github_identity.mint_begin`/`mint_complete` and its
`github_identity_pending_mint` table, migration 088): it is not in the design
note's list of ops that move, and it completes a binding by calling
entorhinal's `agent.set_github_identity` on the `reserved:prefrontal-core` row.

**Names.** Entorhinal declares capability `agent-identity/v1` beside
`project-identity/v1` (`crates/entorhinal-module/src/main.rs:849,992-996`);
`requires` stays empty. Agent ops carry an `agent.` prefix so they never collide
with the project ops it already serves (`resolve`, `enumerate`, `remove`,
`journal_tail`). Agent and grants op bodies and scoped tool bodies are
snake_case, as core's are; project ops keep their camelCase;
`agent.fleet_identity` uses the join vector's camelCase.
- Reads: `agent.resolve`, `agent.resolve_name`, `agent.list`,
  `agent.peer_roster`, `agent.avatar_read`, `agent.github_identity`,
  `agent.fleet_identity`, `agent.snapshot`, `agent.changes`, `grants.list`,
  `grants.would_ask`.
- Mutations: `agent.create`, `agent.rename`, `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose`, `agent.merge`, `agent.set_hire_cap`, `agent.import`,
  `grants.revoke`. Every one requires `request_key` from every principal row,
  the import included; a missing key refuses `request_key_required` (after the
  principal-row check, see check order). Project ops keep today's optional
  `requestKey`.
- Manifest: the management surface keeps every operation it lists today
  (`main.rs:855-962`) and adds every read and mutation above plus the four
  scoped tool names as management aliases of their ops; the tool surface is
  exactly the four scoped tools below.
- Scoped agent tools, each an extra name for the named op: `create_project` →
  `register` (new project only), `create_head` → `agent.create` role `head`,
  `create_hire` → `agent.create` role `hiree`, `dispose_agent` →
  `agent.dispose`. Parameters are closed (an extra field, `supervisor_agent_id`
  and `role` included, refuses `invalid_request`), all with a required
  `request_key`: `create_project` `{name, roots, workspace_id?, request_key}`;
  `create_head` and `create_hire` `{project_id, name, tag?, request_key}` (an
  absent `tag` is stored as the empty string, since core's `agent.tag` is NOT
  NULL, core `076_agent_registry.sql:5`); `dispose_agent`
  `{agent_id, request_key}`. Before admission, the digests, the request-key
  checks and the journal append, a tool call is converted to its op's body:
  `create_project` to `register`'s `{name, roots, workspaceId, requestKey}`
  (never `projectId` or `derivedRootParents`); `create_head`/`create_hire` gain
  their role; `dispose_agent` is unchanged. No role is injected into `register`
  or `agent.dispose`. A tool name called by a non-scoped principal, on either
  surface, is handled as the named op under that principal's row of the write
  table. Scoped `create_project` is the only scoped path that creates a
  workspace (naming a `workspace_id` that does not exist creates it, as
  `register` does, `crates/entorhinal-core/src/mutations.rs:430`); operator
  `register`, `assign_workspace` (`mutations.rs:495`), `upgrade_implicit` and
  `seed_import` keep creating workspace rows as today.
- Wire bodies of ops core serves today, `agent.resolve` excepted: core's
  current request and reply body for the op of the same name, with exactly
  these edits, listed per op in a table pinned in this repository: (a)
  residence, wake, delivery-derived (`bounced_deliveries`), sleep and persona
  (`persona_ref`) fields removed; a request carrying any removed field refuses
  `invalid_request`, and core's relay strips them; (b) core's per-agent
  `generation` renamed `agent_generation`; (c) `incarnation` and fleet
  `generation` added, required, to every success reply, so a relay passes them
  through (R5); (d) `request_key` added to every mutation request; (e) optional
  `supervisor_agent_id` added to `agent.create` (absent and null both mean
  none; inside the request digest). Agent and grants replies, cached ones
  included, never carry `noop`: the shared mutation path inserts it into every
  object reply (`crates/entorhinal-core/src/mutations.rs:343-347`) and the agent
  and grants handlers do not apply that insertion. `agent.github_identity`
  carries `app_slug`, `credential_ref`, `installation_id`, `agent_generation`
  and no secret (`agent_assertion.rs:311-352` in the evidence). Avatar is stored
  as genome, type and version; the fingerprint is computed at serve time
  (`fleet_overview.rs:96-109`).
- Presence, per surface: fields core already serves keep core's presence rule;
  every field this spec adds to a snake_case agent or grants body, core's or
  new, is always present and JSON null when empty; `agent.fleet_identity`
  follows the join vector (absent optionals omitted, as
  `fleet_overview.rs:53-83` does, and `agents` omitted when unchanged).
- New ops (every success reply carries `incarnation` and `generation`):
  - `agent.import` `{snapshot_path, request_key}`: a path to a copy of core's
    store, opened SQLite read-only; `hire_identity_mapping` is read from the
    same file. A source with no `agent` table is a zero-agent source. Reply
    `{agents_imported, claims_imported}`, each equal to the source's row count.
  - `agent.set_hire_cap` `{project_id, cap, request_key}`: `cap` a non-negative
    integer, or null to return to the default 10. A cap below the live hire
    count is admitted; existing hires stay live and later hire creates refuse
    `hire_cap_reached` naming the new cap. Reply `{project_id, cap}`, `cap` the
    effective value (10 after a null).
  - `agent.snapshot` `{}`: one unpaged reply `{incarnation, generation, agents,
    claims}`, `generation` read in the same transaction and usable as the first
    `agent.changes` cursor.
  - `agent.changes` `{incarnation, cursor, limit, wait}`: `limit` default 500,
    maximum 1000; `wait` default true. Reply `{incarnation, generation, cursor,
    entries}`.
  - `agent.fleet_identity` `{token?}`: reply `{incarnation, generation, token,
    unchanged, agents?}`. These names are normative; if `session.transcript_page`'s
    unchanged token, once quoted, differs, the join vector is amended to match
    this spec. `token` is the fleet pair as an opaque string; a request without
    `token` gets a full reply with `unchanged: false`; with the current token,
    `unchanged: true` and `agents` omitted. Lists non-terminal agents only (as
    `fleet_overview.rs:254-266` does), one row per agent whatever the number of
    project roots, `project.canonicalRoot` being the project's first root in
    `canonical_root` order, and omits residence, board, `pendingAskCount` and
    `latestActivityMs`.
  - `grants.list`, `grants.would_ask`, `grants.revoke`: the commons `grants.*`
    bodies, plus `incarnation` and `generation`; `would_ask` answers a
    `hire_cap_reached` refusal with the cap.
- Agent row (an `agent.snapshot` element and a change entry's `row`): the
  `agent.list` element (core's body with the listed edits) plus whichever of
  these it lacks: avatar genome, type and version, the full GitHub identity with
  `credential_ref`, `name_version`, `name_normalization_version`,
  `updated_at_ms`; and always `status` (`live|retired|merged`), `merged_into`,
  `supervisor_agent_id`, `request_key`, `created_at_ms`, `terminal_at_ms`,
  `agent_generation`. Claim: `{claim_id, agent_id, namespace_kind,
  namespace_key, normalized_name, display_name, claimed_at_ms, released_at_ms}`
  (core's `076_agent_registry.sql:76-86`, per the panel). A decoder test pins
  both.
- Resolve reply (new shape, not an edited core body): `{agent_id, status,
  merged_into, incarnation, generation}`, `status` one of `live`, `retired`,
  `merged`, `unknown`, `merged_into` null unless merged. Storage failures keep
  the existing handler errors (`crates/entorhinal-module/src/main.rs:271-290`):
  `storage_unavailable` when the store is not open, `storage_error` when a read
  on an open store fails; neither is a status value.
- Change entry: `{seq, incarnation, generation, op, agent_id, agent_generation,
  row, claims, old_display_name, new_display_name, status, merged_into,
  supervisor_agent_id}`, `row` the agent's row after the entry and `claims` the
  claim rows it inserted or released, so a consumer rebuilds entorhinal's agent
  and claim tables without generating any value. The last five are on every
  entry, null unless the op sets them. Every entry has its own journal `seq`.
  `op` is one of a closed list: `agent.create`, `agent.rename` (sets
  `old_display_name`, `new_display_name`), `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose` (sets `status: "retired"`), `agent.merge` (sets `status:
  "merged"`, `merged_into`), `hire_retired_by_supervisor` (sets `status:
  "retired"`, `supervisor_agent_id`; `agent_id` is the hire), and
  `agent.import` (one per imported agent: its row and every claim, terminal rows
  included). Every other journal entry (project ops, `agent.set_hire_cap`, grant
  mint and revoke, the `agent.cutover` marker) is non-identity.
- Error codes: `authority_not_cut_over`, `write_not_permitted`,
  `read_not_permitted`, `direct_identity_write_not_admitted`,
  `flow_scope_not_admitted`, `consent_not_bound`, `consent_unavailable`,
  `consent_route_closed`, `consent_pending` (carries `card_id`),
  `consent_denied`, `consent_binding_mismatch`, `requester_not_live`,
  `hire_cap_reached` (carries the cap), `live_head_exists` (carries `agent_id`
  and `request_key` or null), `request_key_required`, `request_key_conflict`,
  `request_key_reused_across_ops`, `invalid_agent_name`, `invalid_agent_id`,
  `invalid_role_shape`, `invalid_request`, `not_found` (`mutations.rs:466`,
  naming the missing project or workspace id), `agent_name_taken`,
  `agent_not_found`, `agent_terminal`, `invalid_supervisor`,
  `merge_target_not_live`, `bound_by_live_agent`, `import_already_done`,
  `import_invariant_failed` (carries `check`, one of `agent_id`, `persona_ref`,
  `claim_mismatch`, `live_head`, `merged_into`, `binding`, `supervisor`, and the
  offending ids), `snapshot_required`. Existing codes pass through unchanged:
  every `register` refusal on the scoped `create_project` path
  (`mutations.rs:369-433`), `entropy_unavailable` from id minting
  (`binding.rs:80-84`), and `encode_failed`.
- Card kind `entorhinal.identity_request`, options Approve, Approve Always and
  Always fleet-wide, Deny (which Always options a card shows is under Grants).

**Two rules that predate this work.** The daemon never depends on entorhinal,
because it supervises it. Paths compare with the daemon's canonical path
function and are never re-normalised on entorhinal's side. Any change touching
either goes to SUBC for review before it ships.

**Agent ids are minted the opposite way to project ids.** Project ids are
deterministic (`pj-` plus a BLAKE3 hash of name and roots) and have no permanent
tombstone. Agent ids are random, never derived from name or path, and
permanently tombstoned, because a reused id would inherit the previous holder's
grants, bot ownership and approvals. New ids are `agent_` plus 16 lowercase hex
(8 random bytes from the store's id source, `binding.rs:80`), minted in the
transaction that executes the create, never earlier. Ids move unchanged in the
import. Validation is syntactic and needs no store read: `agent_` plus 16 or 8
lowercase hex is well-formed (only imported rows have the 8-hex form, e.g.
`agent_16013c86`; a create never mints one); anything else refuses
`invalid_agent_id`, and the import refuses a source id of any other form
(`import_invariant_failed`, check `agent_id`). A well-formed id with no row
resolves `unknown`, and a mutation naming it refuses `agent_not_found`.

**A retired or merged id stays resolvable, with its status.** Core keeps tables
that reference agent ids and that no database constraint can enforce after the
move: `session_scope.agent_id`, `flow_install.author_agent_id`,
`flow_install_grant.agent_id`, `gh_route_speech_receipt`, and
`persona_active_revision` (keyed `agent:<agent_id>`). A resolve never collapses
retired into unknown. Entorhinal stores and serves terminal reasons only as
`retired` and `merged`; the import maps core's `deleted` → `retired` and
`merged` → `merged`, so resolve, snapshot and feed agree.

**Identity invariants carried over verbatim.** `agent_id` is stable for life.
Names are NFC for display and case-folded for lookup, by core's own function
(quoted once gathered, not re-derived); an invalid name refuses
`invalid_agent_name`, an active collision `agent_name_taken`. Rename claims the
new name and releases the old atomically and sets `name_version` to its
previous value plus 1 (`agent_claims.rs:262-266` in the evidence); no other
mutation changes `name_version`. One active claim per namespace and per agent;
released claims keep history. Dispose and merge set the terminal row and
`terminal_at_ms` and release the active claim in one transaction; a mutation of
a terminal agent refuses `agent_terminal`; merge into self or into a non-live or
absent target refuses `merge_target_not_live` before anything is released
(`agent_claims.rs:289-339` in the evidence). At most one live head per project,
schema-enforced as an index on `(project_id) WHERE role = 'head' AND
terminal_reason IS NULL`, as core's `uq_live_head` is today; a violation
surfaces as `live_head_exists`, never `storage_error`.

**Create shape checks, in the handler.** As core does
(`agent_registry_ops.rs:3038-3049` in the evidence), a create refuses
`invalid_role_shape` unless: assistant has `project_id` and `workspace_id`
NULL; workspace_head has `project_id` NULL and `workspace_id` set; head and
hiree have `project_id` set and no `workspace_id` in the request. A named
`project_id` resolves through one alias hop, as project `resolve` does
(`crates/entorhinal-core/src/lib.rs:296`), and the current id is stored; an id
that resolves to no current project, or a workspace_head's `workspace_id` that
is not a current workspace, refuses `not_found`. Head and hiree rows store
`workspace_id` as core does: a new create writes the resolved namespace key
(`agent_claims.rs:390-395` in the evidence) and an imported row keeps its value,
NULL included; no CHECK forces NULL or NOT NULL for those roles. The namespace
of each role, including a head or hire on an unplaced project, follows core's
`namespace_for_create` once quoted.

**No personas, residence, wake or sleep.** The agent record holds identity
only: entorhinal's `agent` table has no `persona_ref`, `residence_*`,
`wake_policy_json`, `wake_policy_version` or `sleep` column (core
`076_agent_registry.sql:9-18`). The import drops residence, wake and sleep
values whatever they hold; a request carrying `persona_ref` refuses
`invalid_request`, and the import refuses `import_invariant_failed`
(`persona_ref`) if any row has it non-NULL.

**Generations.** Two values, never sharing a name:
- **`agent_generation`**, the per-agent value core stores as `agent.generation`
  (migration 109) and signs as `binding_generation`
  (`agent_assertion.rs:352`). Only 109's column is carried; its
  `agent_generation_on_mutation` trigger is not installed. A create writes 1;
  each later mutation of that agent's row writes its previous value plus 1,
  once, in the handler; the import carries core's value unchanged. The written
  value is in the journal entry and replay assigns it rather than incrementing.
  Mutations of other agents, projects or workspaces do not change it. Core's bot
  token embeds it with the incarnation.
- **Fleet `(incarnation, generation)`**, where `generation` is the journal head
  `MAX(seq)`. It versions reads, the feed, relay replies, and the fleet view's
  unchanged token. Any journal append invalidates the token, project ops
  included, so a project rename or workspace move is never missed; an extra
  invalidation from an unrelated project op is accepted
  (`fleet-view-join-vector.md:211-215`).
The **incarnation** is a random id minted at process start and never persisted
(not in a column, journal payload, cached reply or the import marker), because
restoring an older backup lowers both generations and anything in the file
would be restored with it. It is attached at serve time to every agent-op and
grants-op reply; project op replies keep today's shape (`generation`, no
`incarnation`). Every restart therefore invalidates outstanding tokens and
cursors; the operator docs say so. A same-key retry served from the request-key
cache, the import's included, returns the stored result, whose `generation` is
the one recorded at commit (`mutations.rs:338-347`), with the current
incarnation.

**The journal records the attested principal.** `registry_journal` gains
columns `principal` (the principal the handler admitted, with the stamped agent
id for a scoped call), `origin` (Callosum's `origin`, NULL when absent) and
`request_digest` (the request digest defined under Consent, NULL for unscoped
project ops), and keeps `actor` (`crates/entorhinal-core/src/lib.rs:41-49`) as
the caller's label: the request's `actor` field, defaulting to `module` as
project mutations do today (`mutations.rs:390`). `Direct` covers every local
process, so it narrows who wrote a row but does not prove a human did.

**Journal op and request key.** A journal row's `op` is the request's
management op, which the request-key cache and the cross-op check use
(`mutations.rs:312-336`). A row journaled by an agent or grants op, or by a
scoped `create_project` (op `register`, non-NULL scoped `principal`), serves its
cached reply only when the caller's principal and request digest match the
row's, otherwise `request_key_conflict`, whether the call was card- or
grant-admitted. Operator project ops, and rows journaled before this migration
(NULL `principal`), keep today's body-insensitive cache
(`mutations.rs:312-314`). A key is looked up in the journal first (a hit is
served by these rules), then in the pending record. A supervisor's dispose
journals `agent.dispose` with `supervisor_agent_id` in the payload, and the feed
presents it as `hire_retired_by_supervisor`, so a `dispose_agent` retry and an
`agent.dispose` call with the same key hit the same cache. The import journals
one `agent.import` row per imported agent (request key NULL), then the
`agent.cutover` marker row, which carries the import's request key and cached
reply (counts and generation), all in one transaction; the cache and cross-op
check treat the marker row as `agent.import`'s, so a zero-agent import is cached
too.

**Credential references, never credentials.** `credential_ref` points into
claustrum. No secret moves into entorhinal.

**Check order for agent and grants mutations.** Each step runs only if the
previous passed; none before (7) writes a pending record, card or grant.
(1) Principal row, before and after the marker: `Direct` →
`direct_identity_write_not_admitted` (R2; `grants.revoke` excepted); a stamp
with a non-empty `flow_id` → `flow_scope_not_admitted`, even when it carries an
agent id (a null or absent `flow_id` is not a flow stamp); a scoped stamp with
no agent id, `reserved:callosum`, other modules, or an op outside the caller's
column (a scoped `grants.revoke` of another agent's grants included) →
`write_not_permitted`.
(2) Request key: missing → `request_key_required`; journal cache, then pending
record.
(3) Marker absent → `authority_not_cut_over` (`Direct` `grants.revoke`
included; the import: present → `import_already_done`).
(4) Scoped requester resolved live in the executing transaction, else
`requester_not_live` (unknown, retired or merged agent id); a scoped
`grants.revoke` of the caller's own grants is exempt.
(5) On an approved retry, effect and coverage recompute (Consent).
(6) Domain checks: id syntax, role shape, name validity and collision,
project resolution (`not_found`), for a scoped hire create that the caller is
the target project's live head (`write_not_permitted`), for a scoped
`create_project` that the resolved project id is new (`write_not_permitted`),
supervisor, existence, terminal state, cap, one live head, placement.
(7) Grant, else consent switch off → `consent_not_bound`, switch on and provider
unreachable → `consent_unavailable`, else card.

**The import.** It reads `agent`, `agent_name_claim` and
`hire_identity_mapping` from the source; a source with none of them is a
zero-agent source. Every imported row stores request key NULL and copies
`created_at_ms`, `updated_at_ms`, `terminal_at_ms`, `name_version` and
`name_normalization_version` unchanged (core `076_agent_registry.sql:10,19-22,29`
makes them NOT NULL or paired with the terminal reason). Before committing, it
checks, and otherwise refuses `import_invariant_failed` with nothing written:
every `agent_id` is `agent_` plus 8 or 16 lowercase hex (`agent_id`);
`persona_ref` NULL on every row; each live agent has exactly one active claim,
matching re-normalisation of its display name (`claim_mismatch`, also for zero
active claims); at most one live head per project within the source
(`live_head`); every `merged_into` names a source row (`merged_into`); every
non-terminal row's non-NULL `project_id` is a current project row (an alias
does not satisfy it) and every non-terminal workspace_head's `workspace_id` a
current workspace row (`binding`); a hire's supervisor taken from `079`, when
non-NULL, names a source row whose role is head and whose `project_id` equals
the hire's, live or terminal (`supervisor`), since supervisors never change and
heads retire. Terminal rows import as they are, because `remove` legitimately
leaves such rows. A successful import commits its rows and the marker,
including from a zero-agent source. The marker, not the row count, ends
`authority_not_cut_over`. `rebuild` keeps today's gate (`main.rs:225-240`) and
replays every agent op, the cap, grants and the marker; pending consent records
live outside the journal and `rebuild` leaves them.

**Who may read what.** Identity reads (`agent.resolve`, `agent.resolve_name`,
`agent.list`, `agent.peer_roster`, `agent.avatar_read`, `agent.github_identity`,
`agent.fleet_identity`, `agent.snapshot`, `agent.changes`) are admitted for every
principal, `reserved:callosum`, other reserved modules, `Direct`, `Unverified`
and flow-stamped calls included, as queries are today (`main.rs:225-228`), and
answer before the marker as after it (an empty store is a success).
`grants.list` and `grants.would_ask` for agent A are admitted only from a scoped
call stamped A and from the operator row (`Direct` or
`reserved:prefrontal-core`); anything else refuses `read_not_permitted`.

**Who may write what.** Each mutation is admitted by exactly one row below,
under the check order above; anything else refuses `write_not_permitted`.

Until SUBC's `ck` gate lands, the operator row for agent identity is
`reserved:prefrontal-core` only, and entorhinal admits every operator-row
operation from it. Core's agent-identity write ops (dispose, merge, rename, tag,
labels, avatar, GitHub identity) plus new ones for the hire cap and the import
relay to entorhinal under `reserved:prefrontal-core`, behind core's unchanged
primary-operator caller check. An agent-identity write from `Direct` refuses
`direct_identity_write_not_admitted`, with a message naming core's relay; the
one exception is `grants.revoke` from `Direct`, admitted because revoking only
narrows authority. Once the `ck` gate lands, a gated `Direct` write is admitted
as the operator's and core's relay ops are removed in a later cut; that row
changes in an entorhinal release, never by detecting the gate at runtime.

The scoped column applies to a call stamped with an agent session and a live
entorhinal agent id, whether it names a tool or the management op. Admission in
it keys on the stamped agent, not its role: the janitor (whatever role the
operator row creates it with) and a head reach the same cells, and the card is
the user's check. A session with no entorhinal agent has no admitting row: in
this cut a new agent is created only by a live scoped agent or by the operator
row, and core does not relay non-operator self-registration.

| Operation | scoped agent call | operator row | `reserved:callosum` |
| --- | --- | --- | --- |
| `create_project` (and the workspace it creates) | grant, else card | as today (project ops) | refused |
| `agent.create` head | grant, else card; no cap | prefrontal-core: admitted | refused |
| `agent.create` hire | caller must be the live head of the target project, else `write_not_permitted`; cap, then grant, else card | prefrontal-core: admitted after the cap | refused |
| `agent.create` assistant, workspace_head | refused | prefrontal-core: admitted | refused |
| `agent.dispose` own hire (caller is its recorded supervisor) | admitted, no card | prefrontal-core: admitted | refused |
| `agent.dispose` any other agent, `agent.merge` | card (no Always) | prefrontal-core: admitted | refused |
| rename, tag, labels, avatar, GitHub identity | card (no Always) | prefrontal-core (relay, and the mint ceremony): admitted | refused |
| `agent.set_hire_cap`, `agent.import` | refused | prefrontal-core: admitted | refused |
| `grants.revoke` | own grants only | any grant (`Direct` included) | refused |

Every scoped mutation, card-free and grant-admitted paths included, resolves the
stamped agent live in the executing transaction, else `requester_not_live` with
nothing written; `grants.revoke` of the caller's own grants is exempt. Core
removes scopes through its replica, so a retired agent's stamped call can still
arrive.

**Supervisor.** A scoped hire create records the stamped calling head as
supervisor (the tool body cannot name one). A management-op `agent.create` hire
naming a `supervisor_agent_id` other than the stamped caller refuses
`invalid_supervisor`, and a card's approver is never recorded. An operator-row
hire create records NULL unless it names a `supervisor_agent_id` that is the
live head of the hire's project, else `invalid_supervisor`. A create of any
other role naming a `supervisor_agent_id` refuses `invalid_supervisor`; head,
assistant and workspace_head rows have NULL supervisor. The supervisor never
changes after create.

A scoped `create_project` whose resolved project id already exists (a rename,
an extension, or an identical re-register) refuses `write_not_permitted` and
writes nothing; other project and workspace mutations keep today's gate
(`main.rs:216-240`: `Direct` or `reserved:prefrontal-core`). SUBC's planned
`ck` gate (a step an agent's shell cannot satisfy) is what later lets a
`Direct` write be trusted as the operator's.

**Hire cap.** Each project has a cap on live hires, default 10 when unset,
counted as non-terminal `role = hiree` rows bound to that project (a dormant
hire counts). It applies to every hire create, from any principal and whatever
grant is held, and to no other role: at cap N the create refuses
`hire_cap_reached` naming N, and `grants.would_ask` reports that refusal with N
rather than "no card". The import ignores the cap. The cap is a journaled
operator mutation with a replay arm; removing the project deletes its cap row in
the same entry, so a recreated project id starts at the default.

**Grants.** Entorhinal stores its own "Approve Always" grants, keyed by
requesting agent id, action class (create workspace, create project, create
head, create hire) and scope: the workspace for project and head creates, the
head's own project for hires, or `fleet`. A class is an effect predicate: a
`create_project` that names a new workspace needs both the create-workspace and
create-project classes. A request raises one card covering exactly the classes
in its resolved effect that no held grant covers; a held class is never carded
again. When the missing classes include create-workspace, or the target has no
existing workspace (unplaced, or one this request would create), the card
offers Always fleet-wide and no per-workspace Always; otherwise Approve Always
at the workspace (for a hire, the project) scope and Always fleet-wide;
dispose, merge, rename, tag, labels, avatar and GitHub-identity cards offer no
Always. A workspace-scoped grant covers only its class in that workspace. The
pending record stores the classes the card offered; an Always answer mints one
grant row per offered class, never another, at the chosen scope, recording the
card id, only from an answer to a card entorhinal raised, and only in the
transaction that executes the request: a retry ending `spent` mints nothing.
Grants are journal entries, so `rebuild` reproduces them. Removing a project or
workspace revokes grants scoped to it in the same entry. Shapes follow the
common `grants.*` interface in `commons`.

**Project lifecycle with agents bound.** A non-terminal agent binds a project
when its `project_id` names it; it binds a workspace when it is a
workspace_head whose `workspace_id` names it, or its project is placed in it (a
head's or hire's stored `workspace_id` alone binds nothing). `remove` of a bound
project or workspace refuses `bound_by_live_agent`, which also covers a project
merge-by-alias that would put two heads on one project. Any op whose effect
changes a bound project's placement refuses `bound_by_live_agent` and writes
nothing, the whole call refused rather than a row skipped, whatever the entry
op: `assign_workspace`, `register` attaching an unplaced project to a workspace
(`mutations.rs:415,430`), `upgrade_implicit` and `seed_import`. A project whose
agents are all terminal moves. Refused ops are not journaled, so replay is
unaffected. Retargeting claims on a move is not part of this cut. Identity reads
derive workspace through project placement, so `upgrade_implicit`'s replay arm,
which today writes no workspace or placement rows
(`crates/entorhinal-core/src/mutations.rs:851-863`), is fixed to write the same
rows the live op writes.

**The simulator fix.** Apps reach entorhinal through Callosum stamped
`reserved:callosum`. Callosum's simulator profile allows the same mutating and
card-answering operations as the phone's, and simulator device keys are stored
unencrypted, so any process running as the user can act as a simulator. A
`reserved:callosum` mutation is therefore not the user's tap, and a card a
simulator can answer approves nothing. `reserved:callosum` mutations stay
refused until the mutating profile is fixed; the row then changes in an
entorhinal release, not by runtime detection.

**Consent.** Entorhinal carries no card logic; it depends on `consent/v1`
(implemented by `cingulate`) and raises card kind `entorhinal.identity_request`.
- Binding is an explicit switch, off in every release until Callosum removes
  simulators from the answering profile; an entorhinal release turns it on, and
  a provider's presence never binds it. Switch off: every card path refuses
  `consent_not_bound` and no grant is minted, whatever is registered. Switch on
  and provider not reachable: `consent_unavailable`. Switch on and reachable: a
  card. Tests set the switch, a fake provider, `origin`, `actor` and a clock
  through a handler test seam.
- `consent/v1` is never listed in the manifest's `requires`: the daemon holds a
  module not-ready while a required capability has no provider
  (`main.rs:986-989`), which would stop project ops, identity reads and operator
  writes.
- Entorhinal reads the calling agent from the daemon's inbound stamp on a scoped
  call, so it moves to subc-protocol 0.29 (pinned 0.28.1 today, `Cargo.toml:9`)
  and the subc-client-rs release built on it (0.25.2 today, `Cargo.toml:8`,
  `Cargo.lock:639-655`), which must deliver the per-request stamp to the
  handler, before prefrontal emits `flow_id`, and tells SUBC when stamp reading
  starts.
- To raise a card it opens its route to `cingulate` under the caller's session
  scope as a targeted carrier (core lists `reserved:entorhinal` with
  `destinations: ["cingulate"]`; the daemon checks it at `route.open`; cingulate
  reads the agent from its own stamp). Entorhinal supplies no subject field and
  opens that route only while handling a live inbound scoped request from that
  session, whether it names a tool or the management op.
- A closed route (`scope_ended`, `carrier_removed`) returns
  `consent_route_closed` and is never retried in a loop.
- No store lock is held across a consent wait or a feed long-poll: today every
  op runs under the store mutex (`main.rs:271-290`), so the wait runs outside
  `with_store`, and a resolve during a wait succeeds.
- Two digests, BLAKE3 over RFC 8785 canonical JSON. The **request digest**
  covers `{op, requesting agent id, op body after tool conversion and without
  the request key}`; it identifies a request for `request_key_conflict`. The
  **binding digest** covers the request digest plus the resolved effect; it is
  the card's `binding`. The effect is computed from current state, never from a
  minted value: `create_project` → the project id it would mint, whether that
  id already exists, whether a workspace is created, and the target workspace;
  `create_head`/`create_hire` → the alias-resolved current project id, that
  project's current workspace or none, the role and the normalised name;
  dispose, rename, tag, labels, avatar, GitHub identity → the target agent id
  resolved live; merge → that plus `merged_into`. On an approved retry the
  effect and the required classes are recomputed before any other domain
  check; if the binding digest differs, or a required class is covered neither
  by the card's offered classes nor by a grant held now (a grant revoked while
  pending), the call refuses `consent_binding_mismatch` and writes nothing.
- Request keys are one global space (`request_key` UNIQUE, `lib.rs:46`). The
  pending record (requesting agent, op, request key, request digest, binding
  digest, offered classes, card id, expiry, state) is unique on request key,
  stored outside the journal, and survives a restart and `rebuild` until the
  card's 24 h expiry. A same-key, same-agent, same-request-digest call while
  pending returns the same card with no second card or journal row, even if the
  effect has since changed; simultaneous first calls produce one card. A
  same-key call from another agent or with another request digest refuses
  `request_key_conflict`; a key journaled under another op refuses
  `request_key_reused_across_ops`.
- States and a same-key call's reply: `pending` → the same card; `denied` →
  `consent_denied`; `expired` → the record is replaced and a new card raised;
  `consumed` → the cached result; `spent` (an approved retry refused by any
  check of steps 4 to 6, e.g. `consent_binding_mismatch`, `requester_not_live`,
  `agent_name_taken`, `agent_terminal`, `not_found`, a passed-through `register`
  refusal, `invalid_supervisor`, `hire_cap_reached`, `live_head_exists`,
  `bound_by_live_agent`) → that same refusal, with no mutation and no grant.
- A wait is capped at 2 minutes; then the call returns `consent_pending` with the
  card id. An approved retry rechecks in one transaction, then consumes the
  approval and appends the journal row; a concurrent retry finds it consumed and
  gets the cached result. No journal row is written for a pending or denied
  call.

**Two-step create.** Identity is minted first, residence bound second by core.
- An interrupted create leaves a dormant agent, a state that already exists, is
  rendered, and has an operator action.
- `agent.create` is idempotent by its request key. The key is recorded on the
  row; it is an opaque correlation token.
- `live_head_exists` names the occupying head's agent id and its request key, or
  an explicit null for an imported head. Entorhinal cannot see residence, so the
  refusal carries no dormant bit.
- No automatic sweep: a dormant head from an interrupted create is
  indistinguishable from one created on purpose and not yet started.

**An identity change feed that consumers pull.** Core resolves agents on almost
every delivery, so it keeps a local read replica fed from entorhinal and never
asks entorhinal on those paths; an entorhinal restart must not stop peer
messages or room posts. The replica's projection is entorhinal's agent and claim
tables, all rows.
- `agent.changes` returns identity entries with seq above the cursor, in journal
  order, up to `limit`. The reply cursor is the highest seq examined: the last
  identity entry returned, or, when trailing entries were non-identity, the last
  of those. A scan that finds only non-identity entries above the cursor
  returns at once with empty `entries` and the advanced cursor. With `wait` and
  no entry at all above the cursor, it holds the call until an entry commits or
  25 s pass, re-reading under a short lock and holding none between reads.
- `agent.snapshot` returns every agent row (terminal included) and every claim
  (active and released) with the `(incarnation, generation)` read in the same
  transaction, so changes after that generation complete it with nothing
  skipped.
- A request whose `incarnation` is not current, or whose cursor is above the
  head, answers `snapshot_required` with no entries; a same-incarnation gap is
  ordinary catch-up.
- Identity consumers use only this feed. `journal_tail`
  (`crates/entorhinal-core/src/lib.rs:436-467`) becomes a project-journal read:
  it returns project-op rows only, with `generation` read in the same
  transaction as today. A reply with fewer rows than `limit` holds every
  project row up to `generation`, so the caller advances to `generation`; a
  full reply advances to its last row's seq.

The feed is pulled: entorhinal opens no route to consumers and gains no
dependency on core. Merge is a retirement for every purpose outside entorhinal.
Authority checks that must be fresh (the bot-token mint, anything that gates a
grant) read entorhinal directly and fail closed.

**Nothing precludes mirroring identity across machines later.** Replay never
re-mints: minted ids and every other generated value are in the journal entry.
The identity record holds no machine-local value; agents reference projects by
`project_id`, never a root path. Each process mints its own incarnation.

**Clean cutover of authority.** One import copies the registry from core's
store; in the same cut the store and the authority move to entorhinal. Core's
identity write ops are not removed: they become relays under
`reserved:prefrontal-core` with core's caller check unchanged, and
`agent.create` becomes identity-first then residence. Core's identity read ops
that running host plugins call keep serving from its replica until a bundle
without them is running, each removal checked against that bundle's decoders.
That list is core's. An install whose core store never applied migration 076
(every install created after the cut) obtains the marker by one zero-agent
import. Entorhinal cannot tell a fresh install's zero-agent import from a
premature one, so core's trigger must exclude a store that once held identity
tables.

### Dependencies (other owners; none is an acceptance item here)

- **ALF, core's half:**
  - residence moves to its own table under today's all-or-nothing group CHECK;
  - `agent.create` becomes identity-first then residence, and sends the
    identity step under `reserved:prefrontal-core` only for a caller core's
    primary-operator check admits; core does not relay a non-operator session's
    self-registration;
  - core's agent-identity write ops (dispose, merge, rename, tag, labels,
    avatar, GitHub identity) become relays under `reserved:prefrontal-core`,
    plus new relay ops for the hire cap and the import, with the
    primary-operator caller check unchanged. Every relayed write, the create's
    identity step and `mint_complete`'s `agent.set_github_identity` included,
    sends a `request_key`, strips the removed fields and renames `generation`
    to `agent_generation`. Relay contract: entorhinal's typed refusal passes
    through verbatim, never wrapped; the relay reply carries the new
    `(incarnation, generation)`; core's replica catches up from the change
    feed, so a read in the same turn may lag by one poll, and the generation in
    the reply lets a caller tell. The relays are removed in a later cut, after
    the `ck` gate admits gated `Direct` writes;
  - on first start with the marker absent, core calls the import relay with a
    zero-agent source only if its migration ledger shows 076 never applied; a
    store whose identity tables were dropped by the cut migration never
    triggers it, and core waits for the operator's import;
  - the bot-token mint reads claims and `agent_generation` from entorhinal,
    embeds the incarnation, and refuses when entorhinal cannot answer; token
    verifiers compare both;
  - the pending-mint ceremony stays in core and binds through
    `agent.set_github_identity`;
  - `agent.fleet_overview` is deleted; core serves its half keyed by `agent_id`,
    with `pendingAskCount` attributed by agent;
  - the seven operator scripts call core's relay ops, as they call core's ops
    today, with `script/disable-nodark-outside-cortexkit.ts` (raw SQL on
    `agent`) as its own item and test;
  - core's replica, fed from `agent.changes`, acts on retirements
    (`agent.dispose`, `hire_retired_by_supervisor`, `agent.merge`) exactly as
    terminalisation does today (`agent_claims.rs:343-365`): bounce queued
    deliveries on dispose but not merge, supersede undelivered wake fires on
    both, revoke authored flows, remove scopes; and surfaces
    `hire_retired_by_supervisor` to the operator;
  - `reserved:entorhinal` is listed as a carrier with
    `destinations: ["cingulate"]`;
  - core confirms that the named-session mint check stays sound once its two
    reads are in different stores.
- **ALF, cingulate:** `consent/v1` with the opaque `binding`, separate subject
  and requester fields, the two error codes, and admitting `reserved:entorhinal`
  as requester for kind `entorhinal.identity_request` with its four options.
- **CKIOS, with the TUI and desktop owners:** the two-call fleet view per the
  join vector, uploaded against the vector before the cut.
- **CALLO:** expose entorhinal's identity reads to the phone profile; remove
  simulators from the answering profile (gates the consent switch) and from the
  mutating profile (gates app-initiated writes).
- **SUBC:** subc-protocol 0.29 and a subc-client-rs release on it that hands the
  per-request inbound stamp (agent id, `flow_id`, `origin`) to the handler
  (today the principal is recorded once per route at bind,
  `main.rs:180-183,362-366`); the `ck` gate; review of entorhinal becoming a
  stamp reader, a carrier and a tool provider; the daemon's carrier check at
  `route.open`, which this repository does not test.

### Cutover order

1. Entorhinal ships on 0.29 with the identity store, reads, mutations, tools and
   import while core is authoritative, placed with `--migrates` so a backup is
   taken before first open. The identity store is empty, the marker absent, the
   consent switch off, identity reads answer from the empty store, and agent
   mutations refuse `authority_not_cut_over`. A binary without the agent
   migrations refuses the migrated store as ahead of it
   (`crates/entorhinal-core/src/lib.rs:146-156`), so rolling back means
   restoring that backup.
2. Callosum exposes identity reads to the phone; the phone build with the
   two-call fleet view is uploaded against the vector.
3. **The cut, in one restart window:** stop core and copy its store; place
   core's build with the replica, without its identity tables (its ledger keeps
   076 applied, so it never self-imports), with its identity write ops turned
   into relays (and `agent.create` identity-first then residence); the operator
   calls core's import relay with the copy's path; the import verifies its
   invariants and commits the marker, or refuses, and until it commits every
   relayed write refuses `authority_not_cut_over`; release the phone build.
4. After the cut: compare the fleet view against the vector, check a bot-token
   mint end to end, confirm a retirement reaches core through the feed, and
   measure how long a new agent shows `unknown`, writing that bound into the
   join vector.
5. Later, in order: Callosum's answering hardening, then an entorhinal release
   turns the consent switch on (janitor and head creation work); Callosum's
   mutating hardening (app-initiated writes admitted); SUBC's `ck` gate, after
   which an entorhinal release admits gated `Direct` writes as the operator's
   and a later cut removes core's relay ops.

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

Entorhinal also becomes a tool provider. A janitor agent (the default agent of a
fresh Alfonso install) creates workspaces, projects and heads, and a head
creates hires under its own supervision. Each such request asks the user through
the consent capability (Approve, Approve Always, Deny) unless a standing grant
already covers it.

**This campaign is entorhinal's half only.** Core's half, the consent module,
the clients and Callosum are separate work with named owners, listed under
dependencies. Every acceptance item here is evidenced in this repository.



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

## open_questions
- No open question remains. Open question 1 ("What admits an operator's
  agent-identity write?") is closed by chair rulings R1 to R5; the result is in
  constraints under "Who may write what", the clean cutover paragraph, core's
  dependency item and cutover order.
  
  The measured bound on the fleet view's `unknown` state is not open: it is a
  measurement taken after the cut (cutover order, step 4).
  
  Facts still to be quoted from core's repository before the slices that depend
  on them start (none is a design choice): the body and pinned tests of
  `normalize_agent_name` and `namespace_for_create`; the post-082 `agent` DDL and
  its role/binding CHECK; whether `hire_identity_mapping` (079) names a
  supervisor; the request and reply structs of core's agent ops; the commons
  `grants.*` shapes; the `session.transcript_page` unchanged-token fields; and how
  a 0.29 manifest declares scoped tools.
  ruling: closed by chair rulings (refire 2)
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

### recorded owner answer 5

Campaign `ct_00000000-0000-403c-98db-8c1edc263cd8`, round 4, decision sequence 3 (source: owner decision):

park for chair rulings

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

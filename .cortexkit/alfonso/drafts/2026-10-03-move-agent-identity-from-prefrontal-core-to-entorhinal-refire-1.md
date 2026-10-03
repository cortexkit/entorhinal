---
title: "2026-10-03-move-agent-identity-from-prefrontal-core-to-entorhinal refire 1"
status: draft
rounds_cap: 4
refired_from: "ct_00000000-0000-4022-98db-89e6b948e980"
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

1. **The identity store holds every field the import needs, and the rules that
   protect it.** Schema covers ids, names and claims (with history), role,
   project and workspace binding, tag, labels, avatar inputs (no stored
   fingerprint column), GitHub identity with `credential_ref` and no secret
   column, per-agent `generation`, terminal rows with `merged_into`, supervisor,
   create request key, hire caps, grants, and pending consent records. Tests
   prove:
   - the one-live-head index refuses a second live head;
   - names normalise and case-fold by core's function; an invalid name refuses
     `invalid_agent_name`;
   - rename is atomic; after dispose, the same display name can be created again
     in that namespace;
   - merge into self or a terminal id refuses `merge_target_not_live` and the
     source claim stays active;
   - both legacy id formats are accepted; new ids are random, never name-derived.
2. **The import is exact, atomic and replayable.** It opens a copy of core's
   store read-only and refuses a non-empty identity store. Placement by
   migration:
   - carried: `076` (`agent`, `agent_name_claim`), `080`
     (`github_identity_json`), `082` (the `agent` rebuild; its `wake_fire`
     stays), `109` (`generation`), `112` (avatar), `126` (`labels_json`);
   - read as input only: `079` (`hire_identity_mapping`), for supervisor if it
     records one;
   - stays in core: `076`'s `machine_binding`, `agent_delivery`, `residence_flip`
     and activation marker, `088`, `091`, `110`, `116`, `121`, `127`, `171`,
     `177`.
   It reproduces every agent, claim (active and released), label, avatar, tag,
   role, binding, GitHub identity, per-agent generation and terminal row with
   ids unchanged; supervisor is NULL unless `079` names one, and an imported
   head's request key is NULL. Before committing it checks, and otherwise
   refuses with nothing written:
   - every `persona_ref` is NULL;
   - row and claim counts equal core's, and each live agent has exactly one
     active claim matching re-normalisation of its display name;
   - at most one live head per project;
   - every `merged_into` target exists;
   - every agent `project_id`/`workspace_id` exists in entorhinal.
   Every imported row is a journal entry carrying its values. Tests: a fixture
   pins the mapping including `agent_16013c86` and a 16-hex id; a dangling
   project reference refuses; a fault injected mid-import leaves the store empty
   and a rerun succeeds; import then `rebuild` equals the imported state;
   project and workspace rows are untouched.
3. **Resolves answer.** A live id resolves `live`, a disposed id `retired`
   (including one imported as `deleted`), a merged id `merged` with its
   successor, an absent id `unknown`, a storage failure `unavailable`; each a
   distinct result in a test.
4. **Generations can be trusted.** Reads carry `(incarnation, generation)`; a
   restart changes the incarnation. Renaming agent B advances B's per-agent
   generation; renaming A or `approve_root` leaves B's unchanged. Across an
   identity mutation and `rebuild`, both generations are reproduced and a
   later mutation issues above them.
5. **Every write is admitted by exactly one row of the authorization table.** A
   test matrix calls each operation-by-principal cell, including:
   - every agent mutation refusing `authority_not_cut_over` before the import;
   - `consent_not_bound` unbound, `consent_unavailable` down;
   - a scoped head disposing its own hire with consent unbound: admitted,
     `hire_retired_by_supervisor` journaled once even on a same-key retry;
     another head's hire: card, refused on deny; a NULL-supervisor hire: card;
   - `flow_scope_not_admitted` for a `flow_id` stamp; `reserved:callosum`
     refused;
   - a grant admitting within its scope only; a create-project grant refusing to
     admit a `register` that renames an existing project or creates a workspace;
   - hire cap: with cap 10 the 11th live hire refuses `hire_cap_reached` naming
     10, retiring one admits the next, cap 2 refuses the 3rd, and
     `grants.would_ask` reports the refusal;
   - an unrelated agent's `grants.revoke` refused;
   - `remove` of a project bound by a live agent refuses `bound_by_live_agent`;
     removing a project revokes its grants, and recreating the id revives none.
   The operator-row cells follow open question 1.
6. **Consent is used only as designed.** Tests prove:
   - the cingulate route opens only inside a live inbound tool call from that
     session; a closed route returns `consent_route_closed` without retrying;
   - no store lock is held during a consent wait, and a resolve during one
     succeeds;
   - an approved card executes only a request whose digest matches; a changed
     field, another agent's card, or a second concurrent retry produce no
     effect beyond one execution;
   - a same-key call while pending returns the same card and writes no journal
     row; approval still executes after a restart within the card's lifetime;
     a requester retired while pending is refused;
   - grants are minted only from answers to entorhinal's own cards.
7. **The fleet view's identity half matches the join vector**
   (`docs/designs/fleet-view-join-vector.md`). The identity-half cases pass:
   - the avatar fingerprint pinned to a fixed genome, plus the type-and-version
     absent case, computed at serve time;
   - the unchanged token, which rename, tag, label, avatar re-roll, role or
     binding change, GitHub identity change, and a restart each invalidate;
   - `workspace` derived through the project only, so a `workspace_head` row
     omits it, as today (`fleet_overview.rs:364-379`);
   - duplicate ids a named error; an empty fleet distinct from a failed read.
8. **Two-step create is safe to retry.** A same-key retry returns the same
   agent; a missing key refuses `request_key_required`. `live_head_exists` names
   the occupier's agent id and request key, with an explicit null for an
   imported head.
9. **The journal records who wrote each identity row:** the attested principal
   and `origin` beside the caller's label.
10. **The change feed is complete and ordered.** Tests prove:
    - snapshot then every change reaches the same agent rows and claims as
      entorhinal, with mutations committed concurrently during multi-page
      consumption and none skipped at the snapshot boundary;
    - rename, dispose and merge entries decode with the change-entry fields
      listed under constraints, Names; a decoder test fails if one is missing;
    - another incarnation or a cursor above the head answers `snapshot_required`
      with no entries; a gap returns the missing entries in order; an idle poll
      returns within 25 s and does not block a concurrent mutation;
    - replay reproduces minted ids rather than generating new ones.
11. **Entorhinal runs on subc-protocol 0.29** and decodes stamps carrying
    `flow_id`.

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
`project-identity/v1`. Agent ops carry an `agent.` prefix so they never collide
with the project ops it already serves (`resolve`, `enumerate`, `remove`,
`journal_tail`).
- Reads: `agent.resolve`, `agent.resolve_name`, `agent.list`,
  `agent.peer_roster`, `agent.avatar_read`, `agent.github_identity` (claims and
  per-agent generation for the bot-token mint), `agent.fleet_identity` (the
  identity half, join-vector field names), `agent.snapshot`, `agent.changes`,
  `grants.list`, `grants.would_ask`.
- Mutations: `agent.create`, `agent.rename`, `agent.update_tag`,
  `agent.set_labels`, `agent.set_avatar`, `agent.set_github_identity`,
  `agent.dispose`, `agent.merge`, `agent.set_hire_cap`, `agent.import`,
  `grants.revoke`. Scoped agents get tools for the creates (workspace, project,
  head, hire) and `agent.dispose`.
- Change entry: `{seq, incarnation, generation, op, agent_id, agent_generation}`
  plus, per op: create, the full row; rename, `old_display_name` and
  `new_display_name`; tag, labels, avatar, GitHub identity, the new value;
  dispose, `status: "retired"`; merge, `status: "merged"` and `merged_into`;
  `hire_retired_by_supervisor`, `supervisor_agent_id` and `hire_agent_id`.
- Error codes: `authority_not_cut_over`, `write_not_permitted`,
  `flow_scope_not_admitted`, `consent_not_bound`, `consent_unavailable`,
  `consent_route_closed`, `consent_pending` (carries `card_id`),
  `consent_denied`, `hire_cap_reached` (carries the cap), `live_head_exists`
  (carries `agent_id` and `request_key` or null), `request_key_required`,
  `request_key_reused_across_ops`, `invalid_agent_name`,
  `merge_target_not_live`, `bound_by_live_agent`, `snapshot_required`.
- Card kind `entorhinal.identity_request`, options Approve, Approve Always and
  Always fleet-wide (create classes only), Deny.

**Two rules that predate this work.** The daemon never depends on entorhinal,
because it supervises it. Paths compare with the daemon's canonical path
function and are never re-normalised on entorhinal's side. Any change touching
either goes to SUBC for review before it ships.

**Agent ids are minted the opposite way to project ids.** Project ids are
deterministic (`pj-` plus a BLAKE3 hash of name and roots) and have no permanent
tombstone. Agent ids are random, never derived from name or path, and
permanently tombstoned, because a reused id would inherit the previous holder's
grants, bot ownership and approvals. Ids move unchanged in the import, and the
live store holds two formats: `agent_` plus 16 hex, and `agent_` plus 8 hex for
one agent (`agent_16013c86`). Validation accepts both for existing rows; new ids
use the current format.

**A retired or merged id stays resolvable, with its status.** Core keeps tables
that reference agent ids and that no database constraint can enforce after the
move: `session_scope.agent_id`, `flow_install.author_agent_id`,
`flow_install_grant.agent_id`, `gh_route_speech_receipt`, and
`persona_active_revision` (keyed `agent:<agent_id>`). A resolve answers exactly
one of `live`, `retired`, `merged` (with `merged_into`), `unknown`,
`unavailable`, and never collapses retired into unknown. Core's
`terminal_reason` values map `deleted` → `retired` and `merged` → `merged`.

**Identity invariants carried over verbatim.** `agent_id` is stable for life.
Names are NFC for display and case-folded for lookup, by core's own function
(quoted once gathered, not re-derived); an invalid name refuses
`invalid_agent_name`. Rename claims the new name and releases the old
atomically; one active claim per namespace and per agent; released claims keep
history. Dispose and merge set the terminal row and release the active claim in
one transaction; merge into self or into a non-live or absent target refuses
`merge_target_not_live` before anything is released
(`agent_claims.rs:289-339` in the evidence). At most one live head per project,
schema-enforced as an index on `(project_id) WHERE role = 'head' AND
terminal_reason IS NULL`, as core's `uq_live_head` is today.

**No personas.** The agent record holds identity only. Core's
`agent.persona_ref` is not carried; the import asserts it is NULL on every row
and refuses otherwise.

**Generations.** Two values, kept apart:
- **Per-agent `generation`**, as today (`agent.generation`, migration 109, signed
  as `binding_generation`, `agent_assertion.rs:352`). Imported unchanged; each
  mutation of that agent's row sets it strictly above its previous value; the
  written value is recorded in the journal entry so replay reproduces it.
  Mutations of other agents, projects or workspaces do not change it. Core's bot
  token embeds it with the incarnation.
- **Fleet `(incarnation, generation)`**, where generation is the journal head
  `MAX(seq)`. It versions reads, the feed and the fleet view's unchanged token
  (the `session.transcript_page` shape, derived from this pair).
The **incarnation** is a random id minted at process start and never persisted,
because restoring an older backup shortens the journal and lowers both
generations, and anything stored in the file would be restored with it. Every
entorhinal restart therefore invalidates outstanding tokens; the operator docs
state that.

**The journal records the attested principal.** Identity rows record the
principal the handler admitted, plus Callosum's `origin` when present, with the
request's `actor` label kept beside them as a hint. `Direct` covers every local
process, so it narrows who wrote a row but does not prove a human did.

**Credential references, never credentials.** `credential_ref` points into
claustrum. No secret moves into entorhinal.

**Who may write what.** Before the import completes, every agent-identity
mutation refuses `authority_not_cut_over`, whatever the principal. After it,
each mutation is admitted by exactly one row below; anything else refuses
`write_not_permitted`. "Operator row" is the `reserved:prefrontal-core` and
`Direct` treatment of agent identity, which open question 1 settles.

| Operation | scoped agent tool call (janitor, head) | operator row | `reserved:callosum` |
| --- | --- | --- | --- |
| create workspace, project | grant, else card | as today (project ops) | refused (simulator fix) |
| `agent.create` head or hire | cap check, then grant, else card | prefrontal-core: admitted, core keeps its caller check | refused |
| `agent.dispose` own hire (caller is its recorded supervisor) | admitted, no card, journals `hire_retired_by_supervisor` | open question 1 | refused |
| `agent.dispose` any other agent, `agent.merge` | card (no Always) | open question 1 | refused |
| rename, tag, labels, avatar, GitHub identity | card (no Always) | open question 1; prefrontal-core always for the mint ceremony | refused |
| `agent.set_hire_cap` | refused | open question 1 | refused |
| `grants.revoke` | own grants only | any grant | refused |

A call whose stamp carries `flow_id` is not a scoped agent call: identity
mutations from it refuse `flow_scope_not_admitted`. Reads (`grants.list`,
`grants.would_ask`, resolves) are open to the grant's own agent and the operator
row. Project and workspace mutations other than the creates keep today's gate
(`main.rs:216-240`: `Direct` or `reserved:prefrontal-core`). SUBC's planned
`ck` gate (a step an agent's shell cannot satisfy) is what later lets a `Direct`
write be trusted as the operator's.

**Hire cap.** Each project has a cap on live hires, default 10 when unset,
counted as non-terminal `role = hiree` rows bound to that project (residence is
not visible here, so a dormant hire counts). It applies to every hire create,
from any principal and whatever grant is held: at cap N, the create refuses
`hire_cap_reached` naming N, and `grants.would_ask` never answers "no card" for a
create the cap would refuse. The import is not a create and ignores the cap. The
cap is a journaled operator mutation with a replay arm.

**Grants.** Entorhinal stores its own "Approve Always" grants, keyed by
requesting agent id, action class (create workspace, create project, create
head, create hire) and scope: the workspace for workspace, project and head
creates, the head's own project for hires, or the explicit `fleet` scope, a
separate card option never implied by a workspace grant. One answer mints one
grant row, recording the card id, and only from an answer to a card entorhinal
raised. A class is an effect predicate, not a method: a create-project grant
admits only a `register` that mints a new project id in the granted workspace
and creates no workspace; a `register` that renames or extends an existing
project, or an `assign_workspace` that creates a workspace, needs its own card.
Grants and caps are journal entries, so `rebuild` reproduces them. Removing a
project or workspace revokes grants scoped to it in the same entry, so a
recreated deterministic project id never revives one. Shapes follow the common
`grants.list`, `grants.revoke`, `grants.would_ask` interface in `commons`.

**Project lifecycle with agents bound.** `remove` of a project or workspace that
a non-terminal agent binds refuses `bound_by_live_agent`, which also covers a
project merge-by-alias that would put two heads on one project.
`assign_workspace` of a project with non-terminal agents refuses the same way
until core's claim-namespace rule is quoted.

**The simulator fix.** Apps reach entorhinal through Callosum stamped
`reserved:callosum`. Callosum's simulator profile allows the same mutating and
card-answering operations as the phone's, and simulator device keys are stored
unencrypted, so any process running as the user can act as a simulator. So a
`reserved:callosum` mutation is not the user's tap, and a card a simulator can
answer approves nothing. Entorhinal therefore binds `consent/v1` only after
Callosum removes simulators from the answering profile; until then every card
path refuses `consent_not_bound` and no grant is minted. `reserved:callosum`
mutations stay refused until the mutating profile is fixed too; the row then
changes in an entorhinal release, not by runtime detection.

**Consent.** Entorhinal carries no card logic; it depends on `consent/v1`
(implemented by `cingulate`) and raises card kind `entorhinal.identity_request`.
- No capability bound: refuse `consent_not_bound` (permanent until configured).
  Bound but down: `consent_unavailable` (transient). Never one code for both.
- Entorhinal reads the calling agent from the daemon's inbound stamp on a scoped
  tool call, so it moves to subc-protocol 0.29 before prefrontal emits `flow_id`
  and tells SUBC when stamp reading starts.
- To raise a card it opens its route to `cingulate` under the caller's session
  scope as a targeted carrier (core lists `reserved:entorhinal` with
  `destinations: ["cingulate"]`; the daemon checks it at `route.open`; cingulate
  reads the agent from its own stamp). Entorhinal supplies no subject field and
  opens that route **only while handling a live inbound tool call from that
  session**.
- A closed route (`scope_ended`, `carrier_removed`) returns
  `consent_route_closed` and is never retried in a loop.
- No store lock is held across a consent wait or a feed long-poll: today every
  op runs under the store mutex (`main.rs:271-290`), so the wait runs outside
  `with_store`, and a resolve during a wait succeeds.
- Binding: the opaque `binding` is BLAKE3 over the RFC 8785 canonical JSON of
  `{op, requesting agent id, request body without request_key}`; a one-field
  change does not match. The pending record (card id, digest, agent, expiry,
  state `pending`/`consumed`) is stored outside the journal and survives a
  restart until the card's 24 h expiry.
- A wait is capped at 2 minutes; then the call returns `consent_pending` with the
  card id. A same-key call while pending returns the same card without a second
  card or journal row. On an approved retry, entorhinal rechecks that the
  requester is live, the cap, and one-live-head, then consumes the approval and
  appends the journal row in one transaction; a concurrent retry finds it
  consumed and gets the cached result. The journal row is written only on
  execution, never for a pending or denied call (`mutations.rs:290-337` caches
  replies by request key).

**Two-step create.** Identity is minted first, residence bound second by core.
- An interrupted create leaves a dormant agent, a state that already exists, is
  rendered, and has an operator action.
- `agent.create` requires a request key (missing → `request_key_required`), and
  is idempotent by it. The key is recorded on the row; it is an opaque
  correlation token carrying nothing a caller would mind seeing in an error.
- A one-live-head refusal `live_head_exists` names the occupying head's agent id
  and its request key, or an explicit null for an imported head. Entorhinal
  cannot see residence, so the refusal carries no dormant bit.
- No automatic sweep: a dormant head from an interrupted create is
  indistinguishable from one created on purpose and not yet started.

**An identity change feed that consumers pull.** Core resolves agents on almost
every delivery, so it keeps a local read replica fed from entorhinal and never
asks entorhinal on those paths; an entorhinal restart must not stop peer
messages or room posts.
- `agent.changes` returns identity entries after a cursor, in journal order. The
  cursor is the seq of the last entry returned (or the head when none), never
  the head alone, and it advances past non-identity entries. It long-polls up to
  25 s, re-reading under a short lock.
- `agent.snapshot` returns every agent row and active claim with the
  `(incarnation, seq)` read in the same transaction, so changes after that seq
  complete it with nothing skipped.
- A cursor from another incarnation or above the head answers
  `snapshot_required` immediately with no entries; a same-incarnation gap is
  ordinary catch-up.
- Identity consumers use only this feed. `journal_tail` stays a project-journal
  read and is not an identity feed.

The feed is pulled: entorhinal opens no route to consumers and gains no
dependency on core. Merge is a retirement for every purpose outside entorhinal.
Authority checks that must be fresh (the bot-token mint, anything that gates a
grant) read entorhinal directly and fail closed.

**Nothing precludes mirroring identity across machines later.** Replay never
re-mints: minted ids and every other generated value are in the journal entry.
Ids are random. The identity record holds no machine-local value; agents
reference projects by `project_id`, never a root path. Each process mints its own
incarnation, so pairs from different machines never collide.

**Clean cutover of authority.** One import copies the registry from core's
store; in the same cut authority moves and core's identity write ops are
removed, except `agent.create`, which becomes identity-first then residence.
Core's identity read ops that running host plugins call keep serving from its
replica until a bundle without them is running, each removal checked against
that bundle's decoders. That list is core's.

### Dependencies (other owners; none is an acceptance item here)

- **ALF, core's half:**
  - residence moves to its own table under today's all-or-nothing group CHECK;
  - `agent.create` becomes identity-first then residence, keeping core's caller
    check, and always sends a request key;
  - the bot-token mint reads claims and the per-agent generation from
    entorhinal, embeds the incarnation, and refuses when entorhinal cannot
    answer; token verifiers are updated to compare both;
  - the pending-mint ceremony stays in core and binds through
    `agent.set_github_identity`;
  - `agent.fleet_overview` is deleted; core serves its half keyed by `agent_id`,
    with `pendingAskCount` attributed by agent;
  - the seven operator scripts are repointed per open question 1, with
    `script/disable-nodark-outside-cortexkit.ts` (raw SQL on `agent`) as its own
    item and test;
  - core's replica, fed from `agent.changes`, acts on retirements exactly as
    terminalisation does today (`agent_claims.rs:343-365`): bounce queued
    deliveries on dispose but not merge, supersede undelivered wake fires on
    both, revoke authored flows, remove scopes; and surfaces
    `hire_retired_by_supervisor` to the operator;
  - `reserved:entorhinal` is listed as a carrier with
    `destinations: ["cingulate"]`;
  - core confirms against its implementation that the named-session mint check
    stays sound once its two reads are in different stores.
- **ALF, cingulate:** `consent/v1` with the opaque `binding`, separate subject
  and requester fields, the two error codes, and admitting `reserved:entorhinal`
  as requester for kind `entorhinal.identity_request` with its four options.
- **CKIOS, with the TUI and desktop owners:** the two-call fleet view per the
  join vector, uploaded against the vector before the cut.
- **CALLO:** expose entorhinal's identity reads to the phone profile; remove
  simulators from the answering profile (gates every consent outcome) and from
  the mutating profile (gates app-initiated writes).
- **SUBC:** subc-protocol 0.29; the `ck` gate; review of entorhinal becoming a
  stamp reader and a carrier.

### Cutover order

1. Entorhinal ships on 0.29 with the identity store, reads, mutations, tools and
   import while core is authoritative. The identity store is empty and agent
   mutations refuse `authority_not_cut_over`.
2. Callosum exposes identity reads to the phone; the phone build with the
   two-call fleet view is uploaded against the vector.
3. **The cut, in one restart window:** run the import against a snapshot of
   core's store; it verifies its invariants or refuses; place core's build with
   the replica and without its identity tables and write ops (except
   `agent.create`); release the phone build.
4. After the cut: compare the fleet view against the vector, check a bot-token
   mint end to end, confirm a retirement reaches core through the feed, and
   measure how long a new agent shows `unknown`, writing that bound into the
   join vector.
5. Later, in order: Callosum's answering hardening, then cingulate is bound
   (janitor and head creation work); Callosum's mutating hardening (app-initiated
   writes admitted); SUBC's `ck` gate.

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
  cross-module read on its delivery paths.


## open_questions
- 1. **What admits an operator's agent-identity write?** (owner decision). The
  table's "operator row" cells for dispose, merge, rename, tag, labels, avatar,
  GitHub identity and the hire cap are open. `Direct` has no session scope, so
  the only card route this draft defines cannot raise a card for it, and before
  cingulate is bound any card path refuses. Options: core relays these writes
  under `reserved:prefrontal-core` with its primary-operator check (then core
  keeps those write ops past the cut); `Direct` is admitted as today until
  SUBC's `ck` gate; or identity outside creation is frozen until a defined
  card path for `Direct` exists. The seven repointed scripts follow whichever
  is chosen.
  
  The measured bound on the fleet view's `unknown` state is not open: it is a
  measurement taken after the cut (cutover order, step 4).
  ruling: closed by chair rulings (refire 1)
  answer: See the normative chair rulings in the chair rulings (refire) section.

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded owner answer 1

Campaign `ct_00000000-0000-4022-98db-89e6b948e980`, round 0, decision sequence 2 (source: owner decision):

park for chair rulings

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

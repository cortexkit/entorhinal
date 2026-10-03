---
title: "Move agent identity into entorhinal, and make it a consent-gated tool provider"
date: 2026-10-03
status: draft
rigor_proposed: r3
evidence:
  include:
    - docs/designs/fleet-view-join-vector.md
    - crates/entorhinal-core/src/lib.rs
    - crates/entorhinal-core/src/mutations.rs
    - crates/entorhinal-core/src/binding.rs
    - crates/entorhinal-module/src/main.rs
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/agent-identity-entorhinal.md
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/docs/designs/consent-module.md
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/migrations/076_agent_registry.sql
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_registry.rs:678-720
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-store/src/agent_claims.rs:100-420
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:41-93
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:148-327
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_registry_ops.rs:3005-3079
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/agent_assertion.rs:1-720
    - /Users/ufukaltinok/Work/Projects/CortexKit/prefrontal/crates/prefrontal-core-module/src/fleet_overview.rs:1-482
---

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

## constraints

**Ownership.** Entorhinal takes agent ids, names and their normalisation, name
claims, role, project and workspace binding, the one-live-head rule, tags,
labels, avatar, GitHub identity, disposal, merge, and the supervisor of every
hire. Core keeps residence, delivery, wake routing, board state, personas and
scope registration, and stamps an entorhinal-issued `agent_id` on the scopes it
registers.

**Two rules that predate this work.** The daemon never depends on entorhinal,
because it supervises it. Paths compare with the daemon's canonical path
function and are never re-normalised on entorhinal's side. Any change touching
either goes to SUBC for review before it ships.

**Agent ids are minted the opposite way to project ids.** Project ids are
deterministic (`pj-` plus a BLAKE3 hash of name and roots) and have no permanent
tombstone, so the same inputs mint the same id again. Agent ids are random, never derived from name or path,
and permanently tombstoned, because a reused id would inherit the previous
holder's grants, bot ownership and approvals. Ids move unchanged in the import,
and the live store holds two formats: `agent_` plus 16 hex, and `agent_` plus
8 hex for one agent that predates the current format (`agent_16013c86`).
Validation accepts both for existing rows; new ids use the current format.

**A retired or merged id stays resolvable, with its status.** Core keeps tables
that reference agent ids and cannot be enforced by any database constraint
after the move: `session_scope.agent_id`, `flow_install.author_agent_id`,
`flow_install_grant.agent_id`, `gh_route_speech_receipt`, and
`persona_active_revision` (keyed `agent:<agent_id>`). Those rows stay readable
only if the tombstone answers. So a resolve distinguishes live, retired, merged
(with `merged_into`), unknown, and unavailable, and never collapses retired
into unknown.

**Identity invariants carried over verbatim.** `agent_id` is stable for life.
Names are NFC for display and case-folded for lookup; an invalid name refuses
with a typed error. Rename claims the new name and releases the old atomically;
one active claim per namespace and per agent; released claims keep history.
Disposal and merge keep terminal rows. At most one live head per project,
**schema-enforced** as an index on `(project_id) WHERE role = 'head' AND
terminal_reason IS NULL`, as in core's `uq_live_head` today. The rule names no
residence column, so it moves intact.

**No personas.** The agent record holds identity only. Core's
`agent.persona_ref` is NULL on all 35 live rows and is not carried. Personas are
an orchestrator concept, and core keeps its own agent-to-persona mapping.

**The generation gates credential validity.** Core embeds entorhinal's
generation in the GitHub bot tokens it mints, so a consumer can refuse a token
minted against identity that has since changed. Replay already preserves the
generation and a post-replay mutation issues above every earlier value (pinned
by `generation_never_decreases_or_repeats_across_a_journal_replay`). Restoring
the store from an older backup does not: the generation is `MAX(seq)` over a
journal the restore shortens. So every identity read that carries a generation
also carries an **incarnation**: a random id minted at process start and never
persisted. Anything stored in the file would be restored with it and collide
again, while a restore always involves a restart. Every entorhinal restart
therefore invalidates outstanding tokens, and that is stated in the operator
docs, not only here.

**The journal records the attested principal.** `registry_journal.actor` today
is the label the request carried (`ck-projects`, `module`). Identity rows also
record the principal the handler admitted, plus Callosum's `origin` when
present, with the label kept beside them as a hint. `Direct` covers every local
process, so it narrows who wrote a row but does not prove a human did.

**Credential references, never credentials.** `credential_ref` points into
claustrum. No secret moves into entorhinal.

**Who may write what.** Every identity mutation is admitted by exactly one of
these rules, and anything else refuses:

| Caller | Agent identity mutations | Project and workspace mutations |
| --- | --- | --- |
| `reserved:prefrontal-core` | admitted; core keeps its own caller check, so today's operator-driven creation keeps working | admitted, as today |
| scoped agent tool call (janitor, head) | standing grant, else a consent card | standing grant, else a consent card |
| `Direct` (any local process, including an agent's shell) | a consent card labelled "an unverified local process"; never admitted on the stamp alone | admitted, as today, until SUBC's `ck` gate lands |
| `reserved:callosum` (the apps) | refused until the simulator fix below | refused until the same |
| anything else | refused | refused |

Direct agent-identity writes are stricter than project writes because today
creating an agent already requires core's primary-operator check; admitting
`Direct` would weaken that. Project writes are already open to `Direct`. An
agent's shell can run the local `ck` CLI and so reaches entorhinal as `Direct`;
SUBC owns a planned gate on `ck` (a sudo-like step or secret an agent process
cannot supply) that will stop agents writing through it, after which both kinds
of `Direct` write can be trusted as the operator's.

**The simulator fix.** Requests from the Alfonso apps reach entorhinal through
Callosum, the federation module, stamped `reserved:callosum`. Callosum decides
which operations each kind of device may call through named profiles. Its
simulator profile currently allows the same mutating and card-answering
operations as the phone's, and simulator device keys are stored unencrypted, so
any process running as the user can send requests as a simulator. Until Callosum
removes those operations from the simulator profile, a `reserved:callosum`
mutation cannot be trusted as the user's own tap, and a consent card does not
help because a simulator could answer it too.

**Consent.** Entorhinal carries no card logic. It depends on the `consent/v1`
capability (implemented by `cingulate`).
- With no capability bound, a mutation that needs consent refuses
  `consent_not_bound`, which is permanent until configured. With one bound but
  down, it refuses `consent_unavailable`, which is transient. They never share a
  code.
- Entorhinal reads the calling agent from the daemon's inbound stamp on a scoped
  tool call. That makes it a `ScopeAttributes` reader, so it moves to
  subc-protocol 0.29 before prefrontal emits `flow_id`, and tells SUBC when stamp
  reading starts.
- To raise a card for that agent, entorhinal opens its route to `cingulate`
  under the caller's session scope as a targeted carrier. Core lists
  `reserved:entorhinal` with `destinations: ["cingulate"]` on agent sessions;
  the daemon checks it at `route.open`; cingulate reads the agent from its own
  inbound stamp. Entorhinal supplies no subject field.
- The remaining trust is that entorhinal may act for that agent toward cingulate
  while the session is live. The daemon cannot narrow it further, so entorhinal
  opens that route **only while handling a live inbound tool call from that
  session**.
- A closed route (`scope_ended`, `carrier_removed`) means the card cannot be
  raised now. The tool call returns that and never retries in a loop.
- Entorhinal never holds a store transaction across a consent wait: it asks,
  waits, then opens the write.
- A card lives up to 24 h and a wait is capped at 2 minutes, so a tool call that
  is still pending returns "pending, card X". The agent calls again after
  approval. Entorhinal sends an opaque `binding` that is a digest of the exact
  request, and executes only when the approved card's returned binding matches
  the digest of the request being retried. One approval never authorizes a
  different request.

**Mutation rules, as decided by the project owner (the operator) on
2026-10-03.**
- "Approve Always" covers only where it was granted: the workspace for creating
  projects and heads, the head's own project for hires. A fleet-wide Always is a
  separate, explicit option.
- Each project has a cap on live hires, default 10, stored in entorhinal and
  changed by the operator. A hire over the cap refuses with a typed error naming
  the cap, whatever standing grant the head holds.
- A head may retire its own hires without a card, and the operator gets a
  notice. "Its own" means the supervisor agent id recorded on the hire at
  creation; retiring someone else's hire needs consent.

**Standing grants live here.** Entorhinal stores its own "Approve Always" grants,
keyed by requesting agent id, action class (create workspace, create project,
create head, create hire) and the scope chosen at grant time, and serves the
common `grants.list`, `grants.revoke` and `grants.would_ask`. A grant is minted
only from an answer to a card entorhinal itself raised, recording that card's id.
The request and reply shapes are the common ones pinned in the shared `commons`
repository, with test vectors that every provider checks itself against.

**Two-step create.** Identity is minted first, residence bound second by core.
- A create interrupted between the steps leaves a dormant agent, which is a
  state that already exists, is already rendered, and already has an operator
  action.
- Creates are idempotent by request key, so a retry returns the same agent
  instead of colliding with the one-live-head index.
- The request key is recorded on the agent row. A one-live-head refusal names
  the dormant head and its request key, so the operator can retry the original
  request or dispose of the head. The key is an opaque correlation token and
  must carry nothing a caller would mind seeing in an error.
- No automatic sweep: a dormant head from an interrupted create is
  indistinguishable from one created on purpose and not yet started.

**Lifecycle notices.** Rename, retirement and merge are published on the
existing journal tail, so core and providers follow them without a new channel.
Merge is a retirement for every purpose outside entorhinal: core revokes a
merged agent's authored flows exactly as for a disposed one.

**Clean cutover, no coexistence.** This machine is the only install. There is
no dual-read period and no compatibility layer: one import copies the registry
from core's store, and core removes its identity tables and ops in the same cut.

## acceptance sketch

Each acceptance item below is backed by a test or a command in this repository.

1. **The identity store holds every field the import needs, and the rules that
   protect it.** Schema covers ids, names and claims, role, project and
   workspace binding, tag, labels, avatar, GitHub identity with
   `credential_ref`, terminal rows with `merged_into`, supervisor, and the
   create request key. Tests prove:
   - the one-live-head index refuses a second live head;
   - names normalise and case-fold;
   - rename is atomic;
   - terminal rows persist;
   - both legacy id formats are accepted;
   - new ids are random and never derived from a name.
2. **The import is exact.** Run against a read-only copy of core's live store,
   it reproduces every agent, claim, label, avatar, tag, role, binding and
   GitHub identity with ids unchanged. Its inventory includes the migrations
   core's own design note omitted (`079`, `080` which creates
   `agent.github_identity_json`, `082` which rebuilds the `agent` table, `088`,
   `110`, `116`, `127`, `171`). An invariant check passes on the result, and a
   fixture test pins the mapping.
3. **Tombstones answer.** A retired id resolves as retired, a merged id as
   merged with its successor, an absent id as unknown, and a storage failure as
   unavailable. Each is a distinct result in a test.
4. **The generation can be trusted.** Identity reads carry
   `(incarnation, generation)`. A test proves a restart changes the
   incarnation, and the replay invariant stays green.
5. **Every write is admitted by exactly one row of the authorization table.** A
   principal-by-operation test matrix covers every cell, including:
   - `consent_not_bound` with no capability bound;
   - `consent_unavailable` when it is down;
   - `Direct` agent writes going to a card;
   - `reserved:callosum` refused;
   - a standing grant admitting within its scope and not outside it;
   - the hire cap refusing at 11 live hires;
   - a head retiring its own hire with a notice, and refused for another head's.
6. **Consent is used only as designed.** Tests prove:
   - the cingulate route opens only inside a live inbound tool call from that
     session;
   - no store transaction is open while a consent wait is in progress;
   - a closed route returns "cannot ask now" without retrying;
   - an approved card executes only the request whose digest it carries;
   - standing grants are minted only from answers to entorhinal's own cards.
7. **The fleet view's identity half matches the join vector.** The fleet view
   lists agents by joining an identity half (served by entorhinal) to a session
   half (served by core). `docs/designs/fleet-view-join-vector.md` defines that
   join and its test cases. The cases that belong to the identity half pass:
   - the avatar fingerprint pinned to a fixed genome, plus the case with type
     and version absent;
   - the unchanged token, which a re-roll or a label change invalidates;
   - duplicate ids refused;
   - an empty fleet.
8. **Two-step create is safe to retry.** A same-key retry returns the same
   agent. A one-live-head refusal names the dormant head and its request key.
9. **The journal records who wrote each identity row.** It holds the attested
   principal and `origin` beside the caller's label.
10. **Lifecycle notices reach the journal tail.** Rename, retire and merge
    appear there in a shape a follower can act on.
11. **Entorhinal runs on subc-protocol 0.29.** It decodes stamps carrying
    `flow_id`.

## dependencies (other owners; none is an acceptance item here)

- **ALF, core's half:**
  - residence moves to its own table under today's all-or-nothing group CHECK;
  - create becomes identity-first then residence, keeping core's caller check;
  - the bot-token mint reads claims from entorhinal, embeds
    `(incarnation, generation)`, and refuses when entorhinal cannot answer;
  - `agent.fleet_overview` is deleted, and core serves its half keyed by
    `agent_id`, with `pendingAskCount` attributed by agent;
  - the seven operator scripts are repointed, with
    `script/disable-nodark-outside-cortexkit.ts` (raw SQL on `agent`) as its own
    item with its own test;
  - core follows lifecycle notices to revoke flows and remove scopes;
  - `reserved:entorhinal` is listed as a carrier with
    `destinations: ["cingulate"]`;
  - core's identity tables and ops are removed in the cut;
  - core confirms against its implementation, not only the design, that the
    named-session mint check stays sound once its two reads are in different
    stores.
- **ALF, cingulate:** `consent/v1` with the opaque `binding`, separate subject
  and requester fields, and the two error codes. Until it is bound, entorhinal
  refuses agent-initiated writes, so neither waits for the other.
- **CKIOS, with the TUI and desktop owners:** the two-call fleet view per the
  join vector, uploaded against the vector before the cut.
- **CALLO:** expose entorhinal's identity reads to the phone profile; remove
  simulators from the mutating and answering profiles. The second gates
  app-initiated creation only.
- **SUBC:** subc-protocol 0.29; the `ck` gate that an agent's shell cannot
  satisfy; review of entorhinal becoming a stamp reader and a carrier.

## cutover order

1. Entorhinal is placed on 0.29 with the identity store, reads, mutations,
   tools and import, while core is still authoritative. Its identity store is
   empty, nothing consumes it yet, and agent-initiated writes refuse
   `consent_not_bound`.
2. Callosum exposes entorhinal's identity reads to the phone. The phone build
   with the two-call fleet view is built and uploaded against the vector.
3. **The cut, in one restart window:** run the import against a snapshot of
   core's store; verify invariants; place core's build that reads identity from
   entorhinal and has its identity tables and ops removed; release the phone
   build.
4. After the cut: compare the fleet view against the vector, check a bot-token
   mint end to end, and confirm a retirement notice reaches core.
5. Later, independently: cingulate is bound, after which janitor and head
   creation work; Callosum's simulator hardening, after which app-initiated
   creation is admitted; SUBC's `ck` gate, after which `Direct` project writes
   tighten too.

## non-goals

- Flow identity. Flows are an attribute (`ScopeAttributes.flow_id`, set and
  resolved by core), not an agent kind, so entorhinal mints nothing for them.
- Personas, and `agent.role_tool_permissions`. The latter's "role" is a worker
  profile kind (`explore`, `librarian`, `mason`), and its handler cannot read
  the registry.
- Per-agent permission state of other providers. Their grants stay with them,
  keyed by entorhinal's agent id.
- Card rendering, presence, push, or any part of cingulate.
- Building `agent.fleet_overview` anywhere. It is deleted, not moved.
- Any dual-read, proxy or compatibility phase.

## open_questions

- Does anything bound how long the fleet view's `unknown` state can last, the
  window where entorhinal knows an agent and core has no row for it? Nothing is
  measured yet. The bound is measured after the cut and written into the join
  vector, rather than each client choosing its own tolerance.

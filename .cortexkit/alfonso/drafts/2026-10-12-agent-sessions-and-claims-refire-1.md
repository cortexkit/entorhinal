---
title: "2026-10-12-agent-sessions-and-claims refire 1"
status: draft
rounds_cap: 1
mint: auto
refired_from: "ct_00000000-0000-408a-98dd-e572e1ab76c8"
integration_ref: "main"
evidence:
  include:
    - ".cortexkit/alfonso/context/agent-sync-draft11-excerpt.md"
    - "crates/entorhinal-core/src/lib.rs:100-240"
    - "crates/entorhinal-core/src/log_schema.rs"
    - "crates/entorhinal-module/src/main.rs:440-800"
    - "crates/entorhinal-module/src/log_client.rs"
    - "crates/entorhinal-module/src/catch_up.rs"
    - "crates/entorhinal-module/src/shared_write.rs:2370-2470"
    - "crates/entorhinal-module/src/agent_ops.rs"
    - "crates/entorhinal-module/src/cli.rs:1-140"
    - "mutations.toml"
---

## acceptance_sketch
1. The orchestrator registers a session for a live, unclaimed agent, and the binding is stored. A second registration of the same ref for another agent is refused with `ref_bound_elsewhere`.
2. With a cached claim held by another device, registration is refused with `held_elsewhere`, naming the holder and its display name. With the claim held here, or absent, or at epoch 0, it's admitted. After a restart with a failing engram connector, a cached claim at epoch 1 whose holder is the persisted `self_author` is still admitted, and `agent.where` answers `held_here`.
3. `Direct`, another module and a flow route are refused with the stated codes, in the stated order. With no orchestrator bound, an ambiguous provider, or a resolution transport failure, the answer is `orchestrator_unbound`. All of this runs in-process against the provider fake.
4. `agent.session_liveness` maps active, idle, gone, lapsed, missing, unbound and a waiting mirror exactly as specified, in request order, and the same session id under two harnesses stays two sessions.
5. A liveness batch carrying `lapsed` or an unrecognised state is accepted, that session reads as unknown, and `enumerate`'s annotation of the other sessions is unchanged. The feed's non-orchestrator refusal is proven in the cutover slice.
6. Refresh caches claims from the fake engram: absent, held here, held elsewhere, a stale older version that must not replace a newer cached one, and a stale absent that must not replace a cached claim at epoch 1 or higher. `agent.where` reports each correctly without calling engram.
7. With engram unreachable, and with engram hung while catch-up holds the log `writer` mutex, startup, every read and `agent.register_session` still answer.
8. `member.set_name` sets this node's name. A forged entry naming another author stalls apply at its position and writes no row. The name appears in `agent.where` on the other node. A node that joins after the setter's re-enable supersede still reads it from the snapshot.
9. Verify and rebuild stay clean with bindings, cached claims and names present, and the machine-local tables never appear in the journal or the identity log.

## constraints
### External dependencies
- **SUBC: reserved capability.** Built since subc-core 0.7.0, and live on the operator's Mac: `subc.jsonc` has `"reserved_capabilities": {"agent-orchestrator/v1": "prefrontal-core"}` (added 2026-10-12, by rescan). Any other module declaring it is refused at HELLO with `reserved_capability`, and never appears in the catalog.
  - Entorhinal resolves the provider with `SubcConsumer::resolve_provider("agent-orchestrator/v1")` in subc-client-rs 0.28.0, the version already pinned (`Cargo.toml:8`, asserted at `crates/entorhinal-module/src/agent_reads.rs:1574-1581`); no bump. It filters `catalog.list` on `capabilities.provides` and returns the sole provider's module id, or a typed `capability_unprovided` or `capability_ambiguous`. It needs nothing in entorhinal's manifest. The catalog carries no principal, so the caller is admitted when its daemon-stamped principal equals `reserved:<that module id>`.
  - Resolve on every call (one local round trip), or cache with these rules: never cache `capability_unprovided`, drop the cache when the provider's route closes, and treat `capability_ambiguous` as a refusal, never a pick.
  - Prefrontal-core doesn't declare `agent-orchestrator/v1` yet, so the liveness feed keeps its current admission (`reserved:prefrontal-core` by name) until the feed cutover below.
- **Prefrontal-core: liveness feed.** It keeps sending ordered liveness batches (core's `projects_consumer.rs`, `SessionLivenessEntry`). Each entry carries `harness` (`opencode` or `pi`) and `sessionId`, the harness's own unprefixed session id (an OpenCode `ses_…` id or a Pi session id). The pair `(harness, sessionId)` is the session key, never concatenated. It is stable across a host restart that resumes the same session. States today are `active`, `idle` and `gone`. Core will add `lapsed` (lease expired, not a clean end), possibly via a later harness module that takes over the feed; nothing here waits for it.
- **Engram: generic record.** `record.read {subject}` returns `{absent, version, body, author, self_author, stale}`; `record.cas {subject, expected_version, body, write_id}` returns `{version}` or refuses `version_moved {current_version, author}`; `record.read_many {subjects}` (up to 64). Callers: only `reserved:entorhinal`. Resend rule: the same `write_id` with the same `expected_version` and body answers the landed `{version}`; the same `write_id` with anything else is refused `id_reused`. `record.read_many` is called in production by refresh, so its service id and bind identity are not test-only details. The record client's module doc pins the rest the way `crates/entorhinal-module/src/log_client.rs:5-28` does: the engram source file serving `record.*` and the engram commit, service id and bind identity, check order, size caps on subject, body and `read_many`, and the closed refusal set.
  - Assumptions the engram commit must confirm: (a) a record reply's `self_author` is the same pseudonym as the identity log's `author` for the same device; (b) what `stale: true` means, and that an absent reply can carry it; (c) `id_reused` and `version_moved {current_version, author}` are the served refusal names.
  - The in-tree fake enforces exactly these rules and is re-pinned to the commit when it arrives. The commit is not readable from this workspace and must be supplied before s2 is dispatched.

### Machine-local state
- One store migration, V8, adds every table this campaign needs: `MIGRATIONS` becomes `[Migration; 8]` (`crates/entorhinal-core/src/lib.rs:122`), and the ahead-refusal message and its test (`binary_without_log_migration_refuses_migrated_store_as_ahead`) move to schema version 8. Machine-local tables, outside the identity journal and never shared:
  - `session_binding`: `ref_kind`, `ref` (primary key together), `agent_id`, `harness`, `session_id`, `registered_at_ms`.
  - `claim_cache`: `agent_id` (primary key), `holder` (author pseudonym, or NULL when the record is absent), `epoch`, `cut_id`, `version`, `read_at_ms`, `stale`.
  - `self_identity`: one row holding this device's `self_author` and when it was learned.
- `self_author` is learned from exactly two sources: the `author` of an applied identity-log entry whose `signed_by_self` is true (the rotation-stable pseudonym, `crates/entorhinal-module/src/log_client.rs:13-16`), and the `self_author` of an engram record reply. An entry with `signed_by_self: false` never sets it; a joiner reads the first member's bootstrap snapshot that way (`crates/entorhinal-module/src/fake_log.rs:283`), and a joiner never writes a snapshot (`crates/entorhinal-module/src/enable.rs:273-297` refuses a supersede unless every prior entry is self-signed).
  - Consequence: a joiner with no live agents has no `self_author` until its first own shared write applies or refresh has an agent to read; until then `member.set_name` refuses `self_author_unknown` and every claim at epoch 1 or higher reads `held_elsewhere`.
  - Test: in a two-node fixture, B joins with no agents and a failing record fake; `ck projects log name` on B refuses `self_author_unknown`, and B's `self_identity` never holds A's author.
- Once a value is persisted, a different value from either source does not overwrite it; it is recorded as the health error `self_author_conflict`.
- `verify` and `rebuild` ignore the machine-local tables, and nothing about them enters `registry_journal`.
- Persisting author pseudonyms retires the identity-log client's pin "Authors are only compared in memory, never persisted, logged or displayed" (`crates/entorhinal-module/src/log_client.rs:15-16`). That doc line changes. The mutation rows `reader-requires-author` and `reader-canonical-author` stay.
- Health stays lock-free: any new gauge is an atomic.

### Locks, paths and admission
- `agent.register_session`, the refresh result writer and `member.set_name`'s local name row run as plain SQLite transactions on the store connection. `agent.register_session` and refresh never take the tokio `writer` mutex that serializes shared-log writes and that background catch-up holds while waiting on engram. `member.set_name` is a shared-log write and takes the normal append path (`shared_candidate`, `Write::decode`, the `writer` mutex, append).
- Only `agent.register_session` and `agent.refresh_claims` take the early path in `write_wait` (`crates/entorhinal-module/src/shared_write.rs:2392-2404`): `refuse_flow_write`, then their own admission, then `return` from execution, as `projects.session_liveness` does.
- Refresh runs on its own spawned task, never inside the catch-up worker, and holds no SQLite transaction across an engram call: it reads engram first, then records the results in one short transaction. Each record call has the deadline `READ_DEADLINE` (`crates/entorhinal-module/src/catch_up.rs:11`). Test under `start_paused`: with the record fake hung and the log fake healthy, a `register` shared write completes within its deadline after the catch-up tick.
- Resolution goes through a `ProviderResolver` trait, injected like `LogConnector`. An in-tree fake scripts a provider id, `capability_unprovided`, `capability_ambiguous` and transport failure. Acceptance 1–5 run in-process against it.
- The resolved provider is a per-request value and never enters `route_admissions` (`crates/entorhinal-module/src/main.rs:530-535`), because `admit` reads that map for the route's lifetime (`main.rs:546-551`, `main.rs:786-788`). `write_wait` resolves after `refuse_flow_write` and passes the result into execution alongside the route key. `admit` for `agent.register_session` compares the route principal with `reserved:<provider>` from that value, and refuses `orchestrator_unbound` when none is passed, so the synchronous `handle_request` seam fails closed. A transport failure or `capability_ambiguous` answers `orchestrator_unbound` and never admits.
  - Tests: after one admitted served `agent.register_session` on route R, a sync `handle_request` of the same body on R answers `orchestrator_unbound`; after the fake resolver switches provider, the next served call from the old principal answers `caller_forbidden`. Mutation row: dropping the principal comparison turns the `caller_forbidden` test red.
- Refusal order for `agent.register_session`: flow route, then resolution (`orchestrator_unbound`), then principal (`caller_forbidden`), then `authority_not_cut_over`, then agent (`unknown_agent`, `gone`), then claim (`held_elsewhere`), then ref (`ref_bound_elsewhere`). `caller_forbidden` and `orchestrator_unbound` are new codes, distinct from `write_not_permitted` and `direct_identity_write_not_admitted`.
- Every decode failure on the new ops (unknown field, missing field, a length or count outside its bound) is refused with `invalid_request`, the code other agent ops use (`crates/entorhinal-module/src/agent_reads.rs:66`).
- The manifest pin test (`crates/entorhinal-module/src/agent_reads.rs`) lists the new ops with their kinds: `agent.register_session`, `agent.refresh_claims` and `member.set_name` are mutations; `agent.session_liveness` and `agent.where` are queries. The three mutations join `crate::MUTATING_METHODS` (`crates/entorhinal-module/src/main.rs:443`), not `agent_ops::MUTATING_METHODS`, so `refuse_flow_write` applies and `Direct` reaches `caller_forbidden` rather than `direct_identity_write_not_admitted`. `project_mutations_keep_their_gate_before_and_after_cutover_including_rebuild` (`crates/entorhinal-module/src/agent_ops.rs:922-926`) excludes them. The existing sync-path test that posts `projects.session_liveness` from `reserved:prefrontal-core` through `handle_request` stays green.

### `agent.register_session`
- Request: `{agent_id, ref_kind, ref, harness, session_id}`. `ref_kind` is 1 to 64 bytes, `ref` 1 to 512, `harness` 1 to 32 and `session_id` 1 to 256. `harness` and `session_id` are the liveness feed's session key, as core sends them.
- Admitted only from the principal `reserved:<provider>` of the module that `ProviderResolver` names. `Direct` and every other caller get `caller_forbidden`.
- In one SQLite transaction: read the agent's cached claim and the persisted `self_author`. If the claim is held by another device (`epoch >= 1`, and `holder` is not the persisted `self_author`, or none is persisted), refuse with `held_elsewhere`. Its detail names the holder pseudonym, its display name when known, and the epoch. Otherwise upsert the binding.
- A ref already bound to a different agent is refused with `ref_bound_elsewhere`, naming that agent. This applies even when the bound session reads `gone`. Re-registering the same ref for the same agent updates `harness`, `session_id` and `registered_at_ms`, and succeeds.
- An absent cache row, or a claim at epoch 0, reads as unclaimed and is admitted. Nothing calls engram on this path.

### `agent.session_liveness`
- Request: `{refs: [{ref_kind, ref}]}`, holding 1 to 256 refs. Reply: one state per ref, in request order: `active`, `gone` or `unknown`.
- A ref with no binding is `unknown`. Otherwise the bound session's state comes from the liveness mirror, looked up by `(harness, session_id)`. `active` and `idle` both answer `active`, because the host process is open either way. `gone` answers `gone`. Anything else answers `unknown`: no entry, `lapsed`, any unrecognised state, or a mirror waiting for its first full snapshot after a restart.
- The mirror decoder (`LivenessSession`, `crates/entorhinal-module/src/main.rs`) reads `harness` and keys on `(harness, sessionId)`. An entry with no `harness` is keyed with an empty harness and never matches a binding. `canonical_root` stays required. `enumerate`'s root annotation and `tests::enumerate_annotation_joins_liveness_by_root_containment` are unchanged, and a `lapsed` or unrecognised-state session annotates as `unknown`.
- The decoder accepts `lapsed` and any unknown state string without refusing the batch, and stores them as unknown.
- Reads are open to every principal, as other reads are.

### Liveness feed cutover (separate slice, on the chair's order)
- The feed moves from admission by `reserved:prefrontal-core` to admission by the module that provides `agent-orchestrator/v1`. With nothing bound, the feed is refused `orchestrator_unbound`; a sender other than the bound provider is refused `caller_forbidden`.
- A refused batch changes nothing: not `last_seq`, the sessions map or the stale latch. It bumps only an atomic refusal counter.
- The stale latch is set when the resolved provider changes or the provider's route closes (`on_route_gone`, `crates/entorhinal-module/src/main.rs:612-614`), so with nothing bound `agent.session_liveness` and `enumerate` answer unknown until the next full snapshot.
- Tests: a `Direct` batch refused while a provider is bound leaves every `agent.session_liveness` answer unchanged; after the provider's route is gone, every answer is unknown until the next snapshot.
- This is dispatched only after prefrontal-core declares the capability and SUBC has reviewed the change, which touches daemon coupling. Until then, the feed's admission is unchanged.

### Claim cache and `agent.where`
- Subject `claim:<agent_id>`. Body: a versioned JSON document defined by entorhinal, `{v: 1, agent_id, holder, epoch, cut_id, reason}`. `reason` is set only on a forced take. The codec refuses three cases by name: an unknown `v` (`claim_version_unknown`), a body whose `agent_id` differs from the subject's (`claim_subject_mismatch`), and a malformed body (`claim_malformed`).
- Refresh triggers, and no others: startup once the identity log is enabled; completion of `identity_log.enable`; the catch-up worker's first successful engram read after `log_state` was non-zero or after the previous refresh failed; and `agent.refresh_claims`. No private timer. A refresh failure is recorded in atomic gauges only and never changes `log_state`. Test under `tokio::test(start_paused)`: the record fake answers `unavailable` then succeeds; advancing the 30 s catch-up tick yields exactly one refresh, after the success.
- `agent.refresh_claims {}`: admitted from `Direct` only (others `caller_forbidden`), writes only `claim_cache` and `self_identity`, awaits the refresh and replies `{refreshed, failed}` counts, or `engram_unavailable` when no record call succeeded. `ck agents where --refresh` calls it, then `agent.where`.
- Refresh reads every live agent's claim with `record.read_many` in batches of up to 64 and applies each reply with this precedence:
  - A present, non-stale reply whose version is at least the cached one replaces the row.
  - Any reply whose version is older than the cached one keeps the body, and updates `read_at_ms` and `stale`.
  - An absent reply with `stale: true` against a cached version of 1 or higher keeps the body and marks it stale.
  - An absent, non-stale reply caches as unclaimed.
- `agent.where {agent_id}` replies `{status: unclaimed | held_here | held_elsewhere, holder, holder_name, epoch, cut_id, read_at_ms, stale}` from the cache only. An unknown agent is refused `unknown_agent`, a retired or merged one `gone`. `held_here` compares the holder with the persisted `self_author`, so it survives a restart with engram unreachable. With no `self_author` persisted, a claim at epoch 1 or higher reads as `held_elsewhere` with no name, and is never assumed to be held here.
- CLI: `ck agents where <agent> [--refresh]`, accepting an id or a name as the other `ck agents` verbs do.

### The claim compare-and-swap client
- A typed client for `record.cas`, built and tested against the fake. Retries resend the same `write_id`, `expected_version` and bytes, a lost reply is settled by the resend rule above, and `version_moved` is surfaced with the current version and author. Nothing in part 1 calls it outside tests.

### Member display names
- A new shared identity-log op, `member.set_name {author, display_name}`, decoded with `deny_unknown_fields`. It's admitted only from `Direct`; any other caller gets `caller_forbidden`. It's not a confirmable write. It carries no request key, so a retry after `engram_outcome_unknown` may append a second, equal entry; that is harmless. Refusals:
  - `identity_log_disabled` when the log is not enabled, `identity_log_enabling` while it is enabling or joining.
  - `self_author_unknown` when no `self_author` is persisted.
  - `not_self_author` when `author` isn't the persisted `self_author`.
  - `display_name_invalid` unless the trimmed name is 1 to 64 characters.
- Remote apply compares the body's `author` with the log entry's `author` pseudonym, never with `signer`, which is the signing key. `RemoteEntry` (`crates/entorhinal-core/src/remote_apply.rs:33-36`) gains `author`, and both of its constructors fill it. A mismatch is an `apply_failed` stall at that position, like every other invalid entry: `identity_log_stalled`, no name row, applied position unchanged.
- Names are stored in a shared table `member_name` keyed by `author`, which joins `DERIVED_TABLES`. `remote_apply::apply` admits `member.set_name` beside `AGENT_OPS`. `delete_and_replay` gets a `member.set_name` arm for local `origin='here'` rows and for the shared-image branch. `member_name` is not added to `PROJECT_SHARED_TABLES`, because a `project.shared` upsert would bypass the author check.
- A `shared.snapshot` carries at most its own author's name, in a dedicated `ProjectEntry` field with `#[serde(default, skip_serializing_if = "Option::is_none")]`, so a snapshot by a node with no name has no such key (`ProjectEntry` is `deny_unknown_fields`, `crates/entorhinal-core/src/shared_entry.rs:33-42`). Byte test: a supersede by a node with no name serialises to exactly the pre-change bytes. Apply refuses (stalls) a snapshot naming any other author. `enable`'s supersede includes the writer's current name, so a later joiner still learns it; a reader-side guard stalls a superseding snapshot that omits a name its replaced prefix set, with its own mutation row (the names analogue of `supersede-keeps-every-agent`).
- Rollout: every member runs this release before the first `ck projects log name`. An older member stalls at the first name entry or name-carrying snapshot by design, its shared writes refuse `identity_log_stalled`, and it recovers on upgrade with no operator repair (test: a store stalled at a name entry under the old decoder, reopened with the new binary, applies it and advances).
- CLI: `ck projects log name <display name>`, a Direct operator write.
- `agent.where` fills `holder_name` from this table.

### Tests and fences
- Every refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`.
- No test asserts elapsed real time. All tests pass on Linux, macOS and Windows CI.
- Register-session atomicity: a cfg(test) hook fires inside the registration transaction, after the claim read. A cache write started at the hook commits only after the registration and isn't visible to it, so the reply equals the pre-hook state. A cache write committed before the transaction begins is seen.
- Two-node fixtures build each node's record fake and its `FakeLog` view from one 16-byte author: the record fake returns it as `self_author` and stamps it as `author` on records that node wrote.
- Rebuild after `member.set_name` is cell-identical, following the `cells()` pattern in `crates/entorhinal-core/src/log_schema.rs`.

## intent
The operator approved AGENT_SYNC draft 11 (engram repository, `AGENT_SYNC.md`): agents move between the operator's Macs, and entorhinal owns where each agent lives. This campaign builds the parts of entorhinal's share that don't depend on the move itself. They're also what every module will ask entorhinal before it starts an agent anywhere:

- **Session registry.** The orchestrator registers every session it starts for an agent with `agent.register_session`. Entorhinal checks its cached claim and records the binding in one transaction, so no move can land between the check and the registration.
- **Session liveness.** `agent.session_liveness(refs)` answers `active`, `gone` or `unknown` per ref, from the liveness mirror entorhinal already keeps. `unknown` counts as busy.
- **The checkout claim cache and `agent.where`.** A machine-local cache of each agent's claim record (holder, epoch, last cut), read from engram's generic encrypted compare-and-swap record and refreshed at startup and on reconnect. `agent.where` answers from the cache only, never from engram.
- **Member display names** in the shared identity log, keyed by the device pseudonym (`author`) engram returns, so `agent.where` can name the holding Mac.

The move command, the move record and the participants (`agent-data/v1`) are part 2, a separate campaign.


## non_goals
- Moving agents: `agent.move`, the move record, the move driver, participant calls, forks, lid close. Part 2.
- Writing claims. Part 1 reads and caches them, and builds and tests the claim body codec and the compare-and-swap client against a fake. The first claim write is part 2's move.
- Unbinding: `agent.session_end` and removing `session_binding` rows are part 2. In part 1 a bound ref stays bound to its agent, even once its session reads `gone`.
- Any per-turn check. Nothing here runs per turn.
- Changes in prefrontal-core, engram or the subc daemon. Those are external dependencies, listed in the constraints.

## open_questions
None.


## slice_hints
- s1, sessions: migration V8 with every new table (`session_binding`, `claim_cache`, `self_identity`, `member_name`), the `ProviderResolver` seam and its fake, `agent.register_session` with orchestrator admission, `agent.session_liveness`, the `(harness, session_id)` mirror key, and `lapsed`. The feed keeps today's admission.
- s2, claims (after s1): the engram record client and fake, the claim codec, refresh with persisted `self_author`, `agent.where`, and the register-session claim check.
- s3, names (after s2): `member.set_name` with its remote-apply, replay and snapshot arms, `author` on `RemoteEntry`, `ck projects log name`, and two-node acceptance.
- s4, feed cutover (after s3, dispatched only on the chair's order once prefrontal-core declares `agent-orchestrator/v1` and SUBC has reviewed it): the feed admitted from the capability provider, `orchestrator_unbound` for the feed, and the non-orchestrator refusal test.

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded supplied ruling 1

Campaign `ct_00000000-0000-408a-98dd-e572e1ab76c8`, round 2, decision sequence 4 (source: {"ask_id":"owner-loop-ct_00000000-0000-408a-98dd-e572e1ab76c8-r2","provenance":"answer_time","source":"owner_loop_answer"}):

R1. Two reserved capabilities, not one. The subc daemon binds each to exactly one module id in subc.jsonc; both are bound to prefrontal-core today (SUBC, 2026-10-12), and `agent-host/v1` moves to `thalamus-harness` later, by rebinding.
- `agent-orchestrator/v1`: registers sessions for the work it owns, and later relays phone-confirmed moves.
- `agent-host/v1`: registers host sessions, and is the only source of the liveness feed.
- `agent.register_session` is admitted from the sole claimant of either capability. The binding stores which role registered it (`registered_by: orchestrator | host`). With neither claimed, it refuses `orchestrator_unbound`.
- The liveness feed is admitted only from `agent-host/v1`'s sole claimant. With no claimant, it refuses `host_unbound`. That refusal neither changes the mirror nor flips its stale latch, so the mirror keeps its last state, and the producer retries.
- Both are resolved per request with `SubcConsumer::resolve_provider`, never cached in route admission. `capability_ambiguous` is a refusal, never a pick. There is no by-name principal anywhere in entorhinal's code.
- Release order is the chair's: core's build that declares both capabilities goes live before entorhinal's build with this admission is placed.

R2. Liveness `cause`. A feed entry may carry an optional `cause` string (`host_not_seen` and `lease_expired` are named; other text is stored as given). `agent.session_liveness` returns `{state, cause}` per ref, with `cause` present only when the state is `unknown` and the entry carried one.

R3. `agent.for_session {harness, session_id}` returns `{agent_id, ref_kind, ref}`, or refuses `not_found`. It's a read, open to every caller, served from `session_binding`. `harness` is an opaque string (in use: `opencode`, `pi`, `claude-code`).

R4. Sessions with no agent are never registered, and entorhinal never mints an agent from a session. The liveness feed still carries them, as today. State this as a non-goal.

R5. Role claims, a new section; prefrontal's Plexus will build against it.
- `role.read {role}` returns `{role, known, held_here, epoch, holder, holder_name, source, stale}`. `source` is `cloud | local`. It's open to every caller and served from the persisted cache, never waiting on engram.
- `known: false` only when this node has never read the role; then `held_here: false`, `epoch: 0` and `holder: null`. A role held elsewhere always answers `known: true`, `epoch >= 1` and a holder. `held_here` requires `epoch >= 1` and holder equal to this node's `self_author`.
- Role subjects refresh every 30 s, plus at startup and on reconnect. After a restart, the persisted entry answers with `stale: true` until a refresh lands. A temporary failure keeps the cached answer with `stale: true`.
- An integrity failure on the role's record (engram `verify_failed` or `decrypt_failed`) makes `role.read` refuse `claim_integrity_failed`. The mark persists across restarts, and only a later verified read clears it.
- `role.take {role, expected_epoch, request_id, reason}` returns `{epoch, held_here: true}`. `request_id` is 32 lowercase hex, mapped to the record `write_id`. Refusals: `epoch_moved {current_epoch, holder}` and `id_reused`. A retry with the same id and input returns the original result.
- `role.take` is admitted only from `Direct`, through the existing operator-confirmation flow, with a summary naming the role. It refuses `operator_presence_unavailable` where there is no presence provider. It is not admitted from any module principal.
- Role names match `[a-z0-9_]{1,64}`, and the subject is `role:<name>`. The body is versioned JSON `{v: 1, role, holder, epoch, reason}`.
- CLI: `ck roles take <role>` and `ck roles read <role>`.

R6. Agent claims get the same handling: `agent.where` gains `known`. A persisted integrity-failure mark makes `agent.where` refuse `claim_integrity_failed`, and makes `agent.register_session` for that agent refuse `claim_integrity_failed` (fail closed).

R7. Name checks against the engram record contract as published (generic record spec ct_00000000-0000-4077-98dd-e5065e639510):
- write_ids are remembered for the most recent 64 versions of a subject. Past that, a lost reply is settled by reading the subject: the write landed when the version's `author` is this node and its body matches.
- `record.read_many` takes up to 64 subjects.
- Refusal names follow the identity log's set, plus `version_moved`.

### recorded supplied ruling 2

Campaign `ct_00000000-0000-408a-98dd-e572e1ab76c8`, round 3, decision sequence 0 (source: {"body_bytes":222,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-408a-98dd-e572e1ab76c8-20261012T153725Z.md"}):

Fold the seven chair rulings recorded with the round-2 answer (R1 to R7) and check the round-2 fixes: the per-request provider resolution, `agent.refresh_claims`, the spawned refresh task, and the field omitted when unset.

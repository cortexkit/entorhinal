---
title: "2026-10-12-agent-sessions-and-claims refire 2"
status: draft
rounds_cap: 1
mint: auto
refired_from: "ct_00000000-0000-410d-98dd-e75fe784fe18"
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
1. The orchestrator's claimant registers a session for a live, unclaimed agent. The binding is stored with `registered_by: orchestrator`. With `agent-host/v1` bound to another module, that module's registration is stored with `registered_by: host`. A second registration of the same ref for another agent is refused with `ref_bound_elsewhere`.
2. Claim checks on registration:
   - A cached claim held by another device is refused with `held_elsewhere`, naming the holder and its display name.
   - A cached claim carrying the integrity-failure mark is refused with `claim_integrity_failed`.
   - A claim held here, an absent claim and a claim at epoch 0 are all admitted.
   - After a restart with a failing engram connector, a cached claim at epoch 1 whose holder is the persisted `self_author` is still admitted, and `agent.where` answers `held_here`.
3. `Direct`, another module and a flow route are refused with the stated codes, in the stated order. When neither capability has a sole claimant, whether it is unprovided, ambiguous or failed in transport, the answer is `orchestrator_unbound`. All of this runs in-process against the provider fake.
4. `agent.session_liveness` maps active, idle, gone, lapsed, missing, unbound and a waiting mirror exactly as specified, in request order. `cause` appears only on an `unknown` answer whose entry carried one, and its key is absent otherwise. The same session id under two harnesses stays two sessions.
5. A liveness batch carrying `lapsed` or an unrecognised state is accepted. That session reads as unknown, and `enumerate`'s annotation of the other sessions is unchanged. The cutover slice proves two things: the feed's `host_unbound` refusal, which leaves the mirror and the stale latch unchanged, and its `caller_forbidden` refusal of a non-host sender.
6. Refresh caches claims from the fake engram, covering each of these cases:
   - absent
   - held here
   - held elsewhere
   - a stale older version, which must not replace a newer cached one
   - a stale absent, which must not replace a cached claim at epoch 1 or higher
   - `verify_failed`, which sets the persisted integrity mark; a later verified read clears it

   `agent.where` reports each case correctly, including `known`, without calling engram, and refuses `claim_integrity_failed` while the mark is set.
7. With engram unreachable, and with engram hung while catch-up holds the log `writer` mutex, startup, every read and `agent.register_session` still answer.
8. `member.set_name` sets this node's name. A forged entry naming another author stalls apply at its position and writes no row. The name appears in `agent.where` on the other node. A node that joins after the setter's re-enable supersede still reads the name from the snapshot.
9. Verify and rebuild stay clean with bindings, cached claims, cached roles and names present. The machine-local tables never appear in the journal or the identity log.
10. `agent.for_session` returns the binding for a registered `(harness, session_id)`, including `claude-code`, and refuses `not_found` for an unregistered one.
11. Role claims:
    - `role.read` answers `known: false` for a role never read. It answers held-here and held-elsewhere roles with the stated invariants, and answers `stale: true` after a restart until a refresh lands.
    - `role.take` from `Direct` with operator confirmation returns `{epoch, held_here: true}`.
    - A retry with the same `request_id` returns the original result.
    - A moved epoch refuses `epoch_moved`, and a reused id with other input refuses `id_reused`.
    - A module principal is refused.
    - With no presence provider, `role.take` refuses `operator_presence_unavailable`.
12. A lost `record.cas` reply past the 64-version write_id window is settled by reading the subject's author and body.

## constraints
### External dependencies
- **SUBC: reserved capabilities.** Built since subc-core 0.7.0. The daemon binds each reserved capability to exactly one module id in `subc.jsonc`. Any other module declaring it is refused at HELLO with `reserved_capability` and never appears in the catalog. Two capabilities are reserved, and both are bound to `prefrontal-core` today (SUBC, 2026-10-12):
  - `agent-orchestrator/v1` registers sessions for the work it owns. Later it relays phone-confirmed moves.
  - `agent-host/v1` registers host sessions and is the only source of the liveness feed. It moves to `thalamus-harness` later by rebinding, and entorhinal does not change.
  - Entorhinal resolves each capability with `SubcConsumer::resolve_provider(<capability>)` in subc-client-rs 0.28.0, the version already pinned (`Cargo.toml:8`, asserted at `crates/entorhinal-module/src/agent_reads.rs:1574-1581`). No bump is needed. The call filters `catalog.list` on `capabilities.provides` and returns either the sole provider's module id or a typed `capability_unprovided` or `capability_ambiguous`. It needs nothing in entorhinal's manifest. The catalog carries no principal, so a caller is admitted when its daemon-stamped principal equals `reserved:<that module id>`.
  - Each capability is resolved per request (one local round trip) and is never cached in route admission. `capability_ambiguous` is a refusal, never a pick.
  - Reach of the no-by-name rule: no op this campaign adds or changes (`agent.register_session`, `agent.for_session`, `agent.session_liveness`, `agent.refresh_claims`, `agent.where`, `member.set_name`, `role.read`, `role.take`, and the liveness feed from s4) compares a principal with a module name. The existing `WRITER_MODULE` gate on the `agent.*` identity relay writes and on the project methods (`authorize_write`; `crates/entorhinal-module/src/agent_ops.rs:922-936`) stays as it is; changing it is a non-goal of this campaign. Proof: the thalamus-harness rebinding tests below, and the s4 removal of the by-name feed admission.
  - Release order is the chair's: prefrontal-core's build that declares both capabilities goes live before entorhinal's build with this admission is placed.
- **Prefrontal-core: liveness feed.** It keeps sending ordered liveness batches (core's `projects_consumer.rs`, `SessionLivenessEntry`). Each entry carries two fields:
  - `harness`: an opaque string. The values in use are `opencode`, `pi` and `claude-code`.
  - `sessionId`: the harness's own unprefixed session id.

  The pair `(harness, sessionId)` is the session key and is never concatenated. It stays stable across a host restart that resumes the same session. An entry may also carry an optional `cause` string. `host_not_seen` and `lease_expired` are named values; any other text is stored as given. States today are `active`, `idle` and `gone`. Core will add `lapsed` (the lease expired; not a clean end), possibly through a later harness module that takes over the feed. Nothing here waits for it. Sessions with no agent stay in the feed.
- **Engram: generic record.** The contract is the published generic record spec ct_00000000-0000-4077-98dd-e5065e639510.
  - `record.read {subject}` returns `{absent, version, body, author, self_author, stale}`.
  - `record.cas {subject, expected_version, body, write_id}` returns `{version}` or refuses `version_moved {current_version, author}`.
  - `record.read_many {subjects}` takes up to 64 subjects.
  - Callers: only `reserved:entorhinal`.
  - Refusal names are the identity log's set plus `version_moved`.
  - Resend rule: engram remembers write_ids for the 64 most recent versions of a subject. Within that window, resending the same `write_id` with the same `expected_version` and body answers the landed `{version}`. The same `write_id` with anything else is refused `id_reused`. Past the window, a lost reply is settled by reading the subject: the write landed when the version's `author` is this node and its body matches.
  - Refresh calls `record.read_many` in production, so its service id and bind identity are not test-only details.
  - The record client's module doc pins the rest the way `crates/entorhinal-module/src/log_client.rs:5-28` does: the engram source file serving `record.*`, the engram commit, the service id and bind identity, the check order, the size caps on subject, body and `read_many`, and the closed refusal set.
  - The engram commit must confirm two assumptions: (a) a record reply's `self_author` is the same pseudonym as the identity log's `author` for the same device; (b) what `stale: true` means, and whether an absent reply can carry it.
  - The in-tree fake enforces exactly these rules. It is re-pinned to the commit when the commit arrives. The commit can't be read from this workspace and must be supplied before s2 is dispatched.

### Machine-local state
- One store migration, V8, adds every table this campaign needs. `MIGRATIONS` becomes `[Migration; 8]` (`crates/entorhinal-core/src/lib.rs:122`). The ahead-refusal message and its test (`binary_without_log_migration_refuses_migrated_store_as_ahead`) move to schema version 8. The machine-local tables sit outside the identity journal and are never shared:
  - `session_binding`: `ref_kind` and `ref` (the primary key together), `agent_id`, `harness`, `session_id`, `registered_by` (`orchestrator` or `host`) and `registered_at_ms`, with a unique index on `(harness, session_id)`.
  - `claim_cache`: `agent_id` (primary key), `holder` (an author pseudonym, or NULL when the record is absent), `epoch`, `cut_id`, `version`, `read_at_ms`, `stale` and `integrity_failed`.
  - `role_cache`: `role` (primary key), plus the fields `role.read` answers (including `source`), the record `version`, `read_at_ms` and `integrity_failed`.
  - `role_take_request`: `request_id` (primary key), `role`, `expected_epoch`, `reason`, the `expected_version` and exact body bytes of the cas attempt, the result `epoch` (NULL until known) and `created_at_ms`. One row per approved take, so it grows only with operator approvals.
  - `self_identity`: one row holding this device's `self_author` and when it was learned.
- `self_author` is learned from exactly two sources:
  - the `author` of an applied identity-log entry whose `signed_by_self` is true. This is the rotation-stable pseudonym (`crates/entorhinal-module/src/log_client.rs:13-16`). The `self_identity` row is written in the same SQLite transaction that applies the entry or installs the snapshot, and in the transaction that only advances position for an own entry read back (`committed_own_entry_only_advances_position_without_reapplying`). Test: a fault injected after apply, then reopen; `self_identity` holds the author.
  - the `self_author` of an engram record reply, written in the refresh or `role.take` result transaction.

  An entry with `signed_by_self: false` never sets it. A joiner reads the first member's bootstrap snapshot as such an entry (`crates/entorhinal-module/src/fake_log.rs:283`), and a joiner never writes a snapshot: `crates/entorhinal-module/src/enable.rs:273-297` refuses a supersede unless every prior entry is self-signed.
  - Consequence: a joiner with no live agents has no `self_author` until its first own shared write applies or until refresh has an agent to read. Until then `member.set_name` refuses `self_author_unknown`, and every claim at epoch 1 or higher reads `held_elsewhere`.
  - Test: in a two-node fixture, B joins with no agents and a failing record fake. `ck projects log name` on B refuses `self_author_unknown`, and B's `self_identity` never holds A's author.
- Once a value is persisted, a different value from either source does not overwrite it. The difference is recorded as the health error `self_author_conflict`.
- `verify` and `rebuild` ignore the machine-local tables, and nothing about them enters `registry_journal`.
- Persisting author pseudonyms retires the identity-log client's pin "Authors are only compared in memory, never persisted, logged or displayed" (`crates/entorhinal-module/src/log_client.rs:15-16`), so that doc line changes. The mutation rows `reader-requires-author` and `reader-canonical-author` stay.
- Health stays lock-free: any new gauge is an atomic.

### Locks, paths and admission
- The new mutating ops are `agent.register_session`, `agent.refresh_claims`, `member.set_name` and `role.take`. All four join `crate::MUTATING_METHODS` (`crates/entorhinal-module/src/main.rs:443`), so `refuse_flow_write` refuses them on a flow route with `flow_scope_not_admitted`, and `handle_served_request` never routes them to the query path (`main.rs:749-751`). None joins `agent_ops::MUTATING_METHODS` or `agent_ops::CONFIRMABLE_METHODS`, so `Direct` never reaches `direct_identity_write_not_admitted` or `precheck_agent_confirmation`, which refuses any method it does not know (`crates/entorhinal-core/src/agent/store.rs:816-839`).
- `authorize_write` answers Ok for these four, and from s4 for `projects.session_liveness`. Each op's own rule in `admit` then decides admission. Without this, `authorize_write`, which runs after `refuse_flow_write` on both paths (`crates/entorhinal-module/src/shared_write.rs:2395-2398`, `main.rs:548-549`), would refuse a non-prefrontal claimant `write_not_permitted`. `project_mutations_keep_their_gate_before_and_after_cutover_including_rebuild` (`crates/entorhinal-module/src/agent_ops.rs:922-936`) excludes them. Mutation row: removing the pass-through turns the `thalamus-harness` registration test red.
- Dispatch:
  - `agent.register_session` takes the early path in `write_wait` (`shared_write.rs:2402-2404`), as `projects.session_liveness` does: `refuse_flow_write`, resolution, `admit`, then `return` from `execute`.
  - `agent.refresh_claims` and `role.take` get their own async arms in `handle_served_request`, beside `identity_log.enable` (`main.rs:752-756`), and never reach `write_wait`. On the synchronous `handle_request` seam, `execute` runs flow and principal admission, then refuses both with `served_path_required`, so the seam fails closed.
  - `member.set_name` is a shared-log write and takes the normal append path: `shared_candidate`, `Write::decode`, the `writer` mutex, then append.
- `agent.register_session`, the refresh result writer, `role.take`'s result writer and `member.set_name`'s local name row run as plain SQLite transactions on the store connection. Only `member.set_name` takes the tokio `writer` mutex. That mutex serializes shared-log writes, and background catch-up holds it while waiting on engram.
- Refresh runs on its own spawned task, never inside the catch-up worker. Neither refresh nor `role.take` holds a SQLite transaction across an engram call: each reads or writes engram first, then records the results in one short transaction. Each record call has the deadline `READ_DEADLINE` (`crates/entorhinal-module/src/catch_up.rs:11`). Tests under `start_paused`, with the log fake hung while catch-up holds the `writer` mutex: a `register` shared write completes within its deadline after the catch-up tick while the record fake is also hung; an approved `role.take` completes against a healthy record fake.
- Resolution goes through a `ProviderResolver` trait, injected like `LogConnector`. An in-tree fake scripts each capability separately: a provider id, `capability_unprovided`, `capability_ambiguous` or a transport failure. Acceptance items 1 to 5 run in-process against it.
- The resolved providers are per-request values and never enter `route_admissions` (`main.rs:530-535`), because `admit` reads that map for the route's lifetime (`main.rs:546-551`, `main.rs:786-788`). `write_wait` resolves after `refuse_flow_write` and passes the result into `execute` alongside the route key.
  - `admit` for `agent.register_session` compares the route principal with `reserved:<provider>` for each capability that resolved to a sole claimant.
  - A capability whose resolution failed in transport or answered `capability_ambiguous` admits no one.
  - When neither capability has a sole claimant, or no value is passed, `admit` refuses `orchestrator_unbound`. That is how the synchronous seam fails closed.
  - Tests:
    - With `agent-host/v1` resolved to `thalamus-harness`, a served `agent.register_session` from `reserved:thalamus-harness` is stored with `registered_by: host`.
    - After one admitted served `agent.register_session` on route R, a sync `handle_request` of the same body on R answers `orchestrator_unbound`.
    - After the fake resolver switches a provider, the next served call from the old principal answers `caller_forbidden`.
    - Mutation row: dropping the principal comparison turns the `caller_forbidden` test red.
- Refusal order for `agent.register_session`:
  1. flow route (`flow_scope_not_admitted`)
  2. resolution (`orchestrator_unbound`)
  3. principal (`caller_forbidden`)
  4. `authority_not_cut_over`
  5. agent (`unknown_agent`, `gone`)
  6. claim (`claim_integrity_failed`, then `held_elsewhere`)
  7. ref (`ref_bound_elsewhere`)
  8. session key (`session_bound_elsewhere`)

  `caller_forbidden`, `orchestrator_unbound`, `host_unbound`, `session_bound_elsewhere` and `served_path_required` are new codes, distinct from `write_not_permitted` and `direct_identity_write_not_admitted`.
- Every decode failure on the new ops is refused with `invalid_request`, the code other agent ops use (`crates/entorhinal-module/src/agent_reads.rs:66`). That covers an unknown field, a missing field, and a length, count or pattern outside its bound.
- The manifest pin test (`crates/entorhinal-module/src/agent_reads.rs`) lists the new ops with their kinds:
  - Mutations: `agent.register_session`, `agent.refresh_claims`, `member.set_name` and `role.take`.
  - Queries: `agent.session_liveness`, `agent.where`, `agent.for_session` and `role.read`.
- The existing sync-path test that posts `projects.session_liveness` from `reserved:prefrontal-core` through `handle_request` is rewritten in the feed cutover: on that seam no provider is resolved, so the post refuses `host_unbound` and leaves the mirror unchanged. Admission of the feed is proven on the served path against the resolver fake.
- Reply nulls: in `agent.where`, `role.read` and the `held_elsewhere` refusal detail, a field whose value is unknown or absent is present as `null` (for example `holder` and `cut_id` on an unclaimed agent, `holder_name` for a holder with no name). Only liveness `cause` is omitted. Each read has a byte-level reply test.

### `agent.register_session`
- Request: `{agent_id, ref_kind, ref, harness, session_id}`. Lengths: `ref_kind` 1 to 64 bytes, `ref` 1 to 512, `harness` 1 to 32, `session_id` 1 to 256. `harness` and `session_id` are the liveness feed's session key, as core sends it.
- Admission:
  - The call is admitted from the sole claimant of `agent-orchestrator/v1` or the sole claimant of `agent-host/v1`. `Direct` and every other caller get `caller_forbidden`.
  - The binding stores the role the caller was admitted under in `registered_by`. A caller that claims both capabilities, as prefrontal-core does today, is stored as `orchestrator`.
  - With neither capability claimed, the call refuses `orchestrator_unbound`.
- Everything below runs in one SQLite transaction, which reads the agent's cached claim and the persisted `self_author`:
  - If the claim row carries the integrity-failure mark, refuse `claim_integrity_failed`.
  - If the claim is held by another device, refuse `held_elsewhere`. Held by another device means `epoch >= 1` and `holder` is not the persisted `self_author`, or no `self_author` is persisted. The refusal detail names the holder pseudonym, its display name (or `null`), and the epoch.
  - Otherwise upsert the binding.
- A ref already bound to a different agent is refused with `ref_bound_elsewhere`, naming that agent. This applies even when the bound session reads `gone`. A `(harness, session_id)` already bound to a different ref is refused with `session_bound_elsewhere`, naming that ref and its agent, even when both refs belong to the same agent. Re-registering the same ref for the same agent updates `harness`, `session_id`, `registered_by` and `registered_at_ms`, and succeeds when the new session key is free.
- An absent cache row, or a claim at epoch 0, reads as unclaimed and is admitted. Nothing calls engram on this path.

### `agent.for_session`
- Request: `{harness, session_id}`, with the same bounds as `agent.register_session`. `harness` is opaque and is not checked against a list.
- Reply: the one `{agent_id, ref_kind, ref}` the unique session key selects in `session_binding`, or the refusal `not_found`. It's a read, open to every caller. Test: a second ref registered with a bound session key is refused `session_bound_elsewhere`, and `agent.for_session` still answers the first.

### `agent.session_liveness`
- Request: `{refs: [{ref_kind, ref}]}`, holding 1 to 256 refs. Reply: one `{state, cause}` per ref, in request order. `state` is `active`, `gone` or `unknown`. `cause` is present only when `state` is `unknown` and the session's mirror entry carried one. Otherwise the key is omitted.
- A ref with no binding is `unknown`. Otherwise the bound session's state comes from the liveness mirror, looked up by `(harness, session_id)`:
  - `active` and `idle` both answer `active`, because the host process is open either way.
  - `gone` answers `gone`.
  - Everything else answers `unknown`: no entry, `lapsed`, any unrecognised state, or a mirror waiting for its first full snapshot after a restart.
- The mirror decoder (`LivenessSession`, `crates/entorhinal-module/src/main.rs:728-736`, where `state` is already a free `String`) reads `harness` and the optional `cause`, and keys on `(harness, sessionId)`. An entry with no `harness` is keyed with an empty harness and never matches a binding. `canonical_root` stays required. `enumerate`'s root annotation and `tests::enumerate_annotation_joins_liveness_by_root_containment` are unchanged, and a `lapsed` or unrecognised-state session annotates as `unknown`.
- The decoder accepts `lapsed` and any unknown state string without refusing the batch, and stores them as unknown.
- Reads are open to every principal, as other reads are.

### Liveness feed cutover (separate slice)
- The feed moves from admission by `reserved:prefrontal-core` by name to admission by the sole claimant of `agent-host/v1`, resolved per batch. The by-name admission is removed, and `authorize_write` passes the feed through to this rule.
- Refusals:
  - With no claimant (unprovided, ambiguous or a transport failure), the feed is refused `host_unbound`.
  - Any other sender is refused `caller_forbidden`.
- A refused batch changes nothing: not `last_seq`, not the sessions map, not the stale latch. The mirror keeps its last state and the producer retries. A refusal bumps only an atomic refusal counter.
- The mirror records the route key and principal of the last admitted batch's sender, used only for the latch and never for admission. The stale latch is set when a batch is admitted from a different principal than the recorded one (the record is then replaced), and when `on_route_gone` (`crates/entorhinal-module/src/main.rs:612-614`) closes the recorded route. After that, `agent.session_liveness` and `enumerate` answer unknown until the next full snapshot. Each of the two latch sets has a mutation row.
- Tests:
  - A `Direct` batch refused while a claimant is bound leaves every `agent.session_liveness` answer unchanged.
  - A batch refused `host_unbound` leaves every answer and the latch unchanged.
  - After the provider's route is gone, every answer is unknown until the next snapshot.
  - With `agent-host/v1` rebound to `thalamus-harness`, its batch is admitted and sets the latch, and a batch from the orchestrator's claimant is refused `caller_forbidden`.
- Placement follows the release order above.

### Claim cache and `agent.where`
- The subject is `claim:<agent_id>`. The body is a versioned JSON document defined by entorhinal: `{v: 1, agent_id, holder, epoch, cut_id, reason}`. `reason` is set only on a forced take. The codec refuses three cases by name:
  - an unknown `v`: `claim_version_unknown`
  - a body whose `agent_id` differs from the subject's: `claim_subject_mismatch`
  - a malformed body: `claim_malformed`
- Refresh has these triggers and no others:
  - startup, once the identity log is enabled
  - completion of `identity_log.enable`
  - the catch-up worker's first successful engram read after `log_state` was non-zero or after the previous refresh failed (this is "reconnect" for both agent and role subjects)
  - `agent.refresh_claims`

  Agent claims have no private timer. A refresh failure is recorded in atomic gauges only and never changes `log_state`. Test under `tokio::test(start_paused)`: the record fake answers `unavailable`, then succeeds; advancing the 30 s catch-up tick yields exactly one refresh, after the success.
- `agent.refresh_claims {}` is admitted from `Direct` only; other callers get `caller_forbidden`. While the identity log is not enabled it refuses `identity_log_disabled`. It writes only `claim_cache` and `self_identity`. It signals the spawned refresh task and awaits the end of a refresh that started after the call was received; a refresh already in flight is not duplicated, and the call waits for the one queued after it. It replies `{refreshed, failed}` counts, or `engram_unavailable` when no record call succeeded. `ck agents where --refresh` calls it, then `agent.where`.
- Refresh reads every live agent's claim with `record.read_many`, in batches of up to 64, and applies each reply by this table. "Kept" means the cached row's body; an integrity-failed reply carries none. The same table governs `role_cache`, with one test case per row for each cache:

  | reply | cached row | result |
  |---|---|---|
  | `verify_failed` or `decrypt_failed` | present | body kept, mark set, `read_at_ms` updated |
  | `verify_failed` or `decrypt_failed` | none | row created: `holder` NULL, `epoch` 0, `version` 0, `stale` true, mark set |
  | verified present, codec refuses | any | as the integrity rows above; the codec code is counted in a health gauge |
  | verified present, decodes, version at least the cached one or no row | any | row replaced, `stale` copied from the reply, mark cleared |
  | verified, version older than the cached one | present | body and mark kept, `stale` true, `read_at_ms` updated |
  | absent, `stale: true` | version 1 or higher | body and mark kept, `stale` true |
  | absent, `stale: true` | none, or version 0 | unclaimed at epoch 0, `stale` true, mark kept |
  | absent, not stale | any | unclaimed at epoch 0, `stale` false, mark cleared |

  The mark is cleared only by the two rows that say so.
- `agent.where {agent_id}` replies from the cache only with `{known, status: unclaimed | held_here | held_elsewhere, holder, holder_name, epoch, cut_id, read_at_ms, stale}`.
  - `known` is false only when the agent has no cache row. The agent then reads `unclaimed` at epoch 0, with `holder`, `holder_name`, `cut_id` and `read_at_ms` null and `stale` true.
  - An unknown agent is refused `unknown_agent`, and a retired or merged one `gone`.
  - An agent whose row carries the integrity-failure mark is refused `claim_integrity_failed`.
  - `held_here` compares the holder with the persisted `self_author`, so it survives a restart with engram unreachable. With no `self_author` persisted, a claim at epoch 1 or higher reads as `held_elsewhere` with `holder_name` null and is never assumed to be held here.
- CLI: `ck agents where <agent> [--refresh]`, accepting an id or a name as the other `ck agents` verbs do.

### Role claims
- Role names match `[a-z0-9_]{1,64}`, and the subject is `role:<name>`. The body is versioned JSON, `{v: 1, role, holder, epoch, reason}`, decoded with the claim codec's rules and named refusals.
- `role.read {role}` replies `{role, known, held_here, epoch, holder, holder_name, source, stale}`. It's open to every caller and served from `role_cache`. It never waits on engram and does not depend on the identity log's state.
  - `known` is false only when this node has never read the role. Then `held_here` is false, `epoch` is 0, and `holder`, `holder_name` and `source` are null. A role held elsewhere always answers `known: true`, `epoch >= 1` and a holder.
  - `held_here` requires `epoch >= 1` and a holder equal to the persisted `self_author`. `holder_name` comes from `member_name`.
  - `source` is `cloud` when the row was last written from a verified refresh reply, and `local` when it was last written from this node's own `role.take` result and no refresh reply has replaced it since. Test: take answers `local`; a refresh then answers `cloud`; after a restart the row keeps its `source` with `stale: true`.
  - A `role.read` for a role that has never been read adds that role to the set refresh reads. The set holds at most 256 roles; a never-read role beyond that is refused `role_set_full`.
- Refresh reads role subjects on the spawned refresh task every 30 s, plus at startup and on reconnect, applying the table above. After a restart, the persisted entry answers with `stale: true` until a refresh lands. A temporary failure keeps the cached answer with `stale: true`.
- An integrity failure on the role's record (engram `verify_failed` or `decrypt_failed`) makes `role.read` refuse `claim_integrity_failed`. The mark persists across restarts, and only a later verified read clears it.
- `role.take {role, expected_epoch, request_id, reason}` replies `{epoch, held_here: true}`. `request_id` is 32 lowercase hex characters and is used as the record `write_id`. "Same input" means the same `{role, expected_epoch, reason}`.
  1. The `role_take_request` ledger is consulted before any prompt or engram call. The same id with different input refuses `id_reused`. The same id with the same input returns the stored result when one is recorded, and otherwise resends the stored attempt (same `write_id`, `expected_version` and bytes) without a new prompt, because the operator approved those bytes.
  2. A new id is confirmed by the operator, then the take reads the subject. If the epoch differs from `expected_epoch`, it refuses `epoch_moved {current_epoch, holder}`. Otherwise one short transaction stores the ledger row with the read version and the body `{holder: self_author, epoch: expected_epoch + 1, reason}`, and the take writes it with `record.cas` at that version. The read after approval is the revalidation; there is no in-transaction revalidation.
  3. A `version_moved` answers `epoch_moved`, unless the 64-version settlement read shows the write landed. A lost reply is settled by the compare-and-swap client's rules. The result is recorded in the ledger and in `role_cache` with `source: local`.

  Test: a take whose reply is dropped after it landed, retried with the same id and input, returns the original `{epoch, held_here: true}` and writes nothing new.
- `role.take` is admitted only from `Direct`, through its own arm, which reuses the operator-confirmation pieces: `ConfirmationGuard::take`, `OPERATOR_BOUND`, route-closure cancellation, and `confirm_operator` with a summary naming the role (`crates/entorhinal-module/src/shared_write.rs:2428-2447`). It does not use `precheck_agent_confirmation` or the `writer` lock. Where there is no presence provider, it refuses `operator_presence_unavailable`. It is not admitted from any module principal, which gets `caller_forbidden`.
- CLI: `ck roles take <role>` and `ck roles read <role>`, through a new `Face::Roles` with `ck-roles` and `ckdev-roles` names and its help text (`crates/entorhinal-module/src/cli.rs:95-125`). `face_mapping_accepts_paths_case_and_one_exe_suffix` (row `s1-face-from-argv0`) is extended. Placing the `ck-roles` link in CortexKit's bin folder is an installer dependency, not in scope.

### The claim compare-and-swap client
- A typed client for `record.cas`, built and tested against the fake.
  - Retries resend the same `write_id`, `expected_version` and bytes.
  - A lost reply is settled by the resend rule while the write is among the subject's 64 most recent versions. Past that window, it is settled by reading the subject: the write landed when the version's `author` is this node and its body matches.
  - `version_moved` is surfaced with the current version and author.
- In part 1 only `role.take` calls it. No agent claim is written.

### Member display names
- `member.set_name {author, display_name}` is a new shared identity-log op, decoded with `deny_unknown_fields`.
  - It's admitted only from `Direct`; any other caller, `reserved:prefrontal-core` included, gets `caller_forbidden`.
  - It's not a confirmable write.
  - It carries no request key, so a retry after `engram_outcome_unknown` may append a second, equal entry. That is harmless.

  Refusals:
  - `identity_log_disabled` when the log is not enabled, and `identity_log_enabling` while it is enabling or joining.
  - `self_author_unknown` when no `self_author` is persisted.
  - `not_self_author` when `author` isn't the persisted `self_author`.
  - `display_name_invalid` unless the trimmed name is 1 to 64 characters.
- Remote apply compares the body's `author` with the log entry's `author` pseudonym, never with `signer`, which is the signing key. `RemoteEntry` (`crates/entorhinal-core/src/remote_apply.rs:33-36`) gains `author`, filled at its four production sites: from the log entry at `crates/entorhinal-module/src/catch_up.rs:68` and `crates/entorhinal-module/src/enable.rs:299`, and copied from the source part at `crates/entorhinal-core/src/remote_apply.rs:353` and `crates/entorhinal-core/src/enable_state.rs:419`. `signed_by_self` is consumed in catch-up and does not travel on `RemoteEntry`. A mismatch is an `apply_failed` stall at that position, like every other invalid entry: `identity_log_stalled`, no name row, and the applied position unchanged.
- Names are stored in a shared table, `member_name`, keyed by `author`. It joins `DERIVED_TABLES`.
  - `remote_apply::apply` admits `member.set_name` beside `AGENT_OPS`.
  - `delete_and_replay` gets a `member.set_name` arm for local `origin='here'` rows and for the shared-image branch.
  - `member_name` is not added to `PROJECT_SHARED_TABLES`, because a `project.shared` upsert would bypass the author check.
- Snapshot handling:
  - A `shared.snapshot` carries at most its own author's name, in the `ProjectEntry` field `member_name: {author, display_name}` with `#[serde(default, skip_serializing_if = "Option::is_none")]`, on part 1 only; other parts must omit it. A snapshot by a node with no name therefore has no such key. `ProjectEntry` is `deny_unknown_fields` (`crates/entorhinal-core/src/shared_entry.rs:33-42`), and `validate` refuses the field on `project.shared` entries as it refuses `agents`.
  - Byte test: a supersede by a node with no name serialises to exactly the pre-change bytes.
  - Apply refuses (stalls) a snapshot naming any other author.
  - `enable`'s supersede includes the writer's current name, so a later joiner still learns it.
  - A reader-side guard stalls a superseding snapshot that omits a name its replaced prefix set. The guard has its own mutation row, the names analogue of `supersede-keeps-every-agent`.
- Rollout: every member runs this release before the first `ck projects log name`. By design, an older member stalls at the first name entry or name-carrying snapshot, and its shared writes refuse `identity_log_stalled`. It recovers on upgrade with no operator repair. Test: a store stalled at a name entry under the old decoder, reopened with the new binary, applies the entry and advances.
- CLI: `ck projects log name <display name>`, a Direct operator write.
- `agent.where` fills `holder_name` from this table.

### Tests and fences
- Every refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`.
- No test asserts elapsed real time. All tests pass on Linux, macOS and Windows CI.
- Register-session atomicity: a cfg(test) hook fires inside the registration transaction, after the claim read. A cache write started at the hook commits only after the registration and isn't visible to it, so the reply equals the pre-hook state. A cache write committed before the transaction begins is seen.
- Two-node fixtures build each node's record fake and its `FakeLog` view from one 16-byte author. The record fake returns that author as `self_author` and stamps it as `author` on records the node wrote.
- Rebuild after `member.set_name` is cell-identical, following the `cells()` pattern in `crates/entorhinal-core/src/log_schema.rs`.

## intent
The operator approved AGENT_SYNC draft 11 (engram repository, `AGENT_SYNC.md`): agents move between the operator's Macs, and entorhinal owns where each agent lives. This campaign builds the parts of entorhinal's share that don't depend on the move itself. These are also what every module will ask entorhinal before it starts an agent anywhere:

- **Session registry.** The claimant of `agent-orchestrator/v1` or `agent-host/v1` registers every session it starts for an agent with `agent.register_session`. Entorhinal checks its cached claim and records the binding in one transaction, so no move can land between the check and the registration. `agent.for_session` maps a session back to its binding.
- **Session liveness.** `agent.session_liveness(refs)` answers `active`, `gone` or `unknown` per ref, from the liveness mirror entorhinal already keeps. `unknown` counts as busy.
- **The checkout claim cache and `agent.where`.** A machine-local cache of each agent's claim record (holder, epoch, last cut). It is read from engram's generic encrypted compare-and-swap record and refreshed at startup and on reconnect. `agent.where` answers from the cache only, never from engram.
- **Role claims.** `role.read` and the operator-confirmed `role.take` work on the same record. Prefrontal's Plexus builds against them.
- **Member display names** in the shared identity log, keyed by the device pseudonym (`author`) that engram returns, so `agent.where` can name the holding Mac.

The move command, the move record and the participants (`agent-data/v1`) are part 2, a separate campaign.

## non_goals
- Moving agents: `agent.move`, the move record, the move driver, participant calls, forks and lid close. These are part 2.
- Writing agent claims. Part 1 reads and caches them, and it builds and tests the claim body codec and the compare-and-swap client against a fake. The first agent claim write is part 2's move. The only record write in part 1 is `role.take`.
- Unbinding: `agent.session_end` and removing `session_binding` rows are part 2. In part 1 a bound ref stays bound to its agent, even once its session reads `gone`.
- Sessions with no agent. They are never registered, and entorhinal never mints an agent from a session. The liveness feed still carries them, as it does today.
- Any per-turn check. Nothing here runs per turn.
- Changes in prefrontal-core, engram or the subc daemon. Those are external dependencies, listed in the constraints.

## open_questions
None.



## slice_hints
- s1, sessions:
  - migration V8 with every new table (`session_binding` with `registered_by`, `claim_cache`, `role_cache`, `self_identity`, `member_name`)
  - the per-capability `ProviderResolver` seam and its fake
  - `agent.register_session`, admitted from the claimant of `agent-orchestrator/v1` or `agent-host/v1`
  - `agent.for_session`
  - `agent.session_liveness` with `cause`
  - the `(harness, session_id)` mirror key, and `lapsed`

  The feed keeps today's admission until s4.
- s2, claims (after s1):
  - the engram record client and fake
  - the claim codec
  - refresh with persisted `self_author` and the integrity-failure mark
  - `agent.where` with `known`
  - the register-session claim checks
- s3, names (after s2):
  - `member.set_name` with its remote-apply, replay and snapshot arms
  - `author` on `RemoteEntry`
  - `ck projects log name`
  - two-node acceptance
- s4, feed cutover (after s3; placed under the release order in the constraints):
  - the feed admitted only from `agent-host/v1`'s sole claimant
  - `host_unbound`, which leaves the mirror and the latch unchanged
  - removal of the by-name `reserved:prefrontal-core` admission
  - the rewritten sync-path test and the non-host refusal test
- s5, roles (after s2):
  - `role.read` and its 30 s refresh
  - `role.take` through the operator-confirmation flow, on the compare-and-swap client
  - the 64-version lost-reply settlement
  - `ck roles take` and `ck roles read`

## chair rulings (refire)

The following chair rulings are normative and override conflicting earlier text.



<!-- spec-refire-ledger-projection:v1 -->

### recorded supplied ruling 2

Campaign `ct_00000000-0000-408a-98dd-e572e1ab76c8`, round 3, decision sequence 0 (source: {"body_bytes":222,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-408a-98dd-e572e1ab76c8-20261012T153725Z.md"}):

Fold the seven chair rulings recorded with the round-2 answer (R1 to R7) and check the round-2 fixes: the per-request provider resolution, `agent.refresh_claims`, the spawned refresh task, and the field omitted when unset.

### recorded supplied ruling 3

Campaign `ct_00000000-0000-410d-98dd-e75fe784fe18`, round 0, decision sequence 4 (source: {"ask_id":"owner-loop-ct_00000000-0000-410d-98dd-e75fe784fe18-r0","provenance":"answer_time","source":"owner_loop_answer"}):

R8. The scoped reading of R1 is right. The no-by-name rule covers every op and feed this campaign adds or changes. The existing `WRITER_MODULE` gate on the `agent.*` identity relay writes and on the project write methods (`authorize_write`) stays exactly as it is. Changing it is out of scope, and non_goals says so.

R9. Evidence: `subc-client-rs` 0.28.0 (`src/consumer.rs:1374`) exports `pub async fn resolve_provider(&self, capability: &str) -> Result<String, CallError>` on `SubcConsumer`, plus `resolve_providers` returning `Vec<String>`.
- With no claimant it returns `CallError::CapabilityUnprovided { capability }`, code `capability_unprovided`.
- With more than one it returns `CallError::CapabilityAmbiguous { capability, claimants }`, code `capability_ambiguous`.
- A malformed identifier returns `CallError::InvalidCapabilityIdentifier`, code `invalid_capability_identifier`.
- It filters `catalog.list` on `capabilities.provides`. Per SUBC, it needs nothing in the caller's manifest.

R10. Evidence: `enumerate`'s liveness annotation (`crates/entorhinal-module/src/main.rs:1037-1066`, `annotate_liveness`) emits no state string. It sets only `last_route_activity_ms`: the newest `last_activity_ms` among the mirror's sessions whose `canonical_root` is inside one of the project's roots. It doesn't filter on state; `gone` sessions leave the mirror when pruned. Keying the mirror by `(harness, sessionId)` must not change that output for a given set of sessions.

R11. Evidence: prefrontal-core's live `SessionLivenessEntry` (`prefrontal/crates/prefrontal-core-module/src/projects_consumer.rs`, camelCase on the wire) serializes:
- a required `harness: String` under the JSON key `harness`;
- `sessionId`, `projectIdHint` (optional), `canonicalRoot`, `state` (`active` | `idle` | `gone`, lowercase) and `lastActivityMs`.
Every batch core sends today carries `harness`. Entorhinal currently ignores it.

### recorded owner answer 4

Campaign `ct_00000000-0000-410d-98dd-e75fe784fe18`, round 2, decision sequence 0 (source: refire_from:ct_00000000-0000-408a-98dd-e572e1ab76c8):

Chair rulings, agreed in the fleet's agent-sync channel after this draft was written. Each replaces the draft text it contradicts.

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

### recorded supplied ruling 5

Campaign `ct_00000000-0000-410d-98dd-e75fe784fe18`, round 2, decision sequence 1 (source: {"body_bytes":223,"boundary":"ledger_payload","order":5,"provenance":"supplied_file","source_path":"/Users/ufukaltinok/Work/Projects/CortexKit/entorhinal/.cortexkit/alfonso/prompts/rulings-ct_00000000-0000-410d-98dd-e75fe784fe18-20261012T160029Z.md"}):

Fold rulings R8 to R11 and review the round-0 mechanisms: the feed pass-through, the async arms, the role-take ledger, the cache decision table, the source definition, the unique session key, and the scoped no-by-name rule.

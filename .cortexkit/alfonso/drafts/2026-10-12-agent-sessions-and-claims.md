---
title: "Agent sync in entorhinal, part 1: session registry, session liveness, the checkout claim cache, and member names"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - "/Users/ufukaltinok/Work/Projects/CortexKit/engram/AGENT_SYNC.md:16-23"
    - "/Users/ufukaltinok/Work/Projects/CortexKit/engram/AGENT_SYNC.md:50-110"
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
- Any per-turn check. Nothing here runs per turn.
- Changes in prefrontal-core, engram or the subc daemon. Those are external dependencies, listed below.

## constraints

### External dependencies
- **SUBC: reserved capability.** Built since subc-core 0.7.0, and live on the operator's Mac: `subc.jsonc` has `"reserved_capabilities": {"agent-orchestrator/v1": "prefrontal-core"}` (added 2026-10-12, by rescan). Any other module declaring it is refused at HELLO with `reserved_capability`, and never appears in the catalog.
  - Entorhinal resolves the provider with `SubcConsumer::resolve_provider("agent-orchestrator/v1")` in subc-client-rs. It filters `catalog.list` on `capabilities.provides` and returns the sole provider's module id, or a typed `capability_unprovided` or `capability_ambiguous`. It needs nothing in entorhinal's manifest. The catalog carries no principal, so the caller is admitted when its daemon-stamped principal equals `reserved:<that module id>`.
  - Resolve on every call (one local round trip), or cache with these rules: never cache `capability_unprovided`, drop the cache when the provider's route closes, and treat `capability_ambiguous` as a refusal, never a pick.
  - Prefrontal-core doesn't declare `agent-orchestrator/v1` yet. So the liveness feed's admission change ships only after core declares it, or the mirror would refuse every batch. The chair orders that release.
- **Prefrontal-core: liveness feed.** It keeps sending ordered liveness batches (core's `projects_consumer.rs`, `SessionLivenessEntry`). Each entry carries `harness` (`opencode` or `pi`) and `sessionId`, the harness's own unprefixed session id (an OpenCode `ses_…` id or a Pi session id). The pair `(harness, sessionId)` is the session key, never concatenated. It is stable across a host restart that resumes the same session. States today are `active`, `idle` and `gone`. Core will add `lapsed` (lease expired, not a clean end), possibly via a later harness module that takes over the feed; nothing here waits for it.
- **Engram: generic record.** `record.read {subject}` returns `{absent, version, body, author, self_author, stale}`; `record.cas {subject, expected_version, body, write_id}` returns `{version}` or refuses `version_moved {current_version, author}`; `record.read_many {subjects}` (up to 64). Callers: only `reserved:entorhinal`. Built against an in-tree fake that enforces the agreed rules, as the identity-log client was.

### Machine-local state
- One store migration adds two machine-local tables, outside the identity journal and never shared:
  - `session_binding`: `ref_kind`, `ref` (primary key together), `agent_id`, `harness`, `session_id`, `registered_at_ms`.
  - `claim_cache`: `agent_id` (primary key), `holder` (author pseudonym, or NULL when the record is absent), `epoch`, `cut_id`, `version`, `read_at_ms`, `stale`.
- `verify` and `rebuild` ignore both tables, and nothing about them enters `registry_journal`.
- Health stays lock-free: any new gauge is an atomic.

### `agent.register_session`
- Request: `{agent_id, ref_kind, ref, harness, session_id}`. `ref_kind` and `ref` are opaque non-empty strings with a stated length cap. `harness` and `session_id` are the liveness feed's session key, as core sends them.
- Admitted only from the daemon-attested principal of the module that provides `agent-orchestrator/v1` in the catalog. Refused with `orchestrator_unbound` when no module provides it, and with `caller_forbidden` from any other caller, `Direct` included. Flow-scoped routes are refused as for every write.
- Refused with `authority_not_cut_over` before the identity switchover, `unknown_agent` for an unknown agent, and `gone` for a retired or merged one.
- In one store transaction: read the agent's cached claim. If it's held by another device (`epoch >= 1` and `holder != self`), refuse `held_elsewhere`, with a detail naming the holder pseudonym, its display name when known, and the epoch. Otherwise upsert the binding.
- A ref already bound to a different agent is refused with `ref_bound_elsewhere`, naming that agent. Re-registering the same ref for the same agent updates `harness`, `session_id` and `registered_at_ms`, and succeeds.
- An absent cache row, or a claim at epoch 0, reads as unclaimed and is admitted. Nothing calls engram on this path.

### `agent.session_liveness`
- Request: `{refs: [{ref_kind, ref}]}`, bounded in count. Reply: one state per ref, in request order: `active`, `gone` or `unknown`.
- A ref with no binding is `unknown`. Otherwise the bound session's state comes from the liveness mirror, looked up by `(harness, session_id)`. `active` and `idle` both answer `active`, because the host process is open either way. `gone` answers `gone`. Anything else (no entry, `lapsed`, any unrecognised state, or a mirror waiting for its first full snapshot after a restart) answers `unknown`.
- The liveness mirror is keyed by `(harness, sessionId)`. Today it decodes only `sessionId` and ignores `harness`, so two harnesses' sessions with the same id would collide. The decoder reads `harness` and keys on the pair; an entry with no `harness` is keyed with an empty harness and never matches a binding.
- The liveness decoder accepts `lapsed` and any unknown state string without refusing the batch, and stores them as unknown.
- Reads are open to every principal, as other reads are.

### Liveness feed admission
- The liveness feed is admitted from the module that provides `agent-orchestrator/v1`, instead of `reserved:prefrontal-core` by name. With nothing bound, the feed is refused (`orchestrator_unbound`), and the mirror reports every session as unknown.
- This touches daemon coupling, so the chair sends it to SUBC for review before it ships.

### Claim cache and `agent.where`
- Subject `claim:<agent_id>`. Body: a versioned JSON document defined by entorhinal, `{v: 1, agent_id, holder, epoch, cut_id, reason}`. `reason` is set only on a forced take. The codec refuses an unknown `v`, a body whose `agent_id` differs from the subject's, and a malformed body, by name.
- Refresh: at startup once the identity log is enabled, whenever engram becomes reachable again after a failure, and on demand (`ck agents where --refresh`). Every live agent's claim is read with `record.read_many` in batches. A stale reply updates `read_at_ms` and the `stale` flag but never replaces a cached version with an older one. An absent record caches as unclaimed.
- Refresh runs on its own task, takes the store's write lock only to record results, and never blocks reads, health or `agent.register_session`.
- `agent.where {agent_id}` replies `{status: unclaimed | held_here | held_elsewhere, holder, holder_name, epoch, cut_id, read_at_ms, stale}` from the cache only. `held_here` compares the holder with this device's `self_author` from the last engram reply. With no `self_author` known yet, an epoch-1-or-higher claim reads as `held_elsewhere` with no name, and is never assumed to be held here.
- CLI: `ck agents where <agent>`.

### The claim compare-and-swap client
- A typed client for `record.cas`, built and tested against the fake: retries resend the same `write_id` and bytes, a lost reply is settled by the agreed retry rule, and `version_moved` is surfaced with the current version and author. Nothing in part 1 calls it outside tests.

### Member display names
- A new shared identity-log op, `member.set_name {author, display_name}`. A node may set only its own name: `author` must equal its `self_author`, and remote apply refuses any entry whose author field differs from the entry's signer. Display names are trimmed, non-empty and capped at 64 characters.
- Stored in a shared table keyed by `author`, replayed by rebuild, and covered by `verify`.
- CLI: `ck projects log name <display name>`, a Direct operator write. It needs the identity log enabled, and refuses otherwise.
- `agent.where` fills `holder_name` from this table.

### Tests and fences
- Every refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`.
- No test asserts elapsed real time. All tests pass on Linux, macOS and Windows CI.
- The register-session check and the binding write are one transaction. A test parks a cache update between them and shows that the registration sees either the old state or the new state, never a mix.

## acceptance_sketch
1. The orchestrator registers a session for a live, unclaimed agent, and the binding is stored. A second registration of the same ref for another agent is refused with `ref_bound_elsewhere`.
2. With a cached claim held by another device, registration is refused with `held_elsewhere`, naming the holder and its display name. With the claim held here, or absent, or at epoch 0, it's admitted.
3. `Direct`, another module and a flow route are refused. With no orchestrator bound, `orchestrator_unbound`.
4. `agent.session_liveness` maps active, idle, gone, lapsed, missing, unbound and a waiting mirror exactly as specified, in request order, and the same session id under two harnesses stays two sessions.
5. A liveness batch from a non-orchestrator is refused. One carrying `lapsed` is accepted, and the session reads as unknown.
6. Refresh caches claims from the fake engram: absent, held here, held elsewhere, and a stale older version that must not replace a newer cached one. `agent.where` reports each correctly without calling engram.
7. With engram unreachable, startup, every read and `agent.register_session` still answer.
8. `member.set_name` sets this node's name, a forged entry naming another author is refused on apply, and the name appears in `agent.where` on the other node.
9. Verify and rebuild stay clean, and the machine-local tables never appear in the journal or the identity log.

## open_questions
None.

## slice_hints
- s1, sessions: the migration's `session_binding` table, `agent.register_session` with orchestrator admission, `agent.session_liveness`, `lapsed`, and the feed's admission change.
- s2, claims: the `claim_cache` table, the engram record client and fake, the claim codec, refresh, `agent.where`, and the register-session claim check.
- s3, names: `member.set_name` in the shared log, `ck projects log name`, and two-node acceptance.

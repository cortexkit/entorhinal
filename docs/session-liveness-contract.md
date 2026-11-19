# Session liveness contract

This contract covers how entorhinal learns which agent sessions are alive and
how it answers others. Two operations and a bus bucket take part:

- `agent.register_session` binds an agent's session reference to the host
  session running it.
- `prefrontal.session-liveness` is the bus bucket entorhinal watches for
  session states.
- `agent.session_liveness` answers `active`, `gone` or `unknown` for session
  references.

`agent.for_session` and `agent.session_for_ref` read the bindings.

The code is in `crates/entorhinal-module/src/sessions.rs` (admission,
registration, answers), `crates/entorhinal-module/src/liveness_bus.rs` (the
bucket mirror), and
`RegistryStore::register_session` in `crates/entorhinal-core/src/lib.rs` (the
binding row). Test names below are functions in those files' test modules.

## 1. Vocabulary

| Term | Meaning |
|---|---|
| session reference | `(ref_kind, ref)`: the name an agent's session has in the agent world. `ref_kind` is 1–64 bytes, `ref` 1–512 bytes. |
| session key | `(harness, session_id)`: the host session that runs it. `harness` is 1–32 bytes, `session_id` 1–256 bytes. The two parts are kept apart; the same `session_id` under two harnesses is two sessions. |
| binding | One `session_binding` row: reference, agent, session key, project root (1–4096 bytes), the role that registered it and when. |
| orchestrator | The sole provider of `agent-orchestrator/v1`, as the daemon resolves it. |
| host | The sole provider of `agent-host/v1`, as the daemon resolves it. |
| mirror | entorhinal's in-memory copy of the bucket's session states. |
| authoritative | The mirror has a live watch, a finished initial replay and a valid reconciliation marker. |

The answer vocabulary of `agent.session_liveness` is closed: `active`, `gone`,
`unknown`.

## 2. Registration

1. `agent.register_session` is admitted only from `reserved:<module>` where the
   module is the daemon's sole provider of `agent-orchestrator/v1` or
   `agent-host/v1`. No fixed module name decides it. A module that provides
   both registers as `orchestrator`.
2. If neither capability resolves to exactly one provider, the request refuses
   `orchestrator_unbound`, with a detail naming what the daemon answered for
   each capability (`resolved`, `capability_unprovided`,
   `capability_ambiguous`, `invalid_capability_identifier`,
   `transport_failure`). Another caller refuses `caller_forbidden`. A flow
   scoped route refuses `flow_scope_not_admitted` before entorhinal asks the
   daemon anything.
3. Each route caches its own resolution. A cached answer that would refuse is
   refreshed once before refusing; a transport failure or timeout is never
   cached; closing the route or changing its principal drops the cache.
4. The store then refuses, in this order, all inside the transaction that
   writes the row: `authority_not_cut_over` (agent identity has not been
   imported yet), `unknown_agent`, `gone` (retired or merged), `agent_moving`
   (an open move fences the agent, or reserves this reference for an agent
   arriving from another machine; see
   [agent-move-contract.md](agent-move-contract.md)),
   `claim_integrity_failed`, `held_elsewhere` (a cached
   claim at epoch 1 or higher names another machine), `ref_bound_elsewhere` (the
   reference belongs to another agent) and `session_bound_elsewhere` (the
   session key already binds another reference). A refused registration
   writes nothing.
5. Registering the same reference again for the same agent moves it to the new
   session key and records the new role and time.
6. Bindings are machine-local. They are never journaled and never sent to the
   identity log.

Tests: `a_host_provider_other_than_the_executive_registers_as_host`,
`the_orchestrator_and_a_provider_of_both_register_as_orchestrator`,
`no_sole_provider_refuses_every_caller_orchestrator_unbound`,
`admission_refuses_flow_then_resolution_then_principal`,
`a_cached_registration_mismatch_refreshes_once_before_refusal`,
`registration_transport_failure_caches_neither_capability`,
`registration_refuses_cutover_agent_ref_and_session_in_order`,
`re_registering_a_reference_updates_its_session_and_role`.

## 3. The bucket

Entorhinal uses its credentialed bus connection to watch the local
`prefrontal.session-liveness` KV bucket in stream order, with history and
tombstones. It never polls a session host.

1. **Keys and values.** A session key is written as
   `<harness>.<hex-encoded sessionId>` (for example,
   `broca.73657373696f6e5f73616d706c65` for `session_sample`).
   Entorhinal treats the KV key as opaque: identities come from the JSON value,
   not from splitting the key. A value
   carries `harness`, `sessionId`, `state` (`active` or `idle`),
   `lastActivityMs` and nullable `projectId`. Unknown fields are ignored.
   Prefrontal reports `active` while the session's head is in a nonterminal
   residence flip (a handover in progress on one machine), and fails closed
   to `active` if it cannot read flips. Entorhinal relies on that state to
   refuse a cross-machine move mid-handover with `session_active`.
2. **Reconciliation.** `_reconciled` carries `{bootId, sessions}`. A valid PUT
   marks reconciliation complete; deleting it withdraws that marker. Other
   underscore-prefixed PUTs are ignored.
3. **Trust.** The mirror is authoritative only when all three conditions hold:
   the watch is live, its initial replay is finished, and a valid `_reconciled`
   marker has arrived. A session updated after the marker may replay later,
   so the marker alone is not enough. An empty bucket also waits for a marker
   and the replay boundary.
4. **Outages.** Startup, a failed watch, a connection replacement or a decode
   error leaves session liveness `unknown`. Entorhinal clears the mirror and
   retries with bounded backoff; a new watch must replay and reconcile again.
   The mirror is volatile and never journaled.
5. **Upkeep.** Only an observed DEL for a previously decoded session may enqueue
   an end stamp, and only when the mirror is authoritative and the session is
   gone. A surviving key for the same identity prevents that stamp. Initial
   tombstones, missing keys and watch failures do not stamp bindings ended:
   the bucket does not cover every host.

Tests: `liveness_bus_requires_live_reconciled_replay`,
`liveness_bus_decodes_prefrontal_values_and_orders_keys_before_marker`,
`liveness_bus_key_updated_after_marker_waits_for_initial_replay`,
`liveness_bus_empty_bucket_waits_for_marker_on_first_delivery`,
`liveness_bus_watch_error_clears_rewatches_and_replays_marker`,
`liveness_bus_upkeep_stamps_only_observed_authoritative_deletes`.

## 4. Answers

`agent.session_liveness` takes 1–256 references and returns one answer per
reference, in request order. An empty or longer list, or a reference outside
its byte bounds, refuses `invalid_request`. It is a read, open to every caller.

| Situation | Answer |
|---|---|
| No binding for the reference | `unknown` |
| Binding with no session key | `gone` |
| Mirror not authoritative | `unknown` |
| Mirror entry says `active` or `idle` | `active` |
| Authoritative mirror has no entry for the session key | `gone` |

What is guaranteed:

- `active` only when the mirror is authoritative and the bucket's latest value for
  that session key is `active` or `idle`.
- A missing session key is `gone` only in an authoritative mirror, except for
  a binding that has no session key at all.
- Losing authority makes session-key answers `unknown`, never `gone`.
  Callers treat `unknown` as busy.

What is not guaranteed: that the bucket covers every host or that its values
are current. Entorhinal answers from the last values it received.

Tests: `session_liveness_maps_every_state_in_request_order`,
`malformed_session_requests_are_invalid_request`.

## 5. Use in `enumerate`

When the mirror is authoritative, `enumerate` sets each project's
`last_route_activity_ms` to the newest `lastActivityMs` among mirrored
sessions whose `projectId` matches the project. A deleted session never
counts. When the mirror is not authoritative or no session matches, the field is left
absent: absent means "not known", not "no activity". Other modules use this
field to find folders nobody works in; entorhinal itself never removes
anything because of it.

Test: `enumerate_annotation_joins_liveness_by_project_id`.

## 6. Lookups

- `agent.for_session {harness, session_id}` returns the agent and reference
  bound to a session key, or `not_found`.
- `agent.session_for_ref {ref_kind, ref}` returns the agent, session key and
  project root bound to a reference, or `not_found`.

Both are reads, open to every caller. Tests:
`for_session_returns_the_binding_or_not_found`,
`session_for_ref_returns_the_matching_binding_and_tracks_reregistration`.

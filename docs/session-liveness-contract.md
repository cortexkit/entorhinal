# Session liveness contract

This contract covers how entorhinal learns which agent sessions are alive and
how it answers others. Three operations take part:

- `agent.register_session` binds an agent's session reference to the host
  session running it.
- `projects.session_liveness` is the feed: the session host pushes the state of
  its sessions to entorhinal.
- `agent.session_liveness` answers `active`, `gone` or `unknown` for session
  references.

`agent.for_session` and `agent.session_for_ref` read the bindings.

The code is in `crates/entorhinal-module/src/sessions.rs` (admission,
registration, answers), `ProjectsHandler::session_liveness` and
`LivenessState` in `crates/entorhinal-module/src/main.rs` (the feed), and
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
| mirror | entorhinal's in-memory copy of the host's session states, built from the feed. |
| stale | The mirror cannot vouch for anything and is waiting for a snapshot. |

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

## 3. The feed

A batch is `{seq, snapshot?, sessions?}`. `seq` is required; `snapshot`
defaults to `false`; `sessions` defaults to empty. Each session carries
`sessionId`, `canonicalRoot` and `state`, and optionally `harness`,
`lastActivityMs` and `cause`. The reply is
`{accepted, snapshotRequested, tracked}`.

1. **Admission.** Only `reserved:<host>` may send a batch. If the host does not
   resolve to one provider, the batch refuses `host_unbound`; another caller
   refuses `caller_forbidden`. A refused batch changes nothing: not the mirror,
   not the sequence, not the stale latch.
2. **Snapshots.** A batch with `snapshot: true` replaces the whole mirror,
   whatever its `seq`, and clears the stale latch. An empty snapshot is
   information: it says the host tracks nothing. Only an explicit
   `snapshot: true` counts; a batch that omits it is a delta.
3. **Deltas.** On first contact any `seq` is accepted as the baseline, but the
   mirror stays stale. Afterwards a delta at or below the last applied `seq` is
   refused without being applied and makes the mirror stale; a delta that
   skips ahead is applied and makes the mirror stale.
4. **The stale latch.** The mirror starts stale when the module starts. It also
   goes stale when the sending principal changes or the sender's route closes.
   It stays stale until a snapshot arrives, and every reply while stale carries
   `snapshotRequested: true`.
5. **States.** A session reported `gone` leaves the mirror and is remembered in
   a separate set, capped at 4,096 sessions; when the cap is exceeded the
   oldest is forgotten. Any other state string is kept as sent, including
   states this build does not know.
6. **Volatile.** The mirror lives in memory only. It is never journaled and is
   lost on restart, which is why a restarted module starts stale.

Tests: `liveness_feed_non_host_callers_preserve_every_answer`,
`liveness_feed_unbound_preserves_mirror_sequence_and_latch`,
`liveness_feed_flow_refuses_before_resolution`,
`a_fresh_receiver_asks_for_a_snapshot_until_one_arrives`,
`a_snapshot_replaces_the_mirror_and_an_empty_one_still_counts`,
`a_batch_that_does_not_say_it_is_a_snapshot_cannot_clear_staleness`,
`session_liveness_gap_detection_and_snapshot_rebase`,
`an_equal_sequence_batch_cannot_resurrect_a_gone_session`,
`an_older_batch_cannot_resurrect_a_session_that_is_already_gone`,
`liveness_feed_rebinding_to_thalamus_latches_until_snapshot`,
`liveness_feed_recorded_route_closing_latches_until_snapshot`,
`unfamiliar_states_are_accepted_and_leave_enumerate_annotation_unchanged`.

## 4. Answers

`agent.session_liveness` takes 1–256 references and returns one answer per
reference, in request order. An empty or longer list, or a reference outside
its byte bounds, refuses `invalid_request`. It is a read, open to every caller.

| Situation | Answer |
|---|---|
| No binding for the reference | `unknown` |
| Binding with no session key | `gone` |
| Mirror stale | `unknown` (with the mirrored `cause`, if any) |
| Mirror entry says `active` or `idle` | `active` |
| Mirror entry says anything else | `unknown` (with its `cause`, if any) |
| No mirror entry, key in the `gone` set | `gone` |
| No mirror entry, key not remembered | `unknown` |

What is guaranteed:

- `active` only when the mirror is not stale and the host's latest report for
  that session key is `active` or `idle`.
- `gone` is never produced for a session whose latest report was not `gone`,
  except for a binding that has no session key at all.
- Forgetting a `gone` session (the 4,096 cap, or a snapshot) can only turn its
  answer into `unknown`, never into `active`. Callers treat `unknown` as busy.

What is not guaranteed: that the host's reports are current. entorhinal
answers from the last reports it received; it never polls the host.

Tests: `session_liveness_maps_every_state_in_request_order`,
`malformed_session_requests_are_invalid_request`.

## 5. Use in `enumerate`

When the mirror is not stale and not empty, `enumerate` sets each project's
`last_route_activity_ms` to the newest `lastActivityMs` among mirrored
sessions whose `canonicalRoot` is inside one of the project's roots. A `gone`
session never counts. When the mirror is stale or empty, the field is left
absent: absent means "not known", not "no activity". Other modules use this
field to find folders nobody works in; entorhinal itself never removes
anything because of it.

Test: `enumerate_annotation_joins_liveness_by_root_containment`.

## 6. Lookups

- `agent.for_session {harness, session_id}` returns the agent and reference
  bound to a session key, or `not_found`.
- `agent.session_for_ref {ref_kind, ref}` returns the agent, session key and
  project root bound to a reference, or `not_found`.

Both are reads, open to every caller. Tests:
`for_session_returns_the_binding_or_not_found`,
`session_for_ref_returns_the_matching_binding_and_tracks_reregistration`.

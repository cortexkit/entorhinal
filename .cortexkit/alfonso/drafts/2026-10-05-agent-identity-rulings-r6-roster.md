---
rounds: 1
---
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

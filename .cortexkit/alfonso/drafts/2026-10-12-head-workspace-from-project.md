---
title: "Heads and hirees take their workspace from their project; placement carries their names"
status: draft
rounds_cap: 3
mint: auto
integration_ref: "main"
evidence:
  include:
    - "docs/designs/head-workspace-follows-placement.md"
    - "crates/entorhinal-core/src/agent/mod.rs:1-100"
    - "crates/entorhinal-core/src/agent/claims.rs"
    - "crates/entorhinal-core/src/agent/journal.rs"
    - "crates/entorhinal-core/src/agent/store.rs:320-740"
    - "crates/entorhinal-core/src/agent/fleet.rs:20-70"
    - "crates/entorhinal-core/src/agent/reads.rs:40-60"
    - "crates/entorhinal-core/src/agent/reads.rs:260-340"
    - "crates/entorhinal-core/src/agent/import.rs:250-312"
    - "crates/entorhinal-core/src/mutations.rs:560-600"
    - "crates/entorhinal-core/src/mutations.rs:620-715"
    - "crates/entorhinal-core/src/mutations.rs:840-930"
    - "crates/entorhinal-core/src/mutations.rs:1100-1140"
    - "crates/entorhinal-core/src/mutations.rs:1480-1760"
    - "crates/entorhinal-module/src/main.rs:530-640"
    - "crates/entorhinal-module/src/main.rs:1560-1620"
    - "crates/entorhinal-module/src/agent_ops.rs"
    - "crates/entorhinal-module/src/shared_write.rs:2370-2470"
    - "crates/entorhinal-module/src/cli.rs:1-140"
    - "crates/entorhinal-module/src/served_vectors.rs"
    - "mutations.toml"
---

## intent
An agent belongs to a workspace in exactly one way. A workspace head stores its workspace and has no project. A head or hiree stores its project and no workspace: its workspace is always its project's current placement. An assistant has neither. This was prefrontal-core's original schema (its migration 076). It drifted when core's 2026-08-13 seed registered 29 heads' names under their own project id as a stand-in, and core's migration 082 then copied that stand-in into `agent.workspace_id`. Entorhinal ported core's post-082 shape, and the agent-identity switchover imports those rows unchanged, by operator ruling.

The design is agreed in `docs/designs/head-workspace-follows-placement.md`. This campaign implements it in entorhinal:

- Creating a head or hiree stores no workspace. Its name claim is still keyed on its project's placement (`namespace_kind = 'workspace'`, `namespace_key` = the placement), as today.
- Placing a project in a workspace, or moving it to another, carries the active name claims of its live heads and hirees into the target workspace, in the same transaction, after checking every name against the target. This replaces today's `bound_by_live_agent` refusal for placement changes.
- One operator write corrects the imported data after the switchover. It clears the stored workspace on every head and hiree, and moves every live head's or hiree's active claim that isn't keyed on its project's placement into that placement.
- Every agent change is journaled with an after-image, so verify, rebuild, `agent.changes` (prefrontal-core's replica feed) and the shared identity log carry it like any other agent write.

After release, the operator will run the correction, retire five unneeded heads with `ck agents retire`, and move three projects (`opencode`, `pi-mono`, `oh-my-pi`) out of `cortexkit` into a workspace each. Those are operator actions, not slices.

## non_goals
- Any change to the switchover import. `agent.import` stays verbatim: it writes whatever workspace and claims core holds.
- Workspace heads: they keep storing their own workspace, and removing their workspace stays refused while they're live.
- Allowing an agent-bearing project to have no workspace. Creating a head or hiree still needs a placed project (`unresolved_workspace`), and taking a project with live agents out of every workspace, or removing it, stays refused (`bound_by_live_agent`). Projects without live agents may stay unplaced, as today.
- Changing name lookup. `agent.resolve_name` without a workspace already refuses with `name_ambiguous` and lists candidates; that behaviour stays.
- Retired and merged agents: their claims are never moved, so their names stay reserved where they were.
- Prefrontal-core's replica changes. Core accepts the new op and heads with no workspace on its side (external dependency, below).
- A schema CHECK forbidding a workspace on heads. Rebuild replays imported rows that carry one, so the invariant is enforced in code and by `verify`, not by the table.

## constraints

### Create
- `agent.create` for `head` and `hiree` stores `workspace_id = NULL`. The name claim is keyed on the project's placement, exactly as now (`validate_create`). Validation order and error codes are unchanged.
- Replies that echo the agent row (`agent.create`, `resolve`, `list`, digest) then show `workspace_id: null` for heads and hirees. `agent.fleet_identity` already derives the workspace from placement and is unchanged.

### The new op: `agent.set_namespace`
- One journal op, `agent.set_namespace`, added to `IDENTITY_CHANGE_OPS`, so `agent.changes`, rebuild and the identity log all handle it. Its entry has the same shape as `agent.rename`'s: the agent's full after-image row, plus the claims it touched (the released claim and the new active claim). When only the stored workspace is cleared, the claims list is empty.
- Every `agent.set_namespace` bumps the agent's `agent_generation` and `updated_at_ms`, including a pure claim move, so every consumer sees the change.
- Replay goes through `replay_agent_entry`, like every other agent op.
- Released claims keep their history. The new claim is a normal `claims::claim` in the target namespace.

### Placement carries names
- Every operation that places a project in a workspace or moves it to another carries its live heads and hirees: `assign_workspace`, `register` with a workspace, `upgrade_implicit` with a workspace, and every other call site of `ensure_project_attachment_allowed` or `ensure_project_unbound` that changes placement (not removal). The slice lists each call site it changes and why.
- Before anything is written, every carried agent's current name is checked against the target workspace (`claims::check_name`). If any name is taken by another live claim there, the whole operation is refused with `name_conflict`, its detail naming the moving agent, the agent holding the name, and the target workspace. Nothing is journaled.
- On success, in one transaction: the project op's journal entry first, then one `agent.set_namespace` entry per carried agent, ordered by `agent_id`.
- Rebuild replays the same sequence: the project op's replay must not refuse a placement change that has live agents bound, because the `agent.set_namespace` entries that follow it carry the names.
- Unplacing a project that has live agents, removing such a project, and removing a workspace that live agents are bound to all stay refused with `bound_by_live_agent`.
- A placement change that carries agents is an agent identity write:
  - from a `Direct` caller it goes through the operator confirmation flow (`write_wait`), with a summary naming the project, the target workspace and the names that move, within `OPERATOR_SUMMARY_LIMIT`, re-checked inside the write as today (`operator_approval_stale`);
  - before the switchover it refuses with `authority_not_cut_over`;
  - on a flow-scoped route it refuses with `flow_scope_not_admitted`, as every write does.
  A placement change that carries no agents keeps today's admission with no prompt.

### The correction
- A new operator write, `agent.repair_workspaces`, admitted only from `Direct` with operator confirmation, and only after the switchover. It takes a `request_key`.
- For every agent with role `head` or `hiree`: if its stored `workspace_id` is not NULL, clear it. If it's live and its active claim isn't keyed on its project's placement, move the claim into the placement. Each changed agent gets one `agent.set_namespace` entry, ordered by `agent_id`.
- A live head or hiree whose project has no placement is refused by name (`unresolved_workspace`, naming the agent and the project), and the whole correction writes nothing. The operator retires or places those first.
- All name checks run before any write. Any collision refuses the whole correction with `name_conflict`, as above.
- Running it again when nothing needs changing writes nothing and journals nothing.
- The confirmation summary states the counts (for example "clear the stored workspace on 37 heads and hirees and move 24 names into their project's workspace") and stays within the limit.
- CLI: `ck agents repair-workspaces` prints a read-only preview by default: every agent it would change, the namespace each name moves from and to, and any refusal. `--yes` sends the write. This mirrors `ck projects attach`.

### Shared identity log
- With sharing enabled, a placement change that carries agents, and the correction, each stay one shared write. The project rows and every agent and claim after-image travel in the same entry, and a remote node applies them atomically.
- Remote apply accepts `agent.set_namespace` entries, and still refuses any entry that alters a name claim belonging to a different agent.

### Prefrontal-core ordering
- Prefrontal-core's replica applies only the op names it knows, and fails the whole `agent.changes` page on any other. Core is adding `agent.set_namespace`, and heads and hirees with no workspace, on its side.
- Entorhinal publishes one real `agent.set_namespace` entry, generated by its own handler, under `test-vectors/agent-identity-changes-v1/`, with a test that compares it byte for byte. The chair hands it to core. Release order (core first) is the chair's, not a slice's.

### Tests and fences
- Every new refusal and invariant has a test that fails when its guard is broken, and a row in `mutations.toml`.
- No test asserts real elapsed time. Every test passes on Linux, macOS and Windows CI.
- Verify and rebuild stay clean after every new path, checked in tests on a store with imported stale rows (raw SQL setup, never test-only branches in production code).

## acceptance_sketch
1. A new head and a new hiree store `workspace_id = NULL`, and their claims are keyed on their project's placement.
2. On a store holding imported heads with stale workspaces (some placed, some whose claim is keyed on their own project id), the preview lists exactly the agents that change, and the correction clears every stored workspace and moves exactly the stale claims. A second run journals nothing. Verify and rebuild are clean after it.
3. The correction refuses, writing nothing, when a live head's project is unplaced, and when two moved names collide in a workspace.
4. Moving a project with a live head and a live hiree to another workspace moves both claims, journals the project op then two `agent.set_namespace` entries, and passes verify and rebuild.
5. The same move into a workspace where the head's name is taken refuses with `name_conflict` naming both agents and the workspace, and journals nothing.
6. Placing a project with no live agents needs no prompt; the same placement with a live head from `Direct` prompts with the exact summary, and an approval that goes stale is refused.
7. Unplacing or removing a project with live agents, and removing a workspace that live agents are bound to, still refuse with `bound_by_live_agent`.
8. A retired head's claim never moves.
9. `agent.changes` from before a move carries the `agent.set_namespace` entries with the expected after-images, and the published vector matches the handler's bytes.
10. With sharing enabled on two nodes, a carried move and the correction converge on both, and a forged entry that moves another agent's claim is refused.

## open_questions
None.

## slice_hints
- s1, core: create stores no workspace; the `agent.set_namespace` op, its entry, replay, feed and identity-ops membership; the published vector.
- s2, core: placement carries names (every call site, live and replay), and the correction's core function.
- s3, module: admission and confirmation for carrying placements and for `agent.repair_workspaces`, the CLI preview and `--yes`, README, manifest descriptions, and the two-node shared-log acceptance.

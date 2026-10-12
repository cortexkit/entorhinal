# Heads and hirees take their workspace from their project

Status: direction agreed with the operator (2026-10-12). Open: where the heads of projects outside every workspace keep their names.

## The rule

An agent belongs to a workspace in exactly one way:

- A **workspace head** has no project. It stores its workspace, and that is the only stored workspace on any agent.
- A **head** or **hiree** stores its project and no workspace. Its workspace is always its project's current placement.
- An **assistant** has neither.

This is the shape prefrontal-core's first agent schema enforced (migration 076): `role IN ('head','hiree') AND project_id IS NOT NULL AND workspace_id IS NULL`.

Names stay unique per workspace. A head's or hiree's active name claim is keyed on its project's placement (`namespace_kind = 'workspace'`, `namespace_key` = the placement). Placing a project in a workspace, or moving it to another, moves the active claims of its live heads and hirees in the same transaction. Before anything changes, every name is checked against the target workspace. If one is taken, the whole placement is refused with `name_conflict`, naming both agents. This replaces today's `bound_by_live_agent` refusal for placement changes. Removing a project or a workspace that live agents are bound to stays refused.

Retired and merged agents keep their historical claims untouched, so their names stay reserved where they were.

## How the stored workspace drifted

On 2026-08-13, prefrontal-core's one-time seed created 29 heads. It couldn't resolve a project's workspace yet: that lookup was disabled until the project directory moved to entorhinal. So it left each head's workspace empty, but registered each name under the head's own project id as a stand-in (`cutover_ceremony.rs`, `ceremony_seed_row`). Two days later, core's migration 082 allowed heads to carry a workspace and filled it from the name claim's namespace. The stand-in became each seeded head's stored workspace. Heads created afterwards went through the normal create path and got their project's real placement. Entorhinal ported core's post-082 shape.

On the 2026-10-11 store copies, the 37 heads split as follows. 8 created after the seed store `cortexkit`, which matches placement. 24 seeded heads in placed projects store their own project id. 5 seeded heads in unplaced projects store their own project id.

## The correction, after the identity switchover

The switchover imports agent records unchanged. Afterwards, one entorhinal-owned change:

1. clears the stored workspace on every head and hiree;
2. moves the active claim of every live head or hiree whose claim isn't keyed on its project's placement into that placement, after the same name check.

Both are journaled with one after-image per agent, so verify, rebuild, `agent.changes` and the identity log carry them like any other agent write.

Operator rulings that shape the data:

- The 5 heads of unplaced projects (GOOSE, ORW, TMIGN, TOKSCALE, XURL) are no longer needed. They are retired, keeping their ids as tombstones.
- Nothing outside `~/Work/Projects/CortexKit` belongs in the `cortexkit` workspace. Today that means `opencode`, `pi-mono` and `oh-my-pi`, whose heads are OC, PI and OMP. Where they go is the open question above.

## Ordering with prefrontal-core

Prefrontal-core's replica applies only the `agent.changes` op names it knows. Any other op fails the whole page, which stops the replica advancing. Entorhinal publishes an example entry for every new op, core's replica serves it, and only then does entorhinal emit it. Core's replica must also accept a head or hiree with no workspace.

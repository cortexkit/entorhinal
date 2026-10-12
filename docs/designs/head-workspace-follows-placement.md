# A head's workspace follows its project's placement

Status: proposal, awaiting the operator's choice between the two options below.

## The problem

Every head, and every hiree, records the workspace it belongs to (`agent.workspace_id`). Its active name claim is keyed on the same value (`agent_name_claim.namespace_kind = 'workspace'`, `namespace_key = workspace_id`). That key is what makes a head's name unique, and what `workspace/NAME` addressing looks up.

The agent records entorhinal imports from prefrontal-core at the identity switchover don't always match placement. On the 2026-10-11 store copies, 37 heads came across:

| Group | Heads | Stored workspace | Project placement |
| --- | ---: | --- | --- |
| Correct | 8 | `cortexkit` | `cortexkit` |
| Placed, wrong key | 24 | the head's own project id | `cortexkit` |
| Unplaced | 5 | the head's own project id | none |

The switchover imports these values unchanged, by the operator's ruling: it moves authority and changes no data. The consequences after the switchover are the same as before it:

- The 24 placed heads can't be found by `cortexkit/NAME`, because their names live in a namespace named after their own project.
- The 5 unplaced heads are frozen. A rename needs the project's workspace (`unresolved_workspace`), and placing the project is refused while a live agent is bound to it (`bound_by_live_agent`, `ensure_project_attachment_allowed`). Only retiring them still works.

The guard exists for a real reason. If a project changed workspace under a live head, the head's name claim would point at a workspace the project no longer belongs to. Two heads with the same name in different workspaces could then collide on the move. So today placement is simply locked while agents are bound.

## Option A: a one-time correction, placement stays locked

Add one operator write that, for every live head and hiree whose stored workspace differs from its project's current placement:

1. checks that the agent's name is free in the placement workspace (refusing the whole correction, naming both agents, on any collision);
2. sets `workspace_id` to the placement;
3. releases the active claim and claims the same name in the placement workspace.

All of it runs in one transaction, journaled with one after-image per agent, so verify, rebuild, `agent.changes` and the identity log carry it like any other agent write. It fixes the 24 placed heads. The 5 unplaced heads stay frozen until someone retires them, and the same drift can come back for any project whose placement changes by another path.

## Option B: placement moves its agents (recommended)

Make the placement change itself carry the bound agents along, instead of refusing:

- Placing an unplaced project, or moving a placed one, re-keys every live head and hiree bound to it into the new workspace in the same transaction. It checks every name against the target workspace first, and refuses the whole placement with `name_conflict` (naming both agents) if any collides.
- Workspace heads are unchanged: they belong to their own workspace, and removing that workspace stays refused while they're live.
- Each re-keyed agent gets a journaled after-image in the same transaction, so the change feed, rebuild and the identity log see it.

A head's workspace can then never differ from its project's placement. The correction for the 24 placed heads becomes this same operation run once against their current placement, and the 5 unplaced heads are fixed by placing their projects normally (`ck workspaces assign`).

The cost is a behaviour change: moving a project to another workspace changes its agents' `workspace/NAME` address. Core's own readers already take workspace from placement, so they're unaffected.

## Common to both

- Retired and merged agents keep their historical claims untouched: their names stay reserved where they were.
- The re-key is admitted like every other agent write: relayed from prefrontal-core, or from the operator through a Touch ID prompt that names the change.
- Prefrontal-core's replica must read the re-key entry as a plain after-image update of the agent and its claims.

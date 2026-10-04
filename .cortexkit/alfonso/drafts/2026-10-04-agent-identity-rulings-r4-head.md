---
rounds: 1
integration_ref: aa2eeb0c7082d1edf88b2861d916dbebee981ddd
---
These rulings move the campaign onto the current HEAD for one final review round, then the slice plan. The scope cut in R7, R8 and R12 stands unchanged.

R15 Read evidence and cut slices at HEAD `aa2eeb0`, not `ff07aff`. Revise only the constraints section for this ruling. Add one constraint stating that, between `ff07aff` and `aa2eeb0`, the only source changes are in `crates/entorhinal-module/src/main.rs` (plus the 0.1.15 version bump): `7657d38` added flow admission (at `route.bind` the handler records a `RouteAdmission` holding the route's principal and the bind stamp's `flow_id`; `ProjectsHandler::admit` is the single admission point, running `refuse_flow_write` and then `authorize_write`), and `aa2eeb0` reworded the refused-liveness-batch warning, which stays out of scope under R14.

R16 Revise the constraints and acceptance_sketch sections for this ruling. Constraint: every new agent-identity mutating method is admitted through `ProjectsHandler::admit` with `refuse_flow_write` as its first check, and is listed in the methods `refuse_flow_write` refuses, so a flow-scoped route can never reach it. Entorhinal does not declare `flow-scopes/v1`; the daemon keeps flows away, and this refusal is the second line. Reading the calling agent from the scope stamp stays deferred to the second campaign (R12). Acceptance: a test asserts that each new mutating method answers `flow_scope_not_admitted` on a flow-scoped route, even with the `reserved:prefrontal-core` principal.

R17 The intent, non_goals and open_questions sections need no revision for R15 to R17: keep their current text exactly. The earlier "findings lack detail" decision is closed, and no question is open. Findings in this round must state their detail.

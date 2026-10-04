---
rounds: 1
integration_ref: aa2eeb0c7082d1edf88b2861d916dbebee981ddd
---
These rulings move the campaign onto the current HEAD for one final review round, then the slice plan. The scope cut in R7, R8 and R12 stands unchanged: this campaign moves agent identity into entorhinal, and the deferred list in R12 is the whole of the second campaign.

R15 Read evidence and cut slices at HEAD `aa2eeb0`, not `ff07aff`. Between the two, the only source changes are in `crates/entorhinal-module/src/main.rs` (plus the version bump to 0.1.15 in `crates/entorhinal-module/Cargo.toml` and `Cargo.lock`):
- `7657d38` added route admission for flows. At `route.bind` the handler records a `RouteAdmission` (the route's principal and the `flow_id` from the bind's scope stamp, if any). `ProjectsHandler::admit` is now the single admission point the request handler calls: it first runs `refuse_flow_write`, which refuses every mutating method on a flow-scoped route with `flow_scope_not_admitted`, and then `authorize_write` on the principal.
- `aa2eeb0` reworded the warning for a refused session-liveness batch.
R14's statement that the session-liveness receiver is out of scope still holds, and covers the reworded warning too.

R16 The flow refusal is a fixed part of admission. Any slice that changes admission (adding agent-identity methods, the relay from core, or the refusal of `Direct` identity writes) extends `ProjectsHandler::admit` and keeps `refuse_flow_write` as its first check. Every new mutating method must be in the list `refuse_flow_write` consults, so a flow-scoped route can't reach it; a test asserts that for each new mutating method. Entorhinal does not declare `flow-scopes/v1`: the daemon keeps flows away from it, and this refusal is the second line behind that. Reading the calling agent from the scope stamp stays deferred to the second campaign (R12). This campaign reads only `flow_id`, which is already done.

R17 The earlier "findings lack detail" decision is closed. No question is open. Findings in this round must state their detail, since that round's detail was lost.

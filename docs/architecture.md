# Architecture

entorhinal is the registry the other CortexKit modules ask "which project is
this directory?" and "who is this agent?". It runs as one supervised process
per Mac, under the subc daemon, with one SQLite store. This document names the
parts, follows the data through them, and lists the rules that most often
surprise a reader of the code.

## Parts

| Part | Where | What it does |
|---|---|---|
| Domain and store | `crates/entorhinal-core` | Every rule about projects, roots, workspaces, agents, names, moves and consent, and the SQLite store that holds them. Synchronous; knows nothing about subc. |
| Module | `crates/entorhinal-module/src/main.rs` | The `ck-entorhinal` process. The subc client library handles the connection to the daemon; this crate supplies the manifest, route admission, dispatch and health. |
| CLI faces | `crates/entorhinal-module/src/cli.rs` | The same binary run as `ck-projects`, `ck-workspaces`, `ck-agents` or `ck-roles` is a command-line client. With no arguments, or only `--subc <file>`, it is the module. |
| Shared-log writer | `shared_write.rs` | Runs shared writes one at a time and sends them to engram's identity log. |
| Catch-up | `catch_up.rs`, `enable.rs` | Reads the identity log and applies other Macs' entries; enables and joins the log. |
| Claim and role refresh | `claims.rs`, `roles.rs` | Background tasks that cache which Mac holds each agent and role. |
| Move driver | `move_driver.rs`, `move_source.rs`, `move_receiver.rs` | Background tasks that drive open agent moves. |
| Agent tools and consent | `tool_provider.rs`, `tool_writes.rs`, `consent.rs`, `permissions.rs` | Tools agents call, consent cards through Cingulate, grants, the audit. |
| Sessions | `sessions.rs` | Session bindings and the in-memory session liveness mirror. |
| Test stand-ins | `fake_*.rs` | In-process versions of the daemon, engram's log, records and session door, Cingulate and agent-data participants. Compiled only for tests. |

Other modules entorhinal talks to, always through daemon routes:

- **engram** stores the identity log, claim and move records, and the session
  door used during moves.
- **Cingulate** shows consent cards on the operator's phone and aggregates the
  permissions page.
- **The executive** (`prefrontal-core`) registers repositories, relays the
  operator's agent changes, and owned agent identity before it was imported
  here.
- **The orchestrator and host** (the providers of `agent-orchestrator/v1` and
  `agent-host/v1`) register sessions and feed session liveness.
- **agent-data participants** hold per-agent data and take part in moves.

## How data flows

**Startup.** The daemon spawns the module with `--subc <connection file>`.
After the handshake the daemon sends a storage descriptor; the module opens the store
(taking the single-writer lease, applying migrations) and starts the
catch-up, claim refresh, role refresh and move driver tasks. Until the store is
open, requests refuse `storage_unavailable`.

**A query.** The daemon delivers the request on a route. The module looks up
the route's admission (principal, scope stamp, flow), and runs the query on
the read connection, which sees one committed snapshot. Queries never wait for
the writer, engram or a prompt.

**A local write.** Admission, then the core runs the mutation: one SQLite
transaction appends the journal row and updates the derived tables. The reply
is stored with the request key so a retry returns the same bytes.

**A shared write** (identity log enabled). The writer lock serializes it. The
writer catches up, records a pending entry, prepares the transaction, sends
the entry to engram and commits only when engram confirms the append at the
expected position. See [identity-log-contract.md](identity-log-contract.md).

**A confirmed write.** For an operator write that needs Touch ID, the module
checks the write against a read snapshot first, asks the daemon to show the
prompt, and only then joins the write path, which checks everything again.

**An agent tool call.** The caller comes from the scope stamp. A write is
validated, then authorized by a grant, `own_hire`, or a consent card answered
through Cingulate, then re-checked and committed with its audit row. See
[consent-contract.md](consent-contract.md).

**A move.** Admission writes a local checkpoint; the move driver then talks to
participants, engram's records and session door, and the other Mac's
entorhinal through those records. See
[agent-move-contract.md](agent-move-contract.md).

## Rules that bite

1. **Reads never block.** Reads use their own WAL connection and never wait
   for the writer lock, engram, a participant or a Touch ID prompt. Health is
   computed from counters only, so the daemon's health check stays prompt even
   when a write is stuck.
2. **The journal is the truth for derived tables.** Anything `rebuild` would
   reproduce must be written through a journal row carrying the exact values.
   Machine-local state (consent, moves, sessions, caches) is deliberately
   outside the journal. See [data-model.md](data-model.md).
3. **Paths and authority never travel.** Only the tables listed in
   `shared_entry.rs` go to the identity log. Root paths, approvals, grants and
   caps stay on their Mac, and nothing received from another Mac authorizes a
   write here.
4. **Flows cannot write.** A route opened under a flow's scope refuses every
   mutation, whoever opened it.
5. **`Direct` is not the operator.** Every local process connects as
   `Direct`. Agent identity writes from `Direct` therefore need a Touch ID
   prompt; without one they refuse. See
   [module-surface.md](module-surface.md).
6. **A newer store is refused.** A binary refuses a store whose schema is
   ahead of its own; downgrades are not supported.
7. **Absent is not zero.** Fields such as `last_route_activity_ms`,
   `lastErrorAt` or a workspace root are left out when unknown. A reader must
   not treat a missing value as 0 or as "none".
8. **Identity is not residence.** Agent identity (name, role, placement) is
   shared; which Mac holds an agent is a claim in engram, cached locally and
   changed only by a move.
9. **Approval belongs to one checkout.** Approval is keyed by a binding's
   registration epoch, so a replaced checkout at the same path is never
   approved by accident. See [projects-and-roots.md](projects-and-roots.md).
10. **Session liveness is volatile.** It lives in memory, starts stale after a
    restart, and answers `unknown` until the host sends a snapshot. See
    [session-liveness-contract.md](session-liveness-contract.md).

## Health

The module reports `Ok`, `Degraded` (the store is waiting for a predecessor's
lease, the identity log is stalled or unavailable, this Mac's author identity
conflicts, or a move reported errors, rejected inbox entries or a task panic)
or `Failing` (the store is not open). Its metrics include the generation,
query counters, liveness counters, identity-log state and progress, claim
refresh counters and open moves.

## Further reading

- [data-model.md](data-model.md): tables, journal, migrations.
- [module-surface.md](module-surface.md): every operation and its admission.
- Contracts: [identity log](identity-log-contract.md),
  [agent moves](agent-move-contract.md), [consent](consent-contract.md),
  [session liveness](session-liveness-contract.md).
- Guides: [projects and roots](projects-and-roots.md), [agents](agents.md),
  [sharing](sharing.md), [moves](moves.md), [mutations](mutations.md).

# Architecture

entorhinal is CortexKit's registry of projects and agents. Other modules ask it "which project is this directory?", "who is this agent, and which machine holds it?" and "which session belongs to which agent?". It runs as one process per machine, `ck-entorhinal`, supervised by the subc daemon, with one SQLite store. Several machines that belong to one operator keep the shared part of that store in step through an encrypted log hosted by engram.

It owns:

- **Projects and checkouts:** durable project ids (`pj-…`), the folders (roots) each project has on this machine, which physical checkout a root is, and whether the operator approved it.
- **Workspaces:** named groups of projects. Agent names are unique within a workspace.
- **Agent identity:** names, roles, tags, labels, avatars and placement. Agent identity moved here from prefrontal-core in a one-time import; core keeps a read replica fed from entorhinal.
- **Residence:** which machine holds each agent or role (a claim record in engram, cached here), and moving an idle agent between machines.
- **Sessions:** which agent each host session belongs to, which runner hosts it, when it ended, and its live state from the host's liveness feed.
- **Agent tools and consent:** the tools agents call to create workspaces, projects, heads and hires, with consent cards, standing grants and an audit.
- **Packages:** the CortexKit package catalog on this machine (registries, verified package files, each agent's pinned packages) and publishing.
- **The user profile:** the operator's name, form of address and expertise, shared to every machine.

## Who talks to it

All traffic goes through daemon routes; entorhinal opens no network ports. The only network traffic it starts itself is HTTPS to package registries.

- **prefrontal-core (the executive)** registers repositories, relays agent identity changes, reads `agent.changes` into its replica, registers and ends sessions as the `agent-orchestrator/v1` provider, and feeds session liveness as the `agent-host/v1` provider.
- **Any module** reads project and agent identity. Magic Context keys memories on project ids and reads `context.*`; Plexus reads role claims; the apps read fleet identity and locations through Callosum.
- **engram** hosts the shared identity log (`agent-sync` service: `identity_log.*` and the `record.*` claim records) and the session door used to carry move data (`session-door` service: `cut.*`).
- **Cingulate** shows consent cards on the operator's phone and reads and revokes standing grants through `permissions/v1`.
- **The daemon** shows operator presence prompts (`operator.confirm`), and its `supervisor.list` and `catalog.list` tell entorhinal which modules take part in moves and consume packages.
- **agent-data participants** (any module declaring `agent-data/v1`) hold per-agent data and take part in moves. entorhinal is one of them, for package files and session bindings.
- **Catalog consumers** (modules declaring `ck-catalog-consumer-<kind>/v1`) check and prefetch packages before an install.
- **The claustrum vault** signs package publications; entorhinal never holds a signing key.

## Terms

- **Project id:** `pj-` plus 16 hex digits of a BLAKE3 hash of the name and roots, minted once at registration and stored, so it never changes when a folder moves, the repository is re-cloned or history is rewritten. An unregistered folder answers an implicit id, `pj-implicit1-…`, derived from its path.
- **Root:** one folder of a project on this machine. **Root key:** the machine-neutral name of a root, normally its owned `origin` remote (`owner/repo`); other machines match their checkout to the project through it. **Binding:** which physical checkout a root is, recorded by a random token in `.git/cortexkit-root-incarnation`. **Approval:** the operator's trust in one binding, keyed by its registration epoch, so a replaced checkout at the same path is not approved.
- **Workspace head, head, hiree:** a workspace head leads a workspace and stores it. A head leads one project; a hiree works under a head. Heads and hirees store no workspace: theirs is their project's.
- **Journal:** the append-only `registry_journal` table. Every change to a derived table is a journal row, and `verify` and `rebuild` replay the journal to reproduce those tables. **Generation:** the journal's highest sequence number. **Incarnation:** a random id per process; a cursor or token from another incarnation is not resumable.
- **Shared entry:** the after-image of the shared tables a write changed, appended to the identity log. **Identity log:** engram's append-only, encrypted log of shared entries, one per operator account. Sharing is `disabled`, `enabling`, `joining` or `enabled`.
- **Claim:** engram's record of which machine holds an agent (`claim:<agent>`) or a role (`role:<name>`), with an epoch that rises on each change. Epoch 0 means unclaimed.
- **Move and cut:** a move transfers an agent's claim and data from one machine to another. Its data travels as a cut: a sealed set of streams in engram's session door.
- **Session binding:** an agent's session reference (`ref_kind`, `ref`) mapped to the host session (`harness`, `session_id`) and the runner that hosts it.
- **Pin:** an exact package version recorded for an agent. **Catalog generation:** one machine-local set of an agent's installed packages, prepared, then activated. Not the same as the journal generation.

## Code map

| Crate | Role |
|---|---|
| `crates/entorhinal-catalog` | Pure package-catalog verification: canonical JSON, archive reading, signature and key-chain checks, status records, versions and the deterministic resolver. No store, no I/O, no async. |
| `crates/entorhinal-core` | The store and every domain rule: projects, roots, workspaces, agents, sessions, moves, consent, catalog state, the user profile, the journal, shared entries and applying remote ones. Synchronous; knows nothing about subc. |
| `crates/entorhinal-module` | The `ck-entorhinal` binary: the supervised module (manifest, route admission, dispatch, health, background tasks, every client of another module) and the `ck projects`, `ck workspaces`, `ck agents`, `ck roles` and `ck packages` commands. |

The binary picks its role from its own name: run as `ck-projects`, `ck-workspaces`, `ck-agents`, `ck-roles` or `ck-packages` (links the placement step creates from the `cli_faces` list `ck-entorhinal --manifest` prints), it is a command-line client that connects to the daemon. Run as itself with no arguments or only `--subc <file>`, it is the module.

`STRUCTURE.md` has the file map and where to add new code.

## What the module declares

- **Capabilities:** `project-identity/v1` and `agent-identity/v1` (modules that need identity declare these `required`), `bind-no-project/v1`, `permissions/v1`, `tool-provider/v1`, `agent-data/v1` and tool-provider call modes. It requires nothing: a `requires` entry on engram would make the daemon refuse every route here, reads included, while engram is down.
- **Surfaces:** the management surface of module `entorhinal` (every operation in `docs/module-surface.md`), the tool-provider role (agent tools), and the `agent-data` internal service (its own move participant).
- **Watches:** `agent.fleet_identity_watch`, `agent.where_watch` and `catalog.status_watch` are delivered as watches. At most 16 are open at once across all callers.

## State

One SQLite store in the folder the daemon's storage descriptor names, opened behind a single-writer lease. Migrations (`MIGRATIONS` in `crates/entorhinal-core/src/lib.rs`) run at open, and a binary refuses a store whose schema is newer than its own. The store's state falls into three tiers:

1. **Shared, journal-derived:** `workspace`, `project`, `workspace_document`, `project_workspace`, `project_alias`, `project_root_key`, `catalog_pin_set`, `catalog_pin`, `user_profile`, `user_expertise`, `agent`, `agent_name_claim` and `member_name`. With sharing enabled, every change to them goes to the identity log. The list is in `shared_entry.rs`.
2. **Machine-local, journal-derived:** roots, bindings, approvals, owned remotes, workspace members and workspace roots. They are journaled so `verify` holds, but never sent: paths and authority belong to one machine. `DERIVED_TABLES` in `mutations.rs` lists every journal-derived table.
3. **Machine-local, outside the journal:** session bindings and network history, the claim and role caches, move checkpoints and settings, consent requests, grants and audit, catalog registries, statuses, plans and generations, and the store's origin. The journal cannot rebuild them and nothing replicates them.

Package files live outside SQLite, in a content-addressed folder beside the store, and are re-verified on every read. At startup the module writes `engram-catalog.json` beside the store, telling engram's backup how to copy `store.db`.

## How a request runs

**Startup.** The daemon starts the module with `--subc <connection file>`. In HELLO_ACK it supplies the storage descriptor and this machine's id. The module opens the store, retrying while an exiting predecessor still holds the lease, checks that the store was made on this machine (see the foreign-store rule), and starts its background tasks. Until the store is open, requests refuse `store_unavailable`.

**Admission.** Every route's principal, scope stamp and flow come from the daemon at bind, never from the request. Each operation then passes its own rules in `ProjectsHandler::admit_with` (`main.rs`):

| Operation | Admitted from |
|---|---|
| Identity, project, session, location and context reads | any principal |
| Project and workspace writes | `Direct` (the `ck` commands) or `reserved:prefrontal-core` |
| Agent identity writes | `reserved:prefrontal-core`, or `Direct` after an operator presence prompt |
| Operator-only writes (enabling sharing, store adoption, splitting a root, registry and publisher trust, publishing, seeding, role takes, hire caps) | `Direct`, most after a presence prompt |
| `agent.move` | `Direct` after a presence prompt, the `agent-orchestrator/v1` provider (which relays moves the operator starts from the phone, with no prompt here), or `reserved:engram` for a lid-close move when the operator turned those on |
| `agent.register_session` | the provider of `agent-orchestrator/v1` or `agent-host/v1`, resolved from the daemon per route and cached |
| `agent.end_session`, `agent.set_session_network` | the provider of `agent-orchestrator/v1` |
| The session liveness feed | the provider of `agent-host/v1` |
| `user.get` | `reserved:prefrontal-core`, `reserved:callosum` or `Direct` |
| `user.set` | `reserved:prefrontal-core`, `reserved:callosum`, or `Direct` after a presence prompt |
| `permissions.list`, `permissions.revoke` | `Direct` or `reserved:cingulate` |
| `agent.audit_list` | `Direct` or `reserved:callosum` |
| `catalog.read_blob` | a module that declares a catalog consumer capability |
| `catalog.agent_generation`, `catalog.ack_activation` | `reserved:prefrontal-core` |
| Agent tools | the agent the route's scope stamp attests, with that agent's role |

A route opened under a flow's scope refuses every write (`flow_scope_not_admitted`). These checks keep modules from changing the registry by mistake. They are not a security boundary against the operator's own processes, which can connect as `Direct` or open the store file.

**A query** runs on the read-only connection, which sees one committed snapshot. It never waits for the writer, engram, a participant or a prompt.

**A local write** is one SQLite transaction on the writer: it appends the journal row and updates the derived tables. The reply is stored with the request key, so a retry with the same key returns the same bytes.

**A shared write** (sharing enabled) goes through `shared_write.rs`, which runs one at a time. The writer catches up with the log, records a pending entry, prepares the transaction on a blocking thread, asks engram to append the entry at the expected position, and commits only when engram confirms it. If confirmation does not arrive, or engram refuses the position, the transaction rolls back and the pending entry is settled later from the log.

**An operator-confirmed write** is validated against a read snapshot first, so a write that would fail never prompts. Then the daemon shows the presence prompt with no lock held, and the write path validates again, refusing `operator_approval_stale` if the change differs from what the operator approved. One prompt can be outstanding at a time.

**An agent tool call** (`tool_writes.rs`) names its caller only through the scope stamp. The write is validated, then authorized by a standing grant, the head's ownership of its own hire, or a consent card answered on the phone through Cingulate. It is then validated again and committed with its audit row.

**Session registration** binds a session reference to a host session for one agent. Registering an existing session again under a new reference, harness or runner updates the same row in place. The liveness feed's batches update an in-memory mirror; ended sessions are written back to their bindings by a background task.

**A move** is admitted in one transaction that records a checkpoint only if no new session started. On the source, the move driver has every participant freeze and export the agent's data, seals the cut in engram's session door, and publishes the move. The receiving machine picks it up from its inbox, has the participants import the data, takes the claim, and activates them. Only after activation does the source release its copy. Each step is recorded before the next, is idempotent and survives a restart. `docs/agent-move-contract.md` defines every step and refusal.

**A package install** is two tool calls. `catalog_prepare` resolves, fetches and verifies the closure and asks each consumer module to check it, building a stored plan. `catalog_install` then shows the consent card and writes the pins, the catalog generation and any publisher grant in one transaction. Core activates the generation with `catalog.ack_activation`.

## Background tasks

All start after HELLO_ACK. Each holds the store weakly and takes a handle only for one blocking job, so none keeps the database open across a network wait.

- **Catch-up** (`catch_up.rs`): every 30 s while sharing is on (`enabling`, `joining` or `enabled`), reads the identity log and applies other machines' entries.
- **Claim refresh** (`claims.rs`): reads agent claims from engram when signalled: at startup, after an engram reconnect, after an agent is created or another machine's entries are applied, on the next catch-up tick after a failed pass, and on `ck agents where --refresh`. It has no timer of its own.
- **Role refresh** (`roles.rs`): reads role claims every 30 s.
- **Move driver** (`move_driver.rs`): drives open moves, polls the move inbox every 30 s, and releases finished cuts.
- **Session upkeep** (`session_upkeep.rs`): writes end times from the liveness feed and prunes bindings that ended more than 30 days ago.
- **Catalog status poll** (`catalog_poll.rs`): refreshes package status for the registry scopes agents' pins use, at startup and then hourly at a stored random offset, backing off up to 6 h after failures. Live flows requiring capabilities also refresh every ten minutes, retry failures after one minute (doubling to ten), and become stale after fifteen minutes without confirmation.
- **Starter bundle seed** (`catalog_seed.rs`): seeds the content store once if a starter bundle is present and the store is empty.

## Rules that bite

1. **Reads never block.** Reads use their own connection and never wait for the writer lock, engram, a participant or a prompt. Health reads only atomic counters, so the daemon's health check answers even when a write is stuck.
2. **Never hold the database across a network call, or on an async worker thread.** Database work that may wait for the writer runs on the blocking pool with a deadline shorter than the caller's timeout. Holding a store handle across an engram call kept SQLite files open on Windows, and running writes on async threads once starved every reader and the health check until the daemon restarted the module.
3. **The journal is the truth for derived tables.** Anything `rebuild` would reproduce is written through a journal row carrying the exact values. Even a machine-local change that clears derived state is recorded as a journal op (`store.adopt`), so the journal alone still reproduces every derived table.
4. **Paths and authority never travel.** Only the shared tables go to the identity log. Root paths, approvals, grants, caps and catalog trust stay on their machine, and nothing received from another machine authorizes a write here.
5. **Core decodes strictly.** prefrontal-core refuses an unknown identity op in `agent.changes` and an unknown `via` on `resolve`. Before entorhinal emits a new value core reads, core must already accept it, tested against a sample entorhinal publishes under `test-vectors/`. Release 0.1.28 broke every worker launch by emitting `via: "worktree"` too early.
6. **`Direct` is not the operator.** Every local process connects as `Direct`, agent shells included. Agent identity writes, profile writes and operator-only actions from `Direct` therefore need a presence prompt, and refuse `operator_presence_unavailable` where none exists (Linux).
7. **A store from another machine is read-only.** The store records the daemon's machine id at first start. A store restored onto a different machine refuses every write (`store_from_another_machine`) and starts no background task acting as this machine until the operator adopts it with `ck projects adopt-store`.
8. **Identity is not residence.** Agent identity is shared. Which machine holds an agent is a claim in engram, cached locally and changed only by a move. With sharing `disabled`, every live agent reads as unclaimed at epoch 0.
9. **Session liveness is volatile.** It lives in memory, starts stale after a restart, and answers `unknown` until the host sends a snapshot. Moves treat `unknown` and `lapsed` as busy.
10. **Absent is not zero.** Fields such as `lastErrorAt`, a workspace root or a session's `runner` are left out when unknown. A reader must not treat a missing value as 0 or "none".
11. **Two reply shapes.** Module routes wrap success as `{"result": …}`. Tool routes answer bare `tool-provider/v1` bodies.
12. **A migration is one-way.** A binary refuses a newer store, so a release that migrates is placed with a store snapshot and rolled back by restoring it. A migration that has never shipped may be amended in place instead of adding a new one.

## Health

The module reports `Ok`; `Degraded` while it waits for a predecessor's lease, when the store is from another machine, when the identity log is stalled or failing, when this machine's author identity conflicts, or after move errors, rejected inbox entries or a move task panic; and `Failing` while the store is not open. Its metrics cover the generation, query counts and latency, `readyAfterMs`, liveness counters, identity-log state and progress, claim refresh counters, open moves, catalog polling, open watches and pending cut releases.

## Further reading

- [Data model](docs/data-model.md): tables, journal, migrations.
- [Module surface](docs/module-surface.md): every operation, its admission and reply.
- Contracts: [identity log](docs/identity-log-contract.md), [agent moves](docs/agent-move-contract.md), [consent](docs/consent-contract.md), [session liveness](docs/session-liveness-contract.md), [memory context](docs/memory-context.md), [catalog](docs/catalog.md).
- Guides: [projects and roots](docs/projects-and-roots.md), [agents](docs/agents.md), [sharing between machines](docs/sharing.md), [moving agents](docs/moves.md).
- `test-vectors/`: byte-exact sample replies other modules pin.

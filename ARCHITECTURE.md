# Architecture

entorhinal is CortexKit's registry of projects and agents. Other modules ask it "which project is this directory?", "who is this agent, and which machine holds it?" and "which session belongs to which agent?". It runs as one process per machine, `ck-entorhinal`, supervised by the subc daemon, with one SQLite store. Several machines that belong to one operator keep the shared part of that store in step through a signed, sealed log on the operator's NATS hub.

It owns:

- **Projects and checkouts:** durable project ids (`pj-…`), the folders (roots) each project has on this machine, which physical checkout a root is, and whether the operator approved it.
- **Workspaces:** named groups of projects. Agent names are unique within a workspace.
- **Agent identity:** names, roles, tags, labels, avatars and placement. Agent identity moved here from prefrontal-core in a one-time import; core keeps a read replica fed from entorhinal.
- **Residence:** which machine holds each agent or role, watched from the hub's `KV_residence` bucket. Agent claims are durable; roles are renewable leases. An idle agent moves only with its data.
- **Sessions:** which agent each host session belongs to, which runner hosts it, when it ended, and its live state from prefrontal's session-liveness bus bucket.
- **Agent tools and consent:** the tools agents call to create workspaces, projects, heads and hires, with consent cards, standing grants and an audit.
- **Packages:** the CortexKit package catalog on this machine (registries, verified package files, each agent's pinned packages) and publishing.
- **The user profile:** the operator's name, form of address and expertise, shared to every machine.

## Who talks to it

All traffic goes through daemon routes; entorhinal opens no network ports. The only network traffic it starts itself is HTTPS to package registries.

- **prefrontal (the executive)** attaches worker containers, relays agent identity changes, reads `agent.changes` into its replica, registers and ends sessions as the `agent-orchestrator/v1` provider, and writes the session-liveness bus bucket entorhinal watches.
- **Any module** reads project and agent identity. Magic Context keys memories on project ids and reads `context.*`; Plexus reads role claims; the apps read fleet identity and locations through Callosum.
- **The NATS hub** hosts `HUB_IDLOG` and `KV_residence`. Envelopes are signed using the vault and sealed under the fleet epoch key. Readers verify records before applying them locally.
- **engram** carries move data through its session door (`session-door`: `cut.*`), separate from move coordination on the hub.
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
- **Shared entry:** the after-image of the shared tables a write changed, appended to the identity log. **Identity log:** the append-only log of shared entries, one per operator account. Sharing is off by default; a machine can bootstrap an empty hub or install an existing hub registry.
- **Claim:** a verified record of which machine holds an agent or a role, served from a local watch. The hub keys are `residence.<agent>` and `role.<name>`. The served `epoch` is the claim generation, not the encryption epoch or the KV revision. Generation 0 means unclaimed.
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
- **Watches:** `agent.fleet_identity_watch`, `agent.where_watch`, `agent.sessions_watch` and `catalog.status_watch` are delivered as watches. One route holds at most 16, and at most 64 are open across all callers.

## State

One SQLite store in the folder the daemon's storage descriptor names, opened behind a single-writer lease. Migrations (`MIGRATIONS` in `crates/entorhinal-core/src/lib.rs`) run at open, and a binary refuses a store whose schema is newer than its own. The store's state falls into three tiers:

1. **Shared, journal-derived:** workspaces and documents, projects and placement, root keys, package pins, user profile and expertise, agents and their names, and machine identities and names. Their shared changes go to the hub log when sharing is enabled. The table list is in `shared_entry.rs`.
2. **Machine-local, journal-derived:** roots, bindings, approvals, owned remotes, workspace members and workspace roots. They are journaled so `verify` holds, but never sent: paths and authority belong to one machine. `DERIVED_TABLES` in `mutations.rs` lists every journal-derived table.
3. **Machine-local, outside the journal:** session bindings and network history, local role enrollment and generation floors, hub write attempts, move checkpoints and settings, consent requests, grants and audit, catalog registries, statuses, plans and generations, and the store's origin. The journal cannot rebuild them and nothing replicates them.

Package files live outside SQLite, in a content-addressed folder beside the store, and are re-verified on every read. At startup the module writes `engram-catalog.json` beside the store, telling engram's backup how to copy `store.db`.

## How a request runs

**Startup.** The daemon starts the module with `--subc <connection file>`. In HELLO_ACK it supplies the storage descriptor and this machine's id. The module opens the store, retrying while an exiting predecessor still holds the lease, checks that the store was made on this machine (rule 7 under "Rules that bite"), and starts its background tasks. While storage is not ready, requests that need it refuse `store_unavailable`; a store still sharing the old way reports `legacy_sharing_unsupported` instead.

**Admission.** Every route's principal, scope stamp and flow come from the daemon at bind, never from the request. Each operation then passes its own rules in `ProjectsHandler::admit_with` (`main.rs`):

| Operation | Admitted from |
|---|---|
| Identity, project, session, location and context reads | any principal |
| `register`, `attach_root`, `add_root`, `approve_root`, `approve_project`, `approve_projects` | `Direct` after an operator presence prompt |
| `set_workspace_root`, `approve_workspace_root` | `Direct` after an operator presence prompt; clearing a workspace root does not prompt |
| `unapprove_root`, `unapprove_project`, `unapprove_workspace_root` | `Direct`, without a prompt |
| Other project and workspace writes, including `attach_derived_parent` | Generally `Direct` (the `ck` commands) or `reserved:prefrontal`; see [the module surface](docs/module-surface.md#projects) for exceptions |
| Agent identity writes | `reserved:prefrontal`, or `Direct` after an operator presence prompt |
| Operator-only writes (enabling sharing, store adoption, splitting a root, registry and publisher trust, publishing, seeding, role takes, hire caps) | `Direct`, most after a presence prompt |
| `agent.move` | `Direct` after a presence prompt, the `agent-orchestrator/v1` provider (which relays moves the operator starts from the phone, with no prompt here), or `reserved:engram` for a lid-close move when the operator turned those on |
| `agent.register_session` | the provider of `agent-orchestrator/v1` or `agent-host/v1`, resolved from the daemon per route and cached |
| `agent.end_session`, `agent.set_session_network` | the provider of `agent-orchestrator/v1` |
| `user.get` | `reserved:prefrontal`, `reserved:callosum` or `Direct` |
| `user.set` | `reserved:prefrontal`, `reserved:callosum`, or `Direct` after a presence prompt |
| `permissions.list`, `permissions.revoke` | `Direct` or `reserved:cingulate` |
| `agent.audit_list` | `Direct` or `reserved:callosum` |
| `catalog.read_blob` | a module that declares a catalog consumer capability |
| `catalog.agent_generation`, `catalog.ack_activation` | `reserved:prefrontal` |
| Agent tools | the agent the route's scope stamp attests, with that agent's role |

A route opened under a flow's scope refuses every write (`flow_scope_not_admitted`). These checks keep modules from changing the registry by mistake. They are not a security boundary against the operator's own processes, which can connect as `Direct` or open the store file.

**A query** runs on the read-only connection, which sees one committed snapshot. It never waits for the writer, engram, a participant or a prompt.

**A local write** is one SQLite transaction on the writer: it appends the journal row and updates the derived tables. The reply is stored with the request key, so a retry with the same key returns the same bytes.

**A shared write** (sharing enabled) goes through `shared_write.rs`. The writer records the attempt and appends at the expected hub head. It applies only after the append is confirmed directly or settled from the log.

**An operator-confirmed write** is validated against a read snapshot first, so a write that would fail never prompts. Then the daemon shows the presence prompt with no lock held, and the write path validates again, refusing `operator_approval_stale` if the change differs from what the operator approved. One prompt can be outstanding at a time.

**An agent tool call** (`tool_writes.rs`) names its caller only through the scope stamp. The write is validated, then authorized by a standing grant, the head's ownership of its own hire, or a consent card answered on the phone through Cingulate. It is then validated again and committed with its audit row.

**Session registration** binds a session reference to a host session for one agent. Registering an existing session again under a new reference, harness or runner updates the same row in place. A watch of prefrontal's session-liveness bucket updates an in-memory mirror; observed deletes from an authoritative mirror queue binding end stamps for a background task.

**A move** is admitted in one transaction that records a checkpoint only if no new session started. On the source, the move driver has every participant freeze and export the agent's data, seals the cut in engram's session door, and publishes the move. The receiver sees the move on the hub watch and stages the cut through its participants. The source hands residence to the receiver, which then activates the staged data. Only after activation does the source release its copy. Each step is recorded before the next, is idempotent and survives a restart. `docs/agent-move-contract.md` defines every step and refusal.

**A package install** is two tool calls. `catalog_prepare` resolves, fetches and verifies the closure and asks each consumer module to check it, building a stored plan. `catalog_install` then shows the consent card and writes the pins, the catalog generation and any publisher grant in one transaction. Core activates the generation with `catalog.ack_activation`.

## Background tasks

All start after HELLO_ACK. Each holds the store weakly and takes a handle only for one blocking job, so none keeps the database open across a network wait.

- **Hub log and KV**: tail `HUB_IDLOG` and watch `KV_residence`. Reconnects re-establish the streams and reconcile the local view before serving fresh claims. The only periodic hub requests during healthy idle operation are renewals for held role leases, each a CAS preceded by a fleet epoch read.
- **Hub setup and recovery** (`catch_up.rs`): resumes a saved bootstrap plan, joins an existing hub from an empty registry, or starts the installed log tail.
- **Move driver** (`move_driver.rs`): watches for hub move records, drives local checkpoints and releases finished cuts.
- **Session liveness** (`liveness_bus.rs`): watches `prefrontal.session-liveness`, replays its rows and reconciliation marker, and clears and retries the mirror after an outage.
- **Session upkeep** (`session_upkeep.rs`): writes end times from observed authoritative bucket deletes and prunes bindings that ended more than 30 days ago.
- **Catalog status poll** (`catalog_poll.rs`): refreshes package status for the registry scopes agents' pins use, at startup and then hourly at a stored random offset, backing off up to 6 h after failures. Live flows requiring capabilities also refresh every ten minutes, retry failures after one minute (doubling to ten), and become stale after fifteen minutes without confirmation.
- **Starter bundle seed** (`catalog_seed.rs`): seeds the content store once if a starter bundle is present and the store is empty.
- **Bus change publisher** (`bus_changes.rs`): keeps six revision markers in the account-local `entorhinal.changes` KV bucket current after journal commits, catalog status changes and changes to the cached fleet epoch view; retries bus outages without delaying reads.

## Rules that bite

1. **Reads never block.** Reads use their own connection and never wait for the writer lock, engram, a participant or a prompt. Health reads only atomic counters, so the daemon's health check answers even when a write is stuck.
2. **Never hold the database across a network call, or on an async worker thread.** Database work that may wait for the writer runs on the blocking pool with a deadline shorter than the caller's timeout. Holding a store handle across an engram call kept SQLite files open on Windows, and running writes on async threads once starved every reader and the health check until the daemon restarted the module.
3. **The journal is the truth for derived tables.** Anything `rebuild` would reproduce is written through a journal row carrying the exact values. Even a machine-local change that clears derived state is recorded as a journal op (`store.adopt`), so the journal alone still reproduces every derived table.
4. **Paths and authority never travel.** Only the shared tables go to the identity log. Root paths, approvals, standing grants, hire caps and catalog trust stay on their machine, and nothing received from another machine authorizes a write here.
5. **Core decodes strictly.** prefrontal refuses an unknown identity op in `agent.changes` and an unknown `via` on `resolve`. Before entorhinal emits a new value core reads, core must already accept it, tested against a sample entorhinal publishes under `test-vectors/`. Release 0.1.28 broke every worker launch by emitting `via: "worktree"` too early.
6. **`Direct` is not the operator.** Every local process connects as `Direct`, agent shells included. Agent identity writes, profile writes and operator-only actions from `Direct` therefore need a presence prompt, and refuse `operator_presence_unavailable` where none exists (Linux).
7. **A store from another machine is read-only.** The store records the daemon's machine id at first start. A store restored onto a different machine refuses every write (`store_from_another_machine`) and starts no background task acting as this machine until the operator adopts it with `ck projects adopt-store`.
8. **Identity is not residence.** The identity log does not grant residence. Agent residence comes from its verified claim; role possession also needs a live lease notice. With sharing `disabled`, every live agent reads as unclaimed at epoch 0.
9. **Session liveness is volatile.** It lives in memory and trusts only a live bucket watch with a finished initial replay and a valid `_reconciled` marker. Until all three hold, session keys answer `unknown`. Moves treat `unknown` as busy. Only an observed DEL in an authoritative mirror may queue an end stamp, never absence alone.
10. **Absent is not zero.** Fields such as `lastErrorAt`, a workspace root or a session's `runner` are left out when unknown. A reader must not treat a missing value as 0 or "none".
11. **Two reply shapes.** Module routes wrap success as `{"result": …}`. Tool routes answer bare `tool-provider/v1` bodies.
12. **A migration is one-way.** A binary refuses a newer store, so a release that migrates is placed with a store snapshot and rolled back by restoring it. A migration that has never shipped may be amended in place instead of adding a new one.

## Health

The module reports `Ok`; `Degraded` while it waits for a predecessor's lease, when the store is from another machine, when the identity log is stalled or failing, or after move errors or a move task panic; and `Failing` while the store is not open. Its metrics cover the generation, query counts and latency, `readyAfterMs`, liveness counters, identity-log state and progress, hub watches and leases, open moves, catalog polling, open watches and pending cut releases.

## Further reading

- [Data model](docs/data-model.md): tables, journal, migrations.
- [Module surface](docs/module-surface.md): every operation, its admission and reply.
- Contracts: [hub log](docs/hub-log-contract.md), [agent moves](docs/agent-move-contract.md), [consent](docs/consent-contract.md), [session liveness](docs/session-liveness-contract.md), [memory context](docs/memory-context.md), [catalog](docs/catalog.md).
- Guides: [projects and roots](docs/projects-and-roots.md), [agents](docs/agents.md), [sharing between machines](docs/sharing.md), [moving agents](docs/moves.md).
- `test-vectors/`: byte-exact sample replies other modules pin.

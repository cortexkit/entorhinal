# Codebase Structure

A Cargo workspace of three crates. This file is a map; to find a specific symbol, search the code rather than this document. `ARCHITECTURE.md` explains how the parts work together.

## Workspace

```text
crates/
  entorhinal-catalog/   pure package-catalog verification and resolution (no store, no I/O)
  entorhinal-core/      the SQLite store and every domain rule; knows nothing about subc
  entorhinal-module/    the ck-entorhinal binary: the subc module and the ck command faces
docs/                   public contracts, the operation reference and operator guides
test-vectors/           byte-exact sample replies and requests that other modules pin
scripts/                check-cortexkit-layout.sh (CI's .cortexkit/ layout check)
.cortexkit/             ignored internal notes and drafts; only WORKER.md is tracked
.engram                 which ignored folders engram's backup copies
```

The dependency direction is one way: `entorhinal-module` → `entorhinal-core` → `entorhinal-catalog`. Anything that can be decided from the store alone belongs in core; anything that talks to the daemon, another module or the network belongs in the module crate. Every CortexKit dependency comes from crates.io; `crates/entorhinal-core/tests/dependency_sources.rs` fails on a path dependency outside the repository.

## `crates/entorhinal-catalog/src`

Pure functions over bytes: every byte, trust input and time is an argument. `jcs.rs` (canonical JSON, integer numbers only), `archive.rs` (strict tar), `names.rs`, `semver.rs`, `record.rs` (package records and manifests), `statements.rs` (publisher key certificates and the registry's status-key chain), `status.rs` (status records and the never-go-backwards rule), `resolver.rs` (the deterministic resolver), `bundle.rs` (the starter bundle), `publish.rs` (packing and digests). `tests/reproduction.rs` reproduces the contract vectors under `tests/vectors/ck-catalog-v1/`, copied from prefrontal with their source pinned in `SOURCE.txt`.

## `crates/entorhinal-core/src`

- `lib.rs`: `RegistryStore` (open, lease, the `MIGRATIONS` chain, the read connection) and the project reads: `resolve`, `enumerate`, `journal_tail`, implicit ids.
- `mutations.rs`: project, workspace and root writes, the journal, request-key replies, `verify`, `rebuild` and `DERIVED_TABLES`.
- `binding.rs`, `root_keys.rs`, `ownership.rs`: checkout bindings and approval, machine-neutral root keys, owned remotes.
- `agent/`: agent identity. `schema.rs` (migration 4), `validate.rs` and `limits.rs` (field rules), `names.rs` (name claims), `store.rs` (writes), `reads.rs` and `fleet.rs` (reads), `feed.rs` (`agent.changes`), `carry.rs` (workspace placement of heads and hirees), `avatars.rs`, `import.rs` (the one-time import from core), `consent.rs` (machine-local consent state, migration 10).
- `shared_entry.rs`, `remote_apply.rs`: building shared entries from after-images and applying entries from the identity log. `log_schema.rs`, `enable_state.rs`, `pending.rs`: the local log state, enabling, and pending shared writes.
- `session_lifecycle.rs`, `session_network.rs`, `session_carry.rs`, `session_residence.rs`: session bindings, their network history, carrying them in a move, and the residence answer used on restore.
- `claim_cache.rs`, `role_cache.rs`, `where_notify.rs`: cached agent and role claims, and the invalidation signal for location watches.
- `move_record.rs`, `move_store.rs`: move and inbox record codecs and the machine-local move checkpoint.
- `catalog.rs`, `catalog_trust.rs`, `catalog_grants.rs`, `catalog_agent.rs`, `catalog_resolution.rs`: registries and statuses, publisher grants, pins, plans and generations, and adapting stored pins to the pure resolver.
- `memory.rs`, `context.rs`: project domains, workspace documents and the `context.*` reads.
- `user_profile.rs`, `store_origin.rs`: the user profile, and which machine made the store.
- `read_connection.rs`: the WAL reader every query uses.
- `*_tests.rs`, `*/tests.rs`: unit tests next to the code they cover. `tests/` holds integration tests (agent import, journal replay and golden files, git metadata, rebinding, the SQLite build, Windows ACLs, dependency sources) and their fixtures. `examples/rehearse_import.rs` rehearses the identity import on store copies.

## `crates/entorhinal-module/src`

- `main.rs`: the module. Manifest, `ProjectsHandler`, route admission (`admit_with`, `authorize_write`), the two dispatch paths (`execute` for store operations, `handle_served_request` for operations that wait on a prompt, the shared writer or another module), health, startup and the background-task starts.
- `cli.rs`: the `ck projects`, `ck workspaces`, `ck agents`, `ck roles` and `ck packages` faces. `catalog_cli.rs` (the `ck packages` verbs) and `workspace_document_cli.rs` (`ck workspaces document`) are submodules of it.
- Identity log: `shared_write.rs` (one shared write at a time), `catch_up.rs`, `enable.rs`, `log_client.rs` (engram's `agent-sync` log), `member.rs` (machine display names).
- Claims and roles: `claims.rs`, `roles.rs`, `record_client.rs` (engram's `record.*`), `claim_codec.rs`.
- Agents: `agent_ops.rs` (writes), `agent_reads.rs` (reads and the `agent.changes` long-poll), `watches.rs` (the two watches).
- Sessions: `sessions.rs` (registration, end, network, reads and the liveness mirror), `session_upkeep.rs` (end times and pruning).
- Moves: `move_admission.rs`, `move_driver.rs`, `move_source.rs`, `move_receiver.rs`, `move_ops.rs` (status and operator controls), `cut_release.rs`, `participant_discovery.rs` and `participant_client.rs` (agent-data participants), `door_client.rs` (engram's session door). entorhinal's own participant data lives in `catalog_move/` (package files) and `session_move/` (session bindings).
- Agent tools and consent: `tool_provider.rs` (catalog and caller admission), `tool_writes.rs`, `consent.rs`, `consent_client.rs` (Cingulate), `permissions.rs` (`permissions/v1`, audit, hire caps).
- Catalog: `catalog_ops.rs` (registries and blob reads), `catalog_network.rs` (HTTPS client), `catalog_trust.rs` (registry verification and the pinned `CORTEXKIT_ROOT`), `catalog_blob.rs` (content-addressed files), `catalog_agent.rs` (prepare, install, activation), `catalog_consumer.rs`, `catalog_grants.rs`, `catalog_poll.rs`, `catalog_seed.rs`, `catalog_sql.rs` (blocking-pool SQL with deadlines), `catalog_publish/`.
- Other operations: `memory_context.rs`, `user_profile.rs`, `store_origin.rs`.
- Test-only (compiled under `cfg(test)`): `fake_*.rs` (in-process daemon, log, records, session door, Cingulate, participants, registry), `served_vectors.rs` (generates and checks `test-vectors/`), `two_machine_acceptance.rs` and `two_machine_acceptance/` (two stores talking through stand-ins), `cut_barrier_rehearsal.rs`, `consent_decoder.rs` (Cingulate's decoder ported as an oracle, included by `fake_cingulate.rs`), and the `*_tests.rs` files.
- `tests/`: integration tests that run the built binary (manifest, CLI faces and launch, binary names, retry quoting, Windows ACLs) and `real_daemon_e2e.rs`, which needs a sibling `subconscious` checkout and is ignored by default. `tests/fixtures/` holds frozen Cingulate and permissions samples.

## Where to add new code

- **A store rule or table:** in `entorhinal-core`, in the file that owns the area. Write it through the journal when `rebuild` must reproduce it; keep it off the journal only when it is genuinely machine-local.
- **A migration:** a `V<N>_…` constant in the owning core file and a new entry at the end of `MIGRATIONS` in `lib.rs`. A migration that has never shipped in a release is amended in place instead. Add journal-derived tables to `DERIVED_TABLES`, and shared ones to the lists in `shared_entry.rs`. Tests derive the schema version from the chain, never a literal.
- **A module operation:** the handler in the module file for its area, a dispatch arm in `execute` or `handle_served_request`, its admission in `main.rs` (`MUTATING_METHODS`, or its own rule), its manifest entry with a description, and its row in `docs/module-surface.md`. A reply other modules decode gets a sample under `test-vectors/`, generated in `served_vectors.rs`.
- **A new value core decodes** (an identity op, a `resolve` field): publish the sample and have core accept it before entorhinal emits it.
- **A CLI verb:** parse and usage text in `cli.rs` for its face; it sends an existing module operation.
- **An agent tool:** its catalog entry and capability tag in `tool_provider.rs`, its write in `tool_writes.rs`.
- **A call to another module:** a client file in the module crate, over a `SubcConsumer` connection, never the module's own registered connection. Log transport failures with the target and method. Never hold a store handle across the call.
- **A background task:** a `start_*` function called from `on_hello_ack`, holding the store slot weakly, doing SQL only on the blocking pool, skipped for a store from another machine, with a stop that tests can await.
- **A test stand-in for another module:** a `fake_*.rs` file in the module crate.

## Conventions

- Files are snake_case; tests sit beside the code as `*_tests.rs` or a `tests.rs` submodule.
- Every build, check and test runs with `--workspace`; one crate alone compiles a second copy of shared dependencies.
- Tests never touch the live store. They open scratch stores, run other modules as stand-ins, assert order and outcome rather than elapsed time, and go through `ProjectsHandler` with an admitted route when they prove an admission or wire rule.
- Test-spawned binaries run as `ckdev-…` copies, never `ck-…`.
- The vectors in `test-vectors/` are compared byte for byte; each folder's README says how to regenerate it and its `SHA256SUMS`.
- User-facing text is OS-neutral: "machine", "operator confirmation" or "presence prompt", naming Touch ID or Windows Hello only as platform examples.
- `CONTRIBUTING.md` has the gate CI runs and the commit rules.

# Security

## Reporting a vulnerability

Report security problems privately, through GitHub's private vulnerability
reporting on this repository (the **Security** tab, then **Report a
vulnerability**). Do not open a public issue, pull request or discussion for a
suspected vulnerability.

A useful report names the commit or release you tested, the operation or
command involved, what you expected entorhinal to refuse, and what it did
instead. A failing test against the serving path (`ProjectsHandler` in
`crates/entorhinal-module/src/main.rs`) is the most direct evidence.

## Threat model

entorhinal is a local module. It runs as the operator's own OS user, under the
subc daemon, on each of the operator's machines. This section says what it trusts,
what it refuses, where its state lives and what it does not defend against.
[docs/module-surface.md](docs/module-surface.md) has the admission rule for
every operation.

### What entorhinal trusts

- **The daemon's route principal.** When a caller opens a route, the subc
  daemon stamps it with a principal: `Direct` (a local client such as `ck`),
  `Reserved { module_id }` (a module the daemon supervises and has
  authenticated), or `Unverified`. entorhinal records the principal at bind
  time (`RouteAdmission::from_bind` in `main.rs`) and decides every write from
  it. A write on a route with no recorded principal is refused, never treated
  as coming from the operator. Reads are open to every caller.
- **The daemon's scope stamp.** A route opened on behalf of an agent carries a
  scope stamp naming the agent, its scope and whether it runs under a flow (an
  automation an agent installs to run without a session). The agent tools take
  the caller's identity from that stamp only; a tool argument can never name a
  different caller (`ProjectsHandler::admit_agent_tool` in `tool_provider.rs`).
- **The daemon's capability resolution.** Session registration and the session
  liveness feed are admitted from whichever module the daemon resolves as the
  sole provider of `agent-orchestrator/v1` or `agent-host/v1`, not from a fixed
  module name (`sessions.rs`).
- **engram's identity log.** Entries in the shared identity log are signed,
  encrypted and verified by engram. entorhinal receives plaintext with an
  author pseudonym and a `signed_by_self` flag, and runs its own author checks
  on top (below). It does not implement the cryptography.

### What entorhinal refuses

- **Unconfirmed `Direct` agent writes.** Every local process, including an
  agent's own shell, reaches entorhinal as `Direct`. So a `Direct` caller can
  change agent identity only for the methods in `agent_ops::CONFIRMABLE_METHODS`,
  and only after the operator approves the exact change at a presence prompt
  the daemon shows (Touch ID on macOS, Windows Hello on Windows). Linux has no
  presence provider, so on Linux these writes refuse
  `operator_presence_unavailable`. Any other `Direct` agent identity write is refused with
  `direct_identity_write_not_admitted` (`authorize_write` in `main.rs`). Hire
  caps, role takes, operator moves, move aborts and the lid-close setting use
  the same prompt. Tests: `writes_are_refused_unless_the_operator_or_the_executive_opened_the_route`,
  `relay_bytes_are_unchanged_and_unconfirmed_seams_stay_closed`.
- **Flow-scoped writes.** A route opened under a flow's scope refuses every
  mutating method with `flow_scope_not_admitted`, whatever principal opened it,
  so a flow cannot borrow the executive's write access by having it forward a
  request (`refuse_flow_write` in `main.rs`). Reads stay open. Tests:
  `a_flow_scoped_route_cannot_write_even_with_the_executives_principal`,
  `every_agent_mutation_is_flow_refused_for_every_principal_before_and_after_cutover`.
  The agent tools also refuse flow scopes, worker scopes and scopes the owner
  did not authorize, with `caller_forbidden`.
- **Forged identity-log entries.** Catch-up refuses an entry whose author is
  missing or not canonical, a member name whose embedded author differs from
  the entry's author, and a snapshot that claims to supersede entries another
  member wrote. A refused entry stops catch-up (`identity_log_stalled`); it is
  never skipped. Tests: `read_requires_author_and_catch_up_stalls`,
  `s3_member_author_mismatch_rolls_back_and_signer_is_not_author`,
  `forged_or_malformed_supersedes_stall_joiners_members_and_restarted_members`.
  See [docs/identity-log-contract.md](docs/identity-log-contract.md).
- **Shared authority.** Consent grants, consent requests, the audit, hire caps,
  move checkpoints, session bindings and cached claims are machine-local. They
  are never written to the identity log, so another machine's state never
  authorizes a write here (`consent_state_is_local_persistent_and_not_rebuilt_or_exported`).

### Where state lives

- **The store.** One SQLite database in WAL mode. The subc daemon chooses its
  path and hands it to entorhinal at startup in a storage descriptor;
  entorhinal does not pick it. The storage library (`cortexkit-store`) creates
  the store folder owner-only (mode `0700` on Unix, with `0600` files) and, on
  Windows, with a protected DACL for the current user. The Windows test
  `store_directory_is_protected_and_all_entries_have_owner_only_dacls` checks
  every file the store creates. See [docs/data-model.md](docs/data-model.md).
- **One file in each registered checkout.** Binding a root writes a random
  token to `cortexkit-root-incarnation` in the checkout's git common directory
  (`binding.rs`). It is git metadata, never tracked content, and it is how
  entorhinal notices that a checkout at a registered path was replaced.
- **Reads.** entorhinal reads each registered root's `.git` marker and git
  config to identify the checkout and its remotes. It spawns no processes and
  connects only to the subc daemon; other modules (engram, Cingulate) are
  reached through daemon routes.

### What it does not defend against

- **A compromised daemon.** The daemon mints the principals and scope stamps
  entorhinal trusts and shows the presence prompts. A daemon that lies about
  either can make entorhinal accept any write.
- **The same OS user with direct disk access.** Any process running as the
  operator's user can open the store file and change it, or rewrite a
  checkout's incarnation token. The principal checks keep a module with no
  business writing here from changing the registry by mistake; they are not a
  boundary against code running as the same user. A binary does refuse to open
  a store whose schema is ahead of its own, but it does not detect edits.
- **A compromised engram or a stolen log key.** entorhinal's author checks
  assume engram reports authors and signatures truthfully.
- **Network storage.** The store must be on a local disk. SQLite's WAL and
  shared-memory files are not supported on network shares.

### macOS privacy permissions

entorhinal needs none for its own state. A registered root under a folder macOS
protects (Desktop, Documents, Downloads, iCloud Drive) needs Files & Folders
access granted to entorhinal before its git config can be read. Without it, the
root reports no remotes and a `remotesError` carrying the OS error, and
`resolve_remote` cannot find a repository it owns.

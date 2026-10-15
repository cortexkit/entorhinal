# Projects and roots

How to register projects, approve their checkouts, attach a checkout on a
second Mac, repair a replaced checkout and choose which git remotes a root
owns. Every command here talks to the running module through the subc daemon.
The admission rules are in [module-surface.md](module-surface.md).

## Concepts

- **Project.** A durable id (`pj-…`) and a name. The id never changes, even
  when its folders move.
- **Implicit project.** A directory that is not inside any registered project
  still resolves, to an id derived from its path (`pj-implicit1-…`). It is
  marked `NOT REGISTERED` and authorizes nothing.
- **Root.** A registered folder of a project. A project can have several.
  Root paths are local to each Mac.
- **Binding.** Which checkout a root currently is. On binding, entorhinal
  writes a random token to `cortexkit-root-incarnation` in the checkout's git
  common directory and records it with a fresh registration epoch. A root's
  identity is one of `bound` (the token on disk matches), `unbound` (never
  bound), `replaced` (a different checkout is there now) or `unverifiable`
  (the token or git metadata cannot be read).
- **Approval.** Permission for autonomous work in a root. It belongs to one
  registration epoch, so it never carries over to a replaced checkout or to a
  root removed and added again. States: `approved`, `unapproved`.
- **Worker container.** A folder of worktrees derived from a root, attached to
  that root's current approved binding with `attach_derived_parent`. There is
  no `ck` command for it; the executive attaches containers.
- **Workspace.** A named group of projects, with an optional root folder the
  operator sets.

Exit codes for every `ck projects` and `ck workspaces` command: 0 success, 2
transport failure, 3 refused by the registry (the message starts with the
refusal code), 64 usage error. Directories may be relative; they are made
canonical before they are sent, and writes refuse a directory that does not
exist.

## Register a project

```sh
ck projects register my-app ~/src/my-app
```

The project gets a new id, its root is bound and starts **unapproved**.

Verify:

```sh
ck projects resolve ~/src/my-app     # project id, name, workspace
ck projects trust ~/src/my-app       # each root's identity and approval
```

A linked git worktree outside registered roots and worker containers resolves to
its main checkout's project when its shared git common directory contains a token
held by a current registered binding. The reply uses `via: "worktree"`, keeps the
queried path in `canonicalRoot`, and names the main root in `worktreeOf`. Workspace
fields follow the project, but root approval and binding fields are omitted: a
worktree is not approved merely because its main checkout is. Missing, unreadable
or unmatched tokens (including retired bindings) leave the path implicit.

Reverse: `ck projects remove <project-id>`. The id stops resolving, its
bindings are retired and approvals go with them. Removing a project that still
has live agents bound to it is refused.

## Approve a project for autonomous work

```sh
ck projects approve ~/src/my-app
```

This approves every root of the project the directory belongs to. It is
refused unless every root is identified (`bound`): a `replaced` root refuses
`root_replaced`, an unbound one `root_not_bound`. The text reply names each
folder and project (`approved <root> (project <name>, <id>)`); already approved
roots add `(unchanged)`. `unapprove` prints `unapproved` with the same folder
and project identity.

Verify: `ck projects trust ~/src/my-app` shows `approved` on every root.

Reverse: `ck projects unapprove ~/src/my-app`.

## Add another root

```sh
ck projects add-root <project-id> ~/src/my-app-docs
ck projects approve ~/src/my-app-docs
```

The new root starts unapproved. Reverse:
`ck projects remove-root <project-id> ~/src/my-app-docs`, which retires its
binding and any worker containers attached to it. Removing a project's last
root is refused (`last_root`); remove the project instead.

## Repair a replaced checkout

When a checkout at a registered path is deleted and cloned again, or replaced
by a different repository, the root reads `replaced` and its approval is gone.

```sh
ck projects trust ~/src/my-app       # identity: replaced
ck projects rebind ~/src/my-app
ck projects approve ~/src/my-app
```

`rebind` binds the checkout that is there now, keeping the project id. It never
approves; it prints the `ck projects approve` command to run. On an intact
binding it changes nothing. On an `unverifiable` root it refuses
`root_identity_unverifiable` with the reason; fix the checkout first.

Verify: `ck projects trust` shows `bound` and `approved`.

## Attach a checkout on another Mac

With the shared identity log enabled ([sharing.md](sharing.md)), projects
registered on one Mac appear on the others, without folders. Each root has a
machine-neutral **root key**: `owner/repo` from its owned GitHub remote, or a
label. To give a shared project a folder on this Mac:

```sh
ck projects attach ~/src/my-app                     # preview: matched project and key
ck projects attach ~/src/my-app --yes               # attach (unapproved)
ck projects approve ~/src/my-app
```

A checkout with an owned GitHub remote matches the project that holds its
`owner/repo` key; `--project <id>` is then only a cross-check, and a different
project refuses `root_key_conflict`. A checkout without one needs both
`--label <L>` and `--project <id>`, naming a label key that project already
has. Attaching refuses `identity_log_disabled` while the shared identity log
is off,
`root_key_not_found` when no project has the key, and `remotes_unreadable`
when the checkout's git config cannot be read. A checkout whose `owner/repo`
already belongs to another project cannot be registered as a new one
(`root_key_exists`); attach it instead.

Reverse: `ck projects remove-root <project-id> ~/src/my-app`.

## Choose which remotes a root owns

By default a root owns its `origin` remote. Owned remotes are how
`resolve_remote` finds the project for a GitHub repository and how the root
key is chosen.

```sh
ck projects owned-remotes ~/src/my-app origin upstream   # exactly these
ck projects owned-remotes ~/src/my-app --none            # none
ck projects owned-remotes ~/src/my-app --default         # back to origin
```

A bare `ck projects owned-remotes <root>` with no names is refused, so owning
nothing must be asked for by name. The names are stored; the repositories they
point to are read from the checkout's git config each time. Only remote URLs
written directly in the repository's common `config` are read: conditional
includes (`includeIf`) and URL rewrites (`insteadOf`) are not supported. A root
under a folder macOS protects needs Files & Folders access for entorhinal (see
[SECURITY.md](../SECURITY.md)); without it the root reports a `remotesError`.

Verify: `ck projects trust ~/src/my-app --json` lists each remote and whether
it is owned.

## Workspaces

```sh
ck workspaces list
ck workspaces assign <project-id> <workspace-id>
ck workspaces set-root <workspace-id> ~/src
ck workspaces clear-root <workspace-id>
```

`assign` moves the project if it is already in another workspace. If the
project has a live head or hirees, their name claims move with it; from `ck`
that change needs Touch ID, and it is refused if a moving name is taken in the
target workspace. A workspace root is only what the operator sets; when none is
set, none is reported.

Reverse: assign the project back, or run
`ck workspaces clear-root <workspace-id>`.

## Check the store

```sh
ck projects verify
```

Replays the journal on a private copy and compares every derived table,
without writing. A clean store reports `ok: true`. Drift can be repaired with
the `rebuild` operation, which replays the journal onto the live tables. There
is no `ck` command for `rebuild`; it is a recovery operation on the module
surface. See [data-model.md](data-model.md#4-verify-and-rebuild).

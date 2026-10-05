# Repository ownership

Each registered root owns the GitHub repository named by its **origin** remote
by default. Other remotes (notably a fork's upstream) are reported, not owned.
An operator can replace the owned **remote names** for one root, including with
an empty set. Names are case-sensitive, non-empty, sorted and deduplicated; they
need not exist in git yet. A missing override uses `["origin"]`; an explicit
empty override owns nothing. Removing a root deletes its override atomically.

Repository owner and repo comparisons are **case-insensitive**. Git config is
read live on every query and ownership check; repository identities are not
stored. Binding identity and approval do not determine repository ownership.
Multiple roots in the same project may own the same repository, but different
projects may not. Out-of-band git config edits can introduce ambiguity.

## Operations (entorhinal management surface)

Requests use the usual `{"method": ..., "params": ...}` envelope. Successful
replies use `{"result": ...}`. Project reads have no method prefix.

### set_owned_remotes

```json
{"method":"set_owned_remotes","params":{"root":"/canonical/root","remotes":["origin","mirror"],"requestKey":"optional-key","actor":"optional-actor"}}
```

`remotes: null` deletes the override and restores the default; `remotes: []`
stores explicit ownership of nothing. Root paths use the same canonicalization
as `remove_root`; the resulting path must be a registered root, not a subfolder.

```json
{"result":{"projectId":"pj-example","root":"/canonical/root","remotes":["mirror","origin"],"generation":12,"noop":false}}
```

`remotes` in the reply is the effective sorted set of owned **names**, including
names that currently have no GitHub remote. Reset returns `["origin"]`. Changing
default to an explicit `["origin"]` is effectful; an identical override is a
no-op. Effectful calls journal the names, request key, actor and route principal
in one transaction. Request-key retries return the original cached bytes.
Replay restores names without reading git or rechecking conflicts.

This write, like other project writes, admits `Direct` and the executive
(`reserved:prefrontal-core`). Errors: `not_found` for an unregistered root,
`invalid_params` for empty names or malformed params, `repository_owned` if any
newly effective owned remote currently points to another project's owned repo
(message: `<owner>/<repo> belongs to <other-project-id>`),
`request_key_reused_across_ops` for a key used by another op,
`write_not_permitted` for other principals, and `flow_scope_not_admitted` for
any flow-scoped write. Path resolution failures use the existing `storage_error`
mapping for path errors. Failures write nothing.

CLI: `ck projects owned-remotes <root> <name>...` owns exactly those names,
`--none` owns nothing, and `--default` resets to `origin`. A bare
`owned-remotes <root>` is refused rather than read as "own nothing".
`ck projects owned-remotes <root> --default` resets. The CLI prints the resulting
owned names; `--default` cannot be combined with names.

### resolve_remote

Open to every principal, including flow-scoped routes:

```json
{"method":"resolve_remote","params":{"owner":"UALTINOK","repo":"OpenCode"}}
```

The result has exactly one of these status-specific shapes, plus the registry
`generation` and the running module's `incarnation` on every reply:

```json
{"result":{"status":"found","projectId":"pj-example","root":"/canonical/root","generation":12,"incarnation":"module-incarnation"}}
{"result":{"status":"none","generation":12,"incarnation":"module-incarnation"}}
{"result":{"status":"ambiguous","projectIds":["pj-a","pj-b"],"generation":12,"incarnation":"module-incarnation"}}
```

Only owned GitHub remotes across registered project roots are searched. Owners
are distinct **projects**, not roots. For one owning project, `root` is its first
matching root in canonical path order. Ambiguous `projectIds` are sorted
ascending. Absence and ambiguity are successful reads, not errors. Malformed
params return `invalid_params`; storage unavailability uses the existing
`storage_unavailable` / `storage_error` errors.

## Root records and uniqueness

With root records enabled, `resolve`, `resolve_project_id` and `enumerate` retain
all existing fields and add only `owned` to each GitHub remote:

```json
{"name":"origin","owner":"ualtinok","repo":"opencode","owned":true}
{"name":"upstream","owner":"public","repo":"upstream","owned":false}
```

Attached worktree records use their source registered root's effective names;
worktrees are not additional registered owners in `resolve_remote`.

`register` (when supplying roots) and `add_root` compare incoming owned remotes
against other projects' owned remotes, case-insensitively, and refuse
`repository_owned` with the same message shape and no writes. A new root has
the origin default. Registering an existing root preserves its override.
The existing strict mutation-path errors (`not_canonical`, `root_not_found`)
continue to apply to `register` and `add_root`.
Neither check runs during replay: history is reproduced, not rejudged against
the current git configuration.
